//! Parquet layers: the same entries as columns key (DELTA_BYTE_ARRAY: prefix
//! coded), commit, new, replaced (DELTA_BINARY_PACKED), zstd, page statistics
//! and the page index (column and offset indexes), small row groups. Where
//! the page indexes start is the layer's metadata in engine state, so a cold
//! reader fetches indexes and footer in one GET.

use crate::run::{schema, Run};
use crate::store::Sparse;
use arrow_array::builder::{BinaryBuilder, Int64Builder};
use arrow_array::RecordBatch;
use bytes::Bytes;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder, RowSelection,
    RowSelector,
};
use parquet::arrow::ArrowWriter;
use parquet::basic::Type;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::metadata::page_index::PageIndexProvider;
use parquet::file::metadata::{
    PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader, SortingColumn,
};
use parquet::file::page_index::column_index::ColumnIndexMetaData;
use parquet::file::page_index::index_reader::{decode_column_index, decode_offset_index};
use parquet::file::page_index::offset_index::OffsetIndexMetaData;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::reader::{ChunkReader, Length};
use parquet::file::statistics::Statistics;
use parquet::schema::types::ColumnPath;
use std::collections::HashMap;
use std::io::Write;
use std::ops::Range;
use std::sync::Arc;

/// Key pages per read unit (`unit_pages=`).
pub static UNIT_PAGES: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(2);

#[derive(Clone, Copy, Debug)]
pub struct Conf {
    pub rg_rows: usize,
    pub page_bytes: usize,
    pub page_rows: usize,
    pub level: i32,
}

pub struct Writer<W: Write + Send> {
    w: ArrowWriter<W>,
    key: BinaryBuilder,
    commit: Int64Builder,
    new: Int64Builder,
    replaced: Int64Builder,
    n: usize,
}

const CHUNK: usize = 65536;

impl<W: Write + Send> Writer<W> {
    pub fn new(out: W, c: Conf) -> Self {
        let ints = ["commit", "new", "replaced"];
        let mut p = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(c.level).unwrap()))
            .set_dictionary_enabled(false)
            .set_statistics_enabled(EnabledStatistics::Page)
            .set_max_row_group_row_count(Some(c.rg_rows))
            .set_data_page_size_limit(c.page_bytes)
            .set_data_page_row_count_limit(c.page_rows)
            .set_write_batch_size(1024.min(c.page_rows))
            .set_column_encoding(ColumnPath::from("key"), Encoding::DELTA_BYTE_ARRAY)
            .set_sorting_columns(Some(vec![
                SortingColumn {
                    column_idx: 0,
                    descending: false,
                    nulls_first: false,
                },
                SortingColumn {
                    column_idx: 1,
                    descending: true,
                    nulls_first: false,
                },
            ]));
        for i in ints {
            p = p.set_column_encoding(ColumnPath::from(i), Encoding::DELTA_BINARY_PACKED);
        }
        Writer {
            w: ArrowWriter::try_new(out, schema(), Some(p.build())).unwrap(),
            key: BinaryBuilder::with_capacity(CHUNK, CHUNK * 48),
            commit: Int64Builder::with_capacity(CHUNK),
            new: Int64Builder::with_capacity(CHUNK),
            replaced: Int64Builder::with_capacity(CHUNK),
            n: 0,
        }
    }

    pub fn push(&mut self, key: &[u8], commit: u64, new: Option<u64>, replaced: Option<u64>) {
        self.key.append_value(key);
        self.commit.append_value(commit as i64);
        self.new.append_option(new.map(|v| v as i64));
        self.replaced.append_option(replaced.map(|v| v as i64));
        self.n += 1;
        if self.n == CHUNK {
            self.write();
        }
    }

    fn write(&mut self) {
        if self.n == 0 {
            return;
        }
        let b = RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(self.key.finish()),
                Arc::new(self.commit.finish()),
                Arc::new(self.new.finish()),
                Arc::new(self.replaced.finish()),
            ],
        )
        .unwrap();
        self.w.write(&b).unwrap();
        self.n = 0;
    }

    /// The file is written: its size and where its page indexes start.
    pub fn finish(mut self) -> (u64, u64) {
        self.write();
        let meta = self.w.finish().unwrap();
        let size = self.w.bytes_written() as u64;
        let mut start = size;
        for rg in meta.row_groups() {
            for c in rg.columns() {
                for o in [c.column_index_offset(), c.offset_index_offset()]
                    .into_iter()
                    .flatten()
                {
                    start = start.min(o as u64);
                }
            }
        }
        (size, start)
    }
}

/// A buffer the caller keeps a handle on, for an `ArrowWriter` to own.
#[derive(Clone, Default)]
pub struct Shared(pub Arc<std::sync::Mutex<Vec<u8>>>);

impl Write for Shared {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn hit(keys: Option<&[Vec<u8>]>, lo: Option<&[u8]>, hi: Option<&[u8]>, a: &[u8], b: &[u8]) -> bool {
    match keys {
        Some(ks) => {
            let i = ks.partition_point(|k| k.as_slice() < a);
            i < ks.len() && ks[i].as_slice() <= b
        }
        None => lo.is_none_or(|lo| b >= lo) && hi.is_none_or(|hi| a < hi),
    }
}

/// Fetched ranges as a Parquet chunk reader of the whole file.
pub struct Chunks {
    pub sparse: Sparse,
    pub len: u64,
}

impl Length for Chunks {
    fn len(&self) -> u64 {
        self.len
    }
}

impl ChunkReader for Chunks {
    type T = bytes::buf::Reader<Bytes>;
    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        for (s, b) in &self.sparse.parts {
            if *s <= start && start < s + b.len() as u64 {
                return Ok(bytes::Buf::reader(b.slice((start - s) as usize..)));
            }
        }
        panic!("parquet read at {start} was not fetched")
    }
    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        Ok(self.sparse.slice(start..start + length as u64))
    }
}

pub struct Meta {
    pub meta: Arc<ParquetMetaData>,
    pub arrow: ArrowReaderMetadata,
}

/// Where the footer starts, from the file's last 8 bytes.
pub fn footer_start(data: &[u8]) -> u64 {
    let n = data.len();
    let len = u32::from_le_bytes(data[n - 8..n - 4].try_into().unwrap()) as u64;
    n as u64 - 8 - len
}

/// The page indexes of some row groups: the key column's column index, and
/// every column's offset index.
#[derive(Debug, Default)]
pub struct Partial {
    ci: HashMap<usize, ColumnIndexMetaData>,
    oi: HashMap<(usize, usize), OffsetIndexMetaData>,
}

impl PageIndexProvider for Partial {
    fn has_offset_indexes(&self) -> bool {
        true
    }
    fn has_column_indexes(&self) -> bool {
        true
    }
    fn column_index(&self, rg: usize, col: usize) -> Option<&ColumnIndexMetaData> {
        if col == 0 {
            self.ci.get(&rg)
        } else {
            None
        }
    }
    fn offset_index(&self, rg: usize, col: usize) -> Option<&OffsetIndexMetaData> {
        self.oi.get(&(rg, col))
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl Meta {
    /// The footer alone, from `[footer_start, size)`: no page index yet.
    pub fn footer(tail: &[u8]) -> Arc<ParquetMetaData> {
        Arc::new(ParquetMetaDataReader::decode_metadata(&tail[..tail.len() - 8]).unwrap())
    }

    /// Row groups whose key statistics may hold keys in `[lo, hi)`, or `keys`.
    pub fn row_groups(
        meta: &ParquetMetaData,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        keys: Option<&[Vec<u8>]>,
    ) -> Vec<usize> {
        (0..meta.num_row_groups())
            .filter(|&rg| {
                let (a, b) = Self::key_bounds(meta.row_group(rg).column(0).statistics().unwrap());
                hit(keys, lo, hi, &a, &b)
            })
            .collect()
    }

    /// The byte ranges of those row groups' page indexes.
    pub fn index_ranges(meta: &ParquetMetaData, rgs: &[usize]) -> Vec<Range<u64>> {
        let mut out = Vec::new();
        for &rg in rgs {
            for (c, col) in meta.row_group(rg).columns().iter().enumerate() {
                if c == 0 {
                    let o = col.column_index_offset().unwrap() as u64;
                    out.push(o..o + col.column_index_length().unwrap() as u64);
                }
                let o = col.offset_index_offset().unwrap() as u64;
                out.push(o..o + col.offset_index_length().unwrap() as u64);
            }
        }
        out
    }

    /// The footer with those row groups' page indexes, fetched.
    pub fn with_index(meta: &ParquetMetaData, rgs: &[usize], sp: &Sparse) -> Meta {
        let mut p = Partial::default();
        for &rg in rgs {
            for (c, col) in meta.row_group(rg).columns().iter().enumerate() {
                if c == 0 {
                    let o = col.column_index_offset().unwrap() as u64;
                    let b = sp.slice(o..o + col.column_index_length().unwrap() as u64);
                    p.ci.insert(rg, decode_column_index(&b, Type::BYTE_ARRAY).unwrap());
                }
                let o = col.offset_index_offset().unwrap() as u64;
                let b = sp.slice(o..o + col.offset_index_length().unwrap() as u64);
                p.oi.insert((rg, c), decode_offset_index(&b).unwrap());
            }
        }
        let meta = Arc::new(
            meta.clone()
                .into_builder()
                .set_page_index(Some(Arc::new(p)))
                .build(),
        );
        let arrow = ArrowReaderMetadata::try_new(meta.clone(), ArrowReaderOptions::new()).unwrap();
        Meta { meta, arrow }
    }

    pub fn parse(tail: Sparse, len: u64) -> Meta {
        let r = Chunks { sparse: tail, len };
        let meta = ParquetMetaDataReader::new()
            .with_page_index_policy(PageIndexPolicy::Required)
            .parse_and_finish(&r)
            .unwrap();
        let meta = Arc::new(meta);
        let arrow = ArrowReaderMetadata::try_new(
            meta.clone(),
            ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required),
        )
        .unwrap();
        Meta { meta, arrow }
    }

    pub fn key_bounds(s: &Statistics) -> (Vec<u8>, Vec<u8>) {
        match s {
            Statistics::ByteArray(v) => (
                v.min_opt().unwrap().data().to_vec(),
                v.max_opt().unwrap().data().to_vec(),
            ),
            _ => unreachable!(),
        }
    }

    /// Each page of row group `rg`'s key column: its rows and its key bounds.
    fn key_pages(&self, rg: usize) -> Vec<(Range<u64>, Vec<u8>, Vec<u8>)> {
        let rows = self.meta.row_group(rg).num_rows() as u64;
        let pi = self.meta.page_index_for_row_group(rg);
        let oi = pi.offset_index(0).unwrap();
        let ci = pi.column_index(0).unwrap();
        let locs = oi.page_locations();
        let ColumnIndexMetaData::BYTE_ARRAY(ci) = ci else {
            unreachable!()
        };
        (0..locs.len())
            .map(|i| {
                let a = locs[i].first_row_index as u64;
                let b = locs
                    .get(i + 1)
                    .map(|l| l.first_row_index as u64)
                    .unwrap_or(rows);
                (
                    a..b,
                    ci.min_value(i).unwrap().to_vec(),
                    ci.max_value(i).unwrap().to_vec(),
                )
            })
            .collect()
    }

    /// The units to read for keys in `[lo, hi)`, or for `keys` (sorted): per
    /// row group, its row selection and the byte ranges of every column's
    /// pages that hold a selected row.
    pub fn plan(
        &self,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        keys: Option<&[Vec<u8>]>,
    ) -> Vec<(usize, RowSelection, Vec<Range<u64>>)> {
        let mut units = Vec::new();
        for rg in 0..self.meta.num_row_groups() {
            let (min, max) =
                Self::key_bounds(self.meta.row_group(rg).column(0).statistics().unwrap());
            let hit = |a: &[u8], b: &[u8]| hit(keys, lo, hi, a, b);
            if !hit(&min, &max)
                || self
                    .meta
                    .page_index_for_row_group(rg)
                    .offset_index(0)
                    .is_none()
            {
                continue;
            }
            let rows: Vec<Range<u64>> = self
                .key_pages(rg)
                .into_iter()
                .filter(|(_, a, b)| hit(a, b))
                .map(|(r, _, _)| r)
                .collect();
            let total = self.meta.row_group(rg).num_rows() as u64;
            let pi = self.meta.page_index_for_row_group(rg);
            // A unit is a few key pages' rows, as a block is ours: reads stream
            // at the same grain, and a first page stops as early.
            for group in rows.chunks(UNIT_PAGES.load(std::sync::atomic::Ordering::Relaxed).max(1)) {
                let mut sel = Vec::new();
                let mut at = 0;
                for r in group {
                    if r.start > at {
                        sel.push(RowSelector::skip((r.start - at) as usize));
                    }
                    sel.push(RowSelector::select((r.end - r.start) as usize));
                    at = r.end;
                }
                if at < total {
                    sel.push(RowSelector::skip((total - at) as usize));
                }
                let mut ranges = Vec::new();
                for c in 0..4 {
                    let locs = pi.offset_index(c).unwrap().page_locations();
                    for (i, l) in locs.iter().enumerate() {
                        let a = l.first_row_index as u64;
                        let b = locs
                            .get(i + 1)
                            .map(|l| l.first_row_index as u64)
                            .unwrap_or(total);
                        if group.iter().any(|r| r.start < b && a < r.end) {
                            ranges.push(
                                l.offset as u64..l.offset as u64 + l.compressed_page_size as u64,
                            );
                        }
                    }
                }
                units.push((rg, RowSelection::from(sel), ranges));
            }
        }
        units
    }

    pub fn decode(&self, len: u64, sparse: &Sparse, rg: usize, sel: &RowSelection) -> Vec<Run> {
        let r = Chunks {
            sparse: sparse.clone(),
            len,
        };
        let rows = self.meta.row_group(rg).num_rows() as usize;
        ParquetRecordBatchReaderBuilder::new_with_metadata(r, self.arrow.clone())
            .with_row_groups(vec![rg])
            .with_row_selection(sel.clone())
            .with_batch_size(rows.max(1))
            .build()
            .unwrap()
            .map(|b| Run::of(&b.unwrap()))
            .collect()
    }
}
