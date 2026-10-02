//! Written content as Arrow record batches, read in place: the key column,
//! and either a declared revision column or each row in the digest grammar
//! (docs/row-digest.md), whatever its physical types.

use crate::digest::{self, Digest, Entries, Scalar};
use crate::format::{Error, Result};
use crate::rows::{Arena, Versions};
use crate::sort::Keys;
use arrow_array::cast::AsArray;
use arrow_array::types::*;
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::{DataType, IntervalUnit, TimeUnit};
use rayon::prelude::*;

fn err<T>(msg: impl Into<String>) -> Result<T> {
    Err(Error::Value(msg.into()))
}

/// Row `i` of a column split into chunks: which chunk, and where in it.
struct Chunks {
    starts: Vec<usize>,
    lookup: Vec<u32>, // the chunk holding row `j << 12`, for every j
}

impl Chunks {
    fn new(lens: impl Iterator<Item = usize>) -> Chunks {
        let mut starts = vec![0];
        for n in lens {
            starts.push(starts.last().unwrap() + n);
        }
        let n = *starts.last().unwrap();
        let mut lookup = Vec::with_capacity((n >> 12) + 1);
        let mut c = 0u32;
        for j in 0..=(n >> 12) {
            while starts[c as usize + 1] <= j << 12 && (c as usize) + 2 < starts.len() {
                c += 1;
            }
            lookup.push(c);
        }
        Chunks { starts, lookup }
    }

    fn len(&self) -> usize {
        *self.starts.last().unwrap()
    }

    #[inline]
    fn locate(&self, i: usize) -> (usize, usize) {
        let mut c = self.lookup[i >> 12] as usize;
        while self.starts[c + 1] <= i {
            c += 1;
        }
        (c, i - self.starts[c])
    }
}

// -- keys -----------------------------------------------------------------------------

enum Bytes {
    Utf8(arrow_array::StringArray),
    LargeUtf8(arrow_array::LargeStringArray),
    Utf8View(arrow_array::StringViewArray),
    Binary(arrow_array::BinaryArray),
    LargeBinary(arrow_array::LargeBinaryArray),
    BinaryView(arrow_array::BinaryViewArray),
}

impl Bytes {
    fn of(a: &ArrayRef) -> Option<Bytes> {
        Some(match a.data_type() {
            DataType::Utf8 => Bytes::Utf8(a.as_string::<i32>().clone()),
            DataType::LargeUtf8 => Bytes::LargeUtf8(a.as_string::<i64>().clone()),
            DataType::Utf8View => Bytes::Utf8View(a.as_string_view().clone()),
            DataType::Binary => Bytes::Binary(a.as_binary::<i32>().clone()),
            DataType::LargeBinary => Bytes::LargeBinary(a.as_binary::<i64>().clone()),
            DataType::BinaryView => Bytes::BinaryView(a.as_binary_view().clone()),
            _ => return None,
        })
    }

    #[inline]
    fn value(&self, i: usize) -> &[u8] {
        match self {
            Bytes::Utf8(a) => a.value(i).as_bytes(),
            Bytes::LargeUtf8(a) => a.value(i).as_bytes(),
            Bytes::Utf8View(a) => a.value(i).as_bytes(),
            Bytes::Binary(a) => a.value(i),
            Bytes::LargeBinary(a) => a.value(i),
            Bytes::BinaryView(a) => a.value(i),
        }
    }
}

/// A string or binary key column, in place.
pub struct ArrowKeys {
    chunks: Vec<Bytes>,
    at: Chunks,
}

impl Keys for ArrowKeys {
    fn len(&self) -> usize {
        self.at.len()
    }
    #[inline]
    fn key(&self, i: usize) -> &[u8] {
        let (c, j) = self.at.locate(i);
        self.chunks[c].value(j)
    }
}

fn column(batches: &[RecordBatch], name: &str) -> Result<Vec<ArrayRef>> {
    let mut out = Vec::with_capacity(batches.len());
    for b in batches {
        let Some(a) = b.column_by_name(name) else {
            return err(format!("no column {name:?}"));
        };
        if a.null_count() > 0 {
            return err(format!("column {name:?} has nulls"));
        }
        out.push(a.clone());
    }
    Ok(out)
}

/// The key column: strings in place, integers as their decimal text (packed
/// once) — the rule Python rows follow (`solera.stores.key_text`). Binary
/// columns hold index keys as they are, so they are keys only with `binary`.
pub fn keys(batches: &[RecordBatch], name: &str, binary: bool) -> Result<Box<dyn Keys + Send>> {
    let cols = column(batches, name)?;
    let at = Chunks::new(cols.iter().map(|a| a.len()));
    let is_text = |a: &ArrayRef| {
        matches!(
            a.data_type(),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        )
    };
    if cols.iter().all(|a| binary || is_text(a)) {
        if let Some(chunks) = cols.iter().map(Bytes::of).collect::<Option<Vec<_>>>() {
            return Ok(Box::new(ArrowKeys { chunks, at }));
        }
    }
    let mut arena = Arena::default();
    let mut buf = Vec::new();
    for a in &cols {
        if !a.data_type().is_integer() {
            return err(format!(
                "key column {name:?} must hold strings or integers, not {}",
                a.data_type()
            ));
        }
        for i in 0..a.len() {
            buf.clear();
            text(a.as_ref(), i, &mut buf)?;
            arena.push(&buf);
        }
    }
    Ok(Box::new(arena))
}

// -- versions ---------------------------------------------------------------------------

/// A leaf value as the digest grammar sees it; `None` for a container.
fn scalar(a: &dyn Array, i: usize) -> Result<Option<Scalar<'_>>> {
    use TimeUnit::*;
    if a.is_null(i) {
        return Ok(Some(Scalar::Null));
    }
    macro_rules! int {
        ($t:ty) => {
            Scalar::Int(a.as_primitive::<$t>().value(i) as i128)
        };
    }
    let ns = |unit: &TimeUnit, v: i64| -> i128 {
        v as i128
            * match unit {
                Second => 1_000_000_000,
                Millisecond => 1_000_000,
                Microsecond => 1_000,
                Nanosecond => 1,
            }
    };
    Ok(Some(match a.data_type() {
        DataType::Null => Scalar::Null,
        DataType::Boolean => Scalar::Bool(a.as_boolean().value(i)),
        DataType::Int8 => int!(Int8Type),
        DataType::Int16 => int!(Int16Type),
        DataType::Int32 => int!(Int32Type),
        DataType::Int64 => int!(Int64Type),
        DataType::UInt8 => int!(UInt8Type),
        DataType::UInt16 => int!(UInt16Type),
        DataType::UInt32 => int!(UInt32Type),
        DataType::UInt64 => int!(UInt64Type),
        DataType::Float16 => Scalar::Float(a.as_primitive::<Float16Type>().value(i).to_f64()),
        DataType::Float32 => Scalar::Float(a.as_primitive::<Float32Type>().value(i) as f64),
        DataType::Float64 => Scalar::Float(a.as_primitive::<Float64Type>().value(i)),
        DataType::Decimal128(_, s) => Scalar::Decimal(
            a.as_primitive::<Decimal128Type>().value(i).to_string(),
            *s as i64,
        ),
        DataType::Decimal256(_, s) => Scalar::Decimal(
            a.as_primitive::<Decimal256Type>().value(i).to_string(),
            *s as i64,
        ),
        DataType::Utf8 => Scalar::Str(a.as_string::<i32>().value(i).as_bytes()),
        DataType::LargeUtf8 => Scalar::Str(a.as_string::<i64>().value(i).as_bytes()),
        DataType::Utf8View => Scalar::Str(a.as_string_view().value(i).as_bytes()),
        DataType::Binary => Scalar::Bytes(a.as_binary::<i32>().value(i)),
        DataType::LargeBinary => Scalar::Bytes(a.as_binary::<i64>().value(i)),
        DataType::BinaryView => Scalar::Bytes(a.as_binary_view().value(i)),
        DataType::FixedSizeBinary(_) => Scalar::Bytes(a.as_fixed_size_binary().value(i)),
        DataType::Date32 => Scalar::Date(a.as_primitive::<Date32Type>().value(i) as i64),
        DataType::Date64 => Scalar::Date(
            a.as_primitive::<Date64Type>()
                .value(i)
                .div_euclid(86_400_000),
        ),
        DataType::Time32(Second) => {
            Scalar::Time(a.as_primitive::<Time32SecondType>().value(i) as i64 * 1_000_000_000)
        }
        DataType::Time32(_) => {
            Scalar::Time(a.as_primitive::<Time32MillisecondType>().value(i) as i64 * 1_000_000)
        }
        DataType::Time64(Microsecond) => {
            Scalar::Time(a.as_primitive::<Time64MicrosecondType>().value(i) * 1_000)
        }
        DataType::Time64(_) => Scalar::Time(a.as_primitive::<Time64NanosecondType>().value(i)),
        DataType::Timestamp(u, tz) => {
            let v = match u {
                Second => a.as_primitive::<TimestampSecondType>().value(i),
                Millisecond => a.as_primitive::<TimestampMillisecondType>().value(i),
                Microsecond => a.as_primitive::<TimestampMicrosecondType>().value(i),
                Nanosecond => a.as_primitive::<TimestampNanosecondType>().value(i),
            };
            Scalar::Timestamp(ns(u, v), tz.is_some())
        }
        DataType::Duration(u) => {
            let v = match u {
                Second => a.as_primitive::<DurationSecondType>().value(i),
                Millisecond => a.as_primitive::<DurationMillisecondType>().value(i),
                Microsecond => a.as_primitive::<DurationMicrosecondType>().value(i),
                Nanosecond => a.as_primitive::<DurationNanosecondType>().value(i),
            };
            Scalar::Duration(ns(u, v))
        }
        DataType::Interval(IntervalUnit::YearMonth) => {
            Scalar::Interval(a.as_primitive::<IntervalYearMonthType>().value(i), 0, 0)
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            let v = a.as_primitive::<IntervalDayTimeType>().value(i);
            Scalar::Interval(0, v.days, v.milliseconds as i64 * 1_000_000)
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            let v = a.as_primitive::<IntervalMonthDayNanoType>().value(i);
            Scalar::Interval(v.months, v.days, v.nanoseconds)
        }
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::FixedSizeList(_, _)
        | DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::Struct(_)
        | DataType::Map(_, _)
        | DataType::Dictionary(_, _) => return Ok(None),
        other => {
            return err(format!(
                "cannot digest a column of type {other}: declare `revision=` on the output"
            ))
        }
    }))
}

/// The value at row `i` in the digest grammar (docs/row-digest.md).
fn value(a: &dyn Array, i: usize, out: &mut Vec<u8>) -> Result<()> {
    if let Some(s) = scalar(a, i)? {
        s.encode(out);
        return Ok(());
    }
    match a.data_type() {
        DataType::List(_) => list(out, a.as_list::<i32>().value(i).as_ref()),
        DataType::LargeList(_) => list(out, a.as_list::<i64>().value(i).as_ref()),
        DataType::FixedSizeList(_, _) => list(out, a.as_fixed_size_list().value(i).as_ref()),
        DataType::ListView(_) => list(out, a.as_list_view::<i32>().value(i).as_ref()),
        DataType::LargeListView(_) => list(out, a.as_list_view::<i64>().value(i).as_ref()),
        DataType::Struct(fields) => {
            let s = a.as_struct();
            let mut e = Entries::default();
            for (f, col) in fields.iter().zip(s.columns()) {
                value(col.as_ref(), i, e.name(f.name().as_bytes()))?;
                e.end();
            }
            e.write(out, true)
        }
        DataType::Map(_, _) => {
            let m = a.as_map();
            let (start, end) = (
                m.value_offsets()[i] as usize,
                m.value_offsets()[i + 1] as usize,
            );
            let (keys, values) = (m.keys().as_ref(), m.values().as_ref());
            let record = matches!(
                keys.data_type(),
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            );
            let mut e = Entries::default();
            let mut k = Vec::new();
            for j in start..end {
                if keys.is_null(j) {
                    return err("a map key is null");
                }
                k.clear();
                match scalar(keys, j)? {
                    Some(Scalar::Str(name)) if record => k.extend_from_slice(name),
                    _ => value(keys, j, &mut k)?,
                }
                value(values, j, e.name(&k))?;
                e.end();
            }
            e.write(out, record)
        }
        DataType::Dictionary(kt, _) => {
            let d = a.as_any_dictionary();
            let key = match kt.as_ref() {
                DataType::Int8 => a.as_dictionary::<Int8Type>().keys().value(i) as usize,
                DataType::Int16 => a.as_dictionary::<Int16Type>().keys().value(i) as usize,
                DataType::Int32 => a.as_dictionary::<Int32Type>().keys().value(i) as usize,
                DataType::Int64 => a.as_dictionary::<Int64Type>().keys().value(i) as usize,
                DataType::UInt8 => a.as_dictionary::<UInt8Type>().keys().value(i) as usize,
                DataType::UInt16 => a.as_dictionary::<UInt16Type>().keys().value(i) as usize,
                DataType::UInt32 => a.as_dictionary::<UInt32Type>().keys().value(i) as usize,
                _ => a.as_dictionary::<UInt64Type>().keys().value(i) as usize,
            };
            value(d.values().as_ref(), key, out)
        }
        _ => unreachable!("every other type is a scalar"),
    }
}

fn list(out: &mut Vec<u8>, items: &dyn Array) -> Result<()> {
    digest::list(out, items.len());
    for j in 0..items.len() {
        value(items, j, out)?;
    }
    Ok(())
}

/// The text of a scalar value, for a key or a declared revision.
fn text(a: &dyn Array, i: usize, out: &mut Vec<u8>) -> Result<()> {
    match scalar(a, i)? {
        Some(s) => s.render(out),
        None => err(format!(
            "a revision must be a scalar, not {}",
            a.data_type()
        )),
    }
}

/// A declared revision column: each row's version is its value's text.
pub struct Revision {
    chunks: Vec<ArrayRef>,
    at: Chunks,
}

impl Revision {
    pub fn new(batches: &[RecordBatch], name: &str) -> Result<Revision> {
        let chunks = column(batches, name)?;
        let at = Chunks::new(chunks.iter().map(|a| a.len()));
        Ok(Revision { chunks, at })
    }
}

impl Versions for Revision {
    fn fill(&mut self, rows: &[u32], out: &mut Arena) -> Result<()> {
        let parts: Vec<Arena> = rows
            .par_chunks(1024)
            .map(|c| {
                let (mut a, mut buf) = (Arena::default(), Vec::new());
                for &r in c {
                    let (c, j) = self.at.locate(r as usize);
                    buf.clear();
                    text(self.chunks[c].as_ref(), j, &mut buf)?;
                    a.push(&buf);
                }
                Ok(a)
            })
            .collect::<Result<_>>()?;
        for p in parts {
            out.append(p);
        }
        Ok(())
    }
}

/// Row digests (`row(r)`), the key column left out: folded into each key's group.
pub struct RowDigest {
    batches: Vec<RecordBatch>,
    columns: Vec<(Vec<u8>, usize)>, // name, index, in name order
    at: Chunks,
}

impl RowDigest {
    pub fn new(batches: Vec<RecordBatch>, skip: &[String]) -> Result<RowDigest> {
        let mut columns: Vec<(Vec<u8>, usize)> = match batches.first() {
            Some(b) => b
                .schema()
                .fields()
                .iter()
                .enumerate()
                .filter(|(_, f)| !skip.contains(f.name()))
                .map(|(i, f)| (f.name().as_bytes().to_vec(), i))
                .collect(),
            None => Vec::new(),
        };
        columns.sort();
        if let Some(w) = columns.windows(2).find(|w| w[0].0 == w[1].0) {
            return err(format!(
                "column {:?} appears twice",
                String::from_utf8_lossy(&w[0].0)
            ));
        }
        let at = Chunks::new(batches.iter().map(|b| b.num_rows()));
        Ok(RowDigest {
            batches,
            columns,
            at,
        })
    }

    /// The row's record, its fields already in name order: each value is
    /// encoded into `values`, then the ones not null are framed into `buf`.
    fn digest(&self, row: usize, buf: &mut Vec<u8>, values: &mut Values) -> Result<Digest> {
        let (c, j) = self.at.locate(row);
        let b = &self.batches[c];
        let (data, spans) = values;
        data.clear();
        spans.clear();
        for (k, (_, i)) in self.columns.iter().enumerate() {
            let start = data.len();
            value(b.column(*i).as_ref(), j, data)?;
            if &data[start..] != b"n" {
                spans.push((k, start, data.len()));
            }
        }
        buf.clear();
        buf.extend_from_slice(&digest::ROW);
        buf.push(b'r');
        crate::format::put_varint(buf, spans.len() as u64);
        for &(k, start, end) in spans.iter() {
            digest::put_len(buf, &self.columns[k].0);
            buf.extend_from_slice(&data[start..end]);
        }
        Ok(digest::framed(buf))
    }
}

/// A row's encoded values, and the column, start and end of each not null.
type Values = (Vec<u8>, Vec<(usize, usize, usize)>);

impl Versions for RowDigest {
    fn fill(&mut self, rows: &[u32], out: &mut Arena) -> Result<()> {
        let parts: Vec<Vec<Digest>> = rows
            .par_chunks(1024)
            .map(|c| {
                let (mut buf, mut values) = (Vec::new(), Values::default());
                c.iter()
                    .map(|&r| self.digest(r as usize, &mut buf, &mut values))
                    .collect()
            })
            .collect::<Result<_>>()?;
        for d in parts.iter().flatten() {
            out.push(d);
        }
        Ok(())
    }

    fn rows(&self) -> bool {
        true
    }
}
