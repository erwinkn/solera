//! Stamped layers (docs/key-index-from-first-principles.md), the phase 2
//! prototype (W57, T31): bench code on branch `design/key-index-fp`.
//!
//! Two block formats, one container. A block on disk is a 13-byte header
//! (compressed length, CRC-32 of the compressed bytes, raw length, all u32
//! little-endian, then a format byte) and zstd-compressed raw bytes. Files
//! are blocks back to back: no index, filter or footer. Writers hand the
//! block boundaries back to the caller, which keeps them as the layer's
//! index (`index_decode`).
//!
//! - A **delta** block (format 0) holds a commit's entries: key, change kind
//!   (added, updated, removed) and a source's payload. Its stamp is the
//!   commit's generation, which the caller passes in when reading it.
//! - A **layer** block (format 1) holds stamped entries: key, present at the
//!   layer's end, stamp (the generation of the key's last change), flips
//!   (the generations where it was added or removed, newest first) and a
//!   payload. Stamps are stored from the block's smallest.
//!
//! Kernels, all over byte strings the caller holds, inputs listed as
//! `(chunks, stamp)`: a chunk is whole blocks back to back, in key order;
//! `stamp` is a delta's generation (ignored for layer blocks).
//!
//! - `layers_merge`: inputs oldest first; per key the newest state, flips
//!   united, those at or below the cut dropped; with `bottom`, absent keys go
//!   to the graveyard writer, or away if no flip is left.
//! - `layers_scan`: Δ(P, H) over inputs newest first: per key with an entry
//!   stamped after g(P), its presence at P (presence at H flipped once per
//!   flip after g(P)) and at H, its stamp and payload; keys absent at both
//!   ends are left out. P = None is −∞: present keys only.
//! - `layers_lookup`: the newest entry of each of the given keys.

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedBytes;
use pyo3::types::{PyBytes, PyList};
use std::cmp::Ordering;
use std::collections::BinaryHeap;

pub const DELTA: u8 = 0;
pub const LAYER: u8 = 1;
const HEADER: usize = 13;

pub const ADDED: u8 = 0;
pub const UPDATED: u8 = 1;
pub const REMOVED: u8 = 2;
const KIND_MASK: u8 = 3;
const KIND_PAYLOAD: u8 = 4;

const PRESENT: u8 = 1;
const PAYLOAD: u8 = 2;
const FLIPS: u8 = 4;
const START: u8 = 8;

type Res<T> = Result<T, String>;

fn err(e: String) -> PyErr {
    PyValueError::new_err(e)
}

#[derive(Clone, Debug, PartialEq)]
pub struct Entry {
    pub key: Vec<u8>,
    pub present: bool,
    pub start: bool, // present just before the layer's first commit
    pub stamp: u64,
    pub flips: Vec<u64>, // newest first, each <= stamp
    pub payload: Option<Vec<u8>>,
}

// -- varints and blocks -------------------------------------------------------------

fn put_varint(out: &mut Vec<u8>, mut n: u64) {
    while n >= 0x80 {
        out.push((n as u8) | 0x80);
        n >>= 7;
    }
    out.push(n as u8);
}

fn get_varint(buf: &[u8], pos: &mut usize) -> Res<u64> {
    let mut n = 0u64;
    let mut shift = 0;
    loop {
        let b = *buf.get(*pos).ok_or("truncated varint")?;
        *pos += 1;
        n |= ((b & 0x7f) as u64) << shift;
        if b < 0x80 {
            return Ok(n);
        }
        shift += 7;
        if shift > 63 {
            return Err("varint too long".into());
        }
    }
}

fn get_bytes<'a>(buf: &'a [u8], pos: &mut usize, n: usize) -> Res<&'a [u8]> {
    let s = buf.get(*pos..*pos + n).ok_or("truncated block")?;
    *pos += n;
    Ok(s)
}

fn shared(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b).take_while(|(x, y)| x == y).count()
}

fn put_key(out: &mut Vec<u8>, prev: &[u8], key: &[u8]) {
    let s = shared(prev, key);
    put_varint(out, s as u64);
    put_varint(out, (key.len() - s) as u64);
    out.extend_from_slice(&key[s..]);
}

fn get_key(buf: &[u8], pos: &mut usize, prev: &[u8]) -> Res<Vec<u8>> {
    let s = get_varint(buf, pos)? as usize;
    let l = get_varint(buf, pos)? as usize;
    if s > prev.len() {
        return Err("shared prefix past the previous key".into());
    }
    let mut k = Vec::with_capacity(s + l);
    k.extend_from_slice(&prev[..s]);
    k.extend_from_slice(get_bytes(buf, pos, l)?);
    Ok(k)
}

fn encode_layer_block(entries: &[Entry]) -> Vec<u8> {
    let mut out = Vec::new();
    put_varint(&mut out, entries.len() as u64);
    let gmin = entries.iter().map(|e| e.stamp).min().unwrap_or(0);
    put_varint(&mut out, gmin);
    let mut prev: &[u8] = &[];
    for e in entries {
        put_key(&mut out, prev, &e.key);
        let mut f = if e.present { PRESENT } else { 0 };
        if e.payload.is_some() {
            f |= PAYLOAD;
        }
        if !e.flips.is_empty() {
            f |= FLIPS;
        }
        if e.start {
            f |= START;
        }
        out.push(f);
        put_varint(&mut out, e.stamp - gmin);
        if !e.flips.is_empty() {
            put_varint(&mut out, e.flips.len() as u64);
            let mut last = e.stamp;
            for &x in &e.flips {
                put_varint(&mut out, last - x);
                last = x;
            }
        }
        if let Some(p) = &e.payload {
            put_varint(&mut out, p.len() as u64);
            out.extend_from_slice(p);
        }
        prev = &e.key;
    }
    out
}

/// A delta entry's kind, from its stamped form: one flip at its stamp is
/// an add (present) or a remove (absent); none is an update.
fn kind_of(e: &Entry) -> u8 {
    match (e.present, e.flips.is_empty()) {
        (true, true) => UPDATED,
        (true, false) => ADDED,
        (false, _) => REMOVED,
    }
}

fn encode_delta_block(entries: &[Entry]) -> Vec<u8> {
    let mut out = Vec::new();
    put_varint(&mut out, entries.len() as u64);
    let mut prev: &[u8] = &[];
    for e in entries {
        put_key(&mut out, prev, &e.key);
        let mut k = kind_of(e);
        if e.payload.is_some() {
            k |= KIND_PAYLOAD;
        }
        out.push(k);
        if let Some(p) = &e.payload {
            put_varint(&mut out, p.len() as u64);
            out.extend_from_slice(p);
        }
        prev = &e.key;
    }
    out
}

fn decode_block(format: u8, raw: &[u8], stamp: u64, out: &mut Vec<Entry>) -> Res<()> {
    let mut pos = 0;
    let n = get_varint(raw, &mut pos)? as usize;
    let gmin = if format == LAYER { get_varint(raw, &mut pos)? } else { 0 };
    let mut prev: Vec<u8> = Vec::new();
    for _ in 0..n {
        let key = get_key(raw, &mut pos, &prev)?;
        let f = *raw.get(pos).ok_or("truncated block")?;
        pos += 1;
        let e = if format == DELTA {
            let kind = f & KIND_MASK;
            let payload = if f & KIND_PAYLOAD != 0 {
                let l = get_varint(raw, &mut pos)? as usize;
                Some(get_bytes(raw, &mut pos, l)?.to_vec())
            } else {
                None
            };
            Entry {
                key: key.clone(),
                present: kind != REMOVED,
                start: kind != ADDED,
                stamp,
                flips: if kind == UPDATED { vec![] } else { vec![stamp] },
                payload,
            }
        } else {
            let s = gmin + get_varint(raw, &mut pos)?;
            let mut flips = Vec::new();
            if f & FLIPS != 0 {
                let m = get_varint(raw, &mut pos)? as usize;
                let mut last = s;
                for _ in 0..m {
                    last = last.checked_sub(get_varint(raw, &mut pos)?).ok_or("flip past zero")?;
                    flips.push(last);
                }
            }
            let payload = if f & PAYLOAD != 0 {
                let l = get_varint(raw, &mut pos)? as usize;
                Some(get_bytes(raw, &mut pos, l)?.to_vec())
            } else {
                None
            };
            Entry { key: key.clone(), present: f & PRESENT != 0, start: f & START != 0, stamp: s, flips, payload }
        };
        prev = key;
        out.push(e);
    }
    Ok(())
}

/// Parse one block at `pos` of `buf`: (format, raw bytes, the next position).
fn read_block(buf: &[u8], pos: usize) -> Res<(u8, Vec<u8>, usize)> {
    let h = buf.get(pos..pos + HEADER).ok_or("truncated block header")?;
    let clen = u32::from_le_bytes(h[0..4].try_into().unwrap()) as usize;
    let crc = u32::from_le_bytes(h[4..8].try_into().unwrap());
    let rlen = u32::from_le_bytes(h[8..12].try_into().unwrap()) as usize;
    let format = h[12];
    let data = buf.get(pos + HEADER..pos + HEADER + clen).ok_or("truncated block")?;
    if crc32fast::hash(data) != crc {
        return Err(format!("block checksum mismatch at offset {pos}"));
    }
    let raw = zstd::bulk::decompress(data, rlen).map_err(|e| e.to_string())?;
    if raw.len() != rlen {
        return Err("block raw length mismatch".into());
    }
    Ok((format, raw, pos + HEADER + clen))
}

// -- the writer -------------------------------------------------------------------------

struct IndexEntry {
    file: u32,
    offset: u64,
    length: u32,
    first: Vec<u8>,
    max_stamp: u64,
    count: u32,
}

/// Entries in key order, as blocks of about `block_size` raw bytes, in files
/// of about `file_limit` bytes; `finish` hands back the files and the block
/// boundaries (the layer's index).
#[pyclass(module = "solera._native")]
pub struct LayerWriter {
    format: u8,
    block_size: usize,
    level: i32,
    file_limit: usize,
    pending: Vec<Entry>,
    pending_raw: usize,
    files: Vec<(Vec<u8>, u64)>,
    index: Vec<IndexEntry>,
    last: Option<Vec<u8>>,
    entries: u64,
}

impl LayerWriter {
    pub fn create(format: u8, block_size: usize, level: i32, file_limit: usize) -> Self {
        LayerWriter {
            format,
            block_size,
            level,
            file_limit,
            pending: Vec::new(),
            pending_raw: 0,
            files: vec![(Vec::new(), 0)],
            index: Vec::new(),
            last: None,
            entries: 0,
        }
    }

    pub fn push(&mut self, e: Entry) -> Res<()> {
        if let Some(l) = &self.last {
            if e.key.as_slice() <= l.as_slice() {
                return Err("keys must be sorted and unique".into());
            }
        }
        self.last = Some(e.key.clone());
        self.pending_raw += e.key.len() + 6 + 3 * e.flips.len() + e.payload.as_ref().map_or(0, |p| p.len() + 2);
        self.pending.push(e);
        self.entries += 1;
        if self.pending_raw >= self.block_size {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Res<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let raw = if self.format == LAYER {
            encode_layer_block(&self.pending)
        } else {
            encode_delta_block(&self.pending)
        };
        let data = zstd::bulk::compress(&raw, self.level).map_err(|e| e.to_string())?;
        if self.files.last().unwrap().0.len() >= self.file_limit {
            self.files.push((Vec::new(), 0));
        }
        let file = self.files.len() - 1;
        let (buf, n) = self.files.last_mut().unwrap();
        let offset = buf.len() as u64;
        buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
        buf.extend_from_slice(&crc32fast::hash(&data).to_le_bytes());
        buf.extend_from_slice(&(raw.len() as u32).to_le_bytes());
        buf.push(self.format);
        buf.extend_from_slice(&data);
        *n += self.pending.len() as u64;
        self.index.push(IndexEntry {
            file: file as u32,
            offset,
            length: (HEADER + data.len()) as u32,
            first: self.pending[0].key.clone(),
            max_stamp: self.pending.iter().map(|e| e.stamp).max().unwrap_or(0),
            count: self.pending.len() as u32,
        });
        self.pending.clear();
        self.pending_raw = 0;
        Ok(())
    }

    /// (files as (bytes, entries), index bytes); empty files are left out.
    pub fn finish_raw(&mut self) -> Res<(Vec<(Vec<u8>, u64)>, Vec<u8>)> {
        self.flush()?;
        let files: Vec<(Vec<u8>, u64)> = std::mem::take(&mut self.files).into_iter().filter(|f| f.1 > 0).collect();
        let mut ix = Vec::new();
        put_varint(&mut ix, self.index.len() as u64);
        let mut prev: &[u8] = &[];
        for b in &self.index {
            put_varint(&mut ix, b.file as u64);
            put_varint(&mut ix, b.offset);
            put_varint(&mut ix, b.length as u64);
            put_key(&mut ix, prev, &b.first);
            put_varint(&mut ix, b.max_stamp);
            put_varint(&mut ix, b.count as u64);
            prev = &b.first;
        }
        Ok((files, ix))
    }
}

fn finished<'py>(py: Python<'py>, w: &mut LayerWriter) -> PyResult<(Bound<'py, PyList>, Bound<'py, PyBytes>)> {
    let (files, ix) = py.detach(|| w.finish_raw()).map_err(err)?;
    let out = PyList::empty(py);
    for (data, n) in files {
        out.append((PyBytes::new(py, &data), n))?;
    }
    Ok((out, PyBytes::new(py, &ix)))
}

#[pymethods]
impl LayerWriter {
    #[new]
    #[pyo3(signature = (format, *, block_size=16384, level=1, file_limit=67108864))]
    fn new(format: u8, block_size: usize, level: i32, file_limit: usize) -> PyResult<Self> {
        if format != DELTA && format != LAYER {
            return Err(err(format!("unknown format {format}")));
        }
        Ok(LayerWriter::create(format, block_size, level, file_limit))
    }

    /// A delta's entries: keys, one kind byte each (0 added, 1 updated,
    /// 2 removed), payloads or None.
    #[pyo3(signature = (keys, kinds, payloads=None))]
    fn add_delta(&mut self, keys: Vec<PyBackedBytes>, kinds: PyBackedBytes, payloads: Option<Vec<Option<PyBackedBytes>>>) -> PyResult<()> {
        if kinds.len() != keys.len() || payloads.as_ref().is_some_and(|p| p.len() != keys.len()) {
            return Err(err("keys, kinds and payloads differ in length".into()));
        }
        for (i, k) in keys.iter().enumerate() {
            let kind = kinds[i];
            if kind > REMOVED {
                return Err(err(format!("unknown kind {kind}")));
            }
            let payload = payloads.as_ref().and_then(|p| p[i].as_ref().map(|b| b.to_vec()));
            let e = Entry {
                key: k.to_vec(),
                present: kind != REMOVED,
                start: kind != ADDED,
                stamp: 0,
                flips: if kind == UPDATED { vec![] } else { vec![0] },
                payload,
            };
            self.push(e).map_err(err)?;
        }
        Ok(())
    }

    /// Present keys with one stamp and no flips (a first load), as an arena:
    /// `data` and `offsets` (n + 1 little-endian u64).
    fn add_arena(&mut self, py: Python<'_>, data: PyBackedBytes, offsets: PyBackedBytes, stamp: u64) -> PyResult<()> {
        let off: Vec<u64> = offsets.chunks_exact(8).map(|c| u64::from_le_bytes(c.try_into().unwrap())).collect();
        py.detach(|| {
            for w in off.windows(2) {
                let key = data[w[0] as usize..w[1] as usize].to_vec();
                self.push(Entry { key, present: true, start: false, stamp, flips: vec![], payload: None })?;
            }
            Ok(())
        })
        .map_err(err)
    }

    fn entries(&self) -> u64 {
        self.entries
    }

    fn finish<'py>(&mut self, py: Python<'py>) -> PyResult<(Bound<'py, PyList>, Bound<'py, PyBytes>)> {
        finished(py, self)
    }
}

/// A layer index: (first keys, files, offsets, lengths, newest stamps,
/// entry counts), one per block.
#[pyfunction]
#[allow(clippy::type_complexity)]
pub fn index_decode<'py>(
    py: Python<'py>,
    data: PyBackedBytes,
) -> PyResult<(Bound<'py, PyList>, Vec<u32>, Vec<u64>, Vec<u32>, Vec<u64>, Vec<u32>)> {
    let mut pos = 0;
    let n = get_varint(&data, &mut pos).map_err(err)? as usize;
    let (mut keys, mut files, mut offs, mut lens, mut stamps, mut counts) =
        (Vec::with_capacity(n), Vec::with_capacity(n), Vec::with_capacity(n), Vec::with_capacity(n), Vec::with_capacity(n), Vec::with_capacity(n));
    let mut prev: Vec<u8> = Vec::new();
    let r: Res<()> = (|| {
        for _ in 0..n {
            files.push(get_varint(&data, &mut pos)? as u32);
            offs.push(get_varint(&data, &mut pos)?);
            lens.push(get_varint(&data, &mut pos)? as u32);
            let k = get_key(&data, &mut pos, &prev)?;
            stamps.push(get_varint(&data, &mut pos)?);
            counts.push(get_varint(&data, &mut pos)? as u32);
            keys.push(k.clone());
            prev = k;
        }
        Ok(())
    })();
    r.map_err(err)?;
    let list = PyList::new(py, keys.iter().map(|k| PyBytes::new(py, k)))?;
    Ok((list, files, offs, lens, stamps, counts))
}

// -- streams over inputs ---------------------------------------------------------------

/// The entries of one input, in key order: whole blocks back to back, in
/// one or more chunks; a delta block's entries take `stamp`.
pub struct Stream<'a> {
    chunks: Vec<&'a [u8]>,
    ci: usize,
    pos: usize,
    stamp: u64,
    buf: Vec<Entry>,
    bi: usize,
}

impl<'a> Stream<'a> {
    pub fn new(chunks: Vec<&'a [u8]>, stamp: u64) -> Self {
        Stream { chunks, ci: 0, pos: 0, stamp, buf: Vec::new(), bi: 0 }
    }

    fn next(&mut self) -> Res<Option<Entry>> {
        loop {
            if self.bi < self.buf.len() {
                let e = std::mem::replace(
                    &mut self.buf[self.bi],
                    Entry { key: vec![], present: false, start: false, stamp: 0, flips: vec![], payload: None },
                );
                self.bi += 1;
                return Ok(Some(e));
            }
            if self.ci >= self.chunks.len() {
                return Ok(None);
            }
            let chunk = self.chunks[self.ci];
            if self.pos >= chunk.len() {
                self.ci += 1;
                self.pos = 0;
                continue;
            }
            let (format, raw, next) = read_block(chunk, self.pos)?;
            self.pos = next;
            self.buf.clear();
            self.bi = 0;
            decode_block(format, &raw, self.stamp, &mut self.buf)?;
        }
    }
}

/// Heap item: the smallest key first, then the input with the lowest rank.
struct Head {
    key: Vec<u8>,
    rank: usize,
}

impl PartialEq for Head {
    fn eq(&self, o: &Self) -> bool {
        self.key == o.key && self.rank == o.rank
    }
}
impl Eq for Head {}
impl PartialOrd for Head {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for Head {
    fn cmp(&self, o: &Self) -> Ordering {
        o.key.cmp(&self.key).then(o.rank.cmp(&self.rank))
    }
}

/// A k-way merge of streams by key: `next_group` returns every stream's
/// entry for the next key, as (rank, entry), rank ascending.
struct Merger<'a> {
    streams: Vec<Stream<'a>>,
    heads: Vec<Option<Entry>>,
    heap: BinaryHeap<Head>,
}

impl<'a> Merger<'a> {
    fn new(mut streams: Vec<Stream<'a>>) -> Res<Self> {
        let mut heads = Vec::with_capacity(streams.len());
        let mut heap = BinaryHeap::new();
        for (rank, s) in streams.iter_mut().enumerate() {
            let e = s.next()?;
            if let Some(e) = &e {
                heap.push(Head { key: e.key.clone(), rank });
            }
            heads.push(e);
        }
        Ok(Merger { streams, heads, heap })
    }

    fn next_group(&mut self, group: &mut Vec<(usize, Entry)>) -> Res<bool> {
        group.clear();
        let Some(top) = self.heap.pop() else { return Ok(false) };
        let key = top.key;
        let mut rank = top.rank;
        loop {
            let e = self.heads[rank].take().unwrap();
            let nxt = self.streams[rank].next()?;
            if let Some(n) = &nxt {
                if n.key <= e.key {
                    return Err("an input is not sorted".into());
                }
                self.heap.push(Head { key: n.key.clone(), rank });
            }
            self.heads[rank] = nxt;
            group.push((rank, e));
            match self.heap.peek() {
                Some(h) if h.key == key => rank = self.heap.pop().unwrap().rank,
                _ => break,
            }
        }
        Ok(true)
    }
}

type Input = (Vec<PyBackedBytes>, u64);

fn streams(inputs: &[Input]) -> Vec<Stream<'_>> {
    inputs.iter().map(|(chunks, stamp)| Stream::new(chunks.iter().map(|c| c.as_ref()).collect(), *stamp)).collect()
}

// -- merge -------------------------------------------------------------------------------

pub struct MergeOut {
    pub main: (Vec<(Vec<u8>, u64)>, Vec<u8>),
    pub side: (Vec<(Vec<u8>, u64)>, Vec<u8>),
    pub entries_in: u64,
    pub dropped: u64,
}

/// Inputs oldest first. Per key: the newest entry's state, the oldest's
/// presence at the start, every input's flips newer than `cut`, newest
/// first. Entries go to one of two parts:
///
/// - **main**: what head reads and readers after the layer need: present
///   keys, and (above the base) absent keys that were present at the start,
///   which shadow older layers;
/// - **side**: what only a reader whose P falls inside the layer needs: in the
///   base (`bottom`, nothing older to shadow) every absent key, elsewhere
///   keys absent at both ends (added and removed inside).
///
/// An absent key with no flip left serves no reader at or after the cut and
/// shadows nothing if it was absent at the start (or is in the base): dropped.
pub fn merge(inputs: Vec<Stream<'_>>, cut: u64, bottom: bool, block_size: usize, level: i32, file_limit: usize) -> Res<MergeOut> {
    let mut m = Merger::new(inputs)?;
    let mut main = LayerWriter::create(LAYER, block_size, level, file_limit);
    let mut side = LayerWriter::create(LAYER, block_size, level, file_limit);
    let mut group = Vec::new();
    let (mut entries_in, mut dropped) = (0u64, 0u64);
    while m.next_group(&mut group)? {
        entries_in += group.len() as u64;
        let mut flips: Vec<u64> = group.iter().flat_map(|(_, e)| e.flips.iter().copied()).filter(|&f| f > cut).collect();
        flips.sort_unstable_by(|a, b| b.cmp(a));
        flips.dedup();
        let start = group[0].1.start; // ranks ascending: the oldest input first
        let (_, newest) = group.pop().unwrap(); // and the newest last
        let e = Entry { flips, start, ..newest };
        if e.present {
            main.push(e)?;
        } else if e.start && !bottom {
            main.push(e)?;
        } else if e.flips.is_empty() {
            dropped += 1;
        } else {
            side.push(e)?;
        }
    }
    Ok(MergeOut { main: main.finish_raw()?, side: side.finish_raw()?, entries_in, dropped })
}

/// `inputs` oldest first. Returns (main files, main index, side files,
/// side index, entries read, entries dropped).
#[pyfunction]
#[pyo3(signature = (inputs, cut, bottom, *, block_size=16384, level=1, file_limit=67108864))]
#[allow(clippy::type_complexity)]
pub fn layers_merge<'py>(
    py: Python<'py>,
    inputs: Vec<Input>,
    cut: u64,
    bottom: bool,
    block_size: usize,
    level: i32,
    file_limit: usize,
) -> PyResult<(Bound<'py, PyList>, Bound<'py, PyBytes>, Bound<'py, PyList>, Bound<'py, PyBytes>, u64, u64)> {
    let out = py.detach(|| merge(streams(&inputs), cut, bottom, block_size, level, file_limit)).map_err(err)?;
    let files = |fs: &[(Vec<u8>, u64)]| -> PyResult<Bound<'py, PyList>> {
        let l = PyList::empty(py);
        for (d, n) in fs {
            l.append((PyBytes::new(py, d), *n))?;
        }
        Ok(l)
    };
    Ok((
        files(&out.main.0)?,
        PyBytes::new(py, &out.main.1),
        files(&out.side.0)?,
        PyBytes::new(py, &out.side.1),
        out.entries_in,
        out.dropped,
    ))
}

// -- Δ(P, H) -----------------------------------------------------------------------------

pub struct Row {
    pub key: Vec<u8>,
    pub at_p: bool,
    pub at_h: bool,
    pub stamp: u64,
    pub payload: Option<Vec<u8>>,
}

/// Inputs newest first; `g_p` None is P = −∞. Keys in (`after`, `upto`];
/// stops after `limit` rows and returns the last key it delivered, or None
/// if it reached `upto` (or the inputs' end).
pub fn scan(
    inputs: Vec<Stream<'_>>,
    g_p: Option<u64>,
    after: Option<&[u8]>,
    upto: Option<&[u8]>,
    limit: usize,
    out: &mut Vec<Row>,
) -> Res<Option<Vec<u8>>> {
    let mut m = Merger::new(inputs)?;
    let mut group = Vec::new();
    while m.next_group(&mut group)? {
        let key = &group[0].1.key;
        if after.is_some_and(|a| key.as_slice() <= a) {
            continue;
        }
        if upto.is_some_and(|u| key.as_slice() > u) {
            break;
        }
        // group is by rank ascending: rank 0 is the newest input.
        let newer: Vec<&Entry> = group.iter().map(|(_, e)| e).filter(|e| g_p.is_none_or(|g| e.stamp > g)).collect();
        let Some(newest) = newer.first() else { continue };
        let at_h = newest.present;
        let at_p = match g_p {
            None => false,
            Some(g) => {
                let odd = newer.iter().map(|e| e.flips.iter().filter(|&&f| f > g).count()).sum::<usize>() % 2 == 1;
                at_h ^ odd
            }
        };
        if !at_p && !at_h {
            continue;
        }
        out.push(Row {
            key: newest.key.clone(),
            at_p,
            at_h,
            stamp: newest.stamp,
            payload: if at_h { newest.payload.clone() } else { None },
        });
        if out.len() >= limit {
            return Ok(Some(out.last().unwrap().key.clone()));
        }
    }
    Ok(None)
}

/// Returns (keys, presence at P, presence at H (one byte each), stamps,
/// payloads, the last key delivered when `limit` stopped it, else None).
#[pyfunction]
#[pyo3(signature = (inputs, g_p, after=None, upto=None, limit=usize::MAX))]
#[allow(clippy::type_complexity)]
pub fn layers_scan<'py>(
    py: Python<'py>,
    inputs: Vec<Input>,
    g_p: Option<u64>,
    after: Option<PyBackedBytes>,
    upto: Option<PyBackedBytes>,
    limit: usize,
) -> PyResult<(Bound<'py, PyList>, Bound<'py, PyBytes>, Bound<'py, PyBytes>, Vec<u64>, Bound<'py, PyList>, Option<Bound<'py, PyBytes>>)> {
    let mut rows = Vec::new();
    let last = py
        .detach(|| scan(streams(&inputs), g_p, after.as_deref(), upto.as_deref(), limit, &mut rows))
        .map_err(err)?;
    let keys = PyList::new(py, rows.iter().map(|r| PyBytes::new(py, &r.key)))?;
    let at_p: Vec<u8> = rows.iter().map(|r| r.at_p as u8).collect();
    let at_h: Vec<u8> = rows.iter().map(|r| r.at_h as u8).collect();
    let stamps: Vec<u64> = rows.iter().map(|r| r.stamp).collect();
    let payloads = PyList::empty(py);
    for r in &rows {
        match &r.payload {
            Some(p) => payloads.append(PyBytes::new(py, p))?,
            None => payloads.append(py.None())?,
        }
    }
    Ok((keys, PyBytes::new(py, &at_p), PyBytes::new(py, &at_h), stamps, payloads, last.map(|k| PyBytes::new(py, &k))))
}

// -- lookups ------------------------------------------------------------------------------

/// Visit a block's entries in key order without building them: `visit` gets
/// the key (in a buffer reused across entries) and the entry's position in
/// `raw`, and returns false to stop.
fn walk_block(format: u8, raw: &[u8], mut visit: impl FnMut(&[u8], usize) -> Res<bool>) -> Res<()> {
    let mut pos = 0;
    let n = get_varint(raw, &mut pos)? as usize;
    if format == LAYER {
        get_varint(raw, &mut pos)?;
    }
    let mut key: Vec<u8> = Vec::new();
    for _ in 0..n {
        let s = get_varint(raw, &mut pos)? as usize;
        let l = get_varint(raw, &mut pos)? as usize;
        if s > key.len() {
            return Err("shared prefix past the previous key".into());
        }
        key.truncate(s);
        key.extend_from_slice(get_bytes(raw, &mut pos, l)?);
        let at = pos;
        if !visit(&key, at)? {
            return Ok(());
        }
        // Skip the entry's fields.
        let f = *raw.get(pos).ok_or("truncated block")?;
        pos += 1;
        if format == DELTA {
            if f & KIND_PAYLOAD != 0 {
                let l = get_varint(raw, &mut pos)? as usize;
                get_bytes(raw, &mut pos, l)?;
            }
        } else {
            get_varint(raw, &mut pos)?;
            if f & FLIPS != 0 {
                let m = get_varint(raw, &mut pos)?;
                for _ in 0..m {
                    get_varint(raw, &mut pos)?;
                }
            }
            if f & PAYLOAD != 0 {
                let l = get_varint(raw, &mut pos)? as usize;
                get_bytes(raw, &mut pos, l)?;
            }
        }
    }
    Ok(())
}

/// The entry whose fields start at `at` (just after its key) in a block.
fn entry_at(format: u8, raw: &[u8], gmin: u64, stamp: u64, key: &[u8], at: usize) -> Res<Entry> {
    let mut pos = at;
    let f = *raw.get(pos).ok_or("truncated block")?;
    pos += 1;
    if format == DELTA {
        let kind = f & KIND_MASK;
        let payload = if f & KIND_PAYLOAD != 0 {
            let l = get_varint(raw, &mut pos)? as usize;
            Some(get_bytes(raw, &mut pos, l)?.to_vec())
        } else {
            None
        };
        return Ok(Entry {
            key: key.to_vec(),
            present: kind != REMOVED,
            start: kind != ADDED,
            stamp,
            flips: if kind == UPDATED { vec![] } else { vec![stamp] },
            payload,
        });
    }
    let s = gmin + get_varint(raw, &mut pos)?;
    let mut flips = Vec::new();
    if f & FLIPS != 0 {
        let m = get_varint(raw, &mut pos)?;
        let mut last = s;
        for _ in 0..m {
            last = last.checked_sub(get_varint(raw, &mut pos)?).ok_or("flip past zero")?;
            flips.push(last);
        }
    }
    let payload = if f & PAYLOAD != 0 {
        let l = get_varint(raw, &mut pos)? as usize;
        Some(get_bytes(raw, &mut pos, l)?.to_vec())
    } else {
        None
    };
    Ok(Entry { key: key.to_vec(), present: f & PRESENT != 0, start: f & START != 0, stamp: s, flips, payload })
}

type Found = Option<(bool, u64, Option<Vec<u8>>)>;

/// Inputs newest first (each its chunks and a delta's stamp), keys sorted:
/// per key, the newest entry's (present, stamp, payload), or None if no
/// input holds it. Blocks are walked without building their entries.
pub fn lookup(inputs: &[(Vec<&[u8]>, u64)], keys: &[&[u8]]) -> Res<Vec<Found>> {
    let mut out: Vec<Found> = vec![None; keys.len()];
    for (chunks, stamp) in inputs {
        let mut i = 0;
        'chunks: for chunk in chunks {
            let mut pos = 0;
            while pos < chunk.len() {
                if i == keys.len() {
                    break 'chunks;
                }
                let (format, raw, next) = read_block(chunk, pos)?;
                pos = next;
                let gmin = if format == LAYER {
                    let mut p = 0;
                    get_varint(&raw, &mut p)?;
                    get_varint(&raw, &mut p)?
                } else {
                    0
                };
                walk_block(format, &raw, |key, at| {
                    while i < keys.len() && keys[i] < key {
                        i += 1;
                    }
                    if i == keys.len() {
                        return Ok(false);
                    }
                    if keys[i] == key && out[i].is_none() {
                        let e = entry_at(format, &raw, gmin, *stamp, key, at)?;
                        out[i] = Some((e.present, e.stamp, e.payload));
                    }
                    Ok(true)
                })?;
            }
        }
    }
    Ok(out)
}

#[pyfunction]
#[allow(clippy::type_complexity)]
pub fn layers_lookup<'py>(py: Python<'py>, inputs: Vec<Input>, keys: Vec<PyBackedBytes>) -> PyResult<Bound<'py, PyList>> {
    let ks: Vec<&[u8]> = keys.iter().map(|k| k.as_ref()).collect();
    if ks.windows(2).any(|w| w[0] >= w[1]) {
        return Err(err("lookup keys must be sorted and unique".into()));
    }
    let ins: Vec<(Vec<&[u8]>, u64)> = inputs.iter().map(|(c, s)| (c.iter().map(|x| x.as_ref()).collect(), *s)).collect();
    let found = py.detach(|| lookup(&ins, &ks)).map_err(err)?;
    let out = PyList::empty(py);
    for f in found {
        match f {
            None => out.append(py.None())?,
            Some((present, stamp, payload)) => {
                let p = payload.map(|p| PyBytes::new(py, &p).into_any()).unwrap_or_else(|| py.None().into_bound(py));
                out.append((present, stamp, p))?
            }
        }
    }
    Ok(out)
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<LayerWriter>()?;
    m.add_function(wrap_pyfunction!(index_decode, m)?)?;
    m.add_function(wrap_pyfunction!(layers_merge, m)?)?;
    m.add_function(wrap_pyfunction!(layers_scan, m)?)?;
    m.add_function(wrap_pyfunction!(layers_lookup, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn e(k: &str, present: bool, stamp: u64, flips: &[u64]) -> Entry {
        Entry { key: k.as_bytes().to_vec(), present, start: false, stamp, flips: flips.to_vec(), payload: None }
    }

    fn layer(entries: &[Entry]) -> Vec<u8> {
        let mut w = LayerWriter::create(LAYER, 64, 1, 1 << 20);
        for x in entries {
            w.push(x.clone()).unwrap();
        }
        let (files, _) = w.finish_raw().unwrap();
        files.into_iter().map(|f| f.0).collect::<Vec<_>>().concat()
    }

    #[test]
    fn round_trip_and_merge_keep_flips() {
        let older = layer(&[e("a", true, 5, &[5]), e("b", true, 3, &[])]);
        let newer = layer(&[e("a", false, 8, &[8]), e("c", true, 9, &[9])]);
        let out = merge(vec![Stream::new(vec![&older], 0), Stream::new(vec![&newer], 0)], 0, false, 64, 1, 1 << 20).unwrap();
        let data: Vec<u8> = out.main.0.iter().flat_map(|f| f.0.clone()).collect();
        let mut s = Stream::new(vec![&data], 0);
        let mut got = vec![];
        while let Some(x) = s.next().unwrap() {
            got.push(x);
        }
        // a: added in the older layer (absent at its start), removed in the
        // newer: absent at both ends of the merged layer, so on the side.
        assert_eq!(got, vec![e("b", true, 3, &[]), e("c", true, 9, &[9])]);
        let side: Vec<u8> = out.side.0.iter().flat_map(|f| f.0.clone()).collect();
        let mut s = Stream::new(vec![&side], 0);
        assert_eq!(s.next().unwrap(), Some(e("a", false, 8, &[8, 5])));
        // Presence of a at P = 6: absent now, one flip after 6: present then.
        let mut rows = vec![];
        scan(vec![Stream::new(vec![&data], 0), Stream::new(vec![&side], 0)], Some(6), None, None, usize::MAX, &mut rows).unwrap();
        let a = rows.iter().find(|r| r.key == b"a").unwrap();
        assert!(a.at_p && !a.at_h);
        assert!(rows.iter().all(|r| r.key != b"b"));
    }

    #[test]
    fn bottom_merge_sends_absent_keys_to_the_graveyard() {
        let base = layer(&[e("a", true, 1, &[]), e("d", true, 1, &[])]);
        let top = layer(&[e("a", false, 8, &[8]), e("d", false, 4, &[4])]);
        let out = merge(vec![Stream::new(vec![&base], 0), Stream::new(vec![&top], 0)], 5, true, 64, 1, 1 << 20).unwrap();
        assert_eq!(out.dropped, 1); // d: removed at 4, at or below the cut
        let grave: Vec<u8> = out.side.0.iter().flat_map(|f| f.0.clone()).collect();
        let mut s = Stream::new(vec![&grave], 0);
        assert_eq!(s.next().unwrap(), Some(e("a", false, 8, &[8])));
        assert_eq!(s.next().unwrap(), None);
    }
}
