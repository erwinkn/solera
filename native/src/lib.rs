//! `solera._native`: the `.kx` key index format and the per-key work on it.
//!
//! Two kinds of functions. Kernels over byte strings the caller holds —
//! encoding, decoding, filter checks, lookups, bounded scans — mirror
//! `solera/keys/_python.py`, the format's reference. Jobs stream over a whole
//! index — a full replacement, a compaction, a recount: a `Job` asks for the
//! file segments it needs and hands back the files it writes, and Python does
//! the I/O in between. Keys, versions and file contents cross the boundary
//! as `bytes`; flags as a `bytes` with one byte per entry.

pub mod arrow;
pub mod format;
pub mod jobs;
pub mod rows;
pub mod sort;
pub mod stream;

use std::sync::Arc;

use arrow_array::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use arrow_array::RecordBatch;
use pyo3::create_exception;
use pyo3::exceptions::{PyKeyError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedBytes;
use pyo3::types::{PyBytes, PyCapsule, PyDict, PyList, PyString};

use format::{Error, Options};
use jobs::{Compact, Count, Replace, Step};
use rows::{Arena, Constant, Source, Stream, Table, Versions};
use stream::Segment;

create_exception!(
    _native,
    FormatError,
    PyValueError,
    "A key index file is malformed or fails a checksum."
);

fn to_py(e: Error) -> PyErr {
    match e {
        Error::Format(m) => FormatError::new_err(m),
        Error::Value(m) => PyValueError::new_err(m),
        Error::Callback(e) => match e.downcast::<PyErr>() {
            Ok(e) => *e,
            Err(e) => PyValueError::new_err(e.to_string()),
        },
    }
}

fn slices(v: &[PyBackedBytes]) -> Vec<&[u8]> {
    v.iter().map(|b| b.as_ref()).collect()
}

fn list_of_bytes<'py>(py: Python<'py>, items: &[Vec<u8>]) -> PyResult<Bound<'py, PyList>> {
    PyList::new(py, items.iter().map(|b| PyBytes::new(py, b)))
}

fn arena_list<'py>(py: Python<'py>, a: &Arena) -> PyResult<Bound<'py, PyList>> {
    PyList::new(py, (0..a.len()).map(|i| PyBytes::new(py, a.get(i))))
}

fn options(block_size: usize, level: u32, bits_per_item: u64, k: u8, codec: u8) -> Options {
    Options {
        block_size,
        level,
        bits_per_item,
        k,
        codec,
    }
}

// -- kernels ----------------------------------------------------------------------------

#[pyfunction]
#[pyo3(signature = (keys, versions, deleted, *, block_size=65536, level=1, bits_per_item=14, k=10, codec=1))]
#[allow(clippy::too_many_arguments)]
fn encode_file<'py>(
    py: Python<'py>,
    keys: Vec<PyBackedBytes>,
    versions: Vec<PyBackedBytes>,
    deleted: PyBackedBytes,
    block_size: usize,
    level: u32,
    bits_per_item: u64,
    k: u8,
    codec: u8,
) -> PyResult<Bound<'py, PyBytes>> {
    let o = options(block_size, level, bits_per_item, k, codec);
    let out =
        format::encode_file(&slices(&keys), &slices(&versions), &deleted, o).map_err(to_py)?;
    Ok(PyBytes::new(py, &out))
}

/// Entries (sorted, unique keys) as files of about `max_file_bytes` each.
#[pyfunction]
#[pyo3(signature = (keys, versions, deleted, *, block_size=65536, level=1, bits_per_item=14, k=10, codec=1, max_file_bytes=67108864))]
#[allow(clippy::too_many_arguments)]
fn write_files<'py>(
    py: Python<'py>,
    keys: Vec<PyBackedBytes>,
    versions: Vec<PyBackedBytes>,
    deleted: PyBackedBytes,
    block_size: usize,
    level: u32,
    bits_per_item: u64,
    k: u8,
    codec: u8,
    max_file_bytes: usize,
) -> PyResult<Bound<'py, PyList>> {
    if versions.len() != keys.len() || deleted.len() != keys.len() {
        return Err(PyValueError::new_err(
            "keys, versions and deleted must have the same length",
        ));
    }
    let o = options(block_size, level, bits_per_item, k, codec);
    let files = py
        .detach(|| {
            let mut w = stream::Writer::new(o, max_file_bytes);
            for i in 0..keys.len() {
                w.push(&keys[i], &versions[i], deleted[i] != 0)?;
            }
            w.finish(false)?;
            Ok(w.files.into_iter().collect::<Vec<_>>())
        })
        .map_err(to_py)?;
    list_of_bytes(py, &files)
}

#[pyfunction]
fn decode_block<'py>(
    py: Python<'py>,
    data: PyBackedBytes,
    codec: u8,
) -> PyResult<(Bound<'py, PyList>, Bound<'py, PyList>, Bound<'py, PyBytes>)> {
    let (keys, versions, flags) = format::decode_block(&data, codec).map_err(to_py)?;
    Ok((
        list_of_bytes(py, &keys)?,
        list_of_bytes(py, &versions)?,
        PyBytes::new(py, &flags),
    ))
}

#[pyfunction]
fn bloom_check_keys<'py>(
    py: Python<'py>,
    bits: PyBackedBytes,
    nbits: u64,
    k: u8,
    keys: Vec<PyBackedBytes>,
) -> Bound<'py, PyBytes> {
    PyBytes::new(
        py,
        &format::bloom_check_keys(&bits, nbits, k, &slices(&keys)),
    )
}

#[pyfunction]
fn bloom_check_tombstones<'py>(
    py: Python<'py>,
    bits: PyBackedBytes,
    nbits: u64,
    k: u8,
    keys: Vec<PyBackedBytes>,
) -> Bound<'py, PyBytes> {
    PyBytes::new(
        py,
        &format::bloom_check_tombstones(&bits, nbits, k, &slices(&keys)),
    )
}

#[pyfunction]
fn bloom_check_pairs<'py>(
    py: Python<'py>,
    bits: PyBackedBytes,
    nbits: u64,
    k: u8,
    keys: Vec<PyBackedBytes>,
    versions: Vec<PyBackedBytes>,
) -> PyResult<Bound<'py, PyBytes>> {
    if keys.len() != versions.len() {
        return Err(PyValueError::new_err(
            "keys and versions must have the same length",
        ));
    }
    Ok(PyBytes::new(
        py,
        &format::bloom_check_pairs(&bits, nbits, k, &slices(&keys), &slices(&versions)),
    ))
}

#[pyfunction]
fn sort_entries<'py>(
    py: Python<'py>,
    keys: Vec<Bound<'py, PyBytes>>,
    versions: Vec<Bound<'py, PyBytes>>,
    deleted: PyBackedBytes,
) -> PyResult<(Bound<'py, PyList>, Bound<'py, PyList>, Bound<'py, PyBytes>)> {
    let n = keys.len();
    if versions.len() != n || deleted.len() != n {
        return Err(PyValueError::new_err(
            "keys, versions and deleted must have the same length",
        ));
    }
    let key_slices: Vec<&[u8]> = keys.iter().map(|b| b.as_bytes()).collect();
    let order = format::sort_order(&key_slices).map_err(to_py)?;
    // Hand back the original bytes objects, reordered: no copies.
    let sk = PyList::new(py, order.iter().map(|&i| keys[i].clone()))?;
    let sv = PyList::new(py, order.iter().map(|&i| versions[i].clone()))?;
    let sd: Vec<u8> = order.iter().map(|&i| deleted[i]).collect();
    Ok((sk, sv, PyBytes::new(py, &sd)))
}

type Triple<'py> = (Bound<'py, PyBytes>, Bound<'py, PyList>, Bound<'py, PyBytes>);

#[pyfunction]
fn lookup<'py>(
    py: Python<'py>,
    blocks: Vec<PyBackedBytes>,
    codec: u8,
    keys: Vec<PyBackedBytes>,
) -> PyResult<Triple<'py>> {
    let (found, versions, deleted) =
        format::lookup(&slices(&blocks), codec, &slices(&keys)).map_err(to_py)?;
    Ok((
        PyBytes::new(py, &found),
        list_of_bytes(py, &versions)?,
        PyBytes::new(py, &deleted),
    ))
}

#[pyfunction]
fn merge_range<'py>(
    py: Python<'py>,
    runs: Vec<Vec<PyBackedBytes>>,
    codec: u8,
    after: Option<PyBackedBytes>,
    upto: Option<PyBackedBytes>,
    drop_deleted: bool,
) -> PyResult<(Bound<'py, PyList>, Bound<'py, PyList>, Bound<'py, PyBytes>)> {
    let runs: Vec<Vec<&[u8]>> = runs.iter().map(|r| slices(r)).collect();
    let (k, v, f) = format::merge_range(
        &runs,
        codec,
        after.as_ref().map(|a| a.as_ref()),
        upto.as_ref().map(|u| u.as_ref()),
        drop_deleted,
    )
    .map_err(to_py)?;
    Ok((
        list_of_bytes(py, &k)?,
        list_of_bytes(py, &v)?,
        PyBytes::new(py, &f),
    ))
}

// -- tails ----------------------------------------------------------------------------

#[pyfunction]
fn filter_nbits(items: u64, bits_per_item: u64) -> u64 {
    format::filter_nbits(items, bits_per_item)
}

fn footer_dict<'py>(py: Python<'py>, f: &format::Footer) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    d.set_item("codec", f.codec)?;
    d.set_item("entries", f.entries)?;
    d.set_item("filters_offset", f.filters_offset)?;
    d.set_item("filters_length", f.filters_length)?;
    d.set_item("index_offset", f.index_offset)?;
    d.set_item("index_length", f.index_length)?;
    d.set_item("index_crc", f.index_crc)?;
    Ok(d)
}

#[pyfunction]
fn parse_footer<'py>(py: Python<'py>, footer: &[u8]) -> PyResult<Bound<'py, PyDict>> {
    footer_dict(py, &format::parse_footer(footer).map_err(to_py)?)
}

/// A file's block index from its last bytes (`part` ends at `file_size`).
#[pyfunction]
fn parse_index<'py>(py: Python<'py>, part: &[u8], file_size: u64) -> PyResult<Bound<'py, PyDict>> {
    let t = format::parse_index(part, file_size).map_err(to_py)?;
    let d = footer_dict(py, &t.footer)?;
    d.set_item("size", file_size)?;
    d.set_item("min_key", PyBytes::new(py, &t.min_key))?;
    d.set_item("max_key", PyBytes::new(py, &t.max_key))?;
    let blocks = PyList::empty(py);
    for (first, off, size, n, crc) in &t.blocks {
        blocks.append((PyBytes::new(py, first), off, size, n, crc))?;
    }
    d.set_item("blocks", blocks)?;
    Ok(d)
}

/// A file's tail — filters, index and footer (`tail` ends at `file_size`).
#[pyfunction]
fn parse_tail<'py>(py: Python<'py>, tail: &[u8], file_size: u64) -> PyResult<Bound<'py, PyDict>> {
    let d = parse_index(py, tail, file_size)?;
    let filters = format::parse_filters(tail, file_size).map_err(to_py)?;
    for (name, (nbits, k, bits)) in ["key_filter", "pair_filter", "tomb_filter"]
        .into_iter()
        .zip(filters)
    {
        d.set_item(name, (nbits, k, PyBytes::new(py, bits)))?;
    }
    Ok(d)
}

#[pyfunction]
fn check_block(data: &[u8], crc: u32) -> PyResult<()> {
    if crc32fast::hash(data) != crc {
        return Err(FormatError::new_err("block checksum mismatch"));
    }
    Ok(())
}

// -- written content ------------------------------------------------------------------

/// Takes a whole Arrow C stream (`__arrow_c_stream__`), without the GIL: a
/// producer may need it on another thread.
fn arrow_batches(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<Vec<RecordBatch>> {
    let capsule = obj
        .call_method1("__arrow_c_stream__", (py.None(),))?
        .cast_into::<PyCapsule>()
        .map_err(|_| PyTypeError::new_err("__arrow_c_stream__ must return a PyCapsule"))?;
    let ptr = capsule.pointer_checked(Some(c"arrow_array_stream"))?;
    // SAFETY: the capsule holds an ArrowArrayStream; taking it leaves a released one behind.
    let stream =
        unsafe { FFI_ArrowArrayStream::from_raw(ptr.as_ptr() as *mut FFI_ArrowArrayStream) };
    struct Reader(ArrowArrayStreamReader);
    // SAFETY: the Arrow C stream interface lets a consumer pull from any thread.
    unsafe impl Send for Reader {}
    let reader = Reader(
        ArrowArrayStreamReader::try_new(stream)
            .map_err(|e| PyValueError::new_err(e.to_string()))?,
    );
    py.detach(move || reader.0.collect::<Result<Vec<_>, _>>())
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

fn key_of(obj: &Bound<'_, PyAny>, out: &mut Vec<u8>) -> PyResult<()> {
    if let Ok(b) = obj.cast::<PyBytes>() {
        out.extend_from_slice(b.as_bytes());
        return Ok(());
    }
    let s = match obj.cast::<PyString>() {
        Ok(s) => s.clone(),
        Err(_) => obj.str()?,
    };
    match s.to_str() {
        Ok(t) => out.extend_from_slice(t.as_bytes()),
        // Lone surrogates: as `key_bytes` encodes them.
        Err(_) => out.extend_from_slice(
            s.call_method1("encode", ("utf-8", "surrogateescape"))?
                .cast::<PyBytes>()?
                .as_bytes(),
        ),
    }
    Ok(())
}

/// A version function over Python rows, called a window of rows at a time.
struct PyVersions {
    rows: Py<PyList>,
    f: Py<PyAny>,
}

impl Versions for PyVersions {
    fn fill(&mut self, rows: &[u32], out: &mut Arena) -> format::Result<()> {
        Python::attach(|py| -> PyResult<()> {
            let list = self.rows.bind(py);
            let f = self.f.bind(py);
            for &r in rows {
                let v = f.call1((list.get_item(r as usize)?,))?;
                out.push(v.cast::<PyBytes>()?.as_bytes());
            }
            Ok(())
        })
        .map_err(|e| Error::Callback(Box::new(e)))
    }
}

/// The written content of a full replacement, sorted (unless it arrived
/// sorted) and checked for duplicate keys.
#[pyclass(module = "solera._native")]
struct Rows {
    table: Option<Table>,
    len: usize,
    presorted: bool,
}

impl Rows {
    fn new(
        py: Python<'_>,
        keys: Box<dyn sort::Keys + Send>,
        versions: Box<dyn Versions>,
    ) -> PyResult<Rows> {
        let table = py.detach(|| Table::new(keys, versions)).map_err(to_py)?;
        Ok(Rows {
            len: table.len(),
            presorted: table.presorted(),
            table: Some(table),
        })
    }
}

#[pymethods]
impl Rows {
    /// Python rows: each row's key is `row[key]` (the row itself when `key` is
    /// None) as its `str`, UTF-8 encoded; its version is `version(row)`, or
    /// `version` itself when it is `bytes`. Keys are packed once; versions are
    /// computed as the replacement reaches each row.
    #[staticmethod]
    #[pyo3(signature = (rows, key, version))]
    fn objects(
        py: Python<'_>,
        rows: Bound<'_, PyList>,
        key: Option<Bound<'_, PyAny>>,
        version: Bound<'_, PyAny>,
    ) -> PyResult<Rows> {
        let mut keys = Arena::default();
        keys.ends.reserve(rows.len());
        for (i, row) in rows.iter().enumerate() {
            match &key {
                None => key_of(&row, &mut keys.data)?,
                Some(k) => match row.cast::<PyDict>() {
                    Ok(d) => match d.get_item(k)? {
                        Some(v) => key_of(&v, &mut keys.data)?,
                        None => return Err(PyKeyError::new_err(k.clone().unbind())),
                    },
                    Err(_) => key_of(&row.get_item(k)?, &mut keys.data)?,
                },
            }
            keys.ends.push(keys.data.len());
            if i % 65536 == 65535 {
                py.detach(|| ()); // let other threads run: this loop holds the GIL
            }
        }
        keys.data.shrink_to_fit();
        let versions: Box<dyn Versions> = match version.cast::<PyBytes>() {
            Ok(b) => Box::new(Constant(b.as_bytes().to_vec())),
            Err(_) => Box::new(PyVersions {
                rows: rows.unbind(),
                f: version.unbind(),
            }),
        };
        Rows::new(py, Box::new(keys), versions)
    }

    /// Arrow data (any object with `__arrow_c_stream__`), read in place: keys
    /// from the `key` column, versions from the `revision` column's text, or
    /// else a digest of each row (`arrow.rs`).
    #[staticmethod]
    #[pyo3(signature = (data, key, revision=None))]
    fn arrow(
        py: Python<'_>,
        data: Bound<'_, PyAny>,
        key: &str,
        revision: Option<&str>,
    ) -> PyResult<Rows> {
        let batches = arrow_batches(py, &data)?;
        let keys = arrow::keys(&batches, key).map_err(to_py)?;
        let versions: Box<dyn Versions> = match revision {
            Some(r) => Box::new(arrow::Revision::new(&batches, r).map_err(to_py)?),
            None => Box::new(arrow::RowDigest::new(batches).map_err(to_py)?),
        };
        Rows::new(py, keys, versions)
    }

    fn __len__(&self) -> usize {
        self.len
    }

    /// Whether the keys arrived sorted, so no sort ran.
    #[getter]
    fn presorted(&self) -> bool {
        self.presorted
    }
}

/// A chunk of a sorted stream: an Arrow stream of (key, version) columns, or
/// a list of `(key, version)` pairs.
fn chunk(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<(Arena, Arena)> {
    let (mut keys, mut versions) = (Arena::default(), Arena::default());
    if obj.hasattr("__arrow_c_stream__")? {
        let batches = arrow_batches(py, obj)?;
        let Some(first) = batches.first() else {
            return Ok((keys, versions));
        };
        let schema = first.schema();
        if schema.fields().len() < 2 {
            return Err(PyValueError::new_err(
                "a sorted chunk has key and version columns",
            ));
        }
        let (kn, vn) = (
            schema.field(0).name().clone(),
            schema.field(1).name().clone(),
        );
        let k = arrow::keys(&batches, &kn).map_err(to_py)?;
        let mut v = arrow::Revision::new(&batches, &vn).map_err(to_py)?;
        for i in 0..k.len() {
            keys.push(k.key(i));
        }
        let rows: Vec<u32> = (0..k.len() as u32).collect();
        v.fill(&rows, &mut versions).map_err(to_py)?;
        return Ok((keys, versions));
    }
    for item in obj.try_iter()? {
        let item = item?;
        key_of(&item.get_item(0)?, &mut keys.data)?;
        keys.ends.push(keys.data.len());
        key_of(&item.get_item(1)?, &mut versions.data)?;
        versions.ends.push(versions.data.len());
    }
    Ok((keys, versions))
}

// -- jobs -------------------------------------------------------------------------------

enum Kind {
    Replace(Box<Replace>),
    Compact(Box<Compact>),
    Count(Count),
}

/// A streaming job over an index's runs (see the module documentation).
/// `step()` returns `("run", r)` when run `r` needs `feed` or `end`,
/// `("rows", None)` when a streamed replacement needs `feed_rows` or
/// `end_rows`, `("file", data)` for each file written, and `None` when done.
#[pyclass(module = "solera._native")]
struct Job {
    kind: Kind,
}

impl Job {
    fn merge(&mut self) -> &mut stream::Merge {
        match &mut self.kind {
            Kind::Replace(j) => &mut j.merge,
            Kind::Compact(j) => &mut j.merge,
            Kind::Count(j) => &mut j.merge,
        }
    }

    fn replacement(&mut self) -> PyResult<&mut Replace> {
        match &mut self.kind {
            Kind::Replace(j) => Ok(j),
            _ => Err(PyTypeError::new_err("not a replacement")),
        }
    }
}

#[pymethods]
impl Job {
    /// The merge-join of the written content (`rows`, or with None a stream
    /// fed sorted chunks) against `runs` existing runs, newest first. At most
    /// `collect` changed keys are kept for `collected`.
    #[staticmethod]
    #[pyo3(signature = (rows, runs, *, block_size=65536, level=1, bits_per_item=14, k=10, codec=1, max_file_bytes=67108864, collect=0))]
    #[allow(clippy::too_many_arguments)]
    fn replace(
        rows: Option<PyRefMut<'_, Rows>>,
        runs: usize,
        block_size: usize,
        level: u32,
        bits_per_item: u64,
        k: u8,
        codec: u8,
        max_file_bytes: usize,
        collect: usize,
    ) -> PyResult<Job> {
        let src = match rows {
            Some(mut r) => Source::Table(
                r.table
                    .take()
                    .ok_or_else(|| PyValueError::new_err("rows already used"))?,
            ),
            None => Source::Stream(Stream::default()),
        };
        let o = options(block_size, level, bits_per_item, k, codec);
        Ok(Job {
            kind: Kind::Replace(Box::new(Replace::new(
                src,
                runs,
                o,
                max_file_bytes,
                collect,
            ))),
        })
    }

    /// Merges `runs` (newest first) into new files; `drop_deleted` when
    /// nothing older lies below.
    #[staticmethod]
    #[pyo3(signature = (runs, *, drop_deleted, block_size=65536, level=1, bits_per_item=14, k=10, codec=1, max_file_bytes=67108864))]
    #[allow(clippy::too_many_arguments)]
    fn compact(
        runs: usize,
        drop_deleted: bool,
        block_size: usize,
        level: u32,
        bits_per_item: u64,
        k: u8,
        codec: u8,
        max_file_bytes: usize,
    ) -> Job {
        let o = options(block_size, level, bits_per_item, k, codec);
        Job {
            kind: Kind::Compact(Box::new(Compact::new(
                runs,
                drop_deleted,
                o,
                max_file_bytes,
            ))),
        }
    }

    /// Counts the live keys of `runs` (newest first).
    #[staticmethod]
    fn count(runs: usize) -> Job {
        Job {
            kind: Kind::Count(Count::new(runs)),
        }
    }

    /// The next consecutive blocks of run `r`: `data` holds them, `blocks`
    /// gives each one's offset in `data`, compressed size and CRC.
    fn feed(&mut self, r: usize, data: PyBackedBytes, blocks: Vec<(usize, usize, u32)>, codec: u8) {
        self.merge().runs[r].feed(Segment {
            data: Arc::new(data),
            blocks,
            codec,
        });
    }

    fn end(&mut self, r: usize) {
        self.merge().runs[r].end();
    }

    fn feed_rows(&mut self, py: Python<'_>, rows: Bound<'_, PyAny>) -> PyResult<()> {
        let (k, v) = chunk(py, &rows)?;
        match &mut self.replacement()?.src {
            Source::Stream(s) => s.feed(k, v).map_err(to_py),
            Source::Table(_) => Err(PyTypeError::new_err("not a streamed replacement")),
        }
    }

    fn end_rows(&mut self) -> PyResult<()> {
        if let Source::Stream(s) = &mut self.replacement()?.src {
            s.end();
        }
        Ok(())
    }

    fn step<'py>(
        &mut self,
        py: Python<'py>,
    ) -> PyResult<Option<(&'static str, Bound<'py, PyAny>)>> {
        let kind = &mut self.kind;
        let (step, file) = py
            .detach(|| {
                let (step, writer) = match kind {
                    Kind::Replace(j) => (j.step()?, Some(&mut j.writer)),
                    Kind::Compact(j) => (j.step()?, Some(&mut j.writer)),
                    Kind::Count(j) => (j.step()?, None),
                };
                let file = match step {
                    Step::File => writer.and_then(|w| w.files.pop_front()),
                    _ => None,
                };
                Ok((step, file))
            })
            .map_err(to_py)?;
        Ok(match step {
            Step::Run(r) => Some(("run", r.into_pyobject(py)?.into_any())),
            Step::Rows => Some(("rows", py.None().into_bound(py))),
            Step::File => Some(("file", PyBytes::new(py, &file.unwrap()).into_any())),
            Step::Done => None,
        })
    }

    /// A replacement's counts: new live keys, deleted keys, changed versions.
    #[getter]
    fn added(&mut self) -> PyResult<u64> {
        Ok(self.replacement()?.added)
    }

    #[getter]
    fn removed(&mut self) -> PyResult<u64> {
        Ok(self.replacement()?.removed)
    }

    #[getter]
    fn changed(&mut self) -> PyResult<u64> {
        Ok(self.replacement()?.changed)
    }

    /// A count's live keys.
    #[getter]
    fn live(&self) -> PyResult<u64> {
        match &self.kind {
            Kind::Count(j) => Ok(j.live),
            _ => Err(PyTypeError::new_err("not a count")),
        }
    }

    /// A replacement's written keys and deleted keys, or None past `collect`.
    fn collected<'py>(
        &mut self,
        py: Python<'py>,
    ) -> PyResult<Option<(Bound<'py, PyList>, Bound<'py, PyList>)>> {
        let c = &self.replacement()?.collected;
        match (&c.upserts, &c.removes) {
            (Some(u), Some(r)) => Ok(Some((arena_list(py, u)?, arena_list(py, r)?))),
            _ => Ok(None),
        }
    }
}

#[pymodule]
#[pyo3(name = "_native")]
fn solera_native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("CODEC_NONE", format::CODEC_NONE)?;
    m.add("CODEC_ZLIB", format::CODEC_ZLIB)?;
    m.add("FOOTER_SIZE", format::FOOTER_SIZE)?;
    m.add("FormatError", m.py().get_type::<FormatError>())?;
    m.add_function(wrap_pyfunction!(encode_file, m)?)?;
    m.add_function(wrap_pyfunction!(write_files, m)?)?;
    m.add_function(wrap_pyfunction!(decode_block, m)?)?;
    m.add_function(wrap_pyfunction!(bloom_check_keys, m)?)?;
    m.add_function(wrap_pyfunction!(bloom_check_pairs, m)?)?;
    m.add_function(wrap_pyfunction!(bloom_check_tombstones, m)?)?;
    m.add_function(wrap_pyfunction!(sort_entries, m)?)?;
    m.add_function(wrap_pyfunction!(lookup, m)?)?;
    m.add_function(wrap_pyfunction!(merge_range, m)?)?;
    m.add_function(wrap_pyfunction!(filter_nbits, m)?)?;
    m.add_function(wrap_pyfunction!(parse_footer, m)?)?;
    m.add_function(wrap_pyfunction!(parse_index, m)?)?;
    m.add_function(wrap_pyfunction!(parse_tail, m)?)?;
    m.add_function(wrap_pyfunction!(check_block, m)?)?;
    m.add_class::<Rows>()?;
    m.add_class::<Job>()?;
    Ok(())
}
