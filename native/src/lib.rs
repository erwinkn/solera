//! `solera._native`: the key index's per-key work (docs/key-index-design.md,
//! docs/key-index-format.md) and the written content it reads.
//!
//! The stamped-layer index lives in `layers.rs` (its bindings in
//! `layers_py.rs`): blocks, deltas, merges, joins, scans and lookups over bytes
//! the caller holds, and streaming jobs that ask for the segments they need
//! and hand back the files they write, Python doing the I/O in between. Here:
//! a write's keys (`Rows`, read from Python rows, DataFrames or Arrow, and
//! sorted), its sorted entries (`SortedEntries`), and content digests. Keys,
//! payloads and file contents cross the boundary as `bytes` (a payload `None`
//! where an entry carries none), generations as `int`.

pub mod arrow;
pub mod delta;
pub mod entries;
pub mod error;
pub mod layers;
mod layers_py;
pub mod rows;
pub mod sort;
pub mod stream;

use std::sync::Arc;

use arrow_array::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use arrow_array::RecordBatch;
use pyo3::create_exception;
use pyo3::exceptions::{PyKeyError, PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedBytes;
use pyo3::types::{PyBool, PyBytes, PyCapsule, PyDict, PyInt, PyList, PyString};

use error::Error;
use rayon::prelude::*;
use rows::{Arena, Constant, Cursor, Payloads, Source, Table};

create_exception!(
    _native,
    FormatError,
    PyValueError,
    "A key index file is malformed or fails a checksum."
);
create_exception!(
    _native,
    LimitError,
    PyValueError,
    "Well-formed input over a limit: more entries or bytes than the caller takes."
);

/// Runs a binding's body, turning a panic into a `RuntimeError`. Unguarded,
/// pyo3 raises `PanicException`, a `BaseException` that every `except
/// Exception` in the engine and the worker lets through.
fn guard<T>(body: impl FnOnce() -> PyResult<T>) -> PyResult<T> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(body)).unwrap_or_else(|payload| {
        let what = payload
            .downcast_ref::<&str>()
            .map(|s| s.to_string())
            .or_else(|| payload.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "a panic with no message".into());
        Err(PyRuntimeError::new_err(format!(
            "solera._native panicked: {what}"
        )))
    })
}

fn to_py(e: Error) -> PyErr {
    match e {
        Error::Format(m) => FormatError::new_err(m),
        Error::Value(m) => PyValueError::new_err(m),
        Error::Limit(m) => LimitError::new_err(m),
        Error::Callback(e) => match e.downcast::<PyErr>() {
            Ok(e) => *e,
            Err(e) => PyValueError::new_err(e.to_string()),
        },
    }
}

fn slices(v: &[PyBackedBytes]) -> Vec<&[u8]> {
    v.iter().map(|b| b.as_ref()).collect()
}

fn arena_list<'py>(py: Python<'py>, a: &Arena) -> PyResult<Bound<'py, PyList>> {
    PyList::new(py, (0..a.len()).map(|i| PyBytes::new(py, a.get(i))))
}

/// Payloads as a list: `bytes`, or None where an entry carries none.
fn payload_list<'py, 'a>(
    py: Python<'py>,
    items: impl IntoIterator<Item = Option<&'a [u8]>>,
) -> PyResult<Bound<'py, PyList>> {
    let out = PyList::empty(py);
    for p in items {
        match p {
            Some(b) => out.append(PyBytes::new(py, b))?,
            None => out.append(py.None())?,
        }
    }
    Ok(out)
}

fn opt(p: &Option<PyBackedBytes>) -> Option<&[u8]> {
    p.as_ref().map(|b| b.as_ref())
}

/// Sorted entries as columns: keys, generations, deleted flags (one byte
/// each), payloads (None for none).
type Columns<'py> = (
    Bound<'py, PyList>,
    Vec<u64>,
    Bound<'py, PyBytes>,
    Bound<'py, PyList>,
);

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
    arrow::unique_columns(&arrow_array::RecordBatchReader::schema(&reader.0)).map_err(to_py)?;
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

/// A row's key, by the one rule rows and stores share (`solera.stores.key_text`):
/// a `str`, UTF-8 encoded, or an `int` (not a `bool`) as its decimal text.
fn row_key(obj: &Bound<'_, PyAny>, out: &mut Vec<u8>) -> PyResult<()> {
    if obj.cast::<PyString>().is_ok()
        || (obj.cast::<PyInt>().is_ok() && obj.cast::<PyBool>().is_err())
    {
        return key_of(obj, out);
    }
    Err(PyValueError::new_err(format!(
        "a key must be a str or an int, not {}",
        obj.get_type().name()?
    )))
}

/// `row[name]` of a mapping row, None when it has no such field.
fn field<'py>(
    row: &Bound<'py, PyAny>,
    name: &Bound<'py, PyString>,
) -> PyResult<Option<Bound<'py, PyAny>>> {
    match row.cast::<PyDict>() {
        Ok(d) => d.get_item(name),
        Err(_) => match row.get_item(name) {
            Ok(v) => Ok(Some(v)),
            Err(e) if e.is_instance_of::<PyKeyError>(row.py()) => Ok(None),
            Err(e) => Err(e),
        },
    }
}

/// Mapping rows' keys, `row[key]`, packed once.
fn record_keys<'py>(py: Python<'py>, rows: &Bound<'py, PyAny>, key: &str) -> PyResult<Arena> {
    let name = PyString::new(py, key);
    let mut keys = Arena::default();
    for (i, row) in rows.try_iter()?.enumerate() {
        let row = row?;
        let k = field(&row, &name)?.ok_or_else(|| PyKeyError::new_err(key.to_string()))?;
        row_key(&k, &mut keys.data)?;
        keys.ends.push(keys.data.len());
        if i % 65536 == 65535 {
            py.detach(|| ()); // let other threads run: this loop holds the GIL
        }
    }
    keys.data.shrink_to_fit();
    Ok(keys)
}

/// `(key, version)` pairs' versions, a window of rows at a time under the
/// GIL: `str` or `bytes`, or None for none.
struct Versions(Py<PyList>);

impl Versions {
    fn fill_py(
        &self,
        py: Python<'_>,
        rows: &[u32],
        out: &mut Vec<Option<Vec<u8>>>,
    ) -> PyResult<()> {
        let list = self.0.bind(py);
        for &r in rows {
            let v = list.get_item(r as usize)?.get_item(1)?;
            if v.is_none() {
                out.push(None);
            } else {
                let mut buf = Vec::new();
                key_of(&v, &mut buf)?;
                out.push(Some(buf));
            }
        }
        Ok(())
    }
}

impl Payloads for Versions {
    fn fill(&self, rows: &[u32], out: &mut Vec<Option<Vec<u8>>>) -> error::Result<()> {
        Python::attach(|py| self.fill_py(py, rows, out)).map_err(|e| Error::Callback(Box::new(e)))
    }
}

/// Packs each item's key once: `key(item)` as its `str`, UTF-8 encoded.
fn pack<'py>(
    py: Python<'py>,
    items: &Bound<'py, PyList>,
    key: impl Fn(&Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>>,
) -> PyResult<Arena> {
    let mut keys = Arena::default();
    keys.ends.reserve(items.len());
    for (i, item) in items.iter().enumerate() {
        key_of(&key(&item)?, &mut keys.data)?;
        keys.ends.push(keys.data.len());
        if i % 65536 == 65535 {
            py.detach(|| ()); // let other threads run: this loop holds the GIL
        }
    }
    keys.data.shrink_to_fit();
    Ok(keys)
}

/// Keys, and their payloads where they carry them, as lists.
type Entries<'py> = (Bound<'py, PyList>, Bound<'py, PyList>);

/// A pass over `Rows`, a chunk of keys and payloads at a time (`Rows.chunks`).
#[pyclass(module = "solera._native")]
struct Chunks {
    cursor: Cursor,
    size: usize,
}

#[pymethods]
impl Chunks {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__<'py>(&mut self, py: Python<'py>) -> PyResult<Option<Entries<'py>>> {
        guard(|| {
            let (keys, payloads) = (PyList::empty(py), PyList::empty(py));
            while keys.len() < self.size && self.cursor.read().map_err(to_py)? {
                let (k, p) = self.cursor.entry();
                keys.append(PyBytes::new(py, k))?;
                payloads.append(p.map(|p| PyBytes::new(py, p)))?;
            }
            Ok((!keys.is_empty()).then_some((keys, payloads)))
        })
    }
}

/// A keyed write's keys, sorted (unless they arrived sorted) and read a key
/// at a time: every key is the group of rows that carry it. Only the key
/// of a row is read; a source's keys may carry a version each.
#[pyclass(module = "solera._native", frozen)]
struct Rows {
    table: Arc<Table>,
}

impl Rows {
    fn new(
        py: Python<'_>,
        keys: Box<dyn sort::Keys + Send>,
        payloads: Option<Box<dyn Payloads>>,
    ) -> Rows {
        Rows {
            table: Arc::new(py.detach(|| Table::new(keys, payloads))),
        }
    }
}

/// Keys, packed.
fn packed_keys(keys: &[Bound<'_, PyAny>]) -> PyResult<Arena> {
    let mut out = Arena::default();
    for k in keys {
        row_key(k, &mut out.data)?;
        out.ends.push(out.data.len());
    }
    Ok(out)
}

#[pymethods]
impl Rows {
    /// Rows as mappings: the key is `row[key]`, a `str` or an `int`.
    #[staticmethod]
    fn records(py: Python<'_>, rows: Bound<'_, PyList>, key: &str) -> PyResult<Rows> {
        guard(|| {
            let keys = record_keys(py, rows.as_any(), key)?;
            Ok(Rows::new(py, Box::new(keys), None))
        })
    }

    /// Rows read a column at a time — a DataFrame through pandas alone, no
    /// Arrow: `columns[i]` holds column `names[i]`'s value of every row.
    #[staticmethod]
    fn columns(
        py: Python<'_>,
        names: Vec<String>,
        columns: Vec<Bound<'_, PyList>>,
        key: &str,
    ) -> PyResult<Rows> {
        guard(|| {
            if names.len() != columns.len() {
                return Err(PyValueError::new_err("a name for every column"));
            }
            let mut sorted: Vec<&String> = names.iter().collect();
            sorted.sort();
            if let Some(w) = sorted.windows(2).find(|w| w[0] == w[1]) {
                return Err(PyValueError::new_err(format!(
                    "column {:?} appears twice",
                    w[0]
                )));
            }
            let n = columns.first().map_or(0, |c| c.len());
            if columns.iter().any(|c| c.len() != n) {
                return Err(PyValueError::new_err("columns of different lengths"));
            }
            let k = names
                .iter()
                .position(|c| c == key)
                .ok_or_else(|| PyKeyError::new_err(key.to_string()))?;
            let mut keys = Arena::default();
            keys.ends.reserve(n);
            for i in 0..n {
                row_key(&columns[k].get_item(i)?, &mut keys.data)?;
                keys.ends.push(keys.data.len());
                if i % 65536 == 65535 {
                    py.detach(|| ()); // let other threads run: this loop holds the GIL
                }
            }
            Ok(Rows::new(py, Box::new(keys), None))
        })
    }

    /// `(key, value)` pairs (a `keyed=True` output).
    #[staticmethod]
    fn values(py: Python<'_>, items: Bound<'_, PyList>) -> PyResult<Rows> {
        guard(|| {
            let keys = pack(py, &items, |item| item.get_item(0))?;
            Ok(Rows::new(py, Box::new(keys), None))
        })
    }

    /// A source's `(key, version)` pairs: the version `str` or `bytes`, or
    /// None for none.
    #[staticmethod]
    fn pairs(py: Python<'_>, items: Bound<'_, PyList>) -> PyResult<Rows> {
        guard(|| {
            let keys = pack(py, &items, |item| item.get_item(0))?;
            let versions: Box<dyn Payloads> = Box::new(Versions(items.unbind()));
            Ok(Rows::new(py, Box::new(keys), Some(versions)))
        })
    }

    /// Keys, each with `payload` if given (a partition set's elements: empty).
    #[staticmethod]
    #[pyo3(signature = (keys, payload=None))]
    fn keys(py: Python<'_>, keys: Bound<'_, PyList>, payload: Option<&[u8]>) -> PyResult<Rows> {
        guard(|| {
            let packed = pack(py, &keys, |k| Ok(k.clone()))?;
            let payloads = payload.map(|p| Box::new(Constant(p.to_vec())) as Box<dyn Payloads>);
            Ok(Rows::new(py, Box::new(packed), payloads))
        })
    }

    /// Arrow data (any object with `__arrow_c_stream__`), its `key` column
    /// read in place.
    #[staticmethod]
    fn arrow(py: Python<'_>, data: Bound<'_, PyAny>, key: &str) -> PyResult<Rows> {
        guard(|| {
            let batches = arrow_batches(py, &data)?;
            let keys = arrow::keys(&batches, key, false).map_err(to_py)?;
            Ok(Rows::new(py, keys, None))
        })
    }

    /// Rows, not keys.
    fn __len__(&self) -> usize {
        self.table.len()
    }

    /// Whether the keys arrived sorted, so no sort ran.
    #[getter]
    fn presorted(&self) -> bool {
        self.table.presorted()
    }

    /// Every key and its payload (None for none), in key order.
    fn entries<'py>(&self, py: Python<'py>) -> PyResult<Entries<'py>> {
        guard(|| {
            let mut c = Cursor::new(self.table.clone());
            let (keys, payloads) = (PyList::empty(py), PyList::empty(py));
            while c.read().map_err(to_py)? {
                let (k, p) = c.entry();
                keys.append(PyBytes::new(py, k))?;
                payloads.append(p.map(|p| PyBytes::new(py, p)))?;
            }
            Ok((keys, payloads))
        })
    }

    /// Every key and its payload, in key order, `size` at a time.
    fn chunks(&self, size: usize) -> PyResult<Chunks> {
        guard(|| {
            if size == 0 {
                return Err(PyValueError::new_err("a page holds at least one key"));
            }
            Ok(Chunks {
                cursor: Cursor::new(self.table.clone()),
                size,
            })
        })
    }

    /// The rows of each of `keys`, as indices into what the rows were made
    /// from, each key's in their order there: `(rows, ends)`, key `i`'s rows
    /// at `rows[ends[i - 1]:ends[i]]`. A key the write does not hold is a
    /// `KeyError`.
    fn find<'py>(
        &self,
        py: Python<'py>,
        keys: Vec<Bound<'py, PyAny>>,
    ) -> PyResult<(Bound<'py, PyList>, Bound<'py, PyList>)> {
        guard(|| {
            let packed = packed_keys(&keys)?;
            let table = self.table.clone();
            let found: Vec<Option<Vec<u32>>> = py.detach(|| {
                (0..packed.len())
                    .into_par_iter()
                    .map(|i| table.find(packed.get(i)))
                    .collect()
            });
            let (rows, ends) = (PyList::empty(py), PyList::empty(py));
            let mut n = 0usize;
            for (i, f) in found.into_iter().enumerate() {
                let Some(f) = f else {
                    return Err(PyKeyError::new_err(keys[i].clone().unbind()));
                };
                n += f.len();
                for r in f {
                    rows.append(r)?;
                }
                ends.append(n)?;
            }
            Ok((rows, ends))
        })
    }

    /// Whether the write holds `key`.
    fn __contains__(&self, key: Bound<'_, PyAny>) -> PyResult<bool> {
        guard(|| {
            let mut k = Vec::new();
            row_key(&key, &mut k)?;
            Ok(self.table.find(&k).is_some())
        })
    }
}

/// A chunk of a sorted stream of keys: a list of keys (`str`, `int` or
/// `bytes`) or Arrow data whose first column holds them — or with `key`,
/// rows (a list of mappings, or Arrow data) keyed by that column.
fn chunk(py: Python<'_>, obj: &Bound<'_, PyAny>, key: Option<&str>) -> PyResult<Arena> {
    if obj.hasattr("__arrow_c_stream__")? {
        let batches = arrow_batches(py, obj)?;
        let Some(first) = batches.first() else {
            return Ok(Arena::default());
        };
        let name = match key {
            Some(k) => k.to_string(),
            None => first.schema().field(0).name().clone(),
        };
        let k = arrow::keys(&batches, &name, key.is_none()).map_err(to_py)?;
        let mut keys = Arena::default();
        for i in 0..k.len() {
            keys.push(k.key(i));
        }
        return Ok(keys);
    }
    match key {
        Some(k) => record_keys(py, obj, k),
        None => {
            let mut keys = Arena::default();
            for item in obj.try_iter()? {
                key_of(&item?, &mut keys.data)?;
                keys.ends.push(keys.data.len());
            }
            Ok(keys)
        }
    }
}

// -- sorted runs ------------------------------------------------------------------------

/// A write's entries in key order (`entries.rs`): upserts, each with its payload
/// if it carries one, and removes. Immutable once built, so readers share it.
#[pyclass(module = "solera._native", frozen)]
struct SortedEntries {
    inner: Arc<entries::SortedEntries>,
}

fn sorted_entries(r: error::Result<entries::SortedEntries>) -> PyResult<SortedEntries> {
    Ok(SortedEntries {
        inner: Arc::new(r.map_err(to_py)?),
    })
}

#[pymethods]
impl SortedEntries {
    /// Upserts of `keys` (any order, each key once), each with its payload
    /// if `payloads` gives one (None for none), and the removes of
    /// `removes`; a key both written and removed is an error.
    #[staticmethod]
    #[pyo3(signature = (keys, payloads=None, removes=vec![]))]
    fn of(
        py: Python<'_>,
        keys: Vec<PyBackedBytes>,
        payloads: Option<Vec<Option<PyBackedBytes>>>,
        removes: Vec<PyBackedBytes>,
    ) -> PyResult<SortedEntries> {
        guard(|| {
            sorted_entries(py.detach(|| {
                let payloads: Option<Vec<Option<&[u8]>>> =
                    payloads.as_ref().map(|p| p.iter().map(opt).collect());
                entries::SortedEntries::of(&slices(&keys), payloads.as_deref(), &slices(&removes))
            }))
        })
    }

    /// Every key of `rows`, with its payload, in key order, and the removes
    /// of `removes`. Reads the rows, which stay usable.
    #[staticmethod]
    #[pyo3(signature = (rows, removes=vec![]))]
    fn from_rows(
        py: Python<'_>,
        rows: PyRef<'_, Rows>,
        removes: Vec<PyBackedBytes>,
    ) -> PyResult<SortedEntries> {
        guard(|| {
            let mut src = Source::Table(Box::new(Cursor::new(rows.table.clone())));
            sorted_entries(
                py.detach(|| entries::SortedEntries::from_source(&mut src, &slices(&removes))),
            )
        })
    }

    /// Sorted entries from their transport form, one delta file, every block
    /// checked (`FormatError` when one fails), decoding at most `max_entries`
    /// entries and `max_bytes` bytes (`LimitError` past either).
    #[staticmethod]
    #[pyo3(signature = (data, *, max_entries=u64::MAX, max_bytes=u64::MAX))]
    fn decode(
        py: Python<'_>,
        data: PyBackedBytes,
        max_entries: u64,
        max_bytes: u64,
    ) -> PyResult<SortedEntries> {
        guard(|| {
            sorted_entries(
                py.detach(|| entries::SortedEntries::decode(&data, max_entries, max_bytes)),
            )
        })
    }

    /// The transport form: one delta file.
    #[pyo3(signature = (*, block_size=16384, level=1))]
    fn encode<'py>(
        &self,
        py: Python<'py>,
        block_size: usize,
        level: i32,
    ) -> PyResult<Bound<'py, PyBytes>> {
        guard(|| {
            let out = py
                .detach(|| self.inner.encode(block_size, level))
                .map_err(to_py)?;
            Ok(PyBytes::new(py, &out))
        })
    }

    fn __len__(&self) -> usize {
        self.inner.len()
    }

    #[getter]
    fn upserts(&self) -> usize {
        self.inner.len() - self.inner.removes()
    }

    #[getter]
    fn removes(&self) -> usize {
        self.inner.removes()
    }

    /// Bytes held.
    #[getter]
    fn nbytes(&self) -> usize {
        self.inner.nbytes()
    }

    fn keys<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        guard(|| arena_list(py, &self.inner.keys))
    }

    /// Keys, generations, deleted flags, payloads (None for none).
    fn entries<'py>(&self, py: Python<'py>) -> PyResult<Columns<'py>> {
        guard(|| {
            let r = &self.inner;
            let flags: Vec<u8> = r.deleted.iter().map(|&d| d as u8).collect();
            Ok((
                arena_list(py, &r.keys)?,
                r.generations.clone(),
                PyBytes::new(py, &flags),
                payload_list(py, (0..r.len()).map(|i| r.payload(i)))?,
            ))
        })
    }
}

// -- file digests ------------------------------------------------------------------------

/// A file's content digest: XXH3-128, as hex.
#[pyfunction]
fn content_digest(py: Python<'_>, data: PyBackedBytes) -> PyResult<String> {
    guard(|| {
        Ok({
            let h = py.detach(|| xxhash_rust::xxh3::xxh3_128(&data));
            format!("{h:032x}")
        })
    })
}

/// Panics through the guard, on this thread or a rayon worker's: what the
/// guard's test (tests/sdk/test_native_guard.py) needs, no bug at hand.
#[pyfunction]
#[pyo3(signature = (message, parallel=false))]
fn _panic(py: Python<'_>, message: &str, parallel: bool) -> PyResult<()> {
    guard(|| {
        py.detach(|| {
            if parallel {
                (0..4)
                    .into_par_iter()
                    .for_each(|i| assert!(i < 3, "{message}"));
            } else {
                panic!("{message}");
            }
        });
        Ok(())
    })
}

#[pymodule]
#[pyo3(name = "_native")]
fn solera_native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("FormatError", m.py().get_type::<FormatError>())?;
    m.add("LimitError", m.py().get_type::<LimitError>())?;
    m.add_class::<SortedEntries>()?;
    m.add_class::<Rows>()?;
    m.add_class::<Chunks>()?;
    layers_py::register(m)?;
    m.add_function(wrap_pyfunction!(content_digest, m)?)?;
    m.add_function(wrap_pyfunction!(_panic, m)?)?;
    Ok(())
}
