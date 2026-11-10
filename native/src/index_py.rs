//! Python bindings of the change index (`native/index`, the `solera-index`
//! crate). One call per query, commit or merge: Rust does the object-store
//! I/O and the decoding with the GIL released, on a runtime shared by every
//! index in the process.
//!
//! Layers cross as dicts (the engine journals them): `path`, `size`,
//! `index_start`, `top_start`, `lo`, `hi`, `floor` (None unless a base),
//! `entries`, `keys`, `first_key`, `last_key`, `flags`. Keys are `str`;
//! versions are `int` (a generation), `str` (a source's version) or None
//! (absent).

use std::collections::HashMap;
use std::sync::OnceLock;

use pyo3::create_exception;
use pyo3::exceptions::{PyKeyError, PyOSError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyDict, PyInt, PyList, PyString, PyTuple};
use solera_index as ix;
use solera_index::{Error, LayerRef, Query, Run, Version};

use crate::{guard, FormatError};

create_exception!(
    _native,
    ChainError,
    FormatError,
    "A merge found entries that do not chain: an entry's replaced version is not the previous entry's new one."
);

/// The runtime, made by this process: its threads do not survive a fork, so
/// a forked child that calls in fails loudly instead of hanging.
pub(crate) fn rt() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<(tokio::runtime::Runtime, u32)> = OnceLock::new();
    let (rt, pid) = RT.get_or_init(|| {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("solera-index")
            .enable_all()
            .build()
            .expect("the change index's runtime");
        (rt, std::process::id())
    });
    assert_eq!(
        *pid,
        std::process::id(),
        "the change index cannot be used in a process forked after its first use"
    );
    rt
}

pub(crate) fn err(e: Error) -> PyErr {
    match e {
        Error::Format(m) => FormatError::new_err(m),
        Error::Chain(m) => ChainError::new_err(m),
        Error::Invalid(m) => PyValueError::new_err(m),
        Error::Store(m) => PyOSError::new_err(m.to_string()),
    }
}

pub(crate) enum Owned {
    Absent,
    Int(u64),
    Str(String),
}

impl Owned {
    pub(crate) fn of(v: &Bound<'_, PyAny>) -> PyResult<Owned> {
        if v.is_none() {
            Ok(Owned::Absent)
        } else if v.is_instance_of::<PyString>() {
            Ok(Owned::Str(v.extract()?))
        } else if v.is_instance_of::<PyInt>() && !v.is_instance_of::<PyBool>() {
            Ok(Owned::Int(v.extract()?))
        } else {
            Err(PyTypeError::new_err(format!(
                "a version is an int, a str or None, got {}",
                v.get_type().name()?
            )))
        }
    }

    fn as_version(&self) -> Version<'_> {
        match self {
            Owned::Absent => Version::Absent,
            Owned::Int(n) => Version::Int(*n),
            Owned::Str(s) => Version::Bytes(s.as_bytes()),
        }
    }
}

pub(crate) fn version<'py>(py: Python<'py>, v: Version<'_>) -> PyResult<Bound<'py, PyAny>> {
    Ok(match v {
        Version::Absent => py.None().into_bound(py),
        Version::Int(n) => n.into_pyobject(py)?.into_any(),
        Version::Bytes(b) => match std::str::from_utf8(b) {
            Ok(s) => PyString::new(py, s).into_any(),
            Err(_) => PyBytes::new(py, b).into_any(),
        },
    })
}

pub(crate) fn key<'py>(py: Python<'py>, k: &[u8]) -> Bound<'py, PyAny> {
    match std::str::from_utf8(k) {
        Ok(s) => PyString::new(py, s).into_any(),
        Err(_) => PyBytes::new(py, k).into_any(),
    }
}

fn field<'py, T: for<'a> FromPyObject<'a, 'py>>(d: &Bound<'py, PyDict>, name: &str) -> PyResult<T> {
    match d.get_item(name)? {
        Some(v) => v.extract().map_err(Into::into),
        None => Err(PyKeyError::new_err(format!("a layer needs {name:?}"))),
    }
}

fn layer_in(d: &Bound<'_, PyDict>) -> PyResult<LayerRef> {
    Ok(LayerRef {
        path: field(d, "path")?,
        size: field(d, "size")?,
        index_start: field(d, "index_start")?,
        top_start: field(d, "top_start")?,
        lo: field(d, "lo")?,
        hi: field(d, "hi")?,
        floor: field(d, "floor")?,
        entries: field(d, "entries")?,
        keys: field(d, "keys")?,
        first_key: field::<String>(d, "first_key")?.into_bytes(),
        last_key: field::<String>(d, "last_key")?.into_bytes(),
        flags: field(d, "flags")?,
    })
}

pub(crate) fn layers_in(ls: &Bound<'_, PyAny>) -> PyResult<Vec<LayerRef>> {
    ls.try_iter()?
        .map(|l| layer_in(l?.cast::<PyDict>()?))
        .collect()
}

fn layer_out<'py>(py: Python<'py>, l: &LayerRef) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    d.set_item("path", &l.path)?;
    d.set_item("size", l.size)?;
    d.set_item("index_start", l.index_start)?;
    d.set_item("top_start", l.top_start)?;
    d.set_item("lo", l.lo)?;
    d.set_item("hi", l.hi)?;
    d.set_item("floor", l.floor)?;
    d.set_item("entries", l.entries)?;
    d.set_item("keys", l.keys)?;
    d.set_item("first_key", key(py, &l.first_key))?;
    d.set_item("last_key", key(py, &l.last_key))?;
    d.set_item("flags", l.flags)?;
    Ok(d)
}

fn query(
    lo: Option<String>,
    hi: Option<String>,
    after: Option<String>,
    limit: Option<u64>,
    include: Option<Vec<String>>,
    exclude: Option<Vec<String>>,
) -> PyResult<Query> {
    let (include, exclude) = (include.unwrap_or_default(), exclude.unwrap_or_default());
    let pattern = if include.is_empty() && exclude.is_empty() {
        None
    } else {
        Some(ix::Pattern::new(&include, &exclude).map_err(err)?)
    };
    Ok(Query {
        lo: lo.map(String::into_bytes),
        hi: hi.map(String::into_bytes),
        after: after.map(String::into_bytes),
        limit,
        pattern,
    })
}

/// `(rows, next)`: rows `(key, commit, new, replaced)`, one per key; `next`
/// the key to pass as `after=` for the next page, None at the end.
fn page<'py>(py: Python<'py>, p: &ix::Page) -> PyResult<Bound<'py, PyTuple>> {
    let r = &p.run;
    let rows = PyList::empty(py);
    for i in 0..r.len() {
        let e = r.rows(i).start;
        rows.append((
            key(py, r.key(i)),
            r.commit(e),
            version(py, r.new_version(e))?,
            version(py, r.replaced_version(e))?,
        ))?;
    }
    let next = p.next.as_deref().map(|k| key(py, k));
    (rows, next).into_pyobject(py)
}

/// One output's change index on an object store (`file:///…`, `s3://bucket/prefix`).
#[pyclass(module = "solera._native", frozen)]
pub struct ChangeIndex {
    pub(crate) ix: ix::ChangeIndex,
}

#[pymethods]
impl ChangeIndex {
    /// `options` are object_store's (for S3: `aws_access_key_id`, `aws_endpoint`, `aws_allow_http`, …).
    #[new]
    /// `disk_dir`: keep whole layer files there (for a remote store), up to
    /// `disk_bytes`. `disk_store` names the store (and prefix) whose layers
    /// they are, stably across restarts: the directory's files for another
    /// name are removed.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (url, options=None, *, memory_bytes=256 << 20, disk_dir=None, disk_store=None, disk_bytes=16 << 30, block_bytes=32 << 10, level=1))]
    fn new(
        py: Python<'_>,
        url: &str,
        options: Option<HashMap<String, String>>,
        memory_bytes: u64,
        disk_dir: Option<std::path::PathBuf>,
        disk_store: Option<String>,
        disk_bytes: u64,
        block_bytes: usize,
        level: i32,
    ) -> PyResult<Self> {
        guard(|| {
            let disk = match (disk_dir, disk_store) {
                (Some(dir), Some(store)) => Some(ix::DiskOptions {
                    dir,
                    bytes: disk_bytes,
                    store,
                }),
                (None, None) => None,
                _ => return Err(PyValueError::new_err("disk_dir and disk_store go together")),
            };
            let opts = ix::StoreOptions {
                memory_bytes,
                disk,
                ..Default::default()
            };
            let store = py
                .detach(|| {
                    ix::Store::from_url(
                        url,
                        options.unwrap_or_default(),
                        rt().handle().clone(),
                        opts,
                    )
                })
                .map_err(err)?;
            let write = ix::WriteOptions {
                block_bytes,
                level,
                ..Default::default()
            };
            Ok(ChangeIndex {
                ix: ix::ChangeIndex::new(store, write),
            })
        })
    }

    /// Write commit `commit`'s layer at `path` from its entries `(key, new,
    /// replaced)`, one per changed key, in any order. Returns the layer.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (path, commit, entries, *, time_ms=0, producer=Vec::new(), live_before=0))]
    fn write_commit<'py>(
        &self,
        py: Python<'py>,
        path: &str,
        commit: u64,
        entries: &Bound<'py, PyAny>,
        time_ms: u64,
        producer: Vec<u8>,
        live_before: u64,
    ) -> PyResult<Bound<'py, PyDict>> {
        guard(|| {
            let mut es: Vec<(String, Owned, Owned)> = Vec::new();
            for e in entries.try_iter()? {
                let e = e?;
                let t = e.cast::<PyTuple>()?;
                if t.len() != 3 {
                    return Err(PyTypeError::new_err("an entry is (key, new, replaced)"));
                }
                es.push((
                    t.get_item(0)?.extract()?,
                    Owned::of(&t.get_item(1)?)?,
                    Owned::of(&t.get_item(2)?)?,
                ));
            }
            es.sort_unstable_by(|a, b| a.0.cmp(&b.0));
            if let Some(w) = es.windows(2).find(|w| w[0].0 == w[1].0) {
                return Err(PyValueError::new_err(format!(
                    "{:?} is listed twice",
                    w[0].0
                )));
            }
            let mut run = Run::new();
            for (k, new, replaced) in &es {
                run.push_key(k.as_bytes());
                run.push_entry(commit, new.as_version(), replaced.as_version());
            }
            let nc = ix::NewCommit {
                commit,
                time_ms,
                producer: &producer,
                live_before,
            };
            let l = py
                .detach(|| rt().block_on(self.ix.write_commit(path, nc, &run)))
                .map_err(err)?;
            layer_out(py, &l)
        })
    }

    /// Keys whose state differs between commits `c1` and `c2`: rows `(key,
    /// newest commit in the window, version at c2, version at c1)`.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (layers, c1, c2, *, lo=None, hi=None, after=None, limit=None, include=None, exclude=None))]
    fn diff<'py>(
        &self,
        py: Python<'py>,
        layers: &Bound<'py, PyAny>,
        c1: u64,
        c2: u64,
        lo: Option<String>,
        hi: Option<String>,
        after: Option<String>,
        limit: Option<u64>,
        include: Option<Vec<String>>,
        exclude: Option<Vec<String>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        guard(|| {
            let ls = layers_in(layers)?;
            let q = query(lo, hi, after, limit, include, exclude)?;
            let p = py
                .detach(|| rt().block_on(self.ix.diff(&ls, c1, c2, &q)))
                .map_err(err)?;
            page(py, &p)
        })
    }

    /// Every key present at commit `c`: rows `(key, commit that set it, version, replaced)`.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (layers, c, *, lo=None, hi=None, after=None, limit=None, include=None, exclude=None))]
    fn scan<'py>(
        &self,
        py: Python<'py>,
        layers: &Bound<'py, PyAny>,
        c: u64,
        lo: Option<String>,
        hi: Option<String>,
        after: Option<String>,
        limit: Option<u64>,
        include: Option<Vec<String>>,
        exclude: Option<Vec<String>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        guard(|| {
            let ls = layers_in(layers)?;
            let q = query(lo, hi, after, limit, include, exclude)?;
            let p = py
                .detach(|| rt().block_on(self.ix.scan(&ls, c, &q)))
                .map_err(err)?;
            page(py, &p)
        })
    }

    /// The listed keys present at commit `c`, rows as `scan`'s.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (layers, c, keys, *, lo=None, hi=None, after=None, limit=None, include=None, exclude=None))]
    fn get<'py>(
        &self,
        py: Python<'py>,
        layers: &Bound<'py, PyAny>,
        c: u64,
        keys: Vec<String>,
        lo: Option<String>,
        hi: Option<String>,
        after: Option<String>,
        limit: Option<u64>,
        include: Option<Vec<String>>,
        exclude: Option<Vec<String>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        guard(|| {
            let ls = layers_in(layers)?;
            let q = query(lo, hi, after, limit, include, exclude)?;
            let p = py
                .detach(|| rt().block_on(self.ix.get(&ls, c, &keys, &q)))
                .map_err(err)?;
            page(py, &p)
        })
    }

    /// Every retained entry of each key: rows `(key, [(commit, new, replaced), …])`,
    /// newest first; with `commits=(lo, hi)`, only those commits' entries,
    /// reading only the layers that overlap them. A base's entries at or below
    /// its floor are the state there.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (layers, *, commits=None, lo=None, hi=None, after=None, limit=None, include=None, exclude=None))]
    fn history<'py>(
        &self,
        py: Python<'py>,
        layers: &Bound<'py, PyAny>,
        commits: Option<(u64, u64)>,
        lo: Option<String>,
        hi: Option<String>,
        after: Option<String>,
        limit: Option<u64>,
        include: Option<Vec<String>>,
        exclude: Option<Vec<String>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        guard(|| {
            let ls = layers_in(layers)?;
            let q = query(lo, hi, after, limit, include, exclude)?;
            let (a, b) = commits.unwrap_or((0, u64::MAX));
            let p = py
                .detach(|| rt().block_on(self.ix.window(&ls, a..=b, &q)))
                .map_err(err)?;
            let r = &p.run;
            let rows = PyList::empty(py);
            for i in 0..r.len() {
                let es = PyList::empty(py);
                for e in r.rows(i) {
                    es.append((
                        r.commit(e),
                        version(py, r.new_version(e))?,
                        version(py, r.replaced_version(e))?,
                    ))?;
                }
                rows.append((key(py, r.key(i)), es))?;
            }
            (rows, p.next.as_deref().map(|k| key(py, k))).into_pyobject(py)
        })
    }

    /// How many keys `diff` would return, without building them.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (layers, c1, c2, *, lo=None, hi=None, after=None, limit=None, include=None, exclude=None))]
    fn count_diff(
        &self,
        py: Python<'_>,
        layers: &Bound<'_, PyAny>,
        c1: u64,
        c2: u64,
        lo: Option<String>,
        hi: Option<String>,
        after: Option<String>,
        limit: Option<u64>,
        include: Option<Vec<String>>,
        exclude: Option<Vec<String>>,
    ) -> PyResult<u64> {
        guard(|| {
            let ls = layers_in(layers)?;
            let q = query(lo, hi, after, limit, include, exclude)?;
            let mut n = 0;
            py.detach(|| rt().block_on(self.ix.diff_with(&ls, c1, c2, &q, |_, _, _, _| n += 1)))
                .map_err(err)?;
            Ok(n)
        })
    }

    /// How many keys `scan` would return, without building them.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (layers, c, *, lo=None, hi=None, after=None, limit=None, include=None, exclude=None))]
    fn count_scan(
        &self,
        py: Python<'_>,
        layers: &Bound<'_, PyAny>,
        c: u64,
        lo: Option<String>,
        hi: Option<String>,
        after: Option<String>,
        limit: Option<u64>,
        include: Option<Vec<String>>,
        exclude: Option<Vec<String>>,
    ) -> PyResult<u64> {
        guard(|| {
            let ls = layers_in(layers)?;
            let q = query(lo, hi, after, limit, include, exclude)?;
            let mut n = 0;
            py.detach(|| rt().block_on(self.ix.state_with(&ls, c, None, &q, |_, _, _, _| n += 1)))
                .map_err(err)?;
            Ok(n)
        })
    }

    /// Merge layers `start..end` of the list into one at `path`, folding the
    /// entries at or below `cut` into a base if `start` is 0. Returns the layer.
    fn merge<'py>(
        &self,
        py: Python<'py>,
        layers: &Bound<'py, PyAny>,
        start: usize,
        end: usize,
        cut: u64,
        path: &str,
    ) -> PyResult<Bound<'py, PyDict>> {
        guard(|| {
            let ls = layers_in(layers)?;
            let l = py
                .detach(|| rt().block_on(self.ix.merge(&ls, start..end, cut, path)))
                .map_err(err)?;
            layer_out(py, &l)
        })
    }

    /// The headers of commits `lo..=hi` the layers still hold.
    fn commits<'py>(
        &self,
        py: Python<'py>,
        layers: &Bound<'py, PyAny>,
        lo: u64,
        hi: u64,
    ) -> PyResult<Bound<'py, PyList>> {
        guard(|| {
            let ls = layers_in(layers)?;
            let hs = py
                .detach(|| rt().block_on(self.ix.commits(&ls, lo, hi)))
                .map_err(err)?;
            let out = PyList::empty(py);
            for h in hs {
                let d = PyDict::new(py);
                d.set_item("commit", h.commit)?;
                d.set_item("time_ms", h.time_ms)?;
                d.set_item("flags", h.flags)?;
                d.set_item("added", h.added)?;
                d.set_item("updated", h.updated)?;
                d.set_item("removed", h.removed)?;
                d.set_item("live", h.live)?;
                d.set_item("min_key", key(py, &h.min_key))?;
                d.set_item("max_key", key(py, &h.max_key))?;
                d.set_item("producer", PyBytes::new(py, &h.producer))?;
                out.append(d)?;
            }
            Ok(out)
        })
    }

    /// The next merge the size rule asks for, as `(start, end)`, or None.
    #[staticmethod]
    fn pick_merge(layers: &Bound<'_, PyAny>, cut: u64) -> PyResult<Option<(usize, usize)>> {
        let ls = layers_in(layers)?;
        ix::check_layers(&ls).map_err(err)?;
        Ok(ix::pick_merge(&ls, cut).map(|r| (r.start, r.end)))
    }

    /// Counters since the index was made: GETs, bytes read, cache hits, …
    fn stats<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let c = self.ix.store.stats.counts();
        let d = PyDict::new(py);
        d.set_item("gets", c.gets)?;
        d.set_item("get_bytes", c.get_bytes)?;
        d.set_item("puts", c.puts)?;
        d.set_item("put_bytes", c.put_bytes)?;
        d.set_item("hits", c.hits)?;
        d.set_item("misses", c.misses)?;
        d.set_item("waits", c.waits)?;
        d.set_item("disk_reads", c.disk_reads)?;
        d.set_item("disk_bytes", c.disk_bytes)?;
        d.set_item("fills", c.fills)?;
        Ok(d)
    }
}

/// Whether `key` matches the include and exclude globs as the index reads them.
#[pyfunction]
#[pyo3(signature = (key, include, exclude=Vec::new()))]
fn index_pattern_matches(key: &str, include: Vec<String>, exclude: Vec<String>) -> PyResult<bool> {
    Ok(ix::Pattern::new(&include, &exclude)
        .map_err(err)?
        .matches(key.as_bytes()))
}

pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("ChainError", m.py().get_type::<ChainError>())?;
    m.add_class::<ChangeIndex>()?;
    m.add_function(wrap_pyfunction!(index_pattern_matches, m)?)?;
    Ok(())
}
