//! Python bindings of the stamped layers (`layers.rs`). Inputs are given as
//! `(chunks, commit, generation)`: whole blocks back to back, in key order,
//! and the commit and generation a delta's entries take (a layer's carry
//! their own).

use std::sync::Arc;

use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedBytes;
use pyo3::types::{PyBytes, PyList};

use crate::layers::{
    self, BlockWriter, DeltaWriter, Glob, LayerJoin, LayerMerge, LayerStream, Stamp, Step,
};
use crate::rows::{Cursor, Overlay, Source, Stream};
use crate::stream::Bytes;
use crate::{chunk, guard, to_py, Rows, SortedEntries};

type Input = (Vec<PyBackedBytes>, u64, u64);

fn streams(inputs: Vec<Input>) -> Vec<LayerStream> {
    inputs
        .into_iter()
        .map(|(chunks, commit, generation)| {
            LayerStream::of(
                chunks.into_iter().map(|c| Arc::new(c) as Bytes).collect(),
                Stamp { commit, generation },
            )
        })
        .collect()
}

fn glob_of(g: Option<PyBackedBytes>) -> PyResult<Option<Glob>> {
    g.map(|g| Glob::new(&g)).transpose().map_err(to_py)
}

fn opt_bytes<'py>(py: Python<'py>, b: Option<&[u8]>) -> Bound<'py, PyAny> {
    match b {
        Some(b) => PyBytes::new(py, b).into_any(),
        None => py.None().into_bound(py),
    }
}

/// Files as `(data, entries, first key, last key)`.
fn files<'py>(py: Python<'py>, w: &mut BlockWriter) -> PyResult<Bound<'py, PyList>> {
    let out = PyList::empty(py);
    while let Some(f) = w.ready.pop_front() {
        out.append((
            PyBytes::new(py, &f.data),
            f.entries,
            PyBytes::new(py, &f.first),
            PyBytes::new(py, &f.last),
        ))?;
    }
    Ok(out)
}

/// Δ(P, H) over `inputs` (newest first, those ending after P; P None is
/// −∞): `(keys, present at P, present at H — one byte each —, generations,
/// payloads, the last key delivered when `limit` stopped it, else None)`.
#[pyfunction]
#[pyo3(signature = (inputs, p, *, after=None, upto=None, limit=usize::MAX, glob=None, keys=None))]
#[allow(clippy::type_complexity, clippy::too_many_arguments)]
pub fn layers_scan<'py>(
    py: Python<'py>,
    inputs: Vec<Input>,
    p: Option<u64>,
    after: Option<PyBackedBytes>,
    upto: Option<PyBackedBytes>,
    limit: usize,
    glob: Option<PyBackedBytes>,
    keys: Option<Vec<PyBackedBytes>>,
) -> PyResult<(
    Bound<'py, PyList>,
    Bound<'py, PyBytes>,
    Bound<'py, PyBytes>,
    Vec<u64>,
    Bound<'py, PyList>,
    Option<Bound<'py, PyBytes>>,
)> {
    guard(|| {
        let glob = glob_of(glob)?;
        let mut out = Vec::new();
        let last = py
            .detach(|| {
                let ks: Option<Vec<&[u8]>> = keys
                    .as_ref()
                    .map(|k| k.iter().map(|x| x.as_ref()).collect());
                layers::scan(
                    streams(inputs),
                    p,
                    after.as_deref(),
                    upto.as_deref(),
                    limit,
                    glob,
                    ks.as_deref(),
                    &mut out,
                )
            })
            .map_err(to_py)?;
        let payloads = PyList::empty(py);
        for d in &out {
            payloads.append(opt_bytes(py, d.payload.as_deref()))?;
        }
        Ok((
            PyList::new(py, out.iter().map(|d| PyBytes::new(py, &d.key)))?,
            PyBytes::new(py, &out.iter().map(|d| d.before as u8).collect::<Vec<_>>()),
            PyBytes::new(py, &out.iter().map(|d| d.after as u8).collect::<Vec<_>>()),
            out.iter().map(|d| d.generation).collect(),
            payloads,
            last.map(|k| PyBytes::new(py, &k)),
        ))
    })
}

/// Per key (sorted, unique), the newest entry among `inputs` (newest
/// first): `(present, generation, payload)`, or None.
#[pyfunction]
pub fn layers_lookup<'py>(
    py: Python<'py>,
    inputs: Vec<Input>,
    keys: Vec<PyBackedBytes>,
) -> PyResult<Bound<'py, PyList>> {
    guard(|| {
        let found = py
            .detach(|| {
                let ks: Vec<&[u8]> = keys.iter().map(|k| k.as_ref()).collect();
                layers::lookup(streams(inputs), &ks)
            })
            .map_err(to_py)?;
        let out = PyList::empty(py);
        for f in found {
            match f {
                None => out.append(py.None())?,
                Some(e) => {
                    out.append((e.present, e.generation, opt_bytes(py, e.payload.as_deref())))?
                }
            }
        }
        Ok(out)
    })
}

/// A file's entries, every block checked (a delta's at `commit`,
/// `generation`): `(key, present, start, commit, generation, flips, payload,
/// replaced)` each.
#[pyfunction]
pub fn layers_decode<'py>(
    py: Python<'py>,
    data: PyBackedBytes,
    commit: u64,
    generation: u64,
) -> PyResult<Bound<'py, PyList>> {
    guard(|| {
        let entries = py
            .detach(|| layers::decode(&data, Stamp { commit, generation }))
            .map_err(to_py)?;
        let out = PyList::empty(py);
        for e in entries {
            out.append((
                PyBytes::new(py, &e.key),
                e.present,
                e.start,
                e.commit,
                e.generation,
                e.flips,
                opt_bytes(py, e.payload.as_deref()),
                e.replaced,
            ))?;
        }
        Ok(out)
    })
}

/// A part's index: `(first keys, files, offsets, lengths, entries, newest
/// commits)`, one per block.
#[pyfunction]
#[allow(clippy::type_complexity)]
pub fn layers_index<'py>(
    py: Python<'py>,
    data: PyBackedBytes,
) -> PyResult<(
    Bound<'py, PyList>,
    Vec<u32>,
    Vec<u64>,
    Vec<u32>,
    Vec<u32>,
    Vec<u64>,
)> {
    guard(|| {
        let blocks = layers::index_decode(&data).map_err(to_py)?;
        Ok((
            PyList::new(py, blocks.iter().map(|b| PyBytes::new(py, &b.first)))?,
            blocks.iter().map(|b| b.file).collect(),
            blocks.iter().map(|b| b.offset).collect(),
            blocks.iter().map(|b| b.length).collect(),
            blocks.iter().map(|b| b.entries).collect(),
            blocks.iter().map(|b| b.newest).collect(),
        ))
    })
}

/// One byte per key: whether it matches the glob.
#[pyfunction]
pub fn glob_match<'py>(
    py: Python<'py>,
    glob: PyBackedBytes,
    keys: Vec<PyBackedBytes>,
) -> PyResult<Bound<'py, PyBytes>> {
    guard(|| {
        let g = Glob::new(&glob).map_err(to_py)?;
        Ok(PyBytes::new(
            py,
            &keys.iter().map(|k| g.matches(k) as u8).collect::<Vec<_>>(),
        ))
    })
}

/// One byte per block of first keys `firsts` (sorted): whether a key from
/// the block's first to the next block's first (or `upper` for the last;
/// None: unbounded) may match the glob.
#[pyfunction]
#[pyo3(signature = (glob, firsts, upper=None))]
pub fn glob_blocks<'py>(
    py: Python<'py>,
    glob: PyBackedBytes,
    firsts: Vec<PyBackedBytes>,
    upper: Option<PyBackedBytes>,
) -> PyResult<Bound<'py, PyBytes>> {
    guard(|| {
        let g = Glob::new(&glob).map_err(to_py)?;
        let out: Vec<u8> = py.detach(|| {
            (0..firsts.len())
                .map(|i| {
                    let hi: Option<&[u8]> = if i + 1 < firsts.len() {
                        Some(&firsts[i + 1])
                    } else {
                        upper.as_deref()
                    };
                    g.may_match_between(&firsts[i], hi) as u8
                })
                .collect()
        });
        Ok(PyBytes::new(py, &out))
    })
}

/// A commit's delta from its sorted written entries, resolved against
/// `inputs` (newest first: the blocks the keys fall in, or whole parts):
/// `(files, index, added, changed, removed)`. With `replaced`, each change
/// records the generation it replaced (an immutable store's cleanup).
#[pyfunction]
#[pyo3(signature = (inputs, written, *, replaced=false, block_size=16384, level=1, file_limit=67108864))]
#[allow(clippy::type_complexity)]
pub fn layers_resolve<'py>(
    py: Python<'py>,
    inputs: Vec<Input>,
    written: PyRef<'_, SortedEntries>,
    replaced: bool,
    block_size: usize,
    level: i32,
    file_limit: usize,
) -> PyResult<(Bound<'py, PyList>, Bound<'py, PyBytes>, u64, u64, u64)> {
    guard(|| {
        let sorted = written.inner.clone();
        let mut w = DeltaWriter::new(block_size, level, file_limit, replaced);
        py.detach(|| layers::resolve(streams(inputs), &sorted, &mut w))
            .map_err(to_py)?;
        let index = layers::index_encode(&w.out.blocks);
        Ok((
            files(py, &mut w.out)?,
            PyBytes::new(py, &index),
            w.added,
            w.changed,
            w.removed,
        ))
    })
}

enum Kind {
    Merge(Box<LayerMerge>),
    Join(Box<LayerJoin>),
}

/// A streaming job over layers, driven by `step()`: `("run", r)` when input
/// `r` needs `feed(r, data)` (whole blocks) or `end(r)`; `("rows", None)`
/// when a streamed write needs `feed_rows` or `end_rows`; `("file", (part,
/// data, entries, first, last))` for each file written (`part` "main" or
/// "side"; a join's are "main"); None when done. `finish()` then gives the
/// parts' indexes and counts.
#[pyclass(module = "solera._native")]
pub struct LayerJob {
    kind: Kind,
    key: Option<String>,
}

#[pymethods]
impl LayerJob {
    /// Merging adjacent layers: `inputs` oldest first, each `(commit,
    /// generation)` (what a delta's entries take). Flips at or below `cut`
    /// go (None: no cut yet); `bottom` when the oldest input is the base; with `check`, every
    /// key's presence is checked against its start and flips.
    #[staticmethod]
    #[pyo3(signature = (inputs, *, cut, bottom, check=false, block_size=16384, level=1, file_limit=67108864))]
    #[allow(clippy::too_many_arguments)]
    fn merge(
        inputs: Vec<(u64, u64)>,
        cut: Option<u64>,
        bottom: bool,
        check: bool,
        block_size: usize,
        level: i32,
        file_limit: usize,
    ) -> LayerJob {
        let ins = inputs
            .into_iter()
            .map(|(commit, generation)| LayerStream::new(Stamp { commit, generation }))
            .collect();
        LayerJob {
            kind: Kind::Merge(Box::new(LayerMerge::new(
                ins, cut, bottom, check, block_size, level, file_limit,
            ))),
            key: None,
        }
    }

    /// A streamed write: `rows` (a `Rows` table), or None for sorted chunks
    /// fed by `feed_rows` (with `key`, rows keyed by that column), or
    /// `sorted` entries; merge-joined with `inputs` (newest first). A patch,
    /// or with `replace` the whole new content.
    #[staticmethod]
    #[pyo3(signature = (inputs, *, rows=None, sorted=None, replace=false, replaced=false, key=None, overlay=None, block_size=16384, level=1, file_limit=67108864))]
    #[allow(clippy::too_many_arguments)]
    fn join(
        inputs: Vec<(u64, u64)>,
        rows: Option<PyRef<'_, Rows>>,
        sorted: Option<PyRef<'_, SortedEntries>>,
        replace: bool,
        replaced: bool,
        key: Option<String>,
        overlay: Option<PyRef<'_, SortedEntries>>,
        block_size: usize,
        level: i32,
        file_limit: usize,
    ) -> PyResult<LayerJob> {
        guard(|| {
            let src = match (rows, sorted, overlay) {
                (Some(r), None, None) => Source::Table(Box::new(Cursor::new(r.table.clone()))),
                (None, Some(s), None) => Source::Entries(s.inner.clone(), 0),
                (None, None, None) => Source::Stream(Stream::default()),
                (None, None, Some(o)) => {
                    Source::Overlay(Box::new(Overlay::new(Stream::default(), o.inner.clone())))
                }
                _ => {
                    return Err(PyValueError::new_err(
                        "one of rows, sorted, or a stream (with an overlay)",
                    ))
                }
            };
            let ins = inputs
                .into_iter()
                .map(|(commit, generation)| LayerStream::new(Stamp { commit, generation }))
                .collect();
            let delta = DeltaWriter::new(block_size, level, file_limit, replaced);
            let job = LayerJoin::new(src, ins, replace, delta).map_err(to_py)?;
            Ok(LayerJob {
                kind: Kind::Join(Box::new(job)),
                key,
            })
        })
    }

    fn feed(&mut self, r: usize, data: PyBackedBytes) -> PyResult<()> {
        let m = match &mut self.kind {
            Kind::Merge(j) => &mut j.merger,
            Kind::Join(j) => &mut j.merger,
        };
        let Some(s) = m.inputs.get_mut(r) else {
            return Err(PyValueError::new_err(format!("no input {r}")));
        };
        s.feed(Arc::new(data));
        Ok(())
    }

    fn end(&mut self, r: usize) -> PyResult<()> {
        let m = match &mut self.kind {
            Kind::Merge(j) => &mut j.merger,
            Kind::Join(j) => &mut j.merger,
        };
        let Some(s) = m.inputs.get_mut(r) else {
            return Err(PyValueError::new_err(format!("no input {r}")));
        };
        s.end();
        Ok(())
    }

    fn feed_rows(&mut self, py: Python<'_>, rows: Bound<'_, PyAny>) -> PyResult<()> {
        guard(|| {
            let keys = chunk(py, &rows, self.key.as_deref())?;
            let Kind::Join(j) = &mut self.kind else {
                return Err(PyTypeError::new_err("not a write"));
            };
            match j.src.stream() {
                Some(s) => s.feed(keys).map_err(to_py),
                None => Err(PyTypeError::new_err("not a streamed write")),
            }
        })
    }

    fn end_rows(&mut self) -> PyResult<()> {
        let Kind::Join(j) = &mut self.kind else {
            return Err(PyTypeError::new_err("not a write"));
        };
        if let Some(s) = j.src.stream() {
            s.end();
        }
        Ok(())
    }

    fn step<'py>(
        &mut self,
        py: Python<'py>,
    ) -> PyResult<Option<(&'static str, Bound<'py, PyAny>)>> {
        guard(|| {
            let kind = &mut self.kind;
            let step = py
                .detach(|| match kind {
                    Kind::Merge(j) => j.step(),
                    Kind::Join(j) => j.step(),
                })
                .map_err(to_py)?;
            Ok(match step {
                Step::Run(r) => Some(("run", r.into_pyobject(py)?.into_any())),
                Step::Rows => Some(("rows", py.None().into_bound(py))),
                Step::Done => None,
                Step::File => {
                    let (part, w) = match &mut self.kind {
                        Kind::Merge(j) if !j.main.ready.is_empty() => ("main", &mut j.main),
                        Kind::Merge(j) => ("side", &mut j.side),
                        Kind::Join(j) => ("main", &mut j.delta.out),
                    };
                    let f = w.ready.pop_front().unwrap();
                    let t = (
                        part,
                        PyBytes::new(py, &f.data),
                        f.entries,
                        PyBytes::new(py, &f.first),
                        PyBytes::new(py, &f.last),
                    );
                    Some(("file", t.into_pyobject(py)?.into_any()))
                }
            })
        })
    }

    /// After the last step: the parts' indexes (`main`, `side`) and counts.
    fn finish<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let d = pyo3::types::PyDict::new(py);
        match &self.kind {
            Kind::Merge(j) => {
                d.set_item(
                    "main",
                    PyBytes::new(py, &layers::index_encode(&j.main.blocks)),
                )?;
                d.set_item(
                    "side",
                    PyBytes::new(py, &layers::index_encode(&j.side.blocks)),
                )?;
                d.set_item("read", j.read)?;
                d.set_item("dropped", j.dropped)?;
                d.set_item("written", j.main.entries + j.side.entries)?;
            }
            Kind::Join(j) => {
                d.set_item(
                    "main",
                    PyBytes::new(py, &layers::index_encode(&j.delta.out.blocks)),
                )?;
                d.set_item("added", j.delta.added)?;
                d.set_item("changed", j.delta.changed)?;
                d.set_item("removed", j.delta.removed)?;
                d.set_item("written", j.delta.out.entries)?;
            }
        }
        Ok(d.into_any())
    }
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<LayerJob>()?;
    m.add_function(wrap_pyfunction!(layers_scan, m)?)?;
    m.add_function(wrap_pyfunction!(layers_lookup, m)?)?;
    m.add_function(wrap_pyfunction!(layers_decode, m)?)?;
    m.add_function(wrap_pyfunction!(layers_index, m)?)?;
    m.add_function(wrap_pyfunction!(layers_resolve, m)?)?;
    m.add_function(wrap_pyfunction!(glob_match, m)?)?;
    m.add_function(wrap_pyfunction!(glob_blocks, m)?)?;
    Ok(())
}
