//! Python bindings of staleness (B7, `solera_tracking::stale`): the events
//! that tell the core an edge's conditions and an output's full run, and the
//! questions the engine and the console ask. Each is one call; the ones that
//! read the index release the GIL.
//!
//! Statuses are dicts: `{"state": "fresh" | "stale" | "held" | "unknown" |
//! "unwatched", "key": str | None, "reason": str | None, "since_ms": int | None}`.
//! "unknown" means what is known no longer holds: `stale_refresh` it.

use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::RwLock;

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyTuple};
use solera_tracking as tr;
use tr::event::{Event, Part};
use tr::plan::Indexes;
use tr::progress::Key;
use tr::stale::{Counts, Due, Partition, SetWatch, Status, Watch};

use crate::index_py::{key, rt};
use crate::tracking_py::{conditions, err, key_in};

/// Failed keys waiting for their retry, per edge: `{edge: [key, ...]}`.
/// B6's due index replaces it.
pub(crate) struct NotDue(HashMap<Key, HashSet<Key>>);

impl NotDue {
    pub(crate) fn of(d: Option<&Bound<'_, PyDict>>) -> PyResult<NotDue> {
        let mut out = HashMap::new();
        if let Some(d) = d {
            for (e, ks) in d.iter() {
                let mut keys = HashSet::new();
                for k in ks.try_iter()? {
                    keys.insert(key_in(&k?)?);
                }
                out.insert(e.extract::<Vec<u8>>()?.into(), keys);
            }
        }
        Ok(NotDue(out))
    }
}

impl Due for NotDue {
    fn not_due(&self, edge: &[u8], key: &[u8]) -> bool {
        self.0.get(edge).is_some_and(|s| s.contains(key))
    }

    fn revision(&self, edge: &[u8]) -> u64 {
        let Some(keys) = self.0.get(edge) else {
            return 0;
        };
        let mut keys: Vec<&Key> = keys.iter().collect();
        keys.sort();
        let mut h = std::hash::DefaultHasher::new();
        keys.hash(&mut h);
        h.finish()
    }
}

fn one_event(py: Python<'_>, parts: Vec<Part>) -> Option<Bound<'_, PyBytes>> {
    (!parts.is_empty()).then(|| PyBytes::new(py, &Event::of(parts).encode()))
}

pub(crate) fn event_watch<'py>(
    py: Python<'py>,
    edge: Vec<u8>,
    conds: &Bound<'py, PyAny>,
    own: Option<Vec<u8>>,
    gate: bool,
) -> PyResult<Bound<'py, PyBytes>> {
    let part = Part::Watch(SetWatch {
        edge: edge.into(),
        watch: Watch {
            conditions: conditions(conds)?,
            own: own.map(Into::into),
            gate,
        },
    });
    Ok(one_event(py, vec![part]).expect("one part"))
}

pub(crate) fn event_incomplete(py: Python<'_>, output: Vec<u8>, on: bool) -> Bound<'_, PyBytes> {
    let part = Part::Incomplete(tr::stale::Incomplete {
        output: output.into(),
        on,
    });
    one_event(py, vec![part]).expect("one part")
}

fn asks(
    state: &RwLock<tr::Tracking>,
    edges: Option<Vec<Vec<u8>>>,
) -> PyResult<Vec<tr::stale::Ask>> {
    let t = state.read().unwrap();
    let edges: Vec<Vec<u8>> = match edges {
        Some(e) => e,
        None => t
            .stale
            .inputs
            .iter()
            .filter(|(_, i)| i.watch.is_some())
            .map(|(e, _)| e.to_vec())
            .collect(),
    };
    let mut out = vec![];
    for e in edges {
        if t.stale_status(&e).map_err(err)? == Status::Unknown {
            out.push(t.stale_ask(&e).map_err(err)?);
        }
    }
    Ok(out)
}

/// Brings edges current (all watched keyed edges, by default): the event to
/// journal and apply, or None if every one already is.
pub(crate) fn refresh<'py>(
    py: Python<'py>,
    state: &RwLock<tr::Tracking>,
    ixs: &dyn Indexes,
    edges: Option<Vec<Vec<u8>>>,
    time_ms: u64,
    not_due: Option<&Bound<'py, PyDict>>,
) -> PyResult<Option<Bound<'py, PyBytes>>> {
    let due = NotDue::of(not_due)?;
    let asks = asks(state, edges)?;
    let parts = py
        .detach(|| {
            rt().block_on(async {
                let mut parts = vec![];
                for a in &asks {
                    if let Some(w) = tr::stale::refresh(a, ixs, &due, time_ms).await? {
                        parts.push(Part::Witness(w));
                    }
                }
                Ok::<_, tr::Error>(parts)
            })
        })
        .map_err(err)?;
    Ok(one_event(py, parts))
}

pub(crate) fn retry_due<'py>(
    py: Python<'py>,
    state: &RwLock<tr::Tracking>,
    ixs: &dyn Indexes,
    edge: Vec<u8>,
    keys: Vec<String>,
    time_ms: u64,
    not_due: Option<&Bound<'py, PyDict>>,
) -> PyResult<Option<Bound<'py, PyBytes>>> {
    let due = NotDue::of(not_due)?;
    let ask = state.read().unwrap().stale_ask(&edge).map_err(err)?;
    let mut keys: Vec<Key> = keys.into_iter().map(|k| k.into_bytes().into()).collect();
    keys.sort();
    let w = py
        .detach(|| rt().block_on(tr::stale::retry_due(&ask, ixs, &due, &keys, time_ms)))
        .map_err(err)?;
    Ok(one_event(py, w.into_iter().map(Part::Witness).collect()))
}

fn status_dict<'py>(py: Python<'py>, s: &Status) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    let (name, k, reason, since) = match s {
        Status::Unwatched => ("unwatched", None, None, None),
        Status::Held => ("held", None, Some("upstream incomplete"), None),
        Status::Unknown => ("unknown", None, None, None),
        Status::Fresh => ("fresh", None, None, None),
        Status::Stale {
            key,
            reason,
            since_ms,
        } => (
            "stale",
            key.as_deref(),
            Some(reason.name()),
            Some(*since_ms),
        ),
    };
    d.set_item("state", name)?;
    d.set_item("key", k.map(|k| key(py, k)))?;
    d.set_item("reason", reason)?;
    d.set_item("since_ms", since)?;
    Ok(d)
}

/// What is known of each edge (all edges, by default), without reading the index.
pub(crate) fn status<'py>(
    py: Python<'py>,
    state: &RwLock<tr::Tracking>,
    edges: Option<Vec<Vec<u8>>>,
) -> PyResult<Bound<'py, PyDict>> {
    let t = state.read().unwrap();
    let edges: Vec<Vec<u8>> = edges.unwrap_or_else(|| t.edges.keys().map(|e| e.to_vec()).collect());
    let out = PyDict::new(py);
    for e in edges {
        let s = t.stale_status(&e).map_err(err)?;
        out.set_item(PyBytes::new(py, &e), status_dict(py, &s)?)?;
    }
    Ok(out)
}

/// How many keys the edge owes, by class, and every reason that holds;
/// cached until it or its upstream moves. None while the gate holds it (D197).
pub(crate) fn count<'py>(
    py: Python<'py>,
    state: &RwLock<tr::Tracking>,
    ixs: &dyn Indexes,
    counts: &Counts,
    edge: Vec<u8>,
    not_due: Option<&Bound<'py, PyDict>>,
) -> PyResult<Option<Bound<'py, PyDict>>> {
    let due = NotDue::of(not_due)?;
    let ask = state.read().unwrap().stale_ask(&edge).map_err(err)?;
    let Some(o) = py
        .detach(|| rt().block_on(counts.get(&ask, ixs, &due)))
        .map_err(err)?
    else {
        return Ok(None);
    };
    let d = PyDict::new(py);
    for (name, v) in ["added", "updated", "removed"].into_iter().zip(o.counts) {
        d.set_item(name, v)?;
    }
    let reasons: Vec<&str> = o.reasons.iter().map(|r| r.name()).collect();
    d.set_item("reasons", reasons)?;
    Ok(Some(d))
}

/// A page of the keys the edge owes: `([(key, class), ...], next)`.
pub(crate) fn keys<'py>(
    py: Python<'py>,
    state: &RwLock<tr::Tracking>,
    ixs: &dyn Indexes,
    edge: Vec<u8>,
    after: Option<String>,
    limit: usize,
) -> PyResult<Bound<'py, PyTuple>> {
    let ask = state.read().unwrap().stale_ask(&edge).map_err(err)?;
    let after: Option<Key> = after.map(|k| k.into_bytes().into());
    let (rows, next) = py
        .detach(|| rt().block_on(ask.keys(ixs, after.as_ref(), limit)))
        .map_err(err)?;
    let out = PyList::empty(py);
    for c in &rows {
        out.append((key(py, &c.key), c.class.name()))?;
    }
    (out, next.as_deref().map(|k| key(py, k))).into_pyobject(py)
}

/// D177: the edge has processed every key upstream holds under its patterns.
pub(crate) fn complete(
    py: Python<'_>,
    state: &RwLock<tr::Tracking>,
    ixs: &dyn Indexes,
    edge: Vec<u8>,
) -> PyResult<bool> {
    let ask = state.read().unwrap().stale_ask(&edge).map_err(err)?;
    py.detach(|| rt().block_on(ask.complete(ixs))).map_err(err)
}

/// One partition's input edges and outputs.
pub(crate) type Names = (Vec<Vec<u8>>, Vec<Vec<u8>>);

/// Per partition `(inputs, outputs)`: `(stale: bool | None, upstream_stale: bool)`.
pub(crate) fn graph(
    state: &RwLock<tr::Tracking>,
    partitions: Vec<Names>,
) -> PyResult<Vec<(Option<bool>, bool)>> {
    let parts: Vec<Partition> = partitions
        .into_iter()
        .map(|(i, o)| Partition {
            inputs: i.into_iter().map(Into::into).collect(),
            outputs: o.into_iter().map(Into::into).collect(),
        })
        .collect();
    tr::stale::graph(&state.read().unwrap(), &parts).map_err(err)
}
