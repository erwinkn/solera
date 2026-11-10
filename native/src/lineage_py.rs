//! Python bindings of lineage (B10, `solera_tracking::lineage`): what a batch
//! received, a key's trace through the batches that read it, and flow per
//! window of time. Each is one call; the ones that read the index release
//! the GIL. Records cross as the bytes `Record::encode` wrote.
//!
//! An answer that history no longer reaches is
//! `{"state": "not_retained", "output": bytes, "commit": int}`: the input's
//! index was cut past a commit the record names.

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};
use solera_tracking as tr;
use tr::codec::Ver;
use tr::lineage::{Answer, Received};
use tr::plan::Indexes;
use tr::progress::{Holding, Reason};
use tr::record::Record;

use crate::guard;
use crate::index_py::{key, rt, version};
use crate::tracking_py::{err, Tracking};

fn reason(r: Reason) -> &'static str {
    match r {
        Reason::Failed => "failed",
        Reason::HeldBack => "held_back",
        Reason::Deviation => "deviation",
        Reason::KeyList => "key_list",
        Reason::Unsettled => "unsettled",
    }
}

/// `{"key", "class", "held", "held_output", "planned", "deviated",
/// "loaded", "reason", "deleted"}`: `held` is the version held before, None
/// when nothing was or when `held_output` (the consumer held whatever its own
/// output held, at no known version); `loaded` is the version loaded where
/// `deviated`.
fn row<'py>(py: Python<'py>, r: &Received) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    d.set_item("key", key(py, &r.key))?;
    d.set_item("class", r.class.name())?;
    let held = match &r.held {
        Holding::Version(v, _) => v.clone(),
        _ => Ver::Absent,
    };
    d.set_item("held", version(py, held.as_version())?)?;
    d.set_item("held_output", r.held == Holding::Held)?;
    d.set_item("planned", version(py, r.planned.as_version())?)?;
    d.set_item("deviated", r.loaded.is_some())?;
    let loaded = r.loaded.as_ref().unwrap_or(&Ver::Absent);
    d.set_item("loaded", version(py, loaded.as_version())?)?;
    d.set_item("reason", r.reason.map(reason))?;
    d.set_item("deleted", r.deleted)?;
    Ok(d)
}

fn answer<'py, T>(
    py: Python<'py>,
    a: &Answer<T>,
    found: impl FnOnce(&Bound<'py, PyDict>, &T) -> PyResult<()>,
) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    match a {
        Answer::Found(t) => {
            d.set_item("state", "found")?;
            found(&d, t)?;
        }
        Answer::NotRetained { output, commit } => {
            d.set_item("state", "not_retained")?;
            d.set_item("output", PyBytes::new(py, output))?;
            d.set_item("commit", *commit)?;
        }
    }
    Ok(d)
}

/// What one batch received (HIS-7, HIS-8), from its record: `{"state":
/// "found", "keys": [row, ...]}` in key order, a row as `row` says. `own`
/// is the consumer's own output, which a record whose progress was held
/// needs; `cleaned` the input's cleanup cursor (0 before any cleanup). A
/// digest that does not match raises ValueError naming the batch. Costs what
/// planning the batch cost.
#[pyfunction]
#[pyo3(signature = (tracking, record, own=None, cleaned=0))]
fn lineage_input<'py>(
    py: Python<'py>,
    tracking: &Bound<'py, Tracking>,
    record: &[u8],
    own: Option<Vec<u8>>,
    cleaned: u64,
) -> PyResult<Bound<'py, PyDict>> {
    guard(|| {
        let t = tracking.get();
        let rec = Record::decode(record).map_err(err)?;
        let logs = t.state.read().unwrap().logs(&rec, own.as_deref());
        let ixs = t.indexes.read().unwrap().clone();
        let a = py
            .detach(|| rt().block_on(tr::lineage::input(&logs, &rec, ixs.as_ref(), cleaned)))
            .map_err(err)?;
        answer(py, &a, |d, keys| {
            let rows = PyList::empty(py);
            for r in keys {
                rows.append(row(py, r)?)?;
            }
            d.set_item("keys", rows)
        })
    })
}

/// The trace of `key` on `output` (§12): `{"floor", "at_floor", "changes",
/// "batches"}`. `changes` are `(commit, version, replaced)`, newest first,
/// above the floor of the output's index; `at_floor` is the key's version
/// there once older changes were folded. `batches` answers each of
/// `records` (`(record, own)` pairs, each reading `output`): None when the
/// batch did not take the key, else `{"state": "found", "key": row}`.
#[pyfunction]
#[pyo3(signature = (tracking, output, key, records, cleaned=0))]
fn lineage_trace<'py>(
    py: Python<'py>,
    tracking: &Bound<'py, Tracking>,
    output: Vec<u8>,
    key: String,
    records: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    cleaned: u64,
) -> PyResult<Bound<'py, PyDict>> {
    guard(|| {
        let t = tracking.get();
        let recs = records
            .iter()
            .map(|(b, own)| {
                let rec = Record::decode(b).map_err(err)?;
                if *rec.output != *output {
                    return Err(pyo3::exceptions::PyValueError::new_err(
                        "a traced record reads the traced output",
                    ));
                }
                Ok((rec, own.as_deref()))
            })
            .collect::<PyResult<Vec<_>>>()?;
        let (layers, logs) = {
            let s = t.state.read().unwrap();
            let layers = s
                .outputs
                .get(&output[..])
                .map_or(vec![], |o| o.layers.clone());
            let logs: Vec<_> = recs.iter().map(|(r, own)| s.logs(r, *own)).collect();
            (layers, logs)
        };
        let ixs = t.indexes.read().unwrap().clone();
        let k = key.as_bytes();
        let (h, got) = py
            .detach(|| {
                rt().block_on(async {
                    let ix = ixs.index(&output)?;
                    let h = tr::lineage::history(ix, &layers, k).await?;
                    let mut got = Vec::with_capacity(recs.len());
                    for ((rec, _), logs) in recs.iter().zip(&logs) {
                        got.push(
                            tr::lineage::received_key(logs, rec, k, ixs.as_ref(), cleaned).await?,
                        );
                    }
                    Ok::<_, tr::Error>((h, got))
                })
            })
            .map_err(err)?;
        let d = PyDict::new(py);
        d.set_item("floor", h.floor)?;
        let at_floor = h.at_floor.unwrap_or(Ver::Absent);
        d.set_item("at_floor", version(py, at_floor.as_version())?)?;
        let changes = PyList::empty(py);
        for (c, v, was) in &h.changes {
            changes.append((
                *c,
                version(py, v.as_version())?,
                version(py, was.as_version())?,
            ))?;
        }
        d.set_item("changes", changes)?;
        let batches = PyList::empty(py);
        for a in &got {
            match a {
                Answer::Found(None) => batches.append(py.None())?,
                a => batches.append(answer(py, a, |d, r| {
                    d.set_item("key", row(py, r.as_ref().expect("a key found"))?)
                })?)?,
            }
        }
        d.set_item("batches", batches)?;
        Ok(d)
    })
}

/// Flow (§12) per window of `bucket_ms`, oldest first, from `(time_ms,
/// record)` pairs: `{"start_ms", "batches", "full_runs", "added",
/// "updated", "removed", "unchanged"}`. Reuse is a window's keys over the
/// upstream's live count, which its commit headers hold.
#[pyfunction]
fn lineage_flow<'py>(
    py: Python<'py>,
    records: Vec<(u64, Vec<u8>)>,
    bucket_ms: u64,
) -> PyResult<Bound<'py, PyList>> {
    guard(|| {
        let recs = records
            .iter()
            .map(|(t, b)| Ok((*t, Record::decode(b)?)))
            .collect::<tr::Result<Vec<_>>>()
            .map_err(err)?;
        let flow = tr::lineage::flow(recs.iter().map(|(t, r)| (*t, r)), bucket_ms).map_err(err)?;
        let out = PyList::empty(py);
        for f in flow {
            let d = PyDict::new(py);
            d.set_item("start_ms", f.start_ms)?;
            d.set_item("batches", f.batches)?;
            d.set_item("full_runs", f.resets)?;
            for (name, n) in ["added", "updated", "removed", "unchanged"]
                .iter()
                .zip(f.counts)
            {
                d.set_item(*name, n)?;
            }
            out.append(d)?;
        }
        Ok(out)
    })
}

pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(lineage_input, m)?)?;
    m.add_function(wrap_pyfunction!(lineage_trace, m)?)?;
    m.add_function(wrap_pyfunction!(lineage_flow, m)?)
}
