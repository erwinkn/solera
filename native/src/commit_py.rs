//! Python bindings of output commits (B4, `solera_tracking::commit`): build
//! and write a commit from the changed keys' transport form, in one call with
//! the GIL released; and that transport form from Python lists.

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};
use solera_index::Version;
use solera_tracking as tr;
use solera_tracking::commit::{self, Change, Changes, Mode, Reader, Spec};
use solera_tracking::plan::Indexes;

use crate::index_py::{self, key, layer_out, rt};
use crate::{guard, FormatError};

pub(crate) fn err(e: tr::Error) -> PyErr {
    match e {
        tr::Error::Format(m) => FormatError::new_err(m),
        tr::Error::Invalid(m) => PyValueError::new_err(m),
        tr::Error::Index(e) => index_py::err(e),
    }
}

/// `Tracking.build_commit`: the next commit of `output`, as the state holds it.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_commit<'py>(
    py: Python<'py>,
    state: &std::sync::RwLock<tr::Tracking>,
    out: tr::Output,
    ixs: &dyn Indexes,
    output: &[u8],
    changes: &[u8],
    version: u64,
    path: &str,
    time_ms: u64,
    producer: &[u8],
    mode: &str,
    at: Option<u64>,
    ends_full_run: bool,
    collect: usize,
) -> PyResult<Option<Bound<'py, PyDict>>> {
    let mode = match (mode, at) {
        ("patch", None) => Mode::Patch,
        ("replace", None) => Mode::Replace,
        ("clear", None) => Mode::Clear,
        ("report", Some(at)) => Mode::Report(at),
        _ => {
            return Err(PyValueError::new_err(format!(
                "mode {mode:?} with at={at:?}: patch, replace or clear without at, report with it"
            )))
        }
    };
    let spec = Spec {
        version,
        time_ms,
        producer,
        path,
        mode,
        ends_full_run,
        collect,
    };
    let id: tr::Id = output.into();
    let mut notice = tr::stale::Notice::new(
        &state.read().unwrap(),
        output,
        out.head + 1,
        spec.mode == Mode::Clear,
        time_ms,
    );
    let built = guard(|| {
        py.detach(|| {
            let mut src = Reader::new(changes)?;
            let mut each = |k: &[u8], v: Version<'_>| notice.push(k, v);
            rt().block_on(commit::build_each(
                ixs, &id, &out, spec, &mut src, &mut each,
            ))
        })
        .map_err(err)
    })?;
    let Some(b) = built else {
        return Ok(None);
    };
    let d = PyDict::new(py);
    let mut parts = vec![tr::Part::OutputCommit(b.part.clone())];
    parts.extend(notice.finish());
    let ev = tr::Event::of(parts).encode();
    d.set_item("event", PyBytes::new(py, &ev))?;
    d.set_item("commit", b.part.layer.hi)?;
    d.set_item("layer", layer_out(py, &b.part.layer)?)?;
    d.set_item("live", b.part.live)?;
    d.set_item("added", b.added)?;
    d.set_item("updated", b.updated)?;
    d.set_item("removed", b.removed)?;
    let keys = |ks: &[Box<[u8]>]| PyList::new(py, ks.iter().map(|k| key(py, k)));
    match &b.keys {
        Some(k) => {
            d.set_item("upserted_keys", keys(&k.upserted)?)?;
            d.set_item("removed_keys", keys(&k.removed)?)?;
        }
        None => {
            d.set_item("upserted_keys", py.None())?;
            d.set_item("removed_keys", py.None())?;
        }
    }
    Ok(Some(d))
}

/// The changed keys' transport form: `upserts` a list of keys, or a dict of
/// key to version word (None: no version), and the removed keys. A key named
/// twice is a `ValueError`.
#[pyfunction]
#[pyo3(signature = (upserts, removes=Vec::new()))]
fn encode_changes<'py>(
    py: Python<'py>,
    upserts: &Bound<'py, PyAny>,
    removes: Vec<String>,
) -> PyResult<Bound<'py, PyBytes>> {
    let mut owned: Vec<(String, Option<String>)> = Vec::new();
    if let Ok(d) = upserts.cast::<PyDict>() {
        for (k, v) in d.iter() {
            owned.push((k.extract()?, v.extract()?));
        }
    } else {
        for k in upserts.try_iter()? {
            owned.push((k?.extract()?, None));
        }
    }
    let mut items: Vec<(&[u8], Change<'_>)> = owned
        .iter()
        .map(|(k, v)| {
            (
                k.as_bytes(),
                Change::Upsert(v.as_deref().map(str::as_bytes)),
            )
        })
        .collect();
    items.extend(removes.iter().map(|k| (k.as_bytes(), Change::Removed)));
    let bytes = py
        .detach(|| Changes::of(&items).map(|c| c.encode()))
        .map_err(err)?;
    Ok(PyBytes::new(py, &bytes))
}

pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(encode_changes, m)?)
}
