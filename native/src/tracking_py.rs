//! Python bindings of the tracking core (`native/tracking`, the
//! `solera-tracking` crate): `Tracking`, the object that holds the core's
//! state. Events and snapshots cross as bytes the engine journals without
//! reading them.

use std::sync::RwLock;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use solera_tracking as tr;

use crate::FormatError;

fn err(e: tr::Error) -> PyErr {
    match e {
        tr::Error::Format(m) => FormatError::new_err(m),
        tr::Error::Invalid(m) => PyValueError::new_err(m),
        tr::Error::Index(e) => PyValueError::new_err(format!("{e:?}")),
    }
}

#[pyclass(frozen, name = "Tracking", module = "solera._native")]
struct Tracking {
    state: RwLock<tr::Tracking>,
}

#[pymethods]
impl Tracking {
    #[new]
    fn new() -> Tracking {
        Tracking {
            state: RwLock::new(tr::Tracking::new()),
        }
    }

    #[staticmethod]
    fn restore(py: Python<'_>, snapshot: &[u8]) -> PyResult<Tracking> {
        let state = py.detach(|| tr::Tracking::restore(snapshot)).map_err(err)?;
        Ok(Tracking {
            state: RwLock::new(state),
        })
    }

    fn snapshot<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        let bytes = py.detach(|| self.state.read().unwrap().snapshot());
        PyBytes::new(py, &bytes)
    }

    /// Applies one journaled event, whole or not at all.
    fn apply(&self, event: &[u8]) -> PyResult<()> {
        self.state.write().unwrap().apply(event).map_err(err)
    }

    /// B4: builds and writes the next commit of `output` from its changed
    /// keys (`encode_changes`'s form); None if nothing changed. See
    /// `commit_py::build_commit`.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (index, output, changes, *, version, path, time_ms=0, producer=Vec::new(), mode="patch", at=None, collect=0))]
    fn build_commit<'py>(
        &self,
        py: Python<'py>,
        index: &Bound<'py, crate::index_py::ChangeIndex>,
        output: &[u8],
        changes: &[u8],
        version: u64,
        path: &str,
        time_ms: u64,
        producer: Vec<u8>,
        mode: &str,
        at: Option<u64>,
        collect: usize,
    ) -> PyResult<Option<Bound<'py, pyo3::types::PyDict>>> {
        let out = self
            .state
            .read()
            .unwrap()
            .outputs
            .get(output)
            .cloned()
            .unwrap_or_default();
        crate::commit_py::build_commit(
            py, out, index, output, changes, version, path, time_ms, &producer, mode, at, collect,
        )
    }
}

pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Tracking>()
}
