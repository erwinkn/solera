//! A keyed batch's keys as columns, for the worker (docs/tracking-design.md;
//! SOLE-24): the plan's keys, classes and versions decoded once
//! (`PlannedKeys`), what a source's loader served read once (`Served`), and
//! the two compared in one pass (`PlannedKeys.observe`). Python reads a
//! column whole, when it asks for it, never a key at a time.

use pyo3::exceptions::{PyKeyError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString, PyTuple, PyType};
use solera_tracking as tr;
use std::borrow::Cow;
use std::sync::Arc;
use tr::codec::Ver;
use tr::plan::Plan;
use tr::progress::Class;

use crate::guard;
use crate::index_py::{key, version};
use crate::rows::Arena;

/// Not delivered: a key planned added that its source served absent.
const NONE: u8 = 0;
const CLASSES: [Class; 4] = [
    Class::Added,
    Class::Updated,
    Class::Removed,
    Class::Unchanged,
];
const REMOVED: u8 = Class::Removed as u8;

struct Planned {
    keys: Arena,
    classes: Vec<u8>,
    versions: Vec<Ver>,
    /// The positions of the keys a batch loads (all but the removed), in
    /// key order.
    loads: Vec<u32>,
}

impl Planned {
    /// The position of a key the batch loads.
    fn find(&self, k: &[u8]) -> Option<usize> {
        self.loads
            .binary_search_by(|&p| self.keys.get(p as usize).cmp(k))
            .ok()
            .map(|i| self.loads[i] as usize)
    }
}

/// One keyed input's keys in a batch, with their classes and the versions
/// they are planned at, from a plan's bytes.
#[pyclass(module = "solera._native", frozen)]
pub(crate) struct PlannedKeys {
    inner: Arc<Planned>,
}

#[pymethods]
impl PlannedKeys {
    #[staticmethod]
    fn decode(py: Python<'_>, plan: &[u8], input: usize) -> PyResult<PlannedKeys> {
        guard(|| {
            let planned = py.detach(|| -> PyResult<Planned> {
                let mut plan = Plan::decode(plan).map_err(crate::tracking_py::err)?;
                let n = plan.inputs.len();
                if input >= n {
                    return Err(PyValueError::new_err(format!(
                        "the plan has {n} keyed inputs"
                    )));
                }
                let cands = std::mem::take(&mut plan.inputs[input].keys);
                let mut keys = Arena::default();
                keys.ends.reserve(cands.len());
                let mut classes = Vec::with_capacity(cands.len());
                let mut versions = Vec::with_capacity(cands.len());
                for c in cands {
                    keys.push(&c.key);
                    classes.push(c.class as u8);
                    versions.push(c.version);
                }
                let mut loads: Vec<u32> = (0..classes.len() as u32)
                    .filter(|&p| classes[p as usize] != REMOVED)
                    .collect();
                let sorted = loads
                    .windows(2)
                    .all(|w| keys.get(w[0] as usize) < keys.get(w[1] as usize));
                if !sorted {
                    loads.sort_by(|&a, &b| keys.get(a as usize).cmp(keys.get(b as usize)));
                }
                Ok(Planned {
                    keys,
                    classes,
                    versions,
                    loads,
                })
            })?;
            Ok(PlannedKeys {
                inner: Arc::new(planned),
            })
        })
    }

    /// No keys: an input whose walk is done.
    #[new]
    fn new() -> PlannedKeys {
        PlannedKeys {
            inner: Arc::new(Planned {
                keys: Arena::default(),
                classes: vec![],
                versions: vec![],
                loads: vec![],
            }),
        }
    }

    /// Every key of the batch, removed ones too.
    fn __len__(&self) -> usize {
        self.inner.classes.len()
    }

    /// How many keys the batch loads: all but the removed.
    fn loads(&self) -> usize {
        self.inner.loads.len()
    }

    /// The keys the batch loads, in the plan's order, or sorted.
    #[pyo3(signature = (sorted=false))]
    fn load_keys<'py>(&self, py: Python<'py>, sorted: bool) -> PyResult<Bound<'py, PyList>> {
        let p = &*self.inner;
        if sorted {
            return PyList::new(py, p.loads.iter().map(|&i| key(py, p.keys.get(i as usize))));
        }
        PyList::new(
            py,
            (0..p.classes.len())
                .filter(|&i| p.classes[i] != REMOVED)
                .map(|i| key(py, p.keys.get(i))),
        )
    }

    /// The version a loaded key is planned at (None: none); a `KeyError`
    /// for a key the batch does not load.
    fn version<'py>(&self, py: Python<'py>, k: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        match text(k).and_then(|t| self.inner.find(t.as_bytes())) {
            Some(i) => version(py, self.inner.versions[i].as_version()),
            None => Err(PyKeyError::new_err(k.clone().unbind())),
        }
    }

    /// The batch's classes, from what a source served (None: the input is
    /// not a source's, and its classes are the plan's), and the keys it
    /// served otherwise than planned, `[key, version]` each (None: absent).
    /// A key served absent is removed if the consumer held it, else not
    /// delivered; one served at another word than planned is updated if it
    /// was unchanged. A key planned at a generation has no word to compare.
    #[pyo3(signature = (served=None))]
    fn observe<'py>(
        &self,
        py: Python<'py>,
        served: Option<PyRef<'py, Served>>,
    ) -> PyResult<(Classes, Bound<'py, PyList>)> {
        guard(|| {
            let p = &*self.inner;
            let mut classes = p.classes.clone();
            let loaded = PyList::empty(py);
            if let Some(s) = served {
                for (i, cls) in classes.iter_mut().enumerate() {
                    if *cls == REMOVED {
                        continue;
                    }
                    let k = p.keys.get(i);
                    let Some(got) = s.lookup(k) else {
                        loaded.append(PyList::new(py, [key(py, k), py.None().into_bound(py)])?)?;
                        *cls = if *cls == Class::Added as u8 {
                            NONE
                        } else {
                            REMOVED
                        };
                        continue;
                    };
                    let Ver::Bytes(planned) = &p.versions[i] else {
                        continue;
                    };
                    if std::str::from_utf8(planned).is_err() || **planned == *got.as_bytes() {
                        continue;
                    }
                    loaded.append(PyList::new(
                        py,
                        [key(py, k), PyString::new(py, got).into_any()],
                    )?)?;
                    if *cls == Class::Unchanged as u8 {
                        *cls = Class::Updated as u8;
                    }
                }
            }
            let classes = Classes {
                planned: self.inner.clone(),
                classes,
            };
            Ok((classes, loaded))
        })
    }
}

/// A batch's keys by class, after `observe`.
#[pyclass(module = "solera._native", frozen)]
pub(crate) struct Classes {
    planned: Arc<Planned>,
    classes: Vec<u8>,
}

#[pymethods]
impl Classes {
    /// The keys of a class (`added`, `updated`, `removed`, `unchanged`), in
    /// the plan's order.
    fn keys<'py>(&self, py: Python<'py>, class: &str) -> PyResult<Bound<'py, PyTuple>> {
        let Some(c) = CLASSES.iter().find(|c| c.name() == class) else {
            return Err(PyValueError::new_err(format!("no class {class:?}")));
        };
        let c = *c as u8;
        let keys: Vec<_> = (0..self.classes.len())
            .filter(|&i| self.classes[i] == c)
            .map(|i| key(py, self.planned.keys.get(i)))
            .collect();
        PyTuple::new(py, keys)
    }

    /// Whether any key is delivered.
    fn any(&self) -> bool {
        self.classes.iter().any(|&c| c != NONE)
    }
}

fn text<'a>(k: &'a Bound<'_, PyAny>) -> Option<Cow<'a, str>> {
    k.cast::<PyString>().ok().and_then(|s| s.to_cow().ok())
}

/// What a source's loader served of the keys asked of it, across every load
/// of one reader: each key with the version it was served at, or absent.
/// Read by Python as a mapping, `key -> version` (None: served absent).
#[pyclass(module = "solera._native")]
#[derive(Default)]
pub(crate) struct Served {
    /// Sorted, each key once.
    keys: Arena,
    versions: Vec<Option<Box<str>>>,
}

impl Served {
    fn position(&self, k: &[u8]) -> Option<usize> {
        let (mut lo, mut hi) = (0, self.keys.len());
        while lo < hi {
            let mid = (lo + hi) / 2;
            match self.keys.get(mid).cmp(k) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return Some(mid),
            }
        }
        None
    }

    /// The version `k` was served at; None when it was served absent or
    /// not asked for.
    fn lookup(&self, k: &[u8]) -> Option<&str> {
        self.position(k).and_then(|i| self.versions[i].as_deref())
    }

    /// Sorted, each key once: a later serving of a key replaces an earlier.
    fn normalize(&mut self) {
        let n = self.keys.len();
        if (1..n).all(|i| self.keys.get(i - 1) < self.keys.get(i)) {
            return;
        }
        let mut order: Vec<usize> = (0..n).collect();
        order.sort_by(|&a, &b| self.keys.get(a).cmp(self.keys.get(b)));
        let mut keys = Arena::default();
        let mut versions: Vec<Option<Box<str>>> = Vec::with_capacity(n);
        for (j, &i) in order.iter().enumerate() {
            let last = order
                .get(j + 1)
                .is_none_or(|&next| self.keys.get(next) != self.keys.get(i));
            if last {
                keys.push(self.keys.get(i));
                versions.push(self.versions[i].take());
            }
        }
        self.keys = keys;
        self.versions = versions;
    }

    fn version_at<'py>(&self, py: Python<'py>, i: usize) -> Bound<'py, PyAny> {
        match &self.versions[i] {
            Some(v) => PyString::new(py, v).into_any(),
            None => py.None().into_bound(py),
        }
    }
}

#[pymethods]
impl Served {
    #[new]
    fn new() -> Served {
        Served::default()
    }

    /// A keyed loader's answer, `{key: Loaded(row, version=…)}`, read once
    /// and added to what this reader served: of each of `keys` (None: every
    /// key it answered), whether it was served and at which version (a key
    /// served again takes its later version). Returns its rows (every key's,
    /// flat, in the order of `keys`, a value that is a list being that key's
    /// rows; or with `grouped` a dict of each key's) and the first key whose
    /// answer is not a `loaded` with a version (None when every one is).
    #[pyo3(signature = (answer, keys, loaded, grouped=false))]
    fn read<'py>(
        &mut self,
        py: Python<'py>,
        answer: &Bound<'py, PyAny>,
        keys: Option<Bound<'py, PyList>>,
        loaded: &Bound<'py, PyType>,
        grouped: bool,
    ) -> PyResult<(Bound<'py, PyAny>, Option<Bound<'py, PyAny>>)> {
        guard(|| {
            let dict = answer.cast::<PyDict>().ok();
            let names: Vec<Bound<'py, PyAny>> = match &keys {
                Some(k) => k.iter().collect(),
                None => answer.try_iter()?.collect::<PyResult<_>>()?,
            };
            let version_attr = PyString::intern(py, "version");
            let value_attr = PyString::intern(py, "value");
            let rows = PyList::empty(py);
            let groups = PyDict::new(py);
            let served = self;
            served.keys.ends.reserve(names.len());
            served.versions.reserve(names.len());
            let mut bad = None;
            for name in &names {
                let item = match dict {
                    Some(d) => d.get_item(name)?,
                    None => Some(answer.call_method1("get", (name,))?),
                };
                served.keys.push(name.str()?.to_cow()?.as_bytes());
                let Some(item) = item.filter(|v| !v.is_none()) else {
                    served.versions.push(None); // observed absent
                    continue;
                };
                let v = match item.is_instance(loaded)? {
                    true => item.getattr(&version_attr)?,
                    false => py.None().into_bound(py),
                };
                if v.is_none() {
                    served.versions.push(None);
                    bad = Some(name.clone());
                    break;
                }
                served.versions.push(Some(v.str()?.to_cow()?.into()));
                let value = item.getattr(&value_attr)?;
                if grouped {
                    let group = match value.cast::<PyList>() {
                        Ok(l) => l.clone(),
                        Err(_) => PyList::new(py, [value])?,
                    };
                    groups.set_item(name, group)?;
                } else if let Ok(l) = value.cast::<PyList>() {
                    for r in l.iter() {
                        rows.append(r)?;
                    }
                } else {
                    rows.append(value)?;
                }
            }
            served.normalize();
            let out = if grouped {
                groups.into_any()
            } else {
                rows.into_any()
            };
            Ok((out, bad))
        })
    }

    /// Every key served at a version, with that version.
    fn versions<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let d = PyDict::new(py);
        for (i, v) in self.versions.iter().enumerate() {
            if let Some(v) = v {
                d.set_item(key(py, self.keys.get(i)), PyString::new(py, v))?;
            }
        }
        Ok(d)
    }

    fn __len__(&self) -> usize {
        self.keys.len()
    }

    fn __contains__(&self, k: &Bound<'_, PyAny>) -> bool {
        text(k).is_some_and(|t| self.position(t.as_bytes()).is_some())
    }

    fn __getitem__<'py>(
        &self,
        py: Python<'py>,
        k: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        match text(k).and_then(|t| self.position(t.as_bytes())) {
            Some(i) => Ok(self.version_at(py, i)),
            None => Err(PyKeyError::new_err(k.clone().unbind())),
        }
    }

    #[pyo3(signature = (k, default=None))]
    fn get<'py>(
        &self,
        py: Python<'py>,
        k: &Bound<'py, PyAny>,
        default: Option<Bound<'py, PyAny>>,
    ) -> Bound<'py, PyAny> {
        match text(k).and_then(|t| self.position(t.as_bytes())) {
            Some(i) => self.version_at(py, i),
            None => default.unwrap_or_else(|| py.None().into_bound(py)),
        }
    }

    fn __iter__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        Ok(self.keys(py)?.into_any().try_iter()?.into_any())
    }

    fn keys<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        PyList::new(py, (0..self.keys.len()).map(|i| key(py, self.keys.get(i))))
    }

    fn items<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        PyList::new(
            py,
            (0..self.keys.len()).map(|i| (key(py, self.keys.get(i)), self.version_at(py, i))),
        )
    }
}

pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PlannedKeys>()?;
    m.add_class::<Classes>()?;
    m.add_class::<Served>()?;
    Ok(())
}
