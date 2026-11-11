//! A keyed batch's keys as columns, for the worker (docs/tracking-design.md;
//! SOLE-24): the plan's keys, classes and versions decoded once
//! (`PlannedKeys`), what a source's loader served read once (`Served`), and
//! the two compared in one pass (`PlannedKeys.observe`). Python reads a
//! column whole, when it asks for it, never a key at a time.

use pyo3::exceptions::{PyKeyError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyString, PyTuple, PyType};
use solera_tracking as tr;
use std::cmp::Ordering;
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
                if u32::try_from(cands.len()).is_err() {
                    return Err(PyValueError::new_err(format!(
                        "a batch of {} keys: at most {} on one input",
                        cands.len(),
                        u32::MAX
                    )));
                }
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

    /// The keys the batch loads, in the plan's order, each with the version
    /// it is planned at (None: none).
    fn versions<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let p = &*self.inner;
        let d = PyDict::new(py);
        for i in (0..p.classes.len()).filter(|&i| p.classes[i] != REMOVED) {
            d.set_item(
                key(py, p.keys.get(i)),
                version(py, p.versions[i].as_version())?,
            )?;
        }
        Ok(d)
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

/// A key's bytes as a loader names it: a `str` (lone surrogates escaped,
/// as `key_bytes` encodes them), `bytes`, else its text.
fn key_bytes(k: &Bound<'_, PyAny>, out: &mut Vec<u8>) -> PyResult<()> {
    if let Ok(s) = k.cast::<PyString>() {
        match s.to_cow() {
            Ok(t) => out.extend_from_slice(t.as_bytes()),
            Err(_) => out.extend_from_slice(
                s.call_method1("encode", ("utf-8", "surrogateescape"))?
                    .cast::<PyBytes>()?
                    .as_bytes(),
            ),
        }
    } else if let Ok(b) = k.cast::<PyBytes>() {
        out.extend_from_slice(b.as_bytes());
    } else {
        out.extend_from_slice(k.str()?.to_cow()?.as_bytes());
    }
    Ok(())
}

/// What a source's loader served of the keys asked of it, across every load
/// of one reader: each key with the version it was served at, or absent.
#[pyclass(module = "solera._native")]
#[derive(Default)]
pub(crate) struct Served {
    /// Sorted, each key once.
    keys: Arena,
    versions: Vec<Option<Box<str>>>,
}

impl Served {
    /// The version `k` was served at; None when it was served absent or
    /// not asked for.
    fn lookup(&self, k: &[u8]) -> Option<&str> {
        let (mut lo, mut hi) = (0, self.keys.len());
        while lo < hi {
            let mid = (lo + hi) / 2;
            match self.keys.get(mid).cmp(k) {
                Ordering::Less => lo = mid + 1,
                Ordering::Greater => hi = mid,
                Ordering::Equal => return self.versions[mid].as_deref(),
            }
        }
        None
    }

    /// Adds one load's servings: sorted first, a key served twice keeping
    /// its later version, then merged in, a key served before taking the
    /// new version. A load whose keys all follow the last served (pages in
    /// key order) is appended: linear in the load, not in what came before.
    fn absorb(&mut self, keys: Arena, mut versions: Vec<Option<Box<str>>>) {
        let mut order: Vec<usize> = (0..keys.len()).collect();
        order.sort_by(|&a, &b| keys.get(a).cmp(keys.get(b)));
        order.dedup_by(|later, earlier| {
            let same = keys.get(*later) == keys.get(*earlier);
            if same {
                *earlier = *later;
            }
            same
        });
        let follows = match (order.first(), self.keys.len()) {
            (None, _) => return,
            (_, 0) => true,
            (Some(&f), n) => keys.get(f) > self.keys.get(n - 1),
        };
        if follows {
            for i in order {
                self.keys.push(keys.get(i));
                self.versions.push(versions[i].take());
            }
            return;
        }
        let (old_keys, mut old_versions) = (
            std::mem::take(&mut self.keys),
            std::mem::take(&mut self.versions),
        );
        let (mut a, mut b) = (0, 0);
        while a < old_keys.len() || b < order.len() {
            let take_new = match (a < old_keys.len(), b < order.len()) {
                (true, true) => match keys.get(order[b]).cmp(old_keys.get(a)) {
                    Ordering::Less => true,
                    Ordering::Equal => {
                        a += 1;
                        true
                    }
                    Ordering::Greater => false,
                },
                (false, _) => true,
                (true, false) => false,
            };
            if take_new {
                self.keys.push(keys.get(order[b]));
                self.versions.push(versions[order[b]].take());
                b += 1;
            } else {
                self.keys.push(old_keys.get(a));
                self.versions.push(old_versions[a].take());
                a += 1;
            }
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
    /// rows; or with `grouped` a dict of each key's) and, when a key's
    /// answer is not a `loaded` with a version, `(key,)` of the first such
    /// key, which is not added (None when every answer is).
    #[pyo3(signature = (answer, keys, loaded, grouped=false))]
    fn read<'py>(
        &mut self,
        py: Python<'py>,
        answer: &Bound<'py, PyAny>,
        keys: Option<Bound<'py, PyList>>,
        loaded: &Bound<'py, PyType>,
        grouped: bool,
    ) -> PyResult<(Bound<'py, PyAny>, Option<Bound<'py, PyTuple>>)> {
        guard(|| {
            let dict = answer.cast_exact::<PyDict>().ok();
            let names: Vec<Bound<'py, PyAny>> = match &keys {
                Some(k) => k.iter().collect(),
                None => answer.try_iter()?.collect::<PyResult<_>>()?,
            };
            let version_attr = PyString::intern(py, "version");
            let value_attr = PyString::intern(py, "value");
            let rows = PyList::empty(py);
            let groups = PyDict::new(py);
            let mut served = Arena::default();
            served.ends.reserve(names.len());
            let mut versions = Vec::with_capacity(names.len());
            let mut name_bytes = Vec::new();
            let mut bad = None;
            for name in &names {
                let item = match dict {
                    Some(d) => d.get_item(name)?,
                    None => Some(answer.call_method1("get", (name,))?),
                };
                name_bytes.clear();
                key_bytes(name, &mut name_bytes)?;
                let Some(item) = item.filter(|v| !v.is_none()) else {
                    served.push(&name_bytes); // observed absent
                    versions.push(None);
                    continue;
                };
                let v = match item.is_instance(loaded)? {
                    true => item.getattr(&version_attr)?,
                    false => py.None().into_bound(py),
                };
                if v.is_none() {
                    bad = Some(PyTuple::new(py, [name])?);
                    break;
                }
                let version: Box<str> = v.str()?.to_cow()?.into();
                let value = item.getattr(&value_attr)?;
                served.push(&name_bytes);
                versions.push(Some(version));
                if grouped {
                    let group = match value.cast::<PyList>() {
                        Ok(l) => l.clone(),
                        Err(_) => PyList::new(py, [value])?,
                    };
                    groups.set_item(name, group)?;
                } else if let Ok(l) = value.cast_exact::<PyList>() {
                    for r in l.iter() {
                        rows.append(r)?;
                    }
                } else if value.is_instance_of::<PyList>() {
                    for r in value.try_iter()? {
                        rows.append(r?)?;
                    }
                } else {
                    rows.append(value)?;
                }
            }
            self.absorb(served, versions);
            let out = if grouped {
                groups.into_any()
            } else {
                rows.into_any()
            };
            Ok((out, bad))
        })
    }

    /// The version `k` was served at; `default` when it was served absent or
    /// not asked for (the per-key path reads it as a dict).
    #[pyo3(signature = (k, default=None))]
    fn get<'py>(
        &self,
        py: Python<'py>,
        k: &Bound<'py, PyAny>,
        default: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        guard(|| {
            let mut b = Vec::new();
            key_bytes(k, &mut b)?;
            Ok(match self.lookup(&b) {
                Some(v) => PyString::new(py, v).into_any(),
                None => default.unwrap_or_else(|| py.None().into_bound(py)),
            })
        })
    }

    /// The version `k` was served at: a `KeyError` when it was served absent
    /// or not asked for.
    fn __getitem__<'py>(
        &self,
        py: Python<'py>,
        k: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let got = self.get(py, k, None)?;
        if got.is_none() {
            return Err(PyKeyError::new_err(k.clone().unbind()));
        }
        Ok(got)
    }

    /// Whether `k` was served at a version.
    fn __contains__(&self, py: Python<'_>, k: &Bound<'_, PyAny>) -> PyResult<bool> {
        Ok(!self.get(py, k, None)?.is_none())
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

    /// Every key asked of the loader, in key order, with the version it was
    /// served at (None: absent).
    fn items<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let items = (0..self.keys.len())
            .map(|i| {
                let v = match &self.versions[i] {
                    Some(v) => PyString::new(py, v).into_any(),
                    None => py.None().into_bound(py),
                };
                (key(py, self.keys.get(i)), v)
            })
            .collect::<Vec<_>>();
        PyList::new(py, items)
    }
}

pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PlannedKeys>()?;
    m.add_class::<Classes>()?;
    m.add_class::<Served>()?;
    Ok(())
}
