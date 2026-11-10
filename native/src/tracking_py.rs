//! Python bindings of the tracking core (`native/tracking`, the
//! `solera-tracking` crate): `Tracking`, the object that holds the core's state.
//!
//! Events, snapshots, plans and records cross as bytes the engine stores
//! without reading them. A plan or an owed question is one call that
//! releases the GIL and runs on the index's runtime; `apply` is short and
//! synchronous. Keys are `str`; versions `int`, `str` or None, as in the
//! change index's bindings.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use pyo3::exceptions::{PyKeyError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyString, PyTuple};
use solera_index as ix;
use solera_tracking as tr;
use tr::codec::Ver;
use tr::event::{Event, Part};
use tr::plan::{InputRequest, Mode, Plan, Request};
use tr::progress::{Class, Conditions, Holding, Key, Matcher, Pat, Patterns, Point, Reason};

use crate::index_py::{key, layers_in, rt, version, ChangeIndex, Owned};
use crate::{guard, FormatError};

pub(crate) fn err(e: tr::Error) -> PyErr {
    match e {
        tr::Error::Format(m) => FormatError::new_err(m),
        tr::Error::Invalid(m) => PyValueError::new_err(m),
        tr::Error::Index(e) => crate::index_py::err(e),
    }
}

fn owned(v: &Bound<'_, PyAny>) -> PyResult<Ver> {
    Ok(match Owned::of(v)? {
        Owned::Absent => Ver::Absent,
        Owned::Int(n) => Ver::Int(n),
        Owned::Str(s) => Ver::Bytes(s.into_bytes().into()),
    })
}

pub(crate) fn key_in(k: &Bound<'_, PyAny>) -> PyResult<Key> {
    Ok(k.extract::<String>()?.into_bytes().into())
}

fn opt_key(k: Option<String>) -> Option<Key> {
    k.map(|s| s.into_bytes().into())
}

fn item<'py>(d: &Bound<'py, PyDict>, name: &str) -> PyResult<Option<Bound<'py, PyAny>>> {
    Ok(d.get_item(name)?.filter(|v| !v.is_none()))
}

fn need<'py>(d: &Bound<'py, PyDict>, name: &str, what: &str) -> PyResult<Bound<'py, PyAny>> {
    item(d, name)?.ok_or_else(|| PyKeyError::new_err(format!("{what} needs {name:?}")))
}

fn pat(p: &Bound<'_, PyAny>) -> PyResult<Pat> {
    let d = p.cast::<PyDict>()?;
    if let Some(g) = item(d, "glob")? {
        return Ok(Pat::Glob(g.extract()?));
    }
    if let Some(r) = item(d, "regex")? {
        return Ok(Pat::Regex(r.extract()?));
    }
    Err(PyValueError::new_err(
        "a pattern is {\"glob\": …} or {\"regex\": …}",
    ))
}

/// Whether an input takes a key, from its manifest spec
/// (`solera.patterns.spec(…)`; `None`: every key), with the matcher the
/// planner uses.
#[pyclass(frozen, module = "solera._native")]
struct KeyMatcher {
    include: Vec<Matcher>,
    exclude: Vec<(String, Matcher)>,
}

#[pymethods]
impl KeyMatcher {
    #[new]
    #[pyo3(signature = (patterns=None))]
    fn new(patterns: Option<&Bound<'_, PyAny>>) -> PyResult<KeyMatcher> {
        let mut m = KeyMatcher {
            include: vec![],
            exclude: vec![],
        };
        let Some(p) = patterns.filter(|p| !p.is_none()) else {
            return Ok(m);
        };
        let p = p.cast::<PyDict>()?;
        let one = |x: &Bound<'_, PyAny>| Matcher::new(&pat(x)?).map_err(err);
        if let Some(inc) = item(p, "include")?.filter(|v| !v.is_none()) {
            for x in inc.try_iter()? {
                m.include.push(one(&x?)?);
            }
        }
        if let Some(exc) = item(p, "exclude")?.filter(|v| !v.is_none()) {
            for x in exc.try_iter()? {
                let x = x?;
                m.exclude
                    .push((x.get_item(0)?.extract()?, one(&x.get_item(1)?)?));
            }
        }
        Ok(m)
    }

    fn included(&self, key: &str) -> bool {
        self.include.is_empty() || self.include.iter().any(|m| m.matches(key.as_bytes()))
    }

    /// The name of the first `exclude` pattern that matches the key.
    fn excluded_by(&self, key: &str) -> Option<String> {
        self.exclude
            .iter()
            .find(|(_, m)| m.matches(key.as_bytes()))
            .map(|(n, _)| n.clone())
    }

    fn __call__(&self, key: &str) -> bool {
        self.takes(key.as_bytes())
    }
}

impl KeyMatcher {
    fn takes(&self, key: &[u8]) -> bool {
        (self.include.is_empty() || self.include.iter().any(|m| m.matches(key)))
            && !self.exclude.iter().any(|(_, m)| m.matches(key))
    }
}

/// Keys a `Tracking.taken` scan reads in one page.
const TAKEN_PAGE: u64 = 64 << 10;

/// Refuses a pattern outside Solera's regex dialect, with what it lacks.
#[pyfunction]
fn regex_check(pattern: String) -> PyResult<()> {
    Matcher::new(&Pat::Regex(pattern)).map_err(err)?;
    Ok(())
}

/// Conditions from `{"patterns": solera.patterns.spec(…), "context": bytes, "definition": bytes, "rebuild": int}`.
pub(crate) fn conditions(d: &Bound<'_, PyAny>) -> PyResult<Arc<Conditions>> {
    let d = d.cast::<PyDict>()?;
    let mut include = vec![];
    let mut exclude = vec![];
    if let Some(p) = item(d, "patterns")? {
        let p = p.cast::<PyDict>()?;
        if let Some(inc) = item(p, "include")? {
            for x in inc.try_iter()? {
                include.push(pat(&x?)?);
            }
        }
        if let Some(exc) = item(p, "exclude")? {
            for x in exc.try_iter()? {
                let x = x?;
                let named = x.cast::<PyAny>()?;
                let p = if named.is_instance_of::<PyDict>() {
                    named.clone()
                } else {
                    named.get_item(1)?
                };
                exclude.push(pat(&p)?);
            }
        }
    }
    let bytes = |name: &str| -> PyResult<Box<[u8]>> {
        Ok(item(d, name)?
            .map(|v| v.extract::<Vec<u8>>())
            .transpose()?
            .unwrap_or_default()
            .into())
    };
    Ok(Arc::new(Conditions {
        patterns: Patterns::new(include, exclude).map_err(err)?,
        context: bytes("context")?,
        definition: bytes("definition")?,
        rebuild: item(d, "rebuild")?
            .map(|v| v.extract())
            .transpose()?
            .unwrap_or(0),
    }))
}

fn input(d: &Bound<'_, PyAny>) -> PyResult<InputRequest> {
    let d = d.cast::<PyDict>()?;
    Ok(InputRequest {
        edge: need(d, "edge", "an input")?.extract::<Vec<u8>>()?.into(),
        conditions: conditions(&need(d, "conditions", "an input")?)?,
        own: item(d, "own")?
            .map(|v| v.extract::<Vec<u8>>())
            .transpose()?
            .map(Into::into),
    })
}

fn request(d: &Bound<'_, PyDict>) -> PyResult<Request> {
    let mode: String = item(d, "mode")?
        .map(|v| v.extract())
        .transpose()?
        .unwrap_or_else(|| "incremental".into());
    let mode = match mode.as_str() {
        "incremental" => Mode::Incremental,
        "all" => Mode::All,
        "keys" => {
            let mut keys: Vec<Key> = vec![];
            for k in need(d, "keys", "a key list")?.try_iter()? {
                keys.push(key_in(&k?)?);
            }
            Mode::Keys(keys)
        }
        m => {
            return Err(PyValueError::new_err(format!(
                "mode {m:?}: incremental, all or keys"
            )))
        }
    };
    let mut inputs = vec![];
    if let Some(ins) = item(d, "inputs")? {
        for i in ins.try_iter()? {
            inputs.push(input(&i?)?);
        }
    }
    let mut unkeyed = vec![];
    if let Some(us) = item(d, "unkeyed")? {
        for u in us.try_iter()? {
            unkeyed.push(u?.extract::<Vec<u8>>()?.into());
        }
    }
    Ok(Request {
        mode,
        reset: item(d, "reset")?
            .map(|v| v.extract())
            .transpose()?
            .unwrap_or(false),
        after: item(d, "after")?.map(|k| key_in(&k)).transpose()?,
        limit: need(d, "limit", "a plan")?.extract()?,
        inputs,
        unkeyed,
    })
}

fn class_name(c: Class) -> &'static str {
    c.name()
}

fn counts<'py>(py: Python<'py>, n: [u64; 4]) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    for (c, v) in [
        Class::Added,
        Class::Updated,
        Class::Removed,
        Class::Unchanged,
    ]
    .into_iter()
    .zip(n)
    {
        d.set_item(class_name(c), v)?;
    }
    Ok(d)
}

/// Owed keys that are not due, per input: each input's `not_due` list in a
/// plan request. B6's due index replaces it.
struct NotDue(Vec<HashSet<Key>>);

impl NotDue {
    fn of(d: &Bound<'_, PyDict>) -> PyResult<NotDue> {
        let mut out = vec![];
        if let Some(ins) = item(d, "inputs")? {
            for i in ins.try_iter()? {
                let mut keys = HashSet::new();
                if let Some(ks) = item(i?.cast::<PyDict>()?, "not_due")? {
                    for k in ks.try_iter()? {
                        keys.insert(key_in(&k?)?);
                    }
                }
                out.push(keys);
            }
        }
        Ok(NotDue(out))
    }
}

impl tr::plan::Hold for NotDue {
    fn held_back(&self, input: usize, key: &[u8]) -> bool {
        self.0.get(input).is_some_and(|s| s.contains(key))
    }
}

fn one_event(part: Part) -> Vec<u8> {
    Event::of(vec![part]).encode()
}

/// The change indexes outputs are attached to.
struct Attached(HashMap<Key, ix::ChangeIndex>);

impl tr::plan::Indexes for Attached {
    fn index(&self, output: &[u8]) -> tr::Result<&ix::ChangeIndex> {
        match self.0.get(output) {
            Some(i) => Ok(i),
            None => Err(tr::Error::Invalid(format!(
                "no change index attached for output {}",
                String::from_utf8_lossy(output)
            ))),
        }
    }
}

/// The tracking core's state: what every consumer holds of its inputs, and
/// every output's layers. Changed only by `apply(event)`.
#[pyclass(frozen, name = "Tracking", module = "solera._native")]
struct Tracking {
    state: RwLock<tr::Tracking>,
    indexes: RwLock<Arc<Attached>>,
    /// B7: owed counts, kept until the edge or its upstream moves.
    counts: tr::stale::Counts,
}

impl Tracking {
    fn of(state: tr::Tracking) -> Tracking {
        Tracking {
            state: RwLock::new(state),
            indexes: RwLock::new(Arc::new(Attached(HashMap::new()))),
            counts: Default::default(),
        }
    }

    fn view(&self, req: &Request) -> PyResult<(tr::plan::View, Arc<Attached>)> {
        let view = self.state.read().unwrap().view(req).map_err(err)?;
        Ok((view, self.indexes.read().unwrap().clone()))
    }
}

#[pymethods]
impl Tracking {
    #[new]
    fn new() -> Tracking {
        Tracking::of(tr::Tracking::new())
    }

    #[staticmethod]
    fn restore(py: Python<'_>, snapshot: &[u8]) -> PyResult<Tracking> {
        guard(|| {
            Ok(Tracking::of(
                py.detach(|| tr::Tracking::restore(snapshot)).map_err(err)?,
            ))
        })
    }

    fn snapshot<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        guard(|| {
            let bytes = py.detach(|| self.state.read().unwrap().snapshot());
            Ok(PyBytes::new(py, &bytes))
        })
    }

    /// Applies one journaled event, whole or not at all.
    fn apply(&self, event: &[u8]) -> PyResult<()> {
        guard(|| self.state.write().unwrap().apply(event).map_err(err))
    }

    /// Whether `apply` would take the event, applying nothing.
    fn check(&self, event: &[u8]) -> PyResult<()> {
        guard(|| {
            let ev = Event::decode(event).map_err(err)?;
            self.state.read().unwrap().check_event(&ev).map_err(err)
        })
    }

    /// One output as the state holds it: `{"head", "live", "layers"}`, or
    /// None before its first commit.
    fn output<'py>(&self, py: Python<'py>, output: &[u8]) -> PyResult<Option<Bound<'py, PyDict>>> {
        let Some(o) = self.state.read().unwrap().outputs.get(output).cloned() else {
            return Ok(None);
        };
        let d = PyDict::new(py);
        d.set_item("head", o.head)?;
        d.set_item("live", o.live)?;
        let layers = PyList::empty(py);
        for l in &o.layers {
            layers.append(crate::index_py::layer_out(py, l)?)?;
        }
        d.set_item("layers", layers)?;
        Ok(Some(d))
    }

    /// Whether `output` holds at its head a key after `after` that
    /// `patterns` (`solera.patterns.spec(…)`) take: a scan of its attached
    /// index, a page at a time, stopping at the first such key. Nothing
    /// crosses per key; False before its first commit. With `edge`, a key
    /// that edge processed in a point (a key list's) does not count; a point
    /// that keeps an absent holding (failed, held back) does not process it.
    #[pyo3(signature = (output, patterns=None, after=None, edge=None))]
    fn taken(
        &self,
        py: Python<'_>,
        output: &[u8],
        patterns: Option<&Bound<'_, PyAny>>,
        after: Option<String>,
        edge: Option<Vec<u8>>,
    ) -> PyResult<bool> {
        guard(|| {
            let matcher = KeyMatcher::new(patterns)?;
            let (o, progress) = {
                let state = self.state.read().unwrap();
                let progress =
                    edge.and_then(|e| state.edges.get(e.as_slice()).map(|e| e.progress.clone()));
                (state.outputs.get(output).cloned(), progress)
            };
            let Some(o) = o else {
                return Ok(false);
            };
            let points = progress.as_deref().and_then(tr::progress::Progress::keyed);
            let ixs = self.indexes.read().unwrap().clone();
            py.detach(|| {
                rt().block_on(async {
                    let ix = tr::plan::Indexes::index(ixs.as_ref(), output)?;
                    let mut q = ix::Query {
                        after: after.map(String::into_bytes),
                        limit: Some(TAKEN_PAGE),
                        ..Default::default()
                    };
                    loop {
                        let mut found = false;
                        let next = ix
                            .state_with(&o.layers, o.head, None, &q, |k, _, _, _| {
                                found = found
                                    || (matcher.takes(k)
                                        && points
                                            .is_none_or(|p| !p.point(k).is_some_and(processed)))
                            })
                            .await
                            .map_err(tr::Error::Index)?;
                        match next {
                            _ if found => return Ok(true),
                            Some(n) => q.after = Some(n),
                            None => return Ok(false),
                        }
                    }
                })
            })
            .map_err(err)
        })
    }

    /// The change index that holds `output`'s layers (runtime configuration, not
    /// state). Every output on one object store should share one index, and
    /// so its cache.
    fn attach(&self, output: Vec<u8>, index: PyRef<'_, ChangeIndex>) {
        let mut ixs = self.indexes.write().unwrap();
        let mut map = ixs
            .0
            .iter()
            .map(|(k, v)| (k.clone(), ix::ChangeIndex::new(v.store.clone(), v.write)))
            .collect::<HashMap<_, _>>();
        map.insert(
            output.into(),
            ix::ChangeIndex::new(index.ix.store.clone(), index.ix.write),
        );
        *ixs = Arc::new(Attached(map));
    }

    /// Tests only, until the output commit's event part (unit B4) lands:
    /// sets an output's layers and head.
    #[pyo3(signature = (output, layers, head, live, full_run=None))]
    fn _set_output(
        &self,
        output: Vec<u8>,
        layers: &Bound<'_, PyAny>,
        head: u64,
        live: u64,
        full_run: Option<u64>,
    ) -> PyResult<()> {
        let layers = layers_in(layers)?;
        self.state.write().unwrap().outputs.insert(
            output.into(),
            tr::Output {
                layers,
                head,
                live,
                full_run,
            },
        );
        Ok(())
    }

    /// Plans one batch. Returns the plan's bytes (they go into the attempt's
    /// spec untouched) and a summary: its range, and per input its head and
    /// its keys by class.
    fn plan<'py>(
        &self,
        py: Python<'py>,
        request: &Bound<'py, PyDict>,
    ) -> PyResult<Bound<'py, PyDict>> {
        guard(|| {
            let req = self::request(request)?;
            let not_due = NotDue::of(request)?;
            let (view, ixs) = self.view(&req)?;
            let (plan, bytes) = py
                .detach(|| {
                    let plan =
                        rt().block_on(tr::plan::plan(&view, &req, ixs.as_ref(), &not_due))?;
                    let bytes = plan.encode();
                    Ok::<_, tr::Error>((plan, bytes))
                })
                .map_err(err)?;
            let d = PyDict::new(py);
            d.set_item("plan", PyBytes::new(py, &bytes))?;
            d.set_item("lo", plan.range.lo.as_deref().map(|k| key(py, k)))?;
            d.set_item("hi", plan.range.hi.as_deref().map(|k| key(py, k)))?;
            let inputs = PyList::empty(py);
            for p in &plan.inputs {
                let i = PyDict::new(py);
                i.set_item("edge", PyBytes::new(py, &p.edge))?;
                i.set_item("head", p.head)?;
                i.set_item("keys", p.keys.len())?;
                i.set_item("held_back", p.held_back.len())?;
                i.set_item("counts", counts(py, p.counts())?)?;
                inputs.append(i)?;
            }
            d.set_item("inputs", inputs)?;
            let unkeyed = PyList::empty(py);
            for u in &plan.unkeyed {
                let i = PyDict::new(py);
                i.set_item("edge", PyBytes::new(py, &u.edge))?;
                i.set_item("head", u.head)?;
                i.set_item("through", u.through)?;
                i.set_item("owed", u.owed())?;
                unkeyed.append(i)?;
            }
            d.set_item("unkeyed", unkeyed)?;
            Ok(d)
        })
    }

    /// One keyed input's keys, classes and versions, from a plan's bytes.
    #[staticmethod]
    fn plan_keys<'py>(py: Python<'py>, plan: &[u8], input: usize) -> PyResult<Bound<'py, PyTuple>> {
        guard(|| {
            let plan = Plan::decode(plan).map_err(err)?;
            let Some(p) = plan.inputs.get(input) else {
                return Err(PyValueError::new_err(format!(
                    "the plan has {} keyed inputs",
                    plan.inputs.len()
                )));
            };
            let (keys, classes, versions) =
                (PyList::empty(py), PyList::empty(py), PyList::empty(py));
            for c in &p.keys {
                keys.append(key(py, &c.key))?;
                classes.append(class_name(c.class))?;
                versions.append(version(py, c.version.as_version())?)?;
            }
            (keys, classes, versions).into_pyobject(py)
        })
    }

    /// What committing a plan writes to progress, from what the worker
    /// reports per keyed input: (failed keys, (key, version) loaded at
    /// another version than planned), each sorted. Returns the event, the
    /// batch records, and per keyed input the keys it held back (loaded at
    /// another version than planned while upstream was mid full run or after
    /// a clear since the plan): they keep what they held.
    #[pyo3(signature = (plan, reports, producer=Vec::new(), output_commit=None))]
    fn progress_ops<'py>(
        &self,
        py: Python<'py>,
        plan: &[u8],
        reports: &Bound<'py, PyAny>,
        producer: Vec<u8>,
        output_commit: Option<u64>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        guard(|| {
            let mut rs = vec![];
            for r in reports.try_iter()? {
                let r = r?;
                let t = r
                    .cast::<PyTuple>()
                    .map_err(|_| PyTypeError::new_err("a report is (failed, loaded)"))?;
                let mut rep = tr::progress::InputReport::default();
                for k in t.get_item(0)?.try_iter()? {
                    rep.failed.push(key_in(&k?)?);
                }
                for e in t.get_item(1)?.try_iter()? {
                    let e = e?;
                    rep.loaded
                        .push((key_in(&e.get_item(0)?)?, owned(&e.get_item(1)?)?));
                }
                rs.push(rep);
            }
            let (event, records, held_back) = py
                .detach(|| {
                    let plan = Plan::decode(plan)?;
                    let ops = self.state.read().unwrap().progress_ops(
                        &plan,
                        &rs,
                        &producer,
                        output_commit,
                    )?;
                    Ok::<_, tr::Error>((
                        ops.event().encode(),
                        ops.records.iter().map(|r| r.encode()).collect::<Vec<_>>(),
                        ops.held_back,
                    ))
                })
                .map_err(err)?;
            let records = PyList::new(py, records.iter().map(|r| PyBytes::new(py, r)))?;
            let held_back = PyList::new(
                py,
                held_back
                    .iter()
                    .map(|ks| PyList::new(py, ks.iter().map(|k| key(py, k))))
                    .collect::<PyResult<Vec<_>>>()?,
            )?;
            (PyBytes::new(py, &event), records, held_back).into_pyobject(py)
        })
    }

    /// Owed keys of one keyed edge after `after`, at most `limit`, with their
    /// classes, and where the next page starts.
    #[pyo3(signature = (edge, conditions, own=None, after=None, limit=1000))]
    fn owed<'py>(
        &self,
        py: Python<'py>,
        edge: Vec<u8>,
        conditions: &Bound<'py, PyAny>,
        own: Option<Vec<u8>>,
        after: Option<String>,
        limit: usize,
    ) -> PyResult<Bound<'py, PyTuple>> {
        guard(|| {
            let req = one_input(edge, conditions, own)?;
            let (view, ixs) = self.view(&req)?;
            let after = opt_key(after);
            let (keys, next) = py
                .detach(|| {
                    rt().block_on(tr::plan::owed_keys(
                        &view,
                        &req.inputs[0],
                        ixs.as_ref(),
                        after.as_ref(),
                        limit,
                    ))
                })
                .map_err(err)?;
            let rows = PyList::empty(py);
            for c in &keys {
                rows.append((key(py, &c.key), class_name(c.class)))?;
            }
            (rows, next.as_deref().map(|k| key(py, k))).into_pyobject(py)
        })
    }

    /// How many keys of one keyed edge are owed, by class.
    #[pyo3(signature = (edge, conditions, own=None))]
    fn owed_count<'py>(
        &self,
        py: Python<'py>,
        edge: Vec<u8>,
        conditions: &Bound<'py, PyAny>,
        own: Option<Vec<u8>>,
    ) -> PyResult<Bound<'py, PyDict>> {
        guard(|| {
            let req = one_input(edge, conditions, own)?;
            let (view, ixs) = self.view(&req)?;
            let n = py
                .detach(|| rt().block_on(tr::plan::owed_count(&view, &req.inputs[0], ixs.as_ref())))
                .map_err(err)?;
            counts(py, n)
        })
    }

    #[staticmethod]
    fn event_edge_open<'py>(
        py: Python<'py>,
        edge: Vec<u8>,
        input: Vec<u8>,
        keyed: bool,
    ) -> Bound<'py, PyBytes> {
        PyBytes::new(
            py,
            &one_event(Part::EdgeOpen(tr::progress::EdgeOpen {
                edge: edge.into(),
                input: input.into(),
                keyed,
            })),
        )
    }

    #[staticmethod]
    fn event_edge_close<'py>(py: Python<'py>, edge: Vec<u8>) -> Bound<'py, PyBytes> {
        PyBytes::new(
            py,
            &one_event(Part::EdgeClose(tr::progress::EdgeClose {
                edge: edge.into(),
            })),
        )
    }

    #[staticmethod]
    #[pyo3(signature = (from_, to, input))]
    fn event_edge_rename<'py>(
        py: Python<'py>,
        from_: Vec<u8>,
        to: Vec<u8>,
        input: Vec<u8>,
    ) -> Bound<'py, PyBytes> {
        PyBytes::new(
            py,
            &one_event(Part::EdgeRename(tr::progress::EdgeRename {
                from: from_.into(),
                to: to.into(),
                input: input.into(),
            })),
        )
    }

    #[staticmethod]
    fn event_reset<'py>(py: Python<'py>, edge: Vec<u8>, held: bool) -> Bound<'py, PyBytes> {
        PyBytes::new(
            py,
            &one_event(Part::Reset(tr::progress::Reset {
                edge: edge.into(),
                held,
            })),
        )
    }

    /// Several events as one, their parts in order.
    #[staticmethod]
    fn join<'py>(py: Python<'py>, events: Vec<Vec<u8>>) -> PyResult<Bound<'py, PyBytes>> {
        let mut parts = vec![];
        for e in &events {
            parts.extend(Event::decode(e).map_err(err)?.parts);
        }
        Ok(PyBytes::new(py, &Event::of(parts).encode()))
    }

    /// B4: builds and writes the next commit of `output` from its changed
    /// keys (`encode_changes`'s form), on the change index attached for it;
    /// None if nothing changed. See
    /// `commit_py::build_commit`. `changes` is `bytes`, or the path of a
    /// local file holding them, which nothing writes while the build runs
    /// (the engine's own temporary file): mapped here, so a large write's
    /// changes are read in place rather than held in memory.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (output, changes, *, version, path, time_ms=0, producer=Vec::new(), mode="patch", at=None, ends_full_run=false, collect=0))]
    fn build_commit<'py>(
        &self,
        py: Python<'py>,
        output: &[u8],
        changes: &Bound<'py, PyAny>,
        version: u64,
        path: &str,
        time_ms: u64,
        producer: Vec<u8>,
        mode: &str,
        at: Option<u64>,
        ends_full_run: bool,
        collect: usize,
    ) -> PyResult<Option<Bound<'py, pyo3::types::PyDict>>> {
        let mapped: memmap2::Mmap;
        let data: &[u8] = if let Ok(b) = changes.cast_exact::<PyBytes>() {
            b.as_bytes()
        } else if let Ok(file) = changes.cast_exact::<PyString>() {
            let file = std::fs::File::open(file.to_str()?)?;
            // SAFETY: the map is private to this call, and its file is the caller's own,
            // written by nothing while the build runs.
            mapped = unsafe { memmap2::Mmap::map(&file) }?;
            &mapped
        } else {
            return Err(PyTypeError::new_err(
                "changes: bytes, or a local file's path",
            ));
        };
        let out = self
            .state
            .read()
            .unwrap()
            .outputs
            .get(output)
            .cloned()
            .unwrap_or_default();
        let ixs = self.indexes.read().unwrap().clone();
        crate::commit_py::build_commit(
            py,
            &self.state,
            out,
            ixs.as_ref(),
            output,
            data,
            version,
            path,
            time_ms,
            &producer,
            mode,
            at,
            ends_full_run,
            collect,
        )
    }

    /// B7: the event telling the core an edge's conditions now (journal it
    /// when the edge opens and whenever they change). `gate`: the edge waits
    /// while its upstream is in the middle of a full run.
    #[staticmethod]
    #[pyo3(signature = (edge, conditions, own=None, gate=true))]
    fn event_watch<'py>(
        py: Python<'py>,
        edge: Vec<u8>,
        conditions: &Bound<'py, PyAny>,
        own: Option<Vec<u8>>,
        gate: bool,
    ) -> PyResult<Bound<'py, PyBytes>> {
        crate::stale_py::event_watch(py, edge, conditions, own, gate)
    }

    /// B7: brings edges' staleness current at their upstream's head (every
    /// watched keyed edge whose status is unknown, by default). Returns the
    /// event to journal and apply, or None.
    #[pyo3(signature = (edges=None, *, time_ms=0, not_due=None))]
    fn stale_refresh<'py>(
        &self,
        py: Python<'py>,
        edges: Option<Vec<Vec<u8>>>,
        time_ms: u64,
        not_due: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Option<Bound<'py, PyBytes>>> {
        let ixs = self.indexes.read().unwrap().clone();
        guard(|| crate::stale_py::refresh(py, &self.state, ixs.as_ref(), edges, time_ms, not_due))
    }

    /// B7: each edge's status as known, with no index read: `{edge: status}`.
    #[pyo3(signature = (edges=None))]
    fn stale_status<'py>(
        &self,
        py: Python<'py>,
        edges: Option<Vec<Vec<u8>>>,
    ) -> PyResult<Bound<'py, PyDict>> {
        guard(|| crate::stale_py::status(py, &self.state, edges))
    }

    /// B7: how many keys `edge` owes, by class: `due` now (what
    /// `stale_keys` lists) and `waiting` (failing keys waiting for their
    /// retry, keys held back), and every reason that holds (STA-2); cached
    /// until it, its upstream or its failure records move. None while the
    /// gate holds it (D197).
    #[pyo3(signature = (edge, *, not_due=None))]
    fn stale_count<'py>(
        &self,
        py: Python<'py>,
        edge: Vec<u8>,
        not_due: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Option<Bound<'py, PyDict>>> {
        let ixs = self.indexes.read().unwrap().clone();
        guard(|| crate::stale_py::count(py, &self.state, ixs.as_ref(), &self.counts, edge, not_due))
    }

    /// B7: a page of the keys `edge` owes and are due now (the count's
    /// `due`): `([(key, class), ...], next)`.
    #[pyo3(signature = (edge, after=None, limit=1000, *, not_due=None))]
    fn stale_keys<'py>(
        &self,
        py: Python<'py>,
        edge: Vec<u8>,
        after: Option<String>,
        limit: usize,
        not_due: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let ixs = self.indexes.read().unwrap().clone();
        guard(|| crate::stale_py::keys(py, &self.state, ixs.as_ref(), edge, after, limit, not_due))
    }

    /// B7 (D177): `edge` has processed every key upstream holds under its patterns.
    fn stale_complete(&self, py: Python<'_>, edge: Vec<u8>) -> PyResult<bool> {
        let ixs = self.indexes.read().unwrap().clone();
        guard(|| crate::stale_py::complete(py, &self.state, ixs.as_ref(), edge))
    }

    /// B7: per partition `(input edges, outputs)`, `(stale, upstream_stale)`;
    /// stale is None when an input's status is unknown and none is stale.
    fn stale_graph(
        &self,
        partitions: Vec<crate::stale_py::Names>,
    ) -> PyResult<Vec<(Option<bool>, bool)>> {
        guard(|| crate::stale_py::graph(&self.state, partitions))
    }

    /// B4: the clear that started `output`'s full run, while it is under way.
    fn full_run(&self, output: &[u8]) -> Option<u64> {
        self.state
            .read()
            .unwrap()
            .outputs
            .get(output)
            .and_then(|o| o.full_run)
    }

    /// B4: the event that ends `output`'s full run without a commit: its last
    /// batch changed nothing, or a person let downstream proceed.
    #[staticmethod]
    fn event_full_run_done<'py>(py: Python<'py>, output: Vec<u8>) -> Bound<'py, PyBytes> {
        PyBytes::new(
            py,
            &one_event(Part::FullRunDone(tr::commit::FullRunDone {
                output: output.into(),
            })),
        )
    }
}

fn one_input(
    edge: Vec<u8>,
    conditions: &Bound<'_, PyAny>,
    own: Option<Vec<u8>>,
) -> PyResult<Request> {
    Ok(Request {
        mode: Mode::Incremental,
        reset: false,
        after: None,
        limit: 1,
        inputs: vec![InputRequest {
            edge: edge.into(),
            conditions: self::conditions(conditions)?,
            own: own.map(Into::into),
        }],
        unkeyed: vec![],
    })
}

/// What a stored outcome crosses as: (outcome, tries, deploy, forced, since,
/// last, next_at, until, message).
type StoredTuple = (u8, u64, u64, u64, u64, u64, u64, u64, Vec<u8>);

fn stored_of(t: StoredTuple) -> tr::outcomes::Stored {
    let (outcome, tries, deploy, forced, since, last, next_at, until, message) = t;
    tr::outcomes::Stored {
        outcome,
        tries,
        deploy,
        forced,
        since,
        last,
        next_at,
        until,
        message: message.into(),
    }
}

/// A key's stored outcome after what it came to (docs/per-key-processing.md
/// §9, the transition table): None for no record. `fresh`: the try ran on
/// other input than `prior`'s, which then counts for nothing.
#[pyfunction]
#[pyo3(signature = (prior, fresh, kind, message, retry_after, retry_for, now, deploy, forced, retries))]
#[allow(clippy::too_many_arguments)]
fn outcome_transition<'py>(
    py: Python<'py>,
    prior: Option<StoredTuple>,
    fresh: bool,
    kind: &str,
    message: &[u8],
    retry_after: Option<f64>,
    retry_for: Option<f64>,
    now: f64,
    deploy: u64,
    forced: u64,
    retries: u64,
) -> PyResult<Option<Bound<'py, PyTuple>>> {
    let Some(kind) = tr::outcomes::Kind::parse(kind) else {
        return Err(PyValueError::new_err(format!("no outcome kind {kind:?}")));
    };
    let r = tr::outcomes::Reported {
        kind,
        message: message.into(),
        retry_after,
        retry_for,
    };
    let prior = prior.map(stored_of);
    let Some(s) = tr::outcomes::transition(prior.as_ref(), fresh, &r, now, deploy, forced, retries)
    else {
        return Ok(None);
    };
    let t = (
        s.outcome,
        s.tries,
        s.deploy,
        s.forced,
        s.since,
        s.last,
        s.next_at,
        s.until,
        PyBytes::new(py, &s.message),
    );
    Ok(Some(t.into_pyobject(py)?))
}

/// Whether a retry takes a key with this stored outcome now (§9): `forced`
/// is the forced-request counter per outcome code (index 0 unused).
#[pyfunction]
fn outcome_eligible(
    outcome: u8,
    next_at: u64,
    deploy: u64,
    forced: u64,
    now: f64,
    deploys: u64,
    requests: [u64; 6],
) -> bool {
    let s = tr::outcomes::Stored {
        outcome,
        tries: 0,
        deploy,
        forced,
        since: 0,
        last: 0,
        next_at,
        until: 0,
        message: Box::default(),
    };
    tr::outcomes::eligible(
        &s,
        now,
        &tr::outcomes::Counters {
            deploy: deploys,
            forced: requests,
        },
    )
}

/// Seconds before the next try after `tries` transient failures (§9).
#[pyfunction]
fn outcome_backoff(tries: u64) -> f64 {
    tr::outcomes::transition::backoff(tries)
}

pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(outcome_transition, m)?)?;
    m.add_function(wrap_pyfunction!(outcome_eligible, m)?)?;
    m.add_function(wrap_pyfunction!(outcome_backoff, m)?)?;
    m.add_class::<KeyMatcher>()?;
    m.add_function(wrap_pyfunction!(regex_check, m)?)?;
    m.add_class::<Tracking>()
}

/// Whether a point says its consumer processed the key since its progress
/// started over: one that keeps an absent holding (a failed or held-back
/// key over the empty base) does not (D177).
fn processed(p: &Point) -> bool {
    p.holding != Holding::Absent
        || !matches!(
            p.reason,
            Reason::Failed | Reason::HeldBack | Reason::Unsettled
        )
}
