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

use pyo3::create_exception;
use pyo3::exceptions::{PyKeyError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyString, PyTuple};
use solera_index as ix;
use solera_tracking as tr;
use tr::codec::Ver;
use tr::event::{Event, Part};
use tr::plan::{InputRequest, LogRequest, Mode, Plan, Request};
use tr::progress::{read_digest, Class, Conditions, Key, Matcher, Pat, Patterns};

use crate::index_py::{key, layers_in, rt, version, ChangeIndex, Owned};
use crate::{guard, FormatError};

create_exception!(
    _native,
    UnsettledError,
    PyValueError,
    "A batch's commit is refused for good, and the batch is planned again: its input became unsettled since the commit was built (its fence), or a per-key edge's progress or records moved since it was planned."
);

create_exception!(
    _native,
    OutcomesMovedError,
    PyValueError,
    "A per-key batch's outcomes were built from a state that has moved since: catch up and build them again."
);

pub(crate) fn err(e: tr::Error) -> PyErr {
    match e {
        tr::Error::Format(m) => FormatError::new_err(m),
        tr::Error::Invalid(m) => PyValueError::new_err(m),
        tr::Error::Index(e) => crate::index_py::err(e),
        tr::Error::Unsettled => UnsettledError::new_err(tr::error::UNSETTLED),
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
    let mut logs = vec![];
    if let Some(ls) = item(d, "logs")? {
        for l in ls.try_iter()? {
            let l = l?;
            let l = l
                .cast::<PyDict>()
                .map_err(|_| PyTypeError::new_err("a log is a dict: edge, limit"))?;
            logs.push(LogRequest {
                edge: need(l, "edge", "a log")?.extract::<Vec<u8>>()?.into(),
                limit: need(l, "limit", "a log")?.extract()?,
            });
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
        logs,
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
pub(crate) struct Attached(HashMap<Key, ix::ChangeIndex>);

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
pub(crate) struct Tracking {
    pub(crate) state: RwLock<tr::Tracking>,
    pub(crate) indexes: RwLock<Arc<Attached>>,
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

    /// Whether `apply` would take the event, applying nothing. Refused with
    /// `OutcomesMovedError` when its per-key outcomes are what the state
    /// refuses: they were built from a state that moved since, and are
    /// built again.
    fn check(&self, event: &[u8]) -> PyResult<()> {
        guard(|| {
            let ev = Event::decode(event).map_err(err)?;
            let state = self.state.read().unwrap();
            let Err(e) = state.check_event(&ev) else {
                return Ok(());
            };
            let own: Vec<Part> = ev
                .parts
                .iter()
                .filter(|p| match p {
                    Part::Outcomes(_) => true,
                    Part::OutputCommit(c) => tr::state::core_owned(&c.output),
                    _ => false,
                })
                .cloned()
                .collect();
            if !matches!(e, tr::Error::Unsettled) && !own.is_empty() {
                if let Err(moved) = state.check_event(&Event::of(own)) {
                    return Err(OutcomesMovedError::new_err(moved.to_string()));
                }
            }
            Err(err(e))
        })
    }

    /// Every edge the state holds, as `(edge, the output it reads)`.
    fn edges<'py>(&self, py: Python<'py>) -> Vec<(Bound<'py, PyBytes>, Bound<'py, PyBytes>)> {
        let state = self.state.read().unwrap();
        state
            .edges
            .iter()
            .map(|(id, e)| (PyBytes::new(py, id), PyBytes::new(py, &e.input)))
            .collect()
    }

    /// The edges whose walk from empty has not reached the end: a keyed one
    /// with an empty piece, a log's that is still walking.
    fn walking<'py>(&self, py: Python<'py>) -> Vec<Bound<'py, PyBytes>> {
        let state = self.state.read().unwrap();
        let empty = |e: &tr::state::Edge| match e.progress.as_ref() {
            tr::progress::Progress::Keyed(k) => k
                .pieces(&tr::progress::KeyRange::all())
                .iter()
                .any(|p| p.value == tr::progress::Value::Empty),
            tr::progress::Progress::Log(l) => l.walking,
        };
        state
            .edges
            .iter()
            .filter(|(_, e)| empty(e))
            .map(|(id, _)| PyBytes::new(py, id))
            .collect()
    }

    /// Every output the engine named (never a per-key edge's record outputs).
    fn outputs<'py>(&self, py: Python<'py>) -> Vec<Bound<'py, PyBytes>> {
        let state = self.state.read().unwrap();
        state
            .outputs
            .keys()
            .filter(|id| !tr::state::core_owned(id))
            .map(|id| PyBytes::new(py, id))
            .collect()
    }

    /// The event that names `output`, so it has a life before anything reads
    /// or writes it. It keeps the life it has.
    #[staticmethod]
    fn event_declare<'py>(py: Python<'py>, output: Vec<u8>) -> Bound<'py, PyBytes> {
        PyBytes::new(
            py,
            &one_event(Part::Declare(tr::commit::Declare {
                output: output.into(),
            })),
        )
    }

    /// The event that ends `output`'s life: it leaves the state, and its next
    /// naming starts a new one. Refused while an edge reads it.
    #[staticmethod]
    fn event_retire<'py>(py: Python<'py>, output: Vec<u8>) -> Bound<'py, PyBytes> {
        PyBytes::new(
            py,
            &one_event(Part::Retire(tr::retain::Retire {
                output: output.into(),
            })),
        )
    }

    /// One edge as the state holds it, or None when it has none: `input`, the
    /// output it reads; `life`, which changes each time it opens or starts
    /// over; `empty`, the key its first empty piece starts after ("": from
    /// the first key), None once a walk has reached the end. A log's edge
    /// has `log` instead of `empty`: `through`, the commit it consumed
    /// through; `walking`, whether its walk from the log's start goes on;
    /// `restarted`, whether the log started over past what it read.
    fn edge<'py>(&self, py: Python<'py>, edge: &[u8]) -> PyResult<Option<Bound<'py, PyDict>>> {
        let (e, life, restarted) = {
            let state = self.state.read().unwrap();
            let Some(e) = state.edges.get(edge).cloned() else {
                return Ok(None);
            };
            let restarted = e.progress.log().is_some_and(|l| {
                let input = state.outputs.get(&e.input);
                l.restarted(input.map_or(&[][..], |o| &o.layers))
            });
            (e, state.per_key.get(edge).map_or(0, |k| k.life), restarted)
        };
        let d = PyDict::new(py);
        d.set_item("input", PyBytes::new(py, &e.input))?;
        d.set_item("life", life)?;
        if let Some(l) = e.progress.log() {
            let log = PyDict::new(py);
            log.set_item("through", l.through)?;
            log.set_item("walking", l.walking)?;
            log.set_item("restarted", restarted)?;
            d.set_item("log", log)?;
            return Ok(Some(d));
        }
        let empty = e.progress.keyed().and_then(|k| {
            let pieces = k.pieces(&tr::progress::KeyRange::all());
            let first = pieces
                .into_iter()
                .find(|p| p.value == tr::progress::Value::Empty)?;
            Some(
                first
                    .range
                    .lo
                    .map_or_else(String::new, |lo| String::from_utf8_lossy(&lo).into_owned()),
            )
        });
        d.set_item("empty", empty)?;
        Ok(Some(d))
    }

    /// The event that installs on the log `output` the layer its writer
    /// wrote (`ChangeIndex.log_write`), with what it leaves: `(event,
    /// {"commit", "live"})`. Refused unless the layer is the commit after
    /// the head.
    #[pyo3(signature = (output, layer, ends_full_run=false))]
    fn log_commit<'py>(
        &self,
        py: Python<'py>,
        output: &[u8],
        layer: &Bound<'py, PyDict>,
        ends_full_run: bool,
    ) -> PyResult<Bound<'py, PyTuple>> {
        guard(|| {
            let layer = crate::index_py::layer_in(layer)?;
            let commit = self
                .state
                .read()
                .unwrap()
                .log_commit(output, layer, ends_full_run)
                .map_err(err)?;
            let d = PyDict::new(py);
            d.set_item("commit", commit.layer.hi)?;
            d.set_item("live", commit.live)?;
            let event = one_event(Part::OutputCommit(commit));
            (PyBytes::new(py, &event), d).into_pyobject(py)
        })
    }

    /// One output as the state holds it: `{"life", "head", "live", "layers"}`,
    /// or None while nothing has named it.
    fn output<'py>(&self, py: Python<'py>, output: &[u8]) -> PyResult<Option<Bound<'py, PyDict>>> {
        let Some(o) = self.state.read().unwrap().outputs.get(output).cloned() else {
            return Ok(None);
        };
        let d = PyDict::new(py);
        d.set_item("life", o.life)?;
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
    /// that edge holds a point for (a key list processed it) does not count.
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
                                        && points.is_none_or(|p| p.point(k).is_none()))
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
        self.state
            .write()
            .unwrap()
            .set_output(output.into(), layers, head, live, full_run);
        Ok(())
    }

    /// Plans one batch. Returns the plan's bytes (they go into the attempt's
    /// spec untouched) and a summary: its range, per keyed input its head and
    /// its keys by class, and per log its head and the commits the batch
    /// reads, `from + 1..=to`. Refused with `UnsettledError` when a log
    /// started over past what its edge read and the request is no reset.
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
            let logs = PyList::empty(py);
            for l in &plan.logs {
                let i = PyDict::new(py);
                i.set_item("edge", PyBytes::new(py, &l.edge))?;
                i.set_item("head", l.head)?;
                i.set_item("from", l.from)?;
                i.set_item("to", l.to)?;
                logs.append(i)?;
            }
            d.set_item("logs", logs)?;
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
    /// another version than planned, and optionally the keys it did not
    /// process and holds back), each sorted. Returns the event, the
    /// batch records, and per keyed input the keys it held back: on an input
    /// mid full run or cleared since the plan, a key loaded at another version
    /// than planned, or one this input did not take, another input did, and
    /// this input's patterns take; held back on every input, they keep what
    /// they held. A report
    /// names only keys its own input took.
    ///
    /// With `outcomes` (a per-key batch, B6), the event also commits the
    /// outcomes of one keyed input's keys (`OutcomesIn`), and a fourth item
    /// says what each reported key's record holds after: `{key: outcome}`,
    /// none for a key left with no record. Refused with `OutcomesMovedError`
    /// when the edge must catch up first (`outcome_catch_up`), and with
    /// `UnsettledError` when its progress or records moved since the batch
    /// was planned (a catch-up dropped a failure it took): it is planned again.
    #[pyo3(signature = (plan, reports, producer=Vec::new(), output_commit=None, outcomes=None))]
    fn progress_ops<'py>(
        &self,
        py: Python<'py>,
        plan: &[u8],
        reports: &Bound<'py, PyAny>,
        producer: Vec<u8>,
        output_commit: Option<u64>,
        outcomes: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        guard(|| {
            let mut rs = vec![];
            for r in reports.try_iter()? {
                let r = r?;
                let t = r.cast::<PyTuple>().map_err(|_| {
                    PyTypeError::new_err("a report is (failed, loaded) or (failed, loaded, held)")
                })?;
                let mut rep = tr::progress::InputReport::default();
                for k in t.get_item(0)?.try_iter()? {
                    rep.failed.push(key_in(&k?)?);
                }
                for e in t.get_item(1)?.try_iter()? {
                    let e = e?;
                    rep.loaded
                        .push((key_in(&e.get_item(0)?)?, owned(&e.get_item(1)?)?));
                }
                if t.len() > 2 {
                    for k in t.get_item(2)?.try_iter()? {
                        rep.held.push(key_in(&k?)?);
                    }
                }
                rs.push(rep);
            }
            let outcomes = outcomes.map(OutcomesIn::of).transpose()?;
            let ixs = self.indexes.read().unwrap().clone();
            let (event, records, held_back, after) = py
                .detach(|| {
                    let plan = Plan::decode(plan)?;
                    let (mut ops, view) = {
                        let state = self.state.read().unwrap();
                        let view = match &outcomes {
                            None => None,
                            Some(o) => {
                                let Some(p) = plan.inputs.get(o.input) else {
                                    return Err(tr::Error::Invalid(
                                        "no such keyed input in the plan".into(),
                                    ));
                                };
                                // A catch-up sets points where it drops a failure: a per-key
                                // edge's progress moves under a batch that took the key.
                                let moved = state.edges.get(&p.edge).and_then(|e| {
                                    let k = e.progress.keyed()?;
                                    Some(read_digest(k, plan.reset, &plan.range) != p.expect)
                                });
                                if moved == Some(true) {
                                    return Ok(Err(Refused::Replan(
                                        "the per-key edge's progress moved since the batch was planned"
                                            .into(),
                                    )));
                                }
                                Some(state.outcome_view(&p.edge)?)
                            }
                        };
                        let ops = state.progress_ops(&plan, &rs, &producer, output_commit)?;
                        (ops, view)
                    };
                    let mut after = None;
                    let mut parts = vec![];
                    if let (Some(o), Some(v)) = (&outcomes, &view) {
                        let loaded = rs.get(o.input).map_or(&[][..], |r| &r.loaded[..]);
                        let built = rt().block_on(o.commit(
                            ixs.as_ref(),
                            v,
                            &plan,
                            loaded,
                            &producer,
                            &mut ops,
                        ))?;
                        match built {
                            Ok((p, a)) => (parts, after) = (p, Some(a)),
                            Err(refused) => return Ok(Err(refused)),
                        }
                    }
                    let mut event = ops.event();
                    event.parts.extend(parts);
                    Ok::<_, tr::Error>(Ok((
                        event.encode(),
                        ops.records.iter().map(|r| r.encode()).collect::<Vec<_>>(),
                        ops.held_back,
                        after,
                    )))
                })
                .map_err(err)?
                .map_err(Refused::err)?;
            let records = PyList::new(py, records.iter().map(|r| PyBytes::new(py, r)))?;
            let held_back = PyList::new(
                py,
                held_back
                    .iter()
                    .map(|ks| PyList::new(py, ks.iter().map(|k| key(py, k))))
                    .collect::<PyResult<Vec<_>>>()?,
            )?;
            let event = PyBytes::new(py, &event);
            let Some(after) = after else {
                return (event, records, held_back).into_pyobject(py);
            };
            let stored = PyDict::new(py);
            for (k, code) in &after {
                stored.set_item(key(py, k), tr::outcomes::transition::NAMES[*code as usize])?;
            }
            (event, records, held_back, stored).into_pyobject(py)
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

    /// B6: the ids of a per-key edge's record outputs, `(failures, due)`:
    /// the core's own, attached to the change index like any output.
    #[staticmethod]
    fn outcome_outputs<'py>(
        py: Python<'py>,
        edge: &[u8],
    ) -> (Bound<'py, PyBytes>, Bound<'py, PyBytes>) {
        let id = |kind| PyBytes::new(py, &tr::outcomes::output_id(kind, edge));
        (id(tr::outcomes::FAILURES), id(tr::outcomes::DUE))
    }

    /// B6: the event that drops a per-key edge's failures gone moot since it
    /// last looked (KEY-3, KEY-12): a failing key whose input went back to
    /// what the consumer holds, under `conditions` (the edge's now). None
    /// when there is nothing to record. Run after its input's commits and
    /// before the edge is planned or its batch committed.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (edge, conditions, *, now, deploy, retries, time_ms, failures_path, due_path))]
    fn outcome_catch_up<'py>(
        &self,
        py: Python<'py>,
        edge: Vec<u8>,
        conditions: &Bound<'py, PyAny>,
        now: f64,
        deploy: u64,
        retries: u64,
        time_ms: u64,
        failures_path: String,
        due_path: String,
    ) -> PyResult<Option<Bound<'py, PyBytes>>> {
        guard(|| {
            let conds = self::conditions(conditions)?;
            let clock = Clock {
                now,
                deploy,
                retries,
                time_ms,
                failures_path,
                due_path,
            };
            let view = self
                .state
                .read()
                .unwrap()
                .outcome_view(&edge)
                .map_err(err)?;
            let ixs = self.indexes.read().unwrap().clone();
            let parts = py
                .detach(|| {
                    rt().block_on(tr::outcomes::commit::catch_up(
                        ixs.as_ref(),
                        &view,
                        &conds,
                        &clock.ctx(b""),
                    ))
                })
                .map_err(err)?;
            Ok(parts.map(|p| PyBytes::new(py, &Event::of(p).encode())))
        })
    }

    /// B6: a per-key edge's stored outcomes, one row per failing key in key
    /// order: `(key, {outcome, tries, deploy, forced, since, last, next_at,
    /// until, message, tried})`, `tried` the version it last failed on,
    /// `next_at` and `until` None where they do not apply. `keys`: those
    /// keys' rows; else a page after `after`, of the outcomes named in
    /// `outcomes` if any, and where the next page starts.
    #[pyo3(signature = (edge, *, keys=None, after=None, limit=1000, outcomes=None))]
    fn outcome_records<'py>(
        &self,
        py: Python<'py>,
        edge: Vec<u8>,
        keys: Option<Vec<String>>,
        after: Option<String>,
        limit: usize,
        outcomes: Option<Vec<String>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        guard(|| {
            let codes = match outcomes {
                None => None,
                Some(names) => Some(
                    names
                        .iter()
                        .map(|n| outcome_code(n))
                        .collect::<PyResult<Vec<u8>>>()?,
                ),
            };
            let fid = tr::outcomes::output_id(tr::outcomes::FAILURES, &edge);
            let out = self
                .state
                .read()
                .unwrap()
                .outputs
                .get(&fid)
                .cloned()
                .unwrap_or_default();
            let ixs = self.indexes.read().unwrap().clone();
            let wanted = |r: &tr::outcomes::Record| {
                codes.as_ref().is_none_or(|c| c.contains(&r.stored.outcome))
            };
            let (rows, next) = py
                .detach(|| {
                    rt().block_on(async {
                        if let Some(mut keys) = keys {
                            keys.sort_unstable();
                            keys.dedup();
                            let ks: Vec<&[u8]> = keys.iter().map(|k| k.as_bytes()).collect();
                            let found =
                                tr::outcomes::commit::records(ixs.as_ref(), &fid, &out, &ks)
                                    .await?;
                            let rows = found.into_iter().filter(|(_, r)| wanted(r)).collect();
                            return Ok::<_, tr::Error>((rows, None));
                        }
                        let mut rows: Vec<(Key, tr::outcomes::Record)> = vec![];
                        if out.live == 0 || limit == 0 {
                            return Ok((rows, None));
                        }
                        let ix = tr::plan::Indexes::index(ixs.as_ref(), &fid)?;
                        let mut q = ix::Query {
                            after: after.map(String::into_bytes),
                            limit: Some(limit.max(1024) as u64),
                            ..Default::default()
                        };
                        loop {
                            let page = ix.scan(&out.layers, out.head, &q).await?;
                            for i in 0..page.run.len() {
                                let ix::Version::Bytes(b) =
                                    page.run.new_version(page.run.rows(i).start)
                                else {
                                    return Err(tr::Error::Format(
                                        "a record that is not a version word".into(),
                                    ));
                                };
                                let r = tr::outcomes::Record::decode(b)?;
                                if !wanted(&r) {
                                    continue;
                                }
                                if rows.len() == limit {
                                    let last = rows[limit - 1].0.clone();
                                    return Ok((rows, Some(last)));
                                }
                                rows.push((page.run.key(i).into(), r));
                            }
                            match page.next {
                                Some(n) => q.after = Some(n),
                                None => return Ok((rows, None)),
                            }
                        }
                    })
                })
                .map_err(err)?;
            let list = PyList::empty(py);
            for (k, r) in &rows {
                list.append((key(py, k), record_dict(py, r)?))?;
            }
            (list, next.as_deref().map(|k| key(py, k))).into_pyobject(py)
        })
    }

    /// B6: how many of a per-key edge's keys each stored outcome holds:
    /// `{outcome: n}`, every outcome named.
    fn outcome_counts<'py>(&self, py: Python<'py>, edge: &[u8]) -> PyResult<Bound<'py, PyDict>> {
        let counts = self
            .state
            .read()
            .unwrap()
            .per_key
            .get(edge)
            .map_or([0; 6], |k| k.counts);
        let d = PyDict::new(py);
        for (code, name) in tr::outcomes::transition::NAMES.iter().enumerate().skip(1) {
            d.set_item(*name, counts[code])?;
        }
        Ok(d)
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
    /// batch changed nothing.
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

fn outcome_code(name: &str) -> PyResult<u8> {
    match tr::outcomes::transition::NAMES
        .iter()
        .position(|n| *n == name)
    {
        Some(c) if c > 0 => Ok(c as u8),
        _ => Err(PyValueError::new_err(format!(
            "no stored outcome {name:?}: rejected, failed, retrying, canceled or timed_out"
        ))),
    }
}

fn record_dict<'py>(py: Python<'py>, r: &tr::outcomes::Record) -> PyResult<Bound<'py, PyDict>> {
    let s = &r.stored;
    let d = PyDict::new(py);
    d.set_item(
        "outcome",
        tr::outcomes::transition::NAMES[s.outcome as usize],
    )?;
    d.set_item("tries", s.tries)?;
    d.set_item("deploy", s.deploy)?;
    d.set_item("forced", s.forced)?;
    d.set_item("since", s.since)?;
    d.set_item("last", s.last)?;
    d.set_item("next_at", (s.next_at > 0).then_some(s.next_at))?;
    d.set_item("until", (s.until > 0).then_some(s.until))?;
    d.set_item("message", String::from_utf8_lossy(&s.message))?;
    d.set_item("tried", version(py, r.on.as_version())?)?;
    Ok(d)
}

/// The clock and the layer paths a per-key edge's record commits are
/// built with. Forced retries are not counted yet: their counters are 0.
struct Clock {
    now: f64,
    deploy: u64,
    retries: u64,
    time_ms: u64,
    failures_path: String,
    due_path: String,
}

impl Clock {
    fn of(d: &Bound<'_, PyDict>) -> PyResult<Clock> {
        let what = "outcomes";
        Ok(Clock {
            now: need(d, "now", what)?.extract()?,
            deploy: need(d, "deploy", what)?.extract()?,
            retries: need(d, "retries", what)?.extract()?,
            time_ms: need(d, "time_ms", what)?.extract()?,
            failures_path: need(d, "failures_path", what)?.extract()?,
            due_path: need(d, "due_path", what)?.extract()?,
        })
    }

    fn ctx<'a>(&'a self, producer: &'a [u8]) -> tr::outcomes::commit::Ctx<'a> {
        tr::outcomes::commit::Ctx {
            now: self.now,
            counters: tr::outcomes::Counters {
                deploy: self.deploy,
                forced: [0; 6],
            },
            retries: self.retries,
            time_ms: self.time_ms,
            producer,
            failures_path: &self.failures_path,
            due_path: &self.due_path,
        }
    }
}

/// Why a per-key batch's outcomes were not built.
enum Refused {
    /// The edge must catch up first: the engine catches up and builds again.
    Moved(String),
    /// The edge's records moved since the batch was planned: it is planned again.
    Replan(String),
}

impl Refused {
    fn err(self) -> PyErr {
        match self {
            Refused::Moved(m) => OutcomesMovedError::new_err(m),
            Refused::Replan(m) => UnsettledError::new_err(m),
        }
    }
}

/// `progress_ops`'s `outcomes=`: one keyed input's (`input`, its index in
/// the plan) keys that did not succeed, `(key, kind, message, retry_after,
/// retry_for)` sorted by key, and the conditions the batch was planned
/// under (`plan` takes them so).
struct OutcomesIn {
    input: usize,
    reported: Vec<(Key, tr::outcomes::Reported)>,
    conditions: Arc<Conditions>,
    clock: Clock,
}

impl OutcomesIn {
    fn of(d: &Bound<'_, PyDict>) -> PyResult<OutcomesIn> {
        let what = "outcomes";
        let mut reported = vec![];
        for r in need(d, "reported", what)?.try_iter()? {
            let r = r?;
            let kind: String = r.get_item(1)?.extract()?;
            let Some(kind) = tr::outcomes::Kind::parse(&kind) else {
                return Err(PyValueError::new_err(format!("no outcome kind {kind:?}")));
            };
            let message: Option<String> = r.get_item(2)?.extract()?;
            reported.push((
                key_in(&r.get_item(0)?)?,
                tr::outcomes::Reported {
                    kind,
                    message: message.unwrap_or_default().into_bytes().into(),
                    retry_after: r.get_item(3)?.extract()?,
                    retry_for: r.get_item(4)?.extract()?,
                },
            ));
        }
        Ok(OutcomesIn {
            input: need(d, "input", what)?.extract()?,
            reported,
            conditions: conditions(&need(d, "conditions", what)?)?,
            clock: Clock::of(d)?,
        })
    }

    /// The outcome parts of the batch's event, and each reported key's
    /// stored outcome after it (`(key, code)`, for keys left with a record).
    async fn commit(
        &self,
        ixs: &Attached,
        v: &tr::outcomes::commit::View,
        plan: &Plan,
        loaded: &[(Key, Ver)],
        producer: &[u8],
        ops: &mut tr::progress::Ops,
    ) -> tr::Result<Result<(Vec<Part>, Vec<(Key, u8)>), Refused>> {
        use tr::outcomes::{commit as oc, output_id, record_digest, FAILURES};
        // The core refuses a commit an edge did not catch up for: said here, so
        // that the engine catches up and builds again.
        let looked = (v.per_key.seen, v.per_key.cond);
        let now = (v.upstream.head, tr::outcomes::cond_id(&self.conditions));
        if v.failures.live > 0 && looked != now {
            return Ok(Err(Refused::Moved(
                "catch up first: the edge's failures were not looked at through its input's head"
                    .into(),
            )));
        }
        let ctx = self.clock.ctx(producer);
        let fid = output_id(FAILURES, &v.edge);
        let parts = match oc::commit(
            ixs,
            v,
            plan,
            self.input,
            &self.reported,
            loaded,
            &self.conditions,
            &ctx,
            ops,
        )
        .await
        {
            Ok(parts) => parts,
            Err(e @ tr::Error::Invalid(_)) => {
                let p = &plan.inputs[self.input];
                let mut touched: Vec<&[u8]> = p.touched().map(|k| k.as_ref()).collect();
                touched.sort_unstable();
                touched.dedup();
                let read = oc::records(ixs, &fid, &v.failures, &touched)
                    .await?
                    .iter()
                    .map(|(k, r)| record_digest(k, r))
                    .fold(0, u64::wrapping_add);
                if read != p.records {
                    return Ok(Err(Refused::Replan(
                        "the edge's per-key records moved since the batch was planned".into(),
                    )));
                }
                return Err(e);
            }
            Err(e) => return Err(e),
        };
        // What the records hold after: the output with the commit's layer on top.
        let mut out = v.failures.clone();
        for part in &parts {
            if let Part::OutputCommit(c) = part {
                if c.output == fid {
                    out.layers.push(c.layer.clone());
                    out.head = c.layer.hi;
                    out.live = c.live;
                }
            }
        }
        let keys: Vec<&[u8]> = self.reported.iter().map(|(k, _)| k.as_ref()).collect();
        let after = oc::records(ixs, &fid, &out, &keys)
            .await?
            .into_iter()
            .map(|(k, r)| (k, r.stored.outcome))
            .collect();
        Ok(Ok((parts, after)))
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
        logs: vec![],
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
    m.add("UnsettledError", m.py().get_type::<UnsettledError>())?;
    m.add(
        "OutcomesMovedError",
        m.py().get_type::<OutcomesMovedError>(),
    )?;
    m.add_function(wrap_pyfunction!(outcome_transition, m)?)?;
    m.add_function(wrap_pyfunction!(outcome_eligible, m)?)?;
    m.add_function(wrap_pyfunction!(outcome_backoff, m)?)?;
    m.add_class::<KeyMatcher>()?;
    m.add_function(wrap_pyfunction!(regex_check, m)?)?;
    m.add_class::<Tracking>()
}
