//! Python bindings of retention and cleanup (B9, `solera_tracking::retain`):
//! the cut and what holds it, the alarm, cleanup's steps and the versions
//! they release, merges, orphan layer files, and the events that journal
//! them. Each is one call on a `Tracking`; the ones that read or write the
//! index release the GIL.
//!
//! A pin is the engine's `(output, commit, holder, name)`: holder "run" (a
//! retained run's batch records, see `retain_record_pins`) or "attempt"
//! (the head a live attempt planned at), `name` the engine's bytes for it.
//! A cut is `(commit, holder, name)`, where holder may also be "head"
//! (nothing holds it), "cursor" (cleanup's), "piece" or "seen" (`name` is
//! the edge).

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyTuple};
use solera_index::{pick_merge, LayerRef};
use solera_tracking as tr;
use tr::event::{Event, Part};
use tr::plan::Indexes;
use tr::retain::{Cut, Holder, Pin, Retire, Track};

use crate::guard;
use crate::index_py::{key, layer_out, rt, version};
use crate::tracking_py::{err, Tracking};

type PinIn = (Vec<u8>, u64, String, Vec<u8>);

fn pins_in(pins: Vec<PinIn>) -> PyResult<Vec<Pin>> {
    pins.into_iter()
        .map(|(output, commit, holder, name)| {
            let by = match holder.as_str() {
                "run" => Holder::Run(name.into()),
                "attempt" => Holder::Attempt(name.into()),
                _ => {
                    return Err(PyValueError::new_err(format!(
                        "a pin is held by a \"run\" or an \"attempt\", not {holder:?}"
                    )))
                }
            };
            Ok(Pin {
                output: output.into(),
                commit,
                by,
            })
        })
        .collect()
}

fn holder_out<'py>(py: Python<'py>, h: &Holder) -> (&'static str, Option<Bound<'py, PyBytes>>) {
    let (kind, name): (_, Option<&[u8]>) = match h {
        Holder::Head => ("head", None),
        Holder::Cursor => ("cursor", None),
        Holder::Piece(e) => ("piece", Some(e)),
        Holder::Seen(e) => ("seen", Some(e)),
        Holder::Run(r) => ("run", Some(r)),
        Holder::Attempt(a) => ("attempt", Some(a)),
    };
    (kind, name.map(|n| PyBytes::new(py, n)))
}

fn cut_out<'py>(py: Python<'py>, c: &Cut) -> PyResult<Bound<'py, PyTuple>> {
    let (holder, name) = holder_out(py, &c.by);
    (c.commit, holder, name).into_pyobject(py)
}

fn one_event(part: Part) -> Vec<u8> {
    Event::of(vec![part]).encode()
}

/// Every output's cut: `{output: (commit, holder, name)}`, from the state
/// and `pins` of one loop tick: call it on the event loop.
#[pyfunction]
#[pyo3(signature = (tracking, pins=vec![]))]
fn retain_cuts<'py>(
    py: Python<'py>,
    tracking: PyRef<'py, Tracking>,
    pins: Vec<PinIn>,
) -> PyResult<Bound<'py, PyDict>> {
    guard(|| {
        let pins = pins_in(pins)?;
        let cuts = tr::retain::cuts(&tracking.state.read().unwrap(), &pins);
        let d = PyDict::new(py);
        for (output, c) in &cuts {
            d.set_item(PyBytes::new(py, output), cut_out(py, c)?)?;
        }
        Ok(d)
    })
}

/// The alarm (CLN-13): every output whose entries above its cut pass
/// `multiple` times its live keys, as dicts `{"output", "above", "live",
/// "cut": (commit, holder, name)}`. A "piece" holder is released with
/// `Tracking.event_reset(edge, held=True)`: the consumer owes a full run.
#[pyfunction]
#[pyo3(signature = (tracking, *, multiple, pins=vec![]))]
fn retain_alarms<'py>(
    py: Python<'py>,
    tracking: PyRef<'py, Tracking>,
    multiple: u64,
    pins: Vec<PinIn>,
) -> PyResult<Bound<'py, PyList>> {
    guard(|| {
        let pins = pins_in(pins)?;
        let t = tracking.state.read().unwrap();
        let out = PyList::empty(py);
        for a in tr::retain::alarms(&t, tr::retain::cuts(&t, &pins), multiple) {
            let d = PyDict::new(py);
            d.set_item("output", PyBytes::new(py, &a.output))?;
            d.set_item("above", a.above)?;
            d.set_item("live", a.live)?;
            d.set_item("cut", cut_out(py, &a.cut)?)?;
            out.append(d)?;
        }
        Ok(out)
    })
}

/// The pins one retained batch record (`plan`'s record bytes) holds, for
/// the run `run` of the consumer whose output is `own`. The engine keeps,
/// per retained run, the lowest commit per output, and drops them with the
/// run.
#[pyfunction]
fn retain_record_pins<'py>(
    py: Python<'py>,
    record: &[u8],
    own: &[u8],
    run: &[u8],
) -> PyResult<Bound<'py, PyList>> {
    guard(|| {
        let rec = tr::record::Record::decode(record).map_err(err)?;
        let out = PyList::empty(py);
        for p in tr::retain::record_pins(&rec, own, run) {
            let (holder, name) = holder_out(py, &p.by);
            out.append((PyBytes::new(py, &p.output), p.commit, holder, name))?;
        }
        Ok(out)
    })
}

/// The cleanup cursor's next step on `output`: `(from, to, event)`, or
/// None when nothing is released. Call it on the event loop, with the pins
/// of that tick. Delete every version
/// `retain_released(tracking, event)` lists, then journal `event`.
#[pyfunction]
#[pyo3(signature = (tracking, output, pins=vec![]))]
fn retain_step<'py>(
    py: Python<'py>,
    tracking: PyRef<'py, Tracking>,
    output: &[u8],
    pins: Vec<PinIn>,
) -> PyResult<Option<Bound<'py, PyTuple>>> {
    guard(|| {
        let pins = pins_in(pins)?;
        let Some(s) = tr::retain::step(&tracking.state.read().unwrap(), output, &pins) else {
            return Ok(None);
        };
        let (from, to) = (s.from, s.to);
        let event = PyBytes::new(py, &one_event(Part::Cleaned(s)));
        Ok(Some((from, to, event).into_pyobject(py)?))
    })
}

/// A page of the versions the step `event` (from `retain_step`) releases,
/// at most `limit` keys after `after`: `([(key, version), ...], next)`.
/// Refused unless the step still applies (its cursor, in its life).
#[pyfunction]
#[pyo3(signature = (tracking, event, *, after=None, limit=10_000))]
fn retain_released<'py>(
    py: Python<'py>,
    tracking: PyRef<'py, Tracking>,
    event: &[u8],
    after: Option<String>,
    limit: u64,
) -> PyResult<Bound<'py, PyTuple>> {
    guard(|| {
        let s = match &Event::decode(event).map_err(err)?.parts[..] {
            [Part::Cleaned(s)] => s.clone(),
            _ => return Err(PyValueError::new_err("not a cleanup step's event")),
        };
        let layers = {
            let t = tracking.state.read().unwrap();
            s.check(&t).map_err(err)?;
            t.outputs[&s.output].layers.clone()
        };
        let ixs = tracking.indexes.read().unwrap().clone();
        let ix = ixs.index(&s.output).map_err(err)?;
        let after = after.map(String::into_bytes);
        let r = py
            .detach(|| rt().block_on(tr::retain::released(ix, &layers, &s, after, limit)))
            .map_err(err)?;
        let out = PyList::empty(py);
        for (k, v) in &r.versions {
            out.append((key(py, k), version(py, v.as_version())?))?;
        }
        (out, r.next.as_deref().map(|k| key(py, k))).into_pyobject(py)
    })
}

/// An output's layers and cut at one instant: `retain_merge_plan` takes
/// them on the event loop, `retain_merge` builds from them off it.
#[pyclass(frozen, module = "solera._native")]
pub struct MergePlan {
    output: Vec<u8>,
    layers: Vec<LayerRef>,
    #[pyo3(get)]
    cut: u64,
}

/// The outputs the size rule would merge layers of now with every entry
/// kept (a cut of 0, so nothing folds into a base): the engine's outputs,
/// never a per-key edge's record outputs. It reads every output's list and
/// copies none: call it on the event loop.
#[pyfunction]
fn retain_merges_due<'py>(
    py: Python<'py>,
    tracking: PyRef<'py, Tracking>,
) -> Vec<Bound<'py, PyBytes>> {
    let t = tracking.state.read().unwrap();
    t.outputs
        .iter()
        .filter(|(id, o)| !tr::state::core_owned(id) && pick_merge(&o.layers, 0).is_some())
        .map(|(id, _)| PyBytes::new(py, id))
        .collect()
}

/// The merge the size rule asks for on `output` now, or None when none is
/// due: its layers and its cut, from the state and `pins`, the engine's
/// pins, read in one loop tick. Pins only rise, so the cut holds until the
/// merge applies, however long the build waits. Call it on the event loop.
#[pyfunction]
#[pyo3(signature = (tracking, output, pins=vec![]))]
fn retain_merge_plan(
    tracking: PyRef<'_, Tracking>,
    output: &[u8],
    pins: Vec<PinIn>,
) -> PyResult<Option<MergePlan>> {
    guard(|| {
        let pins = pins_in(pins)?;
        let t = tracking.state.read().unwrap();
        let layers = t.outputs.get(output).map_or(vec![], |o| o.layers.clone());
        let cut = tr::retain::cut(&t, output, &pins).commit;
        Ok(pick_merge(&layers, cut).map(|_| MergePlan {
            output: output.into(),
            layers,
            cut,
        }))
    })
}

/// Builds `plan`'s merge at `path` (a fresh random token, never reused;
/// refused before anything is written if any output's list holds it now):
/// `{"event", "replaced": [path, ...], "layer"}`. Reads and writes the
/// whole run: call it off the event loop. Its event is refused if another
/// merge took its layers since, or the output was retired. Once `event`
/// applies, the replaced files may be deleted.
#[pyfunction]
fn retain_merge<'py>(
    py: Python<'py>,
    tracking: PyRef<'py, Tracking>,
    plan: PyRef<'py, MergePlan>,
    path: &str,
) -> PyResult<Bound<'py, PyDict>> {
    guard(|| {
        let held = tracking
            .state
            .read()
            .unwrap()
            .outputs
            .values()
            .any(|o| o.layers.iter().any(|l| l.path == path));
        if held {
            return Err(PyValueError::new_err(format!(
                "the layer path {path} is written twice"
            )));
        }
        let ixs = tracking.indexes.read().unwrap().clone();
        let ix = ixs.index(&plan.output).map_err(err)?;
        let (output, layers, cut) = (&plan.output, &plan.layers, plan.cut);
        let m = py
            .detach(|| rt().block_on(tr::retain::merge(ix, output, layers, cut, path)))
            .map_err(err)?
            .expect("the plan picked a merge");
        let d = PyDict::new(py);
        d.set_item("replaced", &m.replaced)?;
        d.set_item("layer", layer_out(py, &m.layer)?)?;
        d.set_item("event", PyBytes::new(py, &one_event(Part::Merged(m))))?;
        Ok(d)
    })
}

/// The layer files that may be deleted: those `listed` that neither any
/// output's layer list nor a build in flight (`building`: paths of commits
/// and merges started and neither journaled nor given up) names. List
/// first, then call this on the event loop.
#[pyfunction]
fn retain_orphans(
    tracking: PyRef<'_, Tracking>,
    listed: Vec<String>,
    building: Vec<String>,
) -> PyResult<Vec<String>> {
    guard(|| {
        Ok(tr::retain::orphans(
            &tracking.state.read().unwrap(),
            &listed,
            &building,
        ))
    })
}

/// Cleanup of `output` starts at its head `at`: journal it when the output
/// is deployed on a store that names objects by version, before its first
/// commit or in the same event.
#[pyfunction]
fn retain_event_track<'py>(py: Python<'py>, output: Vec<u8>, at: u64) -> Bound<'py, PyBytes> {
    PyBytes::new(
        py,
        &one_event(Part::Track(Track {
            output: output.into(),
            at,
        })),
    )
}

/// `output` is gone with its data: its layers and cursor leave the state.
/// Delete its files after the event applies.
#[pyfunction]
fn retain_event_retire<'py>(py: Python<'py>, output: Vec<u8>) -> Bound<'py, PyBytes> {
    PyBytes::new(
        py,
        &one_event(Part::Retire(Retire {
            output: output.into(),
        })),
    )
}

pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(retain_cuts, m)?)?;
    m.add_function(wrap_pyfunction!(retain_alarms, m)?)?;
    m.add_function(wrap_pyfunction!(retain_record_pins, m)?)?;
    m.add_function(wrap_pyfunction!(retain_step, m)?)?;
    m.add_function(wrap_pyfunction!(retain_released, m)?)?;
    m.add_class::<MergePlan>()?;
    m.add_function(wrap_pyfunction!(retain_merges_due, m)?)?;
    m.add_function(wrap_pyfunction!(retain_merge_plan, m)?)?;
    m.add_function(wrap_pyfunction!(retain_merge, m)?)?;
    m.add_function(wrap_pyfunction!(retain_orphans, m)?)?;
    m.add_function(wrap_pyfunction!(retain_event_track, m)?)?;
    m.add_function(wrap_pyfunction!(retain_event_retire, m)?)
}
