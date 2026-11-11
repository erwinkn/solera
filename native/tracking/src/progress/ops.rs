//! From a plan and what the worker reports, what the batch commit writes to
//! progress (§5, "What a batch commit writes") and the batch records.

use std::collections::BTreeSet;

use crate::codec::Ver;
use crate::error::{invalid, Result};
use crate::event::{Event, Part};
use crate::plan::{InputPlan, Mode, Plan, PLANNER_VERSION};
use crate::progress::{
    owed, read_digest, unsettled, BatchProgress, Consumed, Holding, Key, Point, Reason, Value,
};
use crate::record::Record;
use crate::state::Tracking;

/// What the worker reports for one keyed input: the keys that failed, and the
/// version each other key was actually loaded at. A key it does not name was
/// loaded as planned.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct InputReport {
    /// Sorted, distinct.
    pub failed: Vec<Key>,
    /// Sorted by key, distinct.
    pub loaded: Vec<(Key, Ver)>,
}

/// The event parts a batch commit applies to progress, and one record per
/// keyed edge.
#[derive(Clone, Debug)]
pub struct Ops {
    pub parts: Vec<Part>,
    pub records: Vec<Record>,
    /// Per keyed input, sorted: the keys this commit held back. On an input
    /// whose output was mid full run, or cleared since the plan's head, a key
    /// loaded at another version than planned, or a key of the batch that the
    /// input's patterns take but the input did not (a load of it there would
    /// have no planned version to check), is held back on every input of the batch,
    /// since its output is one value computed from all of them.
    /// Each keeps what it held; whether it failed is not recorded.
    pub held_back: Vec<Vec<Key>>,
}

impl Ops {
    pub fn event(&self) -> Event {
        Event::of(self.parts.clone())
    }
}

impl Tracking {
    /// What committing `plan` writes: per keyed input, the new piece over the
    /// batch's range at the head it planned at, under the conditions it was
    /// planned under, and a point for exactly the keys whose holding differs
    /// from what that piece says (failed, held back, loaded at another
    /// version); a key list writes points only. Per unkeyed input, consumed
    /// through the head it planned at.
    pub fn progress_ops(
        &self,
        plan: &Plan,
        reports: &[InputReport],
        producer: &[u8],
        output_commit: Option<u64>,
    ) -> Result<Ops> {
        if reports.len() != plan.inputs.len() {
            return invalid(format!(
                "{} reports for {} keyed inputs",
                reports.len(),
                plan.inputs.len()
            ));
        }
        if reports.iter().any(|r| {
            r.failed.windows(2).any(|w| w[0] >= w[1])
                || r.loaded.windows(2).any(|w| w[0].0 >= w[1].0)
        }) {
            return invalid("a report's keys are sorted and distinct");
        }
        if plan
            .inputs
            .iter()
            .any(|p| p.keys.windows(2).any(|w| w[0].key >= w[1].key))
        {
            return invalid("a plan's keys are sorted and distinct");
        }
        let taken = |p: &InputPlan, k: &Key| p.keys.binary_search_by(|c| c.key.cmp(k)).is_ok();
        if plan.inputs.iter().zip(reports).any(|(p, r)| {
            !r.failed.iter().all(|k| taken(p, k)) || !r.loaded.iter().all(|(k, _)| taken(p, k))
        }) {
            return invalid("a report names a key the batch did not take");
        }
        let mut keyed = Vec::with_capacity(plan.inputs.len());
        for p in &plan.inputs {
            let Some((input, k)) = self
                .edges
                .get(&p.edge)
                .and_then(|e| Some((&e.input, e.progress.keyed()?)))
            else {
                return invalid(format!(
                    "no keyed edge {}",
                    String::from_utf8_lossy(&p.edge)
                ));
            };
            if **input != *p.output {
                return invalid("the edge reads another output than the batch planned");
            }
            if read_digest(k, plan.reset, &plan.range) != p.expect {
                return invalid("the edge's progress moved since the batch was planned");
            }
            keyed.push(k);
        }
        let unsettled = plan
            .inputs
            .iter()
            .map(|p| unsettled(self, &p.output, p.head))
            .collect::<Result<Vec<bool>>>()?;
        let mut batch: Vec<&Key> = plan
            .inputs
            .iter()
            .flat_map(|p| p.keys.iter().map(|c| &c.key))
            .collect();
        batch.sort_unstable();
        batch.dedup();
        let unsure: Vec<Vec<&Key>> = plan
            .inputs
            .iter()
            .zip(reports)
            .map(|(p, r)| {
                // the batch's keys this input did not take, by one merge of
                // two sorted lists
                let mut j = 0;
                let untaken = batch.iter().copied().filter(move |&k| {
                    while p.keys.get(j).is_some_and(|c| &c.key < k) {
                        j += 1;
                    }
                    p.keys.get(j).is_none_or(|c| &c.key != k) && p.conditions.patterns.takes(k)
                });
                moved(p, r).chain(untaken).collect()
            })
            .collect();
        let hold: BTreeSet<&Key> = unsure
            .iter()
            .zip(&unsettled)
            .filter(|(_, &u)| u)
            .flat_map(|(ks, _)| ks.iter().copied())
            .collect();
        let mut parts = Vec::new();
        let mut records = Vec::new();
        let mut held_back = Vec::new();
        for ((((p, report), &unsettled), keyed), unsure) in plan
            .inputs
            .iter()
            .zip(reports)
            .zip(&unsettled)
            .zip(keyed)
            .zip(&unsure)
        {
            let failed = |k: &Key| report.failed.binary_search(k).is_ok();
            let loaded = |k: &Key| {
                report
                    .loaded
                    .binary_search_by(|(x, _)| x.cmp(k))
                    .ok()
                    .map(|i| &report.loaded[i].1)
            };
            let kept_unsure = unsure.iter().any(|k| !hold.contains(k));
            let mut held = Vec::new();
            let mut points: Vec<(Key, Point)> = Vec::new();
            let key_list = matches!(plan.mode, Mode::Keys(_));
            for c in &p.keys {
                let wanted = Holding::of(c.version.as_version(), &p.conditions, &c.key);
                if hold.contains(&c.key) {
                    held.push(c.key.clone());
                    if key_list || owed(&c.held, &wanted) {
                        points.push((
                            c.key.clone(),
                            Point {
                                holding: c.held.clone(),
                                reason: Reason::Unsettled,
                                seen: (!key_list).then(|| c.version.clone()),
                            },
                        ));
                    }
                    continue;
                }
                if failed(&c.key) {
                    if key_list || owed(&c.held, &wanted) {
                        points.push((
                            c.key.clone(),
                            Point {
                                holding: c.held.clone(),
                                reason: Reason::Failed,
                                seen: (!key_list).then(|| c.version.clone()),
                            },
                        ));
                    }
                    continue;
                }
                let got = loaded(&c.key).unwrap_or(&c.version);
                let holding = Holding::of(got.as_version(), &p.conditions, &c.key);
                if key_list {
                    points.push((
                        c.key.clone(),
                        Point {
                            holding,
                            reason: Reason::KeyList,
                            seen: None,
                        },
                    ));
                } else if owed(&holding, &wanted) {
                    points.push((
                        c.key.clone(),
                        Point {
                            holding,
                            reason: Reason::Deviation,
                            seen: Some(c.version.clone()),
                        },
                    ));
                }
            }
            for h in &p.held_back {
                let wanted = Holding::of(h.version.as_version(), &p.conditions, &h.key);
                if owed(&h.held, &wanted) {
                    points.push((
                        h.key.clone(),
                        Point {
                            holding: h.held.clone(),
                            reason: Reason::HeldBack,
                            seen: Some(h.version.clone()),
                        },
                    ));
                }
            }
            points.sort_by(|a, b| a.0.cmp(&b.0));
            let replaced = if plan.reset {
                keyed.slice(&crate::progress::KeyRange::all())
            } else {
                keyed.slice(&plan.range)
            };
            held_back.push(held);
            records.push(Record {
                producer: producer.to_vec(),
                edge: p.edge.clone(),
                output: p.output.clone(),
                mode: match plan.mode {
                    Mode::Incremental => 0,
                    Mode::All => 1,
                    Mode::Keys(_) => 2,
                },
                reset: plan.reset,
                range: plan.range.clone(),
                head: p.head,
                conditions: p.conditions.clone(),
                replaced,
                points: points.clone(),
                counts: p.counts(),
                digest: p.digest(),
                planner: PLANNER_VERSION,
                own_head: p.own_head,
                output_commit,
                keys: match &plan.mode {
                    Mode::Keys(_) => Some(p.keys.iter().map(|c| c.key.clone()).collect()),
                    _ => None,
                },
                waiting: p.waiting.clone(),
            });
            parts.push(Part::Batch(BatchProgress {
                edge: p.edge.clone(),
                reset: plan.reset,
                range: plan.range.clone(),
                piece: (!key_list).then(|| Value::At(p.head, p.conditions.clone())),
                points,
                expect: p.expect,
                fence: (kept_unsure && !unsettled).then(|| (p.output.clone(), p.head)),
            }));
        }
        for u in &plan.unkeyed {
            let c = Consumed {
                edge: u.edge.clone(),
                through: u.head,
                expect: u.through,
            };
            c.check(self)?;
            parts.push(Part::Consumed(c));
        }
        Ok(Ops {
            parts,
            records,
            held_back,
        })
    }
}

/// The keys `r` loaded at another version than `p` planned.
fn moved<'a>(p: &'a InputPlan, r: &'a InputReport) -> impl Iterator<Item = &'a Key> + 'a {
    r.loaded
        .iter()
        .filter(|(k, got)| {
            p.keys.binary_search_by(|c| c.key.cmp(k)).is_ok_and(|i| {
                let wanted = Holding::of(p.keys[i].version.as_version(), &p.conditions, k);
                owed(&Holding::of(got.as_version(), &p.conditions, k), &wanted)
            })
        })
        .map(|(k, _)| k)
}
