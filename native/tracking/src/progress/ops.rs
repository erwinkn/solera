//! From a plan and what the worker reports, what the batch commit writes to
//! progress (§5, "What a batch commit writes") and the batch records.

use crate::codec::Ver;
use crate::error::{invalid, Result};
use crate::event::{Event, Part};
use crate::plan::{Mode, Plan, PLANNER_VERSION};
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
    /// Per keyed input, sorted: the keys this commit held back because they
    /// were loaded at another version than planned while the input's output
    /// was mid full run or after a clear since the plan's head. Each keeps
    /// what it held; whether it failed is not recorded.
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
        let mut parts = Vec::new();
        let mut records = Vec::new();
        let mut held_back = Vec::new();
        for (p, report) in plan.inputs.iter().zip(reports) {
            let Some(keyed) = self.edges.get(&p.edge).and_then(|e| e.progress.keyed()) else {
                return invalid(format!(
                    "no keyed edge {}",
                    String::from_utf8_lossy(&p.edge)
                ));
            };
            if read_digest(keyed, plan.reset, &plan.range) != p.expect {
                return invalid("the edge's progress moved since the batch was planned");
            }
            if report.failed.windows(2).any(|w| w[0] >= w[1])
                || report.loaded.windows(2).any(|w| w[0].0 >= w[1].0)
            {
                return invalid("a report's keys are sorted and distinct");
            }
            let failed = |k: &Key| report.failed.binary_search(k).is_ok();
            let loaded = |k: &Key| {
                report
                    .loaded
                    .binary_search_by(|(x, _)| x.cmp(k))
                    .ok()
                    .map(|i| &report.loaded[i].1)
            };
            let unsettled = unsettled(self, &p.output, p.head);
            let mut any_moved = false;
            let mut held = Vec::new();
            let mut points: Vec<(Key, Point)> = Vec::new();
            let key_list = matches!(plan.mode, Mode::Keys(_));
            for c in &p.keys {
                let wanted = Holding::of(c.version.as_version(), &p.conditions, &c.key);
                let moved = loaded(&c.key).is_some_and(|v| {
                    owed(&Holding::of(v.as_version(), &p.conditions, &c.key), &wanted)
                });
                any_moved |= moved;
                if unsettled && moved {
                    held.push(c.key.clone());
                    if key_list || owed(&c.held, &wanted) {
                        points.push((
                            c.key.clone(),
                            Point {
                                holding: c.held.clone(),
                                reason: Reason::HeldBack,
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
            });
            parts.push(Part::Batch(BatchProgress {
                edge: p.edge.clone(),
                reset: plan.reset,
                range: plan.range.clone(),
                piece: (!key_list).then(|| Value::At(p.head, p.conditions.clone())),
                points,
                expect: p.expect,
                fence: any_moved.then_some((p.head, unsettled)),
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
