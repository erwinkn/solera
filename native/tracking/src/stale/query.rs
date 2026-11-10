//! Questions about staleness: an edge's status, the owed count (computed on
//! request and cached until the upstream or the progress moves), the page of
//! owed keys, completeness (D177), and staleness down the graph.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

use solera_index::Query;

use crate::error::Result;
use crate::plan::{owed_count, owed_keys, Cand, Indexes};
use crate::progress::{Key, Progress, Value};
use crate::stale::{Ask, Reason, State};
use crate::state::{Id, Tracking};

/// What an edge's staleness is now.
#[derive(Clone, Debug, PartialEq)]
pub enum Status {
    /// No conditions were journaled for the edge yet.
    Unwatched,
    /// Its upstream is in the middle of a full run and the gate holds it (rule 8).
    Held,
    /// What is known no longer holds: refresh it.
    Unknown,
    Fresh,
    /// Something is due. `key` is the witness (None on an unkeyed edge).
    Stale {
        key: Option<Key>,
        reason: Reason,
        since_ms: u64,
    },
}

impl Tracking {
    /// Rule 7: what is known, if it holds now (the edge's progress and
    /// conditions have not moved, and nothing was committed upstream since).
    pub fn stale_status(&self, edge: &[u8]) -> Result<Status> {
        let Some(e) = self.edges.get(edge) else {
            return crate::error::invalid(format!("no edge {}", String::from_utf8_lossy(edge)));
        };
        let head = self.outputs.get(&e.input).map_or(0, |o| o.head);
        if let Progress::Unkeyed(through) = *e.progress {
            return Ok(if head > through {
                Status::Stale {
                    key: None,
                    reason: Reason::InputChanged,
                    since_ms: 0,
                }
            } else {
                Status::Fresh
            });
        }
        let i = &self.stale.inputs[edge];
        if i.watch.is_none() {
            return Ok(Status::Unwatched);
        }
        if self.stale.held(i, &e.input) {
            return Ok(Status::Held);
        }
        Ok(match i.current().filter(|k| k.x == head) {
            None => Status::Unknown,
            Some(k) => match &k.state {
                State::Fresh => Status::Fresh,
                State::Stale {
                    key,
                    reason,
                    since_ms,
                } => Status::Stale {
                    key: Some(key.clone()),
                    reason: *reason,
                    since_ms: *since_ms,
                },
            },
        })
    }
}

/// Owed counts by class (added, updated, removed), computed on request and
/// kept until the edge's progress, its conditions or its upstream's head move.
/// While the gate holds the edge there is no number (D197): None.
#[derive(Default)]
pub struct Counts(Mutex<HashMap<Id, Counted>>);

/// Counts by class, with the edge's generation and the head they hold for.
type Counted = (u64, u64, [u64; 4]);

impl Counts {
    pub async fn get(&self, ask: &Ask, ixs: &dyn Indexes) -> Result<Option<[u64; 4]>> {
        if ask.held {
            return Ok(None);
        }
        let at = (ask.input.gen, ask.head);
        if let Some(&(g, h, n)) = self.0.lock().unwrap().get(&ask.edge) {
            if (g, h) == at {
                return Ok(Some(n));
            }
        }
        let n = owed_count(&ask.view, &ask.req, ixs).await?;
        self.0
            .lock()
            .unwrap()
            .insert(ask.edge.clone(), (at.0, at.1, n));
        Ok(Some(n))
    }

    /// Drops what is kept for edges that are gone.
    pub fn retain(&self, t: &Tracking) {
        self.0
            .lock()
            .unwrap()
            .retain(|e, _| t.stale.inputs.contains_key(e));
    }
}

impl Ask {
    /// The owed keys after `after`, at most `limit`, and where the next page starts.
    pub async fn keys(
        &self,
        ixs: &dyn Indexes,
        after: Option<&Key>,
        limit: usize,
    ) -> Result<(Vec<Cand>, Option<Key>)> {
        owed_keys(&self.view, &self.req, ixs, after, limit).await
    }

    /// D177: every upstream key the patterns take has been processed since
    /// progress last started over: no `empty` piece holds one that no point covers.
    pub async fn complete(&self, ixs: &dyn Indexes) -> Result<bool> {
        if self.head == 0 {
            return Ok(true);
        }
        let ix = ixs.index(&self.output)?;
        let keyed = self.keyed();
        let now = &self.req.conditions;
        for piece in keyed.pieces(&Default::default()) {
            if piece.value != Value::Empty {
                continue;
            }
            let mut q = Query {
                lo: piece.range.lo.as_deref().map(<[u8]>::to_vec),
                hi: piece.range.hi.as_deref().map(<[u8]>::to_vec),
                pattern: now.patterns.index_pattern(),
                ..Default::default()
            };
            let mut limit = 16;
            loop {
                q.limit = Some(limit);
                let page = ix.scan(&self.layers, self.head, &q).await?;
                for i in 0..page.run.len() {
                    let k = page.run.key(i);
                    if now.patterns.takes(k) && keyed.point(k).is_none() {
                        return Ok(false);
                    }
                }
                match page.next {
                    Some(n) => q.after = Some(n),
                    None => break,
                }
                limit = (limit * 4).min(1 << 20);
            }
        }
        Ok(true)
    }
}

/// One consumer partition: the edges it reads, the outputs it writes.
pub struct Partition {
    pub inputs: Vec<Id>,
    pub outputs: Vec<Id>,
}

/// Per partition: stale (any input stale; None if some input's status is
/// unknown and none is stale) and upstream stale (some partition it reads
/// from, directly or further up, is stale).
pub fn graph(t: &Tracking, parts: &[Partition]) -> Result<Vec<(Option<bool>, bool)>> {
    let mut stale = Vec::with_capacity(parts.len());
    for p in parts {
        let mut s = Some(false);
        for e in &p.inputs {
            match t.stale_status(e)? {
                Status::Stale { .. } => {
                    s = Some(true);
                    break;
                }
                Status::Unknown | Status::Unwatched => s = None,
                Status::Fresh | Status::Held => {}
            }
        }
        stale.push(s);
    }
    let mut readers: HashMap<&[u8], Vec<usize>> = HashMap::new();
    for (j, p) in parts.iter().enumerate() {
        for e in &p.inputs {
            if let Some(edge) = t.edges.get(e) {
                readers.entry(&edge.input).or_default().push(j);
            }
        }
    }
    let mut up = vec![false; parts.len()];
    let mut queue: VecDeque<usize> = (0..parts.len())
        .filter(|&j| stale[j] == Some(true))
        .collect();
    let mut seen = vec![false; parts.len()];
    while let Some(j) = queue.pop_front() {
        if std::mem::replace(&mut seen[j], true) {
            continue;
        }
        for o in &parts[j].outputs {
            for &r in readers.get(o.as_ref()).map_or(&[][..], |v| v) {
                up[r] = true;
                queue.push_back(r);
            }
        }
    }
    Ok(stale.into_iter().zip(up).collect())
}
