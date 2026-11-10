//! What is due by time (docs/tracking-design.md §11, D191): the retry clock
//! and retry passes.
//!
//! The clock is the core's output \0d + edge: one entry per retrying or
//! timed-out key, keyed by its due time and then the key, so "what is due
//! now" is one read from the start, and the next due time is its first key.
//! A batch commit moves a key's entry with its record ([`crate::outcomes`]).
//!
//! A retry pass takes due keys in key order, so that a retry batch reads its
//! keys' versions upstream in one range rather than scattered (N6). It
//! starts at a time T under the counters then (deploy, forced requests):
//!
//! - **By the clock**: the keys due at T, at most `PASS_KEYS` of them, sorted
//!   and written as the pass's list (output \0p + edge). Each retry batch
//!   reads the next keys of the list: it reads its due keys and walks
//!   nothing else.
//! - **By a deploy or a forced request** (a class of keys becomes due at
//!   once, whatever the time): no list; each retry batch walks the records
//!   in key order from where the last stopped, taking the eligible ones. A
//!   pass walks each record once.
//!
//! A batch takes a listed key only if it is still eligible at T and its
//! input has not changed since it failed; a key whose input changed is owed
//! by its piece and comes with its change (KEY-8).

use solera_index::{Query, Version};

use crate::codec::Ver;
use crate::commit::{build, Change, Changes, Held as Source, Mode as CommitMode, Spec};
use crate::error::{invalid, Result};
use crate::event::Part;
use crate::outcomes::commit::{records, View};
use crate::outcomes::transition::{eligible, Counters, FAILED, NAMES};
use crate::outcomes::{output_id, record_digest, PassStart, Record, DUE, FAILURES, PASS};
use crate::plan::{Cand, Indexes, InputPlan, Mode, Plan};
use crate::progress::{owed, read_digest, Class, Conditions, Holding, Key, KeyRange};
use crate::state::{Id, Output, Tracking};

/// The most keys a pass by the clock lists; keys due past them wait for the
/// next pass.
pub const PASS_KEYS: usize = 1 << 20;
const PAGE: u64 = 65_536;

/// The due time of a clock entry's key.
fn due_at(k: &[u8]) -> Result<(u64, &[u8])> {
    if k.len() < 8 {
        return invalid("a clock entry without its time");
    }
    Ok((u64::from_be_bytes(k[..8].try_into().unwrap()), &k[8..]))
}

/// The time the edge's first retry by the clock comes due, if any.
pub async fn next_due(ixs: &dyn Indexes, edge: &[u8], due: &Output) -> Result<Option<u64>> {
    if due.live == 0 {
        return Ok(None);
    }
    let did = output_id(DUE, edge);
    let q = Query {
        limit: Some(1),
        ..Default::default()
    };
    let page = ixs.index(&did)?.scan(&due.layers, due.head, &q).await?;
    if page.run.is_empty() {
        return Ok(None);
    }
    Ok(Some(due_at(page.run.key(0))?.0))
}

/// Whether a deploy or a forced request has made a class of records
/// eligible since the last pass.
pub fn by_counters(v: &View, c: &Counters) -> bool {
    let k = &v.per_key;
    (c.deploy > k.last.deploy && k.counts[FAILED as usize] > 0)
        || (1..NAMES.len()).any(|i| c.forced[i] > k.last.forced[i] && k.counts[i] > 0)
}

/// Starts a retry pass at `now`, if anything is due: the parts of the event
/// that begins it (its list's commit and [`PassStart`]), or None.
pub async fn start(
    ixs: &dyn Indexes,
    v: &View,
    pass_list: &Output,
    now: u64,
    counters: Counters,
    path: &str,
) -> Result<Option<Vec<Part>>> {
    if v.per_key.pass.is_some() {
        return invalid("a retry pass is already under way");
    }
    let start = Part::PassStart(PassStart {
        edge: v.edge.clone(),
        n: v.per_key.passes + 1,
        at: now,
        counters,
    });
    if by_counters(v, &counters) {
        return Ok(Some(vec![start]));
    }
    let did = output_id(DUE, &v.edge);
    let mut keys: Vec<Key> = Vec::new();
    let mut after: Option<Vec<u8>> = None;
    'pages: while v.due.live > 0 && keys.len() < PASS_KEYS {
        let q = Query {
            after: after.take(),
            limit: Some(PAGE),
            ..Default::default()
        };
        let page = ixs.index(&did)?.scan(&v.due.layers, v.due.head, &q).await?;
        for i in 0..page.run.len() {
            let (t, k) = due_at(page.run.key(i))?;
            if t > now || keys.len() == PASS_KEYS {
                break 'pages;
            }
            keys.push(k.into());
        }
        match page.next {
            Some(n) => after = Some(n),
            None => break,
        }
    }
    if keys.is_empty() {
        return Ok(None);
    }
    keys.sort_unstable();
    keys.dedup();
    let mut ch = Changes::new();
    for k in &keys {
        ch.push(k, Change::Upsert(Some(b"")))?;
    }
    let spec = Spec {
        version: 0,
        time_ms: now.saturating_mul(1000),
        producer: b"",
        path,
        mode: CommitMode::Patch,
        collect: 0,
    };
    let pid = output_id(PASS, &v.edge);
    let built = build(ixs, &pid, pass_list, spec, &mut Source(Some(&ch))).await?;
    let built = built.expect("a fresh list of keys");
    Ok(Some(vec![Part::OutputCommit(built.part), start]))
}

/// What planning a retry batch reads of the state.
pub struct RetryView {
    pub outcomes: View,
    pub pass_list: Output,
    pub input: Id,
    pub layers: Vec<solera_index::LayerRef>,
    pub head: u64,
}

impl Tracking {
    pub fn retry_view(&self, edge: &[u8]) -> Result<RetryView> {
        let outcomes = self.outcome_view(edge)?;
        let input = self.edges[edge].input.clone();
        let (layers, head) = self
            .outputs
            .get(&input)
            .map_or((vec![], 0), |o| (o.layers.clone(), o.head));
        Ok(RetryView {
            pass_list: self
                .outputs
                .get(&output_id(PASS, edge))
                .cloned()
                .unwrap_or_default(),
            outcomes,
            input,
            layers,
            head,
        })
    }
}

/// The next retry batch of the pass under way: at most `limit` keys of its
/// list (or of the records, for a pass by counters) after its place, each
/// still eligible and unchanged upstream since it failed, as a key list
/// planned under `now`'s conditions. Its range runs from the pass's place to
/// the last key read (to +∞ when the pass reaches its end).
pub async fn plan(
    ixs: &dyn Indexes,
    r: &RetryView,
    now: &std::sync::Arc<Conditions>,
    limit: usize,
) -> Result<Plan> {
    let v = &r.outcomes;
    let Some(pass) = &v.per_key.pass else {
        return invalid("no retry pass under way");
    };
    if limit == 0 {
        return invalid("a batch takes at least one key");
    }
    let fid = output_id(FAILURES, &v.edge);
    let walk = by_counters(v, &pass.counters) || r.pass_list.head == 0;
    // The last key read: where the batch's range ends.
    let mut last: Option<Key> = None;
    let mut found = std::collections::BTreeMap::new();
    let mut more;
    let at = pass.at as f64;
    if walk {
        let mut after = pass.after.as_ref().map(|k| k.to_vec());
        loop {
            let q = Query {
                after: after.take(),
                limit: Some(PAGE.min(limit as u64 * 4)),
                ..Default::default()
            };
            let page = ixs
                .index(&fid)?
                .scan(&v.failures.layers, v.failures.head, &q)
                .await?;
            more = page.next.is_some();
            for i in 0..page.run.len() {
                let Version::Bytes(b) = page.run.new_version(page.run.rows(i).start) else {
                    return invalid("a record that is not a version word");
                };
                let rec = Record::decode(b)?;
                let k: Key = page.run.key(i).into();
                last = Some(k.clone());
                if eligible(&rec.stored, at, &pass.counters) {
                    found.insert(k, rec);
                    if found.len() == limit {
                        more = more || i + 1 < page.run.len();
                        break;
                    }
                }
            }
            if found.len() == limit || !more {
                break;
            }
            after = page.next;
        }
    } else {
        let pid = output_id(PASS, &v.edge);
        let q = Query {
            after: pass.after.as_ref().map(|k| k.to_vec()),
            limit: Some(limit as u64),
            ..Default::default()
        };
        let page = ixs
            .index(&pid)?
            .scan(&r.pass_list.layers, r.pass_list.head, &q)
            .await?;
        more = page.next.is_some();
        let keys: Vec<&[u8]> = (0..page.run.len()).map(|i| page.run.key(i)).collect();
        last = keys.last().map(|k| (*k).into());
        for (k, rec) in records(ixs, &fid, &v.failures, &keys).await? {
            if eligible(&rec.stored, at, &pass.counters) {
                found.insert(k, rec);
            }
        }
    }
    let keys: Vec<&[u8]> = found.keys().map(|k| k.as_ref()).collect();
    let recs: Vec<&Record> = found.values().collect();
    // What upstream has now: what a key failed on, unless the input changed
    // it since. The input's changes since the batch's oldest failure, over
    // the batch's range, say which did (N6: a batch reads its keys' changes,
    // not the input's state over the part of the key space it spans), as
    // long as those changes skip the input's largest layer; else reading
    // the state is cheaper (measured: 128 ms a batch against 179).
    let from = recs.iter().map(|r| r.at).min().unwrap_or(r.head);
    let window = from < r.head
        && r.layers
            .iter()
            .max_by_key(|l| l.size)
            .is_some_and(|big| big.hi <= from);
    let known = window || from >= r.head;
    let mut heads: Vec<Option<Ver>> = recs
        .iter()
        .map(|rec| (known && !rec.on.is_absent()).then(|| rec.on.clone()))
        .collect();
    let input = ixs.index(&r.input)?;
    if let Some(last) = keys.last().filter(|_| window) {
        let q = Query {
            lo: pass.after.as_ref().map(|k| k.to_vec()),
            hi: Some(last.to_vec()),
            ..Default::default()
        };
        let mut j = 0;
        input
            .window_with(&r.layers, from + 1..=r.head, &q, |g| {
                while j < keys.len() && keys[j] < g.key(0) {
                    j += 1;
                }
                if j < keys.len() && keys[j] == g.key(0) {
                    let newest = g.rows(0).start;
                    if g.commit(newest) > recs[j].at {
                        heads[j] = Some(Ver::of(g.new_version(newest)));
                    }
                }
            })
            .await?;
    }
    let unknown: Vec<&[u8]> = keys
        .iter()
        .zip(&heads)
        .filter(|(_, h)| h.is_none())
        .map(|(k, _)| *k)
        .collect();
    if r.head > 0 && !unknown.is_empty() {
        let page = input
            .get(&r.layers, r.head, &unknown, &Query::default())
            .await?;
        let mut j = 0;
        for i in 0..page.run.len() {
            while keys[j] != page.run.key(i) {
                j += 1;
            }
            heads[j] = Some(Ver::of(page.run.new_version(page.run.rows(i).start)));
        }
    }
    let heads = heads.into_iter().map(Option::unwrap_or_default);
    let conds = &v.per_key.conds;
    let mut taken: Vec<Cand> = Vec::new();
    let mut moot: Vec<Key> = Vec::new();
    for ((k, rec), version) in found.iter().zip(heads) {
        let wanted = Holding::of(version.as_version(), now, k);
        let held = rec.held.holding(conds)?;
        if !owed(&held, &wanted) {
            moot.push(k.clone());
        } else if !owed(&rec.tried(k, conds)?, &wanted) {
            taken.push(Cand {
                key: k.clone(),
                class: Class::of(&held, &wanted),
                version: if matches!(wanted, Holding::Absent) {
                    Ver::Absent
                } else {
                    version
                },
                held,
            });
        }
    }
    let range = KeyRange {
        lo: pass.after.clone(),
        hi: if more { last } else { None },
    };
    let keyed = v.progress.keyed().expect("a per-key edge is keyed");
    let input = InputPlan {
        edge: v.edge.clone(),
        output: r.input.clone(),
        head: r.head,
        conditions: now.clone(),
        own_head: None,
        keys: taken,
        held_back: vec![],
        moot,
        expect: read_digest(keyed, false, &range),
        records: 0,
    };
    let mut input = input;
    input.records = input
        .touched()
        .filter_map(|k| found.get(k).map(|r| record_digest(k, r)))
        .fold(0, u64::wrapping_add);
    Ok(Plan {
        mode: Mode::Keys(input.keys.iter().map(|c| c.key.clone()).collect()),
        reset: false,
        range,
        inputs: vec![input],
        unkeyed: vec![],
        pass: Some(pass.n),
    })
}
