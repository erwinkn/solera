//! The cut and cleanup over random histories on the real change index
//! (§13, phase 2 of SOLE-7's model): an upstream output, on a store that
//! names objects by version (cleaned) or not, with patches, replaces and
//! clears; consumers
//! that plan at the head, commit late, pause, die and are released; runs
//! that keep their batch records for a while; merges built from a cut taken
//! earlier and applied later; cleanup steps in pages; crashed builds and
//! merged-away files swept as orphans. After every step: no live attempt
//! can load a deleted version, every pin can still diff to the head, the
//! index equals a brute-force fold, and cleanup deleted exactly what the
//! commits up to its cursor replaced. Each rule switched off alone breaks
//! one of these. `RETAIN_SEEDS` sets how many histories.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use solera_index::*;

use super::cut::{self as rules, OFF};
use super::*;
use crate::codec::Ver;
use crate::commit::{build, Change, Changes, Held, Mode, Spec};
use crate::event::{Event, Part};
use crate::outcomes::{output_id, PerKey, DUE, FAILURES};
use crate::plan::OneIndex;
use crate::progress::{
    read_digest, BatchProgress, Conditions, EdgeClose, EdgeOpen, EdgeRename, KeyRange, Reset,
    Slice, Value,
};
use crate::record::Record;
use crate::state::Output;

const ITEMS: &[u8] = b"items";
const EDGES: [&[u8]; 4] = [b"copy", b"tally", b"checks", b"archive"];
const KEYS: usize = 24;

type St = BTreeMap<Vec<u8>, u64>;

fn key(i: usize) -> Vec<u8> {
    format!("k{i:02}").into_bytes()
}

fn items() -> Id {
    ITEMS.into()
}

fn int(v: Version<'_>) -> Option<u64> {
    match v {
        Version::Absent => None,
        Version::Int(n) => Some(n),
        Version::Bytes(_) => panic!("a cleaned output's versions are generations"),
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
}

/// A rule the model switches off.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Off {
    None,
    /// Core: the cut ignores consumers' pieces.
    Pieces,
    /// Core: the cut ignores the cleanup cursor.
    Cursor,
    /// Core: the cut ignores B6's `seen` of a per-key edge with failing keys.
    Seen,
    /// Engine: the cut ignores retained runs' records.
    Records,
    /// Engine: the cut ignores live attempts' heads.
    CutAttempts,
    /// Engine: cleanup ignores live attempts' heads.
    CleanupAttempts,
}

struct Attempt {
    name: Vec<u8>,
    edge: Id,
    head: u64,
    range: KeyRange,
    expect: u64,
    slice: Slice,
}

struct Model {
    rng: SmallRng,
    off: Off,
    mem: Arc<InMemory>,
    ix: ChangeIndex,
    t: Tracking,
    cond: Arc<Conditions>,
    /// The state after each commit, and what each commit replaced.
    states: Vec<St>,
    replaced: Vec<Vec<(Vec<u8>, u64)>>,
    attempts: Vec<Attempt>,
    /// Retained runs, oldest first, with their records' pins; how many are kept.
    runs: VecDeque<Vec<Pin>>,
    keep: usize,
    deleted: BTreeSet<(Vec<u8>, u64)>,
    /// Merges built and not yet journaled, each with how many merges had
    /// applied when it was built (two may be built from one list).
    building: Vec<(Merged, u64)>,
    merges: u64,
    n: u64,
}

type Res = std::result::Result<(), String>;

impl Model {
    fn new(seed: u64, off: Off, rt: &tokio::runtime::Runtime) -> Model {
        let mut rng = SmallRng::seed_from_u64(seed);
        let mem = Arc::new(InMemory::new());
        let store = Store::new(
            mem.clone(),
            Path::from("t"),
            rt.handle().clone(),
            StoreOptions {
                memory_bytes: [0, 1 << 20][rng.random_range(0..2)],
                ..Default::default()
            },
        )
        .unwrap();
        let ix = ChangeIndex::new(
            store,
            WriteOptions {
                block_bytes: rng.random_range(1..200),
                level: 1,
                seg_blocks: rng.random_range(1..4),
            },
        );
        let mut t = Tracking::new();
        let mut parts = vec![];
        if rng.random_bool(0.5) {
            parts.push(Part::Track(Track {
                output: items(),
                at: 0,
            }));
        }
        parts.extend(EDGES.iter().map(|e| {
            Part::EdgeOpen(EdgeOpen {
                edge: (*e).into(),
                input: items(),
                keyed: true,
            })
        }));
        t.apply_event(Event::of(parts)).unwrap();
        let keep = rng.random_range(0..4);
        Model {
            rng,
            off,
            mem,
            ix,
            t,
            cond: Arc::new(Conditions::default()),
            states: vec![St::new()],
            replaced: vec![vec![]],
            attempts: vec![],
            runs: VecDeque::new(),
            keep,
            deleted: BTreeSet::new(),
            building: vec![],
            merges: 0,
            n: 0,
        }
    }

    fn head(&self) -> u64 {
        self.t.outputs.get(ITEMS).map_or(0, |o| o.head)
    }

    fn layers(&self) -> Vec<LayerRef> {
        self.t
            .outputs
            .get(ITEMS)
            .map_or(vec![], |o| o.layers.clone())
    }

    fn name(&mut self, what: &str) -> String {
        self.n += 1;
        format!("{what}{}-{:x}", self.n, self.rng.random::<u32>())
    }

    fn apply(&mut self, part: Part) -> std::result::Result<(), crate::Error> {
        self.t.apply_event(Event::of(vec![part]))
    }

    fn pins(&self, for_cut: bool) -> Vec<Pin> {
        let attempts = match self.off {
            Off::CutAttempts => !for_cut,
            Off::CleanupAttempts => for_cut,
            _ => true,
        };
        let mut pins = vec![];
        if attempts {
            pins.extend(self.attempts.iter().map(|a| Pin {
                output: items(),
                commit: a.head,
                by: Holder::Attempt(a.name.clone().into()),
            }));
        }
        if for_cut && self.off != Off::Records {
            pins.extend(self.runs.iter().flatten().cloned());
        }
        pins
    }

    /// An upstream commit; sometimes its engine dies after writing the layer.
    async fn commit(&mut self) -> Res {
        let c = self.head() + 1;
        let mode = match self.rng.random_range(0..10) {
            0 => Mode::Clear,
            1 => Mode::Replace,
            _ => Mode::Patch,
        };
        let mut named = vec![];
        for i in 0..KEYS {
            if self.rng.random_bool(0.3) {
                let ch = if self.rng.random_bool(0.3) {
                    Change::Removed
                } else {
                    Change::Upsert(None)
                };
                named.push((key(i), ch));
            }
        }
        let items_: Vec<(&[u8], Change<'_>)> = named.iter().map(|(k, c)| (&k[..], *c)).collect();
        let changes = Changes::of(&items_).unwrap();
        let old = self.states.last().unwrap().clone();
        let mut now = if mode == Mode::Patch {
            old.clone()
        } else {
            St::new()
        };
        for (k, ch) in &named {
            match ch {
                Change::Removed => {
                    now.remove(k);
                }
                Change::Upsert(_) => {
                    now.insert(k.clone(), c);
                }
            }
        }
        let out = self.t.outputs.get(ITEMS).cloned().unwrap_or_default();
        let path = format!("items/{}.lay", self.name("c"));
        let spec = Spec {
            version: c,
            time_ms: 0,
            producer: b"p",
            path: &path,
            mode,
            ends_full_run: mode == Mode::Clear,
            collect: 0,
        };
        let built = build(
            &OneIndex(&self.ix),
            &items(),
            &out,
            spec,
            &mut Held(Some(&changes)),
        )
        .await
        .map_err(|e| format!("commit {c}: {e}"))?;
        let Some(b) = built else {
            return (now == old)
                .then_some(())
                .ok_or(format!("commit {c} changed nothing"));
        };
        if self.rng.random_bool(0.05) {
            return Ok(());
        }
        self.apply(Part::OutputCommit(b.part))
            .map_err(|e| format!("commit {c}: {e}"))?;
        let gone = old
            .iter()
            .filter(|(k, v)| now.get(*k) != Some(v))
            .map(|(k, v)| (k.clone(), *v))
            .collect();
        self.states.push(now);
        self.replaced.push(gone);
        Ok(())
    }

    fn range(&mut self) -> KeyRange {
        let (a, b) = (
            self.rng.random_range(0..=KEYS),
            self.rng.random_range(0..=KEYS),
        );
        let (lo, hi) = (a.min(b), a.max(b));
        if lo == hi {
            return KeyRange::all();
        }
        KeyRange {
            lo: (lo > 0).then(|| key(lo - 1).into()),
            hi: (hi < KEYS).then(|| key(hi - 1).into()),
        }
    }

    /// A consumer plans a batch at the head.
    fn plan(&mut self) {
        let free: Vec<&[u8]> = EDGES
            .iter()
            .copied()
            .filter(|e| !self.attempts.iter().any(|a| *a.edge == **e))
            .collect();
        if free.is_empty() {
            return;
        }
        let edge: Id = free[self.rng.random_range(0..free.len())].into();
        let range = self.range();
        let k = self.t.edges[&edge].progress.keyed().unwrap();
        let (expect, slice) = (read_digest(k, false, &range), k.slice(&range));
        let name = self.name("a").into_bytes();
        let head = self.head();
        self.attempts.push(Attempt {
            name,
            edge,
            head,
            range,
            expect,
            slice,
        });
    }

    /// A live attempt commits its batch, and its run keeps the record.
    fn finish(&mut self) -> Res {
        if self.attempts.is_empty() {
            return Ok(());
        }
        let a = self
            .attempts
            .remove(self.rng.random_range(0..self.attempts.len()));
        let k = self.t.edges[&a.edge].progress.keyed().unwrap();
        if read_digest(k, false, &a.range) != a.expect {
            return Ok(());
        }
        let part = BatchProgress {
            edge: a.edge.clone(),
            reset: false,
            range: a.range.clone(),
            piece: Some(Value::At(a.head, self.cond.clone())),
            head: a.head,
            points: vec![],
            expect: a.expect,
            fence: None,
        };
        self.apply(Part::Batch(part))
            .map_err(|e| format!("a batch planned at {} was refused: {e}", a.head))?;
        let rec = Record {
            producer: a.name.clone(),
            edge: a.edge,
            output: items(),
            mode: 0,
            reset: false,
            range: a.range,
            head: a.head,
            conditions: self.cond.clone(),
            replaced: a.slice,
            marks: vec![],
            counts: [0; 4],
            digest: 0,
            planner: 0,
            own_head: None,
            output_commit: None,
            keys: None,
            waiting: vec![],
        };
        let run = self.name("r");
        self.runs
            .push_back(record_pins(&rec, b"own", run.as_bytes()));
        while self.runs.len() > self.keep {
            self.runs.pop_front();
        }
        Ok(())
    }

    /// A merge: built from a cut now, journaled now or a few steps later.
    async fn merge(&mut self) -> Res {
        if self.building.len() == 2 || !self.building.is_empty() && self.rng.random_bool(0.5) {
            let i = self.rng.random_range(0..self.building.len());
            let (m, at) = self.building.remove(i);
            let replaced = m.replaced.clone();
            match self.apply(Part::Merged(m)) {
                Ok(()) => self.merges += 1,
                Err(_) if at < self.merges => return Ok(()),
                Err(e) => return Err(format!("a merge built earlier was refused: {e}")),
            }
            if self.rng.random_bool(0.8) {
                for p in replaced {
                    self.mem
                        .delete(&Path::from(format!("t/{p}")))
                        .await
                        .unwrap();
                }
            }
            return Ok(());
        }
        let layers = self.layers();
        if layers.is_empty() {
            return Ok(());
        }
        let cut = cut(&self.t, ITEMS, &self.pins(true)).commit;
        let path = format!("items/{}.lay", self.name("m"));
        let m = if self.rng.random_bool(0.5) {
            merge(&self.ix, ITEMS, &layers, cut, &path).await
        } else {
            let end = self.rng.random_range(1..=layers.len());
            let start = if self.rng.random_bool(0.7) {
                0
            } else {
                self.rng.random_range(0..end)
            };
            self.ix
                .merge(&layers, start..end, cut, &path)
                .await
                .map(|layer| {
                    Some(Merged {
                        output: items(),
                        replaced: layers[start..end].iter().map(|l| l.path.clone()).collect(),
                        layer,
                    })
                })
                .map_err(Into::into)
        };
        let Some(m) = m.map_err(|e| format!("a merge at cut {cut}: {e}"))? else {
            return Ok(());
        };
        self.building.push((m, self.merges));
        if self.rng.random_bool(0.5) {
            return Box::pin(self.merge()).await;
        }
        Ok(())
    }

    /// A cleanup step, page by page, then journaled.
    async fn clean(&mut self) -> Res {
        let Some(s) = step(&self.t, ITEMS, &self.pins(false)) else {
            return Ok(());
        };
        let (layers, limit) = (self.layers(), self.rng.random_range(1..8));
        let mut after = None;
        loop {
            let page = released(&self.ix, &layers, &s, after, limit)
                .await
                .map_err(|e| format!("cleanup from {} cannot read its window: {e}", s.from))?;
            for (k, v) in page.versions {
                let Ver::Int(n) = v else {
                    return Err(format!("released {v:?}"));
                };
                if !self.deleted.insert((k.to_vec(), n)) {
                    return Err(format!("{k:?}@{n} released twice"));
                }
            }
            match page.next {
                Some(n) => after = Some(n),
                None => break,
            }
        }
        self.apply(Part::Cleaned(s))
            .map_err(|e| format!("a cleanup step was refused: {e}"))
    }

    /// B6 looks for reverts on an edge through the head (its `seen` only
    /// rises), and some of its keys fail or none do. While some do, its
    /// next look reads the window after `seen`.
    fn pass(&mut self) {
        let edge: Id = EDGES[self.rng.random_range(0..EDGES.len())].into();
        let head = self.head();
        let k = self.t.per_key.entry(edge.clone()).or_default();
        k.seen = k.seen.max(head);
        let failing = Output {
            live: self.rng.random_range(0..2),
            head: 1,
            ..Default::default()
        };
        self.t.outputs.insert(output_id(FAILURES, &edge), failing);
    }

    /// A person releases a consumer's place: it owes a full run.
    fn release(&mut self) {
        let edge = EDGES[self.rng.random_range(0..EDGES.len())];
        if !self.attempts.iter().any(|a| *a.edge == *edge) {
            self.apply(Part::Reset(Reset {
                edge: edge.into(),
                held: true,
            }))
            .unwrap();
        }
    }

    /// The sweep: list the output's prefix, delete what [`orphans`] names.
    async fn sweep(&mut self) -> Res {
        let listed: Vec<String> = self
            .mem
            .list_with_delimiter(Some(&Path::from("t/items")))
            .await
            .unwrap()
            .objects
            .into_iter()
            .map(|o| o.location.as_ref().strip_prefix("t/").unwrap().to_string())
            .collect();
        let building: Vec<String> = self
            .building
            .iter()
            .map(|(m, _)| m.layer.path.clone())
            .collect();
        let layers = self.layers();
        for p in orphans(&self.t, &listed, &building) {
            if layers.iter().any(|l| l.path == p) || building.contains(&p) {
                return Err(format!("{p} is live, and named an orphan"));
            }
            self.mem
                .delete(&Path::from(format!("t/{p}")))
                .await
                .unwrap();
        }
        Ok(())
    }

    /// The invariants.
    async fn check(&self) -> Res {
        let (head, layers) = (self.head(), self.layers());
        let want = &self.states[head as usize];
        let page = self
            .ix
            .scan(&layers, head, &Query::default())
            .await
            .map_err(|e| format!("scan at the head: {e}"))?;
        let got: St = (0..page.run.len())
            .map(|i| {
                let r = page.run.rows(i).start;
                (
                    page.run.key(i).to_vec(),
                    int(page.run.new_version(r)).unwrap(),
                )
            })
            .collect();
        if &got != want {
            return Err(format!(
                "the index at {head} holds {got:?}, the log {want:?}"
            ));
        }
        let mut marks: Vec<(u64, String)> = vec![];
        for (id, e) in &self.t.edges {
            let k = e.progress.keyed().unwrap();
            marks.extend(
                k.commits()
                    .map(|c| (c, format!("{}'s piece", String::from_utf8_lossy(id)))),
            );
        }
        marks.extend(
            self.runs
                .iter()
                .flatten()
                .map(|p| (p.commit, "a retained record".into())),
        );
        marks.extend(
            self.attempts
                .iter()
                .map(|a| (a.head, "a live attempt's head".into())),
        );
        for (id, k) in &self.t.per_key {
            if self
                .t
                .outputs
                .get(&output_id(FAILURES, id))
                .is_some_and(|o| o.live > 0)
            {
                marks.push((k.seen, format!("{}'s seen", String::from_utf8_lossy(id))));
            }
        }
        let cursor = self.t.retain.cursors.get(ITEMS).map(|c| c.at);
        marks.extend(cursor.map(|c| (c, "the cleanup cursor".into())));
        for (c, what) in marks {
            let page = self
                .ix
                .diff(&layers, c, head, &Query::default())
                .await
                .map_err(|e| format!("{what} at {c} cannot diff to {head}: {e}"))?;
            let was = &self.states[c as usize];
            let mut seen = BTreeSet::new();
            for i in 0..page.run.len() {
                let (k, r) = (page.run.key(i), page.run.rows(i).start);
                let (new, old) = (
                    int(page.run.new_version(r)),
                    int(page.run.replaced_version(r)),
                );
                if new != want.get(k).copied() || old != was.get(k).copied() {
                    return Err(format!(
                        "{what}: diff {c}..{head} of {k:?} is {old:?} -> {new:?}"
                    ));
                }
                seen.insert(k.to_vec());
            }
            let moved = was
                .keys()
                .chain(want.keys())
                .any(|k| was.get(k) != want.get(k) && !seen.contains(k));
            if moved {
                return Err(format!("{what}: diff {c}..{head} misses a key"));
            }
        }
        for a in &self.attempts {
            for (k, v) in &self.states[a.head as usize] {
                if a.range.contains(k) && self.deleted.contains(&(k.clone(), *v)) {
                    return Err(format!(
                        "a live attempt planned at {} loads {k:?}@{v}, which cleanup deleted",
                        a.head
                    ));
                }
            }
        }
        let owed: BTreeSet<(Vec<u8>, u64)> = self.replaced[1..=cursor.unwrap_or(0) as usize]
            .iter()
            .flatten()
            .cloned()
            .collect();
        if owed != self.deleted {
            return Err(format!(
                "cleanup at {cursor:?} deleted {:?}, the commits up to it replaced {owed:?}",
                self.deleted
            ));
        }
        Ok(())
    }

    async fn run(&mut self, steps: usize) -> Res {
        for _ in 0..steps {
            match self.rng.random_range(0..100) {
                0..30 => self.commit().await?,
                30..45 => self.plan(),
                45..60 => self.finish()?,
                60..65 => {
                    if !self.attempts.is_empty() {
                        let i = self.rng.random_range(0..self.attempts.len());
                        self.attempts.remove(i);
                    }
                }
                65..80 => self.merge().await?,
                80..87 => self.clean().await?,
                87..90 => self.pass(),
                90..93 => self.release(),
                93..96 => {
                    self.runs.pop_front();
                }
                _ => self.sweep().await?,
            }
            self.check().await?;
        }
        Ok(())
    }
}

fn seeds() -> u64 {
    std::env::var("RETAIN_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(150)
}

fn history(seed: u64, off: Off, rt: &tokio::runtime::Runtime) -> Res {
    OFF.with(|o| {
        o.set(match off {
            Off::Pieces => rules::PIECES,
            Off::Cursor => rules::CURSOR,
            Off::Seen => rules::SEEN,
            _ => 0,
        })
    });
    let mut m = Model::new(seed, off, rt);
    let got = rt.block_on(m.run(250));
    OFF.with(|o| o.set(0));
    got.map_err(|e| format!("seed {seed}: {e}"))
}

#[test]
fn random_histories_keep_what_is_read_and_delete_what_is_released() {
    let rt = runtime();
    for seed in 0..seeds() {
        if let Err(e) = history(seed, Off::None, &rt) {
            panic!("{e}");
        }
    }
}

#[test]
fn each_rule_switched_off_breaks_a_history() {
    let rt = runtime();
    for off in [
        Off::Pieces,
        Off::Cursor,
        Off::Seen,
        Off::Records,
        Off::CutAttempts,
        Off::CleanupAttempts,
    ] {
        let broke = (0..seeds()).find_map(|seed| history(seed, off, &rt).err());
        assert!(broke.is_some(), "{off:?} switched off breaks no history");
        eprintln!("{off:?}: {}", broke.unwrap());
    }
}

/// A model with a cleaned output, `n` upstream commits and nothing else.
fn commits(rt: &tokio::runtime::Runtime, n: usize) -> Model {
    let mut m = (0..)
        .map(|seed| Model::new(seed, Off::None, rt))
        .find(|m| m.t.retain.cursors.contains_key(ITEMS))
        .unwrap();
    while (m.head() as usize) < n {
        let before = m.head();
        rt.block_on(m.commit()).unwrap();
        if m.head() == before {
            continue;
        }
    }
    m
}

#[test]
fn parts_and_cursors_round_trip() {
    let rt = runtime();
    let m = commits(&rt, 3);
    let layer = m.layers()[1].clone();
    for p in [
        Part::Merged(Merged {
            output: items(),
            replaced: vec!["a".into(), "b".into()],
            layer,
        }),
        Part::Track(Track {
            output: items(),
            at: 4,
        }),
        cleaned(&m.t, 1, 3),
        Part::Retire(Retire { output: items() }),
    ] {
        let ev = Event::of(vec![p]);
        assert_eq!(Event::decode(&ev.encode()).unwrap(), ev);
    }
    let back = Tracking::restore(&m.t.snapshot()).unwrap();
    assert_eq!(back, m.t);
    assert_eq!(back.retain.cursors[ITEMS].at, 0);
}

#[test]
fn a_merge_is_refused_unless_it_replaces_adjacent_layers_folds_within_the_cut_and_writes_a_new_path(
) {
    let rt = runtime();
    let mut m = commits(&rt, 4);
    let layers = m.layers();
    let built = |m: &Model, run: std::ops::Range<usize>, cut: u64, path: &str| {
        let layer = rt
            .block_on(m.ix.merge(&layers, run.clone(), cut, path))
            .unwrap();
        Merged {
            output: items(),
            replaced: layers[run].iter().map(|l| l.path.clone()).collect(),
            layer,
        }
    };
    // the cursor at 0 holds the cut: no fold
    let fold = built(&m, 0..2, 2, "items/f.lay");
    let e =
        m.t.check_event(&Event::of(vec![Part::Merged(fold.clone())]));
    assert!(format!("{e:?}").contains("Cursor"), "{e:?}");
    m.apply(cleaned(&m.t, 0, 4)).unwrap();
    // a consumer's piece at 1 holds it now
    m.apply(Part::Batch(BatchProgress {
        edge: EDGES[0].into(),
        reset: false,
        range: KeyRange::all(),
        piece: Some(Value::At(1, m.cond.clone())),
        head: 1,
        points: vec![],
        expect: read_digest(&Default::default(), false, &KeyRange::all()),
        fence: None,
    }))
    .unwrap();
    let e = m.t.check_event(&Event::of(vec![Part::Merged(fold)]));
    assert!(format!("{e:?}").contains("Piece"), "{e:?}");
    let ok = built(&m, 0..2, 1, "items/g.lay");
    // the same path as a layer in the list
    let mut twice = ok.clone();
    twice.layer.path = layers[3].path.clone();
    assert!(m
        .t
        .check_event(&Event::of(vec![Part::Merged(twice)]))
        .is_err());
    // a merged layer that drops or invents a clear
    let mut clears = ok.clone();
    clears.layer.clears.push(ok.layer.hi);
    assert!(m
        .t
        .check_event(&Event::of(vec![Part::Merged(clears)]))
        .is_err());
    // replaced layers not adjacent
    let mut gap = ok.clone();
    gap.replaced = vec![layers[0].path.clone(), layers[2].path.clone()];
    assert!(m
        .t
        .check_event(&Event::of(vec![Part::Merged(gap)]))
        .is_err());
    m.apply(Part::Merged(ok.clone())).unwrap();
    assert_eq!(m.layers().len(), 3);
    assert_eq!(m.layers()[0].floor, Some(1));
    // the same merge again: its layers are gone
    assert!(m.apply(Part::Merged(ok)).is_err());
    // a merge past the base keeps no floor, and covers what it replaces
    let layers = m.layers();
    let tail = rt
        .block_on(m.ix.merge(&layers, 1..3, 4, "items/h.lay"))
        .unwrap();
    let mut short = Merged {
        output: items(),
        replaced: layers[1..3].iter().map(|l| l.path.clone()).collect(),
        layer: tail,
    };
    m.t.check_event(&Event::of(vec![Part::Merged(short.clone())]))
        .unwrap();
    short.replaced.pop();
    assert!(m.apply(Part::Merged(short)).is_err());
}

#[test]
fn a_merge_to_a_held_path_writes_nothing_and_a_merge_shares_no_event() {
    let rt = runtime();
    let mut m = commits(&rt, 4);
    m.apply(cleaned(&m.t, 0, 4)).unwrap();
    let layers = m.layers();
    let held = layers[0].path.clone();
    let bytes = |m: &Model| {
        rt.block_on(m.mem.get(&Path::from(format!("t/{held}"))))
            .unwrap()
    };
    let before = rt.block_on(bytes(&m).bytes()).unwrap();
    let e = rt
        .block_on(merge(&m.ix, ITEMS, &layers, 4, &held))
        .unwrap_err();
    assert!(e.to_string().contains("written twice"), "{e}");
    assert_eq!(rt.block_on(bytes(&m).bytes()).unwrap(), before);
    let ok = rt
        .block_on(merge(&m.ix, ITEMS, &layers, 4, "items/m.lay"))
        .unwrap()
        .unwrap();
    let edge = EDGES[0];
    let batch = Part::Batch(BatchProgress {
        edge: edge.into(),
        reset: false,
        range: KeyRange::all(),
        piece: Some(Value::At(2, m.cond.clone())),
        head: 2,
        points: vec![],
        expect: read_digest(&Default::default(), false, &KeyRange::all()),
        fence: None,
    });
    let e =
        m.t.check_event(&Event::of(vec![Part::Merged(ok.clone()), batch]))
            .unwrap_err();
    assert!(e.to_string().contains("an event of its own"), "{e}");
    m.apply(Part::Merged(ok)).unwrap();
}

#[test]
fn a_batch_whose_input_folded_past_its_head_is_refused() {
    let rt = runtime();
    let mut m = commits(&rt, 4);
    m.apply(cleaned(&m.t, 0, 4)).unwrap();
    // an attempt of a fresh consumer planned at 2, unpinned: a merge folds to 3
    let layers = m.layers();
    let layer = rt
        .block_on(m.ix.merge(&layers, 0..3, 3, "items/f.lay"))
        .unwrap();
    m.apply(Part::Merged(Merged {
        output: items(),
        replaced: layers[0..3].iter().map(|l| l.path.clone()).collect(),
        layer,
    }))
    .unwrap();
    let batch = |h, key_list: bool| {
        Part::Batch(BatchProgress {
            edge: EDGES[1].into(),
            reset: false,
            range: KeyRange::all(),
            piece: (!key_list).then(|| Value::At(h, Arc::new(Conditions::default()))),
            head: h,
            points: vec![],
            expect: read_digest(&Default::default(), false, &KeyRange::all()),
            fence: None,
        })
    };
    for key_list in [false, true] {
        let e =
            m.t.check_event(&Event::of(vec![batch(2, key_list)]))
                .unwrap_err();
        assert!(e.to_string().contains("folded past"), "{e}");
    }
    m.apply(batch(3, true)).unwrap();
    m.apply(batch(3, false)).unwrap();
}

#[test]
fn the_cursor_moves_only_from_where_it_is_and_within_the_head() {
    let rt = runtime();
    let mut m = commits(&rt, 3);
    let life = m.t.retain.cursors[ITEMS].life;
    let step = |from, to| {
        Part::Cleaned(Cleaned {
            output: items(),
            life,
            from,
            to,
        })
    };
    assert!(m.apply(step(1, 2)).is_err());
    assert!(m.apply(step(0, 4)).is_err());
    assert!(m.apply(step(0, 0)).is_err());
    let s = super::step(&m.t, ITEMS, &[]).unwrap();
    let e = rt
        .block_on(released(&m.ix, &m.layers(), &s, None, 0))
        .unwrap_err();
    assert!(e.to_string().contains("at least one key"), "{e}");
    m.apply(step(0, 2)).unwrap();
    assert!(m.apply(step(0, 2)).is_err(), "a step journaled twice");
    // a step stops at the oldest head a live attempt planned at
    let attempt = |c| Pin {
        output: items(),
        commit: c,
        by: Holder::Attempt(b"a".as_slice().into()),
    };
    let run = Pin {
        output: items(),
        commit: 0,
        by: Holder::Run(b"r".as_slice().into()),
    };
    assert_eq!(step_of(&m.t, &[attempt(2), run.clone()]), None);
    assert_eq!(step_of(&m.t, &[run]), Some((2, 3)));
    // an output not cleaned has no cursor, and one is tracked from its head only
    let other: Id = b"other".as_slice().into();
    assert_eq!(super::step(&m.t, &other, &[]), None);
    assert!(m
        .apply(Part::Track(Track {
            output: items(),
            at: 3
        }))
        .is_err());
    assert!(m
        .apply(Part::Track(Track {
            output: other.clone(),
            at: 1
        }))
        .is_err());
    m.apply(Part::Track(Track {
        output: other.clone(),
        at: 0,
    }))
    .unwrap();
    assert_eq!(cut(&m.t, &other, &[]).commit, 0);
}

fn cleaned(t: &Tracking, from: u64, to: u64) -> Part {
    Part::Cleaned(Cleaned {
        output: items(),
        life: t.retain.cursors.get(ITEMS).map_or(0, |c| c.life),
        from,
        to,
    })
}

fn step_of(t: &Tracking, pins: &[Pin]) -> Option<(u64, u64)> {
    step(t, ITEMS, pins).map(|s| (s.from, s.to))
}

#[test]
fn a_per_key_edge_with_failing_keys_holds_the_cut_at_seen() {
    let rt = runtime();
    let mut m = commits(&rt, 4);
    m.apply(cleaned(&m.t, 0, 4)).unwrap();
    let edge: Id = EDGES[2].into();
    m.t.per_key.insert(
        edge.clone(),
        PerKey {
            seen: 2,
            ..Default::default()
        },
    );
    assert_eq!(cut(&m.t, ITEMS, &[]).by, Holder::Head);
    m.t.outputs.insert(
        output_id(FAILURES, &edge),
        Output {
            live: 1,
            head: 1,
            ..Default::default()
        },
    );
    assert_eq!(
        cut(&m.t, ITEMS, &[]),
        Cut {
            commit: 2,
            by: Holder::Seen(edge)
        }
    );
}

#[test]
fn the_alarm_names_what_holds_the_cut() {
    let rt = runtime();
    let mut m = commits(&rt, 12);
    let head = m.head();
    m.apply(cleaned(&m.t, 0, head)).unwrap();
    let run = Pin {
        output: items(),
        commit: 1,
        by: Holder::Run(b"monthly".as_slice().into()),
    };
    let cuts = cuts(&m.t, std::slice::from_ref(&run));
    assert_eq!(cuts[ITEMS].by, run.by);
    let o = &m.t.outputs[ITEMS];
    let above = rules::above(o, 1);
    assert_eq!(above, o.layers[1..].iter().map(|l| l.entries).sum::<u64>());
    let multiple = above / o.live.max(1);
    let got = alarms(&m.t, cuts.clone(), multiple.saturating_sub(1));
    assert_eq!(got.len(), 1);
    assert_eq!((got[0].above, &got[0].cut.by), (above, &run.by));
    assert!(alarms(&m.t, cuts, multiple + 1).is_empty());
    assert!(
        alarms(&m.t, super::cuts(&m.t, &[]), 0).is_empty(),
        "nothing is above the head"
    );
}

#[test]
fn record_pins_hold_the_oldest_replaced_piece_and_the_own_head() {
    let cond = Arc::new(Conditions::default());
    let piece = |c| crate::progress::Piece {
        range: KeyRange::all(),
        value: Value::At(c, cond.clone()),
    };
    let rec = Record {
        producer: vec![],
        edge: b"e".as_slice().into(),
        output: items(),
        mode: 0,
        reset: false,
        range: KeyRange::all(),
        head: 9,
        conditions: cond.clone(),
        replaced: Slice {
            pieces: vec![piece(5), piece(3)],
            points: vec![],
        },
        marks: vec![],
        counts: [0; 4],
        digest: 0,
        planner: 0,
        own_head: Some(4),
        output_commit: None,
        keys: None,
        waiting: vec![],
    };
    let by = Holder::Run(b"r".as_slice().into());
    assert_eq!(
        record_pins(&rec, b"own", b"r"),
        vec![
            Pin {
                output: items(),
                commit: 3,
                by: by.clone()
            },
            Pin {
                output: b"own".as_slice().into(),
                commit: 4,
                by
            }
        ]
    );
}

#[test]
fn orphans_are_the_listed_files_no_layer_and_no_build_names() {
    let rt = runtime();
    let m = commits(&rt, 2);
    let layers: Vec<String> = m.layers().iter().map(|l| l.path.clone()).collect();
    let listed: Vec<String> = layers
        .iter()
        .cloned()
        .chain(["items/crashed.lay".into(), "items/merging.lay".into()])
        .collect();
    assert_eq!(
        orphans(&m.t, &listed, &["items/merging.lay".into()]),
        vec!["items/crashed.lay".to_string()]
    );
    // a path is live whatever output id holds it (B6 renames move layers)
    let mut moved = m.t.clone();
    let o = moved.outputs.remove(ITEMS).unwrap();
    moved.outputs.insert(b"renamed".as_slice().into(), o);
    assert_eq!(orphans(&moved, &layers, &[]), Vec::<String>::new());
    moved.outputs.clear();
    assert_eq!(orphans(&moved, &layers, &[]), layers);
}

#[test]
fn an_output_retires_once_no_edge_reads_it() {
    let rt = runtime();
    let mut m = commits(&rt, 2);
    let retire = || Part::Retire(Retire { output: items() });
    assert!(m.apply(retire()).is_err());
    for e in EDGES {
        m.apply(Part::EdgeClose(crate::progress::EdgeClose {
            edge: e.into(),
        }))
        .unwrap();
    }
    m.apply(retire()).unwrap();
    assert!(!m.t.outputs.contains_key(ITEMS));
    assert!(m.t.retain.cursors.is_empty());
    assert!(m.apply(retire()).is_err());
    // a per-key edge's records go with the edge, not alone
    let edge: Id = EDGES[2].into();
    let records = output_id(DUE, &edge);
    m.t.per_key.insert(edge.clone(), PerKey::default());
    m.t.outputs.insert(
        records.clone(),
        Output {
            head: 1,
            ..Default::default()
        },
    );
    let e = m
        .apply(Part::Retire(Retire {
            output: records.clone(),
        }))
        .unwrap_err();
    assert!(e.to_string().contains("goes with it"), "{e}");
    m.t.per_key.remove(&edge);
    m.apply(Part::Retire(Retire { output: records })).unwrap();
}

#[test]
fn a_step_or_merge_of_an_earlier_life_is_refused_in_the_next() {
    let rt = runtime();
    let mut m = commits(&rt, 4);
    let stale = super::step(&m.t, ITEMS, &[]).unwrap();
    let layers = m.layers();
    let layer = rt
        .block_on(m.ix.merge(&layers, 1..3, 0, "items/old.lay"))
        .unwrap();
    let old_merge = Merged {
        output: items(),
        replaced: layers[1..3].iter().map(|l| l.path.clone()).collect(),
        layer,
    };
    for e in EDGES {
        m.apply(Part::EdgeClose(EdgeClose { edge: e.into() }))
            .unwrap();
    }
    m.apply(Part::Retire(Retire { output: items() })).unwrap();
    m.apply(Part::Track(Track {
        output: items(),
        at: 0,
    }))
    .unwrap();
    (m.states, m.replaced) = (vec![St::new()], vec![vec![]]);
    while m.head() < 4 {
        rt.block_on(m.commit()).unwrap();
    }
    let fresh = super::step(&m.t, ITEMS, &[]).unwrap();
    assert_eq!((fresh.from, fresh.to), (stale.from, stale.to));
    assert_ne!(fresh.life, stale.life);
    let e = m.apply(Part::Cleaned(stale)).unwrap_err();
    assert!(e.to_string().contains("in life"), "{e}");
    let e = m.apply(Part::Merged(old_merge)).unwrap_err();
    assert!(e.to_string().contains("no longer adjacent"), "{e}");
    m.apply(Part::Cleaned(fresh)).unwrap();
}

#[test]
fn cleanup_refuses_a_replaced_version_that_is_not_a_generation() {
    let rt = runtime();
    let mut m = commits(&rt, 0);
    for (c, v) in [(1u64, &b"v1"[..]), (2, b"v2"), (3, b"v1")] {
        let changes = Changes::of(&[(&b"k"[..], Change::Upsert(Some(v)))]).unwrap();
        let out = m.t.outputs.get(ITEMS).cloned().unwrap_or_default();
        let path = format!("items/b{c}.lay");
        let spec = Spec {
            version: c,
            time_ms: 0,
            producer: b"p",
            path: &path,
            mode: Mode::Patch,
            ends_full_run: false,
            collect: 0,
        };
        let b = rt
            .block_on(build(
                &OneIndex(&m.ix),
                &items(),
                &out,
                spec,
                &mut Held(Some(&changes)),
            ))
            .unwrap()
            .unwrap();
        m.apply(Part::OutputCommit(b.part)).unwrap();
    }
    let s = super::step(&m.t, ITEMS, &[]).unwrap();
    let e = rt
        .block_on(released(&m.ix, &m.layers(), &s, None, 10))
        .unwrap_err();
    assert!(e.to_string().contains("not a generation"), "{e}");
}

#[test]
fn an_edge_moves_to_an_input_only_above_its_floor() {
    let rt = runtime();
    let mut m = commits(&rt, 4);
    m.apply(cleaned(&m.t, 0, 4)).unwrap();
    let layers = m.layers();
    let layer = rt
        .block_on(m.ix.merge(&layers, 0..3, 3, "items/f.lay"))
        .unwrap();
    m.apply(Part::Merged(Merged {
        output: items(),
        replaced: layers[0..3].iter().map(|l| l.path.clone()).collect(),
        layer,
    }))
    .unwrap();
    let other: Id = b"other".as_slice().into();
    m.t.outputs.insert(
        other.clone(),
        Output {
            head: 1,
            ..Default::default()
        },
    );
    let edge: Id = b"moved".as_slice().into();
    m.apply(Part::EdgeOpen(EdgeOpen {
        edge: edge.clone(),
        input: other.clone(),
        keyed: true,
    }))
    .unwrap();
    m.apply(Part::Batch(BatchProgress {
        edge: edge.clone(),
        reset: false,
        range: KeyRange::all(),
        piece: Some(Value::At(1, m.cond.clone())),
        head: 1,
        points: vec![],
        expect: read_digest(&Default::default(), false, &KeyRange::all()),
        fence: None,
    }))
    .unwrap();
    let rename = |input: &Id| {
        Part::EdgeRename(EdgeRename {
            from: edge.clone(),
            to: edge.clone(),
            input: input.clone(),
        })
    };
    let e =
        m.t.check_event(&Event::of(vec![rename(&items())]))
            .unwrap_err();
    assert!(e.to_string().contains("below its new input's floor"), "{e}");
    m.apply(rename(&other)).unwrap();
    // a per-key edge whose keys fail holds its seen, with no piece at all
    let failing: Id = b"failing".as_slice().into();
    m.apply(Part::EdgeOpen(EdgeOpen {
        edge: failing.clone(),
        input: other.clone(),
        keyed: true,
    }))
    .unwrap();
    m.t.per_key.insert(
        failing.clone(),
        PerKey {
            seen: 1,
            ..Default::default()
        },
    );
    m.t.outputs.insert(
        output_id(FAILURES, &failing),
        Output {
            live: 1,
            head: 1,
            ..Default::default()
        },
    );
    let e = m
        .apply(Part::EdgeRename(EdgeRename {
            from: failing.clone(),
            to: failing.clone(),
            input: items(),
        }))
        .unwrap_err();
    assert!(e.to_string().contains("Seen"), "{e}");
}
