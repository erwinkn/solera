//! The queries, at any commit at or above the floor: `diff`, `scan`, `get`,
//! `history`; writing a commit; and merging layers.

use crate::error::{invalid, Error, Result};
use crate::format::{lossy, parse_commits, CommitHeader, LayerRef, WriteOptions, Writer};
use crate::glob::Pattern;
use crate::read::{Bounds, KeyList, Walk};
use crate::run::{Run, Version};
use crate::store::Store;
use std::ops::{Bound, Range, RangeInclusive};
use std::sync::Arc;

/// What part of the key space a query reads, and how much of it it returns.
#[derive(Clone, Debug, Default)]
pub struct Query {
    /// Keys in `(lo, hi]`; `None` is unbounded.
    pub lo: Option<Vec<u8>>,
    pub hi: Option<Vec<u8>>,
    /// Only keys after this one (a page's `next`).
    pub after: Option<Vec<u8>>,
    /// At most this many keys.
    pub limit: Option<u64>,
    pub pattern: Option<Pattern>,
}

impl Query {
    fn bounds(&self) -> Bounds {
        let mut b = Bounds::all();
        for k in [&self.lo, &self.after].into_iter().flatten() {
            b.raise(Bound::Excluded(k.clone()));
        }
        if let Some(hi) = &self.hi {
            b.lower(Bound::Included(hi.clone()));
        }
        if let Some(p) = &self.pattern {
            let (lo, hi) = p.bounds();
            b.raise(lo);
            b.lower(hi);
        }
        b
    }
}

/// A query's result: one key per group, in key order, and where the next page
/// starts if the limit stopped it.
///
/// - `diff`: one entry per key: the newest commit in the window, `new` the
///   version at C2, `replaced` the version at C1.
/// - `scan`, `get`: one entry per key present at C: the entry that set its
///   version.
/// - `history`: every retained entry, newest first. A base's entries at or
///   below its floor are the state there (`replaced` absent), not changes.
#[derive(Debug, Default)]
pub struct Page {
    pub run: Run,
    pub next: Option<Vec<u8>>,
}

/// Checks a layer list: oldest first, contiguous commits from commit 1, a base
/// only first. Returns the commits it answers at: `floor..=head`.
pub fn check_layers(layers: &[LayerRef]) -> Result<(u64, u64)> {
    let Some(first) = layers.first() else {
        return Ok((0, 0));
    };
    if first.lo != 1 {
        return invalid(format!(
            "the oldest layer starts at commit {}, not 1",
            first.lo
        ));
    }
    for (i, l) in layers.iter().enumerate() {
        if l.lo > l.hi || (i > 0 && l.lo != layers[i - 1].hi + 1) {
            return invalid(format!(
                "layers are oldest first with contiguous commits: {} covers {}..={} after {}",
                l.path,
                l.lo,
                l.hi,
                if i > 0 { layers[i - 1].hi } else { 0 }
            ));
        }
        if i > 0 && l.floor.is_some() {
            return invalid(format!("{}: only the oldest layer may be a base", l.path));
        }
        l.check()?;
    }
    Ok((first.floor.unwrap_or(0), layers.last().unwrap().hi))
}

fn at(c: u64, (floor, head): (u64, u64), what: &str) -> Result<()> {
    if c < floor || c > head {
        return invalid(format!(
            "{what} {c} is outside the commits the index answers at, {floor}..={head}"
        ));
    }
    Ok(())
}

/// The newest-first layers a read at commits `(c1, c2]` needs.
fn pick<'a>(layers: &'a [LayerRef], c1: u64, c2: u64, b: &Bounds) -> Vec<&'a LayerRef> {
    layers
        .iter()
        .rev()
        .filter(|l| l.hi > c1 && l.lo <= c2 && b.overlaps(l))
        .collect()
}

/// The change index over one object store: queries, commits and merges. The
/// store's cache is shared by every call.
pub struct ChangeIndex {
    pub store: Arc<Store>,
    pub write: WriteOptions,
}

impl ChangeIndex {
    pub fn new(store: Arc<Store>, write: WriteOptions) -> ChangeIndex {
        ChangeIndex { store, write }
    }

    async fn walk(
        &self,
        layers: Vec<&LayerRef>,
        q: &Query,
        keys: Option<KeyList>,
        all_at_once: bool,
    ) -> Result<Option<Walk>> {
        let b = q.bounds();
        if b.is_empty() || q.limit == Some(0) || keys.as_ref().is_some_and(|k| k.is_empty()) {
            return Ok(None);
        }
        Ok(Some(
            Walk::open(
                &self.store,
                &layers,
                b,
                keys,
                q.pattern.clone(),
                all_at_once,
            )
            .await?,
        ))
    }

    /// Keys whose state differs between C1 and C2: `on(key, newest commit
    /// in the window, version at C2, version at C1)` for each, in key order.
    /// Returns where the next page starts if the limit stopped it.
    pub async fn diff_with(
        &self,
        layers: &[LayerRef],
        c1: u64,
        c2: u64,
        q: &Query,
        mut on: impl FnMut(&[u8], u64, Version<'_>, Version<'_>),
    ) -> Result<Option<Vec<u8>>> {
        let range = check_layers(layers)?;
        at(c1, range, "C1")?;
        at(c2, range, "C2")?;
        if c1 > c2 {
            return invalid(format!("diff from {c1} to {c2}: C1 is after C2"));
        }
        let ls = pick(layers, c1, c2, &q.bounds());
        let Some(mut w) = self.walk(ls, q, None, false).await? else {
            return Ok(None);
        };
        let mut g = Run::new();
        let limit = q.limit.unwrap_or(u64::MAX);
        let mut n = 0;
        while w.next(&mut g).await? {
            let rows = g.rows(0);
            let mut window = rows.filter(|&r| g.commit(r) > c1 && g.commit(r) <= c2);
            let Some(newest) = window.next() else {
                continue;
            };
            let oldest = window.last().unwrap_or(newest);
            let (before, after) = (g.replaced_version(oldest), g.new_version(newest));
            if before == after {
                continue;
            }
            on(g.key(0), g.commit(newest), after, before);
            n += 1;
            if n == limit {
                return Ok(Some(g.key(0).to_vec()));
            }
        }
        Ok(None)
    }

    /// `diff_with` into a page: one entry per key, `new` the version at C2,
    /// `replaced` the version at C1.
    pub async fn diff(&self, layers: &[LayerRef], c1: u64, c2: u64, q: &Query) -> Result<Page> {
        let mut run = Run::new();
        let next = self
            .diff_with(layers, c1, c2, q, |k, c, new, replaced| {
                run.push_key(k);
                run.push_entry(c, new, replaced);
            })
            .await?;
        Ok(Page { run, next })
    }

    /// Keys present at C, each with the entry that set its version:
    /// `on(key, commit, version, replaced)`. With `keys`, only those.
    pub async fn state_with(
        &self,
        layers: &[LayerRef],
        c: u64,
        keys: Option<KeyList>,
        q: &Query,
        mut on: impl FnMut(&[u8], u64, Version<'_>, Version<'_>),
    ) -> Result<Option<Vec<u8>>> {
        let range = check_layers(layers)?;
        at(c, range, "commit")?;
        let ls = pick(layers, 0, c, &q.bounds());
        let all_at_once = keys.is_some();
        let Some(mut w) = self.walk(ls, q, keys, all_at_once).await? else {
            return Ok(None);
        };
        let mut g = Run::new();
        let limit = q.limit.unwrap_or(u64::MAX);
        let mut n = 0;
        while w.next(&mut g).await? {
            let Some(r) = g.rows(0).find(|&r| g.commit(r) <= c) else {
                continue;
            };
            let v = g.new_version(r);
            if v.is_absent() {
                continue;
            }
            on(g.key(0), g.commit(r), v, g.replaced_version(r));
            n += 1;
            if n == limit {
                return Ok(Some(g.key(0).to_vec()));
            }
        }
        Ok(None)
    }

    async fn state_page(
        &self,
        layers: &[LayerRef],
        c: u64,
        keys: Option<KeyList>,
        q: &Query,
    ) -> Result<Page> {
        let mut run = Run::new();
        let next = self
            .state_with(layers, c, keys, q, |k, c, new, replaced| {
                run.push_key(k);
                run.push_entry(c, new, replaced);
            })
            .await?;
        Ok(Page { run, next })
    }

    /// Every key present at C, with its version.
    pub async fn scan(&self, layers: &[LayerRef], c: u64, q: &Query) -> Result<Page> {
        self.state_page(layers, c, None, q).await
    }

    /// The listed keys' state at C (those present).
    pub async fn get<K: AsRef<[u8]>>(
        &self,
        layers: &[LayerRef],
        c: u64,
        keys: &[K],
        q: &Query,
    ) -> Result<Page> {
        self.state_page(layers, c, Some(KeyList::new(keys)), q)
            .await
    }

    /// Every retained entry of each key, newest first: `on(group)` with one
    /// key and its entries.
    pub async fn history_with(
        &self,
        layers: &[LayerRef],
        q: &Query,
        on: impl FnMut(&Run),
    ) -> Result<Option<Vec<u8>>> {
        self.window_with(layers, 0..=u64::MAX, q, on).await
    }

    /// The entries of the commits in `commits`, key by key, newest first,
    /// reading only the layers that overlap them; keys without one are
    /// skipped. (A cleanup cursor stepping over commits reads each one's
    /// replaced versions this way.)
    pub async fn window_with(
        &self,
        layers: &[LayerRef],
        commits: RangeInclusive<u64>,
        q: &Query,
        mut on: impl FnMut(&Run),
    ) -> Result<Option<Vec<u8>>> {
        check_layers(layers)?;
        let (lo, hi) = (*commits.start(), *commits.end());
        if lo > hi {
            return Ok(None);
        }
        let ls = pick(layers, lo.saturating_sub(1), hi, &q.bounds());
        let Some(mut w) = self.walk(ls, q, None, false).await? else {
            return Ok(None);
        };
        let all = lo == 0 && hi == u64::MAX;
        let (mut g, mut f) = (Run::new(), Run::new());
        let limit = q.limit.unwrap_or(u64::MAX);
        let mut n = 0;
        while w.next(&mut g).await? {
            if all {
                on(&g);
            } else {
                f.clear();
                for r in g.rows(0).filter(|&r| commits.contains(&g.commit(r))) {
                    if f.is_empty() {
                        f.push_key(g.key(0));
                    }
                    f.push_entry_from(&g, r);
                }
                if f.is_empty() {
                    continue;
                }
                on(&f);
            }
            n += 1;
            if n == limit {
                return Ok(Some(g.key(0).to_vec()));
            }
        }
        Ok(None)
    }

    /// The entries of the commits in `commits`, as `window_with`.
    pub async fn window(
        &self,
        layers: &[LayerRef],
        commits: RangeInclusive<u64>,
        q: &Query,
    ) -> Result<Page> {
        let mut run = Run::new();
        let next = self
            .window_with(layers, commits, q, |g| run.push_key_from(g, 0))
            .await?;
        Ok(Page { run, next })
    }

    /// Every retained entry of each key, newest first.
    pub async fn history(&self, layers: &[LayerRef], q: &Query) -> Result<Page> {
        let mut run = Run::new();
        let next = self
            .history_with(layers, q, |g| run.push_key_from(g, 0))
            .await?;
        Ok(Page { run, next })
    }

    /// Every entry of one layer, key by key, read as a query reads it (for
    /// verifying a layer; the list's rules do not apply).
    pub async fn entries(&self, layer: &LayerRef) -> Result<Entries> {
        Ok(Entries(
            Walk::open(&self.store, &[layer], Bounds::all(), None, None, false).await?,
        ))
    }

    /// The headers of the commits `lo..=hi` the layers hold.
    pub async fn commits(
        &self,
        layers: &[LayerRef],
        lo: u64,
        hi: u64,
    ) -> Result<Vec<CommitHeader>> {
        check_layers(layers)?;
        let mut out = Vec::new();
        for l in layers.iter().filter(|l| l.hi >= lo && l.lo <= hi) {
            out.extend(
                self.read_commits(l)
                    .await?
                    .into_iter()
                    .filter(|h| h.commit >= lo && h.commit <= hi),
            );
        }
        Ok(out)
    }

    async fn read_commits(&self, l: &LayerRef) -> Result<Vec<CommitHeader>> {
        let top = self.store.top(l).await?;
        let b = self
            .store
            .get_range(&l.path, top.commits_start..top.index_start)
            .await?;
        parse_commits(&b).map_err(|e| match e {
            Error::Format(m) => Error::Format(format!("{}: {m}", l.path)),
            e => e,
        })
    }

    /// Write one commit's layer at `path`: its entries, one per changed key
    /// (their commit numbers are ignored). Returns the layer's reference.
    pub async fn write_commit(
        &self,
        path: &str,
        commit: NewCommit<'_>,
        entries: &Run,
    ) -> Result<LayerRef> {
        let mut h = CommitHeader {
            commit: commit.commit,
            time_ms: commit.time_ms,
            producer: commit.producer.to_vec(),
            ..Default::default()
        };
        let mut w = Writer::new(self.write, commit.commit, commit.commit, None)?;
        for i in 0..entries.len() {
            let rows = entries.rows(i);
            if rows.len() != 1 {
                return invalid(format!(
                    "{:?}: a commit holds one entry per key",
                    lossy(entries.key(i))
                ));
            }
            let (new, replaced) = (
                entries.new_version(rows.start),
                entries.replaced_version(rows.start),
            );
            w.push(entries.key(i), commit.commit, new, replaced)?;
            match (new.is_absent(), replaced.is_absent()) {
                (false, true) => h.added += 1,
                (false, false) => h.updated += 1,
                _ => h.removed += 1,
            }
        }
        h.live = (commit.live_before + h.added)
            .checked_sub(h.removed)
            .ok_or_else(|| {
                Error::Invalid(format!(
                    "commit {} removes more keys than were live",
                    commit.commit
                ))
            })?;
        if !entries.is_empty() {
            h.min_key = entries.key(0).to_vec();
            h.max_key = entries.key(entries.len() - 1).to_vec();
        }
        let mut l = w.finish(&[h])?;
        let mut up = self.store.upload(path)?;
        up.write(w.take()).await?;
        up.finish().await?;
        l.path = path.to_string();
        Ok(l)
    }

    /// Merge the adjacent layers `run` of the list into one at `path`. Every
    /// entry above the cut is kept. A merge that starts at the oldest layer
    /// folds the entries at or below the cut into a base: one entry per key
    /// present there. Fails if entries do not chain.
    pub async fn merge(
        &self,
        layers: &[LayerRef],
        run: Range<usize>,
        cut: u64,
        path: &str,
    ) -> Result<LayerRef> {
        check_layers(layers)?;
        if run.is_empty() || run.end > layers.len() {
            return invalid(format!("merge of layers {run:?} of {}", layers.len()));
        }
        let ins = &layers[run.clone()];
        let (lo, hi) = (ins[0].lo, ins[ins.len() - 1].hi);
        let from_start = run.start == 0;
        let floor = if from_start {
            Some(ins[0].floor.unwrap_or(0).max(cut.min(hi))).filter(|&f| f > 0)
        } else {
            None
        };
        let fold = floor.unwrap_or(0);
        let newest_first: Vec<&LayerRef> = ins.iter().rev().collect();
        let (walk, commits) = futures::future::join(
            Walk::open(&self.store, &newest_first, Bounds::all(), None, None, false),
            futures::future::try_join_all(ins.iter().map(|l| self.read_commits(l))),
        )
        .await;
        let mut walk = walk?;
        let headers: Vec<CommitHeader> = commits?
            .into_iter()
            .flatten()
            .filter(|h| h.commit > fold)
            .collect();
        let mut w = Writer::new(self.write, lo, hi, floor)?;
        let mut up = self.store.upload(path)?;
        let mut g = Run::new();
        while walk.next(&mut g).await? {
            let rows = g.rows(0);
            for r in rows.start..rows.end - 1 {
                if g.replaced_version(r) != g.new_version(r + 1) {
                    return Err(Error::Chain(format!(
                        "{:?}: commit {} replaced {:?}, but commit {} left {:?}",
                        lossy(g.key(0)),
                        g.commit(r),
                        g.replaced_version(r),
                        g.commit(r + 1),
                        g.new_version(r + 1)
                    )));
                }
            }
            let oldest = rows.end - 1;
            if from_start && !g.replaced_version(oldest).is_absent() {
                return Err(Error::Chain(format!(
                    "{:?}: its first entry, commit {}, replaced {:?}, but nothing was there",
                    lossy(g.key(0)),
                    g.commit(oldest),
                    g.replaced_version(oldest)
                )));
            }
            for r in rows.clone() {
                if g.commit(r) <= fold {
                    let v = g.new_version(r);
                    if !v.is_absent() {
                        w.push(g.key(0), g.commit(r), v, Version::Absent)?;
                    }
                    break;
                }
                w.push(
                    g.key(0),
                    g.commit(r),
                    g.new_version(r),
                    g.replaced_version(r),
                )?;
            }
            if w.pending() >= self.store.opts.part_bytes {
                up.write(w.take()).await?;
            }
        }
        let mut out = w.finish(&headers)?;
        up.write(w.take()).await?;
        up.finish().await?;
        out.path = path.to_string();
        Ok(out)
    }
}

/// A layer's entries, one key at a time.
pub struct Entries(Walk);

impl Entries {
    /// The next key and its entries, newest first, into `group`; false at the end.
    pub async fn next(&mut self, group: &mut Run) -> Result<bool> {
        self.0.next(group).await
    }
}

/// A commit to write: its number, when, who made it (opaque to the index), and
/// the live key count before it.
#[derive(Clone, Copy, Debug)]
pub struct NewCommit<'a> {
    pub commit: u64,
    pub time_ms: u64,
    pub producer: &'a [u8],
    pub live_before: u64,
}

const MIB: u64 = 1 << 20;

/// The next merge the size rule asks for, if any: four adjacent layers, newest
/// first, whose sizes are within 30× of each other and whose sum stays within
/// 4× the bytes of all newer layers plus 1 MiB; past 32 layers, the newest
/// adjacent pair that fits; then, once the layers at or below the cut reach a
/// quarter of the base, a fold of them into it (the run starts at 0).
pub fn pick_merge(layers: &[LayerRef], cut: u64) -> Option<Range<usize>> {
    let b = usize::from(layers.first().is_some_and(|l| l.floor.is_some()));
    let n = layers.len() - b;
    let sizes: Vec<u64> = layers.iter().map(|l| l.size).collect();
    let fits = |a: usize, e: usize| {
        let size: u64 = sizes[a..e].iter().sum();
        let newer: u64 = sizes[e..].iter().sum();
        size <= 4 * newer + MIB
    };
    if n >= 4 {
        for a in (b..=layers.len() - 4).rev() {
            let s = &sizes[a..a + 4];
            let (max, min) = (*s.iter().max().unwrap(), *s.iter().min().unwrap());
            if max <= 30 * min.max(1) && fits(a, a + 4) {
                return Some(a..a + 4);
            }
        }
    }
    if n > 32 {
        if let Some(a) = (b..layers.len() - 1).rev().find(|&a| fits(a, a + 2)) {
            return Some(a..a + 2);
        }
    }
    let below = layers[b..].iter().take_while(|l| l.hi <= cut).count();
    if below > 0 {
        let size: u64 = sizes[b..b + below].iter().sum();
        let base = if b == 1 { sizes[0] } else { 0 };
        if size * 4 >= base {
            return Some(0..b + below);
        }
    }
    None
}
