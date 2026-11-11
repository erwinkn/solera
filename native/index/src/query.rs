//! The queries, at any commit at or above the floor: `diff`, `scan`, `get`,
//! `history`; writing a commit; and merging layers.

use crate::error::{invalid, Error, Result};
use crate::format::{lossy, parse_commits, CommitHeader, LayerRef, WriteOptions, Writer, CLEAR};
use crate::glob::Pattern;
use crate::read::{Bounds, KeyList, Walk};
use crate::run::{Run, Version};
use crate::store::{Store, Upload};
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
/// - `diff`: one entry per key: the newest commit in the window that wrote
///   or removed it, `new` the version at C2, `replaced` the version at C1.
///   A key written again with the version it had still counts that write.
/// - `scan`, `get`: one entry per key present at C: the entry that set its
///   version.
/// - `history`: every retained entry, newest first. A base's entries at or
///   below its floor are the state there (`replaced` absent), not changes.
///   At a commit that clears, the entries are the changes it made (see
///   `window_with`).
///
/// The state at C is what was written from the newest commit at or below C
/// that clears.
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

/// The newest commit at or below `c` that clears, or 0: the state at `c` is
/// what was written from it on.
fn clear_at(layers: &[LayerRef], c: u64) -> u64 {
    layers
        .iter()
        .rev()
        .flat_map(|l| l.clears.iter().rev())
        .find(|&&k| k <= c)
        .copied()
        .unwrap_or(0)
}

/// A change of one key: its commit, the row of its new version (none: removed
/// by a clear) and the row of the version it replaced.
type Change = (u64, Option<usize>, Option<usize>);

fn version(g: &Run, row: Option<usize>) -> Version<'_> {
    row.map_or(Version::Absent, |r| g.new_version(r))
}

/// The changes of `g`'s key in `commits`, oldest first, replayed from `start`
/// (the newest clear before them, or 0) over the commits in them that clear
/// (`clears`, ascending). At a clear, a key present before and not written
/// there is removed, and a key written there replaced what was present (and
/// did not change if it wrote that again).
fn replay(
    g: &Run,
    start: u64,
    commits: &RangeInclusive<u64>,
    clears: &[u64],
    changes: &mut Vec<Change>,
) {
    changes.clear();
    // the row of the version present, and the next clear
    let (mut cur, mut next): (Option<usize>, usize) = (None, 0);
    for r in g.rows(0).rev() {
        let c = g.commit(r);
        if c < start || c > *commits.end() {
            continue;
        }
        while next < clears.len() && clears[next] < c {
            if let Some(p) = cur.take() {
                changes.push((clears[next], None, Some(p)));
            }
            next += 1;
        }
        let at_clear = next < clears.len() && clears[next] == c;
        next += at_clear as usize;
        if commits.contains(&c) && !(at_clear && version(g, cur) == g.new_version(r)) {
            changes.push((c, Some(r), cur));
        }
        cur = Some(r).filter(|&r| !g.new_version(r).is_absent());
    }
    for &k in &clears[next..] {
        if let Some(p) = cur.take() {
            changes.push((k, None, Some(p)));
        }
    }
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

    /// Walks the layers a read at commits `(c1, c2]` needs, newest first:
    /// those holding any of these commits that `q`'s key bounds reach and,
    /// for a read of listed keys, whose keys span one of them.
    /// Clears decide what a read reads, so a layer of these commits that
    /// lists any is checked against its file even when the key bounds leave
    /// it out. (A layer that lists none is trusted until it is read, as its
    /// key bounds are.)
    async fn walk(
        &self,
        layers: &[LayerRef],
        c1: u64,
        c2: u64,
        q: &Query,
        keys: Option<KeyList>,
        all_at_once: bool,
    ) -> Result<Option<Walk>> {
        let b = q.bounds();
        if b.is_empty() || q.limit == Some(0) || keys.as_ref().is_some_and(|k| k.is_empty()) {
            return Ok(None);
        }
        let mut ls = Vec::new();
        for l in layers.iter().rev().filter(|l| l.hi > c1 && l.lo <= c2) {
            let listed = keys
                .as_ref()
                .is_none_or(|k| k.any_within(&l.first_key, &l.last_key));
            if b.overlaps(l) && listed {
                ls.push(l);
            } else if !l.clears.is_empty() {
                self.store.top(l).await?;
            }
        }
        Ok(Some(
            Walk::open(&self.store, &ls, b, keys, q.pattern.clone(), all_at_once).await?,
        ))
    }

    /// Keys whose state differs between C1 and C2: `on(key, newest commit
    /// in the window that wrote or removed it, version at C2, version at
    /// C1)` for each, in key order. A key a clear removed changed at that
    /// clear, and a key a later clear wrote again with the same version
    /// reports that clear, as `scan` would.
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
        if c1 == c2 {
            return Ok(None);
        }
        // A clear in the window: the states at C1 and at C2 are compared, which
        // reads the whole state at C1.
        let (k1, k2) = (clear_at(layers, c1), clear_at(layers, c2));
        let across = k2 > c1;
        let clears: Vec<u64> = layers
            .iter()
            .flat_map(|l| l.clears.iter().copied())
            .filter(|&k| k > c1 && k <= c2)
            .collect();
        let mut changes: Vec<Change> = Vec::new();
        let from = if across { k1.saturating_sub(1) } else { c1 };
        let Some(mut w) = self.walk(layers, from, c2, q, None, false).await? else {
            return Ok(None);
        };
        let mut g = Run::new();
        let limit = q.limit.unwrap_or(u64::MAX);
        let mut n = 0;
        while w.next(&mut g).await? {
            let (commit, before, after) = if across {
                replay(&g, k1, &(c1 + 1..=c2), &clears, &mut changes);
                let (Some(&(c, new, _)), Some(&(.., was))) = (changes.last(), changes.first())
                else {
                    continue;
                };
                // a key written again at a clear changed there too
                let written = g.rows(0).map(|r| g.commit(r)).find(|&w| w <= c2);
                (
                    c.max(written.unwrap_or(0)),
                    version(&g, was),
                    version(&g, new),
                )
            } else {
                let mut window = g.rows(0).filter(|&r| g.commit(r) > c1 && g.commit(r) <= c2);
                let Some(newest) = window.next() else {
                    continue;
                };
                let oldest = window.next_back().unwrap_or(newest);
                (
                    g.commit(newest),
                    g.replaced_version(oldest),
                    g.new_version(newest),
                )
            };
            if before == after {
                continue;
            }
            on(g.key(0), commit, after, before);
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
        let k = clear_at(layers, c);
        let all_at_once = keys.is_some();
        let from = k.saturating_sub(1);
        let Some(mut w) = self.walk(layers, from, c, q, keys, all_at_once).await? else {
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
            if v.is_absent() || g.commit(r) < k {
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
    ///
    /// With a clear in `commits`, the entries are the changes it made: at a
    /// commit that clears, a key present before and not written there is
    /// removed, and a key written there replaced its version from before
    /// (and is left out if that did not change). That reads the state before
    /// the window.
    pub async fn window_with(
        &self,
        layers: &[LayerRef],
        commits: RangeInclusive<u64>,
        q: &Query,
        on: impl FnMut(&Run),
    ) -> Result<Option<Vec<u8>>> {
        self.window_in(layers, commits, None, q, on).await
    }

    /// `window_with` over the listed keys only, reading their blocks and no
    /// others.
    pub async fn window_keys_with<K: AsRef<[u8]>>(
        &self,
        layers: &[LayerRef],
        commits: RangeInclusive<u64>,
        keys: &[K],
        q: &Query,
        on: impl FnMut(&Run),
    ) -> Result<Option<Vec<u8>>> {
        self.window_in(layers, commits, Some(KeyList::new(keys)), q, on)
            .await
    }

    async fn window_in(
        &self,
        layers: &[LayerRef],
        commits: RangeInclusive<u64>,
        keys: Option<KeyList>,
        q: &Query,
        mut on: impl FnMut(&Run),
    ) -> Result<Option<Vec<u8>>> {
        let bounds = check_layers(layers)?;
        let (lo, hi) = (*commits.start(), *commits.end());
        if lo > hi {
            return Ok(None);
        }
        if !(lo == 0 && hi == u64::MAX) {
            at(lo.saturating_sub(1), (bounds.0, u64::MAX), "a window from")?;
        }
        let clears: Vec<u64> = layers
            .iter()
            .flat_map(|l| l.clears.iter().copied())
            .filter(|c| commits.contains(c))
            .collect();
        if !clears.is_empty() {
            return self
                .changes_with(layers, commits, &clears, keys, q, on)
                .await;
        }
        let from = lo.saturating_sub(1);
        let all_at_once = keys.is_some();
        let Some(mut w) = self.walk(layers, from, hi, q, keys, all_at_once).await? else {
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

    /// `window_with` over commits that hold `clears` (ascending): each key's
    /// entries replayed from the state before the window, so that a clear
    /// removes what it found.
    async fn changes_with(
        &self,
        layers: &[LayerRef],
        commits: RangeInclusive<u64>,
        clears: &[u64],
        keys: Option<KeyList>,
        q: &Query,
        mut on: impl FnMut(&Run),
    ) -> Result<Option<Vec<u8>>> {
        let (lo, hi) = (*commits.start(), *commits.end());
        let start = clear_at(layers, lo.saturating_sub(1));
        let from = start.saturating_sub(1);
        let all_at_once = keys.is_some();
        let Some(mut w) = self.walk(layers, from, hi, q, keys, all_at_once).await? else {
            return Ok(None);
        };
        let (mut g, mut f) = (Run::new(), Run::new());
        let mut changes: Vec<Change> = Vec::new();
        let limit = q.limit.unwrap_or(u64::MAX);
        let mut n = 0;
        while w.next(&mut g).await? {
            replay(&g, start, &commits, clears, &mut changes);
            if changes.is_empty() {
                continue;
            }
            f.clear();
            f.push_key(g.key(0));
            for &(c, new, was) in changes.iter().rev() {
                f.push_entry(c, version(&g, new), version(&g, was));
            }
            on(&f);
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

    /// The entries of the commits in `commits` of the listed keys, as
    /// `window_keys_with`.
    pub async fn window_keys<K: AsRef<[u8]>>(
        &self,
        layers: &[LayerRef],
        commits: RangeInclusive<u64>,
        keys: &[K],
        q: &Query,
    ) -> Result<Page> {
        let mut run = Run::new();
        let next = self
            .window_keys_with(layers, commits, keys, q, |g| run.push_key_from(g, 0))
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
        self.store
            .read_checked(&l.path, top.commits_start..top.index_start, |b| {
                parse_commits(b)
            })
            .await
            .map_err(|e| match e {
                Error::Format(m) => Error::Format(format!("{}: {m}", l.path)),
                e => e,
            })
    }

    /// Write one commit's layer at `path`: its entries, one per changed key
    /// (their commit numbers are ignored). Returns the layer's reference; a
    /// commit with no entries writes a layer too.
    pub async fn write_commit(
        &self,
        path: &str,
        commit: NewCommit<'_>,
        entries: &Run,
    ) -> Result<LayerRef> {
        let mut w = self.commit_writer(path, commit)?;
        for i in 0..entries.len() {
            let rows = entries.rows(i);
            if rows.len() != 1 {
                return invalid(format!(
                    "{:?}: a commit holds one entry per key",
                    lossy(entries.key(i))
                ));
            }
            w.push(
                entries.key(i),
                entries.new_version(rows.start),
                entries.replaced_version(rows.start),
            )
            .await?;
        }
        w.write().await
    }

    /// Write one commit's layer at `path` as its entries arrive: parts are
    /// uploaded as they fill, so the writer holds one part, not the commit.
    pub fn commit_writer(&self, path: &str, commit: NewCommit<'_>) -> Result<CommitWriter> {
        Ok(CommitWriter {
            w: Writer::new(self.write, commit.commit, commit.commit, None)?,
            up: self.store.upload(path)?,
            part: self.store.opts.part_bytes,
            h: CommitHeader {
                commit: commit.commit,
                time_ms: commit.time_ms,
                flags: if commit.clear { CLEAR } else { 0 },
                producer: commit.producer.to_vec(),
                ..Default::default()
            },
            live_before: if commit.clear { 0 } else { commit.live_before },
            path: path.to_string(),
        })
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
        // entries older than the newest clear at or below the fold are gone
        // from the base
        let clears: Vec<u64> = ins.iter().flat_map(|l| l.clears.iter().copied()).collect();
        let kept = clears
            .iter()
            .rev()
            .find(|&&k| k <= fold)
            .copied()
            .unwrap_or(0);
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
            for r in rows.clone() {
                // what was there before: nothing after a clear or before the
                // first entry, else the previous entry's version
                let c = g.commit(r);
                let since = if r + 1 < rows.end { g.commit(r + 1) } else { 0 };
                let k = clears.partition_point(|&k| k <= c);
                let cleared = k > 0 && clears[k - 1] > since;
                let want = if cleared || r + 1 == rows.end {
                    if !cleared && !from_start {
                        continue;
                    }
                    Version::Absent
                } else {
                    g.new_version(r + 1)
                };
                if g.replaced_version(r) != want {
                    let there = if cleared {
                        format!("commit {} cleared", clears[k - 1])
                    } else if r + 1 < rows.end {
                        format!("commit {} left {want:?}", g.commit(r + 1))
                    } else {
                        "nothing was there".to_string()
                    };
                    return Err(Error::Chain(format!(
                        "{:?}: commit {c} replaced {:?}, but {there}",
                        lossy(g.key(0)),
                        g.replaced_version(r),
                    )));
                }
            }
            for r in rows.clone() {
                if g.commit(r) <= fold {
                    let v = g.new_version(r);
                    if !v.is_absent() && g.commit(r) >= kept {
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

/// One commit's layer, written key by key (`ChangeIndex::commit_writer`).
/// After an error, drop it: nothing is written.
pub struct CommitWriter {
    w: Writer,
    up: Upload,
    part: usize,
    h: CommitHeader,
    live_before: u64,
    path: String,
}

impl CommitWriter {
    /// The next changed key, keys ascending, with its new and replaced
    /// versions.
    pub async fn push(
        &mut self,
        key: &[u8],
        new: Version<'_>,
        replaced: Version<'_>,
    ) -> Result<()> {
        if self.w.entries() > 0 && key == self.h.max_key {
            return invalid(format!(
                "{:?}: a commit holds one entry per key",
                lossy(key)
            ));
        }
        if self.h.flags & CLEAR != 0 && !replaced.is_absent() {
            return invalid(format!(
                "{:?}: commit {} clears, so it replaces nothing, not {replaced:?}",
                lossy(key),
                self.h.commit
            ));
        }
        self.w.push(key, self.h.commit, new, replaced)?;
        if self.w.entries() == 1 {
            self.h.min_key = key.to_vec();
        }
        self.h.max_key.clear();
        self.h.max_key.extend_from_slice(key);
        match (new.is_absent(), replaced.is_absent()) {
            (false, true) => self.h.added += 1,
            (false, false) => self.h.updated += 1,
            _ => self.h.removed += 1,
        }
        if self.w.pending() >= self.part {
            self.up.write(self.w.take()).await?;
        }
        Ok(())
    }

    /// The layer, or `None` (and nothing written) when nothing was pushed to
    /// a commit that does not clear.
    pub async fn finish(self) -> Result<Option<LayerRef>> {
        if self.w.entries() == 0 && self.h.flags & CLEAR == 0 {
            return Ok(None);
        }
        self.write().await.map(Some)
    }

    async fn write(mut self) -> Result<LayerRef> {
        self.h.live = (self.live_before + self.h.added)
            .checked_sub(self.h.removed)
            .ok_or_else(|| {
                Error::Invalid(format!(
                    "commit {} removes more keys than were live",
                    self.h.commit
                ))
            })?;
        let mut l = self.w.finish(std::slice::from_ref(&self.h))?;
        self.up.write(self.w.take()).await?;
        self.up.finish().await?;
        l.path = self.path;
        Ok(l)
    }
}

/// A layer's entries, one key at a time.
pub struct Entries(Walk);

impl Entries {
    /// The next key and its entries, newest first, into `group`; false at the
    /// end. Not cancel-safe: a call dropped before it returns leaves the
    /// reader unusable.
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
    /// The commit clears the state first (a full run's first commit): its
    /// entries replace nothing, and it removes every key it does not write.
    pub clear: bool,
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
        let size = sizes[a..e].iter().fold(0u64, |t, &x| t.saturating_add(x));
        let newer = sizes[e..].iter().fold(0u64, |t, &x| t.saturating_add(x));
        size <= newer.saturating_mul(4).saturating_add(MIB)
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
        let size = sizes[b..b + below]
            .iter()
            .fold(0u64, |t, &x| t.saturating_add(x));
        let base = if b == 1 { sizes[0] } else { 0 };
        if size.saturating_mul(4) >= base {
            return Some(0..b + below);
        }
    }
    None
}
