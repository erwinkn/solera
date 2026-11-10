//! Building one output commit: the keys a write changed, joined with the
//! output's own index at its head, become the commit's entries, each with the
//! version it replaced (docs/tracking-design.md §4, D184, D187). The layer is
//! written; the event part that installs it is returned for the engine to
//! journal. A commit that changes nothing writes nothing (INC-14).
//!
//! Reads the state only: run it off the event loop. The part's check refuses
//! it if another commit reached the output meanwhile.

use solera_index::{ChangeIndex, CommitWriter, NewCommit, Query, Run, Version};

use crate::plan::Indexes;

use crate::commit::changes::{Change, Changes, Source};
use crate::commit::OutputCommit;
use crate::error::{invalid, Result};
use crate::state::{Id, Output};

/// Keys of the head's state read per page, when a commit replaces it.
const PAGE: u64 = 65_536;

/// What a commit is.
#[derive(Clone, Copy, Debug)]
pub struct Spec<'a> {
    /// The version an upsert without a version word gets: the attempt's
    /// generation, or a source commit's.
    pub version: u64,
    pub time_ms: u64,
    /// Who made it, encoded by the engine: run, batch, attempt; or a source report.
    pub producer: &'a [u8],
    /// Where its layer is written.
    pub path: &'a str,
    pub mode: Mode,
    /// Keep up to this many changed keys in `Built::keys` (0: none).
    pub collect: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    /// The changes patch the output: keys not named stay as they are.
    Patch,
    /// The changes name every key the output holds afterwards (a source's
    /// full listing): a key present at the head and not named is removed.
    Replace,
    /// A load's report on a source (P13): a key's report counts only if the
    /// log did not change that key after this commit, the batch's head.
    Report(u64),
}

/// A commit built and written, not yet installed.
#[derive(Clone, Debug, PartialEq)]
pub struct Built {
    pub part: OutputCommit,
    pub added: u64,
    pub updated: u64,
    pub removed: u64,
    /// The changed keys, if no more than `Spec::collect`.
    pub keys: Option<Changed>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Changed {
    pub upserted: Vec<Box<[u8]>>,
    pub removed: Vec<Box<[u8]>>,
}

struct Entries {
    w: CommitWriter,
    added: u64,
    updated: u64,
    removed: u64,
    keys: Option<Changed>,
    collect: usize,
    version: u64,
}

impl Entries {
    async fn put(&mut self, key: &[u8], change: Change<'_>, old: Version<'_>) -> Result<()> {
        let new = match change {
            Change::Upsert(None) => Version::Int(self.version),
            Change::Upsert(Some(v)) => Version::Bytes(v),
            Change::Removed => Version::Absent,
        };
        if new == old {
            return Ok(());
        }
        self.w.push(key, new, old).await?;
        match (new.is_absent(), old.is_absent()) {
            (false, true) => self.added += 1,
            (false, false) => self.updated += 1,
            _ => self.removed += 1,
        }
        if let Some(k) = &mut self.keys {
            if k.upserted.len() + k.removed.len() == self.collect {
                self.keys = None;
            } else if new.is_absent() {
                k.removed.push(key.into());
            } else {
                k.upserted.push(key.into());
            }
        }
        Ok(())
    }
}

/// Builds and writes the next commit of `output` (`out` as the state holds
/// it, or the default for an output with no commit yet) from `changes`.
/// None if nothing changed.
pub async fn build(
    ixs: &dyn Indexes,
    output: &Id,
    out: &Output,
    spec: Spec<'_>,
    changes: &mut dyn Source,
) -> Result<Option<Built>> {
    let ix = ixs.index(output)?;
    let head = out.head;
    let commit = head + 1;
    if let Mode::Report(at) = spec.mode {
        if at > head {
            return invalid(format!("a load at commit {at}, after the head {head}"));
        }
    }
    let nc = NewCommit {
        commit,
        time_ms: spec.time_ms,
        producer: spec.producer,
        live_before: out.live,
    };
    let mut e = Entries {
        w: ix.commit_writer(spec.path, nc)?,
        added: 0,
        updated: 0,
        removed: 0,
        keys: (spec.collect > 0).then(Changed::default),
        collect: spec.collect,
        version: spec.version,
    };
    let mut ch = Changes::new();
    if spec.mode == Mode::Replace {
        replace(ix, out, changes, &mut ch, &mut e).await?;
    } else {
        while changes.next(&mut ch)? {
            patch(ix, out, spec.mode, &ch, &mut e).await?;
        }
    }
    let Some(layer) = e.w.finish().await? else {
        return Ok(None);
    };
    Ok(Some(Built {
        part: OutputCommit {
            output: output.clone(),
            layer,
            live: out.live + e.added - e.removed,
        },
        added: e.added,
        updated: e.updated,
        removed: e.removed,
        keys: e.keys,
    }))
}

/// One chunk of a patch: its keys' state at the head, by one `get`.
async fn patch(
    ix: &ChangeIndex,
    out: &Output,
    mode: Mode,
    ch: &Changes,
    e: &mut Entries,
) -> Result<()> {
    let keys: Vec<&[u8]> = (0..ch.len()).map(|i| ch.key(i)).collect();
    let keys = &keys;
    let state = |c: u64| async move {
        if out.head == 0 {
            return Ok::<Run, crate::Error>(Run::new());
        }
        Ok(ix.get(&out.layers, c, keys, &Query::default()).await?.run)
    };
    let now = state(out.head).await?;
    let then = match mode {
        Mode::Report(at) if at < out.head => Some(state(at).await?),
        _ => None,
    };
    let (mut j, mut t) = (0, 0);
    for i in 0..ch.len() {
        let k = ch.key(i);
        let old = at_key(&now, &mut j, k);
        if let Some(then) = &then {
            if at_key(then, &mut t, k) != old {
                continue;
            }
        }
        e.put(k, ch.change(i), old).await?;
    }
    Ok(())
}

/// The version `run` (one entry per key, in key order) holds for `key`,
/// moving `j` forward; keys are asked in ascending order.
fn at_key<'r>(run: &'r Run, j: &mut usize, key: &[u8]) -> Version<'r> {
    while *j < run.len() && run.key(*j) < key {
        *j += 1;
    }
    if *j < run.len() && run.key(*j) == key {
        run.new_version(run.rows(*j).start)
    } else {
        Version::Absent
    }
}

/// A full listing: the changes merged with the head's whole state, page by
/// page; a key at the head and not named is removed.
async fn replace(
    ix: &ChangeIndex,
    out: &Output,
    changes: &mut dyn Source,
    ch: &mut Changes,
    e: &mut Entries,
) -> Result<()> {
    let mut page = Run::new();
    let mut after: Option<Vec<u8>> = None;
    let mut more = out.head > 0;
    let (mut i, mut j) = (0, 0);
    let mut listed = changes.next(ch)?;
    loop {
        if listed && i == ch.len() {
            listed = changes.next(ch)?;
            i = 0;
        }
        if j == page.len() && more {
            let q = Query {
                after: after.take(),
                limit: Some(PAGE),
                ..Default::default()
            };
            let p = ix.scan(&out.layers, out.head, &q).await?;
            more = p.next.is_some();
            after = p.next;
            page = p.run;
            j = 0;
            continue;
        }
        let k = (listed && i < ch.len()).then(|| ch.key(i));
        let old = (j < page.len()).then(|| page.key(j));
        match (k, old) {
            (None, None) => return Ok(()),
            (Some(k), Some(o)) if k == o => {
                e.put(k, ch.change(i), page.new_version(page.rows(j).start))
                    .await?;
                i += 1;
                j += 1;
            }
            (Some(k), o) if o.is_none_or(|o| k < o) => {
                e.put(k, ch.change(i), Version::Absent).await?;
                i += 1;
            }
            (_, Some(o)) => {
                e.put(o, Change::Removed, page.new_version(page.rows(j).start))
                    .await?;
                j += 1;
            }
            (Some(_), None) => unreachable!(),
        }
    }
}
