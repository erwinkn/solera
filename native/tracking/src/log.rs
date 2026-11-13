//! A log: an output that only appends rows, read by commit.
//!
//! A log is an output of the change index like any other, keyed by position:
//! a commit's rows are cut into parts, and each part is one entry, its key
//! the commit number then the part's number within the commit, its version
//! the part's bytes. Nothing here reads what a part holds: whoever writes a
//! log encodes its rows, a few to a part.
//!
//! So a log's commits are layers: a merge packs many commits into one
//! object, a full run's first commit clears, and the state lists them as it
//! lists every output's. A key is written once and never changed, so the
//! rows of commits `from + 1..=to` are the keys in that range present at the
//! head, and reading them is one scan of a key range.

use solera_index::{ChangeIndex, LayerRef, NewCommit, Query, Version};

use crate::error::{invalid, Result};

/// The key of one part: commit, then part, in hexadecimal of a fixed width,
/// so keys sort in the order the rows were appended. Text, as every key the
/// engine journals in a layer's description is.
pub fn key(commit: u64, part: u32) -> String {
    format!("{commit:016x}{part:08x}")
}

/// The commit a key belongs to.
pub fn commit_of(key: &[u8]) -> Result<u64> {
    let commit = (key.len() == 24)
        .then(|| std::str::from_utf8(&key[..16]).ok())
        .flatten()
        .and_then(|c| u64::from_str_radix(c, 16).ok());
    match commit {
        Some(c) => Ok(c),
        None => invalid("a log's key is 24 hexadecimal digits: this output is no log"),
    }
}

/// The commit a log's content starts over at, as far as its layers still
/// say: its newest commit that clears, 0 when they show none. Merges fold a
/// clear away only once every reader is past it ([`crate::retain`]).
pub fn cleared(layers: &[LayerRef]) -> u64 {
    layers
        .iter()
        .filter_map(|l| l.clears.last().copied())
        .max()
        .unwrap_or(0)
}

/// The first commit that may hold rows present at the head: the newest
/// clear, else the oldest row the layers hold (every row they hold is
/// present when none of them clears).
pub fn start(layers: &[LayerRef]) -> u64 {
    match cleared(layers) {
        0 => layers
            .iter()
            .find(|l| l.entries > 0)
            .and_then(|l| commit_of(&l.first_key).ok())
            .unwrap_or(1),
        c => c,
    }
}

/// Writes commit `commit.commit`'s layer at `path`: one entry per part, in
/// order. A commit that clears writes a layer even with no part.
pub async fn write<P: AsRef<[u8]>>(
    ix: &ChangeIndex,
    path: &str,
    commit: NewCommit<'_>,
    parts: &[P],
) -> Result<LayerRef> {
    if parts.is_empty() && !commit.clear {
        return invalid("a log's commit appends at least one part");
    }
    if u32::try_from(parts.len()).is_err() {
        return invalid(format!("{} parts in one commit of a log", parts.len()));
    }
    let n = commit.commit;
    let mut w = ix.commit_writer(path, commit)?;
    for (i, p) in parts.iter().enumerate() {
        let k = key(n, i as u32);
        w.push(k.as_bytes(), Version::Bytes(p.as_ref()), Version::Absent)
            .await?;
    }
    Ok(w.finish().await?.expect("a part, or a clear"))
}

/// The parts of commits `from + 1..=to` present at `head`, in order, a page
/// at a time: `(parts, next)`, each part with its commit, `next` what to
/// pass as `after` for the following page (None at the end).
pub async fn read(
    ix: &ChangeIndex,
    layers: &[LayerRef],
    head: u64,
    (from, to): (u64, u64),
    after: Option<Vec<u8>>,
    limit: u64,
) -> Result<(Vec<(u64, Vec<u8>)>, Option<Vec<u8>>)> {
    let q = Query {
        lo: Some(key(from, u32::MAX).into_bytes()),
        hi: Some(key(to, u32::MAX).into_bytes()),
        after,
        limit: Some(limit),
        pattern: None,
    };
    let page = ix.scan(layers, head, &q).await?;
    let r = &page.run;
    let mut parts = Vec::with_capacity(r.len());
    for i in 0..r.len() {
        let Version::Bytes(b) = r.new_version(r.rows(i).start) else {
            return invalid("a log's entry holds a part's bytes: this output is no log");
        };
        parts.push((commit_of(r.key(i))?, b.to_vec()));
    }
    Ok((parts, page.next))
}
