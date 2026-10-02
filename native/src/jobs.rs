//! The streaming jobs over an index: a full replacement (the merge-join of
//! the written content with the index), a patch (the merge-join of a sorted
//! run of upserts and removes with it), a compaction, and a recount. Each is
//! driven by `step`, which runs until it needs input or has a file to hand
//! over; the caller does the I/O.

use std::cmp::Ordering;

use crate::format::{Options, Result};
use crate::garbage::GarbageWriter;
use crate::rows::{Arena, Source};
use crate::stream::{Merge, Next, State, Writer};

pub enum Step {
    /// Existing run `r` needs its next segment, or its end.
    Run(usize),
    /// The written content needs its next sorted chunk, or its end.
    Rows,
    /// A file is ready: `Writer::files`.
    File,
    /// A garbage file is ready: `Compact::garbage`'s `files`.
    Garbage,
    Done,
}

/// Keys a replacement changed, up to a limit: beyond it, `None`.
pub struct Collected {
    pub upserts: Option<Arena>,
    pub removes: Option<Arena>,
    limit: usize,
}

impl Collected {
    fn new(limit: usize) -> Collected {
        Collected {
            upserts: Some(Arena::default()),
            removes: Some(Arena::default()),
            limit,
        }
    }

    fn add(&mut self, key: &[u8], removed: bool) {
        let total = self.upserts.as_ref().map_or(0, |a| a.len())
            + self.removes.as_ref().map_or(0, |a| a.len());
        if total >= self.limit {
            self.upserts = None;
            self.removes = None;
        }
        let side = if removed {
            &mut self.removes
        } else {
            &mut self.upserts
        };
        if let Some(a) = side {
            a.push(key);
        }
    }
}

/// A full replacement: every written key against the live keys of the index.
/// New keys and changed versions are written, unchanged ones dropped, and
/// live keys not written become deletions. Written entries carry the
/// writer's `generation` as their locator, and a changed or deleted key its
/// predecessor, its version and locator.
pub struct Replace {
    pub src: Source,
    pub merge: Merge,
    pub writer: Writer,
    pub added: u64,
    pub removed: u64,
    pub changed: u64,
    pub collected: Collected,
    generation: u64,
    old: Option<bool>, // Some(true): the merge holds a live entry; Some(false): exhausted
    done: bool,
}

impl Replace {
    pub fn new(
        src: Source,
        runs: usize,
        o: Options,
        max_file_bytes: usize,
        collect: usize,
        generation: u64,
    ) -> Replace {
        Replace {
            src,
            merge: Merge::new(runs),
            writer: Writer::new(o, max_file_bytes),
            added: 0,
            removed: 0,
            changed: 0,
            collected: Collected::new(collect),
            generation,
            old: None,
            done: false,
        }
    }

    pub fn step(&mut self) -> Result<Step> {
        loop {
            if !self.writer.files.is_empty() {
                return Ok(Step::File);
            }
            if self.done {
                return Ok(Step::Done);
            }
            if self.old.is_none() {
                match self.merge.next_key()? {
                    Next::Entry if self.merge.deleted() => {}
                    Next::Entry => self.old = Some(true),
                    Next::Need(r) => return Ok(Step::Run(r)),
                    Next::End => self.old = Some(false),
                }
                continue;
            }
            let new = match self.src.state()? {
                State::Starved => return Ok(Step::Rows),
                s => s == State::Ready,
            };
            let old = self.old == Some(true);
            let ord = match (new, old) {
                (false, false) => {
                    self.writer.finish(false)?;
                    self.done = true;
                    continue;
                }
                (true, false) => std::cmp::Ordering::Less,
                (false, true) => std::cmp::Ordering::Greater,
                (true, true) => self.src.key().cmp(self.merge.key()),
            };
            match ord {
                std::cmp::Ordering::Less => {
                    let (k, v) = self.src.entry();
                    self.writer.push(k, v, false, self.generation, None)?;
                    self.collected.add(k, false);
                    self.added += 1;
                    self.src.advance();
                }
                std::cmp::Ordering::Greater => {
                    let (k, predecessor) = (
                        self.merge.key(),
                        (self.merge.version(), self.merge.locator()),
                    );
                    self.writer
                        .push(k, b"", true, self.generation, Some(predecessor))?;
                    self.collected.add(k, true);
                    self.removed += 1;
                    self.old = None;
                }
                std::cmp::Ordering::Equal => {
                    let (k, v) = self.src.entry();
                    if v != self.merge.version() {
                        let predecessor = (self.merge.version(), self.merge.locator());
                        self.writer
                            .push(k, v, false, self.generation, Some(predecessor))?;
                        self.collected.add(k, false);
                        self.changed += 1;
                    }
                    self.src.advance();
                    self.old = None;
                }
            }
        }
    }
}

/// A patch: a sorted run of upserts and removes (`deleted`) against the
/// live keys of the index. An upsert of a new key or a changed version is
/// written, an unchanged one dropped; a remove of a live key becomes a
/// deletion. Keys the run does not mention are passed over. Written entries
/// carry `generation` as their locator, and a changed or deleted key its
/// predecessor. Stops reading the index once the run is done.
pub struct Patch {
    pub merge: Merge,
    pub writer: Writer,
    keys: Arena,
    versions: Arena,
    deleted: Vec<bool>,
    i: usize,
    pub added: u64,
    pub removed: u64,
    pub changed: u64,
    pub collected: Collected,
    generation: u64,
    old: Option<bool>, // Some(true): the merge holds an entry; Some(false): exhausted
    done: bool,
}

impl Patch {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        keys: Arena,
        versions: Arena,
        deleted: Vec<bool>,
        runs: usize,
        o: Options,
        max_file_bytes: usize,
        collect: usize,
        generation: u64,
    ) -> Patch {
        Patch {
            merge: Merge::new(runs),
            writer: Writer::new(o, max_file_bytes),
            keys,
            versions,
            deleted,
            i: 0,
            added: 0,
            removed: 0,
            changed: 0,
            collected: Collected::new(collect),
            generation,
            old: None,
            done: false,
        }
    }

    pub fn step(&mut self) -> Result<Step> {
        loop {
            if !self.writer.files.is_empty() {
                return Ok(Step::File);
            }
            if self.done {
                return Ok(Step::Done);
            }
            if self.i == self.keys.len() {
                self.writer.finish(false)?;
                self.done = true;
                continue;
            }
            if self.old.is_none() {
                self.old = Some(match self.merge.next_key()? {
                    Next::Entry => true,
                    Next::Need(r) => return Ok(Step::Run(r)),
                    Next::End => false,
                });
                continue;
            }
            let (key, remove) = (self.keys.get(self.i), self.deleted[self.i]);
            let ord = if self.old == Some(true) {
                key.cmp(self.merge.key())
            } else {
                Ordering::Less
            };
            match ord {
                Ordering::Greater => self.old = None, // an index key the run does not mention
                Ordering::Less => {
                    if !remove {
                        let v = self.versions.get(self.i);
                        self.writer.push(key, v, false, self.generation, None)?;
                        self.collected.add(key, false);
                        self.added += 1;
                    }
                    self.i += 1;
                }
                Ordering::Equal => {
                    let live = !self.merge.deleted();
                    let predecessor = (self.merge.version(), self.merge.locator());
                    if remove {
                        if live {
                            self.writer
                                .push(key, b"", true, self.generation, Some(predecessor))?;
                            self.collected.add(key, true);
                            self.removed += 1;
                        }
                    } else {
                        let v = self.versions.get(self.i);
                        if !live {
                            self.writer.push(key, v, false, self.generation, None)?;
                            self.collected.add(key, false);
                            self.added += 1;
                        } else if v != predecessor.0 {
                            self.writer
                                .push(key, v, false, self.generation, Some(predecessor))?;
                            self.collected.add(key, false);
                            self.changed += 1;
                        }
                    }
                    self.i += 1;
                    self.old = None;
                }
            }
        }
    }
}

/// Merges runs into new files, newest entry winning; with `drop_deleted`
/// (nothing older below), deletions go. With `garbage`, every entry the
/// merge drops that names an object — a live entry shadowed by a newer one
/// of its key, at another version or locator — is written to garbage files.
pub struct Compact {
    pub merge: Merge,
    pub writer: Writer,
    pub garbage: Option<GarbageWriter>,
    drop_deleted: bool,
    done: bool,
}

impl Compact {
    pub fn new(
        runs: usize,
        drop_deleted: bool,
        garbage: bool,
        o: Options,
        max_file_bytes: usize,
    ) -> Compact {
        Compact {
            merge: Merge::new(runs),
            writer: Writer::new(o, max_file_bytes),
            garbage: garbage.then(|| GarbageWriter::new(o.codec, o.level, max_file_bytes)),
            drop_deleted,
            done: false,
        }
    }

    pub fn step(&mut self) -> Result<Step> {
        loop {
            if !self.writer.files.is_empty() {
                return Ok(Step::File);
            }
            if self.garbage.as_ref().is_some_and(|g| !g.files.is_empty()) {
                return Ok(Step::Garbage);
            }
            if self.done {
                return Ok(Step::Done);
            }
            match self.merge.next_key()? {
                Next::Entry => {
                    let deleted = self.merge.deleted();
                    let (k, v, loc) =
                        (self.merge.key(), self.merge.version(), self.merge.locator());
                    if let Some(g) = &mut self.garbage {
                        for (ov, odel, oloc) in self.merge.shadowed() {
                            if !odel && (ov != v || oloc != loc) {
                                g.push(k, ov, oloc);
                            }
                        }
                    }
                    if !(deleted && self.drop_deleted) {
                        // A merge keeps locators; predecessors belong to delta files only.
                        self.writer.push(k, v, deleted, loc, None)?;
                    }
                }
                Next::Need(r) => return Ok(Step::Run(r)),
                Next::End => {
                    self.writer.finish(false)?;
                    if let Some(g) = &mut self.garbage {
                        g.finish();
                    }
                    self.done = true;
                }
            }
        }
    }
}

/// Counts the live keys of the merged runs.
pub struct Count {
    pub merge: Merge,
    pub live: u64,
}

impl Count {
    pub fn new(runs: usize) -> Count {
        Count {
            merge: Merge::new(runs),
            live: 0,
        }
    }

    pub fn step(&mut self) -> Result<Step> {
        loop {
            match self.merge.next_key()? {
                Next::Entry => self.live += !self.merge.deleted() as u64,
                Next::Need(r) => return Ok(Step::Run(r)),
                Next::End => return Ok(Step::Done),
            }
        }
    }
}
