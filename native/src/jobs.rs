//! The streaming jobs over an index: a full replacement (the merge-join of
//! the written content with the index), a patch (the merge-join of a sorted
//! run of upserts and removes with it, or of a run that replaces it all), a
//! compaction, and a recount. Each is
//! driven by `step`, which runs until it needs input or has a file to hand
//! over; the caller does the I/O.

use std::cmp::Ordering;
use std::sync::Arc;

use crate::delta::{Delta, Old};
use crate::format::{Error, Options, Result};
use crate::garbage::GarbageWriter;
use crate::rows::Source;
use crate::run::SortedRun;
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

/// A full replacement: every written key against the live keys of the index
/// (`delta.rs` decides each), live keys not written deleted.
pub struct Replace {
    pub src: Source,
    pub merge: Merge,
    pub delta: Delta,
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
            delta: Delta::new(o, max_file_bytes, collect, generation),
            old: None,
            done: false,
        }
    }

    pub fn step(&mut self) -> Result<Step> {
        loop {
            if !self.delta.writer.files.is_empty() {
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
                    self.delta.finish()?;
                    self.done = true;
                    continue;
                }
                (true, false) => Ordering::Less,
                (false, true) => Ordering::Greater,
                (true, true) => self.src.key().cmp(self.merge.key()),
            };
            let m = &self.merge;
            match ord {
                Ordering::Less => {
                    let (k, v) = self.src.entry();
                    self.delta.apply(k, Some(v), Old::Absent)?;
                    self.src.advance();
                }
                Ordering::Greater => {
                    let was = Old::Live(m.version(), m.locator());
                    self.delta.apply(m.key(), None, was)?;
                    self.old = None;
                }
                Ordering::Equal => {
                    let (k, v) = self.src.entry();
                    self.delta
                        .apply(k, Some(v), Old::Live(m.version(), m.locator()))?;
                    self.src.advance();
                    self.old = None;
                }
            }
        }
    }
}

/// A sorted run against the index: a patch — keys it does not mention
/// passed over, reading stops once the run is done — or with `replace` the
/// whole new content, live keys it omits deleted. Each key as `delta.rs`
/// decides.
pub struct Patch {
    pub merge: Merge,
    pub delta: Delta,
    run: Arc<SortedRun>,
    replace: bool,
    i: usize,
    old: Option<bool>, // Some(true): the merge holds an entry; Some(false): exhausted
    done: bool,
}

impl Patch {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        run: Arc<SortedRun>,
        replace: bool,
        runs: usize,
        o: Options,
        max_file_bytes: usize,
        collect: usize,
        generation: u64,
    ) -> Result<Patch> {
        if replace && run.removes() > 0 {
            return Err(Error::Value("a replacement has no removes".into()));
        }
        Ok(Patch {
            merge: Merge::new(runs),
            delta: Delta::new(o, max_file_bytes, collect, generation),
            run,
            replace,
            i: 0,
            old: None,
            done: false,
        })
    }

    pub fn step(&mut self) -> Result<Step> {
        loop {
            if !self.delta.writer.files.is_empty() {
                return Ok(Step::File);
            }
            if self.done {
                return Ok(Step::Done);
            }
            let ended = self.i == self.run.len();
            if ended && (!self.replace || self.old == Some(false)) {
                self.delta.finish()?;
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
            let m = &self.merge;
            let held = self.old == Some(true);
            let was = if held && !m.deleted() {
                Old::Live(m.version(), m.locator())
            } else {
                Old::Absent
            };
            let ord = match (ended, held) {
                (true, _) => Ordering::Greater,
                (false, true) => self.run.key(self.i).cmp(m.key()),
                (false, false) => Ordering::Less,
            };
            match ord {
                Ordering::Greater => {
                    // An index key the run does not mention: gone, in a replacement.
                    if self.replace {
                        self.delta.apply(m.key(), None, was)?;
                    }
                    self.old = None;
                }
                Ordering::Less => {
                    let (k, w) = (self.run.key(self.i), self.run.write(self.i));
                    self.delta.apply(k, w, Old::Absent)?;
                    self.i += 1;
                }
                Ordering::Equal => {
                    let (k, w) = (self.run.key(self.i), self.run.write(self.i));
                    self.delta.apply(k, w, was)?;
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
