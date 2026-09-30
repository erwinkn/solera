//! The streaming jobs over an index: a full replacement (the merge-join of
//! the written content with the index), a compaction, and a recount. Each is
//! driven by `step`, which runs until it needs input or has a file to hand
//! over; the caller does the I/O.

use crate::format::{Options, Result};
use crate::rows::{Arena, Source};
use crate::stream::{Merge, Next, State, Writer};

pub enum Step {
    /// Existing run `r` needs its next segment, or its end.
    Run(usize),
    /// The written content needs its next sorted chunk, or its end.
    Rows,
    /// A file is ready: `Writer::files`.
    File,
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
/// live keys not written become deletions.
pub struct Replace {
    pub src: Source,
    pub merge: Merge,
    pub writer: Writer,
    pub added: u64,
    pub removed: u64,
    pub changed: u64,
    pub collected: Collected,
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
    ) -> Replace {
        Replace {
            src,
            merge: Merge::new(runs),
            writer: Writer::new(o, max_file_bytes),
            added: 0,
            removed: 0,
            changed: 0,
            collected: Collected::new(collect),
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
            let new = match self.src.state() {
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
                    let (k, v) = self.src.entry()?;
                    self.writer.push(k, v, false)?;
                    self.collected.add(k, false);
                    self.added += 1;
                    self.src.advance();
                }
                std::cmp::Ordering::Greater => {
                    let k = self.merge.key();
                    self.writer.push(k, b"", true)?;
                    self.collected.add(k, true);
                    self.removed += 1;
                    self.old = None;
                }
                std::cmp::Ordering::Equal => {
                    let (k, v) = self.src.entry()?;
                    if v != self.merge.version() {
                        self.writer.push(k, v, false)?;
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

/// Merges runs into new files, newest entry winning; with `drop_deleted`
/// (nothing older below), deletions go.
pub struct Compact {
    pub merge: Merge,
    pub writer: Writer,
    drop_deleted: bool,
    done: bool,
}

impl Compact {
    pub fn new(runs: usize, drop_deleted: bool, o: Options, max_file_bytes: usize) -> Compact {
        Compact {
            merge: Merge::new(runs),
            writer: Writer::new(o, max_file_bytes),
            drop_deleted,
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
            match self.merge.next_key()? {
                Next::Entry => {
                    let deleted = self.merge.deleted();
                    if !(deleted && self.drop_deleted) {
                        let (k, v) = (self.merge.key(), self.merge.version());
                        self.writer.push(k, v, deleted)?;
                    }
                }
                Next::Need(r) => return Ok(Step::Run(r)),
                Next::End => {
                    self.writer.finish(false)?;
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
