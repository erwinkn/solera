//! The streaming jobs over an index: the merge-join of written content with
//! it — a patch, or a full replacement — a compaction, and a scan. Each is
//! driven by `step`, which runs until it needs input or has a file to hand
//! over; the caller does the I/O.

use std::cmp::Ordering;

use crate::delta::{Delta, Old, Write};
use crate::format::{Error, Options, Result};
use crate::rows::Source;
use crate::stream::{Merge, Next, State, Writer};

pub enum Step {
    /// Existing run `r` needs its next segment, or its end.
    Run(usize),
    /// The written content needs its next sorted chunk, or its end.
    Rows,
    /// A file is ready: `Writer::files`.
    File,
    /// A page of entries is ready: `Scan::page`.
    Page,
    Done,
}

/// The merge-join of written content — rows, a stream of sorted chunks, or
/// sorted entries — with the live keys of the index, each key as `delta.rs`
/// decides. A patch passes over the index keys it does not mention and
/// stops reading the index once its content is done; a replacement
/// (`replace`) is the whole new content, and live keys it omits are deleted.
pub struct Join {
    pub src: Source,
    pub merge: Merge,
    pub delta: Delta,
    replace: bool,
    old: Option<bool>, // Some(true): the merge holds a live entry; Some(false): exhausted
    done: bool,
}

impl Join {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        src: Source,
        replace: bool,
        runs: usize,
        o: Options,
        max_file_bytes: usize,
        collect: usize,
        generation: u64,
    ) -> Result<Join> {
        if replace && matches!(&src, Source::Entries(sorted, _) if sorted.removes() > 0) {
            return Err(Error::Value("a replacement has no removes".into()));
        }
        Ok(Join {
            src,
            merge: Merge::new(runs),
            delta: Delta::new(o, max_file_bytes, collect, generation),
            replace,
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
            let new = match self.src.state()? {
                State::Starved => return Ok(Step::Rows),
                s => s == State::Ready,
            };
            if !new && !self.replace {
                self.delta.finish()?; // a patch is done with its content
                self.done = true;
                continue;
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
                    self.delta
                        .apply(self.src.key(), self.src.write(), Old::Absent)?;
                    self.src.advance();
                }
                Ordering::Greater => {
                    // An index key the content does not mention: gone, in a replacement.
                    if self.replace {
                        let was = Old::Live(m.generation(), m.payload());
                        self.delta.apply(m.key(), Write::Remove, was)?;
                    }
                    self.old = None;
                }
                Ordering::Equal => {
                    let was = Old::Live(m.generation(), m.payload());
                    self.delta.apply(self.src.key(), self.src.write(), was)?;
                    self.src.advance();
                    self.old = None;
                }
            }
        }
    }
}

/// Merges runs into new files, newest entry winning; with `drop_deleted`
/// (nothing older below), deletions go. The objects the dropped entries name
/// need no list of their own: exact deltas name each one as a predecessor.
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
                    let m = &self.merge;
                    let (k, gen, deleted) = (m.key(), m.generation(), m.deleted());
                    if !(deleted && self.drop_deleted) {
                        // Predecessors belong to delta files only.
                        self.writer.push(k, gen, deleted, m.payload(), None)?;
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

/// The merged entries of the runs past `after`, newest winning, deleted ones
/// included, `limit` at a time: one merge read once across pages, where a
/// page-by-page read starts the merge over for each (`KeyIndex.pending_pages`).
pub struct Scan {
    pub merge: Merge,
    after: Option<Vec<u8>>,
    limit: usize,
    pub page: Vec<(Vec<u8>, u64, bool, Option<Vec<u8>>)>,
}

impl Scan {
    pub fn new(runs: usize, after: Option<Vec<u8>>, limit: usize) -> Scan {
        Scan {
            merge: Merge::new(runs),
            after,
            limit: limit.max(1),
            page: Vec::new(),
        }
    }

    pub fn step(&mut self) -> Result<Step> {
        loop {
            match self.merge.next_key()? {
                Next::Entry => {
                    let key = self.merge.key();
                    if self.after.as_deref().is_some_and(|after| key <= after) {
                        continue;
                    }
                    let entry = (
                        key.to_vec(),
                        self.merge.generation(),
                        self.merge.deleted(),
                        self.merge.payload().map(<[u8]>::to_vec),
                    );
                    self.page.push(entry);
                    if self.page.len() >= self.limit {
                        return Ok(Step::Page);
                    }
                }
                Next::Need(r) => return Ok(Step::Run(r)),
                Next::End if self.page.is_empty() => return Ok(Step::Done),
                Next::End => return Ok(Step::Page),
            }
        }
    }
}
