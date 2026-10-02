//! The streaming jobs over an index: the merge-join of written content with
//! it — a patch, or a full replacement — a compaction, and a recount. Each is
//! driven by `step`, which runs until it needs input or has a file to hand
//! over; the caller does the I/O.

use std::cmp::Ordering;

use crate::delta::{Delta, Old, Write};
use crate::format::{Error, Options, Result};
use crate::garbage::GarbageWriter;
use crate::rows::Source;
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

/// The merge-join of written content — rows, a stream of sorted chunks, or
/// a sorted run — with the live keys of the index, each key as `delta.rs`
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
        if replace && matches!(&src, Source::Run(run, _) if run.removes() > 0) {
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
/// (nothing older below), deletions go. With `garbage`, every entry the
/// merge drops that names an object — a live entry shadowed by a newer one
/// of its key, at another generation — is written to garbage files.
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
                    let m = &self.merge;
                    let (k, gen, deleted) = (m.key(), m.generation(), m.deleted());
                    if let Some(g) = &mut self.garbage {
                        for (odel, ogen) in m.shadowed() {
                            if !odel && ogen != gen {
                                g.push(k, ogen);
                            }
                        }
                    }
                    if !(deleted && self.drop_deleted) {
                        // Predecessors belong to delta files only.
                        self.writer.push(k, gen, deleted, m.payload(), None)?;
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
