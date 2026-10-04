//! The streaming jobs over an index: the merge-join of written content with
//! it — a patch, or a full replacement — a span merge, and a scan. Each is
//! driven by `step`, which runs until it needs input or has a file to hand
//! over; the caller does the I/O.

use std::cmp::Ordering;

use crate::delta::{Delta, Old, Write};
use crate::format::{Error, Options, Result};
use crate::rows::Source;
use crate::spans::{retain, Version};
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

/// Adjacent spans merged into one (docs/key-index-design.md § One merge):
/// runs, newest first, fed a segment at a time. Each key's versions, read
/// run by run, come newest first; the merge keeps the newest and each one a
/// live endpoint sees (`spans::retain`), with the predecessor on the oldest
/// kept, and writes them to one span's files. With `base` (the output starts
/// at commit 0), its initial segment keeps live keys only. `segments[i]`
/// counts the versions written with `i` of the (sorted) endpoints at or
/// below their generation.
pub struct SpanMerge {
    pub merge: Merge,
    pub writer: Writer,
    pub segments: Vec<u64>,
    endpoints: Vec<u64>,
    base: bool,
    /// The key being gathered, and the run read for it next.
    key: Option<Vec<u8>>,
    at: usize,
    versions: Vec<Version>,
    done: bool,
}

impl SpanMerge {
    pub fn new(
        runs: usize,
        mut endpoints: Vec<u64>,
        base: bool,
        o: Options,
        max_file_bytes: usize,
    ) -> SpanMerge {
        endpoints.sort_unstable();
        endpoints.dedup();
        SpanMerge {
            merge: Merge::new(runs),
            writer: Writer::new(o, max_file_bytes).repeating(),
            segments: vec![0; endpoints.len() + 1],
            endpoints,
            base,
            key: None,
            at: 0,
            versions: Vec::new(),
            done: false,
        }
    }

    pub fn step(&mut self) -> Result<Step> {
        let runs = &mut self.merge.runs;
        loop {
            if !self.writer.files.is_empty() {
                return Ok(Step::File);
            }
            if self.done {
                return Ok(Step::Done);
            }
            if self.key.is_none() {
                // The smallest key at any run's head: every run ready or done first.
                let mut min: Option<usize> = None;
                for r in 0..runs.len() {
                    match runs[r].state()? {
                        State::Starved => return Ok(Step::Run(r)),
                        State::Done => continue,
                        State::Ready => {
                            let (b, i) = runs[r].current();
                            if min.is_none_or(|m| {
                                let (mb, mi) = runs[m].current();
                                b.key(i) < mb.key(mi)
                            }) {
                                min = Some(r);
                            }
                        }
                    }
                }
                let Some(m) = min else {
                    self.writer.finish(false)?;
                    self.done = true;
                    continue;
                };
                let (b, i) = runs[m].current();
                self.key = Some(b.key(i).to_vec());
                self.at = 0;
                self.versions.clear();
            }
            let key = self.key.as_deref().expect("gathering a key");
            while self.at < runs.len() {
                match runs[self.at].state()? {
                    State::Starved => return Ok(Step::Run(self.at)),
                    State::Done => self.at += 1,
                    State::Ready => {
                        let (b, i) = runs[self.at].current();
                        if b.key(i) != key {
                            self.at += 1;
                            continue;
                        }
                        let v = Version {
                            generation: b.generation(i),
                            deleted: b.deleted(i),
                            payload: b.payload(i).map(<[u8]>::to_vec),
                            predecessor: b.predecessor(i),
                            prior: b.prior(i).and_then(|(_, p)| p).map(<[u8]>::to_vec),
                        };
                        if self
                            .versions
                            .last()
                            .is_some_and(|p| v.generation >= p.generation)
                        {
                            return Err(Error::Format(format!(
                                "versions of {:?} out of order",
                                String::from_utf8_lossy(key)
                            )));
                        }
                        self.versions.push(v);
                        runs[self.at].advance();
                    }
                }
            }
            for v in retain(&self.versions, &self.endpoints, self.base) {
                self.segments[self.endpoints.partition_point(|&e| e <= v.generation)] += 1;
                self.writer.push(
                    key,
                    v.generation,
                    v.deleted,
                    v.payload.as_deref(),
                    v.predecessor.map(|g| (g, v.prior.as_deref())),
                )?;
            }
            self.key = None;
        }
    }
}
