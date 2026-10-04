//! The streaming jobs over an index: the merge-join of written content with
//! it — a patch, or a full replacement — a span merge, and a page of a
//! span read. Each is
//! driven by `step`, which runs until it needs input or has a file to hand
//! over; the caller does the I/O.

use std::cmp::Ordering;

use crate::delta::{Delta, Old, Write};
use crate::format::{Error, Options, Result};
use crate::rows::Source;
use crate::spans::{At, Changed, Fold, Retainer, Version};
use crate::stream::{Merge, Next, State, Stream, Writer};

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

/// Each key's versions across runs (newest first), gathered run by run as
/// the runs are fed: a key's versions may run across blocks, segments and
/// files, and a run starved mid-key asks for its next segment. One version
/// at a time: nothing holds a key's whole history (A17 R10).
#[derive(Default)]
struct Gather {
    key: Option<Vec<u8>>,
    at: usize,
    prev: Option<u64>,
}

enum Gathered {
    /// Run `r` needs its next segment, or its end.
    Need(usize),
    /// The next key begins: its versions follow.
    Key,
    Version(Version),
    /// The key's versions are all gathered.
    KeyDone(Vec<u8>),
    End,
}

impl Gather {
    fn next(&mut self, runs: &mut [Stream]) -> Result<Gathered> {
        let Some(key) = self.key.as_deref() else {
            // The smallest key at any run's head: every run ready or done first.
            let mut min: Option<usize> = None;
            for r in 0..runs.len() {
                match runs[r].state()? {
                    State::Starved => return Ok(Gathered::Need(r)),
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
                return Ok(Gathered::End);
            };
            let (b, i) = runs[m].current();
            self.key = Some(b.key(i).to_vec());
            (self.at, self.prev) = (0, None);
            return Ok(Gathered::Key);
        };
        while self.at < runs.len() {
            match runs[self.at].state()? {
                State::Starved => return Ok(Gathered::Need(self.at)),
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
                    if self.prev.is_some_and(|p| v.generation >= p) {
                        return Err(Error::Format(format!(
                            "versions of {:?} out of order",
                            String::from_utf8_lossy(key)
                        )));
                    }
                    self.prev = Some(v.generation);
                    runs[self.at].advance();
                    return Ok(Gathered::Version(v));
                }
            }
        }
        Ok(Gathered::KeyDone(self.key.take().expect("a key gathered")))
    }
}

/// Adjacent spans merged into one (docs/key-index-design.md § One merge):
/// runs, newest first, fed a segment at a time. Each key's versions, read
/// run by run, come newest first; the merge keeps the newest and each one a
/// live endpoint sees (`spans::Retainer`), with the predecessor on the
/// oldest kept, and writes them to one span's files as they are decided.
/// With `base` (the output starts at commit 0), its initial segment keeps
/// live keys only. `segments[i]` counts the versions written with `i` of the
/// (sorted) endpoints at or below their generation.
pub struct SpanMerge {
    pub merge: Merge,
    pub writer: Writer,
    pub segments: Vec<u64>,
    retainer: Retainer,
    gather: Gather,
    done: bool,
}

impl SpanMerge {
    pub fn new(
        runs: usize,
        endpoints: Vec<u64>,
        base: bool,
        o: Options,
        max_file_bytes: usize,
    ) -> SpanMerge {
        let retainer = Retainer::new(endpoints, base);
        SpanMerge {
            merge: Merge::new(runs),
            writer: Writer::new(o, max_file_bytes).repeating(),
            segments: vec![0; retainer.endpoints().len() + 1],
            retainer,
            gather: Gather::default(),
            done: false,
        }
    }

    fn write(&mut self, v: Version) -> Result<()> {
        let key = self.gather.key.as_deref().expect("a key gathered");
        self.segments[self
            .retainer
            .endpoints()
            .partition_point(|&e| e <= v.generation)] += 1;
        self.writer.push(
            key,
            v.generation,
            v.deleted,
            v.payload.as_deref(),
            v.predecessor.map(|g| (g, v.prior.as_deref())),
        )
    }

    pub fn step(&mut self) -> Result<Step> {
        loop {
            if !self.writer.files.is_empty() {
                return Ok(Step::File);
            }
            if self.done {
                return Ok(Step::Done);
            }
            match self.gather.next(&mut self.merge.runs)? {
                Gathered::Need(r) => return Ok(Step::Run(r)),
                Gathered::Key => {}
                Gathered::Version(v) => {
                    if let Some(kept) = self.retainer.push(v) {
                        self.write(kept)?;
                    }
                }
                Gathered::KeyDone(key) => {
                    if let Some(kept) = self.retainer.finish() {
                        self.gather.key = Some(key); // the key `write` names
                        self.write(kept)?;
                        self.gather.key = None;
                    }
                }
                Gathered::End => {
                    self.writer.finish(false)?;
                    self.done = true;
                }
            }
        }
    }
}

/// What a span read keeps of each key: its change over a range, or its
/// state at a reserved endpoint (`bound` None: the head).
pub enum ReadAs {
    Changes {
        g_p: u64,
        g_n1: Option<u64>,
    },
    At {
        bound: Option<u64>,
        drop_deleted: bool,
    },
}

enum Folding {
    Skip,
    Changed(Changed),
    At(At),
}

/// One page of a span read: runs (spans, newest first) fed a segment at a
/// time from the block holding `after`, each key past it folded as it
/// streams by (`spans::Changed`, `spans::At`), until `limit` keys are kept
/// and one more key is seen, or the runs end. A key's versions may run
/// across blocks and files: the page asks for what it lacks and never
/// stops inside a key, so every page but the last advances (A17 R2), and
/// it holds a few segments and one key's fold, whatever the key's history.
pub struct SpanRead {
    pub merge: Merge,
    gather: Gather,
    read: ReadAs,
    after: Option<Vec<u8>>,
    limit: usize,
    folding: Folding,
    /// Keys, classes (`spans::ADDED`…; 0 for a state at an endpoint) and versions.
    pub page: Vec<(Vec<u8>, u8, Version)>,
    /// The last key examined, and whether a key follows it.
    pub last: Option<Vec<u8>>,
    pub more: bool,
    done: bool,
}

impl SpanRead {
    pub fn new(runs: usize, read: ReadAs, after: Option<Vec<u8>>, limit: usize) -> SpanRead {
        SpanRead {
            merge: Merge::new(runs),
            gather: Gather::default(),
            read,
            after,
            limit: limit.max(1),
            folding: Folding::Skip,
            page: Vec::new(),
            last: None,
            more: false,
            done: false,
        }
    }

    pub fn step(&mut self) -> Result<Step> {
        loop {
            if self.done {
                return Ok(Step::Done);
            }
            match self.gather.next(&mut self.merge.runs)? {
                Gathered::Need(r) => return Ok(Step::Run(r)),
                Gathered::Key => {
                    let key = self.gather.key.as_deref().expect("a key gathered");
                    if self.after.as_deref().is_some_and(|a| key <= a) {
                        self.folding = Folding::Skip;
                    } else if self.page.len() == self.limit {
                        (self.more, self.done) = (true, true);
                        return Ok(Step::Page);
                    } else {
                        self.folding = match self.read {
                            ReadAs::Changes { g_p, g_n1 } => {
                                Folding::Changed(Changed::new(g_p, g_n1))
                            }
                            ReadAs::At { bound, .. } => Folding::At(At::new(bound)),
                        };
                    }
                }
                Gathered::Version(v) => match &mut self.folding {
                    Folding::Skip => {}
                    Folding::Changed(c) => c.push(v)?,
                    Folding::At(a) => a.push(v)?,
                },
                Gathered::KeyDone(key) => match std::mem::replace(&mut self.folding, Folding::Skip)
                {
                    Folding::Skip => {}
                    Folding::Changed(c) => {
                        if let Some((class, v)) = c.finish() {
                            self.page.push((key.clone(), class, v));
                        }
                        self.last = Some(key);
                    }
                    Folding::At(a) => {
                        let drop = matches!(
                            self.read,
                            ReadAs::At {
                                drop_deleted: true,
                                ..
                            }
                        );
                        if let Some(v) = a.found.filter(|v| !(drop && v.deleted)) {
                            self.page.push((key.clone(), 0, v));
                        }
                        self.last = Some(key);
                    }
                },
                Gathered::End => {
                    self.done = true;
                    return Ok(Step::Page);
                }
            }
        }
    }
}
