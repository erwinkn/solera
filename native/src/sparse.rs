//! The sparse reader's per-key state (docs/resolved-commits.md §6), kept
//! native from start to delta. Python chooses files and fetches their tails
//! and blocks; this decides, for each of the sorted entries — by its
//! position, never a Python object — what the index holds: read from a
//! block, absent by the key filters, or — for an upsert carrying no payload,
//! which changes the key whatever its entry — live by the key and tombstone
//! filters; then writes the delta by the one rule (`delta.rs`).

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::delta::{Delta, Old, Write};
use crate::entries::SortedEntries;
use crate::format::{key_item, may_hold, tomb_item, Options, Result};
use crate::stream::Block;

#[derive(Clone)]
enum Known {
    /// Not decided yet.
    Unknown,
    /// No live entry: read deleted, or no file's key filter holds it.
    Absent,
    /// Read live, at this generation, with this payload.
    Live(u64, Option<Vec<u8>>),
    /// Live, as the filters said.
    Other,
    /// The filters cannot decide: an exact read in the files holding it.
    Maybe,
}

/// A Bloom filter as a tail holds it: bits, then its bit count and probes.
pub type Filter<'a> = (u64, u8, &'a [u8]);

pub struct Sparse {
    pub sorted: Arc<SortedEntries>,
    known: Vec<Known>,
    tomb: Vec<bool>,
    key: Vec<bool>,
    /// Per file the filters ran on (an id the caller gives): positions its key filter holds.
    holders: BTreeMap<usize, Vec<u32>>,
    pub inferred: bool,
}

impl Sparse {
    pub fn new(sorted: Arc<SortedEntries>) -> Sparse {
        let n = sorted.len();
        Sparse {
            sorted,
            known: vec![Known::Unknown; n],
            tomb: vec![false; n],
            key: vec![false; n],
            holders: BTreeMap::new(),
            inferred: false,
        }
    }

    /// Entries still undecided.
    pub fn unknown(&self) -> usize {
        self.known
            .iter()
            .filter(|k| matches!(k, Known::Unknown))
            .count()
    }

    /// Entries an exact read must decide.
    pub fn maybe(&self) -> usize {
        self.known
            .iter()
            .filter(|k| matches!(k, Known::Maybe))
            .count()
    }

    /// The positions of the sorted entries' keys in `[min, max]`.
    pub fn span(&self, min: &[u8], max: &[u8]) -> (usize, usize) {
        let keys = &self.sorted.keys;
        let (mut lo, mut hi) = (0, keys.len());
        while lo < hi {
            let mid = (lo + hi) / 2;
            if keys.get(mid) < min {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        let start = lo;
        hi = keys.len();
        while lo < hi {
            let mid = (lo + hi) / 2;
            if keys.get(mid) <= max {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        (start, lo)
    }

    /// The positions a read of one file decides: with `file`, the ones the
    /// filters left to it; else the undecided ones in `[lo, hi)`.
    fn targets(&self, file: Option<usize>, lo: usize, hi: usize) -> Vec<u32> {
        match file {
            Some(f) => self
                .holders
                .get(&f)
                .map(|ps| {
                    ps.iter()
                        .copied()
                        .filter(|&p| matches!(self.known[p as usize], Known::Maybe))
                        .collect()
                })
                .unwrap_or_default(),
            None => (lo..hi)
                .filter(|&p| matches!(self.known[p], Known::Unknown))
                .map(|p| p as u32)
                .collect(),
        }
    }

    /// The block each target lies in, by the file's blocks' first keys.
    fn block_of(firsts: &[&[u8]], key: &[u8]) -> Option<usize> {
        let i = firsts.partition_point(|f| *f <= key);
        i.checked_sub(1)
    }

    /// The blocks a read of one file needs (see `targets`), ascending.
    pub fn blocks(
        &self,
        firsts: &[&[u8]],
        file: Option<usize>,
        lo: usize,
        hi: usize,
    ) -> Vec<usize> {
        let mut out: Vec<usize> = self
            .targets(file, lo, hi)
            .iter()
            .filter_map(|&p| Sparse::block_of(firsts, self.sorted.key(p as usize)))
            .collect();
        out.dedup();
        out
    }

    /// Reads the targets (see `targets`) in the fetched `blocks` of one file —
    /// `(index, bytes)`, CRCs checked by the caller: a key it holds is decided,
    /// live or deleted; one it lacks stays as it was.
    pub fn read(
        &mut self,
        blocks: &[(usize, &[u8])],
        codec: u8,
        firsts: &[&[u8]],
        file: Option<usize>,
        lo: usize,
        hi: usize,
    ) -> Result<()> {
        let targets = self.targets(file, lo, hi);
        let fetched: BTreeMap<usize, &[u8]> = blocks.iter().copied().collect();
        // Targets come in key order, so blocks in file order: one decoded at a time.
        let mut current: Option<(usize, Block)> = None;
        for p in targets {
            let key = self.sorted.key(p as usize);
            let Some(b) = Sparse::block_of(firsts, key) else {
                continue;
            };
            let Some(&raw) = fetched.get(&b) else {
                continue; // not fetched: not this read's
            };
            if current.as_ref().is_none_or(|(i, _)| *i != b) {
                current = Some((b, Block::decode(raw, codec)?));
            }
            let blk = &current.as_ref().expect("just decoded").1;
            let (mut l, mut h) = (0, blk.len());
            while l < h {
                let mid = (l + h) / 2;
                match blk.key(mid).cmp(key) {
                    std::cmp::Ordering::Less => l = mid + 1,
                    std::cmp::Ordering::Greater => h = mid,
                    std::cmp::Ordering::Equal => {
                        self.known[p as usize] = if blk.deleted(mid) {
                            Known::Absent
                        } else {
                            Known::Live(blk.generation(mid), blk.payload(mid).map(<[u8]>::to_vec))
                        };
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    /// Runs one file's filters over the undecided positions in `[lo, hi)`;
    /// `file` names it for the exact reads that follow.
    pub fn filter(&mut self, file: usize, lo: usize, hi: usize, keys: Filter, tombs: Filter) {
        let mut item = Vec::new();
        let mut held = Vec::new();
        for p in lo..hi {
            if !matches!(self.known[p], Known::Unknown) {
                continue;
            }
            let key = self.sorted.key(p);
            key_item(&mut item, key);
            if !may_hold(keys.2, &item, keys.0, keys.1) {
                continue;
            }
            held.push(p as u32);
            self.key[p] = true;
            tomb_item(&mut item, key);
            self.tomb[p] |= may_hold(tombs.2, &item, tombs.0, tombs.1);
        }
        if !held.is_empty() {
            self.holders.insert(file, held);
        }
    }

    /// Decides what the filters can: no key filter holds a key — absent; an
    /// upsert with no payload, which no tombstone filter holds — live, unless
    /// `exact`; the rest need an exact read.
    pub fn classify(&mut self, exact: bool) {
        for p in 0..self.known.len() {
            if !matches!(self.known[p], Known::Unknown) {
                continue;
            }
            let bare = matches!(self.sorted.write(p), Write::Upsert(None));
            self.known[p] = if !self.key[p] {
                Known::Absent
            } else if bare && !self.tomb[p] && !exact {
                self.inferred = true;
                Known::Other
            } else {
                Known::Maybe
            };
        }
    }

    /// What the index holds for entry `p`.
    fn old(&self, p: usize) -> Old<'_> {
        match &self.known[p] {
            Known::Live(g, payload) => Old::Live(*g, payload.as_deref()),
            Known::Other => Old::Other,
            _ => Old::Absent,
        }
    }

    /// Each live entry read: position, generation, payload.
    pub fn live(&self) -> impl Iterator<Item = (usize, u64, Option<&[u8]>)> {
        self.known.iter().enumerate().filter_map(|(p, k)| match k {
            Known::Live(g, payload) => Some((p, *g, payload.as_deref())),
            _ => None,
        })
    }

    /// The delta of the sorted entries over what was read, by the one rule.
    pub fn delta(
        &self,
        o: Options,
        max_file_bytes: usize,
        collect: usize,
        generation: u64,
    ) -> Result<Delta> {
        let mut d = Delta::new(o, max_file_bytes, collect, generation);
        for p in 0..self.sorted.len() {
            d.apply(self.sorted.key(p), self.sorted.write(p), self.old(p))?;
        }
        d.finish()?;
        Ok(d)
    }
}
