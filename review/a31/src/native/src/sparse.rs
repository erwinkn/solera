//! The sparse reader's per-key state (docs/resolved-commits.md §6), kept
//! native from start to delta. Python chooses files and fetches their tails
//! and blocks; this decides, for each of the sorted entries — by its
//! position, never a Python object — what the index holds: read from a
//! block, or absent by the key filters; then writes the delta by the one
//! rule (`delta.rs`). Writes are exact: a key a filter holds is always read,
//! for the predecessor its delta entry names (docs/key-index-design.md).

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::delta::{Delta, Old};
use crate::entries::SortedEntries;
use crate::format::{key_item, may_hold, Options, Result};
use crate::stream::Block;

#[derive(Clone)]
enum Known {
    /// Not decided yet.
    Unknown,
    /// No live entry: read deleted, or no file's key filter holds it.
    Absent,
    /// Read live, at this generation, with this payload.
    Live(u64, Option<Vec<u8>>),
    /// The filters cannot decide: an exact read in the files holding it.
    Maybe,
}

/// A Bloom filter as a tail holds it: bits, then its bit count and probes.
pub type Filter<'a> = (u64, u8, &'a [u8]);

pub struct Sparse {
    pub sorted: Arc<SortedEntries>,
    known: Vec<Known>,
    key: Vec<bool>,
    /// Per file the filters ran on (an id the caller gives): positions its key filter holds.
    holders: BTreeMap<usize, Vec<u32>>,
}

impl Sparse {
    pub fn new(sorted: Arc<SortedEntries>) -> Sparse {
        let n = sorted.len();
        Sparse {
            sorted,
            known: vec![Known::Unknown; n],
            key: vec![false; n],
            holders: BTreeMap::new(),
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

    /// The blocks a key's newest entry may lie in, by the file's blocks'
    /// first keys: the last one starting below it, and — a span's file may
    /// hold a key several times, its versions crossing into the next block —
    /// the next, if it starts with the key.
    fn blocks_of(firsts: &[&[u8]], key: &[u8]) -> impl Iterator<Item = usize> {
        let i = firsts.partition_point(|f| *f < key);
        let next = (i < firsts.len() && firsts[i] == key).then_some(i);
        i.checked_sub(1).into_iter().chain(next)
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
            .flat_map(|&p| Sparse::blocks_of(firsts, self.sorted.key(p as usize)))
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
        // Targets come in key order, so blocks in file order: few decoded at a time.
        let mut decoded: BTreeMap<usize, Block> = BTreeMap::new();
        for p in targets {
            let key = self.sorted.key(p as usize);
            for b in Sparse::blocks_of(firsts, key) {
                let Some(&raw) = fetched.get(&b) else {
                    continue; // not fetched: not this read's
                };
                if !decoded.contains_key(&b) {
                    decoded.retain(|&i, _| i + 1 >= b); // earlier blocks are done with
                    decoded.insert(b, Block::decode(raw, codec)?);
                }
                let blk = &decoded[&b];
                // The key's first entry: its newest version.
                let i = blk.lower_bound(key);
                if i < blk.len() && blk.key(i) == key {
                    self.known[p as usize] = if blk.deleted(i) {
                        Known::Absent
                    } else {
                        Known::Live(blk.generation(i), blk.payload(i).map(<[u8]>::to_vec))
                    };
                    break;
                }
            }
        }
        Ok(())
    }

    /// Runs one file's key filter over the undecided positions in `[lo, hi)`;
    /// `file` names it for the exact reads that follow.
    pub fn filter(&mut self, file: usize, lo: usize, hi: usize, keys: Filter) {
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
        }
        if !held.is_empty() {
            self.holders.insert(file, held);
        }
    }

    /// Decides what the filters can: no key filter holds a key — absent; the
    /// rest need an exact read.
    pub fn classify(&mut self) {
        for p in 0..self.known.len() {
            if !matches!(self.known[p], Known::Unknown) {
                continue;
            }
            self.known[p] = if self.key[p] {
                Known::Maybe
            } else {
                Known::Absent
            };
        }
    }

    /// What the index holds for entry `p`.
    fn old(&self, p: usize) -> Old<'_> {
        match &self.known[p] {
            Known::Live(g, payload) => Old::Live(*g, payload.as_deref()),
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
