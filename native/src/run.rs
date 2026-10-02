//! A sorted run: a write's entries in key order — upserts at their versions,
//! and removes — as the key index takes them end to end. Built once (from
//! lists, from rows, or from the `.kx` transport form, every fact checked),
//! read by every resolver, and encoded only to cross the wire. A delta
//! file's entries held in memory (the engine's summaries) are one too, with
//! their locators.

use crate::format::{
    decompress_at_most, fmt_err, parse_index_at_most, slice_at, sort_order, Error, Index, Options,
    Result,
};

/// What a parsed index holds, in bytes: its share of a decoding budget.
fn index_bytes(idx: &Index) -> u64 {
    let blocks: usize = idx.blocks.iter().map(|b| b.0.len() + 32).sum();
    (idx.min_key.len() + idx.max_key.len() + blocks) as u64
}
use crate::rows::{Arena, Source};
use crate::stream::{read_entry, State, Writer};

#[derive(Default)]
pub struct SortedRun {
    pub keys: Arena,
    pub versions: Arena,
    pub deleted: Vec<bool>,
    pub locators: Vec<u64>,
    removes: usize,
}

fn limit<T>(what: impl std::fmt::Display) -> Result<T> {
    Err(Error::Limit(what.to_string()))
}

impl SortedRun {
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    pub fn removes(&self) -> usize {
        self.removes
    }

    #[inline]
    pub fn key(&self, i: usize) -> &[u8] {
        self.keys.get(i)
    }

    /// Entry `i`'s write: its version, or None for a remove.
    #[inline]
    pub fn write(&self, i: usize) -> Option<&[u8]> {
        (!self.deleted[i]).then(|| self.versions.get(i))
    }

    /// Bytes held.
    pub fn nbytes(&self) -> usize {
        self.keys.data.capacity()
            + self.versions.data.capacity()
            + 8 * (self.keys.ends.capacity() + self.versions.ends.capacity())
            + self.deleted.capacity()
            + 8 * self.locators.capacity()
    }

    /// Gives back what building over-allocated: a run is held as long as it is read.
    fn shrink(mut self) -> SortedRun {
        self.keys.data.shrink_to_fit();
        self.keys.ends.shrink_to_fit();
        self.versions.data.shrink_to_fit();
        self.versions.ends.shrink_to_fit();
        self.deleted.shrink_to_fit();
        self.locators.shrink_to_fit();
        self
    }

    fn push(&mut self, key: &[u8], version: &[u8], deleted: bool, locator: u64) -> Result<()> {
        if let Some(last) = self.keys.len().checked_sub(1) {
            if key <= self.keys.get(last) {
                let what = if key == self.keys.get(last) {
                    "written and removed"
                } else {
                    "out of order"
                };
                return Err(Error::Value(format!(
                    "key {:?} {what}",
                    String::from_utf8_lossy(key)
                )));
            }
        }
        self.keys.push(key);
        self.versions.push(version);
        self.deleted.push(deleted);
        self.locators.push(locator);
        self.removes += deleted as usize;
        Ok(())
    }

    /// Upserts of `keys` at `versions` (any order, each key once) and the
    /// removes of `removes` (any order, repeats ignored); a key both
    /// written and removed is an error.
    pub fn of(keys: &[&[u8]], versions: &[&[u8]], removes: &[&[u8]]) -> Result<SortedRun> {
        if keys.len() != versions.len() {
            return Err(Error::Value(
                "keys and versions must have the same length".into(),
            ));
        }
        let order = sort_order(keys)?;
        let mut b = Builder::new(removes, keys.len());
        for i in order {
            b.upsert(keys[i], versions[i])?;
        }
        b.finish()
    }

    /// The keys and versions of `rows`, in key order, and `removes`.
    pub fn from_source(src: &mut Source, removes: &[&[u8]]) -> Result<SortedRun> {
        let mut b = Builder::new(removes, 0);
        while src.state()? == State::Ready {
            let (k, v) = src.entry();
            b.upsert(k, v)?;
            src.advance();
        }
        b.finish()
    }

    /// A run from its transport form, a `.kx` file, after checking every
    /// fact a reader relies on: the footer, the index and each block's
    /// checksum, each block's entries against its index entry and the
    /// file's against the footer, keys strictly increasing. Decodes at most
    /// `max_entries` entries and `max_bytes` bytes (decompressed, and keys
    /// and versions decoded): past either, an `Error::Limit`, whatever the
    /// file claims.
    pub fn decode(kx: &[u8], max_entries: u64, max_bytes: u64) -> Result<SortedRun> {
        let idx = parse_index_at_most(kx, kx.len() as u64, max_bytes)?;
        let declared = idx.footer.entries;
        if declared > max_entries {
            return limit(format!("{declared} entries, over {max_entries}"));
        }
        let mut indexed = 0u64;
        for b in &idx.blocks {
            indexed = indexed.saturating_add(b.3);
        }
        if indexed != declared {
            return fmt_err("the index's entries do not match the footer");
        }
        let mut run = SortedRun::default();
        // One budget: the index decoded, then the blocks, then their keys and versions.
        let mut budget = max_bytes.saturating_sub(index_bytes(&idx));
        let take = |budget: &mut u64, n: usize| -> Result<()> {
            match budget.checked_sub(n as u64) {
                Some(left) => {
                    *budget = left;
                    Ok(())
                }
                None => limit(format!("more than {max_bytes} bytes decoded")),
            }
        };
        let mut key: Vec<u8> = Vec::new();
        for (first, offset, size, entries, crc) in &idx.blocks {
            let Some(raw) = slice_at(kx, *offset, *size) else {
                return fmt_err("block out of bounds");
            };
            if crc32fast::hash(raw) != *crc {
                return fmt_err("block checksum mismatch");
            }
            let data = decompress_at_most(raw, idx.footer.codec, budget)?;
            take(&mut budget, data.len())?;
            let (start, mut pos) = (run.len(), 0usize);
            key.clear();
            while pos < data.len() {
                if run.len() as u64 >= max_entries {
                    return limit(format!("more than {max_entries} entries"));
                }
                let f = read_entry(&data, &mut pos)?;
                if f.shared > key.len() {
                    return fmt_err("bad shared prefix length");
                }
                let suffix = &data[f.suffix.0..f.suffix.1];
                let version = &data[f.version.0..f.version.1];
                take(&mut budget, f.shared + suffix.len() + version.len())?;
                key.truncate(f.shared);
                key.extend_from_slice(suffix);
                run.push(&key, version, f.flags & 1 != 0, f.locator)
                    .map_err(|_| Error::Format("keys out of order".into()))?;
            }
            if (run.len() - start) as u64 != *entries {
                return fmt_err("a block's entries do not match its index");
            }
            if run.len() > start && run.key(start) != first.as_slice() {
                return fmt_err("a block's first key does not match its index");
            }
        }
        if run.len() as u64 != declared {
            return fmt_err("the entries do not match the footer");
        }
        if !run.is_empty()
            && (run.key(0) != idx.min_key.as_slice()
                || run.key(run.len() - 1) != idx.max_key.as_slice())
        {
            return fmt_err("the keys do not match the index's range");
        }
        Ok(run.shrink())
    }

    /// The transport form: one `.kx` file.
    pub fn encode(&self, o: Options) -> Result<Vec<u8>> {
        let mut w = Writer::new(o, usize::MAX);
        for i in 0..self.len() {
            w.push(
                self.key(i),
                self.versions.get(i),
                self.deleted[i],
                self.locators[i],
                None,
            )?;
        }
        w.finish(true)?;
        Ok(w.files.pop_front().expect("finish(true) writes a file"))
    }

    /// The first entry of the newest-wins merge of `runs` (newest first) past
    /// `after`, then the next, up to `limit`: `(run, entry)` each, and whether
    /// any key lies past them.
    pub fn merge(
        runs: &[&SortedRun],
        after: Option<&[u8]>,
        limit: usize,
    ) -> (Vec<(usize, usize)>, bool) {
        let mut at: Vec<usize> = runs
            .iter()
            .map(|r| match after {
                Some(a) => r.keys.partition_point_le(a),
                None => 0,
            })
            .collect();
        let mut out = Vec::new();
        loop {
            // The smallest head key, the newest run holding it.
            let mut best: Option<(usize, &[u8])> = None;
            for (r, run) in runs.iter().enumerate() {
                if at[r] < run.len() {
                    let k = run.key(at[r]);
                    if best.is_none_or(|(_, b)| k < b) {
                        best = Some((r, k));
                    }
                }
            }
            let Some((r, key)) = best else {
                return (out, false);
            };
            if out.len() == limit {
                return (out, true);
            }
            out.push((r, at[r]));
            for (s, run) in runs.iter().enumerate() {
                if at[s] < run.len() && run.key(at[s]) == key {
                    at[s] += 1;
                }
            }
        }
    }
}

impl Arena {
    /// How many of these sorted strings are `<= x`.
    fn partition_point_le(&self, x: &[u8]) -> usize {
        let (mut lo, mut hi) = (0, self.len());
        while lo < hi {
            let mid = (lo + hi) / 2;
            if self.get(mid) <= x {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }
}

/// Upserts in key order merged with sorted removes.
struct Builder<'a> {
    run: SortedRun,
    removes: Vec<&'a [u8]>,
    next: usize,
}

impl<'a> Builder<'a> {
    fn new(removes: &[&'a [u8]], upserts: usize) -> Builder<'a> {
        let mut removes = removes.to_vec();
        removes.sort_unstable();
        removes.dedup();
        let mut run = SortedRun::default();
        run.keys.ends.reserve(upserts + removes.len());
        run.versions.ends.reserve(upserts + removes.len());
        Builder {
            run,
            removes,
            next: 0,
        }
    }

    fn upsert(&mut self, key: &[u8], version: &[u8]) -> Result<()> {
        while self.next < self.removes.len() && self.removes[self.next] <= key {
            self.run.push(self.removes[self.next], b"", true, 0)?;
            self.next += 1;
        }
        self.run.push(key, version, false, 0)
    }

    fn finish(mut self) -> Result<SortedRun> {
        for k in &self.removes[self.next..] {
            self.run.push(k, b"", true, 0)?;
        }
        Ok(self.run.shrink())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::CODEC_ZLIB;

    const O: Options = Options {
        block_size: 64,
        level: 1,
        bits_per_item: 14,
        k: 10,
        codec: CODEC_ZLIB,
    };

    #[test]
    fn of_encode_decode_round_trip() {
        let run = SortedRun::of(
            &[b"c", b"a", b"e"],
            &[b"3", b"1", b"5"],
            &[b"d", b"b", b"d"],
        )
        .unwrap();
        let keys: Vec<&[u8]> = (0..run.len()).map(|i| run.key(i)).collect();
        assert_eq!(keys, [b"a", b"b", b"c", b"d", b"e"]);
        assert_eq!(run.removes(), 2);
        assert_eq!(run.write(1), None);
        assert_eq!(run.write(2), Some(&b"3"[..]));
        let back = SortedRun::decode(&run.encode(O).unwrap(), 5, 1 << 20).unwrap();
        assert_eq!(back.keys.data, run.keys.data);
        assert_eq!(back.deleted, run.deleted);
        assert!(matches!(
            SortedRun::decode(&run.encode(O).unwrap(), 4, 1 << 20),
            Err(Error::Limit(_))
        ));
        assert!(matches!(
            SortedRun::decode(&run.encode(O).unwrap(), 5, 8),
            Err(Error::Limit(_))
        ));
        assert!(SortedRun::of(&[b"a"], &[b"1"], &[b"a"]).is_err());
        assert!(SortedRun::of(&[b"a", b"a"], &[b"1", b"2"], &[]).is_err());
    }

    #[test]
    fn merge_newest_wins_from_a_cursor() {
        let new = SortedRun::of(&[b"b", b"d"], &[b"n", b"n"], &[]).unwrap();
        let old = SortedRun::of(&[b"a", b"b", b"c", b"e"], &[&b"o"[..]; 4], &[]).unwrap();
        let runs = [&new, &old];
        let (got, more) = SortedRun::merge(&runs, Some(b"a"), 3);
        assert_eq!(got, [(0, 0), (1, 2), (0, 1)]);
        assert!(more);
        let (got, more) = SortedRun::merge(&runs, Some(b"d"), 3);
        assert_eq!(got, [(1, 3)]);
        assert!(!more);
    }
}
