//! Sorted entries: a write's entries in key order — upserts, each with its
//! payload if it carries one, and removes — as the key index takes them end
//! to end. Built once (from lists, from rows, or from the `.kx` transport
//! form, every fact checked), read by every resolver, and encoded only to
//! cross the wire. A delta file's entries held in memory (the engine's
//! summaries) are one too, with their generations.

use crate::format::{
    decompress_at_most, fmt_err, parse_index_at_most, slice_at, sort_order, Error, Index, Options,
    Result,
};

/// What a parsed index holds, in bytes: its share of a decoding budget.
fn index_bytes(idx: &Index) -> u64 {
    let blocks: usize = idx.blocks.iter().map(|b| b.0.len() + 32).sum();
    (idx.min_key.len() + idx.max_key.len() + blocks) as u64
}
use crate::delta::Write;
use crate::rows::{Arena, Source};
use crate::stream::{read_entry, State, Writer};

#[derive(Default)]
pub struct SortedEntries {
    pub keys: Arena,
    /// Payloads, one per entry (empty where there is none: `has_payload`).
    pub payloads: Arena,
    pub has_payload: Vec<bool>,
    pub deleted: Vec<bool>,
    pub generations: Vec<u64>,
    removes: usize,
}

fn limit<T>(what: impl std::fmt::Display) -> Result<T> {
    Err(Error::Limit(what.to_string()))
}

impl SortedEntries {
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

    /// Entry `i`'s payload, if it carries one.
    #[inline]
    pub fn payload(&self, i: usize) -> Option<&[u8]> {
        self.has_payload[i].then(|| self.payloads.get(i))
    }

    /// Entry `i`'s write: an upsert with its payload, or a remove.
    #[inline]
    pub fn write(&self, i: usize) -> Write<'_> {
        if self.deleted[i] {
            Write::Remove
        } else {
            Write::Upsert(self.payload(i))
        }
    }

    /// Bytes held.
    pub fn nbytes(&self) -> usize {
        self.keys.data.capacity()
            + self.payloads.data.capacity()
            + 8 * (self.keys.ends.capacity() + self.payloads.ends.capacity())
            + 2 * self.deleted.capacity()
            + 8 * self.generations.capacity()
    }

    /// Gives back what building over-allocated: sorted entries are held as long as they are read.
    pub(crate) fn shrink(mut self) -> SortedEntries {
        self.keys.data.shrink_to_fit();
        self.keys.ends.shrink_to_fit();
        self.payloads.data.shrink_to_fit();
        self.payloads.ends.shrink_to_fit();
        self.has_payload.shrink_to_fit();
        self.deleted.shrink_to_fit();
        self.generations.shrink_to_fit();
        self
    }

    pub(crate) fn push(
        &mut self,
        key: &[u8],
        generation: u64,
        deleted: bool,
        payload: Option<&[u8]>,
    ) -> Result<()> {
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
        self.payloads.push(payload.unwrap_or_default());
        self.has_payload.push(payload.is_some());
        self.deleted.push(deleted);
        self.generations.push(generation);
        self.removes += deleted as usize;
        Ok(())
    }

    /// Upserts of `keys` (any order, each key once), each with its payload
    /// if `payloads` gives one, and the removes of `removes` (any order,
    /// repeats ignored); a key both written and removed is an error.
    pub fn of(
        keys: &[&[u8]],
        payloads: Option<&[Option<&[u8]>]>,
        removes: &[&[u8]],
    ) -> Result<SortedEntries> {
        if payloads.is_some_and(|p| p.len() != keys.len()) {
            return Err(Error::Value(
                "keys and payloads must have the same length".into(),
            ));
        }
        let order = sort_order(keys)?;
        let mut b = Builder::new(removes, keys.len());
        for i in order {
            b.upsert(keys[i], payloads.and_then(|p| p[i]))?;
        }
        b.finish()
    }

    /// The keys of `rows`, in key order, with their payloads, and `removes`.
    pub fn from_source(src: &mut Source, removes: &[&[u8]]) -> Result<SortedEntries> {
        let mut b = Builder::new(removes, 0);
        while src.state()? == State::Ready {
            let (k, p) = src.entry();
            b.upsert(k, p)?;
            src.advance();
        }
        b.finish()
    }

    /// Sorted entries from their transport form, a `.kx` file, after checking every
    /// fact a reader relies on: the footer, the index and each block's
    /// checksum, each block's entries against its index entry and the
    /// file's against the footer, keys strictly increasing. Decodes at most
    /// `max_entries` entries and `max_bytes` bytes (decompressed, and keys
    /// and payloads decoded): past either, an `Error::Limit`, whatever the
    /// file claims.
    pub fn decode(kx: &[u8], max_entries: u64, max_bytes: u64) -> Result<SortedEntries> {
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
        let mut sorted = SortedEntries::default();
        // One budget: the index decoded, then the blocks, then their keys and payloads.
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
            let (start, mut pos) = (sorted.len(), 0usize);
            key.clear();
            while pos < data.len() {
                if sorted.len() as u64 >= max_entries {
                    return limit(format!("more than {max_entries} entries"));
                }
                let f = read_entry(&data, &mut pos)?;
                if f.shared > key.len() {
                    return fmt_err("bad shared prefix length");
                }
                let suffix = &data[f.suffix.0..f.suffix.1];
                let payload = f.payload(&data);
                take(
                    &mut budget,
                    f.shared + suffix.len() + payload.map_or(0, <[u8]>::len),
                )?;
                key.truncate(f.shared);
                key.extend_from_slice(suffix);
                sorted
                    .push(&key, f.generation, f.deleted(), payload)
                    .map_err(|_| Error::Format("keys out of order".into()))?;
            }
            if (sorted.len() - start) as u64 != *entries {
                return fmt_err("a block's entries do not match its index");
            }
            if sorted.len() > start && sorted.key(start) != first.as_slice() {
                return fmt_err("a block's first key does not match its index");
            }
        }
        if sorted.len() as u64 != declared {
            return fmt_err("the entries do not match the footer");
        }
        if !sorted.is_empty()
            && (sorted.key(0) != idx.min_key.as_slice()
                || sorted.key(sorted.len() - 1) != idx.max_key.as_slice())
        {
            return fmt_err("the keys do not match the index's range");
        }
        Ok(sorted.shrink())
    }

    /// The transport form: one `.kx` file.
    pub fn encode(&self, o: Options) -> Result<Vec<u8>> {
        let mut w = Writer::new(o, usize::MAX);
        for i in 0..self.len() {
            w.push(
                self.key(i),
                self.generations[i],
                self.deleted[i],
                self.payload(i),
                None,
            )?;
        }
        w.finish(true)?;
        Ok(w.files.pop_front().expect("finish(true) writes a file"))
    }
}

/// Upserts in key order merged with sorted removes.
struct Builder<'a> {
    sorted: SortedEntries,
    removes: Vec<&'a [u8]>,
    next: usize,
}

impl<'a> Builder<'a> {
    fn new(removes: &[&'a [u8]], upserts: usize) -> Builder<'a> {
        let mut removes = removes.to_vec();
        removes.sort_unstable();
        removes.dedup();
        let mut sorted = SortedEntries::default();
        sorted.keys.ends.reserve(upserts + removes.len());
        sorted.payloads.ends.reserve(upserts + removes.len());
        Builder {
            sorted,
            removes,
            next: 0,
        }
    }

    fn upsert(&mut self, key: &[u8], payload: Option<&[u8]>) -> Result<()> {
        while self.next < self.removes.len() && self.removes[self.next] <= key {
            self.sorted.push(self.removes[self.next], 0, true, None)?;
            self.next += 1;
        }
        self.sorted.push(key, 0, false, payload)
    }

    fn finish(mut self) -> Result<SortedEntries> {
        for k in &self.removes[self.next..] {
            self.sorted.push(k, 0, true, None)?;
        }
        Ok(self.sorted.shrink())
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
        let sorted = SortedEntries::of(
            &[b"c", b"a", b"e"],
            Some(&[Some(b"3"), None, Some(b"")]),
            &[b"d", b"b", b"d"],
        )
        .unwrap();
        let keys: Vec<&[u8]> = (0..sorted.len()).map(|i| sorted.key(i)).collect();
        assert_eq!(keys, [b"a", b"b", b"c", b"d", b"e"]);
        assert_eq!(sorted.removes(), 2);
        assert!(matches!(sorted.write(1), Write::Remove));
        assert!(matches!(sorted.write(0), Write::Upsert(None)));
        assert!(matches!(sorted.write(2), Write::Upsert(Some(b"3"))));
        assert!(matches!(sorted.write(4), Write::Upsert(Some(b""))));
        let back = SortedEntries::decode(&sorted.encode(O).unwrap(), 5, 1 << 20).unwrap();
        assert_eq!(back.keys.data, sorted.keys.data);
        assert_eq!(back.deleted, sorted.deleted);
        assert_eq!(back.has_payload, sorted.has_payload);
        assert_eq!(back.payloads.data, sorted.payloads.data);
        assert!(matches!(
            SortedEntries::decode(&sorted.encode(O).unwrap(), 4, 1 << 20),
            Err(Error::Limit(_))
        ));
        assert!(matches!(
            SortedEntries::decode(&sorted.encode(O).unwrap(), 5, 8),
            Err(Error::Limit(_))
        ));
        assert!(SortedEntries::of(&[b"a"], None, &[b"a"]).is_err());
        assert!(SortedEntries::of(&[b"a", b"a"], None, &[]).is_err());
    }
}
