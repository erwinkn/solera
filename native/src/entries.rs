//! Sorted entries: a write's entries in key order — upserts, each with its
//! payload if it carries one, and removes — as the key index takes them end
//! to end. Built once (from lists, from rows, or from the transport form, a
//! delta file, every fact checked), read by every resolver, and encoded only
//! to cross the wire.

use crate::delta::Write;
use crate::error::{Error, Result};
use crate::rows::{Arena, Source};
use crate::sort::sort_order;
use crate::stream::State;

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

    /// Sorted entries from their transport form, one delta file
    /// (`layers::decode_run`): every block checked, at most `max_entries`
    /// entries and `max_bytes` bytes decoded (`Error::Limit` past either).
    pub fn decode(data: &[u8], max_entries: u64, max_bytes: u64) -> Result<SortedEntries> {
        crate::layers::decode_run(data, max_entries, max_bytes)
    }

    /// The transport form: one delta file (`layers::encode_run`).
    pub fn encode(&self, block_size: usize, level: i32) -> Result<Vec<u8>> {
        crate::layers::encode_run(self, block_size, level)
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
        let data = sorted.encode(64, 1).unwrap();
        let back = SortedEntries::decode(&data, 5, 1 << 20).unwrap();
        assert_eq!(back.keys.data, sorted.keys.data);
        assert_eq!(back.deleted, sorted.deleted);
        assert_eq!(back.has_payload, sorted.has_payload);
        assert_eq!(back.payloads.data, sorted.payloads.data);
        assert!(matches!(
            SortedEntries::decode(&data, 4, 1 << 20),
            Err(Error::Limit(_))
        ));
        assert!(matches!(
            SortedEntries::decode(&data, 5, 8),
            Err(Error::Limit(_))
        ));
        let mut flipped = data.clone();
        *flipped.last_mut().unwrap() ^= 1;
        assert!(matches!(
            SortedEntries::decode(&flipped, 5, 1 << 20),
            Err(Error::Format(_))
        ));
        assert!(SortedEntries::decode(&[], 0, 0).unwrap().is_empty());
        assert!(SortedEntries::of(&[b"a"], None, &[b"a"]).is_err());
        assert!(SortedEntries::of(&[b"a", b"a"], None, &[]).is_err());
    }
}
