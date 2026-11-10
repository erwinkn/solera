//! Entries in flat buffers: keys back to back, one row per entry, versions as
//! integers or byte strings in a side arena. A run groups each key's entries,
//! newest first. Decoded blocks, a commit's input, query results and the
//! per-key view a walk hands out are all runs, so nothing is allocated per key.

use std::ops::Range;

/// The entry has a new version (else the commit removed the key).
pub const NEW: u8 = 1;
/// The entry has a replaced version (else the commit added the key).
pub const REPLACED: u8 = 2;
/// The new version is a byte string (else an integer).
pub const NEW_BYTES: u8 = 4;
/// The replaced version is a byte string (else an integer).
pub const REPLACED_BYTES: u8 = 8;
pub(crate) const KNOWN_FLAGS: u8 = NEW | REPLACED | NEW_BYTES | REPLACED_BYTES;

/// A version: an opaque token compared only for equality. The engine's own
/// versions are integers (an attempt's generation); a source's are strings.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Version<'a> {
    Absent,
    Int(u64),
    Bytes(&'a [u8]),
}

impl Version<'_> {
    pub fn is_absent(&self) -> bool {
        matches!(self, Version::Absent)
    }
}

#[derive(Default, Clone, Debug)]
pub struct Run {
    keys: Vec<u8>,
    key_end: Vec<u32>,
    ent_end: Vec<u32>,
    commit: Vec<u64>,
    flags: Vec<u8>,
    new: Vec<u64>,
    replaced: Vec<u64>,
    vbytes: Vec<u8>,
}

impl Run {
    pub fn new() -> Run {
        Run::default()
    }

    pub fn clear(&mut self) {
        self.keys.clear();
        self.key_end.clear();
        self.ent_end.clear();
        self.commit.clear();
        self.flags.clear();
        self.new.clear();
        self.replaced.clear();
        self.vbytes.clear();
    }

    /// Keys in the run.
    pub fn len(&self) -> usize {
        self.key_end.len()
    }

    pub fn is_empty(&self) -> bool {
        self.key_end.is_empty()
    }

    /// Entries in the run, all keys together.
    pub fn entries(&self) -> usize {
        self.commit.len()
    }

    /// Heap bytes held, for accounting.
    pub fn bytes(&self) -> usize {
        self.keys.capacity()
            + self.vbytes.capacity()
            + (self.key_end.capacity() + self.ent_end.capacity()) * 4
            + (self.commit.capacity() + self.new.capacity() + self.replaced.capacity()) * 8
            + self.flags.capacity()
    }

    #[inline]
    pub fn key(&self, i: usize) -> &[u8] {
        let a = if i == 0 {
            0
        } else {
            self.key_end[i - 1] as usize
        };
        &self.keys[a..self.key_end[i] as usize]
    }

    #[inline]
    pub fn last_key(&self) -> Option<&[u8]> {
        self.len().checked_sub(1).map(|i| self.key(i))
    }

    /// The rows of key `i`'s entries, newest first.
    #[inline]
    pub fn rows(&self, i: usize) -> Range<usize> {
        let a = if i == 0 {
            0
        } else {
            self.ent_end[i - 1] as usize
        };
        a..self.ent_end[i] as usize
    }

    #[inline]
    pub fn commit(&self, row: usize) -> u64 {
        self.commit[row]
    }

    #[inline]
    pub fn flags(&self, row: usize) -> u8 {
        self.flags[row]
    }

    #[inline]
    pub fn new_version(&self, row: usize) -> Version<'_> {
        self.version(self.flags[row], NEW, NEW_BYTES, self.new[row])
    }

    #[inline]
    pub fn replaced_version(&self, row: usize) -> Version<'_> {
        self.version(
            self.flags[row],
            REPLACED,
            REPLACED_BYTES,
            self.replaced[row],
        )
    }

    #[inline]
    fn version(&self, f: u8, present: u8, bytes: u8, v: u64) -> Version<'_> {
        if f & present == 0 {
            Version::Absent
        } else if f & bytes == 0 {
            Version::Int(v)
        } else {
            let (a, n) = ((v >> 32) as usize, (v & 0xffff_ffff) as usize);
            Version::Bytes(&self.vbytes[a..a + n])
        }
    }

    /// Start a new key; its entries follow with `push_entry`.
    #[inline]
    pub fn push_key(&mut self, key: &[u8]) {
        self.keys.extend_from_slice(key);
        self.key_end.push(self.keys.len() as u32);
        self.ent_end.push(self.commit.len() as u32);
    }

    /// Start a new key from the first `shared` bytes of the last key plus a
    /// suffix (prefix coding); `shared` is at most the last key's length.
    #[inline]
    pub(crate) fn push_key_prefixed(&mut self, shared: usize, suffix: &[u8]) {
        let n = self.key_end.len();
        let start = if n >= 2 {
            self.key_end[n - 2] as usize
        } else {
            0
        };
        self.keys.extend_from_within(start..start + shared);
        self.keys.extend_from_slice(suffix);
        self.key_end.push(self.keys.len() as u32);
        self.ent_end.push(self.commit.len() as u32);
    }

    /// An entry of the last key.
    #[inline]
    pub fn push_entry(&mut self, commit: u64, new: Version<'_>, replaced: Version<'_>) {
        let (fnew, vnew) = self.put_version(new, NEW, NEW_BYTES);
        let (frep, vrep) = self.put_version(replaced, REPLACED, REPLACED_BYTES);
        self.push_raw(commit, fnew | frep, vnew, vrep);
    }

    #[inline]
    pub(crate) fn push_raw(&mut self, commit: u64, flags: u8, new: u64, replaced: u64) {
        self.commit.push(commit);
        self.flags.push(flags);
        self.new.push(new);
        self.replaced.push(replaced);
        *self.ent_end.last_mut().expect("an entry needs a key") = self.commit.len() as u32;
    }

    /// Row `row` of `other`, as an entry of this run's last key.
    #[inline]
    pub fn push_entry_from(&mut self, other: &Run, row: usize) {
        self.push_entry(
            other.commit(row),
            other.new_version(row),
            other.replaced_version(row),
        );
    }

    /// Key `i` of `other` and its entries.
    pub fn push_key_from(&mut self, other: &Run, i: usize) {
        self.push_key(other.key(i));
        for row in other.rows(i) {
            self.push_entry_from(other, row);
        }
    }

    #[inline]
    pub(crate) fn put_version(&mut self, v: Version<'_>, present: u8, bytes: u8) -> (u8, u64) {
        match v {
            Version::Absent => (0, 0),
            Version::Int(n) => (present, n),
            Version::Bytes(b) => {
                let a = self.vbytes.len() as u64;
                self.vbytes.extend_from_slice(b);
                (present | bytes, a << 32 | b.len() as u64)
            }
        }
    }
}
