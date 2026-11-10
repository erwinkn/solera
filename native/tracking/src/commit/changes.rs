//! The keys a write changed, in key order: each an upsert, with the version
//! word it carries if it has one (a source's etag, a dynamic partition's
//! empty version), or a removal. A worker hands them over in their transport
//! form, blocks the engine decodes one at a time, so a commit of any size is
//! built in bounded memory.
//!
//! Transport: `MAGIC`, then blocks `[raw_len u32][rows u32][clen u32][xxh3 u32]
//! [zstd]`. A block's rows: the key prefix-coded against the previous key
//! (shared length, suffix length, suffix), a kind byte, and for an upsert
//! with a version its length and bytes. Keys ascend strictly across blocks.

use crate::codec::{Dec, Enc};
use crate::error::{format, invalid, Result};
use xxhash_rust::xxh3::xxh3_64;

pub const MAGIC: [u8; 8] = *b"SOLCHG01";
const BLOCK_RAW: usize = 256 << 10;
const MAX_RAW: u32 = 16 << 20;
const HEADER: usize = 16;

const UPSERT: u8 = 0;
const UPSERT_VERSION: u8 = 1;
const REMOVE: u8 = 2;

/// What a write did to one key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Change<'a> {
    /// Written, with the version word it carries if any.
    Upsert(Option<&'a [u8]>),
    Removed,
}

/// Changed keys in key order, back to back.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Changes {
    keys: Vec<u8>,
    key_end: Vec<u32>,
    kind: Vec<u8>,
    vers: Vec<u8>,
    ver_end: Vec<u32>,
}

impl Changes {
    pub fn new() -> Changes {
        Changes::default()
    }

    /// Changes in any order; a key named twice is refused.
    pub fn of(items: &[(&[u8], Change<'_>)]) -> Result<Changes> {
        let mut order: Vec<usize> = (0..items.len()).collect();
        order.sort_unstable_by(|&a, &b| items[a].0.cmp(items[b].0));
        let mut c = Changes::new();
        for i in order {
            c.push(items[i].0, items[i].1)?;
        }
        Ok(c)
    }

    pub fn len(&self) -> usize {
        self.key_end.len()
    }

    pub fn is_empty(&self) -> bool {
        self.key_end.is_empty()
    }

    pub fn clear(&mut self) {
        self.keys.clear();
        self.key_end.clear();
        self.kind.clear();
        self.vers.clear();
        self.ver_end.clear();
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
    pub fn change(&self, i: usize) -> Change<'_> {
        match self.kind[i] {
            REMOVE => Change::Removed,
            UPSERT => Change::Upsert(None),
            _ => {
                let a = if i == 0 {
                    0
                } else {
                    self.ver_end[i - 1] as usize
                };
                Change::Upsert(Some(&self.vers[a..self.ver_end[i] as usize]))
            }
        }
    }

    pub fn last_key(&self) -> Option<&[u8]> {
        self.len().checked_sub(1).map(|i| self.key(i))
    }

    /// The next change; its key must follow the last one.
    pub fn push(&mut self, key: &[u8], change: Change<'_>) -> Result<()> {
        if let Some(last) = self.last_key() {
            if key <= last {
                return invalid(format!(
                    "{:?} {} {:?}: changed keys ascend, each once",
                    String::from_utf8_lossy(key),
                    if key == last { "repeats" } else { "after" },
                    String::from_utf8_lossy(last)
                ));
            }
        }
        self.keys.extend_from_slice(key);
        self.key_end.push(self.keys.len() as u32);
        self.kind.push(match change {
            Change::Upsert(None) => UPSERT,
            Change::Upsert(Some(v)) => {
                self.vers.extend_from_slice(v);
                UPSERT_VERSION
            }
            Change::Removed => REMOVE,
        });
        self.ver_end.push(self.vers.len() as u32);
        Ok(())
    }

    /// The transport form.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = MAGIC.to_vec();
        let mut raw = Enc::new();
        let mut rows = 0u32;
        let mut prev: &[u8] = &[];
        for i in 0..self.len() {
            let k = self.key(i);
            let shared = k.iter().zip(prev).take_while(|(a, b)| a == b).count();
            raw.uv(shared as u64);
            raw.bytes(&k[shared..]);
            match self.change(i) {
                Change::Upsert(None) => raw.u8(UPSERT),
                Change::Upsert(Some(v)) => {
                    raw.u8(UPSERT_VERSION);
                    raw.bytes(v);
                }
                Change::Removed => raw.u8(REMOVE),
            }
            rows += 1;
            prev = k;
            if raw.buf.len() >= BLOCK_RAW {
                block(&mut out, &raw.buf, rows);
                raw.buf.clear();
                rows = 0;
                prev = &[];
            }
        }
        if rows > 0 {
            block(&mut out, &raw.buf, rows);
        }
        out
    }
}

fn block(out: &mut Vec<u8>, raw: &[u8], rows: u32) {
    let c = zstd::bulk::compress(raw, 1).expect("zstd compression");
    for v in [raw.len() as u32, rows, c.len() as u32, xxh3_64(&c) as u32] {
        out.extend_from_slice(&v.to_le_bytes());
    }
    out.extend_from_slice(&c);
}

/// Where a commit's changes come from, a chunk at a time, in key order.
pub trait Source {
    /// Fills \`out\` (cleared first) with the next chunk; false when none is left.
    fn next(&mut self, out: &mut Changes) -> Result<bool>;
}

/// Changes held in memory: one chunk.
pub struct Held<'a>(pub Option<&'a Changes>);

impl Source for Held<'_> {
    fn next(&mut self, out: &mut Changes) -> Result<bool> {
        out.clear();
        match self.0.take() {
            Some(c) => {
                out.clone_from(c);
                Ok(true)
            }
            None => Ok(false),
        }
    }
}

/// The transport form, decoded a block at a time, every block checked.
pub struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
    last: Option<Vec<u8>>,
    raw: Vec<u8>,
    z: zstd::bulk::Decompressor<'static>,
}

impl<'a> Reader<'a> {
    pub fn new(data: &'a [u8]) -> Result<Reader<'a>> {
        if data.get(..MAGIC.len()) != Some(&MAGIC[..]) {
            return format("not a changed-keys file (bad magic)");
        }
        Ok(Reader {
            data,
            pos: MAGIC.len(),
            last: None,
            raw: Vec::new(),
            z: zstd::bulk::Decompressor::new().expect("zstd decompressor"),
        })
    }
}

impl Source for Reader<'_> {
    fn next(&mut self, out: &mut Changes) -> Result<bool> {
        out.clear();
        if self.pos == self.data.len() {
            return Ok(false);
        }
        let Some(h) = self.data.get(self.pos..self.pos + HEADER) else {
            return format("truncated block header");
        };
        let u = |i: usize| u32::from_le_bytes(h[i..i + 4].try_into().unwrap());
        let (rlen, rows, clen, check) = (u(0), u(4), u(8) as usize, u(12));
        let start = self.pos + HEADER;
        let Some(c) = self.data.get(start..start + clen) else {
            return format("truncated block");
        };
        if xxh3_64(c) as u32 != check {
            return format("block checksum mismatch");
        }
        if rlen > MAX_RAW || rows == 0 || rows > rlen {
            return format("bad block header");
        }
        self.raw.clear();
        self.raw.reserve(rlen as usize);
        let n = self
            .z
            .decompress_to_buffer(c, &mut self.raw)
            .map_err(|e| crate::Error::Format(format!("zstd: {e}")))?;
        if n != rlen as usize {
            return format("block length mismatch");
        }
        let mut d = Dec::new(&self.raw);
        let mut key: Vec<u8> = Vec::new();
        for _ in 0..rows {
            let shared = d.uv()? as usize;
            let suffix = d.bytes()?;
            if shared > key.len() {
                return format("bad key prefix");
            }
            key.truncate(shared);
            key.extend_from_slice(suffix);
            let change = match d.u8()? {
                UPSERT => Change::Upsert(None),
                UPSERT_VERSION => Change::Upsert(Some(d.bytes()?)),
                REMOVE => Change::Removed,
                k => return format(format!("bad change kind {k}")),
            };
            if self.last.as_deref().is_some_and(|l| key.as_slice() <= l) {
                return format("changed keys do not ascend");
            }
            out.push(&key, change)
                .map_err(|_| crate::Error::Format("changed keys do not ascend".into()))?;
        }
        self.last = out.last_key().map(<[u8]>::to_vec);
        d.end()?;
        self.pos = start + clen;
        Ok(true)
    }
}
