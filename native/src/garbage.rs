//! Garbage files (`.kg`, docs/key-index-format.md § Garbage files): the
//! entries a compaction dropped, for an immutable store to discard the
//! objects they name. Unlike a `.kx` file, one key may appear at several
//! generations — a merge can drop a key's entries from more than one level.

use std::collections::VecDeque;

use crate::format::{
    compress, decompress, fmt_err, get_bytes, get_varint, put_bytes, put_varint, Result,
};

pub const MAGIC: &[u8; 4] = b"CKG1";
pub const VERSION: u16 = 2;
pub const FOOTER_SIZE: usize = 24;
const BLOCK: usize = 64 * 1024;

/// Writes dropped entries as garbage files of about `max_file_bytes` each,
/// queued in `files` for the caller to take.
pub struct GarbageWriter {
    codec: u8,
    level: u32,
    max_file_bytes: usize,
    block: Vec<u8>,
    out: Vec<u8>,
    blocks: u32,
    entries: u64,
    pub files: VecDeque<Vec<u8>>,
    pub total: u64,
}

impl GarbageWriter {
    pub fn new(codec: u8, level: u32, max_file_bytes: usize) -> GarbageWriter {
        GarbageWriter {
            codec,
            level,
            max_file_bytes,
            block: Vec::with_capacity(BLOCK + 256),
            out: Vec::new(),
            blocks: 0,
            entries: 0,
            files: VecDeque::new(),
            total: 0,
        }
    }

    pub fn push(&mut self, key: &[u8], generation: u64) {
        put_bytes(&mut self.block, key);
        put_varint(&mut self.block, generation);
        self.entries += 1;
        self.total += 1;
        if self.block.len() >= BLOCK {
            self.close_block();
            if self.out.len() >= self.max_file_bytes {
                self.cut();
            }
        }
    }

    fn close_block(&mut self) {
        if self.block.is_empty() {
            return;
        }
        let data = compress(&self.block, self.codec, self.level);
        self.out
            .extend_from_slice(&(data.len() as u32).to_le_bytes());
        self.out
            .extend_from_slice(&crc32fast::hash(&data).to_le_bytes());
        self.out.extend_from_slice(&data);
        self.blocks += 1;
        self.block.clear();
    }

    fn cut(&mut self) {
        let mut out = std::mem::take(&mut self.out);
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&VERSION.to_le_bytes());
        out.push(self.codec);
        out.push(0);
        out.extend_from_slice(&self.entries.to_le_bytes());
        out.extend_from_slice(&self.blocks.to_le_bytes());
        out.extend_from_slice(MAGIC);
        self.files.push_back(out);
        self.blocks = 0;
        self.entries = 0;
    }

    /// Closes the last file, if it holds anything.
    pub fn finish(&mut self) {
        self.close_block();
        if self.entries > 0 {
            self.cut();
        }
    }
}

/// Every entry of a garbage file: keys, generations.
pub fn decode(data: &[u8]) -> Result<(Vec<Vec<u8>>, Vec<u64>)> {
    if data.len() < FOOTER_SIZE {
        return fmt_err("garbage file too short");
    }
    let foot = &data[data.len() - FOOTER_SIZE..];
    if &foot[0..4] != MAGIC || &foot[20..24] != MAGIC {
        return fmt_err("bad garbage file magic");
    }
    let version = u16::from_le_bytes([foot[4], foot[5]]);
    if version != VERSION {
        return fmt_err(format!("unsupported garbage file version {version}"));
    }
    let codec = foot[6];
    let entries = u64::from_le_bytes(foot[8..16].try_into().unwrap());
    let blocks = u32::from_le_bytes(foot[16..20].try_into().unwrap());
    let body = &data[..data.len() - FOOTER_SIZE];
    let (mut keys, mut generations) = (Vec::new(), Vec::new());
    let (mut pos, mut seen) = (0usize, 0u32);
    while pos < body.len() {
        if pos + 8 > body.len() {
            return fmt_err("truncated garbage block header");
        }
        let n = u32::from_le_bytes(body[pos..pos + 4].try_into().unwrap()) as usize;
        let crc = u32::from_le_bytes(body[pos + 4..pos + 8].try_into().unwrap());
        pos += 8;
        let Some(blk) = body.get(pos..pos + n) else {
            return fmt_err("truncated garbage block");
        };
        if crc32fast::hash(blk) != crc {
            return fmt_err("garbage block checksum mismatch");
        }
        pos += n;
        seen += 1;
        let raw = decompress(blk, codec)?;
        let mut p = 0;
        while p < raw.len() {
            keys.push(get_bytes(&raw, &mut p)?.to_vec());
            generations.push(get_varint(&raw, &mut p)?);
        }
    }
    if seen != blocks || keys.len() as u64 != entries {
        return fmt_err("garbage file counts do not match its footer");
    }
    Ok((keys, generations))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::CODEC_ZLIB;

    #[test]
    fn round_trip_splits_and_repeats_keys() {
        let mut w = GarbageWriter::new(CODEC_ZLIB, 1, 100_000);
        for i in 0..50_000u64 {
            let key = format!("k{:06}", i / 2); // each key twice: two dropped generations
            w.push(key.as_bytes(), i);
        }
        w.finish();
        assert!(w.files.len() > 1);
        let mut n = 0u64;
        for f in &w.files {
            let (k, g) = decode(f).unwrap();
            for i in 0..k.len() {
                let j = n + i as u64;
                assert_eq!(k[i], format!("k{:06}", j / 2).into_bytes());
                assert_eq!(g[i], j);
            }
            n += k.len() as u64;
        }
        assert_eq!(n, 50_000);
        let mut bad = w.files[0].clone();
        bad[10] ^= 1;
        assert!(decode(&bad).is_err());
    }
}
