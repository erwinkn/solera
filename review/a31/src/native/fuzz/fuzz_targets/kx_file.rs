//! Any bytes, read as a whole `.kx` file and as its parts by every reader
//! that takes bytes from object storage: an error, never a panic, and never
//! more memory than its limit allows. Each input runs twice: as it is, and
//! with its index's and filters' CRCs made to match, as a hostile file's
//! would, so mutations reach the parsers behind the checksums.
#![no_main]

use libfuzzer_sys::fuzz_target;
use solera_native::entries::SortedEntries;
use solera_native::format::{lookup, parse_filters, parse_index_at_most, FOOTER_SIZE};
use solera_native::stream::Block;

const LIMIT: u64 = 1 << 20;

/// `data` with the footer's index CRC and the filters' trailing CRC
/// recomputed, where the footer's offsets point inside it.
fn with_matching_crcs(data: &[u8]) -> Option<Vec<u8>> {
    let mut out = data.to_vec();
    let f = out.len().checked_sub(FOOTER_SIZE)?;
    let at = |o: usize, n: usize| -> Option<u64> {
        let mut le = [0u8; 8];
        le[..n].copy_from_slice(out.get(f + o..f + o + n)?);
        Some(u64::from_le_bytes(le))
    };
    let (filters_at, filters_len) = (at(16, 8)? as usize, at(24, 4)? as usize);
    let (index_at, index_len) = (at(28, 8)? as usize, at(36, 4)? as usize);
    if let Some(index) = out.get(index_at..index_at.checked_add(index_len)?) {
        let crc = crc32fast::hash(index).to_le_bytes();
        out[f + 40..f + 44].copy_from_slice(&crc);
    }
    if filters_len >= 4 {
        if let Some(end) = filters_at.checked_add(filters_len).filter(|&e| e <= f) {
            let crc = crc32fast::hash(&out[filters_at..end - 4]).to_le_bytes();
            out[end - 4..end].copy_from_slice(&crc);
        }
    }
    Some(out)
}

fuzz_target!(|data: &[u8]| {
    read_all(data);
    if let Some(fixed) = with_matching_crcs(data) {
        read_all(&fixed);
    }
});

fn read_all(data: &[u8]) {
    let size = data.len() as u64;
    let _ = parse_filters(data, size);
    let _ = SortedEntries::decode(data, 10_000, LIMIT);
    let Ok(idx) = parse_index_at_most(data, size, LIMIT) else {
        return;
    };
    let mut blocks = Vec::new();
    for &(_, offset, len, _, _) in &idx.blocks {
        let (Ok(at), Ok(len)) = (usize::try_from(offset), usize::try_from(len)) else {
            return;
        };
        let Some(b) = at.checked_add(len).and_then(|end| data.get(at..end)) else {
            return;
        };
        let _ = Block::decode_at_most(b, idx.footer.codec, LIMIT);
        blocks.push(b);
    }
    // `lookup` decodes without a limit: the blocks it is handed have passed
    // the index's checks, as here.
    let keys: [&[u8]; 3] = [b"", &idx.min_key, &idx.max_key];
    let mut keys = keys.to_vec();
    keys.sort();
    let _ = lookup(&blocks, idx.footer.codec, &keys);
}
