//! Any entries, written as a layer's blocks and read back: the same entries,
//! whatever the block size; and a part's index lists its blocks exactly.
#![no_main]

use std::collections::BTreeMap;

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use solera_native::layers::{decode, index_decode, index_encode, BlockWriter, Entry, Stamp, LAYER};

#[derive(Arbitrary, Debug)]
struct Input {
    entries: BTreeMap<Vec<u8>, (bool, bool, u32, u32, Vec<u16>, Option<Vec<u8>>)>,
    block_size: u16,
}

fuzz_target!(|input: Input| {
    let entries: Vec<Entry> = input
        .entries
        .into_iter()
        .map(|(key, (present, start, commit, generation, gaps, payload))| {
            let commit = (commit as u64) + (1 << 16);
            let mut flips = Vec::new(); // newest first, at or below the last change
            let mut at = commit;
            for g in gaps {
                at = at.saturating_sub(g as u64);
                flips.push(at);
            }
            Entry {
                key,
                present,
                start,
                commit,
                generation: generation as u64,
                flips,
                payload: if present { payload } else { None },
                replaced: None,
            }
        })
        .collect();
    let mut w = BlockWriter::new(LAYER, input.block_size as usize, 1, usize::MAX);
    for e in &entries {
        w.push(e.clone()).unwrap();
    }
    w.finish().unwrap();
    let data: Vec<u8> = w.ready.drain(..).flat_map(|f| f.data).collect();
    assert_eq!(decode(&data, Stamp::default()).unwrap(), entries);
    assert_eq!(index_decode(&index_encode(&w.blocks)).unwrap(), w.blocks);
});
