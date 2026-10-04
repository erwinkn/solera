//! Any entries, encoded into a `.kx` file and decoded back: the same
//! entries, whatever the block size and codec.
#![no_main]

use std::collections::BTreeMap;

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use solera_native::format::{decode_block, encode_file, file_blocks, Options};

#[derive(Arbitrary, Debug)]
struct Input {
    entries: BTreeMap<Vec<u8>, (u64, bool, Option<Vec<u8>>, Option<u64>)>,
    block_size: u16,
    zlib: bool,
}

fuzz_target!(|input: Input| {
    let keys: Vec<&[u8]> = input.entries.keys().map(Vec::as_slice).collect();
    let vals: Vec<_> = input.entries.values().collect();
    let generations: Vec<u64> = vals.iter().map(|v| v.0).collect();
    let deleted: Vec<u8> = vals.iter().map(|v| v.1 as u8).collect();
    let payloads: Vec<Option<&[u8]>> = vals.iter().map(|v| v.2.as_deref()).collect();
    let predecessors: Vec<Option<u64>> = vals.iter().map(|v| v.3).collect();
    let o = Options {
        block_size: input.block_size as usize + 1,
        level: 1,
        bits_per_item: 10,
        k: 7,
        codec: input.zlib as u8,
    };
    let file = encode_file(&keys, &generations, &deleted, &payloads, &predecessors, o).unwrap();
    let (codec, blocks) = file_blocks(&file).unwrap();
    let mut back = (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for b in blocks {
        let raw = &file[b.offset as usize..(b.offset + b.size) as usize];
        let ((k, g, d, p), pred) = decode_block(raw, codec).unwrap();
        back.0.extend(k);
        back.1.extend(g);
        back.2.extend(d);
        back.3.extend(p);
        back.4.extend(pred);
    }
    assert_eq!(back.0, keys);
    assert_eq!(back.1, generations);
    assert_eq!(back.2, deleted);
    assert_eq!(back.3, payloads.iter().map(|p| p.map(<[u8]>::to_vec)).collect::<Vec<_>>());
    assert_eq!(back.4, predecessors);
});
