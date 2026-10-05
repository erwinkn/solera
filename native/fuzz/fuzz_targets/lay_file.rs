//! Any bytes, read as a `.lay` file by every reader that takes bytes from
//! object storage — decoded at a stamp, scanned for Δ from a commit, looked
//! up, as a part's index, as a resolver request's run: an error, never a
//! panic, and never more memory than a block's limit allows. Each block's
//! CRC is checked before it decompresses, so the fuzzer reaches the entry
//! parsers only through inputs whose CRCs hold: seeds give it real files
//! to mutate (`seeds.py`).
#![no_main]

use std::sync::Arc;

use libfuzzer_sys::fuzz_target;
use solera_native::layers::{decode, decode_run, index_decode, lookup, scan, LayerStream, Stamp};
use solera_native::stream::Bytes;

fuzz_target!(|data: &[u8]| {
    let stamp = Stamp {
        commit: 9,
        generation: 1 << 40,
    };
    let _ = decode(data, stamp);
    let _ = decode(data, Stamp::default());
    let _ = index_decode(data);
    let _ = decode_run(data, 1 << 12, 1 << 20);
    let chunk: Bytes = Arc::new(data.to_vec());
    let mut out = Vec::new();
    let streams = vec![LayerStream::of(vec![chunk.clone()], stamp)];
    let _ = scan(streams, Some(3), None, None, 100, None, None, &mut out);
    let _ = lookup(vec![LayerStream::of(vec![chunk], stamp)], &[b"a", b"m", b"z"]);
});
