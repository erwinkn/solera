//! A17 R10: one key may hold a version per live endpoint, so a span merge and
//! a page of a span read must not hold a key's whole history. Counted with
//! the allocator itself (live bytes, not RSS, which the allocator's own
//! caching blurs): what either job holds above its input does not grow with
//! the number of versions.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::sync::Arc;

use solera_native::format::{file_blocks, Options, CODEC_ZLIB};
use solera_native::jobs::{ReadAs, SpanMerge, SpanRead, Step};
use solera_native::stream::{Bytes, Segment, Writer};

struct Counting;

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(l) };
        if !p.is_null() {
            let now = LIVE.fetch_add(l.size(), Relaxed) + l.size();
            PEAK.fetch_max(now, Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        unsafe { System.dealloc(p, l) };
        LIVE.fetch_sub(l.size(), Relaxed);
    }
}

#[global_allocator]
static A: Counting = Counting;

const O: Options = Options {
    block_size: 65536,
    level: 1,
    bits_per_item: 14,
    k: 10,
    codec: CODEC_ZLIB,
};
const PAYLOAD: usize = 256 << 10;

/// One span's files: `versions` versions of one key, newest first, each a
/// 256 KiB payload no compression shrinks, then a few other keys.
fn span(versions: u64) -> Vec<Bytes> {
    let mut w = Writer::new(O, 4 << 20).repeating();
    let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut payload = vec![0u8; PAYLOAD];
    for g in (1..=versions).rev() {
        for b in payload.chunks_mut(8) {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            b.copy_from_slice(&x.to_le_bytes()[..b.len()]);
        }
        w.push(b"hot", g + 10, false, Some(&payload), None).unwrap();
    }
    for k in [b"x".as_slice(), b"y", b"z"] {
        w.push(k, 5, false, None, None).unwrap();
    }
    w.finish(true).unwrap();
    w.files.into_iter().map(|f| Arc::new(f) as Bytes).collect()
}

fn merge_held(files: &[Bytes]) -> usize {
    let base = LIVE.load(Relaxed);
    PEAK.store(base, Relaxed);
    let endpoints: Vec<u64> = (1..=10_000).collect(); // every version stays
    let mut job = SpanMerge::new(1, endpoints, false, O, 4 << 20);
    let mut fed = 0;
    loop {
        match job.step().unwrap() {
            Step::Run(r) => feed(&mut job.merge.runs[r], files, &mut fed),
            Step::File => drop(job.writer.files.pop_front()),
            Step::Done => break,
            _ => unreachable!(),
        }
    }
    PEAK.load(Relaxed) - base
}

fn read_held(files: &[Bytes]) -> usize {
    let base = LIVE.load(Relaxed);
    PEAK.store(base, Relaxed);
    let read = ReadAs::Changes { g_p: 0, g_n1: None };
    let mut job = SpanRead::new(1, read, None, 2);
    let mut fed = 0;
    loop {
        match job.step().unwrap() {
            Step::Run(r) => feed(&mut job.merge.runs[r], files, &mut fed),
            Step::Page => {}
            Step::Done => break,
            _ => unreachable!(),
        }
    }
    assert_eq!(
        job.page
            .iter()
            .map(|(k, _, _)| k.as_slice())
            .collect::<Vec<_>>(),
        [b"hot".as_slice(), b"x"]
    );
    PEAK.load(Relaxed) - base
}

fn feed(run: &mut solera_native::stream::Stream, files: &[Bytes], fed: &mut usize) {
    let Some(f) = files.get(*fed) else {
        run.end();
        return;
    };
    let data: &[u8] = (**f).as_ref();
    let (codec, blocks) = file_blocks(data).unwrap();
    run.feed(Segment {
        data: f.clone(),
        blocks: blocks
            .iter()
            .map(|b| (b.offset as usize, b.size as usize, b.crc))
            .collect(),
        codec,
    });
    *fed += 1;
}

#[test]
fn a_hot_keys_history_is_streamed_not_held() {
    // Both past the writer's batch of 64 blocks, so its buffers are the same:
    let small = span(256); // 64 MiB of one key's versions
    let big = span(1024); // 256 MiB
    assert!(big.len() > 16);
    let (merge_small, merge_big) = (merge_held(&small), merge_held(&big));
    let (read_small, read_big) = (read_held(&small), read_held(&big));
    let mib = |b: usize| b >> 20;
    eprintln!(
        "held (MiB): merge {} -> {}, read {} -> {}",
        mib(merge_small),
        mib(merge_big),
        mib(read_small),
        mib(read_big)
    );
    // Four times the history: what each job holds grows by less than a few versions.
    assert!(
        merge_big < merge_small + 8 * PAYLOAD,
        "{merge_small} -> {merge_big}"
    );
    assert!(
        read_big < read_small + 8 * PAYLOAD,
        "{read_small} -> {read_big}"
    );
}
