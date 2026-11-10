//! Reads hold bounded memory however large the layer (their own test binary:
//! it measures the heap, one test at a time).

use object_store::local::LocalFileSystem;
use object_store::path::Path;
use solera_index::*;
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, Mutex};

struct Counting;

static NOW: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
static ONE_AT_A_TIME: Mutex<()> = Mutex::new(());

fn grew(n: usize) {
    let now = NOW.fetch_add(n, Relaxed) + n;
    PEAK.fetch_max(now, Relaxed);
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        grew(l.size());
        unsafe { System.alloc(l) }
    }

    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        NOW.fetch_sub(l.size(), Relaxed);
        unsafe { System.dealloc(p, l) }
    }

    unsafe fn realloc(&self, p: *mut u8, l: Layout, size: usize) -> *mut u8 {
        if size > l.size() {
            grew(size - l.size());
        } else {
            NOW.fetch_sub(l.size() - size, Relaxed);
        }
        unsafe { System.realloc(p, l, size) }
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

/// Keys and versions that do not compress, so the layer is as large as its
/// key count says.
fn mix(i: u64) -> u64 {
    let x = i.wrapping_mul(0x9e37_79b9_7f4a_7c15);
    (x ^ (x >> 29)).wrapping_mul(0xbf58_476d_1ce4_e5b9)
}

fn key(i: u64) -> Vec<u8> {
    format!("k{i:08}{:016x}", mix(i)).into_bytes()
}

#[test]
fn a_read_of_listed_keys_holds_a_bounded_window() {
    let _one = ONE_AT_A_TIME.lock().unwrap();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let dir = std::env::temp_dir().join(format!("solera-index-window-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let local = Arc::new(LocalFileSystem::new_with_prefix(&dir).unwrap());
    let opts = StoreOptions {
        memory_bytes: 0,
        ..Default::default()
    };
    let store = Store::new(local, Path::from(""), rt.handle().clone(), opts).unwrap();
    let ix = ChangeIndex::new(store, WriteOptions::default());
    let n = 5_000_000u64;
    let layer = rt.block_on(async {
        let mut run = Run::new();
        for i in 0..n {
            run.push_key(&key(i));
            run.push_entry(1, Version::Int(mix(i)), Version::Absent);
        }
        let nc = NewCommit {
            commit: 1,
            time_ms: 0,
            producer: b"",
            live_before: 0,
        };
        ix.write_commit("big", nc, &run).await.unwrap()
    });
    let keys: Vec<Vec<u8>> = (0..n).step_by(500).map(key).collect();
    let base = NOW.load(Relaxed);
    PEAK.store(base, Relaxed);
    let got = rt.block_on(ix.get(std::slice::from_ref(&layer), 1, &keys, &Query::default()));
    assert_eq!(got.unwrap().run.len(), keys.len());
    let held = PEAK.load(Relaxed) - base;
    let _ = std::fs::remove_dir_all(dir);
    assert!(
        held < 40 << 20,
        "a get over a {} MB layer held {} MB",
        layer.size >> 20,
        held >> 20
    );
}

#[test]
fn a_commit_count_allocates_only_for_the_headers_read() {
    let _one = ONE_AT_A_TIME.lock().unwrap();
    let mut raw = vec![0xff, 0xff, 0xff, 0x03];
    raw.resize(64 << 20, 0);
    let c = zstd::bulk::compress(&raw, 1).unwrap();
    let mut b = (raw.len() as u32).to_le_bytes().to_vec();
    b.extend((xxhash_rust::xxh3::xxh3_64(&c) as u32).to_le_bytes());
    b.extend(&c);
    drop(raw);
    let base = NOW.load(Relaxed);
    PEAK.store(base, Relaxed);
    let got = format::parse_commits(&b);
    let held = PEAK.load(Relaxed) - base;
    assert!(matches!(got, Err(Error::Format(_))), "{got:?}");
    assert!(
        held < 128 << 20,
        "a damaged commits section held {} MB",
        held >> 20
    );
}

#[test]
fn a_small_layer_of_long_keys_decodes_its_index_within_bounds() {
    let _one = ONE_AT_A_TIME.lock().unwrap();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let o = WriteOptions {
        block_bytes: 1,
        level: 1,
        seg_blocks: 1024,
    };
    let mut w = Writer::new(o, 1, 1, None).unwrap();
    let prefix = vec![b'p'; format::MAX_KEY - 8];
    for i in 0..6144u64 {
        let mut k = prefix.clone();
        k.extend_from_slice(format!("{i:08}").as_bytes());
        w.push(&k, 1, Version::Int(i), Version::Absent).unwrap();
    }
    let h = CommitHeader {
        commit: 1,
        added: 6144,
        live: 6144,
        ..Default::default()
    };
    let mut l = w.finish(&[h]).unwrap();
    l.path = "long".into();
    let mem = Arc::new(object_store::memory::InMemory::new());
    let opts = StoreOptions {
        memory_bytes: 0,
        ..Default::default()
    };
    let store = Store::new(mem.clone(), Path::from(""), rt.handle().clone(), opts).unwrap();
    let ix = ChangeIndex::new(store, WriteOptions::default());
    rt.block_on(async {
        use object_store::ObjectStoreExt;
        mem.put(&Path::from("long"), w.take().into()).await.unwrap();
    });
    drop(w);
    let base = NOW.load(Relaxed);
    PEAK.store(base, Relaxed);
    let got = rt.block_on(ix.commits(std::slice::from_ref(&l), 1, 1));
    let held = PEAK.load(Relaxed) - base;
    assert_eq!(got.unwrap().len(), 1);
    assert!(
        held < 2 * format::MAX_RAW,
        "the index of a {} KB layer held {} MB",
        l.size >> 10,
        held >> 20
    );
}
