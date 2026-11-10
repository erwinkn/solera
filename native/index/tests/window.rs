//! A read of listed keys holds a bounded window of fetched bytes, however
//! densely its keys hit the layer (its own test binary: it measures the heap).

use object_store::local::LocalFileSystem;
use object_store::path::Path;
use solera_index::*;
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::sync::Arc;

struct Counting;

static NOW: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

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
    assert!(
        held < 40 << 20,
        "a get over a {} MB layer held {} MB",
        layer.size >> 20,
        held >> 20
    );
    let _ = std::fs::remove_dir_all(dir);
}
