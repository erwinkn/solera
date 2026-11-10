//! Reads and writes hold bounded memory however large the layer (their own
//! test binary: it measures the heap, one test at a time).

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
fn a_commit_writer_holds_parts_not_the_commit() {
    let _one = ONE_AT_A_TIME.lock().unwrap();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let dir = std::env::temp_dir().join(format!("solera-index-writer-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let local = Arc::new(LocalFileSystem::new_with_prefix(&dir).unwrap());
    let opts = StoreOptions {
        part_bytes: 5 << 20,
        ..Default::default()
    };
    let store = Store::new(local, Path::from(""), rt.handle().clone(), opts).unwrap();
    let ix = ChangeIndex::new(store, WriteOptions::default());
    let nc = NewCommit {
        commit: 1,
        time_ms: 0,
        producer: b"",
        live_before: 0,
    };
    let base = NOW.load(Relaxed);
    PEAK.store(base, Relaxed);
    let layer = rt.block_on(async {
        let mut w = ix.commit_writer("big", nc).unwrap();
        for i in 0..5_000_000u64 {
            w.push(&key(i), Version::Int(mix(i)), Version::Absent)
                .await
                .unwrap();
        }
        w.finish().await.unwrap().unwrap()
    });
    let held = PEAK.load(Relaxed) - base;
    assert!(
        held < 40 << 20,
        "a commit writer of a {} MB layer held {} MB",
        layer.size >> 20,
        held >> 20
    );
    let _ = std::fs::remove_dir_all(dir);
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
    let ls = std::slice::from_ref(&l);
    let first = Query {
        limit: Some(1),
        ..Default::default()
    };
    let some: Vec<Vec<u8>> = (0..6144u64)
        .step_by(7)
        .map(|i| [&prefix[..], format!("{i:08}").as_bytes()].concat())
        .collect();
    // a read holds one decoded segment of its own, besides its input
    let measure = |what: &str, read: &dyn Fn() -> usize, want: usize, input: usize| {
        let base = NOW.load(Relaxed);
        PEAK.store(base, Relaxed);
        assert_eq!(read(), want, "{what}");
        let held = PEAK.load(Relaxed) - base;
        assert!(
            held < 2 * format::MAX_RAW + input,
            "{what} of a {} KB layer held {} MB",
            l.size >> 10,
            held >> 20
        );
    };
    measure(
        "commits",
        &|| rt.block_on(ix.commits(ls, 1, 1)).unwrap().len(),
        1,
        0,
    );
    measure(
        "a scan of one key",
        &|| rt.block_on(ix.scan(ls, 1, &first)).unwrap().run.len(),
        1,
        0,
    );
    let q = Query::default();
    let input = some.iter().map(|k| k.len()).sum::<usize>() * 2;
    measure(
        "a get",
        &|| rt.block_on(ix.get(ls, 1, &some, &q)).unwrap().run.len(),
        some.len(),
        input,
    );
}

#[test]
fn a_read_of_a_layer_of_many_short_blocks_plans_a_bounded_window() {
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
    let n = 4_000_000u64;
    let mut w = Writer::new(o, 1, 1, None).unwrap();
    let mut bytes = Vec::new();
    for i in 0..n {
        w.push(&i.to_be_bytes(), 1, Version::Int(i), Version::Absent)
            .unwrap();
        if w.pending() > 1 << 20 {
            bytes.extend_from_slice(&w.take());
        }
    }
    let h = CommitHeader {
        commit: 1,
        added: n,
        live: n,
        ..Default::default()
    };
    let mut l = w.finish(&[h]).unwrap();
    bytes.extend_from_slice(&w.take());
    drop(w);
    l.path = "short".into();
    let mem = Arc::new(object_store::memory::InMemory::new());
    let opts = StoreOptions {
        memory_bytes: 0,
        ..Default::default()
    };
    let store = Store::new(mem.clone(), Path::from(""), rt.handle().clone(), opts).unwrap();
    let ix = ChangeIndex::new(store, WriteOptions::default());
    rt.block_on(async {
        use object_store::ObjectStoreExt;
        mem.put(&Path::from("short"), bytes.into()).await.unwrap();
    });
    let ls = std::slice::from_ref(&l);
    let some: Vec<Vec<u8>> = (0..n)
        .step_by(4001)
        .map(|i| i.to_be_bytes().to_vec())
        .collect();
    let measure = |what: &str, read: &dyn Fn() -> usize, want: usize| {
        let base = NOW.load(Relaxed);
        PEAK.store(base, Relaxed);
        assert_eq!(read(), want, "{what}");
        let held = PEAK.load(Relaxed) - base;
        assert!(
            held < 16 << 20,
            "{what} of a layer of {n} blocks held {} MB",
            held >> 20
        );
    };
    let first = Query {
        limit: Some(1),
        ..Default::default()
    };
    measure(
        "a scan of one key",
        &|| rt.block_on(ix.scan(ls, 1, &first)).unwrap().run.len(),
        1,
    );
    let q = Query::default();
    measure(
        "a get",
        &|| rt.block_on(ix.get(ls, 1, &some, &q)).unwrap().run.len(),
        some.len(),
    );
    let every = || {
        let mut e = rt.block_on(ix.entries(&l)).unwrap();
        let (mut group, mut seen) = (Run::new(), 0);
        while rt.block_on(e.next(&mut group)).unwrap() {
            seen += 1;
        }
        seen
    };
    measure("a read of every entry", &every, n as usize);
}

#[test]
fn reads_of_many_segments_hold_a_bounded_batch_of_them() {
    let _one = ONE_AT_A_TIME.lock().unwrap();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let o = WriteOptions {
        block_bytes: 1,
        level: 1,
        seg_blocks: 1,
    };
    let n = 2_000_000u64;
    let dir = std::env::temp_dir().join(format!("solera-index-sparse-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let mut f = std::fs::File::create(dir.join("sparse")).unwrap();
    let mut w = Writer::new(o, 1, 1, None).unwrap();
    for i in 0..n {
        w.push(&i.to_be_bytes(), 1, Version::Int(i), Version::Absent)
            .unwrap();
        if w.pending() > 1 << 20 {
            std::io::Write::write_all(&mut f, &w.take()).unwrap();
        }
    }
    let h = CommitHeader {
        commit: 1,
        added: n,
        live: n,
        ..Default::default()
    };
    let mut l = w.finish(&[h]).unwrap();
    std::io::Write::write_all(&mut f, &w.take()).unwrap();
    drop((w, f));
    l.path = "sparse".into();
    // files, not memory: a local GET allocates what it reads; the blocks
    // fetched ahead stay within WINDOW plus one max_get
    let local = Arc::new(LocalFileSystem::new_with_prefix(&dir).unwrap());
    let opts = StoreOptions {
        max_get: 4 << 20,
        ..Default::default()
    };
    let store = Store::new(local, Path::from(""), rt.handle().clone(), opts).unwrap();
    let ix = ChangeIndex::new(store, WriteOptions::default());
    let ls = std::slice::from_ref(&l);
    let q = Query::default();
    let one = [n.to_be_bytes()];
    assert_eq!(rt.block_on(ix.get(ls, 1, &one, &q)).unwrap().run.len(), 0);
    let some: Vec<Vec<u8>> = (0..n)
        .step_by(2001)
        .map(|i| i.to_be_bytes().to_vec())
        .collect();
    let base = NOW.load(Relaxed);
    PEAK.store(base, Relaxed);
    let got = rt.block_on(ix.get(ls, 1, &some, &q)).unwrap().run.len();
    let held = PEAK.load(Relaxed) - base;
    assert_eq!(got, some.len());
    assert!(
        held < 40 << 20,
        "a get over {} MB of index held {} MB",
        (l.top_start - l.index_start) >> 20,
        held >> 20
    );
    let first = Query {
        limit: Some(1),
        ..Default::default()
    };
    let base = NOW.load(Relaxed);
    PEAK.store(base, Relaxed);
    let got = rt.block_on(ix.scan(ls, 1, &first)).unwrap().run.len();
    let held = PEAK.load(Relaxed) - base;
    let _ = std::fs::remove_dir_all(&dir);
    assert_eq!(got, 1);
    assert!(
        held < 16 << 20,
        "a scan of one key over {n} segments held {} MB",
        held >> 20
    );
}

#[test]
fn a_read_that_joins_another_reads_fetch_keeps_only_its_own_blocks() {
    let _one = ONE_AT_A_TIME.lock().unwrap();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let dir = std::env::temp_dir().join(format!("solera-index-join-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let local = Arc::new(LocalFileSystem::new_with_prefix(&dir).unwrap());
    let n = 1_000_000u64;
    let layer = rt.block_on(async {
        let store = Store::new(
            local.clone(),
            Path::from(""),
            rt.handle().clone(),
            StoreOptions::default(),
        )
        .unwrap();
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
        ChangeIndex::new(store, WriteOptions::default())
            .write_commit("joined", nc, &run)
            .await
            .unwrap()
    });
    // slow GETs, so that the second read finds the first one's in flight
    let slow = Arc::new(object_store::throttle::ThrottledStore::new(
        LocalFileSystem::new_with_prefix(&dir).unwrap(),
        object_store::throttle::ThrottleConfig {
            wait_get_per_call: std::time::Duration::from_millis(500),
            ..Default::default()
        },
    ));
    let store = Store::new(
        slow,
        Path::from(""),
        rt.handle().clone(),
        StoreOptions::default(),
    )
    .unwrap();
    let ix = Arc::new(ChangeIndex::new(store, WriteOptions::default()));
    rt.block_on(ix.commits(std::slice::from_ref(&layer), 1, 1))
        .unwrap();
    let keys: Arc<Vec<Vec<u8>>> = Arc::new((0..n / 2).map(key).collect());
    let (ix2, l2, k2) = (ix.clone(), layer.clone(), keys.clone());
    let base = NOW.load(Relaxed);
    let bulk = rt.spawn(async move {
        let q = Query::default();
        ix2.get(std::slice::from_ref(&l2), 1, &k2, &q)
            .await
            .map(|p| p.run.len())
    });
    std::thread::sleep(std::time::Duration::from_millis(200));
    let mut entries = rt.block_on(ix.entries(&layer)).unwrap();
    let mut group = Run::new();
    assert!(rt.block_on(entries.next(&mut group)).unwrap());
    assert_eq!(rt.block_on(bulk).unwrap().unwrap(), n as usize / 2);
    drop(group);
    let held = NOW.load(Relaxed).saturating_sub(base);
    drop((entries, keys));
    let _ = std::fs::remove_dir_all(&dir);
    assert!(
        ix.store.stats.counts().waits > 0,
        "the reads did not share a fetch"
    );
    assert!(
        held < 4 << 20,
        "a paused read that joined a bulk read's fetches held {} MB",
        held >> 20
    );
}
