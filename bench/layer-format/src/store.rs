//! A simulated object store, as `solera.keys.io.ObjectIO` models one: every
//! GET waits 30 ms plus its bytes at 80 MB/s, at most 64 in flight. Ranges a
//! read asks for are coalesced across gaps under `COALESCE` and split at
//! `RANGE`; both formats go through the same planner.

use bytes::Bytes;
use std::collections::HashMap;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::Semaphore;

/// Ranges closer than this are fetched as one GET (`coalesce=`, bytes).
pub static COALESCE: AtomicU64 = AtomicU64::new(64 << 10);
pub const RANGE: u64 = 16 << 20;

#[derive(Clone)]
pub struct Store {
    objects: Arc<RwLock<HashMap<String, Bytes>>>,
    pub net: Option<(f64, f64)>, // (seconds per request, bytes per second); None: a warm local cache
    sem: Arc<Semaphore>,
    pub gets: Arc<AtomicU64>,
    pub bytes: Arc<AtomicU64>,
}

impl Store {
    pub fn new() -> Self {
        Store {
            objects: Default::default(),
            net: None,
            sem: Arc::new(Semaphore::new(64)),
            gets: Default::default(),
            bytes: Default::default(),
        }
    }

    /// The same objects, read cold (`Some(latency)`) or warm (`None`), with fresh counters.
    pub fn view(&self, net: Option<(f64, f64)>) -> Self {
        Store {
            net,
            gets: Default::default(),
            bytes: Default::default(),
            ..self.clone()
        }
    }

    pub fn put(&self, path: &str, data: Bytes) {
        self.objects.write().unwrap().insert(path.to_string(), data);
    }

    pub fn whole(&self, path: &str) -> Bytes {
        self.objects.read().unwrap()[path].clone()
    }

    pub fn counts(&self) -> (u64, u64) {
        (self.gets.load(Relaxed), self.bytes.load(Relaxed))
    }

    /// One GET: a fresh copy of the bytes, as a download would make.
    pub async fn get(&self, path: &str, r: Range<u64>) -> Bytes {
        let _permit = self.sem.acquire().await.unwrap();
        let data = {
            let objects = self.objects.read().unwrap();
            Bytes::copy_from_slice(&objects[path][r.start as usize..r.end as usize])
        };
        self.gets.fetch_add(1, Relaxed);
        self.bytes.fetch_add(data.len() as u64, Relaxed);
        if let Some((rtt, bw)) = self.net {
            tokio::time::sleep(Duration::from_secs_f64(rtt + data.len() as f64 / bw)).await;
        }
        data
    }

    /// The ranges, fetched as coalesced GETs in parallel: a sparse view of the object.
    pub async fn fetch(&self, path: &str, ranges: &[Range<u64>]) -> Sparse {
        let mut sorted: Vec<Range<u64>> =
            ranges.iter().filter(|r| r.end > r.start).cloned().collect();
        sorted.sort_by_key(|r| r.start);
        let mut merged: Vec<Range<u64>> = Vec::new();
        for r in sorted {
            match merged.last_mut() {
                Some(m)
                    if r.start <= m.end + COALESCE.load(Relaxed)
                        && r.end.max(m.end) - m.start <= RANGE =>
                {
                    m.end = m.end.max(r.end)
                }
                _ => merged.push(r),
            }
        }
        let mut split = Vec::new();
        for m in merged {
            let mut s = m.start;
            while s < m.end {
                split.push(s..(s + RANGE).min(m.end));
                s += RANGE;
            }
        }
        let parts =
            futures::future::join_all(split.iter().map(|r| self.get(path, r.clone()))).await;
        Sparse {
            parts: split.into_iter().map(|r| r.start).zip(parts).collect(),
        }
    }
}

/// Fetched ranges of one object, by their start.
#[derive(Default, Clone)]
pub struct Sparse {
    pub parts: Vec<(u64, Bytes)>,
}

impl Sparse {
    pub fn slice(&self, r: Range<u64>) -> Bytes {
        for (s, b) in &self.parts {
            if *s <= r.start && r.end <= s + b.len() as u64 {
                return b.slice((r.start - s) as usize..(r.end - s) as usize);
            }
        }
        panic!("range {r:?} was not fetched");
    }
}
