use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use bytes::Bytes;
use futures::future::{BoxFuture, FutureExt, Shared};
use futures::stream::{self, StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use pyo3::buffer::PyBuffer;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

type Range = (String, u64, u64);
type Plan = Vec<Vec<Range>>;
type Store = (Arc<dyn ObjectStore>, String);
type SharedGet = Shared<BoxFuture<'static, Result<Bytes, Arc<String>>>>;

static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
static STORES: Mutex<Option<HashMap<String, Store>>> = Mutex::new(None);
static CACHE: Mutex<Option<HashMap<(String, u64, u64), SharedGet>>> = Mutex::new(None);
static ITERS_PER_US: OnceLock<f64> = OnceLock::new();
static GETS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);
static CALLS: AtomicU64 = AtomicU64::new(0);

fn runtime() -> &'static tokio::runtime::Runtime {
    RUNTIME.get_or_init(|| {
        let workers = std::env::var("BENCH_A_WORKERS").ok().and_then(|v| v.parse().ok()).unwrap_or(4);
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(workers)
            .enable_all()
            .build()
            .unwrap()
    })
}

fn store(url: &str, options: Vec<(String, String)>) -> PyResult<Store> {
    let key = format!("{url}|{options:?}");
    let mut stores = STORES.lock().unwrap();
    let stores = stores.get_or_insert_with(HashMap::new);
    if let Some(s) = stores.get(&key) {
        return Ok(s.clone());
    }
    let parsed = url::Url::parse(url).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    let (s, prefix) = object_store::parse_url_opts(&parsed, options).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    let entry: Store = (Arc::from(s), prefix.to_string());
    stores.insert(key, entry.clone());
    Ok(entry)
}

fn spin(iters: u64) -> u64 {
    let mut x: u64 = 0x9E3779B97F4A7C15;
    for _ in 0..iters {
        x = std::hint::black_box(x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407));
    }
    x
}

fn iters_per_us() -> f64 {
    *ITERS_PER_US.get_or_init(|| {
        let n = 50_000_000;
        let t = Instant::now();
        std::hint::black_box(spin(n));
        n as f64 / t.elapsed().as_micros() as f64
    })
}

fn burn(bufs: &[&[u8]], decode_us_per_mb: f64) -> u64 {
    let mut acc = 0u64;
    let mut total = 0usize;
    for b in bufs {
        total += b.len();
        for w in b.chunks_exact(8) {
            acc ^= u64::from_le_bytes(w.try_into().unwrap());
        }
    }
    let iters = total as f64 / 1e6 * decode_us_per_mb * iters_per_us();
    acc ^ spin(iters as u64)
}

fn full_path(prefix: &str, rel: &str) -> Path {
    if prefix.is_empty() { Path::from(rel) } else { Path::from(format!("{prefix}/{rel}")) }
}

async fn get(store: Arc<dyn ObjectStore>, path: Path, start: u64, end: u64) -> Result<Bytes, String> {
    GETS.fetch_add(1, Relaxed);
    let b = store.get_range(&path, start..end).await.map_err(|e| e.to_string())?;
    BYTES.fetch_add(b.len() as u64, Relaxed);
    Ok(b)
}

async fn fetch(store: &Store, range: Range, cache: bool) -> Result<Bytes, String> {
    let path = full_path(&store.1, &range.0);
    if !cache {
        return get(store.0.clone(), path, range.1, range.2).await;
    }
    let shared = {
        let mut c = CACHE.lock().unwrap();
        let c = c.get_or_insert_with(HashMap::new);
        let s = store.0.clone();
        c.entry((path.to_string(), range.1, range.2))
            .or_insert_with(|| get(s, path, range.1, range.2).map(|r| r.map_err(Arc::new)).boxed().shared())
            .clone()
    };
    shared.await.map_err(|e| e.to_string())
}

async fn run_plan(store: Store, plan: Plan, decode_us: f64, concurrency: usize, cache: bool) -> Result<(u64, u64), String> {
    let mut fetched = 0u64;
    let mut check = 0u64;
    for round in plan {
        let bufs: Vec<Bytes> = stream::iter(round)
            .map(|r| fetch(&store, r, cache))
            .buffer_unordered(concurrency)
            .try_collect()
            .await?;
        fetched += bufs.iter().map(|b| b.len() as u64).sum::<u64>();
        check ^= tokio::task::spawn_blocking(move || {
            let slices: Vec<&[u8]> = bufs.iter().map(|b| b.as_ref()).collect();
            burn(&slices, decode_us)
        })
        .await
        .map_err(|e| e.to_string())?;
    }
    Ok((fetched, check))
}

/// Variant A, blocking: the whole query in Rust, GIL released.
#[pyfunction]
fn run_a(py: Python<'_>, url: &str, options: Vec<(String, String)>, plan: Plan, decode_us: f64, concurrency: usize, cache: bool) -> PyResult<(u64, u64)> {
    CALLS.fetch_add(1, Relaxed);
    let s = store(url, options)?;
    py.detach(|| runtime().block_on(run_plan(s, plan, decode_us, concurrency, cache)))
        .map_err(PyRuntimeError::new_err)
}

/// Variant A, awaitable: spawns the query and calls `done((fetched, check) | error_str)` from a
/// blocking-pool thread when it finishes. Python wraps it in an asyncio future.
#[pyfunction]
fn start_a(url: &str, options: Vec<(String, String)>, plan: Plan, decode_us: f64, concurrency: usize, cache: bool, done: Py<PyAny>) -> PyResult<()> {
    CALLS.fetch_add(1, Relaxed);
    let s = store(url, options)?;
    runtime().spawn(async move {
        let res = run_plan(s, plan, decode_us, concurrency, cache).await;
        tokio::task::spawn_blocking(move || {
            Python::attach(|py| {
                let r = match res {
                    Ok(v) => done.call1(py, (v,)),
                    Err(e) => done.call1(py, (e,)),
                };
                if let Err(e) = r {
                    e.print(py);
                }
            })
        });
    });
    Ok(())
}

struct Raw(*const u8, usize);
unsafe impl Send for Raw {}
unsafe impl Sync for Raw {}

/// Variant B: Rust holds the plan; Python fetches. `step` decodes the round's buffers
/// (zero-copy via the buffer protocol, GIL released) and returns the next round's ranges.
#[pyclass]
struct QueryB {
    plan: std::vec::IntoIter<Vec<Range>>,
    decode_us: f64,
    check: u64,
}

#[pymethods]
impl QueryB {
    #[new]
    fn new(plan: Plan, decode_us: f64) -> Self {
        QueryB { plan: plan.into_iter(), decode_us, check: 0 }
    }

    fn step(&mut self, py: Python<'_>, bufs: Vec<Bound<'_, PyAny>>) -> PyResult<Option<Vec<Range>>> {
        CALLS.fetch_add(1, Relaxed);
        if !bufs.is_empty() {
            let held: Vec<PyBuffer<u8>> = bufs.iter().map(PyBuffer::get).collect::<PyResult<_>>()?;
            let raws: Vec<Raw> = held.iter().map(|b| Raw(b.buf_ptr() as *const u8, b.len_bytes())).collect();
            let decode_us = self.decode_us;
            let c = py.detach(move || {
                let slices: Vec<&[u8]> = raws.iter().map(|r| unsafe { std::slice::from_raw_parts(r.0, r.1) }).collect();
                burn(&slices, decode_us)
            });
            self.check ^= c;
            drop(held);
        }
        Ok(self.plan.next())
    }
}

#[pyfunction]
fn calibrate() -> f64 {
    iters_per_us()
}

#[pyfunction]
fn stats() -> (u64, u64, u64) {
    (GETS.load(Relaxed), BYTES.load(Relaxed), CALLS.load(Relaxed))
}

#[pyfunction]
fn reset() {
    GETS.store(0, Relaxed);
    BYTES.store(0, Relaxed);
    CALLS.store(0, Relaxed);
    if let Some(c) = CACHE.lock().unwrap().as_mut() {
        c.clear();
    }
}

/// CPU burn alone (for calibration checks).
#[pyfunction]
fn burn_mb(py: Python<'_>, mb: f64, decode_us: f64) -> u64 {
    let n = (mb * 1e6) as usize;
    py.detach(|| {
        let v = vec![1u8; n];
        burn(&[&v], decode_us)
    })
}

#[pymodule]
fn index_io_bench(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(run_a, m)?)?;
    m.add_function(wrap_pyfunction!(start_a, m)?)?;
    m.add_function(wrap_pyfunction!(calibrate, m)?)?;
    m.add_function(wrap_pyfunction!(stats, m)?)?;
    m.add_function(wrap_pyfunction!(reset, m)?)?;
    m.add_function(wrap_pyfunction!(burn_mb, m)?)?;
    m.add_class::<QueryB>()?;
    Ok(())
}
