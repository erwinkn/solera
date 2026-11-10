//! The I/O under the index: range GETs on an object store, coalesced; one
//! in-process cache, shared by every query, of blocks and parsed metadata; and
//! single flight, so that concurrent queries needing the same block or
//! segment wait on one GET instead of each making its own.

use crate::disk::{Disk, DiskOptions, Temp};
use crate::error::{Error, Result};
use crate::format::{parse_segment, Index, LayerRef, Top};
use bytes::Bytes;
use futures::future::{BoxFuture, FutureExt, Shared};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, WriteMultipart};
use std::collections::{BTreeMap, HashMap};
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use tokio::sync::Semaphore;

#[derive(Clone, Debug)]
pub struct StoreOptions {
    /// Bytes of blocks and metadata kept in memory, shared by every query.
    pub memory_bytes: u64,
    /// Ranges closer than this are fetched as one GET.
    pub coalesce: u64,
    /// A GET never spans more than this.
    pub max_get: u64,
    /// Metadata under this size is read in one GET with the top level.
    pub one_get: u64,
    /// GETs in flight at once.
    pub max_inflight: usize,
    /// Uploads larger than this go in parts of this size.
    pub part_bytes: usize,
    /// Blocks fetched in batches up to this size are cached; larger batches
    /// are bulk reads (a scan, a merge) and leave the cache alone.
    pub cache_batch: u64,
    /// Keep whole layer files on local disk (for a remote store).
    pub disk: Option<DiskOptions>,
}

impl Default for StoreOptions {
    fn default() -> Self {
        StoreOptions {
            memory_bytes: 256 << 20,
            coalesce: 64 << 10,
            max_get: 16 << 20,
            one_get: 1 << 20,
            max_inflight: 64,
            part_bytes: 16 << 20,
            cache_batch: 8 << 20,
            disk: None,
        }
    }
}

/// Counters since the store was made.
#[derive(Default, Debug)]
pub struct Stats {
    pub gets: AtomicU64,
    pub get_bytes: AtomicU64,
    pub puts: AtomicU64,
    pub put_bytes: AtomicU64,
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    pub waits: AtomicU64,
    pub disk_reads: AtomicU64,
    pub disk_bytes: AtomicU64,
    pub fills: AtomicU64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct Counts {
    pub gets: u64,
    pub get_bytes: u64,
    pub puts: u64,
    pub put_bytes: u64,
    pub hits: u64,
    pub misses: u64,
    pub waits: u64,
    pub disk_reads: u64,
    pub disk_bytes: u64,
    pub fills: u64,
}

impl Stats {
    pub fn counts(&self) -> Counts {
        Counts {
            gets: self.gets.load(Relaxed),
            get_bytes: self.get_bytes.load(Relaxed),
            puts: self.puts.load(Relaxed),
            put_bytes: self.put_bytes.load(Relaxed),
            hits: self.hits.load(Relaxed),
            misses: self.misses.load(Relaxed),
            waits: self.waits.load(Relaxed),
            disk_reads: self.disk_reads.load(Relaxed),
            disk_bytes: self.disk_bytes.load(Relaxed),
            fills: self.fills.load(Relaxed),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Part {
    Top,
    Seg(u32),
    Block(u64),
}

type Key = (Arc<str>, Part);

#[derive(Clone)]
pub(crate) enum Cached {
    Block(Bytes),
    Top(Arc<Top>),
    Seg(Arc<Index>),
}

impl Cached {
    fn weight(&self) -> usize {
        64 + match self {
            Cached::Block(b) => b.len(),
            Cached::Top(t) => t.bytes(),
            Cached::Seg(s) => s.bytes(),
        }
    }
}

/// What a fetched range is, to check and parse it.
#[derive(Clone, Debug)]
pub(crate) enum Want {
    Top {
        layer: LayerRef,
    },
    Seg {
        rlen: u32,
        check: u32,
        blocks: u32,
    },
    /// `keep`: cache it. A bulk read (a batch past `cache_batch`) does not,
    /// so that a scan or a merge leaves the planners' blocks where they are.
    Block {
        check: u32,
        keep: bool,
    },
}

type Load = Shared<BoxFuture<'static, std::result::Result<Arc<Vec<Cached>>, Error>>>;

struct Lru {
    map: HashMap<Key, (Cached, u64, usize)>,
    order: BTreeMap<u64, Key>,
    tick: u64,
    total: usize,
    budget: usize,
}

impl Lru {
    fn get(&mut self, k: &Key) -> Option<Cached> {
        let e = self.map.get_mut(k)?;
        self.order.remove(&e.1);
        self.tick += 1;
        e.1 = self.tick;
        self.order.insert(self.tick, k.clone());
        Some(e.0.clone())
    }

    fn insert(&mut self, k: Key, v: Cached) {
        let w = v.weight();
        if w > self.budget {
            return;
        }
        if let Some(old) = self.map.remove(&k) {
            self.order.remove(&old.1);
            self.total -= old.2;
        }
        self.tick += 1;
        self.order.insert(self.tick, k.clone());
        self.map.insert(k, (v, self.tick, w));
        self.total += w;
        while self.total > self.budget {
            let Some((_, k)) = self.order.pop_first() else {
                break;
            };
            if let Some(e) = self.map.remove(&k) {
                self.total -= e.2;
            }
        }
    }
}

struct State {
    lru: Lru,
    inflight: HashMap<Key, (Load, usize)>,
}

pub struct Store {
    inner: Arc<dyn ObjectStore>,
    prefix: Path,
    pub(crate) rt: tokio::runtime::Handle,
    pub(crate) opts: StoreOptions,
    state: Mutex<State>,
    sem: Semaphore,
    disk: Option<Disk>,
    pub stats: Stats,
}

impl Store {
    /// A store over `inner`, every path under `prefix`; `rt` runs prefetches
    /// and uploads. A disk cache knows the store by `inner`'s description and
    /// `prefix` (for S3, the bucket but not the endpoint): give each store its
    /// own `dir`.
    pub fn new(
        inner: Arc<dyn ObjectStore>,
        prefix: Path,
        rt: tokio::runtime::Handle,
        opts: StoreOptions,
    ) -> Result<Arc<Store>> {
        let id = format!("{inner}/{prefix}");
        Store::open(inner, prefix, rt, opts, &id)
    }

    fn open(
        inner: Arc<dyn ObjectStore>,
        prefix: Path,
        rt: tokio::runtime::Handle,
        opts: StoreOptions,
        id: &str,
    ) -> Result<Arc<Store>> {
        Ok(Arc::new(Store {
            disk: opts.disk.clone().map(|o| Disk::open(o, id)).transpose()?,
            inner,
            prefix,
            rt,
            sem: Semaphore::new(opts.max_inflight),
            state: Mutex::new(State {
                lru: Lru {
                    map: HashMap::new(),
                    order: BTreeMap::new(),
                    tick: 0,
                    total: 0,
                    budget: opts.memory_bytes as usize,
                },
                inflight: HashMap::new(),
            }),
            opts,
            stats: Stats::default(),
        }))
    }

    /// A store from a URL (`file:///…`, `s3://bucket/prefix`, …) and its
    /// options, as `object_store::parse_url_opts` reads them. A disk cache
    /// knows the store by its URL and endpoint.
    pub fn from_url(
        url: &str,
        options: impl IntoIterator<Item = (String, String)>,
        rt: tokio::runtime::Handle,
        opts: StoreOptions,
    ) -> Result<Arc<Store>> {
        let u = url::Url::parse(url).map_err(|e| Error::Invalid(format!("{url}: {e}")))?;
        let options: Vec<(String, String)> = options.into_iter().collect();
        let mut endpoints: Vec<&str> = options
            .iter()
            .filter(|(k, _)| k.to_ascii_lowercase().contains("endpoint"))
            .map(|(_, v)| v.as_str())
            .collect();
        endpoints.sort();
        let id = format!("{u} {}", endpoints.join(" "));
        let (inner, prefix) = object_store::parse_url_opts(&u, options)?;
        Store::open(Arc::from(inner), prefix, rt, opts, &id)
    }

    fn path(&self, path: &str) -> Result<Path> {
        let p = Path::parse(path).map_err(|e| Error::Invalid(format!("{path}: {e}")))?;
        Ok(self.prefix.parts().chain(p.parts()).collect())
    }

    /// One GET.
    pub(crate) async fn get_range(&self, path: &str, r: Range<u64>) -> Result<Bytes> {
        if let Some(d) = self.disk.as_ref().filter(|d| d.has(path)) {
            let (file, range) = (d.file(path), r.clone());
            let got = self
                .rt
                .spawn_blocking(move || crate::disk::read_at(&file, range))
                .await;
            match got {
                Ok(Ok(b)) => {
                    self.stats.disk_reads.fetch_add(1, Relaxed);
                    self.stats.disk_bytes.fetch_add(b.len() as u64, Relaxed);
                    return Ok(Bytes::from(b));
                }
                _ => d.forget(path),
            }
        }
        let p = self.path(path)?;
        let _permit = self.sem.acquire().await.expect("semaphore");
        let b = self
            .inner
            .get_ranges(&p, std::slice::from_ref(&r))
            .await?
            .pop()
            .unwrap_or_default();
        if b.len() as u64 != r.end - r.start {
            return Err(Error::Format(format!("{path}: short read at {r:?}")));
        }
        self.stats.gets.fetch_add(1, Relaxed);
        self.stats.get_bytes.fetch_add(b.len() as u64, Relaxed);
        Ok(b)
    }

    /// A layer's top level, read once and cached; with small metadata, its
    /// segments come in the same GET.
    pub(crate) async fn top(self: &Arc<Self>, l: &LayerRef) -> Result<Arc<Top>> {
        l.check()?;
        let whole = l.size - l.index_start <= self.opts.one_get;
        let start = if whole { l.index_start } else { l.top_start };
        let want = Want::Top { layer: l.clone() };
        let got = self
            .load(
                &Arc::from(l.path.as_str()),
                vec![(Part::Top, start..l.size, want)],
            )
            .await?;
        let Cached::Top(t) = &got[0] else {
            unreachable!()
        };
        t.check(l)?;
        self.fill(l);
        Ok(t.clone())
    }

    /// Fetch what is wanted of one object, from the cache, from a GET already
    /// in flight, or by new GETs (coalesced); in the order asked.
    pub(crate) async fn load(
        self: &Arc<Self>,
        path: &Arc<str>,
        wants: Vec<(Part, Range<u64>, Want)>,
    ) -> Result<Vec<Cached>> {
        let n = wants.len();
        let mut out: Vec<Option<Cached>> = vec![None; n];
        let mut waits: Vec<(usize, Load, usize)> = Vec::new();
        {
            let mut st = self.state.lock().unwrap();
            let mut claims: Vec<usize> = Vec::new();
            for (i, (part, ..)) in wants.iter().enumerate() {
                let key = (path.clone(), part.clone());
                if let Some(v) = st.lru.get(&key) {
                    out[i] = Some(v);
                    self.stats.hits.fetch_add(1, Relaxed);
                } else if let Some((f, j)) = st.inflight.get(&key) {
                    waits.push((i, f.clone(), *j));
                    self.stats.waits.fetch_add(1, Relaxed);
                } else {
                    claims.push(i);
                    self.stats.misses.fetch_add(1, Relaxed);
                }
            }
            claims.sort_by_key(|&i| wants[i].1.start);
            let mut g = 0;
            while g < claims.len() {
                let mut span = wants[claims[g]].1.clone();
                let mut e = g + 1;
                while e < claims.len() {
                    let r = &wants[claims[e]].1;
                    if r.start > span.end + self.opts.coalesce
                        || r.end.max(span.end) - span.start > self.opts.max_get
                    {
                        break;
                    }
                    span.end = span.end.max(r.end);
                    e += 1;
                }
                let items: Vec<(Part, Range<u64>, Want)> =
                    claims[g..e].iter().map(|&i| wants[i].clone()).collect();
                let task = self.rt.spawn(self.clone().fetch(path.clone(), span, items));
                let fut = async move {
                    task.await
                        .unwrap_or_else(|e| Err(Error::Store(format!("a read failed: {e}").into())))
                }
                .boxed()
                .shared();
                for (j, &i) in claims[g..e].iter().enumerate() {
                    st.inflight
                        .insert((path.clone(), wants[i].0.clone()), (fut.clone(), j));
                    waits.push((i, fut.clone(), j));
                }
                g = e;
            }
        }
        let done = futures::future::join_all(waits.iter().map(|w| w.1.clone())).await;
        for ((i, _, j), r) in waits.into_iter().zip(done) {
            out[i] = Some(r?[j].clone());
        }
        Ok(out
            .into_iter()
            .map(|v| v.expect("every want answered"))
            .collect())
    }

    async fn fetch(
        self: Arc<Self>,
        path: Arc<str>,
        span: Range<u64>,
        items: Vec<(Part, Range<u64>, Want)>,
    ) -> std::result::Result<Arc<Vec<Cached>>, Error> {
        let _claim = Claim {
            store: &self,
            path: &path,
            parts: &items,
        };
        let got = self.get_range(&path, span.clone()).await;
        let mut extra: Vec<(Part, Cached)> = Vec::new();
        let parsed: Result<Vec<Cached>> = got.and_then(|b| {
            items
                .iter()
                .map(|(_, r, w)| {
                    let piece =
                        b.slice((r.start - span.start) as usize..(r.end - span.start) as usize);
                    parse(&path, w, piece, r.start, &mut extra)
                })
                .collect()
        });
        let parsed = parsed?;
        let mut st = self.state.lock().unwrap();
        for ((part, _, w), v) in items.iter().zip(&parsed) {
            if !matches!(w, Want::Block { keep: false, .. }) {
                st.lru.insert((path.clone(), part.clone()), v.clone());
            }
        }
        for (part, v) in extra {
            st.lru.insert((path.clone(), part), v);
        }
        Ok(Arc::new(parsed))
    }

    /// With a disk cache, fetch a layer's whole file to it in the background,
    /// unless it is there, being fetched, or too large.
    fn fill(self: &Arc<Self>, l: &LayerRef) {
        let Some(d) = &self.disk else { return };
        if !d.claim(&l.path, l.size) {
            return;
        }
        let (store, path, size) = (self.clone(), l.path.clone(), l.size);
        self.rt.spawn(async move {
            let d = store.disk.as_ref().unwrap();
            if let Err(e) = store.download(&path, size).await {
                eprintln!("solera-index: disk cache fill of {path} failed: {e}");
            }
            d.unclaim(&path);
        });
    }

    async fn download(&self, path: &str, size: u64) -> Result<()> {
        let d = self.disk.as_ref().unwrap();
        let io = |e: std::io::Error| Error::Store(format!("disk cache: {e}").into());
        let mut t = d.temp().map_err(io)?;
        let mut at = 0;
        while at < size {
            let end = (at + self.opts.max_get).min(size);
            let b = self.get_range(path, at..end).await?;
            t.write(&b).map_err(io)?;
            at = end;
        }
        d.commit(t, path, size).map_err(io)?;
        self.stats.fills.fetch_add(1, Relaxed);
        Ok(())
    }

    /// Bytes of the disk cache in use.
    pub fn disk_bytes(&self) -> u64 {
        self.disk.as_ref().map_or(0, |d| d.total())
    }

    /// Start writing an object; its bytes go up in parts once past
    /// `part_bytes`, and to the disk cache as they go.
    pub(crate) fn upload(self: &Arc<Self>, path: &str) -> Result<Upload> {
        let tee = match &self.disk {
            Some(d) => Some(
                d.temp()
                    .map_err(|e| Error::Store(format!("disk cache: {e}").into()))?,
            ),
            None => None,
        };
        Ok(Upload {
            store: self.clone(),
            path: self.path(path)?,
            name: path.to_string(),
            buf: Vec::new(),
            parts: None,
            tee,
            size: 0,
        })
    }
}

/// A fetch's claim on its parts: while it lives, other reads of them wait
/// on the fetch; it ends however the fetch does (done, failed, panicked).
struct Claim<'a> {
    store: &'a Store,
    path: &'a Arc<str>,
    parts: &'a [(Part, Range<u64>, Want)],
}

impl Drop for Claim<'_> {
    fn drop(&mut self) {
        let mut st = self.store.state.lock().unwrap_or_else(|e| e.into_inner());
        for (part, ..) in self.parts {
            st.inflight.remove(&(self.path.clone(), part.clone()));
        }
    }
}

fn parse(
    path: &str,
    w: &Want,
    b: Bytes,
    start: u64,
    extra: &mut Vec<(Part, Cached)>,
) -> Result<Cached> {
    let at = |e: Error| match e {
        Error::Format(m) => Error::Format(format!("{path}: {m}")),
        e => e,
    };
    Ok(match w {
        Want::Block { check, keep } => {
            if crate::format::checksum(&b) != *check {
                return Err(Error::Format(format!(
                    "{path}: block checksum mismatch at {start}"
                )));
            }
            Cached::Block(if *keep { Bytes::copy_from_slice(&b) } else { b })
        }
        Want::Seg {
            rlen,
            check,
            blocks,
        } => Cached::Seg(Arc::new(
            parse_segment(&b, *rlen, *check, *blocks).map_err(at)?,
        )),
        Want::Top { layer } => {
            let tail = &b[(layer.top_start - start) as usize..];
            let top = Top::parse(tail, layer.top_start, layer.size).map_err(at)?;
            top.check(layer)?;
            if start == layer.index_start {
                for i in 0..top.segs.len() {
                    let (a, n) = (
                        (top.segs.off[i] - start) as usize,
                        top.segs.clen[i] as usize,
                    );
                    let Some(c) = b.get(a..a + n) else {
                        return Err(Error::Format(format!(
                            "{path}: segment {i} outside the index"
                        )));
                    };
                    let seg = parse_segment(c, top.segs.rlen[i], top.segs.check[i], top.segs.n[i])
                        .map_err(at)?;
                    extra.push((Part::Seg(i as u32), Cached::Seg(Arc::new(seg))));
                }
            }
            Cached::Top(Arc::new(top))
        }
    })
}

/// An object being written: one PUT if it stays small, else a multipart upload.
pub(crate) struct Upload {
    store: Arc<Store>,
    path: Path,
    name: String,
    buf: Vec<u8>,
    parts: Option<WriteMultipart>,
    tee: Option<Temp>,
    size: u64,
}

impl Drop for Upload {
    /// An upload dropped unfinished (an error, a cancelled merge) gives its
    /// parts back: S3 keeps them, billed, until aborted.
    fn drop(&mut self) {
        if let Some(w) = self.parts.take() {
            self.store.rt.spawn(async move {
                let _ = w.abort().await;
            });
        }
    }
}

impl Upload {
    pub(crate) async fn write(&mut self, mut bytes: Vec<u8>) -> Result<()> {
        self.size += bytes.len() as u64;
        if let Some(t) = &mut self.tee {
            t.write(&bytes)
                .map_err(|e| Error::Store(format!("disk cache: {e}").into()))?;
        }
        let part = self.store.opts.part_bytes;
        if self.parts.is_none() {
            if self.buf.is_empty() {
                self.buf = std::mem::take(&mut bytes);
            } else {
                self.buf.extend_from_slice(&bytes);
            }
            if self.buf.len() < part {
                return Ok(());
            }
            let up = self.store.inner.put_multipart(&self.path).await?;
            self.parts = Some(WriteMultipart::new_with_chunk_size(up, part));
            bytes = std::mem::take(&mut self.buf);
        }
        let w = self.parts.as_mut().unwrap();
        self.store
            .stats
            .put_bytes
            .fetch_add(bytes.len() as u64, Relaxed);
        w.put(Bytes::from(bytes));
        w.wait_for_capacity(4).await?;
        Ok(())
    }

    pub(crate) async fn finish(mut self) -> Result<()> {
        self.store.stats.puts.fetch_add(1, Relaxed);
        match self.parts.take() {
            None => {
                self.store
                    .stats
                    .put_bytes
                    .fetch_add(self.buf.len() as u64, Relaxed);
                let buf = std::mem::take(&mut self.buf);
                self.store
                    .inner
                    .put(&self.path, PutPayload::from(buf))
                    .await?;
            }
            Some(w) => {
                w.finish().await?;
            }
        }
        if let (Some(t), Some(d)) = (self.tee.take(), &self.store.disk) {
            if d.claim(&self.name, self.size) {
                let kept = d.commit(t, &self.name, self.size);
                d.unclaim(&self.name);
                kept.map_err(|e| Error::Store(format!("disk cache: {e}").into()))?;
            }
        }
        Ok(())
    }
}
