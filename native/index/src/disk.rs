//! The on-disk cache: whole layer files kept on the engine's local disk, so
//! that plans after the first read (and after a restart) read from disk, not
//! from the object store. Layer files are immutable and their paths are never
//! reused, so a cached file is never stale. A file arrives complete (written
//! to a temporary name, then renamed): when a layer is first opened, in the
//! background, and when this process writes one. Least recently used files go
//! once the cache passes its size.

use crate::error::{Error, Result};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::Write;
use std::ops::Range;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::Mutex;

#[derive(Clone, Debug)]
pub struct DiskOptions {
    pub dir: PathBuf,
    /// Names the object store (and prefix) whose layers these are, stably
    /// across restarts: files kept for another name are removed on open.
    pub store: String,
    /// Bytes of files kept; a layer larger than a quarter of this is not cached.
    pub bytes: u64,
}

pub(crate) struct Disk {
    pub(crate) o: DiskOptions,
    st: Mutex<DiskState>,
    _lock: std::fs::File,
}

#[derive(Default)]
struct DiskState {
    files: HashMap<String, (u64, u64)>,
    order: BTreeMap<u64, String>,
    tick: u64,
    total: u64,
    filling: HashSet<String>,
}

/// A layer path as a file name, and back: `%`, `/` and a leading `.`
/// escaped (the cache's own files start with a dot).
fn name(path: &str) -> String {
    let n = path.replace('%', "%25").replace('/', "%2F");
    match n.strip_prefix('.') {
        Some(rest) => format!("%2E{rest}"),
        None => n,
    }
}

fn unname(n: &str) -> String {
    n.replace("%2E", ".")
        .replace("%2F", "/")
        .replace("%25", "%")
}

impl DiskState {
    fn touch(&mut self, path: &str) -> bool {
        let Some(e) = self.files.get_mut(path) else {
            return false;
        };
        self.order.remove(&e.1);
        self.tick += 1;
        e.1 = self.tick;
        self.order.insert(self.tick, path.to_string());
        true
    }
}

impl Disk {
    /// The cache in `o.dir` for the store `o.store` names, with the complete
    /// files already there. The directory is this store's alone while it is
    /// open (a lock file); files another store left there are removed.
    pub(crate) fn open(o: DiskOptions) -> Result<Disk> {
        let io =
            |e: std::io::Error| Error::Store(format!("disk cache {}: {e}", o.dir.display()).into());
        std::fs::create_dir_all(&o.dir).map_err(io)?;
        let lock = std::fs::File::options()
            .create(true)
            .truncate(false)
            .write(true)
            .open(o.dir.join(".lock"))
            .map_err(io)?;
        if lock.try_lock().is_err() {
            return Err(Error::Invalid(format!(
                "disk cache {}: another store has it open",
                o.dir.display()
            )));
        }
        let owner = o.dir.join(".store");
        let mine = std::fs::read(&owner).is_ok_and(|b| b == o.store.as_bytes());
        let mut found = Vec::new();
        for e in std::fs::read_dir(&o.dir).map_err(io)? {
            let e = e.map_err(io)?;
            let n = e.file_name().to_string_lossy().into_owned();
            if n == ".lock" || n == ".store" {
                continue;
            }
            if n.starts_with('.') || !mine {
                let _ = std::fs::remove_file(e.path());
                continue;
            }
            let m = e.metadata().map_err(io)?;
            found.push((m.modified().ok(), unname(&n), m.len()));
        }
        if !mine {
            std::fs::write(&owner, &o.store).map_err(io)?;
        }
        found.sort();
        let disk = Disk {
            o,
            st: Mutex::new(DiskState::default()),
            _lock: lock,
        };
        for (_, path, size) in found {
            disk.add(&path, size);
        }
        Ok(disk)
    }

    pub(crate) fn file(&self, path: &str) -> PathBuf {
        self.o.dir.join(name(path))
    }

    /// Whether `path` is here (and it becomes the most recently used).
    pub(crate) fn has(&self, path: &str) -> bool {
        self.st.lock().unwrap().touch(path)
    }

    /// Drop a file that could not be read.
    pub(crate) fn forget(&self, path: &str) {
        let mut st = self.st.lock().unwrap();
        if let Some((size, tick)) = st.files.remove(path) {
            st.order.remove(&tick);
            st.total -= size;
        }
        let _ = std::fs::remove_file(self.file(path));
    }

    /// Claim the fill of `path` if it is worth caching and nobody has it.
    pub(crate) fn claim(&self, path: &str, size: u64) -> bool {
        let mut st = self.st.lock().unwrap();
        size.saturating_mul(4) <= self.o.bytes
            && !st.files.contains_key(path)
            && st.filling.insert(path.to_string())
    }

    pub(crate) fn unclaim(&self, path: &str) {
        self.st.lock().unwrap().filling.remove(path);
    }

    /// A temporary file in the cache's directory, to become a layer's by
    /// `commit`; removed if dropped before.
    pub(crate) fn temp(&self) -> std::io::Result<Temp> {
        let path = self.o.dir.join(format!(".tmp-{:016x}", rand_id()));
        let file = std::fs::File::create(&path)?;
        Ok(Temp {
            path,
            file,
            kept: false,
        })
    }

    /// The temporary file `t` is complete: it becomes `path`'s.
    pub(crate) fn commit(&self, mut t: Temp, path: &str, size: u64) -> std::io::Result<()> {
        t.file.flush()?;
        std::fs::rename(&t.path, self.file(path))?;
        t.kept = true;
        self.add(path, size);
        Ok(())
    }

    fn add(&self, path: &str, size: u64) {
        let mut evict = Vec::new();
        {
            let mut st = self.st.lock().unwrap();
            if !st.touch(path) {
                st.tick += 1;
                let t = st.tick;
                st.files.insert(path.to_string(), (size, t));
                st.order.insert(t, path.to_string());
                st.total += size;
            }
            while st.total > self.o.bytes {
                let Some((_, p)) = st.order.pop_first() else {
                    break;
                };
                if let Some((s, _)) = st.files.remove(&p) {
                    st.total -= s;
                }
                evict.push(p);
            }
        }
        for p in evict {
            let _ = std::fs::remove_file(self.file(&p));
        }
    }

    pub(crate) fn total(&self) -> u64 {
        self.st.lock().unwrap().total
    }
}

pub(crate) struct Temp {
    path: PathBuf,
    file: std::fs::File,
    kept: bool,
}

impl Temp {
    pub(crate) fn write(&mut self, b: &[u8]) -> std::io::Result<()> {
        self.file.write_all(b)
    }
}

impl Drop for Temp {
    fn drop(&mut self) {
        if !self.kept {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

/// Bytes of a cached file.
pub(crate) fn read_at(file: &std::path::Path, r: Range<u64>) -> std::io::Result<Vec<u8>> {
    let f = std::fs::File::open(file)?;
    if r.start > r.end || r.end > f.metadata()?.len() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            format!("range {r:?} past the file's end"),
        ));
    }
    let mut b = vec![0u8; (r.end - r.start) as usize];
    f.read_exact_at(&mut b, r.start)?;
    Ok(b)
}

fn rand_id() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
    static N: AtomicU64 = AtomicU64::new(0);
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as u64);
    xxhash_rust::xxh3::xxh3_64(
        &[
            t.to_le_bytes(),
            N.fetch_add(1, Relaxed).to_le_bytes(),
            (std::process::id() as u64).to_le_bytes(),
        ]
        .concat(),
    )
}
