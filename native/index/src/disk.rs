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
    /// Paths being filled, and the bytes set aside for them.
    filling: HashSet<String>,
    reserved: u64,
    /// Fills that failed, and copies dropped because they could not be read:
    /// when, and how many times. Not filled again for a minute, doubling with
    /// each failure up to about an hour, so that a layer damaged in the store
    /// is not downloaded again after every read. A fill that succeeds does
    /// not reset the count. At most `FAILED` are remembered.
    failed: HashMap<String, (std::time::Instant, u32)>,
}

const FAILED: usize = 1024;

/// How long a fill that failed `n + 1` times in a row is not tried again.
fn backoff(n: u32) -> std::time::Duration {
    std::time::Duration::from_secs(60 << n.min(6))
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
    /// Note one more failure of `path`, within the bound.
    fn fail(&mut self, path: &str) {
        let n = self.failed.get(path).map_or(0, |&(_, n)| n + 1);
        if n == 0 && self.failed.len() >= FAILED {
            self.failed.retain(|_, &mut (t, n)| t.elapsed() < backoff(n));
            let oldest = self.failed.iter().min_by_key(|(_, &(t, _))| t);
            if let Some(p) = oldest
                .filter(|_| self.failed.len() >= FAILED)
                .map(|(p, _)| p.clone())
            {
                self.failed.remove(&p);
            }
        }
        self.failed
            .insert(path.to_string(), (std::time::Instant::now(), n));
    }

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
    /// open (a lock file, waited on briefly); files another store's cache left
    /// there are removed. A non-empty directory that is not a cache is refused.
    pub(crate) fn open(o: DiskOptions) -> Result<Disk> {
        let io =
            |e: std::io::Error| Error::Store(format!("disk cache {}: {e}", o.dir.display()).into());
        std::fs::create_dir_all(&o.dir).map_err(io)?;
        let owner = o.dir.join(".store");
        if !owner.exists()
            && std::fs::read_dir(&o.dir)
                .map_err(io)?
                .any(|e| e.is_ok_and(|e| e.file_name() != ".lock"))
        {
            return Err(Error::Invalid(format!(
                "disk cache {}: the directory holds files and is not a cache",
                o.dir.display()
            )));
        }
        let lock = std::fs::File::options()
            .create(true)
            .truncate(false)
            .write(true)
            .open(o.dir.join(".lock"))
            .map_err(io)?;
        let mut tries = 0;
        while lock.try_lock().is_err() {
            tries += 1;
            if tries == 100 {
                return Err(Error::Invalid(format!(
                    "disk cache {}: another store has it open",
                    o.dir.display()
                )));
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        let mine = std::fs::read(&owner).is_ok_and(|b| b == o.store.as_bytes());
        let mut found = Vec::new();
        for e in std::fs::read_dir(&o.dir).map_err(io)? {
            let e = e.map_err(io)?;
            let n = e.file_name().to_string_lossy().into_owned();
            if n == ".lock" || n == ".store" {
                continue;
            }
            let m = e.metadata().map_err(io)?;
            // too short to be a layer: a fill cut off before it was renamed
            // cannot leave one, so it is not ours to keep
            if n.starts_with('.') || !mine || m.len() < crate::format::TRAILER as u64 {
                let _ = std::fs::remove_file(e.path());
                continue;
            }
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

    /// Drop a file that could not be read; it is not filled again for a while.
    pub(crate) fn forget(&self, path: &str) {
        let mut st = self.st.lock().unwrap();
        if let Some((size, tick)) = st.files.remove(path) {
            st.order.remove(&tick);
            st.total -= size;
        }
        st.fail(path);
        let _ = std::fs::remove_file(self.file(path));
    }

    /// Whether `path` can be kept: its file name fits a directory entry.
    pub(crate) fn keeps(&self, path: &str) -> bool {
        name(path).len() <= 240
    }

    /// Claim the fill of `path` if it is worth caching, nobody has it and it
    /// has not failed; room is made for it now, so the fill never needs more
    /// than the cache's size. The claim ends with `commit` or `abandon`.
    pub(crate) fn claim(&self, path: &str, size: u64) -> bool {
        let mut st = self.st.lock().unwrap();
        let failed = st
            .failed
            .get(path)
            .is_some_and(|&(t, n)| t.elapsed() < backoff(n));
        if !self.keeps(path)
            || size.saturating_mul(4) > self.o.bytes
            || st.reserved + size > self.o.bytes
            || st.files.contains_key(path)
            || failed
            || !st.filling.insert(path.to_string())
        {
            return false;
        }
        st.reserved += size;
        let evict = self.make_room(&mut st);
        drop(st);
        self.remove(evict);
        true
    }

    /// Set `more` bytes aside for a claimed fill that grows as it is written
    /// (a write-through); false, with nothing set aside, if they do not fit.
    pub(crate) fn grow(&self, path: &str, more: u64) -> bool {
        let mut st = self.st.lock().unwrap();
        if !st.filling.contains(path) || st.reserved + more > self.o.bytes {
            return false;
        }
        st.reserved += more;
        let evict = self.make_room(&mut st);
        drop(st);
        self.remove(evict);
        true
    }

    /// A claim given up, not failed: its room is given back.
    pub(crate) fn release(&self, path: &str, size: u64) {
        let mut st = self.st.lock().unwrap();
        if st.filling.remove(path) {
            st.reserved -= size;
        }
    }

    /// A claimed fill that failed: its room is given back, and it is not
    /// tried again for a while. Ending a claim twice changes nothing.
    pub(crate) fn abandon(&self, path: &str, size: u64) {
        let mut st = self.st.lock().unwrap();
        if st.filling.remove(path) {
            st.reserved -= size;
            st.fail(path);
        }
    }

    /// Evict least recently used files until the files and the room set
    /// aside fit the cache's size.
    fn make_room(&self, st: &mut DiskState) -> Vec<String> {
        let mut evict = Vec::new();
        while st.total + st.reserved > self.o.bytes {
            let Some((_, p)) = st.order.pop_first() else {
                break;
            };
            if let Some((s, _)) = st.files.remove(&p) {
                st.total -= s;
            }
            evict.push(p);
        }
        evict
    }

    fn remove(&self, paths: Vec<String>) {
        for p in paths {
            let _ = std::fs::remove_file(self.file(&p));
        }
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

    /// The temporary file `t` of a claimed fill is complete: it becomes
    /// `path`'s, and the claim ends (abandoned if the rename fails).
    pub(crate) fn commit(&self, mut t: Temp, path: &str, size: u64) -> std::io::Result<()> {
        if let Err(e) = t
            .file
            .flush()
            .and_then(|_| std::fs::rename(&t.path, self.file(path)))
        {
            self.abandon(path, size);
            return Err(e);
        }
        t.kept = true;
        let mut st = self.st.lock().unwrap();
        if st.filling.remove(path) {
            st.reserved -= size;
        }
        drop(st);
        self.add(path, size);
        Ok(())
    }

    fn add(&self, path: &str, size: u64) {
        let mut st = self.st.lock().unwrap();
        if !st.touch(path) {
            st.tick += 1;
            let t = st.tick;
            st.files.insert(path.to_string(), (size, t));
            st.order.insert(t, path.to_string());
            st.total += size;
        }
        let evict = self.make_room(&mut st);
        drop(st);
        self.remove(evict);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failed_fills_are_remembered_within_a_bound() {
        let dir = std::env::temp_dir().join(format!("solera-disk-failed-{}", std::process::id()));
        let d = Disk::open(DiskOptions {
            dir: dir.clone(),
            store: "s".into(),
            bytes: 1 << 20,
        })
        .unwrap();
        for i in 0..3 * FAILED {
            let p = format!("layer-{i}");
            assert!(d.claim(&p, 1));
            d.abandon(&p, 1);
        }
        let st = d.st.lock().unwrap();
        assert_eq!(st.failed.len(), FAILED);
        assert!(st.failed.contains_key(&format!("layer-{}", 3 * FAILED - 1)));
        assert_eq!(st.reserved, 0);
        drop(st);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn files_found_on_open_are_kept_within_the_budget() {
        let dir = std::env::temp_dir().join(format!("solera-disk-found-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(".store"), "s").unwrap();
        for i in 0..1000 {
            std::fs::write(dir.join(format!("layer-{i}")), [0u8; 100]).unwrap();
        }
        // the newest, so the least likely to be evicted
        std::thread::sleep(std::time::Duration::from_millis(20));
        for i in 0..1000 {
            std::fs::write(dir.join(format!("z-empty-{i}")), b"").unwrap();
        }
        let d = Disk::open(DiskOptions {
            dir: dir.clone(),
            store: "s".into(),
            bytes: 1000,
        })
        .unwrap();
        let st = d.st.lock().unwrap();
        assert_eq!(st.files.len(), 10);
        assert_eq!(st.total, 1000);
        drop(st);
        let left = std::fs::read_dir(&dir).unwrap().count();
        assert_eq!(left, 10 + 2);
        let _ = std::fs::remove_dir_all(dir);
    }
}
