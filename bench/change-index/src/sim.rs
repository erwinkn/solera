//! The format benchmark's simulated object store, over local files: every
//! request waits 30 ms plus its bytes at 80 MB/s (one sleep per request), at
//! most 64 in flight. `None`: a local copy, no wait.

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
};
use std::ops::Range;
use std::time::Duration;
use tokio::sync::Semaphore;

#[derive(Debug)]
pub struct Sim {
    pub inner: LocalFileSystem,
    pub net: Option<(f64, f64)>,
    sem: Semaphore,
}

impl Sim {
    pub fn new(root: &std::path::Path, net: Option<(f64, f64)>) -> Sim {
        Sim {
            inner: LocalFileSystem::new_with_prefix(root).unwrap(),
            net,
            sem: Semaphore::new(64),
        }
    }
}

impl std::fmt::Display for Sim {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Sim({:?})", self.net)
    }
}

#[async_trait]
impl ObjectStore for Sim {
    async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(&self, location: &Path, opts: PutMultipartOptions) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        let _permit = self.sem.acquire().await.unwrap();
        let out = self.inner.get_ranges(location, ranges).await?;
        if let Some((rtt, bw)) = self.net {
            let n: usize = out.iter().map(|b| b.len()).sum();
            tokio::time::sleep(Duration::from_secs_f64(rtt + n as f64 / bw)).await;
        }
        Ok(out)
    }

    fn delete_stream(&self, locations: BoxStream<'static, Result<Path>>) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
