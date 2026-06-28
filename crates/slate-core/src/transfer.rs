use crate::{progress::ProgressEvent, store_url};
use anyhow::Result;
use bytes::Bytes;
use futures::StreamExt;
use object_store::{MultipartUpload, path::Path};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;
use tracing::info;
use uuid::Uuid;

const DEFAULT_CHUNK_SIZE: usize = 16 * 1024 * 1024; // 16 MiB
const DEFAULT_PARALLEL_CHUNKS: usize = 8;
const DEFAULT_PARALLEL_OBJECTS: usize = 8;

fn chunk_size() -> usize {
    std::env::var("SLATE_chunk_size()_MIB")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .map(|mib| mib * 1024 * 1024)
        .unwrap_or(DEFAULT_CHUNK_SIZE)
}

fn parallel_chunks() -> usize {
    std::env::var("SLATE_parallel_chunks()")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_PARALLEL_CHUNKS)
}

fn parallel_objects() -> usize {
    std::env::var("SLATE_parallel_objects()")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_PARALLEL_OBJECTS)
}

pub struct TransferEngine;

impl TransferEngine {
    pub async fn run(
        job_id: Uuid,
        src_url: &str,
        dst_url: &str,
        progress_tx: mpsc::Sender<ProgressEvent>,
    ) -> Result<u64> {
        let src = store_url::resolve(src_url)?;
        let dst = store_url::resolve(dst_url)?;
        let dst_supports_multipart = is_cloud_url(dst_url);

        let list_prefix = if src.prefix.as_ref().is_empty() {
            None
        } else {
            Some(&src.prefix)
        };

        let objects: Vec<_> = src
            .store
            .list(list_prefix)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .filter_map(|r| r.ok())
            .collect();

        let total_bytes: u64 = objects.iter().map(|o| o.size as u64).sum();
        info!(job_id = %job_id, objects = objects.len(), total_bytes, "starting transfer");

        let transferred = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let start = Instant::now();

        // Transfer parallel_objects() files concurrently; each file uses parallel_chunks()
        // range-GETs internally. Total concurrent requests = parallel_objects() × parallel_chunks().
        let src_store = src.store.clone();
        let dst_store = dst.store.clone();
        let src_prefix = src.prefix.clone();
        let dst_prefix = dst.prefix.clone();
        let transferred_clone = transferred.clone();
        let progress_tx_clone = progress_tx.clone();

        futures::stream::iter(objects)
            .map(|obj_meta| {
                let src_store = src_store.clone();
                let dst_store = dst_store.clone();
                let src_prefix = src_prefix.clone();
                let dst_prefix = dst_prefix.clone();
                let transferred = transferred_clone.clone();
                let progress_tx = progress_tx_clone.clone();
                let dst_supports_multipart = dst_supports_multipart;

                async move {
                    let obj_size = obj_meta.size as u64;
                    let src_path = obj_meta.location.clone();

                    let rel = src_path
                        .as_ref()
                        .strip_prefix(src_prefix.as_ref())
                        .unwrap_or(src_path.as_ref())
                        .trim_start_matches('/');

                    let dst_path = if dst_prefix.as_ref().is_empty() {
                        Path::from(rel)
                    } else {
                        Path::from(format!("{}/{}", dst_prefix, rel).as_str())
                    };

                    if obj_size > chunk_size() as u64 {
                        if dst_supports_multipart {
                            chunked_copy_multipart(&src_store, &src_path, &dst_store, &dst_path, obj_size).await?;
                        } else {
                            chunked_copy_buffered(&src_store, &src_path, &dst_store, &dst_path, obj_size).await?;
                        }
                    } else {
                        let data = src_store.get(&src_path).await?.bytes().await?;
                        dst_store.put(&dst_path, data.into()).await?;
                    }

                    let done = transferred.fetch_add(obj_size, std::sync::atomic::Ordering::Relaxed) + obj_size;
                    let elapsed = start.elapsed().as_secs_f64();
                    let throughput_mbps = if elapsed > 0.0 { (done as f64 / elapsed) / 1_000_000.0 } else { 0.0 };
                    let _ = progress_tx.send(ProgressEvent {
                        job_id,
                        bytes_transferred: done,
                        bytes_total: Some(total_bytes),
                        throughput_mbps,
                    }).await;

                    Ok::<(), anyhow::Error>(())
                }
            })
            .buffer_unordered(parallel_objects())
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        let transferred = transferred.load(std::sync::atomic::Ordering::Relaxed);

        info!(job_id = %job_id, transferred, "transfer complete");
        Ok(transferred)
    }
}

fn is_cloud_url(url: &str) -> bool {
    url.starts_with("s3://") || url.starts_with("gs://") || url.starts_with("az://")
}

/// Cloud → cloud: multipart upload so we never hold the full object in memory.
/// Peak memory = chunk_size() × parallel_chunks() = 128 MiB regardless of object size.
async fn chunked_copy_multipart(
    src_store: &Arc<dyn object_store::ObjectStore>,
    src_path: &Path,
    dst_store: &Arc<dyn object_store::ObjectStore>,
    dst_path: &Path,
    size: u64,
) -> Result<()> {
    let chunks = (size as usize + chunk_size() - 1) / chunk_size();
    let mut upload = dst_store.put_multipart(dst_path).await?;

    // `buffered` (ordered) keeps parallel_chunks() in flight while preserving part order.
    let mut stream = futures::stream::iter(0..chunks)
        .map(|i| {
            let src_store = src_store.clone();
            let src_path = src_path.clone();
            async move {
                let offset = i * chunk_size();
                let end = ((i + 1) * chunk_size()).min(size as usize);
                src_store
                    .get_range(&src_path, offset..end)
                    .await
                    .map_err(anyhow::Error::from)
            }
        })
        .buffered(parallel_chunks());

    while let Some(chunk) = stream.next().await {
        upload.put_part(chunk?.into()).await?;
    }

    upload.complete().await?;
    Ok(())
}

/// Cloud/local → local: fetch all chunks in parallel, sort, write once.
/// For local destinations where multipart upload doesn't apply.
async fn chunked_copy_buffered(
    src_store: &Arc<dyn object_store::ObjectStore>,
    src_path: &Path,
    dst_store: &Arc<dyn object_store::ObjectStore>,
    dst_path: &Path,
    size: u64,
) -> Result<()> {
    let chunks = (size as usize + chunk_size() - 1) / chunk_size();

    let mut parts: Vec<(usize, Bytes)> = futures::stream::iter(0..chunks)
        .map(|i| {
            let src_store = src_store.clone();
            let src_path = src_path.clone();
            async move {
                let offset = i * chunk_size();
                let end = ((i + 1) * chunk_size()).min(size as usize);
                let data = src_store
                    .get_range(&src_path, offset..end)
                    .await
                    .map_err(anyhow::Error::from)?;
                Ok::<(usize, Bytes), anyhow::Error>((i, data))
            }
        })
        .buffer_unordered(parallel_chunks())
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<_>>()?;

    parts.sort_by_key(|(i, _)| *i);

    let all: Bytes = parts
        .into_iter()
        .fold(bytes::BytesMut::new(), |mut acc, (_, b)| {
            acc.extend_from_slice(&b);
            acc
        })
        .freeze();

    dst_store.put(dst_path, all.into()).await?;
    Ok(())
}
