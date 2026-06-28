use crate::{progress::ProgressEvent, store_url};
use anyhow::Result;
use futures::StreamExt;
use object_store::{GetOptions, GetRange, MultipartUpload, path::Path};
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tracing::info;
use uuid::Uuid;

// rclone defaults: 64 MiB chunks × 4 parallel per file, files > 256 MiB.
// Fewer, larger requests wins at cross-region latency — 9 requests vs 67 for a 529 MiB file.
const DEFAULT_CHUNK_SIZE: usize = 64 * 1024 * 1024;
const DEFAULT_PARALLEL_CHUNKS: usize = 4;
const DEFAULT_PARALLEL_OBJECTS: usize = 16;
const DEFAULT_STREAM_CONCURRENCY: usize = 16;

fn chunk_size() -> usize {
    std::env::var("SLATE_CHUNK_SIZE_MIB")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .map(|mib| mib * 1024 * 1024)
        .unwrap_or(DEFAULT_CHUNK_SIZE)
}

fn parallel_chunks() -> usize {
    std::env::var("SLATE_PARALLEL_CHUNKS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_PARALLEL_CHUNKS)
}

fn parallel_objects() -> usize {
    std::env::var("SLATE_PARALLEL_OBJECTS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_PARALLEL_OBJECTS)
}

fn stream_concurrency() -> usize {
    std::env::var("SLATE_PARALLEL_OBJECTS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_STREAM_CONCURRENCY)
}

#[derive(Clone, Copy, PartialEq)]
enum Strategy {
    // Parallel range-GETs per object with seekable local writes — default.
    // Wins at cross-region and same-region. Matches rclone's multi-thread-streams approach.
    Chunked,
    // Full-object streaming — available via SLATE_STRATEGY=stream.
    Stream,
}

fn strategy() -> Strategy {
    match std::env::var("SLATE_STRATEGY").as_deref() {
        Ok("stream") => Strategy::Stream,
        _ => Strategy::Chunked,
    }
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
        let dst_local_root = local_root(dst_url);

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
        let strat = strategy();
        info!(
            job_id = %job_id,
            objects = objects.len(),
            total_bytes,
            strategy = if strat == Strategy::Stream { "stream" } else { "chunked" },
            "starting transfer"
        );

        let transferred = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let start = Instant::now();

        let src_store = src.store.clone();
        let dst_store = dst.store.clone();
        let src_prefix = src.prefix.clone();
        let dst_prefix = dst.prefix.clone();
        let transferred_clone = transferred.clone();
        let progress_tx_clone = progress_tx.clone();

        let concurrency = if strat == Strategy::Stream {
            stream_concurrency()
        } else {
            parallel_objects()
        };

        futures::stream::iter(objects)
            .map(|obj_meta| {
                let src_store = src_store.clone();
                let dst_store = dst_store.clone();
                let src_prefix = src_prefix.clone();
                let dst_prefix = dst_prefix.clone();
                let transferred = transferred_clone.clone();
                let progress_tx = progress_tx_clone.clone();
                let dst_local_root = dst_local_root.clone();

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

                    match strat {
                        Strategy::Stream => {
                            if dst_supports_multipart {
                                stream_copy_multipart(&src_store, &src_path, &dst_store, &dst_path).await?;
                            } else {
                                let data = src_store.get(&src_path).await?.bytes().await?;
                                dst_store.put(&dst_path, data.into()).await?;
                            }
                        }
                        Strategy::Chunked => {
                            if obj_size > chunk_size() as u64 {
                                if dst_supports_multipart {
                                    chunked_copy_multipart(&src_store, &src_path, &dst_store, &dst_path, obj_size).await?;
                                } else if let Some(root) = &dst_local_root {
                                    // Pre-allocate file and write chunks at their offsets as they complete —
                                    // eliminates the need to buffer the full object in RAM (rclone's approach).
                                    let fs_path = format!("{}/{}", root.trim_end_matches('/'), rel);
                                    chunked_copy_seekable(&src_store, &src_path, &fs_path, obj_size).await?;
                                } else {
                                    chunked_copy_buffered(&src_store, &src_path, &dst_store, &dst_path, obj_size).await?;
                                }
                            } else {
                                let data = src_store.get(&src_path).await?.bytes().await?;
                                dst_store.put(&dst_path, data.into()).await?;
                            }
                        }
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
            .buffer_unordered(concurrency)
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

/// Returns the local filesystem root for a local destination URL, None for cloud.
fn local_root(url: &str) -> Option<String> {
    if url.starts_with("file://") {
        Some(url.strip_prefix("file://").unwrap_or(url).to_string())
    } else if url.starts_with('/') {
        Some(url.to_string())
    } else {
        None
    }
}

/// Stream full object → multipart upload without buffering (cloud→cloud).
async fn stream_copy_multipart(
    src_store: &Arc<dyn object_store::ObjectStore>,
    src_path: &Path,
    dst_store: &Arc<dyn object_store::ObjectStore>,
    dst_path: &Path,
) -> Result<()> {
    let result = src_store.get(src_path).await?;
    let mut stream = result.into_stream();
    let mut upload = dst_store.put_multipart(dst_path).await?;
    while let Some(chunk) = stream.next().await {
        upload.put_part(chunk?.into()).await?;
    }
    upload.complete().await?;
    Ok(())
}

/// Parallel range-GETs → multipart upload (cloud→cloud).
/// parallel_chunks() in flight at a time, ordered for multipart part numbering.
async fn chunked_copy_multipart(
    src_store: &Arc<dyn object_store::ObjectStore>,
    src_path: &Path,
    dst_store: &Arc<dyn object_store::ObjectStore>,
    dst_path: &Path,
    size: u64,
) -> Result<()> {
    let chunks = (size as usize + chunk_size() - 1) / chunk_size();
    let mut upload = dst_store.put_multipart(dst_path).await?;

    let mut stream = futures::stream::iter(0..chunks)
        .map(|i| {
            let src_store = src_store.clone();
            let src_path = src_path.clone();
            async move {
                let offset = i * chunk_size();
                let end = ((i + 1) * chunk_size()).min(size as usize);
                src_store
                    .get_opts(
                        &src_path,
                        GetOptions { range: Some(GetRange::Bounded(offset..end)), ..Default::default() },
                    )
                    .await?
                    .bytes()
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

/// Parallel range-GETs → seekable local file (cloud→local).
/// Pre-allocates the file at full size, then writes each chunk at its byte offset as it arrives.
/// Peak memory = parallel_chunks() × chunk_size() regardless of file size (matches rclone's approach).
async fn chunked_copy_seekable(
    src_store: &Arc<dyn object_store::ObjectStore>,
    src_path: &Path,
    fs_path: &str,
    size: u64,
) -> Result<()> {
    // Create parent directories
    if let Some(parent) = std::path::Path::new(fs_path).parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    // Pre-allocate the file at full size so concurrent writes don't race on metadata
    let file = tokio::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(fs_path)
        .await?;
    file.set_len(size).await?;
    drop(file);

    let chunks = (size as usize + chunk_size() - 1) / chunk_size();
    let fs_path = Arc::new(fs_path.to_string());

    futures::stream::iter(0..chunks)
        .map(|i| {
            let src_store = src_store.clone();
            let src_path = src_path.clone();
            let fs_path = fs_path.clone();
            async move {
                let offset = i * chunk_size();
                let end = ((i + 1) * chunk_size()).min(size as usize);
                let data = src_store
                    .get_opts(
                        &src_path,
                        GetOptions { range: Some(GetRange::Bounded(offset..end)), ..Default::default() },
                    )
                    .await?
                    .bytes()
                    .await?;

                // Each chunk opens the file independently and seeks to its offset.
                // Safe because we pre-allocated and each chunk writes a non-overlapping region.
                let mut f = tokio::fs::OpenOptions::new().write(true).open(fs_path.as_ref()).await?;
                f.seek(std::io::SeekFrom::Start(offset as u64)).await?;
                f.write_all(&data).await?;
                Ok::<(), anyhow::Error>(())
            }
        })
        .buffer_unordered(parallel_chunks())
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<_>>()
}

/// Parallel range-GETs → buffered write (fallback for non-seekable local stores).
async fn chunked_copy_buffered(
    src_store: &Arc<dyn object_store::ObjectStore>,
    src_path: &Path,
    dst_store: &Arc<dyn object_store::ObjectStore>,
    dst_path: &Path,
    size: u64,
) -> Result<()> {
    use bytes::Bytes;
    let chunks = (size as usize + chunk_size() - 1) / chunk_size();

    let mut parts: Vec<(usize, Bytes)> = futures::stream::iter(0..chunks)
        .map(|i| {
            let src_store = src_store.clone();
            let src_path = src_path.clone();
            async move {
                let offset = i * chunk_size();
                let end = ((i + 1) * chunk_size()).min(size as usize);
                let data = src_store
                    .get_opts(
                        &src_path,
                        GetOptions { range: Some(GetRange::Bounded(offset..end)), ..Default::default() },
                    )
                    .await?
                    .bytes()
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

    let all: bytes::Bytes = parts
        .into_iter()
        .fold(bytes::BytesMut::new(), |mut acc, (_, b)| {
            acc.extend_from_slice(&b);
            acc
        })
        .freeze();

    dst_store.put(dst_path, all.into()).await?;
    Ok(())
}
