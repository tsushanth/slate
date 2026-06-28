use crate::{progress::ProgressEvent, store_url};
use anyhow::Result;
use bytes::Bytes;
use futures::StreamExt;
use object_store::{GetOptions, GetRange, MultipartUpload, path::Path};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;
use tracing::info;
use uuid::Uuid;

// Chunking constants — used only in chunked mode (SLATE_STRATEGY=chunked)
const DEFAULT_CHUNK_SIZE: usize = 8 * 1024 * 1024; // 8 MiB
const DEFAULT_PARALLEL_CHUNKS: usize = 16;
const DEFAULT_PARALLEL_OBJECTS: usize = 8;

// Streaming mode default — matches rclone's --transfers default
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
    // SLATE_PARALLEL_OBJECTS overrides stream concurrency too, so a single knob covers both modes
    std::env::var("SLATE_PARALLEL_OBJECTS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_STREAM_CONCURRENCY)
}

#[derive(Clone, Copy, PartialEq)]
enum Strategy {
    // Stream full objects in parallel — lower request count, wins at cross-region latency.
    // Default. Matches how rclone --transfers works.
    Stream,
    // Parallel range-GETs per object — wins at low latency (same-region, local NVMe).
    Chunked,
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

        let transferred = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
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
                                stream_copy_local(&src_store, &src_path, &dst_store, &dst_path).await?;
                            }
                        }
                        Strategy::Chunked => {
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

/// Stream full object → multipart upload without buffering. One HTTP connection per object.
/// Default strategy: fewer requests means less per-request overhead at cross-region latencies.
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

/// Stream full object → local store (single put).
async fn stream_copy_local(
    src_store: &Arc<dyn object_store::ObjectStore>,
    src_path: &Path,
    dst_store: &Arc<dyn object_store::ObjectStore>,
    dst_path: &Path,
) -> Result<()> {
    let data = src_store.get(src_path).await?.bytes().await?;
    dst_store.put(dst_path, data.into()).await?;
    Ok(())
}

/// SLATE_STRATEGY=chunked — parallel range-GETs, wins at same-region / low latency.
/// Peak memory = chunk_size() × parallel_chunks() regardless of object size.
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
