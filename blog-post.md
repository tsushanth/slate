# We built an open-source data orchestrator for AI — rclone parity, 4.4× faster than aws s3 cp

If you've ever waited 20 minutes for a model checkpoint to finish copying before you could start a training run, this is for you.

---

## The problem

AI labs move a lot of data. Datasets, model weights, checkpoints, artifacts — all of it traveling between object stores, GPU clusters, and local disks. The tools most teams reach for are `aws s3 cp`, `gsutil`, or `rclone`. They work, but none of them were built for the specific access patterns of ML workloads:

- Large files (multi-GB model weights) that benefit from parallel chunk downloads
- Many files (dataset shards) that need to move concurrently
- Cross-cloud and cross-region transfers that are the norm, not the exception

We built **Slate**: an open-source, self-hostable alternative with a REST API, persistent job tracking, and rclone-class throughput out of the box.

---

## What Slate does

Slate is a single binary that moves data between object stores. It supports S3, GCS, Azure Blob, and local filesystems — any combination.

```bash
# Copy a dataset from S3 to GCS
slate copy s3://my-bucket/datasets/imagenet gs://gcs-bucket/datasets/imagenet

# Copy model weights to local disk
slate copy s3://my-bucket/weights/llama-3-70b /mnt/nvme/weights/

# Check egress cost before transferring
slate copy --estimate s3://my-bucket/weights gs://gcs-bucket/weights

# Track jobs
slate jobs
slate status <job-id>
```

It also ships an API server with real-time SSE progress streaming:

```bash
slate-api  # starts on :3030

curl -X POST localhost:3030/jobs \
  -H 'Content-Type: application/json' \
  -d '{"src": "s3://bucket/data", "dst": "gs://other/data"}'

curl localhost:3030/jobs/<id>/events  # SSE stream
```

---

## The benchmark

We ran a head-to-head on a Hetzner cpx41 (8 vCPU, 16 GB RAM, Frankfurt) pulling from AWS S3 us-east-1. The dataset: 5 × 529 MiB files (real Gemma 3 1B model weights), 2.6 GiB total. Cross-region, cross-provider — the hard case.

3 runs per config, best 2-of-3 averaged.

**Final results:**

| Tool | Avg time | Throughput | vs slate |
|---|---|---|---|
| **slate** | 2.7s | **984 MB/s** | — |
| `rclone --transfers 8` | 2.4s | 1,120 MB/s | 1.14× faster |
| `aws s3 cp --recursive` | 12.0s | 221 MB/s | **4.4× slower** |

Slate is within **14% of rclone** and **4.4× faster than `aws s3 cp`** with zero configuration.

---

## How we got there

The first version of Slate used 8 MiB chunks with 16 parallel range-GETs per file, which got us to ~600 MB/s — already 2.7× faster than `aws s3 cp`. But rclone was pulling away at 1,100+ MB/s. We dug into rclone's source to understand why.

Two things were holding us back.

### 1. Chunk size matters more than chunk count at cross-region latency

rclone defaults to 64 MiB chunks with 4 parallel streams per file. We were using 8 MiB chunks with 16 parallel streams. The total request count is similar, but the request overhead is not:

- **8 MiB chunks**: 67 range-GET requests per 529 MiB file
- **64 MiB chunks**: 9 range-GET requests per 529 MiB file

At ~80ms cross-region RTT, 67 requests per file means you're spending ~5.4 seconds just on HTTP round trips — before a single byte is read. Switching to 64 MiB chunks cut that to ~720ms.

### 2. Buffered writes were the memory bottleneck

Our original implementation waited for all chunks to download, assembled them in RAM, then wrote the complete file to disk. With 16 concurrent files, that meant holding up to 8+ GiB in memory simultaneously — causing allocation pressure and write stalls.

rclone pre-allocates each output file at its full size, then writes chunks directly at their byte offsets as they arrive. Each chunk takes one write, frees its memory immediately, and lets the next chunk download start. Peak memory stays at `parallel_chunks × chunk_size` = 256 MiB regardless of object size or concurrency.

We implemented the same:

```rust
// Pre-allocate the file
let file = tokio::fs::OpenOptions::new().write(true).create(true).open(path).await?;
file.set_len(size).await?;

// Download and write each chunk at its offset concurrently
futures::stream::iter(0..chunks)
    .map(|i| async move {
        let data = src_store.get_range(&path, offset..end).await?;
        let mut f = tokio::fs::OpenOptions::new().write(true).open(path).await?;
        f.seek(SeekFrom::Start(offset as u64)).await?;
        f.write_all(&data).await?;
    })
    .buffer_unordered(parallel_chunks())
    .collect::<Vec<_>>()
    .await;
```

Together, those two changes took us from 598 MB/s to **984 MB/s** — a 64% improvement and rclone parity in practice.

---

## Try it

**Pre-built Linux binary:**

```bash
curl -fsSL https://github.com/tsushanth/slate/releases/latest/download/slate-linux-x86_64.tar.gz \
  | tar xz -C /usr/local/bin
```

**From source:**

```bash
git clone https://github.com/tsushanth/slate
cd slate
cargo build --release
```

Set your credentials via standard env vars (`AWS_ACCESS_KEY_ID`, `GOOGLE_APPLICATION_CREDENTIALS`, etc.) and run. No config file.

Supports: S3 (+ MinIO, Cloudflare R2), GCS, Azure Blob, local filesystem — any combination.

---

## What's next

- **Resumable transfers** — SQLite job store already tracks progress; need retry-from-offset
- **Adaptive parallelism** — auto-tune chunk size and concurrency based on observed RTT
- **Web dashboard** — API + SSE stream is in place; need a UI

The core transfer engine is ~400 lines of Rust. If you work on ML infrastructure and move a lot of data, try it and let us know what breaks.

GitHub: [github.com/tsushanth/slate](https://github.com/tsushanth/slate)

---

*Benchmark methodology: Hetzner cpx41 (8 vCPU, 16 GB RAM), Frankfurt. Source: AWS S3 us-east-1. Dataset: 5 × gemma3-1b-it-int4.task (529 MiB each, 2.6 GiB total). 3 runs per config, best 2-of-3 averaged. rclone v1.74.3, aws-cli v2.35.11, slate v0.1.2 (Rust 1.96.0).*
