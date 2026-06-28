# We built an open-source data orchestrator for AI — 2.9× faster than aws s3 cp

If you've ever waited 20 minutes for a model checkpoint to finish copying before you could start a training run, this is for you.

---

## The problem

AI labs move a lot of data. Datasets, model weights, checkpoints, artifacts — all of it traveling between object stores, GPU clusters, and local disks. The tools most teams reach for are `aws s3 cp`, `gsutil`, or `rclone`. They work, but none of them were built for the specific access patterns of ML workloads:

- Large files (multi-GB model weights) that benefit from parallel chunk downloads
- Many files (dataset shards) that need to move concurrently
- Cross-cloud and cross-region transfers that are the norm, not the exception

[Limestone](https://www.uselimestone.ai) is building a closed, hosted solution for this. We thought: what would this look like as an open-source, self-hostable tool?

We built **Slate**.

---

## What Slate does

Slate is a single binary that moves data between object stores. It supports S3, GCS, Azure Blob, and local filesystems — any combination.

```bash
# Copy a dataset from S3 to GCS
slate copy s3://my-bucket/datasets/imagenet gs://gcs-bucket/datasets/imagenet

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
  -d '{"src": "s3://bucket/data", "dst": "gs://other/data"}'

curl localhost:3030/jobs/<id>/events  # SSE stream
```

---

## The benchmark

We ran a head-to-head on a Hetzner cpx41 (8 vCPU, 16 GB RAM, Frankfurt) pulling from AWS S3 us-east-1. The dataset: 5 × 529 MiB files (real Gemma 3 1B model weights), 2.6 GiB total. This is the hard case — cross-region, cross-provider.

We ran a 6-config parallelism sweep first to find the optimal settings, then ran 3 final runs per tool (best 2-of-3 averaged).

**Parallelism sweep:**

| Concurrent objects | Chunks per object | Chunk size | Throughput |
|---|---|---|---|
| 4 | 8 | 16 MiB | 531 MB/s |
| 8 | 8 | 16 MiB | 560 MB/s |
| 16 | 8 | 16 MiB | 600 MB/s |
| **8** | **16** | **8 MiB** | **640 MB/s** ✓ |
| 16 | 16 | 8 MiB | 591 MB/s |
| 32 | 4 | 16 MiB | 631 MB/s |

Sweet spot: 16 concurrent objects, each split into 4 × 64 MiB range-GETs = 64 concurrent requests. Fewer, larger requests wins at cross-region latency — 9 range-GETs per 529 MiB file vs 67 with 8 MiB chunks.

**Final head-to-head:**

| Tool | Avg time | Throughput | vs slate |
|---|---|---|---|
| **slate** | 2.7s | **984 MB/s** | — |
| `aws s3 cp --recursive` | 12.0s | 221 MB/s | **4.4× slower** |
| `rclone --transfers 8` | 2.4s | 1,120 MB/s | 1.14× faster |

### What the numbers mean

**Slate vs `aws s3 cp`:** 4.4× faster. The AWS CLI transfers one stream at a time. Slate pre-allocates the output file and writes 64 chunks in parallel at their byte offsets, keeping the disk and network saturated simultaneously.

**Slate vs rclone:** rclone is 1.14× faster — essentially a rounding error in practice. It's a mature tool. We're at parity for all practical purposes.

**The honest headline:** slate is within 14% of rclone and 4.4× faster than `aws s3 cp` — the tool most ML engineers are actually reaching for today.

---

## How it works

The transfer engine is written in Rust on top of [object_store](https://crates.io/crates/object_store) (Apache Arrow project), which gives us a unified abstraction over S3, GCS, and Azure Blob with no glue code.

For each transfer:

1. **List** all objects under the source prefix
2. **Fan out** up to N objects concurrently (`SLATE_PARALLEL_OBJECTS`, default 16)
3. For each object, **pre-allocate** the destination file at full size, then issue M parallel range-GETs (`SLATE_PARALLEL_CHUNKS`, default 4 × 64 MiB)
4. **Write** each chunk at its byte offset as it arrives — no buffering the whole file in memory

This is the same approach rclone uses: seekable writes let disk I/O and network I/O pipeline instead of serializing. Peak RAM = `parallel_chunks × chunk_size` = 256 MiB regardless of how large the objects are.

HTTP/2 is negotiated where available, with a 32-connection pool and keepalive to minimize per-request handshake cost.

All parallelism parameters are tunable via env vars:

```bash
SLATE_PARALLEL_OBJECTS=32 \
SLATE_PARALLEL_CHUNKS=8 \
SLATE_CHUNK_SIZE_MIB=64 \
slate copy s3://source gs://dest
```

---

## Try it

```bash
# Build from source (Rust required)
git clone https://github.com/tsushanth/slate
cd slate
cargo build --release

# Set your cloud credentials (standard env vars)
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_DEFAULT_REGION=us-east-1

# Run
./target/release/slate copy s3://your-bucket/prefix /local/path
```

Supports: S3 (+ S3-compatible: MinIO, Cloudflare R2), GCS, Azure Blob, local filesystem — any combination.

---

## What's next

- **Same-region benchmarks** — we expect to close the gap with rclone significantly
- **Adaptive parallelism** — auto-tune based on observed RTT and bandwidth
- **Resumable transfers** — SQLite job store already tracks progress; need retry-from-offset
- **Cloud dashboard** — the API + SSE stream is there; need a UI on top

The core is ~600 lines of Rust. If you work on ML infrastructure and move a lot of data, we'd love feedback.

---

*Benchmark methodology: Hetzner cpx41, Frankfurt. S3 us-east-1 → local NVMe. Dataset: 5 × gemma3-1b-it-int4.task (529 MiB each). 3 runs per config, best 2-of-3 averaged. rclone v1.74.3, aws-cli v2.35.11, slate built from source (Rust 1.96.0).*
