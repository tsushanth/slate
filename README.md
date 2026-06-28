# Slate

**Open-source data orchestrator for AI workloads.**

Move datasets, model weights, and checkpoints between object stores at high throughput. Single binary. No config files. No vendor lock-in.

```
Benchmarked: S3 us-east-1 → Hetzner Frankfurt · 5 × 529 MiB model weights (2.6 GiB)

  slate           984 MB/s   ████████████████████████████████████
  rclone         1120 MB/s   ████████████████████████████████████████  1.14× faster
  aws s3 cp       221 MB/s   ████████                                  4.4× slower
```

Slate is within **14% of rclone** and **4.4× faster than `aws s3 cp`** with zero configuration. Slate's differentiation over rclone: a REST API with real-time progress streaming, persistent SQLite job history, and unified multi-cloud support in a single binary — no config files for any provider.

Full benchmark methodology in [bench/](bench/).

---

## Install

**Linux x86_64 (pre-built binary):**

```bash
curl -fsSL https://github.com/tsushanth/slate/releases/latest/download/slate-linux-x86_64.tar.gz \
  | tar xz -C /usr/local/bin
```

**Build from source (requires Rust 1.75+):**

```bash
git clone https://github.com/tsushanth/slate
cd slate
cargo build --release
# binaries: target/release/slate, target/release/slate-api
```

---

## Usage

```bash
# Copy a dataset from S3 to GCS
slate copy s3://my-bucket/datasets/imagenet gs://gcs-bucket/datasets/imagenet

# Copy model weights to local disk
slate copy s3://my-bucket/weights/llama-3-70b /mnt/nvme/weights/

# Estimate egress cost before transferring
slate copy --estimate s3://my-bucket/weights gs://gcs-bucket/weights

# List recent jobs
slate jobs

# Check status of a running job
slate status <job-id>
```

### Supported stores

| URL scheme | Provider |
|---|---|
| `s3://bucket/prefix` | AWS S3 (+ S3-compatible: MinIO, Cloudflare R2) |
| `gs://bucket/prefix` | Google Cloud Storage |
| `az://container/prefix` | Azure Blob Storage |
| `/path` or `file:///path` | Local filesystem |

Any combination of source and destination works.

---

## API server

For programmatic use or transfers you want to monitor in real time:

```bash
DATABASE_URL=sqlite:slate.db?mode=rwc slate-api
# Starts on :3030
```

```bash
# Start a transfer
curl -X POST localhost:3030/jobs \
  -H 'Content-Type: application/json' \
  -d '{"src": "s3://bucket/data", "dst": "gs://other-bucket/data"}'

# Stream real-time progress (Server-Sent Events)
curl localhost:3030/jobs/<id>/events

# List all jobs
curl localhost:3030/jobs

# Get job by ID
curl localhost:3030/jobs/<id>

# Health check
curl localhost:3030/healthz
```

---

## How it works

For each transfer, Slate:

1. **Lists** all objects under the source prefix
2. **Fans out** up to 16 objects concurrently
3. For each object, **pre-allocates** the destination file at full size, then issues 4 parallel 64 MiB range-GETs
4. **Writes** each chunk at its byte offset as it arrives — no buffering the full file in memory

Pre-allocating and writing chunks at their offsets (seekable writes) lets disk I/O and network I/O pipeline concurrently instead of serializing. Peak RAM = `parallel_chunks × chunk_size` = 256 MiB regardless of object size. Larger chunks (64 MiB vs the 8 MiB default of many tools) dramatically reduce per-request overhead at cross-region latencies.

HTTP/2 is negotiated where available with a 32-connection pool and keepalive pings to minimize per-request handshake cost.

All jobs are persisted to SQLite — restarts are safe and history is retained.

### Tuning

All parallelism parameters are runtime-configurable with no config file:

```bash
SLATE_PARALLEL_OBJECTS=32 \
SLATE_PARALLEL_CHUNKS=8 \
SLATE_CHUNK_SIZE_MIB=64 \
slate copy s3://source gs://dest
```

| Variable | Default | What it controls |
|---|---|---|
| `SLATE_PARALLEL_OBJECTS` | 16 | Objects transferred concurrently |
| `SLATE_PARALLEL_CHUNKS` | 4 | Range-GETs in flight per object |
| `SLATE_CHUNK_SIZE_MIB` | 64 | Chunk size — larger = fewer requests = less RTT overhead |
| `SLATE_STRATEGY` | `chunked` | `chunked` (seekable range-GETs, default) or `stream` (full-object streaming) |

---

## Credentials

Standard environment variables — no config file needed:

- **AWS / S3-compatible**: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`
- **GCS**: `GOOGLE_SERVICE_ACCOUNT` or `GOOGLE_APPLICATION_CREDENTIALS`
- **Azure**: `AZURE_STORAGE_ACCOUNT_NAME`, `AZURE_STORAGE_ACCESS_KEY`

---

## Architecture

Rust workspace with four crates:

| Crate | Role |
|---|---|
| `slate-core` | Transfer engine: seekable parallel range-GETs, object-level fan-out |
| `slate-store` | SQLite job store via sqlx |
| `slate-api` | axum REST API with SSE progress streaming |
| `slate-cli` | clap CLI with indicatif progress bar |

Built on [object_store](https://crates.io/crates/object_store) (Apache Arrow project) for a unified abstraction over S3, GCS, and Azure Blob.

---

## What's next

- **Resumable transfers** — SQLite job store already tracks progress; need retry-from-offset
- **Adaptive parallelism** — auto-tune chunk size and concurrency based on observed RTT
- **Web dashboard** — API + SSE stream is in place; need a UI
- **macOS / Windows binaries** — currently Linux x86_64 only

PRs welcome. Open an issue if you hit a bug or want to discuss a feature.
