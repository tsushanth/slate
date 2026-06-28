# Slate

**Open-source data orchestrator for AI workloads.**

Move datasets, model weights, and checkpoints between object stores at high throughput. Single binary. No config files. No vendor lock-in.

```
Benchmarked: S3 us-east-1 → Hetzner Frankfurt · 5 × 529 MiB model weights

  slate            984 MB/s   ████████████████████████████████
  aws s3 cp        221 MB/s   ███████                          4.4× slower
  rclone          1120 MB/s   ████████████████████████████████████
```

Slate beats `aws s3 cp` by **4.4×** out of the box with zero configuration. rclone edges ahead at 1.14× — it's a mature tool. Slate's differentiation is the REST API, persistent job tracking, and unified multi-cloud support in a single binary. Full benchmark methodology in [bench/](bench/).

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
# binaries at target/release/slate and target/release/slate-api
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

For programmatic use or long-running transfers you want to monitor:

```bash
DATABASE_URL=sqlite:slate.db?mode=rwc slate-api
# Starts on :3030
```

```bash
# Start a transfer
curl -X POST localhost:3030/jobs \
  -H 'Content-Type: application/json' \
  -d '{"src": "s3://bucket/data", "dst": "gs://other-bucket/data"}'

# Stream real-time progress (SSE)
curl localhost:3030/jobs/<id>/events

# List all jobs
curl localhost:3030/jobs
```

---

## How it works

Slate fires parallel range-GETs per object and transfers multiple objects concurrently. The default configuration issues **128 concurrent requests** per transfer (8 objects × 16 chunks each), saturating available bandwidth while keeping memory bounded.

All parallelism is runtime-configurable:

```bash
SLATE_PARALLEL_OBJECTS=16 \
SLATE_PARALLEL_CHUNKS=8 \
SLATE_CHUNK_SIZE_MIB=16 \
slate copy s3://source gs://dest
```

| Variable | Default | What it controls |
|---|---|---|
| `SLATE_PARALLEL_OBJECTS` | 16 | Objects transferred concurrently |
| `SLATE_PARALLEL_CHUNKS` | 4 | Range-GETs per object in parallel |
| `SLATE_CHUNK_SIZE_MIB` | 64 | Size of each chunk (larger = fewer requests = less RTT overhead) |
| `SLATE_STRATEGY` | `chunked` | `chunked` (seekable parallel range-GETs, default) or `stream` (full-object streaming) |

HTTP/2 is negotiated where available (S3, GCS), with a 32-connection pool and keepalive to reduce per-request overhead.

Jobs are persisted to SQLite — process restarts are safe and history is retained.

---

## Credentials

Standard environment variables — no config file needed:

- **AWS**: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`
- **GCS**: `GOOGLE_SERVICE_ACCOUNT` or `GOOGLE_APPLICATION_CREDENTIALS`
- **Azure**: `AZURE_STORAGE_ACCOUNT_NAME`, `AZURE_STORAGE_ACCESS_KEY`

---

## Architecture

Rust workspace with four crates:

| Crate | Role |
|---|---|
| `slate-core` | Transfer engine: parallel chunked range-GETs, object-level fan-out |
| `slate-store` | SQLite job store via sqlx |
| `slate-api` | axum REST API with SSE progress streaming |
| `slate-cli` | clap CLI with indicatif progress bar |

Built on [object_store](https://crates.io/crates/object_store) (Apache Arrow project) for a unified abstraction over S3, GCS, and Azure Blob.

---

## What's next

- Same-region benchmarks (expecting to close the rclone gap)
- Adaptive parallelism — auto-tune based on observed RTT and bandwidth
- Resumable transfers — SQLite job store tracks progress; need retry-from-offset
- Web dashboard — the API + SSE stream is there; need a UI

PRs welcome.
