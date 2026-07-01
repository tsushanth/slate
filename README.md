# Slate

**Open-source data orchestrator for AI workloads.**

Move datasets, model weights, and checkpoints between object stores with job queuing, retry, pipeline chaining, cron scheduling, cost tracking, and a REST API — in a single self-hostable binary.

```
Benchmarked: S3 us-east-1 → Hetzner Frankfurt · 5 × 529 MiB model weights

  slate           984 MB/s   ████████████████████████████████████
  rclone         1120 MB/s   ████████████████████████████████████████  1.14× faster
  aws s3 cp       221 MB/s   ████████                                  4.4× slower
```

---

## Install

**Linux x86_64 (pre-built binary):**

```bash
curl -fsSL https://github.com/tsushanth/slate/releases/latest/download/slate-linux-x86_64.tar.gz \
  | tar xz -C /usr/local/bin
```

**Python integrations:**

```bash
pip install slate-sdk                          # Python client
pip install apache-airflow-providers-slate     # Airflow operator
pip install prefect-slate                      # Prefect task
```

**Build from source (Rust 1.75+):**

```bash
git clone https://github.com/tsushanth/slate
cd slate && cargo build --release
```

---

## Quick start

```bash
# Start the API server
DATABASE_URL=sqlite:slate.db?mode=rwc slate-api

# Submit a transfer
curl -X POST localhost:3030/jobs \
  -H 'Content-Type: application/json' \
  -d '{"src": "s3://my-bucket/datasets/imagenet", "dst": "gs://other/datasets/imagenet"}'

# Or use the CLI (blocking, with progress bar)
slate copy s3://my-bucket/datasets/imagenet /mnt/nvme/datasets/imagenet
```

---

## Features

### Job queue with priority

```bash
curl -X POST localhost:3030/jobs -d '{
  "src": "s3://bucket/weights",
  "dst": "gs://other/weights",
  "priority": 10
}'
```

Jobs are queued and picked up by a background worker. Higher priority = picked up first. Configurable concurrency via `SLATE_WORKER_CONCURRENCY` (default 4).

### Retry with exponential backoff

Jobs retry automatically on failure — 30s → 5min → 30min. Configurable per job:

```bash
curl -X POST localhost:3030/jobs -d '{
  "src": "s3://bucket/data",
  "dst": "gs://other/data",
  "max_attempts": 5
}'
```

### Pipeline chaining

```bash
# Submit job A
JOB_A=$(curl -s -X POST localhost:3030/jobs \
  -d '{"src": "s3://raw/", "dst": "gs://stage/"}' | jq -r .id)

# Job B won't start until job A completes
curl -X POST localhost:3030/jobs -d "{
  \"src\": \"gs://stage/\",
  \"dst\": \"gs://prod/\",
  \"depends_on\": \"$JOB_A\"
}"
```

### Cron scheduling

Standard 5-field cron syntax:

```bash
# Daily at 2am UTC
curl -X POST localhost:3030/crons -d '{
  "src": "s3://data-lake/raw/",
  "dst": "gs://ml-staging/raw/",
  "cron": "0 2 * * *"
}'

# Every 6 hours
curl -X POST localhost:3030/crons -d '{
  "src": "s3://checkpoints/latest/",
  "dst": "/mnt/nvme/checkpoints/",
  "cron": "0 */6 * * *"
}'

# List / delete schedules
curl localhost:3030/crons
curl -X DELETE localhost:3030/crons/<id>
```

### Webhook callbacks

```bash
curl -X POST localhost:3030/jobs -d '{
  "src": "s3://bucket/data",
  "dst": "gs://other/data",
  "callback_url": "https://your-service/hooks/slate"
}'
# POST fires on completion or terminal failure with job metadata
```

### Cost tracking

```bash
curl localhost:3030/cost              # aggregate across all completed jobs
curl localhost:3030/jobs/<id>/cost    # per-job egress cost estimate
```

### Cancel queued jobs

```bash
curl -X POST localhost:3030/jobs/<id>/cancel
```

### Real-time progress (SSE)

```bash
curl localhost:3030/jobs/<id>/events
# data: {"job_id":"...","bytes_transferred":1073741824,"bytes_total":2684354560,"throughput_mbps":512.3}
```

---

## Airflow integration

```bash
pip install apache-airflow-providers-slate
```

```python
from apache_airflow_providers_slate.operators.slate import SlateTransferOperator

with DAG("ml_pipeline", schedule="@daily") as dag:
    ingest = SlateTransferOperator(
        task_id="ingest_dataset",
        src="s3://raw-data/datasets/imagenet/",
        dst="gs://ml-staging/datasets/imagenet/",
    )
    train = SlateTransferOperator(
        task_id="copy_weights",
        src="gs://ml-staging/weights/llama-3/",
        dst="/mnt/nvme/weights/",
        priority=10,
        max_attempts=5,
    )
    ingest >> train
```

Set up an Airflow connection (`slate_default`, type HTTP, host + port 3030). The operator logs progress every poll and returns job metadata via XCom.

---

## Prefect integration

```bash
pip install prefect-slate
```

```python
from prefect import flow
from prefect_slate import slate_transfer, SlateCredentials

creds = SlateCredentials(base_url="http://slate-api:3030")

@flow
def ml_pipeline():
    result = slate_transfer(
        src="s3://raw/datasets/imagenet/",
        dst="gs://staging/datasets/imagenet/",
        slate_credentials=creds,
    )
    print(f"Transferred {result['bytes_transferred']} bytes at {result['peak_throughput_mbps']:.0f} MB/s")
```

---

## API reference

| Method | Path | Description |
|---|---|---|
| `GET` | `/healthz` | Health check |
| `POST` | `/jobs` | Submit a transfer job |
| `GET` | `/jobs` | List recent jobs |
| `GET` | `/jobs/:id` | Get job by ID |
| `POST` | `/jobs/:id/cancel` | Cancel a queued job |
| `GET` | `/jobs/:id/events` | SSE real-time progress |
| `GET` | `/jobs/:id/cost` | Egress cost estimate for job |
| `GET` | `/cost` | Aggregate egress cost |
| `POST` | `/crons` | Create a recurring schedule |
| `GET` | `/crons` | List schedules |
| `GET` | `/crons/:id` | Get schedule by ID |
| `DELETE` | `/crons/:id` | Delete schedule |

---

## Supported stores

| URL scheme | Provider |
|---|---|
| `s3://bucket/prefix` | AWS S3 (+ MinIO, Cloudflare R2) |
| `gs://bucket/prefix` | Google Cloud Storage |
| `az://container/prefix` | Azure Blob Storage |
| `/path` or `file:///path` | Local filesystem |

---

## Configuration

**Transfer tuning:**

| Variable | Default | Description |
|---|---|---|
| `SLATE_PARALLEL_OBJECTS` | 16 | Objects transferred concurrently |
| `SLATE_PARALLEL_CHUNKS` | 4 | Range-GETs per object |
| `SLATE_CHUNK_SIZE_MIB` | 64 | Chunk size (larger = fewer requests at cross-region latency) |
| `SLATE_STRATEGY` | `chunked` | `chunked` or `stream` |

**Worker:**

| Variable | Default | Description |
|---|---|---|
| `SLATE_WORKER_CONCURRENCY` | 4 | Max concurrent transfers |
| `DATABASE_URL` | `sqlite:slate.db?mode=rwc` | Job store |
| `LISTEN_ADDR` | `0.0.0.0:3030` | API bind address |

**Credentials** — standard provider env vars, no config file needed:

- **AWS**: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`
- **GCS**: `GOOGLE_SERVICE_ACCOUNT` or `GOOGLE_APPLICATION_CREDENTIALS`
- **Azure**: `AZURE_STORAGE_ACCOUNT_NAME`, `AZURE_STORAGE_ACCESS_KEY`

---

## Architecture

Rust workspace:

| Crate | Role |
|---|---|
| `slate-core` | Transfer engine, job model, cost estimation |
| `slate-store` | SQLite job store + cron store |
| `slate-api` | axum REST API + background worker + cron scheduler |
| `slate-cli` | CLI with progress bar |

Built on [object_store](https://crates.io/crates/object_store) (Apache Arrow) for unified S3/GCS/Azure support.

---

## What's next

- **Postgres** — multi-node deployments and team/org scoping
- **Resumable transfers** — retry-from-offset using SQLite progress tracking
- **Adaptive parallelism** — auto-tune based on observed RTT
- **Web dashboard** — job history, cost charts, schedule management

PRs welcome.
