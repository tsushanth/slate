# Slate

Open-source, self-hostable data orchestrator for AI workloads.

Move datasets, model weights, and artifacts between object stores with high throughput, durable job tracking, and zero vendor lock-in.

## Supported stores

| URL scheme | Provider |
|---|---|
| `s3://bucket/prefix` | AWS S3 (and S3-compatible: MinIO, Cloudflare R2) |
| `gs://bucket/prefix` | Google Cloud Storage |
| `az://container/prefix` | Azure Blob Storage |
| `/path` or `file:///path` | Local filesystem |

## Quick start

```bash
# Copy an S3 prefix to GCS
slate copy s3://my-bucket/datasets/imagenet gs://gcs-bucket/datasets/imagenet

# Estimate egress cost before transferring
slate copy --estimate s3://my-bucket/weights gs://gcs-bucket/weights

# List recent jobs
slate jobs

# Check job status
slate status <job-id>
```

## API server

```bash
DATABASE_URL=sqlite:slate.db?mode=rwc slate-api
```

### Endpoints

```
GET  /healthz
POST /jobs                   { "src": "s3://...", "dst": "gs://..." }
GET  /jobs                   list all jobs
GET  /jobs/:id               get job by ID
GET  /jobs/:id/events        SSE real-time progress stream
```

## Build

```bash
cargo build --release
# Binaries: target/release/slate, target/release/slate-api
```

## Configuration

Credentials are read from environment variables using each provider's standard conventions:

- **AWS**: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`
- **GCS**: `GOOGLE_SERVICE_ACCOUNT` or `GOOGLE_APPLICATION_CREDENTIALS`
- **Azure**: `AZURE_STORAGE_ACCOUNT_NAME`, `AZURE_STORAGE_ACCESS_KEY`

## How it works

- Objects larger than 16 MiB are transferred using parallel 16 MiB chunks (8 concurrent per object)
- All jobs are persisted to SQLite — restarts are safe, history is retained
- The API server streams real-time progress via Server-Sent Events
