<!-- Generated 2026-06-28 -->

## Slate Benchmark Results

**Environment:** Hetzner cpx41 (8 vCPU, 16 GB RAM) · Frankfurt, Germany
**Source:** AWS S3 us-east-1 → Hetzner Frankfurt (cross-region, cross-provider)
**Dataset:** 5 files × 529 MiB = 2,645 MiB of real ML model weights (Gemma 3 1B)
**Methodology:** 6-config parallelism sweep → 3 runs at best config, best 2-of-3 averaged

### Parallelism Sweep

| objects | chunks/obj | chunk size | avg time | throughput |
|---------|-----------|------------|----------|------------|
| 4 | 8 | 16 MiB | 4.976s | **531.5 MB/s** |
| 8 | 8 | 16 MiB | 4.724s | **559.9 MB/s** |
| 16 | 8 | 16 MiB | 4.407s | **600.1 MB/s** |
| 8 | 16 | 8 MiB | 4.132s | **640.1 MB/s** ✓ best |
| 16 | 16 | 8 MiB | 4.475s | **591.0 MB/s** |
| 32 | 4 | 16 MiB | 4.188s | **631.5 MB/s** |

### Final Head-to-Head

| Tool | Config | Avg time | Throughput | vs slate |
|------|--------|----------|------------|----------|
| **slate** | obj=8 · chunks=16 · csz=8MiB | 4.117s | **642 MB/s** | — |
| aws s3 cp | --recursive (default) | 11.995s | 220 MB/s | 2.91× slower |
| rclone | --transfers 8 --checkers 16 | 2.304s | 1,148 MB/s | 1.79× faster |
