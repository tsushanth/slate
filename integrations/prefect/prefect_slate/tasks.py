"""
Prefect tasks for Slate data transfers.

Example flow:

    from prefect import flow
    from prefect_slate import slate_transfer, SlateCredentials

    creds = SlateCredentials(base_url="http://slate-api:3030")

    @flow(name="ml-pipeline")
    def ml_pipeline():
        # Ingest raw dataset
        ingest = slate_transfer(
            src="s3://raw-data/datasets/imagenet/",
            dst="gs://ml-staging/datasets/imagenet/",
            slate_credentials=creds,
        )

        # Copy weights to GPU node — depends on ingest completing
        slate_transfer(
            src="gs://ml-staging/weights/llama-3/",
            dst="/mnt/nvme/weights/",
            slate_credentials=creds,
            wait_for=[ingest],
        )

    ml_pipeline()
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

from prefect import task, get_run_logger
from prefect.blocks.core import Block
from pydantic import SecretStr

from slate_sdk import SlateClient, SlateJob

log = logging.getLogger(__name__)


@dataclass
class SlateCredentials:
    """
    Connection details for a Slate API server.

    Args:
        base_url:     Slate API base URL, e.g. "http://slate-api:3030"
        timeout:      HTTP request timeout in seconds (default 30)
    """
    base_url: str
    timeout: int = 30

    def get_client(self) -> SlateClient:
        return SlateClient(self.base_url, timeout=self.timeout)


@task(
    name="slate-transfer",
    description="Transfer data between object stores using Slate.",
    retries=0,  # Slate handles its own retries via max_attempts
    tags=["slate", "data-transfer"],
)
def slate_transfer(
    src: str,
    dst: str,
    slate_credentials: SlateCredentials,
    *,
    priority: int = 0,
    max_attempts: int = 3,
    depends_on_job: Optional[str] = None,
    callback_url: Optional[str] = None,
    poll_interval: float = 3.0,
    timeout: Optional[float] = None,
) -> dict:
    """
    Transfer data between object stores using Slate.

    Args:
        src:                Source URL — s3://, gs://, az://, /local/path
        dst:                Destination URL
        slate_credentials:  SlateCredentials with the API base URL
        priority:           Job priority — higher is picked up first (default 0)
        max_attempts:       Max transfer retries on failure (default 3)
        depends_on_job:     Slate job ID to wait for before starting (for server-side chaining)
        callback_url:       Webhook URL called on completion/failure
        poll_interval:      Seconds between status polls (default 3)
        timeout:            Raise after this many seconds (default: no limit)

    Returns:
        dict with job_id, bytes_transferred, peak_throughput_mbps, started_at, completed_at

    Raises:
        SlateJobFailed if the transfer fails after all retries.
    """
    try:
        logger = get_run_logger()
    except Exception:
        logger = log

    client = slate_credentials.get_client()

    logger.info("Submitting Slate transfer: %s → %s", src, dst)
    job = client.submit(
        src,
        dst,
        priority=priority,
        max_attempts=max_attempts,
        depends_on=depends_on_job,
        callback_url=callback_url,
    )
    logger.info("Job %s queued (attempt will be made by Slate worker)", job.id)

    def on_progress(j: SlateJob) -> None:
        pct = ""
        if j.bytes_total:
            pct = f" ({j.bytes_transferred / j.bytes_total * 100:.1f}%)"
        logger.info(
            "slate job %s — %s  %s%s  %.1f MB/s",
            j.id,
            j.status,
            _human(j.bytes_transferred),
            pct,
            j.peak_throughput_mbps or 0,
        )

    job = client.wait(
        job.id,
        poll_interval=poll_interval,
        timeout=timeout,
        on_progress=on_progress,
    )

    logger.info(
        "Transfer complete: %s at %.1f MB/s (job %s)",
        _human(job.bytes_transferred),
        job.peak_throughput_mbps or 0,
        job.id,
    )

    return {
        "job_id": job.id,
        "src": job.src,
        "dst": job.dst,
        "bytes_transferred": job.bytes_transferred,
        "peak_throughput_mbps": job.peak_throughput_mbps,
        "started_at": job.started_at,
        "completed_at": job.completed_at,
    }


def _human(b: int) -> str:
    if b >= 1_000_000_000:
        return f"{b / 1_000_000_000:.2f} GB"
    if b >= 1_000_000:
        return f"{b / 1_000_000:.2f} MB"
    if b >= 1_000:
        return f"{b / 1_000:.2f} KB"
    return f"{b} B"
