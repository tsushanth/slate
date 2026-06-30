"""
SlateTransferOperator — move data between object stores from an Airflow DAG.

Example DAG:

    from apache_airflow_providers_slate.operators.slate import SlateTransferOperator

    with DAG("ml_pipeline", schedule="@daily") as dag:
        ingest = SlateTransferOperator(
            task_id="ingest_dataset",
            src="s3://raw-data/datasets/imagenet/",
            dst="gs://ml-staging/datasets/imagenet/",
        )

        validate = PythonOperator(task_id="validate", python_callable=validate_fn)

        train = SlateTransferOperator(
            task_id="copy_weights_to_gpu",
            src="gs://ml-staging/weights/llama-3/",
            dst="/mnt/nvme/weights/",
            priority=10,
        )

        ingest >> validate >> train
"""

from __future__ import annotations

import logging
from typing import Any, Optional

from airflow.models import BaseOperator

from apache_airflow_providers_slate.hooks.slate import SlateHook
from slate_sdk import SlateJob

log = logging.getLogger(__name__)


class SlateTransferOperator(BaseOperator):
    """
    Transfer data between object stores using Slate.

    Submits a job to the Slate API and blocks until it completes.
    Raises AirflowException if the transfer fails (after all retries).

    :param src:            Source URL — s3://, gs://, az://, /local/path
    :param dst:            Destination URL
    :param slate_conn_id:  Airflow connection ID for the Slate API (default: slate_default)
    :param priority:       Job priority — higher is picked up first (default: 0)
    :param max_attempts:   Max transfer attempts on failure (default: 3)
    :param poll_interval:  Seconds between status polls (default: 5)
    :param transfer_timeout: Raise after this many seconds regardless of status (default: None)
    """

    template_fields = ("src", "dst")
    ui_color = "#f0e4ff"

    def __init__(
        self,
        *,
        src: str,
        dst: str,
        slate_conn_id: str = SlateHook.default_conn_name,
        priority: int = 0,
        max_attempts: int = 3,
        poll_interval: float = 5.0,
        transfer_timeout: Optional[float] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.src = src
        self.dst = dst
        self.slate_conn_id = slate_conn_id
        self.priority = priority
        self.max_attempts = max_attempts
        self.poll_interval = poll_interval
        self.transfer_timeout = transfer_timeout

    def execute(self, context: Any) -> dict:
        hook = SlateHook(slate_conn_id=self.slate_conn_id)
        client = hook.get_conn()

        log.info("Submitting Slate transfer: %s → %s", self.src, self.dst)
        job = client.submit(
            self.src,
            self.dst,
            priority=self.priority,
            max_attempts=self.max_attempts,
        )
        log.info("Job %s queued", job.id)

        def on_progress(j: SlateJob) -> None:
            pct = ""
            if j.bytes_total:
                pct = f" ({j.bytes_transferred / j.bytes_total * 100:.1f}%)"
            log.info(
                "Job %s — %s  %s / %s%s  %.1f MB/s",
                j.id,
                j.status,
                _human(j.bytes_transferred),
                _human(j.bytes_total or 0),
                pct,
                j.peak_throughput_mbps or 0,
            )

        job = client.wait(
            job.id,
            poll_interval=self.poll_interval,
            timeout=self.transfer_timeout,
            on_progress=on_progress,
        )

        log.info(
            "Transfer complete: %s bytes in %.1f MB/s peak  (job %s)",
            job.bytes_transferred,
            job.peak_throughput_mbps or 0,
            job.id,
        )

        # Return job metadata so downstream tasks can reference it via XCom
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
