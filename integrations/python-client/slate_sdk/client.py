"""
Slate Python SDK — wraps the Slate REST API.

Usage:
    from slate_sdk import SlateClient

    client = SlateClient("http://slate-api:3030")

    # Submit and wait (blocking)
    job = client.transfer("s3://my-bucket/data", "gs://other-bucket/data")
    print(f"Done: {job.bytes_transferred} bytes at {job.peak_throughput_mbps:.1f} MB/s")

    # Submit only (non-blocking)
    job = client.submit(src="s3://...", dst="gs://...")
    job = client.wait(job.id)

    # Pipeline: job B starts only after job A completes
    a = client.submit(src="s3://raw/", dst="gs://stage/")
    b = client.submit(src="gs://stage/", dst="gs://prod/", depends_on=a.id)
    client.wait(b.id)
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import requests


class SlateError(Exception):
    pass


class SlateJobFailed(SlateError):
    def __init__(self, job: "SlateJob"):
        self.job = job
        super().__init__(
            f"Slate job {job.id} failed after {job.attempt} attempt(s): {job.error}"
        )


@dataclass
class SlateJob:
    id: str
    src: str
    dst: str
    status: str
    priority: int
    attempt: int
    max_attempts: int
    bytes_total: Optional[int]
    bytes_transferred: int
    peak_throughput_mbps: Optional[float]
    error: Optional[str]
    created_at: str
    updated_at: str
    started_at: Optional[str]
    completed_at: Optional[str]
    depends_on: Optional[str]
    callback_url: Optional[str]
    run_after: Optional[str]

    @classmethod
    def from_dict(cls, d: dict) -> "SlateJob":
        return cls(
            id=d["id"],
            src=d["src"],
            dst=d["dst"],
            status=d["status"],
            priority=d.get("priority", 0),
            attempt=d.get("attempt", 0),
            max_attempts=d.get("max_attempts", 3),
            bytes_total=d.get("bytes_total"),
            bytes_transferred=d.get("bytes_transferred", 0),
            peak_throughput_mbps=d.get("peak_throughput_mbps"),
            error=d.get("error"),
            created_at=d["created_at"],
            updated_at=d["updated_at"],
            started_at=d.get("started_at"),
            completed_at=d.get("completed_at"),
            depends_on=d.get("depends_on"),
            callback_url=d.get("callback_url"),
            run_after=d.get("run_after"),
        )

    @property
    def is_terminal(self) -> bool:
        return self.status in ("completed", "failed", "cancelled")

    @property
    def succeeded(self) -> bool:
        return self.status == "completed"

    @property
    def gb_transferred(self) -> float:
        return self.bytes_transferred / 1_073_741_824


_TERMINAL = {"completed", "failed", "cancelled"}


class SlateClient:
    """
    Thread-safe Slate API client.

    Args:
        base_url: Base URL of the slate-api server, e.g. "http://localhost:3030"
        timeout:  HTTP request timeout in seconds (default 30).
                  Does not apply to wait() — use wait(timeout=...) for that.
    """

    def __init__(self, base_url: str, timeout: int = 30):
        self.base_url = base_url.rstrip("/")
        self._session = requests.Session()
        self._timeout = timeout

    # ------------------------------------------------------------------
    # Core API wrappers
    # ------------------------------------------------------------------

    def submit(
        self,
        src: str,
        dst: str,
        *,
        priority: int = 0,
        max_attempts: int = 3,
        depends_on: Optional[str] = None,
        callback_url: Optional[str] = None,
        run_after: Optional[datetime] = None,
    ) -> SlateJob:
        """Submit a transfer job and return immediately (non-blocking)."""
        payload: dict = {"src": src, "dst": dst}
        if priority:
            payload["priority"] = priority
        if max_attempts != 3:
            payload["max_attempts"] = max_attempts
        if depends_on:
            payload["depends_on"] = depends_on
        if callback_url:
            payload["callback_url"] = callback_url
        if run_after:
            payload["run_after"] = run_after.isoformat()

        resp = self._session.post(
            f"{self.base_url}/jobs",
            json=payload,
            timeout=self._timeout,
        )
        resp.raise_for_status()
        return SlateJob.from_dict(resp.json())

    def get(self, job_id: str) -> SlateJob:
        """Fetch current job state."""
        resp = self._session.get(
            f"{self.base_url}/jobs/{job_id}", timeout=self._timeout
        )
        resp.raise_for_status()
        return SlateJob.from_dict(resp.json())

    def list(self) -> list[SlateJob]:
        """List recent jobs (latest 200)."""
        resp = self._session.get(f"{self.base_url}/jobs", timeout=self._timeout)
        resp.raise_for_status()
        return [SlateJob.from_dict(j) for j in resp.json()]

    def cancel(self, job_id: str) -> bool:
        """Cancel a queued job. Returns True if cancelled, False if already running/done."""
        resp = self._session.post(
            f"{self.base_url}/jobs/{job_id}/cancel", timeout=self._timeout
        )
        if resp.status_code == 400:
            return False
        resp.raise_for_status()
        return resp.json().get("cancelled", False)

    def cost(self) -> dict:
        """Return aggregate egress cost across all completed jobs."""
        resp = self._session.get(f"{self.base_url}/cost", timeout=self._timeout)
        resp.raise_for_status()
        return resp.json()

    def job_cost(self, job_id: str) -> dict:
        """Return egress cost estimate for a specific job."""
        resp = self._session.get(
            f"{self.base_url}/jobs/{job_id}/cost", timeout=self._timeout
        )
        resp.raise_for_status()
        return resp.json()

    def healthz(self) -> bool:
        """Return True if the API server is reachable and healthy."""
        try:
            resp = self._session.get(
                f"{self.base_url}/healthz", timeout=self._timeout
            )
            return resp.status_code == 200
        except requests.RequestException:
            return False

    # ------------------------------------------------------------------
    # High-level helpers
    # ------------------------------------------------------------------

    def wait(
        self,
        job_id: str,
        *,
        poll_interval: float = 3.0,
        timeout: Optional[float] = None,
        on_progress=None,
    ) -> SlateJob:
        """
        Block until the job reaches a terminal state.

        Args:
            job_id:        Job ID to poll.
            poll_interval: Seconds between polls (default 3).
            timeout:       Raise SlateError after this many seconds (default: no limit).
            on_progress:   Optional callable(job: SlateJob) called on each poll.

        Returns:
            The completed SlateJob.

        Raises:
            SlateJobFailed: if the job reaches status=failed.
            SlateError:     on timeout or API errors.
        """
        deadline = time.monotonic() + timeout if timeout else None
        while True:
            job = self.get(job_id)
            if on_progress:
                on_progress(job)
            if job.is_terminal:
                if not job.succeeded:
                    raise SlateJobFailed(job)
                return job
            if deadline and time.monotonic() > deadline:
                raise SlateError(f"Timed out waiting for job {job_id} after {timeout}s")
            time.sleep(poll_interval)

    def transfer(
        self,
        src: str,
        dst: str,
        *,
        priority: int = 0,
        max_attempts: int = 3,
        depends_on: Optional[str] = None,
        callback_url: Optional[str] = None,
        run_after: Optional[datetime] = None,
        poll_interval: float = 3.0,
        timeout: Optional[float] = None,
        on_progress=None,
    ) -> SlateJob:
        """Submit a transfer and block until it completes. Raises SlateJobFailed on failure."""
        job = self.submit(
            src,
            dst,
            priority=priority,
            max_attempts=max_attempts,
            depends_on=depends_on,
            callback_url=callback_url,
            run_after=run_after,
        )
        return self.wait(
            job.id,
            poll_interval=poll_interval,
            timeout=timeout,
            on_progress=on_progress,
        )
