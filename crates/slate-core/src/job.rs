use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Queued,
    Waiting,    // blocked on depends_on job
    Running,
    Completed,
    Retrying,   // failed, will retry after backoff
    Failed,     // terminal: exhausted max_attempts
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: Uuid,
    pub src: String,
    pub dst: String,
    pub status: JobStatus,
    /// Higher value = picked up sooner by the worker.
    pub priority: i32,
    /// Which attempt we're on (0 = not yet started, 1 = first attempt, …).
    pub attempt: u32,
    pub max_attempts: u32,
    /// Don't start before this time (used for retry backoff and explicit scheduling).
    pub run_after: Option<DateTime<Utc>>,
    /// Block until this job has status = completed.
    pub depends_on: Option<Uuid>,
    /// HTTP POST this URL when the job reaches a terminal state (completed or failed).
    pub callback_url: Option<String>,
    pub bytes_total: Option<u64>,
    pub bytes_transferred: u64,
    pub peak_throughput_mbps: Option<f64>,
    pub error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
}

impl Job {
    pub fn new(req: CreateJobRequest) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4(),
            src: req.src,
            dst: req.dst,
            status: JobStatus::Queued,
            priority: req.priority.unwrap_or(0),
            attempt: 0,
            max_attempts: req.max_attempts.unwrap_or(3),
            run_after: req.run_after,
            depends_on: req.depends_on,
            callback_url: req.callback_url,
            bytes_total: None,
            bytes_transferred: 0,
            peak_throughput_mbps: None,
            error: None,
            created_at: now,
            updated_at: now,
            started_at: None,
            completed_at: None,
        }
    }

    pub fn progress_pct(&self) -> Option<f64> {
        self.bytes_total.map(|total| {
            if total == 0 { 100.0 } else { (self.bytes_transferred as f64 / total as f64) * 100.0 }
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateJobRequest {
    pub src: String,
    pub dst: String,
    /// Higher = picked up sooner. Default 0.
    pub priority: Option<i32>,
    /// Retry up to this many times on failure. Default 3.
    pub max_attempts: Option<u32>,
    /// Don't start before this timestamp (ISO 8601).
    pub run_after: Option<DateTime<Utc>>,
    /// Block this job until the given job ID completes successfully.
    pub depends_on: Option<Uuid>,
    /// HTTP POST this URL when the job reaches a terminal state.
    pub callback_url: Option<String>,
    /// Cron expression — if set, creates a recurring schedule instead of a one-shot job.
    /// Standard 5-field cron syntax: "0 2 * * *" = every day at 2am UTC.
    pub cron: Option<String>,
}

/// A recurring schedule that spawns a new transfer job on each tick.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CronJob {
    pub id: Uuid,
    pub src: String,
    pub dst: String,
    pub cron: String,
    pub priority: i32,
    pub max_attempts: u32,
    pub callback_url: Option<String>,
    /// When the next job should be spawned.
    pub next_run_at: DateTime<Utc>,
    /// ID of the most recently spawned transfer job.
    pub last_job_id: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateCronRequest {
    pub src: String,
    pub dst: String,
    /// Standard 5-field cron: "0 2 * * *" = daily at 02:00 UTC
    pub cron: String,
    pub priority: Option<i32>,
    pub max_attempts: Option<u32>,
    pub callback_url: Option<String>,
}
