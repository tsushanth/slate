use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Queued,
    Running,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: Uuid,
    pub src: String,
    pub dst: String,
    pub status: JobStatus,
    pub bytes_total: Option<u64>,
    pub bytes_transferred: u64,
    pub error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Job {
    pub fn new(src: String, dst: String) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4(),
            src,
            dst,
            status: JobStatus::Queued,
            bytes_total: None,
            bytes_transferred: 0,
            error: None,
            created_at: now,
            updated_at: now,
        }
    }

    pub fn progress_pct(&self) -> Option<f64> {
        self.bytes_total.map(|total| {
            if total == 0 {
                100.0
            } else {
                (self.bytes_transferred as f64 / total as f64) * 100.0
            }
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateJobRequest {
    pub src: String,
    pub dst: String,
}
