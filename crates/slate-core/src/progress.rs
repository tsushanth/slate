use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgressEvent {
    pub job_id: Uuid,
    pub bytes_transferred: u64,
    pub bytes_total: Option<u64>,
    pub throughput_mbps: f64,
}
