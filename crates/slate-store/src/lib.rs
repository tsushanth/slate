use anyhow::Result;
use chrono::{DateTime, Utc};
use slate_core::job::{Job, JobStatus};
use sqlx::{Row, SqlitePool, sqlite::SqlitePoolOptions};
use uuid::Uuid;

pub struct JobStore {
    pool: SqlitePool,
}

impl JobStore {
    pub async fn new(database_url: &str) -> Result<Self> {
        let pool = SqlitePoolOptions::new()
            .max_connections(10)
            .connect(database_url)
            .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS jobs (
                id                   TEXT PRIMARY KEY,
                src                  TEXT NOT NULL,
                dst                  TEXT NOT NULL,
                status               TEXT NOT NULL DEFAULT 'queued',
                priority             INTEGER NOT NULL DEFAULT 0,
                attempt              INTEGER NOT NULL DEFAULT 0,
                max_attempts         INTEGER NOT NULL DEFAULT 3,
                run_after            TEXT,
                depends_on           TEXT,
                callback_url         TEXT,
                bytes_total          INTEGER,
                bytes_transferred    INTEGER NOT NULL DEFAULT 0,
                peak_throughput_mbps REAL,
                error                TEXT,
                created_at           TEXT NOT NULL,
                updated_at           TEXT NOT NULL,
                started_at           TEXT,
                completed_at         TEXT
            )
            "#,
        )
        .execute(&pool)
        .await?;

        Ok(Self { pool })
    }

    pub async fn create(&self, job: &Job) -> Result<()> {
        sqlx::query(
            r#"INSERT INTO jobs (
                id, src, dst, status, priority, attempt, max_attempts,
                run_after, depends_on, callback_url,
                bytes_total, bytes_transferred, peak_throughput_mbps,
                error, created_at, updated_at, started_at, completed_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"#,
        )
        .bind(job.id.to_string())
        .bind(&job.src)
        .bind(&job.dst)
        .bind(status_str(&job.status))
        .bind(job.priority)
        .bind(job.attempt as i64)
        .bind(job.max_attempts as i64)
        .bind(job.run_after.map(|t| t.to_rfc3339()))
        .bind(job.depends_on.map(|u| u.to_string()))
        .bind(&job.callback_url)
        .bind(job.bytes_total.map(|v| v as i64))
        .bind(job.bytes_transferred as i64)
        .bind(job.peak_throughput_mbps)
        .bind(&job.error)
        .bind(job.created_at.to_rfc3339())
        .bind(job.updated_at.to_rfc3339())
        .bind(job.started_at.map(|t| t.to_rfc3339()))
        .bind(job.completed_at.map(|t| t.to_rfc3339()))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get(&self, id: Uuid) -> Result<Option<Job>> {
        let row = sqlx::query(SELECT_ALL)
            .bind(id.to_string())
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|r| row_to_job(&r)))
    }

    pub async fn list(&self) -> Result<Vec<Job>> {
        let rows = sqlx::query(
            &format!("{} WHERE 1=1 ORDER BY priority DESC, created_at DESC LIMIT 200", SELECT_COLS)
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.iter().map(|r| row_to_job(r)).collect())
    }

    /// Atomically claim the next job ready to run:
    /// - status = queued
    /// - run_after is null or in the past
    /// - depends_on is null or the dependency job has status = completed
    /// Returns the claimed job (already updated to running in the DB).
    pub async fn claim_next(&self) -> Result<Option<Job>> {
        let now = Utc::now().to_rfc3339();

        // Find candidate — SQLite is single-writer so SELECT then UPDATE is safe
        let row = sqlx::query(
            r#"SELECT id FROM jobs
               WHERE status = 'queued'
                 AND (run_after IS NULL OR run_after <= ?)
                 AND (
                   depends_on IS NULL
                   OR EXISTS (
                     SELECT 1 FROM jobs dep
                     WHERE dep.id = jobs.depends_on AND dep.status = 'completed'
                   )
                 )
               ORDER BY priority DESC, created_at ASC
               LIMIT 1"#,
        )
        .bind(&now)
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else { return Ok(None) };
        let id: String = row.get("id");

        sqlx::query(
            "UPDATE jobs SET status='running', started_at=?, updated_at=?, attempt=attempt+1 WHERE id=? AND status='queued'"
        )
        .bind(&now)
        .bind(&now)
        .bind(&id)
        .execute(&self.pool)
        .await?;

        let job = sqlx::query(SELECT_ALL)
            .bind(&id)
            .fetch_optional(&self.pool)
            .await?
            .map(|r| row_to_job(&r));

        Ok(job)
    }

    pub async fn update_progress(
        &self,
        id: Uuid,
        bytes_transferred: u64,
        bytes_total: Option<u64>,
        peak_throughput_mbps: Option<f64>,
    ) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        sqlx::query(
            "UPDATE jobs SET bytes_transferred=?, bytes_total=?, peak_throughput_mbps=?, updated_at=? WHERE id=?",
        )
        .bind(bytes_transferred as i64)
        .bind(bytes_total.map(|v| v as i64))
        .bind(peak_throughput_mbps)
        .bind(&now)
        .bind(id.to_string())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn set_completed(&self, id: Uuid, peak_throughput_mbps: Option<f64>) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        sqlx::query(
            "UPDATE jobs SET status='completed', peak_throughput_mbps=?, completed_at=?, updated_at=?, error=NULL WHERE id=?"
        )
        .bind(peak_throughput_mbps)
        .bind(&now)
        .bind(&now)
        .bind(id.to_string())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Schedule a retry or mark terminal failure.
    pub async fn set_failed(&self, id: Uuid, error: &str) -> Result<()> {
        let now = Utc::now().to_rfc3339();

        // Read current attempt count
        let row = sqlx::query("SELECT attempt, max_attempts FROM jobs WHERE id=?")
            .bind(id.to_string())
            .fetch_one(&self.pool)
            .await?;
        let attempt: i64 = row.get("attempt");
        let max_attempts: i64 = row.get("max_attempts");

        if attempt < max_attempts {
            // Exponential backoff: attempt 1→30s, 2→5min, 3→30min
            let backoff_secs: i64 = match attempt {
                1 => 30,
                2 => 300,
                _ => 1800,
            };
            let run_after = (Utc::now() + chrono::Duration::seconds(backoff_secs)).to_rfc3339();
            sqlx::query(
                "UPDATE jobs SET status='queued', run_after=?, error=?, updated_at=? WHERE id=?"
            )
            .bind(&run_after)
            .bind(error)
            .bind(&now)
            .bind(id.to_string())
            .execute(&self.pool)
            .await?;
        } else {
            sqlx::query(
                "UPDATE jobs SET status='failed', error=?, completed_at=?, updated_at=? WHERE id=?"
            )
            .bind(error)
            .bind(&now)
            .bind(&now)
            .bind(id.to_string())
            .execute(&self.pool)
            .await?;
        }
        Ok(())
    }

    pub async fn cancel(&self, id: Uuid) -> Result<bool> {
        let now = Utc::now().to_rfc3339();
        let result = sqlx::query(
            "UPDATE jobs SET status='cancelled', updated_at=? WHERE id=? AND status IN ('queued','retrying')"
        )
        .bind(&now)
        .bind(id.to_string())
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    /// Cost summary: total bytes transferred from each cloud provider.
    pub async fn cost_summary(&self) -> Result<Vec<(String, u64)>> {
        let rows = sqlx::query(
            r#"SELECT
                CASE
                  WHEN src LIKE 's3://%'  THEN 'aws'
                  WHEN src LIKE 'gs://%'  THEN 'gcp'
                  WHEN src LIKE 'az://%'  THEN 'azure'
                  ELSE 'local'
                END as provider,
                SUM(bytes_transferred) as total_bytes
               FROM jobs
               WHERE status = 'completed'
               GROUP BY provider"#,
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .iter()
            .map(|r| {
                let provider: String = r.get("provider");
                let total: i64 = r.get("total_bytes");
                (provider, total as u64)
            })
            .collect())
    }
}

const SELECT_COLS: &str = r#"SELECT
    id, src, dst, status, priority, attempt, max_attempts,
    run_after, depends_on, callback_url,
    bytes_total, bytes_transferred, peak_throughput_mbps,
    error, created_at, updated_at, started_at, completed_at
  FROM jobs"#;

const SELECT_ALL: &str = r#"SELECT
    id, src, dst, status, priority, attempt, max_attempts,
    run_after, depends_on, callback_url,
    bytes_total, bytes_transferred, peak_throughput_mbps,
    error, created_at, updated_at, started_at, completed_at
  FROM jobs WHERE id=?"#;

fn row_to_job(r: &sqlx::sqlite::SqliteRow) -> Job {
    Job {
        id: Uuid::parse_str(r.get::<&str, _>("id")).unwrap(),
        src: r.get("src"),
        dst: r.get("dst"),
        status: parse_status(r.get("status")),
        priority: r.get("priority"),
        attempt: r.get::<i64, _>("attempt") as u32,
        max_attempts: r.get::<i64, _>("max_attempts") as u32,
        run_after: parse_opt_dt(r.get("run_after")),
        depends_on: r.get::<Option<&str>, _>("depends_on")
            .and_then(|s| Uuid::parse_str(s).ok()),
        callback_url: r.get("callback_url"),
        bytes_total: r.get::<Option<i64>, _>("bytes_total").map(|v| v as u64),
        bytes_transferred: r.get::<i64, _>("bytes_transferred") as u64,
        peak_throughput_mbps: r.get("peak_throughput_mbps"),
        error: r.get("error"),
        created_at: r.get::<&str, _>("created_at").parse().unwrap(),
        updated_at: r.get::<&str, _>("updated_at").parse().unwrap(),
        started_at: parse_opt_dt(r.get("started_at")),
        completed_at: parse_opt_dt(r.get("completed_at")),
    }
}

fn parse_opt_dt(s: Option<&str>) -> Option<DateTime<Utc>> {
    s.and_then(|s| s.parse().ok())
}

fn status_str(s: &JobStatus) -> &'static str {
    match s {
        JobStatus::Queued => "queued",
        JobStatus::Waiting => "waiting",
        JobStatus::Running => "running",
        JobStatus::Completed => "completed",
        JobStatus::Retrying => "retrying",
        JobStatus::Failed => "failed",
        JobStatus::Cancelled => "cancelled",
    }
}

fn parse_status(s: &str) -> JobStatus {
    match s {
        "waiting" => JobStatus::Waiting,
        "running" => JobStatus::Running,
        "completed" => JobStatus::Completed,
        "retrying" => JobStatus::Retrying,
        "failed" => JobStatus::Failed,
        "cancelled" => JobStatus::Cancelled,
        _ => JobStatus::Queued,
    }
}
