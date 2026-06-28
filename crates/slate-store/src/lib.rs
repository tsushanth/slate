use anyhow::Result;
use chrono::Utc;
use slate_core::job::{Job, JobStatus};
use sqlx::{sqlite::SqlitePoolOptions, Row, SqlitePool};
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
                id TEXT PRIMARY KEY,
                src TEXT NOT NULL,
                dst TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'queued',
                bytes_total INTEGER,
                bytes_transferred INTEGER NOT NULL DEFAULT 0,
                error TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            "#,
        )
        .execute(&pool)
        .await?;

        Ok(Self { pool })
    }

    pub async fn create(&self, job: &Job) -> Result<()> {
        sqlx::query(
            r#"INSERT INTO jobs (id, src, dst, status, bytes_total, bytes_transferred, error, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
        )
        .bind(job.id.to_string())
        .bind(&job.src)
        .bind(&job.dst)
        .bind(status_str(&job.status))
        .bind(job.bytes_total.map(|v| v as i64))
        .bind(job.bytes_transferred as i64)
        .bind(&job.error)
        .bind(job.created_at.to_rfc3339())
        .bind(job.updated_at.to_rfc3339())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get(&self, id: Uuid) -> Result<Option<Job>> {
        let id_str = id.to_string();
        let row = sqlx::query(
            "SELECT id, src, dst, status, bytes_total, bytes_transferred, error, created_at, updated_at FROM jobs WHERE id = ?",
        )
        .bind(id_str)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|r| row_to_job(&r)))
    }

    pub async fn list(&self) -> Result<Vec<Job>> {
        let rows = sqlx::query(
            "SELECT id, src, dst, status, bytes_total, bytes_transferred, error, created_at, updated_at FROM jobs ORDER BY created_at DESC LIMIT 100",
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.iter().map(|r| row_to_job(r)).collect())
    }

    pub async fn update_progress(
        &self,
        id: Uuid,
        bytes_transferred: u64,
        bytes_total: Option<u64>,
    ) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        sqlx::query(
            "UPDATE jobs SET bytes_transferred = ?, bytes_total = ?, status = 'running', updated_at = ? WHERE id = ?",
        )
        .bind(bytes_transferred as i64)
        .bind(bytes_total.map(|v| v as i64))
        .bind(now)
        .bind(id.to_string())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn set_status(&self, id: Uuid, status: JobStatus, error: Option<String>) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        sqlx::query("UPDATE jobs SET status = ?, error = ?, updated_at = ? WHERE id = ?")
            .bind(status_str(&status))
            .bind(error)
            .bind(now)
            .bind(id.to_string())
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

fn row_to_job(r: &sqlx::sqlite::SqliteRow) -> Job {
    let bytes_total: Option<i64> = r.get("bytes_total");
    Job {
        id: Uuid::parse_str(r.get::<&str, _>("id")).unwrap(),
        src: r.get("src"),
        dst: r.get("dst"),
        status: parse_status(r.get("status")),
        bytes_total: bytes_total.map(|v| v as u64),
        bytes_transferred: r.get::<i64, _>("bytes_transferred") as u64,
        error: r.get("error"),
        created_at: r.get::<&str, _>("created_at").parse().unwrap(),
        updated_at: r.get::<&str, _>("updated_at").parse().unwrap(),
    }
}

fn status_str(s: &JobStatus) -> &'static str {
    match s {
        JobStatus::Queued => "queued",
        JobStatus::Running => "running",
        JobStatus::Completed => "completed",
        JobStatus::Failed => "failed",
    }
}

fn parse_status(s: &str) -> JobStatus {
    match s {
        "running" => JobStatus::Running,
        "completed" => JobStatus::Completed,
        "failed" => JobStatus::Failed,
        _ => JobStatus::Queued,
    }
}
