mod worker;

use anyhow::Result;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Json, Response},
    response::sse::{Event, KeepAlive, Sse},
    routing::{get, post},
    Router,
};
use futures::stream;
use serde_json::json;
use slate_core::{
    cost,
    job::{CreateCronRequest, CreateJobRequest, Job},
    progress::ProgressEvent,
};
use slate_store::JobStore;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::info;
use chrono;
use uuid::Uuid;

#[derive(Clone)]
struct AppState {
    store: Arc<JobStore>,
    progress_tx: broadcast::Sender<ProgressEvent>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "slate_api=info,slate_core=info".into()),
        )
        .init();

    let db_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "sqlite:slate.db?mode=rwc".to_string());

    let store = Arc::new(JobStore::new(&db_url).await?);
    let (progress_tx, _) = broadcast::channel::<ProgressEvent>(1024);

    let concurrency: usize = std::env::var("SLATE_WORKER_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(4);

    worker::start(store.clone(), progress_tx.clone(), concurrency);
    info!("worker started (concurrency={concurrency})");

    let state = AppState { store, progress_tx };

    let app = Router::new()
        .route("/healthz", get(healthz))
        // Jobs
        .route("/jobs", post(create_job).get(list_jobs))
        .route("/jobs/:id", get(get_job))
        .route("/jobs/:id/cancel", post(cancel_job))
        .route("/jobs/:id/events", get(job_events))
        // Cost
        .route("/cost", get(cost_summary))
        .route("/jobs/:id/cost", get(job_cost))
        // Cron schedules
        .route("/crons", post(create_cron).get(list_crons))
        .route("/crons/:id", get(get_cron).delete(delete_cron))
        .with_state(state)
        .layer(tower_http::cors::CorsLayer::permissive())
        .layer(tower_http::trace::TraceLayer::new_for_http());

    let addr = std::env::var("LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:3030".to_string());
    info!("slate-api listening on {addr}");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn healthz() -> &'static str {
    "ok"
}

async fn create_job(
    State(state): State<AppState>,
    Json(req): Json<CreateJobRequest>,
) -> Result<(StatusCode, Json<Job>), AppError> {
    // Validate dependency exists
    if let Some(dep_id) = req.depends_on {
        if state.store.get(dep_id).await?.is_none() {
            return Err(AppError::BadRequest(format!(
                "depends_on job {dep_id} does not exist"
            )));
        }
    }

    let job = Job::new(req);
    state.store.create(&job).await?;
    info!(job_id = %job.id, src = %job.src, dst = %job.dst, "job queued");

    let job = state.store.get(job.id).await?.ok_or(AppError::NotFound)?;
    Ok((StatusCode::CREATED, Json(job)))
}

async fn list_jobs(State(state): State<AppState>) -> Result<Json<Vec<Job>>, AppError> {
    Ok(Json(state.store.list().await?))
}

async fn get_job(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<Job>, AppError> {
    state.store.get(id).await?.map(Json).ok_or(AppError::NotFound)
}

async fn cancel_job(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<serde_json::Value>, AppError> {
    let cancelled = state.store.cancel(id).await?;
    if cancelled {
        Ok(Json(json!({"cancelled": true, "job_id": id})))
    } else {
        Err(AppError::BadRequest(
            "job not found or is already running/completed".into(),
        ))
    }
}

async fn job_events(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Sse<impl futures::Stream<Item = Result<Event, std::convert::Infallible>>> {
    let rx = state.progress_tx.subscribe();
    let s = stream::unfold(rx, move |mut rx| async move {
        loop {
            match rx.recv().await {
                Ok(ev) if ev.job_id == id => {
                    let data = serde_json::to_string(&ev).unwrap_or_default();
                    return Some((Ok(Event::default().data(data)), rx));
                }
                Ok(_) => continue,
                Err(_) => return None,
            }
        }
    });
    Sse::new(s).keep_alive(KeepAlive::default())
}

async fn job_cost(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<serde_json::Value>, AppError> {
    let job = state.store.get(id).await?.ok_or(AppError::NotFound)?;
    let bytes = job.bytes_transferred;
    let estimate = cost::estimate(&job.src, bytes);
    Ok(Json(json!({
        "job_id": id,
        "src": job.src,
        "bytes_transferred": bytes,
        "provider": estimate.provider,
        "rate_per_gb_usd": estimate.rate_per_gb,
        "estimated_egress_usd": (estimate.estimated_usd * 10000.0).round() / 10000.0,
    })))
}

async fn cost_summary(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, AppError> {
    let rows = state.store.cost_summary().await?;
    let breakdown: Vec<_> = rows
        .iter()
        .map(|(provider, bytes)| {
            let rate = match provider.as_str() {
                "aws" => 0.09,
                "gcp" => 0.12,
                "azure" => 0.087,
                _ => 0.0,
            };
            let gb = *bytes as f64 / 1_073_741_824.0;
            json!({
                "provider": provider,
                "bytes_transferred": bytes,
                "gb_transferred": (gb * 100.0).round() / 100.0,
                "rate_per_gb_usd": rate,
                "estimated_egress_usd": (gb * rate * 10000.0).round() / 10000.0,
            })
        })
        .collect();

    let total_usd: f64 = rows.iter().map(|(provider, bytes)| {
        let rate = match provider.as_str() {
            "aws" => 0.09,
            "gcp" => 0.12,
            "azure" => 0.087,
            _ => 0.0,
        };
        (*bytes as f64 / 1_073_741_824.0) * rate
    }).sum();

    Ok(Json(json!({
        "breakdown": breakdown,
        "total_estimated_egress_usd": (total_usd * 10000.0).round() / 10000.0,
    })))
}

async fn create_cron(
    State(state): State<AppState>,
    Json(req): Json<CreateCronRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), AppError> {
    let next = worker::next_cron_run(&req.cron, chrono::Utc::now())
        .map_err(|e| AppError::BadRequest(e.to_string()))?;
    let entry = state.store.create_cron(&req, next).await?;
    info!(cron_id = %entry.id, cron = %entry.cron, next = %entry.next_run_at, "cron created");
    Ok((StatusCode::CREATED, Json(serde_json::to_value(&entry).unwrap())))
}

async fn list_crons(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, AppError> {
    let crons = state.store.list_crons().await?;
    Ok(Json(serde_json::to_value(crons).unwrap()))
}

async fn get_cron(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<serde_json::Value>, AppError> {
    let entry = state.store.get_cron(id).await?.ok_or(AppError::NotFound)?;
    Ok(Json(serde_json::to_value(entry).unwrap()))
}

async fn delete_cron(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<serde_json::Value>, AppError> {
    if state.store.delete_cron(id).await? {
        Ok(Json(json!({"deleted": true, "id": id})))
    } else {
        Err(AppError::NotFound)
    }
}

#[derive(Debug)]
enum AppError {
    NotFound,
    BadRequest(String),
    Internal(anyhow::Error),
}

impl From<anyhow::Error> for AppError {
    fn from(e: anyhow::Error) -> Self {
        AppError::Internal(e)
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        match self {
            AppError::NotFound => (StatusCode::NOT_FOUND, "not found").into_response(),
            AppError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg).into_response(),
            AppError::Internal(e) => {
                (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
            }
        }
    }
}
