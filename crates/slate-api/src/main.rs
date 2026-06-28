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
use slate_core::{
    job::{CreateJobRequest, Job, JobStatus},
    progress::ProgressEvent,
    transfer::TransferEngine,
};
use slate_store::JobStore;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};
use tracing::info;
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

    let state = AppState { store, progress_tx };

    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/jobs", post(create_job).get(list_jobs))
        .route("/jobs/:id", get(get_job))
        .route("/jobs/:id/events", get(job_events))
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
    let job = Job::new(req.src.clone(), req.dst.clone());
    state.store.create(&job).await?;

    let job_id = job.id;
    let store = state.store.clone();
    let progress_tx = state.progress_tx.clone();
    let src = req.src.clone();
    let dst = req.dst.clone();

    tokio::spawn(async move {
        let (tx, mut rx) = mpsc::channel::<ProgressEvent>(64);

        let progress_store = store.clone();
        let bcast = progress_tx.clone();
        tokio::spawn(async move {
            while let Some(ev) = rx.recv().await {
                let _ = progress_store
                    .update_progress(ev.job_id, ev.bytes_transferred, ev.bytes_total)
                    .await;
                let _ = bcast.send(ev);
            }
        });

        match TransferEngine::run(job_id, &src, &dst, tx).await {
            Ok(bytes) => {
                let _ = store.set_status(job_id, JobStatus::Completed, None).await;
                info!(job_id = %job_id, bytes, "job completed");
            }
            Err(e) => {
                let _ = store
                    .set_status(job_id, JobStatus::Failed, Some(e.to_string()))
                    .await;
            }
        }
    });

    let job = state.store.get(job_id).await?.ok_or(AppError::NotFound)?;
    Ok((StatusCode::CREATED, Json(job)))
}

async fn list_jobs(State(state): State<AppState>) -> Result<Json<Vec<Job>>, AppError> {
    let jobs = state.store.list().await?;
    Ok(Json(jobs))
}

async fn get_job(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<Job>, AppError> {
    match state.store.get(id).await? {
        Some(job) => Ok(Json(job)),
        None => Err(AppError::NotFound),
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

#[derive(Debug)]
enum AppError {
    NotFound,
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
            AppError::Internal(e) => {
                (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
            }
        }
    }
}
