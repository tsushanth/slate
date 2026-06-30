use slate_core::{progress::ProgressEvent, transfer::TransferEngine};
use slate_store::JobStore;
use std::sync::Arc;
use tokio::sync::{broadcast, Semaphore};
use tracing::{error, info};

/// Runs the worker event loop in the background.
/// Polls for ready jobs every second, executes up to `concurrency` transfers in parallel.
pub fn start(
    store: Arc<JobStore>,
    progress_tx: broadcast::Sender<ProgressEvent>,
    concurrency: usize,
) {
    tokio::spawn(async move {
        let sem = Arc::new(Semaphore::new(concurrency));
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval.tick().await;

            // Drain all ready jobs up to the concurrency limit
            loop {
                let Ok(permit) = sem.clone().try_acquire_owned() else { break };

                match store.claim_next().await {
                    Ok(Some(job)) => {
                        let store = store.clone();
                        let progress_tx = progress_tx.clone();
                        tokio::spawn(async move {
                            let _permit = permit; // drops when task finishes, freeing the slot
                            run_job(store, progress_tx, job).await;
                        });
                    }
                    Ok(None) => break, // no more ready jobs
                    Err(e) => {
                        error!("worker: claim_next error: {e}");
                        break;
                    }
                }
            }
        }
    });
}

async fn run_job(
    store: Arc<JobStore>,
    progress_tx: broadcast::Sender<ProgressEvent>,
    job: slate_core::job::Job,
) {
    let job_id = job.id;
    info!(job_id = %job_id, src = %job.src, dst = %job.dst, attempt = job.attempt, "starting job");

    let (tx, mut rx) = tokio::sync::mpsc::channel::<ProgressEvent>(64);

    // Forward progress events to store + broadcast channel
    let store_clone = store.clone();
    let bcast = progress_tx.clone();
    let progress_handle = tokio::spawn(async move {
        let mut peak_mbps: f64 = 0.0;
        while let Some(ev) = rx.recv().await {
            peak_mbps = peak_mbps.max(ev.throughput_mbps);
            let _ = store_clone
                .update_progress(ev.job_id, ev.bytes_transferred, ev.bytes_total, Some(peak_mbps))
                .await;
            let _ = bcast.send(ev);
        }
        peak_mbps
    });

    let result = TransferEngine::run(job_id, &job.src, &job.dst, tx).await;
    let peak_mbps = progress_handle.await.ok();

    match result {
        Ok(bytes) => {
            info!(job_id = %job_id, bytes, "job completed");
            let _ = store.set_completed(job_id, peak_mbps).await;
            fire_webhook(&job, "completed", None).await;
        }
        Err(e) => {
            let msg = e.to_string();
            error!(job_id = %job_id, error = %msg, "job failed");
            let _ = store.set_failed(job_id, &msg).await;

            // Refetch to check if it was re-queued for retry or marked terminal
            if let Ok(Some(updated)) = store.get(job_id).await {
                if updated.status == slate_core::job::JobStatus::Failed {
                    fire_webhook(&job, "failed", Some(&msg)).await;
                }
            }
        }
    }
}

async fn fire_webhook(job: &slate_core::job::Job, event: &str, error: Option<&str>) {
    let Some(url) = &job.callback_url else { return };

    let payload = serde_json::json!({
        "event": event,
        "job_id": job.id,
        "src": job.src,
        "dst": job.dst,
        "attempt": job.attempt,
        "error": error,
    });

    match reqwest::Client::new().post(url).json(&payload).send().await {
        Ok(r) => info!(job_id = %job.id, status = %r.status(), "webhook fired"),
        Err(e) => error!(job_id = %job.id, error = %e, "webhook failed"),
    }
}
