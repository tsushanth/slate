use cron::Schedule;
use slate_core::{
    job::{CreateJobRequest, Job},
    progress::ProgressEvent,
    transfer::TransferEngine,
};
use slate_store::JobStore;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::{broadcast, Semaphore};
use tracing::{error, info};

/// Compute the next UTC datetime after `after` for a cron expression.
pub fn next_cron_run(expr: &str, after: chrono::DateTime<chrono::Utc>) -> anyhow::Result<chrono::DateTime<chrono::Utc>> {
    let schedule = Schedule::from_str(expr)
        .map_err(|e| anyhow::anyhow!("invalid cron expression '{}': {}", expr, e))?;
    schedule
        .after(&after)
        .next()
        .ok_or_else(|| anyhow::anyhow!("cron '{}' has no future occurrences", expr))
}

/// Runs the worker event loop in the background.
/// Polls for ready jobs every second, executes up to `concurrency` transfers in parallel.
pub fn start(
    store: Arc<JobStore>,
    progress_tx: broadcast::Sender<ProgressEvent>,
    concurrency: usize,
) {
    // Job worker loop
    let store_w = store.clone();
    let ptx_w = progress_tx.clone();
    tokio::spawn(async move {
        let sem = Arc::new(Semaphore::new(concurrency));
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval.tick().await;
            loop {
                let Ok(permit) = sem.clone().try_acquire_owned() else { break };
                match store_w.claim_next().await {
                    Ok(Some(job)) => {
                        let store = store_w.clone();
                        let progress_tx = ptx_w.clone();
                        tokio::spawn(async move {
                            let _permit = permit;
                            run_job(store, progress_tx, job).await;
                        });
                    }
                    Ok(None) => break,
                    Err(e) => { error!("worker: claim_next error: {e}"); break; }
                }
            }
        }
    });

    // Cron scheduler loop — checks every 30 seconds for due cron jobs
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval.tick().await;
            match store.due_crons().await {
                Ok(due) => {
                    for cron_entry in due {
                        // Spawn a one-shot job for this cron tick
                        let job = Job::new(CreateJobRequest {
                            src: cron_entry.src.clone(),
                            dst: cron_entry.dst.clone(),
                            priority: Some(cron_entry.priority),
                            max_attempts: Some(cron_entry.max_attempts),
                            run_after: None,
                            depends_on: None,
                            callback_url: cron_entry.callback_url.clone(),
                            cron: None,
                        });

                        if let Err(e) = store.create(&job).await {
                            error!("cron: failed to create job for {}: {e}", cron_entry.id);
                            continue;
                        }

                        // Advance the cron entry to its next tick
                        match next_cron_run(&cron_entry.cron, chrono::Utc::now()) {
                            Ok(next) => {
                                if let Err(e) = store.advance_cron(cron_entry.id, job.id, next).await {
                                    error!("cron: failed to advance {}: {e}", cron_entry.id);
                                }
                                info!(cron_id = %cron_entry.id, job_id = %job.id, next = %next, "cron tick spawned");
                            }
                            Err(e) => error!("cron: {e}"),
                        }
                    }
                }
                Err(e) => error!("cron: due_crons error: {e}"),
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
