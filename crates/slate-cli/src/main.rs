use anyhow::Result;
use clap::{Parser, Subcommand};
use indicatif::{ProgressBar, ProgressStyle};
use slate_core::{cost, job::CreateJobRequest, progress::ProgressEvent, transfer::TransferEngine};
use slate_store::JobStore;
use std::time::Instant;
use tokio::sync::mpsc;
use uuid::Uuid;

#[derive(Parser)]
#[command(
    name = "slate",
    about = "High-throughput data orchestrator for AI workloads",
    version
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    #[arg(long, global = true, default_value = "sqlite:slate.db?mode=rwc")]
    db: String,
}

#[derive(Subcommand)]
enum Commands {
    /// Copy data between object stores
    Copy {
        /// Source URL (s3://, gs://, az://, file://, /path)
        src: String,
        /// Destination URL
        dst: String,
        /// Print egress cost estimate without transferring
        #[arg(long)]
        estimate: bool,
    },
    /// List recent jobs
    Jobs,
    /// Show status of a specific job
    Status { id: Uuid },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(std::env::var("RUST_LOG").unwrap_or_else(|_| "error".into()))
        .init();

    let cli = Cli::parse();
    let store = JobStore::new(&cli.db).await?;

    match cli.command {
        Commands::Copy { src, dst, estimate } => {
            if estimate {
                print_egress_estimate(&src, &dst);
                return Ok(());
            }

            let job = slate_core::job::Job::new(CreateJobRequest {
                src: src.clone(),
                dst: dst.clone(),
                priority: None,
                max_attempts: Some(1), // CLI runs are one-shot; retries are an API-layer concern
                run_after: None,
                depends_on: None,
                callback_url: None,
                cron: None,
            });
            store.create(&job).await?;
            println!("Job {} queued", job.id);

            let bar = ProgressBar::new(0);
            bar.set_style(
                ProgressStyle::with_template(
                    "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, ETA {eta})",
                )?
                .progress_chars("=>-"),
            );

            let job_id = job.id;
            let (tx, mut rx) = mpsc::channel::<ProgressEvent>(128);

            let bar2 = bar.clone();
            tokio::spawn(async move {
                while let Some(ev) = rx.recv().await {
                    if let Some(total) = ev.bytes_total {
                        bar2.set_length(total);
                    }
                    bar2.set_position(ev.bytes_transferred);
                }
            });

            let start = Instant::now();
            let result = TransferEngine::run(job_id, &src, &dst, tx).await;

            match result {
                Ok(bytes) => {
                    bar.finish_with_message("done");
                    let elapsed = start.elapsed().as_secs_f64();
                    let throughput = (bytes as f64 / elapsed) / 1_000_000.0;
                    store.set_completed(job_id, Some(throughput)).await?;
                    let egress = cost::estimate(&src, bytes);
                    println!(
                        "\nTransferred {} in {:.1}s  ({:.1} MB/s)  ~${:.4} egress",
                        human_bytes(bytes),
                        elapsed,
                        throughput,
                        egress.estimated_usd,
                    );
                }
                Err(e) => {
                    bar.abandon();
                    store.set_failed(job_id, &e.to_string()).await?;
                    eprintln!("Transfer failed: {e}");
                    std::process::exit(1);
                }
            }
        }

        Commands::Jobs => {
            let jobs = store.list().await?;
            if jobs.is_empty() {
                println!("No jobs found.");
                return Ok(());
            }
            println!(
                "{:<38} {:<12} {:<10} {}",
                "ID", "STATUS", "PROGRESS", "SRC -> DST"
            );
            println!("{}", "-".repeat(100));
            for job in &jobs {
                let pct = job
                    .progress_pct()
                    .map(|p| format!("{p:.0}%"))
                    .unwrap_or_else(|| "-".into());
                println!(
                    "{:<38} {:<12} {:<10} {} -> {}",
                    job.id,
                    format!("{:?}", job.status).to_lowercase(),
                    pct,
                    job.src,
                    job.dst
                );
            }
        }

        Commands::Status { id } => match store.get(id).await? {
            Some(job) => {
                println!("ID:           {}", job.id);
                println!("Status:       {:?}", job.status);
                println!("Source:       {}", job.src);
                println!("Destination:  {}", job.dst);
                println!("Attempt:      {}/{}", job.attempt, job.max_attempts);
                println!("Transferred:  {}", human_bytes(job.bytes_transferred));
                if let Some(total) = job.bytes_total {
                    println!("Total:        {}", human_bytes(total));
                }
                if let Some(mbps) = job.peak_throughput_mbps {
                    println!("Peak speed:   {:.1} MB/s", mbps);
                }
                if job.bytes_transferred > 0 {
                    let e = cost::estimate(&job.src, job.bytes_transferred);
                    println!("Egress cost:  ~${:.4} ({})", e.estimated_usd, e.provider);
                }
                if let Some(err) = &job.error {
                    println!("Error:        {}", err);
                }
                if let Some(t) = job.started_at   { println!("Started:      {t}"); }
                if let Some(t) = job.completed_at { println!("Completed:    {t}"); }
                println!("Created:      {}", job.created_at);
            }
            None => {
                eprintln!("Job {id} not found");
                std::process::exit(1);
            }
        },
    }

    Ok(())
}

fn human_bytes(b: u64) -> String {
    if b >= 1_000_000_000 {
        format!("{:.2} GB", b as f64 / 1_000_000_000.0)
    } else if b >= 1_000_000 {
        format!("{:.2} MB", b as f64 / 1_000_000.0)
    } else if b >= 1_000 {
        format!("{:.2} KB", b as f64 / 1_000.0)
    } else {
        format!("{} B", b)
    }
}

fn print_egress_estimate(src: &str, _dst: &str) {
    let e = cost::estimate(src, 0);
    println!("Egress cost estimate:");
    println!("  Source:  {}", e.provider);
    println!("  Rate:    ${:.4}/GB (approximate list price)", e.rate_per_gb);
    println!();
    println!("Run without --estimate to execute the transfer and see the actual cost.");
}
