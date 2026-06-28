use anyhow::Result;
use clap::{Parser, Subcommand};
use indicatif::{ProgressBar, ProgressStyle};
use slate_core::{job::JobStatus, progress::ProgressEvent, transfer::TransferEngine};
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

            let job = slate_core::job::Job::new(src.clone(), dst.clone());
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
                    store.set_status(job_id, JobStatus::Completed, None).await?;
                    println!(
                        "\nTransferred {} in {:.1}s  ({:.1} MB/s)",
                        human_bytes(bytes),
                        elapsed,
                        throughput
                    );
                }
                Err(e) => {
                    bar.abandon();
                    store
                        .set_status(job_id, JobStatus::Failed, Some(e.to_string()))
                        .await?;
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
                println!("ID:          {}", job.id);
                println!("Status:      {:?}", job.status);
                println!("Source:      {}", job.src);
                println!("Destination: {}", job.dst);
                println!("Transferred: {}", human_bytes(job.bytes_transferred));
                if let Some(total) = job.bytes_total {
                    println!("Total:       {}", human_bytes(total));
                }
                if let Some(err) = &job.error {
                    println!("Error:       {}", err);
                }
                println!("Created:     {}", job.created_at);
                println!("Updated:     {}", job.updated_at);
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

fn print_egress_estimate(src: &str, dst: &str) {
    let src_provider = detect_provider(src);
    let dst_provider = detect_provider(dst);
    let rate = egress_rate(src_provider);

    println!("Egress cost estimate:");
    println!("  Source:      {src_provider}");
    println!("  Destination: {dst_provider}");
    println!("  Rate:        ${rate:.4}/GB (approximate list price)");
    println!();
    println!("Run without --estimate to execute the transfer.");
}

fn detect_provider(url: &str) -> &'static str {
    if url.starts_with("s3://") {
        "AWS S3"
    } else if url.starts_with("gs://") {
        "Google Cloud Storage"
    } else if url.starts_with("az://") {
        "Azure Blob Storage"
    } else {
        "Local / Unknown"
    }
}

fn egress_rate(src: &str) -> f64 {
    match src {
        "AWS S3" => 0.09,
        "Google Cloud Storage" => 0.12,
        "Azure Blob Storage" => 0.087,
        _ => 0.0,
    }
}
