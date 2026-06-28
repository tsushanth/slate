use anyhow::Result;
use clap::Parser;
use rand::RngCore;
use slate_core::{progress::ProgressEvent, transfer::TransferEngine};
use slate_store::JobStore;
use std::{
    fs,
    io::Write,
    path::PathBuf,
    process::Command,
    time::Instant,
};
use tokio::sync::mpsc;
use uuid::Uuid;

#[derive(Parser)]
#[command(
    name = "slate-bench",
    about = "Benchmark Slate against rclone and aws s3 cp"
)]
struct Cli {
    /// Directory to use for generated test data
    #[arg(long, default_value = "/tmp/slate-bench")]
    workdir: PathBuf,

    /// File sizes to benchmark (in MiB), comma-separated
    #[arg(long, default_value = "10,100,500,1024")]
    sizes_mb: String,

    /// Number of runs per configuration (results are averaged)
    #[arg(long, default_value = "3")]
    runs: u32,

    /// Also benchmark rclone if available on PATH
    #[arg(long)]
    rclone: bool,

    /// Also benchmark aws s3 cp if available on PATH
    #[arg(long)]
    aws: bool,

    /// Output results as markdown (default: table to stdout)
    #[arg(long)]
    markdown: bool,

    /// S3 source bucket for cloud benchmark (optional)
    #[arg(long)]
    s3_bucket: Option<String>,

    /// S3 destination prefix
    #[arg(long, default_value = "slate-bench")]
    s3_prefix: String,
}

#[derive(Debug, Clone)]
struct BenchResult {
    tool: String,
    size_mb: u64,
    elapsed_secs: f64,
    throughput_mbps: f64,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(std::env::var("RUST_LOG").unwrap_or_else(|_| "error".into()))
        .init();

    let cli = Cli::parse();

    let sizes_mb: Vec<u64> = cli
        .sizes_mb
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();

    let src_dir = cli.workdir.join("src");
    let dst_dir = cli.workdir.join("dst");
    fs::create_dir_all(&src_dir)?;

    println!("Slate Benchmark Harness");
    println!("=======================");
    println!("Workdir:  {}", cli.workdir.display());
    println!("Sizes:    {} MiB", cli.sizes_mb);
    println!("Runs:     {}", cli.runs);
    println!();

    let mut all_results: Vec<BenchResult> = Vec::new();

    for &size_mb in &sizes_mb {
        let file_name = format!("payload_{size_mb}mb.bin");
        let src_file = src_dir.join(&file_name);

        print!("Generating {size_mb} MiB test file... ");
        std::io::stdout().flush()?;
        generate_file(&src_file, size_mb * 1024 * 1024)?;
        println!("done");

        // --- Slate ---
        let slate_times = run_slate_bench(&src_dir, &dst_dir, cli.runs).await?;
        let slate_avg = avg(&slate_times);
        let slate_mbps = (size_mb as f64) / slate_avg;
        all_results.push(BenchResult {
            tool: "slate".into(),
            size_mb,
            elapsed_secs: slate_avg,
            throughput_mbps: slate_mbps,
        });
        println!(
            "  slate:    {size_mb} MiB in {slate_avg:.2}s  ({slate_mbps:.1} MB/s)"
        );

        // --- cp (baseline) ---
        let cp_times = run_cp_bench(&src_dir, &dst_dir, cli.runs)?;
        let cp_avg = avg(&cp_times);
        let cp_mbps = (size_mb as f64) / cp_avg;
        all_results.push(BenchResult {
            tool: "cp".into(),
            size_mb,
            elapsed_secs: cp_avg,
            throughput_mbps: cp_mbps,
        });
        println!(
            "  cp:       {size_mb} MiB in {cp_avg:.2}s  ({cp_mbps:.1} MB/s)"
        );

        // --- rclone (optional) ---
        if cli.rclone && which("rclone") {
            let rclone_times = run_rclone_bench(&src_dir, &dst_dir, cli.runs)?;
            let rclone_avg = avg(&rclone_times);
            let rclone_mbps = (size_mb as f64) / rclone_avg;
            all_results.push(BenchResult {
                tool: "rclone".into(),
                size_mb,
                elapsed_secs: rclone_avg,
                throughput_mbps: rclone_mbps,
            });
            println!(
                "  rclone:   {size_mb} MiB in {rclone_avg:.2}s  ({rclone_mbps:.1} MB/s)"
            );
        }

        // --- aws s3 cp (optional, cloud only) ---
        if cli.aws {
            if let Some(bucket) = &cli.s3_bucket {
                if which("aws") {
                    let aws_times = run_aws_bench(&src_file, bucket, &cli.s3_prefix, cli.runs)?;
                    let aws_avg = avg(&aws_times);
                    let aws_mbps = (size_mb as f64) / aws_avg;
                    all_results.push(BenchResult {
                        tool: "aws s3 cp".into(),
                        size_mb,
                        elapsed_secs: aws_avg,
                        throughput_mbps: aws_mbps,
                    });
                    println!(
                        "  aws s3 cp:{size_mb} MiB in {aws_avg:.2}s  ({aws_mbps:.1} MB/s)"
                    );
                } else {
                    println!("  aws s3 cp: not found on PATH, skipping");
                }
            } else {
                println!("  aws s3 cp: --s3-bucket required for cloud benchmark");
            }
        }

        println!();
    }

    println!();
    if cli.markdown {
        print_markdown(&all_results);
    } else {
        print_table(&all_results);
    }

    Ok(())
}

async fn run_slate_bench(src: &PathBuf, dst: &PathBuf, runs: u32) -> Result<Vec<f64>> {
    let mut times = Vec::new();
    let _store = JobStore::new("sqlite::memory:").await?;

    for _ in 0..runs {
        if dst.exists() {
            fs::remove_dir_all(dst)?;
        }
        fs::create_dir_all(dst)?;

        let src_url = format!("file://{}", src.display());
        let dst_url = format!("file://{}", dst.display());
        let job_id = Uuid::new_v4();

        let (tx, mut rx) = mpsc::channel::<ProgressEvent>(256);
        tokio::spawn(async move {
            while rx.recv().await.is_some() {}
        });

        let start = Instant::now();
        TransferEngine::run(job_id, &src_url, &dst_url, tx).await?;
        times.push(start.elapsed().as_secs_f64());
    }

    Ok(times)
}

fn run_cp_bench(src: &PathBuf, dst: &PathBuf, runs: u32) -> Result<Vec<f64>> {
    let mut times = Vec::new();
    for _ in 0..runs {
        if dst.exists() {
            fs::remove_dir_all(dst)?;
        }
        fs::create_dir_all(dst)?;

        let start = Instant::now();
        Command::new("cp")
            .arg("-r")
            .arg(src)
            .arg(dst)
            .status()?;
        times.push(start.elapsed().as_secs_f64());
    }
    Ok(times)
}

fn run_rclone_bench(src: &PathBuf, dst: &PathBuf, runs: u32) -> Result<Vec<f64>> {
    let mut times = Vec::new();
    for _ in 0..runs {
        if dst.exists() {
            fs::remove_dir_all(dst)?;
        }
        fs::create_dir_all(dst)?;

        let start = Instant::now();
        Command::new("rclone")
            .args(["copy", "--transfers", "8", "--checkers", "16"])
            .arg(src)
            .arg(dst)
            .status()?;
        times.push(start.elapsed().as_secs_f64());
    }
    Ok(times)
}

fn run_aws_bench(src_file: &PathBuf, bucket: &str, prefix: &str, runs: u32) -> Result<Vec<f64>> {
    let mut times = Vec::new();
    let file_name = src_file.file_name().unwrap().to_string_lossy();
    let s3_dst = format!("s3://{bucket}/{prefix}/{file_name}");

    for _ in 0..runs {
        let start = Instant::now();
        Command::new("aws")
            .args(["s3", "cp", "--no-progress"])
            .arg(src_file)
            .arg(&s3_dst)
            .status()?;
        times.push(start.elapsed().as_secs_f64());
    }
    Ok(times)
}

fn generate_file(path: &PathBuf, size_bytes: u64) -> Result<()> {
    if path.exists() {
        let meta = fs::metadata(path)?;
        if meta.len() == size_bytes {
            return Ok(());
        }
    }

    let mut file = fs::File::create(path)?;
    let mut rng = rand::thread_rng();
    let chunk = 1024 * 1024; // 1 MiB at a time
    let mut remaining = size_bytes as usize;
    let mut buf = vec![0u8; chunk];

    while remaining > 0 {
        let n = remaining.min(chunk);
        rng.fill_bytes(&mut buf[..n]);
        file.write_all(&buf[..n])?;
        remaining -= n;
    }

    Ok(())
}

fn avg(times: &[f64]) -> f64 {
    if times.is_empty() {
        return 0.0;
    }
    // Drop the slowest run (warm-up effect) if we have 3+
    let mut sorted = times.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    if sorted.len() >= 3 {
        sorted.pop();
    }
    sorted.iter().sum::<f64>() / sorted.len() as f64
}

fn which(cmd: &str) -> bool {
    Command::new("which").arg(cmd).output().map(|o| o.status.success()).unwrap_or(false)
}

fn print_table(results: &[BenchResult]) {
    println!("Results");
    println!("-------");
    println!("{:<12} {:>10} {:>12} {:>14}", "Tool", "Size (MiB)", "Time (s)", "Throughput");
    println!("{}", "-".repeat(52));
    for r in results {
        println!(
            "{:<12} {:>10} {:>12.2} {:>13.1} MB/s",
            r.tool, r.size_mb, r.elapsed_secs, r.throughput_mbps
        );
    }
}

fn print_markdown(results: &[BenchResult]) {
    println!("## Slate Benchmark Results\n");
    println!("| Tool | Size | Time | Throughput |");
    println!("|------|------|------|------------|");
    for r in results {
        println!(
            "| {} | {} MiB | {:.2}s | **{:.1} MB/s** |",
            r.tool, r.size_mb, r.elapsed_secs, r.throughput_mbps
        );
    }
    println!();
    // Compute speedup vs cp
    println!("### Speedup vs `cp`\n");
    let cp_results: std::collections::HashMap<u64, f64> = results
        .iter()
        .filter(|r| r.tool == "cp")
        .map(|r| (r.size_mb, r.throughput_mbps))
        .collect();

    println!("| Tool | Size | Speedup |");
    println!("|------|------|---------|");
    for r in results.iter().filter(|r| r.tool != "cp") {
        if let Some(&cp_mbps) = cp_results.get(&r.size_mb) {
            let speedup = r.throughput_mbps / cp_mbps;
            println!("| {} | {} MiB | {:.2}x |", r.tool, r.size_mb, speedup);
        }
    }
}
