//! DC speed test bot.
//!
//! Downloads 100 MB test files from DC1 and DC2 via @DC_Speed_Test, then
//! re-uploads the last file and sends a speed report to @ankify.
//!
//! Secrets (env): API_ID, API_HASH, BOT_TOKEN.
//! RUST_LOG controls log verbosity (e.g. `RUST_LOG=debug`).
//!
//! Transfer concurrency (X/Y) defaults live below in TRANSFER_DEFAULTS.
//! Each can be overridden by an env var of the same name in SCREAMING_CASE,
//! which is how the CI matrix sweeps different X/Y combos without editing
//! this file per run:
//!   DOWNLOAD_TCP_CONNECTIONS, UPLOAD_TCP_CONNECTIONS, MAX_TCP_CONNECTIONS,
//!   DOWNLOAD_PIPELINE_DEPTH, UPLOAD_PIPELINE_DEPTH, BYPASS_TCP_ALLOTMENTS

use ferogram::{Client, InputMessage, TransferHandle, TransferLimits, media};
use std::env;
use std::path::PathBuf;
use std::time::{Duration, Instant};

const SOURCE_CHANNEL: &str = "DC_Speed_Test";
const TARGET: &str = "ankify";
const PROGRESS_INTERVAL: Duration = Duration::from_millis(500);

struct DcFile {
    dc: &'static str,
    msg_id: i32,
}

// DC1 & DC2, 100 MB files only, per the mini concurrency test.
// (65,66,67,68 = DC1,2,4,5 @ 100MB · 69-72 @ 2GB · 73-76 @ 4GB)
const FILES: [DcFile; 2] = [
    DcFile { dc: "DC1", msg_id: 65 },
    DcFile { dc: "DC2", msg_id: 66 },
];

/// Defaults for TransferLimits. Y (tcp connections) hard-clamps to
/// media::MAX_WORKERS_PER_FILE (4); X (pipeline depth) hard-clamps to
/// media::MAX_PIPELINE_DEPTH (8) — see ferogram's transfer_limits.rs.
/// Override any field at runtime with an env var of the same name, e.g.
/// `DOWNLOAD_PIPELINE_DEPTH=4 UPLOAD_PIPELINE_DEPTH=4 cargo run --release`.
const TRANSFER_DEFAULTS: TransferLimits = TransferLimits {
    download_tcp_connections: 1,
    upload_tcp_connections: 1,
    max_tcp_connections: 16,
    download_pipeline_depth: 1,
    upload_pipeline_depth: 1,
    bypass_tcp_allotments: true,
};

fn env_usize(key: &str, default: usize) -> usize {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn env_bool(key: &str, default: bool) -> bool {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn transfer_limits() -> TransferLimits {
    TransferLimits {
        download_tcp_connections: env_usize(
            "DOWNLOAD_TCP_CONNECTIONS",
            TRANSFER_DEFAULTS.download_tcp_connections,
        ),
        upload_tcp_connections: env_usize(
            "UPLOAD_TCP_CONNECTIONS",
            TRANSFER_DEFAULTS.upload_tcp_connections,
        ),
        max_tcp_connections: env_usize(
            "MAX_TCP_CONNECTIONS",
            TRANSFER_DEFAULTS.max_tcp_connections,
        ),
        download_pipeline_depth: env_usize(
            "DOWNLOAD_PIPELINE_DEPTH",
            TRANSFER_DEFAULTS.download_pipeline_depth,
        ),
        upload_pipeline_depth: env_usize(
            "UPLOAD_PIPELINE_DEPTH",
            TRANSFER_DEFAULTS.upload_pipeline_depth,
        ),
        bypass_tcp_allotments: env_bool(
            "BYPASS_TCP_ALLOTMENTS",
            TRANSFER_DEFAULTS.bypass_tcp_allotments,
        ),
    }
}

/// Print requested vs. clamped values so a value above the hard ceiling
/// doesn't silently disappear.
fn print_effective(limits: &TransferLimits) {
    let clamp_y = |n: usize| n.clamp(1, media::MAX_WORKERS_PER_FILE);
    let clamp_x = |n: usize| n.clamp(1, media::MAX_PIPELINE_DEPTH);

    println!(
        "config: Y(down)={} Y(up)={} X(down)={} X(up)={} max_tcp={} bypass={}",
        limits.download_tcp_connections,
        limits.upload_tcp_connections,
        limits.download_pipeline_depth,
        limits.upload_pipeline_depth,
        limits.max_tcp_connections,
        limits.bypass_tcp_allotments
    );

    let eff_dy = clamp_y(limits.download_tcp_connections);
    let eff_uy = clamp_y(limits.upload_tcp_connections);
    let eff_dx = clamp_x(limits.download_pipeline_depth);
    let eff_ux = clamp_x(limits.upload_pipeline_depth);

    println!(
        "effective (after normalize): Y(down)={} Y(up)={} X(down)={} X(up)={}",
        eff_dy, eff_uy, eff_dx, eff_ux
    );

    if eff_dy != limits.download_tcp_connections || eff_uy != limits.upload_tcp_connections {
        println!("note: Y clamped to MAX_WORKERS_PER_FILE = {}", media::MAX_WORKERS_PER_FILE);
    }
    if eff_dx != limits.download_pipeline_depth || eff_ux != limits.upload_pipeline_depth {
        println!("note: X clamped to MAX_PIPELINE_DEPTH = {}", media::MAX_PIPELINE_DEPTH);
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(true)
        .init();

    if let Err(e) = run().await {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

/// Print `handle.progress()` every PROGRESS_INTERVAL until aborted.
fn spawn_progress_printer(label: String, handle: TransferHandle) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(PROGRESS_INTERVAL).await;
            let p = handle.progress();
            println!(
                "{label}: {} ({:.1}%) {} ETA {}s",
                p.bytes_human(),
                p.percent(),
                p.speed_human(),
                p.eta_secs()
            );
        }
    })
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let api_id: i32 = env::var("API_ID")?.parse()?;
    let api_hash = env::var("API_HASH")?;
    let bot_token = env::var("BOT_TOKEN")?;

    let limits = transfer_limits();
    print_effective(&limits);

    let (client, _shutdown) = Client::builder()
        .api_id(api_id)
        .api_hash(api_hash)
        .transfer_limits(limits)
        .connect()
        .await?;

    if !client.is_authorized().await? {
        client.bot_sign_in(&bot_token).await?;
    }

    let me = client.get_me().await?;
    println!("Logged in as @{}", me.username.as_deref().unwrap_or("?"));

    tokio::fs::create_dir_all("downloads").await?;

    let mut report = String::from("DC speed test\n\n");
    let mut last_download: Option<PathBuf> = None;

    for f in FILES {
        println!("Fetching msg {} ({}) from {SOURCE_CHANNEL}", f.msg_id, f.dc);

        let msgs = match client.get_messages(SOURCE_CHANNEL, &[f.msg_id]).await {
            Ok(m) => m,
            Err(e) => {
                println!("{}: could not fetch message {}: {e}", f.dc, f.msg_id);
                report.push_str(&format!("{}: fetch failed\n", f.dc));
                continue;
            }
        };

        let Some(msg) = msgs.into_iter().next() else {
            println!("{}: message {} not found", f.dc, f.msg_id);
            report.push_str(&format!("{}: message not found\n", f.dc));
            continue;
        };

        let Some(media) = msg.media() else {
            println!("{}: message {} has no media", f.dc, f.msg_id);
            report.push_str(&format!("{}: no media\n", f.dc));
            continue;
        };

        let path = format!("downloads/{}.bin", f.dc);
        let handle = TransferHandle::new();
        let started = Instant::now();

        let printer = spawn_progress_printer(format!("{} down", f.dc), handle.clone());
        let result = client.download_file(media, &path).handle(&handle).await;
        printer.abort();

        let bytes = match result {
            Ok(b) => b,
            Err(e) => {
                println!("{}: download failed: {e}", f.dc);
                report.push_str(&format!("{}: download failed\n", f.dc));
                continue;
            }
        };

        let elapsed = started.elapsed().as_secs_f64().max(0.001);
        let mb = bytes as f64 / (1024.0 * 1024.0);
        let speed = mb / elapsed;

        println!("{}: {:.2} MB in {:.2}s -> {:.2} MB/s", f.dc, mb, elapsed, speed);
        report.push_str(&format!(
            "{}: {:.2} MB in {:.2}s ({:.2} MB/s)\n",
            f.dc, mb, elapsed, speed
        ));

        last_download = Some(PathBuf::from(path));
    }

    let Some(path) = last_download else {
        println!("Nothing downloaded, sending report only.");
        client
            .send_message(TARGET, InputMessage::text(report))
            .await?;
        return Ok(());
    };

    println!("Uploading {} to test upload speed", path.display());

    let up_handle = TransferHandle::new();
    let started = Instant::now();
    let printer = spawn_progress_printer("upload".to_string(), up_handle.clone());
    let uploaded = client.upload_file(&path).handle(&up_handle).await?;
    printer.abort();
    let elapsed = started.elapsed().as_secs_f64().max(0.001);

    let size = tokio::fs::metadata(&path).await?.len();
    let mb = size as f64 / (1024.0 * 1024.0);
    let speed = mb / elapsed;

    println!("Upload: {:.2} MB in {:.2}s -> {:.2} MB/s", mb, elapsed, speed);
    report.push_str(&format!(
        "\nUpload: {:.2} MB in {:.2}s ({:.2} MB/s)\n",
        mb, elapsed, speed
    ));

    client
        .send_file(TARGET, uploaded, &InputMessage::text(report))
        .await?;

    println!("Done, report sent to @{TARGET}");
    Ok(())
}
