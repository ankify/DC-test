//! DC speed test bot.
//!
//! Downloads one file per DC from @DC_Speed_Test, then re-uploads the last
//! downloaded file and sends it to @ankify along with a speed report.
//!
//! Needs API_ID, API_HASH and BOT_TOKEN as environment variables.
//! Meant to run in GitHub Actions with those as secrets.

use ferogram::{Client, InputMessage, TransferHandle};
use std::env;
use std::path::PathBuf;
use std::time::Instant;

const SOURCE_CHANNEL: &str = "DC_Speed_Test";
const TARGET: &str = "ankify";

struct DcFile {
    dc: &'static str,
    msg_id: i32,
}

const FILES: [DcFile; 4] = [
    DcFile { dc: "DC1", msg_id: 69 },
    DcFile { dc: "DC2", msg_id: 70 },
    DcFile { dc: "DC4", msg_id: 71 },
    DcFile { dc: "DC5", msg_id: 72 },
];

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let api_id: i32 = env::var("API_ID")?.parse()?;
    let api_hash = env::var("API_HASH")?;
    let bot_token = env::var("BOT_TOKEN")?;

    let (client, _shutdown) = Client::builder()
        .api_id(api_id)
        .api_hash(api_hash)
        .connect()
        .await?;

    if !client.is_authorized().await? {
        client.bot_sign_in(&bot_token).await?;
    }

    let me = client.get_me().await?;
    println!(
        "Logged in as @{}",
        me.username.as_deref().unwrap_or("?")
    );

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

        let result = client.download_file(media, &path).handle(&handle).await;

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
    let uploaded = client.upload_file(&path).handle(&up_handle).await?;
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
