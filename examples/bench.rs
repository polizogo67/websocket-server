//! Load generator: one producer connection per topic, N subscribers spread
//! across the topics, end-to-end latency.
//!
//!     cargo run --release --example bench -- --subscribers 200 --rate 2000 --seconds 10
//!     cargo run --release --example bench -- --topics 50 --subscribers 500
//!
//! Run against a relay started separately (e.g. `cargo run --release`).

use std::time::{Duration, Instant};

use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use tokio::time::MissedTickBehavior;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

#[derive(Parser)]
struct Args {
    /// Relay base URL; topics are opened as <url>/bench-<i>
    #[arg(long, default_value = "ws://127.0.0.1:8765/rebroadcast")]
    url: String,
    /// Number of topics to spread load over
    #[arg(long, default_value_t = 1)]
    topics: u64,
    /// Subscriber connections, assigned round-robin to topics
    #[arg(long, default_value_t = 100)]
    subscribers: u64,
    /// Total messages per second across all topics
    #[arg(long, default_value_t = 1000)]
    rate: u64,
    /// Test duration in seconds
    #[arg(long, default_value_t = 10)]
    seconds: u64,
    /// Payload size in bytes (min 8)
    #[arg(long, default_value_t = 64)]
    size: usize,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let size = args.size.max(8);
    let topics = args.topics.max(1);
    let base = args.url.trim_end_matches('/');
    let topic_url = |t: u64| format!("{base}/bench-{t}");
    let start = Instant::now();
    let total = args.rate * args.seconds;
    // Message n goes to topic n % topics.
    let per_topic = |t: u64| total / topics + u64::from(t < total % topics);

    let mut subs = Vec::new();
    let mut expected_deliveries = 0;
    for i in 0..args.subscribers {
        let topic = i % topics;
        let expected = per_topic(topic);
        expected_deliveries += expected;
        let (ws, _) = connect_async(topic_url(topic))
            .await
            .expect("subscriber connect");
        let (tx, mut rx) = ws.split();
        subs.push(tokio::spawn(async move {
            let _tx = tx; // keep the write half alive so the connection stays open
            let mut latencies = Vec::with_capacity(expected as usize);
            while (latencies.len() as u64) < expected {
                match rx.next().await {
                    Some(Ok(Message::Binary(b))) => {
                        let sent = u64::from_le_bytes(b[..8].try_into().unwrap());
                        let now = start.elapsed().as_nanos() as u64;
                        latencies.push(now.saturating_sub(sent));
                    }
                    Some(Ok(_)) => {}
                    _ => break,
                }
            }
            (latencies, expected)
        }));
    }

    let mut producers = Vec::new();
    for t in 0..topics {
        // Publish-only: the relay delivers nothing back, so no need to drain.
        let url = format!("{}?mode=pub", topic_url(t));
        let (ws, _) = connect_async(url).await.expect("producer connect");
        let (tx, _) = ws.split();
        producers.push(tx);
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    println!(
        "{} topics, {} subscribers, {} msg/s x {}s, {} B payload -> {} deliveries expected",
        topics, args.subscribers, args.rate, args.seconds, size, expected_deliveries
    );

    let mut tick = tokio::time::interval(Duration::from_millis(1));
    tick.set_missed_tick_behavior(MissedTickBehavior::Burst);
    let mut payload = vec![0u8; size];
    let (mut sent, t0) = (0u64, Instant::now());
    while sent < total {
        tick.tick().await;
        let due = ((t0.elapsed().as_secs_f64() * args.rate as f64) as u64).min(total);
        while sent < due {
            payload[..8].copy_from_slice(&(start.elapsed().as_nanos() as u64).to_le_bytes());
            let tx = &mut producers[(sent % topics) as usize];
            tx.feed(Message::binary(payload.clone()))
                .await
                .expect("producer send");
            sent += 1;
        }
        for tx in &mut producers {
            tx.flush().await.expect("producer flush");
        }
    }
    let send_secs = t0.elapsed().as_secs_f64();

    let mut all = Vec::new();
    let mut short = 0;
    for sub in subs {
        match tokio::time::timeout(Duration::from_secs(5), sub).await {
            Ok(Ok((l, expected))) => {
                if (l.len() as u64) < expected {
                    short += 1;
                }
                all.extend(l);
            }
            _ => short += 1,
        }
    }
    let wall = t0.elapsed().as_secs_f64();

    all.sort_unstable();
    let pct = |p: f64| -> f64 {
        if all.is_empty() {
            return 0.0;
        }
        all[((all.len() - 1) as f64 * p) as usize] as f64 / 1e6
    };
    println!(
        "sent {sent} in {send_secs:.2}s, delivered {} ({:.0}/s)",
        all.len(),
        all.len() as f64 / wall
    );
    println!(
        "latency ms  p50 {:.3}  p90 {:.3}  p99 {:.3}  p99.9 {:.3}  max {:.3}",
        pct(0.50),
        pct(0.90),
        pct(0.99),
        pct(0.999),
        pct(1.0)
    );
    if short > 0 {
        println!("WARNING: {short} subscribers missed messages (dropped as slow or timed out)");
    }
}
