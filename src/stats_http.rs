//! Minimal read-only HTTP endpoint for health checks and stats.
//!
//! Routes: `/health`, `/metrics` (Prometheus text), `/stats.json`, `/` (live page).
//! One request per connection; no keep-alive, no framework.

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::metrics::{Metrics, Snapshot};

const PAGE: &str = include_str!("stats.html");

pub async fn serve(listener: TcpListener, metrics: Arc<Metrics>) {
    loop {
        let stream = match listener.accept().await {
            Ok((stream, _)) => stream,
            Err(e) => {
                eprintln!("stats accept failed: {e}");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        let metrics = metrics.clone();
        tokio::spawn(async move {
            let _ = tokio::time::timeout(Duration::from_secs(5), respond(stream, &metrics)).await;
        });
    }
}

async fn respond(mut stream: TcpStream, metrics: &Metrics) -> std::io::Result<()> {
    let mut buf = [0u8; 1024];
    let n = stream.read(&mut buf).await?;
    let request_line = buf[..n]
        .split(|&b| b == b'\r' || b == b'\n')
        .next()
        .unwrap_or_default();
    let mut parts = request_line.split(|&b| b == b' ');
    let method = parts.next().unwrap_or_default();
    let target = parts.next().unwrap_or_default();
    let path = target.split(|&b| b == b'?').next().unwrap_or_default();

    let (status, content_type, body) = if method != b"GET" {
        (
            "405 Method Not Allowed",
            "text/plain",
            "method not allowed\n".to_string(),
        )
    } else {
        match path {
            b"/health" => ("200 OK", "text/plain", "ok\n".to_string()),
            b"/metrics" => (
                "200 OK",
                "text/plain; version=0.0.4",
                prometheus(&metrics.snapshot()),
            ),
            b"/stats.json" => ("200 OK", "application/json", json(&metrics.snapshot())),
            b"/" => ("200 OK", "text/html; charset=utf-8", PAGE.to_string()),
            _ => ("404 Not Found", "text/plain", "not found\n".to_string()),
        }
    };

    let head = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n\
         Cache-Control: no-store\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(body.as_bytes()).await?;
    stream.shutdown().await
}

fn prometheus(s: &Snapshot) -> String {
    let rows: [(&str, &str, &str, u64); 8] = [
        (
            "ws_relay_clients",
            "gauge",
            "Currently connected clients.",
            s.clients,
        ),
        (
            "ws_relay_topics",
            "gauge",
            "Topics with at least one client.",
            s.topics,
        ),
        (
            "ws_relay_connections_total",
            "counter",
            "Accepted WebSocket connections.",
            s.connections_total,
        ),
        (
            "ws_relay_messages_in_total",
            "counter",
            "Messages received from clients.",
            s.messages_in,
        ),
        (
            "ws_relay_bytes_in_total",
            "counter",
            "Payload bytes received from clients.",
            s.bytes_in,
        ),
        (
            "ws_relay_messages_out_total",
            "counter",
            "Messages delivered to clients.",
            s.messages_out,
        ),
        (
            "ws_relay_slow_clients_dropped_total",
            "counter",
            "Clients disconnected for falling behind.",
            s.slow_clients_dropped,
        ),
        (
            "ws_relay_uptime_seconds",
            "gauge",
            "Seconds since start.",
            s.uptime_secs,
        ),
    ];
    let mut out = String::with_capacity(1024);
    for (name, kind, help, value) in rows {
        out.push_str(&format!(
            "# HELP {name} {help}\n# TYPE {name} {kind}\n{name} {value}\n"
        ));
    }
    out
}

fn json(s: &Snapshot) -> String {
    format!(
        "{{\"uptime_secs\":{},\"clients\":{},\"topics\":{},\"connections_total\":{},\"messages_in\":{},\
         \"bytes_in\":{},\"messages_out\":{},\"slow_clients_dropped\":{}}}",
        s.uptime_secs,
        s.clients,
        s.topics,
        s.connections_total,
        s.messages_in,
        s.bytes_in,
        s.messages_out,
        s.slow_clients_dropped
    )
}
