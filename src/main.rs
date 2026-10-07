use std::process::ExitCode;
use std::sync::Arc;

use tokio::net::TcpListener;
use ws_relay::config;
use ws_relay::metrics::Metrics;

#[tokio::main]
async fn main() -> ExitCode {
    let cfg = match config::load() {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("error: {e}");
            return ExitCode::FAILURE;
        }
    };

    let listener = match TcpListener::bind(&cfg.listen).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("error: binding {}: {e}", cfg.listen);
            return ExitCode::FAILURE;
        }
    };
    let metrics = Arc::new(Metrics::default());

    #[cfg(feature = "metrics")]
    match TcpListener::bind(&cfg.metrics_addr).await {
        Ok(l) => {
            eprintln!("stats on http://{}", cfg.metrics_addr);
            tokio::spawn(ws_relay::stats_http::serve(l, metrics.clone()));
        }
        Err(e) => {
            eprintln!("error: binding stats {}: {e}", cfg.metrics_addr);
            return ExitCode::FAILURE;
        }
    }

    eprintln!(
        "ws-relay {} listening on ws://{}{}/<topic>",
        env!("CARGO_PKG_VERSION"),
        cfg.listen,
        cfg.relay.prefix.trim_end_matches('/')
    );
    ws_relay::serve(listener, cfg.relay, metrics, shutdown_signal()).await;
    eprintln!("shutting down");
    ExitCode::SUCCESS
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}
