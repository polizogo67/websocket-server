//! Configuration: CLI flags override the TOML config file, which overrides defaults.

use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;
use serde::Deserialize;

use crate::RelaySettings;

#[derive(Parser, Debug)]
#[command(version, about = "Low-footprint WebSocket fan-out relay")]
struct Cli {
    /// TOML config file
    #[arg(short, long)]
    config: Option<PathBuf>,
    /// Address to bind [default: 0.0.0.0]
    #[arg(long)]
    host: Option<String>,
    /// Port to listen on [default: 8765]
    #[arg(short, long)]
    port: Option<u16>,
    /// Paths at or under this prefix are topics; "/" makes every path one [default: /rebroadcast]
    #[arg(long)]
    prefix: Option<String>,
    /// Per-topic buffer in messages; clients further behind are dropped [default: 512]
    #[arg(long)]
    channel_capacity: Option<usize>,
    /// Maximum concurrently live topics [default: 1024]
    #[arg(long)]
    max_topics: Option<usize>,
    /// Maximum incoming message size in bytes [default: 1048576]
    #[arg(long)]
    max_message_size: Option<usize>,
    /// Drop a client whose socket blocks writes this long, in ms [default: 10000]
    #[arg(long)]
    write_timeout_ms: Option<u64>,
    /// Per-connection read buffer in bytes; main driver of memory per client [default: 4096]
    #[arg(long)]
    read_buffer_size: Option<usize>,
    /// Per-connection write buffer in bytes [default: 16384]
    #[arg(long)]
    write_buffer_size: Option<usize>,
    /// Stats/health HTTP port, on the same host as the WebSocket listener [default: 9100]
    #[cfg(feature = "metrics")]
    #[arg(long)]
    metrics_port: Option<u16>,
    /// Stats/health HTTP host:port, overriding --metrics-port and the shared host
    #[cfg(feature = "metrics")]
    #[arg(long)]
    metrics_addr: Option<String>,
    /// Don't send clients their own messages by default (clients can still pass ?echo=true)
    #[arg(long)]
    no_echo: bool,
    /// Log every connect and disconnect
    #[arg(short, long)]
    verbose: bool,
}

#[derive(Deserialize, Default, Debug)]
#[serde(default, deny_unknown_fields)]
struct FileConfig {
    host: Option<String>,
    port: Option<u16>,
    prefix: Option<String>,
    channel_capacity: Option<usize>,
    max_topics: Option<usize>,
    max_message_size: Option<usize>,
    write_timeout_ms: Option<u64>,
    read_buffer_size: Option<usize>,
    write_buffer_size: Option<usize>,
    metrics_port: Option<u16>,
    metrics_addr: Option<String>,
    echo: Option<bool>,
    verbose: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub listen: String,
    #[cfg(feature = "metrics")]
    pub metrics_addr: String,
    pub relay: RelaySettings,
}

/// Parse CLI flags and the optional config file.
pub fn load() -> Result<Config, String> {
    let cli = Cli::parse();
    let file = match &cli.config {
        Some(path) => {
            let text = std::fs::read_to_string(path)
                .map_err(|e| format!("reading {}: {e}", path.display()))?;
            toml::from_str::<FileConfig>(&text)
                .map_err(|e| format!("parsing {}: {e}", path.display()))?
        }
        None => FileConfig::default(),
    };

    let defaults = RelaySettings::default();
    let host = cli.host.or(file.host).unwrap_or_else(|| "0.0.0.0".into());
    let port = cli.port.or(file.port).unwrap_or(8765);
    let relay = RelaySettings {
        channel_capacity: cli
            .channel_capacity
            .or(file.channel_capacity)
            .unwrap_or(defaults.channel_capacity),
        max_topics: cli
            .max_topics
            .or(file.max_topics)
            .unwrap_or(defaults.max_topics),
        max_message_size: cli
            .max_message_size
            .or(file.max_message_size)
            .unwrap_or(defaults.max_message_size),
        write_timeout: cli
            .write_timeout_ms
            .or(file.write_timeout_ms)
            .map(Duration::from_millis)
            .unwrap_or(defaults.write_timeout),
        read_buffer_size: cli
            .read_buffer_size
            .or(file.read_buffer_size)
            .unwrap_or(defaults.read_buffer_size),
        write_buffer_size: cli
            .write_buffer_size
            .or(file.write_buffer_size)
            .unwrap_or(defaults.write_buffer_size),
        prefix: cli.prefix.or(file.prefix).unwrap_or(defaults.prefix),
        echo: !cli.no_echo && file.echo.unwrap_or(defaults.echo),
        verbose: cli.verbose || file.verbose.unwrap_or(false),
    };

    if relay.channel_capacity == 0 {
        return Err("channel_capacity must be greater than 0".into());
    }
    if relay.max_message_size == 0 {
        return Err("max_message_size must be greater than 0".into());
    }
    if relay.read_buffer_size == 0 {
        return Err("read_buffer_size must be greater than 0".into());
    }
    if relay.max_topics == 0 {
        return Err("max_topics must be greater than 0".into());
    }
    if !relay.prefix.starts_with('/') {
        return Err(format!("prefix must start with '/': {}", relay.prefix));
    }

    #[cfg(not(feature = "metrics"))]
    if file.metrics_addr.is_some() || file.metrics_port.is_some() {
        eprintln!("warning: metrics settings ignored, built without the `metrics` feature");
    }

    Ok(Config {
        listen: host_port(&host, port),
        // Stats follow the WebSocket host, so they're reachable wherever the relay is.
        #[cfg(feature = "metrics")]
        metrics_addr: cli.metrics_addr.or(file.metrics_addr).unwrap_or_else(|| {
            host_port(
                &host,
                cli.metrics_port.or(file.metrics_port).unwrap_or(9100),
            )
        }),
        relay,
    })
}

/// Join host and port; brackets keep IPv6 hosts like "::" valid.
fn host_port(host: &str, port: u16) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}
