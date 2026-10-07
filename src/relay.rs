use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast::{self, error::RecvError, error::TryRecvError};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::http::StatusCode;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, WebSocketConfig};

use crate::metrics::Metrics;
use crate::topics::{Envelope, Payload, Subscription, Topics, topic_for};

/// Runtime behaviour of the relay.
#[derive(Debug, Clone)]
pub struct RelaySettings {
    /// Messages buffered per topic. A client that falls further behind than
    /// this is disconnected. Allocated up front for every live topic.
    pub channel_capacity: usize,
    /// Upper bound on concurrently live topics; new topics are refused beyond it.
    pub max_topics: usize,
    /// Largest message (and frame) a client may send, in bytes.
    pub max_message_size: usize,
    /// How long a write to a single client may block before it is dropped.
    pub write_timeout: Duration,
    /// Per-connection read buffer, allocated up front. Dominates memory per client.
    pub read_buffer_size: usize,
    /// Per-connection bytes buffered before writing to the socket (flush always writes).
    pub write_buffer_size: usize,
    /// Paths at or under this prefix are topics; anything else is rejected.
    /// `/` makes every path a topic.
    pub prefix: String,
    /// Whether senders receive their own messages, unless the client overrides
    /// it with `?echo=true|false` in the URL.
    pub echo: bool,
    /// Log every connect/disconnect.
    pub verbose: bool,
}

impl Default for RelaySettings {
    fn default() -> Self {
        Self {
            channel_capacity: 512,
            max_topics: 1024,
            max_message_size: 1 << 20,
            write_timeout: Duration::from_secs(10),
            read_buffer_size: 4 * 1024,
            write_buffer_size: 16 * 1024,
            prefix: "/rebroadcast".into(),
            echo: true,
            verbose: false,
        }
    }
}

struct Shared {
    topics: Arc<Topics>,
    settings: RelaySettings,
    ws_config: WebSocketConfig,
    metrics: Arc<Metrics>,
    next_client_id: AtomicU64,
}

/// Accept WebSocket clients on `listener` until `shutdown` resolves.
pub async fn serve(
    listener: TcpListener,
    settings: RelaySettings,
    metrics: Arc<Metrics>,
    shutdown: impl Future<Output = ()>,
) {
    assert!(
        settings.channel_capacity > 0,
        "channel_capacity must be > 0"
    );

    let topics = Arc::new(Topics::new(
        settings.channel_capacity,
        settings.max_topics,
        metrics.clone(),
    ));
    let ws_config = WebSocketConfig::default()
        .max_message_size(Some(settings.max_message_size))
        .max_frame_size(Some(settings.max_message_size))
        .read_buffer_size(settings.read_buffer_size)
        .write_buffer_size(settings.write_buffer_size);
    let shared = Arc::new(Shared {
        topics,
        settings,
        ws_config,
        metrics,
        next_client_id: AtomicU64::new(0),
    });

    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            accepted = listener.accept() => match accepted {
                Ok((stream, peer)) => {
                    tokio::spawn(handle_client(stream, peer, shared.clone()));
                }
                Err(e) => {
                    // Usually EMFILE/ENFILE: back off instead of spinning.
                    eprintln!("accept failed: {e}");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            },
        }
    }
}

/// Why a client's session ended.
enum Exit {
    Closed,
    Lagged(u64),
    WriteTimeout,
    Error(String),
}

// The handshake callback's error type is fixed by tungstenite.
#[allow(clippy::result_large_err)]
async fn handle_client(stream: TcpStream, peer: SocketAddr, shared: Arc<Shared>) {
    let _ = stream.set_nodelay(true);

    // The topic is decided (and joined) while the upgrade request is in hand,
    // so a bad path or a full topic table is refused with a proper HTTP status.
    let mut joined: Option<Subscription> = None;
    let mut opts = ClientOptions {
        echo: shared.settings.echo,
        publish_only: false,
    };
    let join_topic = |req: &Request, resp: Response| -> Result<Response, ErrorResponse> {
        let reject = |status: StatusCode, why: &str| {
            let mut err = ErrorResponse::new(Some(why.into()));
            *err.status_mut() = status;
            err
        };
        let topic = topic_for(&shared.settings.prefix, req.uri().path())
            .ok_or_else(|| reject(StatusCode::NOT_FOUND, "not found"))?;
        opts = client_options(req.uri().query(), shared.settings.echo)
            .map_err(|why| reject(StatusCode::BAD_REQUEST, why))?;
        joined = Some(
            shared
                .topics
                .join(topic, !opts.publish_only)
                .ok_or_else(|| reject(StatusCode::SERVICE_UNAVAILABLE, "too many topics"))?,
        );
        Ok(resp)
    };

    let ws = match tokio_tungstenite::accept_hdr_async_with_config(
        stream,
        join_topic,
        Some(shared.ws_config),
    )
    .await
    {
        Ok(ws) => ws,
        Err(e) => {
            if shared.settings.verbose {
                eprintln!("{peer}: handshake failed: {e}");
            }
            return;
        }
    };
    let Some(mut sub) = joined else { return };
    let topic = sub.name.clone();
    let id = shared.next_client_id.fetch_add(1, Ordering::Relaxed);
    let skip_from = (!opts.echo).then_some(id);

    let _client = shared.metrics.client_connected();
    if shared.settings.verbose {
        let mode = match opts {
            ClientOptions {
                publish_only: true, ..
            } => " (publish only)",
            ClientOptions { echo: false, .. } => " (no echo)",
            _ => "",
        };
        eprintln!("{peer}: joined {topic}{mode}");
    }

    let (mut sink, mut source) = ws.split();
    let exit = loop {
        tokio::select! {
            // Deliver before reading more: a client can then never publish
            // faster than it consumes its own topic, so a flooding sender
            // can't lag on its own messages (even when it skips them).
            // Publish-only clients have nothing to deliver and are not throttled.
            biased;
            outgoing = recv(&mut sub.rx) => match outgoing {
                Ok(msg) => {
                    if let Err(exit) = forward(&mut sink, &mut sub.rx, msg, skip_from, &shared).await {
                        break exit;
                    }
                }
                Err(RecvError::Lagged(n)) => break Exit::Lagged(n),
                Err(RecvError::Closed) => break Exit::Closed,
            },
            incoming = source.next() => match incoming {
                Some(Ok(msg @ (Message::Text(_) | Message::Binary(_)))) => {
                    shared.metrics.message_in(msg.len());
                    // Fails only when nobody is receiving (e.g. all publish-only): drop it.
                    let _ = sub.tx.send(Arc::new(Envelope { from: id, msg }));
                }
                // Ping/pong replies are handled inside tungstenite.
                Some(Ok(Message::Close(_))) | None => break Exit::Closed,
                Some(Ok(_)) => {}
                Some(Err(e)) => break Exit::Error(e.to_string()),
            },
        }
    };

    match exit {
        Exit::Closed => {
            if shared.settings.verbose {
                eprintln!("{peer}: left {topic}");
            }
        }
        Exit::Lagged(n) => {
            shared.metrics.slow_client_dropped();
            eprintln!("{peer}: dropped from {topic}, {n} messages behind");
            let frame = CloseFrame {
                code: CloseCode::Again,
                reason: "client too slow".into(),
            };
            let _ = tokio::time::timeout(
                Duration::from_secs(1),
                sink.send(Message::Close(Some(frame))),
            )
            .await;
        }
        Exit::WriteTimeout => {
            shared.metrics.slow_client_dropped();
            eprintln!("{peer}: dropped from {topic}, write timed out");
        }
        Exit::Error(e) => {
            if shared.settings.verbose {
                eprintln!("{peer}: error on {topic}: {e}");
            }
        }
    }
}

/// Write `first` plus everything else already queued for this client, then
/// flush once. Batching keeps syscalls down when traffic is bursty. Messages
/// sent by `skip_from` (this client, when echo is off) are consumed unsent.
async fn forward(
    sink: &mut SplitSink<WebSocketStream<TcpStream>, Message>,
    rx: &mut Option<broadcast::Receiver<Payload>>,
    first: Payload,
    skip_from: Option<u64>,
    shared: &Shared,
) -> Result<(), Exit> {
    let Some(rx) = rx else { return Ok(()) };
    let write = async {
        let mut next = Some(first);
        let mut sent = 0;
        while let Some(payload) = next.take() {
            if skip_from != Some(payload.from) {
                sink.feed(payload.msg.clone()).await?;
                sent += 1;
            }
            match rx.try_recv() {
                Ok(payload) => next = Some(payload),
                Err(TryRecvError::Lagged(n)) => return Ok(Err(Exit::Lagged(n))),
                Err(TryRecvError::Empty | TryRecvError::Closed) => {}
            }
        }
        if sent > 0 {
            sink.flush().await?;
            shared.metrics.messages_out(sent);
        }
        Ok::<_, tokio_tungstenite::tungstenite::Error>(Ok(()))
    };

    match tokio::time::timeout(shared.settings.write_timeout, write).await {
        Ok(Ok(result)) => result,
        Ok(Err(e)) => Err(Exit::Error(e.to_string())),
        Err(_) => Err(Exit::WriteTimeout),
    }
}

/// Next message for a receiving client; never resolves for a publish-only one.
async fn recv(rx: &mut Option<broadcast::Receiver<Payload>>) -> Result<Payload, RecvError> {
    match rx {
        Some(rx) => rx.recv().await,
        None => std::future::pending().await,
    }
}

/// Per-client behaviour chosen with URL query parameters.
#[derive(Debug, Clone, Copy, PartialEq)]
struct ClientOptions {
    /// `echo=true|false`: receive one's own messages.
    echo: bool,
    /// `mode=pub`: publish only, receive nothing. (`mode=pubsub` is the default.)
    publish_only: bool,
}

/// Parse `echo` and `mode` from a query string, ignoring other parameters.
/// A bare `echo` means true; the last occurrence of a key wins. An explicit
/// `echo=true` with `mode=pub` is contradictory and rejected; the server-wide
/// echo default never conflicts.
fn client_options(query: Option<&str>, default_echo: bool) -> Result<ClientOptions, &'static str> {
    let mut echo = None;
    let mut publish_only = false;
    for pair in query.unwrap_or_default().split('&') {
        match pair.split_once('=').unwrap_or((pair, "true")) {
            ("echo", "true" | "1" | "yes") => echo = Some(true),
            ("echo", "false" | "0" | "no") => echo = Some(false),
            ("echo", _) => return Err("echo must be true or false"),
            ("mode", "pubsub") => publish_only = false,
            ("mode", "pub") => publish_only = true,
            ("mode", _) => return Err("mode must be pubsub or pub"),
            _ => {}
        }
    }
    if publish_only && echo == Some(true) {
        return Err("echo=true has no effect with mode=pub");
    }
    Ok(ClientOptions {
        echo: echo.unwrap_or(default_echo),
        publish_only,
    })
}

#[cfg(test)]
mod tests {
    use super::{ClientOptions, client_options};

    fn opts(echo: bool, publish_only: bool) -> Result<ClientOptions, &'static str> {
        Ok(ClientOptions { echo, publish_only })
    }

    #[test]
    fn parses_echo() {
        assert_eq!(client_options(None, true), opts(true, false));
        assert_eq!(client_options(None, false), opts(false, false));
        assert_eq!(client_options(Some("echo=false"), true), opts(false, false));
        assert_eq!(client_options(Some("echo=0"), true), opts(false, false));
        assert_eq!(
            client_options(Some("a=1&echo=no&b"), true),
            opts(false, false)
        );
        assert_eq!(client_options(Some("echo=true"), false), opts(true, false));
        assert_eq!(client_options(Some("echo"), false), opts(true, false));
        assert_eq!(
            client_options(Some("echo=false&echo=true"), true),
            opts(true, false)
        );
        assert_eq!(client_options(Some("other=false"), true), opts(true, false));
        assert!(client_options(Some("echo=maybe"), true).is_err());
    }

    #[test]
    fn parses_mode() {
        assert_eq!(client_options(Some("mode=pub"), true), opts(true, true));
        assert_eq!(client_options(Some("mode=pubsub"), true), opts(true, false));
        assert_eq!(
            client_options(Some("mode=pub&echo=false"), true),
            opts(false, true)
        );
        assert!(client_options(Some("mode=recv"), true).is_err());
        assert!(client_options(Some("mode"), true).is_err());
    }

    #[test]
    fn rejects_explicit_echo_with_publish_only() {
        assert!(client_options(Some("mode=pub&echo=true"), true).is_err());
        assert!(client_options(Some("echo=true&mode=pub"), false).is_err());
        assert!(client_options(Some("echo&mode=pub"), true).is_err());
        // Last occurrence wins, so these end up consistent.
        assert_eq!(
            client_options(Some("echo=true&mode=pub&echo=false"), true),
            opts(false, true)
        );
        assert_eq!(
            client_options(Some("mode=pub&echo=true&mode=pubsub"), true),
            opts(true, false)
        );
        // The server default is not an explicit request.
        assert_eq!(client_options(Some("mode=pub"), true), opts(true, true));
    }
}
