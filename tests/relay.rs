use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use ws_relay::metrics::Metrics;
use ws_relay::{RelaySettings, serve};

type Client = WebSocketStream<MaybeTlsStream<TcpStream>>;

const WAIT: Duration = Duration::from_secs(5);

async fn start(settings: RelaySettings) -> (SocketAddr, Arc<Metrics>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let metrics = Arc::new(Metrics::default());
    tokio::spawn(serve(
        listener,
        settings,
        metrics.clone(),
        std::future::pending(),
    ));
    (addr, metrics)
}

async fn connect(addr: SocketAddr, path: &str) -> Client {
    let (ws, _) = connect_async(format!("ws://{addr}{path}")).await.unwrap();
    ws
}

/// Read until a text message equal to `want` arrives, skipping anything else.
async fn recv_text(ws: &mut Client, want: &str) {
    timeout(WAIT, async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Text(t))) if t.as_str() == want => return,
                Some(Ok(_)) => continue,
                other => panic!("stream ended waiting for {want:?}: {other:?}"),
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {want:?}"));
}

/// The server subscribes a client only after the handshake completes, so a
/// client is guaranteed subscribed once it has received its own echo.
async fn ready(ws: &mut Client, id: usize) {
    let tag = format!("ready-{id}");
    ws.send(Message::text(tag.clone())).await.unwrap();
    recv_text(ws, &tag).await;
}

#[tokio::test]
async fn broadcasts_to_all_clients_including_sender() {
    let (addr, _) = start(RelaySettings::default()).await;
    let mut clients = Vec::new();
    for id in 0..3 {
        let mut ws = connect(addr, "/rebroadcast/room1").await;
        ready(&mut ws, id).await;
        clients.push(ws);
    }

    clients[0].send(Message::text("hello")).await.unwrap();
    for ws in &mut clients {
        recv_text(ws, "hello").await;
    }

    // Binary payloads pass through untouched.
    clients[1]
        .send(Message::binary(vec![0u8, 1, 2, 255]))
        .await
        .unwrap();
    for ws in &mut clients {
        let got = timeout(WAIT, async {
            loop {
                if let Some(Ok(Message::Binary(b))) = ws.next().await {
                    return b;
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(&got[..], &[0u8, 1, 2, 255]);
    }
}

#[tokio::test]
async fn topics_are_isolated() {
    let (addr, _) = start(RelaySettings::default()).await;
    let mut room1_a = connect(addr, "/rebroadcast/room1").await;
    let mut room1_b = connect(addr, "/rebroadcast/room1/").await; // same topic
    let mut nested = connect(addr, "/rebroadcast/app/roomN").await;
    let mut parent = connect(addr, "/rebroadcast/app").await;
    let mut bare = connect(addr, "/rebroadcast").await;
    for (id, ws) in [
        &mut room1_a,
        &mut room1_b,
        &mut nested,
        &mut parent,
        &mut bare,
    ]
    .into_iter()
    .enumerate()
    {
        ready(ws, id).await;
    }

    room1_a.send(Message::text("to-room1")).await.unwrap();
    nested.send(Message::text("to-nested")).await.unwrap();
    recv_text(&mut room1_a, "to-room1").await;
    recv_text(&mut room1_b, "to-room1").await;
    recv_text(&mut nested, "to-nested").await;

    // Everyone else gets a sentinel from their own topic first; anything
    // that leaked from another topic would have arrived before it.
    for (ws, own) in [
        (&mut parent, "p"),
        (&mut bare, "b"),
        (&mut room1_b, "r"),
        (&mut nested, "n"),
    ] {
        ws.send(Message::text(own)).await.unwrap();
        let first = timeout(WAIT, async {
            loop {
                match ws.next().await {
                    Some(Ok(Message::Text(t)))
                        if !t.starts_with("ready-") && t.as_str() != "to-room1" =>
                    {
                        return t.to_string();
                    }
                    Some(Ok(_)) => {}
                    other => panic!("stream ended: {other:?}"),
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(first, own, "message leaked across topics");
    }
}

#[tokio::test]
async fn rejects_paths_outside_prefix() {
    let (addr, _) = start(RelaySettings::default()).await;
    for path in ["/", "/other", "/rebroadcastX", "/re"] {
        assert!(
            connect_async(format!("ws://{addr}{path}")).await.is_err(),
            "{path} accepted"
        );
    }
}

#[tokio::test]
async fn refuses_new_topics_beyond_limit() {
    let settings = RelaySettings {
        max_topics: 2,
        ..Default::default()
    };
    let (addr, _) = start(settings).await;
    let _a = connect(addr, "/rebroadcast/a").await;
    let _b = connect(addr, "/rebroadcast/b").await;
    assert!(
        connect_async(format!("ws://{addr}/rebroadcast/c"))
            .await
            .is_err()
    );
    // Joining an existing topic is still fine.
    let mut a2 = connect(addr, "/rebroadcast/a").await;
    ready(&mut a2, 0).await;
}

#[cfg(feature = "metrics")]
#[tokio::test]
async fn topic_is_freed_when_last_client_leaves() {
    let settings = RelaySettings {
        max_topics: 1,
        ..Default::default()
    };
    let (addr, metrics) = start(settings).await;
    let mut a = connect(addr, "/rebroadcast/a").await;
    ready(&mut a, 0).await;
    assert_eq!(metrics.snapshot().topics, 1);
    a.close(None).await.unwrap();
    drop(a);

    // With the limit at 1, a different topic can only open once "a" is gone.
    timeout(WAIT, async {
        while metrics.snapshot().topics != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("topic never freed");
    let mut b = connect(addr, "/rebroadcast/b").await;
    ready(&mut b, 0).await;
}

#[tokio::test]
async fn slow_client_is_dropped_without_stalling_others() {
    let settings = RelaySettings {
        channel_capacity: 8,
        write_timeout: Duration::from_millis(300),
        ..Default::default()
    };
    let (addr, metrics) = start(settings).await;

    let mut fast = connect(addr, "/rebroadcast/load").await;
    ready(&mut fast, 0).await;
    let mut slow = connect(addr, "/rebroadcast/load").await;
    ready(&mut slow, 1).await;

    // `slow` stops reading. Push far more than socket buffers + ring can hold,
    // while `fast` keeps up.
    let payload = "x".repeat(64 * 1024);
    let total = 400;
    let (mut fast_tx, mut fast_rx) = fast.split();
    let reader = tokio::spawn(async move {
        let mut got = 0;
        while got < total {
            match fast_rx.next().await {
                Some(Ok(Message::Text(t))) if t.len() == 64 * 1024 => got += 1,
                Some(Ok(_)) => {}
                other => panic!("fast client lost connection: {other:?}"),
            }
        }
    });
    for _ in 0..total {
        fast_tx.send(Message::text(payload.clone())).await.unwrap();
    }
    timeout(Duration::from_secs(20), reader)
        .await
        .expect("fast client stalled")
        .unwrap();

    // Draining the slow client now must hit end-of-stream before all messages.
    let drained = timeout(WAIT, async {
        let mut got = 0;
        loop {
            match slow.next().await {
                Some(Ok(Message::Text(t))) if t.len() == 64 * 1024 => got += 1,
                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => return got,
                Some(Ok(_)) => {}
            }
        }
    })
    .await
    .expect("slow client was never disconnected");
    assert!(
        drained < total,
        "slow client received everything ({drained})"
    );

    #[cfg(feature = "metrics")]
    assert_eq!(metrics.snapshot().slow_clients_dropped, 1);
    let _ = metrics;
}

#[cfg(feature = "metrics")]
#[tokio::test]
async fn stats_endpoint_serves_health_and_metrics() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let (addr, metrics) = start(RelaySettings::default()).await;
    let mut ws = connect(addr, "/rebroadcast/load").await;
    ready(&mut ws, 0).await;

    let stats = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let stats_addr = stats.local_addr().unwrap();
    tokio::spawn(ws_relay::stats_http::serve(stats, metrics));

    async fn get(addr: SocketAddr, path: &str) -> String {
        let mut s = TcpStream::connect(addr).await.unwrap();
        s.write_all(format!("GET {path} HTTP/1.1\r\nHost: x\r\n\r\n").as_bytes())
            .await
            .unwrap();
        let mut out = String::new();
        s.read_to_string(&mut out).await.unwrap();
        out
    }

    let health = get(stats_addr, "/health").await;
    assert!(health.starts_with("HTTP/1.1 200 OK"), "{health}");

    let prom = get(stats_addr, "/metrics").await;
    assert!(prom.contains("\nws_relay_clients 1\n"), "{prom}");
    assert!(prom.contains("\nws_relay_topics 1\n"), "{prom}");
    assert!(prom.contains("\nws_relay_messages_in_total 1\n"), "{prom}");

    let json = get(stats_addr, "/stats.json?x=1").await;
    assert!(json.contains("\"clients\":1"), "{json}");

    assert!(get(stats_addr, "/nope").await.starts_with("HTTP/1.1 404"));
}

/// First text message that isn't a `ready-*` handshake marker.
async fn next_text(ws: &mut Client) -> String {
    timeout(WAIT, async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Text(t))) if !t.starts_with("ready-") => return t.to_string(),
                Some(Ok(_)) => {}
                other => panic!("stream ended: {other:?}"),
            }
        }
    })
    .await
    .expect("timed out waiting for a message")
}

#[tokio::test]
async fn echo_false_skips_only_own_messages() {
    let (addr, _) = start(RelaySettings::default()).await;
    let mut quiet = connect(addr, "/rebroadcast/room?echo=false").await;
    let mut other = connect(addr, "/rebroadcast/room").await;
    ready(&mut other, 0).await;

    quiet.send(Message::text("from-quiet")).await.unwrap();
    assert_eq!(next_text(&mut other).await, "from-quiet");
    other.send(Message::text("from-other")).await.unwrap();
    // Had "from-quiet" been echoed, it would arrive before this.
    assert_eq!(next_text(&mut quiet).await, "from-other");
    assert_eq!(next_text(&mut other).await, "from-other"); // default still echoes
}

#[tokio::test]
async fn server_default_echo_can_be_overridden_per_client() {
    let settings = RelaySettings {
        echo: false,
        ..Default::default()
    };
    let (addr, _) = start(settings).await;
    let mut quiet = connect(addr, "/rebroadcast/room").await;
    let mut loud = connect(addr, "/rebroadcast/room?echo=true").await;

    quiet.send(Message::text("q")).await.unwrap();
    assert_eq!(next_text(&mut loud).await, "q");
    loud.send(Message::text("l")).await.unwrap();
    assert_eq!(next_text(&mut loud).await, "l");
    assert_eq!(next_text(&mut quiet).await, "l");
}

#[tokio::test]
async fn rejects_invalid_echo_value() {
    let (addr, _) = start(RelaySettings::default()).await;
    assert!(
        connect_async(format!("ws://{addr}/rebroadcast/room?echo=maybe"))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn publish_only_client_receives_nothing() {
    let (addr, _) = start(RelaySettings::default()).await;
    let mut sender = connect(addr, "/rebroadcast/room?mode=pub").await;
    let mut other = connect(addr, "/rebroadcast/room").await;
    ready(&mut other, 0).await;

    sender.send(Message::text("from-sender")).await.unwrap();
    assert_eq!(next_text(&mut other).await, "from-sender");
    other.send(Message::text("from-other")).await.unwrap();
    assert_eq!(next_text(&mut other).await, "from-other");

    // Nothing at all may arrive at the publish-only client.
    let got = timeout(Duration::from_millis(300), sender.next()).await;
    assert!(got.is_err(), "publish-only client received {got:?}");
}

#[cfg(feature = "metrics")]
#[tokio::test]
async fn publish_only_client_keeps_topic_alive() {
    let (addr, metrics) = start(RelaySettings::default()).await;
    let mut sender = connect(addr, "/rebroadcast/room?mode=pub").await;
    let mut a = connect(addr, "/rebroadcast/room").await;
    ready(&mut a, 0).await;
    a.close(None).await.unwrap();
    drop(a);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        metrics.snapshot().topics,
        1,
        "topic freed while a publish-only client remains"
    );

    // A receiver joining later shares the same topic with the sender.
    let mut b = connect(addr, "/rebroadcast/room").await;
    ready(&mut b, 1).await;
    sender.send(Message::text("still-here")).await.unwrap();
    assert_eq!(next_text(&mut b).await, "still-here");

    sender.close(None).await.unwrap();
    drop(sender);
    b.close(None).await.unwrap();
    drop(b);
    timeout(WAIT, async {
        while metrics.snapshot().topics != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("topic never freed");
}

#[tokio::test]
async fn rejects_invalid_mode() {
    let (addr, _) = start(RelaySettings::default()).await;
    assert!(
        connect_async(format!("ws://{addr}/rebroadcast/room?mode=recv"))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn combined_query_parameters_apply_in_any_order() {
    let (addr, _) = start(RelaySettings::default()).await;
    let mut a = connect(addr, "/rebroadcast/room?echo=false&mode=pubsub").await;
    let mut b = connect(addr, "/rebroadcast/room?mode=pubsub&echo=false").await;
    let mut probe = connect(addr, "/rebroadcast/room").await;
    ready(&mut probe, 0).await;

    a.send(Message::text("from-a")).await.unwrap();
    b.send(Message::text("from-b")).await.unwrap();
    probe.send(Message::text("from-probe")).await.unwrap();
    // Each no-echo client sees the other's message and the probe's, never its own.
    assert_eq!(next_text(&mut a).await, "from-b");
    assert_eq!(next_text(&mut a).await, "from-probe");
    assert_eq!(next_text(&mut b).await, "from-a");
    assert_eq!(next_text(&mut b).await, "from-probe");
}

#[tokio::test]
async fn publish_only_rejects_explicit_echo_true() {
    let (addr, _) = start(RelaySettings::default()).await;
    for query in ["mode=pub&echo=true", "echo=true&mode=pub"] {
        let res = connect_async(format!("ws://{addr}/rebroadcast/room?{query}")).await;
        assert!(res.is_err(), "?{query} accepted");
    }

    // Consistent combinations, and the server default, are still fine.
    let mut p1 = connect(addr, "/rebroadcast/room?mode=pub&echo=false").await;
    let mut p2 = connect(addr, "/rebroadcast/room?mode=pub").await;
    let mut probe = connect(addr, "/rebroadcast/room").await;
    ready(&mut probe, 0).await;
    p1.send(Message::text("from-p1")).await.unwrap();
    p2.send(Message::text("from-p2")).await.unwrap();
    assert_eq!(next_text(&mut probe).await, "from-p1");
    assert_eq!(next_text(&mut probe).await, "from-p2");
    for ws in [&mut p1, &mut p2] {
        let got = timeout(Duration::from_millis(300), ws.next()).await;
        assert!(got.is_err(), "publish-only client received {got:?}");
    }
}
