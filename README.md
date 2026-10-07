# ws-relay

A low-footprint WebSocket fan-out relay written in Rust, organised by topic.
Every URL path under `/rebroadcast` is a topic; a text or binary message a
client sends is broadcast to **all clients on the same topic**, the sender
included, and never to any other topic. There is no HTTP framework: the only
HTTP is the WebSocket handshake and an optional, separate stats endpoint.

## Build

```bash
cargo build --release                          # ./target/release/ws-relay (~1 MB)
cargo build --release --no-default-features    # without the stats endpoint
```

## Run

```bash
./target/release/ws-relay                      # ws://0.0.0.0:8765/rebroadcast/<topic>
./target/release/ws-relay --port 9000 --prefix /pubsub
./target/release/ws-relay --config config.example.toml
./target/release/ws-relay --help
```

Precedence is CLI flags > config file > defaults. See
[`config.example.toml`](config.example.toml) for every option.

## Topics

| Client connects to            | Topic                    |
|-------------------------------|--------------------------|
| `/rebroadcast/room1`          | `/rebroadcast/room1`     |
| `/rebroadcast/room1/`         | `/rebroadcast/room1` (trailing `/` ignored) |
| `/rebroadcast/app/roomN`      | `/rebroadcast/app/roomN` |
| `/rebroadcast/app`            | `/rebroadcast/app` (does **not** see `app/roomN`) |
| `/rebroadcast`                | `/rebroadcast`           |
| `/other`, `/rebroadcastX`     | rejected, 404            |

Matching is exact: there is no hierarchy or wildcard between topics. The query
string is ignored. A topic is created by its first client and freed when its
last client leaves; at most `max_topics` (default 1024) can be live at once,
beyond which new topics get 503.

## Client options

Set per connection with URL query parameters (other parameters are ignored,
invalid values are rejected with 400):

| Parameter                | Effect                                                         |
|--------------------------|----------------------------------------------------------------|
| *(none)*                 | Receive every message on the topic, own messages included      |
| `?echo=false`            | Receive everyone else's messages, but not your own             |
| `?mode=pub`             | Publish-only: publish to the topic, receive nothing at all        |

```
ws://pi:8765/rebroadcast/room1?echo=false
ws://pi:8765/rebroadcast/sensors/temp?mode=pub
```

Both can be combined in one query, in any order (`?mode=pubsub&echo=false`);
if a parameter repeats, the last value wins. `mode=pubsub` is the explicit form
of the default mode.

`echo` defaults to the server's `echo` setting (`--no-echo` turns it off for
everyone; `?echo=true` turns it back on for a client). An explicit `echo=true`
together with `mode=pub` is contradictory and rejected with 400;
`mode=pub&echo=false` and plain `mode=pub` are fine.

## Behaviour

- **Clients must read.** Each topic has a ring of `channel_capacity` (default
  512) messages. A client that falls further behind, or whose socket blocks a
  write for `write_timeout_ms`, is disconnected (close code 1013 "try again
  later") so it can never slow the others down.
- **Fast senders are throttled, not dropped:** the relay delivers pending
  messages to a client before reading its next one, so a client can't publish
  faster than it consumes its own topic. `mode=pub` clients have nothing to
  consume and are not throttled; a publish-only producer that outpaces the
  slowest subscribers will get those subscribers dropped.
- **Zero-copy fan-out:** payloads are reference-counted; one broadcast stores
  the message once regardless of subscriber count.
- `SIGINT`/`SIGTERM` stop accepting and exit.

## Memory

Two allocations dominate, both tunable:

| What                    | Size                                   | Default  |
|-------------------------|----------------------------------------|----------|
| Per connection          | `read_buffer_size` + write buffer + task | ~22 KB |
| Per live topic          | ~32 B × `channel_capacity`             | ~16 KB   |

A larger `channel_capacity` tolerates burstier subscribers at the cost of
memory per topic; on a Pi 5, 128 was too small for 1000 subscribers at ~1M
deliveries/s, 512 had no drops.

## Stats endpoint

Enabled by the default `metrics` feature. It listens on the same host as the
WebSocket relay, port 9100 (`--metrics-port`), so with the default host
`0.0.0.0` it is reachable at `http://<relay-host>:9100/`. Use
`--metrics-addr 127.0.0.1:9100` to keep it local-only.

| Path          | Content                                         |
|---------------|-------------------------------------------------|
| `/`           | Live stats page (rates, clients, topics, drops) |
| `/health`     | `200 ok`                                        |
| `/metrics`    | Prometheus text format                          |
| `/stats.json` | Raw counters                                    |

## Benchmark

```bash
cargo run --release                                        # terminal 1
cargo run --release --example bench -- \
    --topics 1 --subscribers 500 --rate 2000 --seconds 10  # terminal 2
```

One publish-only producer per topic sends at a fixed total rate; subscribers are spread
round-robin across topics and record end-to-end latency. Measured on a
Raspberry Pi 5 (4 GB) with the benchmark on the same board, so latency includes
the load generator competing with the relay for the 4 cores:

| Topics | Subscribers | Rate    | Payload | Deliveries/s | p50     | p99     | Relay peak RSS |
|--------|-------------|---------|---------|--------------|---------|---------|----------------|
| –      | 0 (idle)    | –       | –       | –            | –       | –       | 2.8 MB         |
| 1      | 100         | 1000 /s | 64 B    | ~100k        | 1.6 ms  | 7.4 ms  | 3.9 MB         |
| 1      | 500         | 2000 /s | 64 B    | ~1M          | 11 ms   | 42 ms   | 12 MB          |
| 1      | 1000        | 1000 /s | 512 B   | ~1M          | 38 ms   | 137 ms  | 39 MB          |
| 1000   | 1000        | 1000 /s | 64 B    | ~1k          | 0.16 ms | 0.49 ms | 33 MB          |

## Test

```bash
cargo test
cargo test --no-default-features
```

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
