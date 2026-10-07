//! Relay counters. Without the `metrics` feature every call compiles to nothing.

pub use imp::*;

#[cfg(feature = "metrics")]
mod imp {
    use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
    use std::time::Instant;

    pub struct Metrics {
        started: Instant,
        clients: AtomicU64,
        topics: AtomicU64,
        connections_total: AtomicU64,
        messages_in: AtomicU64,
        bytes_in: AtomicU64,
        messages_out: AtomicU64,
        slow_clients_dropped: AtomicU64,
    }

    /// Point-in-time copy of the counters.
    #[derive(Debug, Clone, Copy)]
    pub struct Snapshot {
        pub uptime_secs: u64,
        pub clients: u64,
        pub topics: u64,
        pub connections_total: u64,
        pub messages_in: u64,
        pub bytes_in: u64,
        pub messages_out: u64,
        pub slow_clients_dropped: u64,
    }

    /// Decrements the connected-client gauge when dropped.
    pub struct ClientGuard<'a>(&'a Metrics);

    impl Drop for ClientGuard<'_> {
        fn drop(&mut self) {
            self.0.clients.fetch_sub(1, Relaxed);
        }
    }

    impl Default for Metrics {
        fn default() -> Self {
            Self {
                started: Instant::now(),
                clients: AtomicU64::new(0),
                topics: AtomicU64::new(0),
                connections_total: AtomicU64::new(0),
                messages_in: AtomicU64::new(0),
                bytes_in: AtomicU64::new(0),
                messages_out: AtomicU64::new(0),
                slow_clients_dropped: AtomicU64::new(0),
            }
        }
    }

    impl Metrics {
        pub fn client_connected(&self) -> ClientGuard<'_> {
            self.clients.fetch_add(1, Relaxed);
            self.connections_total.fetch_add(1, Relaxed);
            ClientGuard(self)
        }

        pub fn set_topics(&self, live: usize) {
            self.topics.store(live as u64, Relaxed);
        }

        pub fn message_in(&self, bytes: usize) {
            self.messages_in.fetch_add(1, Relaxed);
            self.bytes_in.fetch_add(bytes as u64, Relaxed);
        }

        /// Called once per flushed batch, not per message, to limit contention.
        pub fn messages_out(&self, count: u64) {
            self.messages_out.fetch_add(count, Relaxed);
        }

        pub fn slow_client_dropped(&self) {
            self.slow_clients_dropped.fetch_add(1, Relaxed);
        }

        pub fn snapshot(&self) -> Snapshot {
            Snapshot {
                uptime_secs: self.started.elapsed().as_secs(),
                clients: self.clients.load(Relaxed),
                topics: self.topics.load(Relaxed),
                connections_total: self.connections_total.load(Relaxed),
                messages_in: self.messages_in.load(Relaxed),
                bytes_in: self.bytes_in.load(Relaxed),
                messages_out: self.messages_out.load(Relaxed),
                slow_clients_dropped: self.slow_clients_dropped.load(Relaxed),
            }
        }
    }
}

#[cfg(not(feature = "metrics"))]
mod imp {
    #[derive(Default)]
    pub struct Metrics {
        _private: (),
    }

    pub struct ClientGuard;

    impl Metrics {
        #[inline(always)]
        pub fn client_connected(&self) -> ClientGuard {
            ClientGuard
        }
        #[inline(always)]
        pub fn set_topics(&self, _live: usize) {}
        #[inline(always)]
        pub fn message_in(&self, _bytes: usize) {}
        #[inline(always)]
        pub fn messages_out(&self, _count: u64) {}
        #[inline(always)]
        pub fn slow_client_dropped(&self) {}
    }
}
