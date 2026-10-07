//! Topic registry: one broadcast channel per URL path, created when its first
//! client joins and freed when the last one leaves.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::broadcast;
use tokio_tungstenite::tungstenite::Message;

use crate::metrics::Metrics;

/// A broadcast message tagged with the connection that sent it, so clients
/// that opted out of echo can skip their own messages.
pub(crate) struct Envelope {
    pub from: u64,
    pub msg: Message,
}

/// What a topic channel carries. Every ring slot is allocated up front, so an
/// 8-byte `Arc` instead of the 56-byte `Message` shrinks each slot ~2.5x
/// (about 32 vs 80 bytes) for the cost of one allocation per incoming message.
pub(crate) type Payload = Arc<Envelope>;

struct Topic {
    name: Arc<str>,
    tx: broadcast::Sender<Payload>,
    /// Connected clients, publish-only ones included (they hold no receiver, so
    /// the channel's receiver count can't tell when a topic is empty).
    members: usize,
}

pub(crate) struct Topics {
    channels: Mutex<HashMap<Arc<str>, Topic>>,
    capacity: usize,
    max_topics: usize,
    metrics: Arc<Metrics>,
}

/// A client's membership in a topic. Dropping it removes the topic once the
/// last member has left.
pub(crate) struct Subscription {
    topics: Arc<Topics>,
    pub name: Arc<str>,
    pub tx: broadcast::Sender<Payload>,
    /// `None` for publish-only clients, which never receive.
    pub rx: Option<broadcast::Receiver<Payload>>,
}

impl Topics {
    pub fn new(capacity: usize, max_topics: usize, metrics: Arc<Metrics>) -> Self {
        Self {
            channels: Mutex::new(HashMap::new()),
            capacity,
            max_topics,
            metrics,
        }
    }

    /// Join `name`, creating it if needed; `receive: false` joins publish-only.
    /// `None` when the topic limit is reached.
    pub fn join(self: &Arc<Self>, name: &str, receive: bool) -> Option<Subscription> {
        let mut channels = self.channels.lock().unwrap();
        let (name, tx) = match channels.get_mut(name) {
            Some(topic) => {
                topic.members += 1;
                (topic.name.clone(), topic.tx.clone())
            }
            None => {
                if channels.len() >= self.max_topics {
                    return None;
                }
                let name: Arc<str> = name.into();
                let (tx, _) = broadcast::channel(self.capacity);
                let topic = Topic {
                    name: name.clone(),
                    tx: tx.clone(),
                    members: 1,
                };
                channels.insert(name.clone(), topic);
                self.metrics.set_topics(channels.len());
                (name, tx)
            }
        };
        let rx = receive.then(|| tx.subscribe());
        Some(Subscription {
            topics: self.clone(),
            name,
            tx,
            rx,
        })
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        // Under the lock, so a concurrent join can't race the removal.
        let mut channels = self.topics.channels.lock().unwrap();
        if let Some(topic) = channels.get_mut(&self.name) {
            topic.members -= 1;
            if topic.members == 0 {
                channels.remove(&self.name);
                self.topics.metrics.set_topics(channels.len());
            }
        }
    }
}

/// Map a request path to its topic: `path` itself (minus trailing slashes)
/// when it is `prefix` or lies under it, otherwise `None`.
pub(crate) fn topic_for<'a>(prefix: &str, path: &'a str) -> Option<&'a str> {
    let prefix = prefix.trim_end_matches('/');
    let path = path.trim_end_matches('/');
    let rest = path.strip_prefix(prefix)?;
    if !rest.is_empty() && !rest.starts_with('/') {
        return None; // "/rebroadcastX" is not under "/rebroadcast"
    }
    Some(if path.is_empty() { "/" } else { path })
}

#[cfg(test)]
mod tests {
    use super::topic_for;

    #[test]
    fn maps_paths_under_prefix() {
        let p = "/rebroadcast";
        assert_eq!(topic_for(p, "/rebroadcast"), Some("/rebroadcast"));
        assert_eq!(topic_for(p, "/rebroadcast/"), Some("/rebroadcast"));
        assert_eq!(
            topic_for(p, "/rebroadcast/room1"),
            Some("/rebroadcast/room1")
        );
        assert_eq!(
            topic_for(p, "/rebroadcast/room1/"),
            Some("/rebroadcast/room1")
        );
        assert_eq!(
            topic_for(p, "/rebroadcast/app/roomN"),
            Some("/rebroadcast/app/roomN")
        );
        assert_eq!(
            topic_for("/rebroadcast/", "/rebroadcast/a"),
            Some("/rebroadcast/a")
        );
    }

    #[test]
    fn rejects_paths_outside_prefix() {
        let p = "/rebroadcast";
        assert_eq!(topic_for(p, "/"), None);
        assert_eq!(topic_for(p, "/other"), None);
        assert_eq!(topic_for(p, "/rebroadcastX"), None);
        assert_eq!(topic_for(p, "/re"), None);
    }

    #[test]
    fn root_prefix_makes_every_path_a_topic() {
        assert_eq!(topic_for("/", "/"), Some("/"));
        assert_eq!(topic_for("/", "/a/b"), Some("/a/b"));
        assert_eq!(topic_for("", "/a"), Some("/a"));
    }
}
