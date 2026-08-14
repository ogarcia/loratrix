//! Short-lived duplicate suppression.
//!
//! This is what makes fan-out safe. When the relay bridges two meshes through
//! one Matrix room, a message it transmits on the second mesh may come back —
//! via another relay, an MQTT-connected node, or a rebroadcast. MeshCore
//! channel messages carry no sender identity, so we cannot recognize our own
//! echo by looking at who sent it; we recognize it by content instead.
//!
//! The cache is keyed on `(network, channel, text)` and holds entries for a
//! configurable window, so it also absorbs the duplicate packets Meshtastic
//! delivers when a node is reachable over both LoRa and MQTT.

use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::time::{Duration, Instant};

/// Hash identifying "this text, on this channel, on this network".
pub fn message_key(network: &str, channel: Option<u8>, text: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    network.hash(&mut hasher);
    channel.hash(&mut hasher);
    text.hash(&mut hasher);
    hasher.finish()
}

pub struct Dedupe {
    window: Duration,
    seen: HashMap<u64, Instant>,
}

impl Dedupe {
    pub fn new(window: Duration) -> Self {
        Self {
            window,
            seen: HashMap::new(),
        }
    }

    /// Record a key and report whether it is new.
    ///
    /// Returns `false` when the key was already seen inside the window, meaning
    /// the caller should drop the message.
    pub fn admit(&mut self, key: u64, now: Instant) -> bool {
        self.prune(now);
        self.seen.insert(key, now).is_none()
    }

    /// Record a key without asking, for messages the relay itself transmits.
    /// The echo of those must be dropped when it comes back.
    pub fn remember(&mut self, key: u64, now: Instant) {
        self.prune(now);
        self.seen.insert(key, now);
    }

    /// Drop expired entries. Called on every access; the window is short enough
    /// that the map stays small, so a linear sweep is cheaper than bookkeeping.
    fn prune(&mut self, now: Instant) {
        let window = self.window;
        self.seen
            .retain(|_, seen_at| now.duration_since(*seen_at) < window);
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.seen.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const WINDOW: Duration = Duration::from_secs(30);

    #[test]
    fn admits_a_key_once_inside_the_window() {
        let mut dedupe = Dedupe::new(WINDOW);
        let now = Instant::now();
        let key = message_key("lora", Some(0), "hola");

        assert!(dedupe.admit(key, now), "first sighting must pass");
        assert!(!dedupe.admit(key, now), "repeat must be dropped");
        assert!(
            !dedupe.admit(key, now + Duration::from_secs(29)),
            "still inside the window"
        );
    }

    #[test]
    fn admits_again_once_the_window_has_passed() {
        let mut dedupe = Dedupe::new(WINDOW);
        let now = Instant::now();
        let key = message_key("lora", Some(0), "hola");

        assert!(dedupe.admit(key, now));
        assert!(dedupe.admit(key, now + Duration::from_secs(31)));
    }

    #[test]
    fn distinguishes_network_channel_and_text() {
        let base = message_key("lora", Some(0), "hola");
        assert_ne!(base, message_key("mc", Some(0), "hola"));
        assert_ne!(base, message_key("lora", Some(1), "hola"));
        assert_ne!(base, message_key("lora", None, "hola"));
        assert_ne!(base, message_key("lora", Some(0), "adios"));
    }

    #[test]
    fn remembered_keys_suppress_our_own_echo() {
        let mut dedupe = Dedupe::new(WINDOW);
        let now = Instant::now();
        let key = message_key("mc", Some(0), "Alice[lora]: hola");

        dedupe.remember(key, now);
        assert!(
            !dedupe.admit(key, now + Duration::from_secs(1)),
            "the echo of what we sent must be dropped"
        );
    }

    #[test]
    fn expired_entries_are_reclaimed() {
        let mut dedupe = Dedupe::new(WINDOW);
        let now = Instant::now();
        for i in 0..10 {
            dedupe.remember(message_key("lora", Some(0), &i.to_string()), now);
        }
        assert_eq!(dedupe.len(), 10);

        dedupe.remember(
            message_key("lora", Some(0), "later"),
            now + Duration::from_secs(31),
        );
        assert_eq!(dedupe.len(), 1, "the sweep must reclaim stale keys");
    }
}
