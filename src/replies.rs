//! What a Matrix event and a mesh message have to do with each other.
//!
//! A reply names the thing it answers, and the two sides name it differently: on
//! Matrix by event id, on Meshtastic by packet id. Relaying a reply as a reply
//! therefore needs a memory of which was which, and that memory is this.
//!
//! Every relayed message puts an entry in, whichever way it was going, so a reply
//! works for something a mesh said and for something Matrix said alike. The key
//! carries the network as well as the event: a room may bridge two meshes, the
//! same Matrix event then goes out as one packet on each, and a reply has to name
//! the right one.
//!
//! **Entries are not consumed by being read.** A popular message collects several
//! answers, and the second one deserves its thread as much as the first. What
//! removes an entry is age: a reply to something said days ago would point at a
//! packet no node still remembers, and every client that received the original
//! has long since dropped it.
//!
//! Which is also what keeps this bounded, along with a hard ceiling for the case
//! where the window is set generously and the mesh turns out to be busy.
//!
//! Keyed by hash rather than by the ids themselves, the same bargain
//! [`crate::dedupe`] makes and for the same reason: it is eight bytes instead of
//! sixty, and it needs no allocation to look one up. A collision would answer the
//! wrong message, and at these sizes there will not be one.

use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::time::{Duration, Instant};

use crate::mesh::MessageRef;

/// Most entries to hold regardless of the window, so that a long window cannot
/// become unbounded memory. At two dozen bytes an entry this is well under a
/// megabyte, and a LoRa mesh that says this much inside any sane window does not
/// exist.
const MAX_ENTRIES: usize = 10_000;

/// Hash identifying "this Matrix event, as it went out on this network".
fn key(event: &str, network: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    event.hash(&mut hasher);
    network.hash(&mut hasher);
    hasher.finish()
}

pub struct Replies {
    window: Duration,
    known: HashMap<u64, (MessageRef, Instant)>,
}

impl Replies {
    pub fn new(window: Duration) -> Self {
        Self {
            window,
            known: HashMap::new(),
        }
    }

    /// Record that a Matrix event and a mesh message are the same message.
    pub fn remember(&mut self, event: &str, network: &str, message: MessageRef, now: Instant) {
        self.prune(now);
        self.known.insert(key(event, network), (message, now));
    }

    /// What to name on `network` in order to reply to `event`, if we still know.
    ///
    /// Reading does not forget: the same original may be answered many times.
    pub fn message_for(&self, event: &str, network: &str, now: Instant) -> Option<MessageRef> {
        self.known
            .get(&key(event, network))
            .filter(|(_, at)| now.duration_since(*at) < self.window)
            .map(|(message, _)| message.clone())
    }

    /// Drop what has aged out, and then the oldest of whatever is left if there
    /// is still too much of it.
    ///
    /// Called on insertion only. A lookup does not prune, because an expired
    /// entry is already refused on the way out, and sweeping on every read would
    /// charge the common case for the rare one.
    fn prune(&mut self, now: Instant) {
        let window = self.window;
        self.known
            .retain(|_, (_, at)| now.duration_since(*at) < window);

        if self.known.len() < MAX_ENTRIES {
            return;
        }
        // Keep the newest half. Trimming to exactly the ceiling would make the
        // next insertion sort all over again, and the one after that.
        //
        // By key rather than by a cutoff instant, because a burst arriving inside
        // one tick of the clock gives every entry the same age, and a cutoff would
        // then keep all of them or none.
        let mut by_age: Vec<(u64, Instant)> = self
            .known
            .iter()
            .map(|(key, (_, at))| (*key, *at))
            .collect();
        by_age.sort_unstable_by_key(|(_, at)| *at);
        let excess = self.known.len() - MAX_ENTRIES / 2;
        for (key, _) in by_age.into_iter().take(excess) {
            self.known.remove(&key);
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.known.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const WINDOW: Duration = Duration::from_secs(3600);

    #[test]
    fn remembers_which_packet_an_event_is() {
        let mut replies = Replies::new(WINDOW);
        let now = Instant::now();
        replies.remember("$abc", "lora", MessageRef::Packet(42), now);

        assert_eq!(
            replies.message_for("$abc", "lora", now),
            Some(MessageRef::Packet(42))
        );
        assert_eq!(replies.message_for("$other", "lora", now), None);
    }

    /// Why the network is part of the key: one Matrix event bridged to two meshes
    /// is two different packets, and a reply must name the one on its own mesh.
    #[test]
    fn the_same_event_is_a_different_message_on_each_mesh() {
        let mut replies = Replies::new(WINDOW);
        let now = Instant::now();
        replies.remember("$abc", "lora", MessageRef::Packet(1), now);
        replies.remember("$abc", "other", MessageRef::Packet(2), now);

        assert_eq!(
            replies.message_for("$abc", "lora", now),
            Some(MessageRef::Packet(1))
        );
        assert_eq!(
            replies.message_for("$abc", "other", now),
            Some(MessageRef::Packet(2))
        );
        assert_eq!(replies.message_for("$abc", "third", now), None);
    }

    /// A message may be answered more than once, so reading must not forget.
    #[test]
    fn one_message_can_be_replied_to_many_times() {
        let mut replies = Replies::new(WINDOW);
        let now = Instant::now();
        replies.remember("$abc", "lora", MessageRef::Packet(42), now);

        for minute in 1..5 {
            let later = now + Duration::from_secs(60 * minute);
            assert_eq!(
                replies.message_for("$abc", "lora", later),
                Some(MessageRef::Packet(42)),
                "every answer to the same message must find it"
            );
        }
    }

    #[test]
    fn forgets_what_no_node_would_still_remember() {
        let mut replies = Replies::new(WINDOW);
        let now = Instant::now();
        replies.remember("$abc", "lora", MessageRef::Packet(42), now);

        assert_eq!(
            replies.message_for("$abc", "lora", now + Duration::from_secs(3601)),
            None
        );
    }

    #[test]
    fn expired_entries_are_reclaimed_on_insertion() {
        let mut replies = Replies::new(WINDOW);
        let now = Instant::now();
        for i in 0..10 {
            replies.remember(&format!("$e{i}"), "lora", MessageRef::Packet(i), now);
        }
        assert_eq!(replies.len(), 10);

        replies.remember(
            "$later",
            "lora",
            MessageRef::Packet(99),
            now + Duration::from_secs(3601),
        );
        assert_eq!(replies.len(), 1, "the sweep must reclaim stale entries");
    }

    #[test]
    fn a_busy_mesh_cannot_grow_the_table_without_end() {
        let mut replies = Replies::new(WINDOW);
        let now = Instant::now();
        // All inside the window, so only the ceiling can stop this.
        for i in 0..(MAX_ENTRIES as u32 + 100) {
            replies.remember(&format!("$e{i}"), "lora", MessageRef::Packet(i), now);
        }

        assert!(
            replies.len() <= MAX_ENTRIES,
            "the ceiling must hold: {} entries",
            replies.len()
        );
    }

    /// Trimming keeps the newest, because those are the ones still worth a reply.
    #[test]
    fn the_ceiling_drops_the_oldest_first() {
        let mut replies = Replies::new(WINDOW);
        let now = Instant::now();
        for i in 0..MAX_ENTRIES as u32 {
            replies.remember(&format!("$old{i}"), "lora", MessageRef::Packet(i), now);
        }
        let later = now + Duration::from_secs(1);
        replies.remember("$new", "lora", MessageRef::Packet(7), later);

        assert_eq!(
            replies.message_for("$new", "lora", later),
            Some(MessageRef::Packet(7)),
            "what was just said must survive the trim"
        );
    }
}
