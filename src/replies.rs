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
//! Direct messages are remembered here too, but by *correspondent* rather than
//! by message: an inbox room mixes everybody's direct messages together, so what
//! a reply there needs to resolve is which person is being answered and on which
//! network. Naming a message would add nothing — a direct message is a
//! conversation of two, and the address already says which.
//!
//! Keyed by hash rather than by the ids themselves, the same bargain
//! [`crate::dedupe`] makes and for the same reason: it is eight bytes instead of
//! sixty, and it needs no allocation to look one up. A collision would answer the
//! wrong message, and at these sizes there will not be one.

use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::time::{Duration, Instant};

use crate::mesh::{MessageRef, NetworkId, PeerId};

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

/// Who wrote the direct message a Matrix event is mirroring.
pub type Correspondent = (NetworkId, PeerId);

pub struct Replies {
    window: Duration,
    known: HashMap<u64, (MessageRef, Instant)>,
    /// Mirrored direct messages, so an answer written in an inbox room can be
    /// sent back to the person who wrote the message it answers.
    ///
    /// Keyed by event alone, unlike `known`: a direct message arrives on one
    /// network, and which one is part of the answer rather than of the question.
    correspondents: HashMap<u64, (Correspondent, Instant)>,
}

impl Replies {
    pub fn new(window: Duration) -> Self {
        Self {
            window,
            known: HashMap::new(),
            correspondents: HashMap::new(),
        }
    }

    /// Record that a Matrix event is the mirror of a direct message from `peer`.
    pub fn remember_correspondent(
        &mut self,
        event: &str,
        network: &str,
        peer: &PeerId,
        now: Instant,
    ) {
        self.prune(now);
        self.correspondents
            .insert(key(event, ""), ((network.to_owned(), peer.clone()), now));
    }

    /// Who to answer, for a reply to `event`. As above, reading does not forget:
    /// one direct message may be answered more than once.
    pub fn correspondent(&self, event: &str, now: Instant) -> Option<Correspondent> {
        self.correspondents
            .get(&key(event, ""))
            .filter(|(_, at)| now.duration_since(*at) < self.window)
            .map(|(who, _)| who.clone())
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
        self.correspondents
            .retain(|_, (_, at)| now.duration_since(*at) < window);
        trim(&mut self.known);
        trim(&mut self.correspondents);
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.known.len()
    }

    #[cfg(test)]
    fn correspondents(&self) -> usize {
        self.correspondents.len()
    }
}

/// Bring a map back under [`MAX_ENTRIES`] by dropping its oldest entries.
///
/// Down to half the ceiling rather than to the ceiling itself, or the insertion
/// after this one would sort all over again, and the one after that.
///
/// By key rather than by a cutoff instant, because a burst arriving inside one
/// tick of the clock gives every entry the same age, and a cutoff would then
/// keep all of them or none.
fn trim<T>(map: &mut HashMap<u64, (T, Instant)>) {
    if map.len() < MAX_ENTRIES {
        return;
    }
    let mut by_age: Vec<(u64, Instant)> = map.iter().map(|(key, (_, at))| (*key, *at)).collect();
    by_age.sort_unstable_by_key(|(_, at)| *at);
    let excess = map.len() - MAX_ENTRIES / 2;
    for (key, _) in by_age.into_iter().take(excess) {
        map.remove(&key);
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

    #[test]
    fn remembers_who_wrote_a_direct_message() {
        let mut replies = Replies::new(WINDOW);
        let now = Instant::now();
        let alice = PeerId::Node(0x1a2b_3c4d);
        replies.remember_correspondent("$abc", "lora", &alice, now);

        assert_eq!(
            replies.correspondent("$abc", now),
            Some(("lora".to_owned(), alice)),
            "the network is part of the answer: it says where to send it"
        );
        assert_eq!(replies.correspondent("$other", now), None);
    }

    /// One direct message may be answered many times, and it is mirrored into
    /// every listening inbox, so each of those events must resolve too.
    #[test]
    fn a_correspondent_survives_being_read() {
        let mut replies = Replies::new(WINDOW);
        let now = Instant::now();
        let alice = PeerId::KeyPrefix([1, 2, 3, 4, 5, 6]);
        replies.remember_correspondent("$abc", "mc", &alice, now);

        for _ in 0..3 {
            assert!(replies.correspondent("$abc", now).is_some());
        }
    }

    #[test]
    fn a_correspondent_is_forgotten_with_everything_else() {
        let mut replies = Replies::new(WINDOW);
        let now = Instant::now();
        replies.remember_correspondent("$abc", "lora", &PeerId::Node(7), now);

        assert_eq!(
            replies.correspondent("$abc", now + Duration::from_secs(3601)),
            None
        );
        // And the sweep reclaims it rather than leaving it to be refused.
        replies.remember(
            "$fresh",
            "lora",
            MessageRef::Packet(1),
            now + Duration::from_secs(3601),
        );
        assert_eq!(replies.correspondents(), 0);
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
