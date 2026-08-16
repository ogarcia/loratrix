//! The Matrix-side abstraction.
//!
//! Deliberately narrow — five methods — because a second implementation is
//! planned: the `e2ee` feature will swap the lean HTTP client for a matrix-sdk
//! one that can talk to encrypted rooms. Every method added here has to be
//! written twice, so the trait stays at the smallest surface a relay needs.

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::mpsc;

pub mod http;

/// Matrix room id or alias, as written in `[[rooms]].id`.
pub type RoomId = String;

/// Identifier of a Matrix event, which is what a reply names.
pub type EventId = String;

/// A text message observed in a bridged room.
#[derive(Clone, Debug)]
pub struct MatrixMessage {
    pub room_id: RoomId,
    /// Full user id of the sender, used to drop the relay's own echo.
    pub sender: String,
    pub body: String,
    /// This event's own id, so that a reply to it can be recognized later.
    pub event_id: EventId,
    /// The event this one replies to, where it is a reply.
    pub reply_to: Option<EventId>,
}

#[async_trait]
pub trait MatrixSink: Send + Sync {
    /// The relay's own user id.
    fn user_id(&self) -> &str;

    /// Join the bridged rooms, resolving aliases. Idempotent.
    async fn join_rooms(&self, rooms: &[RoomId]) -> Result<()>;

    /// Stream room messages into `tx` until cancelled, reconnecting as needed.
    async fn run(&self, tx: mpsc::Sender<MatrixMessage>) -> Result<()>;

    /// Post a message and report the id the server gave it, which is what ties
    /// it to whatever came back over the mesh.
    async fn send_text(&self, room_id: &str, body: &str) -> Result<EventId>;

    /// Annotate an event with a short key, which clients show as a reaction.
    ///
    /// The fifth method on a trait that was meant to hold four, and worth it for
    /// one reason: a direct message is the only thing here that can be confirmed
    /// to have arrived, and saying so without adding a line to the room needs a
    /// reaction. It is also what relaying the mesh's own reactions will want.
    async fn react(&self, room_id: &str, event_id: &str, key: &str) -> Result<()>;
}

/// Drop the quoted original from the body of a reply.
///
/// A rich reply repeats what it answers as `> <@user:server> …` lines followed
/// by a blank one, so that a client which understands nothing of threading still
/// shows the context. The spec deprecated the practice and clients keep doing it.
///
/// On the mesh that fallback is worse than useless: the quote is charged against
/// a payload of a couple of hundred bytes, and the truncation that follows can
/// eat the actual answer. The relation travels in the packet instead, where the
/// protocol supports it.
///
/// Only called for events that really are replies, so a message that merely
/// begins with `>` keeps every character of it.
pub fn strip_reply_fallback(body: &str) -> &str {
    let mut rest = body;
    while let Some(line) = rest.strip_prefix('>') {
        // A quote line runs to its newline; one without is the whole body, and
        // then there is no answer left to send.
        match line.split_once('\n') {
            Some((_, after)) => rest = after,
            None => return body,
        }
    }
    if rest.len() == body.len() {
        return body;
    }
    // Exactly one blank line separates the quote from the answer. Anything the
    // sender typed beyond it is theirs.
    let answer = rest.strip_prefix('\n').unwrap_or(rest);
    if answer.is_empty() { body } else { answer }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// What Element actually sends, and what was reaching the mesh whole.
    #[test]
    fn drops_the_quoted_original() {
        assert_eq!(
            strip_reply_fallback("> <@alice:example.org> hola a todos\n\nbuenas Alice"),
            "buenas Alice"
        );
    }

    #[test]
    fn drops_a_quote_of_several_lines() {
        assert_eq!(
            strip_reply_fallback("> <@alice:example.org> one\n> two\n> three\n\nbuenas"),
            "buenas"
        );
    }

    /// A quote is not always followed by a blank line, and the answer is still
    /// everything after it.
    #[test]
    fn copes_without_the_blank_line() {
        assert_eq!(strip_reply_fallback("> <@a:x> hola\nbuenas"), "buenas");
    }

    /// Only what the client prepended goes. Whatever the sender typed, including
    /// a quote of their own, is theirs.
    #[test]
    fn keeps_a_quote_the_sender_wrote() {
        assert_eq!(
            strip_reply_fallback("> <@a:x> hola\n\n> and this one is mine"),
            "> and this one is mine"
        );
    }

    /// Nothing to strip, and nothing stripped. This is also the shape of a reply
    /// from a client that does not send the fallback at all.
    #[test]
    fn leaves_an_ordinary_body_alone() {
        assert_eq!(strip_reply_fallback("buenas"), "buenas");
        assert_eq!(strip_reply_fallback(""), "");
    }

    /// Rather than send nothing at all.
    #[test]
    fn keeps_the_body_when_stripping_would_empty_it() {
        assert_eq!(strip_reply_fallback("> <@a:x> hola\n"), "> <@a:x> hola\n");
        assert_eq!(strip_reply_fallback("> no newline"), "> no newline");
    }
}
