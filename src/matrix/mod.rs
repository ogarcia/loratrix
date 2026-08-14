//! The Matrix-side abstraction.
//!
//! Deliberately narrow — four methods — because a second implementation is
//! planned: the `e2ee` feature will swap the lean HTTP client for a matrix-sdk
//! one that can talk to encrypted rooms. Every method added here has to be
//! written twice, so the trait stays at the smallest surface a relay needs.

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::mpsc;

pub mod http;

/// Matrix room id or alias, as written in `[[rooms]].id`.
pub type RoomId = String;

/// A text message observed in a bridged room.
#[derive(Clone, Debug)]
pub struct MatrixMessage {
    pub room_id: RoomId,
    /// Full user id of the sender, used to drop the relay's own echo.
    pub sender: String,
    pub body: String,
}

#[async_trait]
pub trait MatrixSink: Send + Sync {
    /// The relay's own user id.
    fn user_id(&self) -> &str;

    /// Join the bridged rooms, resolving aliases. Idempotent.
    async fn join_rooms(&self, rooms: &[RoomId]) -> Result<()>;

    /// Stream room messages into `tx` until cancelled, reconnecting as needed.
    async fn run(&self, tx: mpsc::Sender<MatrixMessage>) -> Result<()>;

    async fn send_text(&self, room_id: &str, body: &str) -> Result<()>;
}
