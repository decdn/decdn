//! ALPN handler trait and registry.

pub mod probe;

use std::sync::Arc;

use iroh::endpoint::Connection;

/// An ALPN handler processes accepted connections whose negotiated ALPN matches
/// [`Handler::alpn`].
pub trait Handler: Send + Sync + 'static {
    /// The ALPN identifier this handler serves.
    fn alpn(&self) -> &'static [u8];

    /// Handle a single accepted connection. The handler owns the connection until
    /// completion; `dispatch` spawns each invocation on its own task.
    fn handle(
        self: Arc<Self>,
        conn: Connection,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send>>;
}
