//! A caller-owned `cdn/client/v1` QUIC connection kept warm across many hashes.
//!
//! [`WarmConnection`] dials once and stays open, so a caller fetching many hashes
//! from one provider pays the dial + NAT-traversal cost a single time and opens a
//! fresh bi-stream per hash instead. The wire is unchanged — one stream still
//! carries exactly one hash ([`crate::open_progressive_pull_on`] opens the
//! bi-stream); only the client-side teardown differs, because a per-hash pull
//! leaves the connection open for the next one rather than closing it.

use std::time::Duration;

use iroh::endpoint::Connection;
use iroh::{Endpoint, EndpointAddr};

use decdn_protocol::ALPN_CLIENT;

use crate::{PullTimeout, rate_limited};

/// A dialed `cdn/client/v1` connection reused across many hash fetches.
///
/// Each fetch opens a new bi-stream on the SAME connection (one stream = one
/// hash, no multiplexing), amortizing the dial over every hash. The connection is
/// closed exactly once, on this handle's own [`Drop`]; a per-hash pull that
/// borrows the connection ([`crate::open_progressive_pull_on`]) tears down only
/// its own stream and leaves the connection open for the next hash.
#[derive(Debug)]
pub struct WarmConnection {
    /// The live QUIC connection. Cloned into each pull, which leaves it open on
    /// its own teardown; this handle owns the single close.
    conn: Connection,
}

impl WarmConnection {
    /// Dial `target` on the CDN client ALPN and keep the connection warm.
    ///
    /// Bounded as a whole by `open`, the same dial budget
    /// [`crate::open_progressive_pull`] applies to its one-shot dial.
    ///
    /// # Errors
    ///
    /// A connect / transport fault, or `open` elapsing before the connection is
    /// established ([`PullTimeout`]).
    pub async fn connect(
        endpoint: &Endpoint,
        target: EndpointAddr,
        open: Duration,
    ) -> anyhow::Result<Self> {
        let conn = tokio::time::timeout(open, endpoint.connect(target, ALPN_CLIENT))
            .await
            .map_err(|_| anyhow::Error::new(PullTimeout { after: open }))?
            .map_err(|e| rate_limited::transport_error("warm connect failed", e))?;
        Ok(Self { conn })
    }

    /// The live connection, for opening a per-hash bi-stream.
    pub(crate) const fn connection(&self) -> &Connection {
        &self.conn
    }
}

impl Drop for WarmConnection {
    /// Close the warm connection once the caller is done with it. `Connection::close`
    /// is first-wins and idempotent, and every per-hash pull leaves the connection
    /// open, so this is the single teardown for the whole warm connection.
    fn drop(&mut self) {
        self.conn.close(0u32.into(), b"warm-connection-dropped");
    }
}
