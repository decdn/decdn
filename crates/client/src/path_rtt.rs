//! The QUIC path RTT of a connection: what discovery ranks a node on.
//!
//! A first exchange on a new connection rides the relay until hole punching
//! selects a direct path, and the relay path says nothing about the direct
//! path the paid stream then uses. So the RTT waits a short grace for a
//! direct path, and keeps the selected path's estimate when none comes.
//!
//! The RTT is the transport's own estimate, never the time from a request to
//! its answer: a node can do work before it answers (an origin read on a
//! miss), and that work says nothing about how far away the node is.

use std::future::Future;
use std::time::Duration;

use iroh::endpoint::Connection;

/// The longest a discovery open waits after its answer for hole punching to
/// select a direct path.
pub(crate) const DIRECT_PATH_GRACE: Duration = Duration::from_millis(500);

/// A path RTT and whether it is the direct path's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PathRtt {
    /// The RTT estimate.
    pub(crate) rtt: Duration,
    /// Whether a direct path reported the estimate. When `false`, the
    /// estimate is the relay path's, and says little about how far away the
    /// peer is.
    pub(crate) direct: bool,
}

impl PathRtt {
    /// The RTT in milliseconds.
    pub(crate) fn ms(self) -> f64 {
        self.rtt.as_secs_f64() * 1000.0
    }
}

/// The RTT estimate of the direct path `conn` selects, or `None` when the
/// paths stream ends without one. A path is selected only once validated, so
/// its estimate comes from real samples. The caller bounds the wait.
pub(crate) async fn direct_path_rtt(conn: &Connection) -> Option<Duration> {
    use futures_util::StreamExt as _;
    let mut snapshots = conn.paths_stream();
    while let Some(paths) = snapshots.next().await {
        if let Some(path) = paths.iter().find(|path| path.is_ip() && path.is_selected()) {
            return Some(path.rtt());
        }
    }
    None
}

/// The RTT estimate of the path `conn` sends on now, direct or relayed.
fn selected_path_rtt(conn: &Connection) -> Option<Duration> {
    conn.paths()
        .iter()
        .find(iroh::endpoint::Path::is_selected)
        .map(|path| path.rtt())
}

/// `conn`'s path RTT: the direct path's when hole punching selects one within
/// `grace`, else the selected path's. `None` only when the connection has no
/// open path, which a closed connection reports.
pub(crate) async fn path_rtt(conn: &Connection, grace: Duration) -> Option<PathRtt> {
    path_rtt_with(grace, direct_path_rtt(conn), || selected_path_rtt(conn)).await
}

/// [`path_rtt`] over its two sources: `direct` reports the direct path's
/// estimate, and `selected` reads the selected path's estimate once `direct`
/// has had `grace`.
async fn path_rtt_with(
    grace: Duration,
    direct: impl Future<Output = Option<Duration>>,
    selected: impl FnOnce() -> Option<Duration>,
) -> Option<PathRtt> {
    if let Ok(Some(rtt)) = tokio::time::timeout(grace, direct).await {
        return Some(PathRtt { rtt, direct: true });
    }
    selected().map(|rtt| PathRtt { rtt, direct: false })
}

#[cfg(test)]
mod tests;
