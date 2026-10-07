//! Shared TCP bind helper for the node's control-plane listeners (the admin RPC
//! server and the metrics endpoint). Both default to loopback but are
//! operator-settable to a wider bind (`metrics_bind = "0.0.0.0"` for
//! containerised scrapers), which the listeners warn on but allow.
//!
//! Both listeners bind a **fixed** operator-configured port, and both may be
//! re-bound moments after a prior `decdn-node` process exits — an operator
//! restart, or a supervisor relaunch. Without `SO_REUSEADDR` that rebind
//! intermittently fails with `EADDRINUSE`: a control-plane connection the old
//! process handled can still sit in `TIME_WAIT` on the same address, and a plain
//! bind refuses to reuse it. Reaping the old process (`kill` + `wait`) does not
//! clear that kernel-side `TIME_WAIT`, so awaiting exit is not enough; the socket
//! option is.

use std::net::SocketAddr;

use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::TcpListener;

/// `listen(2)` backlog. Matches the value tokio's own `TcpListener::bind` passes
/// (`1024`), so switching to this helper does not change queue behaviour.
const LISTEN_BACKLOG: i32 = 1024;

/// Bind a non-blocking TCP [`TcpListener`] on `addr` with `SO_REUSEADDR` set.
///
/// `SO_REUSEADDR` lets the bind succeed even when a socket from a just-exited
/// process lingers in `TIME_WAIT` on the same local address — the exact race
/// that makes a same-port daemon restart flaky. It does **not** permit two live
/// listeners on one port (that is `SO_REUSEPORT`), so a genuine "port already in
/// use by another running process" still fails loudly with `EADDRINUSE`. The
/// option is set on Unix only, where the daemon runs; on Windows `SO_REUSEADDR`
/// has laxer semantics that would allow two live binds, so it is left off there.
///
/// # Errors
/// Returns the underlying `std::io::Error` if socket creation, `bind`, or
/// `listen` fails.
pub(crate) fn bind_reuseaddr(addr: SocketAddr) -> std::io::Result<TcpListener> {
    let socket = Socket::new(Domain::for_address(addr), Type::STREAM, Some(Protocol::TCP))?;
    // Unix: allow rebinding over a `TIME_WAIT` remnant. Skipped on Windows,
    // where the same option would also permit two live listeners on the port.
    #[cfg(unix)]
    socket.set_reuse_address(true)?;
    // tokio requires the underlying socket be non-blocking before adoption.
    socket.set_nonblocking(true)?;
    socket.bind(&addr.into())?;
    socket.listen(LISTEN_BACKLOG)?;
    TcpListener::from_std(std::net::TcpListener::from(socket))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests;
