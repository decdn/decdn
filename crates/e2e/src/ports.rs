//! Loopback ports for the processes the fixtures spawn.
//!
//! Nextest runs the journeys as parallel processes, and the kernel's ephemeral
//! range serves every `bind(…:0)` and every outbound source port on the host. A
//! port a fixture picks and then releases can go to another process before the
//! child binds it. So a fixture never releases a port it hands to a child:
//!
//! - [`PortReservation`] holds a TCP port for as long as the child uses it. The
//!   child binds the same address with `SO_REUSEADDR`.
//! - [`UdpPortClaim`] picks a UDP port below the ephemeral range, where the
//!   kernel never hands a port out, and claims the number across fixture
//!   processes.
//!
//! A child that can bind port 0 and report the port it got needs neither:
//! `anvil` does (see `chain::spawn_anvil_child`).

use std::hash::{BuildHasher as _, Hasher as _};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

use anyhow::Context;
use socket2::{Domain, Protocol, Socket, Type};

/// A loopback TCP port held for a child process to bind.
///
/// The reservation is a socket bound to `127.0.0.1:<port>` with `SO_REUSEADDR`
/// that never listens. On Linux this has two effects while it lives:
///
/// - The kernel never picks the port for another socket's `bind(…:0)`, nor as
///   the source port of a `connect()`. A bound socket that is not listening
///   still counts as a conflict for the ephemeral search, and `connect()` skips
///   every port that a `bind()` holds.
/// - A child that binds `127.0.0.1:<port>` explicitly with `SO_REUSEADDR` and
///   then listens succeeds. Two `SO_REUSEADDR` sockets conflict on an explicit
///   bind only when the other one is listening.
///
/// `decdn-node`'s admin and metrics listeners (`net::bind_reuseaddr`),
/// `std::net::TcpListener::bind` and tokio's `TcpListener::bind` (the gateway and
/// the dashboard) all set `SO_REUSEADDR` on Unix, so each binds a reserved port.
///
/// Keep the reservation for as long as the child may bind the port, a restart
/// included. Dropping it releases the port.
///
/// Other platforms do not give these guarantees: there the reservation releases
/// its socket when it is made, and the port is only a number that was free.
#[derive(Debug)]
pub struct PortReservation {
    /// The held socket. `None` off Linux.
    _socket: Option<Socket>,
    port: u16,
}

impl PortReservation {
    /// Reserve a free loopback TCP port.
    pub fn tcp() -> anyhow::Result<Self> {
        let socket = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP))
            .context("create a port reservation socket")?;
        socket
            .set_reuse_address(true)
            .context("set SO_REUSEADDR on a port reservation")?;
        let any = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
        socket
            .bind(&any.into())
            .context("bind a port reservation")?;
        let port = socket
            .local_addr()
            .context("port reservation local_addr")?
            .as_socket()
            .context("port reservation is not an inet socket")?
            .port();
        Ok(Self {
            _socket: cfg!(target_os = "linux").then_some(socket),
            port,
        })
    }

    /// The reserved port number.
    #[must_use]
    pub const fn port(&self) -> u16 {
        self.port
    }

    /// The reserved loopback address, `127.0.0.1:<port>`.
    #[must_use]
    pub const fn addr(&self) -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, self.port))
    }
}

/// A UDP port a child process binds on every address (`0.0.0.0:<port>`), such
/// as the daemon's QUIC endpoint.
///
/// iroh binds its UDP socket without `SO_REUSEADDR`, so a held UDP socket would
/// block the child. The claim avoids the kernel instead: the port lies below the
/// ephemeral range, so no `bind(…:0)` and no outbound socket ever gets it. A
/// TCP socket bound to `127.0.0.1:<port>` without `SO_REUSEADDR` is the lock
/// that keeps two fixtures, in this process or another, off the same number.
///
/// Keep the claim for as long as the child may bind the port, a restart
/// included.
#[derive(Debug)]
pub struct UdpPortClaim {
    _lock: Socket,
    port: u16,
}

/// How many random ports [`UdpPortClaim::new`] tries before it fails.
const UDP_CLAIM_ATTEMPTS: usize = 512;

/// The lowest port [`UdpPortClaim`] picks.
const UDP_CLAIM_FLOOR: u16 = 1024;

impl UdpPortClaim {
    /// Claim a free UDP port below the kernel's ephemeral range.
    pub fn new() -> anyhow::Result<Self> {
        let (low, high) = claim_range(ephemeral_low()?)?;
        let span = u64::from(high - low);
        for _ in 0..UDP_CLAIM_ATTEMPTS {
            let offset = std::hash::RandomState::new().build_hasher().finish() % span;
            let port = low.saturating_add(u16::try_from(offset).unwrap_or_default());
            if let Some(claim) = Self::try_port(port) {
                return Ok(claim);
            }
        }
        anyhow::bail!("no free UDP port in {low}..{high} after {UDP_CLAIM_ATTEMPTS} random picks")
    }

    /// Claim `port` if no other fixture holds its lock and no socket holds the
    /// UDP port now.
    fn try_port(port: u16) -> Option<Self> {
        let lock = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP)).ok()?;
        lock.bind(&SocketAddr::from((Ipv4Addr::LOCALHOST, port)).into())
            .ok()?;
        // A plain bind: the daemon's socket sets no `SO_REUSEADDR` either, so
        // this fails exactly when the daemon's bind would.
        drop(std::net::UdpSocket::bind((Ipv4Addr::UNSPECIFIED, port)).ok()?);
        Some(Self { _lock: lock, port })
    }

    /// The claimed port number.
    #[must_use]
    pub const fn port(&self) -> u16 {
        self.port
    }
}

/// The ports a [`UdpPortClaim`] picks from: the half-open range below the
/// ephemeral range's first port `ephemeral_low`, at most as wide as the space
/// under it and never below [`UDP_CLAIM_FLOOR`].
fn claim_range(ephemeral_low: u16) -> anyhow::Result<(u16, u16)> {
    let low = (ephemeral_low / 2).max(UDP_CLAIM_FLOOR);
    anyhow::ensure!(
        low < ephemeral_low,
        "the ephemeral port range starts at {ephemeral_low}, which leaves no unprivileged \
         port below it for a UDP claim"
    );
    Ok((low, ephemeral_low))
}

/// The first port of the kernel's ephemeral range. Linux reads it from
/// `/proc/sys/net/ipv4/ip_local_port_range`. Other platforms use the IANA
/// dynamic range, which starts at 49152.
fn ephemeral_low() -> anyhow::Result<u16> {
    const RANGE: &str = "/proc/sys/net/ipv4/ip_local_port_range";
    if !cfg!(target_os = "linux") {
        return Ok(49152);
    }
    let text = std::fs::read_to_string(RANGE).with_context(|| format!("read {RANGE}"))?;
    parse_ephemeral_low(&text).with_context(|| format!("parse {RANGE}: {text:?}"))
}

/// The first number in an `ip_local_port_range` line (`"32768\t60999\n"`).
fn parse_ephemeral_low(text: &str) -> anyhow::Result<u16> {
    text.split_whitespace()
        .next()
        .context("empty port range")?
        .parse()
        .context("port range start is not a port")
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests;
