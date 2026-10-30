use std::net::{TcpListener, UdpSocket};

use super::*;

/// How many ephemeral binds [`held_reservation_is_never_an_ephemeral_bind`]
/// makes. Each one lands on a random port in the ephemeral range. With its 32
/// ports released rather than held, a run this size lands on one of them about
/// ten times on a default Linux range; with the ports held it never does.
///
/// The test covers `bind(…:0)` only. Linux gives `bind(…:0)` odd ports and
/// `connect()` even ones, so a reservation, itself a `bind(…:0)`, never meets
/// the `connect()` search on an unexhausted range, and a test of it could not
/// fail.
#[cfg(target_os = "linux")]
const EPHEMERAL_PICKS: usize = 5000;

/// How many `bind(…:0)` calls per protocol
/// [`bind_to_port_zero_never_lands_below_ephemeral_low`] makes. A
/// [`UdpPortClaim`] picks a port below `ephemeral_low()` because the kernel
/// hands out no port there. On a host whose range starts lower, that premise
/// fails, and a run this size lands below `ephemeral_low()`.
const ZERO_BINDS: usize = 1000;

/// The bind `decdn-node`'s `net::bind_reuseaddr` makes for its admin and
/// metrics listeners: `SO_REUSEADDR`, an explicit address, then `listen`.
fn node_style_listen(addr: SocketAddr) -> std::io::Result<Socket> {
    let socket = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP))?;
    socket.set_reuse_address(true)?;
    socket.bind(&addr.into())?;
    socket.listen(16)?;
    Ok(socket)
}

#[cfg(target_os = "linux")]
#[test]
fn held_reservation_is_never_an_ephemeral_bind() {
    let held: Vec<PortReservation> = (0..32).map(|_| PortReservation::tcp().unwrap()).collect();
    let ports: std::collections::HashSet<u16> = held.iter().map(PortReservation::port).collect();
    assert_eq!(ports.len(), held.len(), "live reservations share a port");
    for _ in 0..EPHEMERAL_PICKS {
        // `std` sets `SO_REUSEADDR` on Unix, as tokio and the daemon do.
        let other = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let port = other.local_addr().unwrap().port();
        assert!(!ports.contains(&port), "bind(0) got reserved port {port}");
    }
}

#[test]
fn child_binds_a_reservation() {
    let reservation = PortReservation::tcp().unwrap();
    let node = node_style_listen(reservation.addr()).expect("node-style bind on a reserved port");
    drop(node);
    // `std` (and tokio) listeners, as the gateway and the dashboard bind them.
    let std_listener = TcpListener::bind(reservation.addr()).expect("std bind on a reserved port");
    assert_eq!(std_listener.local_addr().unwrap(), reservation.addr());
}

#[test]
fn child_rebinds_a_reservation_after_a_restart() {
    let reservation = PortReservation::tcp().unwrap();
    let first = node_style_listen(reservation.addr()).unwrap();
    // A served connection leaves a `TIME_WAIT` on the port when the server
    // side closes first, as a killed daemon's does.
    let client = std::net::TcpStream::connect(reservation.addr()).unwrap();
    let (served, _) = first.accept().unwrap();
    drop(served);
    drop(first);
    drop(client);
    node_style_listen(reservation.addr()).expect("rebind after the first child exits");
}

#[cfg(target_os = "linux")]
#[test]
fn reservation_refuses_a_plain_bind() {
    let reservation = PortReservation::tcp().unwrap();
    let plain = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP)).unwrap();
    assert!(
        plain.bind(&reservation.addr().into()).is_err(),
        "a bind without SO_REUSEADDR took a held port"
    );
}

#[test]
fn udp_claim_is_below_the_ephemeral_range() {
    let low = ephemeral_low().unwrap();
    let claim = UdpPortClaim::new().unwrap();
    assert!(
        claim.port() < low,
        "claimed {} in the ephemeral range",
        claim.port()
    );
    assert!(claim.port() >= UDP_CLAIM_FLOOR);
}

#[test]
fn bind_to_port_zero_never_lands_below_ephemeral_low() {
    let low = ephemeral_low().unwrap();
    for _ in 0..ZERO_BINDS {
        let tcp = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let udp = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        for port in [
            tcp.local_addr().unwrap().port(),
            udp.local_addr().unwrap().port(),
        ] {
            assert!(
                port >= low,
                "bind(0) got {port}, below ephemeral_low() {low}"
            );
        }
    }
}

#[test]
fn udp_claim_is_exclusive_and_leaves_the_udp_port_free() {
    let claim = UdpPortClaim::new().unwrap();
    assert!(
        UdpPortClaim::try_port(claim.port()).is_none(),
        "a second fixture claimed a held port"
    );
    let daemon = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, claim.port()))
        .expect("the child's UDP bind on a claimed port");
    // A port with a live UDP socket is not claimable, even with no lock on it.
    drop(claim);
    let port = daemon.local_addr().unwrap().port();
    assert!(UdpPortClaim::try_port(port).is_none());
    drop(daemon);
    assert!(UdpPortClaim::try_port(port).is_some());
}

#[test]
fn claim_range_sits_below_the_ephemeral_range() {
    assert_eq!(claim_range(32768).unwrap(), (16384, 32768));
    assert_eq!(claim_range(49152).unwrap(), (24576, 49152));
    assert_eq!(claim_range(1500).unwrap(), (1024, 1500));
    assert!(claim_range(1024).is_err());
    assert!(claim_range(80).is_err());
}

#[test]
fn parse_ephemeral_low_reads_the_range_start() {
    assert_eq!(parse_ephemeral_low("32768\t60999\n").unwrap(), 32768);
    assert!(parse_ephemeral_low("").is_err());
    assert!(parse_ephemeral_low("high 60999").is_err());
}
