use std::net::{TcpListener, UdpSocket};

// The daemon's own bind for its admin and metrics listeners.
use decdn_node::net::bind_reuseaddr as node_listen;

use super::*;

/// How many ephemeral binds [`held_reservation_is_never_an_ephemeral_bind`]
/// makes. A `SO_REUSEADDR` `bind(…:0)` searches the odd ports in the lower
/// half of the ephemeral range first, and on an unexhausted range it lands
/// there, on a random one. The reservations are such binds too, so they land
/// in the same 7057 ports of the default Linux range (32768..60999). With its
/// 32 ports released rather than held, a run this size lands on one of them
/// about 23 times (5000 × 32 / 7057); with the ports held it never does.
///
/// The test covers `bind(…:0)` only. Linux gives `bind(…:0)` odd ports and
/// `connect()` even ones, so a reservation, itself a `bind(…:0)`, never meets
/// the `connect()` search on an unexhausted range, and a test of it could not
/// fail.
#[cfg(target_os = "linux")]
const EPHEMERAL_PICKS: usize = 5000;

/// How many UDP `bind(…:0)` calls
/// [`udp_bind_to_port_zero_never_lands_below_ephemeral_low`] makes. A
/// [`UdpPortClaim`] picks a port below `ephemeral_low()` because the kernel
/// hands out no UDP port there. On Linux and macOS `ephemeral_low()` reads the
/// range the kernel uses, so the test checks that read: had it returned a
/// value 100 ports above the start of the default macOS range (49152..65535),
/// a run this size would land below it about six times. Other platforms use a
/// fixed 49152, so there the test checks that constant.
const ZERO_BINDS: usize = 1000;

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

#[tokio::test]
async fn child_binds_a_reservation() {
    let reservation = PortReservation::tcp().unwrap();
    let node = node_listen(reservation.addr()).expect("the daemon's bind on a reserved port");
    drop(node);
    // `std` (and tokio) listeners, as the gateway and the dashboard bind them.
    let std_listener = TcpListener::bind(reservation.addr()).expect("std bind on a reserved port");
    assert_eq!(std_listener.local_addr().unwrap(), reservation.addr());
}

#[tokio::test]
async fn child_rebinds_a_reservation_after_a_restart() {
    let reservation = PortReservation::tcp().unwrap();
    let first = node_listen(reservation.addr()).unwrap();
    // A served connection leaves a `TIME_WAIT` on the port when the server
    // side closes first, as a killed daemon's does.
    let client = tokio::net::TcpStream::connect(reservation.addr())
        .await
        .unwrap();
    let (served, _) = first.accept().await.unwrap();
    drop(served);
    drop(first);
    drop(client);
    node_listen(reservation.addr()).expect("rebind after the first child exits");
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
fn udp_bind_to_port_zero_never_lands_below_ephemeral_low() {
    let low = ephemeral_low().unwrap();
    for _ in 0..ZERO_BINDS {
        let udp = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let port = udp.local_addr().unwrap().port();
        assert!(
            port >= low,
            "bind(0) got {port}, below ephemeral_low() {low}"
        );
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
fn udp_claim_fails_when_every_pick_is_held() {
    // `std` sets `SO_REUSEADDR`, but the claim's lock does not, so the lock's
    // bind on a listening port fails.
    let held = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let port = held.local_addr().unwrap().port();
    let Some(high) = port.checked_add(1) else {
        return;
    };
    let err = UdpPortClaim::claim_in(port, high).unwrap_err();
    assert!(
        err.to_string().contains("no free UDP port"),
        "unexpected error: {err:#}"
    );
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
    assert_eq!(parse_ephemeral_low("49152\n").unwrap(), 49152);
    assert!(parse_ephemeral_low("").is_err());
    assert!(parse_ephemeral_low("high 60999").is_err());
}

#[test]
fn read_ephemeral_low_reads_a_range_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ip_local_port_range");
    std::fs::write(&path, "32768\t60999\n").unwrap();
    assert_eq!(read_ephemeral_low(&path).unwrap(), 32768);
}

#[test]
fn read_ephemeral_low_names_a_missing_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ip_local_port_range");
    let err = read_ephemeral_low(&path).unwrap_err();
    assert!(
        err.to_string()
            .starts_with(&format!("read {}", path.display())),
        "unexpected error: {err:#}"
    );
    assert_eq!(
        err.root_cause()
            .downcast_ref::<std::io::Error>()
            .map(std::io::Error::kind),
        Some(std::io::ErrorKind::NotFound)
    );
}

#[test]
fn read_ephemeral_low_names_an_unparsable_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ip_local_port_range");
    std::fs::write(&path, "").unwrap();
    let err = read_ephemeral_low(&path).unwrap_err();
    assert!(
        err.to_string()
            .starts_with(&format!("parse {}", path.display())),
        "unexpected error: {err:#}"
    );
}
