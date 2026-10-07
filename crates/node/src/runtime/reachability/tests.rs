use super::*;

fn ip(s: &str) -> IpAddr {
    s.parse().unwrap()
}

fn sock(s: &str) -> SocketAddr {
    s.parse().unwrap()
}

#[test]
fn no_public_address_is_behind_nat() {
    assert_eq!(classify(&[], 4433, &[]), Reachability::BehindNat);
}

#[test]
fn public_and_registered_on_the_bind_port() {
    assert_eq!(
        classify(&[ip("8.8.8.8")], 4433, &[sock("8.8.8.8:4433")]),
        Reachability::PublicRegistered
    );
}

#[test]
fn public_but_unregistered_or_on_another_port_names_the_missing_multiaddr() {
    let want = Reachability::PublicUnregistered {
        missing: vec!["/ip4/8.8.8.8/udp/4433/quic-v1".to_string()],
    };
    assert_eq!(classify(&[ip("8.8.8.8")], 4433, &[]), want);
    assert_eq!(
        classify(&[ip("8.8.8.8")], 4433, &[sock("8.8.8.8:5000")]),
        want
    );
}
