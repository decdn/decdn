use super::*;
use std::net::SocketAddr;

fn ip(s: &str) -> TransportAddr {
    TransportAddr::Ip(s.parse::<SocketAddr>().unwrap())
}

#[test]
fn keeps_relay_and_public_ips_drops_the_rest() {
    let relay = TransportAddr::Relay("https://relay.example./".parse().unwrap());
    let input = vec![
        relay.clone(),
        ip("8.8.8.8:4433"),
        ip("[2606:4700::1111]:4433"),
        ip("10.0.0.5:4433"),
        ip("192.168.1.2:4433"),
        ip("127.0.0.1:4433"),
        ip("100.64.0.1:4433"),
        ip("[fd00::1]:4433"),
        ip("[fe80::1]:4433"),
        ip("[::ffff:10.0.0.1]:4433"),
    ];

    let kept = node_publish_filter().apply(&input).into_owned();

    assert_eq!(
        kept,
        vec![relay, ip("8.8.8.8:4433"), ip("[2606:4700::1111]:4433")]
    );
}
