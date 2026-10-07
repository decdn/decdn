use super::*;

/// The whole point of the helper: the returned listener actually carries
/// `SO_REUSEADDR`. Read the option back off the live socket so a future
/// refactor that drops the `set_reuse_address` call fails here rather than
/// re-introducing the flaky-restart race under the e2e suite.
#[cfg(unix)]
#[tokio::test]
async fn bind_reuseaddr_enables_so_reuseaddr() {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let listener = bind_reuseaddr(addr).expect("bind loopback");
    let sock = socket2::SockRef::from(&listener);
    assert!(
        sock.reuse_address().expect("read SO_REUSEADDR"),
        "control-plane listener must set SO_REUSEADDR"
    );
}

/// `SO_REUSEADDR` must not degrade into `SO_REUSEPORT`: a second live bind on
/// the same concrete port still fails. This guards the "genuine port clash
/// still errors" contract the daemon relies on for fail-fast startup.
#[tokio::test]
async fn bind_reuseaddr_rejects_a_second_live_bind() {
    let first = bind_reuseaddr("127.0.0.1:0".parse().unwrap()).expect("bind loopback");
    let addr = first.local_addr().expect("resolve bound port");
    assert!(
        bind_reuseaddr(addr).is_err(),
        "a second live listener on the same port must fail"
    );
}
