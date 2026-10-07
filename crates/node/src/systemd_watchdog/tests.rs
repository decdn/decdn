use super::*;

fn sock() -> OsString {
    OsString::from("/run/systemd/notify")
}

#[test]
fn heartbeats_at_half_the_watchdog_period() {
    let hb = Heartbeat::from_env_vars(Some(sock()), Some("60000000"), None, 7);
    assert_eq!(
        hb,
        Some(Heartbeat {
            socket: sock(),
            interval: Duration::from_secs(30),
        })
    );
}

#[test]
fn no_heartbeat_without_a_watchdog_period_or_socket() {
    assert_eq!(Heartbeat::from_env_vars(Some(sock()), None, None, 7), None);
    assert_eq!(
        Heartbeat::from_env_vars(Some(sock()), Some("0"), None, 7),
        None
    );
    assert_eq!(
        Heartbeat::from_env_vars(Some(sock()), Some("x"), None, 7),
        None
    );
    assert_eq!(
        Heartbeat::from_env_vars(None, Some("60000000"), None, 7),
        None
    );
    assert_eq!(
        Heartbeat::from_env_vars(Some(OsString::new()), Some("60000000"), None, 7),
        None
    );
}

#[test]
fn heartbeats_only_for_the_named_pid() {
    assert!(Heartbeat::from_env_vars(Some(sock()), Some("60000000"), Some("7"), 7).is_some());
    assert_eq!(
        Heartbeat::from_env_vars(Some(sock()), Some("60000000"), Some("8"), 7),
        None
    );
}

#[cfg(unix)]
#[test]
fn one_notifier_sends_every_heartbeat_to_a_path_socket() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("notify");
    let listener = std::os::unix::net::UnixDatagram::bind(&path)?;
    let notifier = Notifier::new(path.as_os_str())?;
    for _ in 0..2 {
        notifier.notify()?;
        let mut buf = [0u8; 64];
        let n = listener.recv(&mut buf)?;
        assert_eq!(buf.get(..n), Some(&b"WATCHDOG=1"[..]));
    }
    Ok(())
}
