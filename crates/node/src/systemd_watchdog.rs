//! Runtime-liveness heartbeat for the systemd service watchdog.
//!
//! When the unit sets `WatchdogSec=`, systemd passes `WATCHDOG_USEC` and
//! `NOTIFY_SOCKET` to the daemon and restarts it if no `WATCHDOG=1` arrives
//! within that period. The heartbeat is a task on the tokio runtime, so it
//! proves that a runtime worker is free to run work, not only that the process
//! exists. A task that holds every worker without awaiting stops the heartbeat,
//! and systemd then restarts the node instead of reporting it `active` while it
//! serves nothing.
//!
//! The node has no setting for this. The unit's `WatchdogSec=` turns it on and
//! sets the period. Without `WATCHDOG_USEC` the heartbeat does not start.

use std::ffi::OsString;
use std::time::Duration;

/// Where to send the heartbeat, and how often.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Heartbeat {
    /// The `NOTIFY_SOCKET` address: a filesystem path, or an abstract socket
    /// name that starts with `@`.
    pub socket: OsString,
    /// Half the watchdog period, as `sd_watchdog_enabled(3)` recommends.
    pub interval: Duration,
}

impl Heartbeat {
    /// The heartbeat systemd asks for through the environment, or `None` when
    /// it asks for none.
    ///
    /// `WATCHDOG_PID`, when set, names the process the watchdog is for. A
    /// process with another PID does not send the heartbeat.
    #[must_use]
    pub fn from_env_vars(
        notify_socket: Option<OsString>,
        watchdog_usec: Option<&str>,
        watchdog_pid: Option<&str>,
        own_pid: u32,
    ) -> Option<Self> {
        let socket = notify_socket.filter(|s| !s.is_empty())?;
        let usec: u64 = watchdog_usec?.trim().parse().ok()?;
        if usec == 0 {
            return None;
        }
        if let Some(pid) = watchdog_pid
            && pid.trim().parse::<u32>().ok()? != own_pid
        {
            return None;
        }
        let interval = Duration::from_micros(usec / 2).max(Duration::from_millis(1));
        Some(Self { socket, interval })
    }

    /// The heartbeat systemd asks this process for.
    #[must_use]
    pub fn from_env() -> Option<Self> {
        Self::from_env_vars(
            std::env::var_os("NOTIFY_SOCKET"),
            std::env::var("WATCHDOG_USEC").ok().as_deref(),
            std::env::var("WATCHDOG_PID").ok().as_deref(),
            std::process::id(),
        )
    }
}

/// Send one `WATCHDOG=1` datagram to `socket`.
///
/// # Errors
///
/// Fails when the socket cannot be created or the datagram cannot be sent.
#[cfg(unix)]
pub fn notify_watchdog(socket: &std::ffi::OsStr) -> std::io::Result<()> {
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::net::UnixDatagram;

    const MESSAGE: &[u8] = b"WATCHDOG=1";
    let sock = UnixDatagram::unbound()?;
    // A full receive queue must not block a runtime worker.
    sock.set_nonblocking(true)?;
    match socket.as_bytes().strip_prefix(b"@") {
        Some(name) => send_abstract(&sock, name, MESSAGE),
        None => sock.send_to(MESSAGE, socket).map(drop),
    }
}

#[cfg(target_os = "linux")]
fn send_abstract(
    sock: &std::os::unix::net::UnixDatagram,
    name: &[u8],
    message: &[u8],
) -> std::io::Result<()> {
    use std::os::linux::net::SocketAddrExt;
    let addr = std::os::unix::net::SocketAddr::from_abstract_name(name)?;
    sock.send_to_addr(message, &addr).map(drop)
}

#[cfg(all(unix, not(target_os = "linux")))]
fn send_abstract(
    _sock: &std::os::unix::net::UnixDatagram,
    _name: &[u8],
    _message: &[u8],
) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "abstract NOTIFY_SOCKET names exist only on Linux",
    ))
}

/// Start the heartbeat on the current tokio runtime when systemd asks for one.
///
/// Call it from inside the runtime before any other work, so the heartbeat also
/// covers bring-up and shutdown drain. The task runs until the runtime stops.
pub fn spawn() {
    #[cfg(unix)]
    if let Some(heartbeat) = Heartbeat::from_env() {
        tokio::spawn(run(heartbeat));
    }
}

#[cfg(unix)]
async fn run(heartbeat: Heartbeat) {
    let mut ticker = tokio::time::interval(heartbeat.interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut warned = false;
    loop {
        ticker.tick().await;
        match notify_watchdog(&heartbeat.socket) {
            Ok(()) => warned = false,
            Err(error) if !warned => {
                warned = true;
                tracing::warn!(%error, "systemd watchdog heartbeat failed");
            }
            Err(_) => {}
        }
    }
}

#[cfg(test)]
mod tests {
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
    fn sends_watchdog_to_a_path_socket() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("notify");
        let listener = std::os::unix::net::UnixDatagram::bind(&path)?;
        notify_watchdog(path.as_os_str())?;
        let mut buf = [0u8; 64];
        let n = listener.recv(&mut buf)?;
        assert_eq!(buf.get(..n), Some(&b"WATCHDOG=1"[..]));
        Ok(())
    }
}
