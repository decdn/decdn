//! Ports group: probe bindability of the QUIC (UDP) and metrics/admin (TCP)
//! ports. A port already in use while this node's daemon is running is
//! expected, not a fault.

use std::io::ErrorKind;
use std::net::{Ipv4Addr, TcpListener, UdpSocket};

use decdn_common::config::ResolvedConfig;

use super::{Finding, Report, Severity};

/// Classify a bind attempt on `port`: `AddrInUse` while the daemon is
/// running is expected, not a fault.
pub fn classify_bind(
    label: &str,
    port: u16,
    result: Result<(), ErrorKind>,
    daemon_running: bool,
) -> Finding {
    match result {
        Ok(()) => Finding {
            group: "Ports",
            id: "port.bind",
            severity: Severity::Pass,
            title: format!("{label} port {port} is bindable"),
            detail: None,
            remediation: None,
        },
        Err(ErrorKind::AddrInUse) if daemon_running => Finding {
            group: "Ports",
            id: "port.bind",
            severity: Severity::Pass,
            title: format!("{label} port {port} in use by the running node"),
            detail: None,
            remediation: None,
        },
        Err(ErrorKind::AddrInUse) => Finding {
            group: "Ports",
            id: "port.bind",
            severity: Severity::Fail,
            title: format!("{label} port {port} already in use"),
            detail: None,
            remediation: Some(format!("free port {port} or reconfigure {label}")),
        },
        Err(kind) => Finding {
            group: "Ports",
            id: "port.bind",
            severity: Severity::Warn,
            title: format!("{label} port {port} bind failed"),
            detail: Some(format!("error={kind:?}")),
            remediation: None,
        },
    }
}

fn try_tcp(port: u16) -> Result<(), ErrorKind> {
    TcpListener::bind((Ipv4Addr::LOCALHOST, port))
        .map(|_| ())
        .map_err(|e| e.kind())
}

fn try_udp(port: u16) -> Result<(), ErrorKind> {
    UdpSocket::bind((Ipv4Addr::UNSPECIFIED, port))
        .map(|_| ())
        .map_err(|e| e.kind())
}

/// Push bindability findings for the QUIC, metrics, and (if configured)
/// admin ports.
pub fn check_ports(report: &mut Report, cfg: &ResolvedConfig, daemon_running: bool) {
    report.push(classify_bind(
        "quic bind",
        cfg.network.bind_port,
        try_udp(cfg.network.bind_port),
        daemon_running,
    ));
    report.push(classify_bind(
        "metrics",
        cfg.observability.metrics_port,
        try_tcp(cfg.observability.metrics_port),
        daemon_running,
    ));
    if let Some(admin) = cfg.observability.admin_port {
        report.push(classify_bind(
            "admin",
            admin,
            try_tcp(admin),
            daemon_running,
        ));
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests
mod tests {
    use super::*;
    use std::io::ErrorKind;

    #[test]
    fn free_port_passes() {
        assert_eq!(
            classify_bind("admin", 9191, Ok(()), false).severity,
            Severity::Pass
        );
    }

    #[test]
    fn in_use_with_daemon_is_pass_without_is_fail() {
        assert_eq!(
            classify_bind("admin", 9191, Err(ErrorKind::AddrInUse), true).severity,
            Severity::Pass
        );
        assert_eq!(
            classify_bind("admin", 9191, Err(ErrorKind::AddrInUse), false).severity,
            Severity::Fail
        );
    }

    #[test]
    fn other_error_warns() {
        assert_eq!(
            classify_bind("admin", 9191, Err(ErrorKind::PermissionDenied), false).severity,
            Severity::Warn
        );
    }
}
