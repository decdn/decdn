//! Ports group: probe bindability of the QUIC (UDP) and metrics/admin (TCP)
//! ports, and report whether the host has a public address or a configured
//! `network.external_addrs` address. A port already in
//! use while this node's daemon is running is expected, not a fault.

use std::io::ErrorKind;
use std::net::{IpAddr, Ipv4Addr, TcpListener, UdpSocket};

use decdn_common::config::{ResolvedConfig, ResolvedNetwork};
use decdn_common::net::{addr_source, public_quic_addrs, quic_multiaddr, route_public_ips};

use super::{Finding, Report, Severity};

/// Classify a bind attempt on `port`: `AddrInUse` while the daemon is
/// running is expected, not a fault.
pub(crate) fn classify_bind(
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

/// Report whether the host takes direct inbound connections (ADR 001 § Node
/// Discovery). The QUIC addresses a remote peer can dial come from
/// [`public_quic_addrs`], as the daemon counts them: each
/// `network.external_addrs` address on its own port when there are any, else
/// each of `route_public` (the host's route-public IPs) on `network.bind_port`.
/// Both outcomes pass: a host behind NAT is a supported setup whose inbound
/// runs through the relay and hole-punching. A public finding carries a
/// `source=<route|configured> probed=false` detail from [`addr_source`].
pub(crate) fn classify_reachability(route_public: &[IpAddr], network: &ResolvedNetwork) -> Finding {
    let bind_port = network.bind_port;
    let public = public_quic_addrs(route_public, bind_port, &network.external_addrs);
    let source = addr_source(&network.external_addrs);
    if public.is_empty() {
        Finding {
            group: "Ports",
            id: "port.reachability",
            severity: Severity::Pass,
            title: "behind NAT: inbound peers arrive through the relay".to_string(),
            detail: Some(
                "no public address on the default route and no network.external_addrs".to_string(),
            ),
            remediation: Some(format!(
                "for direct inbound, forward UDP {bind_port} to this host, set the forwarded \
                 address in network.external_addrs, restart the node, and register the \
                 address with `decdn node update-multiaddrs`"
            )),
        }
    } else {
        let addrs: Vec<String> = public
            .iter()
            .map(|a| quic_multiaddr(a.ip(), a.port()))
            .collect();
        let mut title = format!("public address: peers dial {} directly", addrs.join(", "));
        if source == "configured" {
            title.push_str(" (verify the forward or load balancer delivers UDP to this host)");
        }
        Finding {
            group: "Ports",
            id: "port.reachability",
            severity: Severity::Pass,
            title,
            detail: Some(format!("source={source} probed=false")),
            remediation: None,
        }
    }
}

/// Push bindability findings for the QUIC, metrics, and (if configured)
/// admin ports, and the host's reachability.
pub(crate) fn check_ports(report: &mut Report, cfg: &ResolvedConfig, daemon_running: bool) {
    report.push(classify_reachability(&route_public_ips(), &cfg.network));
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
mod tests;
