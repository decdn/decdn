//! Chain group: read-only RPC reachability, chain-id match, and contract
//! code-presence at each configured address. No signer, no state change.

use std::time::Duration;

use alloy::primitives::Address;
use alloy::providers::{Provider, ProviderBuilder};
use decdn_common::config::ResolvedConfig;

use super::{Finding, Report, Severity};

pub fn classify_chain_id(configured: u64, onchain: u64) -> Finding {
    let ok = configured == onchain;
    Finding {
        group: "Chain",
        id: "chain.chain_id",
        severity: if ok { Severity::Pass } else { Severity::Fail },
        title: if ok {
            "chain_id matches".into()
        } else {
            "chain_id mismatch".into()
        },
        detail: Some(format!("configured={configured} onchain={onchain}")),
        remediation: if ok {
            None
        } else {
            Some(
                "point blockchain.rpc_url at the network whose id matches blockchain.chain_id"
                    .into(),
            )
        },
    }
}

pub fn classify_code(name: &str, address: &str, code_len: usize) -> Finding {
    let ok = code_len > 0;
    Finding {
        group: "Chain",
        id: "chain.code",
        severity: if ok { Severity::Pass } else { Severity::Fail },
        title: if ok {
            format!("{name} contract present")
        } else {
            format!("{name} has no contract code")
        },
        detail: Some(format!("name={name} address={address} code_len={code_len}")),
        remediation: if ok {
            None
        } else {
            Some(format!(
                "check the {name} address and that rpc_url points at the right network"
            ))
        },
    }
}

/// Dial the RPC and push chain-group findings. All calls are wrapped in
/// `timeout_ms`; an unreachable RPC is a single `Fail` and code checks are
/// skipped.
pub async fn check_chain(report: &mut Report, cfg: &ResolvedConfig, timeout_ms: u64) {
    let dur = Duration::from_millis(timeout_ms);
    let url = match cfg.blockchain.rpc_url.parse() {
        Ok(u) => u,
        Err(e) => {
            report.push(fail_rpc(&format!("rpc_url is not a valid URL: {e}")));
            return;
        }
    };
    let provider = ProviderBuilder::new().connect_http(url);

    let onchain = match tokio::time::timeout(dur, provider.get_chain_id()).await {
        Ok(Ok(id)) => id,
        Ok(Err(e)) => {
            report.push(fail_rpc(&format!("RPC error: {e}")));
            return;
        }
        Err(_) => {
            report.push(fail_rpc(&format!(
                "RPC did not respond within {timeout_ms} ms"
            )));
            return;
        }
    };
    report.push(Finding {
        group: "Chain",
        id: "chain.rpc",
        severity: Severity::Pass,
        title: "RPC reachable".into(),
        detail: Some(format!("chain_id={onchain}")),
        remediation: None,
    });
    report.push(classify_chain_id(cfg.blockchain.chain_id, onchain));

    // (name, address) pairs: required first, then optional configured ones.
    let mut targets: Vec<(&str, &str)> = vec![
        ("payment_pool", cfg.blockchain.payment_pool_address.as_str()),
        (
            "capacity_bond",
            cfg.blockchain.capacity_bond_address.as_str(),
        ),
        ("slash_judge", cfg.blockchain.slash_judge_address.as_str()),
    ];
    if let Some(addr) = cfg.blockchain.content_blacklist_address.as_deref() {
        targets.push(("content_blacklist", addr));
    }
    if let Some(addr) = cfg.blockchain.origin_assignment_address.as_deref() {
        targets.push(("origin_assignment", addr));
    }
    if let Some(addr) = cfg.blockchain.publisher_registry_address.as_deref() {
        targets.push(("publisher_registry", addr));
    }

    for (name, addr_str) in targets {
        let addr = match addr_str.parse::<Address>() {
            Ok(a) => a,
            Err(e) => {
                report.push(Finding {
                    group: "Chain",
                    id: "chain.code",
                    severity: Severity::Fail,
                    title: format!("{name} address is not a valid address"),
                    detail: Some(format!("address={addr_str} err={e}")),
                    remediation: Some(format!("fix the {name} address in config")),
                });
                continue;
            }
        };
        match tokio::time::timeout(dur, provider.get_code_at(addr)).await {
            Ok(Ok(code)) => report.push(classify_code(name, addr_str, code.len())),
            Ok(Err(e)) => report.push(Finding {
                group: "Chain",
                id: "chain.code",
                severity: Severity::Warn,
                title: format!("could not read code for {name}"),
                detail: Some(format!("address={addr_str} err={e}")),
                remediation: Some("retry; check RPC provider limits".into()),
            }),
            Err(_) => report.push(Finding {
                group: "Chain",
                id: "chain.code",
                severity: Severity::Warn,
                title: format!("code read for {name} timed out"),
                detail: Some(format!("address={addr_str} timeout_ms={timeout_ms}")),
                remediation: None,
            }),
        }
    }
}

fn fail_rpc(msg: &str) -> Finding {
    Finding {
        group: "Chain",
        id: "chain.rpc",
        severity: Severity::Fail,
        title: "chain RPC unreachable".into(),
        detail: Some(msg.to_string()),
        remediation: Some("verify blockchain.rpc_url and network connectivity".into()),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests
mod tests {
    use super::*;
    use crate::commands::doctor::Severity;

    #[test]
    fn chain_id_match_passes_mismatch_fails() {
        assert_eq!(classify_chain_id(421_614, 421_614).severity, Severity::Pass);
        assert_eq!(classify_chain_id(421_614, 1).severity, Severity::Fail);
    }

    #[test]
    fn empty_code_fails_nonempty_passes() {
        assert_eq!(
            classify_code("payment_pool", "0xabc", 0).severity,
            Severity::Fail
        );
        assert_eq!(
            classify_code("payment_pool", "0xabc", 1234).severity,
            Severity::Pass
        );
    }
}
