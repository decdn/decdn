//! Chain group: read-only RPC reachability, chain-id match, and contract
//! checks on the governance chain and on each payment chain. No signer, no
//! state change.
//!
//! The governance chain holds `CapacityBond`, `ContentBlacklist` and the other
//! protocol contracts: each configured address must carry code. Each
//! `[[blockchain.payment_chains]]` entry has its own RPC, which must serve the
//! entry's chain id; each pool on it must answer `operatorBps()` and
//! `sweepDomain()` (a `PaymentPool`), its `usdc()` must carry code, and its
//! `sweepRecipient()` must be the configured `cctp_sweep_receiver_address`
//! when one is set.

use std::future::IntoFuture;
use std::time::Duration;

use alloy::primitives::Address;
use alloy::providers::{Provider, ProviderBuilder};
use decdn_common::config::{ResolvedConfig, ResolvedPaymentChain};
use decdn_common::redact::sanitize_err_chain;
use decdn_incentive::Deployment;
use decdn_incentive::payment_pool::PaymentPool;

use super::{Finding, Report, Severity};

/// Compare the configured chain id against the one the RPC reports.
pub(crate) fn classify_chain_id(configured: u64, onchain: u64) -> Finding {
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

/// Compare a payment chain's configured id against the one its RPC reports.
pub(crate) fn classify_payment_chain_id(configured: u64, onchain: u64) -> Finding {
    let ok = configured == onchain;
    Finding {
        group: "Chain",
        id: "chain.payment_chain_id",
        severity: if ok { Severity::Pass } else { Severity::Fail },
        title: if ok {
            format!("payment chain {configured}: chain_id matches")
        } else {
            format!("payment chain {configured}: chain_id mismatch")
        },
        detail: Some(format!("configured={configured} onchain={onchain}")),
        remediation: (!ok).then(|| {
            format!(
                "point the rpc_url of the [[blockchain.payment_chains]] entry with chain_id = \
                 {configured} at that network"
            )
        }),
    }
}

/// Classify a contract-code read: nonzero code length means the contract is
/// deployed at `address`.
pub(crate) fn classify_code(name: &str, address: &str, code_len: usize) -> Finding {
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

/// Classify a pool's identity reads: `operatorBps()` and `sweepDomain()`
/// answer only on a `PaymentPool`.
pub(crate) fn classify_pool_identity(pool: &str, read: Result<(u64, u32), String>) -> Finding {
    match read {
        Ok((bps, domain)) => Finding {
            group: "Chain",
            id: "chain.payment_pool",
            severity: Severity::Pass,
            title: format!("{pool} is a PaymentPool"),
            detail: Some(format!("operator_bps={bps} sweep_domain={domain}")),
            remediation: None,
        },
        Err(err) => Finding {
            group: "Chain",
            id: "chain.payment_pool",
            severity: Severity::Fail,
            title: format!("{pool} does not answer as a PaymentPool"),
            detail: Some(err),
            remediation: Some(
                "check the payment_pools address and its entry's rpc_url and chain_id".into(),
            ),
        },
    }
}

/// Compare a pool's `sweepRecipient()` with the configured
/// `cctp_sweep_receiver_address`. With none configured, report the recipient.
pub(crate) fn classify_sweep_recipient(
    pool: &str,
    configured: Option<Address>,
    onchain: Address,
) -> Finding {
    let ok = configured.is_none_or(|c| c == onchain);
    Finding {
        group: "Chain",
        id: "chain.sweep_recipient",
        severity: if ok { Severity::Pass } else { Severity::Fail },
        title: if ok {
            format!("{pool} sweeps to {onchain}")
        } else {
            format!("{pool} sweeps to another receiver")
        },
        detail: Some(match configured {
            Some(c) => format!("configured={c} onchain={onchain}"),
            None => format!("onchain={onchain} (blockchain.cctp_sweep_receiver_address unset)"),
        }),
        remediation: (!ok).then(|| {
            "the pool belongs to another deployment: remove it from payment_pools, or fix \
             blockchain.cctp_sweep_receiver_address"
                .into()
        }),
    }
}

/// Dial each RPC and push chain-group findings. All calls are wrapped in
/// `timeout_ms`; an unreachable RPC is a single `Fail` and the checks on that
/// chain are skipped.
pub(crate) async fn check_chain(report: &mut Report, cfg: &ResolvedConfig, timeout_ms: u64) {
    check_governance(report, cfg, timeout_ms).await;
    let receiver = cfg
        .blockchain
        .cctp_sweep_receiver_address
        .as_deref()
        .and_then(|a| a.parse::<Address>().ok());
    for chain in &cfg.blockchain.payment_chains {
        check_payment_chain(report, chain, receiver, timeout_ms).await;
    }
}

/// The governance chain: reachable, its id, and code at each contract.
async fn check_governance(report: &mut Report, cfg: &ResolvedConfig, timeout_ms: u64) {
    let Some(provider) = connect(report, &cfg.blockchain.rpc_url, "blockchain.rpc_url") else {
        return;
    };
    let Some(onchain) = chain_id(report, &provider, timeout_ms, "blockchain.rpc_url").await else {
        return;
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
    let mut targets: Vec<(&str, &str)> = vec![(
        "capacity_bond",
        cfg.blockchain.capacity_bond_address.as_str(),
    )];
    if let Some(addr) = cfg.blockchain.content_blacklist_address.as_deref() {
        targets.push(("content_blacklist", addr));
    }
    if let Some(addr) = cfg.blockchain.origin_assignment_address.as_deref() {
        targets.push(("origin_assignment", addr));
    }
    if let Some(addr) = cfg.blockchain.publisher_registry_address.as_deref() {
        targets.push(("publisher_registry", addr));
    }
    if let Some(addr) = cfg.blockchain.cctp_sweep_receiver_address.as_deref() {
        targets.push(("cctp_sweep_receiver", addr));
    }
    for (name, addr_str) in targets {
        check_code(report, &provider, name, addr_str, timeout_ms).await;
    }
}

/// One payment chain: reachable on its own RPC, its id, and each pool.
async fn check_payment_chain(
    report: &mut Report,
    chain: &ResolvedPaymentChain,
    receiver: Option<Address>,
    timeout_ms: u64,
) {
    let what = format!("the rpc_url of payment chain {}", chain.chain_id);
    let Some(provider) = connect(report, &chain.rpc_url, &what) else {
        return;
    };
    let Some(onchain) = chain_id(report, &provider, timeout_ms, &what).await else {
        return;
    };
    let matches = onchain == chain.chain_id;
    report.push(classify_payment_chain_id(chain.chain_id, onchain));
    if !matches {
        return;
    }
    let dur = Duration::from_millis(timeout_ms);
    for pool in &chain.payment_pools {
        let account = Deployment {
            chain_id: chain.chain_id,
            payment_pool: *pool,
        }
        .pool_account()
        .to_string();
        let contract = PaymentPool::new(*pool, &provider);
        let identity = async {
            let bps = timed(dur, contract.operatorBps().call()).await?;
            let domain = timed(dur, contract.sweepDomain().call()).await?;
            Ok::<_, String>((u64::try_from(bps).unwrap_or(u64::MAX), domain))
        }
        .await;
        let is_pool = identity.is_ok();
        report.push(classify_pool_identity(&account, identity));
        if !is_pool {
            continue;
        }
        match timed(dur, contract.usdc().call()).await {
            Ok(usdc) => {
                let name = format!("{account} usdc");
                check_code(report, &provider, &name, &usdc.to_string(), timeout_ms).await;
            }
            Err(err) => report.push(warn_read(&format!("{account} usdc()"), &err)),
        }
        match timed(dur, contract.sweepRecipient().call()).await {
            Ok(onchain) => report.push(classify_sweep_recipient(&account, receiver, onchain)),
            Err(err) => report.push(warn_read(&format!("{account} sweepRecipient()"), &err)),
        }
    }
}

/// Await `call` within `dur`, as a redacted error string on failure.
async fn timed<T, E>(
    dur: Duration,
    call: impl IntoFuture<Output = Result<T, E>>,
) -> Result<T, String>
where
    E: std::error::Error + Send + Sync + 'static,
{
    match tokio::time::timeout(dur, call.into_future()).await {
        Ok(Ok(v)) => Ok(v),
        Ok(Err(e)) => Err(sanitize_err_chain(&anyhow::Error::new(e))),
        Err(_) => Err(format!("no answer within {} ms", dur.as_millis())),
    }
}

/// A read-only provider on `rpc_url`, or a `Fail` naming `what`.
fn connect(report: &mut Report, rpc_url: &str, what: &str) -> Option<impl Provider + use<>> {
    decdn_common::tls::install_crypto_provider();
    match rpc_url.parse() {
        Ok(url) => Some(ProviderBuilder::new().connect_http(url)),
        Err(e) => {
            report.push(fail_rpc(what, &format!("not a valid URL: {e}")));
            None
        }
    }
}

/// The chain id `provider` reports, or a `Fail` naming `what`.
async fn chain_id<P: Provider>(
    report: &mut Report,
    provider: &P,
    timeout_ms: u64,
    what: &str,
) -> Option<u64> {
    match timed(Duration::from_millis(timeout_ms), provider.get_chain_id()).await {
        Ok(id) => Some(id),
        Err(err) => {
            report.push(fail_rpc(what, &format!("RPC error: {err}")));
            None
        }
    }
}

/// Push the code check of `name` at `addr_str`.
async fn check_code<P: Provider>(
    report: &mut Report,
    provider: &P,
    name: &str,
    addr_str: &str,
    timeout_ms: u64,
) {
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
            return;
        }
    };
    match timed(
        Duration::from_millis(timeout_ms),
        provider.get_code_at(addr),
    )
    .await
    {
        Ok(code) => report.push(classify_code(name, addr_str, code.len())),
        Err(err) => report.push(warn_read(&format!("code for {name} at {addr_str}"), &err)),
    }
}

fn warn_read(what: &str, err: &str) -> Finding {
    Finding {
        group: "Chain",
        id: "chain.read",
        severity: Severity::Warn,
        title: format!("could not read {what}"),
        detail: Some(err.to_owned()),
        remediation: Some("retry; check RPC provider limits".into()),
    }
}

fn fail_rpc(what: &str, msg: &str) -> Finding {
    Finding {
        group: "Chain",
        id: "chain.rpc",
        severity: Severity::Fail,
        title: "chain RPC unreachable".into(),
        detail: Some(msg.to_string()),
        remediation: Some(format!("verify {what} and network connectivity")),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests
mod tests;
