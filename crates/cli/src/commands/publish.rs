//! `decdn publish` — origin-publisher control plane (issue #1029).
//!
//! Submits the `PublisherRegistry` / `OriginAssignment` writes that map
//! content to namespaces and propose authorized origins. Mirrors the
//! on-chain-write pattern of `super::register`: resolve chain coordinates,
//! load the keystore signer, build a wallet-filled provider, submit, and
//! print a JSON or `key=value` receipt. `--dry-run` prints the resolved
//! parameters without sending.

use std::io;
use std::path::Path;

use alloy::primitives::{Address, B256, U256};
use anyhow::Context;
use decdn_common::cli;
use decdn_incentive::origin_assignment::OriginAssignment;
use decdn_incentive::publisher_registry::PublisherRegistry;

use crate::commands::chain_ctx;
use crate::commands::chain_ctx::ResolvedPublish;
use crate::commands::fetch::parse_hash;

/// Entry point for `decdn publish`.
pub async fn publish_dispatch(
    args: &cli::PublishArgs,
    global_config: Option<&Path>,
) -> anyhow::Result<()> {
    match &args.command {
        cli::PublishCommand::Namespace(ns) => match &ns.command {
            cli::NamespaceCommand::Create(a) => namespace_create(a, global_config).await,
        },
        cli::PublishCommand::Claim(a) => claim(a, global_config).await,
        cli::PublishCommand::Assign(a) => assign(a, global_config).await,
    }
}

/// Resolve the publisher coordinates and parse the `PublisherRegistry`
/// address. Shared by `namespace create` and `claim`. The signer and provider
/// are built by the caller in its own scope (the wallet-filled provider
/// borrows the signer under Rust 2024 capture rules, so both must be locals).
fn registry_ctx(
    chain: &cli::PublishChainArgs,
    global_config: Option<&Path>,
) -> anyhow::Result<(ResolvedPublish, Address)> {
    let config_path = chain.config.as_deref().or(global_config);
    let file = chain_ctx::load_optional_config(config_path)?;
    let resolved = chain_ctx::resolve_publish(chain, &file)?;
    let registry = chain_ctx::parse_address(
        resolved
            .publisher_registry_address
            .as_deref()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "publisher_registry_address not set (pass \
                     --publisher-registry-address or set \
                     blockchain.publisher_registry_address)"
                )
            })?,
        "publisher_registry_address",
    )?;
    Ok((resolved, registry))
}

/// Reject duplicate operator addresses before submitting — the contract
/// reverts `DuplicateOperator` (`OriginAssignment.proposeAssignment`), so
/// failing fast on the client saves the gas of a doomed transaction.
fn ensure_unique_operators(ops: &[Address]) -> anyhow::Result<()> {
    let mut seen = std::collections::HashSet::with_capacity(ops.len());
    for op in ops {
        if !seen.insert(*op) {
            anyhow::bail!("duplicate operator address: {op:#x}");
        }
    }
    Ok(())
}

/// Fail if the RPC reports a different chain id than the resolved one, so a
/// mis-pointed `--rpc-url` cannot submit to the wrong network. Pure so it is
/// unit-testable; [`ensure_rpc_chain_id`] does the single network read.
fn chain_id_guard(expected: u64, rpc: u64) -> anyhow::Result<()> {
    anyhow::ensure!(
        expected == rpc,
        "chain id mismatch: --chain-id/config expects {expected} but the RPC reports {rpc} \
         (check --rpc-url points at the right network, or pass --chain-id {rpc})",
    );
    Ok(())
}

/// Read the RPC's chain id and enforce it matches `expected` before submitting.
async fn ensure_rpc_chain_id<P: alloy::providers::Provider>(
    provider: &P,
    expected: u64,
) -> anyhow::Result<()> {
    let rpc = provider
        .get_chain_id()
        .await
        .context("failed to read chainId from the RPC (is --rpc-url reachable?)")?;
    chain_id_guard(expected, rpc)
}

// -------------------------------------------------------------------------
// namespace create
// -------------------------------------------------------------------------

/// Result of `namespace create`. `operator`/`namespace_id`/`tx` are `None` on
/// a dry run (which loads no keystore, so the signer address is unknown).
pub(crate) struct NamespaceOutcome {
    pub(crate) operator: Option<Address>,
    pub(crate) registry: Address,
    pub(crate) namespace_id: Option<u64>,
    pub(crate) tx: Option<B256>,
}

pub(crate) fn write_namespace_outcome(
    w: &mut impl io::Write,
    o: &NamespaceOutcome,
    json: bool,
) -> io::Result<()> {
    if json {
        let value = serde_json::json!({
            "submitted": o.tx.is_some(),
            "tx": o.tx.map(|h| format!("{h:#x}")),
            "operator": o.operator.map(|a| format!("{a:#x}")),
            "publisher_registry": format!("{:#x}", o.registry),
            "namespace_id": o.namespace_id,
        });
        return writeln!(w, "{value}");
    }
    if let Some(op) = o.operator {
        writeln!(w, "operator={op:#x}")?;
    }
    writeln!(w, "publisher_registry={:#x}", o.registry)?;
    match (o.namespace_id, o.tx) {
        (Some(id), Some(h)) => writeln!(w, "submitted=true namespace_id={id} tx={h:#x}"),
        _ => writeln!(w, "submitted=false dry_run=true"),
    }
}

async fn namespace_create(
    args: &cli::NamespaceCreateArgs,
    global_config: Option<&Path>,
) -> anyhow::Result<()> {
    let (resolved, registry) = registry_ctx(&args.chain, global_config)?;
    let mut outcome = NamespaceOutcome {
        operator: None,
        registry,
        namespace_id: None,
        tx: None,
    };

    if !args.chain.dry_run {
        // The keystore is decrypted only when actually submitting — a dry run
        // needs no secrets.
        let signer = chain_ctx::load_signer_with_password_file(
            args.chain.keystore_password_file.as_deref(),
            &resolved.keystore,
        )
        .await?;
        let provider = decdn_client_pull::provider::build_provider(&resolved.rpc_url, &signer)?;
        outcome.operator = Some(signer.address());
        ensure_rpc_chain_id(&provider, resolved.chain_id).await?;
        let contract = PublisherRegistry::new(registry, &provider);
        let pending =
            contract.createNamespace().send().await.context(
                "createNamespace failed to send (check RPC, gas, and the registry address)",
            )?;
        let receipt = pending
            .get_receipt()
            .await
            .context("createNamespace sent but the receipt could not be fetched")?;
        let tx = receipt.transaction_hash;
        anyhow::ensure!(receipt.status(), "createNamespace reverted (tx {tx})");
        // The new id is authoritative from the emitted event: the return value
        // is not available from a receipt, and a static pre-call would race a
        // concurrent create (`_nextNamespaceId` is global across publishers).
        let namespace_id = receipt
            .inner
            .logs()
            .iter()
            .find_map(|log| log.log_decode::<PublisherRegistry::NamespaceCreated>().ok())
            .map(|decoded| decoded.inner.data.namespaceId)
            .context("createNamespace receipt missing NamespaceCreated event")?;
        let id_u64 = u64::try_from(namespace_id)
            .context("namespace id exceeds u64 (unexpected on this chain)")?;
        outcome.namespace_id = Some(id_u64);
        outcome.tx = Some(tx);
    }

    let mut out = io::stdout().lock();
    write_namespace_outcome(&mut out, &outcome, args.chain.json)
        .context("failed to write namespace-create output")?;
    Ok(())
}

// -------------------------------------------------------------------------
// claim
// -------------------------------------------------------------------------

pub(crate) struct ClaimOutcome {
    pub(crate) operator: Option<Address>,
    pub(crate) registry: Address,
    pub(crate) namespace_id: u64,
    pub(crate) hash: [u8; 32],
    pub(crate) tx: Option<B256>,
}

pub(crate) fn write_claim_outcome(
    w: &mut impl io::Write,
    o: &ClaimOutcome,
    json: bool,
) -> io::Result<()> {
    let hash_hex = format!("0x{}", alloy::hex::encode(o.hash));
    if json {
        let value = serde_json::json!({
            "submitted": o.tx.is_some(),
            "tx": o.tx.map(|h| format!("{h:#x}")),
            "operator": o.operator.map(|a| format!("{a:#x}")),
            "publisher_registry": format!("{:#x}", o.registry),
            "namespace_id": o.namespace_id,
            "hash": hash_hex,
        });
        return writeln!(w, "{value}");
    }
    if let Some(op) = o.operator {
        writeln!(w, "operator={op:#x}")?;
    }
    writeln!(w, "publisher_registry={:#x}", o.registry)?;
    writeln!(w, "namespace_id={}", o.namespace_id)?;
    writeln!(w, "hash={hash_hex}")?;
    match o.tx {
        Some(h) => writeln!(w, "submitted=true tx={h:#x}"),
        None => writeln!(w, "submitted=false dry_run=true"),
    }
}

async fn claim(args: &cli::ClaimArgs, global_config: Option<&Path>) -> anyhow::Result<()> {
    let hash = parse_hash(&args.hash)?;
    let (resolved, registry) = registry_ctx(&args.chain, global_config)?;
    let mut outcome = ClaimOutcome {
        operator: None,
        registry,
        namespace_id: args.namespace,
        hash,
        tx: None,
    };

    if !args.chain.dry_run {
        let signer = chain_ctx::load_signer_with_password_file(
            args.chain.keystore_password_file.as_deref(),
            &resolved.keystore,
        )
        .await?;
        let provider = decdn_client_pull::provider::build_provider(&resolved.rpc_url, &signer)?;
        outcome.operator = Some(signer.address());
        ensure_rpc_chain_id(&provider, resolved.chain_id).await?;
        let contract = PublisherRegistry::new(registry, &provider);
        let pending = contract
            .claimContent(U256::from(args.namespace), B256::from(hash))
            .send()
            .await
            .context(
                "claimContent failed to send; the signer must own the namespace \
                 (and the claim must not already exist for it)",
            )?;
        let receipt = pending
            .get_receipt()
            .await
            .context("claimContent sent but the receipt could not be fetched")?;
        let tx = receipt.transaction_hash;
        anyhow::ensure!(
            receipt.status(),
            "claimContent reverted (tx {tx}); likely not the namespace owner, or this \
             namespace already claimed this hash (claims are append-only + idempotent)",
        );
        outcome.tx = Some(tx);
    }

    let mut out = io::stdout().lock();
    write_claim_outcome(&mut out, &outcome, args.chain.json)
        .context("failed to write claim output")?;
    Ok(())
}

// -------------------------------------------------------------------------
// assign (propose-only)
// -------------------------------------------------------------------------

pub(crate) struct AssignOutcome {
    pub(crate) operator: Option<Address>,
    pub(crate) origin_assignment: Address,
    pub(crate) namespace_id: u64,
    pub(crate) operators: Vec<Address>,
    pub(crate) tx: Option<B256>,
}

pub(crate) fn write_assign_outcome(
    w: &mut impl io::Write,
    o: &AssignOutcome,
    json: bool,
) -> io::Result<()> {
    if json {
        let value = serde_json::json!({
            "submitted": o.tx.is_some(),
            "tx": o.tx.map(|h| format!("{h:#x}")),
            "operator": o.operator.map(|a| format!("{a:#x}")),
            "origin_assignment": format!("{:#x}", o.origin_assignment),
            "namespace_id": o.namespace_id,
            "operators": o.operators.iter().map(|a| format!("{a:#x}")).collect::<Vec<_>>(),
            "status": if o.tx.is_some() { "proposed_pending_dao" } else { "dry_run" },
        });
        return writeln!(w, "{value}");
    }
    if let Some(op) = o.operator {
        writeln!(w, "operator={op:#x}")?;
    }
    writeln!(w, "origin_assignment={:#x}", o.origin_assignment)?;
    writeln!(w, "namespace_id={}", o.namespace_id)?;
    writeln!(w, "operators={}", o.operators.len())?;
    for a in &o.operators {
        writeln!(w, "  operator={a:#x}")?;
    }
    match o.tx {
        // Propose-only: activation is a separate governance action.
        Some(h) => writeln!(w, "status=proposed_pending_dao tx={h:#x}"),
        None => writeln!(w, "status=dry_run submitted=false"),
    }
}

async fn assign(args: &cli::AssignArgs, global_config: Option<&Path>) -> anyhow::Result<()> {
    let config_path = args.chain.config.as_deref().or(global_config);
    let file = chain_ctx::load_optional_config(config_path)?;
    let resolved = chain_ctx::resolve_publish(&args.chain, &file)?;
    let oa_addr = chain_ctx::parse_address(
        resolved
            .origin_assignment_address
            .as_deref()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "origin_assignment_address not set (pass --origin-assignment-address \
                     or set blockchain.origin_assignment_address)"
                )
            })?,
        "origin_assignment_address",
    )?;
    let operators = args
        .operators
        .iter()
        .map(|s| chain_ctx::parse_address(s, "operator"))
        .collect::<anyhow::Result<Vec<Address>>>()?;
    ensure_unique_operators(&operators)?;

    let mut outcome = AssignOutcome {
        operator: None,
        origin_assignment: oa_addr,
        namespace_id: args.namespace,
        operators: operators.clone(),
        tx: None,
    };

    if !args.chain.dry_run {
        let signer = chain_ctx::load_signer_with_password_file(
            args.chain.keystore_password_file.as_deref(),
            &resolved.keystore,
        )
        .await?;
        let provider = decdn_client_pull::provider::build_provider(&resolved.rpc_url, &signer)?;
        outcome.operator = Some(signer.address());
        ensure_rpc_chain_id(&provider, resolved.chain_id).await?;
        let contract = OriginAssignment::new(oa_addr, &provider);
        let pending = contract
            .proposeAssignment(U256::from(args.namespace), operators)
            .send()
            .await
            .context(
                "proposeAssignment failed to send; the signer must own the namespace and every \
                 operator must be an active bonded node",
            )?;
        let receipt = pending
            .get_receipt()
            .await
            .context("proposeAssignment sent but the receipt could not be fetched")?;
        let tx = receipt.transaction_hash;
        anyhow::ensure!(
            receipt.status(),
            "proposeAssignment reverted (tx {tx}); likely not the namespace owner, an inactive \
             or duplicate operator, or the set exceeds maxOriginsPerNamespace",
        );
        outcome.tx = Some(tx);
    }

    let mut out = io::stdout().lock();
    write_assign_outcome(&mut out, &outcome, args.chain.json)
        .context("failed to write assign output")?;
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn namespace_dry_run_and_submitted_formats() {
        // Dry run: no signer loaded, so no operator line is printed.
        let base = NamespaceOutcome {
            operator: None,
            registry: Address::repeat_byte(0x01),
            namespace_id: None,
            tx: None,
        };
        let mut buf = Vec::new();
        write_namespace_outcome(&mut buf, &base, false).unwrap();
        let dry = String::from_utf8(buf).unwrap();
        assert!(dry.contains("submitted=false dry_run=true"), "{dry}");
        assert!(!dry.contains("operator="), "{dry}");

        let done = NamespaceOutcome {
            operator: Some(Address::repeat_byte(0xCD)),
            namespace_id: Some(9),
            tx: Some(B256::repeat_byte(0x55)),
            ..base
        };
        let mut buf = Vec::new();
        write_namespace_outcome(&mut buf, &done, false).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("operator=0xcdcd"), "{s}");
        assert!(s.contains("namespace_id=9"), "{s}");
        assert!(s.contains("tx=0x5555"), "{s}");
    }

    #[test]
    fn claim_output_formats() {
        let o = ClaimOutcome {
            operator: Some(Address::repeat_byte(0xCD)),
            registry: Address::repeat_byte(0x01),
            namespace_id: 7,
            hash: [0xAB; 32],
            tx: Some(B256::repeat_byte(0x55)),
        };
        let mut buf = Vec::new();
        write_claim_outcome(&mut buf, &o, false).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("namespace_id=7"), "{s}");
        assert!(s.contains("hash=0xabab"), "{s}");
        assert!(s.contains("submitted=true tx=0x5555"), "{s}");
    }

    #[test]
    fn assign_output_states_propose_only() {
        let o = AssignOutcome {
            operator: Some(Address::repeat_byte(0xCD)),
            origin_assignment: Address::repeat_byte(0x02),
            namespace_id: 7,
            operators: vec![Address::repeat_byte(0x11), Address::repeat_byte(0x22)],
            tx: Some(B256::repeat_byte(0x55)),
        };
        let mut buf = Vec::new();
        write_assign_outcome(&mut buf, &o, false).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("operators=2"), "{s}");
        assert!(s.contains("status=proposed_pending_dao"), "{s}");
        assert!(s.contains("tx=0x5555"), "{s}");
    }

    #[test]
    fn unique_operators_accepts_distinct_rejects_dupes() {
        let a = Address::repeat_byte(0x11);
        let b = Address::repeat_byte(0x22);
        assert!(ensure_unique_operators(&[a, b]).is_ok());
        // Same address, however the user spelled it, parses to one `Address`.
        let err = ensure_unique_operators(&[a, b, a]).unwrap_err().to_string();
        assert!(err.contains("duplicate operator address"), "{err}");
        assert!(err.contains(&format!("{a:#x}")), "{err}");
    }

    #[test]
    fn chain_id_guard_matches_and_mismatches() {
        assert!(chain_id_guard(421614, 421614).is_ok());
        let err = chain_id_guard(421614, 31337).unwrap_err().to_string();
        assert!(err.contains("421614"), "{err}");
        assert!(err.contains("31337"), "{err}");
    }
}
