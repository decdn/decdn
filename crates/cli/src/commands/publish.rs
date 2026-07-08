//! `decdn publish` — origin-publisher control plane (issue #1029).
//!
//! Submits the `PublisherRegistry` / `OriginAssignment` writes that map
//! content to namespaces and propose authorized origins. Mirrors the
//! on-chain-write pattern of `super::register`: resolve chain coordinates and
//! parse the target contract address, then — on the submit path only — load
//! the keystore signer, build a wallet-filled provider, submit, and print a
//! JSON or `key=value` receipt. `--dry-run` stops after resolution: it loads
//! no keystore and sends no transaction, printing just the resolved parameters.

use std::io;
use std::path::Path;

use alloy::primitives::{Address, B256, U256};
use alloy::providers::ProviderBuilder;
use alloy::sol_types::SolEvent;
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

/// Verify the RPC network matches `expected` using a read-only (wallet-less)
/// provider, *before* any keystore decryption — so a mis-pointed `--rpc-url`
/// fails fast without an interactive password prompt or the scrypt KDF. The
/// `rpc_url` value is never echoed into the parse error (it commonly carries an
/// API key), matching [`decdn_client_pull::provider::build_provider`].
async fn preflight_chain_id(rpc_url: &str, expected: u64) -> anyhow::Result<()> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse().with_context(|| {
        format!(
            "rpc_url is not a valid URL (<redacted>, {} chars)",
            rpc_url.len()
        )
    })?);
    ensure_rpc_chain_id(&provider, expected).await
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
        // Verify the network before touching secrets — a wrong --rpc-url fails
        // here, not after an interactive password prompt + scrypt KDF.
        preflight_chain_id(&resolved.rpc_url, resolved.chain_id).await?;
        // The keystore is decrypted only when actually submitting — a dry run
        // needs no secrets.
        let signer = chain_ctx::load_signer_with_password_file(
            args.chain.keystore_password_file.as_deref(),
            &resolved.keystore,
        )
        .await?;
        let provider = decdn_client_pull::provider::build_provider(&resolved.rpc_url, &signer)?;
        outcome.operator = Some(signer.address());
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
        // The new id comes from the emitted event: the return value is not in a
        // receipt, and a static pre-call would race a concurrent create
        // (`_nextNamespaceId` is global across publishers). Match the log by
        // event signature first, then decode — so an ABI drift surfaces as a
        // decode error rather than a misleading "missing event".
        let created = receipt
            .inner
            .logs()
            .iter()
            .find(|log| log.topic0() == Some(&PublisherRegistry::NamespaceCreated::SIGNATURE_HASH))
            .context(
                "createNamespace succeeded on-chain but its receipt carried no NamespaceCreated \
                 log (the namespace id could not be recovered; check the tx on a block explorer)",
            )?
            .log_decode::<PublisherRegistry::NamespaceCreated>()
            .context(
                "createNamespace emitted a NamespaceCreated log that failed to decode (ABI \
                 mismatch between this CLI and the deployed PublisherRegistry?)",
            )?;
        let id_u64 = u64::try_from(created.inner.data.namespaceId)
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
        preflight_chain_id(&resolved.rpc_url, resolved.chain_id).await?;
        let signer = chain_ctx::load_signer_with_password_file(
            args.chain.keystore_password_file.as_deref(),
            &resolved.keystore,
        )
        .await?;
        let provider = decdn_client_pull::provider::build_provider(&resolved.rpc_url, &signer)?;
        outcome.operator = Some(signer.address());
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
             namespace already claimed this hash (claims are append-only; re-claiming the \
             same hash reverts)",
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
    /// Unix time the assignment timelock elapses (earliest DAO activation),
    /// from the `AssignmentProposed` event. `None` on a dry run.
    pub(crate) ready_at: Option<u64>,
    /// True when this proposal silently replaced a prior pending one on-chain
    /// (`AssignmentProposalCancelled { autoCleared: true }`).
    pub(crate) replaced_prior: bool,
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
            "ready_at": o.ready_at,
            "replaced_prior": o.replaced_prior,
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
    if let Some(ready_at) = o.ready_at {
        writeln!(w, "ready_at={ready_at}")?;
    }
    if o.replaced_prior {
        writeln!(w, "warning=replaced_existing_pending_proposal")?;
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
        // Moved in (not cloned): the dry-run path never submits, so it does no
        // extra allocation; the submit path clones exactly once below.
        operators,
        tx: None,
        ready_at: None,
        replaced_prior: false,
    };

    if !args.chain.dry_run {
        preflight_chain_id(&resolved.rpc_url, resolved.chain_id).await?;
        let signer = chain_ctx::load_signer_with_password_file(
            args.chain.keystore_password_file.as_deref(),
            &resolved.keystore,
        )
        .await?;
        let provider = decdn_client_pull::provider::build_provider(&resolved.rpc_url, &signer)?;
        outcome.operator = Some(signer.address());
        let contract = OriginAssignment::new(oa_addr, &provider);
        let pending = contract
            .proposeAssignment(U256::from(args.namespace), outcome.operators.clone())
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

        // Surface the timelock deadline (`readyAt`), matching the honest
        // decode in `namespace_create`: match by signature, then decode.
        let proposed = receipt
            .inner
            .logs()
            .iter()
            .find(|log| log.topic0() == Some(&OriginAssignment::AssignmentProposed::SIGNATURE_HASH))
            .context(
                "proposeAssignment succeeded but its receipt carried no AssignmentProposed log \
                 (the timelock deadline could not be recovered; check the tx on a block explorer)",
            )?
            .log_decode::<OriginAssignment::AssignmentProposed>()
            .context(
                "proposeAssignment emitted an AssignmentProposed log that failed to decode (ABI \
                 mismatch between this CLI and the deployed OriginAssignment?)",
            )?;
        outcome.ready_at = Some(
            u64::try_from(proposed.inner.data.readyAt)
                .context("assignment readyAt exceeds u64 (unexpected on this chain)")?,
        );

        // If a prior pending proposal was silently replaced, the contract emits
        // `AssignmentProposalCancelled { autoCleared: true }` in the same tx.
        // No such log is the normal case (nothing was replaced); but a log whose
        // topic matches the signature yet fails to decode is ABI drift, surfaced
        // loudly — matching the honest decode of the two events above rather than
        // silently dropping it and under-reporting the overwrite.
        for log in receipt.inner.logs() {
            if log.topic0() != Some(&OriginAssignment::AssignmentProposalCancelled::SIGNATURE_HASH)
            {
                continue;
            }
            let cancelled = log
                .log_decode::<OriginAssignment::AssignmentProposalCancelled>()
                .context(
                    "proposeAssignment emitted an AssignmentProposalCancelled log that failed to \
                     decode (ABI mismatch between this CLI and the deployed OriginAssignment?)",
                )?;
            if cancelled.inner.data.autoCleared {
                outcome.replaced_prior = true;
            }
        }
    }

    let mut out = io::stdout().lock();
    write_assign_outcome(&mut out, &outcome, args.chain.json)
        .context("failed to write assign output")?;
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
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
            ready_at: Some(1_700_000_000),
            replaced_prior: true,
        };
        let mut buf = Vec::new();
        write_assign_outcome(&mut buf, &o, false).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("operators=2"), "{s}");
        assert!(s.contains("status=proposed_pending_dao"), "{s}");
        assert!(s.contains("tx=0x5555"), "{s}");
        assert!(s.contains("ready_at=1700000000"), "{s}");
        assert!(
            s.contains("warning=replaced_existing_pending_proposal"),
            "{s}"
        );
    }

    #[test]
    fn assign_dry_run_omits_operator_readyat_and_warning() {
        let o = AssignOutcome {
            operator: None,
            origin_assignment: Address::repeat_byte(0x02),
            namespace_id: 7,
            operators: vec![Address::repeat_byte(0x11)],
            tx: None,
            ready_at: None,
            replaced_prior: false,
        };
        let mut buf = Vec::new();
        write_assign_outcome(&mut buf, &o, false).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("status=dry_run submitted=false"), "{s}");
        // The signer `operator=` line sits at column 0; the operator-set lines
        // are indented (`  operator=`). Only the signer line must be absent.
        assert!(!s.lines().any(|l| l.starts_with("operator=")), "{s}");
        assert!(!s.contains("ready_at="), "{s}");
        assert!(!s.contains("warning="), "{s}");
    }

    // JSON writers are the machine-consumable contract — round-trip each so a
    // renamed key or wrong-shaped value fails loudly.
    #[test]
    fn namespace_json_round_trips() {
        let dry = NamespaceOutcome {
            operator: None,
            registry: Address::repeat_byte(0x01),
            namespace_id: None,
            tx: None,
        };
        let mut buf = Vec::new();
        write_namespace_outcome(&mut buf, &dry, true).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["submitted"], serde_json::json!(false));
        assert!(v["tx"].is_null(), "{v}");
        assert!(v["operator"].is_null(), "{v}");
        assert!(v["namespace_id"].is_null(), "{v}");

        let done = NamespaceOutcome {
            operator: Some(Address::repeat_byte(0xCD)),
            registry: Address::repeat_byte(0x01),
            namespace_id: Some(9),
            tx: Some(B256::repeat_byte(0x55)),
        };
        let mut buf = Vec::new();
        write_namespace_outcome(&mut buf, &done, true).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["submitted"], serde_json::json!(true));
        assert_eq!(v["namespace_id"], serde_json::json!(9)); // number, not string
        assert_eq!(
            v["operator"],
            serde_json::json!(format!("{:#x}", done.operator.unwrap()))
        );
    }

    #[test]
    fn claim_json_round_trips() {
        let o = ClaimOutcome {
            operator: Some(Address::repeat_byte(0xCD)),
            registry: Address::repeat_byte(0x01),
            namespace_id: 7,
            hash: [0xAB; 32],
            tx: Some(B256::repeat_byte(0x55)),
        };
        let mut buf = Vec::new();
        write_claim_outcome(&mut buf, &o, true).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["submitted"], serde_json::json!(true));
        assert_eq!(v["namespace_id"], serde_json::json!(7));
        assert_eq!(
            v["hash"],
            serde_json::json!(format!("0x{}", "ab".repeat(32)))
        );
    }

    #[test]
    fn assign_json_round_trips() {
        let o = AssignOutcome {
            operator: Some(Address::repeat_byte(0xCD)),
            origin_assignment: Address::repeat_byte(0x02),
            namespace_id: 7,
            operators: vec![Address::repeat_byte(0x11), Address::repeat_byte(0x22)],
            tx: Some(B256::repeat_byte(0x55)),
            ready_at: Some(1_700_000_000),
            replaced_prior: true,
        };
        let mut buf = Vec::new();
        write_assign_outcome(&mut buf, &o, true).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["status"], serde_json::json!("proposed_pending_dao"));
        assert_eq!(v["operators"].as_array().unwrap().len(), 2);
        assert_eq!(v["ready_at"], serde_json::json!(1_700_000_000));
        assert_eq!(v["replaced_prior"], serde_json::json!(true));
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
        assert!(chain_id_guard(421_614, 421_614).is_ok());
        let err = chain_id_guard(421_614, 31_337).unwrap_err().to_string();
        assert!(err.contains("421614"), "{err}");
        assert!(err.contains("31337"), "{err}");
    }
}
