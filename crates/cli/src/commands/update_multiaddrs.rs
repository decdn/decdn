//! `decdn node update-multiaddrs` — (re)publish the operator's dialable QUIC
//! addresses on-chain (ADR 019 § Multiaddr encoding, #1908).
//!
//! An operator who ran `decdn node register` with no `--multiaddr` published an
//! empty set and is stranded on the iroh relay path: client→node transfers pin
//! at relay throughput and never upgrade to a direct QUIC connection, even when
//! the node is directly dialable. `register` cannot fix it (a second call
//! reverts `NodeAlreadyRegistered`), and the only other route is a hand-encoded
//! `cast send … updateMultiaddrs(bytes)` reproducing `pack_multiaddrs`'s framing
//! by hand. This command is the one-command fix: it packs the addresses with the
//! exact `node_register::pack_multiaddrs` encoding `register` uses and submits
//! `CapacityBond.updateMultiaddrs` from the operator Ethereum key.
//!
//! The set fully REPLACES what is on-chain — the contract stores the field
//! verbatim rather than merging.
//!
//! Like `decdn node deregister`, the contract's guardrails are read and checked
//! before the send so they surface as named errors rather than a bare
//! `execution reverted`: `NodeNotActive` (the operator must be registered), the
//! `maxMultiaddrSize` byte ceiling (`MultiaddrsTooLarge`), and the
//! `multiaddrUpdateCooldown` between updates (`MultiaddrCooldownActive`). The
//! cooldown is compared against the head block timestamp — the same clock the
//! contract uses — so the pre-check and the on-chain check agree.

use std::io;
use std::path::Path;

use alloy::primitives::{Address, B256, Bytes};
use alloy::providers::Provider;
use anyhow::Context;
use decdn_common::cli;
use decdn_incentive::capacity_bond::CapacityBond;
use decdn_incentive::node_register;

use crate::commands::chain_ctx;

/// Entry point for `decdn node update-multiaddrs`.
pub async fn run(
    args: &cli::UpdateMultiaddrsArgs,
    global_config: Option<&Path>,
) -> anyhow::Result<()> {
    let config_path = args.chain.common.config.as_deref().or(global_config);
    let file = chain_ctx::load_optional_config(config_path)?;
    let resolved = chain_ctx::resolve(&args.chain, &file)?;
    let cb_addr = resolved.capacity_bond_address;

    // Reject empty / whitespace-only entries before packing. `--multiaddr` is
    // required, but clap accepts `--multiaddr ""`, and a blank string packs to a
    // zero-length entry — an on-chain address no peer can dial, which recreates
    // the exact relay-pinned footgun this command exists to fix.
    reject_blank_multiaddrs(&args.multiaddrs)?;

    // Pack next: an oversized single entry (past the `uint16` length prefix)
    // is a local encoding error, caught before any keystore decrypt or chain
    // read. The total-size ceiling is a separate, chain-read guardrail below.
    let packed = node_register::pack_multiaddrs(&args.multiaddrs)?;

    let signer = chain_ctx::load_operator_signer(&args.chain.common, &resolved.keystore).await?;
    let operator = signer.address();
    let provider = decdn_client::provider::build_provider(&resolved.rpc_url, &signer)?;
    let bond = CapacityBond::new(cb_addr, &provider);

    let plan = build_plan(&bond, operator, cb_addr, args.multiaddrs.len(), &packed).await?;

    let json = args.chain.common.json;
    if args.chain.common.dry_run {
        let mut out = io::stdout().lock();
        write_plan(&mut out, &plan, json, None, true).context("failed to write dry-run output")?;
        return Ok(());
    }

    // Pre-flight rather than a bare revert: name which guardrail would trip and
    // report its exact chain-read bound. The check order mirrors the contract's
    // (active → size → cooldown) so the first failing gate is the one reported.
    ensure_submittable(&plan)?;

    // Caller-owned slot (#1355): from the send onward the transaction is
    // broadcast and may take effect, so an unreadable receipt must still print
    // the hash rather than read as "nothing happened".
    let mut tx = None;
    let result = chain_ctx::send(
        bond.updateMultiaddrs(Bytes::from(packed)),
        "updateMultiaddrs",
        Some(
            "the node must be active and within the maxMultiaddrSize ceiling, and the \
             multiaddr-update cooldown must have elapsed",
        ),
        &mut tx,
    )
    .await;

    let mut out = io::stdout().lock();
    write_plan(&mut out, &plan, json, tx.as_ref(), false).context("failed to write result")?;
    drop(out);

    result.map(|_| ())
}

/// What `updateMultiaddrs` will publish and the guardrails it must clear, read
/// from the chain before anything is sent. Owned so [`write_plan`] and
/// [`ensure_submittable`] are testable without a chain.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Plan {
    /// The `CapacityBond` the transaction targets.
    pub(crate) capacity_bond: Address,
    /// The operator Ethereum address signing the update.
    pub(crate) operator: Address,
    /// The node id whose addresses are being (re)published. Zero when the
    /// operator has no registration (which also makes `active` false).
    pub(crate) node_id: B256,
    /// Whether the operator is in the active set — the gate `updateMultiaddrs`
    /// enforces first.
    pub(crate) active: bool,
    /// Number of multiaddr strings being published.
    pub(crate) multiaddr_count: usize,
    /// Packed byte length of the `multiaddrs` field the transaction carries.
    pub(crate) packed_size: u64,
    /// Governable `maxMultiaddrSize` ceiling (bytes) the packed size must not
    /// exceed.
    pub(crate) max_multiaddr_size: u64,
    /// `lastMultiaddrUpdate` timestamp (unix seconds); `0` when never updated.
    pub(crate) last_update: u64,
    /// Governable `multiaddrUpdateCooldown` (seconds) between updates.
    pub(crate) cooldown_secs: u64,
    /// Earliest timestamp a new update is accepted: `last_update +
    /// cooldown_secs`.
    pub(crate) ready_at: u64,
    /// Head block timestamp — the clock the contract compares `ready_at`
    /// against, read here so the pre-check and the on-chain check agree.
    pub(crate) head_timestamp: u64,
}

/// Read the operator's registry state and the two governable guardrails, plus
/// the head block timestamp the cooldown is measured against.
async fn build_plan<P: Provider + Clone>(
    bond: &CapacityBond::CapacityBondInstance<P>,
    operator: Address,
    capacity_bond: Address,
    multiaddr_count: usize,
    packed: &[u8],
) -> anyhow::Result<Plan> {
    let ctx = |what: &str| format!("failed to read {what} from CapacityBond at {capacity_bond}");

    let info = bond
        .getNodeByAddress(operator)
        .call()
        .await
        .with_context(|| ctx("getNodeByAddress"))?;
    let max_multiaddr_size = bond
        .maxMultiaddrSize()
        .call()
        .await
        .with_context(|| ctx("maxMultiaddrSize"))?;
    let cooldown = bond
        .multiaddrUpdateCooldown()
        .call()
        .await
        .with_context(|| ctx("multiaddrUpdateCooldown"))?;
    let head_timestamp = chain_ctx::head_timestamp(bond.provider()).await?;

    // Saturating rather than `try_into`: both are governance parameters bounded
    // far below `u64::MAX`, so a value past `u64` is unreachable — and failing
    // the whole command on an unreachable read would be worse than clamping.
    let max_multiaddr_size: u64 = max_multiaddr_size.saturating_to();
    let cooldown_secs: u64 = cooldown.saturating_to();
    let ready_at = info.lastMultiaddrUpdate.saturating_add(cooldown_secs);
    // Packed length is a handful of KB in practice; `u64::MAX` on the
    // impossible overflow keeps the ceiling check erring toward "too large".
    let packed_size = u64::try_from(packed.len()).unwrap_or(u64::MAX);

    Ok(Plan {
        capacity_bond,
        operator,
        node_id: info.nodeId,
        active: info.active,
        multiaddr_count,
        packed_size,
        max_multiaddr_size,
        last_update: info.lastMultiaddrUpdate,
        cooldown_secs,
        ready_at,
        head_timestamp,
    })
}

/// Reject empty or whitespace-only multiaddrs. clap enforces "at least one
/// `--multiaddr`", but not that each is non-blank: `--multiaddr ""` (or `"  "`)
/// packs to a zero-length on-chain entry no peer can dial — the relay-pinned
/// footgun this command fixes, re-created. Pure so the guard is testable without
/// a chain; runs before packing so nothing blank ever reaches the wire.
pub(crate) fn reject_blank_multiaddrs(multiaddrs: &[String]) -> anyhow::Result<()> {
    for (i, ma) in multiaddrs.iter().enumerate() {
        anyhow::ensure!(
            !ma.trim().is_empty(),
            "--multiaddr #{} is empty or whitespace — publish a real QUIC address like \
             `/ip4/203.0.113.10/udp/4433/quic-v1`, or omit it (at least one non-blank \
             address is required).",
            i + 1,
        );
    }
    Ok(())
}

/// Refuse a run the contract would revert, naming the guardrail and its exact
/// bound. Split from `run` so every branch is testable without a chain. The
/// order matches the contract: `NodeNotActive`, then `MultiaddrsTooLarge`, then
/// `MultiaddrCooldownActive`.
pub(crate) fn ensure_submittable(plan: &Plan) -> anyhow::Result<()> {
    anyhow::ensure!(
        plan.active,
        "this operator is not in the active node set — `updateMultiaddrs` would revert \
         NodeNotActive. A registered node re-enters with `decdn node register`. An ejected \
         operator cannot register (it would revert too) and instead exits the bond with \
         `decdn node unbond --all`."
    );
    anyhow::ensure!(
        plan.packed_size <= plan.max_multiaddr_size,
        "packed multiaddrs are {} bytes, over the maxMultiaddrSize ceiling of {} bytes — \
         `updateMultiaddrs` would revert MultiaddrsTooLarge. Publish fewer or shorter \
         addresses.",
        plan.packed_size,
        plan.max_multiaddr_size,
    );
    if plan.head_timestamp < plan.ready_at {
        let wait = plan.ready_at.saturating_sub(plan.head_timestamp);
        anyhow::bail!(
            "the multiaddr-update cooldown is active — `updateMultiaddrs` would revert \
             MultiaddrCooldownActive. Last update was at {} (unix); the next is accepted at \
             {} (unix), ~{} s from the current head at {}.",
            plan.last_update,
            plan.ready_at,
            wait,
            plan.head_timestamp,
        );
    }
    Ok(())
}

/// Write the plan + outcome as JSON or grep-friendly `key=value` lines. Pure
/// (`&mut impl Write`) so the output shape is unit-testable without a chain.
///
/// `dry_run` is carried explicitly rather than inferred from `tx.is_none()`,
/// for the reason `deregister::write_plan` carries it: a real run whose send
/// failed also has no tx, so the absence alone cannot mean "preview".
pub(crate) fn write_plan(
    w: &mut impl io::Write,
    p: &Plan,
    json: bool,
    tx: Option<&B256>,
    dry_run: bool,
) -> io::Result<()> {
    let tx_hex = tx.map(|v| format!("{v:#x}"));
    if json {
        let value = serde_json::json!({
            "submitted": tx.is_some(),
            "dry_run": dry_run,
            "capacity_bond": format!("{:#x}", p.capacity_bond),
            "operator": format!("{:#x}", p.operator),
            "node_id": format!("{:#x}", p.node_id),
            "active": p.active,
            "multiaddrs": p.multiaddr_count,
            "packed_bytes": p.packed_size,
            "max_multiaddr_size": p.max_multiaddr_size,
            "last_update": p.last_update,
            "cooldown_secs": p.cooldown_secs,
            "ready_at": p.ready_at,
            "head_timestamp": p.head_timestamp,
            "update_tx": tx_hex,
        });
        return writeln!(w, "{value}");
    }
    writeln!(w, "capacity_bond={:#x}", p.capacity_bond)?;
    writeln!(w, "operator={:#x}", p.operator)?;
    writeln!(w, "node_id={:#x}", p.node_id)?;
    writeln!(w, "active={}", p.active)?;
    writeln!(w, "multiaddrs={}", p.multiaddr_count)?;
    writeln!(w, "packed_bytes={}", p.packed_size)?;
    writeln!(w, "max_multiaddr_size={}", p.max_multiaddr_size)?;
    writeln!(w, "last_update={}", p.last_update)?;
    writeln!(w, "cooldown_secs={}", p.cooldown_secs)?;
    writeln!(w, "ready_at={}", p.ready_at)?;
    writeln!(w, "head_timestamp={}", p.head_timestamp)?;
    match tx_hex {
        Some(h) => writeln!(w, "update_tx={h}")?,
        None => writeln!(w, "update_tx=skipped")?,
    }
    writeln!(w, "submitted={} dry_run={dry_run}", tx.is_some())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests;
