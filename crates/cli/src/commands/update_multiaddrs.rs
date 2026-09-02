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

    // Pack first: an oversized single entry (past the `uint16` length prefix)
    // is a local encoding error, caught before any keystore decrypt or chain
    // read. The total-size ceiling is a separate, chain-read guardrail below.
    let packed = node_register::pack_multiaddrs(&args.multiaddrs)?;

    let signer = chain_ctx::load_operator_signer(&args.chain.common, &resolved.keystore).await?;
    let operator = signer.address();
    let provider = decdn_client_pull::provider::build_provider(&resolved.rpc_url, &signer)?;
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
    let head_timestamp = head_timestamp(bond.provider()).await?;

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

/// Head block timestamp — the clock `updateMultiaddrs` compares its cooldown
/// against. Read from the chain rather than the local clock so the pre-check
/// uses the same clock the contract will (same rationale as `unbond`'s
/// `head_timestamp`).
async fn head_timestamp<P: Provider>(provider: &P) -> anyhow::Result<u64> {
    let block = provider
        .get_block(alloy::eips::BlockId::latest())
        .await
        .context("failed to read the latest block")?
        .ok_or_else(|| anyhow::anyhow!("no latest block"))?;
    Ok(block.header.timestamp)
}

/// Refuse a run the contract would revert, naming the guardrail and its exact
/// bound. Split from `run` so every branch is testable without a chain. The
/// order matches the contract: `NodeNotActive`, then `MultiaddrsTooLarge`, then
/// `MultiaddrCooldownActive`.
pub(crate) fn ensure_submittable(plan: &Plan) -> anyhow::Result<()> {
    anyhow::ensure!(
        plan.active,
        "this operator is not in the active node set — `updateMultiaddrs` would revert \
         NodeNotActive. Register first with `decdn node register` (or, if ejected, follow \
         the exit in `decdn node deregister`)."
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
mod tests {
    use super::*;

    /// A submittable plan: active, comfortably under the ceiling, cooldown
    /// elapsed. Individual tests knock out one field to exercise each gate.
    fn plan() -> Plan {
        Plan {
            capacity_bond: Address::repeat_byte(0xCB),
            operator: Address::repeat_byte(0x0E),
            node_id: B256::repeat_byte(0xAB),
            active: true,
            multiaddr_count: 1,
            packed_size: 36,
            max_multiaddr_size: 1_024,
            last_update: 1_000,
            cooldown_secs: 3_600,
            ready_at: 4_600,
            head_timestamp: 10_000,
        }
    }

    fn rendered(p: &Plan, json: bool, tx: Option<&B256>, dry_run: bool) -> String {
        let mut buf = Vec::new();
        write_plan(&mut buf, p, json, tx, dry_run).expect("write to a Vec cannot fail");
        String::from_utf8(buf).expect("output is ASCII")
    }

    #[test]
    fn submittable_plan_passes() {
        ensure_submittable(&plan()).expect("an active, in-bounds, cooled-down plan is submittable");
    }

    #[test]
    fn inactive_operator_is_pointed_at_register() {
        let mut p = plan();
        p.active = false;
        let err = ensure_submittable(&p).expect_err("inactive must not proceed");
        let msg = format!("{err}");
        assert!(msg.contains("NodeNotActive"), "names the revert: {msg}");
        assert!(msg.contains("node register"), "names re-entry: {msg}");
    }

    #[test]
    fn oversized_set_reports_size_and_ceiling() {
        let mut p = plan();
        p.packed_size = 2_048;
        p.max_multiaddr_size = 1_024;
        let err = ensure_submittable(&p).expect_err("over the ceiling must not proceed");
        let msg = format!("{err}");
        assert!(
            msg.contains("MultiaddrsTooLarge"),
            "names the revert: {msg}"
        );
        assert!(msg.contains("2048"), "reports the packed size: {msg}");
        assert!(msg.contains("1024"), "reports the ceiling: {msg}");
    }

    /// The packed size is allowed to hit the ceiling exactly — the contract's
    /// check is `> maxMultiaddrSize`, so equality must pass, not trip.
    #[test]
    fn packed_size_equal_to_ceiling_passes() {
        let mut p = plan();
        p.packed_size = 1_024;
        p.max_multiaddr_size = 1_024;
        ensure_submittable(&p).expect("size == ceiling is within bounds");
    }

    #[test]
    fn active_cooldown_reports_ready_at() {
        let mut p = plan();
        p.last_update = 9_000;
        p.cooldown_secs = 3_600;
        p.ready_at = 12_600;
        p.head_timestamp = 10_000; // before ready_at
        let err = ensure_submittable(&p).expect_err("within cooldown must not proceed");
        let msg = format!("{err}");
        assert!(
            msg.contains("MultiaddrCooldownActive"),
            "names the revert: {msg}"
        );
        assert!(msg.contains("12600"), "reports the ready-at time: {msg}");
        assert!(msg.contains("2600"), "reports the remaining wait: {msg}");
    }

    /// Cooldown boundary: the contract accepts `head == ready_at` (its check is
    /// `block.timestamp < readyAt`), so the pre-check must too.
    #[test]
    fn head_equal_to_ready_at_passes() {
        let mut p = plan();
        p.ready_at = 10_000;
        p.head_timestamp = 10_000;
        ensure_submittable(&p).expect("head == ready_at clears the cooldown");
    }

    #[test]
    fn dry_run_reports_no_tx_and_is_distinguishable_from_a_failed_send() {
        let dry = rendered(&plan(), false, None, true);
        assert!(dry.contains("update_tx=skipped"), "{dry}");
        assert!(dry.contains("submitted=false dry_run=true"), "{dry}");

        let failed = rendered(&plan(), false, None, false);
        assert!(
            failed.contains("submitted=false dry_run=false"),
            "a failed send is not a preview: {failed}"
        );
    }

    #[test]
    fn json_carries_the_plan_and_the_tx() {
        let tx = B256::repeat_byte(0xAB);
        let s = rendered(&plan(), true, Some(&tx), false);
        let v: serde_json::Value = serde_json::from_str(&s).expect("valid JSON");
        assert_eq!(
            v.get("submitted").and_then(serde_json::Value::as_bool),
            Some(true)
        );
        assert_eq!(
            v.get("multiaddrs").and_then(serde_json::Value::as_u64),
            Some(1)
        );
        assert_eq!(
            v.get("packed_bytes").and_then(serde_json::Value::as_u64),
            Some(36)
        );
        assert_eq!(
            v.get("max_multiaddr_size")
                .and_then(serde_json::Value::as_u64),
            Some(1_024)
        );
        assert_eq!(
            v.get("update_tx").and_then(serde_json::Value::as_str),
            Some(format!("{tx:#x}").as_str())
        );
    }

    #[test]
    fn json_null_tx_on_a_dry_run() {
        let s = rendered(&plan(), true, None, true);
        let v: serde_json::Value = serde_json::from_str(&s).expect("valid JSON");
        assert!(
            v.get("update_tx").is_some_and(serde_json::Value::is_null),
            "the key is present-and-null, not absent: {s}"
        );
        assert_eq!(
            v.get("dry_run").and_then(serde_json::Value::as_bool),
            Some(true)
        );
    }
}
