//! `decdn node update-region` — (re)attest the operator's region on-chain
//! (ADR 030).
//!
//! An operator who ran `decdn node register` with the wrong `--region`, or whose
//! node has physically relocated, needs to correct the on-chain `regionHint`
//! every downstream reader (`getRegisteredNodes`, off-chain DHT locality
//! ranking, ADR 030 blacklist-scope ripening) keys on. `register` cannot fix it
//! (a second call reverts `NodeAlreadyRegistered`), and the only other route is a
//! hand-encoded `cast send … updateRegion(string)`. This command is the
//! one-command fix: it validates the code as ISO 3166-1 alpha-2, normalizes it,
//! and submits `CapacityBond.updateRegion` from the operator Ethereum key.
//!
//! The region is validated CLI-side because the contract only length-checks the
//! string — an unvalidated code would be stored verbatim and read back as "no
//! locality information" by every consumer that parses it with
//! [`Region::parse`]. The value fully REPLACES the on-chain `regionHint`.
//!
//! Like `decdn node update-multiaddrs`, the contract's guardrails are read and
//! checked before the send so they surface as named errors rather than a bare
//! `execution reverted`: `NodeNotActive` (the operator must be registered) and
//! the `regionStabilityWindow` cooldown between updates (`RegionCooldownActive`).
//! The cooldown is compared against the head block timestamp — the same clock the
//! contract uses — so the pre-check and the on-chain check agree. (The
//! `MAX_REGION_HINT_BYTES` length ceiling needs no pre-check: a validated
//! two-letter code never nears it.)

use std::io;
use std::path::Path;

use alloy::primitives::{Address, B256};
use alloy::providers::Provider;
use anyhow::Context;
use decdn_common::cli;
use decdn_incentive::capacity_bond::CapacityBond;
use decdn_protocol::Region;

use crate::commands::chain_ctx;

/// Entry point for `decdn node update-region`.
pub async fn run(args: &cli::UpdateRegionArgs, global_config: Option<&Path>) -> anyhow::Result<()> {
    let config_path = args.chain.common.config.as_deref().or(global_config);
    let file = chain_ctx::load_optional_config(config_path)?;
    let resolved = chain_ctx::resolve(&args.chain, &file)?;
    let cb_addr = resolved.capacity_bond_address;

    // Validate + normalize before any keystore decrypt or chain read: an invalid
    // code is a local error, and the contract only length-checks, so a garbage
    // string would be stored verbatim and read back as "no locality" by every
    // downstream consumer.
    let region = validate_region(&args.region)?;
    let new_region = region.as_str().to_string();

    let signer = chain_ctx::load_operator_signer(&args.chain.common, &resolved.keystore).await?;
    let operator = signer.address();
    let provider = decdn_client::provider::build_provider(&resolved.rpc_url, &signer)?;
    let bond = CapacityBond::new(cb_addr, &provider);

    let plan = build_plan(&bond, operator, cb_addr, new_region.clone()).await?;

    let json = args.chain.common.json;
    if args.chain.common.dry_run {
        let mut out = io::stdout().lock();
        write_plan(&mut out, &plan, json, None, true).context("failed to write dry-run output")?;
        return Ok(());
    }

    // Pre-flight rather than a bare revert: name which guardrail would trip and
    // report its exact chain-read bound. The check order mirrors the contract's
    // (active → cooldown) so the first failing gate is the one reported.
    ensure_submittable(&plan)?;

    // Caller-owned slot (#1355): from the send onward the transaction is
    // broadcast and may take effect, so an unreadable receipt must still print
    // the hash rather than read as "nothing happened".
    let mut tx = None;
    let result = chain_ctx::send(
        bond.updateRegion(new_region),
        "updateRegion",
        Some("the node must be active and the region-stability cooldown must have elapsed"),
        &mut tx,
    )
    .await;

    let mut out = io::stdout().lock();
    write_plan(&mut out, &plan, json, tx.as_ref(), false).context("failed to write result")?;
    drop(out);

    result.map(|_| ())
}

/// What `updateRegion` will attest and the guardrails it must clear, read from
/// the chain before anything is sent. Owned so [`write_plan`] and
/// [`ensure_submittable`] are testable without a chain.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Plan {
    /// The `CapacityBond` the transaction targets.
    pub(crate) capacity_bond: Address,
    /// The operator Ethereum address signing the update.
    pub(crate) operator: Address,
    /// The node id whose region is being (re)attested. Zero when the operator
    /// has no registration (which also makes `active` false).
    pub(crate) node_id: B256,
    /// Whether the operator is in the active set — the gate `updateRegion`
    /// enforces after the length check.
    pub(crate) active: bool,
    /// The operator's current on-chain region (`regionHint`); empty when never
    /// set.
    pub(crate) current_region: String,
    /// The normalized ISO 3166-1 alpha-2 code being attested.
    pub(crate) new_region: String,
    /// `regionLastChanged` timestamp (unix seconds); stamped at registration and
    /// on every change, so `0` only for a never-registered operator.
    pub(crate) last_changed: u64,
    /// Governable `regionStabilityWindow` (seconds) between updates.
    pub(crate) stability_window_secs: u64,
    /// Earliest timestamp a new update is accepted: `last_changed +
    /// stability_window_secs`.
    pub(crate) ready_at: u64,
    /// Head block timestamp — the clock the contract compares `ready_at`
    /// against, read here so the pre-check and the on-chain check agree.
    pub(crate) head_timestamp: u64,
}

/// Read the operator's registry state and region-attestation state, plus the
/// head block timestamp the cooldown is measured against.
async fn build_plan<P: Provider + Clone>(
    bond: &CapacityBond::CapacityBondInstance<P>,
    operator: Address,
    capacity_bond: Address,
    new_region: String,
) -> anyhow::Result<Plan> {
    let ctx = |what: &str| format!("failed to read {what} from CapacityBond at {capacity_bond}");

    let info = bond
        .getNodeByAddress(operator)
        .call()
        .await
        .with_context(|| ctx("getNodeByAddress"))?;
    let scope = bond
        .regionScopeData(operator)
        .call()
        .await
        .with_context(|| ctx("regionScopeData"))?;
    let head_timestamp = chain_ctx::head_timestamp(bond.provider()).await?;

    // Saturating rather than `try_into`: the stability window is a governance
    // parameter bounded far below `u64::MAX`, so a value past `u64` is
    // unreachable — and failing the whole command on an unreachable read would be
    // worse than clamping.
    let stability_window_secs: u64 = scope.regionStabilityWindow.saturating_to();
    let last_changed = scope.regionLastChanged;
    let ready_at = last_changed.saturating_add(stability_window_secs);

    Ok(Plan {
        capacity_bond,
        operator,
        node_id: info.nodeId,
        active: info.active,
        current_region: scope.regionHint,
        new_region,
        last_changed,
        stability_window_secs,
        ready_at,
        head_timestamp,
    })
}

/// Validate a region string as an ISO 3166-1 alpha-2 code and return the
/// normalized [`Region`]. Pure so the guard is testable without a chain; runs
/// before any keystore decrypt so an invalid code never reaches the wire. The
/// same allowlist [`decdn node register --region`] and the `--region` discovery
/// filter apply, so a code accepted here is one the network recognizes.
pub(crate) fn validate_region(raw: &str) -> anyhow::Result<Region> {
    Region::parse(raw).ok_or_else(|| {
        anyhow::anyhow!(
            "invalid --region {raw:?}: not an accepted ISO 3166-1 alpha-2 code (e.g. `DE`, `US`)",
        )
    })
}

/// Refuse a run the contract would revert, naming the guardrail and its exact
/// bound. Split from `run` so every branch is testable without a chain. The order
/// matches the contract's active → cooldown gates. (The `RegionHintTooLong`
/// length ceiling is not checked here: [`validate_region`] guarantees a two-byte
/// code, which never exceeds the ceiling.)
pub(crate) fn ensure_submittable(plan: &Plan) -> anyhow::Result<()> {
    anyhow::ensure!(
        plan.active,
        "this operator is not in the active node set — `updateRegion` would revert \
         NodeNotActive. A registered node re-enters with `decdn node register`. An ejected \
         operator cannot register (it would revert too) and instead exits the bond with \
         `decdn node unbond --all`."
    );
    if plan.head_timestamp < plan.ready_at {
        let wait = plan.ready_at.saturating_sub(plan.head_timestamp);
        anyhow::bail!(
            "the region-stability cooldown is active — `updateRegion` would revert \
             RegionCooldownActive. The region last changed at {} (unix); the next change is \
             accepted at {} (unix), ~{} s from the current head at {}.",
            plan.last_changed,
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
/// `dry_run` is carried explicitly rather than inferred from `tx.is_none()`, for
/// the reason `update_multiaddrs::write_plan` carries it: a real run whose send
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
            "current_region": p.current_region,
            "new_region": p.new_region,
            "last_changed": p.last_changed,
            "stability_window_secs": p.stability_window_secs,
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
    // Empty when the operator never had a region set; render a sentinel rather
    // than a blank value so the line is always present for scripts.
    let current = if p.current_region.is_empty() {
        "(unset)"
    } else {
        &p.current_region
    };
    writeln!(w, "current_region={current}")?;
    writeln!(w, "new_region={}", p.new_region)?;
    writeln!(w, "last_changed={}", p.last_changed)?;
    writeln!(w, "stability_window_secs={}", p.stability_window_secs)?;
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
