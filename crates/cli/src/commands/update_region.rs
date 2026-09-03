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
    let provider = decdn_client_pull::provider::build_provider(&resolved.rpc_url, &signer)?;
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
    let head_timestamp = head_timestamp(bond.provider()).await?;

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

/// Head block timestamp — the clock `updateRegion` compares its cooldown
/// against. Read from the chain rather than the local clock so the pre-check uses
/// the same clock the contract will (same rationale as `update-multiaddrs`'s
/// `head_timestamp`).
async fn head_timestamp<P: Provider>(provider: &P) -> anyhow::Result<u64> {
    let block = provider
        .get_block(alloy::eips::BlockId::latest())
        .await
        .context("failed to read the latest block")?
        .ok_or_else(|| anyhow::anyhow!("no latest block"))?;
    Ok(block.header.timestamp)
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
mod tests {
    use super::*;

    /// A submittable plan: active and cooldown elapsed. Individual tests knock
    /// out one field to exercise each gate.
    fn plan() -> Plan {
        Plan {
            capacity_bond: Address::repeat_byte(0xCB),
            operator: Address::repeat_byte(0x0E),
            node_id: B256::repeat_byte(0xAB),
            active: true,
            current_region: "US".to_string(),
            new_region: "DE".to_string(),
            last_changed: 1_000,
            stability_window_secs: 3_600,
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
    fn valid_region_is_accepted_and_normalized() {
        let region = validate_region("de").expect("a lowercase code is accepted");
        assert_eq!(region.as_str(), "DE", "the code is uppercased");
        let region = validate_region("  us  ").expect("surrounding whitespace is trimmed");
        assert_eq!(region.as_str(), "US");
    }

    /// The contract only length-checks, so a garbage code must be rejected
    /// CLI-side — the whole reason this command validates.
    #[test]
    fn garbage_region_is_rejected() {
        let err = validate_region("OO").expect_err("a non-allowlisted code must not proceed");
        assert!(
            format!("{err}").contains("ISO 3166-1 alpha-2"),
            "names the expected format: {err}"
        );
    }

    #[test]
    fn non_alpha2_region_is_rejected() {
        validate_region("USA").expect_err("a three-letter code is not alpha-2");
        validate_region("").expect_err("an empty code is not a region");
        validate_region("GLOBAL").expect_err("the GLOBAL sentinel is not an alpha-2 code");
    }

    #[test]
    fn submittable_plan_passes() {
        ensure_submittable(&plan()).expect("an active, cooled-down plan is submittable");
    }

    /// An ejected operator cannot register — `deregister` sends them to
    /// `unbond --all`, and so must this command, not to a `register` that reverts.
    #[test]
    fn inactive_message_routes_ejected_to_unbond_not_register_only() {
        let mut p = plan();
        p.active = false;
        let err = ensure_submittable(&p).expect_err("inactive must not proceed");
        let msg = format!("{err}");
        assert!(msg.contains("NodeNotActive"), "names the revert: {msg}");
        assert!(msg.contains("node register"), "names re-entry: {msg}");
        assert!(
            msg.contains("unbond --all"),
            "names the ejected exit: {msg}"
        );
    }

    #[test]
    fn active_cooldown_reports_ready_at() {
        let mut p = plan();
        p.last_changed = 9_000;
        p.stability_window_secs = 3_600;
        p.ready_at = 12_600;
        p.head_timestamp = 10_000; // before ready_at
        let err = ensure_submittable(&p).expect_err("within cooldown must not proceed");
        let msg = format!("{err}");
        assert!(
            msg.contains("RegionCooldownActive"),
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
        assert!(dry.contains("new_region=DE"), "{dry}");
        assert!(dry.contains("current_region=US"), "{dry}");

        let failed = rendered(&plan(), false, None, false);
        assert!(
            failed.contains("submitted=false dry_run=false"),
            "a failed send is not a preview: {failed}"
        );
    }

    /// An operator that never set a region reads back an empty `regionHint`;
    /// render a sentinel rather than a blank value so the line stays parseable.
    #[test]
    fn unset_current_region_renders_sentinel() {
        let mut p = plan();
        p.current_region = String::new();
        let s = rendered(&p, false, None, true);
        assert!(s.contains("current_region=(unset)"), "{s}");
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
            v.get("new_region").and_then(serde_json::Value::as_str),
            Some("DE")
        );
        assert_eq!(
            v.get("current_region").and_then(serde_json::Value::as_str),
            Some("US")
        );
        assert_eq!(
            v.get("stability_window_secs")
                .and_then(serde_json::Value::as_u64),
            Some(3_600)
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
