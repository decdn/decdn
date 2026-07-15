//! `decdn node bond` — stake to a capacity tier (ADR 019 § Step 2.1–2.2).
//!
//! Idempotent "stake to capacity tier N". The operator passes only `--mbps`;
//! the TOKEN amount is read from the on-chain `bondRequired` curve. The
//! command tops the active bond up to `max(minBond, bondRequired(mbps))`,
//! approving and bonding only the shortfall, then `declareMbps(mbps)`. Bonding
//! the shortfall (rather than a flat amount) is what makes a re-run after a
//! partial failure converge instead of over-bonding. A precondition for
//! `decdn node register`.

use std::io;
use std::path::Path;

use alloy::primitives::{Address, B256, U256};
use alloy::providers::Provider;
use anyhow::Context;
use decdn_common::cli;
use decdn_incentive::Erc20;
use decdn_incentive::capacity_bond::CapacityBond;

use crate::commands::chain_ctx;

/// Entry point for `decdn node bond`.
pub async fn run(args: &cli::BondArgs, global_config: Option<&Path>) -> anyhow::Result<()> {
    let config_path = args.chain.common.config.as_deref().or(global_config);
    let file = chain_ctx::load_optional_config(config_path)?;
    let resolved = chain_ctx::resolve(&args.chain, &file)?;
    let cb_addr = resolved.capacity_bond_address;

    let signer = chain_ctx::load_operator_signer(&args.chain.common, &resolved.keystore).await?;
    let operator = signer.address();
    let provider = decdn_client_pull::provider::build_provider(&resolved.rpc_url, &signer)?;
    let bond = CapacityBond::new(cb_addr, &provider);

    let mbps = U256::from(args.mbps);
    let plan = build_plan(&bond, operator, mbps, cb_addr).await?;

    if args.chain.common.dry_run {
        let mut out = io::stdout().lock();
        write_plan(
            &mut out,
            &plan,
            args.chain.common.json,
            &Outcome::default(),
            true,
        )
        .context("failed to write dry-run output")?;
        return Ok(());
    }

    let outcome = execute(&bond, &provider, &plan, operator, cb_addr, mbps).await?;

    let mut out = io::stdout().lock();
    write_plan(&mut out, &plan, args.chain.common.json, &outcome, false)
        .context("failed to write result")?;
    Ok(())
}

/// What `bond` intends to do, computed from chain state. `pub(crate)` so
/// `decdn setup` can reuse the same top-up plan for its pre-flight checks and
/// confirmation prompt without re-deriving the curve math (#933).
pub(crate) struct Plan {
    pub(crate) mbps: u64,
    pub(crate) token: Address,
    pub(crate) capacity_bond: Address,
    /// `bondRequired(mbps)` under the current curve (base units).
    pub(crate) required: U256,
    /// `max(minBond, required)` — what `registerNode` needs in place.
    pub(crate) target: U256,
    /// Active bond before this command.
    pub(crate) prior: U256,
    /// `target − prior` (saturating) — what we approve + bond.
    pub(crate) shortfall: U256,
    /// Whether `declareMbps` is needed (the operator isn't already at `mbps`).
    pub(crate) needs_declare: bool,
}

/// Transaction hashes from a non-dry run; `None` for steps that were skipped
/// (no shortfall, allowance already sufficient, tier already declared).
#[derive(Default)]
pub(crate) struct Outcome {
    pub(crate) approve: Option<B256>,
    pub(crate) bond: Option<B256>,
    pub(crate) declare: Option<B256>,
}

/// Read chain state and compute the top-up plan. Validates the tier against
/// the on-chain capacity band up front, so an out-of-band `--mbps` is rejected
/// before any bond is posted rather than stranding one behind a `declareMbps`
/// revert.
pub(crate) async fn build_plan<P: Provider + Clone>(
    bond: &CapacityBond::CapacityBondInstance<P>,
    operator: Address,
    mbps: U256,
    cb_addr: Address,
) -> anyhow::Result<Plan> {
    let ctx = || format!("failed to read CapacityBond state at {cb_addr}");
    let min_cap = bond.minCapacityMbps().call().await.with_context(ctx)?;
    let max_cap = bond.maxCapacityMbps().call().await.with_context(ctx)?;
    anyhow::ensure!(
        mbps >= min_cap && mbps <= max_cap,
        "declared capacity {mbps} Mbps is outside the on-chain band [{min_cap}, {max_cap}]; \
         declareMbps would revert",
    );

    let required = bond.bondRequired(mbps).call().await.with_context(ctx)?;
    let min_bond = bond.minBond().call().await.with_context(ctx)?;
    let prior = bond.activeBond(operator).call().await.with_context(ctx)?;
    let declared = bond.declaredMbps(operator).call().await.with_context(ctx)?;
    let token = bond.token().call().await.with_context(ctx)?;

    let target = min_bond.max(required);
    Ok(Plan {
        mbps: u64::try_from(mbps).unwrap_or(u64::MAX),
        token,
        capacity_bond: cb_addr,
        required,
        target,
        prior,
        shortfall: target.saturating_sub(prior),
        needs_declare: declared != mbps,
    })
}

/// Submit the needed transactions in order: approve (if allowance short) →
/// bond (if shortfall) → declareMbps (if not already at the tier). Each step
/// is independently skippable, which is what makes the command idempotent.
pub(crate) async fn execute<P: Provider + Clone>(
    bond: &CapacityBond::CapacityBondInstance<P>,
    provider: P,
    plan: &Plan,
    operator: Address,
    cb_addr: Address,
    mbps: U256,
) -> anyhow::Result<Outcome> {
    let mut outcome = Outcome::default();

    if plan.shortfall > U256::ZERO {
        let token = Erc20::new(plan.token, provider);
        // Balance check before any send so a doomed run fails clean.
        let balance = token
            .balanceOf(operator)
            .call()
            .await
            .with_context(|| format!("failed to read TOKEN balance from {}", plan.token))?;
        anyhow::ensure!(
            balance >= plan.shortfall,
            "insufficient TOKEN: need {} base units to reach the tier-{} bond, hold {}",
            plan.shortfall,
            plan.mbps,
            balance,
        );

        let allowance = token
            .allowance(operator, cb_addr)
            .call()
            .await
            .with_context(|| format!("failed to read TOKEN allowance from {}", plan.token))?;
        if allowance < plan.shortfall {
            let pending = token
                .approve(cb_addr, plan.shortfall)
                .send()
                .await
                .context("approve transaction failed to send")?;
            let receipt = pending
                .get_receipt()
                .await
                .context("approve sent but the receipt could not be fetched")?;
            anyhow::ensure!(
                receipt.status(),
                "approve reverted (tx {})",
                receipt.transaction_hash
            );
            outcome.approve = Some(receipt.transaction_hash);
        }

        let pending = bond
            .bond(plan.shortfall)
            .send()
            .await
            .context("bond transaction failed to send")?;
        let receipt = pending
            .get_receipt()
            .await
            .context("bond sent but the receipt could not be fetched")?;
        anyhow::ensure!(
            receipt.status(),
            "bond reverted (tx {})",
            receipt.transaction_hash
        );
        outcome.bond = Some(receipt.transaction_hash);
    }

    if plan.needs_declare {
        let pending = bond
            .declareMbps(mbps)
            .send()
            .await
            .context("declareMbps transaction failed to send")?;
        let receipt = pending
            .get_receipt()
            .await
            .context("declareMbps sent but the receipt could not be fetched")?;
        anyhow::ensure!(
            receipt.status(),
            "declareMbps reverted (tx {}); is {} Mbps within the capacity band?",
            receipt.transaction_hash,
            plan.mbps,
        );
        outcome.declare = Some(receipt.transaction_hash);
    }

    Ok(outcome)
}

/// Write the plan + outcome as JSON or grep-friendly `key=value` lines. Pure
/// (`&mut impl Write`) so the output shape is unit-testable without a chain.
/// `dry_run` distinguishes a `--dry-run` preview from a real run that was a
/// no-op (already at the target tier) — both submit nothing, but only the
/// former is a dry run.
pub(crate) fn write_plan(
    w: &mut impl io::Write,
    p: &Plan,
    json: bool,
    o: &Outcome,
    dry_run: bool,
) -> io::Result<()> {
    let submitted = o.approve.is_some() || o.bond.is_some() || o.declare.is_some();
    let tx_hex = |h: Option<&B256>| h.map(|v| format!("{v:#x}"));
    if json {
        let value = serde_json::json!({
            "submitted": submitted,
            "dry_run": dry_run,
            "mbps": p.mbps,
            "capacity_bond": format!("{:#x}", p.capacity_bond),
            "token": format!("{:#x}", p.token),
            "bond_required_base": p.required.to_string(),
            "target_bond_base": p.target.to_string(),
            "prior_bond_base": p.prior.to_string(),
            "bonded_base": p.shortfall.to_string(),
            "needs_declare": p.needs_declare,
            "approve_tx": tx_hex(o.approve.as_ref()),
            "bond_tx": tx_hex(o.bond.as_ref()),
            "declare_tx": tx_hex(o.declare.as_ref()),
        });
        return writeln!(w, "{value}");
    }
    // Base units: 1 TOKEN = 1e18 base units.
    writeln!(w, "mbps={}", p.mbps)?;
    writeln!(w, "capacity_bond={:#x}", p.capacity_bond)?;
    writeln!(w, "token={:#x}", p.token)?;
    writeln!(w, "bond_required_base={}", p.required)?;
    writeln!(w, "target_bond_base={}", p.target)?;
    writeln!(w, "prior_bond_base={}", p.prior)?;
    writeln!(w, "bonded_base={}", p.shortfall)?;
    writeln!(w, "needs_declare={}", p.needs_declare)?;
    write_tx_line(w, "approve_tx", o.approve.as_ref())?;
    write_tx_line(w, "bond_tx", o.bond.as_ref())?;
    write_tx_line(w, "declare_tx", o.declare.as_ref())?;
    // A real run already at the target reports `submitted=false dry_run=false`
    // (a genuine no-op), distinct from a `--dry-run` preview.
    writeln!(w, "submitted={submitted} dry_run={dry_run}")
}

/// One `<key>=<tx|skipped>` line. A skipped step (no shortfall, allowance
/// already sufficient, or tier already declared) prints `skipped` so an
/// operator can tell "no-op" from "failed to record".
fn write_tx_line(w: &mut impl io::Write, key: &str, tx: Option<&B256>) -> io::Result<()> {
    match tx {
        Some(h) => writeln!(w, "{key}={h:#x}"),
        None => writeln!(w, "{key}=skipped"),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn plan(prior: u64, required: u64, target: u64, needs_declare: bool) -> Plan {
        Plan {
            mbps: 1000,
            token: Address::repeat_byte(0x11),
            capacity_bond: Address::repeat_byte(0x22),
            required: U256::from(required),
            target: U256::from(target),
            prior: U256::from(prior),
            shortfall: U256::from(target).saturating_sub(U256::from(prior)),
            needs_declare,
        }
    }

    #[test]
    fn real_run_already_bonded_is_noop_not_dry_run() {
        // prior already at/above target, tier already declared → nothing to do.
        // A real run (dry_run=false) that submits nothing must report
        // `dry_run=false`, not masquerade as a dry-run preview (#934 review).
        let p = plan(60_000, 50_000, 60_000, false);
        assert_eq!(p.shortfall, U256::ZERO);
        let mut buf = Vec::new();
        write_plan(&mut buf, &p, false, &Outcome::default(), false).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("bonded_base=0"), "{s}");
        assert!(s.contains("needs_declare=false"), "{s}");
        assert!(s.contains("submitted=false dry_run=false"), "{s}");
        assert!(s.contains("bond_tx=skipped"), "{s}");
    }

    #[test]
    fn dry_run_reports_dry_run_true() {
        let p = plan(0, 50_000, 50_000, true);
        let mut buf = Vec::new();
        write_plan(&mut buf, &p, false, &Outcome::default(), true).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("submitted=false dry_run=true"), "{s}");
    }

    #[test]
    fn shortfall_is_target_minus_prior() {
        // target = max(minBond, required); here required dominates.
        let p = plan(10_000, 50_000, 50_000, true);
        assert_eq!(p.shortfall, U256::from(40_000u64));
    }

    #[test]
    fn target_floor_is_min_bond() {
        // required below minBond → target is the minBond floor, shortfall to it.
        let p = plan(0, 100, 200, true);
        assert_eq!(p.target, U256::from(200u64));
        assert_eq!(p.shortfall, U256::from(200u64));
    }
}
