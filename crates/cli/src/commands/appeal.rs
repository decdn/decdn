//! `decdn appeal slash` — file a slash appeal and post the appeal bond
//! (ADR 028 § Slashing Appeals).
//!
//! Operator-facing entry to the appeal state machine: read the governable
//! `appealBond` from `SlashAppeal`, approve that TOKEN to the contract if the
//! allowance is short, then `openSlashAppeal(slashId, evidenceBundleHash)`. The
//! contract enforces the operator-only, 30-day-window, one-appeal-per-slash and
//! 365-day frequency-cap rules; this command surfaces those reverts verbatim.
//!
//! The role-gated fast-track / grant / uphold actions are governance/multisig
//! operations and are deliberately not exposed by the CLI.

use std::io;
use std::path::Path;

use alloy::primitives::{Address, B256, U256};
use alloy::providers::Provider;
use anyhow::Context;
use decdn_common::cli;
use decdn_incentive::Erc20;
use decdn_incentive::slash_appeal::SlashAppeal;

use crate::commands::chain_ctx;

/// Entry point for `decdn appeal slash`.
pub async fn run(args: &cli::AppealSlashArgs, global_config: Option<&Path>) -> anyhow::Result<()> {
    let config_path = args.chain.common.config.as_deref().or(global_config);
    let file = chain_ctx::load_optional_config(config_path)?;
    let resolved =
        chain_ctx::resolve_appeal(&args.chain, args.slash_appeal_address.as_deref(), &file)?;
    let sa_addr = chain_ctx::parse_address(&resolved.slash_appeal_address, "slash_appeal_address")?;
    let slash_id = parse_slash_id(&args.slash_id)?;
    let evidence = parse_bytes32(&args.evidence_bundle_hash)?;

    let signer = chain_ctx::load_operator_signer(&args.chain, &resolved.keystore).await?;
    let operator = signer.address();
    let provider = decdn_client_pull::provider::build_provider(&resolved.rpc_url, &signer)?;
    let appeal = SlashAppeal::new(sa_addr, &provider);

    let plan = build_plan(&appeal, operator, slash_id, evidence, sa_addr).await?;

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

    let outcome = execute(&appeal, &provider, &plan, operator, sa_addr).await?;

    let mut out = io::stdout().lock();
    write_plan(&mut out, &plan, args.chain.common.json, &outcome, false)
        .context("failed to write result")?;
    Ok(())
}

/// What `appeal slash` intends to do, computed from chain state.
pub(crate) struct Plan {
    pub(crate) slash_id: U256,
    pub(crate) slash_appeal: Address,
    pub(crate) token: Address,
    pub(crate) evidence: B256,
    /// `appealBond()` under the current governable parameter (base units).
    pub(crate) bond: U256,
    /// Whether an `approve` is needed (current allowance below the bond).
    pub(crate) needs_approve: bool,
}

/// Transaction hashes from a non-dry run; `None` for skipped steps.
#[derive(Default)]
pub(crate) struct Outcome {
    pub(crate) approve: Option<B256>,
    pub(crate) open: Option<B256>,
}

/// Read chain state and compute the appeal plan.
pub(crate) async fn build_plan<P: Provider + Clone>(
    appeal: &SlashAppeal::SlashAppealInstance<P>,
    operator: Address,
    slash_id: U256,
    evidence: B256,
    sa_addr: Address,
) -> anyhow::Result<Plan> {
    let ctx = || format!("failed to read SlashAppeal state at {sa_addr}");
    let bond = appeal.appealBond().call().await.with_context(ctx)?;
    let token = appeal.token().call().await.with_context(ctx)?;

    let allowance = Erc20::new(token, appeal.provider())
        .allowance(operator, sa_addr)
        .call()
        .await
        .with_context(|| format!("failed to read TOKEN allowance from {token}"))?;

    Ok(Plan {
        slash_id,
        slash_appeal: sa_addr,
        token,
        evidence,
        bond,
        needs_approve: allowance < bond,
    })
}

/// Submit the needed transactions: approve (if allowance short) → openSlashAppeal.
pub(crate) async fn execute<P: Provider + Clone>(
    appeal: &SlashAppeal::SlashAppealInstance<P>,
    provider: P,
    plan: &Plan,
    operator: Address,
    sa_addr: Address,
) -> anyhow::Result<Outcome> {
    let mut outcome = Outcome::default();

    let token = Erc20::new(plan.token, provider);
    // Balance check before any send so a doomed run fails clean.
    let balance = token
        .balanceOf(operator)
        .call()
        .await
        .with_context(|| format!("failed to read TOKEN balance from {}", plan.token))?;
    anyhow::ensure!(
        balance >= plan.bond,
        "insufficient TOKEN: the appeal bond is {} base units, hold {}",
        plan.bond,
        balance,
    );

    if plan.needs_approve {
        let pending = token
            .approve(sa_addr, plan.bond)
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

    let pending = appeal
        .openSlashAppeal(plan.slash_id, plan.evidence)
        .send()
        .await
        .context("openSlashAppeal transaction failed to send")?;
    let receipt = pending
        .get_receipt()
        .await
        .context("openSlashAppeal sent but the receipt could not be fetched")?;
    anyhow::ensure!(
        receipt.status(),
        "openSlashAppeal reverted (tx {}); is the caller the slashed operator, within the \
         30-day window, with no existing appeal and no active 365-day cap?",
        receipt.transaction_hash,
    );
    outcome.open = Some(receipt.transaction_hash);

    Ok(outcome)
}

/// Parse a `uint256` slash id from a decimal string. Kept as `U256` (not
/// `u64`) because the on-chain `slashId` can exceed `u64::MAX`.
pub(crate) fn parse_slash_id(s: &str) -> anyhow::Result<U256> {
    s.parse::<U256>()
        .map_err(|e| anyhow::anyhow!("invalid slash id {s:?}: expected a uint256 decimal: {e}"))
}

/// Parse a 0x-prefixed 32-byte hex string into a `B256` (the evidence bundle
/// hash). Rejects the wrong length up front with a labelled error.
pub(crate) fn parse_bytes32(s: &str) -> anyhow::Result<B256> {
    s.parse::<B256>().map_err(|e| {
        anyhow::anyhow!(
            "invalid evidence bundle hash {s:?}: expected a 0x-prefixed 32-byte (64 hex char) \
             value: {e}"
        )
    })
}

/// Write the plan + outcome as JSON or grep-friendly `key=value` lines. Pure
/// (`&mut impl Write`) so the output shape is unit-testable without a chain.
pub(crate) fn write_plan(
    w: &mut impl io::Write,
    p: &Plan,
    json: bool,
    o: &Outcome,
    dry_run: bool,
) -> io::Result<()> {
    let submitted = o.approve.is_some() || o.open.is_some();
    let tx_hex = |h: Option<&B256>| h.map(|v| format!("{v:#x}"));
    if json {
        let value = serde_json::json!({
            "submitted": submitted,
            "dry_run": dry_run,
            "slash_id": p.slash_id.to_string(),
            "slash_appeal": format!("{:#x}", p.slash_appeal),
            "token": format!("{:#x}", p.token),
            "evidence_bundle_hash": format!("{:#x}", p.evidence),
            "appeal_bond_base": p.bond.to_string(),
            "needs_approve": p.needs_approve,
            "approve_tx": tx_hex(o.approve.as_ref()),
            "open_tx": tx_hex(o.open.as_ref()),
        });
        return writeln!(w, "{value}");
    }
    writeln!(w, "slash_id={}", p.slash_id)?;
    writeln!(w, "slash_appeal={:#x}", p.slash_appeal)?;
    writeln!(w, "token={:#x}", p.token)?;
    writeln!(w, "evidence_bundle_hash={:#x}", p.evidence)?;
    writeln!(w, "appeal_bond_base={}", p.bond)?;
    writeln!(w, "needs_approve={}", p.needs_approve)?;
    write_tx_line(w, "approve_tx", o.approve.as_ref())?;
    write_tx_line(w, "open_tx", o.open.as_ref())?;
    writeln!(w, "submitted={submitted} dry_run={dry_run}")
}

/// One `<key>=<tx|skipped>` line. A skipped step (allowance already sufficient)
/// prints `skipped`.
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

    fn plan(needs_approve: bool) -> Plan {
        Plan {
            slash_id: U256::from(7u64),
            slash_appeal: Address::repeat_byte(0x11),
            token: Address::repeat_byte(0x22),
            evidence: B256::repeat_byte(0xAB),
            bond: U256::from(1000u64),
            needs_approve,
        }
    }

    #[test]
    fn parse_bytes32_accepts_prefixed_32_bytes() {
        let h = parse_bytes32("0x00000000000000000000000000000000000000000000000000000000000000ab")
            .unwrap();
        assert_eq!(h, B256::with_last_byte(0xab));
    }

    #[test]
    fn parse_bytes32_rejects_wrong_length() {
        assert!(parse_bytes32("0xabcd").is_err());
    }

    #[test]
    fn parse_slash_id_accepts_values_above_u64_max() {
        // u64::MAX + 1 — must not overflow (the whole point of U256).
        let big = "18446744073709551616";
        assert_eq!(
            parse_slash_id(big).unwrap(),
            U256::from(u64::MAX) + U256::from(1u64)
        );
        assert!(parse_slash_id("not-a-number").is_err());
    }

    #[test]
    fn dry_run_reports_dry_run_true() {
        let p = plan(true);
        let mut buf = Vec::new();
        write_plan(&mut buf, &p, false, &Outcome::default(), true).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("submitted=false dry_run=true"), "{s}");
        assert!(s.contains("appeal_bond_base=1000"), "{s}");
        assert!(s.contains("open_tx=skipped"), "{s}");
    }

    #[test]
    fn json_output_carries_fields() {
        let p = plan(false);
        let mut buf = Vec::new();
        write_plan(&mut buf, &p, true, &Outcome::default(), true).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        // slash_id is emitted as a decimal string (uint256 can exceed u64).
        assert_eq!(
            v.get("slash_id").and_then(serde_json::Value::as_str),
            Some("7")
        );
        assert_eq!(
            v.get("needs_approve").and_then(serde_json::Value::as_bool),
            Some(false)
        );
        assert_eq!(
            v.get("dry_run").and_then(serde_json::Value::as_bool),
            Some(true)
        );
        assert!(v.get("approve_tx").is_some_and(serde_json::Value::is_null));
    }
}
