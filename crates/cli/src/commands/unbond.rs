//! `decdn node unbond` — lower the bond (ADR 026 § Capacity-bond curve).
//!
//! The reverse of `decdn node bond`. Lowering a bond is a two-phase on-chain
//! operation — `requestUnbond(amount)` starts a `unbondingPeriod` window and
//! `unbond()` withdraws once it matures — with no atomic refund. Rather than
//! exposing both calls, this command reads chain state and picks the phase, so
//! a re-run after a partial failure converges the way `bond`'s top-up does.
//!
//! Two contract facts drive the shape of this command, and both are surfaced
//! to the operator rather than left to a revert:
//!
//! - A request in flight makes `isActive` false for the WHOLE window (it is a
//!   conjunct of the predicate, independent of the remaining bond). Starting
//!   one therefore takes the node out of the active set for ~14 days, which is
//!   why a terminal run confirms first.
//! - `requestUnbond`'s floor is `bondRequired(declaredMbps)` — the curve, NOT
//!   `minBond`. So the contract permits unbonding into an inactive state. The
//!   three amount flags pick deliberately different floors: `--to-mbps` keeps
//!   the operator eligible, `--all` goes to the contract's own floor.

use std::io::{self, IsTerminal, Write};
use std::path::Path;

use alloy::eips::BlockId;
use alloy::primitives::{Address, B256, U256};
use alloy::providers::Provider;
use anyhow::Context;
use decdn_common::cli;
use decdn_incentive::capacity_bond::CapacityBond;

use crate::commands::chain_ctx;

/// Entry point for `decdn node unbond`.
pub async fn run(args: &cli::UnbondArgs, global_config: Option<&Path>) -> anyhow::Result<()> {
    let config_path = args.chain.common.config.as_deref().or(global_config);
    let file = chain_ctx::load_optional_config(config_path)?;
    let resolved = chain_ctx::resolve(&args.chain, &file)?;
    let cb_addr = resolved.capacity_bond_address;

    let signer = chain_ctx::load_operator_signer(&args.chain.common, &resolved.keystore).await?;
    let operator = signer.address();
    let provider = decdn_client_pull::provider::build_provider(&resolved.rpc_url, &signer)?;
    let bond = CapacityBond::new(cb_addr, &provider);

    let request = AmountRequest::from_args(args)?;
    let plan = build_plan(&bond, &provider, operator, cb_addr, request).await?;

    if args.chain.common.dry_run {
        let mut out = io::stdout().lock();
        write_plan(&mut out, &plan, args.chain.common.json, &Outcome::default())
            .context("failed to write dry-run output")?;
        return Ok(());
    }

    // `Waiting` submits nothing: report and exit non-zero so a scripted retry
    // loop can tell "not yet" from "withdrawn".
    if let Action::Waiting { unlock_at, .. } = plan.action {
        let mut out = io::stdout().lock();
        write_plan(&mut out, &plan, args.chain.common.json, &Outcome::default())
            .context("failed to write result")?;
        anyhow::bail!(
            "unbonding request is still maturing: unlocks at {unlock_at} ({}); \
             re-run `decdn node unbond` then to withdraw",
            format_remaining(plan.remaining_secs()),
        );
    }

    confirm_or_bail(&plan, args.yes)?;

    let outcome = execute(&bond, &plan).await?;

    let mut out = io::stdout().lock();
    write_plan(&mut out, &plan, args.chain.common.json, &outcome)
        .context("failed to write result")?;
    Ok(())
}

/// How much the operator asked to release, normalised from the three mutually
/// exclusive flags. Clap enforces the exclusivity; this enforces that exactly
/// one is present.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AmountRequest {
    /// Reduce the declared tier to this many Mbps and release the surplus.
    ToMbps(u64),
    /// Release everything the curve permits.
    All,
    /// Release exactly this many base units.
    Exact(U256),
    /// No amount flag — only valid when a request is already in flight.
    Unspecified,
}

impl AmountRequest {
    fn from_args(args: &cli::UnbondArgs) -> anyhow::Result<Self> {
        match (args.to_mbps, args.all, args.amount) {
            (Some(mbps), false, None) => Ok(Self::ToMbps(mbps)),
            (None, true, None) => Ok(Self::All),
            (None, false, Some(amount)) => Ok(Self::Exact(U256::from(amount))),
            (None, false, None) => Ok(Self::Unspecified),
            // Clap's ArgGroup rejects multiple amount flags before we get here;
            // this arm exists so a future flag added outside the group can't
            // silently take one branch.
            _ => anyhow::bail!(
                "--to-mbps, --all and --amount are mutually exclusive; pass exactly one"
            ),
        }
    }
}

/// Which phase of the unbonding window this invocation is in, decided from
/// chain state rather than a flag.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Action {
    /// No request in flight: declare down (if needed), then `requestUnbond`.
    Request {
        /// Base units to release.
        release: U256,
        /// Bond retained afterwards.
        retained: U256,
        /// `declareMbps(mbps)` must land first, or `requestUnbond` reverts
        /// `BondBelowCurve` against the still-current tier.
        declare_to: Option<u64>,
    },
    /// A request exists but has not matured — nothing to submit.
    Waiting {
        amount: U256,
        unlock_at: u64,
        now: u64,
    },
    /// A matured request is withdrawable via `unbond()`.
    Withdraw { amount: U256 },
}

/// What `unbond` intends to do, computed from chain state.
pub(crate) struct Plan {
    pub(crate) capacity_bond: Address,
    pub(crate) action: Action,
    /// Active bond before this command.
    pub(crate) prior: U256,
    /// Currently declared capacity tier (Mbps).
    pub(crate) declared_mbps: u64,
    /// Governable unbonding window (seconds), for the unlock-time report.
    pub(crate) unbonding_period: u64,
    /// True when the post-request bond would sit below `minBond`, i.e. the
    /// node cannot return to the active set without re-bonding.
    pub(crate) below_min_bond: bool,
}

impl Plan {
    /// Seconds until a `Waiting` request matures; `0` for every other action.
    pub(crate) const fn remaining_secs(&self) -> u64 {
        match self.action {
            Action::Waiting { unlock_at, now, .. } => unlock_at.saturating_sub(now),
            _ => 0,
        }
    }
}

/// Transaction hashes from a non-dry run; `None` for steps that were skipped
/// (tier already at the target) or not part of this action.
#[derive(Default)]
pub(crate) struct Outcome {
    pub(crate) declare: Option<B256>,
    pub(crate) request: Option<B256>,
    pub(crate) withdraw: Option<B256>,
}

/// Read chain state and decide the action. Every precondition the contract
/// enforces is checked here first, so an impossible request fails before any
/// transaction is sent rather than as a raw revert.
pub(crate) async fn build_plan<P: Provider + Clone, Q: Provider>(
    bond: &CapacityBond::CapacityBondInstance<P>,
    provider: &Q,
    operator: Address,
    cb_addr: Address,
    request: AmountRequest,
) -> anyhow::Result<Plan> {
    let ctx = || format!("failed to read CapacityBond state at {cb_addr}");
    let pending = bond.unbondingOf(operator).call().await.with_context(ctx)?;
    let prior = bond.activeBond(operator).call().await.with_context(ctx)?;
    let declared = bond.declaredMbps(operator).call().await.with_context(ctx)?;
    let min_bond = bond.minBond().call().await.with_context(ctx)?;
    let unbonding_period = bond.unbondingPeriod().call().await.with_context(ctx)?;
    let declared_mbps = u64::try_from(declared).unwrap_or(u64::MAX);
    let period_secs = u64::try_from(unbonding_period).unwrap_or(u64::MAX);

    // A request in flight blocks a new one (`UnbondingInProgress`), so the
    // amount flags cannot apply — say so instead of ignoring them.
    if pending.amount != U256::ZERO {
        anyhow::ensure!(
            request == AmountRequest::Unspecified,
            "an unbonding request for {} base units is already in flight; \
             `requestUnbond` reverts while one is pending. Re-run \
             `decdn node unbond` with no amount flag to withdraw it once it matures.",
            pending.amount,
        );
        let now = head_timestamp(provider).await?;
        let unlock_at = u64::try_from(pending.unlockAt).unwrap_or(u64::MAX);
        let action = if now >= unlock_at {
            Action::Withdraw {
                amount: pending.amount,
            }
        } else {
            Action::Waiting {
                amount: pending.amount,
                unlock_at,
                now,
            }
        };
        return Ok(Plan {
            capacity_bond: cb_addr,
            action,
            prior,
            declared_mbps,
            unbonding_period: period_secs,
            below_min_bond: prior < min_bond,
        });
    }

    anyhow::ensure!(
        request != AmountRequest::Unspecified,
        "no unbonding request in flight and no amount given: pass --to-mbps <MBPS> \
         to reduce the declared tier, --all to release everything the bond curve \
         permits, or --amount <BASE_UNITS> for an exact figure",
    );
    anyhow::ensure!(prior > U256::ZERO, "no active bond to unbond");

    let (release, retained, declare_to) =
        resolve_release(bond, cb_addr, request, prior, declared, min_bond).await?;

    Ok(Plan {
        capacity_bond: cb_addr,
        action: Action::Request {
            release,
            retained,
            declare_to,
        },
        prior,
        declared_mbps,
        unbonding_period: period_secs,
        below_min_bond: retained < min_bond,
    })
}

/// Turn the requested amount into `(release, retained, declare_to)`, rejecting
/// anything the contract would revert on. Split out of [`build_plan`] because
/// this is where the three flags stop agreeing: each picks a different floor,
/// and `--to-mbps` is the only one that moves the declared tier.
async fn resolve_release<P: Provider + Clone>(
    bond: &CapacityBond::CapacityBondInstance<P>,
    cb_addr: Address,
    request: AmountRequest,
    prior: U256,
    declared: U256,
    min_bond: U256,
) -> anyhow::Result<(U256, U256, Option<u64>)> {
    let ctx = || format!("failed to read CapacityBond state at {cb_addr}");
    let declared_mbps = u64::try_from(declared).unwrap_or(u64::MAX);
    // The curve floor the contract itself enforces, evaluated against the tier
    // that will be declared at `requestUnbond` time.
    let curve_floor =
        |mbps: U256| async move { bond.bondRequired(mbps).call().await.with_context(ctx) };

    match request {
        AmountRequest::ToMbps(target_mbps) => {
            let min_cap = bond.minCapacityMbps().call().await.with_context(ctx)?;
            let max_cap = bond.maxCapacityMbps().call().await.with_context(ctx)?;
            let target = U256::from(target_mbps);
            anyhow::ensure!(
                target >= min_cap && target <= max_cap,
                "declared capacity {target_mbps} Mbps is outside the on-chain band \
                 [{min_cap}, {max_cap}]; declareMbps would revert",
            );
            anyhow::ensure!(
                target < declared,
                "--to-mbps {target_mbps} is not below the current declared tier \
                 ({declared_mbps} Mbps); raise capacity with `decdn node bond --mbps \
                 {target_mbps}` instead",
            );
            // Retain the same `max(minBond, bondRequired)` target `bond` tops up
            // to, so the operator stays eligible at the reduced tier.
            let retained = min_bond.max(curve_floor(target).await?);
            let release = prior.saturating_sub(retained);
            anyhow::ensure!(
                release > U256::ZERO,
                "nothing to release: the active bond ({prior}) is already at or below \
                 the {target_mbps} Mbps target ({retained} base units)",
            );
            Ok((release, retained, Some(target_mbps)))
        }
        AmountRequest::All => {
            // The contract's own floor — deliberately below `minBond`.
            let retained = curve_floor(declared).await?;
            let release = prior.saturating_sub(retained);
            anyhow::ensure!(
                release > U256::ZERO,
                "nothing to release: the active bond ({prior}) is already at the curve \
                 floor for {declared_mbps} Mbps ({retained} base units)",
            );
            Ok((release, retained, None))
        }
        AmountRequest::Exact(amount) => {
            anyhow::ensure!(amount > U256::ZERO, "--amount must be greater than zero");
            anyhow::ensure!(
                amount <= prior,
                "--amount {amount} exceeds the active bond ({prior} base units)",
            );
            let retained = prior - amount;
            let floor = curve_floor(declared).await?;
            anyhow::ensure!(
                retained >= floor,
                "releasing {amount} base units would leave {retained}, below the \
                 {declared_mbps} Mbps curve floor of {floor}; requestUnbond would revert \
                 with BondBelowCurve. Lower the tier first with `decdn node bond --mbps \
                 <lower>`, or use `--to-mbps <lower>` to do both in one run.",
            );
            Ok((amount, retained, None))
        }
        // `build_plan` rejects `Unspecified` before calling in, so this arm is
        // unreachable — but return an error rather than panic (the workspace
        // denies `panic`/`unreachable` in the anti-panic policy).
        AmountRequest::Unspecified => {
            anyhow::bail!("internal: no amount requested for a new unbonding request")
        }
    }
}

/// Submit the action's transactions in order. For `Request` the `declareMbps`
/// must precede `requestUnbond`, which evaluates the curve against whatever
/// tier is current at call time.
pub(crate) async fn execute<P: Provider + Clone>(
    bond: &CapacityBond::CapacityBondInstance<P>,
    plan: &Plan,
) -> anyhow::Result<Outcome> {
    let mut outcome = Outcome::default();
    match plan.action {
        Action::Request {
            release,
            declare_to,
            ..
        } => {
            if let Some(mbps) = declare_to {
                let receipt = send(bond.declareMbps(U256::from(mbps)), "declareMbps").await?;
                outcome.declare = Some(receipt);
            }
            let receipt = send(bond.requestUnbond(release), "requestUnbond").await?;
            outcome.request = Some(receipt);
        }
        Action::Withdraw { .. } => {
            let receipt = send(bond.unbond(), "unbond").await?;
            outcome.withdraw = Some(receipt);
        }
        // Filtered out in `run` before `execute` is reached.
        Action::Waiting { .. } => {}
    }
    Ok(outcome)
}

/// Send one call, await its receipt, and fail on a revert — the same
/// send/receipt/`ensure!(status)` triple every step of `bond::execute` uses.
async fn send<C: alloy::contract::CallDecoder, P: Provider>(
    call: alloy::contract::CallBuilder<P, C>,
    label: &str,
) -> anyhow::Result<B256> {
    let pending = call
        .send()
        .await
        .with_context(|| format!("{label} transaction failed to send"))?;
    let receipt = pending
        .get_receipt()
        .await
        .with_context(|| format!("{label} sent but the receipt could not be fetched"))?;
    anyhow::ensure!(
        receipt.status(),
        "{label} reverted (tx {})",
        receipt.transaction_hash
    );
    Ok(receipt.transaction_hash)
}

/// Head block timestamp, used to decide whether a request has matured. Read
/// from the chain rather than the local clock so the comparison uses the same
/// clock `unbond()` will.
async fn head_timestamp<P: Provider>(provider: &P) -> anyhow::Result<u64> {
    let block = provider
        .get_block(BlockId::latest())
        .await
        .context("failed to read the latest block")?
        .ok_or_else(|| anyhow::anyhow!("no latest block"))?;
    Ok(block.header.timestamp)
}

/// What the confirmation gate decides, before any IO. Split from
/// [`confirm_or_bail`] the way `terms::decide` is split from
/// `terms::ensure_accepted`, so the gate that protects an operator from an
/// accidental window-long outage is testable without a TTY.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Confirmation {
    /// Nothing to confirm — a withdrawal only returns TOKEN, and a maturing
    /// request submits nothing.
    NotNeeded,
    /// `--yes` supplied, or asserted-away by automation.
    Bypassed,
    /// Ask on the terminal.
    Prompt,
    /// Headless and unconfirmed: refuse rather than assume consent.
    NeedFlag,
}

/// Decide whether to confirm, from the action and the two ambient signals.
/// Only `Request` costs anything irreversible-for-a-window, so only it asks.
pub(crate) const fn decide_confirmation(
    action: Action,
    yes: bool,
    interactive: bool,
) -> Confirmation {
    if !matches!(action, Action::Request { .. }) {
        return Confirmation::NotNeeded;
    }
    if yes {
        return Confirmation::Bypassed;
    }
    if interactive {
        Confirmation::Prompt
    } else {
        Confirmation::NeedFlag
    }
}

/// Confirm before the first send on a `Request`. Starting a request costs the
/// operator active-set membership for the whole window, which is not obvious
/// from the flags alone — so a terminal run says so and waits for `y`.
/// `Withdraw` needs no confirmation: it only returns TOKEN.
fn confirm_or_bail(plan: &Plan, yes: bool) -> anyhow::Result<()> {
    let Action::Request {
        release, retained, ..
    } = plan.action
    else {
        return Ok(());
    };
    // Interactive only when BOTH streams are terminals: the warning goes to
    // stderr and the answer is read from stdin, so if either is redirected the
    // operator can't see what they'd be agreeing to (same rule as `terms`).
    let interactive = io::stdin().is_terminal() && io::stderr().is_terminal();
    match decide_confirmation(plan.action, yes, interactive) {
        Confirmation::NotNeeded | Confirmation::Bypassed => return Ok(()),
        Confirmation::Prompt | Confirmation::NeedFlag => {}
    }
    let mut err = io::stderr().lock();
    writeln!(
        err,
        "About to release {release} base units (retaining {retained}). Your node will be \
         INACTIVE for the full {} unbonding window, and the TOKEN is only withdrawable \
         after it.",
        format_remaining(plan.unbonding_period),
    )?;
    if plan.below_min_bond {
        writeln!(
            err,
            "The retained bond is below minBond, so the node stays inactive after the \
             withdrawal until it re-bonds."
        )?;
    }
    if !interactive {
        anyhow::bail!(
            "unbond not confirmed: no interactive terminal detected — re-run with `--yes` \
             (or `--dry-run` to preview)."
        );
    }
    write!(err, "Proceed? [y/N] ")?;
    err.flush()?;
    let mut line = String::new();
    io::stdin().read_line(&mut line)?;
    let answer = line.trim().to_ascii_lowercase();
    anyhow::ensure!(answer == "y" || answer == "yes", "unbond cancelled.");
    Ok(())
}

/// Write the plan + outcome as JSON or grep-friendly `key=value` lines. Pure
/// (`&mut impl Write`) so the output shape is unit-testable without a chain.
/// The `phase` key is what distinguishes the three actions for a machine
/// consumer; the amount keys that don't apply to a phase are omitted rather
/// than zeroed, so `release_base=0` never has to be read as "n/a".
pub(crate) fn write_plan(
    w: &mut impl io::Write,
    p: &Plan,
    json: bool,
    o: &Outcome,
) -> io::Result<()> {
    let submitted = o.declare.is_some() || o.request.is_some() || o.withdraw.is_some();
    let tx_hex = |h: Option<&B256>| h.map(|v| format!("{v:#x}"));
    let phase = match p.action {
        Action::Request { .. } => "request",
        Action::Waiting { .. } => "waiting",
        Action::Withdraw { .. } => "withdraw",
    };
    if json {
        let mut value = serde_json::json!({
            "phase": phase,
            "submitted": submitted,
            "capacity_bond": format!("{:#x}", p.capacity_bond),
            "prior_bond_base": p.prior.to_string(),
            "declared_mbps": p.declared_mbps,
            "unbonding_period_secs": p.unbonding_period,
            "below_min_bond": p.below_min_bond,
            "declare_tx": tx_hex(o.declare.as_ref()),
            "request_tx": tx_hex(o.request.as_ref()),
            "withdraw_tx": tx_hex(o.withdraw.as_ref()),
        });
        if let Some(obj) = value.as_object_mut() {
            match p.action {
                Action::Request {
                    release,
                    retained,
                    declare_to,
                } => {
                    obj.insert("release_base".into(), release.to_string().into());
                    obj.insert("retained_bond_base".into(), retained.to_string().into());
                    obj.insert("declare_to_mbps".into(), declare_to.into());
                }
                Action::Waiting {
                    amount, unlock_at, ..
                } => {
                    obj.insert("pending_base".into(), amount.to_string().into());
                    obj.insert("unlock_at".into(), unlock_at.into());
                    obj.insert("remaining_secs".into(), p.remaining_secs().into());
                }
                Action::Withdraw { amount } => {
                    obj.insert("withdrawn_base".into(), amount.to_string().into());
                }
            }
        }
        return writeln!(w, "{value}");
    }
    // Base units: 1 TOKEN = 1e18 base units.
    writeln!(w, "phase={phase}")?;
    writeln!(w, "capacity_bond={:#x}", p.capacity_bond)?;
    writeln!(w, "prior_bond_base={}", p.prior)?;
    writeln!(w, "declared_mbps={}", p.declared_mbps)?;
    match p.action {
        Action::Request {
            release,
            retained,
            declare_to,
        } => {
            writeln!(w, "release_base={release}")?;
            writeln!(w, "retained_bond_base={retained}")?;
            match declare_to {
                Some(mbps) => writeln!(w, "declare_to_mbps={mbps}")?,
                None => writeln!(w, "declare_to_mbps=none")?,
            }
            writeln!(
                w,
                "unbonding_period={}",
                format_remaining(p.unbonding_period)
            )?;
            writeln!(w, "below_min_bond={}", p.below_min_bond)?;
        }
        Action::Waiting {
            amount, unlock_at, ..
        } => {
            writeln!(w, "pending_base={amount}")?;
            writeln!(w, "unlock_at={unlock_at}")?;
            writeln!(w, "remaining={}", format_remaining(p.remaining_secs()))?;
        }
        Action::Withdraw { amount } => {
            writeln!(w, "withdrawn_base={amount}")?;
        }
    }
    write_tx_line(w, "declare_tx", o.declare.as_ref())?;
    write_tx_line(w, "request_tx", o.request.as_ref())?;
    write_tx_line(w, "withdraw_tx", o.withdraw.as_ref())?;
    writeln!(w, "submitted={submitted}")
}

/// One `<key>=<tx|skipped>` line, matching `bond`'s output vocabulary so an
/// operator reading both sees the same shape.
fn write_tx_line(w: &mut impl io::Write, key: &str, tx: Option<&B256>) -> io::Result<()> {
    match tx {
        Some(h) => writeln!(w, "{key}={h:#x}"),
        None => writeln!(w, "{key}=skipped"),
    }
}

/// Forward-looking duration as a coarse `13d 4h` string. The counterpart to
/// `node::format_age`, which renders elapsed time; kept local (and dependency
/// free — the workspace has no `chrono`/`humantime`) because the unbonding
/// window is the only place the CLI reports a future duration.
pub(crate) fn format_remaining(secs: u64) -> String {
    const MIN: u64 = 60;
    const HOUR: u64 = 60 * MIN;
    const DAY: u64 = 24 * HOUR;
    if secs == 0 {
        return "now".to_string();
    }
    if secs < MIN {
        return format!("{secs}s");
    }
    if secs < HOUR {
        return format!("{}m", secs / MIN);
    }
    if secs < DAY {
        return format!("{}h {}m", secs / HOUR, (secs % HOUR) / MIN);
    }
    format!("{}d {}h", secs / DAY, (secs % DAY) / HOUR)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn plan(action: Action, below_min_bond: bool) -> Plan {
        Plan {
            capacity_bond: Address::repeat_byte(0x22),
            action,
            prior: U256::from(60_000u64),
            declared_mbps: 100,
            unbonding_period: 14 * 24 * 3600,
            below_min_bond,
        }
    }

    fn rendered(p: &Plan, json: bool, o: &Outcome) -> String {
        let mut buf = Vec::new();
        write_plan(&mut buf, p, json, o).unwrap();
        String::from_utf8(buf).unwrap()
    }

    #[test]
    fn request_phase_reports_release_retained_and_declare() {
        let p = plan(
            Action::Request {
                release: U256::from(40_000u64),
                retained: U256::from(20_000u64),
                declare_to: Some(50),
            },
            false,
        );
        let s = rendered(&p, false, &Outcome::default());
        assert!(s.contains("phase=request"), "{s}");
        assert!(s.contains("release_base=40000"), "{s}");
        assert!(s.contains("retained_bond_base=20000"), "{s}");
        assert!(s.contains("declare_to_mbps=50"), "{s}");
        assert!(s.contains("unbonding_period=14d 0h"), "{s}");
        assert!(s.contains("below_min_bond=false"), "{s}");
        assert!(s.contains("submitted=false"), "{s}");
    }

    #[test]
    fn all_phase_flags_the_inactive_residual() {
        // `--all` retains only the curve floor, which sits below minBond — the
        // operator has to know the node won't come back without re-bonding.
        let p = plan(
            Action::Request {
                release: U256::from(59_000u64),
                retained: U256::from(1_000u64),
                declare_to: None,
            },
            true,
        );
        let s = rendered(&p, false, &Outcome::default());
        assert!(s.contains("declare_to_mbps=none"), "{s}");
        assert!(s.contains("below_min_bond=true"), "{s}");
    }

    #[test]
    fn waiting_phase_reports_unlock_and_remaining() {
        let p = plan(
            Action::Waiting {
                amount: U256::from(40_000u64),
                unlock_at: 1_000_000 + 3 * 24 * 3600,
                now: 1_000_000,
            },
            false,
        );
        assert_eq!(p.remaining_secs(), 3 * 24 * 3600);
        let s = rendered(&p, false, &Outcome::default());
        assert!(s.contains("phase=waiting"), "{s}");
        assert!(s.contains("pending_base=40000"), "{s}");
        assert!(s.contains("remaining=3d 0h"), "{s}");
        // No release keys leak into a phase they don't describe.
        assert!(!s.contains("release_base"), "{s}");
    }

    #[test]
    fn withdraw_phase_reports_the_tx() {
        let p = plan(
            Action::Withdraw {
                amount: U256::from(40_000u64),
            },
            false,
        );
        let o = Outcome {
            withdraw: Some(B256::repeat_byte(0xab)),
            ..Outcome::default()
        };
        let s = rendered(&p, false, &o);
        assert!(s.contains("phase=withdraw"), "{s}");
        assert!(s.contains("withdrawn_base=40000"), "{s}");
        assert!(s.contains("declare_tx=skipped"), "{s}");
        assert!(s.contains("submitted=true"), "{s}");
    }

    #[test]
    fn json_carries_only_the_active_phase_keys() {
        let p = plan(
            Action::Request {
                release: U256::from(40_000u64),
                retained: U256::from(20_000u64),
                declare_to: Some(50),
            },
            false,
        );
        let s = rendered(&p, true, &Outcome::default());
        let v: serde_json::Value = serde_json::from_str(&s).unwrap();
        assert_eq!(
            v.get("phase").and_then(serde_json::Value::as_str),
            Some("request")
        );
        assert_eq!(
            v.get("release_base").and_then(serde_json::Value::as_str),
            Some("40000")
        );
        assert_eq!(
            v.get("declare_to_mbps").and_then(serde_json::Value::as_u64),
            Some(50)
        );
        assert!(v.get("pending_base").is_none(), "{s}");
        assert!(
            v.get("withdraw_tx").is_some_and(serde_json::Value::is_null),
            "{s}"
        );
    }

    #[test]
    fn remaining_secs_is_zero_off_the_waiting_phase() {
        let p = plan(
            Action::Withdraw {
                amount: U256::from(1u64),
            },
            false,
        );
        assert_eq!(p.remaining_secs(), 0);
    }

    /// The gate exists so an operator cannot start a window-long outage by
    /// accident, so the headless-and-unconfirmed case must REFUSE rather than
    /// assume consent — the one wrong answer here is expensive to undo.
    #[test]
    fn headless_request_without_yes_refuses() {
        let request = Action::Request {
            release: U256::from(1u64),
            retained: U256::ZERO,
            declare_to: None,
        };
        assert_eq!(
            decide_confirmation(request, false, false),
            Confirmation::NeedFlag
        );
        assert_eq!(
            decide_confirmation(request, false, true),
            Confirmation::Prompt
        );
        assert_eq!(
            decide_confirmation(request, true, false),
            Confirmation::Bypassed
        );
    }

    /// A withdrawal only returns TOKEN and the maturing case submits nothing,
    /// so neither may block on a prompt — that would wedge `--json` consumers
    /// and cron-driven withdrawals.
    #[test]
    fn only_a_new_request_is_ever_confirmed() {
        for action in [
            Action::Withdraw {
                amount: U256::from(1u64),
            },
            Action::Waiting {
                amount: U256::from(1u64),
                unlock_at: 2,
                now: 1,
            },
        ] {
            for interactive in [true, false] {
                assert_eq!(
                    decide_confirmation(action, false, interactive),
                    Confirmation::NotNeeded,
                    "{action:?} must not prompt"
                );
            }
        }
    }

    #[test]
    fn format_remaining_units() {
        assert_eq!(format_remaining(0), "now");
        assert_eq!(format_remaining(45), "45s");
        assert_eq!(format_remaining(90), "1m");
        assert_eq!(format_remaining(3600 + 1800), "1h 30m");
        assert_eq!(format_remaining(14 * 24 * 3600), "14d 0h");
        assert_eq!(format_remaining(36 * 3600), "1d 12h");
    }
}
