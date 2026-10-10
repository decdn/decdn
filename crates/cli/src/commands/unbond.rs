//! `decdn node unbond` — lower the bond (ADR 026 § Capacity-bond curve).
//!
//! The reverse of `decdn node bond`. Lowering a bond is a two-phase on-chain
//! operation — `requestUnbond(amount)` starts a `unbondingPeriod` window and
//! `unbond()` withdraws once it matures — with no atomic refund. Rather than
//! exposing both calls, this command reads chain state and picks the phase. A
//! run that lost its `requestUnbond` after `declareMbps` landed resumes on
//! re-run, the way `bond`'s top-up converges; once `requestUnbond` HAS landed,
//! a re-run carrying an amount flag is a deliberate error rather than a resume,
//! since the pending request can no longer be changed.
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
//!
//! `--all` releases everything the curve permits *while the node is still
//! registered*, which is not the same as an exit: the declared tier pins
//! `bondRequired(declaredMbps)` in place. A full exit therefore starts with
//! `decdn node deregister` (#1359), which clears the tier (ADR 003 § Node
//! Registry, ADR 026 § Capacity-bond curve); `--all` then releases the whole
//! bond.
//!
//! An operator who is already INACTIVE cannot deregister — the contract reverts
//! `NodeNotActive` — so this command clears their tier itself, via
//! `declareMbps(0)` (#1361, accepted only from an inactive operator). That is
//! what lets an ejected operator, or one who declared a tier without ever
//! registering, reach a full exit with `--all` alone.

use std::io::{self, IsTerminal, Write};
use std::path::Path;

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
    let provider = decdn_client::provider::build_provider(&resolved.rpc_url, &signer)?;
    let bond = CapacityBond::new(cb_addr, &provider);

    let request = AmountRequest::from_args(args)?;
    let plan = build_plan(&bond, &provider, operator, cb_addr, request).await?;

    let json = args.chain.common.json;
    if args.chain.common.dry_run {
        let mut out = io::stdout().lock();
        write_plan(&mut out, &plan, json, &Outcome::default(), true)
            .context("failed to write dry-run output")?;
        return Ok(());
    }

    // `Waiting` submits nothing: report and exit non-zero so a scripted retry
    // loop can tell "not yet" from "withdrawn".
    if let Action::Waiting { unlock_at, .. } = plan.action {
        let mut out = io::stdout().lock();
        write_plan(&mut out, &plan, json, &Outcome::default(), false)
            .context("failed to write result")?;
        anyhow::bail!(
            "unbonding request is still maturing: unlocks at {unlock_at} ({}); \
             re-run `decdn node unbond` then to withdraw",
            format_remaining(plan.remaining_secs()),
        );
    }

    confirm_or_bail(&plan, args.yes)?;

    // The outcome is owned HERE, not inside `execute`, so a partially-applied
    // sequence survives the error. `execute` sends `declareMbps` then
    // `requestUnbond`; if the second fails after the first mined, the operator's
    // declared tier is already lowered and that tx hash is the only record of
    // it. Returning `Err` with the outcome still inside `execute` would drop it
    // and print nothing at all — leaving them to conclude "nothing happened"
    // while their tier is halved.
    let mut outcome = Outcome::default();
    let result = execute(&bond, &plan, &mut outcome).await;

    let mut out = io::stdout().lock();
    write_plan(&mut out, &plan, json, &outcome, false).context("failed to write result")?;
    drop(out);

    result.with_context(|| match outcome.declare {
        Some(tx) => format!(
            "declareMbps ALREADY LANDED (tx {tx:#x}): the declared tier is now lowered, but \
             NO unbonding request was started and no TOKEN was released. Re-run the identical \
             command to resume — it skips the declare and requests the unbond"
        ),
        None => "no transaction landed; on-chain state is unchanged".to_string(),
    })
}

/// How much the operator asked to release, normalised from the three mutually
/// exclusive flags. Clap's `ArgGroup` enforces the exclusivity; this maps the
/// absent case to `Unspecified`, which is legal only on the withdraw path —
/// `build_plan` is what enforces that.
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

/// Read chain state and decide the action. Every *operator-controllable*
/// precondition the contract enforces is checked here first, so an impossible
/// request fails before any transaction is sent rather than as a raw revert.
/// The exception is `whenNotPaused`, which is not pre-checked — a paused
/// contract still surfaces as a revert from [`chain_ctx::send`].
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
    // Loud, not saturating: `declared_mbps` gates `tier_step`, so a silent
    // clamp to `u64::MAX` would make that guard accept ANY `--to-mbps`. The
    // contract's own `maxCapacityMbps` ceiling (1e6) guarantees this fits, so
    // failing here means we are not talking to a `CapacityBond` at all.
    let declared_mbps = u64::try_from(declared).with_context(|| {
        format!(
            "CapacityBond at {cb_addr} returned declaredMbps={declared}, far above its own \
             maxCapacityMbps ceiling — is `capacity_bond_address` pointing at the right contract?"
        )
    })?;
    // Display-only (the unlock-time report), so a clamp here cannot mis-decide.
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
        let now = chain_ctx::head_timestamp(provider).await?;
        // Compare in U256 so the maturity decision doesn't depend on the
        // saturation constant of a narrowing conversion. `unlock_at` is narrowed
        // only afterwards, for display.
        let matured = U256::from(now) >= pending.unlockAt;
        let unlock_at = u64::try_from(pending.unlockAt).unwrap_or(u64::MAX);
        let action = if matured {
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

    // `_nodes[operator].active`, NOT the composite `isActive`: the latter also
    // requires bond over `minBond` and no request in flight, so it would report
    // a merely-under-bonded operator as inactive and let `--all` send a
    // `declareMbps(0)` the contract rejects. Registered-set membership is the
    // exact flag `declareMbps`'s release branch gates on.
    let active = bond
        .getNodeByAddress(operator)
        .call()
        .await
        .with_context(ctx)?
        .active;

    let (release, retained, declare_to) =
        resolve_release(bond, cb_addr, request, prior, declared, min_bond, active).await?;

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

/// Validate a `--to-mbps` target against the currently declared tier and
/// decide whether `declareMbps` still has to run.
///
/// `target == declared` is **accepted, not rejected**, and this is what makes
/// the command converge: `execute` sends `declareMbps` then `requestUnbond` as
/// two transactions, so a run that lands the first and loses the second leaves
/// the tier already reduced while the bond is still high. Rejecting the equal
/// case would strand exactly the operator this command's idempotence is for —
/// and would point them at `decdn node bond`, which cannot release anything.
/// The `release > 0` check downstream is what catches the genuinely-nothing-
/// to-do case, so accepting equality here costs no safety.
fn tier_step(target_mbps: u64, declared_mbps: u64) -> anyhow::Result<Option<u64>> {
    anyhow::ensure!(
        target_mbps <= declared_mbps,
        "--to-mbps {target_mbps} is above the current declared tier \
         ({declared_mbps} Mbps); raise capacity with `decdn node bond --mbps \
         {target_mbps}` instead",
    );
    Ok((target_mbps < declared_mbps).then_some(target_mbps))
}

/// `--all`'s two coupled decisions: whether to clear the declared tier first,
/// and which tier the retained-bond curve is then evaluated against.
///
/// An INACTIVE operator releases the tier as part of the same run (#1361):
/// `declareMbps(0)` is accepted only from them, and it is the only route to 0
/// they have — `deregisterNode` reverts `NodeNotActive`. `bondRequired(0) == 0`,
/// so the retarget is what turns `--all` into a genuine full exit rather than a
/// release down to the standing tier's floor. An ACTIVE operator keeps the old
/// behavior: for them `declareMbps(0)` reverts, and clearing the tier is
/// `decdn node deregister`'s job.
///
/// Pure, and split out for that reason. The decision is only observable through
/// `bondRequired`'s ARGUMENT, and the `Asserter` the tests below use is a FIFO
/// value queue with no argument awareness — it returns the same queued response
/// whether the call was `bondRequired(0)` or `bondRequired(1000)`. So a test
/// driving `resolve_release` cannot tell the retarget from its absence;
/// mutating it away left every mocked test green (verified). Testing the
/// decision directly is the only thing that actually pins it.
fn all_target(active: bool, declared: U256) -> (Option<u64>, U256) {
    if !active && declared != U256::ZERO {
        (Some(0), U256::ZERO)
    } else {
        (None, declared)
    }
}

/// Turn the requested amount into `(release, retained, declare_to)`, rejecting
/// anything the contract would revert on. Separate from [`build_plan`] because
/// this is where the three flags stop agreeing: each picks a different floor,
/// and `--to-mbps` is the only one that moves the declared tier.
async fn resolve_release<P: Provider + Clone>(
    bond: &CapacityBond::CapacityBondInstance<P>,
    cb_addr: Address,
    request: AmountRequest,
    prior: U256,
    declared: U256,
    min_bond: U256,
    active: bool,
) -> anyhow::Result<(U256, U256, Option<u64>)> {
    let ctx = || format!("failed to read CapacityBond state at {cb_addr}");
    let declared_mbps = u64::try_from(declared).unwrap_or(u64::MAX);
    // The curve floor the contract itself enforces, evaluated against the tier
    // that will be declared at `requestUnbond` time.
    let curve_floor =
        |mbps: U256| async move { bond.bondRequired(mbps).call().await.with_context(ctx) };

    match request {
        AmountRequest::ToMbps(target_mbps) => {
            let target = U256::from(target_mbps);
            let declare_to = tier_step(target_mbps, declared_mbps)?;
            // Band-check ONLY when a `declareMbps` will actually be sent. The
            // band constrains that call and nothing else — `requestUnbond`'s only
            // floor is the curve — so checking it unconditionally would refuse
            // something the contract accepts: if governance raises
            // `minCapacityMbps` above an operator's declared tier, the resume
            // path (`declare_to == None`) is still perfectly legal.
            if declare_to.is_some() {
                let min_cap = bond.minCapacityMbps().call().await.with_context(ctx)?;
                let max_cap = bond.maxCapacityMbps().call().await.with_context(ctx)?;
                anyhow::ensure!(
                    target >= min_cap && target <= max_cap,
                    "declared capacity {target_mbps} Mbps is outside the on-chain band \
                     [{min_cap}, {max_cap}]; declareMbps would revert",
                );
            }
            // Retain the same `max(minBond, bondRequired)` target `bond` tops up
            // to, so the operator stays eligible at the reduced tier. This is the
            // ONLY difference from `--all`, which takes the bare curve floor.
            let retained = min_bond.max(curve_floor(target).await?);
            let release = prior.saturating_sub(retained);
            anyhow::ensure!(
                release > U256::ZERO,
                "nothing to release: the active bond ({prior}) is already at or below \
                 the {target_mbps} Mbps target ({retained} base units)",
            );
            Ok((release, retained, declare_to))
        }
        AmountRequest::All => {
            let (declare_to, target) = all_target(active, declared);
            // The contract's own floor: the bare curve, with no `min_bond.max()`.
            // That is independent of `minBond`, not below it — with the deployed
            // `k`/`α` the curve crosses above `minBond` around 1000 Mbps, so
            // whether this leaves the operator inactive is tier-dependent and is
            // what `below_min_bond` reports.
            let retained = curve_floor(target).await?;
            let release = prior.saturating_sub(retained);
            anyhow::ensure!(
                release > U256::ZERO,
                "nothing to release: the active bond ({prior}) is already at the curve \
                 floor for {declared_mbps} Mbps ({retained} base units)",
            );
            Ok((release, retained, declare_to))
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
        // unreachable. It returns an error rather than `unreachable!` because
        // the workspace anti-panic policy (`Cargo.toml` `[workspace.lints]`
        // denies `panic`) treats a broken assumption as something to surface at
        // the call site, not to abort the process on.
        AmountRequest::Unspecified => {
            anyhow::bail!("internal: no amount requested for a new unbonding request")
        }
    }
}

/// Submit the action's transactions in order. For `Request` the `declareMbps`
/// must precede `requestUnbond`, which evaluates the curve against whatever
/// tier is current at call time.
///
/// `outcome` is borrowed rather than returned so each hash is recorded in the
/// CALLER's value as it lands. Returning it would tie the record to the happy
/// path, and the interesting case here is the unhappy one: a `declareMbps` that
/// mined before `requestUnbond` failed is a real, resumable state change the
/// operator has to be told about.
pub(crate) async fn execute<P: Provider + Clone>(
    bond: &CapacityBond::CapacityBondInstance<P>,
    plan: &Plan,
    outcome: &mut Outcome,
) -> anyhow::Result<()> {
    match plan.action {
        Action::Request {
            release,
            declare_to,
            ..
        } => {
            if let Some(mbps) = declare_to {
                // `0` is the release path (#1361) and is legal precisely because
                // it bypasses the band, so the band question would be actively
                // misleading advice there.
                let hint = if mbps == 0 {
                    "declareMbps(0) is accepted only from an operator that is NOT in the \
                     registered set; if this node is still registered, use `decdn node \
                     deregister` instead"
                        .to_string()
                } else {
                    format!("is {mbps} Mbps within the capacity band?")
                };
                chain_ctx::send(
                    bond.declareMbps(U256::from(mbps)),
                    "declareMbps",
                    Some(&hint),
                    &mut outcome.declare,
                )
                .await?;
            }
            chain_ctx::send(
                bond.requestUnbond(release),
                "requestUnbond",
                None,
                &mut outcome.request,
            )
            .await?;
        }
        Action::Withdraw { .. } => {
            chain_ctx::send(bond.unbond(), "unbond", None, &mut outcome.withdraw).await?;
        }
        // `run` bails on `Waiting` before reaching here. Kept loud rather than a
        // no-op: a silent `Ok` would report `submitted=false` and exit 0, which
        // a wrapper doing `if decdn node unbond; then mark_withdrawn; fi` would
        // read as a completed withdrawal. Same treatment as the unreachable
        // `AmountRequest::Unspecified` arm above.
        Action::Waiting { .. } => anyhow::bail!(
            "internal: reached `execute` with a still-maturing request; nothing was submitted"
        ),
    }
    Ok(())
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
    /// `--yes` supplied: disclose the consequences, but skip the prompt.
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
/// from the flags alone — so the consequences are always disclosed, and a
/// terminal run additionally waits for `y`. `Withdraw` needs no confirmation:
/// it only returns TOKEN.
///
/// `--yes` suppresses the *prompt*, never the disclosure. A scripted
/// `--all --yes` can leave the node permanently inactive; "don't ask" must not
/// silently become "don't tell", so the warning is written for `Bypassed` too.
/// Every decision here comes from [`decide_confirmation`] — nothing is
/// re-derived from `interactive`, so the tested function IS the enforcing one.
fn confirm_or_bail(plan: &Plan, yes: bool) -> anyhow::Result<()> {
    // Interactive only when BOTH streams are terminals: the warning goes to
    // stderr and the answer is read from stdin, so if either is redirected the
    // operator can't see what they'd be agreeing to (same rule as `terms`).
    let interactive = io::stdin().is_terminal() && io::stderr().is_terminal();
    let decision = decide_confirmation(plan.action, yes, interactive);
    if decision == Confirmation::NotNeeded {
        return Ok(());
    }
    let Action::Request {
        release, retained, ..
    } = plan.action
    else {
        return Ok(());
    };

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

    match decision {
        // Disclosed above; the operator asked not to be asked.
        Confirmation::NotNeeded | Confirmation::Bypassed => Ok(()),
        Confirmation::NeedFlag => anyhow::bail!(
            "unbond not confirmed: no interactive terminal detected — re-run with `--yes` \
             (or `--dry-run` to preview)."
        ),
        Confirmation::Prompt => {
            write!(err, "Proceed? [y/N] ")?;
            err.flush()?;
            let mut line = String::new();
            io::stdin().read_line(&mut line)?;
            // Allowlist, not denylist: an empty line (Ctrl-D / EOF) lands on the
            // deny side rather than being read as assent.
            let answer = line.trim().to_ascii_lowercase();
            anyhow::ensure!(answer == "y" || answer == "yes", "unbond cancelled.");
            Ok(())
        }
    }
}

/// Write the plan + outcome as JSON or grep-friendly `key=value` lines. Pure
/// (`&mut impl Write`) so the output shape is unit-testable without a chain.
/// The `phase` key is what distinguishes the three actions for a machine
/// consumer; the amount keys that don't apply to a phase are omitted rather
/// than zeroed, so `release_base=0` never has to be read as "n/a".
///
/// `dry_run` is carried explicitly rather than inferred from `submitted`, for
/// the reason `bond::write_plan` carries it: several real runs also submit
/// nothing (a maturing `Waiting`, or a failed first send), so `submitted=false`
/// alone cannot mean "preview".
pub(crate) fn write_plan(
    w: &mut impl io::Write,
    p: &Plan,
    json: bool,
    o: &Outcome,
    dry_run: bool,
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
            "dry_run": dry_run,
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
    writeln!(w, "submitted={submitted} dry_run={dry_run}")
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
/// free — the CLI's dependency set pulls in no `chrono`/`humantime`) because the
/// unbonding window is the only place the CLI *formats* a future duration;
/// `pool` reports its dispute deadline as a raw timestamp.
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
mod tests;
