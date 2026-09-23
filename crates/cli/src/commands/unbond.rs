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
        let now = head_timestamp(provider).await?;
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
/// `channel` reports its dispute deadline as a raw timestamp.
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
        render(p, json, o, false)
    }

    fn render(p: &Plan, json: bool, o: &Outcome, dry_run: bool) -> String {
        let mut buf = Vec::new();
        write_plan(&mut buf, p, json, o, dry_run).unwrap();
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

    /// Rendering only — the `--all` floor arithmetic itself lives in
    /// `plan_computation::all_retains_the_bare_curve_floor_not_min_bond`.
    #[test]
    fn write_plan_reports_a_below_min_bond_residual() {
        // The operator has to be able to see, in the receipt, that the node
        // won't come back without re-bonding.
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

    /// The convergence case: `execute` sends `declareMbps` and `requestUnbond`
    /// as two transactions, so a run that lands the first and loses the second
    /// leaves `declared == target`. Re-running the identical command must pick
    /// up where it left off — skipping the redundant declare and still
    /// releasing the surplus — not refuse because the tier is no longer
    /// strictly above the target.
    #[test]
    fn a_retry_after_the_declare_landed_still_unbonds() {
        assert_eq!(
            tier_step(50, 50).expect("an equal tier is a resumable retry, not an error"),
            None,
            "the declare already landed, so it must be skipped rather than re-sent"
        );
    }

    #[test]
    fn tier_step_declares_down_and_rejects_raising() {
        assert_eq!(tier_step(50, 100).unwrap(), Some(50));
        let err = tier_step(200, 100).expect_err("raising capacity is not this command's job");
        let msg = format!("{err:#}");
        assert!(msg.contains("above the current declared tier"), "{msg}");
        assert!(msg.contains("decdn node bond --mbps"), "{msg}");
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
        // Exact unit boundaries, where an off-by-one `<` would silently reclassify.
        assert_eq!(format_remaining(60), "1m");
        assert_eq!(format_remaining(3600), "1h 0m");
        assert_eq!(format_remaining(86_400), "1d 0h");
    }

    /// Plan-computation coverage against a mocked provider.
    ///
    /// `resolve_release` decides how much TOKEN moves, and until this module
    /// existed nothing exercised it: the `--all` arm was never executed by any
    /// test (its only e2e use returns from `build_plan`'s pending-request branch
    /// first), and `--to-mbps`'s `max(minBond, ·)` term was inert at the e2e's
    /// chosen tiers — deleting it broke nothing.
    ///
    /// `Asserter` serves `eth_call` responses FIFO, which works here because the
    /// call order is static: `--to-mbps` reads the capacity band only when a
    /// `declareMbps` is actually planned, then `bondRequired`; `--all` and
    /// `--amount` read `bondRequired` alone. Queueing exactly the expected number
    /// of responses is therefore itself an assertion about which reads happen —
    /// a surplus read would fail with an empty-queue error.
    mod plan_computation {
        use alloy::primitives::Bytes;
        use alloy::providers::ProviderBuilder;
        use alloy::providers::mock::Asserter;

        use super::*;

        /// TOKEN base units, for readable fixtures.
        fn token(n: u64) -> U256 {
            U256::from(n) * U256::from(1_000_000_000_000_000_000_u64)
        }

        /// Queue one ABI-encoded `uint256` return for the next `eth_call`.
        fn push_u256(asserter: &Asserter, v: U256) {
            asserter.push_success(&Bytes::from(v.to_be_bytes::<32>().to_vec()));
        }

        /// Run `resolve_release` against a provider serving `calls` in order,
        /// for a REGISTERED operator — the state every flag was designed
        /// around. The inactive variant is [`resolve_inactive`]; it is a
        /// separate helper rather than a sixth parameter so the ten call sites
        /// below keep reading as "an ordinary operator lowering their bond".
        async fn resolve(
            calls: &[U256],
            request: AmountRequest,
            prior: U256,
            declared: u64,
            min_bond: U256,
        ) -> anyhow::Result<(U256, U256, Option<u64>)> {
            resolve_as(calls, request, prior, declared, min_bond, true).await
        }

        /// [`resolve`] for an operator that is NOT in the registered set —
        /// ejected, or bonded-and-declared but never registered (#1361).
        async fn resolve_inactive(
            calls: &[U256],
            request: AmountRequest,
            prior: U256,
            declared: u64,
            min_bond: U256,
        ) -> anyhow::Result<(U256, U256, Option<u64>)> {
            resolve_as(calls, request, prior, declared, min_bond, false).await
        }

        async fn resolve_as(
            calls: &[U256],
            request: AmountRequest,
            prior: U256,
            declared: u64,
            min_bond: U256,
            active: bool,
        ) -> anyhow::Result<(U256, U256, Option<u64>)> {
            let asserter = Asserter::new();
            for v in calls {
                push_u256(&asserter, *v);
            }
            let provider = ProviderBuilder::new().connect_mocked_client(asserter);
            let addr = Address::repeat_byte(0x22);
            let bond = CapacityBond::new(addr, provider);
            resolve_release(
                &bond,
                addr,
                request,
                prior,
                U256::from(declared),
                min_bond,
                active,
            )
            .await
        }

        /// `--all` takes the BARE curve floor. If this ever grows a
        /// `min_bond.max(...)` — the obvious copy-paste from the `--to-mbps` arm
        /// one match arm up — it would retain more than promised and silently
        /// release less TOKEN than the operator asked for.
        #[tokio::test]
        async fn all_retains_the_bare_curve_floor_not_min_bond() {
            // Below the curve/minBond crossover: curve floor 100 < minBond 50_000.
            let (release, retained, declare_to) = resolve(
                &[token(100)],
                AmountRequest::All,
                token(60_000),
                10,
                token(50_000),
            )
            .await
            .expect("a surplus above the curve floor is releasable");
            assert_eq!(retained, token(100), "the bare curve floor, no minBond max");
            assert_eq!(release, token(59_900));
            assert_eq!(
                declare_to, None,
                "--all never moves an ACTIVE operator's declared tier"
            );
            assert!(
                retained < token(50_000),
                "this is the case where --all leaves the node inactive"
            );
        }

        /// The #1361 exit end-to-end through `resolve_release`. This pins the
        /// plumbing — the `active` flag reaches the `--all` arm, a declare is
        /// planned, the release figure is the whole bond — but NOT the retarget
        /// itself, which the mock cannot observe. See
        /// `all_target_retargets_the_curve_for_an_inactive_operator`.
        #[tokio::test]
        async fn all_releases_the_tier_and_everything_for_an_inactive_operator() {
            let (release, retained, declare_to) = resolve_inactive(
                &[U256::ZERO],
                AmountRequest::All,
                token(10_126),
                1000,
                token(50_000),
            )
            .await
            .expect("an inactive operator can release their tier");
            assert_eq!(
                declare_to,
                Some(0),
                "the run must send declareMbps(0) before requestUnbond"
            );
            assert_eq!(retained, U256::ZERO, "bondRequired(0) == 0 — a full exit");
            assert_eq!(
                release,
                token(10_126),
                "the whole remaining bond, including a slashed-below-floor residual"
            );
        }

        /// The retarget itself, tested where it is observable at all.
        ///
        /// `resolve_release` cannot pin this: the decision shows up only in the
        /// ARGUMENT to `bondRequired`, and `Asserter` answers by queue position,
        /// not by calldata. Mutating the retarget away leaves every mocked test
        /// in this module green — verified — while shipping a CLI that refuses
        /// the #1361 exit with "nothing to release", because `retained` would be
        /// `bondRequired(1000) > prior` and the release would floor to 0.
        #[test]
        fn all_target_retargets_the_curve_for_an_inactive_operator() {
            let declared = U256::from(1000u64);

            // Inactive with a standing tier: clear it, and evaluate the curve
            // against 0 — the pair the mock cannot distinguish.
            assert_eq!(all_target(false, declared), (Some(0), U256::ZERO));

            // Active: unchanged. `declareMbps(0)` reverts for them, and the
            // retained bond is their standing tier's floor.
            assert_eq!(all_target(true, declared), (None, declared));

            // Nothing declared: no redundant `declareMbps(0)` to pay gas for and
            // log as a 0 -> 0 change, on either side of the registry.
            assert_eq!(all_target(false, U256::ZERO), (None, U256::ZERO));
            assert_eq!(all_target(true, U256::ZERO), (None, U256::ZERO));
        }

        /// The release is scoped to a tier that actually stands: an inactive
        /// operator with nothing declared must not send a redundant
        /// `declareMbps(0)`, which costs gas and logs a 0 -> 0 tier change.
        #[tokio::test]
        async fn inactive_operator_with_no_tier_sends_no_declare() {
            let (release, retained, declare_to) = resolve_inactive(
                &[U256::ZERO],
                AmountRequest::All,
                token(60_000),
                0,
                token(50_000),
            )
            .await
            .expect("a never-declared operator can still release everything");
            assert_eq!(declare_to, None, "nothing to clear");
            assert_eq!(retained, U256::ZERO);
            assert_eq!(release, token(60_000));
        }

        /// A bonded-but-never-declared operator has `bondRequired(0) == 0`, so
        /// `--all` really does mean all. This is the maximal-consequence path
        /// through the command and had no coverage at all.
        #[tokio::test]
        async fn all_releases_everything_when_no_tier_was_ever_declared() {
            let (release, retained, _) = resolve(
                &[U256::ZERO],
                AmountRequest::All,
                token(60_000),
                0,
                token(50_000),
            )
            .await
            .expect("nothing is retained when no tier is declared");
            assert_eq!(retained, U256::ZERO);
            assert_eq!(release, token(60_000), "the entire bond");
        }

        /// The `max(minBond, ·)` term that distinguishes `--to-mbps` from
        /// `--all`. Below the crossover `minBond` is the operative floor, so
        /// deleting the `max` makes this fail — which is the point.
        #[tokio::test]
        async fn to_mbps_below_the_crossover_retains_min_bond() {
            // Band [10, 200_000], then bondRequired(10) = 100 TOKEN << minBond.
            let (release, retained, declare_to) = resolve(
                &[U256::from(10), U256::from(200_000), token(100)],
                AmountRequest::ToMbps(10),
                token(60_000),
                5_000,
                token(50_000),
            )
            .await
            .expect("a surplus above minBond is releasable");
            assert_eq!(
                retained,
                token(50_000),
                "minBond is the floor here, NOT the 100 TOKEN curve value"
            );
            assert_eq!(release, token(10_000));
            assert_eq!(declare_to, Some(10), "the tier must be declared down first");
        }

        /// Above the crossover the curve dominates and `minBond` is inert — the
        /// complement of the case above, so neither term can be dropped.
        #[tokio::test]
        async fn to_mbps_above_the_crossover_retains_the_curve() {
            let (_, retained, _) = resolve(
                &[U256::from(10), U256::from(200_000), token(115_000)],
                AmountRequest::ToMbps(2_000),
                token(346_000),
                5_000,
                token(50_000),
            )
            .await
            .expect("a surplus above the curve is releasable");
            assert_eq!(retained, token(115_000), "the curve dominates minBond here");
        }

        /// The resume path: tier already at the target, so no `declareMbps` is
        /// planned — and therefore the capacity band is never read. Queueing only
        /// the `bondRequired` response proves the band check is gated on the
        /// declare rather than run unconditionally; an ungated check would try to
        /// read from an empty queue and fail.
        #[tokio::test]
        async fn to_mbps_at_the_current_tier_skips_the_declare_and_the_band_read() {
            let (release, retained, declare_to) = resolve(
                &[token(115_000)],
                AmountRequest::ToMbps(2_000),
                token(346_000),
                2_000,
                token(50_000),
            )
            .await
            .expect("an equal tier is a resumable retry");
            assert_eq!(declare_to, None, "the declare already landed");
            assert_eq!(retained, token(115_000));
            assert_eq!(release, token(231_000), "the surplus is still released");
        }

        /// `--amount` is bounded by the active bond. The guard is structural, not
        /// cosmetic: `retained = prior - amount` is a raw `U256` subtraction and
        /// alloy's `Uint` panics on overflow, in a workspace that denies `panic`.
        #[tokio::test]
        async fn amount_above_the_active_bond_is_rejected_before_subtracting() {
            let err = resolve(
                &[],
                AmountRequest::Exact(token(60_001)),
                token(60_000),
                2_000,
                token(50_000),
            )
            .await
            .expect_err("releasing more than is bonded must be refused");
            let msg = format!("{err:#}");
            assert!(msg.contains("exceeds the active bond"), "{msg}");
        }

        /// The happy `--amount` path, and the boundary: releasing down to exactly
        /// the curve floor is legal.
        #[tokio::test]
        async fn amount_down_to_exactly_the_curve_floor_is_allowed() {
            let (release, retained, declare_to) = resolve(
                &[token(115_000)],
                AmountRequest::Exact(token(231_000)),
                token(346_000),
                2_000,
                token(50_000),
            )
            .await
            .expect("landing exactly on the curve floor is legal");
            assert_eq!(retained, token(115_000));
            assert_eq!(release, token(231_000));
            assert_eq!(declare_to, None, "--amount never moves the tier");
        }

        /// One base unit past the floor must be refused with the remedy, so
        /// `BondBelowCurve` never surfaces as a raw revert.
        #[tokio::test]
        async fn amount_one_unit_below_the_curve_floor_names_the_remedy() {
            let err = resolve(
                &[token(115_000)],
                AmountRequest::Exact(token(231_000) + U256::from(1u64)),
                token(346_000),
                2_000,
                token(50_000),
            )
            .await
            .expect_err("breaching the curve floor must be refused");
            let msg = format!("{err:#}");
            assert!(msg.contains("BondBelowCurve"), "{msg}");
            assert!(msg.contains("--to-mbps"), "{msg}");
        }

        /// "Nothing to release" rather than a `ZeroAmount` revert, for both
        /// curve-derived flags.
        #[tokio::test]
        async fn nothing_to_release_is_refused_locally() {
            let err = resolve(
                &[U256::from(10), U256::from(200_000), token(115_000)],
                AmountRequest::ToMbps(2_000),
                token(115_000),
                5_000,
                token(50_000),
            )
            .await
            .expect_err("already at the target");
            assert!(format!("{err:#}").contains("nothing to release"), "{err:#}");

            let err = resolve(
                &[token(115_000)],
                AmountRequest::All,
                token(115_000),
                2_000,
                token(50_000),
            )
            .await
            .expect_err("already at the curve floor");
            assert!(format!("{err:#}").contains("nothing to release"), "{err:#}");
        }
    }
}
