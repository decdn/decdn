//! `decdn node unbond` — lower the bond (ADR 026 § Capacity-bond curve).
//!
//! The reverse of `decdn node bond`. `unbond(amount)` moves `amount` out of
//! `activeBond` and returns it to the operator in the same transaction. The
//! command may first lower the declared tier with `declareMbps`, so a run is
//! one or two transactions. A run that lost its `unbond` after `declareMbps`
//! landed resumes on re-run, the way `bond`'s top-up converges.
//!
//! Two contract facts drive the shape of this command, and both are surfaced
//! to the operator rather than left to a revert:
//!
//! - The node stays active while the retained bond covers its floor and
//!   leaves the active set at once when it does not, which is why a terminal
//!   run confirms first. An unbond below the floor also ends the operator's
//!   tenure: it clears `firstBondedAt` and the recorded floor, so the
//!   governance age ramp restarts at the next entry. A registered operator
//!   whose recorded floor is already 0 holds no tenure and is already
//!   inactive; a bond back to `minBond` re-enters.
//! - `unbond`'s floor is `bondRequired(declaredMbps)` — the curve, NOT
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

use std::io;
use std::path::Path;

use alloy::primitives::{Address, B256, U256};
use alloy::providers::Provider;
use anyhow::Context;
use decdn_common::cli;
use decdn_incentive::capacity_bond::CapacityBond;

use crate::commands::chain_ctx;
use crate::commands::rotate_key::confirm_or_bail;

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
    let plan = build_plan(&bond, operator, cb_addr, request).await?;

    let json = args.chain.common.json;
    if args.chain.common.dry_run {
        let mut out = io::stdout().lock();
        write_plan(&mut out, &plan, json, &Outcome::default(), true)
            .context("failed to write dry-run output")?;
        return Ok(());
    }

    confirm_or_bail(|w| write_disclosure(w, &plan), "unbond", args.yes)?;

    // The outcome is owned HERE, not inside `execute`, so a partially-applied
    // sequence survives the error. `execute` sends `declareMbps` then
    // `unbond`; if the second fails after the first mined, the operator's
    // declared tier is already lowered and that tx hash is the only record of
    // it. Returning `Err` with the outcome still inside `execute` would drop it
    // and print nothing at all — leaving them to conclude "nothing happened"
    // while their tier is halved.
    let mut outcome = Outcome::default();
    let result = execute(&bond, &plan, &mut outcome).await;

    let mut out = io::stdout().lock();
    let write_result = write_plan(&mut out, &plan, json, &outcome, false);
    drop(out);
    // A failed stdout write must never displace the chain error: under
    // `decdn node unbond --json | head`, Rust ignores SIGPIPE and `writeln!`
    // returns EPIPE, so `?`-ing this first would swallow the resume guidance
    // below. Echo it to stderr (stdout is broken by hypothesis) and let the
    // chain error win.
    if let Err(err) = &write_result {
        eprintln!("warning: failed to write unbond result to stdout: {err}");
    }
    result.with_context(|| resume_hint(&outcome))?;
    write_result.context("failed to write result")?;
    Ok(())
}

/// Operator-facing guidance attached to a failed [`execute`], naming the latest
/// step that may have taken effect.
///
/// Reads [`Outcome`] as [`chain_ctx::send`] populates it: `Some` means "may have
/// taken effect" (confirmed success, or broadcast with an unreadable receipt),
/// and `None` means "not recorded as applied" (never sent, a confirmed revert,
/// or a send whose broadcast is unknown, which the cause below then says).
///
/// `unbond` returns the TOKEN in the same transaction, so a re-run is not
/// harmless while one may be outstanding. alloy's nonce filler resolves against
/// the *pending* block: a re-run while an `unbond` is in the mempool takes the
/// next nonce, re-reads a stale `activeBond`, and submits a second `unbond`. For
/// `--amount` that second transaction releases the same amount again.
///
/// Matched over both steps in the `bond` command's order: the later step wins,
/// so an operator whose TOKEN may already be released is never told that only
/// the tier moved.
pub(crate) fn resume_hint(outcome: &Outcome) -> String {
    match (outcome.unbond, outcome.declare) {
        (Some(tx), _) => format!(
            "unbond tx {tx:#x} may have taken effect — it was submitted, and if the error \
             below is a receipt-fetch timeout rather than a revert its outcome is still \
             unknown. If it mined, the released TOKEN is already back with this operator. \
             Check `activeBond` and {tx:#x} before re-running. Do NOT re-run until {tx:#x} \
             has settled: a re-run while it is pending takes the next nonce and submits a \
             second unbond, and a re-run of `--amount` releases that amount again."
        ),
        (None, Some(tx)) => format!(
            "declareMbps tx {tx:#x} may have taken effect — it was submitted, and if the \
             error below is a receipt-fetch timeout rather than a revert its outcome is \
             still unknown. If it mined, the declared tier is lowered. No unbond is \
             recorded as having taken effect, but if the cause below says a send's outcome \
             is unknown, an unbond may still have been broadcast: check this signer's \
             pending and mined transactions and `activeBond`. Do NOT re-run until {tx:#x} \
             has settled. A re-run of the identical command then skips the declare when the \
             tier is already lowered and releases what is still bonded."
        ),
        (None, None) => "no transaction is recorded as having taken effect: either nothing \
             was broadcast (a pre-flight gas estimate or a pre-flight check rejecting the \
             call), or one was broadcast and confirmed reverted. But if the cause below says \
             a send's outcome is unknown, an unbond may have been broadcast: check this \
             signer's pending and mined transactions and `activeBond` before re-running, \
             because a re-run of `--amount` releases that amount again. Otherwise check the \
             cause below for a tx hash, and the pending nonce, before re-running"
            .to_string(),
    }
}

/// How much the operator asked to release, normalised from the three mutually
/// exclusive flags. Clap's `ArgGroup` requires exactly one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AmountRequest {
    /// Reduce the declared tier to this many Mbps and release the surplus.
    ToMbps(u64),
    /// Release everything the curve permits.
    All,
    /// Release exactly this many base units.
    Exact(U256),
}

impl AmountRequest {
    fn from_args(args: &cli::UnbondArgs) -> anyhow::Result<Self> {
        match (args.to_mbps, args.all, args.amount) {
            (Some(mbps), false, None) => Ok(Self::ToMbps(mbps)),
            (None, true, None) => Ok(Self::All),
            (None, false, Some(amount)) => Ok(Self::Exact(U256::from(amount))),
            // Clap's ArgGroup requires exactly one amount flag before we get
            // here; this arm exists so a future flag added outside the group
            // can't silently take one branch.
            _ => anyhow::bail!(
                "pass exactly one of --to-mbps <MBPS> to reduce the declared tier, --all to \
                 release everything the bond curve permits, or --amount <BASE_UNITS> for an \
                 exact figure"
            ),
        }
    }
}

/// What `unbond` intends to do, computed from chain state.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Plan {
    pub(crate) capacity_bond: Address,
    /// Base units to release.
    pub(crate) release: U256,
    /// Bond retained afterwards.
    pub(crate) retained: U256,
    /// `declareMbps(mbps)` must land first, or `unbond` reverts
    /// `BondBelowCurve` against the still-current tier.
    pub(crate) declare_to: Option<u64>,
    /// Active bond before this command.
    pub(crate) prior: U256,
    /// Currently declared capacity tier (Mbps).
    pub(crate) declared_mbps: u64,
    /// Where the retained bond leaves the operator against its floor.
    pub(crate) standing: Standing,
}

/// Where an unbond leaves the operator against its bond floor
/// (`bondFloorAtActivation`, or the live `minBond` for an operator that is not
/// registered and holds no recorded floor).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Standing {
    /// The retained bond covers the floor, so the node stays active.
    Covered,
    /// The retained bond sits below the floor. The node leaves the active set
    /// and cannot return without re-bonding, and the unbond ends the tenure:
    /// it clears `firstBondedAt` and the recorded floor, which restarts the
    /// governance age ramp at the next entry.
    BelowFloor,
    /// A registered operator whose recorded floor is 0. An earlier unbond
    /// below the floor ended its tenure, so the node is already inactive and
    /// this unbond changes nothing about that. A bond back to `minBond`
    /// re-enters.
    NoTenure,
}

impl Standing {
    /// Classify the retained bond against the operator's floor.
    ///
    /// `registered` is registered-set membership (`_nodes[operator].active`).
    /// A registration is an entry, so a registered operator with a recorded
    /// floor of 0 has had its tenure ended by an unbond. An operator that is
    /// not registered and has no recorded floor is held to the live
    /// `minBond`, the floor a registration would record.
    pub(crate) fn of(
        registered: bool,
        recorded_floor: U256,
        min_bond: U256,
        retained: U256,
    ) -> Self {
        let floor = if recorded_floor != U256::ZERO {
            recorded_floor
        } else if registered {
            return Self::NoTenure;
        } else {
            min_bond
        };
        if retained < floor {
            Self::BelowFloor
        } else {
            Self::Covered
        }
    }

    /// Wire label for the receipt.
    const fn label(self) -> &'static str {
        match self {
            Self::Covered => "covered",
            Self::BelowFloor => "below_floor",
            Self::NoTenure => "no_tenure",
        }
    }
}

/// Transaction hashes from a non-dry run; `None` for steps that were skipped
/// (tier already at the target) or did not land.
#[derive(Default)]
pub(crate) struct Outcome {
    pub(crate) declare: Option<B256>,
    pub(crate) unbond: Option<B256>,
}

/// Read chain state and plan the release. Every *operator-controllable*
/// precondition the contract enforces is checked here first, so an impossible
/// request fails before any transaction is sent rather than as a raw revert.
/// The exception is `whenNotPaused`, which is not pre-checked. `unbond` and
/// `declareMbps(0)` are not pausable; only the non-zero `declareMbps` leg of
/// `--to-mbps` can meet a pause, which then surfaces as a revert from
/// [`chain_ctx::send`].
pub(crate) async fn build_plan<P: Provider + Clone>(
    bond: &CapacityBond::CapacityBondInstance<P>,
    operator: Address,
    cb_addr: Address,
    request: AmountRequest,
) -> anyhow::Result<Plan> {
    let ctx = || format!("failed to read CapacityBond state at {cb_addr}");
    let prior = bond.activeBond(operator).call().await.with_context(ctx)?;
    let declared = bond.declaredMbps(operator).call().await.with_context(ctx)?;
    let min_bond = bond.minBond().call().await.with_context(ctx)?;
    // `isActive` tests the grandfathered floor, which a `minBond` change since
    // the tenure began leaves in place. A floor of 0 means no tenure runs;
    // `Standing::of` reads that against registered-set membership.
    let recorded_floor = bond
        .bondFloorAtActivation(operator)
        .call()
        .await
        .with_context(ctx)?;
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
    anyhow::ensure!(prior > U256::ZERO, "no active bond to unbond");

    // `_nodes[operator].active`, NOT the composite `isActive`: the latter also
    // requires active bond over the operator's floor, so it would report
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
        release,
        retained,
        declare_to,
        prior,
        declared_mbps,
        standing: Standing::of(active, recorded_floor, min_bond, retained),
    })
}

/// Validate a `--to-mbps` target against the currently declared tier and
/// decide whether `declareMbps` still has to run.
///
/// `target == declared` is **accepted, not rejected**, and this is what makes
/// the command converge: `execute` sends `declareMbps` then `unbond` as
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
    // that will be declared at `unbond` time.
    let curve_floor =
        |mbps: U256| async move { bond.bondRequired(mbps).call().await.with_context(ctx) };

    match request {
        AmountRequest::ToMbps(target_mbps) => {
            let target = U256::from(target_mbps);
            let declare_to = tier_step(target_mbps, declared_mbps)?;
            // Band-check ONLY when a `declareMbps` will actually be sent. The
            // band constrains that call and nothing else — `unbond`'s only
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
            // what `Standing` reports.
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
                 {declared_mbps} Mbps curve floor of {floor}; unbond would revert \
                 with BondBelowCurve. Lower the tier first with `decdn node bond --mbps \
                 <lower>`, or use `--to-mbps <lower>` to do both in one run.",
            );
            Ok((amount, retained, None))
        }
    }
}

/// Submit the plan's transactions in order. The `declareMbps` must precede
/// `unbond`, which evaluates the curve against whatever tier is current at
/// call time.
///
/// `outcome` is borrowed rather than returned so each hash is recorded in the
/// CALLER's value as it lands. Returning it would tie the record to the happy
/// path, and the interesting case here is the unhappy one: a `declareMbps` that
/// mined before `unbond` failed is a real, resumable state change the operator
/// has to be told about.
pub(crate) async fn execute<P: Provider + Clone>(
    bond: &CapacityBond::CapacityBondInstance<P>,
    plan: &Plan,
    outcome: &mut Outcome,
) -> anyhow::Result<()> {
    if let Some(mbps) = plan.declare_to {
        // `0` is the release path (#1361) and is legal precisely because it
        // bypasses the band, so the band question would be actively misleading
        // advice there.
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
        bond.unbond(plan.release),
        "unbond",
        None,
        &mut outcome.unbond,
    )
    .await?;
    Ok(())
}

/// The consequences of the planned unbond, pure so the wording is pinned by a
/// test. An unbond can cost the operator active-set membership and its tenure
/// when the retained bond is below its floor, and that is not obvious from the
/// flags alone, so [`confirm_or_bail`] writes this on every run, `--yes`
/// included: a scripted `--all --yes` can leave the node permanently inactive.
pub(crate) fn write_disclosure(w: &mut dyn io::Write, plan: &Plan) -> io::Result<()> {
    writeln!(
        w,
        "About to release {} base units to this operator (retaining {}).",
        plan.release, plan.retained,
    )?;
    match plan.standing {
        Standing::Covered => writeln!(
            w,
            "The retained bond covers this operator's bond floor, so the node stays active."
        ),
        Standing::BelowFloor => writeln!(
            w,
            "The retained bond is below this operator's bond floor, so the node becomes \
             INACTIVE now and stays inactive until it re-bonds. This unbond also ends the \
             tenure: it clears firstBondedAt, which restarts the governance age ramp at the \
             next entry."
        ),
        Standing::NoTenure => writeln!(
            w,
            "This operator is already inactive (no tenure): its recorded bond floor is 0. \
             Re-bond to minBond to re-enter."
        ),
    }
}

/// Write the plan + outcome as JSON or grep-friendly `key=value` lines. Pure
/// (`&mut impl Write`) so the output shape is unit-testable without a chain.
///
/// `dry_run` is carried explicitly rather than inferred from `submitted`, for
/// the reason `bond::write_plan` carries it: a real run whose first send
/// failed also submits nothing, so `submitted=false` alone cannot mean
/// "preview".
pub(crate) fn write_plan(
    w: &mut impl io::Write,
    p: &Plan,
    json: bool,
    o: &Outcome,
    dry_run: bool,
) -> io::Result<()> {
    let submitted = o.declare.is_some() || o.unbond.is_some();
    let tx_hex = |h: Option<&B256>| h.map(|v| format!("{v:#x}"));
    if json {
        let value = serde_json::json!({
            "submitted": submitted,
            "dry_run": dry_run,
            "capacity_bond": format!("{:#x}", p.capacity_bond),
            "prior_bond_base": p.prior.to_string(),
            "declared_mbps": p.declared_mbps,
            "release_base": p.release.to_string(),
            "retained_bond_base": p.retained.to_string(),
            "declare_to_mbps": p.declare_to,
            "below_floor": p.standing == Standing::BelowFloor,
            "standing": p.standing.label(),
            "declare_tx": tx_hex(o.declare.as_ref()),
            "unbond_tx": tx_hex(o.unbond.as_ref()),
        });
        return writeln!(w, "{value}");
    }
    // Base units: 1 TOKEN = 1e18 base units.
    writeln!(w, "capacity_bond={:#x}", p.capacity_bond)?;
    writeln!(w, "prior_bond_base={}", p.prior)?;
    writeln!(w, "declared_mbps={}", p.declared_mbps)?;
    writeln!(w, "release_base={}", p.release)?;
    writeln!(w, "retained_bond_base={}", p.retained)?;
    match p.declare_to {
        Some(mbps) => writeln!(w, "declare_to_mbps={mbps}")?,
        None => writeln!(w, "declare_to_mbps=none")?,
    }
    writeln!(w, "below_floor={}", p.standing == Standing::BelowFloor)?;
    writeln!(w, "standing={}", p.standing.label())?;
    write_tx_line(w, "declare_tx", o.declare.as_ref())?;
    write_tx_line(w, "unbond_tx", o.unbond.as_ref())?;
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests;
