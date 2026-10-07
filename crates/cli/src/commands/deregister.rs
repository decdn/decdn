//! `decdn node deregister` — leave the active set (ADR 003 § Node Registry).
//!
//! The reverse of `decdn node register`, and the first leg of a full bond exit.
//! `deregisterNode()` clears `declaredMbps`, which is what releases the
//! `bondRequired(declaredMbps)` floor `requestUnbond` enforces (#1351); the
//! complete sequence is
//!
//! ```text
//! decdn node deregister  ->  decdn node unbond --all  ->  [window]  ->  decdn node unbond
//! ```
//!
//! Two things about that sequence drive this command's shape, and both are
//! disclosed rather than left to be discovered:
//!
//! - **The bond does not move here.** `deregisterNode` touches the registration
//!   and the tier, nothing else: the deposit stays put and stays fully
//!   slashable. An operator who reads "deregister" as "exit and refund" has
//!   done only the first of three steps. This is the likeliest misconception,
//!   so the summary says so on every run.
//! - **Re-entry is not free.** The call bumps `registrationNonce[nodeId]`,
//!   which invalidates any registration signature already signed against the
//!   old nonce, and clears the tier — so coming back costs a fresh
//!   `declareMbps` + `registerNode` (the bond itself is retained, so no new
//!   funds are needed; ADR 003 § Node Registry).
//!
//! Unlike `unbond` there is no phase to select from chain state — this is one
//! transaction that either applies or reverts. There is still a pre-flight
//! read, of registry state (`getNodeByAddress` + `ejected`, NOT the composite
//! `isActive` — see `build_plan`), so the two states the operator can be in
//! are named up front instead of surfacing as a bare `NodeNotActive` revert:
//! already deregistered, or ejected.
//!
//! Both exit the same way — `decdn node unbond --all`, which releases the tier
//! via `declareMbps(0)` for any inactive operator (#1361) — but they need
//! different advice about RE-ENTRY, which is why the states are distinguished:
//! a blacklist-ejected operator's `registerNode` reverts until governance calls
//! `unEjectNode`, so pointing them at `decdn node register` would be wrong.

use std::io::{self, IsTerminal, Write};
use std::path::Path;

use alloy::primitives::{Address, B256};
use anyhow::Context;
use decdn_common::cli;
use decdn_incentive::capacity_bond::CapacityBond;

use crate::commands::chain_ctx;

/// Entry point for `decdn node deregister`.
pub async fn run(args: &cli::DeregisterArgs, global_config: Option<&Path>) -> anyhow::Result<()> {
    let config_path = args.chain.common.config.as_deref().or(global_config);
    let file = chain_ctx::load_optional_config(config_path)?;
    let resolved = chain_ctx::resolve(&args.chain, &file)?;
    let cb_addr = resolved.capacity_bond_address;

    let signer = chain_ctx::load_operator_signer(&args.chain.common, &resolved.keystore).await?;
    let operator = signer.address();
    let provider = decdn_client::provider::build_provider(&resolved.rpc_url, &signer)?;
    let bond = CapacityBond::new(cb_addr, &provider);

    let plan = build_plan(&bond, operator, cb_addr).await?;

    let json = args.chain.common.json;
    if args.chain.common.dry_run {
        let mut out = io::stdout().lock();
        write_plan(&mut out, &plan, json, None, true).context("failed to write dry-run output")?;
        return Ok(());
    }

    // Pre-flight rather than a revert: `NodeNotActive` alone does not say WHICH
    // of the two inactive states the operator is in, and the two have different
    // exits.
    ensure_active(&plan)?;
    confirm_or_bail(&plan, args.yes)?;

    // Caller-owned slot (#1355): from the send onward the transaction is
    // broadcast and may take effect, so an unreadable receipt must still print
    // the hash rather than read as "nothing happened".
    let mut tx = None;
    let result = chain_ctx::send(
        bond.deregisterNode(),
        "deregisterNode",
        Some("the node must be active — a second deregistration reverts NodeNotActive"),
        &mut tx,
    )
    .await;

    let mut out = io::stdout().lock();
    write_plan(&mut out, &plan, json, tx.as_ref(), false).context("failed to write result")?;
    drop(out);

    result.map(|_| ())
}

/// What `deregisterNode` will do, read from the chain before anything is sent.
/// Owned so [`write_plan`] and [`confirm_or_bail`] are testable without one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Plan {
    pub(crate) capacity_bond: Address,
    /// Whether the node is currently in the active set — the gate
    /// `deregisterNode` itself enforces.
    pub(crate) active: bool,
    /// Set when the operator was removed by the contract rather than by
    /// choice. Distinguishes the two `!active` states, which need different
    /// advice: an ejected operator has no `deregisterNode` to make.
    pub(crate) ejected: bool,
    /// The tier this call will clear, and therefore the `bondRequired` floor it
    /// releases. `0` when none was ever declared.
    pub(crate) declared_mbps: u64,
    /// Bond that stays deposited and slashable afterwards, in base units.
    pub(crate) retained_bond: alloy::primitives::U256,
}

/// Read the operator's registry state.
///
/// `isActive` is the contract's own composite predicate (registered, bonded
/// over `minBond`, no unbonding request in flight), so it is NOT what
/// `deregisterNode` gates on — that reads `_nodes[operator].active` alone. An
/// operator mid-unbonding is `isActive == false` yet still deregisterable. The
/// registered-set membership read below is the precise signal; `isActive` is
/// not consulted.
async fn build_plan<P: alloy::providers::Provider + Clone>(
    bond: &CapacityBond::CapacityBondInstance<P>,
    operator: Address,
    capacity_bond: Address,
) -> anyhow::Result<Plan> {
    let ctx = |what: &str| format!("failed to read {what} from CapacityBond at {capacity_bond}");

    let info = bond
        .getNodeByAddress(operator)
        .call()
        .await
        .with_context(|| ctx("getNodeByAddress"))?;
    let ejected = bond
        .ejected(operator)
        .call()
        .await
        .with_context(|| ctx("ejected"))?;
    let declared = bond
        .declaredMbps(operator)
        .call()
        .await
        .with_context(|| ctx("declaredMbps"))?;
    let retained_bond = bond
        .activeBond(operator)
        .call()
        .await
        .with_context(|| ctx("activeBond"))?;

    Ok(Plan {
        capacity_bond,
        active: info.active,
        ejected,
        // Saturating rather than `try_into`: the tier is bounded by the
        // contract's own capacity band, so a value past `u64` is unreachable —
        // and failing the whole command on an unreachable read would be worse
        // than reporting the clamp.
        declared_mbps: declared.saturating_to(),
        retained_bond,
    })
}

/// Refuse a run that the contract would revert, naming the state and its exit.
/// Split from `run` so both branches are testable without a chain.
fn ensure_active(plan: &Plan) -> anyhow::Result<()> {
    if plan.active {
        return Ok(());
    }
    anyhow::ensure!(
        !plan.ejected,
        "this operator was ejected by the contract, so there is no registration to \
         deregister — `deregisterNode` would revert NodeNotActive. Exit directly with \
         `decdn node unbond --all`, which releases the declared tier for an inactive \
         operator and starts the unbonding window."
    );
    anyhow::bail!(
        "this operator is not in the active node set — either it never registered, or it \
         has already been deregistered. `decdn node register` re-enters; \
         `decdn node unbond --all` exits."
    )
}

/// What the confirmation gate decides, before any IO. Split from
/// [`confirm_or_bail`] the way `unbond::decide_confirmation` is, so the gate is
/// testable without a TTY.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Confirmation {
    /// `--yes` supplied: disclose the consequences, but skip the prompt.
    Bypassed,
    /// Ask on the terminal.
    Prompt,
    /// Headless and unconfirmed: refuse rather than assume consent.
    NeedFlag,
}

/// Decide whether to prompt. Unlike `unbond` there is no `NotNeeded` arm —
/// every run of this command that reaches the gate is submitting the
/// deregistration, and there is no phase (`unbond`'s withdraw) that merely
/// returns TOKEN.
pub(crate) const fn decide_confirmation(yes: bool, interactive: bool) -> Confirmation {
    if yes {
        Confirmation::Bypassed
    } else if interactive {
        Confirmation::Prompt
    } else {
        Confirmation::NeedFlag
    }
}

/// Disclose the consequences and, on a terminal, wait for `y`.
///
/// `--yes` suppresses the *prompt*, never the disclosure — "don't ask" must not
/// silently become "don't tell", the same rule `unbond` follows. The bond line
/// is the load-bearing one: it is the misconception this command most invites.
fn confirm_or_bail(plan: &Plan, yes: bool) -> anyhow::Result<()> {
    // Interactive only when BOTH streams are terminals: the warning goes to
    // stderr and the answer is read from stdin, so if either is redirected the
    // operator can't see what they'd be agreeing to (same rule as `unbond`).
    let interactive = io::stdin().is_terminal() && io::stderr().is_terminal();
    let decision = decide_confirmation(yes, interactive);

    let mut err = io::stderr().lock();
    write_disclosure(&mut err, plan)?;

    match decision {
        // Disclosed above; the operator asked not to be asked.
        Confirmation::Bypassed => Ok(()),
        Confirmation::NeedFlag => anyhow::bail!(
            "deregistration not confirmed: no interactive terminal detected — re-run with \
             `--yes` (or `--dry-run` to preview)."
        ),
        Confirmation::Prompt => {
            write!(err, "Proceed? [y/N] ")?;
            err.flush()?;
            let mut line = String::new();
            io::stdin().read_line(&mut line)?;
            // Allowlist, not denylist: an empty line (Ctrl-D / EOF) lands on the
            // deny side rather than being read as assent.
            let answer = line.trim().to_ascii_lowercase();
            anyhow::ensure!(
                answer == "y" || answer == "yes",
                "deregistration cancelled."
            );
            Ok(())
        }
    }
}

/// The consequence disclosure, pure so its wording is pinned by a test.
pub(crate) fn write_disclosure(w: &mut impl io::Write, plan: &Plan) -> io::Result<()> {
    writeln!(
        w,
        "About to deregister this node. It leaves the active node set immediately and stops \
         being selected for delivery."
    )?;
    // The bond first, because "deregister" reads as "exit and refund".
    writeln!(
        w,
        "Your bond of {} base units is NOT returned by this call. It stays deposited and \
         fully slashable. To withdraw it, follow with `decdn node unbond --all`, wait out \
         the unbonding window, then re-run `decdn node unbond`.",
        plan.retained_bond,
    )?;
    if plan.declared_mbps != 0 {
        writeln!(
            w,
            "The declared tier of {} Mbps is cleared, which is what releases the bond curve's \
             floor and makes that full withdrawal possible.",
            plan.declared_mbps,
        )?;
    }
    writeln!(
        w,
        "Re-entry needs a fresh `decdn node bond --mbps` + `decdn node register`: this bumps \
         the registration nonce, invalidating any registration signature already produced."
    )
}

/// Write the plan + outcome as JSON or grep-friendly `key=value` lines. Pure
/// (`&mut impl Write`) so the output shape is unit-testable without a chain.
///
/// `dry_run` is carried explicitly rather than inferred from `tx.is_none()`,
/// for the reason `unbond::write_plan` carries it: a real run whose send failed
/// also has no tx, so the absence alone cannot mean "preview".
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
            "active": p.active,
            "ejected": p.ejected,
            "declared_mbps": p.declared_mbps,
            "retained_bond_base": p.retained_bond.to_string(),
            "deregister_tx": tx_hex,
        });
        return writeln!(w, "{value}");
    }
    // Base units: 1 TOKEN = 1e18 base units.
    writeln!(w, "capacity_bond={:#x}", p.capacity_bond)?;
    writeln!(w, "active={}", p.active)?;
    writeln!(w, "ejected={}", p.ejected)?;
    writeln!(w, "declared_mbps={}", p.declared_mbps)?;
    writeln!(w, "retained_bond_base={}", p.retained_bond)?;
    match tx_hex {
        Some(h) => writeln!(w, "deregister_tx={h}")?,
        None => writeln!(w, "deregister_tx=skipped")?,
    }
    writeln!(w, "submitted={} dry_run={dry_run}", tx.is_some())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests;
