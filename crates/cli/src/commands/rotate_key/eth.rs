//! `decdn node rotate-key --key eth` — migrate the operator's Ethereum identity
//! (`appendix-operator-key-rotation.md` § EOA → EOA migration).
//!
//! There is no rebinding API for the on-chain address. The address *is* the
//! stake owner — bond, channels, `firstBondedAt`, and the node binding all key
//! on it — so rotating it means moving the whole identity: deregister, request
//! unbonding, wait out the full window, withdraw, then re-bond and re-register
//! from the new address. That is five transactions spread across days, not one
//! command's worth of work, which is what shapes everything below.
//!
//! # Phase is read from the chain, never from a flag
//!
//! The same shape `decdn node unbond` uses, for the same reason: the operator
//! runs this command repeatedly over the window, and a flag saying which step
//! they are on would be a second source of truth that can disagree with the
//! chain. So each invocation reads the registry and picks the next action, and a
//! run that lost its receipt converges on re-run rather than double-submitting.
//!
//! # The tier does not survive the window
//!
//! `deregisterNode` clears `declaredMbps` — that is precisely what releases the
//! `bondRequired` floor and makes a full withdrawal reachable. Region and
//! multiaddrs *do* survive on the registration record, so they are carried
//! forward automatically; the tier cannot be, so the deregister phase prints it
//! and the re-onboarding phase requires `--mbps` when the chain no longer knows
//! it.
//!
//! # The node id changes too
//!
//! Not a choice. `deregisterNode` deactivates but does **not** clear
//! `nodeIdToAddress[nodeId]`, so the old address keeps the binding and
//! `registerNode` from the new address reverts `NodeIdAlreadyBound`. The
//! re-onboarding phase therefore generates a fresh node key, which is what the
//! runbook's step 8 says to do. An operator who needs the *original* id back
//! (for reputation continuity) has to run `--key iroh` on the old address first,
//! rebinding it to a throwaway id to free the original — a chained procedure
//! this command does not automate, and names in its disclosure.
//!
//! # Why the key ordering is looser than the iroh path's
//!
//! The `iroh` path commits `node.secret` only after the bind confirms, because
//! any window where the daemon serves under an unbound key is an unslashable
//! node. Here that window does not exist: re-onboarding runs against an operator
//! that has already deregistered and withdrawn, so it has no bond to be
//! unslashable about and is not serving. Committing the fresh key up front is
//! therefore safe, and it makes the phase resumable — a failed `registerNode`
//! retries against the key already on disk instead of minting a third one.

use std::io;
use std::path::Path;

use alloy::primitives::{Address, B256, U256};
use alloy::providers::Provider;
use alloy::rpc::types::BlockId;
use anyhow::Context;
use decdn_common::cli;
use decdn_common::identity;
use decdn_incentive::capacity_bond::CapacityBond;
use decdn_incentive::node_register;

use crate::commands::rotate_key::confirm_or_bail;
use crate::commands::{bond, chain_ctx, register, terms};

/// Entry point for `decdn node rotate-key --key eth`.
pub(crate) async fn run(
    args: &cli::RotateKeyArgs,
    global_config: Option<&Path>,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        !args.bind_existing,
        "--bind-existing applies to `--key iroh` only. The Ethereum path always registers a FRESH \
         node id: `deregisterNode` leaves `nodeIdToAddress` pointing at the old address, so \
         re-registering the existing id from a new address reverts NodeIdAlreadyBound."
    );

    let config_path = args.chain.common.config.as_deref().or(global_config);
    let file = chain_ctx::load_optional_config(config_path)?;
    let resolved = chain_ctx::resolve(&args.chain, &file)?;
    let cb_addr = resolved.capacity_bond_address;

    let old_signer =
        chain_ctx::load_operator_signer(&args.chain.common, &resolved.keystore).await?;
    let old_operator = old_signer.address();
    let provider = decdn_client_pull::provider::build_provider(&resolved.rpc_url, &old_signer)?;

    let plan = build_plan(&provider, cb_addr, resolved.chain_id, old_operator, args).await?;
    let json = args.chain.common.json;

    if args.chain.common.dry_run {
        let mut out = io::stdout().lock();
        return write_plan(&mut out, &plan, json, &Outcome::default(), true)
            .context("failed to write dry-run output");
    }

    // Nothing to submit while the window matures. Reported and then failed, not
    // silently `Ok`: a wrapper doing `if decdn node rotate-key …; then
    // mark_done; fi` would read a zero exit as "the migration completed".
    if let Phase::Waiting { unlock_at, now, .. } = plan.phase {
        let mut out = io::stdout().lock();
        write_plan(&mut out, &plan, json, &Outcome::default(), false)
            .context("failed to write result")?;
        drop(out);
        anyhow::bail!(
            "unbonding request is still maturing: unlocks at {unlock_at} (in {}s). Re-run this \
             command after that to withdraw, then again with --new-keystore to re-onboard.",
            unlock_at.saturating_sub(now),
        );
    }

    if plan.phase.needs_confirmation() {
        confirm_or_bail(|w| write_disclosure(w, &plan), "migration", args.yes)?;
    }

    // Caller-owned accumulator (#1355): a partial sequence must survive the
    // error, because each landed transaction is a real state change the
    // operator has to know about before re-running.
    let mut outcome = Outcome::default();
    let result = execute(&provider, &plan, args, &resolved, &mut outcome).await;

    let mut out = io::stdout().lock();
    write_plan(&mut out, &plan, json, &outcome, false).context("failed to write result")?;
    drop(out);
    result
}

/// Which step of the migration this invocation performs, decided from chain
/// state rather than a flag.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Phase {
    /// Still in the active set: leave it. Also the last moment the declared
    /// tier is readable, which is why it travels with the phase.
    Deregister { declared_mbps: u64 },
    /// Deregistered with bond still posted and no request in flight: start the
    /// unbonding window over the whole bond.
    Request { amount: U256 },
    /// A request exists but has not matured — nothing to submit.
    Waiting {
        amount: U256,
        unlock_at: u64,
        now: u64,
    },
    /// A matured request is withdrawable.
    Withdraw { amount: U256 },
    /// Bond fully exited: bond, declare, and register from the new address.
    Reonboard {
        new_operator: Address,
        mbps: u64,
        region: String,
        multiaddrs: Vec<String>,
    },
    /// The new address is already active — nothing left to do.
    Complete { new_operator: Address },
}

impl Phase {
    /// Wire label, and the key a machine consumer switches on.
    const fn label(&self) -> &'static str {
        match *self {
            Self::Deregister { .. } => "deregister",
            Self::Request { .. } => "request",
            Self::Waiting { .. } => "waiting",
            Self::Withdraw { .. } => "withdraw",
            Self::Reonboard { .. } => "reonboard",
            Self::Complete { .. } => "complete",
        }
    }

    /// Whether this phase asks before submitting.
    ///
    /// The three that do are the ones that cost something irreversible: leaving
    /// the active set, locking the bond for the window, and spending TOKEN from
    /// a new address. `Withdraw` only returns the operator's own money, and
    /// `Waiting`/`Complete` submit nothing — prompting there would train the
    /// operator to confirm reflexively.
    const fn needs_confirmation(&self) -> bool {
        matches!(
            *self,
            Self::Deregister { .. } | Self::Request { .. } | Self::Reonboard { .. }
        )
    }

    /// The address being migrated onto, once it is known.
    const fn new_operator(&self) -> Option<Address> {
        match *self {
            Self::Reonboard { new_operator, .. } | Self::Complete { new_operator } => {
                Some(new_operator)
            }
            _ => None,
        }
    }
}

/// Transaction hashes from a non-dry run; `None` for steps this phase did not
/// reach.
#[derive(Default)]
pub(crate) struct Outcome {
    pub(crate) deregister: Option<B256>,
    pub(crate) request: Option<B256>,
    pub(crate) withdraw: Option<B256>,
    pub(crate) approve: Option<B256>,
    pub(crate) bond: Option<B256>,
    pub(crate) declare: Option<B256>,
    pub(crate) register: Option<B256>,
    /// Node id minted for the new address, once the re-onboarding phase has
    /// committed it.
    pub(crate) new_node_id: Option<B256>,
}

/// The migration's state, read from the chain before anything is sent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Plan {
    pub(crate) capacity_bond: Address,
    pub(crate) old_operator: Address,
    pub(crate) chain_id: u64,
    pub(crate) phase: Phase,
    /// Bond still posted to the old address.
    pub(crate) active_bond: U256,
    /// Governable unbonding window (seconds), for the downtime report.
    pub(crate) unbonding_period: u64,
    /// The old address's `firstBondedAt` — the age-ramp anchor this migration
    /// destroys, and the number that makes the cost concrete.
    pub(crate) first_bonded_at: u64,
}

/// Read registry state and pick the phase.
async fn build_plan<P: Provider + Clone>(
    provider: &P,
    cb_addr: Address,
    chain_id: u64,
    old_operator: Address,
    args: &cli::RotateKeyArgs,
) -> anyhow::Result<Plan> {
    let bond_contract = CapacityBond::new(cb_addr, provider);
    let ctx = |what: &str| format!("failed to read {what} from CapacityBond at {cb_addr}");

    let info = bond_contract
        .getNodeByAddress(old_operator)
        .call()
        .await
        .with_context(|| ctx("getNodeByAddress"))?;
    let active_bond = bond_contract
        .activeBond(old_operator)
        .call()
        .await
        .with_context(|| ctx("activeBond"))?;
    let pending = bond_contract
        .unbondingOf(old_operator)
        .call()
        .await
        .with_context(|| ctx("unbondingOf"))?;
    let declared = bond_contract
        .declaredMbps(old_operator)
        .call()
        .await
        .with_context(|| ctx("declaredMbps"))?;
    let unbonding_period = bond_contract
        .unbondingPeriod()
        .call()
        .await
        .with_context(|| ctx("unbondingPeriod"))?;
    let first_bonded_at = bond_contract
        .firstBondedAt(old_operator)
        .call()
        .await
        .with_context(|| ctx("firstBondedAt"))?;

    let phase = select_phase(
        provider,
        &bond_contract,
        PhaseInputs {
            active: info.active,
            active_bond,
            pending_amount: pending.amount,
            pending_unlock_at: pending.unlockAt,
            // Saturating rather than `try_into`: the tier is bounded by the
            // contract's own capacity band, so a value past `u64` is
            // unreachable, and failing the whole command on an unreachable read
            // would be worse than reporting the clamp.
            declared_mbps: declared.saturating_to(),
            region_hint: info.regionHint.clone(),
            packed_multiaddrs: info.multiaddrs.to_vec(),
        },
        args,
    )
    .await?;

    Ok(Plan {
        capacity_bond: cb_addr,
        old_operator,
        chain_id,
        phase,
        active_bond,
        unbonding_period: unbonding_period.saturating_to(),
        first_bonded_at,
    })
}

/// The registry reads [`select_phase`] decides from, bundled so the selection
/// signature stays readable.
struct PhaseInputs {
    active: bool,
    active_bond: U256,
    pending_amount: U256,
    pending_unlock_at: U256,
    declared_mbps: u64,
    region_hint: String,
    packed_multiaddrs: Vec<u8>,
}

/// Pick the next action from registry state, in the runbook's order.
async fn select_phase<P: Provider + Clone, Q: Provider>(
    provider: &P,
    bond_contract: &CapacityBond::CapacityBondInstance<Q>,
    st: PhaseInputs,
    args: &cli::RotateKeyArgs,
) -> anyhow::Result<Phase> {
    if st.active {
        return Ok(Phase::Deregister {
            declared_mbps: st.declared_mbps,
        });
    }
    if st.pending_amount == U256::ZERO {
        if st.active_bond > U256::ZERO {
            return Ok(Phase::Request {
                amount: st.active_bond,
            });
        }
    } else {
        // Head timestamp rather than the local clock, so the comparison uses
        // the same clock `unbond()` will.
        let now = head_timestamp(provider).await?;
        let unlock_at = st.pending_unlock_at.saturating_to::<u64>();
        if U256::from(now) < st.pending_unlock_at {
            return Ok(Phase::Waiting {
                amount: st.pending_amount,
                unlock_at,
                now,
            });
        }
        return Ok(Phase::Withdraw {
            amount: st.pending_amount,
        });
    }

    // Bond fully exited. Everything past here needs the new keystore.
    let new_keystore = args.new_keystore.as_deref().ok_or_else(|| {
        anyhow::anyhow!(
            "the bond is fully withdrawn — the remaining step is to re-bond and re-register from \
             the NEW address, so pass `--new-keystore <PATH>` (generate one with `decdn key-gen \
             --output-dir <dir>`). Fund it with TOKEN and a little ETH for gas first."
        )
    })?;
    let new_signer = chain_ctx::load_signer_with_password_file(
        args.chain.common.keystore_password_file.as_deref(),
        new_keystore,
    )
    .await
    .context("failed to load the new keystore")?;
    let new_operator = new_signer.address();

    if bond_contract
        .isActive(new_operator)
        .call()
        .await
        .context("failed to read isActive for the new operator")?
    {
        return Ok(Phase::Complete { new_operator });
    }

    // The tier is the one thing `deregisterNode` destroys, so it has to be
    // supplied once the chain has forgotten it.
    let mbps = args
        .mbps
        .or_else(|| (st.declared_mbps != 0).then_some(st.declared_mbps))
        .ok_or_else(|| {
            anyhow::anyhow!(
                "`deregisterNode` cleared the declared capacity tier, so it cannot be read back — \
                 pass `--mbps <MBPS>` with the tier to declare on the new address (the deregister \
                 phase printed the old one)."
            )
        })?;

    // Region and multiaddrs DO survive on the registration record, so they are
    // carried forward rather than demanded again; the flags override.
    let region = args
        .region
        .clone()
        .or_else(|| (!st.region_hint.is_empty()).then(|| st.region_hint.clone()))
        .ok_or_else(|| {
            anyhow::anyhow!(
                "no region to register with: the old record carries none — pass `--region <CODE>`"
            )
        })?;
    let multiaddrs = if args.multiaddrs.is_empty() {
        node_register::unpack_multiaddrs(&st.packed_multiaddrs)
            .context("failed to decode the old registration's multiaddrs; pass --multiaddr")?
    } else {
        args.multiaddrs.clone()
    };

    Ok(Phase::Reonboard {
        new_operator,
        mbps,
        region,
        multiaddrs,
    })
}

/// Head block timestamp, used to decide whether the window has matured. Read
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

/// Submit the phase's transactions.
async fn execute<P: Provider + Clone>(
    provider: &P,
    plan: &Plan,
    args: &cli::RotateKeyArgs,
    resolved: &chain_ctx::Resolved,
    outcome: &mut Outcome,
) -> anyhow::Result<()> {
    let cb_addr = plan.capacity_bond;
    let bond_contract = CapacityBond::new(cb_addr, provider);

    match &plan.phase {
        Phase::Deregister { .. } => {
            chain_ctx::send(
                bond_contract.deregisterNode(),
                "deregisterNode",
                Some("the node must be active — a second deregistration reverts NodeNotActive"),
                &mut outcome.deregister,
            )
            .await?;
        }
        Phase::Request { amount } => {
            chain_ctx::send(
                bond_contract.requestUnbond(*amount),
                "requestUnbond",
                Some(
                    "the declared tier must already be cleared; run the deregister phase of this \
                     command first",
                ),
                &mut outcome.request,
            )
            .await?;
        }
        Phase::Withdraw { .. } => {
            chain_ctx::send(
                bond_contract.unbond(),
                "unbond",
                None,
                &mut outcome.withdraw,
            )
            .await?;
        }
        Phase::Reonboard {
            mbps,
            region,
            multiaddrs,
            ..
        } => {
            reonboard(args, resolved, plan, *mbps, region, multiaddrs, outcome).await?;
        }
        Phase::Complete { .. } => {}
        // `run` bails on `Waiting` before reaching here. Kept loud rather than a
        // no-op for the reason `unbond::execute`'s twin is: a silent `Ok` would
        // report `submitted=false` and exit 0.
        Phase::Waiting { .. } => anyhow::bail!(
            "internal: reached `execute` with a still-maturing request; nothing was submitted"
        ),
    }
    Ok(())
}

/// Bond, declare, and register from the NEW address.
///
/// Runs against a second provider built on the new signer — every transaction
/// here must originate from the address being migrated onto, and the ed25519
/// ownership proof commits to it, so reusing the old signer's provider would
/// build a proof the contract rejects.
#[allow(clippy::too_many_arguments)]
async fn reonboard(
    args: &cli::RotateKeyArgs,
    resolved: &chain_ctx::Resolved,
    plan: &Plan,
    mbps: u64,
    region: &str,
    multiaddrs: &[String],
    outcome: &mut Outcome,
) -> anyhow::Result<()> {
    let cb_addr = plan.capacity_bond;
    let keystore = args
        .new_keystore
        .as_deref()
        .context("internal: reonboard without --new-keystore")?;
    let new_signer = chain_ctx::load_signer_with_password_file(
        args.chain.common.keystore_password_file.as_deref(),
        keystore,
    )
    .await
    .context("failed to load the new keystore")?;
    let new_provider = decdn_client_pull::provider::build_provider(&resolved.rpc_url, &new_signer)?;
    let bond_contract = CapacityBond::new(cb_addr, &new_provider);
    let new_operator = new_signer.address();

    // ADR 019 § Terms Acceptance — re-registration from a new address is a
    // FRESH registration, so it re-accepts terms. (`--key iroh` never does: a
    // rebind signs the terms-free `BindNodeId` payload.)
    let terms_hash = bond_contract
        .currentTermsHash()
        .call()
        .await
        .with_context(|| {
            format!("failed to read currentTermsHash from CapacityBond at {cb_addr}")
        })?;
    terms::ensure_accepted_async(terms_hash, args.accept_terms).await?;

    // Fresh node key. The old id is still bound to the OLD address
    // (`deregisterNode` does not clear `nodeIdToAddress`), so re-registering it
    // here would revert `NodeIdAlreadyBound`. Committed before the transactions
    // rather than after, because `register::submit_registration` reads the key
    // from disk and because — unlike the iroh path — there is no serving node to
    // strand: this operator has already deregistered and withdrawn.
    let key_path = identity::key_path(&resolved.data_dir);
    let staged = identity::stage_node_key(&resolved.data_dir).with_context(|| {
        format!(
            "failed to stage a new node key in {}",
            resolved.data_dir.display()
        )
    })?;
    let new_node_id = B256::from_slice(staged.public().as_bytes());
    staged.commit().with_context(|| {
        format!(
            "failed to install the new node key at {}; nothing was submitted",
            key_path.display()
        )
    })?;
    outcome.new_node_id = Some(new_node_id);

    // Reuse `decdn node bond`'s plan + execute so the approve → bond →
    // declareMbps sequence, its balance pre-check, and its idempotent
    // skip-what-already-landed behaviour are the same code, not a second
    // implementation that can drift.
    let mbps_u256 = U256::from(mbps);
    let bond_plan = bond::build_plan(&bond_contract, new_operator, mbps_u256, cb_addr).await?;
    let mut bond_outcome = bond::Outcome::default();
    let bond_result = bond::execute(
        &bond_contract,
        &new_provider,
        &bond_plan,
        new_operator,
        cb_addr,
        mbps_u256,
        &mut bond_outcome,
    )
    .await;
    outcome.approve = bond_outcome.approve;
    outcome.bond = bond_outcome.bond;
    outcome.declare = bond_outcome.declare;
    bond_result?;

    let mut register_tx = None;
    let registration = register::submit_registration(
        &new_provider,
        &new_signer,
        &resolved.data_dir,
        cb_addr,
        plan.chain_id,
        region,
        multiaddrs,
        terms_hash,
        false,
        &mut register_tx,
    )
    .await;
    outcome.register = register_tx;
    registration.map(|_| ())
}

/// The consequence disclosure, pure so its wording is pinned by a test.
pub(crate) fn write_disclosure(w: &mut dyn io::Write, p: &Plan) -> io::Result<()> {
    match &p.phase {
        Phase::Deregister { declared_mbps } => {
            writeln!(
                w,
                "About to start an Ethereum-key migration by leaving the active node set. The node \
                 stops being selected for delivery immediately."
            )?;
            writeln!(
                w,
                "RECORD THIS: the declared tier is {declared_mbps} Mbps. This call clears it and \
                 it cannot be read back afterwards — you must pass `--mbps {declared_mbps}` when \
                 re-onboarding on the new address."
            )?;
            writeln!(
                w,
                "The bond of {} base units is NOT returned by this call. Re-run this command to \
                 start the {}-day unbonding window, again after it matures to withdraw, and once \
                 more with --new-keystore to re-onboard.",
                p.active_bond,
                p.unbonding_period / 86_400,
            )?;
        }
        Phase::Request { amount } => {
            writeln!(
                w,
                "About to lock {amount} base units for the full {}-day unbonding window. The bond \
                 stays SLASHABLE for the whole window — keep monitoring.",
                p.unbonding_period / 86_400,
            )?;
        }
        Phase::Reonboard {
            new_operator, mbps, ..
        } => {
            writeln!(
                w,
                "About to re-bond and re-register at {mbps} Mbps from {new_operator:#x}. This \
                 spends TOKEN from the NEW address."
            )?;
            writeln!(
                w,
                "A FRESH node id is generated and registered. The original cannot be reused: \
                 `deregisterNode` leaves it bound to the old address. To carry it across, rebind \
                 it away with `decdn node rotate-key --key iroh` on the OLD address first, then \
                 re-run this."
            )?;
            writeln!(
                w,
                "firstBondedAt resets from {} to now, so the governance age-ramp restarts and \
                 rebuilds to full weight over six months. Peer reputation against the old node id \
                 is stranded.",
                p.first_bonded_at,
            )?;
            writeln!(
                w,
                "Keep the OLD Ethereum keystore reachable until every channel that pinned it as \
                 voucherSigner has settled or expired — it is the only key that can sign a further \
                 voucher or countersign a cooperative close on those."
            )?;
        }
        // The remaining phases either submit nothing or only return the
        // operator's own money, and are gated out by `needs_confirmation`.
        Phase::Waiting { .. } | Phase::Withdraw { .. } | Phase::Complete { .. } => {}
    }
    Ok(())
}

/// Write the plan + outcome as JSON or grep-friendly `key=value` lines. Pure
/// (`&mut impl Write`) so the output shape is unit-testable without a chain.
///
/// `phase` is what a machine consumer switches on; the transaction keys that do
/// not apply to a phase are emitted as `skipped`/`null` rather than omitted, so
/// a consumer never has to distinguish "absent" from "did not run".
pub(crate) fn write_plan(
    w: &mut impl io::Write,
    p: &Plan,
    json: bool,
    o: &Outcome,
    dry_run: bool,
) -> io::Result<()> {
    let phase = p.phase.label();
    let submitted = o.deregister.is_some()
        || o.request.is_some()
        || o.withdraw.is_some()
        || o.bond.is_some()
        || o.register.is_some();
    let hex = |v: Option<B256>| v.map(|h| format!("{h:#x}"));
    let remaining = match p.phase {
        Phase::Waiting { unlock_at, now, .. } => unlock_at.saturating_sub(now),
        _ => 0,
    };
    if json {
        let value = serde_json::json!({
            "submitted": submitted,
            "dry_run": dry_run,
            "key": "eth",
            "phase": phase,
            "capacity_bond": format!("{:#x}", p.capacity_bond),
            "old_operator": format!("{:#x}", p.old_operator),
            "new_operator": p.phase.new_operator().map(|a| format!("{a:#x}")),
            "active_bond_base": p.active_bond.to_string(),
            "unbonding_period_secs": p.unbonding_period,
            "first_bonded_at": p.first_bonded_at,
            "remaining_secs": remaining,
            "new_node_id": hex(o.new_node_id),
            "deregister_tx": hex(o.deregister),
            "request_tx": hex(o.request),
            "withdraw_tx": hex(o.withdraw),
            "approve_tx": hex(o.approve),
            "bond_tx": hex(o.bond),
            "declare_tx": hex(o.declare),
            "register_tx": hex(o.register),
        });
        return writeln!(w, "{value}");
    }
    writeln!(w, "key=eth")?;
    writeln!(w, "phase={phase}")?;
    writeln!(w, "capacity_bond={:#x}", p.capacity_bond)?;
    writeln!(w, "old_operator={:#x}", p.old_operator)?;
    if let Some(new_operator) = p.phase.new_operator() {
        writeln!(w, "new_operator={new_operator:#x}")?;
    }
    writeln!(w, "active_bond_base={}", p.active_bond)?;
    writeln!(w, "unbonding_period_secs={}", p.unbonding_period)?;
    writeln!(w, "first_bonded_at={}", p.first_bonded_at)?;
    writeln!(w, "remaining_secs={remaining}")?;
    for (key, tx) in [
        ("new_node_id", o.new_node_id),
        ("deregister_tx", o.deregister),
        ("request_tx", o.request),
        ("withdraw_tx", o.withdraw),
        ("approve_tx", o.approve),
        ("bond_tx", o.bond),
        ("declare_tx", o.declare),
        ("register_tx", o.register),
    ] {
        match tx {
            Some(h) => writeln!(w, "{key}={h:#x}")?,
            None => writeln!(w, "{key}=skipped")?,
        }
    }
    writeln!(w, "submitted={submitted} dry_run={dry_run}")
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn plan(phase: Phase) -> Plan {
        Plan {
            capacity_bond: Address::repeat_byte(0xCB),
            old_operator: Address::repeat_byte(0x0E),
            chain_id: 31337,
            phase,
            active_bond: U256::from(50_000u64),
            unbonding_period: 14 * 86_400,
            first_bonded_at: 1_700_000_000,
        }
    }

    fn rendered(p: &Plan, json: bool, o: &Outcome, dry_run: bool) -> String {
        let mut buf = Vec::new();
        write_plan(&mut buf, p, json, o, dry_run).expect("write to a Vec cannot fail");
        String::from_utf8(buf).expect("output is ASCII")
    }

    fn disclosure(p: &Plan) -> String {
        let mut buf = Vec::new();
        write_disclosure(&mut buf, p).expect("write to a Vec cannot fail");
        String::from_utf8(buf).expect("output is ASCII")
    }

    fn reonboard_phase() -> Phase {
        Phase::Reonboard {
            new_operator: Address::repeat_byte(0x0F),
            mbps: 5_000,
            region: "DE".to_string(),
            multiaddrs: vec!["/ip4/203.0.113.10/udp/4433/quic-v1".to_string()],
        }
    }

    /// The tier is destroyed by the call being confirmed and cannot be read
    /// back, so the disclosure is the operator's only chance to record it.
    #[test]
    fn deregister_disclosure_prints_the_tier_to_carry_forward() {
        let s = disclosure(&plan(Phase::Deregister {
            declared_mbps: 5_000,
        }));
        assert!(s.contains("RECORD THIS"), "{s}");
        assert!(
            s.contains("--mbps 5000"),
            "names the exact flag to re-pass: {s}"
        );
        assert!(
            s.contains("NOT returned"),
            "the bond does not move here: {s}"
        );
    }

    /// The node id changing is the surprise of this path, and the workaround is
    /// non-obvious enough that omitting it would strand anyone who cares about
    /// reputation continuity.
    #[test]
    fn reonboard_disclosure_names_the_fresh_node_id_and_the_carry_over_route() {
        let s = disclosure(&plan(reonboard_phase()));
        assert!(s.contains("FRESH node id"), "{s}");
        assert!(s.contains("--key iroh"), "names the carry-over route: {s}");
        assert!(
            s.contains("firstBondedAt resets"),
            "names the real cost: {s}"
        );
        assert!(
            s.contains("voucherSigner"),
            "the old keystore retention obligation is easy to miss: {s}"
        );
    }

    /// Prompting on a phase that only returns the operator's own money — or
    /// submits nothing at all — trains reflexive confirmation, which is how the
    /// prompts that matter stop being read.
    #[test]
    fn only_the_costly_phases_prompt() {
        assert!(Phase::Deregister { declared_mbps: 1 }.needs_confirmation());
        assert!(
            Phase::Request {
                amount: U256::from(1u64)
            }
            .needs_confirmation()
        );
        assert!(reonboard_phase().needs_confirmation());

        assert!(
            !Phase::Withdraw {
                amount: U256::from(1u64)
            }
            .needs_confirmation()
        );
        assert!(
            !Phase::Waiting {
                amount: U256::from(1u64),
                unlock_at: 10,
                now: 1
            }
            .needs_confirmation()
        );
        assert!(
            !Phase::Complete {
                new_operator: Address::ZERO
            }
            .needs_confirmation()
        );
    }

    #[test]
    fn waiting_reports_the_remaining_window_and_submits_nothing() {
        let s = rendered(
            &plan(Phase::Waiting {
                amount: U256::from(50_000u64),
                unlock_at: 1_000_000 + 3 * 86_400,
                now: 1_000_000,
            }),
            false,
            &Outcome::default(),
            false,
        );
        assert!(s.contains("phase=waiting"), "{s}");
        assert!(s.contains(&format!("remaining_secs={}", 3 * 86_400)), "{s}");
        assert!(s.contains("submitted=false"), "{s}");
    }

    /// `remaining_secs` describes the waiting phase only; a non-zero value on
    /// any other phase would read as "still blocked" to a polling wrapper.
    #[test]
    fn remaining_secs_is_zero_off_the_waiting_phase() {
        for phase in [
            Phase::Deregister { declared_mbps: 1 },
            Phase::Withdraw {
                amount: U256::from(1u64),
            },
            reonboard_phase(),
        ] {
            let s = rendered(&plan(phase), false, &Outcome::default(), false);
            assert!(s.contains("remaining_secs=0"), "{s}");
        }
    }

    #[test]
    fn json_carries_the_phase_and_every_tx_slot() {
        let outcome = Outcome {
            withdraw: Some(B256::repeat_byte(0xAB)),
            ..Outcome::default()
        };
        let s = rendered(
            &plan(Phase::Withdraw {
                amount: U256::from(50_000u64),
            }),
            true,
            &outcome,
            false,
        );
        let v: serde_json::Value = serde_json::from_str(&s).expect("valid JSON");
        assert_eq!(
            v.get("key").and_then(serde_json::Value::as_str),
            Some("eth")
        );
        assert_eq!(
            v.get("phase").and_then(serde_json::Value::as_str),
            Some("withdraw")
        );
        assert_eq!(
            v.get("submitted").and_then(serde_json::Value::as_bool),
            Some(true)
        );
        assert_eq!(
            v.get("withdraw_tx").and_then(serde_json::Value::as_str),
            Some(format!("{:#x}", B256::repeat_byte(0xAB)).as_str())
        );
        assert!(
            v.get("register_tx").is_some_and(serde_json::Value::is_null),
            "unreached steps are present-and-null, not absent: {s}"
        );
        assert_eq!(
            v.get("active_bond_base")
                .and_then(serde_json::Value::as_str),
            Some("50000"),
            "base units are a decimal STRING — 1e18-scaled values overflow a JSON number"
        );
    }

    /// A withdrawal that only returns money still has to be distinguishable
    /// from a preview, the way every other command's receipt is.
    #[test]
    fn dry_run_is_distinguishable_from_a_failed_send() {
        let dry = rendered(
            &plan(Phase::Withdraw {
                amount: U256::from(1u64),
            }),
            false,
            &Outcome::default(),
            true,
        );
        assert!(dry.contains("submitted=false dry_run=true"), "{dry}");
        let failed = rendered(
            &plan(Phase::Withdraw {
                amount: U256::from(1u64),
            }),
            false,
            &Outcome::default(),
            false,
        );
        assert!(failed.contains("submitted=false dry_run=false"), "{failed}");
    }
}
