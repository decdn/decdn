//! `decdn node rotate-key --key iroh` — rebind the wire `NodeId`
//! (`appendix-operator-key-rotation.md` § iroh node-key rotation only).
//!
//! One transaction. `CapacityBond.bindNodeId` deletes the old
//! `nodeId → address` mapping and writes the new one in the same call, so the
//! two ids swap slashability atomically and the operator is never unslashable
//! mid-rotation. Everything keyed on the Ethereum address — the bond, the
//! declared tier, `firstBondedAt`, every open payment pool, and every
//! voucher already signed against one — is untouched, because the address does
//! not change.
//!
//! # Why the key file is committed last
//!
//! The command has two effects that must agree: a chain write (the binding) and
//! a disk write (`node.secret`). Only one order is safe.
//!
//! Disk first would mean that a reverted or lost `bindNodeId` leaves the daemon
//! holding a key nothing on chain points at — serving, earning, and
//! **unslashable**, the exact fault this command exists to prevent. Chain first
//! merely risks the inverse: the binding moved but the daemon still holds the
//! old key, which is a plain outage (peers reject its announces, clients cannot
//! find it) and is repaired by re-running with `--bind-existing`, or by
//! restoring the `.bak` archive and binding that.
//!
//! So the new key is *staged* — generated into a temp file that
//! [`decdn_common::identity::StagedNodeKey`] removes on drop — signed from
//! while still staged, and committed only after the receipt carries a
//! `NodeIdBound` log. A `--dry-run` produces genuine signatures over a genuine
//! key and still writes nothing — literally nothing, not even `data_dir`
//! itself: staging would create it as a side effect of `ensure_data_dir`, so a
//! preview generates the key in memory only (see [`NewKey::Preview`]) rather
//! than going through the stage that a real run uses.
//!
//! # What rotation costs
//!
//! Nothing economic, and three things that are not: peers score reputation
//! against the observed `NodeId`, so the new identity starts at the neutral
//! default (ADR 008); the Kademlia routing position reseeds (ADR 022); and
//! cached TLS session tickets are invalidated. All three recover on their own.
//!
//! One consequence is sharper than those and is disclosed separately, because
//! neither the runbook nor `SlashJudge` surfaces it today: a slash challenge
//! must cite the **currently bound** `nodeId`
//! (`SlashJudge._checkRegistered` reverts `NodeIdMismatch` otherwise). Evidence
//! gathered against the retired id is still valid — the `slash_sig` it carries
//! is secp256k1 over the Ethereum address, which did not change — but it has to
//! be submitted against the new id.

use std::io;
use std::path::{Path, PathBuf};

use alloy::primitives::{Address, B256, Bytes};
use alloy::providers::Provider;
use alloy::signers::SignerSync;
use alloy::signers::local::PrivateKeySigner;
use alloy::sol_types::SolEvent as _;
use anyhow::Context;
use decdn_common::cli;
use decdn_common::identity::{self, StagedNodeKey};
use decdn_incentive::capacity_bond::CapacityBond;
use decdn_incentive::tx::SendOutcome;
use decdn_incentive::{bind_sig, node_register};

use crate::commands::chain_ctx;
use crate::commands::rotate_key::confirm_or_bail;

/// Entry point for `decdn node rotate-key --key iroh`.
pub(crate) async fn run(
    args: &cli::RotateKeyArgs,
    global_config: Option<&Path>,
) -> anyhow::Result<()> {
    reject_eth_only_flags(args)?;

    let config_path = args.chain.common.config.as_deref().or(global_config);
    let file = chain_ctx::load_optional_config(config_path)?;
    let resolved = chain_ctx::resolve(&args.chain, &file)?;
    let cb_addr = resolved.capacity_bond_address;

    let signer = chain_ctx::load_operator_signer(&args.chain.common, &resolved.keystore).await?;
    let provider = decdn_client::provider::build_provider(&resolved.rpc_url, &signer)?;

    // Stage (or load, or preview-in-memory) the key BEFORE the nonce reads:
    // `registrationNonce` is keyed on the id being bound, so the id has to
    // exist first.
    let mut new_key = NewKey::acquire(
        &resolved.data_dir,
        args.bind_existing,
        args.chain.common.dry_run,
    )?;

    let plan = build_plan(
        &provider,
        &signer,
        &new_key,
        cb_addr,
        resolved.chain_id,
        args.bind_existing,
    )
    .await?;

    let json = args.chain.common.json;
    if args.chain.common.dry_run {
        let mut out = io::stdout().lock();
        write_plan(
            &mut out,
            &plan,
            json,
            &RunOutcome {
                dry_run: true,
                ..RunOutcome::default()
            },
        )
        .context("failed to write dry-run output")?;
        // `new_key` drops here. For `--bind-existing` nothing was ever staged;
        // otherwise it is a `Preview` that never touched disk in the first
        // place — so a preview leaves `node.secret` AND the data dir exactly
        // as they were, including when the data dir did not exist yet.
        return Ok(());
    }

    confirm_or_bail(|w| write_disclosure(w, &plan), "rotation", args.yes)?;

    // Caller-owned slot (#1355): from the send onward the transaction is
    // broadcast and may take effect, so an unreadable receipt must still print
    // the hash rather than read as "nothing happened".
    let mut outcome = SendOutcome::NotSent;
    let sent = decdn_incentive::tx::send_for_receipt(
        CapacityBond::new(cb_addr, &provider).bindNodeId(
            plan.new_node_id,
            Bytes::from(plan.binding_sig.clone()),
            Bytes::from(plan.ed25519_sig.clone()),
        ),
        "bindNodeId",
        Some(
            "most likely the binding nonce advanced under a concurrent registration, the new \
             nodeId is already bound to another address, or a signature digest was mis-built",
        ),
        &mut outcome,
    )
    .await;

    // Report before propagating: from here on the binding may already have
    // moved, and an operator who is told only "it failed" will not know to
    // check. `tx()` is `Some` on a broadcast whose receipt was unreadable, and
    // that case must print too.
    let receipt = match sent {
        Ok(receipt) => receipt,
        Err(err) => {
            // `maybe_effected()` is the question that decides the key's fate, and
            // it is NOT the same as "we have a hash": `MaybeBroadcast` has no
            // hash yet may still have reached the mempool. Whenever the bind may
            // have landed, the staged secret must survive — dropping it here is
            // what makes the failure unrecoverable rather than merely annoying.
            let parked = park_key_if_effected(&mut new_key, &outcome, &plan);
            report(
                &plan,
                json,
                &RunOutcome {
                    tx: outcome.tx().as_ref(),
                    maybe_effected: outcome.maybe_effected(),
                    parked: parked.as_deref(),
                    ..RunOutcome::default()
                },
            );
            return Err(err);
        }
    };

    // Read the hash BEFORE the gate below: every path from here on may have
    // moved the binding, and an error that cannot name the transaction leaves
    // the operator with nothing to check on a block explorer.
    let tx = receipt.transaction_hash;

    let archived = gate_and_install(&receipt, &plan, json, tx, &mut new_key)?;

    // The success path does NOT go through `report`, deliberately. `report`
    // degrades a failed stdout write to stderr so it cannot bury an operation
    // error — but here there is no operation error, and the receipt is the only
    // output. Swallowing the write failure would exit 0 having lost `bind_tx`
    // (the handle for reconciling the on-chain change) and `archived_key` (the
    // rollback artifact the runbook §5 names), which is precisely the "reported
    // success that was not recorded" this command must never produce.
    let mut out = io::stdout().lock();
    write_plan(
        &mut out,
        &plan,
        json,
        &RunOutcome {
            tx: Some(&tx),
            maybe_effected: true,
            archived: archived.as_deref(),
            key_installed: true,
            ..RunOutcome::default()
        },
    )
    .with_context(|| {
        format!(
            "the rotation COMPLETED (tx {tx:#x}, now bound to {:#x}) but its receipt could not \
             be written; the on-chain change stands and is not recorded here",
            plan.new_node_id,
        )
    })?;
    Ok(())
}

/// Verify the receipt actually moved the binding, then install the key.
///
/// Separate from `run` because both of its failure arms share one obligation
/// that is easy to get wrong: the transaction has **mined**, so the key must be
/// preserved rather than dropped, and the receipt must be written before the
/// error propagates. Keeping them in one place holds both arms to that shared
/// obligation, so no post-send path destroys the key. The commit arm is where
/// this bites hardest: `confirm_bound` has just proved the chain names this key,
/// so parking it rather than dropping it is what keeps recovery a `mv`.
///
/// # Errors
///
/// Propagates the gate failure or the install failure, in both cases annotated
/// with where the new key ended up.
fn gate_and_install(
    receipt: &alloy::rpc::types::TransactionReceipt,
    plan: &Plan,
    json: bool,
    tx: B256,
    new_key: &mut NewKey,
) -> anyhow::Result<Option<PathBuf>> {
    // Runbook step 5 verifies the `NodeIdBound` event, and here that check is
    // load-bearing rather than ceremonial: it is the gate on replacing
    // `node.secret`. A receipt that mined without the log means the binding did
    // not move (or the deployed ABI drifted), and committing the key against it
    // is precisely how the daemon ends up unslashable.
    if let Err(err) = confirm_bound(receipt, plan.operator, plan.new_node_id, plan.capacity_bond) {
        // The transaction MINED, so this is not the harmless "nothing happened"
        // case: the binding may or may not have moved. Park the key rather than
        // discard a secret the chain may now be bound to.
        let parked = park_key(new_key, plan);
        report(
            plan,
            json,
            &RunOutcome {
                tx: Some(&tx),
                maybe_effected: true,
                parked: parked.as_deref(),
                ..RunOutcome::default()
            },
        );
        return Err(err).context(describe_key_fate(parked.as_deref(), new_key));
    }

    match new_key.commit() {
        Ok(archived) => Ok(archived),
        Err(err) => {
            // `confirm_bound` just PROVED `nodeIdToAddress[new_node_id] ==
            // operator`, so this is the one post-send path where the chain
            // definitely names this key. Park it rather than destroy it: recovery
            // becomes a `mv` instead of a second
            // `bindNodeId` and another binding nonce. That matters most in
            // `install_staged`'s worst branch, which can leave `node.secret`
            // MISSING — where `--bind-existing` would refuse for want of a key
            // on disk.
            let parked = park_key(new_key, plan);
            report(
                plan,
                json,
                &RunOutcome {
                    tx: Some(&tx),
                    maybe_effected: true,
                    parked: parked.as_deref(),
                    ..RunOutcome::default()
                },
            );
            let fate = describe_key_fate(parked.as_deref(), new_key);
            Err(err).context(format!(
                "the on-chain binding has ALREADY moved to {:#x} (tx {tx:#x}), but installing the \
                 new node key failed. The daemon still holds the old key, which is now unbound \
                 and cannot serve. {fate}",
                plan.new_node_id,
            ))
        }
    }
}

/// One sentence saying where the new key ended up, for the error context on a
/// post-send failure.
///
/// `None` is genuinely three different states, and conflating them is how an
/// operator ends up hunting for a file that does not exist: under
/// `--bind-existing` there was never a separate key to preserve, while a failed
/// `park_key` means there WAS one and it could not be moved. The caller cannot
/// tell them apart from the `Option` alone, so the key itself is consulted.
fn describe_key_fate(parked: Option<&Path>, key: &NewKey) -> String {
    match parked {
        Some(p) => format!(
            "The new key is preserved at {} — move it over `node.secret` to finish the rotation \
             with no further transaction.",
            p.display()
        ),
        None if !key.generated() => {
            "No separate key was involved: `--bind-existing` binds the key already at \
             `node.secret`, which is untouched."
                .to_string()
        }
        None => "The new key could NOT be preserved (see stderr); re-run to bind a fresh one."
            .to_string(),
    }
}

/// Write the outcome to stdout, reporting a write failure to stderr rather than
/// returning it.
///
/// Every call site is on a path that already carries an error — a failed send,
/// a failed gate, a failed install. That is the precondition for this
/// degradation, and it is why the success path deliberately does NOT use this
/// helper: there the receipt is the only output, so losing it must fail the
/// command rather than exit 0. If
/// the stdout write is allowed to `?`, it *replaces* that error: piping this
/// command into `head` is enough to turn "the transaction may have been
/// broadcast; re-sending is not idempotent" into "Broken pipe". The receipt is
/// the less important of the two, so it degrades and the real error survives.
fn report(plan: &Plan, json: bool, o: &RunOutcome<'_>) {
    let mut out = io::stdout().lock();
    if let Err(err) = write_plan(&mut out, plan, json, o) {
        eprintln!("failed to write the result receipt to stdout: {err}");
    }
    let parked = o.parked;
    if let Some(path) = parked {
        eprintln!(
            "the newly generated node key was NOT installed, but has been preserved at {}. \
             Check whether the transaction confirmed: if it did, this file is the private key \
             for the node id now bound on-chain and must be moved over `node.secret` before the \
             daemon can serve. If it did not, delete it.",
            path.display()
        );
    }
}

/// Park the staged key when the transaction may have taken effect; discard it
/// (via `Drop`) when it definitively did not.
///
/// `maybe_effected()` is false for `NotSent`, `Rejected` (never reached the
/// mempool) and `Reverted` (mined, but moved no state — the sharper case, since
/// it burned gas yet left `nodeIdToAddress` untouched). In all three the key is
/// genuinely worthless, and preserving it would only litter the data dir with
/// key material.
fn park_key_if_effected(key: &mut NewKey, outcome: &SendOutcome, plan: &Plan) -> Option<PathBuf> {
    if !outcome.maybe_effected() {
        return None;
    }
    park_key(key, plan)
}

/// Preserve a generated-but-uninstalled key, reporting a failure to stderr.
///
/// Returns `None` in three distinguishable situations, which is why callers
/// consult the key itself (see `describe_key_fate`) rather than this `Option`
/// alone: `--bind-existing` binds a key already at `node.secret`, a `Preview`
/// never reaches a send, and — the one that matters — parking may have *failed*,
/// in which case there was something to preserve and the error is on stderr.
fn park_key(key: &mut NewKey, plan: &Plan) -> Option<PathBuf> {
    match key.park() {
        Ok(path) => path,
        Err(err) => {
            eprintln!(
                "could not preserve the newly generated node key for {:#x}; it has been \
                 discarded: {err}",
                plan.new_node_id
            );
            None
        }
    }
}

/// Refuse `--key eth` flags on the iroh path instead of ignoring them.
///
/// They are not merely inert here: `--mbps` and `--region` describe a
/// re-registration this path never performs, so silently dropping them would
/// let an operator believe a tier or region change was applied. `--accept-terms`
/// is the sharpest — rebinding signs the terms-free `BindNodeId` payload, so
/// passing it suggests an acceptance that never happens.
fn reject_eth_only_flags(args: &cli::RotateKeyArgs) -> anyhow::Result<()> {
    let mut offenders = Vec::new();
    if args.new_keystore.is_some() {
        offenders.push("--new-keystore");
    }
    if args.mbps.is_some() {
        offenders.push("--mbps");
    }
    if args.region.is_some() {
        offenders.push("--region");
    }
    if !args.multiaddrs.is_empty() {
        offenders.push("--multiaddr");
    }
    if args.accept_terms {
        offenders.push("--accept-terms");
    }
    anyhow::ensure!(
        offenders.is_empty(),
        "{} apply to `--key eth` only. Rotating the iroh node key rebinds an existing \
         registration: it does not re-register, so it changes no tier, region, or multiaddr, and \
         signs the terms-free `BindNodeId` payload rather than re-accepting terms.",
        offenders.join(", "),
    );
    Ok(())
}

/// The key this run binds, and whether committing it replaces `node.secret`.
///
/// Both arms have to sign before the transaction is sent, which is why this is
/// an enum over a signing capability rather than over a `SecretKey`: a staged
/// key deliberately does not hand out its secret
/// ([`StagedNodeKey::sign`]), so "sign with whichever key we are binding" is the
/// only shape both arms fit.
enum NewKey {
    /// Freshly generated, living in a temp file. Committing archives the
    /// current `node.secret` and installs this one; dropping removes the temp.
    Staged(Box<StagedNodeKey>),
    /// Already on disk under `--bind-existing`. Committing is a no-op: the file
    /// this binds is the file that is already there, so there is nothing to
    /// archive and nothing to install.
    ///
    /// Boxed for the same reason [`Self::Staged`] is — both arms wrap a
    /// `SecretKey`-sized payload, so leaving either inline sizes the enum to it.
    Existing(Box<iroh::SecretKey>),
    /// A freshly generated key that exists ONLY in memory — used for a
    /// `--dry-run` preview of a key that would otherwise be generated.
    ///
    /// `--dry-run` promises to write nothing, and [`identity::stage_node_key`]
    /// cannot honor that promise on its own: staging calls
    /// `ensure_data_dir`, which CREATES `data_dir` (mode `0o700`) when it is
    /// absent — a real filesystem write, left behind even though the staged
    /// temp file itself is removed on drop. This variant sidesteps
    /// `stage_node_key` entirely, so a preview touches no path on disk at all.
    /// [`Self::commit`] refuses it — reaching that call on a `Preview` would
    /// be this module's own logic error, since `run` only constructs one on
    /// the branch that returns before `commit` is ever called.
    ///
    /// Boxed for the same reason [`Self::Existing`] is.
    Preview(Box<iroh::SecretKey>),
}

impl NewKey {
    /// Generate a new key (staged to disk, or in-memory-only for
    /// `--dry-run`), or load the one already at `node.secret` under
    /// `--bind-existing`.
    fn acquire(data_dir: &Path, bind_existing: bool, dry_run: bool) -> anyhow::Result<Self> {
        if bind_existing {
            // `load_or_generate` would MINT a key when none exists and then
            // bind that, which under a flag whose whole meaning is "use what
            // is already there" would be the opposite of what was asked.
            let key_path = identity::key_path(data_dir);
            anyhow::ensure!(
                key_path.exists(),
                "--bind-existing needs a node key at {}, and there is none. Drop the flag to \
                 generate and bind a fresh key, or restore the key you meant to bind first.",
                key_path.display(),
            );
            return identity::load_or_generate(data_dir)
                .with_context(|| format!("failed to load node key from {}", data_dir.display()))
                .map(|k| Self::Existing(Box::new(k)));
        }
        if dry_run {
            // In memory only — see the `Preview` variant's doc for why this
            // cannot go through `stage_node_key`.
            return Ok(Self::Preview(Box::new(identity::fresh_secret_key())));
        }
        identity::stage_node_key(data_dir)
            .with_context(|| format!("failed to stage a new node key in {}", data_dir.display()))
            .map(|k| Self::Staged(Box::new(k)))
    }

    fn node_id(&self) -> B256 {
        let public = match self {
            Self::Staged(k) => k.public(),
            // Both hold a plain `iroh::SecretKey`, unlike `Staged`'s
            // `StagedNodeKey` wrapper, so they share one arm.
            Self::Existing(k) | Self::Preview(k) => k.public(),
        };
        B256::from_slice(public.as_bytes())
    }

    fn sign(&self, msg: &[u8]) -> [u8; 64] {
        match self {
            Self::Staged(k) => k.sign(msg).to_bytes(),
            Self::Existing(k) | Self::Preview(k) => k.sign(msg).to_bytes(),
        }
    }

    /// Install the key, returning the archive path of the one it replaced.
    /// `None` means nothing was archived — either a fresh install, or the
    /// `--bind-existing` no-op.
    ///
    /// # Errors
    ///
    /// Returns an error for [`Self::Preview`] rather than panicking: a
    /// `Preview` only exists on the `--dry-run` branch of `run`, which returns
    /// before this is ever called, so reaching this arm means that invariant
    /// broke — worth a clear error over an anti-panic-policy violation.
    fn commit(&mut self) -> anyhow::Result<Option<PathBuf>> {
        match self {
            Self::Staged(k) => k.commit(),
            Self::Existing(_) => Ok(None),
            Self::Preview(_) => Err(anyhow::anyhow!(
                "internal: attempted to commit a --dry-run preview key; this should be \
                 unreachable — dry-run returns before commit is called"
            )),
        }
    }

    /// Preserve a generated key without installing it, for the case where the
    /// bind may have landed but could not be confirmed.
    ///
    /// `Ok(None)` means there was nothing to preserve, and both arms that return
    /// it are correct rather than degenerate: `Existing` binds a key that is
    /// already at `node.secret` and so cannot be lost, and `Preview` never
    /// reaches a transaction at all.
    ///
    /// # Errors
    ///
    /// Propagates a failure to rename the staged temp into place.
    fn park(&mut self) -> anyhow::Result<Option<PathBuf>> {
        match self {
            // `keep` consumes the stage, so swap in a throwaway `Preview` to
            // move it out. The enum is dropped by the caller immediately after;
            // the placeholder exists only to satisfy ownership, and `Preview`
            // is the arm with no side effects on drop.
            Self::Staged(_) => {
                let taken =
                    std::mem::replace(self, Self::Preview(Box::new(identity::fresh_secret_key())));
                match taken {
                    Self::Staged(k) => k.keep().map(Some),
                    // Unreachable: the outer match arm already proved `Staged`.
                    other => {
                        *self = other;
                        Ok(None)
                    }
                }
            }
            Self::Existing(_) | Self::Preview(_) => Ok(None),
        }
    }

    const fn generated(&self) -> bool {
        matches!(self, Self::Staged(_) | Self::Preview(_))
    }
}

/// Everything read from chain plus the two signatures, assembled before the
/// confirmation gate so the operator is shown the real transaction. Owned so
/// [`write_plan`] and [`write_disclosure`] are testable without a chain.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Plan {
    pub(crate) capacity_bond: Address,
    pub(crate) operator: Address,
    pub(crate) chain_id: u64,
    /// The nodeId bound to `operator` right now. Always `Some` — an operator
    /// with no binding is refused before a `Plan` is built.
    pub(crate) old_node_id: B256,
    /// `_nodes[operator].active`, NOT the composite `isActive`. This is the
    /// flag `bindNodeId` consults when deciding whether to also patch the
    /// registration record, so it is the one worth reporting.
    pub(crate) active: bool,
    pub(crate) new_node_id: B256,
    /// Whether this run generated the new key (and so will replace
    /// `node.secret`) or is binding one already on disk.
    pub(crate) generated: bool,
    pub(crate) binding_nonce: u64,
    pub(crate) registration_nonce: u64,
    pub(crate) binding_sig: Vec<u8>,
    pub(crate) ed25519_sig: Vec<u8>,
}

/// Read the binding state, refuse the runs the contract would revert or that
/// would be a no-op, then build both signatures.
///
/// Nonces are read even on a dry run: they are inputs to the digests, and a
/// preview that signed over invented nonces would not be a preview of anything.
async fn build_plan<P: Provider + Clone>(
    provider: &P,
    signer: &PrivateKeySigner,
    new_key: &NewKey,
    cb_addr: Address,
    chain_id: u64,
    bind_existing: bool,
) -> anyhow::Result<Plan> {
    let operator = signer.address();
    let bond = CapacityBond::new(cb_addr, provider);
    let ctx = |what: &str| format!("failed to read {what} from CapacityBond at {cb_addr}");

    let old_node_id = bond
        .addressToNodeId(operator)
        .call()
        .await
        .with_context(|| ctx("addressToNodeId"))?;
    anyhow::ensure!(
        old_node_id != B256::ZERO,
        "operator {operator:#x} has no on-chain node binding, so there is nothing to rotate. \
         `decdn node rotate-key` rebinds an existing registration; the initial binding is made by \
         `decdn node register` (after `decdn node bond`)."
    );

    let new_node_id = new_key.node_id();
    anyhow::ensure!(
        new_node_id != old_node_id,
        "the key {} is already the one bound on-chain, so binding it again would consume a \
         binding nonce and change nothing.{}",
        new_node_id,
        if bind_existing {
            " The local key and the on-chain binding already agree — this node is not in the \
             un-slashable state `--bind-existing` repairs."
        } else {
            ""
        },
    );

    // Pre-check the `NodeIdAlreadyBound` revert. `addressToNodeId` above cannot
    // see this: it answers the other direction, so a collision with a DIFFERENT
    // operator's binding is invisible there.
    let holder = bond
        .nodeIdToAddress(new_node_id)
        .call()
        .await
        .with_context(|| ctx("nodeIdToAddress"))?;
    anyhow::ensure!(
        holder == Address::ZERO || holder == operator,
        "node id {new_node_id} is already bound to {holder:#x}, so `bindNodeId` would revert \
         NodeIdAlreadyBound.{}",
        if bind_existing {
            " Another operator holds the key on disk here; it cannot be bound to this address."
        } else {
            " Re-run to generate a different key — a collision with a legitimate other owner is \
             not retryable in place."
        },
    );

    let info = bond
        .getNodeByAddress(operator)
        .call()
        .await
        .with_context(|| ctx("getNodeByAddress"))?;
    let binding_nonce = bond
        .bindingNonce(operator)
        .call()
        .await
        .with_context(|| ctx("bindingNonce"))?;
    // Keyed on the NEW id. Usually 0 for a fresh key, but not necessarily — an
    // id that was bound and unbound before carries a bumped nonce, and signing
    // over 0 there reverts `InvalidEd25519Signature`.
    let registration_nonce = bond
        .registrationNonce(new_node_id)
        .call()
        .await
        .with_context(|| ctx("registrationNonce"))?;

    // EIP-712 binding signature (Ethereum key) over the terms-free `BindNodeId`
    // payload. NOT `register_node_signing_hash`: that typehash additionally
    // commits to `termsHash`, and rebinding deliberately does not re-accept
    // terms (ADR 019 § enforcement at registration only).
    let domain = bind_sig::bind_node_id_domain(chain_id, cb_addr);
    let bind_hash = bind_sig::binding_signing_hash(new_node_id, binding_nonce, &domain);
    let binding_sig = signer.sign_hash_sync(&bind_hash)?.as_bytes().to_vec();

    // ed25519 ownership proof, signed by the key being bound over the contract's
    // digest. The operator address is inside the preimage, so the proof cannot
    // be replayed by another submitter.
    let digest = node_register::ownership_message_digest(
        new_node_id,
        operator,
        chain_id,
        registration_nonce,
    );
    let ed25519_sig = new_key.sign(digest.as_slice()).to_vec();

    Ok(Plan {
        capacity_bond: cb_addr,
        operator,
        chain_id,
        old_node_id,
        active: info.active,
        new_node_id,
        generated: new_key.generated(),
        binding_nonce,
        registration_nonce,
        binding_sig,
        ed25519_sig,
    })
}

/// Verify the receipt carries the `NodeIdBound` log for this exact rebinding.
///
/// Matching on the indexed topics rather than merely finding the event is what
/// makes this a gate: a log for some other operator or some other id would
/// still be a `NodeIdBound`, and accepting it would green-light installing a key
/// the chain does not point at.
fn confirm_bound(
    receipt: &alloy::rpc::types::TransactionReceipt,
    operator: Address,
    new_node_id: B256,
    capacity_bond: Address,
) -> anyhow::Result<()> {
    // Shape-matched but undecodable is a DIFFERENT diagnosis from absent, and
    // the two imply opposite recoveries — so partition rather than filter. A log
    // from this registry carrying this exact event signature that still fails to
    // decode can only be ABI drift, and drift means the binding very likely DID
    // move. Absence means it did not. Discarding the distinction (which the old
    // `.filter_map(…ok())` did) forced the operator to go to a block explorer
    // for something already in hand.
    let mut shape_matched = 0usize;
    let mut decode_error: Option<String> = None;
    let mut found = false;
    for log in receipt
        .inner
        .logs()
        .iter()
        // The emitting contract is part of the claim: a `NodeIdBound` from some
        // other address is not evidence about THIS deployment's registry, and
        // the whole point of this function is that a merely well-shaped log is
        // not good enough to install a key against.
        .filter(|log| log.address() == capacity_bond)
        .filter(|log| log.topic0() == Some(&CapacityBond::NodeIdBound::SIGNATURE_HASH))
    {
        shape_matched += 1;
        match log.log_decode::<CapacityBond::NodeIdBound>() {
            Ok(decoded) => {
                if decoded.inner.data.ethAddress == operator
                    && decoded.inner.data.nodeId == new_node_id
                {
                    found = true;
                    break;
                }
            }
            Err(err) => {
                decode_error.get_or_insert_with(|| err.to_string());
            }
        }
    }
    if found {
        return Ok(());
    }
    let diagnosis = decode_error.map_or_else(
        || {
            "No NodeIdBound log was emitted at all, so the binding did NOT move — the key that \
             was preserved can be deleted."
                .to_string()
        },
        |err| {
            format!(
                "{shape_matched} NodeIdBound log(s) from this registry failed to decode ({err}), \
                 which is an ABI drift between this CLI and the deployed CapacityBond — the \
                 binding most likely DID move. Upgrade the CLI and re-check before deleting the \
                 preserved key."
            )
        },
    );
    anyhow::bail!(
        "bindNodeId mined but its receipt carried no matching NodeIdBound log from \
         {capacity_bond:#x} for {operator:#x} → {new_node_id}. {diagnosis} The new node key was \
         NOT installed."
    )
}

/// The consequence disclosure, pure so its wording is pinned by a test.
pub(crate) fn write_disclosure(w: &mut dyn io::Write, p: &Plan) -> io::Result<()> {
    writeln!(
        w,
        "About to rebind this operator's wire identity from {} to {}.",
        p.old_node_id, p.new_node_id,
    )?;
    // The reassurance first: this is the question every operator has, and
    // getting it wrong is what makes people avoid rotating at all.
    writeln!(
        w,
        "Your bond, declared capacity, firstBondedAt, and every open payment pool are NOT \
         affected — all of them key on the Ethereum address, which does not change. Vouchers \
         already signed against those pools still settle. The old and new node ids swap \
         slashability in the same transaction, so there is no unslashable window."
    )?;
    writeln!(
        w,
        "Resets: peers score reputation against the observed node id, so the new identity starts \
         at the neutral default; the DHT routing position reseeds; cached TLS session tickets are \
         invalidated. All three recover without intervention."
    )?;
    writeln!(
        w,
        "A slash challenge must cite the node id bound AT SUBMISSION time, so any evidence you \
         are about to submit against {} has to be re-submitted against {} instead — it is still \
         valid, since the slash signature covers the Ethereum address.",
        p.old_node_id, p.new_node_id,
    )?;
    if !p.active {
        writeln!(
            w,
            "This operator is not currently in the active set, so bindNodeId will not update the \
             registration record's node id — only the binding. Re-register with `decdn node \
             register` to make the record agree."
        )?;
    }
    if p.generated {
        writeln!(
            w,
            "The current node key is archived alongside itself before the new one is installed. \
             Keep the archive: slash evidence signed under the old identity stays submittable \
             until it ages out."
        )?;
    }
    writeln!(
        w,
        "The daemon is NOT restarted, and the node key is not hot-reloadable. Drain it (`decdn \
         node drain`) and stop it before rotating; start it again afterwards."
    )
}

/// Write the plan + outcome as JSON or grep-friendly `key=value` lines. Pure
/// (`&mut impl Write`) so the output shape is unit-testable without a chain.
///
/// `dry_run` is carried explicitly rather than inferred from `tx.is_none()`, for
/// the reason `deregister::write_plan` carries it: a real run whose send failed
/// also has no tx, so the absence alone cannot mean "preview".
pub(crate) fn write_plan(
    w: &mut impl io::Write,
    p: &Plan,
    json: bool,
    o: &RunOutcome<'_>,
) -> io::Result<()> {
    let RunOutcome {
        tx,
        maybe_effected,
        archived,
        parked,
        dry_run,
        key_installed,
    } = *o;
    // "The id above names a key that is NOT this node's identity on disk."
    //
    // Derived from whether the key was actually persisted, NOT from `dry_run`:
    // a preview is one way to reach that state, but a real run whose send failed
    // is another, and it is the more dangerous one to misreport. Keying this on
    // `dry_run` claimed, on exactly that failure path, that the printed id named
    // a persisted key.
    //
    // `--bind-existing` is never a preview: the key it names is already
    // `node.secret`, whether or not the transaction succeeded.
    let preview_key = p.generated && !key_installed;
    // Three states, not two. `tx.is_some()` is false both for "never sent" and
    // for `MaybeBroadcast` — a transport failure that may still have put the
    // transaction in the mempool. Collapsing them told a `--json` wrapper
    // "nothing happened" on the one path that also parks a key, inviting a
    // re-run that double-submits over the same binding nonce.
    let in_flight_unknown = maybe_effected && tx.is_none();
    let tx_hex = tx.map(|v| format!("{v:#x}"));
    let archived_str = archived.map(|a| a.display().to_string());
    let parked_str = parked.map(|a| a.display().to_string());
    if json {
        let value = serde_json::json!({
            "submitted": tx.is_some(),
            "maybe_broadcast": in_flight_unknown,
            "parked_key": parked_str,
            "dry_run": dry_run,
            "key": "iroh",
            "capacity_bond": format!("{:#x}", p.capacity_bond),
            "operator": format!("{:#x}", p.operator),
            "chain_id": p.chain_id,
            "old_node_id": format!("{:#x}", p.old_node_id),
            "new_node_id": format!("{:#x}", p.new_node_id),
            "generated_key": p.generated,
            "preview_key": preview_key,
            "active": p.active,
            "binding_nonce": p.binding_nonce,
            "registration_nonce": p.registration_nonce,
            "binding_sig": format!("0x{}", alloy::hex::encode(&p.binding_sig)),
            "ed25519_sig": format!("0x{}", alloy::hex::encode(&p.ed25519_sig)),
            "bind_tx": tx_hex,
            "archived_key": archived_str,
        });
        return writeln!(w, "{value}");
    }
    writeln!(w, "key=iroh")?;
    writeln!(w, "capacity_bond={:#x}", p.capacity_bond)?;
    writeln!(w, "operator={:#x}", p.operator)?;
    writeln!(w, "chain_id={}", p.chain_id)?;
    writeln!(w, "old_node_id={:#x}", p.old_node_id)?;
    writeln!(w, "new_node_id={:#x}", p.new_node_id)?;
    writeln!(w, "generated_key={}", p.generated)?;
    writeln!(w, "preview_key={preview_key}")?;
    writeln!(w, "active={}", p.active)?;
    writeln!(w, "binding_nonce={}", p.binding_nonce)?;
    writeln!(w, "registration_nonce={}", p.registration_nonce)?;
    writeln!(w, "binding_sig=0x{}", alloy::hex::encode(&p.binding_sig))?;
    writeln!(w, "ed25519_sig=0x{}", alloy::hex::encode(&p.ed25519_sig))?;
    match tx_hex {
        Some(h) => writeln!(w, "bind_tx={h}")?,
        // Deliberately not `skipped`: the transaction may be in the mempool.
        None if in_flight_unknown => writeln!(w, "bind_tx=unknown")?,
        None => writeln!(w, "bind_tx=skipped")?,
    }
    match archived_str {
        Some(a) => writeln!(w, "archived_key={a}")?,
        None => writeln!(w, "archived_key=none")?,
    }
    match parked_str {
        Some(a) => writeln!(w, "parked_key={a}")?,
        None => writeln!(w, "parked_key=none")?,
    }
    let submitted = if tx.is_some() {
        "true"
    } else if in_flight_unknown {
        "unknown"
    } else {
        "false"
    };
    writeln!(w, "submitted={submitted} dry_run={dry_run}")
}

/// What actually happened, for the receipt renderer.
///
/// A struct rather than more positional parameters: the fields are all
/// `Option`s and `bool`s of similar type, and the call sites differ from each
/// other in exactly the ways that matter most to get right.
#[derive(Clone, Copy, Default)]
pub(crate) struct RunOutcome<'a> {
    /// The transaction hash, when one is known.
    pub(crate) tx: Option<&'a B256>,
    /// `SendOutcome::maybe_effected()` — the send may have taken effect. NOT
    /// the same as `tx.is_some()`: `MaybeBroadcast` is `true` here with no hash.
    pub(crate) maybe_effected: bool,
    /// Where the previous `node.secret` was archived, on a successful install.
    pub(crate) archived: Option<&'a Path>,
    /// Where a generated-but-uninstalled key was preserved.
    pub(crate) parked: Option<&'a Path>,
    pub(crate) dry_run: bool,
    /// The new key is installed **as `node.secret`** — not merely written to
    /// disk. A parked key is persisted and still not the node's identity, which
    /// is why this is not called `key_persisted`.
    pub(crate) key_installed: bool,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests;
