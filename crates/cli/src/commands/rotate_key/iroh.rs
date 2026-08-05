//! `decdn node rotate-key --key iroh` — rebind the wire `NodeId`
//! (`appendix-operator-key-rotation.md` § iroh node-key rotation only).
//!
//! One transaction. `CapacityBond.bindNodeId` deletes the old
//! `nodeId → address` mapping and writes the new one in the same call, so the
//! two ids swap slashability atomically and the operator is never unslashable
//! mid-rotation. Everything keyed on the Ethereum address — the bond, the
//! declared tier, `firstBondedAt`, every open payment channel, and every
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
    let provider = decdn_client_pull::provider::build_provider(&resolved.rpc_url, &signer)?;

    // Stage (or load, or preview-in-memory) the key BEFORE the nonce reads:
    // `registrationNonce` is keyed on the id being bound, so the id has to
    // exist first.
    let new_key = NewKey::acquire(
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
        write_plan(&mut out, &plan, json, None, None, true)
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
            let mut out = io::stdout().lock();
            write_plan(&mut out, &plan, json, outcome.tx().as_ref(), None, false)
                .context("failed to write result")?;
            return Err(err);
        }
    };

    // Runbook step 5 verifies the `NodeIdBound` event, and here that check is
    // load-bearing rather than ceremonial: it is the gate on replacing
    // `node.secret`. A receipt that mined without the log means the binding did
    // not move (or the deployed ABI drifted), and committing the key against it
    // is precisely how the daemon ends up unslashable.
    confirm_bound(&receipt, plan.operator, plan.new_node_id)?;
    let tx = receipt.transaction_hash;

    let archived = new_key.commit().with_context(|| {
        format!(
            "the on-chain binding has ALREADY moved to {:#x} (tx {tx:#x}), but installing the new \
             node key failed. The daemon still holds the old key, which is now unbound and cannot \
             serve. Fix the cause below, then either re-run with `--bind-existing` to bind \
             whatever key is on disk, or restore the archived key and bind that",
            plan.new_node_id,
        )
    })?;

    let mut out = io::stdout().lock();
    write_plan(&mut out, &plan, json, Some(&tx), archived.as_deref(), false)
        .context("failed to write result")?;
    Ok(())
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
    fn commit(self) -> anyhow::Result<Option<PathBuf>> {
        match self {
            Self::Staged(k) => k.commit(),
            Self::Existing(_) => Ok(None),
            Self::Preview(_) => Err(anyhow::anyhow!(
                "internal: attempted to commit a --dry-run preview key; this should be \
                 unreachable — dry-run returns before commit is called"
            )),
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
) -> anyhow::Result<()> {
    let found = receipt
        .inner
        .logs()
        .iter()
        .filter(|log| log.topic0() == Some(&CapacityBond::NodeIdBound::SIGNATURE_HASH))
        .filter_map(|log| log.log_decode::<CapacityBond::NodeIdBound>().ok())
        .any(|log| log.inner.data.ethAddress == operator && log.inner.data.nodeId == new_node_id);
    anyhow::ensure!(
        found,
        "bindNodeId mined but its receipt carried no NodeIdBound log for {operator:#x} → \
         {new_node_id}. The binding did not move (or the deployed CapacityBond ABI has drifted \
         from this CLI). The node key on disk was NOT replaced; check the transaction on a block \
         explorer before retrying."
    );
    Ok(())
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
        "Your bond, declared capacity, firstBondedAt, and every open payment channel are NOT \
         affected — all of them key on the Ethereum address, which does not change. Vouchers \
         already signed against those channels still settle. The old and new node ids swap \
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
    tx: Option<&B256>,
    archived: Option<&Path>,
    dry_run: bool,
) -> io::Result<()> {
    // A generated key is only knowable after it is committed. On a preview the
    // key is never persisted, so the id shown is a real signature subject but
    // NOT the id a later run will bind — say so rather than let it be quoted
    // back. The signatures themselves stay submittable regardless: they cover
    // the CURRENT on-chain nonces, so anyone holding this output can submit
    // `bindNodeId` with them until the operator's `bindingNonce` advances,
    // rebinding the operator to a key whose secret exists nowhere — an
    // off-chain griefing lever the runbook's `--dry-run` paragraph warns about.
    let preview_key = dry_run && p.generated;
    let tx_hex = tx.map(|v| format!("{v:#x}"));
    let archived_str = archived.map(|a| a.display().to_string());
    if json {
        let value = serde_json::json!({
            "submitted": tx.is_some(),
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
        None => writeln!(w, "bind_tx=skipped")?,
    }
    match archived_str {
        Some(a) => writeln!(w, "archived_key={a}")?,
        None => writeln!(w, "archived_key=none")?,
    }
    writeln!(w, "submitted={} dry_run={dry_run}", tx.is_some())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    /// A `--dry-run` preview of a fresh key must not create `data_dir`, let
    /// alone write into it — the bug the FS side effect was: `stage_node_key`
    /// eagerly creates the directory via `ensure_data_dir` even though its
    /// temp file is removed on drop, so a preview against a data dir that
    /// does not exist yet used to leave it behind anyway.
    #[test]
    fn dry_run_acquire_creates_no_data_dir() {
        let tmp = tempfile::tempdir().expect("make a scratch dir");
        let data_dir = tmp.path().join("does-not-exist-yet");
        assert!(!data_dir.exists());

        let key = NewKey::acquire(&data_dir, false, true).expect("preview key must build");
        assert!(
            !data_dir.exists(),
            "a --dry-run preview must not create the data dir"
        );
        assert!(matches!(key, NewKey::Preview(_)));
    }

    /// Same guarantee restated at the type level: a preview can sign (the
    /// receipt needs a real ownership proof) but must refuse to commit,
    /// because nothing should ever call `commit` on one.
    #[test]
    fn preview_key_refuses_to_commit() {
        let key = NewKey::Preview(Box::new(identity::fresh_secret_key()));
        // Signing must still work — a dry run's printed signatures are real.
        let _ = key.sign(b"digest");
        key.commit().expect_err("a preview must never be committed");
    }

    /// `--bind-existing` is unaffected by `dry_run`: it only ever loads, and
    /// `NewKey::acquire`'s existence check runs regardless.
    #[test]
    fn bind_existing_dry_run_still_requires_an_existing_key() {
        let tmp = tempfile::tempdir().expect("make a scratch dir");
        let result = NewKey::acquire(tmp.path(), true, true);
        let err = result
            .err()
            .expect("bind-existing with no key on disk must fail, dry-run or not");
        assert!(format!("{err}").contains("--bind-existing needs a node key"));
    }

    fn plan(generated: bool, active: bool) -> Plan {
        Plan {
            capacity_bond: Address::repeat_byte(0xCB),
            operator: Address::repeat_byte(0x0E),
            chain_id: 31337,
            old_node_id: B256::repeat_byte(0x11),
            active,
            new_node_id: B256::repeat_byte(0x22),
            generated,
            binding_nonce: 3,
            registration_nonce: 0,
            binding_sig: vec![0xAA; 65],
            ed25519_sig: vec![0xBB; 64],
        }
    }

    fn rendered(p: &Plan, json: bool, tx: Option<&B256>, dry_run: bool) -> String {
        let mut buf = Vec::new();
        write_plan(&mut buf, p, json, tx, None, dry_run).expect("write to a Vec cannot fail");
        String::from_utf8(buf).expect("output is ASCII")
    }

    fn disclosure(p: &Plan) -> String {
        let mut buf = Vec::new();
        write_disclosure(&mut buf, p).expect("write to a Vec cannot fail");
        String::from_utf8(buf).expect("output is ASCII")
    }

    fn args(key: cli::RotateKeyTarget) -> cli::RotateKeyArgs {
        cli::RotateKeyArgs {
            key,
            bind_existing: false,
            new_keystore: None,
            mbps: None,
            region: None,
            multiaddrs: Vec::new(),
            accept_terms: false,
            yes: false,
            chain: cli::ChainArgs {
                common: cli::CommonChainArgs {
                    config: None,
                    rpc_url: None,
                    chain_id: None,
                    keystore: None,
                    data_dir: None,
                    keystore_password_file: None,
                    dry_run: false,
                    json: false,
                },
                capacity_bond_address: None,
                swap_venue: None,
                swap_router_address: None,
                swap_quoter_address: None,
                usdc_address: None,
                swap_fee_tier: None,
                swap_balancer_pool: None,
                swap_pool_address: None,
            },
        }
    }

    /// The reassurance is the disclosure's job — an operator who believes
    /// rotating costs them their bond or their open channels will not rotate a
    /// compromised key, which is strictly worse than rotating one.
    #[test]
    fn disclosure_says_the_bond_and_channels_survive() {
        let s = disclosure(&plan(true, true));
        assert!(s.contains("NOT affected"), "{s}");
        assert!(s.contains("still settle"), "names the voucher case: {s}");
        assert!(
            s.contains("no unslashable window"),
            "the atomicity is the point: {s}"
        );
    }

    /// The challenger-side hazard is not in the runbook and not enforced
    /// anywhere in the CLI, so the disclosure is the only place an operator
    /// with evidence in flight can learn it.
    #[test]
    fn disclosure_warns_about_evidence_citing_the_old_node_id() {
        let s = disclosure(&plan(true, true));
        assert!(s.contains("re-submitted against"), "{s}");
    }

    /// The archive note describes an effect that only happens on the generating
    /// path; under `--bind-existing` nothing is moved aside, and claiming
    /// otherwise would send an operator hunting for a file that is not there.
    #[test]
    fn disclosure_omits_the_archive_note_when_binding_an_existing_key() {
        let generated = disclosure(&plan(true, true));
        assert!(generated.contains("archived"), "{generated}");
        let existing = disclosure(&plan(false, true));
        assert!(!existing.contains("archived"), "{existing}");
    }

    /// `bindNodeId` only patches the registration record for an ACTIVE
    /// operator, so an inactive one ends up with a binding and a record that
    /// disagree. Silence there is how that becomes a mystery later.
    #[test]
    fn disclosure_flags_an_inactive_registration() {
        assert!(disclosure(&plan(true, false)).contains("not currently in the active set"));
        assert!(!disclosure(&plan(true, true)).contains("not currently in the active set"));
    }

    #[test]
    fn dry_run_reports_no_tx_and_is_distinguishable_from_a_failed_send() {
        let dry = rendered(&plan(true, true), false, None, true);
        assert!(dry.contains("bind_tx=skipped"), "{dry}");
        assert!(dry.contains("submitted=false dry_run=true"), "{dry}");

        let failed = rendered(&plan(true, true), false, None, false);
        assert!(
            failed.contains("submitted=false dry_run=false"),
            "a failed send is not a preview: {failed}"
        );
    }

    /// A previewed generated key is discarded on drop, so the id it names is
    /// not the id a later real run binds. Reporting it without that caveat
    /// invites an operator to pre-authorize the wrong id somewhere.
    #[test]
    fn preview_key_is_flagged_only_when_a_generated_key_was_discarded() {
        assert!(rendered(&plan(true, true), false, None, true).contains("preview_key=true"));
        // `--bind-existing` previews a key that really is on disk.
        assert!(rendered(&plan(false, true), false, None, true).contains("preview_key=false"));
        // A committed key is not a preview.
        let tx = B256::repeat_byte(0xAB);
        assert!(rendered(&plan(true, true), false, Some(&tx), false).contains("preview_key=false"));
    }

    #[test]
    fn json_carries_both_ids_and_both_signatures() {
        let tx = B256::repeat_byte(0xAB);
        let s = rendered(&plan(true, true), true, Some(&tx), false);
        let v: serde_json::Value = serde_json::from_str(&s).expect("valid JSON");
        assert_eq!(
            v.get("key").and_then(serde_json::Value::as_str),
            Some("iroh")
        );
        assert_eq!(
            v.get("submitted").and_then(serde_json::Value::as_bool),
            Some(true)
        );
        assert_eq!(
            v.get("old_node_id").and_then(serde_json::Value::as_str),
            Some(format!("{:#x}", B256::repeat_byte(0x11)).as_str())
        );
        assert_eq!(
            v.get("new_node_id").and_then(serde_json::Value::as_str),
            Some(format!("{:#x}", B256::repeat_byte(0x22)).as_str())
        );
        assert!(
            v.get("ed25519_sig")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|s| s.starts_with("0xbbbb")),
            "{s}"
        );
        assert_eq!(
            v.get("bind_tx").and_then(serde_json::Value::as_str),
            Some(format!("{tx:#x}").as_str())
        );
    }

    #[test]
    fn json_null_tx_and_archive_on_a_dry_run() {
        let s = rendered(&plan(true, true), true, None, true);
        let v: serde_json::Value = serde_json::from_str(&s).expect("valid JSON");
        assert!(
            v.get("bind_tx").is_some_and(serde_json::Value::is_null),
            "the key is present-and-null, not absent: {s}"
        );
        assert!(
            v.get("archived_key")
                .is_some_and(serde_json::Value::is_null),
            "a preview archives nothing: {s}"
        );
    }

    /// Ignoring an eth-only flag here would let an operator believe a tier or a
    /// terms acceptance was applied by a command that does neither.
    #[test]
    fn eth_only_flags_are_refused_on_the_iroh_path() {
        let mut a = args(cli::RotateKeyTarget::Iroh);
        a.mbps = Some(1000);
        a.accept_terms = true;
        let err = reject_eth_only_flags(&a).expect_err("eth-only flags must not be ignored");
        let msg = format!("{err}");
        assert!(msg.contains("--mbps"), "{msg}");
        assert!(msg.contains("--accept-terms"), "{msg}");
        assert!(msg.contains("--key eth"), "names where they belong: {msg}");

        reject_eth_only_flags(&args(cli::RotateKeyTarget::Iroh))
            .expect("a bare iroh rotation is legal");
    }
}
