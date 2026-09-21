//! `decdn pool` — client-side payment-pool lifecycle.
//!
//! One `PaymentPool` deposit fans out to every provider its owner pays
//! (ADR 003) — there is no per-provider pool. `open` escrows a fresh deposit,
//! `top-up` adds funds to a pool the caller owns, `close` starts the
//! grace-window close, and `reclaim` refunds the residual once that window has
//! elapsed. `list`/`status` is a read-only dump of the tracked buyer pools and
//! their per-lane voucher watermark.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use alloy::primitives::{Address, B256, TxHash, U256};
use alloy::signers::local::PrivateKeySigner;
use anyhow::Context as _;
use decdn_client_pull::buyer_pool::{
    ToppedUpPool, ensure_allowance, escrowed_but_untracked, grade_deposit_credit, open_pool,
    top_up, topped_up_effect,
};
use decdn_common::admin::{AdminRpcClient as _, BuyerPoolsResponse};
use decdn_common::cli::{self, common::expand_tilde};
use decdn_common::config::{DEFAULT_CHAIN_ID, FileConfig, load_file_config};
use decdn_common::redact::sanitize_rpc_display;
use decdn_incentive::buyer_pool::{BuyerLoad, BuyerPoolState, BuyerPoolStore};
use decdn_incentive::buyer_pool_redb::{ReadOnlyBuyerPoolStore, RedbBuyerPoolStore};
use decdn_incentive::eth_identity::{self, PasswordUse, load_signer};
use decdn_incentive::payment_pool::{PaymentPool, enumerate_owned_pools};
use decdn_incentive::{Capability, CapabilityGrant, PoolId, voucher_domain};
use serde::Serialize;

use decdn_client_pull::provider;

use super::buyer_store::{
    BuyerStoreOwner, classify_buyer_store, client_buyer_db, node_buyer_db,
    open_client_store_for_escrow,
};

/// Dispatch `decdn pool <subcommand>`.
pub async fn pool_dispatch(args: &cli::PoolArgs, config_path: Option<&Path>) -> anyhow::Result<()> {
    match &args.command {
        cli::PoolCommand::List(a) => list(a, config_path).await,
        cli::PoolCommand::Open(a) => open(a, config_path).await,
        cli::PoolCommand::TopUp(a) => top_up_cmd(a, config_path).await,
        cli::PoolCommand::Close(a) => close(a, config_path).await,
        cli::PoolCommand::Reclaim(a) => reclaim(a, config_path).await,
        cli::PoolCommand::Assign(a) => assign(a, config_path).await,
    }
}

/// Chain coordinates resolved flag > `[blockchain]`/`[identity]` config >
/// default. Pure (parse-only) so the precedence is unit-testable.
#[derive(Debug)]
struct Resolved {
    rpc_url: String,
    payment_pool: Address,
    chain_id: u64,
    data_dir: PathBuf,
    keystore: PathBuf,
    /// File holding the keystore password, consulted after the
    /// `DECDN_KEYSTORE_PASSWORD` env var and before a prompt. CLI/env-only —
    /// passwords do not belong in a config file even by reference.
    keystore_password_file: Option<PathBuf>,
}

/// Resolve the buyer-store data dir: flag > `[identity]` config > client-scoped
/// `~/.decdn/client` default. Shared by the read-only `list` path (which needs
/// nothing else) and [`resolve_chain`].
fn resolve_data_dir(data_dir: Option<PathBuf>, file: &FileConfig) -> anyhow::Result<PathBuf> {
    data_dir
        .or_else(|| file.identity.as_ref().and_then(|i| i.data_dir.clone()))
        .map(|p| expand_tilde(&p))
        .or_else(cli::default_client_data_dir)
        .ok_or_else(|| {
            anyhow::anyhow!("data_dir not set and no default available (pass --data-dir)")
        })
}

fn resolve_chain(args: &cli::PoolChainArgs, file: &FileConfig) -> anyhow::Result<Resolved> {
    let bc = file.blockchain.as_ref();
    let rpc_url = args
        .rpc_url
        .clone()
        .or_else(|| bc.and_then(|b| b.rpc_url.clone()))
        .ok_or_else(|| anyhow::anyhow!("rpc_url not set (--rpc-url or blockchain.rpc_url)"))?;
    let payment_pool_raw = args
        .payment_pool_address
        .clone()
        .or_else(|| bc.and_then(|b| b.payment_pool_address.clone()))
        .ok_or_else(|| {
            anyhow::anyhow!(
                "payment_pool_address not set (--payment-pool-address or \
                 blockchain.payment_pool_address)"
            )
        })?;
    let payment_pool =
        super::chain_ctx::parse_nonzero_address(&payment_pool_raw, "payment_pool_address")?;
    let chain_id = args
        .chain_id
        .or_else(|| bc.and_then(|b| b.chain_id))
        .unwrap_or(DEFAULT_CHAIN_ID);
    let data_dir = resolve_data_dir(args.data_dir.clone(), file)?;
    let keystore = args
        .keystore
        .clone()
        .or_else(|| bc.and_then(|b| b.eth_keystore.clone()))
        .map_or_else(
            || eth_identity::keystore_path(&data_dir),
            |p| expand_tilde(&p),
        );
    Ok(Resolved {
        rpc_url,
        payment_pool,
        chain_id,
        data_dir,
        keystore,
        keystore_password_file: args.keystore_password_file.as_deref().map(expand_tilde),
    })
}

/// Buyer signer for the on-chain `pool` commands (vouchers +
/// open/top-up/close/reclaim txs). Password from `KEYSTORE_PASSWORD_ENV`, else
/// `--keystore-password-file`, else TTY.
fn load_buyer_signer(chain: &Resolved) -> anyhow::Result<PrivateKeySigner> {
    let password = super::chain_ctx::read_keystore_password(
        &super::chain_ctx::password_sources(
            chain.keystore_password_file.as_deref(),
            PasswordUse::Unlock,
        ),
        "eth keystore password",
    )?
    .into_secret();
    load_signer(&chain.keystore, &password)
}

/// Parse a user-supplied `--pool`: 64 hex chars, optionally `0x`-prefixed (the
/// on-chain `poolId` is `keccak256(owner, ownerPoolNonce)`, a `bytes32`).
fn parse_pool_id(s: &str) -> anyhow::Result<PoolId> {
    B256::from_str(s)
        .map_err(|e| anyhow::anyhow!("invalid --pool {s:?}: expected a 32-byte hex hash: {e}"))
}

/// Reject a `getPool` read whose on-chain `owner` isn't this keystore — a wrong
/// keystore/contract or a stale `--pool` id (a zero `owner` means the pool was
/// never opened on this contract).
fn ensure_owned(on_chain_owner: Address, ours: Address, pool_id: PoolId) -> anyhow::Result<()> {
    anyhow::ensure!(
        on_chain_owner == ours,
        "pool {pool_id} on-chain owner {on_chain_owner} is not this keystore's address {ours} \
         — wrong keystore/contract, or a mistyped --pool"
    );
    Ok(())
}

/// `decdn pool open`: escrow a fresh `PaymentPool` deposit (`openPool`). The
/// resulting pool fans out to every provider the owner later pays — there is no
/// per-provider open.
async fn open(args: &cli::PoolOpenArgs, config_path: Option<&Path>) -> anyhow::Result<()> {
    let file = load_file_config(config_path)?;
    // Before chain coordinates are even resolved, and so before the keystore
    // prompt: escrowing into a store the daemon never reads is the failure,
    // not a degraded outcome, and the answer depends only on the data dir.
    let data_dir = resolve_data_dir(args.chain.data_dir.clone(), &file)?;
    let store = open_client_store_for_escrow(&data_dir, "open a pool")?;
    let chain = resolve_chain(&args.chain, &file)?;
    let signer = Arc::new(load_buyer_signer(&chain)?);
    let owner = signer.address();
    let rpc = provider::build_provider(&chain.rpc_url, &signer)?;
    let contract = PaymentPool::new(chain.payment_pool, rpc.clone());
    let domain = voucher_domain(chain.chain_id, chain.payment_pool);

    let token = contract
        .usdc()
        .call()
        .await
        .map_err(|e| anyhow::anyhow!("read PaymentPool.usdc(): {e}"))?;
    let deposit = U256::from(args.deposit_micro_usdc);

    ensure_allowance(&rpc, token, owner, chain.payment_pool, Some(deposit)).await?;

    let opened = open_pool(
        &contract,
        Arc::clone(&signer),
        &domain,
        token,
        owner,
        deposit,
    )
    .await?;

    // The deposit is escrowed on-chain; a failed local record leaves it
    // untracked (reconcile against the tx).
    store
        .record(&opened.state)
        .map_err(|e| escrowed_but_untracked("buyer pool opened", opened.tx, e))?;

    let out = PoolOpenJson {
        pool_id: format!("{:#x}", opened.state.pool_id),
    };
    serde_json::to_writer_pretty(std::io::stdout(), &out)?;
    println!();
    Ok(())
}

/// `decdn pool open` machine-readable output.
#[derive(Serialize)]
struct PoolOpenJson {
    #[serde(rename = "poolId")]
    pool_id: String,
}

/// `decdn pool top-up`: add funds to a pool the caller owns (`topUp`).
async fn top_up_cmd(args: &cli::PoolTopUpArgs, config_path: Option<&Path>) -> anyhow::Result<()> {
    let file = load_file_config(config_path)?;
    let data_dir = resolve_data_dir(args.chain.data_dir.clone(), &file)?;
    let store = open_client_store_for_escrow(&data_dir, "top up a pool")?;
    let chain = resolve_chain(&args.chain, &file)?;
    let pool_id = parse_pool_id(&args.pool)?;
    let signer = Arc::new(load_buyer_signer(&chain)?);
    let owner = signer.address();
    let rpc = provider::build_provider(&chain.rpc_url, &signer)?;
    let contract = PaymentPool::new(chain.payment_pool, rpc.clone());

    let pool = contract
        .getPool(pool_id)
        .call()
        .await
        .map_err(|e| anyhow::anyhow!("getPool failed: {e}"))?;
    ensure_owned(pool.owner, owner, pool_id)?;

    let additional = U256::from(args.amount_micro_usdc);
    // The pool no longer stores its token — it is the contract's immutable
    // `usdc()`, the same address the open path reads.
    let token = contract
        .usdc()
        .call()
        .await
        .map_err(|e| anyhow::anyhow!("read PaymentPool.usdc(): {e}"))?;
    ensure_allowance(&rpc, token, owner, chain.payment_pool, Some(additional)).await?;

    let ToppedUpPool { credited, tx } = top_up(&contract, pool_id, additional).await?;

    // `add_deposit` reports a backend fault as `Err` and a committed-row
    // mismatch as a non-`Added` `Ok` (the row vanished or now tracks a
    // different pool). Both mean the same thing here: the USDC is escrowed and
    // the local deposit is short by `credited`. Exiting 0 on either would let a
    // wrapper record the pool as funded — after which the low-water check
    // re-fires on every later fetch, and re-running this command escrows again.
    let effect = topped_up_effect(pool_id, credited);
    let new_deposit =
        grade_deposit_credit(store.add_deposit(owner, pool_id, credited), &effect, tx)?;
    println!("topped up pool {pool_id} by {credited} µUSDC; deposit now {new_deposit} µUSDC");
    Ok(())
}

/// Where a landed `closePool` / `reclaim` records that the pool is gone.
///
/// The on-chain leg of both is identical either way; only the local
/// bookkeeping differs, and on a daemon's data dir there is none the CLI can
/// legitimately do (#2078).
#[derive(Debug, Clone, Copy)]
enum LocalBookkeeping<'a> {
    /// Clear the row in the client store this CLI owns.
    Store(&'a RedbBuyerPoolStore),
    /// A `decdn-node` daemon owns this data dir. `decdn pool` has no code path
    /// to that daemon's `buyer.redb`, so its row stays as it is and the
    /// operator is told so — the alternative, a no-op `forget` graded as a
    /// clean close, is the silent lie this variant exists to remove.
    DaemonOwned(&'a Path),
}

impl LocalBookkeeping<'_> {
    /// Build from the store the command opened: `None` means the data dir is a
    /// daemon's, so `buyer_db` names the file that was left untouched.
    const fn new<'a>(
        store: Option<&'a RedbBuyerPoolStore>,
        buyer_db: &'a Path,
    ) -> LocalBookkeeping<'a> {
        match store {
            Some(store) => LocalBookkeeping::Store(store),
            None => LocalBookkeeping::DaemonOwned(buyer_db),
        }
    }

    /// Say, on stderr, that the daemon's row survives and what to do about it.
    /// The on-chain state has already changed, so this is not a warning the
    /// operator may ignore: the daemon still believes it owns a live pool.
    fn report_daemon_row_untouched(buyer_db: &Path, pool_id: PoolId, verb: &str) {
        eprintln!(
            "warning: pool {pool_id} {verb} on-chain, but the daemon's row in {} was NOT \
             cleared — `decdn pool` writes only the client store, and a running decdn-node holds \
             its own exclusively. Run `decdn node pools`: if that pool is listed, the daemon is \
             still using it — restart decdn-node so its bootstrap reconciles against the chain.",
            buyer_db.display()
        );
    }

    /// Clear the row after a mined `closePool`.
    ///
    /// # Errors
    ///
    /// Errors when the client store faulted — see [`grade_local_forget`].
    fn forget_after_close(
        self,
        owner: Address,
        pool_id: PoolId,
        tx: TxHash,
        reclaim_note: &str,
    ) -> anyhow::Result<()> {
        match self {
            Self::Store(store) => grade_local_forget(
                store.forget_if_pool(owner, pool_id),
                pool_id,
                tx,
                reclaim_note,
            ),
            Self::DaemonOwned(buyer_db) => {
                Self::report_daemon_row_untouched(buyer_db, pool_id, "closed");
                Ok(())
            }
        }
    }

    /// Clear the row after a mined `reclaim`. Best-effort on both arms: the
    /// refund itself already landed.
    fn forget_after_reclaim(self, owner: Address, pool_id: PoolId) {
        match self {
            Self::Store(store) => {
                // `Ok(false)` costs nothing: reclaim is permissionless and the
                // row is legitimately absent after a `pool close` or a prior
                // reclaim. An `Err` is not free — the row may survive pointing
                // at a pool that is now `Closed`, whose `deposit` field still
                // reads healthy, so a later fetch reuses it and signs vouchers
                // `redeemMany` rejects outright. Warn rather than fail: the
                // refund itself landed, and re-running the reclaim is what
                // clears the row.
                if let Err(e) = store.forget_if_pool(owner, pool_id) {
                    eprintln!(
                        "warning: pool {pool_id} reclaimed on-chain but clearing it from the \
                         local store failed: {e}; if the row survived, a later fetch reuses this \
                         closed pool and its vouchers are rejected — re-run `decdn pool reclaim \
                         --pool {pool_id}` to clear it"
                    );
                }
            }
            Self::DaemonOwned(buyer_db) => {
                Self::report_daemon_row_untouched(buyer_db, pool_id, "reclaimed");
            }
        }
    }
}

/// Grade the local row-clear that follows a mined `closePool`.
///
/// `pool close` drops the local record *so that* a later `fetch` / `bundle pull`
/// opens a fresh pool rather than reusing one that is winding down. Only the
/// `Err` leaves that in doubt.
///
/// `forget_if_pool` is compare-and-delete, and its `Ok(false)` means the compare
/// found nothing to delete: the owner index holds no row, or it already points at
/// a newer pool. In both states nothing maps this owner to the closed pool, which
/// is the guarantee `close` wanted, and deleting anything further would destroy a
/// live replacement — the lost update the compare-and-delete exists to prevent.
/// Closing a pool this store never tracked lands there routinely: a second
/// machine, a fresh `--data-dir`, or an older pool superseded by a later
/// `pool open`. That is a clean close.
///
/// An `Err` is different. The write faulted, so the row may survive with a live
/// owner index pointing at the closed pool. A later fetch then reuses it and
/// signs vouchers that stop being redeemable at the dispute deadline, against a
/// deposit the owner is about to reclaim.
///
/// The close itself landed, so the error carries `tx` and `reclaim_note`: the
/// reclaim is both the deadline the success line would have printed and the
/// operation that clears the stale row.
///
/// # Errors
///
/// Errors when the store faulted, leaving the row's fate unknown.
fn grade_local_forget(
    outcome: Result<bool, decdn_incentive::StoreError>,
    pool_id: PoolId,
    tx: TxHash,
    reclaim_note: &str,
) -> anyhow::Result<()> {
    match outcome {
        Ok(_) => Ok(()),
        Err(e) => anyhow::bail!(
            "pool {pool_id} closed on-chain (tx {tx}) but clearing it from the local store \
             failed: {e}; the row may survive and send a later fetch back to this closing \
             pool, whose vouchers stop being redeemable at the dispute deadline — \
             {reclaim_note}, which also clears the row"
        ),
    }
}

/// Terminal outcome of a single close/reclaim tx.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TxOutcome {
    /// Mined with a success receipt.
    Landed,
    /// Deterministically reverted (caught at estimation or a failing receipt).
    Reverted,
}

/// A mined-but-reverted tx returns `Ok(receipt)` with `status() == false`, so map
/// the receipt status to the outcome.
const fn receipt_outcome(landed: bool) -> TxOutcome {
    if landed {
        TxOutcome::Landed
    } else {
        TxOutcome::Reverted
    }
}

/// What `close --all` does with one enumerated pool, decided from its on-chain
/// status alone. Only `Open` pools have anything to close.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClosePlan {
    /// `Open` — send `closePool`.
    Close,
    /// Not `Open` — nothing to do; the string is the human-readable reason.
    Skip(&'static str),
}

/// What `reclaim --all` does with one enumerated pool, decided from its status
/// and dispute deadline against the current time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReclaimPlan {
    /// `Closing` and its dispute window has elapsed — send `reclaim`.
    Reclaim,
    /// `Open` — it must be closed first, so there is nothing to reclaim yet.
    SkipOpen,
    /// `Closing` but still inside the dispute window; carries the absolute
    /// Unix deadline so the operator learns when it becomes reclaimable.
    SkipInWindow(u64),
    /// `Closed` — the reclaim already happened (that is what sets `Closed`).
    SkipClosed,
    /// An unrecognized on-chain status (the hidden invalid `sol!` variant) —
    /// nothing safe to reclaim, and not asserted to have been reclaimed.
    SkipUnknown,
}

/// Running counts for a `--all` sweep. The exit code keys off `failed`; the
/// other two are reported for the operator.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct BatchTally {
    /// Pools closed or reclaimed successfully.
    acted: usize,
    /// Pools intentionally left untouched (wrong status / window not elapsed).
    skipped: usize,
    /// Pools whose read or transaction errored.
    failed: usize,
}

/// Classify one enumerated pool for `close --all`.
const fn plan_close(status: PaymentPool::Status) -> ClosePlan {
    match status {
        PaymentPool::Status::Open => ClosePlan::Close,
        PaymentPool::Status::Closing => ClosePlan::Skip("already closing"),
        PaymentPool::Status::Closed => ClosePlan::Skip("already closed"),
        // `sol!` enums carry a hidden invalid variant, so a wildcard is
        // required; an unrecognized status has nothing safe to close.
        _ => ClosePlan::Skip("unknown on-chain status"),
    }
}

/// Classify one enumerated pool for `reclaim --all`. `reclaim` reverts unless
/// the pool is `Closing` past its deadline, so the plan pre-filters to exactly
/// that case and names why each other pool is skipped. `now == deadline` counts
/// as elapsed, matching the on-chain `block.timestamp >= disputeDeadline` gate.
const fn plan_reclaim(status: PaymentPool::Status, dispute_deadline: u64, now: u64) -> ReclaimPlan {
    match status {
        PaymentPool::Status::Open => ReclaimPlan::SkipOpen,
        PaymentPool::Status::Closing if now >= dispute_deadline => ReclaimPlan::Reclaim,
        PaymentPool::Status::Closing => ReclaimPlan::SkipInWindow(dispute_deadline),
        PaymentPool::Status::Closed => ReclaimPlan::SkipClosed,
        // `sol!` enums carry a hidden invalid variant, so a wildcard is
        // required; an unrecognized status is not asserted to be reclaimed.
        _ => ReclaimPlan::SkipUnknown,
    }
}

/// Turn a finished `--all` sweep into a process result. Any failed pool is a
/// nonzero exit; "nothing eligible" is success — the summary line already says
/// so.
///
/// # Errors
///
/// Errors when at least one pool failed to `verb`.
fn batch_result(tally: &BatchTally, verb: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        tally.failed == 0,
        "{} pool(s) failed to {verb}; see the summary above",
        tally.failed
    );
    Ok(())
}

/// Send `closePool(pool_id)`, wait for the receipt, and on success clear the
/// local buyer row so a later fetch opens a fresh pool. Ownership and `Open`
/// status are the caller's responsibility. Returns the reclaim note shared by
/// the success line and the row-clear warning.
///
/// # Errors
///
/// Errors when the send is rejected, the receipt cannot be read, the tx
/// reverted, or the local row-clear faulted after a landed close.
async fn close_and_forget<P>(
    contract: &PaymentPool::PaymentPoolInstance<P>,
    store: LocalBookkeeping<'_>,
    owner: Address,
    pool_id: PoolId,
) -> anyhow::Result<String>
where
    P: alloy::providers::Provider + Clone,
{
    let pending = match contract.closePool(pool_id).send().await {
        Ok(pending) => pending,
        Err(e) if e.as_revert_data().is_some() => {
            anyhow::bail!("closePool reverted on-chain for pool {pool_id}")
        }
        Err(e) => anyhow::bail!("closePool send failed: {e}"),
    };
    let receipt = pending
        .get_receipt()
        .await
        .map_err(|e| anyhow::anyhow!("closePool receipt failed: {e}"))?;

    match receipt_outcome(receipt.status()) {
        TxOutcome::Landed => {
            // The dispute deadline is only known post-close; read it best-effort
            // and never print the pre-close `0` if that read fails. The local
            // record is dropped so a later `fetch`/`bundle pull` opens a fresh
            // pool rather than reusing one that is winding down. Any transport
            // error is redacted here, since the caller may print it directly
            // rather than through `main`'s error boundary.
            let deadline_note = match contract.getPool(pool_id).call().await {
                Ok(p) => format!("after Unix {}", p.disputeDeadline),
                Err(e) => format!(
                    "after the dispute window (couldn't read the exact deadline: {})",
                    sanitize_rpc_display(e)
                ),
            };
            // One note for both the success line and the failure, so the two
            // cannot drift into different instructions for the same deadline.
            let reclaim_note = format!("run `decdn pool reclaim --pool {pool_id}` {deadline_note}");
            store.forget_after_close(owner, pool_id, receipt.transaction_hash, &reclaim_note)?;
            Ok(reclaim_note)
        }
        TxOutcome::Reverted => anyhow::bail!(
            "closePool reverted on-chain for pool {pool_id} (it may have raced a concurrent \
             close)"
        ),
    }
}

/// Send `reclaim(pool_id)`, wait for the receipt, and on success best-effort
/// clear the local row. `reclaim` is permissionless, so no ownership check is
/// needed. Returns `Ok(())` once the refund lands.
///
/// # Errors
///
/// Errors when the send is rejected, the receipt cannot be read, or the tx
/// reverted (not closed, or the dispute window has not elapsed). A failed local
/// row-clear only warns — the refund itself landed.
async fn reclaim_and_forget<P>(
    contract: &PaymentPool::PaymentPoolInstance<P>,
    store: LocalBookkeeping<'_>,
    owner: Address,
    pool_id: PoolId,
) -> anyhow::Result<()>
where
    P: alloy::providers::Provider + Clone,
{
    let pending = match contract.reclaim(pool_id).send().await {
        Ok(pending) => pending,
        Err(e) if e.as_revert_data().is_some() => {
            anyhow::bail!(
                "reclaim reverted on-chain for pool {pool_id} (not closed, or its dispute \
                 window has not elapsed yet)"
            )
        }
        Err(e) => anyhow::bail!("reclaim send failed: {e}"),
    };
    let receipt = pending
        .get_receipt()
        .await
        .map_err(|e| anyhow::anyhow!("reclaim receipt failed: {e}"))?;

    match receipt_outcome(receipt.status()) {
        TxOutcome::Landed => {
            store.forget_after_reclaim(owner, pool_id);
            Ok(())
        }
        TxOutcome::Reverted => anyhow::bail!(
            "reclaim reverted on-chain for pool {pool_id} (not closed, or its dispute window \
             has not elapsed yet)"
        ),
    }
}

/// `decdn pool close`: start the grace-window close on a pool the caller owns
/// (`closePool`). Redemptions stay valid until the dispute deadline; `pool
/// reclaim` refunds the residual after it elapses. `--all` closes every `Open`
/// pool this keystore owns; `--pool` closes exactly one.
async fn close(args: &cli::PoolCloseArgs, config_path: Option<&Path>) -> anyhow::Result<()> {
    let file = load_file_config(config_path)?;
    // Parse the target id before any store/keystore/provider work, so a
    // malformed `--pool` fails fast without touching the chain. `None` is the
    // `--all` sweep (clap's arg group guarantees exactly one of the two).
    let target = args.pool.as_deref().map(parse_pool_id).transpose()?;

    let store_owner = classify_buyer_store(&resolve_data_dir(args.chain.data_dir.clone(), &file)?);
    if target.is_none() {
        // `--all` enumerates from chain, not from the store, so on a node data
        // dir it would close the pool the daemon is paying from right now.
        store_owner.refuse_sweep("close")?;
    }
    let chain = resolve_chain(&args.chain, &file)?;
    let store = store_owner.open_for_write()?;
    let buyer_db = node_buyer_db(&chain.data_dir);
    let books = LocalBookkeeping::new(store.as_ref(), &buyer_db);
    let signer = Arc::new(load_buyer_signer(&chain)?);
    let owner = signer.address();
    let rpc = provider::build_provider(&chain.rpc_url, &signer)?;
    let contract = PaymentPool::new(chain.payment_pool, rpc);

    let Some(pool_id) = target else {
        return close_all(&contract, books, owner).await;
    };

    let pool = contract
        .getPool(pool_id)
        .call()
        .await
        .map_err(|e| anyhow::anyhow!("getPool failed: {e}"))?;
    ensure_owned(pool.owner, owner, pool_id)?;
    anyhow::ensure!(
        matches!(pool.status, PaymentPool::Status::Open),
        "pool {pool_id} is not Open — nothing to close (already closing or closed)"
    );

    let reclaim_note = close_and_forget(&contract, books, owner, pool_id).await?;
    println!("closed pool {pool_id}; dispute window open — {reclaim_note}");
    Ok(())
}

/// `close --all`: enumerate every pool this keystore owns and close the `Open`
/// ones. One pool's read or close failure is recorded and the sweep continues;
/// the exit code is nonzero only if any pool failed.
async fn close_all<P>(
    contract: &PaymentPool::PaymentPoolInstance<P>,
    store: LocalBookkeeping<'_>,
    owner: Address,
) -> anyhow::Result<()>
where
    P: alloy::providers::Provider + Clone,
{
    let ids = enumerate_owned_pools(contract, owner).await?;
    if ids.is_empty() {
        println!("no pools found for this keystore — nothing to close");
        return Ok(());
    }

    let mut tally = BatchTally::default();
    for pool_id in ids {
        let status = match contract.getPool(pool_id).call().await {
            Ok(pool) => pool.status,
            Err(e) => {
                eprintln!(
                    "failed to read pool {pool_id}: {}",
                    decdn_common::redact::sanitize_err_chain(&anyhow::anyhow!("{e}"))
                );
                tally.failed += 1;
                continue;
            }
        };
        match plan_close(status) {
            ClosePlan::Close => match close_and_forget(contract, store, owner, pool_id).await {
                Ok(note) => {
                    println!("closed pool {pool_id}; dispute window open — {note}");
                    tally.acted += 1;
                }
                Err(e) => {
                    eprintln!(
                        "failed to close pool {pool_id}: {}",
                        decdn_common::redact::sanitize_err_chain(&e)
                    );
                    tally.failed += 1;
                }
            },
            ClosePlan::Skip(reason) => {
                println!("skipped pool {pool_id}: {reason}");
                tally.skipped += 1;
            }
        }
    }
    println!(
        "close --all: closed {}, skipped {}, failed {}",
        tally.acted, tally.skipped, tally.failed
    );
    batch_result(&tally, "close")
}

/// `decdn pool reclaim`: refund the residual deposit of a pool once its grace
/// window has elapsed (`reclaim`; permissionless — callable by anyone, but only
/// the owner receives funds). `--all` reclaims every pool this keystore owns
/// whose window has elapsed; `--pool` reclaims exactly one.
async fn reclaim(args: &cli::PoolReclaimArgs, config_path: Option<&Path>) -> anyhow::Result<()> {
    let file = load_file_config(config_path)?;
    // Parse the target id before any store/keystore/provider work, so a
    // malformed `--pool` fails fast without touching the chain. `None` is the
    // `--all` sweep (clap's arg group guarantees exactly one of the two).
    let target = args.pool.as_deref().map(parse_pool_id).transpose()?;

    let store_owner = classify_buyer_store(&resolve_data_dir(args.chain.data_dir.clone(), &file)?);
    if target.is_none() {
        store_owner.refuse_sweep("reclaim")?;
    }
    let chain = resolve_chain(&args.chain, &file)?;
    let store = store_owner.open_for_write()?;
    let buyer_db = node_buyer_db(&chain.data_dir);
    let books = LocalBookkeeping::new(store.as_ref(), &buyer_db);
    let signer = Arc::new(load_buyer_signer(&chain)?);
    let owner = signer.address();
    let rpc = provider::build_provider(&chain.rpc_url, &signer)?;
    let contract = PaymentPool::new(chain.payment_pool, rpc);

    let Some(pool_id) = target else {
        return reclaim_all(&contract, books, owner).await;
    };

    reclaim_and_forget(&contract, books, owner, pool_id).await?;
    println!("reclaimed pool {pool_id}; residual deposit refunded to its owner");
    Ok(())
}

/// `reclaim --all`: enumerate every pool this keystore owns and reclaim the ones
/// whose dispute window has elapsed. Pools still `Open`, still inside the
/// window, or already reclaimed are skipped (in-window pools report when they
/// become reclaimable). One pool's failure is recorded and the sweep continues;
/// the exit code is nonzero only if any pool failed.
async fn reclaim_all<P>(
    contract: &PaymentPool::PaymentPoolInstance<P>,
    store: LocalBookkeeping<'_>,
    owner: Address,
) -> anyhow::Result<()>
where
    P: alloy::providers::Provider + Clone,
{
    let now = unix_now()?;
    let ids = enumerate_owned_pools(contract, owner).await?;
    if ids.is_empty() {
        println!("no pools found for this keystore — nothing to reclaim");
        return Ok(());
    }

    let mut tally = BatchTally::default();
    for pool_id in ids {
        let pool = match contract.getPool(pool_id).call().await {
            Ok(pool) => pool,
            Err(e) => {
                eprintln!(
                    "failed to read pool {pool_id}: {}",
                    decdn_common::redact::sanitize_err_chain(&anyhow::anyhow!("{e}"))
                );
                tally.failed += 1;
                continue;
            }
        };
        match plan_reclaim(pool.status, pool.disputeDeadline, now) {
            ReclaimPlan::Reclaim => {
                match reclaim_and_forget(contract, store, owner, pool_id).await {
                    Ok(()) => {
                        println!(
                            "reclaimed pool {pool_id}; residual deposit refunded to its owner"
                        );
                        tally.acted += 1;
                    }
                    Err(e) => {
                        eprintln!(
                            "failed to reclaim pool {pool_id}: {}",
                            decdn_common::redact::sanitize_err_chain(&e)
                        );
                        tally.failed += 1;
                    }
                }
            }
            ReclaimPlan::SkipOpen => {
                println!("skipped pool {pool_id}: still Open — run `pool close` first");
                tally.skipped += 1;
            }
            ReclaimPlan::SkipInWindow(deadline) => {
                println!(
                    "skipped pool {pool_id}: dispute window open — reclaimable after Unix \
                     {deadline}"
                );
                tally.skipped += 1;
            }
            ReclaimPlan::SkipClosed => {
                println!("skipped pool {pool_id}: already reclaimed");
                tally.skipped += 1;
            }
            ReclaimPlan::SkipUnknown => {
                println!("skipped pool {pool_id}: unrecognized on-chain status");
                tally.skipped += 1;
            }
        }
    }
    println!(
        "reclaim --all: reclaimed {}, skipped {}, failed {}",
        tally.acted, tally.skipped, tally.failed
    );
    batch_result(&tally, "reclaim")
}

/// Resolve the capability's absolute Unix-seconds expiry from the mutually
/// exclusive `--expiry-secs` (relative to now) / `--expiry-at` (absolute)
/// flags. Exactly one is required, and the result must lie in the future — a
/// capability that expires at or before now is dead on arrival (the node stops
/// accepting its vouchers immediately).
fn resolve_expiry(
    now: u64,
    expiry_secs: Option<u64>,
    expiry_at: Option<u64>,
) -> anyhow::Result<u64> {
    let expiry = match (expiry_secs, expiry_at) {
        (Some(secs), None) => now.checked_add(secs).ok_or_else(|| {
            anyhow::anyhow!("--expiry-secs {secs} overflows the Unix epoch from now ({now})")
        })?,
        (None, Some(at)) => at,
        // clap `conflicts_with` rules out `(Some, Some)`; this leaves `(None, None)`.
        _ => anyhow::bail!("exactly one of --expiry-secs or --expiry-at is required"),
    };
    anyhow::ensure!(
        expiry > now,
        "capability expiry {expiry} is not in the future (now is {now}); it would be dead on \
         arrival — pick a later --expiry-at or a positive --expiry-secs"
    );
    Ok(expiry)
}

/// Current Unix time in whole seconds.
///
/// # Errors
///
/// Errors if the system clock is before the Unix epoch. Defaulting to `0` there
/// would let `assign` pass its `expiry > now` guard trivially and print a token
/// that expired decades ago.
fn unix_now() -> anyhow::Result<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .map_err(|e| anyhow::anyhow!("the system clock is before the Unix epoch: {e}"))
}

/// Render an absolute Unix-seconds expiry as `"<ts> (in Nd Nh Nm)"` relative to
/// `now`. The relative tail is what an operator actually reasons about; the raw
/// timestamp is kept for an exact, timezone-free reference.
fn format_expiry(expiry: u64, now: u64) -> String {
    let remaining = expiry.saturating_sub(now);
    let days = remaining / 86_400;
    let hours = (remaining % 86_400) / 3_600;
    let mins = (remaining % 3_600) / 60;
    format!("Unix {expiry} (in {days}d {hours}h {mins}m)")
}

/// `decdn pool assign`: issue an owner-signed spending capability delegating a
/// bounded spend on a pool the caller owns to a delegate `--signer` key, and
/// print the `dcap1:` token to hand to that delegated client (ADR 003
/// §Capability delegation).
///
/// The capability is signed offline with the owner keystore against the pool's
/// EIP-712 domain (`PaymentPool` address + chain id). Offline issuance is valid,
/// so an unreachable RPC only warns — the node is the one that enforces the
/// owner signature against the pool's on-chain owner at redemption time. An
/// owner read that *succeeds* is different evidence: a pool's owner is fixed at
/// open, so a different owner — or no such pool — proves the token is already
/// dead, and no token is printed.
async fn assign(args: &cli::PoolAssignArgs, config_path: Option<&Path>) -> anyhow::Result<()> {
    let file = load_file_config(config_path)?;
    let chain = resolve_chain(&args.chain, &file)?;
    let pool_id = parse_pool_id(&args.pool)?;
    let signer_addr = super::chain_ctx::parse_nonzero_address(&args.signer, "--signer")?;

    let now = unix_now()?;
    let expiry = resolve_expiry(now, args.expiry_secs, args.expiry_at)?;

    let owner_signer = load_buyer_signer(&chain)?;
    let owner = owner_signer.address();
    let domain = voucher_domain(chain.chain_id, chain.payment_pool);
    let spending_cap = args.cap_micro_usdc;

    let capability = Capability {
        signer: signer_addr,
        spending_cap,
        pool_id,
        expiry,
    };
    let signed = capability.sign(&owner_signer, &domain)?;
    let grant = CapabilityGrant::from_signed_capability(&signed);
    let token = grant.to_token();

    // The owner the node will recover from the token — by construction this is
    // the keystore we just signed with, so it doubles as a codec round-trip
    // check before the token ever leaves the machine.
    let recovered = grant
        .owner(&domain)
        .map_err(|e| anyhow::anyhow!("re-recovering the owner from the fresh token failed: {e}"))?;

    // On-chain owner check. A read we could not perform only warns — offline
    // issuance is valid, and the serving node enforces the owner signature
    // against the pool's real owner at redemption. A read that *succeeded* and
    // disagrees is different evidence entirely, and fails: see
    // `ensure_on_chain_owner`.
    match provider::build_provider(&chain.rpc_url, &owner_signer) {
        Ok(rpc) => {
            let contract = PaymentPool::new(chain.payment_pool, rpc);
            ensure_on_chain_owner(&contract, pool_id, owner).await?;
        }
        Err(e) => eprintln!(
            "warning: could not build an RPC provider to check the on-chain owner of pool \
             {pool_id} ({}); issuing anyway — the node verifies the owner signature at redemption",
            sanitize_rpc_display(e)
        ),
    }

    println!("pool:         {pool_id}");
    println!("delegate:     {signer_addr} (the voucher-signing key this authorizes)");
    println!(
        "spending cap: {} USDC ({} µUSDC)",
        format_usdc_u256(U256::from(spending_cap)),
        args.cap_micro_usdc
    );
    println!("expiry:       {}", format_expiry(expiry, now));
    println!("owner:        {recovered} (recovered from the signature)");
    println!("Token (give this to the delegated client):");
    println!("{token}");
    Ok(())
}

/// Refuse to issue a capability the command has already proven dead, and warn
/// when it could not find out either way.
///
/// The two outcomes are different evidence, and it matters that they do not
/// share an exit code. A `getPool` that **succeeded** proves what the token is
/// worth: a pool's owner is fixed at open, so an owner that is not this keystore
/// means the token is rejected at redemption. Printing it and exiting 0 means
/// `decdn pool assign … > delegate.token` writes a file that looks valid and is
/// not, and nothing downstream finds out until a delegate's first voucher
/// bounces. A read that could not be performed proves nothing: offline issuance
/// is valid by design (`Assign` is signed entirely from the owner keystore), and
/// the serving node enforces the owner signature at redemption regardless — so
/// that leg degrades to a warning.
///
/// # Errors
///
/// Errors when the read succeeded and the pool is not owned by `expected` —
/// including a pool that does not exist on this contract.
async fn ensure_on_chain_owner<P>(
    contract: &PaymentPool::PaymentPoolInstance<P>,
    pool_id: PoolId,
    expected: Address,
) -> anyhow::Result<()>
where
    P: alloy::providers::Provider + Clone,
{
    match contract.getPool(pool_id).call().await {
        Ok(pool) => grade_on_chain_owner(pool.owner, expected).into_result(pool_id, expected),
        Err(e) => {
            eprintln!(
                "warning: could not read pool {pool_id} on-chain to confirm ownership ({}); \
                 issuing anyway — the node verifies the owner signature at redemption",
                sanitize_rpc_display(e)
            );
            Ok(())
        }
    }
}

/// The verdict [`ensure_on_chain_owner`] reaches on a read that succeeded.
///
/// Separated from the RPC call so the decision is unit-testable without a
/// provider, and so the two arms — "the chain says no" and "the chain did not
/// answer" — stay distinct.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OwnerVerdict {
    /// The pool exists and this keystore owns it.
    Owned,
    /// No pool exists at this id on this contract. `getPool` zero-fills an
    /// unknown key instead of reverting, so this arrives as a successful read.
    NoSuchPool,
    /// The pool exists and this address owns it instead.
    OwnedByOther(Address),
}

/// Grade the `owner` a successful `getPool` returned against the issuing
/// keystore.
fn grade_on_chain_owner(on_chain: Address, expected: Address) -> OwnerVerdict {
    if on_chain.is_zero() {
        OwnerVerdict::NoSuchPool
    } else if on_chain == expected {
        OwnerVerdict::Owned
    } else {
        OwnerVerdict::OwnedByOther(on_chain)
    }
}

impl OwnerVerdict {
    /// Refuse to issue a capability this verdict has proven dead.
    ///
    /// Both failing arms name what the operator has to change, and they name
    /// different things: a wrong keystore is not a wrong `--pool`.
    ///
    /// # Errors
    ///
    /// Errors on every verdict except [`OwnerVerdict::Owned`].
    fn into_result(self, pool_id: PoolId, expected: Address) -> anyhow::Result<()> {
        match self {
            Self::Owned => Ok(()),
            Self::NoSuchPool => anyhow::bail!(
                "pool {pool_id} does not exist on this PaymentPool contract; a capability for a \
                 pool that was never opened is rejected at redemption, so no token was issued — \
                 check --pool, --payment-pool-address, and --chain-id"
            ),
            Self::OwnedByOther(on_chain) => anyhow::bail!(
                "pool {pool_id} on-chain owner {on_chain} is not this keystore's address \
                 {expected}; a capability signed by a non-owner is rejected at redemption, so no \
                 token was issued — sign with the owner keystore, or check --pool"
            ),
        }
    }
}

/// `decdn pool list` / `status`: read-only dump of the tracked buyer pools and
/// their per-lane voucher watermark.
///
/// Names the store it read, always. There are two, in the same directory and
/// with the same table format but no other relation: the CLI's own
/// `buyer-pools.redb` and a daemon's `buyer.redb`. Reading the first and
/// reporting it as the second makes `pools=0` meaningless on a node host
/// (#2078), so on a daemon's data dir this asks the daemon instead — redb holds
/// that file exclusively for the daemon's lifetime, so there is no disk path to
/// it while the node is up.
async fn list(args: &cli::PoolListArgs, config_path: Option<&Path>) -> anyhow::Result<()> {
    let data_dir = match args.chain.data_dir.clone() {
        Some(dir) => expand_tilde(&dir),
        None => resolve_data_dir(None, &load_file_config(config_path)?)?,
    };

    if args.all {
        return list_all(args, config_path, &data_dir).await;
    }

    match classify_buyer_store(&data_dir) {
        BuyerStoreOwner::Node { data_dir, .. } => {
            list_from_daemon(args, config_path, &data_dir).await
        }
        BuyerStoreOwner::Client { data_dir } => list_from_client_store(args, &data_dir),
    }
}

/// `decdn pool list --all`: every pool this keystore owns on chain.
///
/// The local listings answer "what does this store remember?". This answers
/// "what do I own?", and the two are different questions precisely when it
/// matters: a reset data dir loses the only local record of a funded deposit,
/// and the store-backed listing then prints nothing because the file it reads
/// is the file that went missing (#2072). The chain still has the pool.
///
/// Read-only, so unlike `close --all` / `reclaim --all` it is not refused on a
/// node's data dir. Those two write — they would close the pool the daemon is
/// paying from. This one looks.
///
/// The local view is consulted best-effort, and feeds three things: the
/// `TRACKED` column, the `local_store=` / `local_store_read` flag, and the
/// stderr warning naming rows that will not decode. An unreadable store leaves
/// the column unknown rather than failing the listing, because an unreadable
/// store is the case this exists for.
async fn list_all(
    args: &cli::PoolListArgs,
    config_path: Option<&Path>,
    data_dir: &Path,
) -> anyhow::Result<()> {
    let file = load_file_config(config_path)?;
    let chain = resolve_chain(&args.chain, &file)?;
    let signer = Arc::new(load_buyer_signer(&chain)?);
    let owner = signer.address();
    let rpc = provider::build_provider(&chain.rpc_url, &signer)?;
    let contract = PaymentPool::new(chain.payment_pool, rpc);

    let ids = enumerate_owned_pools(&contract, owner).await?;
    let tracked = tracked_pool_ids(args, config_path, data_dir).await;
    let now = unix_now()?;

    let mut rows = Vec::with_capacity(ids.len());
    for pool_id in ids {
        let pool = contract.getPool(pool_id).call().await.map_err(|e| {
            anyhow::anyhow!(
                "getPool({pool_id}) failed: {}",
                decdn_common::redact::sanitize_err_chain(&anyhow::anyhow!("{e}"))
            )
        })?;
        rows.push(ChainPoolRow {
            pool_id,
            status: pool.status,
            deposit: U256::from(pool.deposit),
            total_redeemed: U256::from(pool.totalRedeemed),
            dispute_deadline: pool.disputeDeadline,
            tracked: tracked.as_ref().and_then(|local| local.verdict(pool_id)),
        });
    }

    // The corrupt rows are named on stderr here too, so `--all` is not the one
    // listing that stays silent about a deposit stranded by an unreadable
    // record. stdout keeps only the table, for scripts.
    if let Some(local) = tracked.as_ref() {
        write_skipped_pools(&mut std::io::stderr().lock(), &local.undecodable())?;
    }

    let mut out = std::io::stdout().lock();
    if args.json {
        let view = PoolListAllJson {
            source: SOURCE_CHAIN,
            owner: format!("{owner:#x}"),
            chain_id: chain.chain_id,
            payment_pool: format!("{:#x}", chain.payment_pool),
            local_store_read: tracked.is_some(),
            pools: rows.iter().map(|r| ChainPoolJson::at(r, now)).collect(),
        };
        serde_json::to_writer_pretty(&mut out, &view)?;
        writeln!(out)?;
    } else {
        write_chain_pools(
            &mut out,
            owner,
            chain.chain_id,
            &rows,
            now,
            tracked.is_some(),
        )?;
    }
    Ok(())
}

/// The state of one pool's row in the local record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RowState {
    /// The row decoded.
    Decoded,
    /// A row exists and will not decode. The escrowed deposit is untracked
    /// until the record is repaired — a different remedy from an absent row, so
    /// it must not render as one.
    Undecodable,
}

/// What the local record says about the pools the chain reports.
///
/// One entry per pool the record mentions, because "the store has no row for
/// this pool" and "the store has a row it cannot decode" are different answers
/// and only the first means the deposit is untracked. An undecodable row is
/// still a row: `pool_id` is the table's primary key, so it survives whatever
/// corrupted the value bytes, and the store demonstrably holds that pool.
///
/// A map rather than two sets, so a pool cannot be both at once.
#[derive(Debug, Default)]
struct TrackedPools(std::collections::BTreeMap<PoolId, RowState>);

impl TrackedPools {
    /// Whether the local record tracks `pool_id`: `None` when a row exists but
    /// could not be decoded, so the honest answer for that one pool is unknown
    /// while every other pool's answer stays exact.
    fn verdict(&self, pool_id: PoolId) -> Option<bool> {
        match self.0.get(&pool_id) {
            Some(RowState::Decoded) => Some(true),
            Some(RowState::Undecodable) => None,
            None => Some(false),
        }
    }

    /// The pools with a row that will not decode, for the operator warning.
    fn undecodable(&self) -> Vec<PoolId> {
        self.0
            .iter()
            .filter(|(_, state)| **state == RowState::Undecodable)
            .map(|(id, _)| *id)
            .collect()
    }

    /// Project a running daemon's `admin_v1_pools` answer.
    ///
    /// `None` when any id on the wire will not parse. That is a format drift
    /// between the daemon's spelling and this binary's, and the failure mode of
    /// guessing is the worst one available: a dropped id renders as `no`, which
    /// tells an operator to close a pool the daemon is paying from right now.
    /// One unparseable id makes the whole answer untrustworthy, so it is
    /// reported as unknown rather than partially believed.
    fn from_wire(resp: &BuyerPoolsResponse) -> Option<Self> {
        let mut rows = std::collections::BTreeMap::new();
        for (id, state) in resp
            .pools
            .iter()
            .map(|p| (p.pool_id.as_str(), RowState::Decoded))
            .chain(
                resp.skipped
                    .iter()
                    .map(|p| (p.as_str(), RowState::Undecodable)),
            )
        {
            rows.insert(PoolId::from_str(id).ok()?, state);
        }
        Some(Self(rows))
    }
}

impl From<BuyerLoad> for TrackedPools {
    fn from(load: BuyerLoad) -> Self {
        Self(
            load.pools
                .iter()
                .map(|p| (p.pool_id, RowState::Decoded))
                .chain(
                    load.skipped
                        .into_iter()
                        .map(|id| (id, RowState::Undecodable)),
                )
                .collect(),
        )
    }
}

/// What the local record tracks, or `None` when it gave no usable answer.
///
/// `None` is a real answer, not a failure: it is what an operator sees when the
/// store is the thing that was lost, and rendering it as "unknown" beside the
/// chain's rows is more honest than reporting every pool untracked. It covers
/// the whole store — one undecodable row makes that one pool unknown, never the
/// others.
///
/// **Every read here is read-only.** `--all` sends no transaction and writes no
/// pool record, and it must not manufacture a store either: creating an empty
/// one and reporting its emptiness is the #2078 defect, and it would also make
/// a lost store — the condition this command exists to surface — indistinguish-
/// able from a store that tracks nothing.
///
/// A store that will not open is named on stderr rather than folded into the
/// column, because `?` alone cannot say whether to restore a backup, wait for
/// another process, or repair a record.
async fn tracked_pool_ids(
    args: &cli::PoolListArgs,
    config_path: Option<&Path>,
    data_dir: &Path,
) -> Option<TrackedPools> {
    match classify_buyer_store(data_dir) {
        BuyerStoreOwner::Client { data_dir } => read_local_pools(&client_buyer_db(&data_dir)),
        BuyerStoreOwner::Node { data_dir, .. } => {
            let buyer_db = node_buyer_db(&data_dir);
            match ReadOnlyBuyerPoolStore::open_file(&buyer_db) {
                Ok(reader) => load_local_pools(&reader, &buyer_db),
                // Only the lock means "a daemon holds this file"; that is the
                // one failure the admin RPC can answer instead. Every other
                // failure is about the file, and asking the daemon would report
                // it as an unknown column with no reason attached.
                Err(decdn_incentive::StoreError::AlreadyOpen { .. }) => {
                    daemon_pool_ids(args, config_path).await
                }
                Err(err) => {
                    warn_unreadable_store(&buyer_db, &err);
                    None
                }
            }
        }
    }
}

/// Read a buyer store at `path` without creating or locking it.
///
/// An absent store is an empty [`TrackedPools`], not an unknown: nothing is
/// tracked, which is exactly what a reset data dir should report against the
/// pools the chain still holds.
fn read_local_pools(path: &Path) -> Option<TrackedPools> {
    match ReadOnlyBuyerPoolStore::open_file(path) {
        Ok(reader) => load_local_pools(&reader, path),
        Err(decdn_incentive::StoreError::Absent { .. }) => Some(TrackedPools::default()),
        Err(err) => {
            warn_unreadable_store(path, &err);
            None
        }
    }
}

/// Hydrate an opened store, naming a read failure rather than silently
/// blanking the column.
fn load_local_pools(reader: &ReadOnlyBuyerPoolStore, path: &Path) -> Option<TrackedPools> {
    match reader.load_all() {
        Ok(load) => Some(TrackedPools::from(load)),
        Err(err) => {
            warn_unreadable_store(path, &err);
            None
        }
    }
}

/// Say on stderr why the `TRACKED` column is unknown.
///
/// The column has one glyph for every reason, so without this an operator
/// cannot tell a store another process holds from one that needs restoring
/// from backup. stdout keeps only the table, for scripts.
fn warn_unreadable_store(path: &Path, err: &decdn_incentive::StoreError) {
    let mut w = std::io::stderr().lock();
    let _ = writeln!(
        w,
        "warning: the local buyer store at {} could not be read ({err}); every TRACKED value \
         below is unknown, which is not the same as untracked",
        path.display()
    );
}

/// Ask a running daemon what it tracks. `None` on any failure — this feeds one
/// column, never the listing itself.
///
/// `skipped` crosses the admin wire as hex strings for exactly this reason: the
/// daemon cannot decode those rows either, and dropping them here would report
/// the pools as untracked.
async fn daemon_pool_ids(
    args: &cli::PoolListArgs,
    config_path: Option<&Path>,
) -> Option<TrackedPools> {
    if args.timeout_ms == 0 {
        // `list` rejects this outright; here the listing still stands, so say
        // what was skipped rather than leaving an unexplained column.
        let mut w = std::io::stderr().lock();
        let _ = writeln!(
            w,
            "warning: --timeout-ms 0 skips the daemon lookup (jsonrpsee reads a zero duration \
             as 'never'), so every TRACKED value below is unknown"
        );
        return None;
    }
    let url =
        crate::commands::node::resolve_admin_url(args.admin_url.as_deref(), config_path).ok()?;
    let client = jsonrpsee::http_client::HttpClientBuilder::default()
        .request_timeout(std::time::Duration::from_millis(args.timeout_ms))
        .build(&url)
        .ok()?;
    let resp: BuyerPoolsResponse = client.pools().await.ok()?;
    let tracked = TrackedPools::from_wire(&resp);
    if tracked.is_none() {
        let mut w = std::io::stderr().lock();
        let _ = writeln!(
            w,
            "warning: the daemon reported a pool id this binary cannot parse, so every TRACKED \
             value below is unknown. The daemon and this CLI disagree on the wire format — \
             check that both are the same build."
        );
    }
    tracked
}

/// Read the CLI's own `buyer-pools.redb` under `data_dir`.
fn list_from_client_store(args: &cli::PoolListArgs, data_dir: &Path) -> anyhow::Result<()> {
    let store_path = client_buyer_db(data_dir);
    let store = RedbBuyerPoolStore::open(data_dir)?;
    let load = store.load_all()?;
    write_store_listing(args, &store_path, SOURCE_CLIENT_STORE, "", load)
}

/// Render a [`BuyerLoad`] this process read out of a redb file itself.
///
/// Shared by the client store and the offline read of a stopped daemon's
/// store. `provenance` is appended to the `store=` line: the two reads produce
/// the same row shape from different files, so which file — and whether a
/// daemon was running — must be on the output, not inferred.
fn write_store_listing(
    args: &cli::PoolListArgs,
    store_path: &Path,
    source: &'static str,
    provenance: &str,
    load: BuyerLoad,
) -> anyhow::Result<()> {
    let BuyerLoad {
        mut pools,
        mut skipped,
    } = load;
    // Stable output regardless of the store's internal key order.
    pools.sort_by_key(|p| p.pool_id);
    skipped.sort_unstable();
    write_skipped_pools(&mut std::io::stderr().lock(), &skipped)?;

    let mut out = std::io::stdout().lock();
    if args.json {
        let view = PoolListJson {
            store: store_path.display().to_string(),
            source,
            pools: pools.iter().map(PoolJson::from).collect(),
            skipped: skipped.iter().map(|p| format!("{p:#x}")).collect(),
        };
        serde_json::to_writer_pretty(&mut out, &view)?;
        writeln!(out)?;
    } else {
        writeln!(out, "store={}{provenance}", store_path.display())?;
        write_pools(&mut out, &pools, &skipped)?;
    }
    Ok(())
}

/// Read a node's `buyer.redb` — through `admin_v1_pools` while the daemon runs,
/// and off disk when it does not.
///
/// The fallback is never the *client* store: that is a different file with
/// unrelated contents, and printing its `pools=0` here is precisely the failure
/// this path exists to prevent. It is the daemon's own file, read-only.
///
/// Only a refused connection takes the offline route, because it is the one
/// failure that positively means nothing is listening. A `Call` error means the
/// daemon ANSWERED and refused, so it is up. Every other transport error and
/// every timeout is ambiguous — a dropped packet, a firewall, a blackholing
/// port — and an ambiguous signal must not be resolved by opening a file a
/// running daemon may own.
///
/// On the refused path the lock settles it: `redb` holds its process-exclusive
/// lock for the lifetime of an open `Database`, so a read-only open succeeding
/// proves no daemon holds the file, and `AlreadyOpen` proves one does — which
/// makes a refused admin port an admin-URL problem rather than a stopped node.
async fn list_from_daemon(
    args: &cli::PoolListArgs,
    config_path: Option<&Path>,
    data_dir: &Path,
) -> anyhow::Result<()> {
    let buyer_db = node_buyer_db(data_dir);
    anyhow::ensure!(
        args.timeout_ms > 0,
        "--timeout-ms must be > 0 (jsonrpsee treats Duration::ZERO as \
         'never' rather than 'sub-millisecond deadline')"
    );
    let url = crate::commands::node::resolve_admin_url(args.admin_url.as_deref(), config_path)?;
    let client = jsonrpsee::http_client::HttpClientBuilder::default()
        .request_timeout(std::time::Duration::from_millis(args.timeout_ms))
        .build(&url)
        .with_context(|| format!("failed to build admin JSON-RPC client for {url}"))?;

    let resp: BuyerPoolsResponse = match client.pools().await {
        Ok(resp) => resp,
        // Connection refused: nothing is listening, so the daemon is very
        // likely down and its store readable. Try it before reporting.
        Err(jsonrpsee::core::client::Error::Transport(inner))
            if crate::commands::node::is_connection_refused(inner.as_ref()) =>
        {
            return list_from_stopped_daemon(args, data_dir, &buyer_db, &url, inner.as_ref());
        }
        Err(err) => {
            return Err(unreachable_daemon_error(
                args, data_dir, &buyer_db, &url, err,
            ));
        }
    };

    let mut out = std::io::stdout().lock();
    if args.json {
        let view = DaemonPoolListJson {
            store: buyer_db.display().to_string(),
            source: SOURCE_DAEMON,
            pools: &resp.pools,
            skipped: &resp.skipped,
        };
        serde_json::to_writer_pretty(&mut out, &view)?;
        writeln!(out)?;
    } else {
        write_skipped_buyer_pools(&mut std::io::stderr().lock(), &resp.skipped)?;
        writeln!(
            out,
            "store={} (read from the running daemon)",
            buyer_db.display()
        )?;
        write_buyer_pools(&mut out, &resp)?;
    }
    Ok(())
}

/// The error for an admin call that produced no pools, explaining which file
/// the answer would have come from.
fn unreachable_daemon_error(
    args: &cli::PoolListArgs,
    data_dir: &Path,
    buyer_db: &Path,
    url: &str,
    err: jsonrpsee::core::client::Error,
) -> anyhow::Error {
    {
        // A `Call` error means the daemon ANSWERED and refused — it is up, so
        // telling the operator to start it would send them the wrong way. Only
        // the transport and timeout arms warrant that advice.
        let answered = matches!(err, jsonrpsee::core::client::Error::Call(_));
        let classified = crate::commands::node::classify_client_error(url, args.timeout_ms, err);
        if answered {
            classified.context(format!(
                "{} is a decdn-node data dir, so its buyer pools live in {}, which this command \
                 reads through the daemon — a running daemon holds that file exclusively. The \
                 daemon answered and refused the read.",
                data_dir.display(),
                buyer_db.display(),
            ))
        } else {
            classified.context(format!(
                "{} is a decdn-node data dir, so the pools that matter are in {} — which a \
                 running daemon holds exclusively and no other process can open. This command \
                 therefore asks the daemon, and the daemon did not answer. Start decdn-node, or \
                 pass --data-dir <client dir> to inspect a client store instead.",
                data_dir.display(),
                buyer_db.display(),
            ))
        }
    }
}

/// Read a stopped daemon's `buyer.redb` off disk, or say why that is not what
/// is happening.
///
/// Three outcomes, and the third is why this is not simply a fallback:
///
/// - the file opens read-only, so no daemon holds it: render it, labelled as a
///   disk read, so provenance stays on the output.
/// - the file is write-locked, so a daemon IS running: the admin URL is wrong,
///   not the node down. Say that instead — it is the answer the operator needs.
/// - anything else: the original unreachable-daemon error, with what the disk
///   read found appended, because "no daemon answered AND the store will not
///   open" is two facts, not one.
fn list_from_stopped_daemon(
    args: &cli::PoolListArgs,
    data_dir: &Path,
    buyer_db: &Path,
    url: &str,
    transport: &(dyn std::error::Error + Send + Sync + 'static),
) -> anyhow::Result<()> {
    match ReadOnlyBuyerPoolStore::open_file(buyer_db) {
        Ok(reader) => {
            let load = reader.load_all()?;
            write_store_listing(
                args,
                buyer_db,
                SOURCE_NODE_STORE_OFFLINE,
                " (read from disk; no daemon running)",
                load,
            )
        }
        Err(decdn_incentive::StoreError::AlreadyOpen { .. }) => anyhow::bail!(
            "admin at {url} refused the connection ({transport}), but a process holds {} \
             exclusively — so a decdn-node IS running against {}. The admin URL or admin_port \
             is wrong, not the node down. Pass --admin-url, or check admin_port in the node's \
             config.",
            buyer_db.display(),
            data_dir.display(),
        ),
        // A crashed daemon is both unreachable AND unrepaired, so this is the
        // common post-mortem case, not a footnote on the generic failure. The
        // remedy names the process that owns the file, which the store cannot.
        Err(decdn_incentive::StoreError::NeedsRepair { .. }) => anyhow::bail!(
            "admin at {url} refused the connection ({transport}), and {} was not shut down \
             cleanly — it needs a repair pass that only a writable open can run. Start \
             decdn-node once against {} to repair it, then read it again.",
            buyer_db.display(),
            data_dir.display(),
        ),
        // No store at all is an answer, not a failure to report: this node has
        // never opened a pool, or the store was deleted to force re-adoption.
        Err(decdn_incentive::StoreError::Absent { .. }) => anyhow::bail!(
            "admin at {url} refused the connection ({transport}), and {} does not exist — this \
             node holds no buyer pools on disk. It has not opened one, or the store was deleted \
             to force re-adoption. Start decdn-node and read it again, or run `decdn pool list \
             --all` to see what this keystore owns on chain.",
            buyer_db.display(),
        ),
        Err(store_err) => Err(anyhow::anyhow!(
            "admin at {url} refused the connection ({transport}); is the node running, and is \
             admin_port configured correctly? Reading {} from disk instead did not work either: \
             {store_err}",
            buyer_db.display(),
        )
        .context(format!(
            "{} is a decdn-node data dir, so the pools that matter are in {} — not in a client \
             store. Pass --data-dir <client dir> to inspect a client store instead.",
            data_dir.display(),
            buyer_db.display(),
        ))),
    }
}

/// One pool as the chain describes it, plus whether the local record has it.
///
/// No `Debug`: the `sol!`-generated `Status` has none. [`status_label`] is the
/// spelling anything user-facing wants anyway.
struct ChainPoolRow {
    /// On-chain `poolId`.
    pool_id: PoolId,
    /// Lifecycle state from `getPool`.
    status: PaymentPool::Status,
    /// Escrowed deposit, in micro-USDC.
    deposit: U256,
    /// Cumulative amount redeemed against the pool, in micro-USDC.
    total_redeemed: U256,
    /// Absolute Unix deadline after which a `Closing` pool is reclaimable. `0`
    /// while the pool is still `Open`.
    dispute_deadline: u64,
    /// Whether the local record tracks this pool; `None` when it gave no usable
    /// answer for this pool. See [`TrackedPools::verdict`].
    tracked: Option<bool>,
}

/// Wire spelling of a `getPool` status, matching the lowercase vocabulary the
/// rest of the `pool` output uses.
const fn status_label(status: PaymentPool::Status) -> &'static str {
    match status {
        PaymentPool::Status::Open => "open",
        PaymentPool::Status::Closing => "closing",
        PaymentPool::Status::Closed => "closed",
        // `sol!` enums carry a hidden invalid variant.
        _ => "?",
    }
}

/// When the pool's residual can be reclaimed, in the operator's terms.
fn reclaimable_label(row: &ChainPoolRow, now: u64) -> String {
    match plan_reclaim(row.status, row.dispute_deadline, now) {
        ReclaimPlan::Reclaim => "now".to_string(),
        ReclaimPlan::SkipOpen => "close it first".to_string(),
        ReclaimPlan::SkipInWindow(deadline) => format_expiry(deadline, now),
        ReclaimPlan::SkipClosed => "already reclaimed".to_string(),
        ReclaimPlan::SkipUnknown => "?".to_string(),
    }
}

/// `yes` / `no` / `?`, where `?` means the local record gave no usable answer
/// for this pool — the whole store was unreadable, or it holds a row for this
/// pool that will not decode. Neither is the claim "this pool is untracked",
/// and the remedies differ: one is a lost store, the other a record to repair.
const fn tracked_label(tracked: Option<bool>) -> &'static str {
    match tracked {
        Some(true) => "yes",
        Some(false) => "no",
        None => "?",
    }
}

/// Render `--all` as the aligned table.
///
/// Same conventions as [`write_pools`]: a `key=value` summary line first so
/// scripts can grep without `--json`, a sentinel when there is nothing, and the
/// one variable-width column last and unpadded.
fn write_chain_pools(
    w: &mut impl Write,
    owner: Address,
    chain_id: u64,
    rows: &[ChainPoolRow],
    now: u64,
    local_store_read: bool,
) -> std::io::Result<()> {
    // `local_store=` is what separates a column of `?` that means "no local
    // answer at all" from one that means "these rows will not decode". Without
    // it the table is silently lossy about the one condition `--all` exists to
    // surface, and only `--json` carries the distinction.
    writeln!(
        w,
        "owner={owner:#x} chain_id={chain_id} pools={} local_store={}",
        rows.len(),
        if local_store_read {
            "read"
        } else {
            "unreadable"
        },
    )?;
    if rows.is_empty() {
        writeln!(w, "(this keystore owns no pools on this contract)")?;
        return Ok(());
    }
    writeln!(
        w,
        "{:<14} {:<8} {:>14} {:>14} {:>7} RECLAIMABLE",
        "POOL", "STATUS", "DEPOSIT", "REDEEMED", "TRACKED"
    )?;
    for row in rows {
        writeln!(
            w,
            "{:<14} {:<8} {:>14} {:>14} {:>7} {}",
            short_hex(&format!("{:#x}", row.pool_id)),
            status_label(row.status),
            format_usdc_u256(row.deposit),
            format_usdc_u256(row.total_redeemed),
            tracked_label(row.tracked),
            reclaimable_label(row, now),
        )?;
    }
    Ok(())
}

/// `--all` as JSON.
///
/// A third shape beside [`PoolListJson`] and [`DaemonPoolListJson`], and
/// deliberately so: these rows come from the chain, carry lifecycle fields the
/// stores do not hold, and carry no lanes. Read `source` before `pools`.
#[derive(Serialize)]
struct PoolListAllJson {
    /// Always [`SOURCE_CHAIN`].
    source: &'static str,
    /// The keystore address the pools were enumerated by.
    owner: String,
    /// Chain id the contract was read on.
    chain_id: u64,
    /// `PaymentPool` address the pools live in.
    payment_pool: String,
    /// Whether the local record could be read at all. When `false`, every
    /// pool's `tracked` is `null` and says nothing about the pool. When `true`,
    /// a `null` `tracked` is specific to that pool: its row will not decode.
    local_store_read: bool,
    /// Every pool the owner holds, in the order the contract enumerates them.
    pools: Vec<ChainPoolJson>,
}

/// One `--all` pool.
#[derive(Serialize)]
struct ChainPoolJson {
    /// On-chain `poolId`, `0x`-prefixed hex.
    pool_id: String,
    /// `open`, `closing`, `closed`, or `?`.
    status: &'static str,
    /// Escrowed deposit, in micro-USDC.
    deposit_micro_usdc: String,
    /// Cumulative redeemed amount, in micro-USDC.
    total_redeemed_micro_usdc: String,
    /// Absolute Unix deadline after which the residual is reclaimable; `0`
    /// while the pool is `Open`.
    dispute_deadline: u64,
    /// True once the dispute window has elapsed and the pool is `Closing`.
    reclaimable_now: bool,
    /// Whether the local record tracks this pool; `null` when the record gave
    /// no usable answer — an unreadable store, or a row for this pool that will
    /// not decode. Read `local_store_read` to tell the two apart.
    tracked: Option<bool>,
}

impl ChainPoolJson {
    /// Project a row as of `now`, so the time-dependent field is computed once
    /// against the same clock the table uses.
    fn at(row: &ChainPoolRow, now: u64) -> Self {
        Self {
            pool_id: format!("{:#x}", row.pool_id),
            status: status_label(row.status),
            deposit_micro_usdc: row.deposit.to_string(),
            total_redeemed_micro_usdc: row.total_redeemed.to_string(),
            dispute_deadline: row.dispute_deadline,
            reclaimable_now: matches!(
                plan_reclaim(row.status, row.dispute_deadline, now),
                ReclaimPlan::Reclaim
            ),
            tracked: row.tracked,
        }
    }
}

/// `source` value for a listing read directly from the CLI's own store.
const SOURCE_CLIENT_STORE: &str = "client_store";
/// `source` value for a listing the running daemon answered.
const SOURCE_DAEMON: &str = "daemon";
/// `source` value for a listing read off a stopped daemon's own store file.
///
/// Its own value, never [`SOURCE_CLIENT_STORE`], even though the row shape is
/// identical: the two come from different files with unrelated contents, and
/// conflating them is the whole defect this area exists to prevent.
const SOURCE_NODE_STORE_OFFLINE: &str = "node_store_offline";
/// `source` value for the chain-authoritative `--all` listing.
const SOURCE_CHAIN: &str = "chain";

/// Warn about every undecodable buyer row on stderr. The `pool_id` (the store's
/// primary key) is the only available repair handle.
fn write_skipped_pools(w: &mut impl Write, skipped: &[PoolId]) -> std::io::Result<()> {
    for pool_id in skipped {
        writeln!(
            w,
            "warning: buyer pool {pool_id:#x} could not be decoded; its deposit remains \
             escrowed but untracked until the record is repaired"
        )?;
    }
    Ok(())
}

/// Render the tracked buyer pools as an aligned table. Pure (writes to any
/// sink) so the layout is unit-testable without a store.
fn write_pools(
    w: &mut impl Write,
    pools: &[BuyerPoolState],
    skipped: &[PoolId],
) -> std::io::Result<()> {
    writeln!(w, "pools={}", pools.len())?;
    if pools.is_empty() {
        if skipped.is_empty() {
            writeln!(w, "(no tracked pools)")?;
        }
        return Ok(());
    }
    writeln!(
        w,
        "{:<14} {:<14} {:<14} {:>14} {:>8}",
        "POOL", "OWNER", "TOKEN", "DEPOSIT", "LANES"
    )?;
    for p in pools {
        writeln!(
            w,
            "{:<14} {:<14} {:<14} {:>14} {:>8}",
            short_hex(&format!("{:#x}", p.pool_id)),
            short_hex(&format!("{:#x}", p.owner)),
            short_hex(&format!("{:#x}", p.token)),
            format_usdc_u256(p.deposit),
            p.lane_count(),
        )?;
    }
    Ok(())
}

/// Render a daemon's buyer-pool snapshot as an aligned table, in the same
/// columns [`write_pools`] uses for the client store, so an operator reading
/// both sees the same shape. Pure (writes to any sink) so the layout is
/// unit-testable without an admin hop.
///
/// Addresses arrive EIP-55 checksummed from the wire, where the client path
/// renders them lowercase — the columns match, the casing does not.
///
/// Writes the table only. Undecodable rows go to stderr through
/// [`write_skipped_buyer_pools`], as they do on the client path, so a script
/// capturing stdout does not find warning lines ahead of `pools=`.
///
/// Shared by `decdn node pools` and by `decdn pool list` when it routes to a
/// running daemon.
pub(crate) fn write_buyer_pools(
    w: &mut impl Write,
    resp: &BuyerPoolsResponse,
) -> std::io::Result<()> {
    writeln!(w, "pools={}", resp.pools.len())?;
    if resp.pools.is_empty() {
        if resp.skipped.is_empty() {
            writeln!(w, "(no tracked pools)")?;
        }
        return Ok(());
    }
    writeln!(
        w,
        "{:<14} {:<14} {:<14} {:>14} {:>8}",
        "POOL", "OWNER", "TOKEN", "DEPOSIT", "LANES"
    )?;
    for p in &resp.pools {
        writeln!(
            w,
            "{:<14} {:<14} {:<14} {:>14} {:>8}",
            short_hex(&p.pool_id),
            short_hex(&p.owner),
            short_hex(&p.token),
            format_usdc_u256(U256::from(p.deposit_micro_usdc)),
            p.lanes.len(),
        )?;
    }
    Ok(())
}

/// Warn about every undecodable row in a daemon's answer, on stderr — the
/// stream [`write_skipped_pools`] uses for the client store's equivalent.
pub(crate) fn write_skipped_buyer_pools(
    w: &mut impl Write,
    skipped: &[String],
) -> std::io::Result<()> {
    for pool_id in skipped {
        writeln!(
            w,
            "warning: buyer pool {pool_id} could not be decoded; its deposit remains escrowed but \
             untracked until the record is repaired"
        )?;
    }
    Ok(())
}

/// Top-level `--json` document for a listing of the CLI's own store.
///
/// `store` and `source` are the half a machine consumer needs to know *which*
/// buyer store answered: the same command on the same host can read either
/// file, and they are unrelated (#2078).
#[derive(Serialize)]
struct PoolListJson {
    store: String,
    source: &'static str,
    pools: Vec<PoolJson>,
    skipped: Vec<String>,
}

/// Top-level `--json` document for a listing the daemon answered.
///
/// Borrows the admin DTOs rather than re-shaping them, so the wire types stay
/// the single definition of the daemon's answer. The consequence is that the
/// two sources do NOT emit the same pool objects: this one carries
/// `deposit_micro_usdc` as a number and EIP-55 addresses, where a client-store
/// listing carries `deposit_usdc` as a decimal string and lowercase addresses.
/// Read `source` before parsing `pools`.
#[derive(Serialize)]
struct DaemonPoolListJson<'a> {
    store: String,
    source: &'static str,
    pools: &'a [decdn_common::admin::BuyerPoolSnapshot],
    skipped: &'a [String],
}

/// Serializable view for `--json`, including the per-lane watermark so a
/// programmatic consumer sees the same fan-out the table's `LANES` count only
/// summarizes.
#[derive(Serialize)]
struct PoolJson {
    pool_id: String,
    owner: String,
    token: String,
    deposit_usdc: String,
    lanes: Vec<LaneJson>,
}

#[derive(Serialize)]
struct LaneJson {
    signer: String,
    provider: String,
    last_amount_usdc: String,
    last_bytes_delivered: String,
}

impl From<&BuyerPoolState> for PoolJson {
    fn from(p: &BuyerPoolState) -> Self {
        Self {
            pool_id: format!("{:#x}", p.pool_id),
            owner: format!("{:#x}", p.owner),
            token: format!("{:#x}", p.token),
            deposit_usdc: format_usdc_u256(p.deposit),
            lanes: p
                .lanes()
                .map(|(lane, progress)| LaneJson {
                    signer: format!("{:#x}", lane.signer),
                    provider: format!("{:#x}", lane.provider),
                    last_amount_usdc: format_usdc_u256(progress.last_amount),
                    last_bytes_delivered: progress.last_bytes.to_string(),
                })
                .collect(),
        }
    }
}

/// Format a `U256` micro-USDC amount as `"N.NNNNNN"`. USDC has 6 decimals
/// (ADR 003), so 1 USDC == `1_000_000` micro-USDC.
fn format_usdc_u256(micro: U256) -> String {
    let scale = U256::from(1_000_000u64);
    let whole = micro / scale;
    // `micro % scale < 1_000_000`, so the `u64` narrowing is always exact.
    let frac = (micro % scale).to::<u64>();
    format!("{whole}.{frac:06}")
}

/// Abbreviate a `0x`-prefixed hex string to `0x` + the first 10 nibbles + `…`
/// for table display; short inputs pass through unchanged. Uses `get(..)` to
/// stay within the workspace `indexing_slicing` clippy denial.
fn short_hex(hex: &str) -> String {
    match hex.get(..12) {
        Some(prefix) if hex.len() > 12 => format!("{prefix}…"),
        _ => hex.to_string(),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn resolve_expiry_absolute_and_relative_agree() {
        let now = 1_000_000u64;
        // Relative: now + secs.
        assert_eq!(resolve_expiry(now, Some(3_600), None).unwrap(), now + 3_600);
        // Absolute: used verbatim when in the future.
        assert_eq!(resolve_expiry(now, None, Some(now + 10)).unwrap(), now + 10);
    }

    #[test]
    fn resolve_expiry_rejects_missing_and_past() {
        let now = 1_000_000u64;
        // Neither flag: clap normally guards this, but the handler still refuses.
        let missing = resolve_expiry(now, None, None).unwrap_err();
        assert!(missing.to_string().contains("exactly one"), "{missing}");
        // An absolute expiry at/behind now is dead on arrival.
        let past = resolve_expiry(now, None, Some(now)).unwrap_err();
        assert!(past.to_string().contains("not in the future"), "{past}");
    }

    #[test]
    fn format_expiry_breaks_out_days_hours_minutes() {
        let now = 1_000_000u64;
        // 1 day + 2 hours + 3 minutes ahead.
        let expiry = now + 86_400 + 2 * 3_600 + 3 * 60;
        let rendered = format_expiry(expiry, now);
        assert!(rendered.contains(&format!("Unix {expiry}")), "{rendered}");
        assert!(rendered.contains("in 1d 2h 3m"), "{rendered}");
    }

    fn args() -> cli::PoolChainArgs {
        cli::PoolChainArgs {
            rpc_url: None,
            payment_pool_address: None,
            chain_id: None,
            keystore: None,
            keystore_password_file: None,
            data_dir: Some(PathBuf::from("/tmp/d")),
        }
    }

    fn config(body: &str) -> FileConfig {
        toml::from_str(body).unwrap()
    }

    #[test]
    fn flags_override_config() {
        let mut a = args();
        a.rpc_url = Some("http://flag:8545".into());
        a.chain_id = Some(99);
        let pp = "0x1111111111111111111111111111111111111111";
        a.payment_pool_address = Some(pp.into());
        let file = config(
            "[blockchain]\nrpc_url = \"http://config:8545\"\nchain_id = 1\n\
             payment_pool_address = \"0x3333333333333333333333333333333333333333\"\n",
        );
        let r = resolve_chain(&a, &file).unwrap();
        assert_eq!(r.rpc_url, "http://flag:8545");
        assert_eq!(r.chain_id, 99);
        assert_eq!(r.payment_pool, Address::from_str(pp).unwrap());
    }

    #[test]
    fn config_fills_unset_flags_and_chain_id_defaults() {
        let file = config(
            "[blockchain]\nrpc_url = \"http://config:8545\"\n\
             payment_pool_address = \"0x3333333333333333333333333333333333333333\"\n",
        );
        let r = resolve_chain(&args(), &file).unwrap();
        assert_eq!(r.rpc_url, "http://config:8545");
        assert_eq!(r.chain_id, DEFAULT_CHAIN_ID);
        assert_eq!(
            r.keystore,
            eth_identity::keystore_path(&PathBuf::from("/tmp/d"))
        );
    }

    /// The password file is CLI/env-only — absent unless the operator passes
    /// the flag, and carried through verbatim when they do. An absolute path
    /// keeps the assertion off the ambient `$HOME` that `expand_tilde` reads.
    #[test]
    fn keystore_password_file_flows_through_and_defaults_to_none() {
        let file = config(
            "[blockchain]\nrpc_url = \"http://config:8545\"\n\
             payment_pool_address = \"0x3333333333333333333333333333333333333333\"\n",
        );
        assert!(
            resolve_chain(&args(), &file)
                .unwrap()
                .keystore_password_file
                .is_none()
        );

        let mut a = args();
        a.keystore_password_file = Some(PathBuf::from("/abs/pw.txt"));
        assert_eq!(
            resolve_chain(&a, &file).unwrap().keystore_password_file,
            Some(PathBuf::from("/abs/pw.txt"))
        );
    }

    #[test]
    fn explicit_data_dir_not_client_scoped() {
        let file = config(
            "[blockchain]\nrpc_url = \"http://config:8545\"\n\
             payment_pool_address = \"0x3333333333333333333333333333333333333333\"\n",
        );
        let r = resolve_chain(&args(), &file).unwrap();
        assert_eq!(r.data_dir, PathBuf::from("/tmp/d"));
        assert_eq!(
            r.keystore,
            eth_identity::keystore_path(&PathBuf::from("/tmp/d"))
        );
    }

    #[test]
    fn missing_payment_pool_errors() {
        let file = config("[blockchain]\nrpc_url = \"http://config:8545\"\n");
        let err = resolve_chain(&args(), &file).unwrap_err();
        assert!(
            err.to_string().contains("payment_pool_address not set"),
            "{err}"
        );
    }

    /// A present-but-zero `payment_pool_address` fails fast via the shared
    /// `parse_nonzero_address` guard rather than as an opaque on-chain revert.
    #[test]
    fn rejects_zero_payment_pool() {
        let file = config(
            "[blockchain]\nrpc_url = \"http://config:8545\"\n\
             payment_pool_address = \"0x0000000000000000000000000000000000000000\"\n",
        );
        let err = resolve_chain(&args(), &file).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("payment_pool_address"), "{err}");
        assert!(msg.contains("must not be the zero address"), "{err}");
    }

    #[test]
    fn parse_pool_id_accepts_hex_and_rejects_garbage() {
        let id = "0x1111111111111111111111111111111111111111111111111111111111111111";
        assert_eq!(parse_pool_id(id).unwrap(), B256::from_str(id).unwrap());
        assert!(parse_pool_id("not-a-hash").is_err());
    }

    #[test]
    fn ensure_owned_accepts_matching_and_rejects_mismatch() {
        let ours = Address::repeat_byte(1);
        let pool_id = B256::repeat_byte(0xab);
        assert!(ensure_owned(ours, ours, pool_id).is_ok());
        let err = ensure_owned(Address::repeat_byte(2), ours, pool_id).unwrap_err();
        assert!(err.to_string().contains("not this keystore's address"));
    }

    fn mk_state(byte: u8, deposit_micro: u64) -> BuyerPoolState {
        BuyerPoolState::new(
            B256::repeat_byte(byte),
            Address::repeat_byte(byte),
            Address::repeat_byte(0xcd),
            U256::from(deposit_micro),
        )
    }

    /// A landed close on a node data dir does not claim the row was cleared —
    /// it reports that the daemon's row survives. The `Ok` here is the
    /// on-chain leg succeeding, which it did.
    #[test]
    fn daemon_owned_bookkeeping_does_not_claim_a_clean_close() {
        let books = LocalBookkeeping::DaemonOwned(Path::new("/var/lib/decdn/buyer.redb"));
        books
            .forget_after_close(
                Address::repeat_byte(0x11),
                B256::repeat_byte(0x22),
                TxHash::repeat_byte(0x33),
                "run `decdn pool reclaim` later",
            )
            .expect("the on-chain close landed; the local row is reported, not graded");
    }

    /// `--all` renders every lifecycle state, and `TRACKED` distinguishes
    /// "the store does not have this pool" from "the store could not be read"
    /// — the second is the case the flag exists for, and reporting it as the
    /// first would be a false claim about the pool.
    #[test]
    fn write_chain_pools_separates_untracked_from_unreadable() {
        const NOW: u64 = 1_000_000;
        let rows = vec![
            ChainPoolRow {
                pool_id: B256::repeat_byte(0x11),
                status: PaymentPool::Status::Open,
                deposit: U256::from(2_500_000u64),
                total_redeemed: U256::from(1_000_000u64),
                dispute_deadline: 0,
                tracked: Some(true),
            },
            ChainPoolRow {
                pool_id: B256::repeat_byte(0x22),
                status: PaymentPool::Status::Closing,
                deposit: U256::from(1_000_000u64),
                total_redeemed: U256::ZERO,
                dispute_deadline: NOW + 3_600,
                tracked: Some(false),
            },
            ChainPoolRow {
                pool_id: B256::repeat_byte(0x33),
                status: PaymentPool::Status::Closed,
                deposit: U256::ZERO,
                total_redeemed: U256::from(1_000_000u64),
                dispute_deadline: NOW - 1,
                tracked: None,
            },
        ];

        let mut buf = Vec::new();
        write_chain_pools(&mut buf, Address::repeat_byte(0xab), 42, &rows, NOW, true).unwrap();
        let out = String::from_utf8(buf).unwrap();

        assert!(out.contains("chain_id=42"), "{out}");
        assert!(out.contains("pools=3"), "{out}");
        assert!(out.contains("open"), "{out}");
        assert!(out.contains("closing"), "{out}");
        assert!(out.contains("closed"), "{out}");
        assert!(out.contains("2.500000"), "{out}");
        assert!(out.contains("close it first"), "{out}");
        assert!(out.contains("already reclaimed"), "{out}");

        let row_for = |byte: u8| {
            let tag = short_hex(&format!("{:#x}", B256::repeat_byte(byte)));
            out.lines()
                .find(|l| l.starts_with(&tag))
                .map(str::to_owned)
                .expect("every row renders")
        };
        let untracked = row_for(0x22);
        assert!(untracked.contains(" no "), "{untracked}");
        let unreadable = row_for(0x33);
        assert!(unreadable.contains(" ? "), "{unreadable}");
    }

    /// A pool whose local row will not decode is NOT untracked: the store holds
    /// a row for it, keyed by a `pool_id` that survives whatever corrupted the
    /// value bytes. Reporting `no` would send the operator to `pool close`
    /// (recover a stranded deposit) when the remedy is repairing a record. One
    /// bad row must also not blank the verdict for the pools either side of it.
    #[test]
    fn an_undecodable_row_is_unknown_not_untracked() {
        let decoded = B256::repeat_byte(0x11);
        let corrupt = B256::repeat_byte(0x22);
        let absent = B256::repeat_byte(0x33);
        let local = TrackedPools(
            [
                (decoded, RowState::Decoded),
                (corrupt, RowState::Undecodable),
            ]
            .into_iter()
            .collect(),
        );

        assert_eq!(local.verdict(decoded), Some(true));
        assert_eq!(local.verdict(absent), Some(false), "no row means untracked");
        assert_eq!(
            local.verdict(corrupt),
            None,
            "a row that will not decode is unknown, not untracked"
        );
        assert_eq!(tracked_label(local.verdict(corrupt)), "?");
    }

    /// A `pool_id` spelling this binary cannot parse must make the whole answer
    /// unknown. Dropping it silently renders the pool `no`, which tells an
    /// operator to close a pool the daemon may be paying from right now.
    ///
    /// The other half of the drift guard is in `decdn-node`, where the wire
    /// spelling is produced: `build_buyer_pools_response_ids_parse_back`.
    #[test]
    fn a_wire_response_round_trips_and_an_unparseable_id_is_not_a_no() {
        let decoded = B256::repeat_byte(0x11);
        let corrupt = B256::repeat_byte(0x22);
        // The `{:#x}` spelling the daemon emits; `decdn-node`'s
        // `build_buyer_pools_response_ids_parse_back` pins that it still does.
        let resp = BuyerPoolsResponse {
            pools: vec![decdn_common::admin::BuyerPoolSnapshot {
                pool_id: format!("{decoded:#x}"),
                owner: format!("{:?}", Address::repeat_byte(0x11)),
                token: format!("{:?}", Address::repeat_byte(0xcd)),
                deposit_micro_usdc: 1,
                lanes: Vec::new(),
            }],
            skipped: vec![format!("{corrupt:#x}")],
        };
        let local = TrackedPools::from_wire(&resp).expect("the daemon's own spelling must parse");
        assert_eq!(local.verdict(decoded), Some(true));
        assert_eq!(local.verdict(corrupt), None);
        assert_eq!(local.verdict(B256::repeat_byte(0x33)), Some(false));

        let drifted = BuyerPoolsResponse {
            pools: Vec::new(),
            skipped: vec!["not-a-pool-id".to_owned()],
        };
        assert!(
            TrackedPools::from_wire(&drifted).is_none(),
            "a spelling this binary cannot parse makes the answer unknown, never `no`"
        );
    }

    /// `BuyerLoad`'s two halves land in the two sets — the conversion is where
    /// `skipped` would otherwise be dropped, which is what made an undecodable
    /// row read as `no`.
    #[test]
    fn a_buyer_load_keeps_its_skipped_rows() {
        let state = BuyerPoolState::new(
            B256::repeat_byte(0x11),
            Address::repeat_byte(0x11),
            Address::repeat_byte(0xcd),
            U256::from(1u64),
        );
        let local = TrackedPools::from(BuyerLoad {
            pools: vec![state],
            skipped: vec![B256::repeat_byte(0x22)],
        });
        assert_eq!(local.verdict(B256::repeat_byte(0x11)), Some(true));
        assert_eq!(local.verdict(B256::repeat_byte(0x22)), None);
    }

    /// An owner with nothing on chain gets a sentinel, not a bare header — the
    /// same shape the store listings use, so one reader parses all of them.
    #[test]
    fn write_chain_pools_empty_emits_sentinel() {
        let mut buf = Vec::new();
        write_chain_pools(&mut buf, Address::repeat_byte(0xab), 1, &[], 0, true).unwrap();
        let out = String::from_utf8(buf).unwrap();
        assert!(out.contains("pools=0"), "{out}");
        assert!(out.contains("owns no pools"), "{out}");
        assert!(!out.contains("POOL "), "no header without rows: {out}");
    }

    /// A `Closing` pool past its window reads as reclaimable in both renderings
    /// from one `plan_reclaim` call — the table and the JSON must not be able
    /// to disagree about it.
    #[test]
    fn reclaimable_now_agrees_between_table_and_json() {
        const NOW: u64 = 500;
        let row = ChainPoolRow {
            pool_id: B256::repeat_byte(0x44),
            status: PaymentPool::Status::Closing,
            deposit: U256::from(1u64),
            total_redeemed: U256::ZERO,
            dispute_deadline: NOW - 1,
            tracked: Some(true),
        };
        assert_eq!(reclaimable_label(&row, NOW), "now");
        assert!(ChainPoolJson::at(&row, NOW).reclaimable_now);

        let inside = ChainPoolRow {
            dispute_deadline: NOW + 60,
            ..row
        };
        assert!(reclaimable_label(&inside, NOW).starts_with("Unix "));
        assert!(!ChainPoolJson::at(&inside, NOW).reclaimable_now);
    }

    /// The daemon renderer shares [`write_pools`]'s columns and its empty
    /// sentinel, so the two listings read as one view.
    #[test]
    fn write_buyer_pools_matches_the_client_table_shape() {
        let mut buf = Vec::new();
        write_buyer_pools(
            &mut buf,
            &BuyerPoolsResponse {
                pools: Vec::new(),
                skipped: Vec::new(),
            },
        )
        .unwrap();
        let out = String::from_utf8(buf).unwrap();
        assert!(out.contains("pools=0"), "{out}");
        assert!(out.contains("(no tracked pools)"), "{out}");

        let mut buf = Vec::new();
        write_buyer_pools(
            &mut buf,
            &BuyerPoolsResponse {
                pools: vec![decdn_common::admin::BuyerPoolSnapshot {
                    pool_id: "0xabcdef0123456789".to_string(),
                    owner: "0x1111111111111111".to_string(),
                    token: "0x2222222222222222".to_string(),
                    deposit_micro_usdc: 1_500_000,
                    lanes: Vec::new(),
                }],
                skipped: vec!["0x4444".to_string()],
            },
        )
        .unwrap();
        let out = String::from_utf8(buf).unwrap();
        assert!(out.contains("1.500000"), "{out}");
        assert!(out.contains("pools=1"), "{out}");
        assert!(
            !out.contains("0x4444"),
            "undecodable rows belong on stderr, not in the table sink: {out}"
        );

        // …and they are still reported, on the stream the client path uses.
        let mut warnings = Vec::new();
        write_skipped_buyer_pools(&mut warnings, &["0x4444".to_string()]).unwrap();
        let warnings = String::from_utf8(warnings).unwrap();
        assert!(warnings.contains("0x4444"), "{warnings}");
        assert!(warnings.contains("escrowed"), "{warnings}");
    }

    #[test]
    fn write_pools_empty_emits_sentinel() {
        let mut buf = Vec::new();
        write_pools(&mut buf, &[], &[]).unwrap();
        let out = String::from_utf8(buf).unwrap();
        assert!(out.contains("pools=0"), "{out}");
        assert!(out.contains("(no tracked pools)"), "{out}");
    }

    #[test]
    fn write_pools_renders_deposit_and_lane_count() {
        let state = mk_state(1, 1_500_000);
        let mut buf = Vec::new();
        write_pools(&mut buf, std::slice::from_ref(&state), &[]).unwrap();
        let out = String::from_utf8(buf).unwrap();
        assert!(out.contains("1.500000"), "{out}");
        assert!(
            out.contains(" 0\n") || out.trim_end().ends_with(" 0"),
            "{out}"
        );
    }

    #[test]
    fn format_usdc_u256_pads_fractional_digits() {
        assert_eq!(format_usdc_u256(U256::from(1_000_000u64)), "1.000000");
        assert_eq!(format_usdc_u256(U256::from(500_000u64)), "0.500000");
    }

    #[test]
    fn short_hex_truncates_long_hashes() {
        let long = "0x1111111111111111111111111111111111111111";
        assert!(short_hex(long).ends_with('…'));
        assert_eq!(short_hex("0x01"), "0x01");
    }

    // ---- landed-on-chain, not-recorded-locally ----------------------------

    fn a_pool() -> PoolId {
        PoolId::from([0x11; 32])
    }

    fn addr(byte: u8) -> Address {
        Address::from([byte; 20])
    }

    fn a_tx() -> TxHash {
        TxHash::from([0xab; 32])
    }

    const RECLAIM_NOTE: &str = "run `decdn pool reclaim --pool 0x11` after Unix 99";

    /// A deleted row is the clean close: nothing maps this owner to the pool
    /// that is now winding down.
    #[test]
    fn a_cleared_row_is_a_clean_close() {
        assert!(grade_local_forget(Ok(true), a_pool(), a_tx(), RECLAIM_NOTE).is_ok());
    }

    /// `forget_if_pool` is compare-and-delete, so `Ok(false)` means it found
    /// nothing to delete — no row, or a row for a newer pool. Neither leaves
    /// this owner pointing at the closed pool, so the close is clean. Closing a
    /// pool this store never tracked (a second machine, a fresh `--data-dir`)
    /// lands here, and failing it would both misreport a correct close and
    /// invite the operator to delete a live replacement row.
    #[test]
    fn a_compare_and_delete_that_matched_nothing_is_still_a_clean_close() {
        assert!(grade_local_forget(Ok(false), a_pool(), a_tx(), RECLAIM_NOTE).is_ok());
    }

    /// Only the backend fault leaves the row's fate unknown, and the close has
    /// already landed — so the error has to carry both the tx and the reclaim,
    /// which is the operation that clears the row it warns about.
    #[test]
    fn a_faulted_clear_fails_and_keeps_the_reclaim_instruction() {
        let err = grade_local_forget(
            Err(decdn_incentive::StoreError::Backend("no space".into())),
            a_pool(),
            a_tx(),
            RECLAIM_NOTE,
        )
        .expect_err("a row of unknown fate must not read as a clean close");
        let msg = format!("{err:#}");
        assert!(msg.contains("closed on-chain"), "{msg}");
        assert!(msg.contains("no space"), "the cause must survive: {msg}");
        assert!(
            msg.contains(&format!("{}", a_tx())),
            "the tx is the handle an operator reconciles against: {msg}"
        );
        assert!(
            msg.contains("decdn pool reclaim"),
            "the close landed, so the reclaim deadline must survive the failure: {msg}"
        );
    }

    // ---- `--all` classification + summary --------------------------------

    #[test]
    fn close_plan_acts_only_on_open() {
        assert_eq!(plan_close(PaymentPool::Status::Open), ClosePlan::Close);
        assert!(matches!(
            plan_close(PaymentPool::Status::Closing),
            ClosePlan::Skip(_)
        ));
        assert!(matches!(
            plan_close(PaymentPool::Status::Closed),
            ClosePlan::Skip(_)
        ));
    }

    #[test]
    fn reclaim_plan_respects_status_and_deadline() {
        // Open must be closed first.
        assert_eq!(
            plan_reclaim(PaymentPool::Status::Open, 100, 200),
            ReclaimPlan::SkipOpen
        );
        // Closing, still inside the window: not yet, and it carries the deadline.
        assert_eq!(
            plan_reclaim(PaymentPool::Status::Closing, 200, 100),
            ReclaimPlan::SkipInWindow(200)
        );
        // Closing, window elapsed (now == deadline is elapsed): reclaim.
        assert_eq!(
            plan_reclaim(PaymentPool::Status::Closing, 200, 200),
            ReclaimPlan::Reclaim
        );
        assert_eq!(
            plan_reclaim(PaymentPool::Status::Closing, 200, 300),
            ReclaimPlan::Reclaim
        );
        // Already reclaimed.
        assert_eq!(
            plan_reclaim(PaymentPool::Status::Closed, 0, 300),
            ReclaimPlan::SkipClosed
        );
    }

    #[test]
    fn batch_result_is_ok_unless_something_failed() {
        // Nothing eligible is success (exit 0) — the summary line said so.
        assert!(batch_result(&BatchTally::default(), "close").is_ok());
        // Acted + skipped, none failed: still success.
        let clean = BatchTally {
            acted: 3,
            skipped: 2,
            failed: 0,
        };
        assert!(batch_result(&clean, "reclaim").is_ok());
        // Any failure is a nonzero exit, and the verb reaches the message.
        let broke = BatchTally {
            acted: 1,
            skipped: 0,
            failed: 2,
        };
        let err = batch_result(&broke, "close").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains('2'), "the failure count must show: {msg}");
        assert!(msg.contains("close"), "the verb must show: {msg}");
    }

    #[test]
    fn the_owner_check_passes_when_the_chain_agrees() {
        assert_eq!(
            grade_on_chain_owner(addr(0xaa), addr(0xaa)),
            OwnerVerdict::Owned
        );
    }

    /// A successful read that disagrees proves the token is dead at redemption.
    /// Emitting it anyway is how `decdn pool assign … > delegate.token` writes a
    /// file that looks valid and is not.
    #[test]
    fn a_disagreeing_owner_read_refuses_to_issue() {
        let verdict = grade_on_chain_owner(addr(0xbb), addr(0xaa));
        assert_eq!(verdict, OwnerVerdict::OwnedByOther(addr(0xbb)));

        let err = verdict
            .into_result(a_pool(), addr(0xaa))
            .expect_err("a proven-dead capability must not be issued");
        let msg = format!("{err:#}");
        assert!(msg.contains("rejected at redemption"), "{msg}");
        assert!(
            msg.contains("no token was issued"),
            "the operator must know nothing usable reached stdout: {msg}"
        );
        // The two addresses are the same type and the verdict is symmetric, so
        // only the message distinguishes them. Swapping them would send the
        // operator to check the wrong key.
        let (on_chain, keystore) = (format!("{}", addr(0xbb)), format!("{}", addr(0xaa)));
        let on_chain_at = msg.find(&on_chain).expect("names the on-chain owner");
        let keystore_at = msg.find(&keystore).expect("names the keystore address");
        assert!(
            on_chain_at < keystore_at,
            "the on-chain owner must be named as the owner, not as the keystore: {msg}"
        );
    }

    /// `getPool` zero-fills an unknown key rather than reverting, so a wrong
    /// `--pool`, `--payment-pool-address` or `--chain-id` arrives as a
    /// *successful* read of a zero owner. That is not an ownership dispute, and
    /// telling the operator to "sign with the owner keystore" sends them after
    /// the wrong fault.
    #[test]
    fn an_unopened_pool_is_diagnosed_as_missing_not_as_a_wrong_keystore() {
        let verdict = grade_on_chain_owner(Address::ZERO, addr(0xaa));
        assert_eq!(verdict, OwnerVerdict::NoSuchPool);

        let err = verdict
            .into_result(a_pool(), addr(0xaa))
            .expect_err("a capability for a pool that does not exist must not be issued");
        let msg = format!("{err:#}");
        assert!(msg.contains("does not exist"), "{msg}");
        assert!(
            msg.contains("--payment-pool-address"),
            "the likely fault is a misconfigured contract or chain, so name them: {msg}"
        );
        assert!(
            !msg.contains("sign with the owner keystore"),
            "a missing pool is not an ownership dispute: {msg}"
        );
    }

    /// The other half of the split: a read that could not be performed proves
    /// nothing, so offline issuance stays a warning and exits 0. Pinned against
    /// a port nothing listens on, so it needs no chain and no provider mock.
    #[tokio::test]
    async fn an_unreachable_rpc_warns_and_still_issues() {
        let signer = PrivateKeySigner::random();
        let rpc = provider::build_provider("http://127.0.0.1:1", &signer)
            .expect("a well-formed URL builds a provider without dialing");
        let contract = PaymentPool::new(Address::ZERO, rpc);
        let owner = signer.address();

        ensure_on_chain_owner(&contract, a_pool(), owner)
            .await
            .expect("offline issuance is valid by design — an unreachable RPC must not fail it");
    }
}
