//! The pool keeper: every tick, read the pool's remaining balance and, if it
//! is below the low-water mark, top it up from the treasury by the refill
//! amount. Its progress is published in [`KeeperStatus`] for metrics.
//!
//! A `topUp` that may have been broadcast without the keeper seeing it mine
//! can still mine, and a second one would escrow the refill twice. The keeper
//! holds further top-ups until that transaction has a receipt, or the
//! treasury's confirmed nonce has passed the nonce it was sent with (decdn/sponsord#40). A
//! `topUp` whose submit returned no tx hash (the send request failed in
//! transport, or the node answered `already known`, `nonce too low` or
//! `replacement transaction underpriced`) can be settled only by its nonce,
//! and so can one whose hash the node does not know. While the node shows
//! no transaction of the top-up, the treasury's confirmed nonce equals its
//! nonce and the pending nonce does not exceed it, no transaction holds the
//! nonce, so the keeper re-sends the `topUp` at it and settles the hold by
//! that receipt; two transactions at one nonce cannot both mine (#2572). A
//! re-send that reverts `PoolNotOpen` ends the hold, since the pool accepts
//! no funds. While the node's mempool holds a transaction at the nonce, or
//! an earlier nonce is unfilled, the hold lasts until the nonce is used. The
//! hold is kept in memory, so a restart clears it.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use alloy::primitives::{B256, TxHash};
use decdn_client::buyer_pool::{PoolNotOpen, TopUpUnconfirmed};
use decdn_common::redact::sanitize_err_chain;
use sponsord_api::MicroUsdc;
use sponsord_api::time::{Clock, SystemClock};
use tokio_util::sync::CancellationToken;

use crate::pool::{PoolChain, TxState};

/// When to top the pool up, and by how much.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KeeperConfig {
    /// Top up once the remaining balance drops below this.
    pub low_water: MicroUsdc,
    /// Amount each top-up adds.
    pub refill: MicroUsdc,
    /// How often the balance is checked.
    pub interval: Duration,
}

/// What the keeper has seen and done, updated as it runs. Every value starts
/// at 0; a timestamp of 0 means "never".
#[derive(Debug, Default)]
pub struct KeeperStatus {
    remaining: AtomicU64,
    last_check_unix: AtomicU64,
    last_topup_unix: AtomicU64,
    topups: AtomicU64,
    failures: AtomicU64,
    topup_unconfirmed_since_unix: AtomicU64,
}

/// A point-in-time copy of [`KeeperStatus`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct KeeperSnapshot {
    /// The pool's remaining balance at the last successful read.
    pub remaining: MicroUsdc,
    /// Unix time of the last successful balance read.
    pub last_check_unix: u64,
    /// Unix time of the last successful top-up. For a top-up that came back
    /// unconfirmed, the time the keeper found its receipt.
    pub last_topup_unix: u64,
    /// Successful top-ups so far.
    pub topups: u64,
    /// Failed chain reads and top-ups so far.
    pub failures: u64,
    /// Unix time a top-up came back unconfirmed, while further top-ups are
    /// held for it; 0 when nothing is held.
    pub topup_unconfirmed_since_unix: u64,
}

impl KeeperStatus {
    /// Copy the current values. Each is read on its own, so a snapshot taken
    /// mid-tick may mix values from before and after it.
    #[must_use]
    pub fn snapshot(&self) -> KeeperSnapshot {
        KeeperSnapshot {
            remaining: MicroUsdc(self.remaining.load(Ordering::Relaxed)),
            last_check_unix: self.last_check_unix.load(Ordering::Relaxed),
            last_topup_unix: self.last_topup_unix.load(Ordering::Relaxed),
            topups: self.topups.load(Ordering::Relaxed),
            failures: self.failures.load(Ordering::Relaxed),
            topup_unconfirmed_since_unix: self.topup_unconfirmed_since_unix.load(Ordering::Relaxed),
        }
    }
}

/// Whether `remaining` has dropped below `low_water`.
#[must_use]
pub fn needs_refill(remaining: MicroUsdc, low_water: MicroUsdc) -> bool {
    remaining < low_water
}

/// Run the keeper until `shutdown` is cancelled, checking first right away.
/// A failed tick is logged, counted, and retried next interval; the loop
/// never exits on its own. An unconfirmed top-up is the exception: no top-up
/// is sent until it is settled (see the module docs).
pub async fn run(
    pool: Arc<dyn PoolChain>,
    pool_id: B256,
    cfg: KeeperConfig,
    status: Arc<KeeperStatus>,
    shutdown: CancellationToken,
) {
    let mut tick = tokio::time::interval(cfg.interval);
    let mut unconfirmed = None;
    loop {
        // Biased: when a tick and the cancellation are both ready, stop rather
        // than start another sweep.
        tokio::select! {
            biased;
            () = shutdown.cancelled() => return,
            _ = tick.tick() => {}
        }
        // A top-up already sent is allowed to finish: cancelling between
        // the approve and the top-up would leave a dangling allowance.
        sweep(
            pool.as_ref(),
            pool_id,
            &cfg,
            &status,
            &SystemClock,
            &mut unconfirmed,
        )
        .await;
    }
}

/// A top-up whose `topUp` may have been broadcast but whose receipt was not
/// read. It may still mine, so no other top-up is sent until it is settled
/// (decdn/sponsord#40).
#[derive(Clone, Copy, Debug)]
struct Unconfirmed {
    /// The `topUp` transaction; `None` when its submit returned no tx hash
    /// (the send request failed in transport, or the node gave an answer
    /// `decdn_incentive::tx::send_broadcast_unknown` reads as uncertain), so
    /// it may be in a mempool without its hash known here. A re-send at the
    /// nonce records its hash here, after which the receipt settles the hold.
    tx: Option<TxHash>,
    /// The nonce the `topUp` was sent with. Once the treasury's confirmed
    /// nonce passes it, the `topUp` has mined or can never mine.
    nonce: u64,
    since_unix: u64,
}

/// Set or clear the hold, and publish it for metrics.
fn set_hold(slot: &mut Option<Unconfirmed>, status: &KeeperStatus, hold: Option<Unconfirmed>) {
    *slot = hold;
    status
        .topup_unconfirmed_since_unix
        .store(hold.map_or(0, |h| h.since_unix), Ordering::Relaxed);
}

#[expect(
    clippy::cognitive_complexity,
    reason = "one branch per pool state, each with its own log event; the tracing macros' expansion is most of the score"
)]
async fn sweep(
    pool: &dyn PoolChain,
    pool_id: B256,
    cfg: &KeeperConfig,
    status: &KeeperStatus,
    clock: &dyn Clock,
    unconfirmed: &mut Option<Unconfirmed>,
) {
    // A sweep that finds a hold sends no top-up, even when it settles it: the
    // balance it reads may come from a node that has not yet seen the held
    // top-up mine. The next sweep decides on a fresh balance. Settling before
    // the balance read makes the `remaining` recorded here include a held
    // top-up that has mined.
    let mut may_top_up = true;
    if let Some(held) = unconfirmed.as_mut() {
        may_top_up = false;
        if !settle(pool, pool_id, cfg.refill, held, status, clock).await {
            set_hold(unconfirmed, status, None);
        }
    }
    let remaining = match pool.remaining(pool_id).await {
        Ok(r) => r,
        Err(e) => {
            status.failures.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                pool = %pool_id,
                "pool remaining read failed: {}",
                sanitize_err_chain(&e)
            );
            return;
        }
    };
    status.remaining.store(remaining.0, Ordering::Relaxed);
    status
        .last_check_unix
        .store(clock.now_unix(), Ordering::Relaxed);
    if !may_top_up || !needs_refill(remaining, cfg.low_water) {
        return;
    }
    match pool.top_up(pool_id, cfg.refill).await {
        Ok(credited) => {
            status.topups.fetch_add(1, Ordering::Relaxed);
            status
                .last_topup_unix
                .store(clock.now_unix(), Ordering::Relaxed);
            status
                .remaining
                .store(remaining.saturating_add(credited).0, Ordering::Relaxed);
            tracing::info!(
                pool = %pool_id, remaining = remaining.0, credited = credited.0,
                "pool topped up"
            );
        }
        Err(e) => {
            status.failures.fetch_add(1, Ordering::Relaxed);
            if let Some(m) = e.downcast_ref::<TopUpUnconfirmed>() {
                let hold = Unconfirmed {
                    tx: m.tx,
                    nonce: m.nonce,
                    since_unix: clock.now_unix(),
                };
                set_hold(unconfirmed, status, Some(hold));
                tracing::error!(
                    pool = %pool_id, tx = ?m.tx, nonce = m.nonce,
                    "pool top-up unconfirmed; holding further top-ups until it has a \
                     receipt or its nonce is used: {}",
                    sanitize_err_chain(&e)
                );
            } else {
                tracing::warn!(
                    pool = %pool_id,
                    "pool top-up failed: {}",
                    sanitize_err_chain(&e)
                );
            }
        }
    }
}

/// What the RPC node shows of a held top-up.
enum Read {
    /// The top-up's hash is known: where the node says it stands.
    Hash(TxHash, TxState),
    /// No hash: the node can show nothing of it.
    NoHash,
}

/// Check the held top-up; returns whether the hold stays. A re-send at a
/// vacant nonce records its hash on `held`.
#[expect(
    clippy::cognitive_complexity,
    reason = "one branch per pool state, each with its own log event; the tracing macros' expansion is most of the score"
)]
async fn settle(
    pool: &dyn PoolChain,
    pool_id: B256,
    refill: MicroUsdc,
    held: &mut Unconfirmed,
    status: &KeeperStatus,
    clock: &dyn Clock,
) -> bool {
    let nonce = held.nonce;
    // The confirmed nonce is read first: a transaction that mines between
    // the reads then shows as mined, or as a used nonce, never as replaced
    // or as vacant.
    let read = async {
        let confirmed = pool.confirmed_nonce().await?;
        let state = match held.tx {
            Some(tx) => Read::Hash(tx, pool.transaction(tx).await?),
            None => Read::NoHash,
        };
        anyhow::Ok((confirmed, state))
    };
    let (confirmed, state) = match read.await {
        Ok(r) => r,
        Err(e) => {
            status.failures.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                pool = %pool_id, tx = ?held.tx, nonce,
                "unconfirmed pool top-up check failed; still holding top-ups: {}",
                sanitize_err_chain(&e)
            );
            return true;
        }
    };
    let held_secs = clock.now_unix().saturating_sub(held.since_unix);
    match state {
        Read::Hash(tx, TxState::Mined) => {
            status.topups.fetch_add(1, Ordering::Relaxed);
            status
                .last_topup_unix
                .store(clock.now_unix(), Ordering::Relaxed);
            tracing::info!(pool = %pool_id, %tx, "unconfirmed pool top-up mined");
            false
        }
        Read::Hash(tx, TxState::Reverted) => {
            tracing::warn!(
                pool = %pool_id, %tx,
                "unconfirmed pool top-up reverted, so nothing was escrowed; resuming top-ups"
            );
            false
        }
        // A used nonce settles it whatever the node says of the hash: a
        // stale or load-balanced node can still report a replaced `topUp` as
        // pending, or not yet have the receipt of one that mined. It is not
        // counted as a top-up; the next sweep reads the balance either way.
        Read::Hash(tx, TxState::Pending { .. } | TxState::Unknown) if confirmed > nonce => {
            tracing::warn!(
                pool = %pool_id, %tx, nonce, confirmed,
                "the RPC node has no receipt for pool top-up {tx}, but its nonce {nonce} \
                 is used, so it has mined or never will; resuming top-ups"
            );
            false
        }
        Read::Hash(tx, TxState::Pending { .. }) => {
            tracing::warn!(
                pool = %pool_id, %tx, nonce, held_secs,
                "pool top-up still pending; holding further top-ups"
            );
            true
        }
        // With no hash, the `topUp` may be the transaction that used its
        // nonce, so it may have refilled the pool. It is not counted as a
        // top-up; the next sweep reads the balance either way.
        Read::NoHash if confirmed > nonce => {
            tracing::warn!(
                pool = %pool_id, nonce, confirmed,
                "nonce {nonce} of the pool top-up whose submit returned no tx hash is \
                 used, so it has mined or never will; resuming top-ups"
            );
            false
        }
        // The node shows no transaction of the top-up (no hash, or a hash it
        // does not know), and its nonce is unused.
        Read::Hash(_, TxState::Unknown) | Read::NoHash => {
            settle_unseen(pool, pool_id, refill, held, status, confirmed, held_secs).await
        }
    }
}

/// Settle a held top-up the RPC node shows no transaction of, by where its
/// nonce stands against the treasury's; returns whether the hold stays.
#[expect(
    clippy::cognitive_complexity,
    reason = "one branch per nonce state, each with its own log event; the tracing macros' expansion is most of the score"
)]
async fn settle_unseen(
    pool: &dyn PoolChain,
    pool_id: B256,
    refill: MicroUsdc,
    held: &mut Unconfirmed,
    status: &KeeperStatus,
    confirmed: u64,
    held_secs: u64,
) -> bool {
    let nonce = held.nonce;
    // How many 0-value self-transfers clear the hold when the top-up was
    // dropped: one per unfilled nonce up to and including the top-up's.
    let self_transfers = nonce.saturating_add(1).saturating_sub(confirmed);
    // An earlier nonce is unfilled, so nothing at the top-up's can mine yet,
    // and the mempool says nothing about it.
    if confirmed < nonce {
        tracing::error!(
            pool = %pool_id, tx = ?held.tx, nonce, confirmed, held_secs,
            "the unconfirmed pool top-up at nonce {nonce} cannot mine yet: the treasury's \
             confirmed nonce is {confirmed}, so an earlier nonce is unfilled; holding \
             further top-ups. If the transaction at that nonce was dropped, send 0-value \
             transactions from the treasury to itself; once nonce {nonce} is next the \
             keeper re-sends the top-up if nothing holds it, and {self_transfers} at most \
             clear the hold"
        );
        return true;
    }
    // The pending nonce is read after the confirmed one: a top-up that mines
    // between the reads then shows as held, never as vacant.
    let pending = match pool.pending_nonce().await {
        Ok(p) => p,
        Err(e) => {
            status.failures.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                pool = %pool_id, tx = ?held.tx, nonce,
                "unconfirmed pool top-up check failed; still holding top-ups: {}",
                sanitize_err_chain(&e)
            );
            return true;
        }
    };
    // Vacant: the top-up's nonce is next, and the node's mempool holds
    // nothing at it. Two transactions at one nonce cannot both mine, so a
    // re-send there cannot escrow twice; the receipt of whichever mines
    // settles the hold.
    if pending <= nonce {
        return resend_at_nonce(pool, pool_id, refill, held, status).await;
    }
    tracing::error!(
        pool = %pool_id, tx = ?held.tx, nonce, confirmed, pending, held_secs,
        "the RPC node shows no transaction of the unconfirmed pool top-up at nonce \
         {nonce}, but its mempool holds a transaction at that nonce, which may be the \
         top-up; holding further top-ups. The keeper re-sends the top-up once nothing \
         holds the nonce. If that transaction was dropped, send 0-value transactions \
         from the treasury to itself until the confirmed nonce passes {nonce} \
         ({self_transfers} at most); the hold then clears"
    );
    true
}

/// Re-send the held top-up at its vacant nonce and record the hash on the
/// hold; returns whether the hold stays.
async fn resend_at_nonce(
    pool: &dyn PoolChain,
    pool_id: B256,
    refill: MicroUsdc,
    held: &mut Unconfirmed,
    status: &KeeperStatus,
) -> bool {
    let nonce = held.nonce;
    match pool.send_top_up_at(pool_id, refill, nonce).await {
        Ok(tx) => {
            tracing::info!(
                pool = %pool_id, %tx, nonce, previous = ?held.tx,
                "no transaction holds nonce {nonce} of the unconfirmed pool top-up; re-sent \
                 topUp at that nonce as {tx}; holding further top-ups until it has a receipt"
            );
            held.tx = Some(tx);
            true
        }
        Err(e) if e.downcast_ref::<PoolNotOpen>().is_some() => {
            status.failures.fetch_add(1, Ordering::Relaxed);
            tracing::error!(
                pool = %pool_id, nonce,
                "re-sending the pool top-up at nonce {nonce} reverted PoolNotOpen: the \
                 pool is closing or closed, so neither it nor the first top-up can \
                 escrow; releasing the hold: {}",
                sanitize_err_chain(&e)
            );
            false
        }
        Err(e) => {
            status.failures.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                pool = %pool_id, nonce,
                "no transaction holds nonce {nonce} of the unconfirmed pool top-up, and \
                 re-sending it failed; still holding top-ups: {}",
                sanitize_err_chain(&e)
            );
            true
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests;
