//! Per-lane voucher accounting. [`next_voucher`] is the pure cumulative math;
//! [`PoolLedger`] serializes voucher *issuance* across concurrent streams that
//! draw on one pool lane. Continued delivery is acceptance (ADR 003/005): a sent
//! voucher commits optimistically — the send itself is the commit — and only an
//! explicit rejection rewinds it. There is no ack to wait for, so the issuance
//! lock spans compute → sign → send and nothing more, and parallel streams to
//! one provider never serialize their payments behind each other's round trips.

use std::future::Future;

use alloy::primitives::{B256, U256};
use decdn_incentive::LaneKey;
use decdn_incentive::chain::{CHUNK_BYTES, MAX_CHAIN_LENGTH};
use decdn_protocol::MB_BYTES;
use decdn_protocol::client::WatermarkBundle;
use tokio::sync::Mutex;

/// A lane's cumulative voucher state: the absolute totals carried by the most
/// recent voucher. Both advance monotonically over the lane's lifetime. There
/// is no nonce — `amount` is the sole monotone ordering and replay key (ADR 005
/// §Voucher wire format).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Cumulative {
    /// Cumulative lane bytes paid for.
    pub bytes: U256,
    /// Cumulative lane amount paid (token base units).
    pub amount: U256,
}

impl Cumulative {
    /// Componentwise sum — the anchor plus what the chain has accrued on top of
    /// it, which together are what the lane is owed.
    #[must_use]
    const fn plus(self, other: Self) -> Self {
        Self {
            bytes: self.bytes.saturating_add(other.bytes),
            amount: self.amount.saturating_add(other.amount),
        }
    }
}

impl From<&WatermarkBundle> for Cumulative {
    /// Decode a wallet-less resume bundle's `u64` totals (issue #1481) into
    /// the same shape [`PoolLedger`] tracks. Infallible — `U256::from`
    /// cannot fail on a `u64` — so a caller can re-seed directly from a
    /// bundle without a `Result`.
    fn from(bundle: &WatermarkBundle) -> Self {
        Self {
            bytes: U256::from(bundle.bytes_delivered),
            amount: U256::from(bundle.amount),
        }
    }
}

/// Compute the next voucher's absolute totals from the live cumulative and the
/// `delta_bytes` newly delivered (on any stream) since the last voucher. The
/// amount delta is `ceil(delta_bytes * rate_per_mb / 1 MiB)` so each voucher's
/// own delta covers its own bytes at the advertised rate (the node checks deltas).
#[must_use]
fn next_voucher(cur: &Cumulative, delta_bytes: u64, rate_per_mb: u64) -> Cumulative {
    let amount_delta = U256::from(delta_bytes)
        .saturating_mul(U256::from(rate_per_mb))
        .div_ceil(U256::from(MB_BYTES));
    Cumulative {
        bytes: cur.bytes.saturating_add(U256::from(delta_bytes)),
        amount: cur.amount.saturating_add(amount_delta),
    }
}

/// One lane's live hash-chain epoch: the payer half of the `PayWord` meter
/// (ADR 003 §Hash-chain metering).
///
/// The seed is **derived, never stored** — `chain::derive_seed(master, lane,
/// epoch)` reproduces it from the payer's signing key, so a restart resumes the
/// same chain with no secret at rest and only `epoch` persisted.
///
/// The whole 256-entry ladder is materialised at open. One pass of
/// `MAX_CHAIN_LENGTH` keccaks fills it (`preimages[255] = seed`, each earlier
/// entry one more hash, `preimages[0] = root`), which turns every later release
/// into an array read. The alternative — re-hashing from the seed per release —
/// would put up to 255 keccaks under the issuance lock on the delivery path, to
/// save 8 KiB per lane.
#[derive(Debug)]
struct ChainEpoch {
    /// Which epoch this is on the lane. Increments on every rollover, and is
    /// the only part of the chain that has to survive a restart.
    id: u64,
    /// `preimages[k]` is the value released at index `k`; `preimages[0]` is the
    /// `chain_root` the voucher commits.
    preimages: Box<[B256; 256]>,
    /// What one chunk adds over the anchor. Fixed for the epoch by the voucher
    /// that opened it.
    chunk_price: U256,
    /// The deepest index released so far. `0` means only the root exists, which
    /// is the state a fresh epoch opens in.
    released: u8,
}

impl ChainEpoch {
    fn open(lane: &LaneKey, master: B256, id: u64, chunk_price: U256) -> Self {
        let seed = decdn_incentive::chain::derive_seed(master, lane, id);
        let mut preimages = Box::new([B256::ZERO; 256]);
        let mut acc = seed;
        // Walk down from the seed at index 255 to the root at index 0, so the
        // ladder costs one pass rather than one pass per entry.
        for index in (0..=usize::from(MAX_CHAIN_LENGTH)).rev() {
            if let Some(slot) = preimages.get_mut(index) {
                *slot = acc;
            }
            acc = alloy::primitives::keccak256(acc);
        }
        Self {
            id,
            preimages,
            chunk_price,
            released: 0,
        }
    }

    /// The head this epoch commits — the value at index 0.
    fn root(&self) -> B256 {
        self.preimages.first().copied().unwrap_or(B256::ZERO)
    }

    /// The next index to release, or `None` once the epoch is spent and the
    /// payer must roll to a fresh root.
    fn next_index(&self) -> Option<u8> {
        (self.released < MAX_CHAIN_LENGTH).then_some(self.released + 1)
    }
}

/// The committed watermark plus the one-step rewind and the ambiguous in-flight
/// voucher, under a single lock so a reader never catches a half-applied update.
/// All three are read from a `Drop` impl, so the lock is a `std::sync::Mutex` —
/// see [`PoolLedger::committed`].
#[derive(Debug)]
struct Pipeline {
    /// What the chain has accrued SINCE the last signature: `released ×
    /// chunk_price` on the money axis and `released × CHUNK_BYTES` on the byte
    /// axis (ADR 003 §Hash-chain metering).
    ///
    /// Kept apart from `committed` — which stays the **signed anchor**, mirroring
    /// exactly what the node stores as its lane watermark — because a chain
    /// extends an anchor rather than replacing it. Folding accrual into the
    /// anchor early would make every later signature an implicit fold, and a
    /// fold that does not also roll to a fresh root is counted twice: once in
    /// the signed amount, and again by the frontier the node still holds under
    /// that same root.
    accrued: Cumulative,
    /// The accrual as of the previous signature, so a rejection rewinds both
    /// halves of the claim together.
    prev_accrued: Cumulative,
    /// The highest voucher whose send SUCCEEDED — presumed accepted, because
    /// continued delivery IS acceptance (ADR 005: only rejection is signalled).
    /// Advanced optimistically on each successful [`PoolLedger::issue`], rewound
    /// one step by [`PoolLedger::resolve_reject`], and hard-set by
    /// [`PoolLedger::reseed`].
    committed: Cumulative,
    /// The committed watermark BEFORE the most-recent successful voucher, so a
    /// later [`PoolLedger::resolve_reject`] can un-commit exactly that voucher —
    /// the one the node declared it never took. A reject terminates the stream
    /// (`handlers/client/wire.rs::write_reject`), and the self-heal path reseeds
    /// from the authenticated bundle immediately after, so a single-step rewind
    /// covers what the settlement window needs.
    prev: Option<Cumulative>,
    /// A voucher ARMED for send whose send did NOT confirm — an ambiguous error,
    /// or a future dropped mid-await. `committed` never advanced to it, but
    /// [`PoolLedger::settlement`] reports it (settle high — the upstream persists
    /// before it would reject, ADR 003), so a pull dropped inside the send still
    /// persists what the upstream may hold. Cleared on the next successful
    /// [`PoolLedger::issue`] or a [`PoolLedger::reseed`].
    armed: Option<Cumulative>,
}

/// What a voucher should do with the lane's hash chain.
///
/// A chain is not a second payment object: the lane has one voucher whose
/// cumulative `amount` is the settlement anchor, and the chain is an optional
/// extension that advances that anchor without another signature. So every
/// voucher makes exactly one of these three statements about it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EpochAction {
    /// Commit to whatever the lane already meters against — the live epoch's
    /// root, or a **sealed** section if it holds none. Opens nothing.
    ///
    /// This is what a per-stream re-anchor and a closing residual voucher both
    /// use. Re-asserting a root the node already holds is free, because an
    /// at-or-below-watermark voucher is already-satisfied rather than rejected,
    /// and settling a partial trailing chunk this way leaves the chain live for
    /// every sibling stream on the lane — which sealing would not.
    Keep,
    /// Commit to the live epoch, opening one if the lane meters nothing yet.
    /// A stream sends this before its first reveal.
    Open,
    /// Retire the live epoch and commit to a fresh root. The payer rolls when it
    /// exhausts a chain, and may roll earlier at its own discretion; either way
    /// the fold is the frontier actually reached, never a flat 255.
    Roll,
    /// Meter nothing: a **sealed** voucher, with a zero root at a zero price.
    /// This is the cooperative close, and the only thing that settles a partial
    /// trailing chunk — a preimage always advances the claim by a whole chunk,
    /// so a residual smaller than one needs a signature to be exact.
    Seal,
}

/// The chain section a voucher signs: what the payer commits to, and what the
/// node checks its own quoted rate against.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChainCommit {
    /// The chain head, or zero on a sealed voucher.
    pub chain_root: B256,
    /// What one chunk adds over the anchor, or zero on a sealed voucher.
    pub chunk_price: U256,
}

impl ChainCommit {
    /// The sealed section: uniformly zero, which is what lets the node's check
    /// be `chain_root == 0 ⟺ chunk_price == 0` with no branch on either side.
    pub const SEALED: Self = Self {
        chain_root: B256::ZERO,
        chunk_price: U256::ZERO,
    };
}

/// A preimage the payer has released, and where it sits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Released {
    /// The released value.
    pub preimage: B256,
    /// Its depth in the chain. Always `1..=MAX_CHAIN_LENGTH` — index 0 names the
    /// root and proves nothing the voucher does not already say, so it never
    /// travels the wire.
    pub index: u8,
    /// The epoch it belongs to, so a caller can tell whether the stream it is
    /// sending on has already carried that epoch's root voucher.
    pub epoch_id: u64,
}

/// Outcome of one [`PoolLedger::meter`] tick.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Metered {
    /// A preimage went out and the lane advanced by one chunk.
    Released(Released),
    /// The epoch has no index left (or the lane holds no chain). Nothing was
    /// sent; the caller signs a rollover voucher and meters again.
    Exhausted,
}

/// One lane's live voucher ledger, shared by every concurrent stream that draws
/// on it. Voucher *issuance* is serialized through the async `issuance` mutex —
/// held across compute → sign → send so vouchers reach the node in strict
/// cumulative order — and released the instant the send returns, because there
/// is no ack to wait for (implicit acceptance, ADR 005). The committed watermark
/// advances the moment a send succeeds; a mid-stream `VoucherRejected` rewinds
/// it through [`Self::resolve_reject`].
#[derive(Debug)]
pub struct PoolLedger {
    /// Serializes issuance (compute the next cumulative → sign → send). Its
    /// guarded value is `()`: the ordering it enforces lives in `pipeline`, and
    /// this is only the token that makes the compute-and-send critical section
    /// mutually exclusive across concurrent issuers. Held across an `await` (the
    /// send), so it is a `tokio::sync::Mutex`, never the sync one below.
    issuance: Mutex<()>,
    /// The committed watermark + rewind + armed voucher. A `std::sync::Mutex`
    /// because it is read from `Drop` (which cannot await): a pull can end by
    /// being DROPPED, not only by returning, and this is the record of money
    /// already spent (#1145 review). Held for a few instructions at a time and
    /// never across an await, so it cannot deadlock with the issuance lock.
    pipeline: std::sync::Mutex<Pipeline>,
    /// The lane's live hash-chain epoch. Guarded by its own sync mutex for the
    /// same reason `pipeline` is, and only ever touched while the `issuance`
    /// lock is held — the payer is one process, so it serializes the chain
    /// index and the rollover decision under a local lock while bytes stream
    /// concurrently (ADR 003 §Concurrent Streams).
    ///
    /// `None` on a lane that has not opened a chain yet: a transfer smaller
    /// than one chunk never meters, and settles through a single sealed
    /// amount-voucher.
    epoch: std::sync::Mutex<Option<ChainEpoch>>,
    /// Lane identity and the payer's master secret, together the derivation
    /// inputs every epoch on this lane is drawn from. Held rather than passed
    /// per call so a rollover needs no cooperation from the caller.
    lane: LaneKey,
    master: B256,
    /// The id the NEXT epoch on this lane will open at. Persisted alongside the
    /// lane's cumulative so a restart never re-opens a root the node has
    /// already seen. An `AtomicU64` because a reader wants it without taking
    /// either mutex.
    next_epoch_id: std::sync::atomic::AtomicU64,
}

impl PoolLedger {
    /// Build a ledger seeded from the lane's persisted cumulative state (the
    /// last voucher issued on earlier streams/invocations). Pass
    /// `Cumulative::default()` for a brand-new lane.
    #[must_use]
    pub fn new(lane: LaneKey, master: B256, epoch: u64, seed: Cumulative) -> Self {
        Self {
            issuance: Mutex::new(()),
            pipeline: std::sync::Mutex::new(Pipeline {
                committed: seed,
                prev: None,
                armed: None,
                accrued: Cumulative::default(),
                prev_accrued: Cumulative::default(),
            }),
            epoch: std::sync::Mutex::new(None),
            lane,
            master,
            next_epoch_id: std::sync::atomic::AtomicU64::new(epoch),
        }
    }

    /// A ledger for a lane that meters **nothing**: no identity, no master, no
    /// chain.
    ///
    /// The unpaid legs use this — the node's own-origin `BackendSource` quotes
    /// rate 0, so it never prices, signs, or meters anything, and a real lane
    /// identity would be a fiction. Callers MUST NOT drive
    /// [`EpochAction::Open`] or [`EpochAction::Roll`] through it: with a zero
    /// master every lane would derive the same seed, which is precisely the
    /// cross-lane reuse this module's seed derivation exists to prevent.
    #[must_use]
    pub fn unmetered(seed: Cumulative) -> Self {
        Self::new(
            LaneKey {
                pool_id: B256::ZERO,
                signer: alloy::primitives::Address::ZERO,
                provider: alloy::primitives::Address::ZERO,
            },
            B256::ZERO,
            0,
            seed,
        )
    }

    /// Lock the pipeline, recovering the inner value on poison. A poisoned lock
    /// means a prior holder panicked mid-write; the guarded value is money, so
    /// recover it rather than propagate — refusing to read here would throw away
    /// the very watermark the mirror exists to save.
    fn pipeline(&self) -> std::sync::MutexGuard<'_, Pipeline> {
        self.pipeline
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Read the committed cumulative WITHOUT awaiting — the drop-safe reader, and
    /// the only one a `Drop` impl can call. It is the highest voucher whose send
    /// succeeded (implicit acceptance), so it is exactly what a *completed*
    /// buffered pull persists.
    #[must_use]
    pub fn committed(&self) -> Cumulative {
        let pipeline = self.pipeline();
        pipeline.committed.plus(pipeline.accrued)
    }

    /// The cumulative a cancelled pull must PERSIST — the drop-path counterpart of
    /// [`Self::committed`], and what a `Drop` guard should actually write (#1122).
    ///
    /// It is [`Self::committed`], except when a voucher is still armed with its
    /// send unconfirmed, in which case it is the HIGHER of the two by `amount`
    /// (the monotone key). Settling high is the safe direction and it is correct:
    /// an armed voucher pays for bytes the upstream ALREADY DELIVERED, and the
    /// upstream persists a voucher before it would reject it, so an ambiguous
    /// failure may leave the upstream holding it — under-reporting would strand
    /// the deposit. A voucher the upstream explicitly REJECTED is never here —
    /// [`Self::resolve_reject`] clears `armed` and rewinds `committed` — so this
    /// cannot inflate our cumulative for bytes the upstream declined.
    #[must_use]
    pub fn settlement(&self) -> Cumulative {
        let pipeline = self.pipeline();
        let owed = pipeline.committed.plus(pipeline.accrued);
        match pipeline.armed {
            Some(armed) if armed.amount > owed.amount => armed,
            _ => owed,
        }
    }

    /// Issue one voucher for `delta_bytes` newly delivered since the last voucher
    /// and SEND it. The send IS the commit: on a successful send the committed
    /// watermark advances to the new voucher (continued delivery is acceptance,
    /// ADR 005), and there is no ack to wait for.
    ///
    /// Holds the issuance lock across compute → arm → `exchange`, so concurrent
    /// issuers on the shared ledger sign strictly increasing cumulatives. The
    /// voucher is ARMED (recorded as the in-flight candidate) *before* `exchange`,
    /// so a future dropped inside the send still has [`Self::settlement`] report
    /// what we owe. An `exchange` error leaves the voucher armed (its fate is
    /// ambiguous: a partial write may have reached the upstream), so `settlement`
    /// settles high while `committed` does not advance.
    ///
    /// `exchange` receives the next [`Cumulative`] (the values to sign and send);
    /// the caller owns signing + framing so this module stays free of EIP-712 /
    /// wire types.
    pub async fn issue<F, Fut>(
        &self,
        delta_bytes: u64,
        rate_per_mb: u64,
        epoch: EpochAction,
        exchange: F,
    ) -> anyhow::Result<Cumulative>
    where
        F: FnOnce(Cumulative, ChainCommit) -> Fut,
        Fut: Future<Output = anyhow::Result<()>>,
    {
        // Serialize issuance. Held across the send below.
        let _issuing = self.issuance.lock().await;

        // Read the accrual and the frontier this voucher builds on under one
        // pipeline lock, so the basis cannot shift between reading it and
        // recording it. Build on the SETTLEMENT frontier, not the anchor alone:
        // a voucher already on the wire must be exceeded, or we re-sign a
        // cumulative the upstream may hold.
        let (frontier, accrued) = {
            let pipeline = self.pipeline();
            let owed = pipeline.committed.plus(pipeline.accrued);
            let frontier = match pipeline.armed {
                Some(armed) if armed.amount > owed.amount => armed,
                _ => owed,
            };
            (frontier, pipeline.accrued)
        };

        // **A voucher that folds must also roll.** Signing the accrued frontier
        // into `amount` is what retires the chain that proved it, so the voucher
        // MUST carry a fresh root — otherwise the node keeps the old root's
        // frontier alongside an amount that already folded it in, and the same
        // chunks are counted twice (ADR 003 §Rollover: the fold and the fresh
        // root are one step, not two).
        //
        // A voucher with nothing accrued folds nothing, so it commits to
        // whatever the caller asked for: an opening anchor, a free re-anchor
        // that re-asserts a root the node already holds, or a sealed close.
        let epoch = if accrued.amount.is_zero() && accrued.bytes.is_zero() {
            epoch
        } else {
            EpochAction::Roll
        };
        let commit = self.commit_epoch(epoch, rate_per_mb);

        let next = {
            let mut pipeline = self.pipeline();
            let next = next_voucher(&frontier, delta_bytes, rate_per_mb);
            pipeline.armed = Some(next);
            next
        };
        // Send. On ANY error the voucher stays armed and the anchor does not
        // advance: a send failure is as ambiguous as a drop, so `settlement`
        // settles high. A rejection is NOT an issuance outcome — it arrives later
        // as a `StreamError` message and is disarmed via `resolve_reject`.
        exchange(next, commit).await?;
        // The send succeeded: commit optimistically. The accrual is now folded
        // into the signed anchor, so it resets to zero — and the previous pair
        // is remembered so a later `resolve_reject` can un-commit exactly this
        // voucher, both halves together.
        {
            let mut pipeline = self.pipeline();
            pipeline.prev = Some(pipeline.committed);
            pipeline.prev_accrued = pipeline.accrued;
            pipeline.committed = next;
            pipeline.accrued = Cumulative::default();
            pipeline.armed = None;
        }
        Ok(next)
    }

    /// Apply an [`EpochAction`] and report what the resulting voucher commits.
    ///
    /// Callers hold the issuance lock, which is what makes "read the epoch,
    /// maybe replace it, report it" one indivisible step against the concurrent
    /// streams sharing this lane.
    fn commit_epoch(&self, action: EpochAction, rate_per_mb: u64) -> ChainCommit {
        let mut slot = self.epoch();
        match action {
            EpochAction::Seal => {
                // A sealed voucher meters no chunk, so its whole chain section
                // is zero. Dropping the epoch is what retires the chain: a
                // preimage released under the old root extends nothing once the
                // node has adopted a zero root.
                *slot = None;
                ChainCommit::SEALED
            }
            EpochAction::Keep => slot
                .as_ref()
                .map_or(ChainCommit::SEALED, |live| ChainCommit {
                    chain_root: live.root(),
                    chunk_price: live.chunk_price,
                }),
            EpochAction::Open => {
                if let Some(live) = slot.as_ref() {
                    return ChainCommit {
                        chain_root: live.root(),
                        chunk_price: live.chunk_price,
                    };
                }
                let opened = self.open_epoch(rate_per_mb);
                let commit = ChainCommit {
                    chain_root: opened.root(),
                    chunk_price: opened.chunk_price,
                };
                *slot = Some(opened);
                commit
            }
            EpochAction::Roll => {
                let opened = self.open_epoch(rate_per_mb);
                let commit = ChainCommit {
                    chain_root: opened.root(),
                    chunk_price: opened.chunk_price,
                };
                *slot = Some(opened);
                commit
            }
        }
    }

    /// Draw the next epoch on this lane and bump the persisted counter.
    ///
    /// Each epoch gets an independent seed from the lane triple plus its own id,
    /// so no two chains this payer opens — across providers, pools, or its own
    /// sibling signers — are derivable from each other. That is the whole
    /// defence against the cross-lane preimage spend, and it is payer-side by
    /// design: reuse costs the payer and pays the node, so no node-side rule
    /// would protect anyone who chose to run without it (ADR 003 §One chain per
    /// lane).
    fn open_epoch(&self, rate_per_mb: u64) -> ChainEpoch {
        let id = self
            .next_epoch_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        ChainEpoch::open(&self.lane, self.master, id, U256::from(rate_per_mb))
    }

    /// Lock the epoch slot, recovering the inner value on poison — same reason
    /// as [`Self::pipeline`].
    fn epoch(&self) -> std::sync::MutexGuard<'_, Option<ChainEpoch>> {
        self.epoch
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The chain this lane is currently metering against, or
    /// [`ChainCommit::SEALED`] if it holds none.
    ///
    /// Read by a stream that needs to know whether it has already carried the
    /// current epoch's root voucher: on each stream, the epoch's `chain_root`
    /// voucher MUST precede that stream's own preimages for that epoch, or the
    /// node cannot name the chain a bare reveal belongs to.
    #[must_use]
    pub fn chain_commit(&self) -> ChainCommit {
        self.epoch()
            .as_ref()
            .map_or(ChainCommit::SEALED, |live| ChainCommit {
                chain_root: live.root(),
                chunk_price: live.chunk_price,
            })
    }

    /// The id of the live epoch, or `None` on a lane metering nothing. A stream
    /// compares this against the epoch it last anchored itself to.
    #[must_use]
    pub fn epoch_id(&self) -> Option<u64> {
        self.epoch().as_ref().map(|live| live.id)
    }

    /// The epoch id a restart should resume from — the counter this lane will
    /// draw its next chain at. Persisted alongside the cumulative; the seed
    /// itself never is.
    #[must_use]
    pub fn next_epoch_id(&self) -> u64 {
        self.next_epoch_id
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Meter one delivered chunk: release the next preimage and SEND it.
    ///
    /// This is the tick the whole design exists for — one keccak-ladder read,
    /// one 33-byte message, no signature, no acknowledgement, and nothing
    /// durable to write before the node sends the next chunk. The released value
    /// is self-proving: nobody derives a deeper preimage from a shallower one
    /// without the seed, so it IS the receipt for every chunk below it.
    ///
    /// Released only AFTER the payer has received and verified the chunk it pays
    /// for, which is what keeps the payer's exposure at zero.
    ///
    /// Holds the issuance lock across compute → send, exactly as [`Self::issue`]
    /// does, so concurrent streams on one lane release strictly deepening
    /// indices and never two values at the same depth.
    ///
    /// Returns [`Metered::Exhausted`] **without sending anything** when the
    /// epoch has no index left. The caller then signs a rollover voucher
    /// ([`EpochAction::Roll`]) — whose amount already folds this chain's
    /// frontier, since every release advanced the committed cumulative — and
    /// meters again.
    ///
    /// # Errors
    ///
    /// Propagates an `exchange` failure. The index is NOT consumed on a failed
    /// send: an unreleased preimage proves nothing, so re-releasing the same
    /// index later is both safe and correct.
    pub async fn meter<F, Fut>(&self, exchange: F) -> anyhow::Result<Metered>
    where
        F: FnOnce(Released) -> Fut,
        Fut: Future<Output = anyhow::Result<()>>,
    {
        let _issuing = self.issuance.lock().await;
        let (released, price) = {
            let slot = self.epoch();
            let Some(live) = slot.as_ref() else {
                return Ok(Metered::Exhausted);
            };
            let Some(index) = live.next_index() else {
                return Ok(Metered::Exhausted);
            };
            let Some(preimage) = live.preimages.get(usize::from(index)).copied() else {
                return Ok(Metered::Exhausted);
            };
            (
                Released {
                    preimage,
                    index,
                    epoch_id: live.id,
                },
                live.chunk_price,
            )
        };

        exchange(released).await?;

        // The reveal is out. Advance the lane's worth by exactly one chunk on
        // both axes — the node credits the same, because it derives both from
        // the same two protocol constants rather than from anything on the wire.
        {
            let mut slot = self.epoch();
            if let Some(live) = slot.as_mut() {
                live.released = released.index;
            }
        }
        {
            let mut pipeline = self.pipeline();
            pipeline.accrued.amount = pipeline.accrued.amount.saturating_add(price);
            pipeline.accrued.bytes = pipeline
                .accrued
                .bytes
                .saturating_add(U256::from(CHUNK_BYTES));
        }
        Ok(Metered::Released(released))
    }

    /// Wallet-less self-heal (issue #1481): overwrite the committed watermark to
    /// `cum` — typically [`Cumulative::from`] a [`WatermarkBundle`] the node
    /// attached to a gated `AmountRegression` / `BytesRegression` / `SpendingCapExhausted`
    /// rejection — and clear the rewind + armed state, since the node has just
    /// told us its authoritative watermark. The next [`Self::issue`] builds on
    /// `cum`, matching what the node will accept next.
    ///
    /// This is a hard overwrite, not a monotonic bump: the caller's prior local
    /// state was wrong (a wallet-less client has no reliable on-chain source for
    /// its watermark until settlement), so the bundle — signer-verified by the
    /// node before it was sent, and re-verified against the client's own key by
    /// `resumable_watermark` before it reaches here — is authoritative. Callers
    /// MUST only pass a cumulative sourced from such a bundle.
    ///
    /// Returns `false` — leaving the ledger untouched — if `cum` does not ADVANCE
    /// past the committed watermark's `amount`. Reseeding heals a watermark that
    /// has fallen BEHIND what the node holds; a bundle at or behind `committed`
    /// proves nothing, and applying it would REGRESS the watermark and re-sign a
    /// spent amount. Guarded here rather than only at the call sites because
    /// monotonicity is the ledger's invariant to keep.
    #[must_use]
    pub fn reseed(&self, cum: Cumulative) -> bool {
        let mut pipeline = self.pipeline();
        if cum.amount <= pipeline.committed.plus(pipeline.accrued).amount {
            return false;
        }
        pipeline.committed = cum;
        pipeline.accrued = Cumulative::default();
        pipeline.prev = None;
        pipeline.prev_accrued = Cumulative::default();
        pipeline.armed = None;
        true
    }

    /// Resolve the most-recent presumed-accepted voucher as explicitly REJECTED:
    /// clear any armed voucher and rewind `committed` to the state before it,
    /// WITHOUT settling that voucher. Called by the receive loop when a
    /// `StreamError::VoucherRejected` arrives. A rejection is the upstream
    /// declaring it never took the voucher, so — unlike an ambiguous failure — it
    /// must not be settled optimistically (that would inflate our cumulative for
    /// bytes the upstream refused to be paid for).
    ///
    /// Returns `false` if there was nothing to rewind — a spurious rejection.
    ///
    /// Assumes the rejected voucher is the LATEST committed one: the one-step
    /// `committed → prev` rewind is exact for the inline-await receive loop, which
    /// issues at most one armed voucher at a time and learns of its rejection before
    /// issuing the next.
    pub fn resolve_reject(&self) -> bool {
        let mut pipeline = self.pipeline();
        pipeline.armed = None;
        match pipeline.prev.take() {
            Some(prev) => {
                // Rewind BOTH halves. The rejected voucher folded whatever had
                // accrued at the time it was signed, so restoring the anchor
                // without giving that accrual back would silently forget chunks
                // the node has already been shown preimages for.
                //
                // The fold comes back ON TOP of whatever has accrued SINCE —
                // reveals released after that signature are still released, and
                // a released preimage cannot be taken back. Overwriting with
                // `prev_accrued` alone would drop them, and the payer would
                // then believe it owes less than the node can already redeem.
                pipeline.committed = prev;
                pipeline.accrued = pipeline
                    .accrued
                    .plus(std::mem::take(&mut pipeline.prev_accrued));
                true
            }
            None => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{PullStalled, UpstreamVoucherRejected};
    use decdn_protocol::client::VoucherRejectReason;
    use std::sync::Arc;
    use std::time::Duration;

    /// 50 concurrent issuers on one ledger sign strictly increasing cumulatives
    /// with no gaps: each successful send commits, so the committed watermark ends
    /// at the exact sum of the 100-byte deltas and never more.
    #[tokio::test]
    async fn concurrent_issue_is_monotonic_and_exact() -> anyhow::Result<()> {
        let ledger = Arc::new(PoolLedger::unmetered(Cumulative::default()));
        let mut handles = Vec::new();
        for _ in 0..50u32 {
            let l = Arc::clone(&ledger);
            handles.push(tokio::spawn(async move {
                l.issue(100, 10, EpochAction::Keep, |_signed, _chain| async {
                    tokio::task::yield_now().await;
                    Ok(())
                })
                .await
            }));
        }

        let mut byte_totals = Vec::new();
        for h in handles {
            byte_totals.push(h.await??.bytes);
        }
        byte_totals.sort_unstable();
        let expected: Vec<U256> = (1..=50u64).map(|k| U256::from(k * 100)).collect();
        assert_eq!(
            byte_totals, expected,
            "every voucher's cumulative is distinct"
        );

        // Every voucher committed ⇒ committed carries all 50, and never more bytes
        // than the 100-per-voucher deltas we actually issued.
        let committed = ledger.committed();
        assert_eq!(committed.bytes, U256::from(5000u64));
        assert_eq!(committed.amount, U256::from(50u64));
        assert_eq!(ledger.settlement(), committed);
        Ok(())
    }

    #[tokio::test]
    async fn failed_send_does_not_commit() -> anyhow::Result<()> {
        let ledger = PoolLedger::unmetered(Cumulative::default());
        let result = ledger
            .issue(100, 10, EpochAction::Keep, |_signed, _chain| async {
                anyhow::bail!("send lost")
            })
            .await;
        assert!(result.is_err(), "a failed send must surface the error");
        // Committed unmoved: a send that never confirmed never advances committed.
        assert_eq!(ledger.committed(), Cumulative::default());
        // But it settles HIGH — the send is ambiguous, so the voucher stays armed.
        assert_eq!(ledger.settlement().bytes, U256::from(100u64));
        Ok(())
    }

    /// The window this exists to close (#1122): a pull dropped inside the send
    /// leaves the upstream possibly holding a voucher we have no committed record
    /// of. Settle low and the deposit is stranded; settle high and it is honoured.
    #[tokio::test]
    async fn a_pull_dropped_inside_the_send_settles_at_the_voucher_it_sent() {
        let ledger = PoolLedger::unmetered(Cumulative::default());
        let dropped = tokio::time::timeout(
            Duration::from_millis(20),
            ledger.issue(100, 10, EpochAction::Keep, |_next, _chain| {
                std::future::pending::<anyhow::Result<()>>()
            }),
        )
        .await;
        assert!(dropped.is_err(), "the send must still be in flight");

        assert_eq!(
            ledger.committed(),
            Cumulative::default(),
            "an unconfirmed voucher must never advance the committed watermark"
        );
        let settled = ledger.settlement();
        assert_eq!(
            settled.bytes,
            U256::from(100u64),
            "settle at the sent voucher"
        );
        assert_eq!(settled.amount, U256::from(1u64));
    }

    /// A voucher the upstream explicitly REJECTED was never taken, so settling at
    /// it would inflate our cumulative. `resolve_reject` rewinds committed WITHOUT
    /// keeping it, so — with nothing else in flight — settlement falls back.
    #[tokio::test]
    async fn a_rejected_voucher_is_not_settled_optimistically() -> anyhow::Result<()> {
        let ledger = PoolLedger::unmetered(Cumulative::default());
        // Issue + successful send: committed advances to the voucher.
        ledger
            .issue(100, 10, EpochAction::Keep, |_next, _chain| async { Ok(()) })
            .await?;
        assert_eq!(ledger.committed().bytes, U256::from(100u64));
        // The upstream rejects it (arrived as a mid-stream VoucherRejected).
        assert!(ledger.resolve_reject(), "the committed voucher is rewound");
        assert_eq!(
            ledger.settlement(),
            Cumulative::default(),
            "an explicitly rejected voucher must not advance what we persist"
        );
        Ok(())
    }

    /// Issue #1481: a wallet-less client cannot reconstruct its watermark from
    /// chain, so a gated rejection carries the node's true watermark back in a
    /// (nonce-free) `WatermarkBundle`. `Cumulative::from` must decode it losslessly.
    #[test]
    fn cumulative_from_bundle_is_lossless() {
        let bundle = WatermarkBundle {
            chain_root: [0u8; 32],
            verified_index: 0,
            tip: [0u8; 32],
            chunk_price: 0,
            amount: u64::MAX,
            bytes_delivered: 1_048_576u64,
            last_signature: vec![0xABu8; 65],
        };
        let cum = Cumulative::from(&bundle);
        assert_eq!(cum.amount, U256::from(u64::MAX));
        assert_eq!(cum.bytes, U256::from(1_048_576u64));
    }

    /// The self-heal itself: an `AmountRegression` rejection with an authenticated
    /// bundle is not a dead end. `reseed` overwrites committed to the node's true
    /// state and clears the armed/rewind state, so the next `issue` builds on the
    /// bundle rather than colliding with what the node already holds.
    #[tokio::test]
    async fn an_amount_regression_rejection_with_a_bundle_self_heals() -> anyhow::Result<()> {
        // The caller's local ledger thinks it is at amount 10 (a wallet-less
        // delegate that never persisted the true watermark), but the node's true
        // watermark — echoed on the gated reject — is amount 50.
        let ledger = PoolLedger::unmetered(Cumulative {
            bytes: U256::from(1000u64),
            amount: U256::from(10u64),
        });
        let bundle = WatermarkBundle {
            chain_root: [0u8; 32],
            verified_index: 0,
            tip: [0u8; 32],
            chunk_price: 0,
            amount: 50u64,
            bytes_delivered: 5000u64,
            last_signature: vec![0xCDu8; 65],
        };

        // A voucher armed then ambiguously failed (settle high) before the reject.
        let _ = ledger
            .issue(100, 10, EpochAction::Keep, |_next, _chain| async {
                anyhow::bail!("ambiguous")
            })
            .await;
        assert!(ledger.settlement().amount > U256::from(10u64));

        // Self-heal: re-seed to the node's authenticated watermark.
        assert!(
            ledger.reseed(Cumulative::from(&bundle)),
            "a bundle ahead of committed must be applied"
        );
        assert_eq!(
            ledger.settlement().amount,
            U256::from(50u64),
            "reseed cleared the armed voucher and reset to the bundle watermark"
        );

        let issued = ledger
            .issue(100, 10, EpochAction::Keep, |_next, _chain| async { Ok(()) })
            .await?;
        assert_eq!(issued.bytes, U256::from(5100u64)); // bundle.bytes_delivered + 100
        assert_eq!(issued.amount, U256::from(51u64)); // bundle.amount + ceil(100*10/MiB)
        Ok(())
    }

    /// The monotonicity guard (#1497 review): a bundle that does NOT advance past
    /// the committed watermark must be refused, leaving the ledger untouched. The
    /// node attaches a bundle to EVERY watermark-gated rejection once any voucher
    /// has been accepted — including a genuinely exhausted lane, whose bundle just
    /// echoes the watermark the client already holds.
    #[tokio::test]
    async fn reseed_refuses_a_bundle_that_does_not_advance_the_watermark() -> anyhow::Result<()> {
        let committed = Cumulative {
            bytes: U256::from(5000u64),
            amount: U256::from(50u64),
        };
        let ledger = PoolLedger::unmetered(committed);
        ledger
            .issue(100, 10, EpochAction::Keep, |_next, _chain| async { Ok(()) })
            .await?;
        let after_issue = ledger.committed();

        // The exhausted-lane echo: same amount we already hold.
        let echo = Cumulative {
            bytes: after_issue.bytes,
            amount: after_issue.amount,
        };
        assert!(
            !ledger.reseed(echo),
            "a bundle at the committed watermark proves nothing and must be refused"
        );
        // And the strictly-behind case must not rewind us either.
        let behind = Cumulative {
            bytes: U256::from(2000u64),
            amount: U256::from(20u64),
        };
        assert!(
            !ledger.reseed(behind),
            "a bundle behind committed must be refused"
        );
        assert_eq!(
            ledger.committed(),
            after_issue,
            "committed must never regress"
        );
        Ok(())
    }

    /// `SpendingCapExhausted` with NO bundle (a genuinely exhausted capability, nothing to
    /// resume from) must not be treated as self-healable — a caller checking
    /// `bundle.is_none()` sees the "give up / top up" signal. This pins the
    /// type-shape contract the resume path depends on.
    #[test]
    fn cap_exceeded_without_a_bundle_is_not_self_healable() -> anyhow::Result<()> {
        let err = anyhow::Error::new(UpstreamVoucherRejected {
            reason: VoucherRejectReason::SpendingCapExhausted,
            bundle: None,
        });
        let upstream = err
            .downcast_ref::<UpstreamVoucherRejected>()
            .ok_or_else(|| anyhow::anyhow!("expected UpstreamVoucherRejected, got: {err:?}"))?;
        assert_eq!(upstream.reason, VoucherRejectReason::SpendingCapExhausted);
        assert!(
            upstream.bundle.is_none(),
            "no bundle means no self-heal path — the caller must surface a top-up need"
        );
        Ok(())
    }

    /// An AMBIGUOUS failure — a stall timeout, a transport reset — is not a
    /// rejection: the upstream may already hold the voucher, so it must leave the
    /// voucher ARMED and settle HIGH, exactly as a drop does.
    #[tokio::test]
    async fn an_ambiguous_failure_settles_high() {
        for make_err in [
            || {
                anyhow::Error::new(PullStalled {
                    after: Duration::from_secs(1),
                })
            },
            || anyhow::anyhow!("connection reset by peer"),
        ] {
            let ledger = PoolLedger::unmetered(Cumulative::default());
            let result = ledger
                .issue(
                    100,
                    10,
                    EpochAction::Keep,
                    move |_next, _chain| async move { Err(make_err()) },
                )
                .await;
            assert!(result.is_err(), "the ambiguous send must surface its error");
            assert_eq!(ledger.committed(), Cumulative::default());
            let settled = ledger.settlement();
            assert_eq!(
                settled.bytes,
                U256::from(100u64),
                "settle at the sent voucher"
            );
        }
    }

    /// After a voucher is armed (a failed send), the NEXT issue must build on it,
    /// not re-sign the same cumulative — which the upstream may already hold.
    #[tokio::test]
    async fn a_later_issue_builds_on_an_armed_voucher() -> anyhow::Result<()> {
        let ledger = PoolLedger::unmetered(Cumulative::default());
        let stalled = ledger
            .issue(100, 10, EpochAction::Keep, |_next, _chain| async {
                Err(anyhow::Error::new(PullStalled {
                    after: Duration::from_secs(1),
                }))
            })
            .await;
        assert!(stalled.is_err());
        // Second issue must build on the armed voucher (bytes 200, not 100).
        let sent = ledger
            .issue(100, 10, EpochAction::Keep, |_next, _chain| async { Ok(()) })
            .await?;
        assert_eq!(
            sent.bytes,
            U256::from(200u64),
            "the next voucher must build on the armed voucher, not collide with it"
        );
        Ok(())
    }

    /// A sequence of successful issues stays ordered and exact — the committed
    /// watermark is the running cumulative, never ahead of what was delivered.
    #[tokio::test]
    async fn a_sequence_of_issues_stays_ordered_and_exact() -> anyhow::Result<()> {
        let ledger = PoolLedger::unmetered(Cumulative::default());
        for expected in 1..=100u64 {
            let sent = ledger
                .issue(100, 10, EpochAction::Keep, |_next, _chain| async { Ok(()) })
                .await?;
            assert_eq!(sent.bytes, U256::from(expected * 100));
        }
        let committed = ledger.committed();
        assert_eq!(committed.bytes, U256::from(10_000u64));
        assert_eq!(ledger.settlement(), committed);
        Ok(())
    }

    #[test]
    fn bytes_accumulate_by_delta() {
        let cur = Cumulative {
            bytes: U256::from(1000u64),
            amount: U256::ZERO,
        };
        assert_eq!(next_voucher(&cur, 500, 10).bytes, U256::from(1500u64));
    }

    #[test]
    fn amount_rounds_up_per_voucher() {
        // 1 byte at rate 10/MiB rounds up to 1 (not 0).
        let cur = Cumulative::default();
        assert_eq!(next_voucher(&cur, 1, 10).amount, U256::from(1u64));
        // The largest sub-MiB delta still rounds up to a full MiB's cost.
        assert_eq!(
            next_voucher(&cur, MB_BYTES - 1, 10).amount,
            U256::from(10u64)
        );
        // A full MiB at rate 10 costs exactly 10.
        assert_eq!(next_voucher(&cur, MB_BYTES, 10).amount, U256::from(10u64));
    }

    #[test]
    fn zero_delta_bumps_nothing() {
        let cur = Cumulative {
            bytes: U256::from(7u64),
            amount: U256::from(3u64),
        };
        let next = next_voucher(&cur, 0, 99);
        assert_eq!(next.bytes, U256::from(7u64));
        assert_eq!(next.amount, U256::from(3u64));
    }
}
