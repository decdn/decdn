//! Per-lane voucher accounting. [`next_voucher`] is the pure cumulative math;
//! [`PoolLedger`] serializes voucher *issuance* across concurrent streams that
//! draw on one pool lane. Continued delivery is acceptance (ADR 003/005): a sent
//! voucher commits optimistically — the send itself is the commit — and only an
//! explicit rejection rewinds it. There is no ack to wait for, so the issuance
//! lock spans compute → sign → send and nothing more, and parallel streams to
//! one provider never serialize their payments behind each other's round trips.

use std::future::Future;
use std::time::Duration;

use alloy::primitives::{B256, U256};
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
    /// Decode a wallet-less resume bundle (issue #1481) into the full lane claim
    /// it represents, in the shape [`PoolLedger`] tracks.
    ///
    /// **This folds the chain half.** A bundle reports two things: the signed
    /// anchor the node last accepted, and the frontier its chain has proved on
    /// top of that anchor. Decoding only the anchor would throw away
    /// `verified_index` chunks the node already holds preimages for — and the
    /// resuming signer would then re-sign from the anchor, open a fresh root
    /// beside a frontier the node is still metering, and watch every reveal that
    /// followed fold nothing (the node keeps the old root: a chain with reveals
    /// under it is worth more than the anchor a lagging voucher offers for it).
    /// So the fold is not an optimisation; it is what makes a resume converge.
    ///
    /// It is the same fold a rollover performs — `amount + verified_index ×
    /// chunk_price`, `bytes + verified_index × CHUNK_BYTES` — and the node's
    /// `watermark_bundle_for_reject` documents it as the signer's side of the
    /// bargain (ADR 005 §Watermark bundle). A bundle carrying no chain
    /// (`verified_index == 0`, the sealed or never-metered case) folds nothing
    /// and decodes to the anchor alone.
    ///
    /// Infallible: the fold is computed in `U256`, so the `u64` inputs cannot
    /// overflow it and a caller can re-seed from a bundle without a `Result`.
    ///
    /// It is also unauthenticated, and deliberately so — this is arithmetic, not
    /// a trust boundary. `verified_index` is a number the node writes and no
    /// signature covers, so a bundle reaches this conversion only through
    /// `resumable_watermark`, which proves the anchor against the client's own
    /// signature and the frontier against the bundle's `tip` before any of it
    /// becomes money. Do not fold a bundle that has not been through that gate.
    fn from(bundle: &WatermarkBundle) -> Self {
        let proved = U256::from(bundle.verified_index);
        Self {
            bytes: U256::from(bundle.bytes_delivered)
                .saturating_add(proved.saturating_mul(U256::from(CHUNK_BYTES))),
            amount: U256::from(bundle.amount)
                .saturating_add(proved.saturating_mul(U256::from(bundle.chunk_price))),
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
/// The seed is **drawn, never stored and never reproduced** — 32 fresh bytes at
/// open, held here for as long as the chain meters and dropped with it. Two
/// chains therefore share a root only with probability 2⁻²⁵⁶, which is the
/// whole of the no-reuse property: there is no derivation input to bind, no
/// counter to persist, and nothing secret at rest.
///
/// A chain never outlives the process that drew it. Resumption after a restart
/// folds the frontier the node proved into a signed amount and opens a fresh
/// chain (ADR 003 §Resumption folds), so re-deriving an old seed is not a
/// capability this type gives up — it is one nothing asks for.
///
/// The whole 256-entry ladder is materialised at open. One pass of
/// `MAX_CHAIN_LENGTH` keccaks fills it (`preimages[255] = seed`, each earlier
/// entry one more hash, `preimages[0] = root`), which turns every later release
/// into an array read. The alternative — re-hashing from the seed per release —
/// would put up to 255 keccaks under the issuance lock on the delivery path, to
/// save 8 KiB per lane.
#[derive(Debug)]
struct ChainEpoch {
    /// `preimages[k]` is the value released at index `k`; `preimages[0]` is the
    /// `chain_root` the voucher commits.
    preimages: Box<[B256; 256]>,
    /// What one chunk adds over the anchor. Fixed for the epoch by the voucher
    /// that opened it, and the other half of the seed.
    chunk_price: U256,
    /// The deepest index released so far. `0` means only the root exists, which
    /// is the state a fresh epoch opens in.
    released: u8,
}

impl ChainEpoch {
    fn open(chunk_price: U256) -> Self {
        let seed = decdn_incentive::chain::random_seed();
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
    /// The proof most recently put on the wire for this lane, identified — not
    /// merely classified — so a rejection rewinds the thing that was actually
    /// refused and nothing else.
    ///
    /// A rejection names no proof. Under the chain the lane emits two kinds and
    /// they rewind differently — a refused voucher un-commits the anchor, a
    /// refused reveal only takes back one chunk of accrual — so guessing wrong
    /// walks the anchor backwards below what the node accepted, and every later
    /// voucher then regresses. And the lane is shared: a rejection is read on
    /// the stream it arrived on, while a sibling may have issued since, so the
    /// KIND alone is not enough to tell whether the refused proof is still the
    /// one this field holds. See [`PoolLedger::resolve_reject`].
    last_proof: Option<LastProof>,
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

/// What the most recent voucher did to the lane's chain slot, and therefore what
/// a rejection of that voucher has to undo.
#[derive(Debug)]
enum Displaced {
    /// The voucher left the chain slot alone — a re-anchor, a residual, an open
    /// on a lane that already had one. A rejection restores nothing.
    Nothing,
    /// The voucher REPLACED the chain slot; a rejection puts this value back.
    /// Itself empty when what it replaced was no chain at all (an open on a bare
    /// lane), which a rejection must restore just as faithfully.
    Replaced(Option<ChainEpoch>),
}

/// Which proof last went out, named exactly, and what undoing it costs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LastProof {
    /// A signed voucher, named by the cumulative it claims. Rewinding it
    /// restores the previous anchor and hands back the accrual that voucher
    /// folded.
    Voucher { amount: U256 },
    /// One released preimage, named by its chain and depth, and worth
    /// `chunk_price` on the money axis and one `CHUNK_BYTES` on the byte axis.
    /// Rewinding it takes back exactly that and leaves the signed anchor alone.
    Reveal {
        chain_root: B256,
        index: u8,
        chunk_price: U256,
    },
}

/// The proof one stream put on the wire, in the terms that identify it on the
/// lane — what a stream must hand [`PoolLedger::resolve_reject`] to say which
/// proof the rejection it just read was for.
///
/// Both names are unique on a lane by construction: issuance is serialized, so
/// cumulative amounts strictly increase and no two vouchers share one, and a
/// chain's indices strictly deepen so no two reveals share a `(root, index)`.
/// That is what lets the ledger tell "the proof I am being told about is still
/// the last thing this lane did" from "a sibling has moved on since".
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamProof {
    /// A signed voucher claiming this cumulative amount.
    Voucher {
        /// The `amount` the voucher was signed over.
        amount: U256,
    },
    /// A preimage released at this depth on this chain.
    Reveal {
        /// The chain the reveal extends, named by its root.
        chain_root: B256,
        /// The depth released.
        index: u8,
    },
}

/// What a voucher should do with the lane's hash chain.
///
/// A chain is not a second payment object: the lane has one voucher whose
/// cumulative `amount` is the settlement anchor, and the chain is an optional
/// extension that advances that anchor without another signature. So every
/// voucher makes exactly one of these three statements about it.
///
/// There is no "seal" statement, because sealing is not a decision a caller
/// makes: [`Self::Keep`] on a lane that meters nothing already commits
/// [`ChainCommit::SEALED`], which is every voucher a sub-chunk transfer ever
/// sends.
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
    /// The chain it belongs to, named by its root — so a caller can tell whether
    /// the stream it is sending on has already carried that chain's root voucher.
    /// The root IS the chain's identity: each chain draws its own secret, so a
    /// fresh chain always has a fresh root.
    pub chain_root: B256,
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
    /// The chain the most recent voucher DISPLACED, so a rejection can put it
    /// back. `None` means that voucher displaced nothing; `Some(x)` restores `x`
    /// (itself `None` for a seal, which displaces a chain with no chain).
    ///
    /// A rollover replaces the chain BEFORE the send, because the voucher has to
    /// carry the new root to be signed at all. If the node then refuses it, the
    /// payer would otherwise be left metering a chain the node never adopted: its
    /// reveals name a root the lane does not track and fold nothing, and its next
    /// re-anchor states a root the node will not accept over a frontier it has
    /// already proved. Keeping one step of history costs one ladder and makes the
    /// rejection exactly undoable.
    ///
    /// Locked AFTER `epoch` wherever both are taken, which is the only order
    /// either is ever acquired in.
    retired: std::sync::Mutex<Displaced>,
    /// Ceiling on a single voucher send. The issuance lock is held across the
    /// send on purpose — vouchers must reach the node in strict cumulative order
    /// — but that means one send is on the critical path of EVERY concurrent
    /// pull sharing this lane. Without a bound, an upstream that stops reading
    /// wedges issuance for the whole lane forever, and no error is ever raised,
    /// so per-blob failover never fires. When a send exceeds this the method
    /// returns an error, releasing the lock and letting the pull leg fail over
    /// to another provider. A timed-out send is treated exactly like any other
    /// ambiguous send: the voucher stays armed and `settlement` settles high.
    send_deadline: Duration,
}

/// Default ceiling on one voucher send (see [`PoolLedger::send_deadline`]). A
/// voucher or preimage is a tiny frame on a low-volume stream, so a send that
/// takes this long means the upstream has stopped acknowledging at the
/// transport level, not that it is merely slow. Generous enough never to fire on
/// a healthy-but-loaded peer, short enough to bound the wedge into a failover.
pub(crate) const VOUCHER_SEND_DEADLINE: Duration = Duration::from_secs(20);

impl PoolLedger {
    /// Build a ledger seeded from the lane's persisted cumulative state (the
    /// last voucher issued on earlier streams/invocations). Pass
    /// `Cumulative::default()` for a brand-new lane, and for the unpaid legs —
    /// the node's own-origin `BackendSource` quotes rate 0, so it never prices,
    /// signs, or meters anything.
    ///
    /// The cumulative is the only seed there is. A chain draws its own secret at
    /// open and is never resumed across a restart, so this constructor takes no
    /// lane identity and no key material: what the ledger inherits from a
    /// previous process is the money it owes, never the chain it owed it under.
    #[must_use]
    pub fn new(seed: Cumulative) -> Self {
        Self {
            issuance: Mutex::new(()),
            pipeline: std::sync::Mutex::new(Pipeline {
                committed: seed,
                prev: None,
                armed: None,
                accrued: Cumulative::default(),
                prev_accrued: Cumulative::default(),
                last_proof: None,
            }),
            epoch: std::sync::Mutex::new(None),
            retired: std::sync::Mutex::new(Displaced::Nothing),
            send_deadline: VOUCHER_SEND_DEADLINE,
        }
    }

    /// Override the per-send voucher-send deadline. Used by tests to exercise
    /// the timeout without waiting the production ceiling; production ledgers
    /// keep `VOUCHER_SEND_DEADLINE`.
    #[must_use]
    pub const fn with_send_deadline(mut self, send_deadline: Duration) -> Self {
        self.send_deadline = send_deadline;
        self
    }

    /// Run one voucher send under [`PoolLedger::send_deadline`]. On timeout it
    /// returns an error rather than blocking the issuance lock forever, so a
    /// stalled upstream becomes a bounded per-blob failover instead of a
    /// lane-wide wedge. The caller treats the error like any send failure: the
    /// armed voucher is left in place and `committed` does not advance.
    async fn under_deadline<Fut>(&self, send: Fut) -> anyhow::Result<()>
    where
        Fut: Future<Output = anyhow::Result<()>>,
    {
        match tokio::time::timeout(self.send_deadline, send).await {
            Ok(result) => result,
            Err(_) => Err(anyhow::anyhow!(
                "voucher send exceeded {:?}; failing the lane to trigger failover",
                self.send_deadline
            )),
        }
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
    /// succeeded (implicit acceptance), so it is exactly what a *completed* pull
    /// persists.
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
        let next = next_voucher(&frontier, delta_bytes, rate_per_mb);
        let commit = self.commit_epoch(epoch, rate_per_mb);
        self.pipeline().armed = Some(next);
        // Send. On ANY error the voucher stays armed and the anchor does not
        // advance: a send failure is as ambiguous as a drop, so `settlement`
        // settles high. A rejection is NOT an issuance outcome — it arrives later
        // as a `StreamError` message and is disarmed via `resolve_reject`.
        self.under_deadline(exchange(next, commit)).await?;
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
            pipeline.last_proof = Some(LastProof::Voucher {
                amount: next.amount,
            });
        }
        Ok(next)
    }

    /// Re-state the lane's signed anchor under the live root, so a stream that
    /// has not carried a voucher for this chain can place its reveals (ADR 003
    /// §Concurrent Streams, Rule 1).
    ///
    /// This is deliberately NOT [`Self::issue`] with a zero delta. Every voucher
    /// the ledger issues builds on the settlement frontier, which folds whatever
    /// the chain has accrued — and a voucher that folds must also roll. A
    /// zero-delta re-anchor issued that way would therefore retire the very chain
    /// it meant to join: it strands every sibling stream's in-flight reveals under
    /// the retired root, where they credit nothing, and it buys a fresh signature
    /// plus a 256-keccak ladder for a message whose whole job is to be free.
    ///
    /// So this re-states `committed` exactly — the anchor the node already holds —
    /// and folds nothing. The node reads it as already-satisfied, takes no new
    /// money from it, and anchors the receiving stream to the root it names.
    /// Nothing is armed, no watermark moves, and `last_proof` is untouched: the
    /// voucher claims nothing, so there is nothing a later rejection could rewind
    /// and nothing an ambiguous send could strand.
    ///
    /// Returns the root the stream is now anchored to, or `None` when the lane
    /// meters no chain — there is nothing to anchor to, and nothing is sent.
    ///
    /// Holds the issuance lock across read → send, so the root it states is the
    /// root that was live when it was sent: a sibling rolling concurrently either
    /// precedes this voucher entirely or follows it.
    pub async fn reanchor<F, Fut>(&self, exchange: F) -> anyhow::Result<Option<B256>>
    where
        F: FnOnce(Cumulative, ChainCommit) -> Fut,
        Fut: Future<Output = anyhow::Result<()>>,
    {
        let _issuing = self.issuance.lock().await;
        let Some(commit) = self.epoch().as_ref().map(|live| ChainCommit {
            chain_root: live.root(),
            chunk_price: live.chunk_price,
        }) else {
            return Ok(None);
        };
        let anchor = self.pipeline().committed;
        self.under_deadline(exchange(anchor, commit)).await?;
        Ok(Some(commit.chain_root))
    }

    /// Apply an [`EpochAction`] and report what the resulting voucher commits.
    ///
    /// Callers hold the issuance lock, which is what makes "read the epoch,
    /// maybe replace it, report it" one indivisible step against the concurrent
    /// streams sharing this lane.
    fn commit_epoch(&self, action: EpochAction, rate_per_mb: u64) -> ChainCommit {
        let mut slot = self.epoch();
        // What this voucher puts in the chain slot, or `None` to leave it alone.
        // A lane holding no chain reports `ChainCommit::SEALED` either way, which
        // is what makes the sealed shape something the ledger arrives at rather
        // than something a caller asks for.
        let replacement = match action {
            EpochAction::Keep => None,
            EpochAction::Open if slot.is_some() => None,
            EpochAction::Open | EpochAction::Roll => Some(Some(open_epoch(rate_per_mb))),
        };
        // Record what this voucher displaced — including "nothing", so a rejected
        // voucher that left the chain alone does not restore some earlier one.
        *self.retired() = match replacement {
            Some(next) => Displaced::Replaced(std::mem::replace(&mut *slot, next)),
            None => Displaced::Nothing,
        };
        slot.as_ref()
            .map_or(ChainCommit::SEALED, |live| ChainCommit {
                chain_root: live.root(),
                chunk_price: live.chunk_price,
            })
    }

    /// Lock the epoch slot, recovering the inner value on poison — same reason
    /// as [`Self::pipeline`].
    fn epoch(&self) -> std::sync::MutexGuard<'_, Option<ChainEpoch>> {
        self.epoch
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Lock the displaced-chain slot — same poison recovery as [`Self::epoch`],
    /// and always taken after it.
    fn retired(&self) -> std::sync::MutexGuard<'_, Displaced> {
        self.retired
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

    /// The root of the live chain, or `None` on a lane metering nothing. A stream
    /// compares this against the chain it last anchored itself to.
    #[must_use]
    pub fn chain_root(&self) -> Option<B256> {
        self.epoch().as_ref().map(ChainEpoch::root)
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
                    chain_root: live.root(),
                },
                live.chunk_price,
            )
        };

        self.under_deadline(exchange(released)).await?;

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
            pipeline.last_proof = Some(LastProof::Reveal {
                chain_root: released.chain_root,
                index: released.index,
                chunk_price: price,
            });
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
        {
            let mut pipeline = self.pipeline();
            if cum.amount <= pipeline.committed.plus(pipeline.accrued).amount {
                return false;
            }
            pipeline.committed = cum;
            pipeline.accrued = Cumulative::default();
            pipeline.prev = None;
            pipeline.prev_accrued = Cumulative::default();
            pipeline.armed = None;
            pipeline.last_proof = None;
        }
        // Retire the live chain, for the same reason a folding voucher must roll:
        // `cum` came from a bundle and therefore already folds that bundle's
        // `verified_index` into its amount. Keeping the epoch would leave the next
        // voucher re-committing a root whose frontier the new anchor has absorbed,
        // and the node would count those chunks twice — once in the amount, once
        // again under the root it never retired. The next `EpochAction::Open`
        // draws a fresh chain against the healed anchor.
        //
        // Sequenced after the pipeline borrow ends rather than nested inside it:
        // the two guards are independent and this keeps them from ever being held
        // together, which is what makes the no-deadlock argument in the field docs
        // hold without reasoning about order.
        *self.epoch() = None;
        *self.retired() = Displaced::Nothing;
        true
    }

    /// Resolve `rejected` — the proof this stream just read a `VoucherRejected`
    /// for — as explicitly refused, undoing exactly what that proof claimed.
    /// Called by the receive loop. A rejection is the upstream declaring it never
    /// took the proof, so — unlike an ambiguous failure — it must not be settled
    /// optimistically (that would inflate our cumulative for bytes the upstream
    /// refused to be paid for).
    ///
    /// Returns `false` if there was nothing to rewind: a spurious rejection, or
    /// one for a proof the lane has already moved past.
    ///
    /// # Why the caller names the proof
    ///
    /// The wire rejection names nothing, and a lane emits two kinds of proof that
    /// rewind differently. A refused VOUCHER un-commits the anchor. A refused
    /// REVEAL must not: the anchor it extends was accepted, and the last voucher
    /// may be many chunks back. Rewinding `committed → prev` on a refused reveal
    /// un-commits a voucher the node is still holding, and the payer's anchor then
    /// sits permanently below the node's — every voucher it signs afterwards
    /// regresses, and a re-anchor states a cumulative the node passed long ago.
    ///
    /// Reading the KIND off the lane is not enough, because the lane is shared.
    /// A rejection is read on the stream that earned it, and a sibling stream
    /// issuing in the meantime replaces what the lane last did: a reveal refused
    /// on stream A, arriving after stream B's rollover was accepted, would find a
    /// Voucher in the slot and un-commit B's ACCEPTED anchor — the exact
    /// permanent divergence the kind-split exists to prevent. So the stream hands
    /// back the proof it sent, by name, and the rewind happens only while that
    /// proof is still the last thing the lane did.
    ///
    /// # Why a stale rejection rewinds nothing
    ///
    /// It is not merely the safe answer, it is the right one. For the lane to
    /// have moved on, a later proof must have been ACCEPTED — the node keeps
    /// delivering, and continued delivery is acceptance. A later voucher folded
    /// the refused reveal's accrual into a signed amount the node took, and a
    /// later reveal proved a depth that pays for every chunk below it. Either
    /// way the money the rejection would claw back is money the node has since
    /// been granted by a proof it did not refuse. Taking it back would put the
    /// payer's anchor below the node's, which is the failure this whole method
    /// is shaped to avoid.
    ///
    /// A rejected reveal gives back exactly one chunk of accrual AND the index it
    /// released. Both, because the index is the accounting: a claim is
    /// `anchor + index × chunk_price`, so a payer that gave back the money but let
    /// the next reveal go one deeper would be charged for the depth it skipped —
    /// the node prices the gap, the payer never accrued it, and the two diverge by
    /// exactly one chunk from then on. Re-releasing the rewound depth is safe: the
    /// node refused it, so it holds no preimage at that index.
    pub fn resolve_reject(&self, rejected: StreamProof) -> bool {
        let mut pipeline = self.pipeline();
        match (rejected, pipeline.last_proof) {
            (
                StreamProof::Reveal { chain_root, index },
                Some(LastProof::Reveal {
                    chain_root: last_root,
                    index: last_index,
                    chunk_price,
                }),
            ) if chain_root == last_root && index == last_index => {
                pipeline.last_proof = None;
                pipeline.accrued.amount = pipeline.accrued.amount.saturating_sub(chunk_price);
                pipeline.accrued.bytes = pipeline
                    .accrued
                    .bytes
                    .saturating_sub(U256::from(CHUNK_BYTES));
                drop(pipeline);
                // Give the index back too — see the doc above. Sequenced after the
                // pipeline guard is released so the two locks are never held
                // together, as in `reseed`. Guarded on the root as well: a sibling
                // that rolled between the release and this rewind left a different
                // chain in the slot, whose index this reveal never advanced.
                if let Some(live) = self.epoch().as_mut()
                    && live.root() == chain_root
                {
                    live.released = live.released.saturating_sub(1);
                }
                true
            }
            (
                StreamProof::Voucher { amount },
                Some(LastProof::Voucher {
                    amount: last_amount,
                }),
            ) if amount == last_amount => {
                pipeline.last_proof = None;
                // The voucher went out and committed, so it is not also armed —
                // a successful `issue` disarms. Clear it only if this rejection
                // names the armed voucher instead, which is the ambiguous send
                // the upstream has now explicitly refused.
                let Some(prev) = pipeline.prev.take() else {
                    return false;
                };
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
                drop(pipeline);
                // Put back the chain this voucher displaced. A rejected
                // rollover already swapped the chain in — the voucher had to
                // carry the new root to be signed — so leaving it would have
                // the payer metering a chain the node refused, whose reveals
                // fold nothing on a lane still tracking the old root.
                let mut epoch = self.epoch();
                if let Displaced::Replaced(chain) =
                    std::mem::replace(&mut *self.retired(), Displaced::Nothing)
                {
                    *epoch = chain;
                }
                true
            }
            // The refused voucher never committed: its send was ambiguous, so the
            // anchor never advanced and disarming IS the whole rewind.
            (StreamProof::Voucher { amount }, _)
                if pipeline.armed.is_some_and(|armed| armed.amount == amount) =>
            {
                pipeline.armed = None;
                true
            }
            _ => false,
        }
    }
}

/// Draw the next chain, priced at the node's quoted rate.
///
/// Each chain gets its own 32-byte secret from the OS, so no two chains this
/// payer opens — across providers, pools, its own sibling signers, or the
/// successive chains of one lane — share a root. That is the whole defence
/// against the cross-lane preimage spend, and it is payer-side by design: reuse
/// costs the payer and pays the node, so no node-side rule would protect anyone
/// who chose to run without it (ADR 003 §One chain per lane).
fn open_epoch(rate_per_mb: u64) -> ChainEpoch {
    ChainEpoch::open(U256::from(rate_per_mb))
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
        let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
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
        let ledger = PoolLedger::new(Cumulative::default());
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

    /// A send that stalls past the lane's deadline fails the issue with an error
    /// — releasing the issuance lock so the pull leg fails over — rather than
    /// blocking every concurrent pull on the lane forever. The stalled voucher is
    /// treated as any ambiguous send: it stays armed and `settlement` settles
    /// high, while `committed` does not advance.
    #[tokio::test]
    async fn a_send_past_the_deadline_errors_instead_of_wedging() -> anyhow::Result<()> {
        let ledger =
            PoolLedger::new(Cumulative::default()).with_send_deadline(Duration::from_millis(20));
        let result = ledger
            .issue(100, 10, EpochAction::Keep, |_next, _chain| {
                // Upstream stopped reading: this send never completes.
                std::future::pending::<anyhow::Result<()>>()
            })
            .await;
        assert!(
            result.is_err(),
            "a send past the deadline must surface an error, not hang"
        );
        assert_eq!(
            ledger.committed(),
            Cumulative::default(),
            "a timed-out send never advances the committed watermark"
        );
        assert_eq!(
            ledger.settlement().bytes,
            U256::from(100u64),
            "an ambiguous timed-out send settles high on the armed voucher"
        );

        // The lock is free again: a subsequent issue on the same ledger proceeds.
        ledger
            .issue(100, 10, EpochAction::Keep, |_next, _chain| async { Ok(()) })
            .await?;
        Ok(())
    }

    /// The window this exists to close (#1122): a pull dropped inside the send
    /// leaves the upstream possibly holding a voucher we have no committed record
    /// of. Settle low and the deposit is stranded; settle high and it is honoured.
    #[tokio::test]
    async fn a_pull_dropped_inside_the_send_settles_at_the_voucher_it_sent() {
        let ledger = PoolLedger::new(Cumulative::default());
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
        let ledger = PoolLedger::new(Cumulative::default());
        // Issue + successful send: committed advances to the voucher.
        let sent = ledger
            .issue(100, 10, EpochAction::Keep, |_next, _chain| async { Ok(()) })
            .await?;
        assert_eq!(ledger.committed().bytes, U256::from(100u64));
        // The upstream rejects it (arrived as a mid-stream VoucherRejected).
        assert!(
            ledger.resolve_reject(voucher_proof(sent)),
            "the committed voucher is rewound"
        );
        assert_eq!(
            ledger.settlement(),
            Cumulative::default(),
            "an explicitly rejected voucher must not advance what we persist"
        );
        Ok(())
    }

    /// A ledger for the metered tests below. Identical to any other — a chain
    /// draws its own secret, so metering needs no identity and no key material.
    fn metered_ledger(seed: Cumulative) -> PoolLedger {
        PoolLedger::new(seed)
    }

    /// Name the voucher a successful `issue` just put on the wire, the way the
    /// issuing stream does.
    fn voucher_proof(sent: Cumulative) -> StreamProof {
        StreamProof::Voucher {
            amount: sent.amount,
        }
    }

    /// Name the reveal a successful `meter` just put on the wire, the way the
    /// releasing stream does.
    fn reveal_proof(metered: Metered) -> anyhow::Result<StreamProof> {
        match metered {
            Metered::Released(released) => Ok(StreamProof::Reveal {
                chain_root: released.chain_root,
                index: released.index,
            }),
            Metered::Exhausted => anyhow::bail!("the epoch was exhausted; nothing was released"),
        }
    }

    /// A rejected REVEAL must not touch the signed anchor.
    ///
    /// The wire rejection names no proof, and the old one-step `committed → prev`
    /// rewind assumed it was always a voucher. Under the chain it often is not: a
    /// refused reveal would un-commit a voucher the node ACCEPTED, dropping the
    /// payer's anchor permanently below the node's — after which every voucher it
    /// signs regresses and every re-anchor states a cumulative the node passed
    /// long ago.
    #[tokio::test]
    async fn a_rejected_reveal_rewinds_the_chunk_not_the_anchor() -> anyhow::Result<()> {
        let ledger = metered_ledger(Cumulative::default());
        // A voucher the node ACCEPTS, then two reveals on top of it.
        ledger
            .issue(1_000, 10, EpochAction::Open, |_n, _c| async { Ok(()) })
            .await?;
        let anchor = ledger.committed();
        ledger.meter(|_r| async { Ok(()) }).await?;
        let second = ledger.meter(|_r| async { Ok(()) }).await?;
        let two_reveals = ledger.committed();
        assert!(two_reveals.amount > anchor.amount);

        assert!(
            ledger.resolve_reject(reveal_proof(second)?),
            "a released reveal is rewindable"
        );
        assert_eq!(
            ledger.committed().amount,
            two_reveals.amount - U256::from(10u64),
            "exactly one chunk comes off"
        );

        // And the anchor the next voucher builds on is untouched: re-anchoring
        // still states what the node accepted, not something behind it.
        let stated = std::sync::Mutex::new(None);
        ledger
            .reanchor(|cum, _chain| {
                *stated
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(cum);
                async { Ok(()) }
            })
            .await?;
        assert_eq!(
            stated
                .into_inner()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            Some(anchor),
            "the signed anchor must survive a rejected reveal"
        );
        Ok(())
    }

    /// The index rewinds with the money, because the index IS the money: a claim
    /// is `anchor + index × chunk_price`. A payer that gave back the chunk but let
    /// the next reveal go one deeper would be charged for the depth it skipped,
    /// and the two sides would diverge by exactly one chunk from then on.
    #[tokio::test]
    async fn a_rejected_reveal_gives_back_its_index_too() -> anyhow::Result<()> {
        let ledger = metered_ledger(Cumulative::default());
        ledger
            .issue(0, 10, EpochAction::Open, |_n, _c| async { Ok(()) })
            .await?;
        let mut last = None;
        for _ in 0..3u8 {
            last = Some(ledger.meter(|_r| async { Ok(()) }).await?);
        }
        let third = last.ok_or_else(|| anyhow::anyhow!("no reveal was released"))?;
        assert!(ledger.resolve_reject(reveal_proof(third)?));

        let next = std::sync::Mutex::new(None);
        ledger
            .meter(|r| {
                *next
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(r.index);
                async { Ok(()) }
            })
            .await?;
        assert_eq!(
            next.into_inner()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            Some(3),
            "the refused depth is released again, not skipped"
        );
        Ok(())
    }

    /// A rejection is rewound against the proof it was FOR, not against whatever
    /// the lane did last.
    ///
    /// One `PoolLedger` is shared by every stream on a lane (`stream_fetch_shared`,
    /// the node's `BuyerLedgers`), and a rejection is read on the stream that
    /// earned it — so the two can interleave: stream A releases a reveal, stream B
    /// rolls and its voucher is ACCEPTED, and only then does A read the rejection
    /// for its reveal. Keyed to the lane's last proof, that rejection would find a
    /// voucher in the slot and un-commit B's accepted anchor, leaving the payer
    /// permanently below the node's watermark — every later voucher regresses, and
    /// the accrual B folded is re-added on top of an anchor that never advanced.
    #[tokio::test]
    async fn a_stale_rejection_cannot_uncommit_a_siblings_accepted_voucher() -> anyhow::Result<()> {
        let ledger = metered_ledger(Cumulative::default());
        ledger
            .issue(0, 10, EpochAction::Open, |_n, _c| async { Ok(()) })
            .await?;
        // Stream A releases a reveal.
        let a_reveal = ledger.meter(|_r| async { Ok(()) }).await?;
        // Stream B rolls; the node accepts the rollover (continued delivery IS
        // acceptance), so the lane's anchor is now B's.
        ledger
            .issue(0, 10, EpochAction::Roll, |_n, _c| async { Ok(()) })
            .await?;
        let accepted = ledger.committed();
        let live = ledger.chain_root();

        // Only now does A read the rejection for its reveal.
        assert!(
            !ledger.resolve_reject(reveal_proof(a_reveal)?),
            "a proof the lane has moved past rewinds nothing"
        );
        assert_eq!(
            ledger.committed(),
            accepted,
            "B's accepted anchor must survive A's stale rejection"
        );
        assert_eq!(
            ledger.chain_root(),
            live,
            "and the chain B opened must stay live"
        );
        Ok(())
    }

    /// A rejected ROLLOVER puts the chain back.
    ///
    /// The chain is swapped in before the send — the voucher has to carry the new
    /// root to be signed at all — so a refusal would otherwise leave the payer
    /// metering a chain the node never adopted: its reveals name a root the lane
    /// does not track and fold nothing, and its next re-anchor states a root the
    /// node will not accept over a frontier it has already proved.
    #[tokio::test]
    async fn a_rejected_rollover_puts_the_chain_back() -> anyhow::Result<()> {
        let ledger = metered_ledger(Cumulative::default());
        ledger
            .issue(0, 10, EpochAction::Open, |_n, _c| async { Ok(()) })
            .await?;
        let live = ledger.chain_root();
        ledger.meter(|_r| async { Ok(()) }).await?;

        let rolled = ledger
            .issue(0, 10, EpochAction::Roll, |_n, _c| async { Ok(()) })
            .await?;
        assert_ne!(ledger.chain_root(), live, "the roll drew a fresh chain");

        assert!(ledger.resolve_reject(voucher_proof(rolled)));
        assert_eq!(
            ledger.chain_root(),
            live,
            "a refused rollover leaves the lane on the chain the node still meters"
        );
        Ok(())
    }

    /// A rejected voucher that displaced NO chain must not resurrect an earlier
    /// one. The displaced slot records "nothing" as distinctly as it records a
    /// chain, or a refused residual would hand the lane back a retired root.
    #[tokio::test]
    async fn a_rejected_non_rolling_voucher_leaves_the_chain_alone() -> anyhow::Result<()> {
        let ledger = metered_ledger(Cumulative::default());
        ledger
            .issue(0, 10, EpochAction::Open, |_n, _c| async { Ok(()) })
            .await?;
        ledger
            .issue(0, 10, EpochAction::Roll, |_n, _c| async { Ok(()) })
            .await?;
        let live = ledger.chain_root();
        // A residual voucher: `Keep`, with nothing accrued, so it displaces nothing.
        let residual = ledger
            .issue(100, 10, EpochAction::Keep, |_n, _c| async { Ok(()) })
            .await?;
        assert!(ledger.resolve_reject(voucher_proof(residual)));
        assert_eq!(
            ledger.chain_root(),
            live,
            "a voucher that displaced no chain restores no chain"
        );
        Ok(())
    }

    /// The bundle decodes to the lane's FULL claim, not just its signed anchor.
    ///
    /// A node that rejects mid-chain reports an anchor plus the frontier its chain
    /// has proved on top. A signer that re-seeded from the anchor alone would open
    /// a fresh root beside a frontier the node is still metering, and every reveal
    /// after that would fold nothing — the resume would never converge. The fold
    /// is the node's documented side of the bargain (ADR 005 §Watermark bundle).
    #[test]
    fn a_bundle_decodes_with_its_proved_frontier_folded_in() {
        let bundle = WatermarkBundle {
            chain_root: [0x9Au8; 32],
            verified_index: 3,
            tip: [0x9Bu8; 32],
            chunk_price: 10,
            amount: 50,
            bytes_delivered: 5_000,
            last_signature: vec![0xCDu8; 65],
        };
        let cum = Cumulative::from(&bundle);
        assert_eq!(
            cum.amount,
            U256::from(80u64),
            "50 anchored + 3 chunks proved at 10 each"
        );
        assert_eq!(
            cum.bytes,
            U256::from(5_000u64) + U256::from(3u64) * U256::from(CHUNK_BYTES),
            "the byte axis folds the same three chunks"
        );
    }

    /// Re-seeding retires the live chain, because the cumulative it installs has
    /// already folded that chain's frontier. Keeping it would leave the next
    /// voucher re-committing a root the new anchor absorbed, and the node would
    /// count those chunks twice — the same double-count the fold-must-roll rule
    /// prevents on the issue path.
    #[tokio::test]
    async fn reseeding_retires_the_chain_whose_frontier_it_folded() -> anyhow::Result<()> {
        let ledger = metered_ledger(Cumulative::default());
        ledger
            .issue(0, 10, EpochAction::Open, |_n, _c| async { Ok(()) })
            .await?;
        let opened = ledger
            .chain_root()
            .ok_or_else(|| anyhow::anyhow!("Open draws a chain"))?;

        let bundle = WatermarkBundle {
            chain_root: [0x9Au8; 32],
            verified_index: 2,
            tip: [0x9Bu8; 32],
            chunk_price: 10,
            amount: 500,
            bytes_delivered: 5_000,
            last_signature: vec![0xCDu8; 65],
        };
        assert!(ledger.reseed(Cumulative::from(&bundle)));
        assert_eq!(
            ledger.chain_root(),
            None,
            "the folded chain must not survive the reseed"
        );

        // And the chain drawn next is a genuinely new one: the fold moved the
        // anchor, and the anchor is what seeds the root.
        ledger
            .issue(0, 10, EpochAction::Open, |_n, _c| async { Ok(()) })
            .await?;
        assert_ne!(ledger.chain_root(), Some(opened));
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
        let ledger = PoolLedger::new(Cumulative {
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
        let ledger = PoolLedger::new(committed);
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
            let ledger = PoolLedger::new(Cumulative::default());
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
        let ledger = PoolLedger::new(Cumulative::default());
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
        let ledger = PoolLedger::new(Cumulative::default());
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
