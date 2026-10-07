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

    /// Componentwise saturating difference — what is left of an accrual once a
    /// voucher has folded part of it.
    #[must_use]
    const fn minus(self, other: Self) -> Self {
        Self {
            bytes: self.bytes.saturating_sub(other.bytes),
            amount: self.amount.saturating_sub(other.amount),
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
    /// payer must roll to a fresh root. The increment is lazy: `released` sits at
    /// `MAX_CHAIN_LENGTH` (`u8::MAX`) once the ladder is spent, where an eager
    /// `+ 1` overflows before the guard can refuse it.
    fn next_index(&self) -> Option<u8> {
        (self.released < MAX_CHAIN_LENGTH).then(|| self.released + 1)
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
    /// Advanced optimistically on each successful [`PoolLedger::issue`] and on a
    /// [`PoolLedger::confirm_armed_stamped`], rewound one step by
    /// [`PoolLedger::resolve_reject`], and hard-set by [`PoolLedger::reseed`].
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
    /// [`PoolLedger::issue`], a matching [`PoolLedger::confirm_armed_stamped`] or
    /// [`PoolLedger::resolve_reject`], or a [`PoolLedger::reseed`].
    armed: Option<Armed>,
    /// How many times a rebase has moved this ledger to a node's watermark
    /// behind it on `amount`. Every voucher is stamped with the generation it
    /// was signed under (`StreamProof::Voucher`), so an `Underpaid` or
    /// `BytesRegression` rejection of a voucher signed before the latest
    /// rebase is recognisably stale.
    generation: u64,
    /// The watermark the latest rebase moved to, until the
    /// caller records it ([`PoolLedger::take_unsaved_rebase`]). A monotone
    /// advance refuses a lower watermark, so the next persist overwrites the
    /// lane record with this anchor once, then advances from it.
    unsaved_rebase: Option<Cumulative>,
    /// The node watermark the latest heal acted on: the bundle a
    /// [`PoolLedger::reseed`], [`PoolLedger::rebase`], or
    /// [`PoolLedger::retire_unadopted_chain`] took. Sibling streams on one lane
    /// take the same rejection with the same bundle. The first heals the lane,
    /// and this lets the rest see that it did, so they leave alone the chain
    /// the healed lane opened next.
    healed_from: Option<Cumulative>,
}

impl Pipeline {
    /// Overwrite the committed watermark with `cum` and clear everything that
    /// was relative to the old one: accrual, rewind, and the armed voucher. The
    /// node has just told us its authoritative watermark, so none of it holds.
    fn overwrite(&mut self, cum: Cumulative) {
        self.committed = cum;
        self.accrued = Cumulative::default();
        self.prev = None;
        self.prev_accrued = Cumulative::default();
        self.armed = None;
        self.last_proof = None;
    }

    /// What the lane owes, reported high: the anchor plus the accrual, or the
    /// armed voucher when it claims more. See [`PoolLedger::settlement`].
    fn settlement(&self) -> Cumulative {
        let owed = self.committed.plus(self.accrued);
        match self.armed {
            Some(armed) if armed.voucher.amount > owed.amount => armed.voucher,
            _ => owed,
        }
    }
}

/// A voucher whose send did not confirm, together with the accrual it folded.
#[derive(Debug, Clone, Copy)]
struct Armed {
    /// The cumulative the voucher signs.
    voucher: Cumulative,
    /// The accrual outstanding when the voucher was armed — what committing it
    /// takes out of `accrued`. Reveals released after the arming stay in
    /// `accrued`, because the voucher never signed them.
    folded: Cumulative,
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
pub(crate) enum StreamProof {
    /// A signed voucher claiming this cumulative amount.
    Voucher {
        /// The `amount` the voucher was signed over.
        amount: U256,
        /// The ledger generation it was signed under ([`PoolLedger::generation`]).
        generation: u64,
    },
    /// A preimage released at this depth on this chain.
    Reveal {
        /// The chain the reveal extends, named by its root.
        chain_root: B256,
        /// The depth released.
        index: u8,
    },
}

/// What [`PoolLedger::rebase`] did with a node's watermark.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Rebase {
    /// The committed watermark moved to the node's, from `from`.
    Rebased {
        /// The committed-plus-accrued watermark the ledger held before.
        from: Cumulative,
    },
    /// The rejected voucher was signed before the latest rebase, so its
    /// rejection says nothing about the healed anchor. Nothing moved.
    Stale,
    /// The watermark is not one this rebase moves to (see
    /// [`PoolLedger::rebase`] and the `BytesRegression` heal). Nothing moved.
    Refused,
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
    /// use. Re-asserting a root the node already holds is free, because a
    /// metering voucher at or below the watermark is already-satisfied and pays
    /// no whole chunk, so the node never rejects it,
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
    /// The lane meters a chain other than the one the stream is anchored to: a
    /// sibling rolled the lane after the stream anchored. Nothing was sent; the
    /// caller re-anchors the stream and meters again.
    Moved,
}

/// One lane's live voucher ledger, shared by every concurrent stream that draws
/// on it. Voucher *issuance* is serialized through the async `issuance` mutex —
/// held across compute → sign → send so vouchers reach the node in strict
/// cumulative order — and released the instant the send returns, because there
/// is no ack to wait for (implicit acceptance, ADR 005). The committed watermark
/// advances the moment a send succeeds; a mid-stream `VoucherRejected` rewinds
/// it through `resolve_reject`.
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
                generation: 0,
                unsaved_rebase: None,
                healed_from: None,
            }),
            epoch: std::sync::Mutex::new(None),
            retired: std::sync::Mutex::new(Displaced::Nothing),
            send_deadline: VOUCHER_SEND_DEADLINE,
        }
    }

    #[cfg(test)]
    /// Override the per-send voucher-send deadline. Used by tests to exercise
    /// the timeout without waiting the production ceiling; production ledgers
    /// keep `VOUCHER_SEND_DEADLINE`.
    #[must_use]
    pub(crate) const fn with_send_deadline(mut self, send_deadline: Duration) -> Self {
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
    /// succeeded (implicit acceptance) or that a terminal `StreamEnd` confirmed
    /// (`confirm_armed_stamped`), plus the accrual since, so it is exactly what a
    /// *completed* pull persists.
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
    /// `resolve_reject` clears `armed` and rewinds `committed` — so this
    /// cannot inflate our cumulative for bytes the upstream declined.
    #[must_use]
    pub fn settlement(&self) -> Cumulative {
        self.pipeline().settlement()
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
        self.issue_stamped(delta_bytes, rate_per_mb, epoch, exchange)
            .await
            .map(|(next, _generation)| next)
    }

    /// [`Self::issue`], also returning the ledger generation the voucher was
    /// signed under. Read under the issuance lock, which [`Self::rebase`] also
    /// takes, so the generation cannot move between signing and reporting it.
    ///
    /// # Errors
    ///
    /// As [`Self::issue`].
    pub(crate) async fn issue_stamped<F, Fut>(
        &self,
        delta_bytes: u64,
        rate_per_mb: u64,
        epoch: EpochAction,
        exchange: F,
    ) -> anyhow::Result<(Cumulative, u64)>
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
            (pipeline.settlement(), pipeline.accrued)
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
        self.pipeline().armed = Some(Armed {
            voucher: next,
            folded: accrued,
        });
        // Send. On ANY error the voucher stays armed and the anchor does not
        // advance: a send failure is as ambiguous as a drop, so `settlement`
        // settles high. A rejection is NOT an issuance outcome — it arrives later
        // as a `StreamError` message and is disarmed via `resolve_reject`.
        self.under_deadline(exchange(next, commit)).await?;
        // The send succeeded: commit optimistically. The accrual is now folded
        // into the signed anchor, so it resets to zero — and the previous pair
        // is remembered so a later `resolve_reject` can un-commit exactly this
        // voucher, both halves together.
        let generation = {
            let mut pipeline = self.pipeline();
            pipeline.prev = Some(pipeline.committed);
            pipeline.prev_accrued = pipeline.accrued;
            pipeline.committed = next;
            pipeline.accrued = Cumulative::default();
            pipeline.armed = None;
            pipeline.last_proof = Some(LastProof::Voucher {
                amount: next.amount,
            });
            pipeline.generation
        };
        Ok((next, generation))
    }

    #[cfg(test)]
    /// [`Self::confirm_armed_stamped`] without the generation.
    pub(crate) async fn confirm_armed(&self, amount: U256) -> Option<Cumulative> {
        self.confirm_armed_stamped(amount)
            .await
            .map(|(confirmed, _generation)| confirmed)
    }

    /// Commit the armed voucher signing `amount` as if its send had confirmed.
    ///
    /// For when the upstream's terminal `StreamEnd` arrives after this stream's
    /// voucher write failed. An honest node ends a stream only once every interval
    /// — the closing voucher included — is credited, so its `StreamEnd` implies it
    /// holds the voucher. Trusting a dishonest one costs nothing:
    /// [`Self::settlement`] already reports the armed voucher. The epoch already
    /// rolled when the voucher was armed, so this is the commit a successful
    /// [`Self::issue`] makes, and payment-based completion sees the leg paid
    /// through its end instead of re-pulling and re-billing the tail.
    ///
    /// Two things differ from the in-`issue` commit, because the issuance lock
    /// was released between the failed send and this call:
    ///
    /// - **Only the fold comes out of `accrued`.** A sibling stream on the shared
    ///   lane may have released reveals on the new chain since the voucher was
    ///   armed. The voucher never signed them, so they stay accrued.
    /// - **Only the named voucher commits.** `armed` belongs to the lane, not the
    ///   stream: a sibling's failed voucher may have been armed on top of this
    ///   one. A `StreamEnd` read on this stream says nothing about that voucher.
    ///
    /// Returns the confirmed cumulative and the ledger generation the voucher was
    /// signed under, or `None` when the armed voucher does not sign `amount`: a later issue, reject, or reseed has already cleared it, or
    /// a sibling's voucher replaced it. The ledger is then left untouched, and
    /// [`Self::settlement`] still settles high on whatever is armed.
    ///
    /// A [`Self::rebase`] clears the armed voucher, so one that is still armed
    /// here was signed under the current generation.
    pub(crate) async fn confirm_armed_stamped(&self, amount: U256) -> Option<(Cumulative, u64)> {
        let _issuing = self.issuance.lock().await;
        let mut pipeline = self.pipeline();
        let armed = pipeline
            .armed
            .filter(|armed| armed.voucher.amount == amount)?;
        pipeline.armed = None;
        pipeline.prev = Some(pipeline.committed);
        pipeline.prev_accrued = armed.folded;
        pipeline.committed = armed.voucher;
        pipeline.accrued = pipeline.accrued.minus(armed.folded);
        pipeline.last_proof = Some(LastProof::Voucher { amount });
        Some((armed.voucher, pipeline.generation))
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
    pub(crate) async fn reanchor<F, Fut>(&self, exchange: F) -> anyhow::Result<Option<B256>>
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
    /// `anchored` is the root the calling stream last carried a voucher for.
    /// The node places a reveal against that root, so a reveal from any other
    /// chain folds nothing there. The check runs under the issuance lock, which
    /// is the only place the live root cannot move: a sibling can roll the lane
    /// between the stream's anchor and this tick. Returns [`Metered::Moved`]
    /// **without sending anything** when the live root differs.
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
    pub async fn meter<F, Fut>(
        &self,
        anchored: Option<B256>,
        exchange: F,
    ) -> anyhow::Result<Metered>
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
            if anchored != Some(live.root()) {
                return Ok(Metered::Moved);
            }
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

    /// Wallet-less self-heal (issue #1481): overwrite the committed watermark to `cum` — typically
    /// [`Cumulative::from`] a [`WatermarkBundle`] the node attached to a watermark-gated rejection
    /// — and clear the rewind + armed state, since the node has just told us its authoritative
    /// watermark. The next [`Self::issue`] builds on `cum`, matching what the node will accept
    /// next.
    ///
    /// This is a hard overwrite, not a monotonic bump: the caller's prior local
    /// state was wrong (a wallet-less client has no reliable on-chain source for
    /// its watermark until settlement), so the bundle — signer-verified by the
    /// node before it was sent, and re-verified against the client's own key by
    /// `resumable_watermark` before it reaches here — is authoritative. Callers
    /// MUST only pass a cumulative sourced from such a bundle.
    ///
    /// Returns `false` — leaving the ledger untouched — if `cum` does not ADVANCE past
    /// [`Self::committed`]'s `amount` (the anchor plus the accrual). Reseeding heals a watermark
    /// that has fallen BEHIND what the node holds; a bundle at or behind `committed` proves
    /// nothing, and applying it would REGRESS the watermark and re-sign a spent amount. Guarded
    /// here rather than only at the call sites because monotonicity is the ledger's invariant to
    /// keep.
    #[must_use]
    pub fn reseed(&self, cum: Cumulative) -> bool {
        {
            let mut pipeline = self.pipeline();
            if cum.amount <= pipeline.committed.plus(pipeline.accrued).amount {
                return false;
            }
            pipeline.overwrite(cum);
            pipeline.healed_from = Some(cum);
        }
        self.retire_chain();
        true
    }

    /// Move the committed watermark DOWN to `cum` — the node's last-accepted
    /// watermark from a [`WatermarkBundle`] on an `Underpaid` rejection.
    ///
    /// An `Underpaid` rejection says this ledger has run AHEAD of the node: it
    /// holds vouchers the node never accepted (a send that committed
    /// optimistically, then persisted across a restart), so every voucher it
    /// signs measures a span the node sees as short, and the lane wedges.
    /// Rebasing to the node's watermark is the only way out.
    ///
    /// Moving down is safe for the payer. The bundle is the node's accepted
    /// state, and the caller admits it only after `resumable_watermark` proves
    /// the anchor against our own signature. Redemption is cumulative and pays
    /// the highest voucher it is shown, so re-signing from a lower anchor can
    /// never make the payer pay more than it has already signed for.
    ///
    /// `proof_generation` is the generation the rejected voucher was signed
    /// under. A voucher signed before the latest rebase draws a stale `Underpaid`
    /// — it measured its span from the anchor this ledger has already left — so
    /// that rejection is [`Rebase::Stale`] and moves nothing. Rebasing on it
    /// would re-sign amounts the node has since accepted from the healed anchor,
    /// with different bytes, which the node refuses as a `BytesRegression`. A
    /// rejection of a voucher from the current generation
    /// is a fresh divergence, and rebases again. `None` means the generation is
    /// unknown, and counts as current.
    ///
    /// Holds the issuance lock, so no voucher is mid-send across the rebase: a
    /// send in flight completes first, and nothing signed from the old anchor
    /// can commit over the new one.
    ///
    /// Returns [`Rebase::Refused`] — leaving the ledger untouched — when `cum`
    /// is ahead of the committed watermark's `amount` (that is
    /// [`Self::reseed`]'s case) or equal to the committed watermark (an echo).
    /// A `cum` equal on `amount` but behind on `bytes` rebases.
    pub async fn rebase(&self, cum: Cumulative, proof_generation: Option<u64>) -> Rebase {
        // A watermark ahead on `amount` is `reseed`'s case, and one equal to
        // ours is an echo that proves nothing. Anything else is the node's
        // accepted state behind ours — including one equal on `amount` but
        // behind on `bytes`, which still makes every span we sign underpay.
        self.rebase_if(cum, proof_generation, |from| {
            cum.amount <= from.amount && cum != from
        })
        .await
    }

    /// Move the committed watermark to `cum` — the node's last-accepted
    /// watermark from a [`WatermarkBundle`] on a `BytesRegression` — when
    /// `cum` is at or behind the ledger on `amount` and ahead of it on
    /// `bytes`.
    ///
    /// That shape says the lane resumed from a lower anchor than the node
    /// holds, and priced its spans from there: its `amount` passed the node's
    /// anchor while its bytes still trailed it. The rebase is safe for the
    /// payer for the reason [`Self::rebase`] gives: `cum` is a voucher the
    /// payer signed.
    ///
    /// The bytes test runs under the issuance lock, beside the generation
    /// test, so a sibling voucher that commits past `cum` while this call
    /// waits makes it [`Rebase::Refused`] instead of moving the ledger below
    /// a voucher the node accepted.
    pub(crate) async fn rebase_ahead_on_bytes(
        &self,
        cum: Cumulative,
        proof_generation: Option<u64>,
    ) -> Rebase {
        self.rebase_if(cum, proof_generation, |from| {
            cum.amount <= from.amount && cum.bytes > from.bytes
        })
        .await
    }

    /// The shared body of [`Self::rebase`] and [`Self::rebase_ahead_on_bytes`]:
    /// under the issuance lock, refuse a stale generation, then move to `cum`
    /// when `admits` accepts the committed-plus-accrued watermark it would
    /// leave.
    async fn rebase_if(
        &self,
        cum: Cumulative,
        proof_generation: Option<u64>,
        admits: impl FnOnce(Cumulative) -> bool,
    ) -> Rebase {
        let _issuing = self.issuance.lock().await;
        let from = {
            let mut pipeline = self.pipeline();
            if proof_generation.is_some_and(|g| g < pipeline.generation) {
                return Rebase::Stale;
            }
            let from = pipeline.committed.plus(pipeline.accrued);
            if !admits(from) {
                return Rebase::Refused;
            }
            pipeline.overwrite(cum);
            pipeline.generation = pipeline.generation.saturating_add(1);
            pipeline.unsaved_rebase = Some(cum);
            pipeline.healed_from = Some(cum);
            from
        };
        self.retire_chain();
        Rebase::Rebased { from }
    }

    /// The generation vouchers are currently signed under: how many times
    /// a rebase has moved this ledger to a node's watermark.
    #[must_use]
    pub fn generation(&self) -> u64 {
        self.pipeline().generation
    }

    /// Take the watermark the latest rebase moved to, if no
    /// persist has recorded it yet. The caller overwrites the lane record with it
    /// once and advances from there; every later persist is a monotone advance
    /// again. Taking it clears it, so a second persist sees `None`.
    #[must_use]
    pub fn take_unsaved_rebase(&self) -> Option<Cumulative> {
        self.pipeline().unsaved_rebase.take()
    }

    /// Retire a live chain the node refused to adopt, after an `UnderFold`
    /// rejection whose `bundle` our ledger already covers on both axes. Returns
    /// the retired root, or `None` when nothing moved.
    ///
    /// A payer that resumes below the node's live claim opens a fresh root, and
    /// the node refuses it `UnderFold`. The rejection lands after the stream has
    /// released reveals under that root, and it rewinds only the last one. So
    /// the ledger can cover the bundle and still meter the refused root. A
    /// reseed does not apply, because the bundle does not advance the ledger. A
    /// retry would re-anchor under the same root and draw the same rejection.
    ///
    /// This folds the pipeline to [`Self::settlement`] and drops the chain, so
    /// the next stream opens a fresh root over the whole fold. A new process
    /// does the same thing: it starts from the persisted settlement with no
    /// chain. The node adopts that voucher, because it folds at least the
    /// bundle's claim. The fold leaves [`Self::settlement`] where it was, so
    /// the persisted watermark advances as usual and needs no
    /// [`Self::take_unsaved_rebase`].
    ///
    /// Nothing moves when a heal has already taken this bundle or a later one.
    /// A sibling stream that took the same rejection first has healed the lane,
    /// and the live chain is then one the healed lane opened since. Nothing
    /// moves on a lane that meters no chain either.
    ///
    /// A chain retired this way may be one the node did adopt. That costs one
    /// more opening voucher and is otherwise safe. The fold is the frontier the
    /// payer reached, so it covers every reveal the node can hold under the
    /// retired root. A sibling anchored to that root opens a fresh one before
    /// its next reveal, as after a reseed.
    ///
    /// Holds the issuance lock, so no proof is mid-send across the fold.
    pub(crate) async fn retire_unadopted_chain(&self, bundle: Cumulative) -> Option<B256> {
        let _issuing = self.issuance.lock().await;
        let root = self.chain_root()?;
        {
            let mut pipeline = self.pipeline();
            if pipeline
                .healed_from
                .is_some_and(|h| h.amount >= bundle.amount && h.bytes >= bundle.bytes)
            {
                return None;
            }
            let settlement = pipeline.settlement();
            pipeline.overwrite(settlement);
            pipeline.healed_from = Some(bundle);
        }
        self.retire_chain();
        Some(root)
    }

    /// Drop the live chain after an overwrite of the committed watermark.
    fn retire_chain(&self) {
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
    pub(crate) fn resolve_reject(&self, rejected: StreamProof) -> bool {
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
                StreamProof::Voucher { amount, .. },
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
            (StreamProof::Voucher { amount, .. }, _)
                if pipeline
                    .armed
                    .is_some_and(|armed| armed.voucher.amount == amount) =>
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
mod tests;
