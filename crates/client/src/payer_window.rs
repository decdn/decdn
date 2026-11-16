//! The payer's credit window (ADR 003 § Credit Window).
//!
//! A node sends a frame only when the lane's confirmed payment covers it, so
//! the payer decides how far its payment runs ahead of delivery. That decision
//! is payer-local policy: [`PayAheadPolicy`] holds its three knobs, and
//! [`PayAheadPolicy::window_bytes`] turns them and one node's track record into
//! a window in bytes.
//!
//! The track record is a [`NodeTrust`], one per node operator address, shared
//! by every lane the process holds at that node. It counts the bytes the payer
//! has verified from the node, the paid bytes it has written off there, and the
//! bytes it currently holds as prepaid credit across all of its lanes. A
//! [`PayerWindows`] registry hands out one [`NodeTrust`] per operator address
//! and keeps the per-run write-off budget.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use alloy::primitives::Address;
use decdn_bao_range::CHUNK_GROUP_BYTES;
use decdn_protocol::client::CHUNK_BYTES;

/// Default [`PayAheadPolicy::cap_micro_usdc`]: the price of 64 MiB at the
/// default node rate of 10 base units per MiB.
pub const DEFAULT_PAY_AHEAD_CAP_MICRO_USDC: u64 = 640;

/// Default [`PayAheadPolicy::divisor`]: the window is half of the verified
/// bytes once that exceeds the floor.
pub const DEFAULT_PAY_AHEAD_DIVISOR: u64 = 2;

/// Default [`PayAheadPolicy::loss_weight`]: one written-off byte cancels twenty
/// verified bytes.
pub const DEFAULT_LOSS_WEIGHT: u64 = 20;

/// Default [`PayAheadPolicy::write_off_budget_micro_usdc`]: four caps. A run
/// that has written off this much stops ramping every node past the floor.
pub const DEFAULT_WRITE_OFF_BUDGET_MICRO_USDC: u64 = 4 * DEFAULT_PAY_AHEAD_CAP_MICRO_USDC;

const BYTES_PER_MIB: u128 = 1 << 20;

/// The smallest window: one chunk plus one chunk group. A preimage pays for a
/// whole chunk, and the bao leaf that completes it can extend one chunk group
/// past the chunk boundary, so a stream always makes progress at the floor.
pub const WINDOW_FLOOR_BYTES: u64 = CHUNK_BYTES + CHUNK_GROUP_BYTES;

/// The payer's three window knobs and its per-run write-off budget. Payer-local
/// configuration: not a wire or governance parameter, and never negotiated.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PayAheadPolicy {
    /// The most USDC (base units) the payer holds as credit at one node, across
    /// every signer. Converted to bytes at the rate the node quotes.
    pub cap_micro_usdc: u64,
    /// The window is `(verified - loss_weight * lost) / divisor` once that
    /// exceeds the floor. A zero divisor reads as one.
    pub divisor: u64,
    /// How many verified bytes one written-off byte cancels.
    pub loss_weight: u64,
    /// The most USDC (base units) this run writes off in total before every
    /// node's window stops ramping past the floor. A zero budget is spent from
    /// the start: every node's window stays at the floor for the whole run,
    /// before any loss.
    pub write_off_budget_micro_usdc: u64,
}

impl PayAheadPolicy {
    /// The documented defaults.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            cap_micro_usdc: DEFAULT_PAY_AHEAD_CAP_MICRO_USDC,
            divisor: DEFAULT_PAY_AHEAD_DIVISOR,
            loss_weight: DEFAULT_LOSS_WEIGHT,
            write_off_budget_micro_usdc: DEFAULT_WRITE_OFF_BUDGET_MICRO_USDC,
        }
    }

    /// The cap in bytes at a node quoting `rate_per_mb` base units per MiB. A
    /// zero rate prices nothing, so the cap is unbounded there.
    #[must_use]
    pub fn cap_bytes(&self, rate_per_mb: u64) -> u64 {
        if rate_per_mb == 0 {
            return u64::MAX;
        }
        let bytes = u128::from(self.cap_micro_usdc) * BYTES_PER_MIB / u128::from(rate_per_mb);
        u64::try_from(bytes).unwrap_or(u64::MAX)
    }

    /// The credit window toward a node, in bytes:
    /// `min(cap, max(floor, (verified - loss_weight * lost) / divisor))`.
    ///
    /// A cap below the floor yields the floor: a stream must always be able to
    /// pay for one chunk and the leaf that completes it.
    #[must_use]
    pub fn window_bytes(&self, verified: u64, lost: u64, rate_per_mb: u64) -> u64 {
        let net = verified.saturating_sub(self.loss_weight.saturating_mul(lost));
        let ramp = net / self.divisor.max(1);
        ramp.max(WINDOW_FLOOR_BYTES)
            .min(self.cap_bytes(rate_per_mb))
            .max(WINDOW_FLOOR_BYTES)
    }
}

impl Default for PayAheadPolicy {
    fn default() -> Self {
        Self::new()
    }
}

/// What one stream pays before it reads on, as `(whole chunks, closing residual
/// bytes)`.
///
/// `window` is the payer's window toward the node, `need` the wire bytes the
/// stream has left to receive, `held` the credit the lane already holds, and
/// `room` how much more credit the lane may take on ([`lane_room`]). The payer
/// keeps the lane's held credit at the window, never past the bytes the stream
/// has left to receive, so no byte of the stream is paid for twice. Credit the
/// lane already holds counts, so a stream on a funded lane pays nothing of its
/// own until the credit runs down.
///
/// Whole chunks go out one preimage each. A residual below one chunk goes out
/// as a closing voucher only when it covers the end of the stream.
#[must_use]
pub(crate) fn due(window: u64, need: u64, held: u64, room: u64) -> (u64, u64) {
    let target = window.min(need);
    let want = target.saturating_sub(held);
    let pay = want.min(room);
    let residual = if target == need && pay == want {
        pay % CHUNK_BYTES
    } else {
        0
    };
    (pay / CHUNK_BYTES, residual)
}

/// How much more credit a lane holding `lane_credit` may take on, given the
/// per-node cap leaves `cap_room`.
///
/// The node sends no frame smaller than one chunk group, so a lane holding
/// less than that gets nothing from the node until it pays. Such a lane may
/// always take on one chunk past the cap, so another lane's idle credit at the
/// node cannot starve it. The test reads the lane's credit, not one stream's
/// view of it, so the cap bounds the exemption to one chunk per lane however
/// many streams the lane opens.
#[must_use]
pub(crate) fn lane_room(cap_room: u64, lane_credit: u64) -> u64 {
    if lane_credit < CHUNK_GROUP_BYTES {
        cap_room.max(CHUNK_BYTES)
    } else {
        cap_room
    }
}

/// [`due`] after a sibling stream spent lane credit this stream counted on.
///
/// The node cuts each frame to the credit it saw and sends the whole frame or
/// none, so a sibling that drew the lane down between the cut and the send
/// leaves the frame short. A shortfall [`due`] rounds down to nothing then
/// leaves the node waiting for a proof. It pays one whole chunk instead, or a
/// closing residual for exactly the rest when less than a chunk is left unpaid,
/// so no byte is paid twice.
#[must_use]
pub(crate) fn due_after_sibling_spend(window: u64, need: u64, held: u64, room: u64) -> (u64, u64) {
    let owed = due(window, need, held, room);
    if owed != (0, 0) || window.min(need) <= held {
        return owed;
    }
    let unpaid = need.saturating_sub(held);
    if unpaid <= CHUNK_BYTES {
        (0, unpaid.min(room))
    } else {
        (u64::from(room >= CHUNK_BYTES), 0)
    }
}

/// One node operator's track record with this payer, and the credit the payer
/// holds there. Shared by every lane at that operator.
#[derive(Debug)]
pub struct NodeTrust {
    verified: AtomicU64,
    lost: AtomicU64,
    outstanding: AtomicU64,
    /// Verified bytes this process added that the peer store does not hold yet.
    /// A save adds them to the stored count, so concurrent processes never
    /// overwrite each other's progress ([`PayerWindows::take_unsaved`]).
    unsaved_verified: AtomicU64,
    /// Written-off bytes this process added that the peer store does not hold
    /// yet, like [`Self::unsaved_verified`].
    unsaved_lost: AtomicU64,
    shared: Arc<RunState>,
}

/// State that spans every node of one run: the policy and the write-offs.
#[derive(Debug)]
struct RunState {
    policy: std::sync::Mutex<PayAheadPolicy>,
    written_off_micro_usdc: AtomicU64,
}

impl RunState {
    fn policy(&self) -> PayAheadPolicy {
        *self
            .policy
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl NodeTrust {
    const fn new(shared: Arc<RunState>, verified: u64, lost: u64) -> Self {
        Self {
            verified: AtomicU64::new(verified),
            lost: AtomicU64::new(lost),
            outstanding: AtomicU64::new(0),
            unsaved_verified: AtomicU64::new(0),
            unsaved_lost: AtomicU64::new(0),
            shared,
        }
    }

    /// A trust record that belongs to no registry, under the default policy.
    /// A lane nothing attaches a registry record to uses it.
    #[must_use]
    pub fn detached() -> Arc<Self> {
        PayerWindows::new(PayAheadPolicy::new()).node(Address::ZERO)
    }

    /// Bytes verified from this node.
    #[must_use]
    pub fn verified(&self) -> u64 {
        self.verified.load(Ordering::Relaxed)
    }

    /// Paid bytes written off at this node.
    #[must_use]
    pub fn lost(&self) -> u64 {
        self.lost.load(Ordering::Relaxed)
    }

    /// Bytes currently held as prepaid credit at this node, across every lane
    /// of this process.
    #[must_use]
    pub fn outstanding(&self) -> u64 {
        self.outstanding.load(Ordering::Relaxed)
    }

    /// Record `n` bytes verified from this node.
    pub fn note_verified(&self, n: u64) {
        add(&self.verified, n);
        add(&self.unsaved_verified, n);
    }

    /// The window toward this node now, in bytes. Once the run's write-offs
    /// have spent the budget, every node reads the floor.
    #[must_use]
    pub fn window_bytes(&self, rate_per_mb: u64) -> u64 {
        let policy = self.shared.policy();
        if self.budget_spent() {
            return WINDOW_FLOOR_BYTES;
        }
        policy.window_bytes(self.verified(), self.lost(), rate_per_mb)
    }

    /// How many more bytes of credit the payer may take on at this node under
    /// the per-node cap, whatever signer holds it.
    #[must_use]
    pub fn cap_room_bytes(&self, rate_per_mb: u64) -> u64 {
        let cap = self
            .shared
            .policy()
            .cap_bytes(rate_per_mb)
            .max(WINDOW_FLOOR_BYTES);
        cap.saturating_sub(self.outstanding())
    }

    /// Count `n` bytes of credit paid ahead at this node.
    pub(crate) fn note_prepaid(&self, n: u64) {
        self.outstanding
            .update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                v.saturating_add(n)
            });
    }

    /// Count `n` bytes of held credit as spent by delivery.
    pub(crate) fn note_spent(&self, n: u64) {
        self.outstanding
            .update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                v.saturating_sub(n)
            });
    }

    /// Write off paid bytes the node did not deliver, at `rate_per_mb`.
    /// `held` is credit the node still holds and did not deliver against. It
    /// leaves the outstanding count. `bad` is credit the node spent on bytes
    /// that never verified. Both grow the node's lost count, and the run's
    /// write-off total grows by their USDC value.
    pub fn write_off(&self, held: u64, bad: u64, rate_per_mb: u64) {
        let n = held.saturating_add(bad);
        if n == 0 {
            return;
        }
        self.note_spent(held);
        add(&self.lost, n);
        add(&self.unsaved_lost, n);
        let value = u128::from(n) * u128::from(rate_per_mb) / BYTES_PER_MIB;
        let value = u64::try_from(value).unwrap_or(u64::MAX);
        let before =
            self.shared
                .written_off_micro_usdc
                .update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                    v.saturating_add(value)
                });
        // Say once, at the crossing, what a spent budget does: without this
        // line the only symptom is a throughput collapse with no cause
        // anywhere. `update` returns the previous total, so exactly one
        // write-off observes the crossing.
        let budget = self.shared.policy().write_off_budget_micro_usdc;
        if before < budget && before.saturating_add(value) >= budget {
            tracing::warn!(
                written_off_micro_usdc = before.saturating_add(value),
                budget_micro_usdc = budget,
                "the run's write-off budget is spent; every node's pay-ahead \
                 window stays at the floor for the rest of this run"
            );
        }
    }

    /// Whether this run's write-offs have used up the policy's budget.
    #[must_use]
    pub fn budget_spent(&self) -> bool {
        self.shared.written_off_micro_usdc.load(Ordering::Relaxed)
            >= self.shared.policy().write_off_budget_micro_usdc
    }
}

/// The per-process registry of [`NodeTrust`] records, one per node operator
/// address, plus the run's policy and write-off total.
#[derive(Debug)]
pub struct PayerWindows {
    shared: Arc<RunState>,
    nodes: std::sync::Mutex<HashMap<Address, Arc<NodeTrust>>>,
}

impl PayerWindows {
    /// An empty registry under `policy`.
    #[must_use]
    pub fn new(policy: PayAheadPolicy) -> Self {
        Self {
            shared: Arc::new(RunState {
                policy: std::sync::Mutex::new(policy),
                written_off_micro_usdc: AtomicU64::new(0),
            }),
            nodes: std::sync::Mutex::new(HashMap::new()),
        }
    }

    /// The registry every lane of this process shares unless it is given
    /// another.
    #[must_use]
    pub fn process() -> &'static Self {
        static PROCESS: std::sync::OnceLock<PayerWindows> = std::sync::OnceLock::new();
        PROCESS.get_or_init(|| Self::new(PayAheadPolicy::new()))
    }

    /// Replace the policy every record of this registry reads.
    pub fn configure(&self, policy: PayAheadPolicy) {
        *self
            .shared
            .policy
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = policy;
    }

    /// The policy in force.
    #[must_use]
    pub fn policy(&self) -> PayAheadPolicy {
        self.shared.policy()
    }

    /// The record for the node operator at `address`, created empty on first
    /// use.
    #[must_use]
    pub fn node(&self, address: Address) -> Arc<NodeTrust> {
        let mut nodes = self
            .nodes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Arc::clone(
            nodes
                .entry(address)
                .or_insert_with(|| Arc::new(NodeTrust::new(Arc::clone(&self.shared), 0, 0))),
        )
    }

    /// Seed the record for `address` from persisted counts. A record the run
    /// already moved keeps the larger of each count.
    pub fn seed(&self, address: Address, verified: u64, lost: u64) {
        let node = self.node(address);
        node.verified.fetch_max(verified, Ordering::Relaxed);
        node.lost.fetch_max(lost, Ordering::Relaxed);
    }

    /// Every record's `(address, verified, lost)`, for persistence.
    #[must_use]
    pub fn snapshot(&self) -> Vec<(Address, u64, u64)> {
        self.nodes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(|(address, node)| (*address, node.verified(), node.lost()))
            .collect()
    }

    /// Take every record's `(address, verified, lost)` that this process added
    /// and has not saved, leaving each unsaved count at zero. A save adds them
    /// to the stored counts, so two processes that share a peer store each add
    /// their own progress and neither overwrites the other's.
    #[must_use]
    pub fn take_unsaved(&self) -> Vec<(Address, u64, u64)> {
        self.nodes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(|(address, node)| {
                (
                    *address,
                    node.unsaved_verified.swap(0, Ordering::Relaxed),
                    node.unsaved_lost.swap(0, Ordering::Relaxed),
                )
            })
            .filter(|&(_, verified, lost)| verified > 0 || lost > 0)
            .collect()
    }

    /// Give back counts [`Self::take_unsaved`] took that a save did not store,
    /// so a later save stores them.
    pub fn return_unsaved(&self, address: Address, verified: u64, lost: u64) {
        let node = self.node(address);
        add(&node.unsaved_verified, verified);
        add(&node.unsaved_lost, lost);
    }

    /// USDC (base units) written off so far this run.
    #[must_use]
    pub fn written_off_micro_usdc(&self) -> u64 {
        self.shared.written_off_micro_usdc.load(Ordering::Relaxed)
    }
}

/// Add `n` to `counter`, saturating.
fn add(counter: &AtomicU64, n: u64) {
    counter.update(Ordering::Relaxed, Ordering::Relaxed, |v| {
        v.saturating_add(n)
    });
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests
mod tests;
