//! In-memory per-channel voucher-activity clock (issue #749).
//!
//! [`crate::ChannelState`] persists *what* the latest accepted voucher was
//! (nonce, amount, bytes), but not *when* it was accepted: the redb schema
//! carries no last-voucher wall-clock, and adding one would be a schema
//! migration touching every `ChannelState` call site. The operator-facing
//! "time since last voucher" signal `decdn node channels` reports
//! (issue #749) does not need durability — a stale-channel diagnostic is
//! about *current* liveness, so an in-process clock that resets on restart
//! is the correct shape.
//!
//! [`VoucherActivity`] is that clock: a `Mutex<HashMap<ChannelId, Instant>>`
//! the voucher-accept path stamps on each acceptance ([`VoucherActivity::touch`])
//! and the admin snapshot reads ([`VoucherActivity::seconds_since`]). It is
//! deliberately *not* wired into [`crate::ChannelState::apply_voucher`] (which
//! is a sync, store-coupled leaf method) — the `decdn-node` client handler
//! stamps it after a successful apply, the same place it fires the redeem hint.
//!
//! A channel hydrated from `channels.redb` at boot has no entry until its next
//! voucher, so [`VoucherActivity::seconds_since`] returns `None` — surfaced to
//! the operator as "never (since restart)" rather than a misleading "0s ago".

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;

use crate::channel::ChannelId;

/// In-memory clock recording the last time this process accepted a voucher
/// on each channel. Thread-safe and cheap to clone the `Arc` around: the
/// client handler holds one reference (writer) and the admin surface holds
/// another (reader). See the module docs for why this is intentionally
/// non-durable.
#[derive(Debug, Default)]
pub struct VoucherActivity {
    /// `Instant` of the most recent `touch` per channel. A poisoned mutex
    /// (a panic while holding it) degrades reads to "no record" and drops
    /// writes rather than propagating the panic — the workspace anti-panic
    /// policy forbids `unwrap`/`expect`, and a lost activity stamp only
    /// makes a channel look staler than it is (a safe direction for a
    /// diagnostic).
    last_voucher_at: Mutex<HashMap<ChannelId, Instant>>,
}

impl VoucherActivity {
    /// Construct an empty activity clock.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that a voucher was just accepted on `channel_id`, stamping
    /// the current [`Instant`]. Called from the voucher-accept success path
    /// after `apply_voucher` returns `Ok`. A poisoned lock silently drops
    /// the stamp (the channel then looks staler than it is — a safe
    /// direction for a diagnostic).
    pub fn touch(&self, channel_id: ChannelId) {
        if let Ok(mut map) = self.last_voucher_at.lock() {
            map.insert(channel_id, Instant::now());
        }
    }

    /// Whole seconds since the last voucher was accepted on `channel_id`,
    /// or `None` when this process has accepted none since startup (no
    /// entry — e.g. a channel hydrated from disk that hasn't been touched,
    /// or a poisoned lock). `Instant`-based, so wall-clock skew can't make
    /// the age go backwards.
    #[must_use]
    pub fn seconds_since(&self, channel_id: ChannelId) -> Option<u64> {
        let map = self.last_voucher_at.lock().ok()?;
        map.get(&channel_id).map(|at| at.elapsed().as_secs())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use alloy::primitives::b256;

    const CH_A: ChannelId =
        b256!("1111111111111111111111111111111111111111111111111111111111111111");
    const CH_B: ChannelId =
        b256!("2222222222222222222222222222222222222222222222222222222222222222");

    #[test]
    fn untouched_channel_reports_none() {
        let activity = VoucherActivity::new();
        assert_eq!(activity.seconds_since(CH_A), None);
    }

    #[test]
    fn touched_channel_reports_some_small_age() {
        let activity = VoucherActivity::new();
        activity.touch(CH_A);
        // Immediately after touch the elapsed whole-seconds is 0, but the
        // entry exists, so the value is `Some(_)` — distinguishing "just
        // saw a voucher" from "never saw one".
        let secs = activity.seconds_since(CH_A);
        assert!(secs.is_some(), "touched channel must report Some, got None");
        assert!(
            secs.unwrap() < 5,
            "age should be near-zero right after touch, got {secs:?}"
        );
        // An unrelated channel is unaffected.
        assert_eq!(activity.seconds_since(CH_B), None);
    }

    #[test]
    fn touch_is_idempotent_per_channel_key() {
        let activity = VoucherActivity::new();
        activity.touch(CH_A);
        activity.touch(CH_A);
        // Two touches on the same channel keep a single entry (HashMap
        // overwrite) — still `Some`, no accumulation.
        assert!(activity.seconds_since(CH_A).is_some());
    }
}
