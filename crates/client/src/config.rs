//! Zero-config tunables for the consumption faces over the paid pull engine
//! (`Downloader`, `Streamer`, #1848).
//!
//! [`PullConfig`] holds only tunables, each with a sensible default. The REQUIRED
//! per-fetch inputs — identity, the connection, the spend budget, the provider
//! set — are not here; they flow through the engine's existing params
//! ([`crate::PoolContext`], the [`iroh::Endpoint`], discovered candidates). That
//! split is what lets a `PullConfig` construct with no network or chain access.
//!
//! The payment interval is deliberately NOT a tunable: it is the fixed protocol
//! constant [`decdn_protocol::client::CHUNK_BYTES`] (1 MiB, #1676).

/// Default [`PullConfig::read_ahead_bytes`]: 16 MiB — sixteen 1 MiB payment
/// intervals of buffering. Enough to keep a consumer's buffer full without
/// fetching (and paying for) far past where it might stop reading.
pub(crate) const DEFAULT_READ_AHEAD_BYTES: u64 = 16 * 1024 * 1024;

/// Default [`PullConfig::streamer_lane_cap`]: two. A `Streamer` keeps a small,
/// bounded set of provider lanes on the front — enough for same-region fan-out,
/// without the wide fan-out a full download wants. The `Downloader` runs one
/// lane per holder instead.
pub(crate) const DEFAULT_STREAMER_LANE_CAP: usize = 2;

/// Zero-config tunables shared by the consumption faces.
///
/// Overrides are opt-in: [`PullConfig::default`] is the whole configuration a
/// caller needs. Override a tunable with its `with_` method:
///
/// ```
/// use decdn_client::PullConfig;
///
/// // Let a stream run 64 MiB ahead of its reader instead of the default.
/// let config = PullConfig::new().with_read_ahead_bytes(64 * 1024 * 1024);
/// assert_eq!(config.streamer_lane_cap, PullConfig::new().streamer_lane_cap);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct PullConfig {
    /// The `Streamer`'s bounded read-ahead: the maximum number of bytes fetched
    /// AHEAD of the consumer's read cursor. Because every fetched byte is paid,
    /// this read-ahead bound is also the outstanding-spend bound for a stream the
    /// consumer abandons early — fetch a two-hour movie's first `read_ahead_bytes`
    /// only, not the whole thing, for a viewer who stops after five minutes. A
    /// value below [`crate::pacer::PULL_WINDOW_FLOOR`] is raised to it: a smaller
    /// window cannot keep a paid leg moving. The `Downloader` ignores it and
    /// fetches the whole blob at full throughput.
    pub read_ahead_bytes: u64,
    /// The `Streamer`'s provider-lane cap: how many discovered holders it fetches
    /// the front across at once (the acquire loop's `max_lanes`). Small by design
    /// — a paced stream wants a little same-region parallelism on the front, not
    /// the wide striping a full download does. The `Downloader` runs one lane
    /// per holder instead.
    pub streamer_lane_cap: usize,
    /// The most USDC (base units) the payer holds as credit at one node across
    /// every signer. The node sends only what a lane has paid for, so this is
    /// the most a node that keeps the credit can take. Converted to bytes at the
    /// rate the node quotes (ADR 003 § Credit Window).
    pub pay_ahead_cap: u64,
    /// The payer's window toward a node is `verified / pay_ahead_divisor` once
    /// that exceeds one chunk plus one chunk group, up to [`Self::pay_ahead_cap`].
    pub pay_ahead_divisor: u64,
    /// How many verified bytes one written-off byte cancels when sizing a
    /// node's window.
    pub loss_weight: u64,
    /// The most USDC (base units) one run writes off in total. Past it every
    /// node's window stays at its floor.
    pub write_off_budget: u64,
}

impl PullConfig {
    /// The default configuration: every tunable at its documented default. `const`
    /// so it is provably free of any network or chain access.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            read_ahead_bytes: DEFAULT_READ_AHEAD_BYTES,
            streamer_lane_cap: DEFAULT_STREAMER_LANE_CAP,
            pay_ahead_cap: crate::payer_window::DEFAULT_PAY_AHEAD_CAP_MICRO_USDC,
            pay_ahead_divisor: crate::payer_window::DEFAULT_PAY_AHEAD_DIVISOR,
            loss_weight: crate::payer_window::DEFAULT_LOSS_WEIGHT,
            write_off_budget: crate::payer_window::DEFAULT_WRITE_OFF_BUDGET_MICRO_USDC,
        }
    }

    /// These tunables with `read_ahead_bytes` replaced.
    #[must_use]
    pub const fn with_read_ahead_bytes(mut self, read_ahead_bytes: u64) -> Self {
        self.read_ahead_bytes = read_ahead_bytes;
        self
    }

    /// These tunables with `streamer_lane_cap` replaced.
    #[must_use]
    pub const fn with_streamer_lane_cap(mut self, streamer_lane_cap: usize) -> Self {
        self.streamer_lane_cap = streamer_lane_cap;
        self
    }

    /// These tunables with the payer window policy replaced.
    #[must_use]
    pub const fn with_pay_ahead(mut self, policy: crate::payer_window::PayAheadPolicy) -> Self {
        self.pay_ahead_cap = policy.cap_micro_usdc;
        self.pay_ahead_divisor = policy.divisor;
        self.loss_weight = policy.loss_weight;
        self.write_off_budget = policy.write_off_budget_micro_usdc;
        self
    }

    /// The payer window policy these tunables name.
    #[must_use]
    pub const fn pay_ahead_policy(&self) -> crate::payer_window::PayAheadPolicy {
        crate::payer_window::PayAheadPolicy {
            cap_micro_usdc: self.pay_ahead_cap,
            divisor: self.pay_ahead_divisor,
            loss_weight: self.loss_weight,
            write_off_budget_micro_usdc: self.write_off_budget,
        }
    }

    /// Put these tunables' window policy in force for every lane of this
    /// process.
    pub fn apply_pay_ahead(&self) {
        crate::payer_window::PayerWindows::process().configure(self.pay_ahead_policy());
    }
}

impl Default for PullConfig {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests;
