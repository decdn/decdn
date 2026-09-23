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
/// bounded set of provider lanes on the front — enough for same-region fan-out
/// and free failover, without the wide fan-out a full download wants. The
/// `Downloader` uncaps this.
pub(crate) const DEFAULT_STREAMER_LANE_CAP: usize = 2;

/// Default [`PullConfig::download_unit_deadline`]: 30 seconds. A downloading lane
/// that makes no verified progress for this long is reassigned to another holder
/// (the multi-source stall watchdog). A full-throughput download has no consumer
/// to pace against, so the watchdog — not consumption backpressure — is what
/// fails a silently-stalled source over.
pub(crate) const DEFAULT_DOWNLOAD_UNIT_DEADLINE: std::time::Duration =
    std::time::Duration::from_secs(30);

/// Zero-config tunables shared by the consumption faces.
///
/// Overrides are opt-in: [`PullConfig::default`] is the whole configuration a
/// caller needs. Override a field with struct-update syntax:
///
/// ```
/// use std::time::Duration;
///
/// use decdn_client::PullConfig;
///
/// // Fail a silent download lane over after 10 s instead of the default.
/// let config = PullConfig {
///     download_unit_deadline: Duration::from_secs(10),
///     ..PullConfig::new()
/// };
/// assert_eq!(config.read_ahead_bytes, PullConfig::new().read_ahead_bytes);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
    /// the front across at once (the multi-source `max_sources`). Small by design
    /// — a paced stream wants a little same-region parallelism and free failover
    /// on the front, not the wide striping a full download does. The `Downloader`
    /// uncaps it.
    pub streamer_lane_cap: usize,
    /// The `Downloader`'s per-lane stall watchdog: a downloading lane that makes
    /// no verified progress for this long is reassigned to another holder. A
    /// full-throughput download has no consumer to pace against, so this — not
    /// consumption backpressure — is what fails a silently-stalled source over.
    /// The `Streamer` ignores it (a paced lane parked on the consumer cursor is
    /// not a stall).
    pub download_unit_deadline: std::time::Duration,
}

impl PullConfig {
    /// The default configuration: every tunable at its documented default. `const`
    /// so it is provably free of any network or chain access.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            read_ahead_bytes: DEFAULT_READ_AHEAD_BYTES,
            streamer_lane_cap: DEFAULT_STREAMER_LANE_CAP,
            download_unit_deadline: DEFAULT_DOWNLOAD_UNIT_DEADLINE,
        }
    }
}

impl Default for PullConfig {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::{DEFAULT_READ_AHEAD_BYTES, PullConfig};

    #[test]
    fn default_read_ahead_is_the_documented_constant() {
        assert_eq!(
            PullConfig::default().read_ahead_bytes,
            DEFAULT_READ_AHEAD_BYTES
        );
        assert_eq!(DEFAULT_READ_AHEAD_BYTES, 16 * 1024 * 1024);
    }

    #[test]
    fn construction_is_a_pure_const() {
        // A `const` value proves construction runs at compile time — no network
        // or chain access can hide in a `const fn`.
        const CFG: PullConfig = PullConfig::new();
        assert_eq!(CFG.read_ahead_bytes, DEFAULT_READ_AHEAD_BYTES);
    }

    #[test]
    fn default_download_unit_deadline_is_the_documented_constant() {
        assert_eq!(
            PullConfig::default().download_unit_deadline,
            super::DEFAULT_DOWNLOAD_UNIT_DEADLINE
        );
        assert_eq!(
            super::DEFAULT_DOWNLOAD_UNIT_DEADLINE,
            std::time::Duration::from_secs(30)
        );
    }
}
