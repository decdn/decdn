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
pub const DEFAULT_READ_AHEAD_BYTES: u64 = 16 * 1024 * 1024;

/// Default [`PullConfig::streamer_lane_cap`]: two. A `Streamer` keeps a small,
/// bounded set of provider lanes on the front — enough for same-region fan-out
/// and free failover, without the wide fan-out a full download wants. The
/// `Downloader` uncaps this.
pub const DEFAULT_STREAMER_LANE_CAP: usize = 2;

/// Zero-config tunables shared by the consumption faces.
///
/// Overrides are opt-in: [`PullConfig::default`] is the whole configuration a
/// caller needs. Fields are added as the faces grow to consume them (#1848); the
/// first is the `Streamer`'s read-ahead bound.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PullConfig {
    /// The `Streamer`'s bounded read-ahead: the maximum number of bytes fetched
    /// AHEAD of the consumer's read cursor. Because every fetched byte is paid,
    /// this read-ahead bound is also the outstanding-spend bound for a stream the
    /// consumer abandons early — fetch a two-hour movie's first `read_ahead_bytes`
    /// only, not the whole thing, for a viewer who stops after five minutes. The
    /// `Downloader` ignores it and fetches the whole blob at full throughput.
    pub read_ahead_bytes: u64,
    /// The `Streamer`'s provider-lane cap: how many discovered holders it fetches
    /// the front across at once (the multi-source `max_sources`). Small by design
    /// — a paced stream wants a little same-region parallelism and free failover
    /// on the front, not the wide striping a full download does. The `Downloader`
    /// uncaps it.
    pub streamer_lane_cap: usize,
}

impl PullConfig {
    /// The default configuration: every tunable at its documented default. `const`
    /// so it is provably free of any network or chain access.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            read_ahead_bytes: DEFAULT_READ_AHEAD_BYTES,
            streamer_lane_cap: DEFAULT_STREAMER_LANE_CAP,
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
}
