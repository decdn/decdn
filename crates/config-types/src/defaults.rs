//! Shared config defaults that both `decdn-cache` and `decdn-common`
//! need to agree on from a single source.

/// Default `User-Agent` header set on every origin request unless
/// overridden via the operator-configured `cache.user_agent`. Embeds
/// `CARGO_PKG_VERSION` of *this* crate (`decdn-config-types`). The
/// workspace does not version-link its members, so this can drift from
/// the running `decdn-node` binary's version — that is acceptable: the
/// `decdn-node/` prefix is the stable contract operators grep on in
/// access logs; the version segment is best-effort attribution (#435,
/// #578). Lets origin operators attribute CDN pull-through traffic and
/// apply origin-side rate limits or routing rules separately from
/// anonymous client traffic.
pub const DEFAULT_USER_AGENT: &str = concat!("decdn-node/", env!("CARGO_PKG_VERSION"));

/// Default ceiling on concurrently-held probe slots before the cache
/// engine starts rejecting new probe holds (#276 / ADR 005). Shared so
/// the config default and the cache engine's own default cannot drift.
pub const DEFAULT_MAX_PROBE_HOLDS: usize = 256;

/// Default TTL in seconds for a memoised live-origin probe answer
/// (#1130 pt3). Long enough that a burst of probes for the same object
/// costs one `HEAD`/`HeadObject`, short enough to track an origin
/// deletion within the probe-hold horizon. Canonical u64 form shared by
/// `decdn-common` (config default) and `decdn-cache` (wrapped as a
/// `Duration`).
pub const DEFAULT_ORIGIN_PROBE_TTL_SEC: u64 = 15;

/// Default TTL in seconds for a memoised `Absent` live-origin probe
/// answer. Short on purpose: it bounds how long a stale `Absent` can
/// hide newly-available own content from a probe, while a random-hash
/// flood never repeats a hash within any window so the short TTL barely
/// changes flood cost.
pub const DEFAULT_ORIGIN_PROBE_NEGATIVE_TTL_SEC: u64 = 2;

/// Default TTL in seconds for a memoised `Fault` live-origin probe
/// answer (#1130 pt3). Longer than the `Absent` TTL because the memo is
/// keyed per hash and a client retrying one hash against a failing
/// origin is the common shape — it collapses a hash's repeats, not the
/// namespace's load. Shorter than the `Present` TTL because a memoised
/// fault costs client-visible availability until it expires, so a
/// recovered origin must be re-probed quickly. The config resolver
/// enforces `negative <= fault <= positive` on operator overrides.
pub const DEFAULT_ORIGIN_PROBE_FAULT_TTL_SEC: u64 = 5;

/// Default per-probe ceiling in milliseconds on the live-origin
/// `HEAD`/`HeadObject` (#1130 pt3). A slow origin must not stall the
/// probe hot path; on overrun the probe answers `has_blob: false` and
/// memoises the miss.
pub const DEFAULT_ORIGIN_PROBE_TIMEOUT_MS: u64 = 2000;

/// Default cap on distinct hashes in the live-origin probe memo
/// (#1130 pt3). Bounds memo memory under a random-hash probe flood.
pub const DEFAULT_ORIGIN_PROBE_MEMO_CAPACITY: u64 = 4096;
