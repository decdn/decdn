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
