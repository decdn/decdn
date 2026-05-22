//! Node-local client reputation ledger.
//!
//! Implements the [ADR 003 §Corrupted delivery](../../../adr/003-payments.md)
//! framing: nodes refuse continued service to keys with elevated
//! voucher-withhold rates and may cap total bytes for keys without
//! established history. State is keyed by the `channel.client` Ethereum
//! address (see [`crate::channel::ChannelState::client`]) — the funder that
//! signs vouchers, not the on-wire `NodeId`.
//!
//! Each [`ClientReputationLedger::record_voucher_received`] /
//! [`ClientReputationLedger::record_voucher_withheld`] call folds a sample
//! into a per-client EWMA in `[0.0, 1.0]`. The EWMA core matches the
//! per-peer scoring in the sibling `decdn-reputation` crate's local
//! module at default settings (ADR 008 §Local Score Calculation,
//! α = 0.1) — both implement `next = (1 - α) · prev + α · sample`
//! followed by a `clamp(0.0, 1.0)`. This ledger omits the configurable
//! `max_delta_per_update` cap that the sibling crate carries, because
//! the only samples here are exactly `0.0` and `1.0` and the
//! single-event cap that knob exists to enforce is already structural.
//!
//! [`ClientReputationLedger::admit`] maps the score to one of three tiers
//! per the ADR framing:
//!
//! - `score >= accept_threshold` → [`Admission::Accept`] (full service);
//! - `score >= reject_threshold` → [`Admission::AcceptCapped`] (serve up to
//!   `byte_cap` before requiring a completed voucher cycle);
//! - unseen client → [`Admission::AcceptCapped`] with the new-client cap;
//! - `score < reject_threshold` → [`Admission::Reject`].
//!
//! Persistence is exposed via [`ClientReputationStore`] (`load_all` /
//! `record` / `forget`), mirroring [`crate::ChannelStateStore`]. The runtime-side
//! redb-backed implementation lands with the `cdn/client/v1` paid-delivery
//! handler (#317); until then this module is a self-contained library
//! component with no live consumer.
//!
//! Out of scope (per #482):
//!
//! - cross-node gossip of client reputation (would re-introduce an on-chain
//!   client-reputation surface ADR 003 explicitly avoids);
//! - any on-chain consequence — this is node-local only.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use alloy::primitives::Address;
use thiserror::Error;

use crate::store::StoreError;

const DEFAULT_ALPHA: f64 = 0.1;
const DEFAULT_INITIAL_SCORE: f64 = 0.5;
const DEFAULT_ACCEPT_THRESHOLD: f64 = 0.70;
const DEFAULT_REJECT_THRESHOLD: f64 = 0.30;
/// Default cap, in bytes, for [`Admission::AcceptCapped`] decisions. 1 GiB.
const DEFAULT_CAP_BYTES: u64 = 1 << 30;
/// Default ceiling on the number of distinct client addresses tracked
/// in memory. Sized against an order-of-magnitude estimate of concurrent
/// unique payers per operator; raise it on operators that see a wider
/// distribution. `0` in the config means "unbounded" (see
/// [`ClientReputationConfig::max_tracked_clients`]).
const DEFAULT_MAX_TRACKED_CLIENTS: usize = 100_000;

/// Validation errors when constructing a [`ClientReputationLedger`].
#[non_exhaustive]
#[derive(Debug, Error, PartialEq)]
pub enum ConfigError {
    /// A `[0.0, 1.0]`-bound field was non-finite or out of range.
    #[error("{field} must be a finite number in [0.0, 1.0], got {value}")]
    OutOfUnitInterval {
        /// The offending field name.
        field: &'static str,
        /// The supplied value.
        value: f64,
    },
    /// `reject_threshold` was not strictly less than `accept_threshold`.
    /// Without this guard the borderline tier (between the two) collapses
    /// and the ledger flips straight from Accept to Reject across one
    /// EWMA tick — operators would lose the "cap, then escalate" rung the
    /// ADR 003 framing relies on.
    #[error("reject_threshold ({reject}) must be strictly less than accept_threshold ({accept})")]
    ThresholdOrdering {
        /// Configured reject threshold.
        reject: f64,
        /// Configured accept threshold.
        accept: f64,
    },
    /// `alpha` was exactly `0.0`. The unit-interval check accepts `0.0`,
    /// but a zero alpha freezes the EWMA at `initial_score` for every
    /// future event — the ledger silently stops responding to behaviour.
    /// `alpha` is documented as `(0.0, 1.0]`; this guard enforces it.
    #[error(
        "alpha must be strictly greater than 0.0; a zero alpha freezes the EWMA at initial_score"
    )]
    AlphaCannotBeZero,
    /// One of the byte-cap knobs was `0`. `AcceptCapped { byte_cap: 0 }`
    /// is functionally `Reject` — the borderline tier silently
    /// collapses. Caps must be strictly positive.
    #[error("{field} must be strictly greater than 0; a zero cap is functionally a reject")]
    ZeroCap {
        /// The offending field name.
        field: &'static str,
    },
}

/// Per-client reputation snapshot. One entry per `channel.client` address.
///
/// `event_count == 0` is the canonical "never observed" sentinel — see the
/// field doc and [`ClientReputationLedger::admit`]. `event_count` and
/// `total_bytes_delivered` saturate at `u64::MAX`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ClientReputation {
    /// EWMA in `[0.0, 1.0]`. `1.0` = always signs; `0.0` = always withholds.
    pub completion_ratio: f64,
    /// Total observed stream events (received + withheld). Saturating.
    pub event_count: u64,
    /// Cumulative bytes delivered to this client (paid or not). Saturating.
    /// Tracked for operator observability; the EWMA is per-event, not
    /// per-byte, by design — see module docs.
    pub total_bytes_delivered: u64,
    /// Unix-microseconds of the most recent update; use
    /// [`ClientReputation::event_count`] (not this field) as the
    /// unambiguous "never observed" signal — `event_count == 0` is the
    /// canonical sentinel and is what [`ClientReputationLedger::admit`]
    /// keys off. This field is `0` in three otherwise-distinct cases:
    /// (a) never observed, (b) the system clock was behind the UNIX
    /// epoch at the most recent update (the time helper saturates to
    /// `0`), or (c) — far enough in the future — overflow saturation to
    /// `u64::MAX`. Operators relying on this as a liveness signal should
    /// treat it as "no observation in the post-epoch era".
    pub last_seen_us: u64,
}

impl Default for ClientReputation {
    fn default() -> Self {
        Self {
            completion_ratio: DEFAULT_INITIAL_SCORE,
            event_count: 0,
            total_bytes_delivered: 0,
            last_seen_us: 0,
        }
    }
}

/// Admission decision for one client encounter.
///
/// `#[must_use]` because dropping the value silently discards the
/// `AcceptCapped` byte cap — a pattern like
/// `Admission::Accept | Admission::AcceptCapped { .. } => serve(req)`
/// would compile cleanly while stripping the deterrent the ledger exists
/// to produce. Callers must explicitly destructure the variant and
/// enforce the cap they receive.
#[non_exhaustive]
#[must_use = "Admission carries an enforcement contract — destructure and act on the byte_cap"]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    /// Full service.
    Accept,
    /// Serve up to `byte_cap` bytes before requiring a completed voucher
    /// cycle. The caller enforces the cap against its own request
    /// bookkeeping; the ledger does not track in-flight bytes.
    AcceptCapped {
        /// Maximum bytes the caller may deliver before the next voucher
        /// settlement point.
        byte_cap: u64,
    },
    /// Refuse the request outright.
    Reject,
}

/// Tunable thresholds and EWMA parameters.
///
/// Defaults follow the ADR 003 §Corrupted delivery framing: new clients
/// (no history) land in the capped tier until a sequence of completed
/// vouchers lifts the EWMA above `accept_threshold`. Validation runs in
/// [`ClientReputationLedger::new`]; an invalid config returns
/// [`ConfigError`].
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ClientReputationConfig {
    /// EWMA smoothing factor in `(0.0, 1.0]`. Default `0.1` (ADR 008 §3).
    pub alpha: f64,
    /// Score assigned on the first observed event before the sample is
    /// folded in. Default `0.5` — neutral.
    pub initial_score: f64,
    /// `score >= accept_threshold` → [`Admission::Accept`]. Default `0.70`.
    pub accept_threshold: f64,
    /// `score <  reject_threshold` → [`Admission::Reject`]. Default `0.30`.
    /// Must be strictly less than `accept_threshold`.
    pub reject_threshold: f64,
    /// Byte cap returned in [`Admission::AcceptCapped`] for an unseen
    /// client. Default 1 GiB.
    pub new_client_cap_bytes: u64,
    /// Byte cap returned in [`Admission::AcceptCapped`] for a known client
    /// whose score sits between the reject and accept thresholds.
    /// Default 1 GiB.
    pub borderline_cap_bytes: u64,
    /// Maximum number of distinct client addresses tracked in memory.
    /// `0` means unbounded — matches the `max_tracked_sources = 0`
    /// convention used by `ResolvedSecurity` in `decdn-common`. Default
    /// `100_000`.
    ///
    /// Enforced in two places:
    ///
    /// - [`ClientReputationLedger::from_snapshot`]: hydrating more than
    ///   `max_tracked_clients` rows drops the entries with the smallest
    ///   `last_seen_us` until the map fits the cap, so the ledger never
    ///   starts above its configured ceiling.
    /// - [`ClientReputationLedger::record_voucher_received`] /
    ///   [`ClientReputationLedger::record_voucher_withheld`]: at
    ///   capacity, a *new* address evicts the entry with the smallest
    ///   `last_seen_us` (oldest observation); updates to an existing
    ///   entry never trigger eviction.
    ///
    /// The cap is defense-in-depth: each new key requires the attacker
    /// to open an L2 payment channel (USDC deposit + gas), so Sybil
    /// growth is structurally rate-limited per ADR 003 §Corrupted
    /// delivery. This bound caps in-memory growth in the worst case the
    /// structural deterrent fails to hold.
    pub max_tracked_clients: usize,
}

impl Default for ClientReputationConfig {
    fn default() -> Self {
        Self {
            alpha: DEFAULT_ALPHA,
            initial_score: DEFAULT_INITIAL_SCORE,
            accept_threshold: DEFAULT_ACCEPT_THRESHOLD,
            reject_threshold: DEFAULT_REJECT_THRESHOLD,
            new_client_cap_bytes: DEFAULT_CAP_BYTES,
            borderline_cap_bytes: DEFAULT_CAP_BYTES,
            max_tracked_clients: DEFAULT_MAX_TRACKED_CLIENTS,
        }
    }
}

/// In-memory ledger of per-client EWMA scores.
///
/// Cheap to share across tasks via [`std::sync::Arc`]; the internal lock is
/// an `RwLock` so [`Self::admit`] / [`Self::score`] / [`Self::snapshot`] do
/// not block one another. Persistence is exposed separately via
/// [`ClientReputationStore`]; the ledger never touches disk directly.
#[derive(Debug)]
pub struct ClientReputationLedger {
    config: ClientReputationConfig,
    state: RwLock<HashMap<Address, ClientReputation>>,
    /// Process-lifetime LRU eviction count. Surfaced via
    /// [`Self::evictions_total`] so the runtime metrics layer can
    /// publish the `decdn_client_reputation_evictions_total` counter
    /// without pulling `iroh-metrics` into this leaf crate.
    evictions_total: AtomicU64,
}

impl ClientReputationLedger {
    /// Construct an empty ledger with `config`, validating threshold and
    /// EWMA fields.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] if any unit-interval field is non-finite or
    /// out of `[0.0, 1.0]`, or if `reject_threshold >= accept_threshold`.
    pub fn new(config: ClientReputationConfig) -> Result<Self, ConfigError> {
        validate(&config)?;
        Ok(Self {
            config,
            state: RwLock::new(HashMap::new()),
            evictions_total: AtomicU64::new(0),
        })
    }

    /// Construct a ledger pre-populated from a previously-persisted
    /// snapshot (the hydration path used by [`ClientReputationStore::load_all`]
    /// at node bring-up, and by tests that need to inject scores at
    /// specific tier boundaries).
    ///
    /// Entries whose `completion_ratio` is non-finite or outside
    /// `[0.0, 1.0]` are skipped — a single corrupt row should not
    /// prevent the ledger from coming up. Per ADR 003 the deterrent is
    /// statistical, not cryptographic, so partial loss is tolerable.
    /// Each skipped row emits a `tracing::warn!` with the affected
    /// address and the offending value; a `tracing::info!` summarises
    /// the load with loaded / skipped counts so operators have visibility
    /// into hydration integrity instead of silent data loss.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] from [`Self::new`]; no entries are inserted
    /// when the config is invalid.
    pub fn from_snapshot(
        config: ClientReputationConfig,
        entries: impl IntoIterator<Item = (Address, ClientReputation)>,
    ) -> Result<Self, ConfigError> {
        validate(&config)?;
        let mut map = HashMap::new();
        let mut skipped: u64 = 0;
        for (addr, rep) in entries {
            if rep.completion_ratio.is_finite() && (0.0..=1.0).contains(&rep.completion_ratio) {
                map.insert(addr, rep);
            } else {
                skipped = skipped.saturating_add(1);
                tracing::warn!(
                    %addr,
                    completion_ratio = rep.completion_ratio,
                    "client_reputation: skipping invalid hydrated row",
                );
            }
        }
        // Enforce the keyspace cap at hydration time too, so the ledger
        // never starts above its configured ceiling. Without this a
        // torn / outdated store could re-introduce the very unbounded-
        // map shape the cap exists to prevent. Uses
        // `select_nth_unstable_by_key` for an O(n) partition (the bulk-
        // shrink primitive the issue scope calls out for "if ever
        // needed"); the eventual redb-backed store should write through
        // the bounded ledger, so a hydration-side cap-overflow is
        // expected to be rare in practice — when it happens, the
        // hydration drops the entries with the smallest `last_seen_us`.
        let bulk_evicted: u64 =
            if config.max_tracked_clients > 0 && map.len() > config.max_tracked_clients {
                let to_remove = map.len() - config.max_tracked_clients;
                let mut pairs: Vec<(Address, u64)> =
                    map.iter().map(|(k, v)| (*k, v.last_seen_us)).collect();
                pairs.select_nth_unstable_by_key(to_remove, |(_, t)| *t);
                for (addr, _) in pairs.iter().take(to_remove) {
                    map.remove(addr);
                }
                u64::try_from(to_remove).unwrap_or(u64::MAX)
            } else {
                0
            };
        tracing::info!(
            loaded = map.len(),
            skipped,
            bulk_evicted,
            "client_reputation: hydration complete",
        );
        Ok(Self {
            config,
            state: RwLock::new(map),
            evictions_total: AtomicU64::new(bulk_evicted),
        })
    }

    /// Fold a "stream completed, voucher received" event into `client`'s
    /// EWMA. `bytes_delivered` is added to the saturating counter; the
    /// EWMA sample itself is `1.0` regardless of byte count (the ledger
    /// is per-event, not per-byte — see module docs).
    ///
    /// Returns the post-update score so callers logging it avoid a
    /// re-lock.
    pub fn record_voucher_received(&self, client: Address, bytes_delivered: u64) -> f64 {
        self.fold_event(client, 1.0, bytes_delivered)
    }

    /// Fold a "stream withheld voucher" event into `client`'s EWMA.
    /// `bytes_delivered` is added to the saturating counter; the EWMA
    /// sample is `0.0`. Returns the post-update score.
    pub fn record_voucher_withheld(&self, client: Address, bytes_delivered: u64) -> f64 {
        self.fold_event(client, 0.0, bytes_delivered)
    }

    /// Decide whether to admit `client`. Maps the per-client EWMA to one
    /// of [`Admission::Accept`], [`Admission::AcceptCapped`], or
    /// [`Admission::Reject`] per the ADR 003 three-tier framing.
    ///
    /// "Unseen" is defined as either no map entry, *or* an entry with
    /// `event_count == 0` (i.e., a hydrated or default-constructed row
    /// that carries no real signal — its `completion_ratio` is just the
    /// default the persistence/struct-literal path put there). Both
    /// resolve to [`Admission::AcceptCapped`] with
    /// [`ClientReputationConfig::new_client_cap_bytes`], so a snapshot
    /// round-trip of an empty row is admission-equivalent to a
    /// never-recorded client and persistence layers don't have to
    /// distinguish the two states.
    pub fn admit(&self, client: Address) -> Admission {
        let guard = self
            .state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let unseen = Admission::AcceptCapped {
            byte_cap: self.config.new_client_cap_bytes,
        };
        let Some(rep) = guard.get(&client) else {
            return unseen;
        };
        if rep.event_count == 0 {
            return unseen;
        }
        if rep.completion_ratio >= self.config.accept_threshold {
            Admission::Accept
        } else if rep.completion_ratio >= self.config.reject_threshold {
            Admission::AcceptCapped {
                byte_cap: self.config.borderline_cap_bytes,
            }
        } else {
            Admission::Reject
        }
    }

    /// Current score for `client`. Returns `None` if never observed so
    /// callers can distinguish "unseen" from "score happens to equal
    /// `initial_score`".
    pub fn score(&self, client: Address) -> Option<f64> {
        let guard = self
            .state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard.get(&client).map(|rep| rep.completion_ratio)
    }

    /// Full reputation entry for `client`, or `None` if never observed.
    /// Used by persistence and metrics.
    pub fn entry(&self, client: Address) -> Option<ClientReputation> {
        let guard = self
            .state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard.get(&client).copied()
    }

    /// Snapshot of all observed `(address, reputation)` pairs. Allocates.
    /// Drives the periodic-persistence path that the runtime will own
    /// (deferred follow-up).
    pub fn snapshot(&self) -> Vec<(Address, ClientReputation)> {
        let guard = self
            .state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard.iter().map(|(k, v)| (*k, *v)).collect()
    }

    fn fold_event(&self, client: Address, sample: f64, bytes_delivered: u64) -> f64 {
        let now_us = unix_micros();
        // Poison recovery: the arithmetic happens entirely on the value
        // we extract, so a prior panic on the write path cannot leave the
        // map structurally torn (no partial mutation between locks).
        let mut guard = self
            .state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Keyspace bound: when a *new* client address arrives at
        // capacity, evict the entry with the smallest `last_seen_us`
        // (oldest observation). Updates to an existing client never
        // evict — `is_new` gates the branch so an established entry
        // can't displace itself. The write lock already single-flights
        // concurrent over-cap observers, so no separate prune-guard
        // atomic is needed (contrast `dispatch.rs::ConnectionLimiter`,
        // which prunes from a read-lock path and needs one). The linear
        // scan is acceptable at the 100k default cap and bounded to one
        // scan per new-key insert. `max_tracked_clients == 0` opts out.
        let is_new = !guard.contains_key(&client);
        if is_new
            && self.config.max_tracked_clients > 0
            && guard.len() >= self.config.max_tracked_clients
            && let Some(oldest_key) = guard
                .iter()
                .min_by_key(|(_, rep)| rep.last_seen_us)
                .map(|(k, _)| *k)
        {
            guard.remove(&oldest_key);
            self.evictions_total.fetch_add(1, Ordering::Relaxed);
        }
        let entry = guard.entry(client).or_insert_with(|| ClientReputation {
            completion_ratio: self.config.initial_score,
            event_count: 0,
            total_bytes_delivered: 0,
            last_seen_us: 0,
        });
        let prev = entry.completion_ratio;
        let next = ((1.0 - self.config.alpha) * prev + self.config.alpha * sample).clamp(0.0, 1.0);
        entry.completion_ratio = next;
        entry.event_count = entry.event_count.saturating_add(1);
        entry.total_bytes_delivered = entry.total_bytes_delivered.saturating_add(bytes_delivered);
        entry.last_seen_us = now_us;
        next
    }

    /// Number of distinct client addresses currently tracked. Surfaced
    /// as a public accessor so the runtime metrics layer can publish
    /// the `decdn_client_reputation_tracked_clients` gauge without an
    /// `iroh-metrics` dependency in `decdn-incentive`.
    #[must_use]
    pub fn tracked_clients(&self) -> usize {
        // Mirror the poison-recovery pattern used by `admit` / `score` /
        // `entry` / `snapshot`. Returning `0` on a poisoned lock would
        // spuriously crater the operator gauge after an unrelated panic
        // — operators would read "ledger reset to empty", which is not
        // what happened.
        self.state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }

    /// Cumulative LRU evictions since this ledger was constructed.
    /// Companion to [`Self::tracked_clients`]; the runtime metrics
    /// layer publishes this as the
    /// `decdn_client_reputation_evictions_total` counter. A persistently
    /// non-zero rate signals either an under-sized
    /// [`ClientReputationConfig::max_tracked_clients`] or an attacker
    /// spending L2 channel-open cost to churn keys faster than the cap
    /// (per ADR 003 §Corrupted delivery, that cost IS the bound).
    #[must_use]
    pub fn evictions_total(&self) -> u64 {
        self.evictions_total.load(Ordering::Relaxed)
    }
}

fn unix_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_micros()).unwrap_or(u64::MAX))
}

fn validate(c: &ClientReputationConfig) -> Result<(), ConfigError> {
    require_unit_interval(c.alpha, "alpha")?;
    if c.alpha == 0.0 {
        return Err(ConfigError::AlphaCannotBeZero);
    }
    require_unit_interval(c.initial_score, "initial_score")?;
    require_unit_interval(c.accept_threshold, "accept_threshold")?;
    require_unit_interval(c.reject_threshold, "reject_threshold")?;
    if c.reject_threshold >= c.accept_threshold {
        return Err(ConfigError::ThresholdOrdering {
            reject: c.reject_threshold,
            accept: c.accept_threshold,
        });
    }
    if c.new_client_cap_bytes == 0 {
        return Err(ConfigError::ZeroCap {
            field: "new_client_cap_bytes",
        });
    }
    if c.borderline_cap_bytes == 0 {
        return Err(ConfigError::ZeroCap {
            field: "borderline_cap_bytes",
        });
    }
    Ok(())
}

fn require_unit_interval(value: f64, field: &'static str) -> Result<(), ConfigError> {
    if value.is_finite() && (0.0..=1.0).contains(&value) {
        Ok(())
    } else {
        Err(ConfigError::OutOfUnitInterval { field, value })
    }
}

/// Durable backing store for per-client reputation entries.
///
/// Mirrors [`crate::ChannelStateStore`] in shape. Unlike that store, the
/// ledger can re-converge from any partial loss: per ADR 003 §Corrupted
/// delivery the deterrent is statistical, not cryptographic. The runtime-side
/// impl (deferred follow-up alongside #317) may therefore reasonably
/// implement persistence as periodic snapshots rather than per-update fsync
/// — the trait contract permits both.
pub trait ClientReputationStore: Send + Sync {
    /// Load every persisted entry. Called once during node bring-up to
    /// hydrate the ledger before the admission path starts.
    ///
    /// Implementations SHOULD silently skip entries whose payload is
    /// logically invalid (e.g., `completion_ratio` non-finite or outside
    /// `[0.0, 1.0]`) rather than failing the load — the ledger tolerates
    /// partial loss per ADR 003 §Corrupted delivery. The hydration
    /// performed by [`ClientReputationLedger::from_snapshot`] applies the
    /// same skip rule, so an impl that surfaces invalid rows here would
    /// be erroring out work the ledger is about to discard anyway.
    /// Reserve [`StoreError`] for *backend*-level failures (truncated
    /// frame, unreadable file, malformed magic) where no recovery is
    /// possible.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] if the backing store is unreadable or
    /// structurally corrupt at the byte / framing level. Logically-
    /// invalid rows MUST NOT cause a load failure.
    fn load_all(&self) -> Result<Vec<(Address, ClientReputation)>, StoreError>;

    /// Persist the reputation row for one client.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] if the write fails.
    fn record(&self, client: Address, state: &ClientReputation) -> Result<(), StoreError>;

    /// Drop the persisted entry for `client`. A no-op if absent.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] if the underlying delete fails.
    fn forget(&self, client: Address) -> Result<(), StoreError>;
}

/// In-memory [`ClientReputationStore`] for tests and the trait's reference
/// semantics. Not durable — drops with the process. The redb-backed runtime
/// implementation lands with the `cdn/client/v1` wiring (#317 follow-up).
#[derive(Debug, Default)]
pub struct MemoryClientReputationStore {
    inner: Mutex<HashMap<Address, ClientReputation>>,
}

impl MemoryClientReputationStore {
    /// Construct an empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot the current entry count. A poisoned mutex reports `0` rather
    /// than panicking — tests on this store should already have failed
    /// louder if a panic poisoned the lock.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.lock().map_or(0, |m| m.len())
    }

    /// `true` when no clients are tracked.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl ClientReputationStore for MemoryClientReputationStore {
    fn load_all(&self) -> Result<Vec<(Address, ClientReputation)>, StoreError> {
        let guard = self
            .inner
            .lock()
            .map_err(|err| StoreError::Backend(format!("memory store mutex poisoned: {err}")))?;
        // Match the trait's SHOULD-skip contract: drop rows whose
        // `completion_ratio` is non-finite or outside `[0.0, 1.0]`. In
        // the memory impl this is mostly defensive (the only writer is
        // `record`), but matching the contract here keeps the reference
        // impl self-consistent — future redb impl reviewers see the
        // shape they're meant to mirror.
        Ok(guard
            .iter()
            .filter(|(_, rep)| {
                rep.completion_ratio.is_finite() && (0.0..=1.0).contains(&rep.completion_ratio)
            })
            .map(|(k, v)| (*k, *v))
            .collect())
    }

    fn record(&self, client: Address, state: &ClientReputation) -> Result<(), StoreError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|err| StoreError::Backend(format!("memory store mutex poisoned: {err}")))?;
        guard.insert(client, *state);
        Ok(())
    }

    fn forget(&self, client: Address) -> Result<(), StoreError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|err| StoreError::Backend(format!("memory store mutex poisoned: {err}")))?;
        guard.remove(&client);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::address;
    use anyhow::{Context, ensure};
    use std::sync::Arc;
    use std::thread;

    fn approx(a: f64, b: f64) -> bool {
        (a - b).abs() < 1e-9
    }

    fn client_a() -> Address {
        address!("00000000000000000000000000000000000000aa")
    }

    fn client_b() -> Address {
        address!("00000000000000000000000000000000000000bb")
    }

    fn ledger() -> anyhow::Result<ClientReputationLedger> {
        Ok(ClientReputationLedger::new(
            ClientReputationConfig::default(),
        )?)
    }

    #[test]
    fn default_config_matches_plan() -> anyhow::Result<()> {
        let c = ClientReputationConfig::default();
        ensure!(approx(c.alpha, 0.1), "alpha drift: {}", c.alpha);
        ensure!(approx(c.initial_score, 0.5));
        ensure!(approx(c.accept_threshold, 0.70));
        ensure!(approx(c.reject_threshold, 0.30));
        ensure!(c.new_client_cap_bytes == 1 << 30);
        ensure!(c.borderline_cap_bytes == 1 << 30);
        Ok(())
    }

    #[test]
    fn invalid_config_rejected() {
        let bad = ClientReputationConfig {
            alpha: f64::NAN,
            ..ClientReputationConfig::default()
        };
        assert!(matches!(
            ClientReputationLedger::new(bad),
            Err(ConfigError::OutOfUnitInterval { field: "alpha", .. })
        ));

        let bad = ClientReputationConfig {
            initial_score: 1.5,
            ..ClientReputationConfig::default()
        };
        assert!(matches!(
            ClientReputationLedger::new(bad),
            Err(ConfigError::OutOfUnitInterval {
                field: "initial_score",
                ..
            })
        ));

        let bad = ClientReputationConfig {
            accept_threshold: 0.5,
            reject_threshold: 0.5,
            ..ClientReputationConfig::default()
        };
        assert!(matches!(
            ClientReputationLedger::new(bad),
            Err(ConfigError::ThresholdOrdering { .. })
        ));

        let bad = ClientReputationConfig {
            accept_threshold: 0.3,
            reject_threshold: 0.7,
            ..ClientReputationConfig::default()
        };
        assert!(matches!(
            ClientReputationLedger::new(bad),
            Err(ConfigError::ThresholdOrdering { .. })
        ));

        // `alpha == 0.0` passes the closed-interval unit check but is
        // documented as `(0.0, 1.0]` — a zero alpha freezes the EWMA.
        let bad = ClientReputationConfig {
            alpha: 0.0,
            ..ClientReputationConfig::default()
        };
        assert!(matches!(
            ClientReputationLedger::new(bad),
            Err(ConfigError::AlphaCannotBeZero)
        ));
    }

    #[test]
    fn unseen_client_admits_capped_with_new_client_cap() -> anyhow::Result<()> {
        let l = ledger()?;
        match l.admit(client_a()) {
            Admission::AcceptCapped { byte_cap } => ensure!(byte_cap == 1 << 30),
            other => anyhow::bail!("expected AcceptCapped for unseen, got {other:?}"),
        }
        ensure!(l.score(client_a()).is_none());
        Ok(())
    }

    #[test]
    fn received_voucher_pulls_score_up() -> anyhow::Result<()> {
        let l = ledger()?;
        // initial 0.5; alpha 0.1; sample 1.0 → 0.9*0.5 + 0.1*1.0 = 0.55
        let next = l.record_voucher_received(client_a(), 1024);
        ensure!(approx(next, 0.55), "got {next}");
        Ok(())
    }

    #[test]
    fn withheld_voucher_pulls_score_down() -> anyhow::Result<()> {
        let l = ledger()?;
        // initial 0.5; alpha 0.1; sample 0.0 → 0.9*0.5 + 0.1*0.0 = 0.45
        let next = l.record_voucher_withheld(client_a(), 1024);
        ensure!(approx(next, 0.45), "got {next}");
        Ok(())
    }

    #[test]
    fn record_returns_post_update_score_not_prev() -> anyhow::Result<()> {
        let l = ledger()?;
        let returned = l.record_voucher_withheld(client_a(), 0);
        let queried = l.score(client_a()).context("client missing after record")?;
        ensure!(approx(returned, queried));
        // Pin the value so a buggy return-prev would not slip through the
        // returned == queried tautology.
        ensure!(approx(returned, 0.45), "expected 0.45, got {returned}");
        Ok(())
    }

    #[test]
    fn many_received_drive_score_toward_one() -> anyhow::Result<()> {
        let l = ledger()?;
        for _ in 0..200 {
            l.record_voucher_received(client_a(), 1);
        }
        let s = l.score(client_a()).context("client missing")?;
        ensure!(s > 0.99, "expected near 1.0, got {s}");
        Ok(())
    }

    #[test]
    fn many_withheld_drive_score_toward_zero() -> anyhow::Result<()> {
        let l = ledger()?;
        for _ in 0..200 {
            l.record_voucher_withheld(client_a(), 1);
        }
        let s = l.score(client_a()).context("client missing")?;
        ensure!(s < 0.01, "expected near 0.0, got {s}");
        Ok(())
    }

    #[test]
    fn admission_at_three_tiers_with_seeded_scores() -> anyhow::Result<()> {
        // Seed three clients straddling the default 0.30 / 0.70 thresholds
        // via `from_snapshot`, then assert each tier resolves correctly.
        let trusted = address!("0000000000000000000000000000000000000001");
        let borderline = address!("0000000000000000000000000000000000000002");
        let rejected = address!("0000000000000000000000000000000000000003");
        let entries = [
            (
                trusted,
                ClientReputation {
                    completion_ratio: 0.9,
                    event_count: 50,
                    total_bytes_delivered: 1_000_000,
                    last_seen_us: 1,
                },
            ),
            (
                borderline,
                ClientReputation {
                    completion_ratio: 0.5,
                    event_count: 10,
                    total_bytes_delivered: 100_000,
                    last_seen_us: 1,
                },
            ),
            (
                rejected,
                ClientReputation {
                    completion_ratio: 0.1,
                    event_count: 20,
                    total_bytes_delivered: 50_000,
                    last_seen_us: 1,
                },
            ),
        ];
        let l = ClientReputationLedger::from_snapshot(
            ClientReputationConfig::default(),
            entries.iter().copied(),
        )?;
        ensure!(matches!(l.admit(trusted), Admission::Accept));
        match l.admit(borderline) {
            Admission::AcceptCapped { byte_cap } => ensure!(byte_cap == 1 << 30),
            other => anyhow::bail!("expected AcceptCapped for borderline, got {other:?}"),
        }
        ensure!(matches!(l.admit(rejected), Admission::Reject));
        Ok(())
    }

    #[test]
    fn admission_exact_boundary_inclusive_on_accept_and_reject() -> anyhow::Result<()> {
        // The decision logic uses `>=` for both thresholds: a score sitting
        // exactly on accept_threshold is Accept; one exactly on
        // reject_threshold is AcceptCapped (not Reject). Lock this in.
        // event_count must be >0 — a zero-event row is treated as unseen
        // (see `default_constructed_entry_treated_as_unseen`).
        let on_accept = address!("0000000000000000000000000000000000000004");
        let on_reject = address!("0000000000000000000000000000000000000005");
        let entries = [
            (
                on_accept,
                ClientReputation {
                    completion_ratio: 0.70,
                    event_count: 1,
                    ..ClientReputation::default()
                },
            ),
            (
                on_reject,
                ClientReputation {
                    completion_ratio: 0.30,
                    event_count: 1,
                    ..ClientReputation::default()
                },
            ),
        ];
        let l = ClientReputationLedger::from_snapshot(
            ClientReputationConfig::default(),
            entries.iter().copied(),
        )?;
        ensure!(matches!(l.admit(on_accept), Admission::Accept));
        ensure!(matches!(l.admit(on_reject), Admission::AcceptCapped { .. }));
        Ok(())
    }

    #[test]
    fn default_constructed_entry_treated_as_unseen() -> anyhow::Result<()> {
        // A persistence layer may round-trip a default-constructed entry
        // (event_count == 0) — `admit` should treat it as admission-
        // equivalent to a never-recorded client, NOT a borderline-tier
        // client whose score happens to equal `initial_score`. The
        // distinction is observable when `new_client_cap_bytes` differs
        // from `borderline_cap_bytes`.
        let cfg = ClientReputationConfig {
            new_client_cap_bytes: 100,
            borderline_cap_bytes: 200,
            ..ClientReputationConfig::default()
        };
        let l = ClientReputationLedger::from_snapshot(
            cfg,
            std::iter::once((client_a(), ClientReputation::default())),
        )?;
        match l.admit(client_a()) {
            Admission::AcceptCapped { byte_cap: 100 } => Ok(()),
            other => anyhow::bail!("expected AcceptCapped {{ byte_cap: 100 }}, got {other:?}"),
        }
    }

    #[test]
    fn independent_clients_tracked_separately() -> anyhow::Result<()> {
        let l = ledger()?;
        for _ in 0..100 {
            l.record_voucher_received(client_a(), 1);
            l.record_voucher_withheld(client_b(), 1);
        }
        let good = l.score(client_a()).context("a missing")?;
        let bad = l.score(client_b()).context("b missing")?;
        ensure!(good > 0.95 && bad < 0.05, "good={good} bad={bad}");
        ensure!(matches!(l.admit(client_a()), Admission::Accept));
        ensure!(matches!(l.admit(client_b()), Admission::Reject));
        Ok(())
    }

    #[test]
    fn entry_reports_event_count_and_bytes() -> anyhow::Result<()> {
        let l = ledger()?;
        l.record_voucher_received(client_a(), 1_000);
        l.record_voucher_withheld(client_a(), 2_000);
        l.record_voucher_received(client_a(), 4_000);
        let e = l.entry(client_a()).context("entry missing")?;
        ensure!(e.event_count == 3, "got {}", e.event_count);
        ensure!(
            e.total_bytes_delivered == 7_000,
            "got {}",
            e.total_bytes_delivered
        );
        ensure!(e.last_seen_us > 0, "last_seen_us still 0");
        Ok(())
    }

    #[test]
    fn snapshot_contains_every_observed_client() -> anyhow::Result<()> {
        let l = ledger()?;
        l.record_voucher_received(client_a(), 1);
        l.record_voucher_withheld(client_b(), 1);
        let snap: HashMap<Address, ClientReputation> = l.snapshot().into_iter().collect();
        ensure!(snap.len() == 2, "expected 2 entries, got {}", snap.len());
        ensure!(snap.contains_key(&client_a()));
        ensure!(snap.contains_key(&client_b()));
        Ok(())
    }

    #[test]
    fn memory_store_round_trip() -> anyhow::Result<()> {
        let s = MemoryClientReputationStore::new();
        let a = ClientReputation {
            completion_ratio: 0.42,
            event_count: 7,
            total_bytes_delivered: 100,
            last_seen_us: 1_234,
        };
        let b = ClientReputation {
            completion_ratio: 0.87,
            event_count: 3,
            total_bytes_delivered: 50,
            last_seen_us: 5_678,
        };
        s.record(client_a(), &a)?;
        s.record(client_b(), &b)?;
        ensure!(s.len() == 2);
        let mut all = s.load_all()?;
        all.sort_by_key(|(addr, _)| *addr);
        let first = all.first().context("missing [0]")?;
        let second = all.get(1).context("missing [1]")?;
        ensure!(*first == (client_a(), a) || *first == (client_b(), b));
        ensure!(*second == (client_a(), a) || *second == (client_b(), b));
        ensure!(first != second);
        Ok(())
    }

    #[test]
    fn memory_store_record_overwrites() -> anyhow::Result<()> {
        let s = MemoryClientReputationStore::new();
        let mut rep = ClientReputation {
            completion_ratio: 0.1,
            event_count: 1,
            total_bytes_delivered: 1,
            last_seen_us: 1,
        };
        s.record(client_a(), &rep)?;
        rep.completion_ratio = 0.9;
        s.record(client_a(), &rep)?;
        let all = s.load_all()?;
        ensure!(all.len() == 1);
        let only = all.first().context("missing [0]")?;
        ensure!(approx(only.1.completion_ratio, 0.9));
        Ok(())
    }

    #[test]
    fn memory_store_forget_removes_entry() -> anyhow::Result<()> {
        let s = MemoryClientReputationStore::new();
        s.record(client_a(), &ClientReputation::default())?;
        s.forget(client_a())?;
        ensure!(s.is_empty());
        // Forgetting an absent client is a no-op.
        s.forget(client_b())?;
        Ok(())
    }

    #[test]
    fn from_snapshot_skips_invalid_completion_ratio() -> anyhow::Result<()> {
        let ok = client_a();
        let nan_row = client_b();
        let out_of_range = address!("0000000000000000000000000000000000000099");
        let entries = [
            (
                ok,
                ClientReputation {
                    completion_ratio: 0.6,
                    ..ClientReputation::default()
                },
            ),
            (
                nan_row,
                ClientReputation {
                    completion_ratio: f64::NAN,
                    ..ClientReputation::default()
                },
            ),
            (
                out_of_range,
                ClientReputation {
                    completion_ratio: 2.0,
                    ..ClientReputation::default()
                },
            ),
        ];
        let l = ClientReputationLedger::from_snapshot(
            ClientReputationConfig::default(),
            entries.iter().copied(),
        )?;
        ensure!(l.score(ok).is_some());
        ensure!(l.score(nan_row).is_none(), "NaN row leaked through");
        ensure!(l.score(out_of_range).is_none(), "out-of-range row leaked");
        Ok(())
    }

    #[test]
    fn concurrent_record_does_not_lose_updates() -> anyhow::Result<()> {
        // Eight threads each fold 1000 withhold events. The load-bearing
        // assertion is `event_count == 8_000`: under a torn
        // read-modify-write `saturating_add` would skip increments and
        // the count would drift below 8000 — that's the strict invariant
        // a lost update breaks. The EWMA-decay check (`score < 1e-4`)
        // is a softer secondary signal: 0.5 * 0.9^8000 ≈ 10^-366 is so
        // steep that a partial torn write (e.g. one missed event in
        // 8000) would still saturate `score` to ~10^-365, easily under
        // 1e-4 — so on its own it would fail to detect anything but a
        // catastrophic loss. Keep both; the count is what actually
        // catches races.
        let l = Arc::new(ledger()?);
        let mut handles = Vec::new();
        for _ in 0..8 {
            let l = Arc::clone(&l);
            handles.push(thread::spawn(move || {
                for _ in 0..1000 {
                    l.record_voucher_withheld(client_a(), 0);
                    let _ = l.admit(client_a());
                }
            }));
        }
        for h in handles {
            h.join()
                .map_err(|_| anyhow::anyhow!("worker thread panicked"))?;
        }
        // Headline: every increment must land.
        let entry = l.entry(client_a()).context("client missing")?;
        ensure!(
            entry.event_count == 8_000,
            "lost update: event_count = {} (expected 8000)",
            entry.event_count
        );
        // Sanity: score in range and EWMA reached the expected decay regime.
        let s = entry.completion_ratio;
        ensure!((0.0..=1.0).contains(&s), "score out of range: {s}");
        ensure!(s < 1e-4, "EWMA failed to converge: {s}");
        ensure!(
            l.snapshot().len() == 1,
            "expected one entry, got {}",
            l.snapshot().len()
        );
        Ok(())
    }

    #[test]
    fn alpha_one_replaces_score_with_sample() -> anyhow::Result<()> {
        // alpha = 1.0 collapses the fold to next = sample. A refactor
        // that re-introduced any inertia (e.g. `(1.0 - alpha).max(eps)`)
        // would silently break this boundary; pin it explicitly.
        let cfg = ClientReputationConfig {
            alpha: 1.0,
            ..ClientReputationConfig::default()
        };
        let l = ClientReputationLedger::new(cfg)?;
        ensure!(approx(l.record_voucher_received(client_a(), 0), 1.0));
        ensure!(approx(l.record_voucher_withheld(client_a(), 0), 0.0));
        ensure!(approx(l.record_voucher_received(client_a(), 0), 1.0));
        Ok(())
    }

    #[test]
    fn accept_threshold_one_is_valid_config() -> anyhow::Result<()> {
        // `require_unit_interval` admits 1.0; the resulting config makes
        // Accept reachable only at score == 1.0. Verify the boundary
        // config constructs cleanly.
        let cfg = ClientReputationConfig {
            accept_threshold: 1.0,
            reject_threshold: 0.999,
            ..ClientReputationConfig::default()
        };
        ClientReputationLedger::new(cfg)?;
        Ok(())
    }

    #[test]
    fn zero_cap_config_rejected() {
        let bad = ClientReputationConfig {
            new_client_cap_bytes: 0,
            ..ClientReputationConfig::default()
        };
        assert!(matches!(
            ClientReputationLedger::new(bad),
            Err(ConfigError::ZeroCap {
                field: "new_client_cap_bytes"
            })
        ));

        let bad = ClientReputationConfig {
            borderline_cap_bytes: 0,
            ..ClientReputationConfig::default()
        };
        assert!(matches!(
            ClientReputationLedger::new(bad),
            Err(ConfigError::ZeroCap {
                field: "borderline_cap_bytes"
            })
        ));
    }

    #[test]
    fn from_snapshot_skips_infinity_and_negative_completion_ratio() -> anyhow::Result<()> {
        // Extend the corruption-skip coverage beyond NaN and 2.0:
        // INFINITY, NEG_INFINITY, and a strictly-negative value should
        // all be dropped by the same `is_finite() && in [0,1]` predicate.
        let inf = address!("0000000000000000000000000000000000000010");
        let neg_inf = address!("0000000000000000000000000000000000000011");
        let negative = address!("0000000000000000000000000000000000000012");
        let neg_zero = address!("0000000000000000000000000000000000000013");
        let entries = [
            (
                inf,
                ClientReputation {
                    completion_ratio: f64::INFINITY,
                    ..ClientReputation::default()
                },
            ),
            (
                neg_inf,
                ClientReputation {
                    completion_ratio: f64::NEG_INFINITY,
                    ..ClientReputation::default()
                },
            ),
            (
                negative,
                ClientReputation {
                    completion_ratio: -0.1,
                    ..ClientReputation::default()
                },
            ),
            (
                // `-0.0` is finite and `(0.0..=1.0).contains(&-0.0) == true`
                // (negative zero compares equal to positive zero), so it
                // should NOT be skipped — pinned here so a future
                // predicate change doesn't quietly start filtering it.
                neg_zero,
                ClientReputation {
                    completion_ratio: -0.0,
                    event_count: 1,
                    ..ClientReputation::default()
                },
            ),
        ];
        let l = ClientReputationLedger::from_snapshot(
            ClientReputationConfig::default(),
            entries.iter().copied(),
        )?;
        ensure!(l.score(inf).is_none(), "INFINITY leaked through");
        ensure!(l.score(neg_inf).is_none(), "NEG_INFINITY leaked through");
        ensure!(l.score(negative).is_none(), "-0.1 leaked through");
        ensure!(l.score(neg_zero).is_some(), "-0.0 should NOT be skipped");
        Ok(())
    }

    #[test]
    fn from_snapshot_empty_iterator_yields_empty_ledger() -> anyhow::Result<()> {
        // First-boot / empty-store path. Trivial structurally, but the
        // realistic case for any fresh node coming up.
        let l = ClientReputationLedger::from_snapshot(
            ClientReputationConfig::default(),
            std::iter::empty(),
        )?;
        ensure!(l.snapshot().is_empty());
        ensure!(l.score(client_a()).is_none());
        // And the unseen-admission shape still holds.
        ensure!(matches!(
            l.admit(client_a()),
            Admission::AcceptCapped { .. }
        ));
        Ok(())
    }

    #[test]
    fn counters_saturate_instead_of_wrapping() -> anyhow::Result<()> {
        // Seed an entry one short of `u64::MAX` on both saturating
        // counters; record a single event; confirm both saturate to
        // `u64::MAX` without wrapping. Without `saturating_add` a wrap
        // here would produce `0`, breaking every counter-derived
        // operator metric forever.
        let near_max = u64::MAX - 1;
        let entries = [(
            client_a(),
            ClientReputation {
                completion_ratio: 0.5,
                event_count: near_max,
                total_bytes_delivered: near_max,
                last_seen_us: 1,
            },
        )];
        let l = ClientReputationLedger::from_snapshot(
            ClientReputationConfig::default(),
            entries.iter().copied(),
        )?;
        // One event pushes to MAX; a second proves the saturation holds.
        l.record_voucher_received(client_a(), 2);
        l.record_voucher_received(client_a(), 2);
        let e = l.entry(client_a()).context("entry missing")?;
        ensure!(e.event_count == u64::MAX, "got {}", e.event_count);
        ensure!(
            e.total_bytes_delivered == u64::MAX,
            "got {}",
            e.total_bytes_delivered
        );
        Ok(())
    }

    #[test]
    fn memory_store_load_all_filters_invalid_rows() -> anyhow::Result<()> {
        // The trait doc says load_all SHOULD skip logically-invalid rows
        // — the memory impl must honour that contract so the reference
        // impl matches the shape the redb impl will mirror.
        let s = MemoryClientReputationStore::new();
        let bad = ClientReputation {
            completion_ratio: f64::NAN,
            event_count: 1,
            total_bytes_delivered: 1,
            last_seen_us: 1,
        };
        let good = ClientReputation {
            completion_ratio: 0.7,
            event_count: 1,
            total_bytes_delivered: 1,
            last_seen_us: 1,
        };
        s.record(client_a(), &bad)?;
        s.record(client_b(), &good)?;
        let loaded = s.load_all()?;
        ensure!(
            loaded.len() == 1,
            "expected 1 row loaded, got {}",
            loaded.len()
        );
        ensure!(loaded.first().context("empty")?.0 == client_b());
        Ok(())
    }

    fn addr_from_index(i: u64) -> Address {
        let mut bytes = [0u8; 20];
        bytes[12..20].copy_from_slice(&i.to_be_bytes());
        Address::from(bytes)
    }

    #[test]
    fn lru_eviction_bounds_growth_under_flood() -> anyhow::Result<()> {
        // Cap-sized ledger flooded with cap+50 distinct clients must stay
        // at exactly `cap` entries; the evictions counter records the
        // overflow. Default cap (100k) would be too slow for a unit test
        // so we build a small-cap ledger explicitly.
        let cfg = ClientReputationConfig {
            max_tracked_clients: 32,
            ..ClientReputationConfig::default()
        };
        let l = ClientReputationLedger::new(cfg)?;
        let flood: u64 = 32 + 50;
        for i in 0..flood {
            l.record_voucher_received(addr_from_index(i), 1);
        }
        ensure!(
            l.tracked_clients() == 32,
            "tracked_clients = {} (expected 32)",
            l.tracked_clients()
        );
        ensure!(
            l.evictions_total() == 50,
            "evictions_total = {} (expected 50)",
            l.evictions_total()
        );
        Ok(())
    }

    #[test]
    fn lru_eviction_drops_oldest_entry() -> anyhow::Result<()> {
        // Three clients seeded with strictly ascending `last_seen_us`;
        // a fourth at-cap insertion must evict the oldest. `from_snapshot`
        // pins the timestamps deterministically — calling `record_*` in
        // sequence would yield adjacent microsecond-bucket timestamps on
        // fast machines and the test would be flaky.
        let cfg = ClientReputationConfig {
            max_tracked_clients: 3,
            ..ClientReputationConfig::default()
        };
        let oldest_addr = address!("00000000000000000000000000000000000000a1");
        let mid_addr = address!("00000000000000000000000000000000000000a2");
        let newer_addr = address!("00000000000000000000000000000000000000a3");
        let entries = [
            (
                oldest_addr,
                ClientReputation {
                    completion_ratio: 0.9,
                    event_count: 1,
                    total_bytes_delivered: 1,
                    last_seen_us: 100,
                },
            ),
            (
                mid_addr,
                ClientReputation {
                    completion_ratio: 0.8,
                    event_count: 1,
                    total_bytes_delivered: 1,
                    last_seen_us: 200,
                },
            ),
            (
                newer_addr,
                ClientReputation {
                    completion_ratio: 0.7,
                    event_count: 1,
                    total_bytes_delivered: 1,
                    last_seen_us: 300,
                },
            ),
        ];
        let l = ClientReputationLedger::from_snapshot(cfg, entries.iter().copied())?;
        ensure!(l.tracked_clients() == 3);
        let arrival = address!("00000000000000000000000000000000000000a4");
        l.record_voucher_received(arrival, 1);
        ensure!(l.tracked_clients() == 3);
        ensure!(l.evictions_total() == 1);
        ensure!(
            l.score(oldest_addr).is_none(),
            "oldest entry should have been evicted"
        );
        ensure!(
            l.score(mid_addr).is_some(),
            "mid entry should still be present"
        );
        ensure!(
            l.score(newer_addr).is_some(),
            "newer entry should still be present"
        );
        ensure!(l.score(arrival).is_some(), "new arrival should be tracked");
        // EWMA state preserved for survivors.
        ensure!(approx(l.score(mid_addr).context("mid missing")?, 0.8));
        ensure!(approx(l.score(newer_addr).context("newer missing")?, 0.7));
        Ok(())
    }

    #[test]
    fn max_tracked_clients_zero_is_unbounded() -> anyhow::Result<()> {
        // `0` matches the `max_tracked_sources = 0` convention in
        // `decdn-common`: opt out of the cap, never evict.
        let cfg = ClientReputationConfig {
            max_tracked_clients: 0,
            ..ClientReputationConfig::default()
        };
        let l = ClientReputationLedger::new(cfg)?;
        for i in 0..1000u64 {
            l.record_voucher_received(addr_from_index(i), 1);
        }
        ensure!(
            l.tracked_clients() == 1000,
            "tracked_clients = {} (expected 1000)",
            l.tracked_clients()
        );
        ensure!(
            l.evictions_total() == 0,
            "evictions_total = {} (expected 0)",
            l.evictions_total()
        );
        Ok(())
    }

    #[test]
    fn from_snapshot_enforces_cap_keeping_newest_by_last_seen_us() -> anyhow::Result<()> {
        // Hydrate cap+2 rows; the two with smallest `last_seen_us` must
        // be dropped, and `evictions_total` should report the bulk
        // truncation so the operator metric reflects it. The cap is
        // documented as a hard invariant; without this guarantee a
        // torn store could re-introduce the unbounded-map shape this
        // PR exists to prevent.
        let cfg = ClientReputationConfig {
            max_tracked_clients: 3,
            ..ClientReputationConfig::default()
        };
        let entries: Vec<(Address, ClientReputation)> = (0..5u64)
            .map(|i| {
                (
                    addr_from_index(i),
                    ClientReputation {
                        completion_ratio: 0.5,
                        event_count: 1,
                        total_bytes_delivered: 1,
                        last_seen_us: 100 + i,
                    },
                )
            })
            .collect();
        let l = ClientReputationLedger::from_snapshot(cfg, entries.iter().copied())?;
        ensure!(
            l.tracked_clients() == 3,
            "tracked_clients = {} (expected 3)",
            l.tracked_clients()
        );
        ensure!(
            l.evictions_total() == 2,
            "evictions_total = {} (expected 2)",
            l.evictions_total()
        );
        // The two oldest are gone; the three newest survive with their
        // EWMA state intact.
        ensure!(
            l.score(addr_from_index(0)).is_none(),
            "idx 0 should be dropped"
        );
        ensure!(
            l.score(addr_from_index(1)).is_none(),
            "idx 1 should be dropped"
        );
        ensure!(
            l.score(addr_from_index(2)).is_some(),
            "idx 2 should survive"
        );
        ensure!(
            l.score(addr_from_index(3)).is_some(),
            "idx 3 should survive"
        );
        ensure!(
            l.score(addr_from_index(4)).is_some(),
            "idx 4 should survive"
        );
        Ok(())
    }

    #[test]
    fn from_snapshot_zero_cap_skips_hydration_truncation() -> anyhow::Result<()> {
        // `max_tracked_clients == 0` opts out of the bound, so hydration
        // must keep every (valid) row regardless of count.
        let cfg = ClientReputationConfig {
            max_tracked_clients: 0,
            ..ClientReputationConfig::default()
        };
        let entries: Vec<(Address, ClientReputation)> = (0..10u64)
            .map(|i| {
                (
                    addr_from_index(i),
                    ClientReputation {
                        completion_ratio: 0.5,
                        event_count: 1,
                        total_bytes_delivered: 1,
                        last_seen_us: 100 + i,
                    },
                )
            })
            .collect();
        let l = ClientReputationLedger::from_snapshot(cfg, entries.iter().copied())?;
        ensure!(l.tracked_clients() == 10);
        ensure!(l.evictions_total() == 0);
        Ok(())
    }

    #[test]
    fn max_tracked_clients_one_evicts_on_every_new_address() -> anyhow::Result<()> {
        // cap=1: each new address evicts the previous one. Repeated
        // updates to the surviving client must NOT evict — that's the
        // `is_new` gate, and a regression that triggered eviction on
        // every update would destroy EWMA history for the only tracked
        // client.
        let cfg = ClientReputationConfig {
            max_tracked_clients: 1,
            ..ClientReputationConfig::default()
        };
        let l = ClientReputationLedger::new(cfg)?;
        for i in 0..5u64 {
            let a = addr_from_index(i);
            l.record_voucher_received(a, 1);
            ensure!(
                l.tracked_clients() == 1,
                "tracked_clients = {} at step {} (expected 1)",
                l.tracked_clients(),
                i
            );
            ensure!(
                l.score(a).is_some(),
                "fresh client missing immediately after insert at step {i}"
            );
        }
        // Four evictions: addrs 0..=3 displaced by the time addr 4 lands.
        ensure!(
            l.evictions_total() == 4,
            "evictions_total = {} (expected 4)",
            l.evictions_total()
        );
        // Existing-client updates must not bump the eviction counter.
        let survivor = addr_from_index(4);
        let evictions_before = l.evictions_total();
        for _ in 0..10 {
            l.record_voucher_received(survivor, 1);
        }
        ensure!(
            l.evictions_total() == evictions_before,
            "evictions_total bumped on existing-client updates: {} -> {}",
            evictions_before,
            l.evictions_total()
        );
        ensure!(l.tracked_clients() == 1);
        Ok(())
    }
}
