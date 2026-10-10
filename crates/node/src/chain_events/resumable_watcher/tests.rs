use super::*;

// The production reorg rewind, exercised by the persisted-cursor cases.
const MARGIN: u64 = crate::chain_events::REORG_MARGIN_BLOCKS;

#[test]
fn persisted_none_starts_at_head() {
    // Cold store: nothing predates the node → scan essentially nothing.
    assert_eq!(resolve_persisted_start(None, 1_000, 0, MARGIN), 1_000);
}

#[test]
fn persisted_none_starts_at_head_ignoring_from_block() {
    // The cold-store floor is head, not the deploy block — a persisted-cursor
    // watcher has no history to replay before its own first lane.
    assert_eq!(resolve_persisted_start(None, 1_000, 200, MARGIN), 1_000);
}

#[test]
fn persisted_some_rewinds_by_margin() {
    assert_eq!(
        resolve_persisted_start(Some(10_000), 20_000, 0, MARGIN),
        10_000 - MARGIN
    );
}

#[test]
fn persisted_some_floored_at_from_block() {
    // The rewound cursor never drops below the deploy floor.
    assert_eq!(resolve_persisted_start(Some(300), 20_000, 250, MARGIN), 250);
}

#[test]
fn persisted_some_clamped_to_head() {
    // A checkpoint ahead of a lagging head clamps to head, not an inverted range.
    assert_eq!(resolve_persisted_start(Some(20_000), 500, 0, MARGIN), 500);
}

#[test]
fn persisted_saturates_at_zero() {
    assert_eq!(resolve_persisted_start(Some(10), 1_000, 0, MARGIN), 0);
}

#[test]
fn head_window_zero_is_live_from_head() {
    // Enumeration-bootstrapped watchers: no backfill, start at head.
    assert_eq!(resolve_head_window_start(5_000, 0, 0), 5_000);
}

#[test]
fn head_window_bounds_recent_lookback() {
    assert_eq!(resolve_head_window_start(50_000, 10_000, 0), 40_000);
}

#[test]
fn head_window_floored_at_from_block() {
    // Slash: head - appeal_window never drops below the deploy floor.
    assert_eq!(resolve_head_window_start(1_000, 10_000, 200), 200);
}

#[test]
fn head_window_floor_above_head_clamps_to_head() {
    assert_eq!(resolve_head_window_start(100, 10, 5_000), 100);
}

#[test]
fn head_window_saturates_at_zero() {
    assert_eq!(resolve_head_window_start(500, 10_000, 0), 0);
}

// The variant → resolver wiring: each `CursorStart` must feed its own fields
// (and the config `from_block`) into the right pure resolver. The
// `resolve_*` tests above cover the arithmetic; these pin the hookup so a
// mis-wired variant (e.g. `HeadMinusWindow` resolving from the wrong floor)
// is caught.

/// `seed` pre-sets the cursor for a `Seeded` start and only that start;
/// every other start resolves its floor on the first tick (`seed` → `None`).
#[test]
fn seed_is_some_only_for_seeded() {
    assert_eq!(CursorStart::Seeded { at: 42 }.seed(), Some(42));
    assert_eq!(
        CursorStart::HeadMinusWindow { window_blocks: 10 }.seed(),
        None
    );
}

/// `HeadMinusWindow` resolves `head - window_blocks`, clamped to `from_block`
/// — pins that the variant feeds its own `window_blocks` and the config floor.
#[test]
fn head_minus_window_initial_from_subtracts_the_window() {
    let start = CursorStart::HeadMinusWindow {
        window_blocks: 1_000,
    };
    assert_eq!(start.initial_from(500, 20_000).ok(), Some(19_000));
}

/// The defensive `Seeded` arm of `initial_from` is unreachable in production
/// (`seed` pre-sets the cursor), but if reached it falls back to the deploy
/// floor rather than panicking.
#[test]
fn seeded_initial_from_defensive_fallback_is_the_deploy_floor() {
    let start = CursorStart::Seeded { at: 5_000 };
    assert_eq!(start.initial_from(500, 20_000).ok(), Some(500));
}

/// A store whose reads always fail, for the load-error policy cases.
struct FailingLoadStore;

impl KeyedCheckpointStore for FailingLoadStore {
    fn load_checkpoint(
        &self,
        _key: CheckpointKey,
    ) -> std::result::Result<Option<u64>, decdn_incentive::StoreError> {
        Err(decdn_incentive::StoreError::Backend("boom".into()))
    }

    fn record_checkpoint(
        &self,
        _key: CheckpointKey,
        _block: u64,
    ) -> std::result::Result<(), decdn_incentive::StoreError> {
        Ok(())
    }
}

/// Minimal in-memory durable store for the tick-loop tests.
#[derive(Default)]
struct MemoryCheckpointStore {
    stored: std::sync::Mutex<std::collections::HashMap<CheckpointKey, u64>>,
}

impl KeyedCheckpointStore for MemoryCheckpointStore {
    fn load_checkpoint(
        &self,
        key: CheckpointKey,
    ) -> std::result::Result<Option<u64>, decdn_incentive::StoreError> {
        Ok(self
            .stored
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&key)
            .copied())
    }

    fn record_checkpoint(
        &self,
        key: CheckpointKey,
        block: u64,
    ) -> std::result::Result<(), decdn_incentive::StoreError> {
        self.stored
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(key, block);
        Ok(())
    }
}

/// A store that counts `flush_checkpoint` calls.
///
/// `MemoryCheckpointStore` takes the trait's default no-op flush, so it
/// cannot observe one — and `KeyedCheckpointStore::flush_checkpoint`
/// defaulting to a no-op is exactly why a watcher whose flush never runs
/// looks fine in every other test.
fn persisted() -> CursorStart {
    CursorStart::FromCheckpoint {
        cold_start: ColdStart::Head,
        checkpoint: Checkpoint {
            store: Arc::new(FailingLoadStore),
            key: CheckpointKey::PoolOpened,
        },
        reorg_margin: MARGIN,
    }
}

/// The configured `reorg_margin` actually reaches `resolve_persisted_start`
/// — the *wiring*, not the arithmetic.
///
/// Nothing else covers this seam, which is why it exists. The
/// `resolve_persisted_start` tests call that pure fn directly, so they prove
/// the rewind *given* a margin. The `cursor_start` pins prove settlement's
/// production config *carries* `REORG_MARGIN_BLOCKS`. Neither proves
/// `initial_from` hands one to the other: hard-coding `0` at that call site
/// passed the entire suite. Two legs of three — the same shape that let
/// #1227 ship documented-but-disabled, one field over.
///
/// The other `FromCheckpoint` tests reach the `None` arm (a `FailingLoadStore`
/// or an empty store), which ignores the margin entirely; only a stored
/// `Some(checkpoint)` exercises the rewind. The three constants are mutually
/// distinct so an argument-order slip among `resolve_persisted_start`'s
/// consecutive `u64`s fails here too.
#[test]
fn persisted_initial_from_engages_the_configured_margin() {
    const CHECKPOINT: u64 = 10_000;
    const HEAD: u64 = 20_000;
    const FROM_BLOCK: u64 = 500;

    let store = Arc::new(MemoryCheckpointStore::default());
    let recorded = store.record_checkpoint(CheckpointKey::PoolOpened, CHECKPOINT);
    assert!(recorded.is_ok(), "seeding the checkpoint must succeed");
    let start = CursorStart::FromCheckpoint {
        cold_start: ColdStart::Head,
        checkpoint: Checkpoint {
            store,
            key: CheckpointKey::PoolOpened,
        },
        reorg_margin: MARGIN,
    };

    assert_eq!(
        start.initial_from(FROM_BLOCK, HEAD).unwrap_or(u64::MAX),
        CHECKPOINT - MARGIN,
        "a resumed floor must be rewound by the start's own reorg_margin"
    );
}

#[test]
fn load_error_is_retryable() {
    // A checkpoint read error must fail the tick into backoff so the read is
    // retried: falling back to head would durably overwrite the stored floor
    // and permanently discard the downtime gap (#751/#762).
    assert!(persisted().initial_from(0, 1_000).is_err());
}
