//! Windowing + reorg-margin primitives shared by every on-chain watcher.
//!
//! The `multiplexed_poller` loop walks each route's `[cursor, head]` gap in
//! bounded `eth_getLogs` windows and rewinds a shallow reorg margin on resume.
//! The windowing itself lives in [`decdn_incentive::log_scan`], so the CLI's
//! one-shot scans learn a provider's range cap the same way the poller does;
//! this module re-exports it and adds the reorg margin, which only a watcher
//! that resumes a durable cursor needs (#1092).
//!
//! Every watcher shares the live-tail windowing. Only the settlement watcher
//! resumes a durable cursor, so only it uses the reorg rewind. The others seed
//! their cursor at a pinned enumeration block (blacklist, slash, staker set) or
//! at head (fee shares) and persist nothing.

pub(crate) use decdn_incentive::log_scan::{Shrunk, WindowSpan, is_range_rejection, window_end};

/// How many blocks the persisted scan checkpoint is rewound before the
/// resume backfill (#751), absorbing a shallow reorg between the last scanned
/// block and the next boot: a `PoolRedeemed` or `PoolReclaimed` re-mined at a
/// slightly different height after a reorg is still inside the rescanned
/// window. Those are the folds with durable effects — the paid watermark on the
/// lane row, and the forgotten lanes. Both folds are absolute (set the paid
/// cumulative, drop the pool's lanes) and so idempotent, so the only cost of
/// the margin is a few extra blocks of `eth_getLogs`. Sized for the
/// shallow reorgs of an Arbitrum-Sepolia-class L2.
///
/// The rewind applies only where a *durable* cursor is resumed — a
/// `HeadMinusWindow` start re-derives its floor from head on every boot, so
/// there is nothing to rewind.
///
/// One live consumer: the settlement watcher ([`crate::payment_settlement`], #751),
/// through [`super::resumable_watcher::CursorStart::FromCheckpoint`]'s `reorg_margin`.
/// The other watchers seed their tail at an enumeration block or at head and
/// persist nothing, so they have no cursor to rewind; a reorg below that block
/// is corrected by the next boot's enumeration.
pub(crate) const REORG_MARGIN_BLOCKS: u64 = 128;
