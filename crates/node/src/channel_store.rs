//! Disk-backed `PoolStateStore` for the node runtime.
//!
//! Implements [`PoolStateStore`] against a set of `redb` database files under
//! `<data_dir>`, one file per durable write family:
//!
//! - `lanes.redb` — seller lane frontier (`lane_state_v1`) and the owner-signed
//!   capability rows (`capability_v1`), which the flush commits in one
//!   transaction.
//! - `settle.redb` — the seller and buyer pending-settle sets.
//! - `floor-loss.redb` — per-signer abandonment-bucket snapshots and their forget
//!   tombstones, committed in one transaction so a `record_bucket` orders against
//!   a `forget_loss` on this file's writer slot.
//! - `checkpoint.redb` — the settlement watcher's scan checkpoints.
//! - `buyer.redb` — the buyer pool state and its owner index.
//!
//! Each file has its own `redb` write-transaction slot, so a commit in one
//! family never waits on an unrelated commit in another: the periodic lane
//! flush, a settlement checkpoint, a floor-loss write on abnormal stream end,
//! and a buyer top-up all proceed on independent writer slots. Families are
//! never written in one transaction — `redb` forbids a transaction spanning two
//! `Database` handles — so a crash between two family commits can leave them at
//! different watermarks, which every family already tolerates (each records and
//! recovers on its own terms).
//!
//! The lane table is buffered in memory: `open()`
//! hydrates the working set from disk, `record`/`forget` mutate that working
//! set only, and an explicit `flush()` call writes every dirty lane and
//! applies every tombstone in one fsynced commit (redb's default
//! [`redb::Durability::Immediate`], set explicitly here so a future redb
//! default change doesn't silently weaken the durability guarantee). A crash
//! between two flushes loses the unflushed lane advances; the caller decides
//! the flush cadence.
//!
//! The owner-signed **capability** table is buffered in the same shape:
//! `put_capability` mutates the in-memory set only (deduping an identical
//! repeat write), and the same `flush()` that lands the dirty lanes writes the
//! dirty capability rows in the same fsynced transaction. A capability present
//! only in the buffer is still served from `get_capability` (it reads the
//! buffer), so the redeemer needs the material only by its first redemption,
//! which the pre-redeem `flush_store_durable` floors on the strict path. The
//! forced close/shutdown path redeems even when that flush fails, so a
//! capability that only ever lived in the buffer can be missing from disk at
//! its first redemption; the residual is that a crash in that window leaves an
//! on-chain registration whose local material is gone.
//!
//! Losing an unflushed capability row on a crash is otherwise recoverable the
//! way the ADR 003 voucher frontier is, but by a different mechanism and so on
//! its own terms: a lost frontier is superseded by the signer's next, higher
//! voucher, while a lost capability row is restored only because the client
//! re-sends the capability on its next request and intake re-persists it.
//!
//! See [`decdn_incentive::store`] for the trait contract and
//! [ADR 003 §Off-chain voucher state persistence] for the protocol rule
//! this implements: a node MUST advance `(last_amount, last_bytes_delivered)`
//! for the lane before it continues delivery, and mirror the advance to disk on
//! a background timer. Without the on-disk mirror a restart re-opens the lane at
//! amount zero and a client can replay a previously-accepted voucher for a
//! second byte delivery (issue #527).
//!
//! The record persists the latest voucher's signature (so the seller
//! redemption path can submit it to the on-chain `PaymentPool.redeem` after a
//! restart without forfeiting the claim), the capability's spending cap, and
//! the capability's expiry (so the node stops serving once the grant lapses).
//! All of these are plain fields of the one `StoredLaneState` record — there is
//! a single on-disk format and no older shape to decode.
//!
//! The seller table above is defined here. The **buyer** pool table (#744) is
//! not: its record codec and every one of its operations live in
//! [`decdn_incentive::buyer_pool_table`], shared with the client's
//! `RedbBuyerPoolStore` (#1246). This file contributes only the buyer table's
//! wiring — the node holds it in its own `buyer.redb`, so a buyer top-up
//! commits on a writer slot separate from the seller lane flush.
//!
//! [ADR 003 §Off-chain voucher state persistence]: ../../../adr/003-payments.md

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use crossbeam_queue::SegQueue;
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;

use alloy::primitives::{Address, B256, U256};
use decdn_common::identity;
use decdn_incentive::buyer_pool_table::BuyerPoolTable;
use decdn_incentive::store::{
    CheckpointKey, KeyedCheckpointStore, PendingSettle, PendingSettleStore, PoolFloorLossStore,
    PoolStateStore, StoreError,
};
use decdn_incentive::{
    AdvanceOutcome, BuyerLoad, BuyerPoolState, BuyerPoolStore, DepositOutcome, LaneChain, LaneKey,
    LaneState, PoolId,
};
use redb::{Database, Durability, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};

/// File name of the seller lane + capability redb database within `data_dir`.
const LANES_DB_FILE: &str = "lanes.redb";

/// File name of the pending-settle redb database (seller and buyer sets).
const SETTLE_DB_FILE: &str = "settle.redb";

/// File name of the floor-loss redb database (abandonment-bucket snapshots +
/// tombstones).
const FLOOR_LOSS_DB_FILE: &str = "floor-loss.redb";

/// File name of the settlement-watcher checkpoint redb database.
const CHECKPOINT_DB_FILE: &str = "checkpoint.redb";

/// File name of the buyer pool redb database (buyer state + owner index).
const BUYER_DB_FILE: &str = "buyer.redb";

/// Byte width of a [`LaneKey`] on disk: `pool_id ‖ signer ‖ provider` =
/// `32 + 20 + 20`.
const LANE_KEY_LEN: usize = 72;

/// On-disk file mode (`0o600` — owner-only read+write). Defense-in-depth: the
/// containing `data_dir` is already enforced to `0o700` by
/// [`decdn_common::identity::ensure_data_dir`], so other local users cannot
/// reach the file via path traversal, but we still tighten the file mode in
/// case the directory ACL is widened out-of-band.
#[cfg(unix)]
const DB_FILE_MODE: u32 = 0o600;

/// `schema_version` this binary writes and decodes. A record carrying a higher
/// value causes `load_all` to refuse to start (see
/// [`StoreError::UnsupportedSchema`]) — a cheap forward tripwire so a store a
/// newer binary wrote is rejected loudly rather than silently mis-decoded. It
/// is not a migration hook: there is one on-disk format and no older shape to
/// read.
const SUPPORTED_SCHEMA_VERSION: u32 = 1;

/// Sanity ceiling on trailing bytes per lane record. Trailing bytes past the
/// known fields are tolerated (a newer writer's additive field is a no-op to
/// us — see [`StoredLaneState`]), but a `remainder.len()` above this threshold
/// is logged as a warning so an honest schema-skew incident or malicious
/// padding attempt is observable in operator logs without re-introducing the
/// strict-decoding regression issue #527's reviewers warned against.
const SANE_TRAILER_MAX_BYTES: usize = 256;

/// redb table holding the per-lane voucher state.
///
/// Key: the [`LaneKey`] encoding `pool_id ‖ signer ‖ provider` (`[u8; 72]`).
/// Value: postcard-encoded [`StoredLaneState`] (variable length).
const LANE_TABLE: TableDefinition<'_, &[u8; LANE_KEY_LEN], &[u8]> =
    TableDefinition::new("lane_state_v1");

/// redb table holding the pending-settle set (#327): pools this node closed
/// on-chain that await the grace window to elapse before their accrued claim
/// finalizes. Lives in the same database file as [`LANE_TABLE`] so a single
/// open + single fsync discipline covers both.
///
/// Key: raw `PoolId` bytes (`[u8; 32]`).
/// Value: the grace-window deadline (Unix seconds). A fixed-width native `u64`
/// value needs no postcard envelope (unlike [`LANE_TABLE`]), so there is no
/// schema-version trailer to evolve here.
const PENDING_SETTLE_TABLE: TableDefinition<'_, &[u8; 32], u64> =
    TableDefinition::new("pending_settle_v1");

/// redb table holding pools this node closed as the **buyer** (#988) — the
/// unilateral close of an unreachable provider's idle pool — that await the
/// grace window. Same shape as [`PENDING_SETTLE_TABLE`] (key: `PoolId` bytes,
/// value: deadline) but a SEPARATE table so the buyer settle sweep and the
/// seller settle sweep never settle each other's closes. Lives in the same
/// database file as [`LANE_TABLE`].
const BUYER_PENDING_SETTLE_TABLE: TableDefinition<'_, &[u8; 32], u64> =
    TableDefinition::new("buyer_pending_settle_v1");

/// redb table holding each on-chain watcher's scan checkpoint (#751, keyed in
/// #1092/#1108): the last block scanned per [`CheckpointKey`], so a bring-up
/// backfill resumes across restarts and covers events landing while the node was
/// down. Lives in the same database file as [`LANE_TABLE`]. One `&str` key per
/// watcher (the [`CheckpointKey::as_str`] literals); a fixed-width native `u64`
/// value needs no postcard envelope.
const WATCHER_CHECKPOINT_TABLE: TableDefinition<'_, &str, u64> =
    TableDefinition::new("watcher_checkpoint_v1");

/// redb table holding each `(pool_id, signer)` lane's abandonment leaky-bucket
/// snapshot (ADR 003 §Pool solvency — the per-signer refilling allowance):
/// `consumed_micro` of un-recouped floor as of the wall-clock `refill_unix_ms`.
/// This exposure appears in no on-chain quantity, so it is persisted here to let a
/// restart resume a signer's throttle where it left off rather than granting a
/// fresh allowance. The signer dimension is the isolation boundary; the bucket
/// refills, so a reader replays the time-refill from `refill_unix_ms` on load.
///
/// The name carries a version because a redb table's key AND value types are part
/// of its identity: reopening a table under a different value shape fails
/// `TableTypeMismatch` at bring-up. Pre-launch there is nothing to migrate, so a
/// superseded `pool_floor_loss_v1`/`_v2` table left in a development store is deleted
/// at open by [`PersistentPoolStateStore::drop_superseded_floor_loss_table`] rather
/// than read — the reset is then a deliberate, logged act instead of a silent empty
/// load that reads exactly like a first boot.
///
/// Key: `pool_id ‖ signer` (`[u8; 52]`, see [`pool_signer_key_bytes`]). Value: the
/// abandonment leaky-bucket snapshot `(consumed_micro, refill_unix_ms)` as a native
/// redb `(u128, u64)` tuple — no postcard envelope, matching the
/// [`PENDING_SETTLE_TABLE`] convention of using redb's built-in scalar encoding for
/// fixed-width numbers.
const POOL_FLOOR_LOSS_TABLE: TableDefinition<'_, &[u8; POOL_SIGNER_KEY_LEN], (u128, u64)> =
    TableDefinition::new("pool_floor_loss_v3");

/// The superseded per-pool floor-loss table, keyed by `pool_id` alone. Nothing
/// reads it; [`PersistentPoolStateStore::drop_superseded_floor_loss_table`] deletes
/// it at open so a development store carrying one does not keep dead rows forever.
const SUPERSEDED_POOL_FLOOR_LOSS_TABLE: TableDefinition<'_, &[u8; 32], u128> =
    TableDefinition::new("pool_floor_loss_v1");

/// The superseded `(pool_id, signer)`-keyed floor-loss table that stored a single
/// monotonic `µUSDC` dead-charge total. Its value shape differs from the live
/// bucket-snapshot table, so it is dropped at open the same way as v1.
const SUPERSEDED_POOL_FLOOR_LOSS_TABLE_V2: TableDefinition<'_, &[u8; POOL_SIGNER_KEY_LEN], u128> =
    TableDefinition::new("pool_floor_loss_v2");

/// redb table of tombstones for pools whose floor-loss rows were
/// [`PoolFloorLossStore::forget_loss`]-ed. A reservation drop reads its bucket
/// snapshot under the in-memory floor lock but persists it from an independent
/// blocking task, so a `record_bucket` can land AFTER the pool's `forget_loss`
/// committed; without the tombstone that late write re-inserts a row for a closed
/// pool, and — the pool id never recurring — nothing would ever delete it again
/// (#1781). `record_bucket` checks this table inside its own write transaction
/// (redb's exclusive writer slot makes the check atomic with the insert) and treats
/// every signer of a tombstoned pool as a no-op. Swept at bring-up
/// ([`PoolFloorLossStore::sweep_forgotten`]), when no persist can be in flight, so
/// tombstones accumulate for at most one process lifetime. Lives in the same
/// database file as [`LANE_TABLE`].
///
/// Key: raw `PoolId` bytes (`[u8; 32]`). Value: none (`()`), presence is the
/// tombstone.
const POOL_FLOOR_LOSS_FORGOTTEN_TABLE: TableDefinition<'_, &[u8; 32], ()> =
    TableDefinition::new("pool_floor_loss_forgotten_v1");

/// Byte width of a `(pool_id, signer)` key on disk: `32 + 20`. Two tables use
/// this shape — the capability table (a capability authorizes one signer under
/// one pool for every provider, so it is keyed by the pair, not the full lane
/// triple) and the floor-loss table (ADR 003 §Pool solvency bounds un-vouchered
/// floor per signer under the per-pool ceiling). The pool id leads, so every row
/// of one pool shares a 32-byte prefix and a range scan selects exactly that
/// pool's rows.
const POOL_SIGNER_KEY_LEN: usize = 52;

/// redb table holding the owner-signed capability material the seller
/// voucher-intake path persists so the redeemer can register a signer on its
/// first on-chain redemption (ADR 003 §Capability delegation). The lane's
/// [`LaneState`] carries the signer's `cap` and `expiry`, but not the owner's
/// signature over the EIP-712 `Capability`; this table holds that signature
/// alongside a copy of the cap/expiry so the redeemer builds the registration
/// payload without a chain read. Lives in the same `lanes.redb` file as
/// [`LANE_TABLE`].
///
/// Key: `pool_id ‖ signer` (`[u8; 52]`). Value: postcard-encoded
/// [`StoredCapability`].
const CAPABILITY_TABLE: TableDefinition<'_, &[u8; POOL_SIGNER_KEY_LEN], &[u8]> =
    TableDefinition::new("capability_v1");

/// Encode a `(pool_id, signer)` pair into its `[u8; 52]` table key, shared by the
/// capability and floor-loss tables.
fn pool_signer_key_bytes(pool_id: B256, signer: Address) -> [u8; POOL_SIGNER_KEY_LEN] {
    let mut out = [0u8; POOL_SIGNER_KEY_LEN];
    out[..32].copy_from_slice(pool_id.as_slice());
    out[32..].copy_from_slice(signer.as_slice());
    out
}

/// Split a `[u8; 52]` `(pool_id, signer)` table key back into its parts. The input
/// is fixed-width and both ranges are constants inside it, so neither the slicing
/// nor the `copy_from_slice` can fail.
fn pool_signer_key_parts(bytes: &[u8; POOL_SIGNER_KEY_LEN]) -> (B256, Address) {
    let mut pool = [0u8; 32];
    pool.copy_from_slice(&bytes[..32]);
    let mut signer = [0u8; 20];
    signer.copy_from_slice(&bytes[32..]);
    (B256::from(pool), Address::from(signer))
}

/// The inclusive `[u8; 52]` key bounds covering EVERY signer row of one pool.
/// redb orders `&[u8; N]` keys lexicographically, so the pool's 32-byte prefix
/// followed by the all-zero and all-`0xff` signers brackets exactly its rows.
fn pool_signer_key_range(pool_id: B256) -> ([u8; POOL_SIGNER_KEY_LEN], [u8; POOL_SIGNER_KEY_LEN]) {
    (
        pool_signer_key_bytes(pool_id, Address::from([0x00u8; 20])),
        pool_signer_key_bytes(pool_id, Address::from([0xffu8; 20])),
    )
}

/// On-disk owner-signed capability record. The `(pool_id, signer)` identity is
/// the table key, so the value carries only the cap/expiry and the owner
/// signature. `spending_cap` is a fixed-width big-endian array (identical to the
/// on-chain representation); `owner_sig` is the raw EIP-712 signature (65-byte
/// ECDSA, or an ERC-1271 payload) verbatim.
///
/// `PartialEq` lets [`PersistentPoolStateStore::put_capability`] dedup an
/// identical repeat write (equal fields ⇒ equal record ⇒ nothing to mark dirty).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StoredCapability {
    spending_cap: [u8; 32],
    expiry: u64,
    owner_sig: Vec<u8>,
}

/// Encode a [`LaneKey`] into its `[u8; 72]` table key: `pool_id ‖ signer ‖
/// provider`.
fn lane_key_bytes(key: &LaneKey) -> [u8; LANE_KEY_LEN] {
    let mut out = [0u8; LANE_KEY_LEN];
    out[..32].copy_from_slice(key.pool_id.as_slice());
    out[32..52].copy_from_slice(key.signer.as_slice());
    out[52..].copy_from_slice(key.provider.as_slice());
    out
}

/// Decode a `[u8; 72]` table key back into its [`LaneKey`] parts
/// `(pool_id, signer, provider)`. Fixed-width slices, so the array indexing is
/// total.
fn lane_key_parts(bytes: &[u8; LANE_KEY_LEN]) -> (B256, Address, Address) {
    let mut pool = [0u8; 32];
    pool.copy_from_slice(&bytes[..32]);
    let mut signer = [0u8; 20];
    signer.copy_from_slice(&bytes[32..52]);
    let mut provider = [0u8; 20];
    provider.copy_from_slice(&bytes[52..]);
    (
        B256::from(pool),
        Address::from(signer),
        Address::from(provider),
    )
}

/// On-disk record — the single, complete lane-state format. The numeric
/// balance fields use fixed-size big-endian byte arrays instead of
/// variable-length integers so the encoded value width is stable across
/// postcard versions and identical to the on-chain representation, making
/// manual inspection straightforward.
///
/// The lane's identity (`pool_id`, `signer`, `provider`) is NOT stored in the
/// value — it is the table key ([`lane_key_bytes`]) — so a value carries only
/// the mutable watermark plus the capability's `cap`/`expiry`.
///
/// `schema_version` is a forward tripwire only (see [`SUPPORTED_SCHEMA_VERSION`]):
/// `decode_record` uses [`postcard::take_from_bytes`], which tolerates trailing
/// bytes, so a record a newer binary wrote with an appended field still decodes
/// its known prefix here rather than erroring on the extra bytes.
///
/// `signature` is a length-prefixed `Vec<u8>` on disk; the in-memory
/// [`LaneState`] carries the stronger `Option<[u8; 65]>`, and the narrowing
/// (empty → `None`, 65 → `Some`, anything else → corrupt) lives in
/// [`StoredLaneState::into_state`].
///
/// `registered_until` is a required field after `expiry`: the observed on-chain
/// capability expiry for this lane's signer. Adding it is a breaking on-disk
/// change — a record written before it fails to decode here (the trailing `u64`
/// is absent, so `take_from_bytes` hits `DeserializeUnexpectedEnd`), it is not
/// silently defaulted. That break is deliberate and unversioned: deCDN is
/// pre-launch with no deployed store to stay compatible with, so
/// [`SUPPORTED_SCHEMA_VERSION`] does not bump.
#[derive(Debug, Serialize, Deserialize)]
struct StoredLaneState {
    schema_version: u32,
    last_amount: [u8; 32],
    last_bytes_delivered: [u8; 32],
    signature: Vec<u8>,
    cap: [u8; 32],
    expiry: u64,
    registered_until: u64,
    /// The lane's live hash-chain epoch, flattened (ADR 003 §Off-chain voucher
    /// state persistence). `tip` is the preimage **bytes** at `verified_index`,
    /// not just the depth: only the payer can produce a value at a given depth,
    /// so a node that kept the index alone would hold an unprovable claim after
    /// a restart — in exactly the abandonment case the chain exists to cover.
    ///
    /// Chain state is **frontier**, so the #1672 durability split covers it
    /// unchanged: losing it on a crash forfeits at most the chunks metered
    /// since the lane's last signature, which is the node's own un-signed tail
    /// and the safe direction to lose.
    chain_root: [u8; 32],
    chunk_price: [u8; 32],
    verified_index: u8,
    tip: [u8; 32],
}

impl From<&LaneState> for StoredLaneState {
    fn from(state: &LaneState) -> Self {
        Self {
            schema_version: SUPPORTED_SCHEMA_VERSION,
            last_amount: state.last_amount().to_be_bytes(),
            last_bytes_delivered: state.last_bytes_delivered().to_be_bytes(),
            signature: state.last_signature().map_or_else(Vec::new, |s| s.to_vec()),
            cap: state.cap.to_be_bytes(),
            expiry: state.expiry,
            registered_until: state.registered_until,
            chain_root: state.chain().chain_root.into(),
            chunk_price: state.chain().chunk_price.to_be_bytes(),
            verified_index: state.chain().verified_index,
            tip: state.chain().tip.into(),
        }
    }
}

/// Narrow an on-disk length-prefixed signature to the in-memory
/// `Option<[u8; 65]>`: empty → `None`, exactly 65 bytes → `Some`, any other
/// length → [`StoreError::Corrupt`] (a malformed record we must not silently
/// submit to the on-chain `PaymentPool.redeemMany`).
fn decode_signature(
    bytes: &[u8],
    pool_id: B256,
    what: &str,
) -> Result<Option<[u8; 65]>, StoreError> {
    if bytes.is_empty() {
        return Ok(None);
    }
    let len = bytes.len();
    Ok(Some(<[u8; 65]>::try_from(bytes).map_err(|_| {
        StoreError::Corrupt {
            pool_id: Some(pool_id),
            detail: format!("stored {what} signature is {len} bytes, expected 0 or 65"),
        }
    })?))
}

impl StoredLaneState {
    /// Reconstruct the in-memory [`LaneState`] via its hydration constructor
    /// (the trusted cross-crate writer, #527/#751), given the lane identity
    /// recovered from the table key. The on-disk `signature` is a
    /// length-prefixed `Vec<u8>`; it is narrowed to the in-memory
    /// `Option<[u8; 65]>` here: empty → `None`, exactly 65 bytes → `Some`, any
    /// other length → [`StoreError::Corrupt`] (a malformed record we must not
    /// silently submit to the on-chain `PaymentPool.redeem`).
    fn into_state(
        self,
        pool_id: B256,
        signer: Address,
        provider: Address,
    ) -> Result<LaneState, StoreError> {
        if self.schema_version > SUPPORTED_SCHEMA_VERSION {
            return Err(StoreError::UnsupportedSchema {
                found: self.schema_version,
                supported: SUPPORTED_SCHEMA_VERSION,
            });
        }
        let last_signature = decode_signature(&self.signature, pool_id, "voucher")?;
        let mut state = LaneState::hydrate(
            pool_id,
            signer,
            provider,
            U256::from_be_bytes(self.cap),
            self.expiry,
            U256::from_be_bytes(self.last_amount),
            U256::from_be_bytes(self.last_bytes_delivered),
            last_signature,
            LaneChain {
                chain_root: B256::from(self.chain_root),
                chunk_price: U256::from_be_bytes(self.chunk_price),
                verified_index: self.verified_index,
                tip: B256::from(self.tip),
            },
        );
        state.registered_until = self.registered_until;
        Ok(state)
    }
}

/// One lane's slot in the in-memory working set.
///
/// `Live` holds the authoritative frontier. `Tombstoned` marks a `forget`-ten
/// lane whose on-disk row is not yet deleted — it stays in the map (rather than
/// being removed) so `flush` learns to delete the row, and so a `record` that
/// resurrects the lane in the same window is decided under the entry's shard
/// lock rather than racing a separate tombstone set. Reads treat `Tombstoned`
/// as absent.
///
/// The `Live` variant carries a full [`LaneState`] inline — no `Box`. That is
/// the point: a stored lane clones out with a memcpy on every `record`/`get`,
/// the hot serve path, and boxing would trade that for a heap indirection on
/// exactly the frequent variant to shrink the rare `Tombstoned` one. Tombstones
/// are transient (the next flush reaps them), so the size skew never
/// accumulates.
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
enum LaneSlot {
    Live(LaneState),
    Tombstoned,
}

/// `redb`-backed persistent implementation of [`PoolStateStore`].
///
/// Construct via [`PersistentPoolStateStore::open`]. One `redb::Database` per
/// write family is owned for the lifetime of this value; drop closes every
/// handle. The lane table is buffered in memory: `record`/`forget` mutate the
/// working set only, and a caller must call [`PoolStateStore::flush`] to commit
/// it to disk. The store is thread-safe — each redb file serialises its own
/// writes via single-writer transactions, families commit on independent writer
/// slots, and reads are MVCC.
///
/// The working set is a [`DashMap`] rather than one mutex-guarded map, so a
/// `record` on the paid-delivery path locks only its own lane's shard — no lane
/// serialises on another, and settlement's `load_all`/`get` no longer contend
/// with `record` (issue #1792 item 1). Each entry's shard lock is the per-lane
/// critical section that keeps a `record`/`forget` race decidable, the role the
/// single buffer mutex played for the whole map.
#[derive(Debug)]
pub struct PersistentPoolStateStore {
    /// Seller lane frontier + capability rows (`lanes.redb`). The periodic
    /// flush commits both tables here in one transaction.
    lanes_db: Database,
    /// Seller + buyer pending-settle sets (`settle.redb`).
    settle_db: Database,
    /// Per-pool floor-loss totals + forget tombstones (`floor-loss.redb`).
    floor_loss_db: Database,
    /// Settlement-watcher scan checkpoints (`checkpoint.redb`).
    checkpoint_db: Database,
    /// Buyer pool state + owner index (`buyer.redb`).
    buyer_db: Database,
    /// Path of the lane store file (`lanes.redb`); returned by [`Self::path`].
    path: PathBuf,
    /// The per-lane working set, hydrated from disk at `open()`.
    lanes: DashMap<LaneKey, LaneSlot>,
    /// Lock-free work-list of lanes changed since the last flush.
    /// `record`/`forget`/`set_registered_until` push their key; `flush` drains
    /// it and re-reads each slot from `lanes` (the source of truth), so a
    /// duplicate or a since-superseded key is harmless. Draining the queue —
    /// rather than clearing a shared dirty set in place — is what bounds the
    /// durable watermark's lag to one flush interval across a concurrent
    /// `record` (ADR 003 §Off-chain voucher state persistence): a `record` that
    /// lands after a key is drained pushes it afresh and is captured next flush.
    dirty: SegQueue<LaneKey>,
    /// Buffered capability rows keyed by `pool_id ‖ signer` (`[u8; 52]` —
    /// [`pool_signer_key_bytes`]). Sharded like `lanes`, and for the same
    /// reason: a `put_capability` on the intake path locks only its own row's
    /// shard. The map needs no slot enum: a row is removed outright by
    /// [`Self::forget`], and the drained key with no row IS the tombstone the
    /// next flush turns into a table delete.
    caps: DashMap<[u8; POOL_SIGNER_KEY_LEN], StoredCapability>,
    /// Lock-free work-list of capability rows changed or removed since the last
    /// flush, drained by `flush` exactly like `dirty`.
    dirty_caps: SegQueue<[u8; POOL_SIGNER_KEY_LEN]>,
}

impl PersistentPoolStateStore {
    /// Open (or create) the lane-state store under `data_dir`.
    ///
    /// `data_dir` is validated through
    /// [`decdn_common::identity::ensure_data_dir`] before any redb operation,
    /// which enforces `0o700` on the directory and rejects insecure modes.
    /// The store file is then chmod'd to `0o600` after creation as
    /// defense-in-depth.
    ///
    /// # Errors
    ///
    /// - [`StoreError::Backend`] when the `data_dir` security check fails.
    /// - [`StoreError::Backend`] when `redb::Database::create` refuses the file.
    /// - [`StoreError::Corrupt`] when the file exists but is zero-length —
    ///   `redb` would otherwise treat that as "create a fresh database" and
    ///   silently reopen the issue #527 replay window.
    /// - [`StoreError::PermissionTighten`] when the post-create chmod fails.
    /// - [`StoreError::Io`] for any other filesystem error while stat-ing.
    ///
    /// When this returns `Err`, the caller MUST abort node bring-up —
    /// starting with a clean store silently forfeits the issue #527 guarantee.
    pub fn open(data_dir: &Path) -> Result<Self, StoreError> {
        Self::open_with(data_dir, Self::tighten_permissions)
    }

    /// Testable form of [`Self::open`] that takes an injectable chmod
    /// function. Non-test callers use [`Self::open`]; tests inject a closure
    /// that simulates chmod failure to exercise the cleanup-branch asymmetry
    /// (the security-critical fix for #527 follow-up review).
    ///
    /// `pub(crate)` deliberately — exposing this beyond the crate would let an
    /// external caller pass a no-op `chmod_fn` and silently weaken the
    /// file-mode hardening.
    pub(crate) fn open_with<F>(data_dir: &Path, chmod_fn: F) -> Result<Self, StoreError>
    where
        F: Fn(&Path) -> Result<(), StoreError>,
    {
        identity::ensure_data_dir(data_dir).map_err(|err| {
            StoreError::Backend(format!(
                "data_dir {} failed security check: {err:#}",
                data_dir.display()
            ))
        })?;

        // One hardened redb file per write family. `lanes.redb` opens first so a
        // corrupt or empty lane store — the file that guards the issue #527
        // voucher-replay window — is the one that aborts bring-up, and every
        // integration test that fault-injects that window names this file.
        let path = data_dir.join(LANES_DB_FILE);
        let lanes_db = Self::open_hardened_db(&path, &chmod_fn)?;
        let settle_db = Self::open_hardened_db(&data_dir.join(SETTLE_DB_FILE), &chmod_fn)?;
        let floor_loss_db = Self::open_hardened_db(&data_dir.join(FLOOR_LOSS_DB_FILE), &chmod_fn)?;
        Self::drop_superseded_floor_loss_table(&floor_loss_db)?;
        let checkpoint_db = Self::open_hardened_db(&data_dir.join(CHECKPOINT_DB_FILE), &chmod_fn)?;
        let buyer_db = Self::open_hardened_db(&data_dir.join(BUYER_DB_FILE), &chmod_fn)?;

        let lanes = Self::hydrate_lanes(&lanes_db)?;
        let caps = Self::hydrate_capabilities(&lanes_db)?;
        Ok(Self {
            lanes_db,
            settle_db,
            floor_loss_db,
            checkpoint_db,
            buyer_db,
            path,
            lanes,
            dirty: SegQueue::new(),
            caps,
            dirty_caps: SegQueue::new(),
        })
    }

    /// Open (or create) one hardened redb file at `path`, applying the empty-file
    /// guard, TOCTOU-safe create, and `0o600` tightening every lane-store family
    /// file gets. Each family lives in its own file so its commits take an
    /// independent `redb` writer slot.
    ///
    /// Delete the superseded floor-loss tables (`pool_floor_loss_v1`, keyed by
    /// `pool_id` alone, and `pool_floor_loss_v2`, the `(pool_id, signer)`-keyed
    /// monotonic dead-charge total) if the store still carries one. The live table
    /// keeps a per-signer leaky-bucket snapshot `(consumed_micro, refill_unix_ms)`,
    /// and a redb table's key AND value types are part of its identity, so the new
    /// shape needs a new name; leaving an old table in place would keep its rows
    /// readable by nothing and make the reset indistinguishable from a first boot.
    /// Pre-launch there is no migration to run, so the rows are dropped — but loudly,
    /// because they will not be re-accrued.
    ///
    /// # Errors
    /// [`StoreError::Backend`] when the delete transaction fails.
    fn drop_superseded_floor_loss_table(db: &Database) -> Result<(), StoreError> {
        let txn = db
            .begin_write()
            .map_err(|e| floor_loss_backend_err("begin_write (superseded drop)", None, e))?;
        let dropped_v1 = txn
            .delete_table(SUPERSEDED_POOL_FLOOR_LOSS_TABLE)
            .map_err(|e| floor_loss_backend_err("delete_table (v1)", None, e))?;
        let dropped_v2 = txn
            .delete_table(SUPERSEDED_POOL_FLOOR_LOSS_TABLE_V2)
            .map_err(|e| floor_loss_backend_err("delete_table (v2)", None, e))?;
        txn.commit()
            .map_err(|e| floor_loss_backend_err("commit (superseded drop)", None, e))?;
        if dropped_v1 || dropped_v2 {
            tracing::warn!(
                dropped_v1,
                dropped_v2,
                "dropped a superseded floor-loss table; its rows do not carry into the \
                 per-signer bucket table and those signers start with a fresh allowance"
            );
        }
        Ok(())
    }

    /// Rejects a zero-length file: `redb::Database::create` treats both "file
    /// does not exist" and "file exists but is empty" as "create a fresh
    /// database" — so a `truncate -s 0` (or a filesystem rollback that nukes
    /// content but preserves the inode) would start with an empty store. For
    /// `lanes.redb` that silently reopens the issue #527 voucher-replay window;
    /// for the other families it silently re-grants budget the lost rows bounded
    /// (a dropped floor-loss row re-grants free-floor budget, a dropped
    /// pending-settle entry forgets an in-flight redemption).
    ///
    /// On a chmod failure the cleanup depends on whether the file pre-existed
    /// this call: a freshly-created file is removed so the next start sees a
    /// clean state; a pre-existing file (real payment state on disk) is
    /// preserved, because a transient chmod failure on a read-only mount or NFS
    /// must not delete live state. The TOCTOU window between the stat and
    /// `Database::create` is closed via `OpenOptions::create_new`.
    fn open_hardened_db<F>(path: &Path, chmod_fn: &F) -> Result<Database, StoreError>
    where
        F: Fn(&Path) -> Result<(), StoreError>,
    {
        let file_existed_before_open = match std::fs::metadata(path) {
            Ok(meta) if meta.len() == 0 => {
                return Err(StoreError::Corrupt {
                    pool_id: None,
                    detail: format!(
                        "redb store file at {} is empty (length 0). \
                         This is either a manual truncation or a filesystem rollback, \
                         either of which silently discards durable payment state \
                         (for lanes.redb this re-opens the issue #527 voucher-replay window). \
                         Restore from backup, or delete the file deliberately to start fresh \
                         (forfeiting prior history).",
                        path.display()
                    ),
                });
            }
            Ok(_) => true,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                match std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(path)
                {
                    Ok(file) => {
                        // Drop the handle immediately; redb opens its own.
                        drop(file);
                        false
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => true,
                    Err(err) => return Err(StoreError::Io(err)),
                }
            }
            Err(err) => return Err(StoreError::Io(err)),
        };

        let db = Database::create(path).map_err(|err| {
            StoreError::Backend(format!(
                "failed to open redb store file at {}: {err}. \
                 Removing the file forfeits its durability guard \
                 — restore from backup or investigate the corruption.",
                path.display()
            ))
        })?;

        if let Err(chmod_err) = chmod_fn(path) {
            // `drop(db)` is load-bearing on Windows: NTFS holds a mandatory
            // exclusive lock on the file handle, so `remove_file` below would
            // error with sharing-violation if the handle outlives.
            drop(db);
            Self::handle_chmod_failure(path, &chmod_err, file_existed_before_open);
            return Err(chmod_err);
        }

        Ok(db)
    }

    /// Read the whole lane table into the in-memory working set at open. A
    /// corrupt or forward-schema record aborts startup (running past it would
    /// reopen the issue #527 voucher-replay window). Every hydrated lane enters
    /// as [`LaneSlot::Live`].
    fn hydrate_lanes(db: &Database) -> Result<DashMap<LaneKey, LaneSlot>, StoreError> {
        let read_txn = db
            .begin_read()
            .map_err(|err| StoreError::Backend(format!("begin_read: {err}")))?;
        let table = match read_txn.open_table(LANE_TABLE) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(DashMap::new()),
            Err(err) => return Err(StoreError::Backend(format!("open_table: {err}"))),
        };
        let out = DashMap::new();
        let iter = table
            .iter()
            .map_err(|err| StoreError::Backend(format!("table iter: {err}")))?;
        for entry in iter {
            let (key_guard, value_guard) =
                entry.map_err(|err| StoreError::Backend(format!("iter entry: {err}")))?;
            let key_bytes: [u8; LANE_KEY_LEN] = *key_guard.value();
            let state = decode_record(&key_bytes, value_guard.value())?;
            out.insert(state.key(), LaneSlot::Live(state));
        }
        Ok(out)
    }

    /// Read the whole capability table into the in-memory working set at open.
    /// A corrupt record aborts startup like a corrupt lane row would — the
    /// redeemer must not silently miss registration material for a signer it is
    /// about to register on-chain.
    fn hydrate_capabilities(
        db: &Database,
    ) -> Result<DashMap<[u8; POOL_SIGNER_KEY_LEN], StoredCapability>, StoreError> {
        let read_txn = db
            .begin_read()
            .map_err(|err| StoreError::Backend(format!("begin_read: {err}")))?;
        let table = match read_txn.open_table(CAPABILITY_TABLE) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(DashMap::new()),
            Err(err) => return Err(StoreError::Backend(format!("open_table: {err}"))),
        };
        let out = DashMap::new();
        let iter = table
            .iter()
            .map_err(|err| StoreError::Backend(format!("table iter: {err}")))?;
        for entry in iter {
            let (key_guard, value_guard) =
                entry.map_err(|err| StoreError::Backend(format!("iter entry: {err}")))?;
            let key_bytes: [u8; POOL_SIGNER_KEY_LEN] = *key_guard.value();
            let (pool_id, signer) = pool_signer_key_parts(&key_bytes);
            let record: StoredCapability =
                postcard::from_bytes(value_guard.value()).map_err(|err| StoreError::Corrupt {
                    pool_id: Some(pool_id),
                    detail: format!("capability postcard decode failed for signer {signer}: {err}"),
                })?;
            out.insert(key_bytes, record);
        }
        Ok(out)
    }

    /// Operator-facing logging for the chmod-failure cleanup branch. Extracted
    /// so the security policy ("preserve pre-existing, remove fresh") lives in
    /// one place; both branches emit `tracing::error!` because both refuse to
    /// bring the node up and the operator needs an error-level signal.
    fn handle_chmod_failure(path: &Path, chmod_err: &StoreError, file_existed_before_open: bool) {
        let observed_mode = Self::observed_mode_string(path);
        if file_existed_before_open {
            tracing::error!(
                path = %path.display(),
                observed_mode = %observed_mode,
                event = "lane_store_chmod_fail_preserve",
                %chmod_err,
                "chmod failed on pre-existing lane state store; refusing to delete (would reopen issue #527 replay window). \
                 Investigate the permission error and chmod the file to 0o600 manually before retry.",
            );
            return;
        }

        // Fresh file path: we definitively created this file ourselves via
        // `OpenOptions::create_new` upstream, so removing it cannot destroy
        // anyone else's voucher state.
        if let Err(remove_err) = std::fs::remove_file(path) {
            tracing::error!(
                %remove_err,
                path = %path.display(),
                observed_mode = %observed_mode,
                event = "lane_store_chmod_fail_remove_failed",
                %chmod_err,
                "chmod failed on freshly-created lane state store, and removal also failed; \
                 file persists with current mode. chmod 0o600 manually before next start.",
            );
        }
    }

    /// Best-effort stringified file mode for operator log lines. Returns
    /// `"unknown"` on stat failure or non-Unix targets — the field is purely
    /// informational, so a missing value is preferable to a panic.
    fn observed_mode_string(path: &Path) -> String {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            match std::fs::metadata(path) {
                Ok(meta) => format!("{:o}", meta.permissions().mode() & 0o777),
                Err(_) => "unknown".into(),
            }
        }
        #[cfg(not(unix))]
        {
            let _ = path;
            "n/a (non-unix)".into()
        }
    }

    /// Tighten the on-disk file mode to `0o600` after open. **Idempotent**: if
    /// the file is already at the target mode, the syscall is skipped so a
    /// read-only mount (EROFS) where the mode is correct from a prior boot does
    /// not brick subsequent startups.
    ///
    /// No-op on non-Unix targets (Windows ACLs are controlled at directory
    /// level, per the comment on [`DB_FILE_MODE`]).
    #[cfg(unix)]
    fn tighten_permissions(path: &Path) -> Result<(), StoreError> {
        use std::os::unix::fs::PermissionsExt;
        let meta = std::fs::metadata(path).map_err(|source| StoreError::PermissionTighten {
            path: path.to_path_buf(),
            source,
        })?;
        if meta.permissions().mode() & 0o777 == DB_FILE_MODE {
            return Ok(());
        }
        let perms = std::fs::Permissions::from_mode(DB_FILE_MODE);
        std::fs::set_permissions(path, perms).map_err(|source| StoreError::PermissionTighten {
            path: path.to_path_buf(),
            source,
        })?;
        Ok(())
    }

    #[cfg(not(unix))]
    fn tighten_permissions(_path: &Path) -> Result<(), StoreError> {
        tracing::debug!(
            "lane state store: file-mode tightening skipped on non-unix; relying on data_dir ACL",
        );
        Ok(())
    }

    /// Filesystem path of the underlying database file. Useful for log messages
    /// and operator runbooks.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Decode one stored record (table value) into a [`LaneState`], given its table
/// key (the lane encoding). Shared by `load_all` and `get`.
///
/// `take_from_bytes` instead of `from_bytes` so a newer writer's additive
/// fields (trailing bytes to this reader) decode cleanly — see
/// [`StoredLaneState`]'s doc and the version tripwire in `into_state`.
fn decode_record(
    key_bytes: &[u8; LANE_KEY_LEN],
    value_bytes: &[u8],
) -> Result<LaneState, StoreError> {
    let (pool_id, signer, provider) = lane_key_parts(key_bytes);
    let (stored, remainder): (StoredLaneState, &[u8]) = postcard::take_from_bytes(value_bytes)
        .map_err(|err| StoreError::Corrupt {
            pool_id: Some(pool_id),
            detail: format!("postcard decode failed: {err}"),
        })?;
    // Trailing-byte tolerance is bounded: a malicious writer could pad megabytes
    // onto every record and silently inflate every read. Log (don't fail) when
    // the trailer beyond the known fields exceeds a small sanity ceiling.
    if remainder.len() > SANE_TRAILER_MAX_BYTES {
        tracing::warn!(
            %pool_id,
            remainder = remainder.len(),
            limit = SANE_TRAILER_MAX_BYTES,
            event = "lane_store_excess_trailer",
            "lane state record has unusually large trailing bytes; possible malicious padding or large-additive-field schema skew",
        );
    }
    stored.into_state(pool_id, signer, provider)
}

impl PoolStateStore for PersistentPoolStateStore {
    /// Every persisted lane, from the in-memory working set hydrated at `open()`.
    /// Tombstoned slots (a `forget`-ten lane not yet flushed) read as absent.
    fn load_all(&self) -> Result<Vec<LaneState>, StoreError> {
        Ok(self
            .lanes
            .iter()
            .filter_map(|entry| match entry.value() {
                LaneSlot::Live(state) => Some(state.clone()),
                LaneSlot::Tombstoned => None,
            })
            .collect())
    }

    fn get(&self, key: LaneKey) -> Result<Option<LaneState>, StoreError> {
        Ok(self.lanes.get(&key).and_then(|entry| match entry.value() {
            LaneSlot::Live(state) => Some(state.clone()),
            LaneSlot::Tombstoned => None,
        }))
    }

    /// Advance the in-memory lane state and mark it dirty. Durability is the
    /// background flush's job (ADR 003 §Off-chain voucher state persistence).
    /// Locks only this lane's [`DashMap`] shard; concurrent `record`s on other
    /// lanes proceed in parallel.
    fn record(&self, state: &LaneState) -> Result<(), StoreError> {
        let key = state.key();
        let mut next = state.clone();
        match self.lanes.entry(key) {
            Entry::Occupied(mut occ) => {
                if let LaneSlot::Live(existing) = occ.get() {
                    next.registered_until = next.registered_until.max(existing.registered_until);
                }
                occ.insert(LaneSlot::Live(next));
            }
            Entry::Vacant(vac) => {
                vac.insert(LaneSlot::Live(next));
            }
        }
        self.dirty.push(key);
        Ok(())
    }

    /// Raise this lane's observed on-chain registration expiry, monotonically —
    /// touches ONLY `registered_until`, never the replay-critical `last_*`
    /// tuple, so it cannot race a concurrent voucher `record` into a lost
    /// update. A no-op for a lane with no live record. Buffered like `record`;
    /// durability is the next `flush`'s job.
    fn set_registered_until(&self, key: LaneKey, registered_until: u64) -> Result<(), StoreError> {
        let Some(mut slot) = self.lanes.get_mut(&key) else {
            return Ok(());
        };
        let LaneSlot::Live(state) = slot.value_mut() else {
            return Ok(());
        };
        if registered_until > state.registered_until {
            state.registered_until = registered_until;
            self.dirty.push(key);
        }
        Ok(())
    }

    /// Tombstone the lane so reads treat it as absent and the next flush deletes
    /// its on-disk row. Idempotent. The slot stays in the map (as a tombstone)
    /// so a concurrent `record` resurrecting the lane is decided under the same
    /// shard lock rather than racing a separate set.
    ///
    /// The lane's capability row goes with it. Registration material is only
    /// ever asked for by [`LaneKey`], so a capability whose lane is gone is
    /// unreachable — keeping it would grow both the resident map and the table
    /// with the count of distinct `(pool_id, signer)` pairs the node has ever
    /// seen, and the signer half of that pair is chosen by whoever funds the
    /// pool. Removal and re-insertion race under the row's own shard lock, and
    /// the drained-but-absent key is what makes the next flush delete the row
    /// rather than leave it to be re-hydrated at the next open.
    fn forget(&self, key: LaneKey) -> Result<(), StoreError> {
        self.lanes.insert(key, LaneSlot::Tombstoned);
        self.dirty.push(key);
        let cap_key = pool_signer_key_bytes(key.pool_id, key.signer);
        if self.caps.remove(&cap_key).is_some() {
            self.dirty_caps.push(cap_key);
        }
        Ok(())
    }

    /// Write every dirty lane, apply every tombstone, and write every dirty
    /// capability row in ONE fsynced redb transaction. Idempotent — a no-op
    /// when clean.
    ///
    /// Double-buffered: the dirty work-list is drained and each lane's slot is
    /// cloned out (a cheap memcpy — [`LaneState`] holds no heap fields), then
    /// encoded, all off the fsync's critical path. The ~1–10 ms fsynced commit
    /// holds no lane shard lock, so a concurrent `record` (a single-shard map
    /// insert) never waits on disk I/O.
    ///
    /// A `record` that lands after its key is drained pushes the key afresh and
    /// is captured by the next flush; the store only needs a monotonic floor,
    /// and the frontier is documented "safe to lose" on a crash (ADR 003
    /// §Off-chain voucher state persistence), so a slightly older value now and a
    /// newer one one cadence later is correct.
    fn flush(&self) -> Result<(), StoreError> {
        // Drain the work-list, deduplicating: a lane touched N times since the
        // last flush sits in the queue N times, but we re-read its slot once.
        let mut drained: HashSet<LaneKey> = HashSet::new();
        while let Some(key) = self.dirty.pop() {
            drained.insert(key);
        }
        let mut drained_caps: HashSet<[u8; POOL_SIGNER_KEY_LEN]> = HashSet::new();
        while let Some(key) = self.dirty_caps.pop() {
            drained_caps.insert(key);
        }
        if drained.is_empty() && drained_caps.is_empty() {
            return Ok(());
        }

        let mut writes: Vec<(LaneKey, Vec<u8>)> = Vec::new();
        let mut tombstones: Vec<LaneKey> = Vec::new();
        for key in &drained {
            // Clone the slot out under the shard lock, then release it before
            // encoding so a concurrent `record` on this lane never waits on the
            // postcard encode.
            let slot = self.lanes.get(key).map(|entry| entry.value().clone());
            match slot {
                Some(LaneSlot::Live(state)) => {
                    match postcard::to_allocvec(&StoredLaneState::from(&state)) {
                        Ok(encoded) => writes.push((*key, encoded)),
                        Err(err) => {
                            // Nothing has been committed yet, but the drain
                            // already emptied these keys out of the work-list.
                            // Re-push every drained key so the next flush retries
                            // them — an early return here without the re-push
                            // would leave the un-encoded lanes permanently
                            // "clean" and never reach disk. The capability
                            // work-list is already drained too, so it re-pushes
                            // on the same path.
                            for key in &drained {
                                self.dirty.push(*key);
                            }
                            for key in &drained_caps {
                                self.dirty_caps.push(*key);
                            }
                            return Err(StoreError::Codec(format!(
                                "postcard encode failed: {err}"
                            )));
                        }
                    }
                }
                Some(LaneSlot::Tombstoned) => tombstones.push(*key),
                // Drained key with no slot: a `forget` re-pushed this key after a
                // flush reaped its tombstone via `remove_if`, so the row is
                // already gone. Nothing to write or delete — skip it.
                None => {}
            }
        }
        let mut cap_writes: Vec<([u8; POOL_SIGNER_KEY_LEN], Vec<u8>)> =
            Vec::with_capacity(drained_caps.len());
        let mut cap_deletes: Vec<[u8; POOL_SIGNER_KEY_LEN]> = Vec::new();
        for key in &drained_caps {
            // Same shape as the lane loop: clone the row out under its shard
            // lock, release, then encode. A drained key with no row is a
            // `forget` tombstone — the row left the map, so the table row goes
            // with it.
            let Some(record) = self.caps.get(key).map(|entry| entry.value().clone()) else {
                cap_deletes.push(*key);
                continue;
            };
            match postcard::to_allocvec(&record) {
                Ok(encoded) => cap_writes.push((*key, encoded)),
                Err(err) => {
                    for key in &drained {
                        self.dirty.push(*key);
                    }
                    for key in &drained_caps {
                        self.dirty_caps.push(*key);
                    }
                    return Err(StoreError::Codec(format!(
                        "capability postcard encode failed: {err}"
                    )));
                }
            }
        }
        let mut snapshot = FlushSnapshot {
            writes,
            tombstones,
            cap_writes,
            cap_deletes,
        };
        // Every batch reaches redb in ascending key order; see
        // [`FlushSnapshot::sort_by_table_key`].
        snapshot.sort_by_table_key();

        // Fsynced commit with NO lane shard lock held.
        if let Err(err) = self.commit_snapshot(&snapshot) {
            // The commit failed after the work-list was drained. Re-push the
            // snapshotted keys so the next flush retries them (the background
            // flusher logs "retrying next tick"). A concurrent `record`/`forget`
            // that superseded a key in the meantime keeps its newer slot; the
            // re-pushed key just re-reads whatever `lanes` now holds.
            for (key, _) in &snapshot.writes {
                self.dirty.push(*key);
            }
            for key in &snapshot.tombstones {
                self.dirty.push(*key);
            }
            for (key, _) in &snapshot.cap_writes {
                self.dirty_caps.push(*key);
            }
            for key in &snapshot.cap_deletes {
                self.dirty_caps.push(*key);
            }
            // The operator-facing flush warning upstream reports only that a
            // flush failed. Name the un-durable volume here, where the counts
            // exist: post-#1789 a failed flush leaves capability material
            // buffered as well as lane frontier, and the two are separately
            // consequential.
            tracing::warn!(
                dirty_lanes = snapshot.writes.len() + snapshot.tombstones.len(),
                dirty_capabilities = snapshot.cap_writes.len() + snapshot.cap_deletes.len(),
                error = %err,
                "lane store commit failed; every drained key is re-queued for the next flush"
            );
            return Err(err);
        }

        // Commit succeeded: reap tombstoned slots whose row is now gone. Guard
        // with `remove_if` so a `record` that resurrected the lane to `Live`
        // after the snapshot keeps its slot (and its freshly-pushed dirty mark).
        for key in &snapshot.tombstones {
            self.lanes
                .remove_if(key, |_, slot| matches!(slot, LaneSlot::Tombstoned));
        }
        Ok(())
    }
}

/// Encoded dirty writes, tombstone keys, and dirty capability rows resolved from
/// the drained work-lists, so [`PersistentPoolStateStore::flush`] can run its
/// fsynced commit with no shard lock held.
struct FlushSnapshot {
    writes: Vec<(LaneKey, Vec<u8>)>,
    tombstones: Vec<LaneKey>,
    cap_writes: Vec<([u8; POOL_SIGNER_KEY_LEN], Vec<u8>)>,
    cap_deletes: Vec<[u8; POOL_SIGNER_KEY_LEN]>,
}

impl FlushSnapshot {
    /// Order both batches by their [`lane_key_bytes`] table key, which is redb's
    /// own key order — redb compares a `&[u8; N]` key lexicographically over the
    /// encoding. Ascending keys hit redb's append fast path, which fills a leaf
    /// page before opening the next instead of leaving each page part-full at a
    /// random split point. Measured over 512 lanes at this key and value size,
    /// the table occupies about 30% fewer leaf pages (47 against 65-68).
    ///
    /// The gain lands on keys appended past the end of the table — new lanes, and
    /// the cold-load case — because an overwrite of a hydrated lane replaces in
    /// place whatever order it arrives in. Steady-state flushes are dominated by
    /// overwrites, so treat this as a bound on how far the table sprawls over its
    /// lifetime rather than a saving on every flush.
    ///
    /// Sorting the encoded key rather than an `Ord` on [`LaneKey`] keeps the
    /// sort key and the redb key the same value, so a change to the key layout
    /// cannot make the two disagree. `writes` and `tombstones` are disjoint —
    /// each drained key resolves to exactly one of a live slot or a tombstone —
    /// so the insert run and the remove run never meet on one key, and sorting
    /// both leaves the whole flush deterministic even though the work-list drains
    /// through a `HashSet` whose iteration order carries no meaning. The
    /// capability keys are already their own table keys, so sorting them by the
    /// raw `[u8; POOL_SIGNER_KEY_LEN]` (lexicographic) gives the same ascending
    /// append pattern on `CAPABILITY_TABLE`.
    fn sort_by_table_key(&mut self) {
        self.writes
            .sort_by_cached_key(|(lane, _)| lane_key_bytes(lane));
        self.tombstones.sort_by_cached_key(lane_key_bytes);
        self.cap_writes.sort_by_key(|(key, _)| *key);
        self.cap_deletes.sort_unstable();
    }

    /// Whether this snapshot touches `CAPABILITY_TABLE` at all.
    const fn touches_capabilities(&self) -> bool {
        !self.cap_writes.is_empty() || !self.cap_deletes.is_empty()
    }
}

impl PersistentPoolStateStore {
    /// Apply a [`FlushSnapshot`] in ONE fsynced redb transaction. Holds no
    /// shard lock — every value was already encoded during the snapshot. All
    /// batches arrive in ascending table-key order, so the insert loops append
    /// rightward through the B-trees.
    fn commit_snapshot(&self, snapshot: &FlushSnapshot) -> Result<(), StoreError> {
        let mut write_txn = self
            .lanes_db
            .begin_write()
            .map_err(|err| StoreError::Backend(format!("begin_write: {err}")))?;
        write_txn
            .set_durability(Durability::Immediate)
            .map_err(|err| StoreError::Backend(format!("set_durability: {err}")))?;
        {
            let mut table = write_txn
                .open_table(LANE_TABLE)
                .map_err(|err| StoreError::Backend(format!("open_table: {err}")))?;
            for (key, encoded) in &snapshot.writes {
                let key_bytes = lane_key_bytes(key);
                table
                    .insert(&key_bytes, encoded.as_slice())
                    .map_err(|err| StoreError::Backend(format!("insert: {err}")))?;
            }
            for key in &snapshot.tombstones {
                let key_bytes = lane_key_bytes(key);
                table
                    .remove(&key_bytes)
                    .map_err(|err| StoreError::Backend(format!("remove: {err}")))?;
            }
            // Only opened when the snapshot has capability work. A lane-only
            // flush must not be able to fail on the capability table: the lane
            // frontier and the capability rows share one transaction, so a
            // capability-side backend error would otherwise abort a commit that
            // carries only voucher watermarks — the loss ADR 003's flush design
            // exists to bound.
            if snapshot.touches_capabilities() {
                let mut cap_table = write_txn
                    .open_table(CAPABILITY_TABLE)
                    .map_err(|err| StoreError::Backend(format!("open_table: {err}")))?;
                for (key, encoded) in &snapshot.cap_writes {
                    cap_table
                        .insert(key, encoded.as_slice())
                        .map_err(|err| StoreError::Backend(format!("insert: {err}")))?;
                }
                for key in &snapshot.cap_deletes {
                    cap_table
                        .remove(key)
                        .map_err(|err| StoreError::Backend(format!("remove: {err}")))?;
                }
            }
        }
        write_txn
            .commit()
            .map_err(|err| StoreError::Backend(format!("commit (fsync): {err}")))?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Capability store (seller first-redemption registration material)
// ---------------------------------------------------------------------------

impl PersistentPoolStateStore {
    /// Stage the owner-signed capability material for `(pool_id, signer)` in
    /// the working set, so the redeemer can register the signer on its first
    /// on-chain redemption. NOT durable on return — [`Self::flush`] is what
    /// writes it.
    ///
    /// Buffered like [`PoolStateStore::record`]: inserts into the in-memory
    /// working set and pushes the row onto the dirty work-list, landing on disk
    /// in the SAME fsynced transaction as the lanes at the next flush. Locks
    /// only this row's [`DashMap`] shard. Deduped: a repeat intake whose
    /// `(spending_cap, expiry, owner_sig)` already equals the buffered row marks
    /// nothing dirty, so repeated capability sends are free (the intake path
    /// calls this on every request carrying a capability).
    ///
    /// A crash between two flushes loses unflushed capability rows. The client
    /// re-sends the capability on its next request and intake re-stages it, and
    /// the pre-redeem `flush_store_durable` floors the material on the strict
    /// redeem paths — see the module docs for the shutdown-path residual.
    pub fn put_capability(
        &self,
        pool_id: B256,
        signer: Address,
        spending_cap: U256,
        expiry: u64,
        owner_sig: &[u8],
    ) {
        let record = StoredCapability {
            spending_cap: spending_cap.to_be_bytes(),
            expiry,
            owner_sig: owner_sig.to_vec(),
        };
        let key_bytes = pool_signer_key_bytes(pool_id, signer);
        // Decide the dedup under the entry's shard lock, so two concurrent
        // intakes of different material for one row cannot both conclude
        // "unchanged" against the value the other is about to replace.
        match self.caps.entry(key_bytes) {
            Entry::Occupied(mut occ) => {
                if occ.get() == &record {
                    return;
                }
                occ.insert(record);
            }
            Entry::Vacant(vac) => {
                vac.insert(record);
            }
        }
        self.dirty_caps.push(key_bytes);
    }

    /// Read the stored capability material for `(pool_id, signer)`, or `None`
    /// if this node holds no capability for it. Reads the in-memory working
    /// set, so a row written by [`Self::put_capability`] but not yet flushed is
    /// still visible — the buffer is the authoritative copy after `open()`
    /// hydrates it, which is also why the read cannot fault.
    fn get_capability(&self, pool_id: B256, signer: Address) -> Option<StoredCapability> {
        let key_bytes = pool_signer_key_bytes(pool_id, signer);
        self.caps.get(&key_bytes).map(|entry| entry.value().clone())
    }
}

/// Write side of the capability table used by the seller voucher-intake path.
/// Kept as a trait so the [`ClientHandler`](crate::handlers::client::ClientHandler)
/// holds it behind an `Arc<dyn CapabilitySink>` and tests can pass `None` (an
/// in-memory store has no capability table).
///
/// Staging is infallible by contract, so the intake path has no persist error
/// to handle: the only way capability material fails to reach disk is a failed
/// [`PoolStateStore::flush`], which is metered and logged where the flush runs.
pub trait CapabilitySink: Send + Sync + std::fmt::Debug {
    /// Stage the owner-signed capability material for `(pool_id, signer)` in
    /// the implementation's working set. NOT durable on return: a
    /// [`PoolStateStore::flush`] on the same store is what commits it.
    fn stage_capability(
        &self,
        pool_id: B256,
        signer: Address,
        spending_cap: U256,
        expiry: u64,
        owner_sig: &[u8],
    );
}

impl CapabilitySink for PersistentPoolStateStore {
    fn stage_capability(
        &self,
        pool_id: B256,
        signer: Address,
        spending_cap: U256,
        expiry: u64,
        owner_sig: &[u8],
    ) {
        self.put_capability(pool_id, signer, spending_cap, expiry, owner_sig);
    }
}

/// [`CapabilitySource`](crate::payment_settlement::CapabilitySource) backed by
/// the persisted `CAPABILITY_TABLE`. The seller voucher-intake path persists a
/// signer's owner-signed capability on first sight; the redeemer reads it here
/// to register the signer on its first on-chain redemption. A signer with no
/// persisted capability yields `None` and is safely skipped by the redeemer.
#[derive(Debug)]
pub struct StoredCapabilitySource {
    inner: std::sync::Arc<PersistentPoolStateStore>,
}

impl StoredCapabilitySource {
    /// Wrap the concrete store. The same `lanes.redb` file backs both the lane
    /// records and the capability table.
    #[must_use]
    pub const fn new(inner: std::sync::Arc<PersistentPoolStateStore>) -> Self {
        Self { inner }
    }
}

impl crate::payment_settlement::CapabilitySource for StoredCapabilitySource {
    fn registration_material(
        &self,
        key: &LaneKey,
    ) -> Option<crate::payment_settlement::CapabilityMaterial> {
        self.inner
            .get_capability(key.pool_id, key.signer)
            .map(|record| crate::payment_settlement::CapabilityMaterial {
                spending_cap: U256::from_be_bytes(record.spending_cap),
                expiry: record.expiry,
                owner_sig: alloy::primitives::Bytes::from(record.owner_sig),
            })
    }
}

// ---------------------------------------------------------------------------
// Buyer-side store (#744)
//
// The record codec and every table operation live in
// `decdn_incentive::buyer_pool_table` — the same code the client's
// `RedbBuyerPoolStore` runs (#1246). This file keeps only the wiring: which
// file the table lives in (`lanes.redb`, shared with the seller, pending-settle,
// and watcher-checkpoint tables) and which `TableDefinition` names it.
// ---------------------------------------------------------------------------

/// Test/e2e-only seam on the concrete store. Kept here rather than on the
/// handle because the buyer reconciliation + mixed-reclaim e2e (#763) holds a
/// `PersistentPoolStateStore`.
#[cfg(any(test, feature = "anvil-e2e"))]
impl PersistentPoolStateStore {
    /// Test/e2e-only: write raw value bytes under a buyer pool key, bypassing
    /// the postcard encoder, to simulate a row left undecodable by a binary
    /// downgrade.
    ///
    /// # Errors
    ///
    /// [`StoreError::Backend`] if the write or durable commit fails.
    pub fn insert_raw_buyer_record(&self, pool_id: PoolId, bytes: &[u8]) -> Result<(), StoreError> {
        BuyerPoolTable::new(&self.buyer_db).insert_raw(pool_id, bytes)
    }
}

/// [`BuyerPoolStore`] adapter over the shared [`PersistentPoolStateStore`].
///
/// Holds an `Arc` to the same store the seller path uses, so both the seller
/// `lane_state_v1` table and the buyer pool table live in one redb file behind
/// one handle. Hand this to the buyer service as `Arc<dyn BuyerPoolStore>`.
#[derive(Debug, Clone)]
pub struct BuyerPoolStoreHandle {
    inner: std::sync::Arc<PersistentPoolStateStore>,
}

impl BuyerPoolStoreHandle {
    /// Wrap a shared persistent store as a buyer-pool store.
    #[must_use]
    pub const fn new(inner: std::sync::Arc<PersistentPoolStateStore>) -> Self {
        Self { inner }
    }

    /// View this store's `lanes.redb` as the buyer pool table.
    ///
    /// Not `const` — it derefs the `Arc`, which const fns cannot do.
    fn table(&self) -> BuyerPoolTable<'_> {
        BuyerPoolTable::new(&self.inner.buyer_db)
    }
}

/// Every method delegates to [`decdn_incentive::buyer_pool_table`]; this newtype
/// contributes the `lanes.redb` wiring, not the logic.
impl BuyerPoolStore for BuyerPoolStoreHandle {
    fn load_all(&self) -> Result<BuyerLoad, StoreError> {
        self.table().load_all()
    }

    fn record(&self, state: &BuyerPoolState) -> Result<(), StoreError> {
        self.table().record(state)
    }

    fn forget(&self, owner: Address) -> Result<(), StoreError> {
        self.table().forget(owner)
    }

    fn forget_if_pool(&self, owner: Address, pool_id: PoolId) -> Result<bool, StoreError> {
        self.table().forget_if_pool(owner, pool_id)
    }

    fn get_by_pool_id(&self, pool_id: PoolId) -> Result<Option<BuyerPoolState>, StoreError> {
        self.table().get_by_pool_id(pool_id)
    }

    fn get_by_owner(&self, owner: Address) -> Result<Option<BuyerPoolState>, StoreError> {
        self.table().get_by_owner(owner)
    }

    fn advance_progress(
        &self,
        owner: Address,
        pool_id: PoolId,
        lane: LaneKey,
        bytes: U256,
        amount: U256,
    ) -> Result<AdvanceOutcome, StoreError> {
        self.table()
            .advance_progress(owner, pool_id, lane, bytes, amount)
    }

    fn add_deposit(
        &self,
        owner: Address,
        pool_id: PoolId,
        additional: U256,
    ) -> Result<DepositOutcome, StoreError> {
        self.table().add_deposit(owner, pool_id, additional)
    }
}

impl PersistentPoolStateStore {
    /// Insert/overwrite a pending-settle entry in `table` (fsync-on-commit). The
    /// `PENDING_SETTLE_TABLE` and `BUYER_PENDING_SETTLE_TABLE` share this body so
    /// the seller and buyer pending sets stay byte-for-byte consistent.
    fn pending_record_in(
        &self,
        table_def: TableDefinition<'_, &[u8; 32], u64>,
        entry: &PendingSettle,
    ) -> Result<(), StoreError> {
        let key: [u8; 32] = entry.pool_id.into();
        let mut write_txn = self
            .settle_db
            .begin_write()
            .map_err(|err| StoreError::Backend(format!("begin_write: {err}")))?;
        // Force fsync-on-commit, same durability discipline as the other tables: the
        // pool is already closing on-chain by the time an entry is written here,
        // so a post-close crash that lost it would strand the settlement
        // obligation — the very gap this fsync guards.
        write_txn
            .set_durability(Durability::Immediate)
            .map_err(|err| StoreError::Backend(format!("set_durability: {err}")))?;
        {
            let mut table = write_txn
                .open_table(table_def)
                .map_err(|err| StoreError::Backend(format!("open_table: {err}")))?;
            table
                .insert(&key, entry.settle_after)
                .map_err(|err| StoreError::Backend(format!("insert: {err}")))?;
        }
        write_txn
            .commit()
            .map_err(|err| StoreError::Backend(format!("commit (fsync): {err}")))?;
        Ok(())
    }

    /// Load every pending-settle entry from `table` (an absent table is the
    /// empty set, not an error — first-boot tolerance).
    fn pending_load_from(
        &self,
        table_def: TableDefinition<'_, &[u8; 32], u64>,
    ) -> Result<Vec<PendingSettle>, StoreError> {
        let read_txn = self
            .settle_db
            .begin_read()
            .map_err(|err| StoreError::Backend(format!("begin_read: {err}")))?;
        let table = match read_txn.open_table(table_def) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(err) => return Err(StoreError::Backend(format!("open_table: {err}"))),
        };
        let mut out = Vec::new();
        let iter = table
            .iter()
            .map_err(|err| StoreError::Backend(format!("table iter: {err}")))?;
        for entry in iter {
            let (key_guard, value_guard) =
                entry.map_err(|err| StoreError::Backend(format!("iter entry: {err}")))?;
            out.push(PendingSettle {
                pool_id: B256::from(*key_guard.value()),
                settle_after: value_guard.value(),
            });
        }
        Ok(out)
    }

    /// Remove a pending-settle entry from `table` (a no-op against a
    /// never-written table, which must not be created as a side effect).
    fn pending_forget_in(
        &self,
        table_def: TableDefinition<'_, &[u8; 32], u64>,
        pool_id: B256,
    ) -> Result<(), StoreError> {
        let key: [u8; 32] = pool_id.into();
        {
            let read_txn = self
                .settle_db
                .begin_read()
                .map_err(|err| StoreError::Backend(format!("begin_read: {err}")))?;
            match read_txn.open_table(table_def) {
                Ok(_) => {}
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(()),
                Err(err) => return Err(StoreError::Backend(format!("open_table: {err}"))),
            }
        }

        let mut write_txn = self
            .settle_db
            .begin_write()
            .map_err(|err| StoreError::Backend(format!("begin_write: {err}")))?;
        write_txn
            .set_durability(Durability::Immediate)
            .map_err(|err| StoreError::Backend(format!("set_durability: {err}")))?;
        {
            let mut table = write_txn
                .open_table(table_def)
                .map_err(|err| StoreError::Backend(format!("open_table: {err}")))?;
            table
                .remove(&key)
                .map_err(|err| StoreError::Backend(format!("remove: {err}")))?;
        }
        write_txn
            .commit()
            .map_err(|err| StoreError::Backend(format!("commit (fsync): {err}")))?;
        Ok(())
    }
}

impl PendingSettleStore for PersistentPoolStateStore {
    fn record_pending(&self, entry: &PendingSettle) -> Result<(), StoreError> {
        self.pending_record_in(PENDING_SETTLE_TABLE, entry)
    }

    fn load_pending(&self) -> Result<Vec<PendingSettle>, StoreError> {
        self.pending_load_from(PENDING_SETTLE_TABLE)
    }

    fn forget_pending(&self, pool_id: B256) -> Result<(), StoreError> {
        self.pending_forget_in(PENDING_SETTLE_TABLE, pool_id)
    }
}

/// Map a redb error from the floor-loss surface into a [`StoreError`],
/// surfacing database corruption distinctly (#1782): `redb::Error::Corrupted`
/// becomes [`StoreError::Corrupt`], so the reservation-drop logging can tell
/// "the payment database is corrupt" (operator intervention: close and reopen —
/// redb refuses further writes after a mid-commit failure) from a transient
/// backend fault. Everything else stays [`StoreError::Backend`].
fn floor_loss_backend_err(
    op: &str,
    pool_id: Option<B256>,
    err: impl Into<redb::Error>,
) -> StoreError {
    match err.into() {
        redb::Error::Corrupted(detail) => StoreError::Corrupt {
            pool_id,
            detail: format!("{op}: {detail}"),
        },
        other => StoreError::Backend(format!("{op}: {other}")),
    }
}

/// Delete every signer row of one pool from an OPEN floor-loss table, inside the
/// caller's write transaction. redb cannot remove while a range iterator borrows
/// the table, so the keys are collected first — the same collect-then-remove shape
/// [`PoolFloorLossStore::sweep_forgotten`] uses. Every row implies at least one
/// admitted floor charge against the pool, so the count is bounded by
/// `remaining − M` divided by one credit window. That is small for an honest pool,
/// whose signers pay and prune. It is NOT small for one that sprays signer
/// identities: a large deposit admits a row per window of headroom, so the bound is
/// the pool's budget rather than any modest constant. Minting a signer needs an
/// owner signature, which is what keeps this off the anonymous-abuse path.
fn remove_pool_floor_rows(
    table: &mut redb::Table<'_, &'static [u8; POOL_SIGNER_KEY_LEN], (u128, u64)>,
    pool_id: B256,
) -> Result<(), StoreError> {
    let (lo, hi) = pool_signer_key_range(pool_id);
    let keys: Vec<[u8; POOL_SIGNER_KEY_LEN]> = {
        let iter = table
            .range::<&[u8; POOL_SIGNER_KEY_LEN]>(&lo..=&hi)
            .map_err(|e| floor_loss_backend_err("range (pool prefix)", Some(pool_id), e))?;
        let mut keys = Vec::new();
        for entry in iter {
            let (key_guard, _) =
                entry.map_err(|e| floor_loss_backend_err("range entry", Some(pool_id), e))?;
            keys.push(*key_guard.value());
        }
        keys
    };
    for key in &keys {
        table
            .remove(key)
            .map_err(|e| floor_loss_backend_err("remove", Some(pool_id), e))?;
    }
    Ok(())
}

impl PoolFloorLossStore for PersistentPoolStateStore {
    fn record_bucket(
        &self,
        pool_id: B256,
        signer: Address,
        consumed_micro: u128,
        refill_unix_ms: u64,
    ) -> Result<(), StoreError> {
        let pool_key: [u8; 32] = pool_id.into();
        let key = pool_signer_key_bytes(pool_id, signer);
        let mut write_txn = self
            .floor_loss_db
            .begin_write()
            .map_err(|e| floor_loss_backend_err("begin_write", Some(pool_id), e))?;
        // Force fsync-on-commit, same durability discipline as the other tables: a
        // lost bucket snapshot after a restart would silently grant a burst-abandoner
        // a fresh allowance on this signer.
        write_txn
            .set_durability(Durability::Immediate)
            .map_err(|e| floor_loss_backend_err("set_durability", Some(pool_id), e))?;
        // A tombstoned pool is closed: this write is a stale in-flight persist that
        // lost the race with `forget_loss`, and honoring it would resurrect a row
        // no later forget will ever delete (#1781). The check sits inside the write
        // transaction — redb's exclusive writer slot orders it against the
        // tombstone insert — so there is no window between check and write. The
        // `open_table` creates the tombstone table on a fresh store; the abort
        // below rolls that back on the no-op paths, and the advancing path commits
        // a real write anyway.
        let advanced = {
            let forgotten = write_txn
                .open_table(POOL_FLOOR_LOSS_FORGOTTEN_TABLE)
                .map_err(|e| floor_loss_backend_err("open_table (tombstones)", Some(pool_id), e))?;
            let tombstoned = forgotten
                .get(&pool_key)
                .map_err(|e| floor_loss_backend_err("get (tombstone)", Some(pool_id), e))?
                .is_some();
            if tombstoned {
                false
            } else {
                // Timestamp-monotonic write: the bucket refills, so a row is not
                // monotonic in `consumed_micro`, but the snapshot with the greatest
                // `refill_unix_ms` is the freshest view. The caller persists from an
                // independent blocking task, so two writes for one lane can land out
                // of order; keeping the greater-timestamp snapshot (ties break to the
                // larger `consumed_micro`) means a reordered stale write never
                // regresses the throttle. `begin_write` holds redb's exclusive writer
                // slot, so the compare and the write are atomic together.
                let mut table = write_txn
                    .open_table(POOL_FLOOR_LOSS_TABLE)
                    .map_err(|e| floor_loss_backend_err("open_table", Some(pool_id), e))?;
                let existing = table
                    .get(&key)
                    .map_err(|e| floor_loss_backend_err("get", Some(pool_id), e))?
                    .map(|v| v.value());
                let keep = existing.is_none_or(|(stored_micro, stored_ms)| {
                    refill_unix_ms > stored_ms
                        || (refill_unix_ms == stored_ms && consumed_micro > stored_micro)
                });
                if keep {
                    table
                        .insert(&key, (consumed_micro, refill_unix_ms))
                        .map_err(|e| floor_loss_backend_err("insert", Some(pool_id), e))?;
                    true
                } else {
                    false
                }
            }
        };
        // Nothing changed, so abort rather than fsync a transaction that holds no
        // change. This is sound only because every writer of `floor-loss.redb`
        // commits with `Durability::Immediate`: `existing` is therefore already
        // durable and at least as fresh as what this call carries (or the pool is
        // tombstoned and the write is stale), so the postcondition holds without a
        // write. A `Durability::None` writer on this file breaks that. Aborting does
        // not free redb's writer slot any earlier than a commit would — the slot was
        // taken at `begin_write` — it saves the fsync, which is what serializes this
        // call against the other floor-loss writers on `floor-loss.redb`'s slot.
        if !advanced {
            return write_txn
                .abort()
                .map_err(|e| floor_loss_backend_err("abort (no-op write)", Some(pool_id), e));
        }
        write_txn
            .commit()
            .map_err(|e| floor_loss_backend_err("commit (fsync)", Some(pool_id), e))?;
        Ok(())
    }

    fn load_buckets(&self) -> Result<Vec<(B256, Address, u128, u64)>, StoreError> {
        let read_txn = self
            .floor_loss_db
            .begin_read()
            .map_err(|e| floor_loss_backend_err("begin_read", None, e))?;
        // A never-written table means no signer has drained a bucket yet —
        // first-boot tolerance, matching every family table in the store.
        let table = match read_txn.open_table(POOL_FLOOR_LOSS_TABLE) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(err) => return Err(floor_loss_backend_err("open_table", None, err)),
        };
        let mut out = Vec::new();
        let iter = table
            .iter()
            .map_err(|e| floor_loss_backend_err("table iter", None, e))?;
        for entry in iter {
            let (key_guard, value_guard) =
                entry.map_err(|e| floor_loss_backend_err("iter entry", None, e))?;
            let (pool_id, signer) = pool_signer_key_parts(key_guard.value());
            let (micro, ms) = value_guard.value();
            out.push((pool_id, signer, micro, ms));
        }
        Ok(out)
    }

    fn forget_loss(&self, pool_id: B256) -> Result<(), StoreError> {
        let pool_key: [u8; 32] = pool_id.into();
        let mut write_txn = self
            .floor_loss_db
            .begin_write()
            .map_err(|e| floor_loss_backend_err("begin_write", Some(pool_id), e))?;
        write_txn
            .set_durability(Durability::Immediate)
            .map_err(|e| floor_loss_backend_err("set_durability", Some(pool_id), e))?;
        // Row deletes and tombstone insert in ONE transaction: a `record_bucket`
        // serialized after this commit sees the tombstone, so the deletes cannot be
        // undone by an in-flight persist (#1781). EVERY signer row of the pool goes
        // — the deposit they all drew on is reclaimed. The tombstone goes in even
        // when the pool never recorded a row — the racing `record_bucket` may be the
        // pool's FIRST — so forget takes no "never-written store" early-return:
        // it must always leave the marker.
        {
            let mut table = write_txn
                .open_table(POOL_FLOOR_LOSS_TABLE)
                .map_err(|e| floor_loss_backend_err("open_table", Some(pool_id), e))?;
            remove_pool_floor_rows(&mut table, pool_id)?;
            let mut forgotten = write_txn
                .open_table(POOL_FLOOR_LOSS_FORGOTTEN_TABLE)
                .map_err(|e| floor_loss_backend_err("open_table (tombstones)", Some(pool_id), e))?;
            forgotten
                .insert(&pool_key, ())
                .map_err(|e| floor_loss_backend_err("insert (tombstone)", Some(pool_id), e))?;
        }
        write_txn
            .commit()
            .map_err(|e| floor_loss_backend_err("commit (fsync)", Some(pool_id), e))?;
        Ok(())
    }

    fn sweep_forgotten(&self) -> Result<usize, StoreError> {
        // No separate existence probe: `open_table` creates a missing table
        // inside this transaction, and the no-op abort below rolls that
        // creation back — the same abort-rolls-back property `record_bucket`'s
        // in-transaction tombstone check relies on. A fresh store therefore
        // ends the sweep exactly as it began. The table existing does NOT
        // imply a tombstone — any committed `record_bucket` creates it empty as
        // a side effect of its check — that case also takes the no-op abort.
        let mut write_txn = self
            .floor_loss_db
            .begin_write()
            .map_err(|e| floor_loss_backend_err("begin_write", None, e))?;
        write_txn
            .set_durability(Durability::Immediate)
            .map_err(|e| floor_loss_backend_err("set_durability", None, e))?;
        let swept = {
            let mut forgotten = write_txn
                .open_table(POOL_FLOOR_LOSS_FORGOTTEN_TABLE)
                .map_err(|e| floor_loss_backend_err("open_table (tombstones)", None, e))?;
            let keys: Vec<[u8; 32]> = {
                let iter = forgotten
                    .iter()
                    .map_err(|e| floor_loss_backend_err("table iter (tombstones)", None, e))?;
                let mut keys = Vec::new();
                for entry in iter {
                    let (key_guard, _) = entry
                        .map_err(|e| floor_loss_backend_err("iter entry (tombstones)", None, e))?;
                    keys.push(*key_guard.value());
                }
                keys
            };
            // Belt-and-braces: `record_bucket`'s in-transaction tombstone check
            // means a tombstoned pool can hold no loss row, so each prefix delete
            // is expected to remove nothing. It keeps the sweep's postcondition —
            // neither row nor tombstone for a forgotten pool — independent of that
            // invariant.
            let mut table = write_txn
                .open_table(POOL_FLOOR_LOSS_TABLE)
                .map_err(|e| floor_loss_backend_err("open_table", None, e))?;
            for key in &keys {
                remove_pool_floor_rows(&mut table, B256::from(*key))?;
                forgotten
                    .remove(key)
                    .map_err(|e| floor_loss_backend_err("remove (tombstone)", None, e))?;
            }
            keys.len()
        };
        // An empty sweep holds no change: skip the fsync, same rationale as
        // `record_bucket`'s no-op abort.
        if swept == 0 {
            return write_txn
                .abort()
                .map_err(|e| floor_loss_backend_err("abort (no-op sweep)", None, e))
                .map(|()| 0);
        }
        write_txn
            .commit()
            .map_err(|e| floor_loss_backend_err("commit (fsync)", None, e))?;
        Ok(swept)
    }
}

/// [`PendingSettleStore`] over the buyer's `buyer_pending_settle_v1` table,
/// isolated from the seller's `pending_settle_v1` table so the two settle
/// sweeps never finalize each other's closes (#988). Wraps the same shared
/// [`PersistentPoolStateStore`] (one redb file, one handle); hand this to the
/// buyer service as `Arc<dyn PendingSettleStore>`.
#[derive(Debug, Clone)]
pub struct BuyerPendingSettleStoreHandle {
    inner: std::sync::Arc<PersistentPoolStateStore>,
}

impl BuyerPendingSettleStoreHandle {
    /// Wrap a shared persistent store as the buyer pending-settle store.
    #[must_use]
    pub const fn new(inner: std::sync::Arc<PersistentPoolStateStore>) -> Self {
        Self { inner }
    }
}

impl PendingSettleStore for BuyerPendingSettleStoreHandle {
    fn record_pending(&self, entry: &PendingSettle) -> Result<(), StoreError> {
        self.inner
            .pending_record_in(BUYER_PENDING_SETTLE_TABLE, entry)
    }

    fn load_pending(&self) -> Result<Vec<PendingSettle>, StoreError> {
        self.inner.pending_load_from(BUYER_PENDING_SETTLE_TABLE)
    }

    fn forget_pending(&self, pool_id: B256) -> Result<(), StoreError> {
        self.inner
            .pending_forget_in(BUYER_PENDING_SETTLE_TABLE, pool_id)
    }
}

impl KeyedCheckpointStore for PersistentPoolStateStore {
    fn load_checkpoint(&self, key: CheckpointKey) -> Result<Option<u64>, StoreError> {
        let read_txn = self
            .checkpoint_db
            .begin_read()
            .map_err(|err| StoreError::Backend(format!("begin_read: {err}")))?;
        // A never-written checkpoint table means "no prior scan" — first boot.
        let table = match read_txn.open_table(WATCHER_CHECKPOINT_TABLE) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(err) => return Err(StoreError::Backend(format!("open_table: {err}"))),
        };
        let value = table
            .get(key.as_str())
            .map_err(|err| StoreError::Backend(format!("get: {err}")))?;
        Ok(value.map(|g| g.value()))
    }

    fn record_checkpoint(&self, key: CheckpointKey, block: u64) -> Result<(), StoreError> {
        let mut write_txn = self
            .checkpoint_db
            .begin_write()
            .map_err(|err| StoreError::Backend(format!("begin_write: {err}")))?;
        // Force fsync-on-commit, same durability discipline as the other tables.
        write_txn
            .set_durability(Durability::Immediate)
            .map_err(|err| StoreError::Backend(format!("set_durability: {err}")))?;
        {
            let mut table = write_txn
                .open_table(WATCHER_CHECKPOINT_TABLE)
                .map_err(|err| StoreError::Backend(format!("open_table: {err}")))?;
            table
                .insert(key.as_str(), block)
                .map_err(|err| StoreError::Backend(format!("insert: {err}")))?;
        }
        write_txn
            .commit()
            .map_err(|err| StoreError::Backend(format!("commit (fsync): {err}")))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::{address, b256};
    use std::sync::Arc;
    use tempfile::TempDir;

    fn data_dir() -> anyhow::Result<TempDir> {
        let dir = TempDir::new()?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700))?;
        }
        Ok(dir)
    }

    /// A lane with a `signer` distinct from `provider`, so a decoder that
    /// transposed the two — or dropped a key segment — fails the assertion
    /// instead of round-tripping by coincidence.
    fn sample(byte: u8) -> LaneState {
        let mut pool = [0u8; 32];
        pool[31] = byte;
        let mut signer = [0u8; 20];
        signer[19] = byte;
        LaneState::hydrate(
            B256::from(pool),
            Address::from(signer),
            address!("00000000000000000000000000000000000000de"),
            U256::from(10_000_000u64),
            1_900_000_000 + u64::from(byte),
            U256::from(byte) * U256::from(1_000u64),
            U256::from(byte) * U256::from(1_024u64),
            Some([byte; 65]),
            LaneChain::NONE,
        )
    }

    /// Build a hydrated [`LaneState`] with the given identity + spending cap.
    /// `pool_byte` seeds the pool id, `signer_byte` the signer; the provider is
    /// a fixed non-signer address so a signer/provider transposition would be
    /// caught.
    fn mk_lane(pool_byte: u8, signer_byte: u8, cap: u64) -> LaneState {
        let mut pool = [0u8; 32];
        pool[31] = pool_byte;
        let mut signer = [0u8; 20];
        signer[19] = signer_byte;
        LaneState::hydrate(
            B256::from(pool),
            Address::from(signer),
            address!("00000000000000000000000000000000000000de"),
            U256::from(cap),
            1_900_000_000,
            U256::from(1_234u64),
            U256::from(4_096u64),
            Some([0xABu8; 65]),
            LaneChain::NONE,
        )
    }

    /// The capability store round-trips: what the seller intake persists via
    /// [`CapabilitySink::stage_capability`] is exactly what the redeemer reads
    /// through [`StoredCapabilitySource`], and a `(pool_id, signer)` with no
    /// stored capability yields `None` (so the redeemer safely skips it).
    #[test]
    fn capability_store_round_trips_and_missing_is_none() -> anyhow::Result<()> {
        use crate::payment_settlement::CapabilitySource;

        let dir = data_dir()?;
        let store = Arc::new(PersistentPoolStateStore::open(dir.path())?);
        let pool_id = b256!("00000000000000000000000000000000000000000000000000000000000000a7");
        let signer = address!("00000000000000000000000000000000000000b5");
        let provider = address!("00000000000000000000000000000000000000c9");
        let owner_sig = vec![0x42u8; 65];
        let spending_cap = U256::from(5_000_000u64);
        let expiry = 1_950_000_000u64;

        // A signer with nothing persisted yields None.
        let source = StoredCapabilitySource::new(Arc::clone(&store));
        let empty_key = LaneKey {
            pool_id,
            signer,
            provider,
        };
        anyhow::ensure!(source.registration_material(&empty_key).is_none());

        // Persist through the write trait, read back through the source.
        CapabilitySink::stage_capability(
            store.as_ref(),
            pool_id,
            signer,
            spending_cap,
            expiry,
            &owner_sig,
        );
        let material = source
            .registration_material(&empty_key)
            .ok_or_else(|| anyhow::anyhow!("expected stored capability material"))?;
        anyhow::ensure!(material.spending_cap == spending_cap);
        anyhow::ensure!(material.expiry == expiry);
        anyhow::ensure!(material.owner_sig.as_ref() == owner_sig.as_slice());

        // The provider segment of the lane key is ignored (capability is keyed by
        // (pool_id, signer)): a different provider under the same signer still
        // resolves the same material.
        let other_provider_key = LaneKey {
            pool_id,
            signer,
            provider: address!("00000000000000000000000000000000000000ff"),
        };
        anyhow::ensure!(source.registration_material(&other_provider_key).is_some());

        // Reads above are served from the working set, so they prove nothing
        // about the table. Flush, reopen, and check every field survives the
        // postcard round-trip — a field-order regression in `StoredCapability`
        // is invisible to an in-memory read.
        store.flush()?;
        drop(source);
        drop(store);
        let reopened = Arc::new(PersistentPoolStateStore::open(dir.path())?);
        let material = StoredCapabilitySource::new(reopened)
            .registration_material(&empty_key)
            .ok_or_else(|| anyhow::anyhow!("expected capability material after reopen"))?;
        anyhow::ensure!(material.spending_cap == spending_cap, "cap survives reopen");
        anyhow::ensure!(material.expiry == expiry, "expiry survives reopen");
        anyhow::ensure!(
            material.owner_sig.as_ref() == owner_sig.as_slice(),
            "owner signature survives reopen"
        );
        Ok(())
    }

    /// Forgetting a lane drops its capability row from the working set AND from
    /// the table: registration material is only ever asked for by [`LaneKey`],
    /// so a row whose lane is gone is unreachable, and keeping it would grow
    /// both with the count of distinct `(pool_id, signer)` pairs ever seen.
    #[test]
    fn forgetting_a_lane_drops_its_capability_row() -> anyhow::Result<()> {
        use crate::payment_settlement::CapabilitySource;

        let dir = data_dir()?;
        let store = Arc::new(PersistentPoolStateStore::open(dir.path())?);
        let pool_id = b256!("00000000000000000000000000000000000000000000000000000000000000d1");
        let signer = address!("00000000000000000000000000000000000000d2");
        let provider = address!("00000000000000000000000000000000000000d3");
        let key = LaneKey {
            pool_id,
            signer,
            provider,
        };
        CapabilitySink::stage_capability(
            store.as_ref(),
            pool_id,
            signer,
            U256::from(9u64),
            2000,
            &[0x11u8; 65],
        );
        store.flush()?;
        anyhow::ensure!(
            StoredCapabilitySource::new(Arc::clone(&store))
                .registration_material(&key)
                .is_some(),
            "the flushed capability is readable before the forget"
        );

        PoolStateStore::forget(store.as_ref(), key)?;
        anyhow::ensure!(
            store.caps.is_empty(),
            "forget drops the row from the working set immediately"
        );
        store.flush()?;
        drop(store);

        let reopened = Arc::new(PersistentPoolStateStore::open(dir.path())?);
        anyhow::ensure!(
            reopened.caps.is_empty(),
            "the flushed delete keeps the row from being re-hydrated"
        );
        anyhow::ensure!(
            StoredCapabilitySource::new(reopened)
                .registration_material(&key)
                .is_none(),
            "a forgotten lane's capability is gone after a reopen"
        );
        Ok(())
    }

    /// One flush carrying lane writes, a lane tombstone, a capability write and
    /// a capability delete lands all four in the same transaction. The two
    /// tables share one commit, so this is the shape that would expose a
    /// capability-side error taking the lane frontier down with it.
    #[test]
    fn flush_persists_mixed_lane_and_capability_work() -> anyhow::Result<()> {
        use crate::payment_settlement::CapabilitySource;

        let dir = data_dir()?;
        let store = Arc::new(PersistentPoolStateStore::open(dir.path())?);
        let kept = mk_lane(0x01, 0xE0, 40);
        let dropped = mk_lane(0x02, 0xE0, 41);
        let kept_key = kept.key();
        let dropped_key = dropped.key();

        // Seed both lanes and both capability rows, and get them on disk.
        PoolStateStore::record(store.as_ref(), &kept)?;
        PoolStateStore::record(store.as_ref(), &dropped)?;
        for lane in [&kept_key, &dropped_key] {
            CapabilitySink::stage_capability(
                store.as_ref(),
                lane.pool_id,
                lane.signer,
                U256::from(7u64),
                3000,
                &[0x33u8; 65],
            );
        }
        store.flush()?;

        // Now one flush that writes a lane, tombstones a lane, rewrites one
        // capability and deletes the other.
        let advanced = mk_lane(0x01, 0xE0, 99);
        PoolStateStore::record(store.as_ref(), &advanced)?;
        CapabilitySink::stage_capability(
            store.as_ref(),
            kept_key.pool_id,
            kept_key.signer,
            U256::from(8u64),
            3001,
            &[0x44u8; 65],
        );
        PoolStateStore::forget(store.as_ref(), dropped_key)?;
        store.flush()?;
        drop(store);

        let reopened = Arc::new(PersistentPoolStateStore::open(dir.path())?);
        let lanes = reopened.load_all()?;
        anyhow::ensure!(lanes.len() == 1, "only the kept lane survives");
        let source = StoredCapabilitySource::new(Arc::clone(&reopened));
        let material = source
            .registration_material(&kept_key)
            .ok_or_else(|| anyhow::anyhow!("kept lane keeps its capability"))?;
        anyhow::ensure!(
            material.spending_cap == U256::from(8u64) && material.expiry == 3001,
            "the capability rewrite landed in the same flush as the lane work"
        );
        anyhow::ensure!(
            source.registration_material(&dropped_key).is_none(),
            "the forgotten lane's capability delete landed in the same flush"
        );
        Ok(())
    }

    /// A `stage_capability` that lands after its key was drained is captured by
    /// the NEXT flush, never dropped — the same guarantee `record` has. The
    /// dedup decides under the row's shard lock, so a changed value always
    /// re-marks the row even while a flush is draining concurrently.
    #[test]
    fn concurrent_capability_writes_during_flush_are_not_lost() -> anyhow::Result<()> {
        use crate::payment_settlement::CapabilitySource;
        use std::sync::atomic::{AtomicBool, Ordering};

        const ROWS: u8 = 8;
        const ROUNDS: u64 = 60;

        let dir = data_dir()?;
        let store = Arc::new(PersistentPoolStateStore::open(dir.path())?);
        let stop = Arc::new(AtomicBool::new(false));

        let flusher = {
            let store = Arc::clone(&store);
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || -> anyhow::Result<()> {
                while !stop.load(Ordering::Relaxed) {
                    store.flush()?;
                    std::thread::yield_now();
                }
                Ok(())
            })
        };

        let mut writers = Vec::new();
        for row in 0..ROWS {
            let store = Arc::clone(&store);
            writers.push(std::thread::spawn(move || {
                for round in 1..=ROUNDS {
                    // `expiry` strictly increases with the round, so the last
                    // staged value for each row is the highest.
                    CapabilitySink::stage_capability(
                        store.as_ref(),
                        B256::repeat_byte(row),
                        Address::repeat_byte(row),
                        U256::from(round),
                        round,
                        &[row; 65],
                    );
                }
            }));
        }
        for w in writers {
            w.join()
                .map_err(|_| anyhow::anyhow!("writer thread panicked"))?;
        }
        stop.store(true, Ordering::Relaxed);
        flusher
            .join()
            .map_err(|_| anyhow::anyhow!("flusher thread panicked"))??;

        store.flush()?;
        drop(store);

        let reopened = Arc::new(PersistentPoolStateStore::open(dir.path())?);
        let source = StoredCapabilitySource::new(reopened);
        for row in 0..ROWS {
            let material = source
                .registration_material(&LaneKey {
                    pool_id: B256::repeat_byte(row),
                    signer: Address::repeat_byte(row),
                    provider: Address::repeat_byte(0xAA),
                })
                .ok_or_else(|| anyhow::anyhow!("row {row} lost every write"))?;
            anyhow::ensure!(
                material.expiry == ROUNDS,
                "row {row} must reach its final staged value on disk, got {}",
                material.expiry
            );
        }
        Ok(())
    }

    /// A repeated intake of an IDENTICAL capability pushes nothing onto the
    /// dirty work-list: the buffered row already equals
    /// `{spending_cap, expiry, owner_sig}`, so the dedup makes repeated sends
    /// free instead of re-queuing an fsync. Only a genuinely changed row pushes.
    #[test]
    fn repeated_identical_capability_write_marks_nothing_dirty() -> anyhow::Result<()> {
        use crate::payment_settlement::CapabilitySource;

        let dir = data_dir()?;
        let store = Arc::new(PersistentPoolStateStore::open(dir.path())?);
        let pool_id = b256!("9000000000000000000000000000000000000000000000000000000000000001");
        let signer = address!("00000000000000000000000000000000000000ee");
        let owner_sig = vec![0x42u8; 65];

        CapabilitySink::stage_capability(
            store.as_ref(),
            pool_id,
            signer,
            U256::from(5u64),
            1000,
            &owner_sig,
        );
        anyhow::ensure!(
            store.dirty_caps.len() == 1,
            "first write marks the row dirty"
        );
        // Identical repeat: nothing pushed.
        CapabilitySink::stage_capability(
            store.as_ref(),
            pool_id,
            signer,
            U256::from(5u64),
            1000,
            &owner_sig,
        );
        anyhow::ensure!(
            store.dirty_caps.len() == 1,
            "identical repeat must not re-mark the row"
        );
        // A changed value pushes the key again; the flush dedups the two.
        CapabilitySink::stage_capability(
            store.as_ref(),
            pool_id,
            signer,
            U256::from(6u64),
            1000,
            &owner_sig,
        );
        anyhow::ensure!(
            store.dirty_caps.len() == 2,
            "a changed value re-marks the row"
        );
        // Flush drains the work-list; the changed value is what lands.
        store.flush()?;
        anyhow::ensure!(
            store.dirty_caps.is_empty(),
            "flush drains the dirty capability work-list"
        );
        let source = StoredCapabilitySource::new(Arc::clone(&store));
        let material = source
            .registration_material(&LaneKey {
                pool_id,
                signer,
                provider: address!("00000000000000000000000000000000000000de"),
            })
            .ok_or_else(|| anyhow::anyhow!("expected stored capability material"))?;
        anyhow::ensure!(
            material.spending_cap == U256::from(6u64),
            "the updated cap landed"
        );
        // After a flush the buffer still holds the row, so re-writing the same
        // value is still a dedup no-op — only a changed value re-marks.
        CapabilitySink::stage_capability(
            store.as_ref(),
            pool_id,
            signer,
            U256::from(6u64),
            1000,
            &owner_sig,
        );
        anyhow::ensure!(
            store.dirty_caps.is_empty(),
            "re-writing the just-flushed value is still a dedup no-op"
        );
        Ok(())
    }

    /// Crash-between-flush replay semantics: an unflushed capability row is
    /// lost on reopen (frontier loss) — but the client re-sends the capability
    /// on its next request, so a second intake restores it, and once flushed
    /// the row survives.
    #[test]
    fn unflushed_capability_is_lost_but_resend_restores_it() -> anyhow::Result<()> {
        use crate::payment_settlement::CapabilitySource;

        let dir = data_dir()?;
        let pool_id = b256!("9100000000000000000000000000000000000000000000000000000000000001");
        let signer = address!("00000000000000000000000000000000000000dd");
        let provider = address!("00000000000000000000000000000000000000de");
        let owner_sig = vec![0x99u8; 65];

        // Write WITHOUT flushing, then drop the store (a crash).
        {
            let store = PersistentPoolStateStore::open(dir.path())?;
            CapabilitySink::stage_capability(
                &store,
                pool_id,
                signer,
                U256::from(7u64),
                2000,
                &owner_sig,
            );
        }
        // Reopen: the row is gone (frontier-loss-ok), so the redeemer would
        // find no material yet.
        let store = Arc::new(PersistentPoolStateStore::open(dir.path())?);
        let source = StoredCapabilitySource::new(Arc::clone(&store));
        anyhow::ensure!(
            source
                .registration_material(&LaneKey {
                    pool_id,
                    signer,
                    provider
                })
                .is_none(),
            "an unflushed capability row is lost on a crash"
        );
        // The client re-sends on its next request; intake re-persists, and a
        // later flush makes it durable. The pre-flush `source` clone must go
        // before reopen — it holds the database handle open.
        CapabilitySink::stage_capability(
            store.as_ref(),
            pool_id,
            signer,
            U256::from(7u64),
            2000,
            &owner_sig,
        );
        store.flush()?;
        drop(source);
        drop(store);
        let store = PersistentPoolStateStore::open(dir.path())?;
        let source = StoredCapabilitySource::new(Arc::new(store));
        let material = source
            .registration_material(&LaneKey {
                pool_id,
                signer,
                provider,
            })
            .ok_or_else(|| anyhow::anyhow!("a flushed capability row must survive a re-open"))?;
        anyhow::ensure!(material.expiry == 2000);
        Ok(())
    }

    #[test]
    fn open_empty_store_returns_no_entries() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let store = PersistentPoolStateStore::open(dir.path())?;
        anyhow::ensure!(store.load_all()?.is_empty());
        Ok(())
    }

    #[test]
    fn record_then_load_round_trip() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let store = PersistentPoolStateStore::open(dir.path())?;
        let a = sample(1);
        let b = sample(2);
        store.record(&a)?;
        store.record(&b)?;
        let mut all = store.load_all()?;
        all.sort_by_key(|s| s.pool_id);
        anyhow::ensure!(all.len() == 2);
        let first = all.first().ok_or_else(|| anyhow::anyhow!("missing [0]"))?;
        let second = all.get(1).ok_or_else(|| anyhow::anyhow!("missing [1]"))?;
        anyhow::ensure!(*first == a);
        anyhow::ensure!(*second == b);
        Ok(())
    }

    #[test]
    fn record_persists_across_reopen() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let s = sample(7);
        {
            let store = PersistentPoolStateStore::open(dir.path())?;
            store.record(&s)?;
            store.flush()?;
        }
        let store = PersistentPoolStateStore::open(dir.path())?;
        let all = store.load_all()?;
        anyhow::ensure!(all.len() == 1);
        let only = all.first().ok_or_else(|| anyhow::anyhow!("missing [0]"))?;
        anyhow::ensure!(*only == s);
        Ok(())
    }

    #[test]
    fn forget_removes_entry() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let store = PersistentPoolStateStore::open(dir.path())?;
        let s = sample(3);
        store.record(&s)?;
        store.forget(s.key())?;
        anyhow::ensure!(store.load_all()?.is_empty());
        // forget on a never-recorded lane is a no-op
        store.forget(LaneKey {
            pool_id: b256!("1111111111111111111111111111111111111111111111111111111111111111"),
            signer: address!("00000000000000000000000000000000000000a1"),
            provider: address!("00000000000000000000000000000000000000b2"),
        })?;
        Ok(())
    }

    #[test]
    fn get_round_trips_and_reports_unknown() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let store = PersistentPoolStateStore::open(dir.path())?;
        let s = sample(9);
        // get on a never-written store (no table yet) is None, not an error.
        anyhow::ensure!(store.get(s.key())?.is_none());

        store.record(&s)?;
        let got = store
            .get(s.key())?
            .ok_or_else(|| anyhow::anyhow!("expected Some"))?;
        anyhow::ensure!(got == s, "get must round-trip incl. signature");
        anyhow::ensure!(
            store
                .get(LaneKey {
                    pool_id: b256!(
                        "2222222222222222222222222222222222222222222222222222222222222222"
                    ),
                    signer: s.signer,
                    provider: s.provider,
                })?
                .is_none(),
            "unknown lane -> None"
        );
        Ok(())
    }

    /// Two lanes that share a `pool_id` but differ in `signer` are distinct rows
    /// — the lane key is the full `(pool_id, signer, provider)` triple, not the
    /// pool id alone. Also proves the decoder recovers `signer`/`provider` from
    /// the key rather than transposing them.
    #[test]
    fn lanes_sharing_a_pool_are_distinct_rows() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let store = PersistentPoolStateStore::open(dir.path())?;
        let base = sample(1);
        let sibling = LaneState::hydrate(
            base.pool_id,
            address!("00000000000000000000000000000000000000c1"),
            base.provider,
            base.cap,
            base.expiry,
            base.last_amount(),
            base.last_bytes_delivered(),
            base.last_signature().copied(),
            LaneChain::NONE,
        );
        store.record(&base)?;
        store.record(&sibling)?;
        anyhow::ensure!(
            store.load_all()?.len() == 2,
            "distinct signers, distinct rows"
        );
        let got = store
            .get(sibling.key())?
            .ok_or_else(|| anyhow::anyhow!("sibling missing"))?;
        anyhow::ensure!(
            got == sibling,
            "sibling lane must round-trip its own signer"
        );
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn file_mode_is_owner_only() -> anyhow::Result<()> {
        use std::os::unix::fs::PermissionsExt;
        let dir = data_dir()?;
        let _store = PersistentPoolStateStore::open(dir.path())?;
        // Every family file is hardened to 0o600, not just the lane store.
        for file in [
            LANES_DB_FILE,
            SETTLE_DB_FILE,
            FLOOR_LOSS_DB_FILE,
            CHECKPOINT_DB_FILE,
            BUYER_DB_FILE,
        ] {
            let path = dir.path().join(file);
            anyhow::ensure!(path.exists(), "{file} must be created at open()");
            let mode = std::fs::metadata(&path)?.permissions().mode() & 0o777;
            anyhow::ensure!(
                mode == DB_FILE_MODE,
                "{file} mode {mode:o} != expected {DB_FILE_MODE:o}"
            );
        }
        Ok(())
    }

    /// **Cleanup-asymmetry regression (#527 follow-up).** On `tighten_permissions`
    /// failure for a FRESHLY-CREATED file, the cleanup branch MUST remove the
    /// partial file so the next start sees a clean state.
    #[test]
    fn fresh_file_chmod_failure_removes_file() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let path_buf = dir.path().join(LANES_DB_FILE);
        anyhow::ensure!(!path_buf.exists(), "precondition: file does not exist");

        // `open_with` now hardens one file per family, so `chmod_fn` is `Fn`: it
        // builds a fresh error each call rather than moving a captured one out.
        // Failing on the first file (lanes.redb) aborts the open there.
        let err = PersistentPoolStateStore::open_with(dir.path(), |path| {
            Err(StoreError::PermissionTighten {
                path: path.to_path_buf(),
                source: std::io::Error::other("simulated chmod failure"),
            })
        })
        .err()
        .ok_or_else(|| anyhow::anyhow!("open_with should propagate the chmod failure"))?;

        anyhow::ensure!(
            matches!(err, StoreError::PermissionTighten { .. }),
            "expected PermissionTighten, got {err:?}",
        );
        anyhow::ensure!(
            !path_buf.exists(),
            "freshly-created file MUST be removed on chmod failure",
        );
        Ok(())
    }

    /// **Cleanup-asymmetry regression (#527 follow-up).** On `tighten_permissions`
    /// failure for a PRE-EXISTING file, the cleanup branch MUST NOT remove the
    /// file — otherwise a transient chmod error on a subsequent boot destroys
    /// live voucher state and reopens the replay window.
    #[test]
    fn pre_existing_file_chmod_failure_preserves_file() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let recorded = sample(7);
        let recorded_key = recorded.key();

        {
            let store = PersistentPoolStateStore::open(dir.path())?;
            store.record(&recorded)?;
            store.flush()?;
        }

        let path_buf = dir.path().join(LANES_DB_FILE);
        anyhow::ensure!(
            path_buf.exists(),
            "precondition: file exists with voucher state"
        );
        let size_before = std::fs::metadata(&path_buf)?.len();

        // `chmod_fn` is `Fn` (see `fresh_file_chmod_failure_removes_file`): fail on
        // whichever family file is offered, building a fresh error each call.
        let err = PersistentPoolStateStore::open_with(dir.path(), |path| {
            Err(StoreError::PermissionTighten {
                path: path.to_path_buf(),
                source: std::io::Error::other("simulated EROFS"),
            })
        })
        .err()
        .ok_or_else(|| anyhow::anyhow!("open_with should propagate the chmod failure"))?;
        anyhow::ensure!(
            matches!(err, StoreError::PermissionTighten { .. }),
            "expected PermissionTighten, got {err:?}",
        );

        anyhow::ensure!(
            path_buf.exists(),
            "pre-existing file MUST NOT be removed on chmod failure (issue #527 replay window)",
        );
        let size_after = std::fs::metadata(&path_buf)?.len();
        anyhow::ensure!(
            size_after == size_before,
            "file size MUST be unchanged ({size_before} → {size_after})",
        );

        let recovered = PersistentPoolStateStore::open(dir.path())?;
        let all = recovered.load_all()?;
        anyhow::ensure!(all.len() == 1);
        let only = all.first().ok_or_else(|| anyhow::anyhow!("missing"))?;
        anyhow::ensure!(only.key() == recorded_key);
        anyhow::ensure!(*only == recorded);
        Ok(())
    }

    /// A record carrying a `schema_version` higher than this binary supports —
    /// `load_all` MUST refuse to decode it (per ADR 003: silently dropping
    /// unknown fields is unsafe because we cannot honour the persistence
    /// invariant for fields we don't understand).
    #[test]
    fn future_schema_version_refuses_to_load() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let s = sample(4);
        let key_bytes = lane_key_bytes(&s.key());
        let forward = StoredLaneState {
            schema_version: SUPPORTED_SCHEMA_VERSION + 1,
            ..StoredLaneState::from(&s)
        };
        let encoded = postcard::to_allocvec(&forward)?;
        {
            let store = PersistentPoolStateStore::open(dir.path())?;
            let mut wtx = store.lanes_db.begin_write()?;
            wtx.set_durability(Durability::Immediate)?;
            {
                let mut table = wtx.open_table(LANE_TABLE)?;
                table.insert(&key_bytes, encoded.as_slice())?;
            }
            wtx.commit()?;
        }
        // Hydration at open() eagerly decodes the whole table, so the future
        // schema version is caught on reopen rather than on the first `load_all`.
        let err = PersistentPoolStateStore::open(dir.path())
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected UnsupportedSchema"))?;
        anyhow::ensure!(
            matches!(err, StoreError::UnsupportedSchema { .. }),
            "{err:?}"
        );
        Ok(())
    }

    /// A newer writer adding an additive field appears to this reader as
    /// trailing bytes; `take_from_bytes` ignores them and the record decodes
    /// cleanly (the regression guard against re-introducing strict decoding).
    #[test]
    fn extra_trailing_bytes_are_tolerated() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let s = sample(0x42);
        let key_bytes = lane_key_bytes(&s.key());
        let mut encoded = postcard::to_allocvec(&StoredLaneState::from(&s))?;
        encoded.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x01, 0x02, 0x03]);
        {
            let store = PersistentPoolStateStore::open(dir.path())?;
            let mut tx = store.lanes_db.begin_write()?;
            tx.set_durability(Durability::Immediate)?;
            {
                let mut t = tx.open_table(LANE_TABLE)?;
                t.insert(&key_bytes, encoded.as_slice())?;
            }
            tx.commit()?;
        }
        // Hydration at open() reads the whole table, so reopen to pick up the
        // record this test wrote directly (bypassing the working set).
        let store = PersistentPoolStateStore::open(dir.path())?;
        let all = store.load_all()?;
        anyhow::ensure!(all.len() == 1);
        let only = all.first().ok_or_else(|| anyhow::anyhow!("missing"))?;
        anyhow::ensure!(*only == s, "decoded record must equal the original");
        Ok(())
    }

    /// A stored signature that is neither empty nor exactly 65 bytes MUST be
    /// rejected as `Corrupt` rather than silently truncated/padded into a value
    /// the seller path would submit to `PaymentPool.redeem` (#751).
    #[test]
    fn wrong_length_signature_is_corrupt() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let s = sample(0x77);
        let key_bytes = lane_key_bytes(&s.key());
        let mut stored = StoredLaneState::from(&s);
        stored.signature = vec![0xCD; 64];
        let encoded = postcard::to_allocvec(&stored)?;
        {
            let store = PersistentPoolStateStore::open(dir.path())?;
            let mut tx = store.lanes_db.begin_write()?;
            tx.set_durability(Durability::Immediate)?;
            {
                let mut t = tx.open_table(LANE_TABLE)?;
                t.insert(&key_bytes, encoded.as_slice())?;
            }
            tx.commit()?;
        }
        // Hydration at open() eagerly decodes the whole table, so the corrupt
        // signature is caught on reopen rather than on the first `load_all`.
        let err = PersistentPoolStateStore::open(dir.path())
            .err()
            .ok_or_else(|| anyhow::anyhow!("wrong-length signature must reject"))?;
        anyhow::ensure!(
            matches!(&err, StoreError::Corrupt { pool_id: Some(id), detail }
                if *id == s.pool_id && detail.contains("expected 0 or 65")),
            "expected Corrupt(...expected 0 or 65...), got {err:?}",
        );
        Ok(())
    }

    /// A corrupt capability row refuses the open, deliberately: the redeemer
    /// must not silently miss registration material for a signer it is about to
    /// register on-chain, and a skipped row would surface only as a reverted
    /// `redeemMany` batch. The diagnostic names BOTH halves of the table key —
    /// the pool alone does not identify the row an operator has to repair.
    #[test]
    fn corrupt_capability_row_refuses_the_open_and_names_the_row() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let pool_id = b256!("00000000000000000000000000000000000000000000000000000000000000c1");
        let signer = address!("00000000000000000000000000000000000000c2");
        let key_bytes = pool_signer_key_bytes(pool_id, signer);
        {
            let store = PersistentPoolStateStore::open(dir.path())?;
            let mut tx = store.lanes_db.begin_write()?;
            tx.set_durability(Durability::Immediate)?;
            {
                let mut table = tx.open_table(CAPABILITY_TABLE)?;
                // Not a `StoredCapability` postcard encoding.
                table.insert(&key_bytes, [0xFFu8; 3].as_slice())?;
            }
            tx.commit()?;
        }
        let err = PersistentPoolStateStore::open(dir.path())
            .err()
            .ok_or_else(|| anyhow::anyhow!("a corrupt capability row must refuse the open"))?;
        anyhow::ensure!(
            matches!(&err, StoreError::Corrupt { pool_id: Some(id), detail }
                if *id == pool_id
                    && detail.contains("capability postcard decode failed")
                    && detail.contains(&signer.to_string())),
            "expected a Corrupt naming the pool and the signer, got {err:?}",
        );
        Ok(())
    }

    /// The watcher scan checkpoint round-trips, overwrites in place, and
    /// survives a reopen — so the next boot resumes the `PoolOpened` backfill
    /// from the persisted block. An empty table reads as `None` (first boot).
    #[test]
    fn watcher_checkpoint_round_trip_and_persist() -> anyhow::Result<()> {
        let dir = data_dir()?;
        {
            let store = PersistentPoolStateStore::open(dir.path())?;
            anyhow::ensure!(store.load_checkpoint(CheckpointKey::PoolOpened)?.is_none());
            store.record_checkpoint(CheckpointKey::PoolOpened, 1_000)?;
            anyhow::ensure!(store.load_checkpoint(CheckpointKey::PoolOpened)? == Some(1_000));
            store.record_checkpoint(CheckpointKey::PoolOpened, 2_500)?;
            anyhow::ensure!(store.load_checkpoint(CheckpointKey::PoolOpened)? == Some(2_500));
        }
        let store = PersistentPoolStateStore::open(dir.path())?;
        anyhow::ensure!(
            store.load_checkpoint(CheckpointKey::PoolOpened)? == Some(2_500),
            "checkpoint must survive a restart"
        );
        Ok(())
    }

    /// Pending-settle entries round-trip, re-stamp on a re-close, survive a
    /// reopen, and forget cleanly (#327). Keyed by `pool_id`.
    #[test]
    fn pending_settle_round_trip_and_persist() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let a = PendingSettle {
            pool_id: sample(1).pool_id,
            settle_after: 1_700_000_000,
        };
        let b = PendingSettle {
            pool_id: sample(2).pool_id,
            settle_after: 1_700_000_500,
        };
        {
            let store = PersistentPoolStateStore::open(dir.path())?;
            store.record_pending(&a)?;
            store.record_pending(&b)?;
            store.record_pending(&PendingSettle {
                settle_after: 1_700_009_999,
                ..a
            })?;
        }
        let store = PersistentPoolStateStore::open(dir.path())?;
        let mut all = store.load_pending()?;
        all.sort_by_key(|p| p.pool_id);
        anyhow::ensure!(all.len() == 2, "overwrite must not add a row");
        let first = all.first().ok_or_else(|| anyhow::anyhow!("missing [0]"))?;
        anyhow::ensure!(first.settle_after == 1_700_009_999, "deadline re-stamped");
        // forget clears it.
        store.forget_pending(a.pool_id)?;
        store.forget_pending(b.pool_id)?;
        anyhow::ensure!(store.load_pending()?.is_empty());
        Ok(())
    }

    /// Two distinct signers on one pool, so every floor-loss test exercises the
    /// `(pool_id, signer)` key rather than a pool-wide row.
    fn loss_signers() -> (Address, Address) {
        (
            address!("00000000000000000000000000000000000000a1"),
            address!("00000000000000000000000000000000000000b2"),
        )
    }

    /// Every [`PoolFloorLossStore`] keeps a `(pool, signer)` lane's freshest bucket
    /// snapshot: a stale, older-timestamp write — two floor-reservation drops on one
    /// lane landing out of order — leaves the greater-timestamp one in place, and a
    /// newer snapshot wins even when its consumed level is lower (the bucket refilled).
    /// It also keeps signers independent (one lane's bucket never gates another's) and
    /// drops EVERY signer row of a forgotten pool. Run over both impls so the memory
    /// store used in tests speaks for the redb store that ships.
    fn assert_floor_bucket_keeps_freshest<S: PoolFloorLossStore>(store: &S) -> anyhow::Result<()> {
        let pool = b256!("7700000000000000000000000000000000000000000000000000000000000000");
        let (s1, s2) = loss_signers();
        store.record_bucket(pool, s1, 5_000, 200)?;
        // An older-timestamp write (a reordered drop) does not clobber the fresher row.
        store.record_bucket(pool, s1, 10, 100)?;
        anyhow::ensure!(store.load_buckets()? == vec![(pool, s1, 5_000u128, 200u64)]);
        // A newer timestamp wins even though its consumed level is lower (refill).
        store.record_bucket(pool, s1, 1_000, 300)?;
        anyhow::ensure!(store.load_buckets()? == vec![(pool, s1, 1_000u128, 300u64)]);
        // A SECOND signer on the same pool is its own row, independent of the first.
        store.record_bucket(pool, s2, 7, 100)?;
        let mut both = store.load_buckets()?;
        both.sort_by_key(|&(_, signer, _, _)| signer);
        anyhow::ensure!(
            both == vec![(pool, s1, 1_000u128, 300u64), (pool, s2, 7u128, 100u64)],
            "per-signer rows are independent, not one pool-wide bucket"
        );
        // Forget is terminal AND pool-wide: the delete drops every signer row and
        // tombstones the pool, so an in-flight persist landing after it — the #1781
        // interleaving — cannot resurrect a row for any signer. Only the bring-up
        // sweep clears the tombstone, and at bring-up no persist can be in flight.
        store.forget_loss(pool)?;
        store.record_bucket(pool, s1, 10, 400)?;
        store.record_bucket(pool, s2, 10, 400)?;
        anyhow::ensure!(
            store.load_buckets()?.is_empty(),
            "a record_bucket landing after forget_loss must not resurrect the row"
        );
        anyhow::ensure!(store.sweep_forgotten()? == 1, "one tombstone swept");
        store.record_bucket(pool, s1, 10, 500)?;
        anyhow::ensure!(
            store.load_buckets()? == vec![(pool, s1, 10u128, 500u64)],
            "after the bring-up sweep the pool id accepts writes again"
        );
        store.forget_loss(pool)?;
        Ok(())
    }

    #[test]
    fn floor_bucket_stores_agree_on_freshness() -> anyhow::Result<()> {
        assert_floor_bucket_keeps_freshest(&decdn_incentive::MemoryPoolFloorLossStore::new())?;
        let dir = data_dir()?;
        assert_floor_bucket_keeps_freshest(&PersistentPoolStateStore::open(dir.path())?)
    }

    /// The freshest snapshot is what reaches disk. The older-timestamp write lands
    /// LAST, so a store that wrote unconditionally would leave it behind for the
    /// reopen to find; this pins the durable outcome, not the mechanism.
    #[test]
    fn redb_pool_floor_bucket_freshest_survives_reopen() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let pool = sample(7).pool_id;
        let (s1, _) = loss_signers();
        {
            let store = PersistentPoolStateStore::open(dir.path())?;
            store.record_bucket(pool, s1, 5_000, 200)?;
            // A stale, older-timestamp snapshot lands LAST — a store that wrote
            // unconditionally would leave it behind for the reopen to find.
            store.record_bucket(pool, s1, 5_000, 200)?;
            store.record_bucket(pool, s1, 10, 100)?;
        }
        let store = PersistentPoolStateStore::open(dir.path())?;
        anyhow::ensure!(store.load_buckets()? == vec![(pool, s1, 5_000u128, 200u64)]);
        Ok(())
    }

    /// A forget tombstone is durable: a `record_bucket` landing after a restart —
    /// there is none in the real system, but the property it pins is that the
    /// tombstone rides the same fsync discipline as the rows — still cannot
    /// resurrect the row, and the bring-up sweep then reclaims the tombstone
    /// without disturbing other pools' rows (#1781).
    #[test]
    fn redb_forget_tombstone_survives_reopen_and_boot_sweep_reclaims_it() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let closed = sample(21).pool_id;
        let live = sample(22).pool_id;
        let (s1, s2) = loss_signers();
        {
            let store = PersistentPoolStateStore::open(dir.path())?;
            // TWO signers on the closed pool, so the forget must clear a whole
            // key prefix rather than a single row.
            store.record_bucket(closed, s1, 700, 100)?;
            store.record_bucket(closed, s2, 800, 100)?;
            store.record_bucket(live, s1, 900, 100)?;
            store.forget_loss(closed)?;
        }
        let store = PersistentPoolStateStore::open(dir.path())?;
        store.record_bucket(closed, s1, 700, 200)?;
        store.record_bucket(closed, s2, 800, 200)?;
        anyhow::ensure!(
            store.load_buckets()? == vec![(live, s1, 900u128, 100u64)],
            "the tombstone survives the reopen and blocks the late write for every signer"
        );
        anyhow::ensure!(store.sweep_forgotten()? == 1, "the boot sweep reclaims it");
        anyhow::ensure!(store.sweep_forgotten()? == 0, "sweep is idempotent");
        anyhow::ensure!(
            store.load_buckets()? == vec![(live, s1, 900u128, 100u64)],
            "the sweep does not disturb live rows"
        );
        Ok(())
    }

    /// `forget_loss` clears a pool's rows at BOTH inclusive bounds of the key range.
    /// Every other test uses interior signer addresses, so nothing else would catch
    /// `..` in place of `..=` in `pool_signer_key_range` — and the all-`0xff` row it
    /// would strand belongs to a pool that is now tombstoned, so `record_bucket` can
    /// never overwrite it and no later `forget_loss` ever deletes it again. That is
    /// the #1781 leak, reintroduced for exactly one address.
    #[test]
    fn forget_loss_clears_both_inclusive_bounds_of_the_key_range() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let store = PersistentPoolStateStore::open(dir.path())?;
        let pool = sample(9).pool_id;
        let neighbour = sample(10).pool_id;
        let lowest = Address::from([0x00u8; 20]);
        let highest = Address::from([0xffu8; 20]);
        store.record_bucket(pool, lowest, 11, 100)?;
        store.record_bucket(pool, highest, 22, 100)?;
        // A neighbouring pool at both bounds too, so the range cannot simply be
        // deleting everything.
        store.record_bucket(neighbour, lowest, 33, 100)?;
        store.record_bucket(neighbour, highest, 44, 100)?;
        anyhow::ensure!(store.load_buckets()?.len() == 4);

        store.forget_loss(pool)?;
        let mut left = store.load_buckets()?;
        left.sort_by_key(|&(_, signer, _, _)| signer);
        anyhow::ensure!(
            left == vec![
                (neighbour, lowest, 33u128, 100u64),
                (neighbour, highest, 44u128, 100u64)
            ],
            "forget must clear the pool's rows at both bounds and neither neighbour's, got {left:?}"
        );
        Ok(())
    }

    /// The #1781 interleaving, raced for real: one thread forgets the pool while
    /// another lands the `record_bucket` a reservation drop dispatched before the
    /// pool closed. Whichever order redb's exclusive writer slot serializes them
    /// in, the terminal state is "no row": record-then-forget deletes it,
    /// forget-then-record hits the tombstone. Without the tombstone, the second
    /// ordering re-inserts a row nothing ever deletes again.
    #[test]
    fn record_bucket_racing_forget_loss_never_leaves_a_row() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let store = std::sync::Arc::new(PersistentPoolStateStore::open(dir.path())?);
        for round in 0u8..8 {
            let pool = B256::repeat_byte(round.saturating_add(0x30));
            let (s1, _) = loss_signers();
            store.record_bucket(pool, s1, 1_000, 100)?;
            let recorder = {
                let store = std::sync::Arc::clone(&store);
                std::thread::spawn(move || store.record_bucket(pool, s1, 2_000, 200))
            };
            let forgetter = {
                let store = std::sync::Arc::clone(&store);
                std::thread::spawn(move || store.forget_loss(pool))
            };
            recorder
                .join()
                .map_err(|_| anyhow::anyhow!("recorder thread panicked"))??;
            forgetter
                .join()
                .map_err(|_| anyhow::anyhow!("forgetter thread panicked"))??;
            anyhow::ensure!(
                store.load_buckets()?.is_empty(),
                "round {round}: a late record_bucket resurrected a forgotten pool's row"
            );
        }
        Ok(())
    }

    /// Out-of-order snapshots for one pool settle on the greatest timestamp. This is
    /// the shape the freshness contract exists for: the caller reads its bucket under
    /// a lock, then persists it from an independent blocking task, so the writes
    /// arrive interleaved. Reading and writing inside one `begin_write` is what makes
    /// the compare-and-keep atomic — splitting the compare into its own read
    /// transaction would reintroduce a lost update, and this test is what would catch
    /// that. Each write stamps `consumed == ts`, so the row settles on the max ts.
    #[test]
    fn concurrent_out_of_order_snapshots_settle_on_the_freshest() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let store = std::sync::Arc::new(PersistentPoolStateStore::open(dir.path())?);
        let pool = sample(11).pool_id;
        let (s1, _) = loss_signers();
        // Jumbled per thread so no thread walks its own timestamps in order, and the
        // global maximum is not written by the last thread to finish.
        let threads: Vec<_> = (0u64..8)
            .map(|t| {
                let store = std::sync::Arc::clone(&store);
                std::thread::spawn(move || -> Result<(), StoreError> {
                    for i in 0u64..64 {
                        let ts = (i.wrapping_mul(37).wrapping_add(t.wrapping_mul(11))) % 500;
                        store.record_bucket(pool, s1, u128::from(ts), ts)?;
                    }
                    Ok(())
                })
            })
            .collect();
        for handle in threads {
            handle
                .join()
                .map_err(|_| anyhow::anyhow!("writer thread panicked"))??;
        }
        let want = (0u64..8)
            .flat_map(|t| {
                (0u64..64).map(move |i| (i.wrapping_mul(37).wrapping_add(t.wrapping_mul(11))) % 500)
            })
            .max()
            .ok_or_else(|| anyhow::anyhow!("empty value set"))?;
        anyhow::ensure!(
            store.load_buckets()? == vec![(pool, s1, u128::from(want), want)],
            "interleaved writers settle on the greatest timestamp, not the last write"
        );
        Ok(())
    }

    /// The floor-loss bucket store round-trips, survives a reopen (durable commit),
    /// and forgets cleanly. Keyed by `(pool_id, signer)`, so two signers on one pool
    /// hold two independent rows and a forget clears both.
    #[test]
    fn redb_pool_floor_bucket_round_trip_and_persist() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let a_pool = sample(1).pool_id;
        let b_pool = sample(2).pool_id;
        let (s1, s2) = loss_signers();
        {
            let store = PersistentPoolStateStore::open(dir.path())?;
            store.record_bucket(a_pool, s1, 1_234_567_890_123u128, 100)?;
            store.record_bucket(a_pool, s2, 7u128, 100)?;
            store.record_bucket(b_pool, s1, 42u128, 100)?;
            // A newer snapshot updates the row in place, not a second row.
            store.record_bucket(a_pool, s1, 999_999_999_999_999u128, 200)?;
        }
        // Reopen over the SAME path — the value must survive the durable commit.
        let store = PersistentPoolStateStore::open(dir.path())?;
        let mut all = store.load_buckets()?;
        all.sort_by_key(|&(id, signer, _, _)| (id, signer));
        anyhow::ensure!(all.len() == 3, "an update must not add a row");
        let first = all.first().ok_or_else(|| anyhow::anyhow!("missing [0]"))?;
        let second = all.get(1).ok_or_else(|| anyhow::anyhow!("missing [1]"))?;
        let third = all.get(2).ok_or_else(|| anyhow::anyhow!("missing [2]"))?;
        anyhow::ensure!(*first == (a_pool, s1, 999_999_999_999_999u128, 200u64));
        anyhow::ensure!(*second == (a_pool, s2, 7u128, 100u64));
        anyhow::ensure!(*third == (b_pool, s1, 42u128, 100u64));

        // forget clears EVERY signer row of the pool, and forget on a
        // never-recorded pool is a no-op.
        store.forget_loss(a_pool)?;
        anyhow::ensure!(
            store.load_buckets()? == vec![(b_pool, s1, 42u128, 100u64)],
            "forget clears both of pool a's signer rows and neither of pool b's"
        );
        store.forget_loss(b_pool)?;
        anyhow::ensure!(store.load_buckets()?.is_empty());
        store.forget_loss(b256!(
            "3333333333333333333333333333333333333333333333333333333333333333"
        ))?;
        Ok(())
    }

    /// Seller and buyer pending-settle sets are isolated: a pool recorded in one
    /// never appears in the other (#988).
    #[test]
    fn buyer_and_seller_pending_settle_sets_are_isolated() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let store = std::sync::Arc::new(PersistentPoolStateStore::open(dir.path())?);
        let buyer = BuyerPendingSettleStoreHandle::new(std::sync::Arc::clone(&store));
        let seller_pool = b256!("aa00000000000000000000000000000000000000000000000000000000000000");
        let buyer_pool = b256!("bb00000000000000000000000000000000000000000000000000000000000000");
        PendingSettleStore::record_pending(
            store.as_ref(),
            &PendingSettle {
                pool_id: seller_pool,
                settle_after: 1_000,
            },
        )?;
        buyer.record_pending(&PendingSettle {
            pool_id: buyer_pool,
            settle_after: 2_000,
        })?;
        let seller_set = PendingSettleStore::load_pending(store.as_ref())?;
        let buyer_set = buyer.load_pending()?;
        anyhow::ensure!(
            seller_set.len() == 1 && seller_set.first().map(|p| p.pool_id) == Some(seller_pool)
        );
        anyhow::ensure!(
            buyer_set.len() == 1 && buyer_set.first().map(|p| p.pool_id) == Some(buyer_pool)
        );
        Ok(())
    }

    #[test]
    fn record_is_buffered_until_flush_then_reload_sees_it() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let s = PersistentPoolStateStore::open(dir.path())?;
        s.record(&mk_lane(1, 0xAA, 2_000_000))?;
        // Buffered in this handle immediately.
        anyhow::ensure!(s.load_all()?.len() == 1, "record visible in-memory");
        // A fresh open BEFORE flush must NOT see it (nothing fsynced yet).
        drop(s);
        let s2 = PersistentPoolStateStore::open(dir.path())?;
        anyhow::ensure!(s2.load_all()?.is_empty(), "unflushed record is not durable");
        s2.record(&mk_lane(1, 0xAA, 2_000_000))?;
        s2.flush()?;
        drop(s2);
        let s3 = PersistentPoolStateStore::open(dir.path())?;
        anyhow::ensure!(s3.load_all()?.len() == 1, "flushed record survives reopen");
        Ok(())
    }

    #[test]
    fn forget_tombstone_applies_on_flush() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let s = PersistentPoolStateStore::open(dir.path())?;
        let lane = mk_lane(2, 0xBB, 100_000);
        s.record(&lane)?;
        s.flush()?;
        s.forget(lane.key())?;
        s.flush()?;
        drop(s);
        let s2 = PersistentPoolStateStore::open(dir.path())?;
        anyhow::ensure!(s2.load_all()?.is_empty(), "flushed forget survives reopen");
        Ok(())
    }

    /// The last byte of a lane's `pool_id`, which [`mk_lane`] varies — a payload
    /// tag that ties an encoded value to the key it belongs with.
    fn pool_tag(lane: &LaneKey) -> u8 {
        lane.pool_id.as_slice().last().copied().unwrap_or_default()
    }

    /// [`FlushSnapshot::sort_by_table_key`] puts both batches in redb key order,
    /// keeps every entry, and keeps each encoded value with its own key. `flush`
    /// builds the snapshot from a work-list drained through a `HashSet`, so this
    /// is the only place the ordering is fixed — redb reads a table back sorted
    /// whatever order it was written in.
    #[test]
    fn flush_snapshot_sorts_into_table_key_order() -> anyhow::Result<()> {
        // Disjoint key sets, as `writes` and `tombstones` are in a snapshot.
        let written: Vec<LaneKey> = (0u8..8).map(|i| mk_lane(i, 0xC0, 10).key()).collect();
        let forgotten: Vec<LaneKey> = (8u8..14).map(|i| mk_lane(i, 0xC0, 10).key()).collect();
        let mut snapshot = FlushSnapshot {
            // Reversed, so the input is worst-case descending.
            writes: written
                .iter()
                .rev()
                .map(|lane| (*lane, vec![pool_tag(lane)]))
                .collect(),
            tombstones: forgotten.iter().rev().copied().collect(),
            // Also reversed: the capability batches sort by their own raw table
            // key, which is what keeps `CAPABILITY_TABLE` appending rightward.
            cap_writes: (0u8..6)
                .rev()
                .map(|i| {
                    let key = pool_signer_key_bytes(B256::repeat_byte(i), Address::repeat_byte(i));
                    (key, vec![i])
                })
                .collect(),
            cap_deletes: (6u8..10)
                .rev()
                .map(|i| pool_signer_key_bytes(B256::repeat_byte(i), Address::repeat_byte(i)))
                .collect(),
        };
        snapshot.sort_by_table_key();

        anyhow::ensure!(
            snapshot
                .writes
                .iter()
                .map(|(lane, _)| lane_key_bytes(lane))
                .is_sorted(),
            "writes ascend by table key"
        );
        anyhow::ensure!(
            snapshot.tombstones.iter().map(lane_key_bytes).is_sorted(),
            "tombstones ascend by table key"
        );
        anyhow::ensure!(
            snapshot.writes.len() == written.len() && snapshot.tombstones.len() == forgotten.len(),
            "sorting drops and duplicates nothing"
        );
        anyhow::ensure!(
            snapshot
                .writes
                .iter()
                .all(|(lane, encoded)| *encoded == vec![pool_tag(lane)]),
            "every value still travels with its own key"
        );
        anyhow::ensure!(
            snapshot.cap_writes.iter().map(|(key, _)| *key).is_sorted(),
            "capability writes ascend by table key"
        );
        anyhow::ensure!(
            snapshot.cap_deletes.is_sorted(),
            "capability deletes ascend by table key"
        );
        anyhow::ensure!(
            snapshot.cap_writes.len() == 6 && snapshot.cap_deletes.len() == 4,
            "sorting the capability batches drops and duplicates nothing"
        );
        anyhow::ensure!(
            snapshot
                .cap_writes
                .iter()
                .all(|(key, encoded)| *encoded == vec![key.first().copied().unwrap_or_default()]),
            "every capability value still travels with its own key"
        );
        Ok(())
    }

    /// A batch large enough to span many leaf pages flushes in one transaction and
    /// hydrates whole at the next open, and it lands in redb packed rather than
    /// sprawling.
    ///
    /// `record` pushes lanes onto a work-list drained through a `HashSet`, so a
    /// test cannot choose the order the snapshot is built in — only
    /// [`FlushSnapshot::sort_by_table_key`] decides what redb sees. The
    /// `leaf_pages` bound is therefore the one assertion that ties `flush` to that
    /// sort: unsorted order measures 65-68 pages here and sorted measures 47, so
    /// dropping the call from `flush` trips the bound.
    #[test]
    fn flush_of_a_large_batch_lands_packed_and_round_trips() -> anyhow::Result<()> {
        use redb::ReadableTableMetadata as _;

        let dir = data_dir()?;
        let mut lanes: Vec<LaneState> = Vec::with_capacity(512);
        for pool in 0u8..=255 {
            for signer in 0u8..2 {
                lanes.push(mk_lane(pool, signer, 1_000 + u64::from(pool)));
            }
        }
        {
            let store = PersistentPoolStateStore::open(dir.path())?;
            for lane in &lanes {
                store.record(lane)?;
            }
            store.flush()?;
        }
        let store = PersistentPoolStateStore::open(dir.path())?;
        anyhow::ensure!(
            store.load_all()?.len() == lanes.len(),
            "every lane hydrates after the reopen"
        );
        for lane in &lanes {
            let got = store
                .get(lane.key())?
                .ok_or_else(|| anyhow::anyhow!("lane missing after reopen"))?;
            anyhow::ensure!(got == *lane, "each lane round-trips to its own record");
        }

        let read_txn = store.lanes_db.begin_read()?;
        let leaf_pages = read_txn.open_table(LANE_TABLE)?.stats()?.leaf_pages();
        anyhow::ensure!(
            leaf_pages <= 55,
            "a sorted flush packs {} lanes into <=55 leaf pages; got {leaf_pages} \
             (unsorted measures 65-68, so this is `flush` skipping the sort)",
            lanes.len()
        );
        Ok(())
    }

    #[test]
    fn flush_when_clean_is_noop_ok() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let s = PersistentPoolStateStore::open(dir.path())?;
        s.flush()?; // nothing dirty
        Ok(())
    }

    /// A lane whose `last_bytes_delivered` watermark is `bytes`, so a test can
    /// advance one lane across rounds and check which value reached disk.
    fn lane_at(pool_byte: u8, signer_byte: u8, bytes: u64) -> LaneState {
        let mut pool = [0u8; 32];
        pool[31] = pool_byte;
        let mut signer = [0u8; 20];
        signer[19] = signer_byte;
        LaneState::hydrate(
            B256::from(pool),
            Address::from(signer),
            address!("00000000000000000000000000000000000000de"),
            U256::from(1_000_000u64),
            1_900_000_000,
            U256::from(bytes),
            U256::from(bytes),
            Some([signer_byte; 65]),
            LaneChain::NONE,
        )
    }

    /// One flush persists a mix of dirty writes and tombstones in a single
    /// commit: the surviving lanes reload, the forgotten one does not.
    #[test]
    fn flush_persists_mixed_writes_and_tombstones() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let keep_a = mk_lane(1, 0xA1, 100_000);
        let keep_b = mk_lane(2, 0xB2, 200_000);
        let drop_c = mk_lane(3, 0xC3, 300_000);
        {
            let s = PersistentPoolStateStore::open(dir.path())?;
            // Persist drop_c first so the later forget produces a real tombstone
            // against an on-disk row, not a no-op against an absent key.
            s.record(&drop_c)?;
            s.flush()?;
            s.record(&keep_a)?;
            s.record(&keep_b)?;
            s.forget(drop_c.key())?;
            s.flush()?;
        }
        let s = PersistentPoolStateStore::open(dir.path())?;
        let mut all = s.load_all()?;
        all.sort_by_key(|l| l.pool_id);
        anyhow::ensure!(all.len() == 2, "one write forgotten, two survive");
        anyhow::ensure!(all.first() == Some(&keep_a));
        anyhow::ensure!(all.get(1) == Some(&keep_b));
        Ok(())
    }

    /// The store only needs a monotonic floor: a record that advances a lane
    /// after a prior flush pushes its key afresh and the newer value reaches disk
    /// on the next flush. Models the "record lands after the work-list is
    /// drained" case — the key pushed after one flush is honored by the next.
    #[test]
    fn monotonic_remark_after_flush_reaches_disk() -> anyhow::Result<()> {
        let dir = data_dir()?;
        {
            let s = PersistentPoolStateStore::open(dir.path())?;
            s.record(&lane_at(9, 0x9A, 1_000))?;
            s.flush()?;
            // Advance the same lane, then flush again — the re-mark must win.
            s.record(&lane_at(9, 0x9A, 5_000))?;
            s.flush()?;
        }
        let s = PersistentPoolStateStore::open(dir.path())?;
        let all = s.load_all()?;
        anyhow::ensure!(all.len() == 1);
        let only = all.first().ok_or_else(|| anyhow::anyhow!("missing [0]"))?;
        anyhow::ensure!(
            only.last_bytes_delivered() == U256::from(5_000u64),
            "the newer watermark, marked after the first flush, must reach disk"
        );
        Ok(())
    }

    /// Records made concurrently with a continuous stream of flushes are never
    /// blocked into a wedge and never lost: the fsync holds no lane shard lock,
    /// so a `record` that lands mid-commit pushes its lane onto the work-list and
    /// a later flush captures it. After the writers finish and one final flush
    /// runs, every lane's last (highest) watermark is on disk.
    #[test]
    fn concurrent_records_during_flush_are_not_lost() -> anyhow::Result<()> {
        use std::sync::atomic::{AtomicBool, Ordering};

        const LANES: u8 = 16;
        const ROUNDS: u64 = 60;

        let dir = data_dir()?;
        let store = Arc::new(PersistentPoolStateStore::open(dir.path())?);
        let stop = Arc::new(AtomicBool::new(false));

        // A flusher committing repeatedly while the writers advance lanes. It
        // yields between commits rather than busy-spinning (a tight loop would
        // burn a core, especially when a flush is a no-op), and surfaces any
        // flush error instead of swallowing it.
        let flusher = {
            let store = Arc::clone(&store);
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || -> anyhow::Result<()> {
                while !stop.load(Ordering::Relaxed) {
                    store.flush()?;
                    std::thread::yield_now();
                }
                Ok(())
            })
        };

        let mut writers = Vec::new();
        for lane in 0..LANES {
            let store = Arc::clone(&store);
            writers.push(std::thread::spawn(move || -> anyhow::Result<()> {
                for round in 1..=ROUNDS {
                    // Watermark strictly increases with the round, so the final
                    // record for each lane carries the highest value.
                    store.record(&lane_at(lane, lane, round * 4_096))?;
                }
                Ok(())
            }));
        }
        for w in writers {
            w.join()
                .map_err(|_| anyhow::anyhow!("writer thread panicked"))??;
        }
        stop.store(true, Ordering::Relaxed);
        flusher
            .join()
            .map_err(|_| anyhow::anyhow!("flusher thread panicked"))??;

        // A final flush drains whatever the last records re-marked, then reopen
        // and confirm every lane reached its highest watermark on disk.
        store.flush()?;
        let store =
            Arc::try_unwrap(store).map_err(|_| anyhow::anyhow!("outstanding store handles"))?;
        drop(store);

        let reopened = PersistentPoolStateStore::open(dir.path())?;
        let all = reopened.load_all()?;
        anyhow::ensure!(all.len() == usize::from(LANES), "every lane persisted");
        for lane in all {
            anyhow::ensure!(
                lane.last_bytes_delivered() == U256::from(ROUNDS * 4_096),
                "each lane must hold its final (highest) watermark, none lost"
            );
        }
        Ok(())
    }
}
