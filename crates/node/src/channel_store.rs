//! Disk-backed `PoolStateStore` for the node runtime.
//!
//! Implements [`PoolStateStore`] against a set of `redb` database files under
//! `<data_dir>`, one file per durable write family:
//!
//! - `lanes.redb` — seller lane frontier (`lane_state_v1`). Each record also
//!   carries its lane's owner-signed capability material (`owner_sig`), so the
//!   redeemer's registration payload lands in the same durable write as the
//!   frontier it backs.
//! - `settle.redb` — the seller and buyer pending-settle sets.
//! - `checkpoint.redb` — the settlement watcher's scan checkpoints.
//! - `buyer.redb` — the buyer pool state and its owner index.
//!
//! Each file has its own `redb` write-transaction slot, so a commit in one
//! family never waits on an unrelated commit in another: the periodic lane
//! flush, a settlement checkpoint, and a buyer top-up all proceed on
//! independent writer slots. Families are
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
//! The **owner-signed capability material** (`owner_sig`) is a field of the lane
//! record, not a side table. It is written by the same `record`/`flush` path as
//! the frontier, in the same fsynced transaction, so it can never be durable-out-
//! of-step with the frontier it backs: whenever a lane's frontier is on disk, its
//! `owner_sig` is too, and a `forget` deletes both in one row removal. A crash
//! between two flushes loses the lane's unflushed advances AND its `owner_sig`
//! together — never one without the other — and both recover the same way: the
//! frontier is superseded by the signer's next, higher voucher, and the material
//! is re-supplied because the client re-sends its capability on the next request
//! and intake re-writes it onto the lane (#1906).
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
//! restart without forfeiting the claim), the capability's spending cap, its
//! expiry (so the node stops serving once the grant lapses), the owner's
//! signature over the capability (`owner_sig`, the `ownerSig` the redeemer
//! submits to register the signer on-chain), and the lane's paid cumulative
//! (`paid_cumulative`, the on-chain redeemed watermark the redeemer subtracts
//! from owed — persisting it stops a restart from re-submitting already-redeemed
//! lanes, #2052). All of these are plain fields of the one `StoredLaneState`
//! record — there is a single on-disk format and no older shape to decode.
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
    CheckpointKey, KeyedCheckpointStore, PendingSettle, PendingSettleStore, PoolStateStore,
    StoreError,
};
use decdn_incentive::{
    AdvanceOutcome, BuyerLoad, BuyerPoolState, BuyerPoolStore, DepositOutcome, LaneChain, LaneKey,
    LaneState, PoolId,
};
use redb::{Database, Durability, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};

/// File name of the seller lane redb database within `data_dir`.
const LANES_DB_FILE: &str = decdn_common::data_dir::NODE_LANES_DB_FILE;

/// File name of the pending-settle redb database (seller and buyer sets).
const SETTLE_DB_FILE: &str = decdn_common::data_dir::NODE_SETTLE_DB_FILE;

/// File name of the settlement-watcher checkpoint redb database.
const CHECKPOINT_DB_FILE: &str = decdn_common::data_dir::NODE_CHECKPOINT_DB_FILE;

/// File name of the buyer pool redb database (buyer state + owner index).
///
/// Shared with the CLI through [`decdn_common::data_dir`] so both sides name
/// the same file: an operator tool that wants this store has to know it is not
/// the client's
/// [`CLIENT_BUYER_DB_FILE`](decdn_common::data_dir::CLIENT_BUYER_DB_FILE).
const BUYER_DB_FILE: &str = decdn_common::data_dir::NODE_BUYER_DB_FILE;

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

/// Byte width of a `(pool_id, signer)` key on disk: `32 + 20`. The superseded
/// owner-signed capability tables use this shape. The pool id leads, so every row
/// of one pool shares a 32-byte prefix and a range scan selects exactly that
/// pool's rows.
const POOL_SIGNER_KEY_LEN: usize = 52;

/// The two superseded owner-signed capability tables, keyed by `pool_id ‖ signer`
/// (`[u8; 52]`). Both are retired: the owner signature now rides the lane record
/// itself (`StoredLaneState::owner_sig`), so it commits in the same transaction
/// as the frontier it backs and can never be durable-out-of-step with it (#1906).
/// Nothing reads these; [`PersistentPoolStateStore::drop_superseded_capability_tables`]
/// deletes both at open so a development store carrying them does not keep dead
/// rows. Pre-launch there is nothing to migrate.
const SUPERSEDED_CAPABILITY_TABLE_V2: TableDefinition<'_, &[u8; POOL_SIGNER_KEY_LEN], &[u8]> =
    TableDefinition::new("capability_v2");
const SUPERSEDED_CAPABILITY_TABLE_V1: TableDefinition<'_, &[u8; POOL_SIGNER_KEY_LEN], &[u8]> =
    TableDefinition::new("capability_v1");

/// Encode a `(pool_id, signer)` pair into its `[u8; 52]` table key. The live
/// tables no longer use this shape; the superseded-capability drop test builds
/// keys with it to inject legacy rows.
#[cfg(test)]
fn pool_signer_key_bytes(pool_id: B256, signer: Address) -> [u8; POOL_SIGNER_KEY_LEN] {
    let mut out = [0u8; POOL_SIGNER_KEY_LEN];
    out[..32].copy_from_slice(pool_id.as_slice());
    out[32..].copy_from_slice(signer.as_slice());
    out
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
/// the mutable watermark plus the capability's `cap`/`expiry` and the owner's
/// signature over that capability (`owner_sig`).
///
/// `schema_version` is a forward tripwire only (see [`SUPPORTED_SCHEMA_VERSION`]):
/// `decode_record` uses [`postcard::take_from_bytes`], which tolerates trailing
/// bytes, so a record a newer binary wrote with an appended field still decodes
/// its known prefix here rather than erroring on the extra bytes.
///
/// `signature` and `owner_sig` are each a length-prefixed `Vec<u8>` on disk; the
/// in-memory [`LaneState`] carries the stronger `Option<[u8; 65]>`, and the
/// narrowing (empty → `None`, 65 → `Some`, anything else → corrupt) lives in
/// [`StoredLaneState::into_state`]. `owner_sig` is the owner's EIP-712 signature
/// over the lane's `Capability` — the `ownerSig` the redeemer submits as a
/// `PaymentPool.redeemMany` `CapabilityReg`. It is a field of the lane record,
/// not a side table, so it is committed in the same fsynced transaction as the
/// voucher frontier it backs and can never be durable-out-of-step with it.
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
    /// The owner's EIP-712 signature over the lane's `Capability` (`r‖s‖v`),
    /// length-prefixed on disk and narrowed to the in-memory `Option<[u8; 65]>`
    /// (empty → `None`, 65 → `Some`, anything else → corrupt) in
    /// [`StoredLaneState::into_state`]. The seller redeemer submits it as a
    /// `PaymentPool.redeemMany` `CapabilityReg` `ownerSig` to register the signer
    /// on-chain (ADR 003 §Capability delegation). A required field like
    /// `registered_until`: adding it is a breaking on-disk change — a record
    /// written before it fails to decode (the trailing length-prefix is absent,
    /// so `take_from_bytes` hits `DeserializeUnexpectedEnd`), it is not silently
    /// defaulted. That break is deliberate and unversioned: deCDN is pre-launch
    /// with no deployed store to stay compatible with.
    owner_sig: Vec<u8>,
    /// The lane's paid cumulative — the on-chain `newPaidCumulative` of the most
    /// recent `PoolRedeemed` this node observed for it — as a fixed-width
    /// big-endian `[u8; 32]`, like `last_amount`/`cap`. The seller redeemer
    /// subtracts it from the lane's owed value to get what it still has to cash;
    /// persisting it here (in the same fsynced row as the frontier) is what stops
    /// a restarted node from forgetting redemptions behind its log-poller cursor
    /// and re-submitting already-redeemed lanes for silent on-chain no-ops
    /// (#2052). A required field like `registered_until`/`owner_sig`: adding it is
    /// a breaking on-disk change (a record written before it fails to decode), not
    /// silently defaulted — deliberate and unversioned, as deCDN is pre-launch.
    paid_cumulative: [u8; 32],
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
            owner_sig: state.owner_sig.map_or_else(Vec::new, |s| s.to_vec()),
            paid_cumulative: state.paid_cumulative.to_be_bytes(),
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
        let owner_sig = decode_signature(&self.owner_sig, pool_id, "owner capability")?;
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
        state.owner_sig = owner_sig;
        state.paid_cumulative = U256::from_be_bytes(self.paid_cumulative);
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
    /// Seller lane frontier (`lanes.redb`). The owner-signed capability material
    /// each lane's redeemer needs rides the lane record itself (`owner_sig`), so
    /// the periodic flush commits it in the same transaction as the frontier.
    lanes_db: Database,
    /// Seller + buyer pending-settle sets (`settle.redb`).
    settle_db: Database,
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
        let checkpoint_db = Self::open_hardened_db(&data_dir.join(CHECKPOINT_DB_FILE), &chmod_fn)?;
        let buyer_db = Self::open_hardened_db(&data_dir.join(BUYER_DB_FILE), &chmod_fn)?;

        let lanes = Self::hydrate_lanes(&lanes_db)?;
        Self::drop_superseded_capability_tables(&lanes_db)?;
        Ok(Self {
            lanes_db,
            settle_db,
            checkpoint_db,
            buyer_db,
            path,
            lanes,
            dirty: SegQueue::new(),
        })
    }

    /// Open (or create) one hardened redb file at `path`, applying the empty-file
    /// guard, TOCTOU-safe create, and `0o600` tightening every lane-store family
    /// file gets. Each family lives in its own file so its commits take an
    /// independent `redb` writer slot.
    ///
    /// Rejects a zero-length file: `redb::Database::create` treats both "file
    /// does not exist" and "file exists but is empty" as "create a fresh
    /// database" — so a `truncate -s 0` (or a filesystem rollback that nukes
    /// content but preserves the inode) would start with an empty store. For
    /// `lanes.redb` that silently reopens the issue #527 voucher-replay window;
    /// for the other families it silently re-grants budget the lost rows bounded
    /// (a dropped pending-settle entry forgets an in-flight redemption).
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

    /// Delete the superseded `capability_v1` / `capability_v2` tables if the
    /// store still carries them. The owner-signed material each row held now rides
    /// the lane record itself ([`StoredLaneState::owner_sig`]), so it commits in
    /// the same transaction as the frontier it backs (#1906) and there is no
    /// side table to keep. Pre-launch there is no migration to run, so the rows
    /// are dropped — but loudly, because they are registration material a signer
    /// would then re-send on its next request rather than silently lose.
    ///
    /// Each table lives in the shared `lanes.redb` file, so a presence check runs
    /// in a READ transaction first and a write transaction opens ONLY when a table
    /// is actually there. A blind `begin_write`/`commit` on every boot would grow
    /// the lane file with an empty commit and break the #527 file-stability
    /// guarantee that `pre_existing_file_chmod_failure_preserves_file` pins.
    ///
    /// # Errors
    /// [`StoreError::Backend`] when a presence check or delete transaction fails.
    fn drop_superseded_capability_tables(db: &Database) -> Result<(), StoreError> {
        Self::drop_superseded_capability_table(
            db,
            SUPERSEDED_CAPABILITY_TABLE_V2,
            "capability_v2",
        )?;
        Self::drop_superseded_capability_table(db, SUPERSEDED_CAPABILITY_TABLE_V1, "capability_v1")
    }

    /// Delete one superseded capability table by definition + name, presence-
    /// checked in a read transaction so a store without it takes no write.
    fn drop_superseded_capability_table(
        db: &Database,
        table: TableDefinition<'_, &[u8; POOL_SIGNER_KEY_LEN], &[u8]>,
        name: &str,
    ) -> Result<(), StoreError> {
        let read_txn = db
            .begin_read()
            .map_err(|err| StoreError::Backend(format!("begin_read ({name} probe): {err}")))?;
        match read_txn.open_table(table) {
            Ok(_) => {}
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(()),
            Err(err) => {
                return Err(StoreError::Backend(format!(
                    "open_table ({name} probe): {err}"
                )));
            }
        }
        drop(read_txn);
        let txn = db
            .begin_write()
            .map_err(|err| StoreError::Backend(format!("begin_write ({name} drop): {err}")))?;
        let dropped = txn
            .delete_table(table)
            .map_err(|err| StoreError::Backend(format!("delete_table ({name}): {err}")))?;
        txn.commit()
            .map_err(|err| StoreError::Backend(format!("commit ({name} drop): {err}")))?;
        if dropped {
            tracing::warn!(
                table = name,
                "dropped the superseded capability table; its owner-signed registration \
                 material now rides the lane record and is re-sent by each client on its \
                 next request"
            );
        }
        Ok(())
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
                    // Never let a record that omits the owner-signed material drop
                    // a grant the lane already holds: like `registered_until`,
                    // `owner_sig` is trusted-writer state set once and kept, and
                    // losing it would strand the redeemer for a lane whose frontier
                    // still advances (#1906).
                    if next.owner_sig.is_none() {
                        next.owner_sig = existing.owner_sig;
                    }
                    // Same rule for the paid watermark: the voucher-record path
                    // carries it from the live in-memory lane, which never learns
                    // the redeemed cumulative (that rides the `PoolRedeemed`
                    // watcher via `set_paid_cumulative`). Keep the persisted value
                    // so a frontier advance cannot regress it to zero (#2052).
                    next.paid_cumulative = next.paid_cumulative.max(existing.paid_cumulative);
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

    /// Raise this lane's paid cumulative, monotonically — touches ONLY
    /// `paid_cumulative`, never the replay-critical `last_*` tuple, so a
    /// `PoolRedeemed` write cannot race a concurrent voucher `record` into a lost
    /// update. A no-op for a lane with no live record (a redemption observed after
    /// the lane was forgotten has nothing to advance). Buffered like `record`;
    /// durability is the next `flush`'s job (#2052).
    fn set_paid_cumulative(&self, key: LaneKey, paid_cumulative: U256) -> Result<(), StoreError> {
        let Some(mut slot) = self.lanes.get_mut(&key) else {
            return Ok(());
        };
        let LaneSlot::Live(state) = slot.value_mut() else {
            return Ok(());
        };
        if paid_cumulative > state.paid_cumulative {
            state.paid_cumulative = paid_cumulative;
            self.dirty.push(key);
        }
        Ok(())
    }

    /// Tombstone the lane so reads treat it as absent and the next flush deletes
    /// its on-disk row. Idempotent. The slot stays in the map (as a tombstone)
    /// so a concurrent `record` resurrecting the lane is decided under the same
    /// shard lock rather than racing a separate set. The lane's owner-signed
    /// capability material (`owner_sig`) is part of the record, so it goes with
    /// the lane in the same delete.
    fn forget(&self, key: LaneKey) -> Result<(), StoreError> {
        self.lanes.insert(key, LaneSlot::Tombstoned);
        self.dirty.push(key);
        Ok(())
    }

    /// Write every dirty lane and apply every tombstone in ONE fsynced redb
    /// transaction. Each lane record carries its own owner-signed capability
    /// material, so nothing is written separately. Idempotent — a no-op when
    /// clean.
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
        if drained.is_empty() {
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
                            // "clean" and never reach disk.
                            for key in &drained {
                                self.dirty.push(*key);
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
        let mut snapshot = FlushSnapshot { writes, tombstones };
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
            tracing::warn!(
                dirty_lanes = snapshot.writes.len() + snapshot.tombstones.len(),
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

/// Encoded dirty writes and tombstone keys resolved from the drained work-list,
/// so [`PersistentPoolStateStore::flush`] can run its fsynced commit with no
/// shard lock held. Each lane record already carries its own owner-signed
/// capability material, so there is no separate capability batch.
struct FlushSnapshot {
    writes: Vec<(LaneKey, Vec<u8>)>,
    tombstones: Vec<LaneKey>,
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
    /// through a `HashSet` whose iteration order carries no meaning.
    fn sort_by_table_key(&mut self) {
        self.writes
            .sort_by_cached_key(|(lane, _)| lane_key_bytes(lane));
        self.tombstones.sort_by_cached_key(lane_key_bytes);
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
        }
        write_txn
            .commit()
            .map_err(|err| StoreError::Backend(format!("commit (fsync): {err}")))?;
        Ok(())
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

    /// The owner-signed capability material rides the lane record: a lane
    /// recorded with an `owner_sig` and flushed reads back with the same
    /// `owner_sig` after a reopen. The material and the voucher frontier are one
    /// row committed in one transaction, so the redeemer's registration material
    /// can never be durable-out-of-step with the frontier it backs (the #1906
    /// stranding gap).
    #[test]
    fn owner_sig_rides_the_lane_record_through_flush_and_reopen() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let store = Arc::new(PersistentPoolStateStore::open(dir.path())?);
        let mut lane = sample(0x5a);
        lane.owner_sig = Some([0x42u8; 65]);
        PoolStateStore::record(store.as_ref(), &lane)?;
        store.flush()?;
        drop(store);

        // Reopen from disk: an in-memory read would prove nothing about the
        // postcard round-trip.
        let reopened = PersistentPoolStateStore::open(dir.path())?;
        let got = PoolStateStore::get(&reopened, lane.key())?
            .ok_or_else(|| anyhow::anyhow!("lane gone after reopen"))?;
        anyhow::ensure!(
            got.owner_sig == Some([0x42u8; 65]),
            "owner_sig survives the reopen with the frontier it backs"
        );
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

    /// A store carrying either superseded capability table drops both at open:
    /// the owner-signed material now rides the lane record itself, so the side
    /// tables are dead. The migration deletes them without reading a row. The
    /// lane frontier in the same file is untouched.
    #[test]
    fn superseded_capability_tables_are_dropped_at_open() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let recorded = sample(9);
        {
            let store = PersistentPoolStateStore::open(dir.path())?;
            store.record(&recorded)?;
            store.flush()?;
        }

        // Inject legacy `capability_v1` AND `capability_v2` rows directly, with
        // arbitrary bytes — the migration never decodes them, it deletes both
        // whole tables.
        let path = dir.path().join(LANES_DB_FILE);
        let key = pool_signer_key_bytes(B256::repeat_byte(0x51), Address::repeat_byte(0x52));
        {
            let db =
                Database::create(&path).map_err(|e| anyhow::anyhow!("open lanes.redb raw: {e}"))?;
            let txn = db.begin_write()?;
            {
                let mut v1 = txn.open_table(SUPERSEDED_CAPABILITY_TABLE_V1)?;
                v1.insert(&key, [0xAAu8; 40].as_slice())?;
                let mut v2 = txn.open_table(SUPERSEDED_CAPABILITY_TABLE_V2)?;
                v2.insert(&key, [0xBBu8; 17].as_slice())?;
            }
            txn.commit()?;
        }

        // Reopen through the store — the drop runs — then close it so the raw
        // read below can take the file.
        {
            let store = PersistentPoolStateStore::open(dir.path())?;
            let lanes = store.load_all()?;
            anyhow::ensure!(lanes.len() == 1, "the lane frontier survives the migration");
        }

        let db = Database::create(&path).map_err(|e| anyhow::anyhow!("reopen raw: {e}"))?;
        let read_txn = db.begin_read()?;
        anyhow::ensure!(
            matches!(
                read_txn.open_table(SUPERSEDED_CAPABILITY_TABLE_V1),
                Err(redb::TableError::TableDoesNotExist(_))
            ),
            "the superseded capability_v1 table must be gone after open"
        );
        anyhow::ensure!(
            matches!(
                read_txn.open_table(SUPERSEDED_CAPABILITY_TABLE_V2),
                Err(redb::TableError::TableDoesNotExist(_))
            ),
            "the superseded capability_v2 table must be gone after open"
        );
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

    /// A lane record with a bad-length `owner_sig` refuses the open: the redeemer
    /// must not submit malformed registration material to `PaymentPool.redeemMany`.
    /// The diagnostic names the pool the operator has to repair.
    #[test]
    fn corrupt_owner_sig_on_a_lane_refuses_the_open() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let mut lane = sample(0xc1);
        // A well-formed lane, then corrupt only its stored owner_sig to a length
        // that is neither empty (None) nor 65 (Some).
        lane.owner_sig = Some([0x11u8; 65]);
        let key_bytes = lane_key_bytes(&lane.key());
        {
            let store = PersistentPoolStateStore::open(dir.path())?;
            let mut stored = StoredLaneState::from(&lane);
            stored.owner_sig = vec![0xFFu8; 3];
            let encoded = postcard::to_allocvec(&stored)?;
            let mut tx = store.lanes_db.begin_write()?;
            tx.set_durability(Durability::Immediate)?;
            {
                let mut table = tx.open_table(LANE_TABLE)?;
                table.insert(&key_bytes, encoded.as_slice())?;
            }
            tx.commit()?;
        }
        let err = PersistentPoolStateStore::open(dir.path())
            .err()
            .ok_or_else(|| anyhow::anyhow!("a corrupt owner_sig must refuse the open"))?;
        anyhow::ensure!(
            matches!(&err, StoreError::Corrupt { pool_id: Some(id), detail }
                if *id == lane.pool_id
                    && detail.contains("owner capability signature is 3 bytes")),
            "expected a Corrupt naming the pool and the owner-signature length, got {err:?}",
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
