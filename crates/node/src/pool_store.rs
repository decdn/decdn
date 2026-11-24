//! Disk-backed `PoolStateStore` for the node runtime.
//!
//! Implements [`PoolStateStore`] against a set of `redb` database files under
//! `<data_dir>`, one file per durable write family:
//!
//! - `lanes.redb` — seller lane frontier (`lane_state_v2`). Each record also
//!   carries its lane's owner-signed capability material (`owner_sig`), so the
//!   redeemer's registration payload lands in the same durable write as the
//!   frontier it backs.
//! - `settle.redb` — the settlement
//!   ledger (`settlement_ledger_v2`): one row per landed `redeemMany`, read by
//!   `admin_v1_earnings`, with the time the ledger started recording
//!   (`settlement_ledger_start_v1`), and the `redeemMany` hashes sent and not
//!   yet decoded (`sent_redeem_v1`).
//! - `buyer.redb` — the buyer pool state and its owner index.
//!
//! Each file has its own `redb` write-transaction slot, so a commit in one
//! family never waits on an unrelated commit in another: the periodic lane
//! flush, a ledger row, and a buyer top-up all proceed on
//! independent writer slots. Families are
//! never written in one transaction — `redb` forbids a transaction spanning two
//! `Database` handles — so a crash between two family commits can leave them at
//! different watermarks, which every family already tolerates (each records and
//! recovers on its own terms).
//!
//! Pool ids repeat across `PaymentPool` deployments, so every key in every
//! table starts with the [`Deployment`] (payment chain id and contract address)
//! the row belongs to. Rows of several deployments share the files without
//! colliding. [`PoolStoreFiles`] owns the one file set. A
//! [`PersistentPoolStateStore`] is the seller view of one deployment over those
//! files ([`PoolStoreFiles::seller_view`]): it hydrates, reads and writes only
//! that deployment's lane rows. A node that
//! accepts several pools holds one view per pool over the same files, and at
//! most one view per deployment exists at a time, so no two working sets
//! buffer the same rows. The settlement ledger and the buyer tables name the
//! deployment per row and per call, so they live on the files.
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
use std::path::Path;
use std::sync::Arc;

use crossbeam_queue::SegQueue;
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;

use alloy::primitives::{Address, B256, U256};
use decdn_common::identity;
/// The `PaymentPool` deployment the store's seller-side state is bound to.
pub use decdn_incentive::Deployment;
use decdn_incentive::buyer_pool_table::BuyerPoolTable;
use decdn_incentive::store::{PoolStateStore, SettlementLedger, SettlementRecord, StoreError};
use decdn_incentive::{
    AdvanceOutcome, BuyerLaneProgress, BuyerLoad, BuyerPoolState, BuyerPoolStore, DepositOutcome,
    LaneChain, LaneKey, LaneState, PoolId,
};
use redb::{Database, Durability, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};

/// File name of the seller lane redb database within `data_dir`.
const LANES_DB_FILE: &str = decdn_common::data_dir::NODE_LANES_DB_FILE;

/// File name of the settlement redb database (settlement ledger and sent
/// `redeemMany` hashes).
const SETTLE_DB_FILE: &str = decdn_common::data_dir::NODE_SETTLE_DB_FILE;

/// File name of the buyer pool redb database (buyer state + owner index).
///
/// Shared with the CLI through [`decdn_common::data_dir`] so both sides name
/// the same file: an operator tool that wants this store has to know it is not
/// the client's
/// [`CLIENT_BUYER_DB_FILE`](decdn_common::data_dir::CLIENT_BUYER_DB_FILE).
const BUYER_DB_FILE: &str = decdn_common::data_dir::NODE_BUYER_DB_FILE;

/// Byte width of a lane key on disk: `deployment ‖ pool_id ‖ signer ‖
/// provider` = `28 + 32 + 20 + 20`.
const LANE_KEY_LEN: usize = Deployment::ENCODED_LEN + 72;

/// Byte width of a settlement-ledger key on disk: `deployment ‖ block_number ‖
/// log_index` = `28 + 8 + 8`.
const SETTLEMENT_KEY_LEN: usize = Deployment::ENCODED_LEN + 16;

// The keys are frozen on-disk identifiers: a width change fails the build here.
const _: () = assert!(LANE_KEY_LEN == 100 && SETTLEMENT_KEY_LEN == 44);

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
/// Key: `deployment ‖ pool_id ‖ signer ‖ provider` ([`lane_key_bytes`],
/// `[u8; 100]`), so one deployment's lanes are one contiguous key range.
/// Value: postcard-encoded [`StoredLaneState`] (variable length).
const LANE_TABLE: TableDefinition<'_, &[u8; LANE_KEY_LEN], &[u8]> =
    TableDefinition::new("lane_state_v2");

/// redb table holding the settlement ledger (#2598): one row per `redeemMany`
/// that paid, decoded from its receipt and read by `admin_v1_earnings`.
/// Lives in `settle.redb`.
///
/// Key: `deployment (28 bytes) ‖ block_number (8 bytes, big-endian) ‖
/// log_index (8 bytes, big-endian)` ([`settlement_key`]), so a table iteration
/// is chain order within each deployment. Value: postcard-encoded
/// [`StoredSettlement`].
const SETTLEMENT_LEDGER_TABLE: TableDefinition<'_, &[u8; SETTLEMENT_KEY_LEN], &[u8]> =
    TableDefinition::new("settlement_ledger_v2");

/// redb table holding the time [`SETTLEMENT_LEDGER_TABLE`] started recording:
/// the open of `settle.redb` that found no stamp (see
/// `PersistentPoolStateStore::bind_ledger_start`). The ledger records every
/// settlement from this time on, whether or not one has landed yet, so
/// `admin_v1_earnings` reports it as the start of its coverage. Lives in
/// `settle.redb` and is dropped with the ledger.
///
/// One row, key [`SETTLEMENT_LEDGER_START_KEY`], value Unix seconds.
const SETTLEMENT_LEDGER_START_TABLE: TableDefinition<'_, &str, u64> =
    TableDefinition::new("settlement_ledger_start_v1");

/// The one key of [`SETTLEMENT_LEDGER_START_TABLE`].
const SETTLEMENT_LEDGER_START_KEY: &str = "started_at";

/// [`SETTLEMENT_LEDGER_TABLE`] opened for reading.
type SettlementLedgerTable = redb::ReadOnlyTable<&'static [u8; SETTLEMENT_KEY_LEN], &'static [u8]>;

/// The `schema_version` [`StoredSettlement`] rows are written with. A row
/// carrying a higher version is [`StoreError::UnsupportedSchema`].
const SETTLEMENT_SCHEMA_VERSION: u32 = 1;

/// Encode a ledger row's `(deployment, block_number, log_index)` into its
/// table key.
fn settlement_key(
    deployment: Deployment,
    block_number: u64,
    log_index: u64,
) -> [u8; SETTLEMENT_KEY_LEN] {
    let mut key = [0u8; SETTLEMENT_KEY_LEN];
    let (head, tail) = key.split_at_mut(Deployment::ENCODED_LEN);
    head.copy_from_slice(&deployment.to_bytes());
    let (block, index) = tail.split_at_mut(8);
    block.copy_from_slice(&block_number.to_be_bytes());
    index.copy_from_slice(&log_index.to_be_bytes());
    key
}

/// Decode a ledger table key back into `(deployment, block_number,
/// log_index)`.
fn settlement_key_parts(
    key: &[u8; SETTLEMENT_KEY_LEN],
) -> Result<(Deployment, u64, u64), StoreError> {
    let (deployment, tail) = key.split_at(Deployment::ENCODED_LEN);
    let (block, index) = tail.split_at(8);
    let corrupt = || StoreError::Corrupt {
        pool_id: None,
        detail: "settlement ledger key has the wrong width".to_owned(),
    };
    Ok((
        Deployment::from_bytes(deployment)?,
        u64::from_be_bytes(block.try_into().map_err(|_| corrupt())?),
        u64::from_be_bytes(index.try_into().map_err(|_| corrupt())?),
    ))
}

/// On-disk value of one [`SETTLEMENT_LEDGER_TABLE`] row: a
/// [`SettlementRecord`] without its key fields. Postcard-encoded, every field
/// required; a row written in another layout fails to decode as
/// [`StoreError::Corrupt`] naming its block and log index. The ledger itself
/// is observability, not a claim, but it shares `settle.redb` with the sent
/// `redeemMany` hashes (see `docs/runbook.md`).
#[derive(Debug, Serialize, Deserialize)]
struct StoredSettlement {
    schema_version: u32,
    tx_hash: [u8; 32],
    landed_at_unix: u64,
    gross_micro_usdc: u64,
    net_micro_usdc: u64,
    bytes_delivered: u64,
    operator: [u8; 20],
}

impl StoredSettlement {
    fn from_record(row: &SettlementRecord) -> Self {
        Self {
            schema_version: SETTLEMENT_SCHEMA_VERSION,
            tx_hash: row.tx_hash.into(),
            landed_at_unix: row.landed_at_unix,
            gross_micro_usdc: row.gross_micro_usdc,
            net_micro_usdc: row.net_micro_usdc,
            bytes_delivered: row.bytes_delivered,
            operator: row.operator.into(),
        }
    }

    fn into_record(
        self,
        deployment: Deployment,
        block_number: u64,
        log_index: u64,
    ) -> Result<SettlementRecord, StoreError> {
        if self.schema_version > SETTLEMENT_SCHEMA_VERSION {
            return Err(StoreError::UnsupportedSchema {
                found: self.schema_version,
                supported: SETTLEMENT_SCHEMA_VERSION,
            });
        }
        Ok(SettlementRecord {
            deployment,
            block_number,
            log_index,
            tx_hash: B256::from(self.tx_hash),
            landed_at_unix: self.landed_at_unix,
            gross_micro_usdc: self.gross_micro_usdc,
            net_micro_usdc: self.net_micro_usdc,
            bytes_delivered: self.bytes_delivered,
            operator: Address::from(self.operator),
        })
    }
}

/// Width of a [`SENT_REDEEM_TABLE`] key: `deployment ‖ tx_hash`.
const SENT_REDEEM_KEY_LEN: usize = Deployment::ENCODED_LEN + 32;

/// redb table holding every `redeemMany` hash the redeemer sent and has not
/// yet decoded a receipt for (`SettlementLedger::record_sent_redeem`), so a
/// receipt a crash or a timed-out wait missed is fetched later. Lives in
/// `settle.redb`. Key: `deployment ‖ tx_hash` ([`sent_redeem_key`]); no value.
const SENT_REDEEM_TABLE: TableDefinition<'_, &[u8; SENT_REDEEM_KEY_LEN], ()> =
    TableDefinition::new("sent_redeem_v1");

/// The [`SENT_REDEEM_TABLE`] key of `tx_hash` on `deployment`.
fn sent_redeem_key(deployment: Deployment, tx_hash: B256) -> [u8; SENT_REDEEM_KEY_LEN] {
    let mut key = [0u8; SENT_REDEEM_KEY_LEN];
    let (head, tail) = key.split_at_mut(Deployment::ENCODED_LEN);
    head.copy_from_slice(&deployment.to_bytes());
    tail.copy_from_slice(tx_hash.as_slice());
    key
}

/// Encode a lane into its `[u8; 100]` table key: `deployment ‖ pool_id ‖
/// signer ‖ provider`.
fn lane_key_bytes(deployment: Deployment, key: &LaneKey) -> [u8; LANE_KEY_LEN] {
    let mut out = [0u8; LANE_KEY_LEN];
    for (slot, byte) in out.iter_mut().zip(
        deployment
            .to_bytes()
            .into_iter()
            .chain(key.pool_id.0)
            .chain(key.signer.into_array())
            .chain(key.provider.into_array()),
    ) {
        *slot = byte;
    }
    out
}

/// Decode a `[u8; 100]` table key back into its parts `(deployment, pool_id,
/// signer, provider)`. Fixed-width arrays, so the indexing is total.
fn lane_key_parts(bytes: &[u8; LANE_KEY_LEN]) -> (Deployment, B256, Address, Address) {
    let mut chain = [0u8; 8];
    chain.copy_from_slice(&bytes[..8]);
    let mut contract = [0u8; 20];
    contract.copy_from_slice(&bytes[8..28]);
    let mut pool = [0u8; 32];
    pool.copy_from_slice(&bytes[28..60]);
    let mut signer = [0u8; 20];
    signer.copy_from_slice(&bytes[60..80]);
    let mut provider = [0u8; 20];
    provider.copy_from_slice(&bytes[80..]);
    (
        Deployment {
            chain_id: u64::from_be_bytes(chain),
            payment_pool: Address::from(contract),
        },
        B256::from(pool),
        Address::from(signer),
        Address::from(provider),
    )
}

/// The first and last possible lane keys of `deployment`: the bounds of a
/// range scan over that deployment's lanes.
fn lane_key_range(deployment: Deployment) -> ([u8; LANE_KEY_LEN], [u8; LANE_KEY_LEN]) {
    let prefix = deployment.to_bytes();
    let mut lo = [0x00u8; LANE_KEY_LEN];
    let mut hi = [0xffu8; LANE_KEY_LEN];
    lo[..Deployment::ENCODED_LEN].copy_from_slice(&prefix);
    hi[..Deployment::ENCODED_LEN].copy_from_slice(&prefix);
    (lo, hi)
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
/// silently defaulted. That break is deliberate and unversioned: before mainnet
/// no store carries a compatibility promise, so [`SUPPORTED_SCHEMA_VERSION`]
/// does not bump. A node that holds a record in the older layout fails `open`
/// with [`StoreError::Corrupt`]; recovery moves `lanes.redb` aside, which
/// forfeits that lane's unredeemed claim.
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
    /// defaulted. That break is deliberate and unversioned; see [`StoredLaneState`].
    owner_sig: Vec<u8>,
    /// The lane's paid cumulative — the on-chain `newPaidCumulative` of the most
    /// recent `PoolRedeemed` this node decoded from its own receipts, or the
    /// `getWatermarks` value its pre-redeem read found — as a fixed-width
    /// big-endian `[u8; 32]`, like `last_amount`/`cap`. The seller redeemer
    /// subtracts it from the lane's owed value to get what it still has to cash;
    /// persisting it here (in the same fsynced row as the frontier) is what stops
    /// a restarted node from forgetting its redemptions and re-submitting
    /// already-redeemed lanes for silent on-chain no-ops (#2052). A required field like `registered_until`/`owner_sig`: adding it is
    /// a breaking on-disk change (a record written before it fails to decode), not
    /// silently defaulted — deliberate and unversioned; see [`StoredLaneState`].
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

/// The node's one set of `redb` store files, shared by every seller view
/// ([`PersistentPoolStateStore`]), the settlement ledger and the buyer tables.
///
/// Construct via [`PoolStoreFiles::open`]. One `redb::Database` per write
/// family is owned for the lifetime of this value; drop closes every handle.
/// `redb` forbids a second `Database` handle to one file, so a process opens
/// the files once and derives every view from them.
#[derive(Debug)]
pub struct PoolStoreFiles {
    /// Seller lane frontier (`lanes.redb`). The owner-signed capability material
    /// each lane's redeemer needs rides the lane record itself (`owner_sig`), so
    /// the periodic flush commits it in the same transaction as the frontier.
    lanes_db: Database,
    /// The settlement ledger and the sent `redeemMany` hashes (`settle.redb`).
    settle_db: Database,
    /// Buyer pool state + owner index (`buyer.redb`).
    buyer_db: Database,
    /// When the settlement ledger started recording; returned by
    /// [`Self::ledger_started_at_unix`].
    ledger_started_at_unix: u64,
    /// The deployments a live seller view serves. A second view of one
    /// deployment would buffer the same rows in a second working set, and the
    /// two flushes would overwrite each other.
    bound: std::sync::Mutex<HashSet<Deployment>>,
}

/// `redb`-backed persistent implementation of [`PoolStateStore`]: the seller
/// view of one `PaymentPool` deployment over the shared [`PoolStoreFiles`].
///
/// Construct via [`PoolStoreFiles::seller_view`]. The lane table is buffered in memory: `record`/`forget` mutate
/// the working set only, and a caller must call [`PoolStateStore::flush`] to
/// commit it to disk. The store is thread-safe — each redb file serialises its
/// own writes via single-writer transactions, families commit on independent
/// writer slots, and reads are MVCC. Dropping the view releases its
/// deployment, so a later view of it hydrates afresh.
///
/// The working set is a [`DashMap`] rather than one mutex-guarded map, so a
/// `record` on the paid-delivery path locks only its own lane's shard — no lane
/// serialises on another, and settlement's `load_all`/`get` do not contend
/// with `record` (issue #1792 item 1). Each entry's shard lock is the per-lane
/// critical section that keeps a `record`/`forget` race decidable.
#[derive(Debug)]
pub struct PersistentPoolStateStore {
    /// The shared store files.
    files: Arc<PoolStoreFiles>,
    /// The `PaymentPool` deployment whose lane rows this store value serves. Every key it writes starts with it.
    deployment: Deployment,
    /// The per-lane working set, hydrated from disk when the view opens.
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

impl Drop for PersistentPoolStateStore {
    fn drop(&mut self) {
        self.files.release(self.deployment);
    }
}

impl PersistentPoolStateStore {
    /// Open the store files under `data_dir` and return the seller view of
    /// `deployment` over them: [`PoolStoreFiles::open`] then
    /// [`PoolStoreFiles::seller_view`], for a test that serves one deployment.
    ///
    /// # Errors
    ///
    /// Every error of [`PoolStoreFiles::open`] and
    /// [`PoolStoreFiles::seller_view`].
    #[cfg(any(test, feature = "test-support"))]
    pub fn open(data_dir: &Path, deployment: Deployment) -> Result<Self, StoreError> {
        PoolStoreFiles::open(data_dir)?.seller_view(deployment)
    }

    /// `open` with an injectable chmod function; see
    /// [`PoolStoreFiles::open_with`].
    #[cfg(test)]
    pub(crate) fn open_with<F>(
        data_dir: &Path,
        deployment: Deployment,
        chmod_fn: F,
    ) -> Result<Self, StoreError>
    where
        F: Fn(&Path) -> Result<(), StoreError>,
    {
        PoolStoreFiles::open_with(data_dir, chmod_fn)?.seller_view(deployment)
    }

    /// The shared store files this view reads and writes.
    #[must_use]
    pub const fn files(&self) -> &Arc<PoolStoreFiles> {
        &self.files
    }

    /// When the settlement ledger started recording, Unix seconds; see
    /// [`PoolStoreFiles::ledger_started_at_unix`].
    #[must_use]
    pub fn ledger_started_at_unix(&self) -> u64 {
        self.files.ledger_started_at_unix()
    }
}

impl PoolStoreFiles {
    /// Open (or create) the store files under `data_dir`.
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
    pub fn open(data_dir: &Path) -> Result<Arc<Self>, StoreError> {
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
    pub(crate) fn open_with<F>(data_dir: &Path, chmod_fn: F) -> Result<Arc<Self>, StoreError>
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
        let lanes_db = Self::open_hardened_db(&data_dir.join(LANES_DB_FILE), &chmod_fn)?;
        let settle_db = Self::open_hardened_db(&data_dir.join(SETTLE_DB_FILE), &chmod_fn)?;
        let buyer_db = Self::open_hardened_db(&data_dir.join(BUYER_DB_FILE), &chmod_fn)?;

        let ledger_started_at_unix =
            Self::bind_ledger_start(&settle_db, crate::payment_settlement::unix_now())?;
        Ok(Arc::new(Self {
            lanes_db,
            settle_db,
            buyer_db,
            ledger_started_at_unix,
            bound: std::sync::Mutex::new(HashSet::new()),
        }))
    }

    /// The seller view of `deployment` over these files: hydrates that
    /// deployment's lanes into a fresh working set. Every other deployment's
    /// rows stay on disk untouched.
    ///
    /// # Errors
    ///
    /// - [`StoreError::Backend`] when a live view of `deployment` already
    ///   exists: two working sets over the same rows would overwrite each
    ///   other's flushes.
    /// - [`StoreError::Corrupt`] or [`StoreError::Backend`] when a lane row of
    ///   `deployment` does not decode or the table cannot be read. Running past
    ///   it would reopen the issue #527 voucher-replay window.
    pub fn seller_view(
        self: &Arc<Self>,
        deployment: Deployment,
    ) -> Result<PersistentPoolStateStore, StoreError> {
        if !self.lock_bound().insert(deployment) {
            return Err(StoreError::Backend(format!(
                "a seller view of {} is already open",
                deployment.pool_account()
            )));
        }
        let lanes = match Self::hydrate_lanes(&self.lanes_db, deployment) {
            Ok(lanes) => lanes,
            Err(err) => {
                self.release(deployment);
                return Err(err);
            }
        };
        Ok(PersistentPoolStateStore {
            files: Arc::clone(self),
            deployment,
            lanes,
            dirty: SegQueue::new(),
        })
    }

    /// When the settlement ledger started recording, Unix seconds: the open of
    /// `settle.redb` that found no stamp (`Self::bind_ledger_start`). The
    /// ledger records every settlement landed from this time on.
    #[must_use]
    pub const fn ledger_started_at_unix(&self) -> u64 {
        self.ledger_started_at_unix
    }

    fn lock_bound(&self) -> std::sync::MutexGuard<'_, HashSet<Deployment>> {
        self.bound
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Release `deployment` when its view drops.
    fn release(&self, deployment: Deployment) {
        self.lock_bound().remove(&deployment);
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
    /// (a dropped sent-`redeemMany` hash forgets an in-flight redemption).
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

    /// The time the settlement ledger started recording.
    ///
    /// - The stamp exists: return it. The read is a READ transaction, so a boot
    ///   takes no write.
    /// - No stamp: write `now_unix` and return it. The ledger records every
    ///   settlement from this boot on. A store has no stamp when `settle.redb`
    ///   is new (including one moved aside), and when its ledger rows were
    ///   written before this table existed: those rows predate the stamp, and
    ///   `admin_v1_earnings` moves its coverage back to the earliest of them.
    ///
    /// # Errors
    /// [`StoreError::Backend`] when a transaction fails.
    fn bind_ledger_start(settle_db: &Database, now_unix: u64) -> Result<u64, StoreError> {
        let read_txn = settle_db.begin_read().map_err(|err| {
            StoreError::Backend(format!("begin_read (settle.redb ledger start): {err}"))
        })?;
        let stamp = match read_txn.open_table(SETTLEMENT_LEDGER_START_TABLE) {
            Ok(table) => table
                .get(SETTLEMENT_LEDGER_START_KEY)
                .map_err(|err| {
                    StoreError::Backend(format!("get (settle.redb ledger start): {err}"))
                })?
                .map(|guard| guard.value()),
            Err(redb::TableError::TableDoesNotExist(_)) => None,
            Err(err) => {
                return Err(StoreError::Backend(format!(
                    "open_table (settle.redb ledger start): {err}"
                )));
            }
        };
        drop(read_txn);
        if let Some(stamp) = stamp {
            return Ok(stamp);
        }
        let mut txn = settle_db.begin_write().map_err(|err| {
            StoreError::Backend(format!("begin_write (settle.redb ledger start): {err}"))
        })?;
        txn.set_durability(Durability::Immediate).map_err(|err| {
            StoreError::Backend(format!("set_durability (settle.redb ledger start): {err}"))
        })?;
        {
            let mut table = txn
                .open_table(SETTLEMENT_LEDGER_START_TABLE)
                .map_err(|err| {
                    StoreError::Backend(format!("open_table (settle.redb ledger start): {err}"))
                })?;
            table
                .insert(SETTLEMENT_LEDGER_START_KEY, now_unix)
                .map_err(|err| {
                    StoreError::Backend(format!("insert (settle.redb ledger start): {err}"))
                })?;
        }
        txn.commit().map_err(|err| {
            StoreError::Backend(format!("commit (settle.redb ledger start): {err}"))
        })?;
        Ok(now_unix)
    }

    /// Read `deployment`'s lanes into the in-memory working set at open. A
    /// corrupt or forward-schema record of that deployment aborts startup
    /// (running past it would reopen the issue #527 voucher-replay window).
    /// Every hydrated lane enters as [`LaneSlot::Live`]. Another deployment's
    /// rows lie outside the scanned key range and are never read.
    fn hydrate_lanes(
        db: &Database,
        deployment: Deployment,
    ) -> Result<DashMap<LaneKey, LaneSlot>, StoreError> {
        let read_txn = db
            .begin_read()
            .map_err(|err| StoreError::Backend(format!("begin_read: {err}")))?;
        let table = match read_txn.open_table(LANE_TABLE) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(DashMap::new()),
            Err(err) => return Err(StoreError::Backend(format!("open_table: {err}"))),
        };
        let out = DashMap::new();
        let (lo, hi) = lane_key_range(deployment);
        let iter = table
            .range(&lo..=&hi)
            .map_err(|err| StoreError::Backend(format!("table range: {err}")))?;
        for entry in iter {
            let (key_guard, value_guard) =
                entry.map_err(|err| StoreError::Backend(format!("iter entry: {err}")))?;
            let key_bytes: [u8; LANE_KEY_LEN] = *key_guard.value();
            let state = decode_record(&key_bytes, value_guard.value())?;
            out.insert(state.key(), LaneSlot::Live(state));
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
    let (_, pool_id, signer, provider) = lane_key_parts(key_bytes);
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
                    // the redeemed cumulative (the redeemer writes that via
                    // `set_paid_cumulative`). Keep the persisted value
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
        snapshot.sort_by_table_key(self.deployment);

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
    fn sort_by_table_key(&mut self, deployment: Deployment) {
        self.writes
            .sort_by_cached_key(|(lane, _)| lane_key_bytes(deployment, lane));
        self.tombstones
            .sort_by_cached_key(|lane| lane_key_bytes(deployment, lane));
    }
}

impl PersistentPoolStateStore {
    /// Apply a [`FlushSnapshot`] in ONE fsynced redb transaction. Holds no
    /// shard lock — every value was already encoded during the snapshot. All
    /// batches arrive in ascending table-key order, so the insert loops append
    /// rightward through the B-trees.
    fn commit_snapshot(&self, snapshot: &FlushSnapshot) -> Result<(), StoreError> {
        let mut write_txn = self
            .files
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
                let key_bytes = lane_key_bytes(self.deployment, key);
                table
                    .insert(&key_bytes, encoded.as_slice())
                    .map_err(|err| StoreError::Backend(format!("insert: {err}")))?;
            }
            for key in &snapshot.tombstones {
                let key_bytes = lane_key_bytes(self.deployment, key);
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
// file the table lives in (`buyer.redb`, its own per-family file, so a buyer
// commit never waits on the seller lane or settlement writer slot) and which `TableDefinition` names it.
// ---------------------------------------------------------------------------

/// Test/e2e-only seam on the store files. Kept here rather than on the handle
/// because the buyer reconciliation + mixed-reclaim e2e (#763) holds the files.
#[cfg(any(test, feature = "anvil-e2e"))]
impl PoolStoreFiles {
    /// Test/e2e-only: write raw value bytes under a buyer pool key, bypassing
    /// the postcard encoder, to simulate a row left undecodable by a binary
    /// downgrade.
    ///
    /// # Errors
    ///
    /// [`StoreError::Backend`] if the write or durable commit fails.
    pub fn insert_raw_buyer_record(
        &self,
        deployment: Deployment,
        pool_id: PoolId,
        bytes: &[u8],
    ) -> Result<(), StoreError> {
        BuyerPoolTable::new(&self.buyer_db).insert_raw(deployment, pool_id, bytes)
    }
}

/// [`BuyerPoolStore`] adapter over the shared [`PoolStoreFiles`].
///
/// Holds an `Arc` to the same files the seller views use. The seller's
/// `lane_state_v2` table and the buyer pool table sit in separate redb files
/// (`lanes.redb` and `buyer.redb`), so neither family's commit waits on the
/// other's writer slot. Hand this to the buyer service as
/// `Arc<dyn BuyerPoolStore>`.
#[derive(Debug, Clone)]
pub struct BuyerPoolStoreHandle {
    inner: Arc<PoolStoreFiles>,
}

impl BuyerPoolStoreHandle {
    /// Wrap the shared store files as a buyer-pool store.
    #[must_use]
    pub const fn new(inner: Arc<PoolStoreFiles>) -> Self {
        Self { inner }
    }

    /// View this store's `buyer.redb` as the buyer pool table.
    ///
    /// Not `const` — it derefs the `Arc`, which const fns cannot do.
    fn table(&self) -> BuyerPoolTable<'_> {
        BuyerPoolTable::new(&self.inner.buyer_db)
    }
}

/// Every method delegates to [`decdn_incentive::buyer_pool_table`]; this newtype
/// contributes the `buyer.redb` wiring, not the logic.
impl BuyerPoolStore for BuyerPoolStoreHandle {
    fn load_all(&self) -> Result<BuyerLoad, StoreError> {
        self.table().load_all()
    }

    fn record(&self, state: &BuyerPoolState) -> Result<(), StoreError> {
        self.table().record(state)
    }

    fn forget(&self, deployment: Deployment, owner: Address) -> Result<(), StoreError> {
        self.table().forget(deployment, owner)
    }

    fn forget_if_pool(
        &self,
        deployment: Deployment,
        owner: Address,
        pool_id: PoolId,
    ) -> Result<bool, StoreError> {
        self.table().forget_if_pool(deployment, owner, pool_id)
    }

    fn get_by_pool_id(
        &self,
        deployment: Deployment,
        pool_id: PoolId,
    ) -> Result<Option<BuyerPoolState>, StoreError> {
        self.table().get_by_pool_id(deployment, pool_id)
    }

    fn get_by_owner(
        &self,
        deployment: Deployment,
        owner: Address,
    ) -> Result<Option<BuyerPoolState>, StoreError> {
        self.table().get_by_owner(deployment, owner)
    }

    fn advance_progress(
        &self,
        deployment: Deployment,
        owner: Address,
        pool_id: PoolId,
        lane: LaneKey,
        bytes: U256,
        amount: U256,
    ) -> Result<AdvanceOutcome, StoreError> {
        self.table()
            .advance_progress(deployment, owner, pool_id, lane, bytes, amount)
    }

    fn rebase_progress(
        &self,
        deployment: Deployment,
        owner: Address,
        pool_id: PoolId,
        lane: LaneKey,
        anchor: BuyerLaneProgress,
        totals: BuyerLaneProgress,
    ) -> Result<AdvanceOutcome, StoreError> {
        self.table()
            .rebase_progress(deployment, owner, pool_id, lane, anchor, totals)
    }

    fn seed_progress(
        &self,
        deployment: Deployment,
        owner: Address,
        pool_id: PoolId,
        lane: LaneKey,
        bytes: U256,
        amount: U256,
    ) -> Result<AdvanceOutcome, StoreError> {
        self.table()
            .seed_progress(deployment, owner, pool_id, lane, bytes, amount)
    }

    fn add_deposit(
        &self,
        deployment: Deployment,
        owner: Address,
        pool_id: PoolId,
        additional: U256,
    ) -> Result<DepositOutcome, StoreError> {
        self.table()
            .add_deposit(deployment, owner, pool_id, additional)
    }

    fn delegate_lanes(
        &self,
        deployment: Deployment,
        pool_id: PoolId,
        signer: Address,
    ) -> Result<Vec<(Address, BuyerLaneProgress)>, StoreError> {
        self.table().delegate_lanes(deployment, pool_id, signer)
    }

    fn advance_delegate_progress(
        &self,
        deployment: Deployment,
        lane: LaneKey,
        totals: BuyerLaneProgress,
    ) -> Result<AdvanceOutcome, StoreError> {
        self.table()
            .advance_delegate_progress(deployment, lane, totals)
    }

    fn rebase_delegate_progress(
        &self,
        deployment: Deployment,
        lane: LaneKey,
        anchor: BuyerLaneProgress,
        totals: BuyerLaneProgress,
    ) -> Result<AdvanceOutcome, StoreError> {
        self.table()
            .rebase_delegate_progress(deployment, lane, anchor, totals)
    }

    fn lane_salt(&self) -> Result<decdn_incentive::lane_signer::LaneSalt, StoreError> {
        self.table().lane_salt()
    }
}

/// The ledger names the deployment per row, so a seller view reads and writes
/// the whole ledger of its files.
impl SettlementLedger for PersistentPoolStateStore {
    fn record_sent_redeem(&self, deployment: Deployment, tx_hash: B256) -> Result<(), StoreError> {
        self.files.record_sent_redeem(deployment, tx_hash)
    }

    fn sent_redeems(&self, deployment: Deployment) -> Result<Vec<B256>, StoreError> {
        self.files.sent_redeems(deployment)
    }

    fn forget_sent_redeem(&self, deployment: Deployment, tx_hash: B256) -> Result<(), StoreError> {
        self.files.forget_sent_redeem(deployment, tx_hash)
    }

    fn record_settlement(&self, row: &SettlementRecord) -> Result<(), StoreError> {
        self.files.record_settlement(row)
    }

    fn load_settlements(&self) -> Result<Vec<SettlementRecord>, StoreError> {
        self.files.load_settlements()
    }

    fn has_settlement_tx(
        &self,
        deployment: Deployment,
        block_number: u64,
        tx_hash: B256,
    ) -> Result<bool, StoreError> {
        self.files
            .has_settlement_tx(deployment, block_number, tx_hash)
    }
}

impl SettlementLedger for PoolStoreFiles {
    fn record_sent_redeem(&self, deployment: Deployment, tx_hash: B256) -> Result<(), StoreError> {
        let key = sent_redeem_key(deployment, tx_hash);
        self.write_sent_redeem(|table| {
            table
                .insert(&key, ())
                .map(|_| ())
                .map_err(|err| StoreError::Backend(format!("insert: {err}")))
        })
    }

    fn sent_redeems(&self, deployment: Deployment) -> Result<Vec<B256>, StoreError> {
        let read_txn = self
            .settle_db
            .begin_read()
            .map_err(|err| StoreError::Backend(format!("begin_read: {err}")))?;
        let table = match read_txn.open_table(SENT_REDEEM_TABLE) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(err) => return Err(StoreError::Backend(format!("open_table: {err}"))),
        };
        let start = sent_redeem_key(deployment, B256::ZERO);
        let end = sent_redeem_key(deployment, B256::repeat_byte(0xff));
        let iter = table
            .range(&start..=&end)
            .map_err(|err| StoreError::Backend(format!("range: {err}")))?;
        let mut out = Vec::new();
        for entry in iter {
            let (key, _) =
                entry.map_err(|err| StoreError::Backend(format!("iter entry: {err}")))?;
            let (_, tx) = key.value().split_at(Deployment::ENCODED_LEN);
            out.push(B256::from_slice(tx));
        }
        Ok(out)
    }

    fn forget_sent_redeem(&self, deployment: Deployment, tx_hash: B256) -> Result<(), StoreError> {
        let key = sent_redeem_key(deployment, tx_hash);
        self.write_sent_redeem(|table| {
            table
                .remove(&key)
                .map(|_| ())
                .map_err(|err| StoreError::Backend(format!("remove: {err}")))
        })
    }

    fn record_settlement(&self, row: &SettlementRecord) -> Result<(), StoreError> {
        let key = settlement_key(row.deployment, row.block_number, row.log_index);
        let value = postcard::to_allocvec(&StoredSettlement::from_record(row))
            .map_err(|err| StoreError::Codec(format!("settlement row encode: {err}")))?;
        let mut write_txn = self
            .settle_db
            .begin_write()
            .map_err(|err| StoreError::Backend(format!("begin_write: {err}")))?;
        // Force fsync-on-commit, same durability discipline as the other tables:
        // the redemption has already landed on-chain, and a row lost to a crash
        // is a settlement the operator never sees in `decdn node earnings`.
        write_txn
            .set_durability(Durability::Immediate)
            .map_err(|err| StoreError::Backend(format!("set_durability: {err}")))?;
        {
            let mut table = write_txn
                .open_table(SETTLEMENT_LEDGER_TABLE)
                .map_err(|err| StoreError::Backend(format!("open_table: {err}")))?;
            table
                .insert(&key, value.as_slice())
                .map_err(|err| StoreError::Backend(format!("insert: {err}")))?;
        }
        write_txn
            .commit()
            .map_err(|err| StoreError::Backend(format!("commit (fsync): {err}")))?;
        Ok(())
    }

    fn load_settlements(&self) -> Result<Vec<SettlementRecord>, StoreError> {
        // A never-written ledger table is the empty ledger: no redemption has
        // landed since the table exists.
        let Some(table) = self.open_settlement_ledger()? else {
            return Ok(Vec::new());
        };
        let iter = table
            .iter()
            .map_err(|err| StoreError::Backend(format!("table iter: {err}")))?;
        let mut out = Vec::new();
        for entry in iter {
            let (key, value) =
                entry.map_err(|err| StoreError::Backend(format!("iter entry: {err}")))?;
            let ((deployment, block_number, log_index), stored) =
                decode_settlement_entry(key.value(), value.value())?;
            out.push(stored.into_record(deployment, block_number, log_index)?);
        }
        Ok(out)
    }

    fn has_settlement_tx(
        &self,
        deployment: Deployment,
        block_number: u64,
        tx_hash: B256,
    ) -> Result<bool, StoreError> {
        let Some(table) = self.open_settlement_ledger()? else {
            return Ok(false);
        };
        // Only this block's rows: the key is `deployment ‖ block ‖ log_index`,
        // so the range is a few entries however large the ledger, and a row
        // elsewhere that does not decode cannot stop the guard.
        let lo = settlement_key(deployment, block_number, 0);
        let hi = settlement_key(deployment, block_number, u64::MAX);
        let iter = table
            .range(&lo..=&hi)
            .map_err(|err| StoreError::Backend(format!("table range: {err}")))?;
        for entry in iter {
            let (key, value) =
                entry.map_err(|err| StoreError::Backend(format!("iter entry: {err}")))?;
            let (_, stored) = decode_settlement_entry(key.value(), value.value())?;
            if B256::from(stored.tx_hash) == tx_hash {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

impl PoolStoreFiles {
    /// Run one fsynced write against [`SENT_REDEEM_TABLE`].
    fn write_sent_redeem(
        &self,
        write: impl FnOnce(
            &mut redb::Table<'_, &'static [u8; SENT_REDEEM_KEY_LEN], ()>,
        ) -> Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        let mut write_txn = self
            .settle_db
            .begin_write()
            .map_err(|err| StoreError::Backend(format!("begin_write: {err}")))?;
        write_txn
            .set_durability(Durability::Immediate)
            .map_err(|err| StoreError::Backend(format!("set_durability: {err}")))?;
        {
            let mut table = write_txn
                .open_table(SENT_REDEEM_TABLE)
                .map_err(|err| StoreError::Backend(format!("open_table: {err}")))?;
            write(&mut table)?;
        }
        write_txn
            .commit()
            .map_err(|err| StoreError::Backend(format!("commit (fsync): {err}")))
    }

    /// Open [`SETTLEMENT_LEDGER_TABLE`] for reading; `None` when no row was
    /// ever written.
    fn open_settlement_ledger(&self) -> Result<Option<SettlementLedgerTable>, StoreError> {
        let read_txn = self
            .settle_db
            .begin_read()
            .map_err(|err| StoreError::Backend(format!("begin_read: {err}")))?;
        match read_txn.open_table(SETTLEMENT_LEDGER_TABLE) {
            Ok(table) => Ok(Some(table)),
            Err(redb::TableError::TableDoesNotExist(_)) => Ok(None),
            Err(err) => Err(StoreError::Backend(format!("open_table: {err}"))),
        }
    }
}

/// Decode one [`SETTLEMENT_LEDGER_TABLE`] entry into its key parts
/// `(deployment, block_number, log_index)` and its value. A value that does
/// not decode is [`StoreError::Corrupt`] naming its block and log index.
fn decode_settlement_entry(
    key: &[u8; SETTLEMENT_KEY_LEN],
    value: &[u8],
) -> Result<((Deployment, u64, u64), StoredSettlement), StoreError> {
    let parts @ (_, block_number, log_index) = settlement_key_parts(key)?;
    let stored = postcard::from_bytes(value).map_err(|err| StoreError::Corrupt {
        pool_id: None,
        detail: format!(
            "settlement ledger row at block {block_number} log {log_index} does not decode: {err}"
        ),
    })?;
    Ok((parts, stored))
}

#[cfg(test)]
mod tests;
