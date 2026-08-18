//! Disk-backed `PoolStateStore` for the node runtime.
//!
//! Implements [`PoolStateStore`] against a single `redb` database file at
//! `<data_dir>/lanes.redb`. The lane table is buffered in memory: `open()`
//! hydrates the working set from disk, `record`/`forget` mutate that working
//! set only, and an explicit `flush()` call writes every dirty lane and
//! applies every tombstone in one fsynced commit (redb's default
//! [`redb::Durability::Immediate`], set explicitly here so a future redb
//! default change doesn't silently weaken the durability guarantee). A crash
//! between two flushes loses the unflushed lane advances; the caller decides
//! the flush cadence.
//!
//! See [`decdn_incentive::store`] for the trait contract and
//! [ADR 003 §Off-chain voucher state persistence] for the protocol rule
//! this implements: a node MUST persist `(last_amount, last_bytes_delivered)`
//! per lane before continuing delivery, otherwise a restart re-opens the lane
//! at amount zero and a client can replay a previously-accepted voucher for a
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
//! wiring — it lives in *this* `lanes.redb`, alongside the seller,
//! pending-settle, and watcher-checkpoint tables, because redb forbids two
//! `Database` handles on one file.
//!
//! [ADR 003 §Off-chain voucher state persistence]: ../../../adr/003-payments.md

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use alloy::primitives::{Address, B256, U256};
use decdn_common::identity;
use decdn_incentive::buyer_pool_table::BuyerPoolTable;
use decdn_incentive::store::{
    CheckpointKey, KeyedCheckpointStore, PendingSettle, PendingSettleStore, PoolFloorLossStore,
    PoolStateStore, StoreError,
};
use decdn_incentive::{
    AdvanceOutcome, BuyerLoad, BuyerPoolState, BuyerPoolStore, DepositOutcome, LaneKey, LaneState,
    PoolId,
};
use redb::{Database, Durability, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};

/// File name of the redb database within `data_dir`.
const LANES_DB_FILE: &str = "lanes.redb";

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
const LANE_TABLE: TableDefinition<&[u8; LANE_KEY_LEN], &[u8]> =
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
const PENDING_SETTLE_TABLE: TableDefinition<&[u8; 32], u64> =
    TableDefinition::new("pending_settle_v1");

/// redb table holding pools this node closed as the **buyer** (#988) — the
/// unilateral close of an unreachable provider's idle pool — that await the
/// grace window. Same shape as [`PENDING_SETTLE_TABLE`] (key: `PoolId` bytes,
/// value: deadline) but a SEPARATE table so the buyer settle sweep and the
/// seller settle sweep never settle each other's closes. Lives in the same
/// database file as [`LANE_TABLE`].
const BUYER_PENDING_SETTLE_TABLE: TableDefinition<&[u8; 32], u64> =
    TableDefinition::new("buyer_pending_settle_v1");

/// redb table holding each on-chain watcher's scan checkpoint (#751, keyed in
/// #1092/#1108): the last block scanned per [`CheckpointKey`], so a bring-up
/// backfill resumes across restarts and covers events landing while the node was
/// down. Lives in the same database file as [`LANE_TABLE`]. One `&str` key per
/// watcher (the [`CheckpointKey::as_str`] literals); a fixed-width native `u64`
/// value needs no postcard envelope.
const WATCHER_CHECKPOINT_TABLE: TableDefinition<&str, u64> =
    TableDefinition::new("watcher_checkpoint_v1");

/// redb table holding each pool's cumulative unrecoverable floor-credit loss
/// (`µUSDC`, ADR 003 §Pool solvency — the `dead_charge`): un-vouchered
/// serve-time exposure that appears in no on-chain quantity and so must be
/// persisted here to survive a restart. Lives in the same database file as
/// [`LANE_TABLE`].
///
/// Key: raw `PoolId` bytes (`[u8; 32]`). Value: the accumulated `µUSDC` total
/// as a native redb `u128` — no postcard envelope, matching the
/// [`PENDING_SETTLE_TABLE`] convention of using redb's built-in scalar
/// encoding for a single fixed-width number.
const POOL_FLOOR_LOSS_TABLE: TableDefinition<&[u8; 32], u128> =
    TableDefinition::new("pool_floor_loss_v1");

/// Byte width of a capability key on disk: `pool_id ‖ signer` = `32 + 20`. A
/// capability authorizes one signer under one pool for every provider, so it is
/// keyed by the `(pool_id, signer)` pair — not the full lane triple.
const CAPABILITY_KEY_LEN: usize = 52;

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
const CAPABILITY_TABLE: TableDefinition<&[u8; CAPABILITY_KEY_LEN], &[u8]> =
    TableDefinition::new("capability_v1");

/// Encode a `(pool_id, signer)` pair into its `[u8; 52]` capability-table key.
fn capability_key_bytes(pool_id: B256, signer: Address) -> [u8; CAPABILITY_KEY_LEN] {
    let mut out = [0u8; CAPABILITY_KEY_LEN];
    out[..32].copy_from_slice(pool_id.as_slice());
    out[32..].copy_from_slice(signer.as_slice());
    out
}

/// On-disk owner-signed capability record. The `(pool_id, signer)` identity is
/// the table key, so the value carries only the cap/expiry and the owner
/// signature. `spending_cap` is a fixed-width big-endian array (identical to the
/// on-chain representation); `owner_sig` is the raw EIP-712 signature (65-byte
/// ECDSA, or an ERC-1271 payload) verbatim.
#[derive(Debug, Serialize, Deserialize)]
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
#[derive(Debug, Serialize, Deserialize)]
struct StoredLaneState {
    schema_version: u32,
    last_amount: [u8; 32],
    last_bytes_delivered: [u8; 32],
    signature: Vec<u8>,
    cap: [u8; 32],
    expiry: u64,
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
        }
    }
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
        let last_signature: Option<[u8; 65]> = if self.signature.is_empty() {
            None
        } else {
            let len = self.signature.len();
            Some(
                <[u8; 65]>::try_from(self.signature).map_err(|_| StoreError::Corrupt {
                    pool_id: Some(pool_id),
                    detail: format!("stored voucher signature is {len} bytes, expected 0 or 65"),
                })?,
            )
        };
        Ok(LaneState::hydrate(
            pool_id,
            signer,
            provider,
            U256::from_be_bytes(self.cap),
            self.expiry,
            U256::from_be_bytes(self.last_amount),
            U256::from_be_bytes(self.last_bytes_delivered),
            last_signature,
        ))
    }
}

/// In-memory working set for the lane (frontier) table. The map is the
/// authoritative copy after `open()` hydrates it from disk; `record`/`forget`
/// mutate it and mark `dirty`/`tombstones`, and `flush` drains those into one
/// fsynced redb transaction. `dirty` and `tombstones` are disjoint: `record`
/// clears a key's tombstone, `forget` clears its dirty mark.
#[derive(Debug, Default)]
struct LaneBuffer {
    lanes: HashMap<LaneKey, LaneState>,
    dirty: HashSet<LaneKey>,
    tombstones: HashSet<LaneKey>,
}

/// `redb`-backed persistent implementation of [`PoolStateStore`].
///
/// Construct via [`PersistentPoolStateStore::open`]. The database is
/// owned for the lifetime of this value; drop closes the handle. The lane
/// table is buffered in memory: `record`/`forget` mutate the buffer only, and
/// a caller must call [`PoolStateStore::flush`] to commit it to disk. The
/// store is thread-safe — redb serialises writes internally via
/// single-writer transactions, and reads are MVCC.
#[derive(Debug)]
pub struct PersistentPoolStateStore {
    db: Database,
    path: PathBuf,
    buffer: Mutex<LaneBuffer>,
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
        F: FnOnce(&Path) -> Result<(), StoreError>,
    {
        identity::ensure_data_dir(data_dir).map_err(|err| {
            StoreError::Backend(format!(
                "data_dir {} failed security check: {err:#}",
                data_dir.display()
            ))
        })?;

        let path = data_dir.join(LANES_DB_FILE);

        // Reject a zero-length file. `redb::Database::create` treats both "file
        // does not exist" and "file exists but is empty" as "create a fresh
        // database" — so a `truncate -s 0 lanes.redb` (or a filesystem rollback
        // that nukes content but preserves the inode) would start with an empty
        // store and silently reopen the issue #527 replay window.
        //
        // We also record whether the file pre-existed so a subsequent chmod
        // failure can distinguish "we just created this file" (safe to remove on
        // cleanup) from "the operator has months of voucher state here" (MUST
        // NOT remove on a transient permission error). The TOCTOU window between
        // this stat and `Database::create` is closed via `OpenOptions::create_new`.
        let file_existed_before_open = match std::fs::metadata(&path) {
            Ok(meta) if meta.len() == 0 => {
                return Err(StoreError::Corrupt {
                    pool_id: None,
                    detail: format!(
                        "lane state store at {} is empty (length 0). \
                         This is either a manual truncation or a filesystem rollback, \
                         either of which silently re-opens the issue #527 voucher-replay window. \
                         Restore from backup, or delete the file deliberately to start fresh \
                         (forfeiting prior voucher history).",
                        path.display()
                    ),
                });
            }
            Ok(_) => true,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                match std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&path)
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

        let db = Database::create(&path).map_err(|err| {
            StoreError::Backend(format!(
                "failed to open lane state store at {}: {err}. \
                 Removing the file forfeits the issue #527 voucher-replay guard \
                 — restore from backup or investigate the corruption.",
                path.display()
            ))
        })?;

        // If the permission tighten fails, behaviour depends on whether the file
        // existed before this `open` call. Fresh file: remove it so the next
        // start sees a clean state. Pre-existing file (real voucher state on
        // disk): do NOT remove — a transient chmod failure on a read-only mount
        // or NFS would otherwise delete the live store and silently reopen the
        // issue #527 replay window.
        if let Err(chmod_err) = chmod_fn(&path) {
            // `drop(db)` is load-bearing on Windows: NTFS holds a mandatory
            // exclusive lock on the file handle, so `remove_file` below would
            // error with sharing-violation if the handle outlives.
            drop(db);
            Self::handle_chmod_failure(&path, &chmod_err, file_existed_before_open);
            return Err(chmod_err);
        }

        let lanes = Self::hydrate_lanes(&db)?;
        Ok(Self {
            db,
            path,
            buffer: Mutex::new(LaneBuffer {
                lanes,
                dirty: HashSet::new(),
                tombstones: HashSet::new(),
            }),
        })
    }

    /// Read the whole lane table into an in-memory map at open. A corrupt or
    /// forward-schema record aborts startup (running past it would reopen the
    /// issue #527 voucher-replay window).
    fn hydrate_lanes(db: &Database) -> Result<HashMap<LaneKey, LaneState>, StoreError> {
        let read_txn = db
            .begin_read()
            .map_err(|err| StoreError::Backend(format!("begin_read: {err}")))?;
        let table = match read_txn.open_table(LANE_TABLE) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(HashMap::new()),
            Err(err) => return Err(StoreError::Backend(format!("open_table: {err}"))),
        };
        let mut out = HashMap::new();
        let iter = table
            .iter()
            .map_err(|err| StoreError::Backend(format!("table iter: {err}")))?;
        for entry in iter {
            let (key_guard, value_guard) =
                entry.map_err(|err| StoreError::Backend(format!("iter entry: {err}")))?;
            let key_bytes: [u8; LANE_KEY_LEN] = *key_guard.value();
            let state = decode_record(&key_bytes, value_guard.value())?;
            out.insert(state.key(), state);
        }
        Ok(out)
    }

    /// Lock the working-set buffer, mapping a poisoned mutex to a backend error
    /// (anti-panic policy — never `unwrap` the guard).
    fn lock_buffer(&self) -> Result<std::sync::MutexGuard<'_, LaneBuffer>, StoreError> {
        self.buffer
            .lock()
            .map_err(|err| StoreError::Backend(format!("lane buffer mutex poisoned: {err}")))
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
    fn load_all(&self) -> Result<Vec<LaneState>, StoreError> {
        let buf = self.lock_buffer()?;
        Ok(buf.lanes.values().cloned().collect())
    }

    fn get(&self, key: LaneKey) -> Result<Option<LaneState>, StoreError> {
        let buf = self.lock_buffer()?;
        Ok(buf.lanes.get(&key).cloned())
    }

    /// Advance the in-memory lane state and mark it dirty. Durability is the
    /// background flush's job (ADR 003 §Off-chain voucher state persistence).
    fn record(&self, state: &LaneState) -> Result<(), StoreError> {
        let key = state.key();
        let mut buf = self.lock_buffer()?;
        buf.lanes.insert(key, state.clone());
        buf.tombstones.remove(&key);
        buf.dirty.insert(key);
        Ok(())
    }

    /// Drop the lane from the working set and mark it for deletion on the next
    /// flush. Idempotent.
    fn forget(&self, key: LaneKey) -> Result<(), StoreError> {
        let mut buf = self.lock_buffer()?;
        buf.lanes.remove(&key);
        buf.dirty.remove(&key);
        buf.tombstones.insert(key);
        Ok(())
    }

    /// Write every dirty lane and apply every tombstone in ONE fsynced redb
    /// transaction, then clear both sets. Idempotent — a no-op when clean. Holds
    /// the buffer lock across the commit so no `record` interleaves the drain.
    fn flush(&self) -> Result<(), StoreError> {
        let mut buf = self.lock_buffer()?;
        if buf.dirty.is_empty() && buf.tombstones.is_empty() {
            return Ok(());
        }
        let mut write_txn = self
            .db
            .begin_write()
            .map_err(|err| StoreError::Backend(format!("begin_write: {err}")))?;
        write_txn
            .set_durability(Durability::Immediate)
            .map_err(|err| StoreError::Backend(format!("set_durability: {err}")))?;
        {
            let mut table = write_txn
                .open_table(LANE_TABLE)
                .map_err(|err| StoreError::Backend(format!("open_table: {err}")))?;
            for key in &buf.dirty {
                let Some(state) = buf.lanes.get(key) else {
                    continue;
                };
                let encoded = postcard::to_allocvec(&StoredLaneState::from(state))
                    .map_err(|err| StoreError::Codec(format!("postcard encode failed: {err}")))?;
                let key_bytes = lane_key_bytes(key);
                table
                    .insert(&key_bytes, encoded.as_slice())
                    .map_err(|err| StoreError::Backend(format!("insert: {err}")))?;
            }
            for key in &buf.tombstones {
                let key_bytes = lane_key_bytes(key);
                table
                    .remove(&key_bytes)
                    .map_err(|err| StoreError::Backend(format!("remove: {err}")))?;
            }
        }
        write_txn
            .commit()
            .map_err(|err| StoreError::Backend(format!("commit (fsync): {err}")))?;
        buf.dirty.clear();
        buf.tombstones.clear();
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Capability store (seller first-redemption registration material)
// ---------------------------------------------------------------------------

impl PersistentPoolStateStore {
    /// Persist the owner-signed capability material for `(pool_id, signer)` so
    /// the redeemer can register the signer on its first on-chain redemption.
    /// Idempotent: a repeated intake of the same capability overwrites with an
    /// identical value. Fsynced on commit like every other write here.
    ///
    /// # Errors
    ///
    /// On a redb backend or postcard-encode failure.
    pub fn put_capability(
        &self,
        pool_id: B256,
        signer: Address,
        spending_cap: U256,
        expiry: u64,
        owner_sig: &[u8],
    ) -> Result<(), StoreError> {
        let record = StoredCapability {
            spending_cap: spending_cap.to_be_bytes(),
            expiry,
            owner_sig: owner_sig.to_vec(),
        };
        let encoded = postcard::to_allocvec(&record)
            .map_err(|err| StoreError::Codec(format!("capability postcard encode: {err}")))?;
        let key_bytes = capability_key_bytes(pool_id, signer);

        let mut write_txn = self
            .db
            .begin_write()
            .map_err(|err| StoreError::Backend(format!("begin_write: {err}")))?;
        write_txn
            .set_durability(Durability::Immediate)
            .map_err(|err| StoreError::Backend(format!("set_durability: {err}")))?;
        {
            let mut table = write_txn
                .open_table(CAPABILITY_TABLE)
                .map_err(|err| StoreError::Backend(format!("open_table: {err}")))?;
            table
                .insert(&key_bytes, encoded.as_slice())
                .map_err(|err| StoreError::Backend(format!("insert: {err}")))?;
        }
        write_txn
            .commit()
            .map_err(|err| StoreError::Backend(format!("commit (fsync): {err}")))?;
        Ok(())
    }

    /// Read the persisted capability material for `(pool_id, signer)`, or `None`
    /// if this node holds no capability for it.
    ///
    /// # Errors
    ///
    /// On a redb backend or postcard-decode failure.
    fn get_capability(
        &self,
        pool_id: B256,
        signer: Address,
    ) -> Result<Option<StoredCapability>, StoreError> {
        let key_bytes = capability_key_bytes(pool_id, signer);
        let read_txn = self
            .db
            .begin_read()
            .map_err(|err| StoreError::Backend(format!("begin_read: {err}")))?;
        let table = match read_txn.open_table(CAPABILITY_TABLE) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(err) => return Err(StoreError::Backend(format!("open_table: {err}"))),
        };
        let Some(value_guard) = table
            .get(&key_bytes)
            .map_err(|err| StoreError::Backend(format!("get: {err}")))?
        else {
            return Ok(None);
        };
        let record: StoredCapability = postcard::from_bytes(value_guard.value())
            .map_err(|err| StoreError::Codec(format!("capability postcard decode: {err}")))?;
        Ok(Some(record))
    }
}

/// Write side of the capability table used by the seller voucher-intake path.
/// Kept as a trait so the [`ClientHandler`](crate::handlers::client::ClientHandler)
/// holds it behind an `Arc<dyn CapabilitySink>` and tests can pass `None` (an
/// in-memory store has no capability table).
pub trait CapabilitySink: Send + Sync + std::fmt::Debug {
    /// Persist the owner-signed capability material for `(pool_id, signer)`.
    ///
    /// # Errors
    ///
    /// On a store backend or codec failure.
    fn store_capability(
        &self,
        pool_id: B256,
        signer: Address,
        spending_cap: U256,
        expiry: u64,
        owner_sig: &[u8],
    ) -> Result<(), StoreError>;
}

impl CapabilitySink for PersistentPoolStateStore {
    fn store_capability(
        &self,
        pool_id: B256,
        signer: Address,
        spending_cap: U256,
        expiry: u64,
        owner_sig: &[u8],
    ) -> Result<(), StoreError> {
        self.put_capability(pool_id, signer, spending_cap, expiry, owner_sig)
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
        match self.inner.get_capability(key.pool_id, key.signer) {
            Ok(Some(record)) => Some(crate::payment_settlement::CapabilityMaterial {
                spending_cap: U256::from_be_bytes(record.spending_cap),
                expiry: record.expiry,
                owner_sig: alloy::primitives::Bytes::from(record.owner_sig),
            }),
            Ok(None) => None,
            Err(err) => {
                // A read fault here is not fatal: the redeemer skips a signer with
                // no material, so the lane simply waits for the next redemption
                // attempt rather than registering against a bad payload.
                tracing::warn!(
                    pool_id = %key.pool_id,
                    signer = %key.signer,
                    error = %err,
                    "capability store read failed; skipping first-redemption registration"
                );
                None
            }
        }
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
        BuyerPoolTable::new(&self.db).insert_raw(pool_id, bytes)
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
        BuyerPoolTable::new(&self.inner.db)
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
        table_def: TableDefinition<&[u8; 32], u64>,
        entry: &PendingSettle,
    ) -> Result<(), StoreError> {
        let key: [u8; 32] = entry.pool_id.into();
        let mut write_txn = self
            .db
            .begin_write()
            .map_err(|err| StoreError::Backend(format!("begin_write: {err}")))?;
        // Force fsync-on-commit, same durability discipline as `record`: the
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
        table_def: TableDefinition<&[u8; 32], u64>,
    ) -> Result<Vec<PendingSettle>, StoreError> {
        let read_txn = self
            .db
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
        table_def: TableDefinition<&[u8; 32], u64>,
        pool_id: B256,
    ) -> Result<(), StoreError> {
        let key: [u8; 32] = pool_id.into();
        {
            let read_txn = self
                .db
                .begin_read()
                .map_err(|err| StoreError::Backend(format!("begin_read: {err}")))?;
            match read_txn.open_table(table_def) {
                Ok(_) => {}
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(()),
                Err(err) => return Err(StoreError::Backend(format!("open_table: {err}"))),
            }
        }

        let mut write_txn = self
            .db
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

impl PoolFloorLossStore for PersistentPoolStateStore {
    fn record_loss(&self, pool_id: B256, micro_usdc: u128) -> Result<(), StoreError> {
        let key: [u8; 32] = pool_id.into();
        let mut write_txn = self
            .db
            .begin_write()
            .map_err(|err| StoreError::Backend(format!("begin_write: {err}")))?;
        // Force fsync-on-commit, same durability discipline as the other
        // tables: a lost dead-charge entry after a restart would silently
        // re-grant a pool a fresh free-floor budget.
        write_txn
            .set_durability(Durability::Immediate)
            .map_err(|err| StoreError::Backend(format!("set_durability: {err}")))?;
        {
            let mut table = write_txn
                .open_table(POOL_FLOOR_LOSS_TABLE)
                .map_err(|err| StoreError::Backend(format!("open_table: {err}")))?;
            // Monotonic write: a pool's `dead_charge` only ever grows. Concurrent
            // reservation drops on the same pool commit from independent blocking
            // threads and can land out of order, so take the max with what is
            // already on disk — a late, smaller write must never regress the row
            // and re-grant already-consumed free-floor budget.
            let existing = table
                .get(&key)
                .map_err(|err| StoreError::Backend(format!("get: {err}")))?
                .map_or(0u128, |v| v.value());
            table
                .insert(&key, existing.max(micro_usdc))
                .map_err(|err| StoreError::Backend(format!("insert: {err}")))?;
        }
        write_txn
            .commit()
            .map_err(|err| StoreError::Backend(format!("commit (fsync): {err}")))?;
        Ok(())
    }

    fn load_losses(&self) -> Result<Vec<(B256, u128)>, StoreError> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|err| StoreError::Backend(format!("begin_read: {err}")))?;
        // A never-written table means no pool has accrued a dead charge yet —
        // first-boot tolerance, matching the other tables in this file.
        let table = match read_txn.open_table(POOL_FLOOR_LOSS_TABLE) {
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
            out.push((B256::from(*key_guard.value()), value_guard.value()));
        }
        Ok(out)
    }

    fn forget_loss(&self, pool_id: B256) -> Result<(), StoreError> {
        let key: [u8; 32] = pool_id.into();

        // Check first whether the table has ever been created. forget on a
        // never-written store is a no-op by contract and must not create the
        // table as a side effect.
        {
            let read_txn = self
                .db
                .begin_read()
                .map_err(|err| StoreError::Backend(format!("begin_read: {err}")))?;
            match read_txn.open_table(POOL_FLOOR_LOSS_TABLE) {
                Ok(_) => {}
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(()),
                Err(err) => return Err(StoreError::Backend(format!("open_table: {err}"))),
            }
        }

        let mut write_txn = self
            .db
            .begin_write()
            .map_err(|err| StoreError::Backend(format!("begin_write: {err}")))?;
        write_txn
            .set_durability(Durability::Immediate)
            .map_err(|err| StoreError::Backend(format!("set_durability: {err}")))?;
        {
            let mut table = write_txn
                .open_table(POOL_FLOOR_LOSS_TABLE)
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
            .db
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
            .db
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
        )
    }

    /// The capability store round-trips: what the seller intake persists via
    /// [`CapabilitySink::store_capability`] is exactly what the redeemer reads
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
        CapabilitySink::store_capability(
            store.as_ref(),
            pool_id,
            signer,
            spending_cap,
            expiry,
            &owner_sig,
        )?;
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
        let store = PersistentPoolStateStore::open(dir.path())?;
        let mode = std::fs::metadata(store.path())?.permissions().mode() & 0o777;
        anyhow::ensure!(
            mode == DB_FILE_MODE,
            "file mode {mode:o} != expected {DB_FILE_MODE:o}"
        );
        Ok(())
    }

    /// **Cleanup-asymmetry regression (#527 follow-up).** On `tighten_permissions`
    /// failure for a FRESHLY-CREATED file, the cleanup branch MUST remove the
    /// partial file so the next start sees a clean state.
    #[test]
    fn fresh_file_chmod_failure_removes_file() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let chmod_failed = std::io::Error::other("simulated chmod failure");
        let path_buf = dir.path().join(LANES_DB_FILE);
        anyhow::ensure!(!path_buf.exists(), "precondition: file does not exist");

        let path_for_closure = path_buf.clone();
        let err = PersistentPoolStateStore::open_with(dir.path(), |_path| {
            Err(StoreError::PermissionTighten {
                path: path_for_closure.clone(),
                source: chmod_failed,
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

        let path_for_closure = path_buf.clone();
        let err = PersistentPoolStateStore::open_with(dir.path(), move |_path| {
            Err(StoreError::PermissionTighten {
                path: path_for_closure,
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
            last_amount: s.last_amount().to_be_bytes(),
            last_bytes_delivered: s.last_bytes_delivered().to_be_bytes(),
            signature: s.last_signature().map_or_else(Vec::new, |x| x.to_vec()),
            cap: s.cap.to_be_bytes(),
            expiry: s.expiry,
        };
        let encoded = postcard::to_allocvec(&forward)?;
        {
            let store = PersistentPoolStateStore::open(dir.path())?;
            let mut wtx = store.db.begin_write()?;
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
            let mut tx = store.db.begin_write()?;
            tx.set_durability(Durability::Immediate)?;
            {
                let mut t = tx.open_table(LANE_TABLE)?;
                t.insert(&key_bytes, encoded.as_slice())?;
            }
            tx.commit()?;
        }
        // Hydration at open() reads the whole table, so reopen to pick up the
        // record this test wrote directly (bypassing the buffer).
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
            let mut tx = store.db.begin_write()?;
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

    /// Each [`CheckpointKey`] is an independent cursor in the one table.
    #[test]
    fn watcher_checkpoint_keys_are_independent() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let store = PersistentPoolStateStore::open(dir.path())?;
        store.record_checkpoint(CheckpointKey::PoolOpened, 100)?;
        store.record_checkpoint(CheckpointKey::Origin, 300)?;
        anyhow::ensure!(store.load_checkpoint(CheckpointKey::PoolOpened)? == Some(100));
        anyhow::ensure!(store.load_checkpoint(CheckpointKey::Origin)? == Some(300));
        store.record_checkpoint(CheckpointKey::Origin, 350)?;
        anyhow::ensure!(store.load_checkpoint(CheckpointKey::PoolOpened)? == Some(100));
        anyhow::ensure!(store.load_checkpoint(CheckpointKey::Origin)? == Some(350));
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

    /// The pool floor-loss dead-charge accumulator round-trips, survives a
    /// reopen (durable commit), and forgets cleanly. Keyed by `pool_id`.
    #[test]
    fn redb_pool_floor_loss_round_trip_and_persist() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let a_pool = sample(1).pool_id;
        let b_pool = sample(2).pool_id;
        {
            let store = PersistentPoolStateStore::open(dir.path())?;
            store.record_loss(a_pool, 1_234_567_890_123u128)?;
            store.record_loss(b_pool, 42u128)?;
            // Overwrite must not add a row.
            store.record_loss(a_pool, 999_999_999_999_999u128)?;
        }
        // Reopen over the SAME path — the value must survive the durable commit.
        let store = PersistentPoolStateStore::open(dir.path())?;
        let mut all = store.load_losses()?;
        all.sort_by_key(|(id, _)| *id);
        anyhow::ensure!(all.len() == 2, "overwrite must not add a row");
        let first = all.first().ok_or_else(|| anyhow::anyhow!("missing [0]"))?;
        let second = all.get(1).ok_or_else(|| anyhow::anyhow!("missing [1]"))?;
        anyhow::ensure!(*first == (a_pool, 999_999_999_999_999u128));
        anyhow::ensure!(*second == (b_pool, 42u128));

        // forget clears it, and forget on a never-recorded pool is a no-op.
        store.forget_loss(a_pool)?;
        store.forget_loss(b_pool)?;
        anyhow::ensure!(store.load_losses()?.is_empty());
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

    #[test]
    fn flush_when_clean_is_noop_ok() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let s = PersistentPoolStateStore::open(dir.path())?;
        s.flush()?; // nothing dirty
        Ok(())
    }
}
