//! Disk-backed `ChannelStateStore` for the node runtime.
//!
//! Implements [`ChannelStateStore`] against a single `redb` database file at
//! `<data_dir>/channels.redb`. Every successful `record` call performs an
//! fsynced commit (redb's default [`redb::Durability::Immediate`], set
//! explicitly here so a future redb default change doesn't silently weaken
//! the durability guarantee).
//!
//! See [`decdn_incentive::store`] for the trait contract and
//! [ADR 003 §Off-chain voucher state persistence] for the protocol rule
//! this implements: a node MUST persist `(last_nonce, last_amount,
//! last_bytes_delivered)` before acknowledging delivery, otherwise a
//! restart re-opens the channel at nonce zero and a client can replay a
//! previously-accepted voucher for a second byte delivery (issue #527).
//!
//! Schema v2 (#327) additionally persists the latest voucher's signature (so
//! the seller settlement path can submit it to the on-chain
//! `closeChannel` / `withdraw` after a restart without forfeiting the claim)
//! and the channel's on-chain expiry (so the node can close and stop serving
//! before `reclaimExpired` becomes available to the client). Both are encoded
//! as trailing postcard segments after the v1 prefix (signature then expiry);
//! v1 records hydrate with an empty signature and `0` expiry and are simply
//! unredeemable until the next voucher re-records them.
//!
//! [ADR 003 §Off-chain voucher state persistence]: ../../../adr/003-payments.md

use std::path::{Path, PathBuf};

use alloy::primitives::{Address, B256, U256};
use decdn_common::identity;
use decdn_incentive::store::{
    ChannelStateStore, PendingSettle, PendingSettleStore, StoreError, WatcherCheckpointStore,
};
use decdn_incentive::{
    AdvanceOutcome, BuyerChannelState, BuyerChannelStore, ChannelId, ChannelState, DepositOutcome,
};
use redb::{Database, Durability, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};

/// File name of the redb database within `data_dir`.
const CHANNELS_DB_FILE: &str = "channels.redb";

/// On-disk file mode (`0o600` — owner-only read+write). Defense-in-depth: the
/// containing `data_dir` is already enforced to `0o700` by
/// [`decdn_common::identity::ensure_data_dir`], so other local users cannot
/// reach the file via path traversal, but we still tighten the file mode in
/// case the directory ACL is widened out-of-band.
#[cfg(unix)]
const DB_FILE_MODE: u32 = 0o600;

/// Highest `schema_version` this binary can decode. On-disk records carrying
/// a higher value will cause `load_all` to refuse to start (see
/// [`StoreError::UnsupportedSchema`]) — opening a forward-incompatible store
/// is unsafe because we cannot honour the persistence invariant for fields
/// we do not understand.
const SUPPORTED_SCHEMA_VERSION: u32 = 2;

/// Sanity ceiling on trailing bytes per record. Trailing bytes are tolerated
/// (forward-compat with additive schema changes — see [`StoredChannelState`]),
/// but a `remainder.len()` above this threshold is logged as a warning so
/// an honest schema-skew incident or malicious padding attempt is observable
/// in operator logs without re-introducing the strict-decoding regression
/// issue #527's reviewers warned against. Sized to fit a few realistic
/// future additive fields (a Vec or two of 32-byte hashes) with headroom.
const SANE_TRAILER_MAX_BYTES: usize = 256;

/// redb table holding the per-channel voucher state. The table name is
/// version-tagged so a future breaking on-disk layout change can ship as
/// `channel_state_v2` with a one-shot migration on open; additive changes
/// stay on `_v1` (postcard skips unknown trailing bytes).
///
/// Key: raw `ChannelId` bytes (`[u8; 32]`).
/// Value: postcard-encoded [`StoredChannelState`] (variable length).
const CHANNEL_TABLE: TableDefinition<&[u8; 32], &[u8]> = TableDefinition::new("channel_state_v1");

/// redb table holding buyer-side channel bookkeeping (#744), keyed by the
/// **upstream provider address** so the cache-miss open trigger can reuse an
/// existing channel instead of opening (and depositing into) a new one. Lives
/// in the same database file as [`CHANNEL_TABLE`] so both stores share one
/// `redb::Database` handle and the file-mode hardening — redb forbids two
/// `Database` handles to the same file, so a second store file would need the
/// whole #527 open/cleanup path duplicated. The two tables never collide:
/// distinct names, distinct key widths (`[u8; 20]` here vs `[u8; 32]`).
///
/// Key: raw provider `Address` bytes (`[u8; 20]`).
/// Value: postcard-encoded [`StoredBuyerChannelState`] (variable length).
const BUYER_CHANNEL_TABLE: TableDefinition<&[u8; 20], &[u8]> =
    TableDefinition::new("buyer_channel_state_v1");

/// Highest buyer-record `schema_version` this binary can decode. Independent
/// of [`SUPPORTED_SCHEMA_VERSION`] (the seller table) — the buyer table is new
/// in #744 with no legacy records, so it starts at 1 and carries `expires_at`
/// inline rather than as a trailing segment.
const BUYER_SUPPORTED_SCHEMA_VERSION: u32 = 1;

/// redb table holding the pending-settle set (#327 / PR #743 review): channels
/// this node closed on-chain that await a `settleChannel` finalization once
/// their dispute window elapses. Lives in the same database file as
/// [`CHANNEL_TABLE`] so a single open + single fsync discipline covers both.
///
/// Key: raw `ChannelId` bytes (`[u8; 32]`).
/// Value: the on-chain `disputeDeadline` (Unix seconds) — `settleChannel`
/// reverts before this, so the sweep gates submission on it. A fixed-width
/// native `u64` value needs no postcard envelope (unlike [`CHANNEL_TABLE`]),
/// so there is no schema-version trailer to evolve here.
const PENDING_SETTLE_TABLE: TableDefinition<&[u8; 32], u64> =
    TableDefinition::new("pending_settle_v1");

/// redb table holding channels this node closed as the **buyer** (#988) — the
/// unilateral `closeChannel` of an unreachable provider's idle channel — that
/// await a `settleChannel` after their dispute window. Same shape as
/// [`PENDING_SETTLE_TABLE`] (key: `ChannelId` bytes, value: `disputeDeadline`)
/// but a SEPARATE table so the buyer settle sweep and the seller settle sweep
/// never settle each other's closes — keeping the two metric families (#989)
/// cleanly attributed. Lives in the same database file as [`CHANNEL_TABLE`].
const BUYER_PENDING_SETTLE_TABLE: TableDefinition<&[u8; 32], u64> =
    TableDefinition::new("buyer_pending_settle_v1");

/// redb table holding the settlement watcher's `ChannelOpened` scan checkpoint
/// (#751): the last block scanned, so the bring-up backfill resumes across
/// restarts and covers channels opened while the node was down. Lives in the
/// same database file as [`CHANNEL_TABLE`]. A single fixed string key holds the
/// block height; a fixed-width native `u64` value needs no postcard envelope.
const WATCHER_CHECKPOINT_TABLE: TableDefinition<&str, u64> =
    TableDefinition::new("watcher_checkpoint_v1");

/// The sole key in [`WATCHER_CHECKPOINT_TABLE`] — the last block scanned for
/// `ChannelOpened`.
const WATCHER_CHECKPOINT_KEY: &str = "channel_opened_last_block";

/// On-disk record. All numeric fields use fixed-size big-endian byte arrays
/// instead of variable-length integers so the encoded value width is stable
/// across postcard versions and identical to the on-chain representation,
/// making manual inspection straightforward.
///
/// `schema_version` lives in the value (not the table name) so a future
/// additive field on this struct can ship without renaming the table.
/// Forward-compat requires the decode site to use [`postcard::take_from_bytes`]
/// (which returns `(T, &[u8])` and tolerates trailing unknown bytes) rather
/// than [`postcard::from_bytes`] (which is strict and errors with
/// `DeserializeTrailingBytes`). A reader carrying an older
/// `SUPPORTED_SCHEMA_VERSION` can then decode the prefix it understands and
/// ignore additive fields a newer writer appended. Breaking shape changes
/// (field removal, field reorder, type change) still require bumping the
/// table name to a new `channel_state_vN` and a one-shot migration on open.
///
/// The schema-v2 voucher signature and channel expiry (#327) are NOT fields
/// of this struct — they are encoded as their own postcard segments
/// (`Vec<u8>` signature then `u64` expiry) appended after this prefix, so the
/// v1 prefix shape stays byte-identical across v1 and v2 and the
/// `take_from_bytes` prefix decode is unchanged. `decode_record` reads those
/// segments when `schema_version >= 2`; a v1 record (no trailing segments)
/// hydrates with an empty signature and `0` expiry.
#[derive(Debug, Serialize, Deserialize)]
struct StoredChannelState {
    schema_version: u32,
    channel_id: [u8; 32],
    client: [u8; 20],
    token: [u8; 20],
    deposit: [u8; 32],
    last_amount: [u8; 32],
    last_nonce: [u8; 32],
    last_bytes_delivered: [u8; 32],
}

impl From<&ChannelState> for StoredChannelState {
    fn from(state: &ChannelState) -> Self {
        Self {
            schema_version: SUPPORTED_SCHEMA_VERSION,
            channel_id: state.channel_id.into(),
            client: state.client.into(),
            token: state.token.into(),
            deposit: state.deposit.to_be_bytes(),
            last_amount: state.last_amount().to_be_bytes(),
            last_nonce: state.last_nonce().to_be_bytes(),
            last_bytes_delivered: state.last_bytes_delivered().to_be_bytes(),
        }
    }
}

/// The schema-v2 trailing payload (#327): the latest voucher signature then the
/// channel expiry, appended after the [`StoredChannelState`] v1 prefix. Bundled
/// into one type (#751) so the field order lives in the struct rather than in
/// mirror-image `record` / `decode_record` call sites.
///
/// **On-disk byte-compatibility:** postcard encodes a struct as the bare
/// concatenation of its fields in declaration order, so encoding this struct is
/// byte-identical to the previous "append `postcard(signature)` then
/// `postcard(expires_at)`" layout — no schema bump, and existing v2 records
/// decode unchanged. `signature` stays a length-prefixed `Vec<u8>` on disk (the
/// in-memory [`ChannelState`] carries the stronger `Option<[u8; 65]>`; the
/// conversion lives at the [`StoredChannelState::into_state`] / `record`
/// boundary). It MUST remain a **trailing** segment, never a prefix field: a v2
/// reader decoding an existing on-disk v1 record (which has no trailer) would
/// otherwise read past end-of-input.
#[derive(Debug, Serialize, Deserialize)]
struct TrailerV2 {
    signature: Vec<u8>,
    expires_at: u64,
}

impl StoredChannelState {
    /// Reconstruct the in-memory [`ChannelState`] via its hydration constructor
    /// (the trusted cross-crate writer, #527/#751). `last_signature` and
    /// `expires_at` are the decoded v2 trailer (empty / `0` for v1 records and
    /// channels with no accepted voucher yet) — see [`StoredChannelState`]'s
    /// doc. The on-disk signature is a length-prefixed `Vec<u8>`; it is narrowed
    /// to the in-memory `Option<[u8; 65]>` here: empty → `None`, exactly 65
    /// bytes → `Some`, any other length → [`StoreError::Corrupt`] (a malformed
    /// record we must not silently submit to the on-chain `closeChannel`).
    fn into_state(
        self,
        last_signature: Vec<u8>,
        expires_at: u64,
        cooperative_close_signed: bool,
    ) -> Result<ChannelState, StoreError> {
        if self.schema_version > SUPPORTED_SCHEMA_VERSION {
            return Err(StoreError::UnsupportedSchema {
                found: self.schema_version,
                supported: SUPPORTED_SCHEMA_VERSION,
            });
        }
        let channel_id = B256::from(self.channel_id);
        let last_signature: Option<[u8; 65]> = if last_signature.is_empty() {
            None
        } else {
            let len = last_signature.len();
            Some(
                <[u8; 65]>::try_from(last_signature).map_err(|_| StoreError::Corrupt {
                    channel_id: Some(channel_id),
                    detail: format!("stored voucher signature is {len} bytes, expected 0 or 65"),
                })?,
            )
        };
        Ok(ChannelState::hydrate(
            channel_id,
            Address::from(self.client),
            Address::from(self.token),
            U256::from_be_bytes(self.deposit),
            U256::from_be_bytes(self.last_amount),
            U256::from_be_bytes(self.last_nonce),
            U256::from_be_bytes(self.last_bytes_delivered),
            last_signature,
            expires_at,
            cooperative_close_signed,
        ))
    }
}

/// `redb`-backed persistent implementation of [`ChannelStateStore`].
///
/// Construct via [`PersistentChannelStateStore::open`]. The database is
/// owned for the lifetime of this value; drop closes the handle. The store
/// is thread-safe — redb serialises writes internally via single-writer
/// transactions, and reads are MVCC.
#[derive(Debug)]
pub struct PersistentChannelStateStore {
    db: Database,
    path: PathBuf,
}

impl PersistentChannelStateStore {
    /// Open (or create) the channel-state store under `data_dir`.
    ///
    /// `data_dir` is validated through
    /// [`decdn_common::identity::ensure_data_dir`] before any redb operation,
    /// which enforces `0o700` on the directory and rejects insecure modes.
    /// The store file is then chmod'd to `0o600` after creation as
    /// defense-in-depth.
    ///
    /// # Errors
    ///
    /// - [`StoreError::Backend`] when the `data_dir` security check fails
    ///   (wrong mode, not a directory, missing capability). Recovery: fix
    ///   the directory's permissions; this is a setup / packaging issue,
    ///   not a runtime fault.
    /// - [`StoreError::Backend`] when `redb::Database::create` refuses the
    ///   file (corrupt magic, truncated header, ENOSPC, EACCES on the file
    ///   itself, or any I/O failure from the underlying file open).
    ///   Recovery: restore from backup or investigate the corruption (do
    ///   NOT just delete the file — that forfeits the issue #527 guard).
    /// - [`StoreError::Corrupt`] when the file exists but is zero-length —
    ///   `redb` would otherwise treat that as "create a fresh database"
    ///   and silently reopen the issue #527 replay window. The variant's
    ///   `detail` field names the file path and how to recover.
    /// - [`StoreError::PermissionTighten`] when the post-create chmod fails
    ///   (read-only mount, EPERM, missing capability). Recovery: chmod
    ///   the file to `0o600` manually; this is an operator action, not a
    ///   retry candidate, and the variant carries the path explicitly so
    ///   log triage can escalate it above transient I/O.
    /// - [`StoreError::Io`] for any other filesystem error encountered
    ///   while stat-ing the file.
    ///
    /// When this returns `Err`, the caller MUST abort node bring-up —
    /// starting with a clean store silently forfeits the issue #527
    /// guarantee.
    pub fn open(data_dir: &Path) -> Result<Self, StoreError> {
        Self::open_with(data_dir, Self::tighten_permissions)
    }

    /// Testable form of [`Self::open`] that takes an injectable chmod
    /// function. Non-test callers use [`Self::open`], which delegates here
    /// with [`Self::tighten_permissions`]; tests inject a closure that
    /// simulates chmod failure to exercise the cleanup-branch asymmetry
    /// (the security-critical fix for #527 follow-up review).
    ///
    /// `pub(crate)` deliberately — exposing this beyond the crate would
    /// let an external caller pass a no-op `chmod_fn` and silently weaken
    /// the file-mode hardening.
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

        let path = data_dir.join(CHANNELS_DB_FILE);

        // Reject a zero-length file. `redb::Database::create` treats both
        // "file does not exist" and "file exists but is empty" as "create
        // a fresh database" — so an operator who runs `truncate -s 0
        // channels.redb` (or a filesystem rollback that nukes content but
        // preserves the inode) would start with an empty store and
        // silently reopen the issue #527 replay window. After the first
        // successful commit the file is non-zero forever in normal
        // operation, so this check is precise: zero-length on disk means
        // the store has been deliberately or accidentally wiped.
        //
        // We also record whether the file pre-existed so a subsequent
        // chmod failure can distinguish "we just created this file" (safe
        // to remove on cleanup) from "the operator has months of voucher
        // state here" (MUST NOT remove on a transient permission error).
        // The TOCTOU window between this stat and `Database::create` is
        // closed via `OpenOptions::create_new` below: when the stat says
        // `NotFound` we try to atomically create the file ourselves, and
        // an `AlreadyExists` error from `create_new` flips us to the
        // "pre-existing, do not delete" branch. A sibling process that
        // populated the file between our stat and our `create_new` is
        // therefore treated identically to a file that was there all
        // along — we never delete its work.
        let file_existed_before_open = match std::fs::metadata(&path) {
            Ok(meta) if meta.len() == 0 => {
                return Err(StoreError::Corrupt {
                    channel_id: None,
                    detail: format!(
                        "channel state store at {} is empty (length 0). \
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
                // Try to claim file creation atomically. `Database::create`
                // below will then either initialise its redb structure
                // inside our empty file (Ok branch) or open the file a
                // racing sibling wrote (AlreadyExists branch).
                match std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&path)
                {
                    Ok(file) => {
                        // Drop the handle immediately; redb opens its own.
                        // Setting perms here would race with the
                        // post-create `tighten_permissions`; defer it.
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
                "failed to open channel state store at {}: {err}. \
                 Removing the file forfeits the issue #527 voucher-replay guard \
                 — restore from backup or investigate the corruption.",
                path.display()
            ))
        })?;

        // If the permission tighten fails, behaviour depends on whether
        // the file existed before this `open` call. For a fresh file
        // (first-ever start, file just created by `Database::create`),
        // remove the partial file so the next start observes a clean
        // "not found" state rather than an out-of-spec file with
        // umask-default mode. For a pre-existing file (subsequent start
        // with real voucher state on disk), do NOT remove: a transient
        // chmod failure on a read-only mount or NFS would otherwise
        // delete the live store and silently reopen the issue #527
        // replay window. The pre-existing-file path leaves the on-disk
        // mode unchanged: usually 0o600 from a prior successful open,
        // but not verified here — a botched manual restore could leave
        // a wider mode, and the operator log message names this so the
        // operator can `stat` the file and decide.
        if let Err(chmod_err) = chmod_fn(&path) {
            // `drop(db)` is load-bearing on Windows: NTFS holds a mandatory
            // exclusive lock on the file handle, so `remove_file` below
            // would error with sharing-violation if the handle outlives.
            // On Unix the drop is a no-op (unlink-while-open is fine), but
            // we keep the symmetry so a future Windows port works without
            // a one-off branch.
            drop(db);
            Self::handle_chmod_failure(&path, &chmod_err, file_existed_before_open);
            return Err(chmod_err);
        }

        Ok(Self { db, path })
    }

    /// Operator-facing logging for the chmod-failure cleanup branch.
    /// Extracted so the security policy ("preserve pre-existing, remove
    /// fresh") lives in one place; the branches always emit
    /// `tracing::error!` (not `warn!`) because both branches refuse to
    /// bring the node up and the operator needs an error-level signal in
    /// log aggregation.
    ///
    /// `file_existed_before_open` is authoritative: it is set to `true`
    /// either when the initial stat saw a non-empty file, or when our
    /// `OpenOptions::create_new` lost the race to a sibling writer. In
    /// both cases the file is not ours to delete.
    fn handle_chmod_failure(path: &Path, chmod_err: &StoreError, file_existed_before_open: bool) {
        let observed_mode = Self::observed_mode_string(path);
        if file_existed_before_open {
            tracing::error!(
                path = %path.display(),
                observed_mode = %observed_mode,
                event = "channel_store_chmod_fail_preserve",
                %chmod_err,
                "chmod failed on pre-existing channel state store; refusing to delete (would reopen issue #527 replay window). \
                 Investigate the permission error and chmod the file to 0o600 manually before retry.",
            );
            return;
        }

        // Fresh file path: we definitively created this file ourselves
        // via `OpenOptions::create_new` upstream, so removing it cannot
        // destroy anyone else's voucher state.
        if let Err(remove_err) = std::fs::remove_file(path) {
            tracing::error!(
                %remove_err,
                path = %path.display(),
                observed_mode = %observed_mode,
                event = "channel_store_chmod_fail_remove_failed",
                %chmod_err,
                "chmod failed on freshly-created channel state store, and removal also failed; \
                 file persists with current mode. chmod 0o600 manually before next start.",
            );
        }
    }

    /// Best-effort stringified file mode for operator log lines. Returns
    /// `"unknown"` on stat failure or non-Unix targets — the field is
    /// purely informational, so a missing value is preferable to a panic
    /// or an `Option<u32>` leaking into every log macro.
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

    /// Tighten the on-disk file mode to `0o600` after open. **Idempotent**:
    /// if the file is already at the target mode, the syscall is skipped
    /// so a read-only mount (EROFS) where the mode is correct from a
    /// prior boot does not brick subsequent startups.
    ///
    /// No-op on non-Unix targets (Windows ACLs are controlled at directory
    /// level, per the comment on [`DB_FILE_MODE`]). The non-Unix branch
    /// emits a one-shot debug log so a future Windows operator can audit
    /// that the tightening was deliberately skipped.
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
            "channel state store: file-mode tightening skipped on non-unix; relying on data_dir ACL",
        );
        Ok(())
    }

    /// Filesystem path of the underlying database file. Useful for log
    /// messages and operator runbooks.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Decode one stored record (table value) into a [`ChannelState`], given its
/// table key (`channel_id` bytes). Shared by `load_all` and `get`.
///
/// `take_from_bytes` instead of `from_bytes` so a newer writer's additive
/// fields (trailing bytes to an older reader) decode cleanly — see
/// [`StoredChannelState`]'s doc and the schema-version handshake in
/// `into_state`.
///
/// What this DOES guard: forward-compat for additive schema changes, and
/// outright structural corruption (insufficient bytes, malformed varint)
/// which `take_from_bytes` rejects. What it does NOT guard: intra-record
/// bit-flips inside fixed-shape fields — redb page checksums catch on-disk
/// bit-rot one layer down before we see the value.
fn decode_record(key_bytes: [u8; 32], value_bytes: &[u8]) -> Result<ChannelState, StoreError> {
    let channel_id = B256::from(key_bytes);
    let (stored, remainder): (StoredChannelState, &[u8]) = postcard::take_from_bytes(value_bytes)
        .map_err(|err| StoreError::Corrupt {
        channel_id: Some(channel_id),
        detail: format!("postcard decode failed: {err}"),
    })?;
    if stored.channel_id != key_bytes {
        return Err(StoreError::Corrupt {
            channel_id: Some(channel_id),
            detail: "channel_id in value does not match table key".into(),
        });
    }
    // Schema v2 (#327) appends a single [`TrailerV2`] postcard segment after the
    // v1 prefix (voucher signature then channel expiry). Decode it only for a
    // version this binary understands; a version above `SUPPORTED_SCHEMA_VERSION`
    // is left for `into_state` to reject (its trailing layout is unknown). A v1
    // record has no trailer and hydrates with an empty signature and `0` expiry.
    let (last_signature, expires_at, after_trailer): (Vec<u8>, u64, &[u8]) =
        if stored.schema_version >= 2 && stored.schema_version <= SUPPORTED_SCHEMA_VERSION {
            let (trailer, rest) =
                postcard::take_from_bytes::<TrailerV2>(remainder).map_err(|err| {
                    StoreError::Corrupt {
                        channel_id: Some(channel_id),
                        detail: format!("postcard decode of v2 trailer failed: {err}"),
                    }
                })?;
            (trailer.signature, trailer.expires_at, rest)
        } else {
            (Vec::new(), 0, remainder)
        };
    // Cooperative-close waiver flag (ADR 003 §Cooperative close): an optional
    // trailing `bool` segment that only ever exists on v2+ records. Gate the
    // decode on `schema_version >= 2`: a v1 record never wrote this segment, so
    // any bytes after its (empty) trailer are NOT a coop-close `bool` and must
    // not be decoded as one — doing so would mis-hydrate the flag or corrupt the
    // load. v2+ records written before the flag existed simply have no trailing
    // bytes, so they default to `false`, the additive forward-compat the
    // `take_from_bytes` design intends.
    let (cooperative_close_signed, leftover): (bool, &[u8]) =
        if stored.schema_version >= 2 && !after_trailer.is_empty() {
            postcard::take_from_bytes::<bool>(after_trailer).map_err(|err| StoreError::Corrupt {
                channel_id: Some(channel_id),
                detail: format!("postcard decode of coop-close flag failed: {err}"),
            })?
        } else {
            (false, after_trailer)
        };
    // Forward-compat allowance is bounded: a malicious writer could pad
    // megabytes onto every record and silently inflate every read. Log
    // (don't fail) when the trailer beyond the known fields exceeds a small
    // sanity ceiling so a future schema-skew incident is observable in
    // operator logs without re-introducing the strict-decoding regression
    // issue #527's reviewers warned against.
    if leftover.len() > SANE_TRAILER_MAX_BYTES {
        tracing::warn!(
            %channel_id,
            remainder = leftover.len(),
            limit = SANE_TRAILER_MAX_BYTES,
            event = "channel_store_excess_trailer",
            "channel state record has unusually large trailing bytes; possible malicious padding or large-additive-field schema skew",
        );
    }
    stored.into_state(last_signature, expires_at, cooperative_close_signed)
}

impl ChannelStateStore for PersistentChannelStateStore {
    fn load_all(&self) -> Result<Vec<ChannelState>, StoreError> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|err| StoreError::Backend(format!("begin_read: {err}")))?;
        // open_table on a never-written database returns TableDoesNotExist;
        // treat that as an empty store rather than an error so first boot
        // (no vouchers ever accepted) succeeds cleanly.
        let table = match read_txn.open_table(CHANNEL_TABLE) {
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
            let key_bytes: [u8; 32] = *key_guard.value();
            let value_bytes = value_guard.value();
            out.push(decode_record(key_bytes, value_bytes)?);
        }
        Ok(out)
    }

    fn get(&self, channel_id: ChannelId) -> Result<Option<ChannelState>, StoreError> {
        let key: [u8; 32] = channel_id.into();
        let read_txn = self
            .db
            .begin_read()
            .map_err(|err| StoreError::Backend(format!("begin_read: {err}")))?;
        let table = match read_txn.open_table(CHANNEL_TABLE) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(err) => return Err(StoreError::Backend(format!("open_table: {err}"))),
        };
        let Some(value_guard) = table
            .get(&key)
            .map_err(|err| StoreError::Backend(format!("get: {err}")))?
        else {
            return Ok(None);
        };
        Ok(Some(decode_record(key, value_guard.value())?))
    }

    fn record(&self, state: &ChannelState) -> Result<(), StoreError> {
        let mut encoded = postcard::to_allocvec(&StoredChannelState::from(state))
            .map_err(|err| StoreError::Codec(format!("postcard encode failed: {err}")))?;
        // Schema v2 (#327): append a single [`TrailerV2`] segment after the v1
        // prefix (signature then expiry). Byte-identical to the previous
        // two-segment layout (#751) — see [`TrailerV2`]. The signature is
        // length-prefixed, so an absent one is still a single `0x00` byte.
        let trailer = TrailerV2 {
            // In-memory `Option<[u8; 65]>` → on-disk length-prefixed `Vec<u8>`
            // (`None` → empty, the v1/no-voucher-yet shape).
            signature: state.last_signature().map_or_else(Vec::new, |s| s.to_vec()),
            expires_at: state.expires_at,
        };
        let trailer_encoded = postcard::to_allocvec(&trailer)
            .map_err(|err| StoreError::Codec(format!("postcard encode of v2 trailer: {err}")))?;
        encoded.extend_from_slice(&trailer_encoded);
        // Cooperative-close waiver flag (ADR 003 §Cooperative close): a trailing
        // `bool` segment after the v2 trailer. Appended rather than folded into
        // `TrailerV2` (which would break decode of existing two-field records)
        // and rather than bumping the schema version — `decode_record` defaults
        // it to `false` when the bytes are absent, so old records decode
        // unchanged and an old binary safely ignores the extra byte (additive
        // forward-compat, the same posture as the v1→v2 trailer append).
        let coop_encoded =
            postcard::to_allocvec(&state.cooperative_close_signed()).map_err(|err| {
                StoreError::Codec(format!("postcard encode of coop-close flag: {err}"))
            })?;
        encoded.extend_from_slice(&coop_encoded);
        let key: [u8; 32] = state.channel_id.into();

        let mut write_txn = self
            .db
            .begin_write()
            .map_err(|err| StoreError::Backend(format!("begin_write: {err}")))?;
        // Force fsync-on-commit. This is redb's default but pinned here so
        // a future default change cannot silently weaken the issue #527
        // guarantee — Durability::None would batch commits and re-open the
        // replay window after a crash.
        write_txn
            .set_durability(Durability::Immediate)
            .map_err(|err| StoreError::Backend(format!("set_durability: {err}")))?;
        {
            let mut table = write_txn
                .open_table(CHANNEL_TABLE)
                .map_err(|err| StoreError::Backend(format!("open_table: {err}")))?;
            table
                .insert(&key, encoded.as_slice())
                .map_err(|err| StoreError::Backend(format!("insert: {err}")))?;
        }
        write_txn
            .commit()
            .map_err(|err| StoreError::Backend(format!("commit (fsync): {err}")))?;
        Ok(())
    }

    fn forget(&self, channel_id: ChannelId) -> Result<(), StoreError> {
        let key: [u8; 32] = channel_id.into();

        // Check first whether the table has ever been created. forget on a
        // never-written store is a no-op by contract and must not create
        // the table as a side effect (which `WriteTransaction::open_table`
        // would do implicitly).
        {
            let read_txn = self
                .db
                .begin_read()
                .map_err(|err| StoreError::Backend(format!("begin_read: {err}")))?;
            match read_txn.open_table(CHANNEL_TABLE) {
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
                .open_table(CHANNEL_TABLE)
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

// ---------------------------------------------------------------------------
// Buyer-side store (#744)
// ---------------------------------------------------------------------------

/// On-disk buyer-channel record. Like [`StoredChannelState`], all numeric
/// fields are fixed-size big-endian byte arrays so the encoded width is stable
/// across postcard versions. Unlike the seller record there is no legacy v1
/// schema to stay byte-compatible with, so `expires_at` is an inline field
/// rather than a trailing segment; `schema_version` still lives in the value
/// so a future additive field can ship without renaming the table (decode uses
/// [`postcard::take_from_bytes`], tolerating trailing bytes).
#[derive(Debug, Serialize, Deserialize)]
struct StoredBuyerChannelState {
    schema_version: u32,
    channel_id: [u8; 32],
    provider: [u8; 20],
    token: [u8; 20],
    deposit: [u8; 32],
    last_amount: [u8; 32],
    last_nonce: [u8; 32],
    last_bytes_delivered: [u8; 32],
    expires_at: u64,
}

impl From<&BuyerChannelState> for StoredBuyerChannelState {
    fn from(state: &BuyerChannelState) -> Self {
        Self {
            schema_version: BUYER_SUPPORTED_SCHEMA_VERSION,
            channel_id: state.channel_id.into(),
            provider: state.provider.into(),
            token: state.token.into(),
            deposit: state.deposit.to_be_bytes(),
            last_amount: state.last_amount.to_be_bytes(),
            last_nonce: state.last_nonce.to_be_bytes(),
            last_bytes_delivered: state.last_bytes_delivered.to_be_bytes(),
            expires_at: state.expires_at,
        }
    }
}

impl StoredBuyerChannelState {
    fn into_state(self) -> Result<BuyerChannelState, StoreError> {
        if self.schema_version > BUYER_SUPPORTED_SCHEMA_VERSION {
            return Err(StoreError::UnsupportedSchema {
                found: self.schema_version,
                supported: BUYER_SUPPORTED_SCHEMA_VERSION,
            });
        }
        Ok(BuyerChannelState {
            channel_id: B256::from(self.channel_id),
            provider: Address::from(self.provider),
            token: Address::from(self.token),
            deposit: U256::from_be_bytes(self.deposit),
            last_amount: U256::from_be_bytes(self.last_amount),
            last_nonce: U256::from_be_bytes(self.last_nonce),
            last_bytes_delivered: U256::from_be_bytes(self.last_bytes_delivered),
            expires_at: self.expires_at,
        })
    }
}

/// Decode one buyer record into a [`BuyerChannelState`], validating that the
/// embedded `provider` matches the table key. Mirrors [`decode_record`] for
/// the seller table (additive-forward-compat via `take_from_bytes`; bounded
/// trailing-bytes warning).
fn decode_buyer_record(
    key_bytes: [u8; 20],
    value_bytes: &[u8],
) -> Result<BuyerChannelState, StoreError> {
    let provider = Address::from(key_bytes);
    let (stored, remainder): (StoredBuyerChannelState, &[u8]) =
        postcard::take_from_bytes(value_bytes).map_err(|err| StoreError::Corrupt {
            channel_id: None,
            detail: format!("buyer record postcard decode failed (provider {provider}): {err}"),
        })?;
    if stored.provider != key_bytes {
        return Err(StoreError::Corrupt {
            channel_id: None,
            detail: format!("buyer record provider {provider} does not match table key"),
        });
    }
    if remainder.len() > SANE_TRAILER_MAX_BYTES {
        tracing::warn!(
            %provider,
            remainder = remainder.len(),
            limit = SANE_TRAILER_MAX_BYTES,
            event = "buyer_channel_store_excess_trailer",
            "buyer channel record has unusually large trailing bytes; possible malicious padding or large-additive-field schema skew",
        );
    }
    stored.into_state()
}

/// Buyer-table operations as **inherent** methods on the shared store. They
/// are named `buyer_*` (rather than implementing [`BuyerChannelStore`] on
/// `PersistentChannelStateStore` directly) so they don't collide with the
/// `ChannelStateStore` trait methods of the same base name (`load_all`,
/// `record`, `forget`) — which would make every concrete-type call site
/// ambiguous. The [`BuyerChannelStoreHandle`] newtype below adapts these into
/// the trait object the buyer service consumes.
impl PersistentChannelStateStore {
    fn buyer_load_all(&self) -> Result<Vec<BuyerChannelState>, StoreError> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|err| StoreError::Backend(format!("begin_read: {err}")))?;
        let table = match read_txn.open_table(BUYER_CHANNEL_TABLE) {
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
            let key_bytes: [u8; 20] = *key_guard.value();
            // Skip (don't fail the whole load) on a single undecodable record.
            // Unlike the seller `load_all` — whose error aborts startup (a
            // corrupt voucher record reopening the #527 replay window is unsafe
            // to run past) — buyer bootstrap is *non-fatal*: a propagated error
            // here would not just disable new buys, it would stop the reclaim
            // sweep from ever spawning, stranding every *other* tracked
            // channel's deposit as unreclaimable (PR #753 review, alpergundogdu).
            // One bad row must not take the others down. Logged at `error!`, not
            // `warn!`: the skipped row's deposit stays escrowed-but-unreclaimable
            // until an operator repairs the record, so it warrants action (and
            // must not be filtered out of alerting). The channel id can't be
            // named — it lives inside the undecodable bytes — so the on-chain
            // provider key is the only handle the operator gets.
            match decode_buyer_record(key_bytes, value_guard.value()) {
                Ok(state) => out.push(state),
                Err(err) => tracing::error!(
                    provider = %Address::from(key_bytes),
                    %err,
                    event = "buyer_channel_store_skip_undecodable_record",
                    "buyer channel hydration: skipping an undecodable record; its escrowed deposit \
                     is untracked and will not be auto-reclaimed until the record is repaired \
                     (other channels remain healthy)",
                ),
            }
        }
        Ok(out)
    }

    fn buyer_record(&self, state: &BuyerChannelState) -> Result<(), StoreError> {
        let encoded = postcard::to_allocvec(&StoredBuyerChannelState::from(state))
            .map_err(|err| StoreError::Codec(format!("buyer record postcard encode: {err}")))?;
        let key: [u8; 20] = state.provider.into();

        let mut write_txn = self
            .db
            .begin_write()
            .map_err(|err| StoreError::Backend(format!("begin_write: {err}")))?;
        write_txn
            .set_durability(Durability::Immediate)
            .map_err(|err| StoreError::Backend(format!("set_durability: {err}")))?;
        {
            let mut table = write_txn
                .open_table(BUYER_CHANNEL_TABLE)
                .map_err(|err| StoreError::Backend(format!("open_table: {err}")))?;
            table
                .insert(&key, encoded.as_slice())
                .map_err(|err| StoreError::Backend(format!("insert: {err}")))?;
        }
        write_txn
            .commit()
            .map_err(|err| StoreError::Backend(format!("commit (fsync): {err}")))?;
        Ok(())
    }

    /// Test/e2e-only: write raw value bytes under a buyer provider key, bypassing
    /// the postcard encoder, to simulate a row left undecodable by a binary
    /// downgrade. The buyer reconciliation + mixed-reclaim e2e (#763) lives in a
    /// separate integration-test file and cannot reach the private buyer table;
    /// this is the seam it uses to seed corruption against a live store.
    #[cfg(any(test, feature = "anvil-e2e"))]
    pub fn insert_raw_buyer_record(
        &self,
        provider: Address,
        bytes: &[u8],
    ) -> Result<(), StoreError> {
        let key: [u8; 20] = provider.into();
        let mut write_txn = self
            .db
            .begin_write()
            .map_err(|err| StoreError::Backend(format!("begin_write: {err}")))?;
        write_txn
            .set_durability(Durability::Immediate)
            .map_err(|err| StoreError::Backend(format!("set_durability: {err}")))?;
        {
            let mut table = write_txn
                .open_table(BUYER_CHANNEL_TABLE)
                .map_err(|err| StoreError::Backend(format!("open_table: {err}")))?;
            table
                .insert(&key, bytes)
                .map_err(|err| StoreError::Backend(format!("insert: {err}")))?;
        }
        write_txn
            .commit()
            .map_err(|err| StoreError::Backend(format!("commit (fsync): {err}")))?;
        Ok(())
    }

    fn buyer_forget(&self, provider: Address) -> Result<(), StoreError> {
        let key: [u8; 20] = provider.into();

        // Do not implicitly create the table on a never-written store.
        {
            let read_txn = self
                .db
                .begin_read()
                .map_err(|err| StoreError::Backend(format!("begin_read: {err}")))?;
            match read_txn.open_table(BUYER_CHANNEL_TABLE) {
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
                .open_table(BUYER_CHANNEL_TABLE)
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

    fn buyer_get_by_provider(
        &self,
        provider: Address,
    ) -> Result<Option<BuyerChannelState>, StoreError> {
        let key: [u8; 20] = provider.into();
        let read_txn = self
            .db
            .begin_read()
            .map_err(|err| StoreError::Backend(format!("begin_read: {err}")))?;
        let table = match read_txn.open_table(BUYER_CHANNEL_TABLE) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(err) => return Err(StoreError::Backend(format!("open_table: {err}"))),
        };
        let Some(value_guard) = table
            .get(&key)
            .map_err(|err| StoreError::Backend(format!("get: {err}")))?
        else {
            return Ok(None);
        };
        Ok(Some(decode_buyer_record(key, value_guard.value())?))
    }

    /// Compare-and-delete inside a single write transaction: remove
    /// `provider`'s row only if the stored `channel_id` matches. Returns
    /// whether a row was deleted.
    fn buyer_forget_if_channel(
        &self,
        provider: Address,
        channel_id: ChannelId,
    ) -> Result<bool, StoreError> {
        let key: [u8; 20] = provider.into();

        // Do not implicitly create the table on a never-written store
        // (`WriteTransaction::open_table` would).
        {
            let read_txn = self
                .db
                .begin_read()
                .map_err(|err| StoreError::Backend(format!("begin_read: {err}")))?;
            match read_txn.open_table(BUYER_CHANNEL_TABLE) {
                Ok(_) => {}
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(false),
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
        let deleted = {
            let mut table = write_txn
                .open_table(BUYER_CHANNEL_TABLE)
                .map_err(|err| StoreError::Backend(format!("open_table: {err}")))?;
            // Read the current row inside the same (serialised) write txn so the
            // match-and-remove is atomic against a concurrent replace.
            let matches = match table
                .get(&key)
                .map_err(|err| StoreError::Backend(format!("get: {err}")))?
            {
                Some(value_guard) => {
                    decode_buyer_record(key, value_guard.value())?.channel_id == channel_id
                }
                None => false,
            };
            if matches {
                table
                    .remove(&key)
                    .map_err(|err| StoreError::Backend(format!("remove: {err}")))?;
            }
            matches
        };
        write_txn
            .commit()
            .map_err(|err| StoreError::Backend(format!("commit (fsync): {err}")))?;
        Ok(deleted)
    }

    /// Atomically advance the committed progress for `provider`'s channel
    /// inside a single write transaction (read → channel-id guard → advance
    /// against the committed watermark → write). See
    /// [`BuyerChannelStore::advance_progress`].
    fn buyer_advance_progress(
        &self,
        provider: Address,
        channel_id: ChannelId,
        nonce: U256,
        bytes_delivered: U256,
        amount: U256,
    ) -> Result<AdvanceOutcome, StoreError> {
        let key: [u8; 20] = provider.into();

        // Do not implicitly create the table on a never-written store
        // (`WriteTransaction::open_table` would).
        {
            let read_txn = self
                .db
                .begin_read()
                .map_err(|err| StoreError::Backend(format!("begin_read: {err}")))?;
            match read_txn.open_table(BUYER_CHANNEL_TABLE) {
                Ok(_) => {}
                Err(redb::TableError::TableDoesNotExist(_)) => {
                    return Ok(AdvanceOutcome::UnknownProvider);
                }
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
                .open_table(BUYER_CHANNEL_TABLE)
                .map_err(|err| StoreError::Backend(format!("open_table: {err}")))?;
            // Read the committed row inside the same (serialised) write txn so
            // the advance is checked against — and written over — the committed
            // watermark, never a stale in-memory snapshot.
            let Some(value_guard) = table
                .get(&key)
                .map_err(|err| StoreError::Backend(format!("get: {err}")))?
            else {
                // No-write outcomes return early so the uncommitted write txn is
                // aborted on drop — avoiding a pointless `Durability::Immediate`
                // fsync on a transaction that changed nothing.
                return Ok(AdvanceOutcome::UnknownProvider);
            };
            let mut state = decode_buyer_record(key, value_guard.value())?;
            // Drop the borrow of `table` held by `value_guard` before mutating.
            drop(value_guard);
            if state.channel_id != channel_id {
                return Ok(AdvanceOutcome::ChannelMismatch);
            }
            if let Err(err) = state.advance(nonce, bytes_delivered, amount) {
                return Ok(AdvanceOutcome::Regressed(err));
            }
            let encoded = postcard::to_allocvec(&StoredBuyerChannelState::from(&state))
                .map_err(|err| StoreError::Codec(format!("buyer record postcard encode: {err}")))?;
            table
                .insert(&key, encoded.as_slice())
                .map_err(|err| StoreError::Backend(format!("insert: {err}")))?;
        }
        write_txn
            .commit()
            .map_err(|err| StoreError::Backend(format!("commit (fsync): {err}")))?;
        Ok(AdvanceOutcome::Advanced)
    }

    /// Atomically add `additional` to the committed deposit for `provider`'s
    /// channel inside a single write transaction. See
    /// [`BuyerChannelStore::add_deposit`].
    fn buyer_add_deposit(
        &self,
        provider: Address,
        channel_id: ChannelId,
        additional: U256,
    ) -> Result<DepositOutcome, StoreError> {
        let key: [u8; 20] = provider.into();

        // Do not implicitly create the table on a never-written store.
        {
            let read_txn = self
                .db
                .begin_read()
                .map_err(|err| StoreError::Backend(format!("begin_read: {err}")))?;
            match read_txn.open_table(BUYER_CHANNEL_TABLE) {
                Ok(_) => {}
                Err(redb::TableError::TableDoesNotExist(_)) => {
                    return Ok(DepositOutcome::UnknownProvider);
                }
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
        let new_deposit = {
            let mut table = write_txn
                .open_table(BUYER_CHANNEL_TABLE)
                .map_err(|err| StoreError::Backend(format!("open_table: {err}")))?;
            let Some(value_guard) = table
                .get(&key)
                .map_err(|err| StoreError::Backend(format!("get: {err}")))?
            else {
                // No-write outcome returns early so the uncommitted write txn is
                // aborted on drop — no pointless `Durability::Immediate` fsync.
                return Ok(DepositOutcome::UnknownProvider);
            };
            let mut state = decode_buyer_record(key, value_guard.value())?;
            drop(value_guard);
            if state.channel_id != channel_id {
                return Ok(DepositOutcome::ChannelMismatch);
            }
            state.deposit = state.deposit.saturating_add(additional);
            let encoded = postcard::to_allocvec(&StoredBuyerChannelState::from(&state))
                .map_err(|err| StoreError::Codec(format!("buyer record postcard encode: {err}")))?;
            table
                .insert(&key, encoded.as_slice())
                .map_err(|err| StoreError::Backend(format!("insert: {err}")))?;
            state.deposit
        };
        write_txn
            .commit()
            .map_err(|err| StoreError::Backend(format!("commit (fsync): {err}")))?;
        Ok(DepositOutcome::Added(new_deposit))
    }
}

/// [`BuyerChannelStore`] adapter over the shared [`PersistentChannelStateStore`].
///
/// Holds an `Arc` to the same store the seller path uses, so both the seller
/// `channel_state_v1` table and the buyer `buyer_channel_state_v1` table live
/// in one redb file behind one handle. Hand this to the buyer service as
/// `Arc<dyn BuyerChannelStore>`.
#[derive(Debug, Clone)]
pub struct BuyerChannelStoreHandle {
    inner: std::sync::Arc<PersistentChannelStateStore>,
}

impl BuyerChannelStoreHandle {
    /// Wrap a shared persistent store as a buyer-channel store.
    #[must_use]
    pub const fn new(inner: std::sync::Arc<PersistentChannelStateStore>) -> Self {
        Self { inner }
    }
}

impl BuyerChannelStore for BuyerChannelStoreHandle {
    fn load_all(&self) -> Result<Vec<BuyerChannelState>, StoreError> {
        self.inner.buyer_load_all()
    }

    fn record(&self, state: &BuyerChannelState) -> Result<(), StoreError> {
        self.inner.buyer_record(state)
    }

    fn forget(&self, provider: Address) -> Result<(), StoreError> {
        self.inner.buyer_forget(provider)
    }

    fn forget_if_channel(
        &self,
        provider: Address,
        channel_id: ChannelId,
    ) -> Result<bool, StoreError> {
        self.inner.buyer_forget_if_channel(provider, channel_id)
    }

    fn get_by_provider(&self, provider: Address) -> Result<Option<BuyerChannelState>, StoreError> {
        self.inner.buyer_get_by_provider(provider)
    }

    fn advance_progress(
        &self,
        provider: Address,
        channel_id: ChannelId,
        nonce: U256,
        bytes_delivered: U256,
        amount: U256,
    ) -> Result<AdvanceOutcome, StoreError> {
        self.inner
            .buyer_advance_progress(provider, channel_id, nonce, bytes_delivered, amount)
    }

    fn add_deposit(
        &self,
        provider: Address,
        channel_id: ChannelId,
        additional: U256,
    ) -> Result<DepositOutcome, StoreError> {
        self.inner
            .buyer_add_deposit(provider, channel_id, additional)
    }
}

impl PersistentChannelStateStore {
    /// Insert/overwrite a pending-settle entry in `table` (fsync-on-commit). The
    /// `PENDING_SETTLE_TABLE` and `BUYER_PENDING_SETTLE_TABLE` share this body so
    /// the seller and buyer pending sets stay byte-for-byte consistent.
    fn pending_record_in(
        &self,
        table_def: TableDefinition<&[u8; 32], u64>,
        entry: &PendingSettle,
    ) -> Result<(), StoreError> {
        let key: [u8; 32] = entry.channel_id.into();
        let mut write_txn = self
            .db
            .begin_write()
            .map_err(|err| StoreError::Backend(format!("begin_write: {err}")))?;
        // Force fsync-on-commit, same durability discipline as `record`: the
        // channel is already `Closing` on-chain by the time an entry is written
        // here, so a post-close crash that lost it would strand the settlement
        // obligation until someone calls `settleChannel` (the channel is no
        // longer `Open`, so the expiry-reclaim path does not recover it) — the
        // very gap this fsync guards. Shared by the seller and buyer tables.
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
                channel_id: B256::from(*key_guard.value()),
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
        channel_id: ChannelId,
    ) -> Result<(), StoreError> {
        let key: [u8; 32] = channel_id.into();
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

impl PendingSettleStore for PersistentChannelStateStore {
    fn record_pending(&self, entry: &PendingSettle) -> Result<(), StoreError> {
        self.pending_record_in(PENDING_SETTLE_TABLE, entry)
    }

    fn load_pending(&self) -> Result<Vec<PendingSettle>, StoreError> {
        self.pending_load_from(PENDING_SETTLE_TABLE)
    }

    fn forget_pending(&self, channel_id: ChannelId) -> Result<(), StoreError> {
        self.pending_forget_in(PENDING_SETTLE_TABLE, channel_id)
    }
}

/// [`PendingSettleStore`] over the buyer's `buyer_pending_settle_v1` table,
/// isolated from the seller's `pending_settle_v1` table so the two settle
/// sweeps never finalize each other's closes (#988). Wraps the same shared
/// [`PersistentChannelStateStore`] (one redb file, one handle); hand this to the
/// buyer service as `Arc<dyn PendingSettleStore>`.
#[derive(Debug, Clone)]
pub struct BuyerPendingSettleStoreHandle {
    inner: std::sync::Arc<PersistentChannelStateStore>,
}

impl BuyerPendingSettleStoreHandle {
    /// Wrap a shared persistent store as the buyer pending-settle store.
    #[must_use]
    pub const fn new(inner: std::sync::Arc<PersistentChannelStateStore>) -> Self {
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

    fn forget_pending(&self, channel_id: ChannelId) -> Result<(), StoreError> {
        self.inner
            .pending_forget_in(BUYER_PENDING_SETTLE_TABLE, channel_id)
    }
}

impl WatcherCheckpointStore for PersistentChannelStateStore {
    fn load_last_seen_block(&self) -> Result<Option<u64>, StoreError> {
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
            .get(WATCHER_CHECKPOINT_KEY)
            .map_err(|err| StoreError::Backend(format!("get: {err}")))?;
        Ok(value.map(|g| g.value()))
    }

    fn record_last_seen_block(&self, block: u64) -> Result<(), StoreError> {
        let mut write_txn = self
            .db
            .begin_write()
            .map_err(|err| StoreError::Backend(format!("begin_write: {err}")))?;
        // Force fsync-on-commit, same durability discipline as the other tables.
        // Each call here fsyncs. To avoid one fsync per distinct block carrying a
        // provider-owned `ChannelOpened` (a backlog drain after a resubscribe, and
        // steady state on a high-fan-out provider), the runtime wraps this store in
        // `payment_settlement::DebouncedCheckpointStore` (#784), which coarsens the
        // write cadence and forces a final flush on graceful shutdown. That is
        // safe because the resume backfill rounds this checkpoint down by
        // `REORG_MARGIN_BLOCKS` and `register_open_channel` is idempotent, so a
        // lagging floor only ever widens the next rescan. This impl stays durable
        // per-call so the floor the debouncer *does* forward is crash-safe.
        write_txn
            .set_durability(Durability::Immediate)
            .map_err(|err| StoreError::Backend(format!("set_durability: {err}")))?;
        {
            let mut table = write_txn
                .open_table(WATCHER_CHECKPOINT_TABLE)
                .map_err(|err| StoreError::Backend(format!("open_table: {err}")))?;
            table
                .insert(WATCHER_CHECKPOINT_KEY, block)
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

    fn sample(byte: u8) -> ChannelState {
        let mut id = [0u8; 32];
        id[31] = byte;
        ChannelState::hydrate(
            id.into(),
            address!("00000000000000000000000000000000000000aa"),
            address!("a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"),
            U256::from(10_000_000u64),
            U256::from(byte) * U256::from(1_000u64),
            U256::from(byte),
            U256::from(byte) * U256::from(1_024u64),
            Some([byte; 65]),
            1_900_000_000 + u64::from(byte),
            false,
        )
    }

    #[test]
    fn open_empty_store_returns_no_entries() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let store = PersistentChannelStateStore::open(dir.path())?;
        anyhow::ensure!(store.load_all()?.is_empty());
        Ok(())
    }

    #[test]
    fn record_then_load_round_trip() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let store = PersistentChannelStateStore::open(dir.path())?;
        let a = sample(1);
        let b = sample(2);
        store.record(&a)?;
        store.record(&b)?;
        let mut all = store.load_all()?;
        all.sort_by_key(|s| s.channel_id);
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
            let store = PersistentChannelStateStore::open(dir.path())?;
            store.record(&s)?;
        }
        let store = PersistentChannelStateStore::open(dir.path())?;
        let all = store.load_all()?;
        anyhow::ensure!(all.len() == 1);
        let only = all.first().ok_or_else(|| anyhow::anyhow!("missing [0]"))?;
        anyhow::ensure!(*only == s);
        Ok(())
    }

    #[test]
    fn forget_removes_entry() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let store = PersistentChannelStateStore::open(dir.path())?;
        let s = sample(3);
        store.record(&s)?;
        store.forget(s.channel_id)?;
        anyhow::ensure!(store.load_all()?.is_empty());
        // forget on a never-recorded channel is a no-op
        store.forget(b256!(
            "1111111111111111111111111111111111111111111111111111111111111111"
        ))?;
        Ok(())
    }

    #[test]
    fn get_round_trips_and_reports_unknown() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let store = PersistentChannelStateStore::open(dir.path())?;
        // get on a never-written store (no table yet) is None, not an error.
        anyhow::ensure!(store.get(sample(1).channel_id)?.is_none());

        let s = sample(9);
        store.record(&s)?;
        let got = store
            .get(s.channel_id)?
            .ok_or_else(|| anyhow::anyhow!("expected Some"))?;
        anyhow::ensure!(got == s, "get must round-trip incl. signature");
        anyhow::ensure!(
            store
                .get(b256!(
                    "2222222222222222222222222222222222222222222222222222222222222222"
                ))?
                .is_none(),
            "unknown channel -> None"
        );
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn file_mode_is_owner_only() -> anyhow::Result<()> {
        use std::os::unix::fs::PermissionsExt;
        let dir = data_dir()?;
        let store = PersistentChannelStateStore::open(dir.path())?;
        let mode = std::fs::metadata(store.path())?.permissions().mode() & 0o777;
        anyhow::ensure!(
            mode == DB_FILE_MODE,
            "file mode {mode:o} != expected {DB_FILE_MODE:o}"
        );
        Ok(())
    }

    /// **Idempotent-chmod regression.** When the file is already at
    /// `DB_FILE_MODE` from a prior successful open, `tighten_permissions`
    /// MUST skip the `set_permissions` syscall so a read-only mount where
    /// the mode is already correct does not turn into a permanent
    /// fault loop. We exercise the skip by setting the mode explicitly
    /// then asserting `tighten_permissions` returns `Ok` without changing
    /// anything observable.
    #[cfg(unix)]
    #[test]
    fn idempotent_chmod_skips_syscall_on_matching_mode() -> anyhow::Result<()> {
        use std::os::unix::fs::PermissionsExt;
        let dir = data_dir()?;
        let store = PersistentChannelStateStore::open(dir.path())?;
        let path = store.path().to_path_buf();
        // First call set the mode to 0o600; second call should be a no-op.
        let mode_before = std::fs::metadata(&path)?.permissions().mode() & 0o777;
        anyhow::ensure!(mode_before == DB_FILE_MODE, "precondition: mode is 0o600");
        // Calling tighten_permissions again must succeed without issuing
        // a real set_permissions (we can't observe the syscall directly,
        // but the mtime should not bump — using metadata stat as the
        // observable proxy).
        PersistentChannelStateStore::tighten_permissions(&path)?;
        let mode_after = std::fs::metadata(&path)?.permissions().mode() & 0o777;
        anyhow::ensure!(mode_after == DB_FILE_MODE, "mode unchanged");
        Ok(())
    }

    /// **Cleanup-asymmetry regression (#527 follow-up).** On `tighten_permissions`
    /// failure for a FRESHLY-CREATED file, the cleanup branch MUST remove
    /// the partial file so the next start sees a clean state. Uses the
    /// `open_with` injection point to simulate a chmod failure.
    #[test]
    fn fresh_file_chmod_failure_removes_file() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let chmod_failed = std::io::Error::other("simulated chmod failure");
        let path_buf = dir.path().join(CHANNELS_DB_FILE);
        anyhow::ensure!(!path_buf.exists(), "precondition: file does not exist");

        let path_for_closure = path_buf.clone();
        let err = PersistentChannelStateStore::open_with(dir.path(), |_path| {
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
    /// failure for a PRE-EXISTING file, the cleanup branch MUST NOT remove
    /// the file — otherwise a transient chmod error on subsequent boot
    /// destroys live voucher state and reopens the replay window. Uses
    /// the `open_with` injection point.
    #[test]
    fn pre_existing_file_chmod_failure_preserves_file() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let recorded = sample(7);
        let recorded_id = recorded.channel_id;

        // First open succeeds and writes a record — simulates a healthy
        // node that has been up before.
        {
            let store = PersistentChannelStateStore::open(dir.path())?;
            store.record(&recorded)?;
        }

        let path_buf = dir.path().join(CHANNELS_DB_FILE);
        anyhow::ensure!(
            path_buf.exists(),
            "precondition: file exists with voucher state"
        );
        let size_before = std::fs::metadata(&path_buf)?.len();

        // Subsequent open with an injected chmod failure: file must NOT
        // be removed, voucher state must be intact, and the chmod error
        // must propagate.
        let path_for_closure = path_buf.clone();
        let err = PersistentChannelStateStore::open_with(dir.path(), move |_path| {
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

        // And the voucher state is still readable via a fresh open with
        // a healthy chmod_fn — proves the data is intact, not just that
        // the file exists at the right size.
        let recovered = PersistentChannelStateStore::open(dir.path())?;
        let all = recovered.load_all()?;
        anyhow::ensure!(all.len() == 1);
        let only = all.first().ok_or_else(|| anyhow::anyhow!("missing"))?;
        anyhow::ensure!(only.channel_id == recorded_id);
        anyhow::ensure!(*only == recorded);
        Ok(())
    }

    /// Write a record carrying a `schema_version` higher than this binary
    /// supports — `load_all` MUST refuse to decode it (per ADR 003:
    /// silently dropping unknown fields is unsafe because we cannot honour
    /// the persistence invariant for fields we don't understand).
    #[test]
    fn future_schema_version_refuses_to_load() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let store = PersistentChannelStateStore::open(dir.path())?;
        let s = sample(1);

        // Encode a record stamped with version SUPPORTED+1, write directly.
        let mut stored = StoredChannelState::from(&s);
        stored.schema_version = SUPPORTED_SCHEMA_VERSION + 1;
        let encoded = postcard::to_allocvec(&stored)?;
        let key: [u8; 32] = s.channel_id.into();
        let mut tx = store.db.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        {
            let mut t = tx.open_table(CHANNEL_TABLE)?;
            t.insert(&key, encoded.as_slice())?;
        }
        tx.commit()?;

        let err = store
            .load_all()
            .err()
            .ok_or_else(|| anyhow::anyhow!("future schema must reject"))?;
        anyhow::ensure!(
            matches!(
                err,
                StoreError::UnsupportedSchema { found, supported }
                    if found == SUPPORTED_SCHEMA_VERSION + 1
                        && supported == SUPPORTED_SCHEMA_VERSION,
            ),
            "expected UnsupportedSchema, got {err:?}",
        );
        Ok(())
    }

    /// **Forward-compat regression.** A newer writer adding an additive
    /// field appears to an older reader as trailing bytes after the known
    /// `StoredChannelState` prefix. With `postcard::take_from_bytes` those
    /// trailing bytes are ignored and the record decodes cleanly; with the
    /// previous `from_bytes` they would have triggered
    /// `DeserializeTrailingBytes` and the record would be misclassified
    /// as `Corrupt`. This test would have failed under the old
    /// implementation and is the regression guard against re-introducing
    /// strict decoding.
    #[test]
    fn extra_trailing_bytes_are_tolerated() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let store = PersistentChannelStateStore::open(dir.path())?;
        let s = sample(0x42);
        let key: [u8; 32] = s.channel_id.into();

        // Encode the real v2 record (prefix + signature + expiry segments),
        // then append plausible *further* additive-field bytes (simulating
        // what a future schema beyond v2 would write after the known segments).
        let mut encoded = postcard::to_allocvec(&StoredChannelState::from(&s))?;
        let sig_vec = s.last_signature().map_or_else(Vec::new, |x| x.to_vec());
        encoded.extend_from_slice(&postcard::to_allocvec(&sig_vec)?);
        encoded.extend_from_slice(&postcard::to_allocvec(&s.expires_at)?);
        // The cooperative-close flag is now a known trailing segment; the
        // "further additive-field bytes" a future schema would write come after
        // it, so encode a valid flag first, then the junk a newer writer appended.
        encoded.extend_from_slice(&postcard::to_allocvec(&s.cooperative_close_signed())?);
        encoded.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x01, 0x02, 0x03]);

        let mut tx = store.db.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        {
            let mut t = tx.open_table(CHANNEL_TABLE)?;
            t.insert(&key, encoded.as_slice())?;
        }
        tx.commit()?;

        let all = store.load_all()?;
        anyhow::ensure!(all.len() == 1, "expected one channel, got {}", all.len());
        let only = all.first().ok_or_else(|| anyhow::anyhow!("missing"))?;
        anyhow::ensure!(
            *only == s,
            "decoded prefix + signature must equal the original record despite trailing bytes",
        );
        Ok(())
    }

    #[test]
    fn cooperative_close_flag_persists_across_reopen() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let mut s = sample(0x55);
        anyhow::ensure!(!s.cooperative_close_signed(), "fixture starts unflagged");
        {
            let store = PersistentChannelStateStore::open(dir.path())?;
            store.record(&s)?;
            // Marking persists the flag (clone-record-swap) and flips it in memory.
            s.mark_cooperative_close_signed(&store)?;
            anyhow::ensure!(s.cooperative_close_signed(), "mark sets the in-memory flag");
        }
        // Re-open from disk so the value comes from a fresh decode, not memory.
        let store = PersistentChannelStateStore::open(dir.path())?;
        let loaded = store
            .get(s.channel_id)?
            .ok_or_else(|| anyhow::anyhow!("record missing after reopen"))?;
        anyhow::ensure!(
            loaded.cooperative_close_signed(),
            "cooperative-close flag must survive persist + reopen",
        );
        anyhow::ensure!(
            loaded == s,
            "the full record must round-trip with the flag set"
        );
        Ok(())
    }

    #[test]
    fn record_without_cooperative_close_segment_decodes_false() -> anyhow::Result<()> {
        // A record written before the flag existed (prefix + v2 trailer only,
        // no coop segment) must decode with the flag defaulted to `false` rather
        // than erroring — the additive forward-compat guarantee.
        let dir = data_dir()?;
        let store = PersistentChannelStateStore::open(dir.path())?;
        let s = sample(0x66);
        let key: [u8; 32] = s.channel_id.into();
        let mut encoded = postcard::to_allocvec(&StoredChannelState::from(&s))?;
        let sig_vec = s.last_signature().map_or_else(Vec::new, |x| x.to_vec());
        encoded.extend_from_slice(&postcard::to_allocvec(&sig_vec)?);
        encoded.extend_from_slice(&postcard::to_allocvec(&s.expires_at)?);
        // Deliberately stop here — no coop-flag segment.
        let mut tx = store.db.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        {
            let mut t = tx.open_table(CHANNEL_TABLE)?;
            t.insert(&key, encoded.as_slice())?;
        }
        tx.commit()?;
        let loaded = store
            .get(s.channel_id)?
            .ok_or_else(|| anyhow::anyhow!("missing"))?;
        anyhow::ensure!(
            !loaded.cooperative_close_signed(),
            "a record with no coop segment must decode the flag as false",
        );
        anyhow::ensure!(loaded == s, "the rest of the record must decode unchanged");
        Ok(())
    }

    /// **`TrailerV2` byte-identity (#751).** Bundling the signature + expiry into
    /// one `TrailerV2` struct MUST encode byte-for-byte identically to the
    /// previous "append `postcard(signature)` then `postcard(expires_at)`"
    /// layout — otherwise existing on-disk v2 records would fail to decode. Pins
    /// the field order so a future reorder is caught here, not on a live store.
    #[test]
    fn trailer_v2_encoding_matches_two_segment_layout() -> anyhow::Result<()> {
        for sig in [Vec::new(), vec![0u8; 65], vec![0xAB; 65]] {
            let expires_at = 1_900_000_123u64;
            // The legacy two-segment encoding.
            let mut legacy = postcard::to_allocvec(&sig)?;
            legacy.extend_from_slice(&postcard::to_allocvec(&expires_at)?);
            // The bundled-struct encoding.
            let bundled = postcard::to_allocvec(&TrailerV2 {
                signature: sig.clone(),
                expires_at,
            })?;
            anyhow::ensure!(
                legacy == bundled,
                "TrailerV2 encoding diverged from the two-segment layout for sig len {}",
                sig.len(),
            );
        }
        Ok(())
    }

    /// **Schema-v1 backward-compat (#327).** A record written by the
    /// pre-signature binary (`schema_version` 1, no trailing segments) MUST
    /// still load — hydrating with an empty signature and `0` expiry rather
    /// than failing the decode. The channel is then unredeemable until the
    /// next voucher re-records it at v2, which is safe.
    #[test]
    fn v1_record_hydrates_with_empty_signature() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let store = PersistentChannelStateStore::open(dir.path())?;
        // A v1 record carried no signature (None) nor an expiry (0).
        let base = sample(0x55);
        let s = ChannelState::hydrate(
            base.channel_id,
            base.client,
            base.token,
            base.deposit,
            base.last_amount(),
            base.last_nonce(),
            base.last_bytes_delivered(),
            None,
            0,
            false,
        );

        // Hand-write a v1-shaped record: prefix only, schema_version forced
        // to 1, NO trailing segments.
        let mut stored = StoredChannelState::from(&s);
        stored.schema_version = 1;
        let encoded = postcard::to_allocvec(&stored)?;
        let key: [u8; 32] = s.channel_id.into();
        let mut tx = store.db.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        {
            let mut t = tx.open_table(CHANNEL_TABLE)?;
            t.insert(&key, encoded.as_slice())?;
        }
        tx.commit()?;

        let all = store.load_all()?;
        anyhow::ensure!(all.len() == 1);
        let only = all.first().ok_or_else(|| anyhow::anyhow!("missing"))?;
        anyhow::ensure!(
            only.last_signature().is_none(),
            "v1 record → empty signature"
        );
        anyhow::ensure!(only.expires_at == 0, "v1 record → zero expiry");
        anyhow::ensure!(*only == s, "v1 prefix fields must round-trip");
        Ok(())
    }

    /// **Signature-length narrowing (#751).** The on-disk signature is a
    /// length-prefixed `Vec<u8>`, but the in-memory `ChannelState` carries the
    /// stronger `Option<[u8; 65]>`. A stored trailer whose signature is neither
    /// empty nor exactly 65 bytes MUST be rejected as `Corrupt` rather than
    /// silently truncated/padded into a value we'd submit to `closeChannel`.
    #[test]
    fn wrong_length_signature_trailer_is_corrupt() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let store = PersistentChannelStateStore::open(dir.path())?;
        let s = sample(0x77);
        let key: [u8; 32] = s.channel_id.into();

        // Hand-write a v2 record whose trailer signature is 64 bytes (one short).
        let mut encoded = postcard::to_allocvec(&StoredChannelState::from(&s))?;
        encoded.extend_from_slice(&postcard::to_allocvec(&TrailerV2 {
            signature: vec![0xCD; 64],
            expires_at: 1_900_000_000,
        })?);
        let mut tx = store.db.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        {
            let mut t = tx.open_table(CHANNEL_TABLE)?;
            t.insert(&key, encoded.as_slice())?;
        }
        tx.commit()?;

        let err = store
            .load_all()
            .err()
            .ok_or_else(|| anyhow::anyhow!("wrong-length signature must reject"))?;
        anyhow::ensure!(
            matches!(
                &err,
                StoreError::Corrupt { channel_id: Some(id), detail }
                    if *id == s.channel_id && detail.contains("expected 0 or 65"),
            ),
            "expected Corrupt(...expected 0 or 65...), got {err:?}",
        );
        Ok(())
    }

    /// The watcher scan checkpoint (#751) round-trips, overwrites in place, and
    /// survives a reopen — so the next boot resumes the `ChannelOpened` backfill
    /// from the persisted block. An empty table reads as `None` (first boot).
    #[test]
    fn watcher_checkpoint_round_trip_and_persist() -> anyhow::Result<()> {
        let dir = data_dir()?;
        {
            let store = PersistentChannelStateStore::open(dir.path())?;
            // Never-written table → None (first-ever boot).
            anyhow::ensure!(store.load_last_seen_block()?.is_none());
            store.record_last_seen_block(1_000)?;
            anyhow::ensure!(store.load_last_seen_block()? == Some(1_000));
            // Overwrite advances the single key (no second row).
            store.record_last_seen_block(2_500)?;
            anyhow::ensure!(store.load_last_seen_block()? == Some(2_500));
        }
        // Survives a reopen.
        let store = PersistentChannelStateStore::open(dir.path())?;
        anyhow::ensure!(
            store.load_last_seen_block()? == Some(2_500),
            "checkpoint must survive a restart"
        );
        Ok(())
    }

    /// The checkpoint table shares the one redb file with the channel-state and
    /// pending-settle tables without colliding.
    #[test]
    fn watcher_checkpoint_is_independent_of_other_tables() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let store = PersistentChannelStateStore::open(dir.path())?;
        let s = sample(7);
        store.record(&s)?;
        store.record_pending(&PendingSettle {
            channel_id: s.channel_id,
            settle_after: 99,
        })?;
        store.record_last_seen_block(4_242)?;
        anyhow::ensure!(store.load_all()?.len() == 1);
        anyhow::ensure!(store.load_pending()?.len() == 1);
        anyhow::ensure!(store.load_last_seen_block()? == Some(4_242));
        Ok(())
    }

    /// Pending-settle entries round-trip and survive a reopen, and the
    /// dispute-deadline value is re-stamped on a re-close (#327 / PR #743).
    #[test]
    fn pending_settle_round_trip_and_persist() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let a = PendingSettle {
            channel_id: sample(1).channel_id,
            settle_after: 1_700_000_000,
        };
        let b = PendingSettle {
            channel_id: sample(2).channel_id,
            settle_after: 1_700_000_500,
        };
        {
            let store = PersistentChannelStateStore::open(dir.path())?;
            store.record_pending(&a)?;
            store.record_pending(&b)?;
            // Re-close re-stamps the same channel's deadline (no extra row).
            store.record_pending(&PendingSettle {
                settle_after: 1_700_009_999,
                ..a
            })?;
        }
        let store = PersistentChannelStateStore::open(dir.path())?;
        let mut all = store.load_pending()?;
        all.sort_by_key(|p| p.channel_id);
        anyhow::ensure!(all.len() == 2, "overwrite must not add a row");
        let first = all.first().ok_or_else(|| anyhow::anyhow!("missing [0]"))?;
        anyhow::ensure!(first.settle_after == 1_700_009_999, "deadline re-stamped");
        Ok(())
    }

    #[test]
    fn pending_settle_forget_and_empty_tolerance() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let store = PersistentChannelStateStore::open(dir.path())?;
        // load/forget on a never-written pending table are no-ops, not errors.
        anyhow::ensure!(store.load_pending()?.is_empty());
        store.forget_pending(sample(9).channel_id)?;

        let entry = PendingSettle {
            channel_id: sample(3).channel_id,
            settle_after: 42,
        };
        store.record_pending(&entry)?;
        store.forget_pending(entry.channel_id)?;
        anyhow::ensure!(store.load_pending()?.is_empty());
        Ok(())
    }

    /// The pending-settle table and the voucher-state table share one
    /// database file without colliding — a channel can carry voucher state
    /// and a pending-settle entry simultaneously (it does, briefly, between
    /// close and forget).
    #[test]
    fn pending_settle_table_is_independent_of_channel_state() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let store = PersistentChannelStateStore::open(dir.path())?;
        let s = sample(7);
        store.record(&s)?;
        store.record_pending(&PendingSettle {
            channel_id: s.channel_id,
            settle_after: 99,
        })?;
        anyhow::ensure!(store.load_all()?.len() == 1);
        anyhow::ensure!(store.load_pending()?.len() == 1);
        // Forgetting the voucher state leaves the pending entry intact.
        store.forget(s.channel_id)?;
        anyhow::ensure!(store.load_all()?.is_empty());
        anyhow::ensure!(store.load_pending()?.len() == 1, "pending row survives");
        Ok(())
    }

    /// Write garbage bytes as the value for a real key — `load_all` MUST
    /// fail with `StoreError::Corrupt` naming the affected channel id.
    /// Guards the postcard-decode error path which is otherwise dead in
    /// the test suite (the happy path always round-trips cleanly).
    #[test]
    fn corrupt_value_bytes_rejected() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let store = PersistentChannelStateStore::open(dir.path())?;
        let s = sample(2);
        let key: [u8; 32] = s.channel_id.into();
        let mut tx = store.db.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        {
            let mut t = tx.open_table(CHANNEL_TABLE)?;
            // 16 zero bytes is far too short for a valid postcard-encoded
            // `StoredChannelState` (which has ~150+ bytes of fixed fields).
            t.insert(&key, &[0u8; 16][..])?;
        }
        tx.commit()?;

        let err = store
            .load_all()
            .err()
            .ok_or_else(|| anyhow::anyhow!("garbage value must reject"))?;
        anyhow::ensure!(
            matches!(
                &err,
                StoreError::Corrupt {
                    channel_id: Some(id),
                    detail
                } if *id == s.channel_id && detail.contains("postcard decode"),
            ),
            "expected Corrupt {{ channel_id: Some(...), detail: ...postcard decode... }}, got {err:?}",
        );
        Ok(())
    }

    /// Write a record whose embedded `channel_id` doesn't match its table
    /// key — `load_all` MUST detect the mismatch and return `Corrupt`.
    /// This catches a future bug where the encode path builds the record
    /// for one channel but writes it under another channel's key.
    #[test]
    fn key_value_channel_id_mismatch_rejected() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let store = PersistentChannelStateStore::open(dir.path())?;
        let s_for_a = sample(0xAA);
        let key_b = sample(0xBB).channel_id; // different key
        let encoded = postcard::to_allocvec(&StoredChannelState::from(&s_for_a))?;
        let key_b_bytes: [u8; 32] = key_b.into();
        let mut tx = store.db.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        {
            let mut t = tx.open_table(CHANNEL_TABLE)?;
            t.insert(&key_b_bytes, encoded.as_slice())?;
        }
        tx.commit()?;

        let err = store
            .load_all()
            .err()
            .ok_or_else(|| anyhow::anyhow!("key/value mismatch must reject"))?;
        anyhow::ensure!(
            matches!(
                &err,
                StoreError::Corrupt {
                    channel_id: Some(id),
                    detail
                } if *id == key_b && detail.contains("does not match table key"),
            ),
            "expected Corrupt {{ channel_id: Some(key_b), detail: ...does not match... }}, got {err:?}",
        );
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Buyer-table tests (#744)
    // -----------------------------------------------------------------------

    fn buyer_sample(byte: u8) -> BuyerChannelState {
        let mut id = [0u8; 32];
        id[31] = byte;
        let mut prov = [0u8; 20];
        prov[19] = byte;
        BuyerChannelState {
            channel_id: id.into(),
            provider: Address::from(prov),
            token: address!("a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"),
            deposit: U256::from(10_000_000u64),
            last_amount: U256::from(byte) * U256::from(1_000u64),
            last_nonce: U256::from(byte),
            last_bytes_delivered: U256::from(byte) * U256::from(1_024u64),
            expires_at: 1_900_000_000 + u64::from(byte),
        }
    }

    #[test]
    fn buyer_open_empty_store_returns_no_entries() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let store = PersistentChannelStateStore::open(dir.path())?;
        let handle = BuyerChannelStoreHandle::new(std::sync::Arc::new(store));
        anyhow::ensure!(handle.load_all()?.is_empty());
        anyhow::ensure!(handle.get_by_provider(buyer_sample(1).provider)?.is_none());
        Ok(())
    }

    #[test]
    fn buyer_record_get_and_persist_across_reopen() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let a = buyer_sample(1);
        let b = buyer_sample(2);
        {
            let handle = BuyerChannelStoreHandle::new(std::sync::Arc::new(
                PersistentChannelStateStore::open(dir.path())?,
            ));
            handle.record(&a)?;
            handle.record(&b)?;
            let got = handle
                .get_by_provider(a.provider)?
                .ok_or_else(|| anyhow::anyhow!("missing a"))?;
            anyhow::ensure!(got == a, "get_by_provider must round-trip");
        }
        // Reopen: records survive.
        let handle = BuyerChannelStoreHandle::new(std::sync::Arc::new(
            PersistentChannelStateStore::open(dir.path())?,
        ));
        let mut all = handle.load_all()?;
        all.sort_by_key(|s| s.provider);
        anyhow::ensure!(all.len() == 2);
        anyhow::ensure!(*all.first().ok_or_else(|| anyhow::anyhow!("[0]"))? == a);
        anyhow::ensure!(*all.get(1).ok_or_else(|| anyhow::anyhow!("[1]"))? == b);
        Ok(())
    }

    #[test]
    fn buyer_record_overwrites_by_provider() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let handle = BuyerChannelStoreHandle::new(std::sync::Arc::new(
            PersistentChannelStateStore::open(dir.path())?,
        ));
        let mut s = buyer_sample(5);
        handle.record(&s)?;
        s.last_nonce = U256::from(99u64);
        s.deposit = U256::from(20_000_000u64);
        handle.record(&s)?;
        anyhow::ensure!(handle.load_all()?.len() == 1, "same provider overwrites");
        let only = handle
            .get_by_provider(s.provider)?
            .ok_or_else(|| anyhow::anyhow!("missing"))?;
        anyhow::ensure!(only.last_nonce == U256::from(99u64));
        anyhow::ensure!(only.deposit == U256::from(20_000_000u64));
        Ok(())
    }

    #[test]
    fn buyer_forget_removes_entry() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let handle = BuyerChannelStoreHandle::new(std::sync::Arc::new(
            PersistentChannelStateStore::open(dir.path())?,
        ));
        let s = buyer_sample(3);
        handle.record(&s)?;
        handle.forget(s.provider)?;
        anyhow::ensure!(handle.load_all()?.is_empty());
        // forget on an unknown provider is a no-op.
        handle.forget(address!("00000000000000000000000000000000000000ff"))?;
        Ok(())
    }

    /// The buyer and seller tables share one redb file but never interfere:
    /// a buyer record and a seller record with overlapping low bytes coexist.
    #[test]
    fn buyer_and_seller_tables_are_independent() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let store = std::sync::Arc::new(PersistentChannelStateStore::open(dir.path())?);
        let seller = sample(7);
        ChannelStateStore::record(store.as_ref(), &seller)?;
        let buyer = buyer_sample(7);
        let handle = BuyerChannelStoreHandle::new(std::sync::Arc::clone(&store));
        handle.record(&buyer)?;

        // Each table sees only its own row.
        anyhow::ensure!(ChannelStateStore::load_all(store.as_ref())?.len() == 1);
        anyhow::ensure!(handle.load_all()?.len() == 1);
        let got_seller = ChannelStateStore::get(store.as_ref(), seller.channel_id)?
            .ok_or_else(|| anyhow::anyhow!("seller row missing"))?;
        anyhow::ensure!(got_seller == seller);
        let got_buyer = handle
            .get_by_provider(buyer.provider)?
            .ok_or_else(|| anyhow::anyhow!("buyer row missing"))?;
        anyhow::ensure!(got_buyer == buyer);
        Ok(())
    }

    /// A buyer record stamped with a future schema version must refuse to
    /// load — same safety posture as the seller table.
    #[test]
    fn buyer_future_schema_version_skipped_on_hydration() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let store = PersistentChannelStateStore::open(dir.path())?;
        let s = buyer_sample(1);
        let mut stored = StoredBuyerChannelState::from(&s);
        stored.schema_version = BUYER_SUPPORTED_SCHEMA_VERSION + 1;
        let encoded = postcard::to_allocvec(&stored)?;
        let key: [u8; 20] = s.provider.into();
        let mut tx = store.db.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        {
            let mut t = tx.open_table(BUYER_CHANNEL_TABLE)?;
            t.insert(&key, encoded.as_slice())?;
        }
        tx.commit()?;

        let handle = BuyerChannelStoreHandle::new(std::sync::Arc::new(store));
        // A future-schema record (the canonical post-downgrade case) must NOT
        // fail hydration — that would disable the whole buyer path and the
        // reclaim sweep (PR #753 review). `load_all` skips it instead.
        anyhow::ensure!(
            handle.load_all()?.is_empty(),
            "future-schema record must be skipped, not propagated, by load_all",
        );
        // The point lookup still surfaces the precise error (it is not on the
        // bootstrap path, so propagating is safe and diagnostic).
        let err = handle
            .get_by_provider(s.provider)
            .err()
            .ok_or_else(|| anyhow::anyhow!("future schema must reject on get_by_provider"))?;
        anyhow::ensure!(
            matches!(
                err,
                StoreError::UnsupportedSchema { found, supported }
                    if found == BUYER_SUPPORTED_SCHEMA_VERSION + 1
                        && supported == BUYER_SUPPORTED_SCHEMA_VERSION,
            ),
            "expected UnsupportedSchema, got {err:?}",
        );
        Ok(())
    }

    /// Garbage value bytes under a real buyer key are skipped by `load_all`
    /// (one bad row must not strand every other channel's deposit — PR #753
    /// review), while a healthy record alongside it survives. The point lookup
    /// (`get_by_provider`) still surfaces `Corrupt`.
    #[test]
    fn buyer_corrupt_value_bytes_skipped_keeps_healthy() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let store = PersistentChannelStateStore::open(dir.path())?;
        let healthy = buyer_sample(2);
        let corrupt_key: [u8; 20] = buyer_sample(3).provider.into();
        let healthy_encoded = postcard::to_allocvec(&StoredBuyerChannelState::from(&healthy))?;
        let healthy_key: [u8; 20] = healthy.provider.into();
        let mut tx = store.db.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        {
            let mut t = tx.open_table(BUYER_CHANNEL_TABLE)?;
            t.insert(&healthy_key, healthy_encoded.as_slice())?;
            t.insert(&corrupt_key, &[0u8; 8][..])?; // far too short for a valid record
        }
        tx.commit()?;

        let handle = BuyerChannelStoreHandle::new(std::sync::Arc::new(store));
        let all = handle.load_all()?;
        anyhow::ensure!(
            all.len() == 1 && all.first() == Some(&healthy),
            "corrupt row must be skipped while the healthy row survives, got {all:?}",
        );
        let err = handle
            .get_by_provider(buyer_sample(3).provider)
            .err()
            .ok_or_else(|| anyhow::anyhow!("garbage value must reject on get_by_provider"))?;
        anyhow::ensure!(
            matches!(&err, StoreError::Corrupt { detail, .. } if detail.contains("postcard decode")),
            "expected Corrupt(postcard decode), got {err:?}",
        );
        Ok(())
    }

    /// A buyer record whose embedded `provider` doesn't match its table key is
    /// skipped by `load_all`; the point lookup still surfaces `Corrupt`.
    #[test]
    fn buyer_provider_key_mismatch_skipped_on_hydration() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let store = PersistentChannelStateStore::open(dir.path())?;
        let s_for_a = buyer_sample(0xAA);
        let key_b: [u8; 20] = buyer_sample(0xBB).provider.into(); // different key
        let encoded = postcard::to_allocvec(&StoredBuyerChannelState::from(&s_for_a))?;
        let mut tx = store.db.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        {
            let mut t = tx.open_table(BUYER_CHANNEL_TABLE)?;
            t.insert(&key_b, encoded.as_slice())?;
        }
        tx.commit()?;

        let handle = BuyerChannelStoreHandle::new(std::sync::Arc::new(store));
        anyhow::ensure!(
            handle.load_all()?.is_empty(),
            "provider/key-mismatch record must be skipped by load_all",
        );
        let err = handle
            .get_by_provider(buyer_sample(0xBB).provider)
            .err()
            .ok_or_else(|| {
                anyhow::anyhow!("provider/key mismatch must reject on get_by_provider")
            })?;
        anyhow::ensure!(
            matches!(&err, StoreError::Corrupt { detail, .. } if detail.contains("does not match table key")),
            "expected Corrupt(does not match table key), got {err:?}",
        );
        Ok(())
    }

    /// Forward-compat: a future writer's additive trailing bytes after the
    /// buyer prefix decode cleanly (`take_from_bytes`). Mirrors the seller
    /// `extra_trailing_bytes_are_tolerated` guard.
    #[test]
    fn buyer_extra_trailing_bytes_are_tolerated() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let store = PersistentChannelStateStore::open(dir.path())?;
        let s = buyer_sample(0x42);
        let key: [u8; 20] = s.provider.into();
        let mut encoded = postcard::to_allocvec(&StoredBuyerChannelState::from(&s))?;
        encoded.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x01]);
        let mut tx = store.db.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        {
            let mut t = tx.open_table(BUYER_CHANNEL_TABLE)?;
            t.insert(&key, encoded.as_slice())?;
        }
        tx.commit()?;

        let handle = BuyerChannelStoreHandle::new(std::sync::Arc::new(store));
        let all = handle.load_all()?;
        anyhow::ensure!(all.len() == 1);
        let only = all.first().ok_or_else(|| anyhow::anyhow!("missing"))?;
        anyhow::ensure!(*only == s, "prefix must decode despite trailing bytes");
        Ok(())
    }

    /// `forget_if_channel` (compare-and-delete) deletes only the matching
    /// channel — the lost-update guard for the reclaim sweep.
    #[test]
    fn buyer_forget_if_channel_is_compare_and_delete() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let handle = BuyerChannelStoreHandle::new(std::sync::Arc::new(
            PersistentChannelStateStore::open(dir.path())?,
        ));
        // CAS on a never-written store is a no-op (false), no table created.
        anyhow::ensure!(
            !handle.forget_if_channel(buyer_sample(1).provider, buyer_sample(1).channel_id)?
        );

        let s = buyer_sample(4);
        handle.record(&s)?;
        // Wrong channel id → not deleted, row survives.
        anyhow::ensure!(
            !handle.forget_if_channel(
                s.provider,
                b256!("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
            )?,
            "mismatched channel must not delete"
        );
        anyhow::ensure!(
            handle.get_by_provider(s.provider)?.is_some(),
            "row must survive"
        );
        // Matching channel id → deleted.
        anyhow::ensure!(handle.forget_if_channel(s.provider, s.channel_id)?);
        anyhow::ensure!(handle.get_by_provider(s.provider)?.is_none());
        Ok(())
    }

    /// #838: interleaving `add_deposit` with `advance_progress` on the same
    /// provider row must lose neither the deposit accrual nor the watermark
    /// advance. The pre-fix `get → mutate → record` (read outside the write txn)
    /// would clobber one writer with the other's stale snapshot; the atomic
    /// in-txn mutators serialise correctly.
    #[test]
    fn buyer_concurrent_top_up_and_progress_preserve_both() -> anyhow::Result<()> {
        const N: u64 = 300;
        let dir = data_dir()?;
        let handle = BuyerChannelStoreHandle::new(std::sync::Arc::new(
            PersistentChannelStateStore::open(dir.path())?,
        ));
        let mut base = buyer_sample(6);
        base.deposit = U256::from(1_000u64);
        base.last_nonce = U256::ZERO;
        base.last_bytes_delivered = U256::ZERO;
        base.last_amount = U256::ZERO;
        handle.record(&base)?;
        let (provider, channel_id) = (base.provider, base.channel_id);

        let depositor = handle.clone();
        let deposit_thread = std::thread::spawn(move || -> anyhow::Result<()> {
            for _ in 0..N {
                let outcome = depositor.add_deposit(provider, channel_id, U256::from(1u64))?;
                anyhow::ensure!(
                    matches!(outcome, DepositOutcome::Added(_)),
                    "deposit outcome {outcome:?}"
                );
            }
            Ok(())
        });
        let advancer = handle.clone();
        let progress_thread = std::thread::spawn(move || -> anyhow::Result<()> {
            for i in 1..=N {
                let outcome = advancer.advance_progress(
                    provider,
                    channel_id,
                    U256::from(i),
                    U256::from(i) * U256::from(1_024u64),
                    U256::from(i) * U256::from(10u64),
                )?;
                anyhow::ensure!(
                    outcome == AdvanceOutcome::Advanced,
                    "advance outcome {outcome:?}"
                );
            }
            Ok(())
        });
        deposit_thread
            .join()
            .map_err(|_| anyhow::anyhow!("deposit thread panicked"))??;
        progress_thread
            .join()
            .map_err(|_| anyhow::anyhow!("progress thread panicked"))??;

        let final_row = handle
            .get_by_provider(provider)?
            .ok_or_else(|| anyhow::anyhow!("row vanished"))?;
        anyhow::ensure!(
            final_row.deposit == U256::from(1_000u64) + U256::from(N),
            "lost a top-up: deposit = {}",
            final_row.deposit
        );
        anyhow::ensure!(
            final_row.last_nonce == U256::from(N),
            "watermark not fully advanced: last_nonce = {}",
            final_row.last_nonce
        );
        Ok(())
    }

    /// #838: the atomic mutators honour the `Durability::Immediate` "MUST commit
    /// durably" contract — an `advance_progress` + `add_deposit` survive a store
    /// close/reopen, while a no-write `ChannelMismatch` persists nothing.
    #[test]
    fn buyer_advance_and_deposit_survive_reopen() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let s = buyer_sample(2);
        let other_channel =
            b256!("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");
        {
            let handle = BuyerChannelStoreHandle::new(std::sync::Arc::new(
                PersistentChannelStateStore::open(dir.path())?,
            ));
            handle.record(&s)?;
            anyhow::ensure!(
                handle.advance_progress(
                    s.provider,
                    s.channel_id,
                    s.last_nonce + U256::from(3u64),
                    s.last_bytes_delivered + U256::from(3_000u64),
                    s.last_amount + U256::from(30u64),
                )? == AdvanceOutcome::Advanced
            );
            anyhow::ensure!(
                handle.add_deposit(s.provider, s.channel_id, U256::from(40u64))?
                    == DepositOutcome::Added(s.deposit + U256::from(40u64))
            );
            // A mismatched (no-write) call must leave nothing extra to persist.
            anyhow::ensure!(
                handle.add_deposit(s.provider, other_channel, U256::from(1u64))?
                    == DepositOutcome::ChannelMismatch
            );
        }
        // Reopen from the same directory: the committed advance + deposit survive.
        let handle = BuyerChannelStoreHandle::new(std::sync::Arc::new(
            PersistentChannelStateStore::open(dir.path())?,
        ));
        let reopened = handle
            .get_by_provider(s.provider)?
            .ok_or_else(|| anyhow::anyhow!("row vanished across reopen"))?;
        anyhow::ensure!(reopened.last_nonce == s.last_nonce + U256::from(3u64));
        anyhow::ensure!(
            reopened.last_bytes_delivered == s.last_bytes_delivered + U256::from(3_000u64)
        );
        anyhow::ensure!(reopened.last_amount == s.last_amount + U256::from(30u64));
        anyhow::ensure!(
            reopened.deposit == s.deposit + U256::from(40u64),
            "mismatched call must not have altered the deposit"
        );
        Ok(())
    }

    /// #838: a stale `advance_progress` reporting totals below the committed
    /// watermark is rejected and leaves the committed watermark intact.
    #[test]
    fn buyer_advance_progress_cannot_regress_committed_watermark() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let handle = BuyerChannelStoreHandle::new(std::sync::Arc::new(
            PersistentChannelStateStore::open(dir.path())?,
        ));
        let mut s = buyer_sample(8);
        s.last_nonce = U256::from(9u64);
        s.last_bytes_delivered = U256::from(9_000u64);
        s.last_amount = U256::from(90u64);
        handle.record(&s)?;

        let outcome = handle.advance_progress(
            s.provider,
            s.channel_id,
            U256::from(5u64),
            U256::from(5_000u64),
            U256::from(50u64),
        )?;
        anyhow::ensure!(
            matches!(outcome, AdvanceOutcome::Regressed(_)),
            "stale progress must be rejected, got {outcome:?}"
        );
        let stored = handle
            .get_by_provider(s.provider)?
            .ok_or_else(|| anyhow::anyhow!("row vanished"))?;
        anyhow::ensure!(
            stored.last_nonce == U256::from(9u64),
            "committed watermark regressed to {}",
            stored.last_nonce
        );
        Ok(())
    }

    /// #838: the atomic mutators are channel-id guarded (a row replaced by a
    /// newer open for the same provider is not clobbered) and report unknown
    /// providers without creating the table.
    #[test]
    fn buyer_advance_and_deposit_guard_on_channel_and_provider() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let handle = BuyerChannelStoreHandle::new(std::sync::Arc::new(
            PersistentChannelStateStore::open(dir.path())?,
        ));
        // Unknown provider on a never-written store → UnknownProvider, no table.
        let ghost = buyer_sample(1);
        anyhow::ensure!(
            handle.advance_progress(
                ghost.provider,
                ghost.channel_id,
                U256::from(1u64),
                U256::from(1u64),
                U256::from(1u64)
            )? == AdvanceOutcome::UnknownProvider
        );
        anyhow::ensure!(
            handle.add_deposit(ghost.provider, ghost.channel_id, U256::from(1u64))?
                == DepositOutcome::UnknownProvider
        );

        let s = buyer_sample(9);
        handle.record(&s)?;
        let other_channel =
            b256!("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");
        anyhow::ensure!(
            handle.advance_progress(
                s.provider,
                other_channel,
                U256::from(99u64),
                U256::from(99u64),
                U256::from(99u64)
            )? == AdvanceOutcome::ChannelMismatch
        );
        anyhow::ensure!(
            handle.add_deposit(s.provider, other_channel, U256::from(1u64))?
                == DepositOutcome::ChannelMismatch
        );
        let stored = handle
            .get_by_provider(s.provider)?
            .ok_or_else(|| anyhow::anyhow!("row vanished"))?;
        anyhow::ensure!(stored == s, "mismatched calls must not mutate the row");
        Ok(())
    }
}
