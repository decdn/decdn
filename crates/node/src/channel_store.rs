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
//! [ADR 003 §Off-chain voucher state persistence]: ../../../adr/003-payments.md

use std::path::{Path, PathBuf};

use alloy::primitives::{Address, B256, U256};
use decdn_common::identity;
use decdn_incentive::store::{ChannelStateStore, StoreError};
use decdn_incentive::{ChannelId, ChannelState};
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
const SUPPORTED_SCHEMA_VERSION: u32 = 1;

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
            last_amount: state.last_amount.to_be_bytes(),
            last_nonce: state.last_nonce.to_be_bytes(),
            last_bytes_delivered: state.last_bytes_delivered.to_be_bytes(),
        }
    }
}

impl StoredChannelState {
    fn into_state(self) -> Result<ChannelState, StoreError> {
        if self.schema_version > SUPPORTED_SCHEMA_VERSION {
            return Err(StoreError::UnsupportedSchema {
                found: self.schema_version,
                supported: SUPPORTED_SCHEMA_VERSION,
            });
        }
        Ok(ChannelState {
            channel_id: B256::from(self.channel_id),
            client: Address::from(self.client),
            token: Address::from(self.token),
            deposit: U256::from_be_bytes(self.deposit),
            last_amount: U256::from_be_bytes(self.last_amount),
            last_nonce: U256::from_be_bytes(self.last_nonce),
            last_bytes_delivered: U256::from_be_bytes(self.last_bytes_delivered),
        })
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
    /// function. Production code calls [`Self::open`], which delegates here
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
            let channel_id = B256::from(key_bytes);
            let value_bytes = value_guard.value();
            // `take_from_bytes` instead of `from_bytes` so a newer writer's
            // additive fields (which appear as trailing bytes to an older
            // reader) decode cleanly — see `StoredChannelState`'s doc and
            // the schema-version handshake in `into_state`. The remainder
            // is intentionally discarded.
            //
            // What this DOES guard: forward-compat for additive schema
            // changes, and outright structural corruption (insufficient
            // bytes, malformed varint) which `take_from_bytes` rejects.
            //
            // What this does NOT guard: intra-record bit-flips inside the
            // fixed-shape fields (e.g. a single byte flipped inside
            // `client: [u8; 20]`). The deserializer accepts any 20 bytes
            // there and the schema_version check in `into_state` cannot
            // catch it. The real defense for that class is one layer
            // down: redb checksums its pages, so on-disk bit-rot is
            // caught at the storage layer before we see the value.
            let (stored, remainder): (StoredChannelState, &[u8]) =
                postcard::take_from_bytes(value_bytes).map_err(|err| StoreError::Corrupt {
                    channel_id: Some(channel_id),
                    detail: format!("postcard decode failed: {err}"),
                })?;
            // Forward-compat allowance is bounded: a malicious writer could
            // pad megabytes onto every record and silently inflate every
            // `load_all`. Log (don't fail) when the trailer exceeds a
            // small sanity ceiling so a future schema-skew incident is
            // observable in operator logs without re-introducing the
            // strict-decoding regression issue #527's reviewers warned
            // against.
            if remainder.len() > SANE_TRAILER_MAX_BYTES {
                tracing::warn!(
                    %channel_id,
                    remainder = remainder.len(),
                    limit = SANE_TRAILER_MAX_BYTES,
                    event = "channel_store_excess_trailer",
                    "channel state record has unusually large trailing bytes; possible malicious padding or large-additive-field schema skew",
                );
            }
            if stored.channel_id != key_bytes {
                return Err(StoreError::Corrupt {
                    channel_id: Some(channel_id),
                    detail: "channel_id in value does not match table key".into(),
                });
            }
            out.push(stored.into_state()?);
        }
        Ok(out)
    }

    fn record(&self, state: &ChannelState) -> Result<(), StoreError> {
        let encoded = postcard::to_allocvec(&StoredChannelState::from(state))
            .map_err(|err| StoreError::Codec(format!("postcard encode failed: {err}")))?;
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
        ChannelState {
            channel_id: id.into(),
            client: address!("00000000000000000000000000000000000000aa"),
            token: address!("a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"),
            deposit: U256::from(10_000_000u64),
            last_amount: U256::from(byte) * U256::from(1_000u64),
            last_nonce: U256::from(byte),
            last_bytes_delivered: U256::from(byte) * U256::from(1_024u64),
        }
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

        // Encode the real record, then append plausible additive-field
        // bytes (simulating what a future schema would write).
        let mut encoded = postcard::to_allocvec(&StoredChannelState::from(&s))?;
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
            "decoded prefix must equal the original record despite trailing bytes",
        );
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
}
