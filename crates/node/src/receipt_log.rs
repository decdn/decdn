//! Append-only download-receipt log for the node runtime (issue #248).
//!
//! A node enqueues one structured [`DownloadReceipt`] per served-and-paid blob
//! at the voucher-acceptance point (see
//! [`crate::handlers::client::ClientHandler`]); the durable write happens off
//! the delivery path in a background writer (see the Seam note below, #803). The
//! log gives the operator a durable, off-chain record of which blobs were
//! delivered and paid for — enabling revenue audit, cross-restart double-spend
//! triage, and dispute evidence without relying solely on on-chain state.
//!
//! # Seam
//!
//! There are two layers. [`ReceiptLog`] is the *disk* boundary — a small trait
//! (mirroring the trait+impl pattern of
//! [`decdn_incentive::store::PoolStateStore`] / [`crate::channel_store`])
//! whose runtime impl is the disk-backed [`JsonlReceiptLog`], appending one JSON
//! object per line to `<data_dir>/download_receipts.jsonl` (JSON Lines).
//! [`ReceiptSink`] is the *hot-path* boundary: the paid-delivery path does not
//! call [`ReceiptLog::append`] directly — it enqueues through the non-blocking
//! [`ReceiptSink::record`], and a single background [`spawn_receipt_writer`]
//! task owns the `JsonlReceiptLog` and performs the actual `append` off the
//! delivery path (#803), so a slow or full disk can never back-pressure
//! delivery. Both seams take an in-memory fake in unit/loopback tests.
//!
//! # Durability
//!
//! Each [`ReceiptLog::append`] writes one line and flushes it to the OS
//! (`write_all` + `flush`) but does **not** fsync per record. The receipt log
//! is an *audit* artifact, not the voucher-replay guard. That guard is the lane
//! watermark in [`crate::channel_store::PersistentPoolStateStore`], which
//! advances **in memory** the moment a voucher or preimage is accepted, reaches
//! disk on the runtime's periodic lane flush
//! (`payment.voucher_commit_interval_ms`, 5 s by default), and is additionally
//! flushed unconditionally before any on-chain redemption.
//!
//! Neither side fsyncs on the delivery path, so after a hard crash the two
//! tails can disagree in either direction. In particular the audit log can be
//! **ahead** of the lane store: a receipt line reaches disk while the lane
//! advance it records is still buffered, so a restart can find a receipt whose
//! watermark the lane store no longer holds. That direction costs revenue, not
//! safety — the node forfeits at most one flush interval of *frontier* (value
//! it has not yet redeemed), which ADR 003 §Off-chain voucher state persistence
//! accepts, while replay protection rests on the *redeemed* watermark that the
//! mandatory pre-redemption flush floors. The other direction, a lost receipt
//! tail, is a gap in the audit record and nothing more. Skipping the per-line
//! fsync keeps the cost off the hot delivery path (see CLAUDE.md / ADR 003).
//!
//! The append is also crash-atomic at the line level only in the usual POSIX
//! sense (a torn final line is possible after a hard crash); JSONL readers MUST
//! tolerate a truncated trailing line.
//!
//! # Rotation (issue #802)
//!
//! Left unbounded the log grows for the entire process lifetime and across
//! restarts (`O_APPEND`), eventually exhausting `data_dir`. [`JsonlReceiptLog`]
//! therefore takes a [`RotationPolicy`] (`max_file_bytes`, `retained_files`):
//! once the live file would exceed `max_file_bytes` it is rotated to a numbered
//! backup (`download_receipts.jsonl.1`, `.2`, …, oldest = highest), a fresh
//! live file is opened, and any backup beyond `retained_files` is deleted. This
//! bounds `data_dir` to roughly `(retained_files + 1) * max_file_bytes`.
//! `retained_files == 0` keeps no backups — the live file is truncated in place
//! on rotation. Rotation is **best-effort**: a rename/open failure mid-rotation
//! never leaves the log without a writable handle and never aborts paid
//! delivery (the background writer treats an `append` error as non-fatal, and
//! delivery is decoupled from it by the bounded queue, #803), it only trims or
//! skips a backup that cycle.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use alloy::primitives::U256;
use iroh_blobs::Hash;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::metrics::Metrics;

/// File name of the receipt log within `data_dir`. Canonical name lives in
/// `decdn_common` so the daemon writer and the `config validate` summary can't
/// disagree on where receipts land (#964).
const RECEIPT_LOG_FILE: &str = decdn_common::config::RECEIPT_LOG_FILE;

/// On-disk file mode (`0o600` — owner-only read+write). Same defense-in-depth
/// rationale as [`crate::channel_store`]: `data_dir` is already `0o700`, but the
/// receipt log can carry per-client delivery metadata, so the file mode is
/// tightened independently in case the directory ACL is widened out-of-band.
#[cfg(unix)]
const RECEIPT_LOG_FILE_MODE: u32 = 0o600;

/// One durable record of a served-and-paid blob (issue #248).
///
/// Written at the voucher-acceptance point, so every receipt corresponds to a
/// cumulative payment the node verified and committed. All identifier fields
/// are lower-hex strings (no `0x` prefix) so the log is self-describing and
/// greppable without a schema:
///
/// - `hash` — BLAKE3 content hash of the delivered blob (raw 32 bytes, hex).
/// - `client_node_id` — iroh `NodeId` (ed25519 public key) of the paying peer
///   (raw 32 bytes, hex).
/// - `voucher_amount` — the accepted voucher's cumulative amount (its sole
///   ordering key; there is no nonce), as a decimal string. A decimal `uint256`
///   (not hex) so an auditor reads the same value the on-chain `PaymentPool` and
///   operator dashboards show; it is reconstructed from the 32-byte big-endian
///   wire amount.
///
/// `size` is the byte count covered by *this* payment proof (the newly
/// delivered, now-paid bytes), and `timestamp_secs` is the node's wall-clock
/// Unix time (seconds) at acceptance.
///
/// # Invariants
///
/// The fields are **private** and the only public constructor is [`new`], which
/// renders the hex and decimal-`uint256` strings from typed inputs. This makes
/// the invariants type-owned: a receipt cannot be built field-wise, and serde's
/// derived `Deserialize` cannot synthesize one with a malformed `hash` /
/// `client_node_id` / `voucher_amount` from outside the crate (the fields stay
/// private to readers). Accessor methods expose the rendered values read-only.
///
/// Field-wise construction from outside the crate does not compile (the fields
/// are private), so the validating [`new`] is the only public path in:
///
/// ```compile_fail
/// use decdn_node::receipt_log::DownloadReceipt;
/// // ERROR: fields `hash`, `size`, ... are private — invariant cannot be bypassed.
/// let _ = DownloadReceipt {
///     hash: "not-a-hash".to_string(),
///     size: 1,
///     client_node_id: "nope".to_string(),
///     voucher_amount: "0xdeadbeef".to_string(),
///     timestamp: 0,
/// };
/// ```
///
/// [`new`]: DownloadReceipt::new
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DownloadReceipt {
    /// BLAKE3 content hash of the delivered blob, lower-hex (64 chars).
    hash: String,
    /// Bytes covered by this payment proof (the newly paid delivery).
    size: u64,
    /// iroh `NodeId` of the paying client, lower-hex (64 chars).
    client_node_id: String,
    /// Accepted voucher cumulative amount, decimal `uint256` string.
    voucher_amount: String,
    /// Node wall-clock Unix time (seconds) at voucher acceptance. The wire key
    /// stays `timestamp` (preserved via `#[serde(rename)]`) so the on-disk JSONL
    /// format is byte-identical to the original; the Rust field name carries the
    /// unit explicitly.
    #[serde(rename = "timestamp")]
    timestamp_secs: u64,
}

impl DownloadReceipt {
    /// Build a receipt from the typed delivery-path values, rendering every
    /// invariant inside the constructor:
    ///
    /// - `hash` is the [`iroh_blobs::Hash`] of the delivered blob — a distinct
    ///   type from `client_node_id_bytes`, so the two 32-byte identifiers cannot
    ///   be transposed at a call site. Rendered to lower-hex (no `0x`).
    /// - `client_node_id_bytes` is the raw 32-byte iroh node id, lower-hex.
    /// - `voucher_amount` is the accepted voucher's cumulative `uint256` amount;
    ///   the decimal string is rendered here so the "decimal uint256" invariant
    ///   is owned by the type, not the caller.
    #[must_use]
    pub fn new(
        hash: &Hash,
        size: u64,
        client_node_id_bytes: &[u8; 32],
        voucher_amount: U256,
        timestamp_secs: u64,
    ) -> Self {
        Self {
            hash: hex_lower(hash.as_bytes()),
            size,
            client_node_id: hex_lower(client_node_id_bytes),
            voucher_amount: voucher_amount.to_string(),
            timestamp_secs,
        }
    }

    /// BLAKE3 content hash of the delivered blob, lower-hex (64 chars, no `0x`).
    #[must_use]
    pub fn hash(&self) -> &str {
        &self.hash
    }

    /// Bytes covered by this payment proof (the newly paid delivery).
    #[must_use]
    pub const fn size(&self) -> u64 {
        self.size
    }

    /// iroh `NodeId` of the paying client, lower-hex (64 chars, no `0x`).
    #[must_use]
    pub fn client_node_id(&self) -> &str {
        &self.client_node_id
    }

    /// Accepted voucher cumulative amount, decimal `uint256` string.
    #[must_use]
    pub fn voucher_amount(&self) -> &str {
        &self.voucher_amount
    }

    /// Node wall-clock Unix time (seconds) at voucher acceptance.
    #[must_use]
    pub const fn timestamp_secs(&self) -> u64 {
        self.timestamp_secs
    }
}

/// Lower-hex encode a 32-byte array without a `0x` prefix. Delegates to
/// `alloy::primitives::hex::encode` (the `const-hex` re-export already pulled in
/// via the `alloy` dependency), which emits lowercase with no `0x` prefix —
/// byte-identical to the prior hand-rolled `{:02x}` loop, and locked against
/// drift by the `serialized_line_is_wire_stable` test.
fn hex_lower(bytes: &[u8; 32]) -> String {
    alloy::primitives::hex::encode(bytes)
}

/// Append-only sink for [`DownloadReceipt`]s.
///
/// The trait exists so the voucher-accept path can be exercised against an
/// in-memory fake in unit tests; the runtime uses [`JsonlReceiptLog`].
/// Implementations MUST be cheap enough to call inline on the delivery path
/// (one buffered line write + flush — no fsync; see the module durability note).
pub trait ReceiptLog: Send + Sync {
    /// Durably-enough append one receipt (write + flush, no fsync).
    ///
    /// # Errors
    ///
    /// Returns the underlying [`std::io::Error`] if serialization or the write
    /// fails. The caller treats a receipt-log failure as non-fatal (the payment
    /// already advanced the lane watermark) and logs it.
    fn append(&self, receipt: &DownloadReceipt) -> std::io::Result<()>;
}

/// Size-based rotation policy for [`JsonlReceiptLog`] (issue #802).
///
/// `max_file_bytes` is the size at which the live file is rotated;
/// `retained_files` is how many numbered backups (`.1`..=`.N`) to keep (`0`
/// truncates the live file in place instead of keeping backups). A
/// `max_file_bytes` of `0` disables rotation entirely (the live file grows
/// unbounded) — a degenerate value the engine accepts but production never
/// produces.
///
/// The engine deliberately accepts any values (the rotation guards in
/// [`JsonlReceiptLog::append`] cope with a sub-line cap, and tests use tiny
/// caps to exercise rotation deterministically). Production policies are built
/// *only* from a [`decdn_common::config::ResolvedReceipts`] via the [`From`]
/// impl below, and that config type is range-validated by
/// `decdn_common::config::resolve_config` — so every live policy is within the
/// operator-facing safe bounds (`[MIN_RECEIPT_MAX_FILE_BYTES,
/// MAX_RECEIPT_MAX_FILE_BYTES]`, `retained_files <= MAX_RECEIPT_RETAINED_FILES`).
#[derive(Debug, Clone, Copy)]
pub struct RotationPolicy {
    max_file_bytes: u64,
    retained_files: u32,
}

impl RotationPolicy {
    /// Build a policy from raw, *unvalidated* limits. Test-only: production
    /// constructs policies from validated config through the [`From`] impl, so
    /// this wider entry point (which accepts out-of-range and degenerate caps)
    /// is not exposed outside `#[cfg(test)]`.
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn new(max_file_bytes: u64, retained_files: u32) -> Self {
        Self {
            max_file_bytes,
            retained_files,
        }
    }
}

impl From<&decdn_common::config::ResolvedReceipts> for RotationPolicy {
    /// The sole production constructor. `ResolvedReceipts` is range-validated by
    /// the config resolver, so the policy is always within the operator-facing
    /// safe bounds — the validation guarantee carries across the type boundary
    /// instead of being re-opened by a raw constructor.
    fn from(r: &decdn_common::config::ResolvedReceipts) -> Self {
        Self {
            max_file_bytes: r.max_file_bytes,
            retained_files: r.retained_files,
        }
    }
}

/// Mutex-guarded writer state: the live append handle plus the in-memory byte
/// count used to decide rotation without an extra `stat` per append.
#[derive(Debug)]
struct LogState {
    file: File,
    written: u64,
}

/// JSON-Lines, append-only [`ReceiptLog`] backed by a single rotating file
/// under `data_dir`.
///
/// One [`File`] handle is held open for the process lifetime behind a [`Mutex`]
/// so concurrent delivery streams cannot interleave partial lines. The handle
/// is opened in append mode (`O_APPEND`), so even across the (already
/// serialized) writes each `write_all` lands at the current end of file.
/// Rotation (issue #802) swaps that handle under the same lock; see the
/// module-level rotation note.
#[derive(Debug)]
pub struct JsonlReceiptLog {
    state: Mutex<LogState>,
    path: PathBuf,
    policy: RotationPolicy,
}

impl JsonlReceiptLog {
    /// Open (or create) the receipt log under `data_dir` with rotation `policy`.
    ///
    /// The file is opened in append mode and, on Unix, created with mode
    /// `0o600`; an existing file's mode is tightened to `0o600` as
    /// defense-in-depth (idempotent — skipped when already correct, so a
    /// read-only mount with the right mode does not brick startup). The
    /// in-memory rotation counter is seeded from the existing file length so a
    /// log that is already near the cap rotates on the next append rather than
    /// only after a full cap's worth of fresh writes.
    ///
    /// Unlike [`crate::channel_store::PersistentPoolStateStore::open`], a
    /// failure here is **not** required to abort node bring-up: the receipt log
    /// is an audit artifact, not the #527 replay guard. The runtime decides the
    /// fatality (it currently logs and continues without the audit log).
    ///
    /// # Errors
    ///
    /// Returns a [`std::io::Error`] if the file cannot be opened/created, its
    /// mode cannot be tightened, or its current length cannot be read.
    pub fn open(data_dir: &Path, policy: RotationPolicy) -> std::io::Result<Self> {
        let path = data_dir.join(RECEIPT_LOG_FILE);
        let file = open_append(&path)?;
        let written = file.metadata()?.len();
        Ok(Self {
            state: Mutex::new(LogState { file, written }),
            path,
            policy,
        })
    }

    /// Filesystem path of the underlying (live) log file. Useful for operator
    /// log lines and runbooks.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Path of the `n`-th rotated backup (`<live>.n`).
    fn backup_path(&self, n: u32) -> PathBuf {
        let mut raw = self.path.clone().into_os_string();
        raw.push(format!(".{n}"));
        PathBuf::from(raw)
    }

    /// Rotate the live file, preserving a usable append handle in `state`.
    ///
    /// Best-effort: every step that can fail leaves `state.file` pointing at a
    /// writable canonical-path handle so the caller's next `append` still
    /// succeeds. Any backup numbered above `retained_files` is pruned first (so
    /// lowering the retention count reclaims stale backups). With
    /// `retained_files == 0` the live file is then truncated in place;
    /// otherwise the oldest retained backup is dropped, the chain is shifted up,
    /// the live file becomes `.1`, and a fresh live file is opened. If opening
    /// the fresh file fails, `.1` is renamed back so the original handle keeps
    /// writing to the canonical path.
    fn rotate(&self, state: &mut LogState) -> std::io::Result<()> {
        let retained = self.policy.retained_files;
        // Prune any backups numbered above the retention count first. This
        // bounds disk even after an operator *lowers* `retained_files` (or sets
        // it to 0) between runs, when stale higher-numbered backups (e.g. `.10`)
        // from a prior, larger setting would otherwise linger forever.
        self.prune_backups_above(retained);

        if retained == 0 {
            state.file.set_len(0)?;
            state.written = 0;
            return Ok(());
        }

        // Drop the oldest retained backup (NotFound is fine before the chain
        // fills) to make room for the shift below.
        match std::fs::remove_file(self.backup_path(retained)) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        // Shift `.i -> .(i+1)` from the top down so no rename clobbers a file
        // that still needs moving. A missing intermediate backup is a benign gap.
        for i in (1..retained).rev() {
            match std::fs::rename(self.backup_path(i), self.backup_path(i + 1)) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
        }
        // Move the live file to `.1`. The open handle in `state.file` follows the
        // inode, so writes through it would now land in `.1` until we swap it.
        std::fs::rename(&self.path, self.backup_path(1))?;
        match open_append(&self.path) {
            Ok(fresh) => {
                state.file = fresh;
                state.written = 0;
                Ok(())
            }
            Err(open_err) => {
                // Restore the canonical path so the still-open `state.file`
                // handle keeps appending to a correctly-named live log.
                let _ = std::fs::rename(self.backup_path(1), &self.path);
                Err(open_err)
            }
        }
    }

    /// Best-effort removal of every backup numbered strictly above `retained`.
    /// Backups are written densely (`.1`..`.k`), so we delete upward from
    /// `retained + 1` and stop at the first gap. This is disk-hygiene, not a
    /// correctness gate, and must never abort an append — so it never returns an
    /// error. It does, however, distinguish the two stop conditions: a missing
    /// index ends the dense run silently, but a *real* error (permissions, busy,
    /// I/O) is not the end of the run — higher-numbered backups may still exist
    /// and would leak past the `(retained_files + 1) * max_file_bytes` disk
    /// bound. Scanning past a gap could run unbounded, so we still stop, but a
    /// real error is logged so the leak is observable rather than silent.
    fn prune_backups_above(&self, retained: u32) {
        let mut n = retained.saturating_add(1);
        loop {
            match std::fs::remove_file(self.backup_path(n)) {
                Ok(()) => {}
                // The dense `.1`..`.k` run ends at the first missing index.
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return,
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        event = "download_receipt_prune_failed",
                        path = %self.backup_path(n).display(),
                        "could not prune stale receipt-log backup; backups above \
                         it may exceed receipts.retained_files until removed manually",
                    );
                    return;
                }
            }
            match n.checked_add(1) {
                Some(next) => n = next,
                None => return,
            }
        }
    }
}

/// Open (or create) an append-mode handle at `path`, applying the `0o600` mode
/// on Unix and tightening an existing file's mode. Shared by `open` and the
/// post-rotation reopen so both honour the same permissions invariant.
fn open_append(path: &Path) -> std::io::Result<File> {
    let mut opts = OpenOptions::new();
    opts.create(true).append(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        opts.mode(RECEIPT_LOG_FILE_MODE);
    }
    let file = opts.open(path)?;
    #[cfg(unix)]
    tighten_permissions(path)?;
    Ok(file)
}

/// Tighten the on-disk file mode to `0o600`. Idempotent: skips the syscall when
/// the mode already matches so a read-only mount with the correct mode (e.g.
/// from a prior boot) does not fail startup. Mirrors
/// [`crate::channel_store::PersistentPoolStateStore`]'s tightening.
#[cfg(unix)]
fn tighten_permissions(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt as _;
    let meta = std::fs::metadata(path)?;
    if meta.permissions().mode() & 0o777 == RECEIPT_LOG_FILE_MODE {
        return Ok(());
    }
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(RECEIPT_LOG_FILE_MODE))
}

impl ReceiptLog for JsonlReceiptLog {
    fn append(&self, receipt: &DownloadReceipt) -> std::io::Result<()> {
        // Serialize to a single line first, so a serialization failure never
        // writes a partial record. `serde_json::to_string` cannot embed a
        // newline in these scalar fields, keeping one-receipt-per-line.
        let mut line = serde_json::to_string(receipt).map_err(std::io::Error::other)?;
        line.push('\n');
        let line_len = u64::try_from(line.len()).unwrap_or(u64::MAX);
        let mut guard = self
            .state
            .lock()
            .map_err(|_| std::io::Error::other("receipt log mutex poisoned"))?;
        // Rotate before writing when this line would push the live file past the
        // cap, but only once the file already holds a record — so a single line
        // larger than the cap still writes (after one rotation) instead of
        // looping. Rotation is best-effort: if it fails, `state.file` is still a
        // writable handle, so we log and fall through to append the receipt to
        // the current file (temporarily exceeding the cap) rather than dropping
        // an audit record. The next append retries the rotation.
        if self.policy.max_file_bytes > 0
            && guard.written > 0
            && guard.written.saturating_add(line_len) > self.policy.max_file_bytes
            && let Err(e) = self.rotate(&mut guard)
        {
            tracing::warn!(
                error = %e,
                event = "download_receipt_rotation_failed",
                path = %self.path.display(),
                "receipt log rotation failed; appending to the current file \
                 (it may exceed receipts.max_file_bytes until the next rotation)",
            );
        }
        match guard
            .file
            .write_all(line.as_bytes())
            // Flush to the OS so the line survives a clean process exit; no
            // fsync — see the module-level durability note.
            .and_then(|()| guard.file.flush())
        {
            Ok(()) => {
                guard.written = guard.written.saturating_add(line_len);
                Ok(())
            }
            Err(e) => {
                // A partial write can land bytes before erroring, so the
                // in-memory counter may now disagree with the file. Resync from
                // metadata (best-effort) so a later rotation isn't decided on a
                // stale count.
                if let Ok(meta) = guard.file.metadata() {
                    guard.written = meta.len();
                }
                Err(e)
            }
        }
    }
}

/// A [`ReceiptLog`] that drops every receipt. Used as the runtime fallback when
/// [`JsonlReceiptLog::open`] fails at bring-up: the audit log is best-effort, so
/// the node serves paid delivery without it rather than refusing to start (the
/// replay guard lives in the separate lane store, which the periodic flush and
/// the pre-redemption flush make durable). The operator gets one `error!` at
/// startup naming the open failure.
#[derive(Debug, Default)]
pub struct NoopReceiptLog;

impl ReceiptLog for NoopReceiptLog {
    fn append(&self, _receipt: &DownloadReceipt) -> std::io::Result<()> {
        Ok(())
    }
}

/// Non-blocking enqueue boundary for [`DownloadReceipt`]s on the paid-delivery
/// hot path (#803).
///
/// The voucher-accept path records a receipt through this seam as each voucher
/// is accepted (acceptance is implicit — delivery simply continues), so the
/// implementation MUST NOT block on disk I/O: a backed-up sink drops the
/// receipt (best-effort, audit-only) rather than stall the payment, which
/// already advanced the lane watermark. The runtime
/// uses [`ChannelReceiptSink`] (hands off to the background
/// [`spawn_receipt_writer`] task); tests use a synchronous fake behind
/// [`DirectReceiptSink`].
pub trait ReceiptSink: Send + Sync {
    /// Best-effort, non-blocking record of one receipt. Never blocks the caller
    /// on disk and never fails delivery — a sink that cannot accept the receipt
    /// drops it.
    fn record(&self, receipt: DownloadReceipt);
}

/// Capacity of the receipt-writer queue. Receipts are roughly one per voucher
/// interval (~1 MiB served), so this absorbs a large burst of voucher
/// acceptances while a disk stall (a slow or full `data_dir`, the realistic
/// end-state of #802) is worked off, without ever back-pressuring paid
/// delivery. On overflow the *audit* receipt is dropped (counted via
/// [`Metrics::receipt_write_dropped`]) rather than the *payment* stalling — the
/// payment already advanced the lane watermark. Rotation (#802)
/// bounds the log on disk; this bounds it in memory.
pub const RECEIPT_LOG_CAPACITY: usize = 1024;

/// Production [`ReceiptSink`]: enqueues to the background writer over a bounded
/// channel. Mirrors the redeem-hint sink (#751): a `Full` channel drops the
/// receipt and counts it (`decdn_receipt_writes_dropped_total`); a `Closed`
/// channel — the writer stopped during shutdown — is silently ignored (the tail
/// drain has already run, or is bounded by the shutdown deadline).
#[derive(Clone)]
pub struct ChannelReceiptSink {
    tx: mpsc::Sender<DownloadReceipt>,
    metrics: Arc<Metrics>,
}

impl std::fmt::Debug for ChannelReceiptSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChannelReceiptSink")
            .field("capacity", &self.tx.max_capacity())
            .finish_non_exhaustive()
    }
}

impl ReceiptSink for ChannelReceiptSink {
    fn record(&self, receipt: DownloadReceipt) {
        match self.tx.try_send(receipt) {
            // Enqueued — the background writer will append it.
            Ok(()) => {}
            // Queue saturated: drop the audit receipt and count it.
            Err(mpsc::error::TrySendError::Full(_)) => self.metrics.receipt_write_dropped(),
            // Writer gone. Expected only during shutdown (after the router drains
            // and the writer's token is cancelled), so it is left uncounted to
            // avoid false alerting — mirroring the redeem-hint `Closed` rationale
            // (#751). A `debug!` still leaves a trace, so a writer that died
            // unexpectedly (e.g. panicked) — which would otherwise drop every
            // subsequent receipt with no signal — is at least observable.
            Err(mpsc::error::TrySendError::Closed(_)) => {
                tracing::debug!(
                    event = "download_receipt_sink_closed",
                    "receipt dropped: writer gone (expected only during shutdown)"
                );
            }
        }
    }
}

/// Synchronous [`ReceiptSink`] that appends inline to a [`ReceiptLog`],
/// swallowing the write error. Test/loopback support only: lets the unit and
/// `client_loopback` tests keep a deterministic in-memory [`ReceiptLog`] fake
/// behind the sink seam the handler depends on, without standing up the
/// background writer or polling for an async drain. Production never uses it —
/// the runtime always routes through [`spawn_receipt_writer`] so disk I/O cannot
/// block paid delivery.
///
/// It deliberately *violates* the [`ReceiptSink`] non-blocking contract (it
/// appends inline on the caller's thread), so it must never be wired onto the
/// paid-delivery path. The type and constructor stay `pub` only because the
/// cross-crate integration tests in `tests/` cannot see `#[cfg(test)]` items;
/// the field is private and the type is `#[doc(hidden)]` so it does not read as
/// a production knob.
#[doc(hidden)]
pub struct DirectReceiptSink(Arc<dyn ReceiptLog>);

impl DirectReceiptSink {
    /// Wrap a synchronous [`ReceiptLog`] as an inline-appending sink. Test and
    /// loopback use only — see the type docs; never wire this onto the
    /// paid-delivery path.
    #[must_use]
    pub fn new(log: Arc<dyn ReceiptLog>) -> Self {
        Self(log)
    }
}

impl std::fmt::Debug for DirectReceiptSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DirectReceiptSink").finish_non_exhaustive()
    }
}

impl ReceiptSink for DirectReceiptSink {
    fn record(&self, receipt: DownloadReceipt) {
        // Audit-only and non-fatal, matching the background writer's handling.
        let _ = self.0.append(&receipt);
    }
}

/// Spawn the single background task that owns the on-disk [`ReceiptLog`] and
/// drains the receipt queue, returning the [`ReceiptSink`] the delivery path
/// enqueues through plus the task `JoinHandle` for graceful shutdown.
///
/// Decouples the audit write from the paid-delivery hot path (#803): the
/// voucher-accept path does only a non-blocking [`ReceiptSink::record`] (a
/// bounded `try_send`) as each voucher is accepted, while this task performs
/// the actual `append` on the blocking pool. A slow or full disk can only fill
/// the queue (and drop audit records, counted) — it can never delay delivery
/// or serialize delivery on the receipt-log mutex. The writer is the *only*
/// caller of [`ReceiptLog::append`], so the log's internal mutex sees no
/// cross-stream contention.
///
/// On `shutdown` cancellation (fired after the router has drained, so no further
/// receipts are produced) the task flushes whatever is already enqueued and
/// exits; await the returned handle within the shutdown deadline to preserve the
/// audit tail.
pub fn spawn_receipt_writer(
    log: Arc<dyn ReceiptLog>,
    metrics: Arc<Metrics>,
    shutdown: CancellationToken,
) -> (Arc<dyn ReceiptSink>, JoinHandle<()>) {
    let (tx, rx) = mpsc::channel(RECEIPT_LOG_CAPACITY);
    let handle = tokio::spawn(receipt_writer_loop(rx, log, shutdown));
    (Arc::new(ChannelReceiptSink { tx, metrics }), handle)
}

/// Drain loop for the background receipt writer (see [`spawn_receipt_writer`]).
/// Appends each receipt FIFO until the queue closes or `shutdown` fires, then
/// flushes the already-enqueued tail before returning.
async fn receipt_writer_loop(
    mut rx: mpsc::Receiver<DownloadReceipt>,
    log: Arc<dyn ReceiptLog>,
    shutdown: CancellationToken,
) {
    loop {
        tokio::select! {
            biased;
            maybe = rx.recv() => match maybe {
                Some(receipt) => append_one(&log, receipt).await,
                // All sinks dropped — nothing more can be enqueued.
                None => break,
            },
            () = shutdown.cancelled() => break,
        }
    }
    // Tail-drain: flush every receipt already enqueued before exiting so the
    // audit tail is not lost on shutdown — the audit-tail guarantee (CLAUDE.md /
    // ADR 003). `try_recv` yields `Empty` once
    // the buffer is drained (and `Disconnected` if the senders are gone); either
    // ends the drain.
    while let Ok(receipt) = rx.try_recv() {
        append_one(&log, receipt).await;
    }
    tracing::debug!("download-receipt writer drained and stopped");
}

/// Append one receipt on the blocking pool, logging a non-fatal failure.
///
/// Offloaded to [`tokio::task::spawn_blocking`] so the synchronous
/// `write_all`/`flush` (and any disk stall under a full or slow `data_dir`)
/// runs on the blocking pool, never on a runtime worker. The single writer
/// awaits each append before the next, preserving receipt order. An append
/// error is non-fatal — the payment already advanced the lane watermark — so it
/// is logged at `warn` and the loop continues.
async fn append_one(log: &Arc<dyn ReceiptLog>, receipt: DownloadReceipt) {
    let log = Arc::clone(log);
    let join = tokio::task::spawn_blocking(move || {
        let res = log.append(&receipt);
        (res, receipt)
    })
    .await;
    // A `JoinError` means the blocking append panicked (`spawn_blocking` tasks
    // are not cancellable, so a panic is the only way here). The receipt was
    // moved into the panicked closure, so its fields are gone — but the loss
    // still gets a structured line, distinct from the normal write-failure path
    // below, rather than relying on the default panic hook's unstructured stderr.
    let (res, receipt) = match join {
        Ok(pair) => pair,
        Err(join_err) => {
            tracing::warn!(
                error = %join_err,
                event = "download_receipt_writer_join_error",
                "download-receipt append task panicked; one audit receipt lost \
                 (payment already committed, audit log only)"
            );
            return;
        }
    };
    if let Err(e) = res {
        tracing::warn!(
            hash = receipt.hash(),
            client_node_id = receipt.client_node_id(),
            voucher_amount = receipt.voucher_amount(),
            error = %e,
            event = "download_receipt_write_failed",
            "failed to append download receipt; payment already committed (audit log only)"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader};
    use tempfile::TempDir;

    fn data_dir() -> std::io::Result<TempDir> {
        let dir = TempDir::new()?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700))?;
        }
        Ok(dir)
    }

    fn sample(byte: u8) -> DownloadReceipt {
        DownloadReceipt::new(
            &Hash::from_bytes([byte; 32]),
            u64::from(byte) * 1024,
            &[byte ^ 0xff; 32],
            U256::from(byte),
            1_700_000_000 + u64::from(byte),
        )
    }

    /// In-memory [`ReceiptLog`] for the writer tests: records every appended
    /// receipt and (optionally) returns an error for any whose `size` is in
    /// `fail_sizes`, without recording it — so a test can prove the writer loop
    /// continues past a non-fatal append error.
    #[derive(Default)]
    struct RecordingLog {
        seen: Mutex<Vec<DownloadReceipt>>,
        fail_sizes: std::collections::HashSet<u64>,
    }

    impl RecordingLog {
        fn snapshot(&self) -> Vec<DownloadReceipt> {
            self.seen.lock().map(|g| g.clone()).unwrap_or_default()
        }
    }

    impl ReceiptLog for RecordingLog {
        fn append(&self, receipt: &DownloadReceipt) -> std::io::Result<()> {
            if self.fail_sizes.contains(&receipt.size()) {
                return Err(std::io::Error::other("simulated receipt write failure"));
            }
            self.seen
                .lock()
                .map_err(|_| std::io::Error::other("RecordingLog mutex poisoned"))?
                .push(receipt.clone());
            Ok(())
        }
    }

    /// The writer drains every queued receipt FIFO and stops cleanly once the
    /// shutdown token is cancelled — the audit-tail-preservation guarantee (#803).
    #[tokio::test]
    async fn writer_drains_all_queued_receipts_then_stops_on_cancel() -> anyhow::Result<()> {
        let log = Arc::new(RecordingLog::default());
        let metrics = Arc::new(Metrics::new());
        let token = CancellationToken::new();
        let (sink, handle) = spawn_receipt_writer(
            Arc::clone(&log) as Arc<dyn ReceiptLog>,
            metrics,
            token.clone(),
        );
        for tag in 0..8u8 {
            sink.record(sample(tag));
        }
        token.cancel();
        handle.await?;
        let seen = log.snapshot();
        anyhow::ensure!(
            seen.len() == 8,
            "expected 8 drained receipts, got {}",
            seen.len()
        );
        for (i, tag) in (0..8u8).enumerate() {
            anyhow::ensure!(
                seen.get(i) == Some(&sample(tag)),
                "receipt {i} out of FIFO order"
            );
        }
        Ok(())
    }

    /// A non-fatal append error does not stop the writer: receipts after the
    /// failing one are still recorded.
    #[tokio::test]
    async fn writer_continues_past_a_failing_append() -> anyhow::Result<()> {
        let log = Arc::new(RecordingLog {
            // sample(2).size() == 2 * 1024; that append errors and is skipped.
            fail_sizes: std::collections::HashSet::from([2 * 1024]),
            ..RecordingLog::default()
        });
        let metrics = Arc::new(Metrics::new());
        let token = CancellationToken::new();
        let (sink, handle) = spawn_receipt_writer(
            Arc::clone(&log) as Arc<dyn ReceiptLog>,
            metrics,
            token.clone(),
        );
        for tag in 0..5u8 {
            sink.record(sample(tag));
        }
        token.cancel();
        handle.await?;
        let seen = log.snapshot();
        let sizes: Vec<u64> = seen.iter().map(DownloadReceipt::size).collect();
        anyhow::ensure!(
            sizes == vec![0, 1024, 3 * 1024, 4 * 1024],
            "the failing append should be skipped but the rest recorded: {sizes:?}"
        );
        Ok(())
    }

    /// The production sink drops on a full queue and counts the drop, never
    /// blocking the caller (the paid-delivery hot path, #803).
    #[test]
    fn channel_sink_drops_and_counts_on_full_queue() -> anyhow::Result<()> {
        let metrics = Arc::new(Metrics::new());
        // Capacity-1 channel with a parked (never-polled) receiver kept alive so
        // the second `record` sees `Full`, not `Closed`.
        let (tx, _rx) = mpsc::channel(1);
        let sink = ChannelReceiptSink {
            tx,
            metrics: Arc::clone(&metrics),
        };
        sink.record(sample(1)); // fills the single slot
        sink.record(sample(2)); // full → dropped + counted
        let text = metrics.encode()?;
        anyhow::ensure!(
            text.lines()
                .any(|l| l == "decdn_receipt_writes_dropped_total 1"),
            "expected exactly one dropped receipt counted:\n{text}"
        );
        Ok(())
    }

    /// A `Closed` channel (writer gone) drops the receipt but is NOT counted —
    /// it is expected during shutdown and must not produce false alerting noise,
    /// mirroring the redeem-hint `Closed` rationale (#751).
    #[test]
    fn channel_sink_closed_drops_without_counting() -> anyhow::Result<()> {
        let metrics = Arc::new(Metrics::new());
        // Drop the receiver so the channel is closed.
        let (tx, rx) = mpsc::channel(4);
        drop(rx);
        let sink = ChannelReceiptSink {
            tx,
            metrics: Arc::clone(&metrics),
        };
        sink.record(sample(1)); // closed → dropped, uncounted
        let text = metrics.encode()?;
        anyhow::ensure!(
            text.lines()
                .any(|l| l == "decdn_receipt_writes_dropped_total 0"),
            "a `Closed` drop must not increment the drop counter:\n{text}"
        );
        Ok(())
    }

    /// A rotation policy whose cap is large enough that the existing
    /// non-rotation tests never rotate. Uses the engine's wider (test-only)
    /// `new` domain — a `u64::MAX` cap is above the production ceiling, which is
    /// exactly why `new` is gated to tests and production goes through
    /// `From<&ResolvedReceipts>`.
    fn no_rotate() -> RotationPolicy {
        RotationPolicy::new(u64::MAX, 4)
    }

    /// A receipt whose serialized line is a fixed width regardless of `tag`
    /// (constant `size`/`nonce`/`timestamp`; only the fixed-width hex `hash` /
    /// `client_node_id` vary). Rotation tests rely on every line being the same
    /// length so the byte cap is deterministic.
    fn fixed(tag: u8) -> DownloadReceipt {
        DownloadReceipt::new(
            &Hash::from_bytes([tag; 32]),
            4096,
            &[tag ^ 0xff; 32],
            U256::from(42u8),
            1_700_000_000,
        )
    }

    /// Byte length of a serialized receipt line (including the trailing `\n`).
    fn line_len(r: &DownloadReceipt) -> anyhow::Result<u64> {
        let mut s = serde_json::to_string(r)?;
        s.push('\n');
        Ok(u64::try_from(s.len())?)
    }

    #[test]
    fn new_renders_hash_and_node_id_as_lower_hex() {
        let r = DownloadReceipt::new(
            &Hash::from_bytes([0xab; 32]),
            42,
            &[0x01; 32],
            U256::from(7u8),
            123,
        );
        assert_eq!(r.hash(), "ab".repeat(32));
        assert_eq!(r.client_node_id(), "01".repeat(32));
        assert_eq!(r.size(), 42);
        assert_eq!(r.voucher_amount(), "7");
        assert_eq!(r.timestamp_secs(), 123);
    }

    /// Lock the on-disk JSONL wire format: the serialized line for a known input
    /// must be byte-identical to the original schema (lower-hex `hash` /
    /// `client_node_id`, decimal-uint256 `voucher_amount` string, Unix-seconds
    /// `timestamp` key, `size` u64). This guards the private-field +
    /// `#[serde(rename = "timestamp")]` refactor against any wire drift.
    #[test]
    fn serialized_line_is_wire_stable() -> anyhow::Result<()> {
        let r = DownloadReceipt::new(
            &Hash::from_bytes([0xab; 32]),
            4096,
            &[0x01; 32],
            U256::from(42u8),
            1_700_000_000,
        );
        let line = serde_json::to_string(&r)?;
        let expected = format!(
            "{{\"hash\":\"{}\",\"size\":4096,\"client_node_id\":\"{}\",\"voucher_amount\":\"42\",\"timestamp\":1700000000}}",
            "ab".repeat(32),
            "01".repeat(32),
        );
        anyhow::ensure!(
            line == expected,
            "wire format drifted:\n got: {line}\nwant: {expected}"
        );
        Ok(())
    }

    #[test]
    fn append_then_read_back_round_trips() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let log = JsonlReceiptLog::open(dir.path(), no_rotate())?;
        let a = sample(1);
        let b = sample(2);
        log.append(&a)?;
        log.append(&b)?;

        let parsed = read_receipts(log.path())?;
        anyhow::ensure!(
            parsed.len() == 2,
            "expected 2 receipts, got {}",
            parsed.len()
        );
        anyhow::ensure!(parsed.first() == Some(&a));
        anyhow::ensure!(parsed.get(1) == Some(&b));
        Ok(())
    }

    #[test]
    fn appends_survive_reopen() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let a = sample(3);
        let b = sample(4);
        {
            let log = JsonlReceiptLog::open(dir.path(), no_rotate())?;
            log.append(&a)?;
        } // drop closes the handle
        {
            // Reopen the SAME data_dir: the prior line must persist and the new
            // line must append after it (not truncate).
            let log = JsonlReceiptLog::open(dir.path(), no_rotate())?;
            log.append(&b)?;
        }
        let parsed = read_receipts(&dir.path().join(RECEIPT_LOG_FILE))?;
        anyhow::ensure!(
            parsed == vec![a, b],
            "receipts did not survive reopen: {parsed:?}"
        );
        Ok(())
    }

    #[test]
    #[cfg(unix)]
    fn open_tightens_file_mode_to_0600() -> anyhow::Result<()> {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = data_dir()?;
        let log = JsonlReceiptLog::open(dir.path(), no_rotate())?;
        log.append(&sample(5))?;
        let mode = std::fs::metadata(log.path())?.permissions().mode() & 0o777;
        anyhow::ensure!(mode == 0o600, "expected 0o600, got {mode:o}");
        Ok(())
    }

    /// Each line that would exceed the cap rotates the live file first, so the
    /// live file holds exactly one record, the newest `retained_files` backups
    /// are kept (`.1` newest), and the oldest is dropped.
    #[test]
    fn rotates_and_retains_bounded_backups() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let cap = line_len(&fixed(0))?; // one line per file
        let log = open_log(dir.path(), cap, 3)?;
        // r0..r4: r0 lands in the live file, each later append rotates first.
        for tag in 0..5u8 {
            log.append(&fixed(tag))?;
        }
        // Live file holds only the newest record.
        anyhow::ensure!(
            read_receipts(log.path())? == vec![fixed(4)],
            "live file not rotated"
        );
        // Newest three backups kept: .1=r3, .2=r2, .3=r1; oldest (r0) dropped.
        anyhow::ensure!(read_receipts(&log.backup_path(1))? == vec![fixed(3)]);
        anyhow::ensure!(read_receipts(&log.backup_path(2))? == vec![fixed(2)]);
        anyhow::ensure!(read_receipts(&log.backup_path(3))? == vec![fixed(1)]);
        anyhow::ensure!(
            !log.backup_path(4).exists(),
            "retention cap exceeded: a 4th backup exists"
        );
        Ok(())
    }

    /// A single line larger than the cap still writes (after one rotation)
    /// instead of looping or being dropped.
    #[test]
    fn oversized_line_writes_after_single_rotation() -> anyhow::Result<()> {
        let dir = data_dir()?;
        // Cap below any real line length.
        let log = open_log(dir.path(), 1, 2)?;
        log.append(&fixed(1))?;
        log.append(&fixed(2))?;
        anyhow::ensure!(
            read_receipts(log.path())? == vec![fixed(2)],
            "second line missing"
        );
        anyhow::ensure!(
            read_receipts(&log.backup_path(1))? == vec![fixed(1)],
            "first line lost"
        );
        Ok(())
    }

    /// `retained_files == 0` truncates the live file in place — no backups.
    #[test]
    fn zero_retained_truncates_in_place() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let cap = line_len(&fixed(0))?;
        let log = open_log(dir.path(), cap, 0)?;
        log.append(&fixed(1))?;
        log.append(&fixed(2))?;
        anyhow::ensure!(
            read_receipts(log.path())? == vec![fixed(2)],
            "live file not truncated"
        );
        anyhow::ensure!(
            !log.backup_path(1).exists(),
            "retained_files == 0 must not leave a backup"
        );
        Ok(())
    }

    /// Reopen seeds the rotation counter from the existing file length, so a log
    /// already at the cap rotates on the next append (the pre-reopen line ends
    /// up in `.1`).
    #[test]
    fn reopen_seeds_written_from_file_length() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let cap = line_len(&fixed(0))?;
        {
            let log = open_log(dir.path(), cap, 2)?;
            log.append(&fixed(1))?;
        } // drop closes the handle; file holds one cap-filling line
        let log = open_log(dir.path(), cap, 2)?;
        log.append(&fixed(2))?;
        anyhow::ensure!(
            read_receipts(log.path())? == vec![fixed(2)],
            "reopen did not rotate"
        );
        anyhow::ensure!(
            read_receipts(&log.backup_path(1))? == vec![fixed(1)],
            "pre-reopen line should have rotated into .1"
        );
        Ok(())
    }

    /// Lowering `retained_files` between runs prunes the now-stale backups on
    /// the next rotation, so disk converges to the smaller bound (#802 review).
    #[test]
    fn lowering_retained_files_prunes_stale_backups() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let cap = line_len(&fixed(0))?;
        {
            // Build up .1..=.4 under a generous retention.
            let log = open_log(dir.path(), cap, 4)?;
            for tag in 0..=5 {
                log.append(&fixed(tag))?;
            }
            anyhow::ensure!(log.backup_path(4).exists(), "setup: .4 should exist");
        }
        // Reopen with a tighter retention and rotate once.
        let log = open_log(dir.path(), cap, 1)?;
        log.append(&fixed(6))?;
        anyhow::ensure!(
            log.backup_path(1).exists(),
            "the single retained backup should survive"
        );
        for stale in 2..=4 {
            anyhow::ensure!(
                !log.backup_path(stale).exists(),
                "stale backup .{stale} should have been pruned"
            );
        }
        Ok(())
    }

    /// The freshly-opened live file and the rotated backups all keep mode
    /// `0o600` after rotation.
    #[test]
    #[cfg(unix)]
    fn rotation_preserves_file_mode_0600() -> anyhow::Result<()> {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = data_dir()?;
        let cap = line_len(&fixed(0))?;
        let log = open_log(dir.path(), cap, 2)?;
        for tag in 0..3u8 {
            log.append(&fixed(tag))?;
        }
        for p in [
            log.path().to_path_buf(),
            log.backup_path(1),
            log.backup_path(2),
        ] {
            let mode = std::fs::metadata(&p)?.permissions().mode() & 0o777;
            anyhow::ensure!(
                mode == 0o600,
                "expected 0o600 for {}, got {mode:o}",
                p.display()
            );
        }
        Ok(())
    }

    /// The PR's central best-effort guarantee (#802): when `rotate()` fails, the
    /// append must NOT be aborted — the receipt still lands in the live file and
    /// `append` returns `Ok` — and the live handle must remain writable at the
    /// canonical path so a *later* append rotates successfully (the same
    /// "preserves a usable canonical-path handle" invariant the reopen rollback
    /// in `rotate` upholds). We force a rotation failure by squatting a directory
    /// on the `.1` backup path so the internal `remove_file`/rename errors out.
    #[test]
    #[cfg(unix)]
    fn rotation_failure_still_appends_and_stays_writable() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let cap = line_len(&fixed(0))?;
        let log = open_log(dir.path(), cap, 1)?;
        log.append(&fixed(0))?; // fills the live file to the cap
        // A directory at `.1` makes the rotation step that reclaims `.1` error
        // (`remove_file` on a directory fails with a non-NotFound error).
        std::fs::create_dir(log.backup_path(1))?;
        // Would rotate first; rotation fails, but the append must still succeed
        // against the (now over-cap) live file rather than drop the record.
        log.append(&fixed(1))?;
        anyhow::ensure!(
            read_receipts(log.path())? == vec![fixed(0), fixed(1)],
            "both receipts must remain in the live file when rotation fails"
        );
        // Clear the squatter; the next append must rotate cleanly, proving the
        // live handle stayed at the canonical path and writable across the
        // failure (i.e. no handle was lost mid-rotation).
        std::fs::remove_dir(log.backup_path(1))?;
        log.append(&fixed(2))?;
        anyhow::ensure!(
            read_receipts(log.path())? == vec![fixed(2)],
            "live file should rotate cleanly once the failure is cleared"
        );
        anyhow::ensure!(
            read_receipts(&log.backup_path(1))? == vec![fixed(0), fixed(1)],
            "the pre-failure records should have rotated into .1"
        );
        Ok(())
    }

    /// A cap that holds several lines exercises the rotation-trigger arithmetic
    /// (`written + line > max_file_bytes`) at a real boundary — the one-line-cap
    /// tests above cannot distinguish `>` from `>=`. With a 3-line cap the live
    /// file fills to exactly three records before the fourth append rotates.
    #[test]
    fn rotates_after_filling_a_multi_line_file() -> anyhow::Result<()> {
        let dir = data_dir()?;
        let l = line_len(&fixed(0))?;
        let log = open_log(dir.path(), l * 3, 2)?;
        for tag in 0..4u8 {
            log.append(&fixed(tag))?;
        }
        // Exactly three records filled the first file (now `.1`); the fourth
        // opened a fresh live file. A `>=` boundary bug would rotate one line
        // early, leaving two records per file.
        anyhow::ensure!(
            read_receipts(&log.backup_path(1))? == vec![fixed(0), fixed(1), fixed(2)],
            "first file should hold exactly three records before rotating"
        );
        anyhow::ensure!(
            read_receipts(log.path())? == vec![fixed(3)],
            "fourth record should be alone in the fresh live file"
        );
        Ok(())
    }

    fn open_log(
        dir: &Path,
        max_file_bytes: u64,
        retained_files: u32,
    ) -> std::io::Result<JsonlReceiptLog> {
        JsonlReceiptLog::open(dir, RotationPolicy::new(max_file_bytes, retained_files))
    }

    fn read_receipts(path: &Path) -> anyhow::Result<Vec<DownloadReceipt>> {
        let reader = BufReader::new(File::open(path)?);
        let mut out = Vec::new();
        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            out.push(serde_json::from_str::<DownloadReceipt>(&line)?);
        }
        Ok(out)
    }
}
