//! Append-only download-receipt log for the node runtime (issue #248).
//!
//! A node records one structured [`DownloadReceipt`] per served-and-paid blob
//! at the voucher-acceptance point (see
//! [`crate::handlers::client::ClientHandler`]). The log gives the operator a
//! durable, off-chain record of which blobs were delivered and paid for —
//! enabling revenue audit, cross-restart double-spend triage, and dispute
//! evidence without relying solely on on-chain state.
//!
//! # Seam
//!
//! [`ReceiptLog`] is a small trait (mirroring the trait+impl pattern of
//! [`decdn_incentive::store::ChannelStateStore`] / [`crate::channel_store`]) so
//! the voucher-accept path can be unit-tested against an in-memory fake without
//! touching disk. The runtime wires the disk-backed
//! [`JsonlReceiptLog`], which appends one JSON object per line to
//! `<data_dir>/download_receipts.jsonl` (JSON Lines).
//!
//! # Durability
//!
//! Each [`ReceiptLog::append`] writes one line and flushes it to the OS
//! (`write_all` + `flush`) but does **not** fsync per record. The receipt log
//! is an *audit* artifact, not the #527 voucher-replay guard — the durable
//! anti-replay watermark is [`crate::channel_store::PersistentChannelStateStore`],
//! which fsyncs every accepted voucher *before* a receipt is ever written. A
//! crash that loses a not-yet-fsynced receipt tail therefore cannot reopen a
//! replay window; at worst the audit log trails the channel store by a few
//! entries. Skipping the per-line fsync keeps the cost off the hot delivery
//! path at the testnet scale this targets (see CLAUDE.md / ADR 003).
//!
//! The append is also crash-atomic at the line level only in the usual POSIX
//! sense (a torn final line is possible after a hard crash); JSONL readers MUST
//! tolerate a truncated trailing line.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use alloy::primitives::U256;
use iroh_blobs::Hash;
use serde::{Deserialize, Serialize};

/// File name of the receipt log within `data_dir`.
const RECEIPT_LOG_FILE: &str = "download_receipts.jsonl";

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
/// - `voucher_nonce` — the accepted voucher's sequence number within its
///   channel, as a decimal string. A decimal `uint256` (not hex) so an auditor
///   reads the same value the on-chain `PaymentChannel` and operator dashboards
///   show; it is reconstructed from the 32-byte big-endian wire nonce.
///
/// `size` is the byte count covered by *this* voucher interval (the newly
/// delivered, now-paid bytes), and `timestamp_secs` is the node's wall-clock
/// Unix time (seconds) at acceptance.
///
/// # Invariants
///
/// The fields are **private** and the only public constructor is [`new`], which
/// renders the hex and decimal-`uint256` strings from typed inputs. This makes
/// the invariants type-owned: a receipt cannot be built field-wise, and serde's
/// derived `Deserialize` cannot synthesize one with a malformed `hash` /
/// `client_node_id` / `voucher_nonce` from outside the crate (the fields stay
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
///     voucher_nonce: "0xdeadbeef".to_string(),
///     timestamp: 0,
/// };
/// ```
///
/// [`new`]: DownloadReceipt::new
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DownloadReceipt {
    /// BLAKE3 content hash of the delivered blob, lower-hex (64 chars).
    hash: String,
    /// Bytes covered by this voucher interval (the newly paid delivery).
    size: u64,
    /// iroh `NodeId` of the paying client, lower-hex (64 chars).
    client_node_id: String,
    /// Accepted voucher sequence number, decimal `uint256` string.
    voucher_nonce: String,
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
    /// - `voucher_nonce` is the accepted voucher's `uint256` nonce; the decimal
    ///   string is rendered here so the "decimal uint256" invariant is owned by
    ///   the type, not the caller.
    #[must_use]
    pub fn new(
        hash: &Hash,
        size: u64,
        client_node_id_bytes: &[u8; 32],
        voucher_nonce: U256,
        timestamp_secs: u64,
    ) -> Self {
        Self {
            hash: hex_lower(hash.as_bytes()),
            size,
            client_node_id: hex_lower(client_node_id_bytes),
            voucher_nonce: voucher_nonce.to_string(),
            timestamp_secs,
        }
    }

    /// BLAKE3 content hash of the delivered blob, lower-hex (64 chars, no `0x`).
    #[must_use]
    pub fn hash(&self) -> &str {
        &self.hash
    }

    /// Bytes covered by this voucher interval (the newly paid delivery).
    #[must_use]
    pub const fn size(&self) -> u64 {
        self.size
    }

    /// iroh `NodeId` of the paying client, lower-hex (64 chars, no `0x`).
    #[must_use]
    pub fn client_node_id(&self) -> &str {
        &self.client_node_id
    }

    /// Accepted voucher sequence number, decimal `uint256` string.
    #[must_use]
    pub fn voucher_nonce(&self) -> &str {
        &self.voucher_nonce
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
    /// already committed to the fsynced channel store) and logs it.
    fn append(&self, receipt: &DownloadReceipt) -> std::io::Result<()>;
}

/// JSON-Lines, append-only [`ReceiptLog`] backed by a single file under
/// `data_dir`.
///
/// One [`File`] handle is held open for the process lifetime behind a [`Mutex`]
/// so concurrent delivery streams cannot interleave partial lines. The handle
/// is opened in append mode (`O_APPEND`), so even across the (already
/// serialized) writes each `write_all` lands at the current end of file.
#[derive(Debug)]
pub struct JsonlReceiptLog {
    file: Mutex<File>,
    path: PathBuf,
}

impl JsonlReceiptLog {
    /// Open (or create) the receipt log under `data_dir`.
    ///
    /// The file is opened in append mode and, on Unix, created with mode
    /// `0o600`; an existing file's mode is tightened to `0o600` as
    /// defense-in-depth (idempotent — skipped when already correct, so a
    /// read-only mount with the right mode does not brick startup).
    ///
    /// Unlike [`crate::channel_store::PersistentChannelStateStore::open`], a
    /// failure here is **not** required to abort node bring-up: the receipt log
    /// is an audit artifact, not the #527 replay guard. The runtime decides the
    /// fatality (it currently logs and continues without the audit log).
    ///
    /// # Errors
    ///
    /// Returns a [`std::io::Error`] if the file cannot be opened/created or its
    /// mode cannot be tightened.
    pub fn open(data_dir: &Path) -> std::io::Result<Self> {
        let path = data_dir.join(RECEIPT_LOG_FILE);
        let mut opts = OpenOptions::new();
        opts.create(true).append(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            opts.mode(RECEIPT_LOG_FILE_MODE);
        }
        let file = opts.open(&path)?;
        #[cfg(unix)]
        tighten_permissions(&path)?;
        Ok(Self {
            file: Mutex::new(file),
            path,
        })
    }

    /// Filesystem path of the underlying log file. Useful for operator log
    /// lines and runbooks.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Tighten the on-disk file mode to `0o600`. Idempotent: skips the syscall when
/// the mode already matches so a read-only mount with the correct mode (e.g.
/// from a prior boot) does not fail startup. Mirrors
/// [`crate::channel_store::PersistentChannelStateStore`]'s tightening.
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
        let mut guard = self
            .file
            .lock()
            .map_err(|_| std::io::Error::other("receipt log mutex poisoned"))?;
        guard.write_all(line.as_bytes())?;
        // Flush to the OS so the line survives a clean process exit; no fsync —
        // see the module-level durability note.
        guard.flush()
    }
}

/// A [`ReceiptLog`] that drops every receipt. Used as the runtime fallback when
/// [`JsonlReceiptLog::open`] fails at bring-up: the audit log is best-effort, so
/// the node serves paid delivery without it rather than refusing to start (the
/// #527 replay guard lives in the separate, fsynced channel store). The
/// operator gets one `error!` at startup naming the open failure.
#[derive(Debug, Default)]
pub struct NoopReceiptLog;

impl ReceiptLog for NoopReceiptLog {
    fn append(&self, _receipt: &DownloadReceipt) -> std::io::Result<()> {
        Ok(())
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
        assert_eq!(r.voucher_nonce(), "7");
        assert_eq!(r.timestamp_secs(), 123);
    }

    /// Lock the on-disk JSONL wire format: the serialized line for a known input
    /// must be byte-identical to the original schema (lower-hex `hash` /
    /// `client_node_id`, decimal-uint256 `voucher_nonce` string, Unix-seconds
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
            "{{\"hash\":\"{}\",\"size\":4096,\"client_node_id\":\"{}\",\"voucher_nonce\":\"42\",\"timestamp\":1700000000}}",
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
        let log = JsonlReceiptLog::open(dir.path())?;
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
            let log = JsonlReceiptLog::open(dir.path())?;
            log.append(&a)?;
        } // drop closes the handle
        {
            // Reopen the SAME data_dir: the prior line must persist and the new
            // line must append after it (not truncate).
            let log = JsonlReceiptLog::open(dir.path())?;
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
        let log = JsonlReceiptLog::open(dir.path())?;
        log.append(&sample(5))?;
        let mode = std::fs::metadata(log.path())?.permissions().mode() & 0o777;
        anyhow::ensure!(mode == 0o600, "expected 0o600, got {mode:o}");
        Ok(())
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
