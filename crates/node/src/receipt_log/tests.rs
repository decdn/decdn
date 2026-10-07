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

/// The [`RawReceipt`] the delivery path enqueues, mirroring [`sample`]'s
/// identifiers minus the timestamp (the background writer stamps that). Used
/// by the sink-level tests, which feed the seam a raw receipt.
fn raw_sample(byte: u8) -> RawReceipt {
    RawReceipt::new(
        Hash::from_bytes([byte; 32]),
        u64::from(byte) * 1024,
        [byte ^ 0xff; 32],
        U256::from(byte),
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
    let (sink, handle) = spawn_receipt_writer(Arc::clone(&log) as Arc<dyn ReceiptLog>, metrics);
    for tag in 0..8u8 {
        sink.record(raw_sample(tag));
    }
    handle.shutdown().await?;
    let seen = log.snapshot();
    anyhow::ensure!(
        seen.len() == 8,
        "expected 8 drained receipts, got {}",
        seen.len()
    );
    // The writer stamps the timestamp itself, so identity is checked on the
    // fields the raw receipt carried through the render, ignoring the
    // timestamp value; `sample(tag)` is the same identity with a fixed stamp.
    for (i, tag) in (0..8u8).enumerate() {
        let Some(got) = seen.get(i) else {
            anyhow::bail!("missing drained receipt at {i}");
        };
        let want = sample(tag);
        anyhow::ensure!(
            got.size() == want.size()
                && got.hash() == want.hash()
                && got.client_node_id() == want.client_node_id()
                && got.voucher_amount() == want.voucher_amount(),
            "receipt {i} identity/order not preserved through render: {got:?}"
        );
        anyhow::ensure!(
            got.timestamp_secs() > 0,
            "the writer must stamp a wall-clock timestamp on receipt {i}"
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
    let (sink, handle) = spawn_receipt_writer(
        Arc::clone(&log) as Arc<dyn ReceiptLog>,
        Arc::clone(&metrics),
    );
    for tag in 0..5u8 {
        sink.record(raw_sample(tag));
    }
    handle.shutdown().await?;
    let seen = log.snapshot();
    let sizes: Vec<u64> = seen.iter().map(DownloadReceipt::size).collect();
    anyhow::ensure!(
        sizes == vec![0, 1024, 3 * 1024, 4 * 1024],
        "the failing append should be skipped but the rest recorded: {sizes:?}"
    );
    // The one failed append is counted.
    let text = metrics.encode()?;
    anyhow::ensure!(
        text.lines()
            .any(|l| l == "decdn_receipt_write_failures_total 1"),
        "{text}"
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
    sink.record(raw_sample(1)); // fills the single slot
    sink.record(raw_sample(2)); // full → dropped + counted
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
    sink.record(raw_sample(1)); // closed → dropped, uncounted
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
