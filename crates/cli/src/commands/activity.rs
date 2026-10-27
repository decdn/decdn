//! The record each `decdn get` run keeps of itself, for `decdn dashboard`.
//!
//! A run writes one JSON file, [`PullRecord`], under [`pulls_dir`]: the same
//! directory whatever `--data-dir` the run uses, so one dashboard sees every
//! run on the host. The file is rewritten whole, through a temp file and a
//! rename, once a second while the run is live and once more when it ends.
//! A reader never sees a torn record.
//!
//! The record is a view, never state a pull depends on: a record that cannot
//! be written is logged and the pull goes on. A run that exits without its
//! last write (a second Ctrl-C exits at once) leaves a `running` record whose
//! heartbeat stops; [`PullRecord::is_stale`] tells a reader so.

use std::collections::HashMap;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Context as _;
use decdn_client::GaveUp;
use decdn_client::bundle::{BundleReport, FileObserver};
use decdn_client::receipt::{ReceiptAmount, ReceiptEvidence, ReceiptLane, ReceiptSummary};
use decdn_client::uri::{DecdnUri, UriTarget};
use decdn_common::redact::sanitize_err_chain;
use serde::{Deserialize, Serialize};

use super::interrupt::Interrupted;

/// How often a live run rewrites its record.
const HEARTBEAT: Duration = Duration::from_secs(1);

/// How many heartbeats pass between two reads of what the run paid. The read
/// takes each lane's issuance lock, so it runs less often than the heartbeat.
const PAID_EVERY: u32 = 5;

/// A `running` record whose heartbeat is older than this belongs to a run that
/// exited without its last write.
pub(crate) const STALE_AFTER: Duration = Duration::from_secs(10);

/// How many records the directory keeps. A run prunes the oldest when it
/// starts.
const KEPT_RECORDS: usize = 200;

/// The directory every run writes its record to: `~/.decdn/activity/pulls`.
/// `None` when the home directory cannot be found.
pub(crate) fn pulls_dir() -> Option<PathBuf> {
    decdn_common::cli::common::default_data_dir().map(|d| d.join("activity").join("pulls"))
}

/// What a `decdn get` URI names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PullKind {
    /// One blob.
    Blob,
    /// A whole bundle.
    Bundle,
    /// One entry of a bundle.
    Entry,
}

/// Where a run is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PullState {
    /// The run is live, or exited without its last write.
    Running,
    /// The run succeeded.
    Done,
    /// The run failed.
    Failed,
    /// Ctrl-C stopped the run.
    Interrupted,
    /// The run gave up after its no-progress limit.
    GaveUp,
}

/// What a run paid, in total or to one node.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Paid {
    /// `µUSDC`.
    pub(crate) amount_micro_usdc: u64,
    /// The wire bytes the amount pays for.
    pub(crate) bytes: u64,
}

impl From<ReceiptAmount> for Paid {
    fn from(amount: ReceiptAmount) -> Self {
        Self {
            amount_micro_usdc: amount.amount,
            bytes: amount.bytes,
        }
    }
}

/// What one node was paid.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct NodePaid {
    /// The node's operator address, EIP-55.
    pub(crate) provider: String,
    /// What it was paid.
    #[serde(flatten)]
    pub(crate) paid: Paid,
}

/// A finished bundle run's entry counts and byte tallies, as its summary
/// prints them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct BundleCounts {
    /// Distinct blobs fetched.
    pub(crate) fetched: u64,
    /// Extra destinations of a blob already fetched.
    pub(crate) linked: u64,
    /// Entries already on disk.
    pub(crate) skipped: u64,
    /// Entries the filter or `--select` dropped.
    pub(crate) excluded: u64,
    /// Entries whose fetch failed.
    pub(crate) failed: u64,
    /// Entries the run ended before their fetch ended.
    pub(crate) not_run: u64,
    /// Content bytes downloaded.
    pub(crate) downloaded: u64,
    /// Content bytes earlier runs fetched and this one resumed.
    pub(crate) resumed_bytes: u64,
    /// Bytes reused from files already on disk, whole or spliced.
    pub(crate) reused_bytes: u64,
}

impl BundleCounts {
    /// The counts of `report`, beside the `excluded` entries the run did not
    /// pull.
    pub(crate) const fn of(report: &BundleReport, excluded: u64) -> Self {
        Self {
            fetched: report.fetched,
            linked: report.linked,
            skipped: report.skipped,
            excluded,
            failed: report.failed,
            not_run: report.not_run,
            downloaded: report.downloaded,
            resumed_bytes: report.resumed_bytes,
            reused_bytes: report.reused_bytes.saturating_add(report.spliced_bytes),
        }
    }
}

/// One `decdn get` run, as its record file holds it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PullRecord {
    /// The URI the run pulls.
    pub(crate) uri: String,
    /// What the URI names.
    pub(crate) kind: PullKind,
    /// Where the run writes: a file, a directory, or `-` for stdout.
    pub(crate) output: String,
    /// The run's process id.
    pub(crate) pid: u32,
    /// When the run started, Unix milliseconds.
    pub(crate) started_unix_ms: u64,
    /// The last write, Unix milliseconds: the run's heartbeat.
    pub(crate) updated_unix_ms: u64,
    /// When the run ended, Unix milliseconds; `None` while it runs.
    pub(crate) finished_unix_ms: Option<u64>,
    /// Where the run is.
    pub(crate) state: PullState,
    /// Content bytes verified so far.
    pub(crate) received_bytes: u64,
    /// The content bytes the run plans to fetch; `None` until known.
    pub(crate) expected_bytes: Option<u64>,
    /// Blobs a bundle run started.
    pub(crate) files_started: u64,
    /// Blobs a bundle run landed.
    pub(crate) files_landed: u64,
    /// What the run paid so far, over every node.
    pub(crate) paid: Paid,
    /// What each node was paid, merged per node.
    pub(crate) nodes: Vec<NodePaid>,
    /// A finished bundle run's counts.
    pub(crate) bundle: Option<BundleCounts>,
    /// Why the run failed, gave up or stopped.
    pub(crate) error: Option<String>,
}

impl PullRecord {
    /// Whether the record says `running` but its heartbeat stopped
    /// [`STALE_AFTER`] before `now_ms`.
    pub(crate) fn is_stale(&self, now_ms: u64) -> bool {
        self.state == PullState::Running
            && now_ms.saturating_sub(self.updated_unix_ms) > millis(STALE_AFTER)
    }
}

/// Keeps one run's record current. Every update is cheap and in memory; a
/// ticker writes the record once a [`HEARTBEAT`].
pub(crate) struct ActivityRecorder {
    path: PathBuf,
    record: Mutex<PullRecord>,
    received: AtomicU64,
    /// Each started blob's last reported position: a bundle blob reports its
    /// whole position on every fetch, so positions replace, never add.
    files: Mutex<HashMap<usize, u64>>,
    evidence: Arc<ReceiptEvidence>,
    ticker: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl ActivityRecorder {
    /// Start the record of a run of `uri` that writes to `output` and pays
    /// through `evidence`, in [`pulls_dir`]. `None`, with a log line, when the
    /// directory cannot be made.
    pub(crate) fn start(
        uri: &DecdnUri,
        uri_text: &str,
        output: &Path,
        evidence: &Arc<ReceiptEvidence>,
    ) -> Option<Arc<Self>> {
        let dir = pulls_dir()?;
        Self::start_in(&dir, uri, uri_text, output, evidence)
            .inspect_err(|e| tracing::warn!("no dashboard record for this run: {e:#}"))
            .ok()
    }

    /// [`Self::start`] in `dir`.
    pub(crate) fn start_in(
        dir: &Path,
        uri: &DecdnUri,
        uri_text: &str,
        output: &Path,
        evidence: &Arc<ReceiptEvidence>,
    ) -> anyhow::Result<Arc<Self>> {
        super::local_http::private_dir(dir)?;
        prune(dir, KEPT_RECORDS.saturating_sub(1));
        let now = now_ms();
        let pid = std::process::id();
        let kind = match uri.target {
            UriTarget::Blob => PullKind::Blob,
            UriTarget::BundleEntry(_) => PullKind::Entry,
            _ => PullKind::Bundle,
        };
        let recorder = Arc::new(Self {
            path: dir.join(format!("{now}-{pid}.json")),
            record: Mutex::new(PullRecord {
                uri: uri_text.to_owned(),
                kind,
                output: output.display().to_string(),
                pid,
                started_unix_ms: now,
                updated_unix_ms: now,
                finished_unix_ms: None,
                state: PullState::Running,
                received_bytes: 0,
                expected_bytes: None,
                files_started: 0,
                files_landed: 0,
                paid: Paid::default(),
                nodes: Vec::new(),
                bundle: None,
                error: None,
            }),
            received: AtomicU64::new(0),
            files: Mutex::new(HashMap::new()),
            evidence: Arc::clone(evidence),
            ticker: Mutex::new(None),
        });
        recorder.write()?;
        let ticking = Arc::clone(&recorder);
        *recorder
            .ticker
            .lock()
            .unwrap_or_else(PoisonError::into_inner) =
            Some(tokio::spawn(async move { ticking.tick().await }));
        Ok(recorder)
    }

    /// Rewrite the record every [`HEARTBEAT`], reading what the run paid
    /// every [`PAID_EVERY`] beats, until [`Self::finish`] stops it.
    async fn tick(self: Arc<Self>) {
        let mut interval = tokio::time::interval(HEARTBEAT);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut beat = 0u32;
        loop {
            interval.tick().await;
            if beat.is_multiple_of(PAID_EVERY) {
                let lanes = self.evidence.lanes().await;
                self.set_paid_from_lanes(&lanes);
            }
            beat = beat.wrapping_add(1);
            if let Err(e) = self.write() {
                tracing::debug!("dashboard record not written: {e:#}");
            }
        }
    }

    /// A blob run's position: `received` of `expected` content bytes.
    pub(crate) fn progress(&self, received: u64, expected: u64) {
        self.received.store(received, Ordering::Relaxed);
        self.update(|r| r.expected_bytes = Some(expected));
    }

    /// A bundle run starts on `download_total` content bytes.
    pub(crate) fn run_started(&self, download_total: Option<u64>) {
        self.update(|r| r.expected_bytes = download_total);
    }

    /// A bundle blob starts; the returned observer reports its position here
    /// and to `inner`.
    pub(crate) fn file_started(
        self: &Arc<Self>,
        inner: Arc<dyn FileObserver>,
    ) -> Arc<dyn FileObserver> {
        let mut slot = 0;
        self.update(|r| {
            slot = usize::try_from(r.files_started).unwrap_or(usize::MAX);
            r.files_started = r.files_started.saturating_add(1);
        });
        Arc::new(RecordedFile {
            recorder: Arc::clone(self),
            slot,
            inner,
        })
    }

    /// A bundle blob landed.
    pub(crate) fn file_landed(&self) {
        self.update(|r| r.files_landed = r.files_landed.saturating_add(1));
    }

    /// A finished bundle run's counts.
    pub(crate) fn bundle_counts(&self, counts: BundleCounts) {
        self.update(|r| r.bundle = Some(counts));
    }

    fn file_delivered(&self, slot: usize, received: u64) {
        let mut files = self.files.lock().unwrap_or_else(PoisonError::into_inner);
        files.insert(slot, received);
        let total = files.values().fold(0u64, |acc, n| acc.saturating_add(*n));
        self.received.store(total, Ordering::Relaxed);
    }

    /// End the record: the run's `result`, and what its receipt `summary`
    /// says it paid. A run with no receipt paid what its lanes say.
    pub(crate) async fn finish(
        &self,
        result: &anyhow::Result<()>,
        summary: Option<ReceiptSummary>,
    ) {
        if let Some(ticker) = self
            .ticker
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
        {
            ticker.abort();
        }
        match summary {
            Some(summary) => self.update(|r| {
                r.paid = summary.paid.into();
                r.nodes = summary
                    .per_provider
                    .iter()
                    .map(|(provider, paid)| NodePaid {
                        provider: provider.to_checksum(None),
                        paid: (*paid).into(),
                    })
                    .collect();
            }),
            None => self.set_paid_from_lanes(&self.evidence.lanes().await),
        }
        let now = now_ms();
        self.update(|r| {
            r.finished_unix_ms = Some(now);
            (r.state, r.error) = outcome(result);
        });
        if let Err(e) = self.write() {
            tracing::warn!("dashboard record not written: {e:#}");
        }
    }

    fn set_paid_from_lanes(&self, lanes: &[ReceiptLane]) {
        let mut nodes: Vec<NodePaid> = Vec::new();
        for lane in lanes {
            let paid = lane.paid();
            let provider = lane.provider.to_checksum(None);
            match nodes.iter_mut().find(|n| n.provider == provider) {
                Some(node) => {
                    node.paid.amount_micro_usdc =
                        node.paid.amount_micro_usdc.saturating_add(paid.amount);
                    node.paid.bytes = node.paid.bytes.saturating_add(paid.bytes);
                }
                None => nodes.push(NodePaid {
                    provider,
                    paid: paid.into(),
                }),
            }
        }
        let total = nodes.iter().fold(Paid::default(), |acc, n| Paid {
            amount_micro_usdc: acc
                .amount_micro_usdc
                .saturating_add(n.paid.amount_micro_usdc),
            bytes: acc.bytes.saturating_add(n.paid.bytes),
        });
        self.update(|r| {
            r.paid = total;
            r.nodes = nodes;
        });
    }

    fn update(&self, f: impl FnOnce(&mut PullRecord)) {
        f(&mut self.record.lock().unwrap_or_else(PoisonError::into_inner));
    }

    /// Write the record through a temp file in its directory and a rename.
    fn write(&self) -> anyhow::Result<()> {
        let record = {
            let mut record = self.record.lock().unwrap_or_else(PoisonError::into_inner);
            record.received_bytes = self.received.load(Ordering::Relaxed);
            record.updated_unix_ms = now_ms();
            record.clone()
        };
        let dir = self.path.parent().unwrap_or(Path::new("."));
        let mut tmp = tempfile::NamedTempFile::new_in(dir)
            .with_context(|| format!("create a temp file in {}", dir.display()))?;
        serde_json::to_writer(&mut tmp, &record).context("serialize the record")?;
        tmp.write_all(b"\n").context("write the record")?;
        tmp.persist(&self.path)
            .with_context(|| format!("write {}", self.path.display()))?;
        Ok(())
    }
}

/// The state and error a finished run's `result` records.
fn outcome(result: &anyhow::Result<()>) -> (PullState, Option<String>) {
    match result {
        Ok(()) => (PullState::Done, None),
        Err(e) if e.downcast_ref::<Interrupted>().is_some() => (PullState::Interrupted, None),
        Err(e) if e.downcast_ref::<GaveUp>().is_some() => {
            (PullState::GaveUp, Some(sanitize_err_chain(e)))
        }
        Err(e) => (PullState::Failed, Some(sanitize_err_chain(e))),
    }
}

/// A bundle blob's observer that also reports its position to the run's
/// record.
struct RecordedFile {
    recorder: Arc<ActivityRecorder>,
    slot: usize,
    inner: Arc<dyn FileObserver>,
}

impl FileObserver for RecordedFile {
    fn delivered(&self, received: u64, expected: u64) {
        self.recorder.file_delivered(self.slot, received);
        self.inner.delivered(received, expected);
    }

    fn discovering(&self) {
        self.inner.discovering();
    }

    fn pending(&self) {
        self.inner.pending();
    }

    fn reconstructing(&self) {
        self.inner.reconstructing();
    }

    fn hashed(&self, bytes: u64) {
        self.inner.hashed(bytes);
    }

    fn finished(&self) {
        self.inner.finished();
    }
}

/// Every record in `dir`, newest first, and how many files there did not
/// read as one.
pub(crate) fn read_records(dir: &Path) -> (Vec<PullRecord>, usize) {
    let mut records = Vec::new();
    let mut unreadable = 0;
    for path in record_files(dir) {
        match std::fs::read(&path)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<PullRecord>(&bytes).ok())
        {
            Some(record) => records.push(record),
            None => unreadable += 1,
        }
    }
    records.sort_by_key(|r| std::cmp::Reverse(r.started_unix_ms));
    (records, unreadable)
}

/// The record files in `dir`: every `*.json`. A temp file a writer has not
/// renamed yet has no such name.
fn record_files(dir: &Path) -> Vec<PathBuf> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    entries
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|ext| ext == "json"))
        .collect()
}

/// Delete all but the newest `keep` records in `dir`, by the start time
/// their names begin with. A file that cannot be deleted stays.
fn prune(dir: &Path, keep: usize) {
    let mut files: Vec<(u64, PathBuf)> = record_files(dir)
        .into_iter()
        .map(|p| (started_of(&p), p))
        .collect();
    if files.len() <= keep {
        return;
    }
    files.sort_by_key(|f| std::cmp::Reverse(f.0));
    for (_, path) in files.into_iter().skip(keep) {
        let _ = std::fs::remove_file(path);
    }
}

/// The start time a record's name begins with; `0` for a name that does not.
fn started_of(path: &Path) -> u64 {
    path.file_stem()
        .and_then(|s| s.to_str())
        .and_then(|s| s.split('-').next())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

/// The current Unix time in milliseconds; `0` on a clock set before 1970.
pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, millis)
}

fn millis(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests;
