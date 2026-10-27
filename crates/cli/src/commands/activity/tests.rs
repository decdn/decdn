use std::str::FromStr as _;

use super::*;

fn uri(text: &str) -> DecdnUri {
    DecdnUri::from_str(text).unwrap()
}

const BLOB: &str = "decdn:af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262";

fn record(started: u64, state: PullState) -> PullRecord {
    PullRecord {
        uri: BLOB.to_owned(),
        kind: PullKind::Blob,
        output: "out".to_owned(),
        pid: 1,
        started_unix_ms: started,
        updated_unix_ms: started,
        state,
        received_bytes: 0,
        expected_bytes: None,
        paid: Paid::default(),
        nodes: Vec::new(),
    }
}

/// A bundle record's file progress and counts.
fn bundle_of(record: &PullRecord) -> (u64, u64, Option<BundleCounts>) {
    match record.kind {
        PullKind::Bundle {
            files_started,
            files_landed,
            counts,
        } => Some((files_started, files_landed, counts)),
        _ => None,
    }
    .expect("a bundle record")
}

fn write(dir: &Path, record: &PullRecord) {
    let path = dir.join(format!("{}-{}.json", record.started_unix_ms, record.pid));
    std::fs::write(path, serde_json::to_vec(record).unwrap()).unwrap();
}

fn done() -> PullState {
    PullState::Done {
        finished_unix_ms: 9,
    }
}

fn start(dir: &Path, text: &str) -> Arc<ActivityRecorder> {
    let evidence = Arc::new(ReceiptEvidence::new());
    ActivityRecorder::start_in(dir, &uri(text), text, Path::new("out"), &evidence).unwrap()
}

fn only(dir: &Path) -> PullRecord {
    let (mut records, unreadable) = read_records(dir).unwrap();
    assert_eq!((records.len(), unreadable), (1, 0), "{records:?}");
    records.remove(0)
}

struct Quiet;

impl FileObserver for Quiet {}

#[tokio::test]
async fn a_run_writes_its_record_and_ends_it() {
    let dir = tempfile::tempdir().unwrap();
    let recorder = start(dir.path(), BLOB);
    let running = only(dir.path());
    assert_eq!(running.state, PullState::Running);
    assert_eq!(running.kind, PullKind::Blob);

    recorder.progress(40, 100);
    recorder.finish(&Ok(()), None).await;
    let ended = only(dir.path());
    assert!(
        matches!(ended.state, PullState::Done { finished_unix_ms } if finished_unix_ms > 0),
        "{ended:?}"
    );
    assert_eq!(ended.received_bytes, 40);
    assert_eq!(ended.expected_bytes, Some(100));
}

/// A running record carries no end, and an ended one its end beside the
/// state, as the reader on the other side of the file sees them.
#[test]
fn the_state_carries_its_end_in_the_record() {
    let running = serde_json::to_value(record(1, PullState::Running)).unwrap();
    assert_eq!(running["state"], "running");
    assert!(running.get("finished_unix_ms").is_none(), "{running}");
    assert!(running.get("error").is_none(), "{running}");
    let failed = serde_json::to_value(record(
        1,
        PullState::Failed {
            finished_unix_ms: 7,
            error: "no node".to_owned(),
        },
    ))
    .unwrap();
    assert_eq!(failed["state"], "failed");
    assert_eq!(failed["finished_unix_ms"], 7);
    assert_eq!(failed["error"], "no node");
    let back: PullRecord = serde_json::from_value(failed).unwrap();
    assert!(matches!(back.state, PullState::Failed { .. }));
}

#[test]
fn every_end_records_its_state() {
    let gave_up: anyhow::Error = GaveUp {
        idle: Duration::from_secs(1),
    }
    .into();
    let interrupted = anyhow::Error::from(Interrupted).context("pull the bundle");
    assert!(matches!(
        PullState::ended(&Ok(()), 3),
        PullState::Done {
            finished_unix_ms: 3
        }
    ));
    assert!(matches!(
        PullState::ended(&Err(interrupted), 3),
        PullState::Interrupted { .. }
    ));
    assert!(matches!(
        PullState::ended(&Err(gave_up), 3),
        PullState::GaveUp { ref error, .. } if !error.is_empty()
    ));
    assert!(matches!(
        PullState::ended(&Err(anyhow::anyhow!("no node holds it")), 3),
        PullState::Failed { ref error, .. } if error == "no node holds it"
    ));
}

#[tokio::test]
async fn a_bundle_blob_reports_its_whole_position_not_a_delta() {
    let dir = tempfile::tempdir().unwrap();
    let bundle = format!("{BLOB}/");
    let recorder = start(dir.path(), &bundle);
    let a = recorder.file_started(&["a.bin".to_owned()], Arc::new(Quiet));
    let b = recorder.file_started(&["b.bin".to_owned()], Arc::new(Quiet));
    a.delivered(10, 100);
    a.delivered(30, 100);
    b.delivered(5, 50);
    recorder.file_landed();
    recorder.finish(&Ok(()), None).await;
    let record = only(dir.path());
    assert_eq!(record.received_bytes, 35);
    assert_eq!(bundle_of(&record), (2, 1, None));
}

/// A second pass after a pool replacement starts a blob again: it counts
/// once, at its latest position.
#[tokio::test]
async fn a_blob_started_again_counts_once() {
    let dir = tempfile::tempdir().unwrap();
    let bundle = format!("{BLOB}/");
    let recorder = start(dir.path(), &bundle);
    let first = recorder.file_started(&["a.bin".to_owned()], Arc::new(Quiet));
    first.delivered(30, 100);
    let second = recorder.file_started(&["a.bin".to_owned()], Arc::new(Quiet));
    second.delivered(60, 100);
    recorder.finish(&Ok(()), None).await;
    let record = only(dir.path());
    assert_eq!(record.received_bytes, 60);
    assert_eq!(bundle_of(&record).0, 1);
}

/// A bundle's counts reach the record with its end, never while it runs.
#[tokio::test]
async fn bundle_counts_land_with_the_end() {
    let dir = tempfile::tempdir().unwrap();
    let bundle = format!("{BLOB}/");
    let recorder = start(dir.path(), &bundle);
    recorder.bundle_counts(BundleCounts {
        fetched: 3,
        ..BundleCounts::default()
    });
    recorder.write().unwrap();
    assert_eq!(bundle_of(&only(dir.path())).2, None);
    recorder.finish(&Ok(()), None).await;
    assert_eq!(bundle_of(&only(dir.path())).2.map(|c| c.fetched), Some(3));
}

/// A live run's record follows it before it ends: the heartbeat writes what
/// the run reported.
#[tokio::test(start_paused = true)]
async fn the_heartbeat_writes_a_live_run() {
    let dir = tempfile::tempdir().unwrap();
    let recorder = start(dir.path(), BLOB);
    recorder.progress(10, 100);
    for _ in 0..3 {
        tokio::time::advance(HEARTBEAT).await;
        tokio::task::yield_now().await;
    }
    let live = only(dir.path());
    assert_eq!(live.state, PullState::Running);
    assert_eq!(live.received_bytes, 10);
    recorder.finish(&Ok(()), None).await;
}

/// The last write is the end, whatever the heartbeat was doing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_end_is_the_last_write() {
    let dir = tempfile::tempdir().unwrap();
    for _ in 0..20 {
        let recorder = start(dir.path(), BLOB);
        tokio::time::sleep(Duration::from_millis(3)).await;
        recorder.finish(&Ok(()), None).await;
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let (records, _) = read_records(dir.path()).unwrap();
    assert!(records.iter().all(|r| !r.state.is_running()), "{records:?}");
}

#[test]
fn what_each_node_was_paid_merges_per_node() {
    let a = Address::repeat_byte(0xaa);
    let b = Address::repeat_byte(0xbb);
    let amount = |amount, bytes| ReceiptAmount { amount, bytes };
    let (total, nodes) =
        paid_per_node([(a, amount(5, 50)), (b, amount(1, 10)), (a, amount(2, 20))].into_iter());
    assert_eq!(
        total,
        Paid {
            amount_micro_usdc: 8,
            bytes: 80
        }
    );
    assert_eq!(nodes.len(), 2);
    assert_eq!(nodes[0].provider, a.to_checksum(None));
    assert_eq!(nodes[0].paid.amount_micro_usdc, 7);
    assert_eq!(nodes[1].paid.bytes, 10);
}

#[test]
fn a_running_record_goes_stale_when_its_heartbeat_stops() {
    let running = record(1_000, PullState::Running);
    assert!(!running.is_stale(1_000 + millis(STALE_AFTER)));
    assert!(running.is_stale(1_001 + millis(STALE_AFTER)));
    assert!(!record(1_000, done()).is_stale(u64::MAX));
}

#[test]
fn records_read_newest_first_and_skip_what_does_not_parse() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), &record(1, done()));
    write(dir.path(), &record(3, PullState::Running));
    write(dir.path(), &record(2, done()));
    std::fs::write(dir.path().join("9-9.json"), b"{not json").unwrap();
    std::fs::write(dir.path().join("notes.txt"), b"ignored").unwrap();
    let (records, unreadable) = read_records(dir.path()).unwrap();
    let started: Vec<_> = records.iter().map(|r| r.started_unix_ms).collect();
    assert_eq!(started, [3, 2, 1]);
    assert_eq!(unreadable, 1);
}

/// A blob or entry record carries no bundle field; a bundle record carries
/// them all beside its kind.
#[test]
fn only_a_bundle_record_carries_bundle_fields() {
    let blob = serde_json::to_value(record(1, done())).unwrap();
    assert_eq!(blob["kind"], "blob");
    for field in ["files_started", "files_landed", "counts", "bundle"] {
        assert!(blob.get(field).is_none(), "{field} in {blob}");
    }
    let mut bundle = record(1, done());
    bundle.kind = PullKind::Bundle {
        files_started: 2,
        files_landed: 1,
        counts: Some(BundleCounts::default()),
    };
    let json = serde_json::to_value(&bundle).unwrap();
    assert_eq!(json["kind"], "bundle");
    assert_eq!(json["files_started"], 2);
    assert_eq!(json["files_landed"], 1);
    assert_eq!(json["counts"]["fetched"], 0);
    assert_eq!(serde_json::from_value::<PullRecord>(json).unwrap(), bundle);
}

/// A record file a build before the bundle kind carried its fields wrote:
/// flat `files_started` / `files_landed` and a `bundle` key on every kind.
fn old_shape(started: u64, kind: &str, uri: &str) -> serde_json::Value {
    serde_json::json!({
        "uri": uri,
        "kind": kind,
        "output": "out",
        "pid": 1,
        "started_unix_ms": started,
        "updated_unix_ms": started,
        "state": "done",
        "finished_unix_ms": started,
        "received_bytes": 5,
        "expected_bytes": 5,
        "files_started": 3,
        "files_landed": 2,
        "paid": { "amount_micro_usdc": 0, "bytes": 0 },
        "nodes": [],
        "bundle": { "fetched": 2, "linked": 0, "skipped": 0, "excluded": 0, "failed": 0,
                    "not_run": 0, "downloaded": 5, "resumed_bytes": 0, "reused_bytes": 0 },
    })
}

/// An old-shape record on disk is no error. It parses: the flat file counts
/// feed the bundle kind, its counts read as none, and the leftover `bundle`
/// key is ignored, as are a blob record's flat bundle fields.
#[test]
fn an_old_shape_record_reads_without_error() {
    let dir = tempfile::tempdir().unwrap();
    let old_bundle = old_shape(1, "bundle", &format!("{BLOB}/"));
    let old_blob = old_shape(2, "blob", BLOB);
    std::fs::write(dir.path().join("1-1.json"), old_bundle.to_string()).unwrap();
    std::fs::write(dir.path().join("2-1.json"), old_blob.to_string()).unwrap();
    write(dir.path(), &record(3, done()));
    let (records, unreadable) = read_records(dir.path()).unwrap();
    assert_eq!(unreadable, 0, "{records:?}");
    let started: Vec<_> = records.iter().map(|r| r.started_unix_ms).collect();
    assert_eq!(started, [3, 2, 1]);
    assert_eq!(records[0], record(3, done()));
    assert_eq!(records[1].kind, PullKind::Blob);
    assert_eq!(bundle_of(&records[2]), (3, 2, None));
}

#[test]
fn prune_keeps_the_newest_records() {
    let dir = tempfile::tempdir().unwrap();
    for started in 1..=5 {
        write(dir.path(), &record(started, done()));
    }
    prune(dir.path(), 2);
    let started: Vec<_> = read_records(dir.path())
        .unwrap()
        .0
        .iter()
        .map(|r| r.started_unix_ms)
        .collect();
    assert_eq!(started, [5, 4]);
}

/// A run that starts in a full directory keeps it at [`KEPT_RECORDS`], its
/// own record among them.
#[tokio::test]
async fn a_run_prunes_to_the_kept_count_when_it_starts() {
    let dir = tempfile::tempdir().unwrap();
    for started in 1..=u64::try_from(KEPT_RECORDS).unwrap() {
        write(dir.path(), &record(started, done()));
    }
    let recorder = start(dir.path(), BLOB);
    let (records, _) = read_records(dir.path()).unwrap();
    assert_eq!(records.len(), KEPT_RECORDS);
    assert!(records[0].state.is_running(), "the new run is the newest");
    assert!(
        records.iter().all(|r| r.started_unix_ms != 1),
        "the oldest went"
    );
    recorder.finish(&Ok(()), None).await;
}

#[test]
fn a_missing_directory_reads_as_no_records() {
    let dir = tempfile::tempdir().unwrap();
    assert_eq!(
        read_records(&dir.path().join("absent")).unwrap(),
        (Vec::new(), 0)
    );
}

/// A path that is not a directory is an error, not an empty list.
#[test]
fn an_unlistable_directory_is_an_error() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("file");
    std::fs::write(&file, b"").unwrap();
    assert!(read_records(&file).is_err());
}
