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
        downloaded_bytes: 0,
        resumed_bytes: 0,
        reused_bytes: 0,
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
    assert_eq!((records.len(), unreadable.len()), (1, 0), "{records:?}");
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

/// Each URI target records as its own kind: a bare hash as a blob run, a
/// trailing slash as a bundle run, an entry path as an entry run.
#[tokio::test]
async fn each_uri_target_records_its_own_kind() {
    for (text, kind) in [
        (BLOB.to_owned(), PullKind::Blob),
        (
            format!("{BLOB}/"),
            PullKind::Bundle {
                files_started: 0,
                files_landed: 0,
                counts: None,
            },
        ),
        (format!("{BLOB}/a/b"), PullKind::Entry),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let recorder = start(dir.path(), &text);
        assert_eq!(only(dir.path()).kind, kind, "{text}");
        recorder.finish(&Ok(()), None).await;
    }
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
    recorder.finish(&Ok(()), None).await;
    let record = only(dir.path());
    assert_eq!(record.received_bytes, 35);
    assert_eq!(bundle_of(&record), (2, 0, None));
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

/// A bundle's counts reach the record with its end, never while it runs;
/// the bytes it reused from whole files on disk come with them.
#[tokio::test]
async fn bundle_counts_land_with_the_end() {
    let dir = tempfile::tempdir().unwrap();
    let bundle = format!("{BLOB}/");
    let recorder = start(dir.path(), &bundle);
    recorder.bundle_counts(
        BundleCounts {
            fetched: 3,
            ..BundleCounts::default()
        },
        12,
    );
    recorder.write().unwrap();
    assert_eq!(bundle_of(&only(dir.path())).2, None);
    recorder.finish(&Ok(()), None).await;
    let ended = only(dir.path());
    assert_eq!(bundle_of(&ended).2.map(|c| c.fetched), Some(3));
    assert_eq!(ended.reused_bytes, 12);
}

/// A blob run's lowest position is its resume base: what its `.partial`
/// held before it streamed. A second pass reports a base this run grew,
/// which does not move it. The rest it downloaded.
#[tokio::test]
async fn a_blob_run_records_its_lowest_position_as_resumed() {
    let dir = tempfile::tempdir().unwrap();
    let recorder = start(dir.path(), BLOB);
    recorder.progress(30, 100);
    recorder.progress(70, 100);
    recorder.progress(60, 100);
    recorder.progress(100, 100);
    recorder.finish(&Ok(()), None).await;
    let ended = only(dir.path());
    assert_eq!(
        (
            ended.received_bytes,
            ended.downloaded_bytes,
            ended.resumed_bytes
        ),
        (100, 70, 30)
    );
}

/// A pass that drops what the store held, after a hash mismatch, fetches
/// the blob again from `0`: the run resumed nothing it kept.
#[tokio::test]
async fn a_refetch_from_zero_clears_the_resume_base() {
    let dir = tempfile::tempdir().unwrap();
    let recorder = start(dir.path(), BLOB);
    recorder.progress(30, 100);
    recorder.progress(100, 100);
    recorder.progress(0, 100);
    recorder.progress(100, 100);
    recorder.finish(&Ok(()), None).await;
    let ended = only(dir.path());
    assert_eq!((ended.downloaded_bytes, ended.resumed_bytes), (100, 0));
}

/// The hashes of two bundle blobs, planned at `a.bin` and `b.bin`.
const HASH_A: [u8; 32] = [0xaa; 32];
const HASH_B: [u8; 32] = [0xbb; 32];

fn plan_ab(recorder: &ActivityRecorder) {
    recorder.plan([("a.bin".to_owned(), HASH_A), ("b.bin".to_owned(), HASH_B)]);
}

/// A bundle run that ends before its blobs land counts what each fetched
/// from its positions: a fresh blob downloaded all it reached, and a blob
/// that resumed a `.partial` downloaded the rest past it.
#[tokio::test]
async fn an_interrupted_bundle_counts_what_its_unlanded_blobs_fetched() {
    let dir = tempfile::tempdir().unwrap();
    let recorder = start(dir.path(), &format!("{BLOB}/"));
    plan_ab(&recorder);
    let fresh = recorder.file_started(&["a.bin".to_owned()], Arc::new(Quiet));
    fresh.delivered(0, 1_000);
    fresh.delivered(512, 1_000);
    let resumed = recorder.file_started(&["b.bin".to_owned()], Arc::new(Quiet));
    resumed.delivered(20, 100);
    resumed.delivered(50, 100);
    recorder.write().unwrap();
    let live = only(dir.path());
    assert_eq!((live.downloaded_bytes, live.resumed_bytes), (542, 20));
    recorder.finish(&Err(Interrupted.into()), None).await;
    let ended = only(dir.path());
    assert_eq!(
        (
            ended.received_bytes,
            ended.downloaded_bytes,
            ended.resumed_bytes,
            ended.reused_bytes
        ),
        (562, 542, 20, 0)
    );
}

/// A landed blob counts what its pull says it fetched, spliced and resumed,
/// not its position, which a splice moves to the blob's size.
#[tokio::test]
async fn a_landed_blob_counts_its_splice_as_reused() {
    let dir = tempfile::tempdir().unwrap();
    let recorder = start(dir.path(), &format!("{BLOB}/"));
    plan_ab(&recorder);
    let file = recorder.file_started(&["a.bin".to_owned()], Arc::new(Quiet));
    file.delivered(0, 100);
    file.delivered(30, 100);
    file.delivered(100, 100);
    recorder.file_landed(Landed {
        hash: HASH_A,
        size: 100,
        paid: 30,
        spliced: 70,
        resumed: 0,
    });
    recorder.finish(&Ok(()), None).await;
    let ended = only(dir.path());
    assert_eq!(
        (
            ended.received_bytes,
            ended.downloaded_bytes,
            ended.resumed_bytes,
            ended.reused_bytes
        ),
        (100, 30, 0, 70)
    );
    assert_eq!(bundle_of(&ended).1, 1);
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
fn records_read_newest_first_and_name_what_does_not_parse() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), &record(1, done()));
    write(dir.path(), &record(3, PullState::Running));
    write(dir.path(), &record(2, done()));
    std::fs::write(dir.path().join("9-9.json"), b"{not json").unwrap();
    let mut old = serde_json::to_value(record(4, done())).unwrap();
    old.as_object_mut().unwrap().remove("resumed_bytes");
    std::fs::write(dir.path().join("4-1.json"), old.to_string()).unwrap();
    std::fs::write(dir.path().join("notes.txt"), b"ignored").unwrap();
    let (records, unreadable) = read_records(dir.path()).unwrap();
    let started: Vec<_> = records.iter().map(|r| r.started_unix_ms).collect();
    assert_eq!(started, [3, 2, 1]);
    let paths: Vec<_> = unreadable.iter().map(|u| u.path.as_str()).collect();
    let named = |name: &str| dir.path().join(name).display().to_string();
    assert_eq!(paths, [named("4-1.json"), named("9-9.json")]);
    assert!(
        unreadable[0].error.contains("resumed_bytes"),
        "{unreadable:?}"
    );
    assert!(!unreadable[1].error.is_empty(), "{unreadable:?}");
}

/// A `*.json` that cannot be read at all is named with the read error.
#[test]
fn a_record_that_cannot_be_read_is_named() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join("1-1.json")).unwrap();
    let (records, unreadable) = read_records(dir.path()).unwrap();
    assert!(records.is_empty());
    let [only] = unreadable.as_slice() else {
        panic!("one unreadable file: {unreadable:?}");
    };
    assert_eq!(only.path, dir.path().join("1-1.json").display().to_string());
    assert!(!only.error.is_empty(), "{only:?}");
}

/// An entry written to a file runs the bundle path, which reports file
/// progress, landings and counts: its record keeps the bytes and none of
/// the rest.
#[tokio::test]
async fn an_entry_run_records_no_bundle_fields() {
    let dir = tempfile::tempdir().unwrap();
    let recorder = start(dir.path(), &format!("{BLOB}/docs/a.bin"));
    recorder.plan([("docs/a.bin".to_owned(), HASH_A)]);
    let file = recorder.file_started(&["docs/a.bin".to_owned()], Arc::new(Quiet));
    file.delivered(8, 100);
    file.delivered(100, 100);
    recorder.file_landed(Landed {
        hash: HASH_A,
        size: 100,
        paid: 22,
        spliced: 70,
        resumed: 8,
    });
    recorder.bundle_counts(
        BundleCounts {
            fetched: 1,
            ..BundleCounts::default()
        },
        0,
    );
    recorder.finish(&Ok(()), None).await;
    let record = only(dir.path());
    assert_eq!(record.kind, PullKind::Entry);
    assert_eq!(
        (
            record.received_bytes,
            record.downloaded_bytes,
            record.resumed_bytes,
            record.reused_bytes
        ),
        (100, 22, 8, 70)
    );
    let json = serde_json::to_value(&record).unwrap();
    for field in ["files_started", "files_landed", "counts", "bundle"] {
        assert!(json.get(field).is_none(), "{field} in {json}");
    }
}

/// A blob record carries no bundle field; a bundle record carries them all
/// beside its kind, its counts as `null` until it has them, and reads back as
/// itself.
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
    bundle.kind = PullKind::Bundle {
        files_started: 2,
        files_landed: 1,
        counts: None,
    };
    let json = serde_json::to_value(&bundle).unwrap();
    assert!(json["counts"].is_null(), "{json}");
    assert_eq!(serde_json::from_value::<PullRecord>(json).unwrap(), bundle);
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
        (Vec::new(), Vec::new())
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
