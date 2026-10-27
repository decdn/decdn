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
        finished_unix_ms: None,
        state,
        received_bytes: 0,
        expected_bytes: None,
        files_started: 0,
        files_landed: 0,
        paid: Paid::default(),
        nodes: Vec::new(),
        bundle: None,
        error: None,
    }
}

fn write(dir: &Path, record: &PullRecord) {
    let path = dir.join(format!("{}-{}.json", record.started_unix_ms, record.pid));
    std::fs::write(path, serde_json::to_vec(record).unwrap()).unwrap();
}

#[tokio::test]
async fn a_run_writes_its_record_and_ends_it() {
    let dir = tempfile::tempdir().unwrap();
    let evidence = Arc::new(ReceiptEvidence::new());
    let recorder =
        ActivityRecorder::start_in(dir.path(), &uri(BLOB), BLOB, Path::new("out"), &evidence)
            .unwrap();
    let (records, unreadable) = read_records(dir.path());
    assert_eq!(unreadable, 0);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].state, PullState::Running);
    assert_eq!(records[0].kind, PullKind::Blob);

    recorder.progress(40, 100);
    recorder.finish(&Ok(()), None).await;
    let (records, _) = read_records(dir.path());
    assert_eq!(records[0].state, PullState::Done);
    assert_eq!(records[0].received_bytes, 40);
    assert_eq!(records[0].expected_bytes, Some(100));
    assert!(records[0].finished_unix_ms.is_some());
}

#[tokio::test]
async fn a_stopped_run_records_why() {
    let dir = tempfile::tempdir().unwrap();
    let evidence = Arc::new(ReceiptEvidence::new());
    let recorder =
        ActivityRecorder::start_in(dir.path(), &uri(BLOB), BLOB, Path::new("out"), &evidence)
            .unwrap();
    recorder.finish(&Err(Interrupted.into()), None).await;
    assert_eq!(read_records(dir.path()).0[0].state, PullState::Interrupted);

    let recorder =
        ActivityRecorder::start_in(dir.path(), &uri(BLOB), BLOB, Path::new("out"), &evidence)
            .unwrap();
    recorder
        .finish(&Err(anyhow::anyhow!("no node holds it")), None)
        .await;
    let failed = read_records(dir.path())
        .0
        .into_iter()
        .find(|r| r.state == PullState::Failed)
        .unwrap();
    assert_eq!(failed.error.as_deref(), Some("no node holds it"));
}

struct Quiet;

impl FileObserver for Quiet {}

#[tokio::test]
async fn a_bundle_blob_reports_its_whole_position_not_a_delta() {
    let dir = tempfile::tempdir().unwrap();
    let evidence = Arc::new(ReceiptEvidence::new());
    let bundle = format!("{BLOB}/");
    let recorder = ActivityRecorder::start_in(
        dir.path(),
        &uri(&bundle),
        &bundle,
        Path::new("."),
        &evidence,
    )
    .unwrap();
    let a = recorder.file_started(Arc::new(Quiet));
    let b = recorder.file_started(Arc::new(Quiet));
    a.delivered(10, 100);
    a.delivered(30, 100);
    b.delivered(5, 50);
    recorder.file_landed();
    recorder.finish(&Ok(()), None).await;
    let record = &read_records(dir.path()).0[0];
    assert_eq!(record.kind, PullKind::Bundle);
    assert_eq!(record.received_bytes, 35);
    assert_eq!((record.files_started, record.files_landed), (2, 1));
}

#[test]
fn a_running_record_goes_stale_when_its_heartbeat_stops() {
    let running = record(1_000, PullState::Running);
    assert!(!running.is_stale(1_000 + millis(STALE_AFTER)));
    assert!(running.is_stale(1_001 + millis(STALE_AFTER)));
    assert!(!record(1_000, PullState::Done).is_stale(u64::MAX));
}

#[test]
fn records_read_newest_first_and_skip_what_does_not_parse() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), &record(1, PullState::Done));
    write(dir.path(), &record(3, PullState::Running));
    write(dir.path(), &record(2, PullState::Failed));
    std::fs::write(dir.path().join("9-9.json"), b"{not json").unwrap();
    std::fs::write(dir.path().join("notes.txt"), b"ignored").unwrap();
    let (records, unreadable) = read_records(dir.path());
    let started: Vec<_> = records.iter().map(|r| r.started_unix_ms).collect();
    assert_eq!(started, [3, 2, 1]);
    assert_eq!(unreadable, 1);
}

#[test]
fn prune_keeps_the_newest_records() {
    let dir = tempfile::tempdir().unwrap();
    for started in 1..=5 {
        write(dir.path(), &record(started, PullState::Done));
    }
    prune(dir.path(), 2);
    let started: Vec<_> = read_records(dir.path())
        .0
        .iter()
        .map(|r| r.started_unix_ms)
        .collect();
    assert_eq!(started, [5, 4]);
}

#[test]
fn a_missing_directory_reads_as_no_records() {
    let dir = tempfile::tempdir().unwrap();
    assert_eq!(read_records(&dir.path().join("absent")), (Vec::new(), 0));
}
