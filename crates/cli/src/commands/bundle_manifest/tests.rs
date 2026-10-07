use super::*;
use std::collections::BTreeMap;

fn sample() -> SavedFile {
    SavedFile {
        hash: "b3:aa".into(),
        size: 10,
        mtime: SavedMtime {
            secs: 1_757_800_000,
            nanos: 5,
        },
        chunks: Some(vec![SavedChunk {
            hash: "b3:bb".into(),
            size: 10,
        }]),
    }
}

#[test]
fn write_then_load_round_trips() {
    let tmp = tempfile::tempdir().expect("tmp");
    let mut updates = BTreeMap::new();
    updates.insert("a/b.txt".to_string(), sample());
    merge_and_write(tmp.path(), SavedManifest::default(), updates).expect("write");

    let loaded = load(tmp.path());
    let got = loaded.get("a/b.txt").expect("entry present");
    assert_eq!(got.hash, "b3:aa");
    assert_eq!(got.size, 10);
    assert_eq!(
        got.mtime,
        SavedMtime {
            secs: 1_757_800_000,
            nanos: 5
        }
    );
}

#[test]
fn missing_file_loads_empty() {
    let tmp = tempfile::tempdir().expect("tmp");
    assert!(load(tmp.path()).get("anything").is_none());
}

#[test]
fn corrupt_file_loads_empty_not_error() {
    let tmp = tempfile::tempdir().expect("tmp");
    std::fs::write(tmp.path().join(SAVED_MANIFEST_NAME), b"{not json").expect("write");
    assert!(load(tmp.path()).get("anything").is_none());
}

#[test]
fn merge_overlays_updates_and_keeps_untouched() {
    let tmp = tempfile::tempdir().expect("tmp");
    let mut first = BTreeMap::new();
    first.insert("keep.txt".to_string(), sample());
    first.insert("change.txt".to_string(), sample());
    merge_and_write(tmp.path(), SavedManifest::default(), first).expect("write1");

    let prior = load(tmp.path());
    let mut upd = BTreeMap::new();
    let mut changed = sample();
    changed.hash = "b3:cc".into();
    upd.insert("change.txt".to_string(), changed);
    merge_and_write(tmp.path(), prior, upd).expect("write2");

    let loaded = load(tmp.path());
    assert_eq!(loaded.get("keep.txt").expect("kept").hash, "b3:aa");
    assert_eq!(loaded.get("change.txt").expect("changed").hash, "b3:cc");
}

#[test]
fn saved_hints_places_offsets_and_validates_sum() {
    let rec = SavedFile {
        hash: "b3:aa".into(),
        size: 30,
        mtime: SavedMtime { secs: 1, nanos: 0 },
        chunks: Some(vec![
            SavedChunk {
                hash: "b3:c0".into(),
                size: 10,
            },
            SavedChunk {
                hash: "b3:c1".into(),
                size: 20,
            },
        ]),
    };
    let got = saved_hints(&rec).expect("hints");
    assert_eq!(got, vec![("b3:c0".into(), 0, 10), ("b3:c1".into(), 10, 20)]);
}

#[test]
fn records_iterates_all_saved_files() {
    let tmp = tempfile::tempdir().expect("tmp");
    let mut updates = BTreeMap::new();
    updates.insert("g1/a.dll".to_string(), sample());
    updates.insert("g2/a.dll".to_string(), sample());
    merge_and_write(tmp.path(), SavedManifest::default(), updates).expect("write");
    let loaded = load(tmp.path());
    let paths: Vec<&str> = loaded.records().map(|(p, _)| p).collect();
    assert_eq!(paths, vec!["g1/a.dll", "g2/a.dll"]);
}

#[test]
fn saved_hints_rejects_bad_sum_and_absent_chunks() {
    let mut rec = SavedFile {
        hash: "b3:aa".into(),
        size: 99,
        mtime: SavedMtime { secs: 1, nanos: 0 },
        chunks: Some(vec![SavedChunk {
            hash: "b3:c0".into(),
            size: 10,
        }]),
    };
    assert!(saved_hints(&rec).is_none()); // 10 != 99
    rec.chunks = None;
    assert!(saved_hints(&rec).is_none());
}
