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
        source: SavedSource::new([3; 32], "src/b.txt"),
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
    assert_eq!(got.source, sample().source);
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
fn an_earlier_version_loads_empty() {
    let tmp = tempfile::tempdir().expect("tmp");
    std::fs::write(
        tmp.path().join(SAVED_MANIFEST_NAME),
        br#"{"version":1,"files":{"a.txt":{"hash":"b3:aa","size":1,"mtime":{"secs":1,"nanos":0}}}}"#,
    )
    .expect("write");
    assert!(load(tmp.path()).get("a.txt").is_none());
    let err = load_checked(tmp.path()).unwrap_err();
    assert!(
        format!("{err:#}").contains("has version 1; this build reads version 2"),
        "{err:#}"
    );
}

#[test]
fn load_checked_passes_a_missing_file_and_names_a_corrupt_one() {
    let tmp = tempfile::tempdir().expect("tmp");
    assert!(
        load_checked(tmp.path())
            .expect("missing is empty")
            .get("a")
            .is_none()
    );
    std::fs::write(tmp.path().join(SAVED_MANIFEST_NAME), b"{not json").expect("write");
    let err = load_checked(tmp.path()).unwrap_err();
    assert!(
        format!("{err:#}").contains("is not a skip-cache"),
        "{err:#}"
    );
}

#[test]
fn source_matches_only_its_bundle_and_path_and_prints_its_uri() {
    let src = sample().source;
    assert!(src.is([3; 32], "src/b.txt"));
    assert!(!src.is([4; 32], "src/b.txt"));
    assert!(!src.is([3; 32], "b.txt"));
    assert_eq!(src.bundle, super::super::format_hash([3; 32]));
    let uri = src.uri().expect("a hash");
    assert_eq!(uri.hash, [3u8; 32]);
    assert_eq!(uri.target, UriTarget::BundleEntry("src/b.txt".into()));
    let bad = SavedSource {
        bundle: "not-a-hash".into(),
        path: "x".into(),
    };
    assert!(bad.uri().is_none());
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
        source: sample().source,
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
        source: sample().source,
    };
    assert!(saved_hints(&rec).is_none()); // 10 != 99
    rec.chunks = None;
    assert!(saved_hints(&rec).is_none());
}

/// A record holds a hash by its bytes, whatever spelling wrote it, and a
/// record whose hash does not parse holds nothing.
#[test]
fn holds_matches_by_bytes_across_spellings() {
    let hex = "ab".repeat(32);
    for spelling in [
        format!("b3:{hex}"),
        format!("0x{}", hex.to_uppercase()),
        hex.clone(),
    ] {
        let rec = SavedFile {
            hash: spelling.clone(),
            ..sample()
        };
        assert!(rec.holds([0xab; 32]), "{spelling}");
        assert!(!rec.holds([0xcd; 32]), "{spelling}");
    }
    assert!(!sample().holds([0xaa; 32]), "`b3:aa` does not parse");
}
