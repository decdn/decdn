use super::*;

/// The suites read the content by these keys, so a renamed key breaks every
/// suite at once. Assert them here, with no chain and no built binary, so a
/// plain `cargo nextest run -p decdn-e2e` catches the rename.
#[test]
fn content_fixture_names_its_keys() {
    let fixture = Content::new().fixture();
    let keys: Vec<&str> = fixture.keys().map(String::as_str).collect();
    assert_eq!(keys.len(), 4, "{keys:?}");
    for key in ["bundle", "files", "blob", "blobBytes"] {
        assert!(keys.contains(&key), "no {key} in {keys:?}");
    }
    let files = fixture
        .get("files")
        .and_then(serde_json::Value::as_object)
        .expect("files is a JSON object");
    assert_eq!(files.len(), 3, "{files:?}");
    for path in ["a.txt", "index.html", "data.bin"] {
        assert!(files.contains_key(path), "no {path} in files");
    }
}

/// pytest wraps its one suite in a `<testsuites>` with its own `name`, which
/// must not match `<testsuite `.
#[test]
fn junit_reads_pytests_wrapped_report() {
    let xml = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<testsuites name=\"pytest \
               tests\"><testsuite \
               name=\"pytest\" errors=\"0\" failures=\"1\" skipped=\"2\" tests=\"7\" \
               time=\"1.23\" timestamp=\"2026-10-30T12:00:00.000000+00:00\" \
               hostname=\"ci\"><testcase classname=\"t\" name=\"a\" time=\"0.1\" \
               /></testsuite></testsuites>";
    let counts = junit::counts(xml).unwrap();
    assert_eq!(
        counts,
        junit::Counts {
            tests: 7,
            failures: 1,
            errors: 0,
            skipped: 2,
        }
    );
    assert_eq!(counts.passed(), 4);
}

#[test]
fn junit_reads_a_single_line_report() {
    let xml = "<testsuite name=\"pytest\" errors=\"1\" failures=\"0\" skipped=\"0\" \
               tests=\"3\"/>";
    let counts = junit::counts(xml).unwrap();
    assert_eq!(
        counts,
        junit::Counts {
            tests: 3,
            failures: 0,
            errors: 1,
            skipped: 0,
        }
    );
    assert_eq!(counts.passed(), 2);
}

#[test]
fn junit_takes_attributes_in_any_order() {
    let xml = "<testsuites><testsuite tests=\"5\" skipped=\"0\" name=\"pytest\" \
               failures=\"0\" errors=\"0\"></testsuite></testsuites>";
    let counts = junit::counts(xml).unwrap();
    assert_eq!(
        counts,
        junit::Counts {
            tests: 5,
            failures: 0,
            errors: 0,
            skipped: 0,
        }
    );
    assert_eq!(counts.passed(), 5);
}

#[test]
fn junit_rejects_a_document_without_a_testsuite() {
    let err = junit::counts("<testsuites></testsuites>").unwrap_err();
    assert!(err.to_string().contains("<testsuite"), "{err:#}");
}

#[test]
fn junit_rejects_a_non_numeric_count() {
    let xml = "<testsuite name=\"pytest\" errors=\"0\" failures=\"x\" skipped=\"0\" \
               tests=\"3\">";
    let msg = format!("{:#}", junit::counts(xml).unwrap_err());
    assert!(msg.contains("failures") && msg.contains("\"x\""), "{msg}");
}

/// nextest runs a test in its crate's directory, so a relative checkout path
/// names something other than what the caller meant.
#[test]
fn python_from_dir_rejects_a_relative_path() {
    let msg = format!(
        "{:#}",
        Python::from_dir(PathBuf::from("sdk-python")).unwrap_err()
    );
    assert!(
        msg.contains("absolute") && msg.contains("sdk-python"),
        "{msg}"
    );
}

#[test]
fn python_from_dir_wants_uv_lock() {
    let dir = tempfile::tempdir().unwrap();
    let msg = format!("{:#}", Python::from_dir(dir.path().to_owned()).unwrap_err());
    assert!(msg.contains("uv.lock"), "{msg}");
}

#[test]
fn python_from_dir_wants_a_synced_venv() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("uv.lock"), "version = 1\n").unwrap();
    let msg = format!("{:#}", Python::from_dir(dir.path().to_owned()).unwrap_err());
    assert!(
        msg.contains(".venv/bin/python") && msg.contains("uv sync --frozen"),
        "{msg}"
    );
}
