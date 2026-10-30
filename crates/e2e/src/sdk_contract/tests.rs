use serde_json::json;

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

/// Each client flavour points its own namespace: the Python loader picks
/// `pointer.namespaces[i]` for flavour `i`, and the TypeScript loader reads
/// `pointer.namespace`, which is the first.
#[test]
fn public_fixture_carries_two_namespaces() {
    let fixture = public_fixture(
        &Content::new(),
        "127.0.0.1:8484",
        &PublicPointer {
            chain_id: 31_337,
            registry: "0x00000000000000000000000000000000000000aa",
            namespaces: &[7, 18_446_744_073_709_551_615],
            private_key: "0x01",
        },
    )
    .unwrap();
    assert_eq!(fixture.get("mode"), Some(&json!("public")));
    assert_eq!(fixture.get("gatewayUrl"), Some(&json!("http://decdn.test")));
    assert_eq!(fixture.get("connect"), Some(&json!("127.0.0.1:8484")));
    // Decimal strings: a u64 above 2^53 does not survive a JSON number.
    // `namespace` is the first of `namespaces`.
    assert_eq!(
        fixture.get("pointer"),
        Some(&json!({
            "chainId": "31337",
            "publisherRegistry": "0x00000000000000000000000000000000000000aa",
            "namespace": "7",
            "namespaces": ["7", "18446744073709551615"],
            "privateKey": "0x01",
        }))
    );
}

/// Each client flavour registers its own capability: the Python loader picks
/// `capabilities[i]` for flavour `i`, and the TypeScript loader reads `host`
/// and `capability`, which are the first.
#[test]
fn loopback_fixture_carries_two_capabilities() {
    let grants = [
        (PAGE_HOSTS[0], "dcap1:first"),
        (PAGE_HOSTS[1], "dcap1:second"),
    ];
    let fixture = loopback_fixture(
        &Content::new(),
        "http://127.0.0.1:8484",
        "127.0.0.1:8484",
        "token",
        &grants,
    )
    .unwrap();
    assert_eq!(fixture.get("mode"), Some(&json!("loopback")));
    assert_eq!(
        fixture.get("gatewayUrl"),
        Some(&json!("http://127.0.0.1:8484"))
    );
    assert_eq!(fixture.get("connect"), Some(&json!("127.0.0.1:8484")));
    assert_eq!(fixture.get("token"), Some(&json!("token")));
    assert_eq!(
        fixture.get("capabilities"),
        Some(&json!([
            { "host": "example.com", "capability": "dcap1:first" },
            { "host": "example.net", "capability": "dcap1:second" },
        ]))
    );
    // The fields the TypeScript loader reads are the first entry's.
    assert_eq!(fixture.get("host"), Some(&json!("example.com")));
    assert_eq!(fixture.get("capability"), Some(&json!("dcap1:first")));
}

/// A fixture with no namespace or no capability has no first entry for the
/// fields the TypeScript loader reads, so the helpers refuse it.
#[test]
fn fixtures_refuse_no_flavour() {
    let public = public_fixture(
        &Content::new(),
        "127.0.0.1:8484",
        &PublicPointer {
            chain_id: 1,
            registry: "0x00",
            namespaces: &[],
            private_key: "0x01",
        },
    );
    assert!(public.is_err());
    let loopback = loopback_fixture(&Content::new(), "http://x", "x", "t", &[]);
    assert!(loopback.is_err());
}
