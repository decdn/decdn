//! Integration tests for `decdn bundle create` (issue #391).
//!
//! Tests call `bundle_create` directly against `tempfile::TempDir`
//! fixtures — same shape as `tests/config_validate.rs`. No `assert_cmd`,
//! no shelling out except for the `--json` stdout test which exercises
//! the actual binary so the public stdout path is covered.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]

use std::fs;
use std::path::{Path, PathBuf};

use decdn_cli::commands::bundle::bundle_create;
use decdn_common::cli::BundleCreateArgs;
use tempfile::TempDir;

fn args(input: &Path, output: &Path) -> BundleCreateArgs {
    BundleCreateArgs {
        input: input.to_path_buf(),
        output: output.to_path_buf(),
        follow_symlinks: false,
        exclude: Vec::new(),
        json: false,
    }
}

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn write_files(root: &Path, files: &[(&str, &[u8])]) {
    for (rel, content) in files {
        let path = root.join(rel);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(&path, content).unwrap();
    }
}

fn read_bundle(path: &Path) -> serde_json::Value {
    let bytes = fs::read(path).unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

// Probe whether `dir` lives on a case-sensitive filesystem by writing a
// lowercase file and checking whether the uppercase path then exists. On
// macOS APFS (default) and NTFS this returns false, so case-only fixtures
// can be skipped instead of failing the suite (#697).
fn is_case_sensitive(dir: &Path) -> bool {
    let lower = dir.join("decdn_case_probe");
    fs::write(&lower, b"x").unwrap();
    // On a case-insensitive FS the uppercase path resolves back to the
    // file we just wrote; on a case-sensitive FS it does not exist.
    let sensitive = !dir.join("DECDN_CASE_PROBE").exists();
    fs::remove_file(&lower).unwrap();
    sensitive
}

// Two runs against the same dir produce byte-identical output. This is
// the single strongest check on the determinism contract.
#[test]
fn determinism_two_runs_byte_identical() {
    let dir = TempDir::new().unwrap();
    let src = dir.path().join("src");
    fs::create_dir(&src).unwrap();
    write_files(
        &src,
        &[
            ("index.html", b"<html></html>"),
            ("a/b.txt", b"hello"),
            ("a/c.txt", b"world"),
        ],
    );

    let out1 = dir.path().join("bundle1.json");
    let out2 = dir.path().join("bundle2.json");

    let runtime = rt();
    runtime.block_on(bundle_create(&args(&src, &out1))).unwrap();
    runtime.block_on(bundle_create(&args(&src, &out2))).unwrap();

    let bytes1 = fs::read(&out1).unwrap();
    let bytes2 = fs::read(&out2).unwrap();
    assert_eq!(
        bytes1, bytes2,
        "two runs must produce byte-identical output"
    );
    assert!(
        !bytes1.ends_with(b"\n"),
        "bundle file must not have a trailing newline"
    );
}

// The richer determinism fixture pins three properties at once:
// (a) bytewise sort survives non-ASCII paths (raw UTF-8, not collation),
// (b) the 64 KiB streaming-hash buffer correctly handles a file
//     larger than one buffer, and
// (c) the entries-array order is stable regardless of FS walk order.
//
// The mixed-case ASCII-uppercase-before-lowercase property lives in the
// case-sensitivity-gated `determinism_mixed_case_bytewise_sort` below,
// because a case-only `A.txt`/`a.txt` pair collapses to one entry on
// case-insensitive filesystems (macOS APFS, NTFS) — see #697.
#[test]
fn determinism_with_non_ascii_and_large_file() {
    let dir = TempDir::new().unwrap();
    let src = dir.path().join("src");
    fs::create_dir(&src).unwrap();

    // Non-ASCII path (UTF-8 encoded). Bytewise sort uses raw UTF-8
    // bytes; collation-aware sort would interleave with ASCII.
    write_files(&src, &[("\u{00e9}.txt", b"e-acute")]); // é
    write_files(&src, &[("\u{00f1}/x.txt", b"n-tilde")]); // ñ
    // Large file (160 KiB > 2× HASH_BUF_SIZE) exercises the streaming
    // hash loop end-to-end.
    let big: Vec<u8> = (0u32..160 * 1024)
        .map(|i| u8::try_from(i % 251).unwrap())
        .collect();
    fs::write(src.join("big.bin"), &big).unwrap();

    let out1 = dir.path().join("b1.json");
    let out2 = dir.path().join("b2.json");
    rt().block_on(bundle_create(&args(&src, &out1))).unwrap();
    rt().block_on(bundle_create(&args(&src, &out2))).unwrap();
    assert_eq!(fs::read(&out1).unwrap(), fs::read(&out2).unwrap());

    // Sort order: ASCII first, then multi-byte UTF-8 (which all start
    // with bytes >= 0xC2).
    let bundle = read_bundle(&out1);
    let paths: Vec<&str> = bundle["entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["path"].as_str().unwrap())
        .collect();
    assert_eq!(paths, vec!["big.bin", "\u{00e9}.txt", "\u{00f1}/x.txt"]);

    // Large-file hash matches the in-memory blake3 of the same bytes.
    let big_entry = bundle["entries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["path"].as_str() == Some("big.bin"))
        .unwrap();
    let expected = format!("b3:{}", blake3::hash(&big).to_hex());
    assert_eq!(big_entry["hash"].as_str().unwrap(), expected);
    assert_eq!(big_entry["size"].as_u64().unwrap(), big.len() as u64);
}

// Bytewise sort puts ASCII uppercase before lowercase ('A' = 0x41,
// 'a' = 0x61), which a Unicode-collation sort would reorder. Skipped on
// case-insensitive filesystems (macOS APFS, NTFS), where `A.txt` and
// `a.txt` collapse to a single directory entry so the pair can't be
// observed (#697). The contract this guards still runs on case-sensitive
// Linux CI.
#[test]
fn determinism_mixed_case_bytewise_sort() {
    let dir = TempDir::new().unwrap();
    let src = dir.path().join("src");
    fs::create_dir(&src).unwrap();
    if !is_case_sensitive(&src) {
        eprintln!(
            "skipping determinism_mixed_case_bytewise_sort: \
             case-insensitive filesystem collapses A.txt/a.txt"
        );
        return;
    }

    write_files(&src, &[("A.txt", b"upper"), ("a.txt", b"lower")]);
    let out = dir.path().join("b.json");
    rt().block_on(bundle_create(&args(&src, &out))).unwrap();

    let bundle = read_bundle(&out);
    let paths: Vec<&str> = bundle["entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["path"].as_str().unwrap())
        .collect();
    assert_eq!(paths, vec!["A.txt", "a.txt"]);
}

// The bundle's own BLAKE3 is the contract publishers distribute. A
// regression that flips compact to pretty would still pass byte-equality
// tests against another run, so this test pins the actual published hash.
#[test]
fn bundle_hash_status_matches_blake3_of_bundle_bytes() {
    let dir = TempDir::new().unwrap();
    let src = dir.path().join("src");
    fs::create_dir(&src).unwrap();
    write_files(&src, &[("a.txt", b"alpha"), ("b.txt", b"beta")]);
    let out = dir.path().join("b.json");

    rt().block_on(bundle_create(&args(&src, &out))).unwrap();

    let bytes = fs::read(&out).unwrap();
    let actual = blake3::hash(&bytes);

    // Recompute the bundle hash from a fresh run via the JSON status
    // line. Couples the published `bundle_hash` to the same file's bytes.
    let stdout = run_bundle_create_json(&src);
    let line = stdout.lines().last().expect("at least one line of stdout");
    let parsed: serde_json::Value = serde_json::from_str(line).unwrap();
    let reported = parsed["bundle_hash"].as_str().unwrap();
    assert_eq!(reported, format!("b3:{}", actual.to_hex()));
}

// `--json` stdout via the built binary. Covers the path the unit test
// of `write_create_report` cannot reach: `bundle_create` writes through
// a locked stdout handle, and we want to know the operator sees what's
// documented.
#[test]
fn json_status_line_shape_via_binary() {
    let dir = TempDir::new().unwrap();
    let src = dir.path().join("src");
    fs::create_dir(&src).unwrap();
    write_files(&src, &[("a.txt", b"x")]);
    let out = dir.path().join("b.json");

    let output = std::process::Command::new(env!("CARGO_BIN_EXE_decdn"))
        .args(["bundle", "create", "--json"])
        .arg("-i")
        .arg(&src)
        .arg("-o")
        .arg(&out)
        .output()
        .expect("run decdn binary");
    assert!(
        output.status.success(),
        "binary failed: stderr = {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).unwrap();
    let line = stdout.lines().last().expect("stdout had at least one line");
    let parsed: serde_json::Value = serde_json::from_str(line).expect("status line is JSON");
    let obj = parsed.as_object().unwrap();
    assert_eq!(obj.len(), 5);
    assert!(obj["bundle"].as_str().unwrap().ends_with("b.json"));
    assert_eq!(obj["entries"].as_u64(), Some(1));
    assert_eq!(obj["total_size"].as_u64(), Some(1));
    assert!(obj["bundle_hash"].as_str().unwrap().starts_with("b3:"));
    assert_eq!(obj["skipped_symlinks"].as_u64(), Some(0));
}

fn run_bundle_create_json(src: &Path) -> String {
    let dir = TempDir::new().unwrap();
    let out = dir.path().join("b.json");
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_decdn"))
        .args(["bundle", "create", "--json"])
        .arg("-i")
        .arg(src)
        .arg("-o")
        .arg(&out)
        .output()
        .expect("run decdn binary");
    assert!(
        output.status.success(),
        "binary failed: stderr = {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

// Empty directory produces a valid empty-entries bundle.
#[test]
fn empty_directory_produces_empty_entries() {
    let dir = TempDir::new().unwrap();
    let src = dir.path().join("empty");
    fs::create_dir(&src).unwrap();
    let out = dir.path().join("b.json");

    rt().block_on(bundle_create(&args(&src, &out))).unwrap();

    let bytes = fs::read(&out).unwrap();
    assert_eq!(bytes, b"{\"version\":1,\"entries\":[]}");
}

#[test]
fn entries_are_sorted_by_posix_path_bytes() {
    let dir = TempDir::new().unwrap();
    let src = dir.path().join("src");
    fs::create_dir(&src).unwrap();
    write_files(
        &src,
        &[("b.txt", b"x"), ("a/c.txt", b"x"), ("a/b.txt", b"x")],
    );
    let out = dir.path().join("b.json");

    rt().block_on(bundle_create(&args(&src, &out))).unwrap();

    let bundle = read_bundle(&out);
    let entries = bundle["entries"].as_array().unwrap();
    let paths: Vec<&str> = entries
        .iter()
        .map(|e| e["path"].as_str().unwrap())
        .collect();
    assert_eq!(paths, vec!["a/b.txt", "a/c.txt", "b.txt"]);
}

#[test]
fn hash_matches_blake3_of_file_content() {
    let dir = TempDir::new().unwrap();
    let src = dir.path().join("src");
    fs::create_dir(&src).unwrap();
    write_files(&src, &[("hello.txt", b"hello world\n")]);
    let out = dir.path().join("b.json");

    rt().block_on(bundle_create(&args(&src, &out))).unwrap();

    let bundle = read_bundle(&out);
    let entry = &bundle["entries"][0];
    let expected = format!("b3:{}", blake3::hash(b"hello world\n").to_hex());
    assert_eq!(entry["hash"].as_str().unwrap(), expected);
    assert_eq!(entry["size"].as_u64().unwrap(), 12);
    assert_eq!(entry["path"].as_str().unwrap(), "hello.txt");
}

#[test]
fn exclude_root_and_nested_via_recursive_glob() {
    let dir = TempDir::new().unwrap();
    let src = dir.path().join("src");
    fs::create_dir(&src).unwrap();
    write_files(
        &src,
        &[
            ("keep.txt", b"k"),
            ("skip.log", b"s"),
            ("nested/skip.log", b"s"),
        ],
    );
    let out = dir.path().join("b.json");

    let mut a = args(&src, &out);
    a.exclude = vec!["*.log".to_string(), "**/*.log".to_string()];
    rt().block_on(bundle_create(&a)).unwrap();

    let bundle = read_bundle(&out);
    let entries = bundle["entries"].as_array().unwrap();
    let paths: Vec<&str> = entries
        .iter()
        .map(|e| e["path"].as_str().unwrap())
        .collect();
    assert_eq!(paths, vec!["keep.txt"]);
}

#[test]
fn exclude_multiple_patterns_or_together() {
    let dir = TempDir::new().unwrap();
    let src = dir.path().join("src");
    fs::create_dir(&src).unwrap();
    write_files(
        &src,
        &[("a.log", b"x"), ("b.tmp", b"x"), ("keep.txt", b"x")],
    );
    let out = dir.path().join("b.json");

    let mut a = args(&src, &out);
    a.exclude = vec!["*.log".to_string(), "*.tmp".to_string()];
    rt().block_on(bundle_create(&a)).unwrap();

    let bundle = read_bundle(&out);
    let paths: Vec<&str> = bundle["entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["path"].as_str().unwrap())
        .collect();
    assert_eq!(paths, vec!["keep.txt"]);
}

// `tmp/*` matches one level under tmp/; `tmp/**` matches recursively.
#[test]
fn exclude_directory_prefix_glob_matches_one_level() {
    let dir = TempDir::new().unwrap();
    let src = dir.path().join("src");
    fs::create_dir(&src).unwrap();
    write_files(
        &src,
        &[("tmp/x", b"x"), ("tmp/sub/y", b"y"), ("keep.txt", b"k")],
    );
    let out = dir.path().join("b.json");

    let mut a = args(&src, &out);
    a.exclude = vec!["tmp/*".to_string()];
    rt().block_on(bundle_create(&a)).unwrap();

    let bundle = read_bundle(&out);
    let paths: Vec<&str> = bundle["entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["path"].as_str().unwrap())
        .collect();
    // `tmp/*` excludes `tmp/x` (one level) but not `tmp/sub/y`.
    assert_eq!(paths, vec!["keep.txt", "tmp/sub/y"]);
}

#[test]
fn invalid_glob_pattern_errors_with_clear_message() {
    let dir = TempDir::new().unwrap();
    let src = dir.path().join("src");
    fs::create_dir(&src).unwrap();
    write_files(&src, &[("a.txt", b"a")]);
    let out = dir.path().join("b.json");

    let mut a = args(&src, &out);
    a.exclude = vec!["[".to_string()];
    let err = rt().block_on(bundle_create(&a)).unwrap_err();
    let msg = format!("{err:#}");
    assert!(
        msg.contains("invalid --exclude pattern"),
        "expected glob-pattern error, got: {msg}"
    );
}

// Missing/non-directory --input is a hard error before walking.
#[test]
fn missing_input_directory_errors() {
    let dir = TempDir::new().unwrap();
    let missing: PathBuf = dir.path().join("does-not-exist");
    let out = dir.path().join("b.json");
    let err = rt()
        .block_on(bundle_create(&args(&missing, &out)))
        .unwrap_err();
    assert!(format!("{err:#}").contains("not an existing directory"));
}

// Symlink that escapes the input root is a hard error when followed.
#[cfg(unix)]
#[test]
fn symlink_escape_errors_when_followed() {
    use std::os::unix::fs::symlink;

    let dir = TempDir::new().unwrap();
    let outside = dir.path().join("outside.txt");
    fs::write(&outside, b"not for the bundle").unwrap();
    let src = dir.path().join("src");
    fs::create_dir(&src).unwrap();
    write_files(&src, &[("real.txt", b"real")]);
    symlink(&outside, src.join("escape.txt")).unwrap();
    let out = dir.path().join("b.json");

    let mut a = args(&src, &out);
    a.follow_symlinks = true;

    let err = rt().block_on(bundle_create(&a)).unwrap_err();
    let msg = format!("{err:#}");
    assert!(msg.contains("outside"), "expected escape error, got: {msg}");
}

// Symlink that escapes is silently skipped (and counted) when not followed.
#[cfg(unix)]
#[test]
fn symlink_skipped_when_not_followed() {
    use std::os::unix::fs::symlink;

    let dir = TempDir::new().unwrap();
    let outside = dir.path().join("outside.txt");
    fs::write(&outside, b"x").unwrap();
    let src = dir.path().join("src");
    fs::create_dir(&src).unwrap();
    write_files(&src, &[("real.txt", b"r")]);
    symlink(&outside, src.join("escape.txt")).unwrap();
    let out = dir.path().join("b.json");

    rt().block_on(bundle_create(&args(&src, &out))).unwrap();

    let bundle = read_bundle(&out);
    let paths: Vec<&str> = bundle["entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["path"].as_str().unwrap())
        .collect();
    assert_eq!(paths, vec!["real.txt"]);
}

// Symlink within the root is followed and recorded under the link's
// name (not the target's), so both link and target get distinct entries.
#[cfg(unix)]
#[test]
fn symlink_within_root_recorded_under_link_name() {
    use std::os::unix::fs::symlink;

    let dir = TempDir::new().unwrap();
    let src = dir.path().join("src");
    fs::create_dir(&src).unwrap();
    write_files(&src, &[("target.txt", b"contents")]);
    symlink("target.txt", src.join("link.txt")).unwrap();
    let out = dir.path().join("b.json");

    let mut a = args(&src, &out);
    a.follow_symlinks = true;

    rt().block_on(bundle_create(&a)).unwrap();

    let bundle = read_bundle(&out);
    let paths: Vec<&str> = bundle["entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["path"].as_str().unwrap())
        .collect();
    assert_eq!(paths, vec!["link.txt", "target.txt"]);
    let expected = format!("b3:{}", blake3::hash(b"contents").to_hex());
    for entry in bundle["entries"].as_array().unwrap() {
        assert_eq!(entry["hash"].as_str().unwrap(), expected);
        assert_eq!(entry["size"].as_u64().unwrap(), 8);
    }
}

// A directory symlink that creates a self-loop must produce a finite
// error rather than hang. walkdir's loop detection (active under
// `follow_links(true)`) should surface a `same file system` / `loop`
// error that we propagate. This is the highest-risk run-time scenario;
// without it, a single self-loop in publisher input would hang the CLI.
#[cfg(unix)]
#[test]
fn symlink_directory_self_loop_errors_finite() {
    use std::os::unix::fs::symlink;
    use std::time::{Duration, Instant};

    let dir = TempDir::new().unwrap();
    let src = dir.path().join("src");
    fs::create_dir(&src).unwrap();
    write_files(&src, &[("real.txt", b"r")]);
    // `loop` -> `.` resolves back to the directory itself.
    symlink(".", src.join("loop")).unwrap();
    let out = dir.path().join("b.json");

    let mut a = args(&src, &out);
    a.follow_symlinks = true;

    let started = Instant::now();
    let result = rt().block_on(bundle_create(&a));
    // walkdir should detect the cycle and surface an error well under
    // the timeout. This is the bound the test really cares about.
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "self-loop symlink must not hang"
    );
    let err = result.expect_err("self-loop must error, not silently produce a partial bundle");
    let msg = format!("{err:#}");
    // Don't pin walkdir's exact message — just the family.
    assert!(
        msg.to_lowercase().contains("loop") || msg.contains("walking"),
        "expected a loop / walk error, got: {msg}"
    );
}
