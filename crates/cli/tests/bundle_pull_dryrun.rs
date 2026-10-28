//! Integration tests for `decdn get <bundle-uri> --dry-run` as a subprocess
//! (issue #2362).
//!
//! The dry run's unmatched `--include`/`--exclude` warnings are printed with
//! `eprintln!`, so only a real subprocess observes the stdout/stderr split the
//! in-process unit tests cannot: dropping the `apply_and_warn` call from the
//! dry-run path leaves every in-process test green. The manifest cache is
//! content-addressed (`<out>/.decdn-bundles/<blake3-of-bytes>.json`), so a
//! hand-written v1 manifest stands in for a paid pull and keeps the test
//! offline: with a cached manifest the run only *attempts* a quote, and a
//! failed quote degrades to a `warning: quote unavailable: …` stderr line.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]

mod common;

use std::fs;
use std::path::Path;
use std::process::Output;

use tempfile::TempDir;

/// A valid v1 bundle manifest with two entries, one of them under a `metal/`
/// subdirectory, so `--exclude 'metal/*'` matches nothing while `**/metal/*`
/// matches the second entry (the `**/` hint's trigger).
fn manifest_json() -> Vec<u8> {
    let entry_hash = format!("b3:{}", blake3::hash(b"entry").to_hex());
    serde_json::json!({
        "version": 1,
        "entries": [
            {"path": "openai-gpt-oss-20b/config.json", "hash": entry_hash, "size": 10},
            {"path": "openai-gpt-oss-20b/metal/model.bin", "hash": entry_hash, "size": 20},
        ],
    })
    .to_string()
    .into_bytes()
}

/// Write `bytes` where a pull into `out` caches the bundle manifest — the
/// cache is keyed by the BLAKE3 of the exact bytes — and return that hash's
/// hex, which is also the `decdn:<hex>/` URI the bytes answer for.
fn cache_manifest(out: &Path, bytes: &[u8]) -> String {
    let hex = blake3::hash(bytes).to_hex().to_string();
    let dir = out.join(".decdn-bundles");
    fs::create_dir_all(&dir).unwrap();
    fs::write(dir.join(format!("{hex}.json")), bytes).unwrap();
    hex
}

/// Run `decdn get 'decdn:<hex>/' -o <out> --exclude 'metal/*' --dry-run` with
/// an isolated HOME.
fn dry_run(home: &Path, out: &Path, hex: &str) -> Output {
    common::decdn_command(home)
        .args([
            "get",
            &format!("decdn:{hex}/"),
            "-o",
            out.to_str().unwrap(),
            "--exclude",
            "metal/*",
            "--dry-run",
        ])
        .output()
        .expect("run decdn binary")
}

// With a cached manifest, the dry run lists the plan on stdout and prints the
// unmatched-pattern warning — with its `**/` hint — on stderr. This is the
// observable contract `EntryFilter::apply_and_warn` owns: delete that call
// from the dry-run path and this test fails while every in-process unit test
// stays green.
#[test]
fn cached_manifest_warns_unmatched_exclude_on_stderr() {
    let home = TempDir::new().unwrap();
    let out = home.path().join("out");
    fs::create_dir(&out).unwrap();
    let hex = cache_manifest(&out, &manifest_json());

    let run = dry_run(home.path(), &out, &hex);
    let stdout = String::from_utf8_lossy(&run.stdout);
    let stderr = String::from_utf8_lossy(&run.stderr);
    assert!(run.status.success(), "stderr: {stderr}");

    // `metal/*` matches no entry (patterns match the whole path from the
    // bundle root), so both entries stay in the plan.
    assert!(
        stdout.contains("would fetch 2 entr(ies)"),
        "stdout: {stdout}"
    );
    assert!(
        stderr.contains("warning: --exclude 'metal/*' matched no entries"),
        "stderr: {stderr}"
    );
    // The hint fires because `**/metal/*` matches the second entry.
    assert!(
        stderr.contains("did you mean '**/metal/*'"),
        "stderr: {stderr}"
    );
}

// Without a cached manifest there are no entries to match, so the dry run
// reports that the patterns apply only once a pull caches the manifest — on
// stdout, with no unmatched-pattern warning on stderr.
#[test]
fn no_cached_manifest_notes_deferred_filters_without_warning() {
    let home = TempDir::new().unwrap();
    let out = home.path().join("out");
    fs::create_dir(&out).unwrap();
    let hex = blake3::hash(&manifest_json()).to_hex().to_string();

    let run = dry_run(home.path(), &out, &hex);
    let stdout = String::from_utf8_lossy(&run.stdout);
    let stderr = String::from_utf8_lossy(&run.stderr);
    assert!(run.status.success(), "stderr: {stderr}");

    assert!(
        stdout.contains(
            "no cached manifest: entries are listed once a pull into this directory caches it"
        ),
        "stdout: {stdout}"
    );
    assert!(
        stdout.contains("; the include/exclude patterns are matched against them then"),
        "stdout: {stdout}"
    );
    assert!(
        !stderr.contains("matched no entries"),
        "no entries were matched against, so no unmatched-pattern warning: {stderr}"
    );
}
