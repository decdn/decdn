//! Integration tests for `decdn get <bundle-uri> --dry-run` as a subprocess
//! (issue #2362).
//!
//! The unmatched `--include`/`--exclude` warnings go to stderr through
//! `eprintln!`, which an in-process test cannot capture. Only the real binary
//! shows that they print at all, and on stderr rather than stdout.
//!
//! A manifest written by hand into the output root's manifest cache stands in
//! for one a paid pull cached: a load accepts any file whose BLAKE3 equals the
//! hash in its name. With the manifest cached, the dry run quotes a non-empty
//! plan.
//! `common::decdn_command`'s sealed environment holds no chain config, so the
//! quote stops at chain resolution, before it binds an endpoint. It prints
//! `warning: quote unavailable: …` on stderr and the run still exits 0.
//!
//! A dry run without `-o` runs from a temporary current directory, against
//! which its default `./<hex>` destination resolves. Those tests show what a
//! default destination refuses and that the directory gains nothing else.

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

use decdn_client::bundle::BundleManifest;
use tempfile::TempDir;

/// A v1 bundle manifest with two entries below `openai-gpt-oss-20b/`, one in
/// its `metal/` directory. `metal/*` matches neither, because a pattern
/// matches the whole path from the bundle root; `**/metal/*` matches the
/// second, which triggers the `**/` hint. The hashes and sizes are
/// placeholders: a dry run fetches nothing.
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

/// Cache `bytes` as the manifest a pull into `out` fetched, and return their
/// BLAKE3 hex, the hash a `decdn:<hex>/` URI names. A load re-hashes the
/// file, so it must hold `bytes` exactly.
fn cache_manifest(out: &Path, bytes: &[u8]) -> String {
    let hash = blake3::hash(bytes);
    let path = BundleManifest::cache_path(out, *hash.as_bytes());
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(&path, bytes).unwrap();
    hash.to_hex().to_string()
}

/// Run `decdn get 'decdn:<hex>/' --dry-run` with no `-o`, from `cwd`, under
/// `common::decdn_command`'s sealed environment: the destination defaults to
/// `./<hex>`, which resolves against `cwd`.
fn default_dry_run(home: &Path, cwd: &Path, hex: &str) -> Output {
    common::decdn_command(home)
        .current_dir(cwd)
        .args(["get", &format!("decdn:{hex}/"), "--dry-run"])
        .output()
        .expect("run decdn binary")
}

/// The names in `dir`, sorted.
fn names_in(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

/// Run `decdn get 'decdn:<hex>/' -o <out> <filters…> --dry-run` under
/// `common::decdn_command`'s sealed environment.
fn dry_run(home: &Path, out: &Path, hex: &str, filters: &[&str]) -> Output {
    common::decdn_command(home)
        .args(["get", &format!("decdn:{hex}/"), "-o", out.to_str().unwrap()])
        .args(filters)
        .arg("--dry-run")
        .output()
        .expect("run decdn binary")
}

/// With a cached manifest, the dry run lists the filtered plan on stdout and
/// prints one warning, with its `**/` hint, on stderr for the one pattern that
/// matched no entry. A pattern that matches gets no warning. The quote fails
/// before it reaches the network, so the run stays offline.
#[test]
fn cached_manifest_warns_unmatched_exclude_on_stderr() {
    let home = TempDir::new().unwrap();
    let out = home.path().join("out");
    fs::create_dir(&out).unwrap();
    let hex = cache_manifest(&out, &manifest_json());

    let run = dry_run(
        home.path(),
        &out,
        &hex,
        &["--exclude", "metal/*", "--exclude", "**/config.json"],
    );
    let stdout = String::from_utf8_lossy(&run.stdout);
    let stderr = String::from_utf8_lossy(&run.stderr);
    assert!(run.status.success(), "stderr: {stderr}");

    assert!(
        stdout.contains("would fetch 1 entr(ies)"),
        "stdout: {stdout}"
    );
    assert!(
        stdout.contains("openai-gpt-oss-20b/metal/model.bin"),
        "stdout: {stdout}"
    );
    assert!(!stdout.contains("config.json"), "stdout: {stdout}");

    assert!(
        stderr.contains("warning: --exclude 'metal/*' matched no entries"),
        "stderr: {stderr}"
    );
    assert!(
        stderr.contains("did you mean '**/metal/*'"),
        "stderr: {stderr}"
    );
    assert!(
        !stderr.contains("'**/config.json' matched"),
        "stderr: {stderr}"
    );
    assert_eq!(
        stderr.matches("matched no entries").count(),
        1,
        "stderr: {stderr}"
    );
    assert!(
        stderr.contains("warning: quote unavailable"),
        "stderr: {stderr}"
    );
}

/// An `--include` that matches no entry empties the plan: stderr carries its
/// warning and hint, and stdout reports that the filters left nothing.
#[test]
fn cached_manifest_warns_unmatched_include_on_stderr() {
    let home = TempDir::new().unwrap();
    let out = home.path().join("out");
    fs::create_dir(&out).unwrap();
    let hex = cache_manifest(&out, &manifest_json());

    let run = dry_run(home.path(), &out, &hex, &["--include", "metal/*"]);
    let stdout = String::from_utf8_lossy(&run.stdout);
    let stderr = String::from_utf8_lossy(&run.stderr);
    assert!(run.status.success(), "stderr: {stderr}");

    assert!(
        stdout.contains("no bundle entries match the include/exclude filters; nothing to fetch"),
        "stdout: {stdout}"
    );
    assert!(
        stderr.contains("warning: --include 'metal/*' matched no entries"),
        "stderr: {stderr}"
    );
    assert!(
        stderr.contains("did you mean '**/metal/*'"),
        "stderr: {stderr}"
    );
}

/// Without a cached manifest there are no entries to match, so the dry run
/// reports that the patterns apply only once a pull caches the manifest — on
/// stdout, with no unmatched-pattern warning on stderr.
#[test]
fn no_cached_manifest_notes_deferred_filters_without_warning() {
    let home = TempDir::new().unwrap();
    let out = home.path().join("out");
    fs::create_dir(&out).unwrap();
    let hex = blake3::hash(&manifest_json()).to_hex().to_string();

    let run = dry_run(home.path(), &out, &hex, &["--exclude", "metal/*"]);
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

/// Without `-o`, the dry run of a whole bundle reads the manifest cached in
/// the bundle's own `./<hex>` directory and refuses a hidden entry path there,
/// as a real pull does: it exits non-zero, names the path and the `-o` that
/// takes it, and the current directory gains nothing but `<hex>`.
#[test]
fn a_default_dry_run_refuses_a_hidden_path_from_the_binary() {
    let home = TempDir::new().unwrap();
    let cwd = home.path().join("work");
    fs::create_dir(&cwd).unwrap();
    let entry_hash = format!("b3:{}", blake3::hash(b"entry").to_hex());
    let manifest = serde_json::json!({
        "version": 1,
        "entries": [
            {"path": "model.bin", "hash": entry_hash, "size": 10},
            {"path": ".decdn/node.toml", "hash": entry_hash, "size": 20},
        ],
    })
    .to_string()
    .into_bytes();
    let hex = blake3::hash(&manifest).to_hex().to_string();
    let bundle_dir = cwd.join(&hex);
    assert_eq!(cache_manifest(&bundle_dir, &manifest), hex);

    let run = default_dry_run(home.path(), &cwd, &hex);
    let stdout = String::from_utf8_lossy(&run.stdout);
    let stderr = String::from_utf8_lossy(&run.stderr);
    assert!(!run.status.success(), "stdout: {stdout}\nstderr: {stderr}");
    assert!(
        stderr.contains(r#"refusing to pull hidden path ".decdn/node.toml""#),
        "stderr: {stderr}"
    );
    assert!(
        stderr.contains(&format!("`-o ./{hex}`")),
        "stderr: {stderr}"
    );
    assert!(!stdout.contains("would fetch"), "stdout: {stdout}");
    assert_eq!(names_in(&cwd), std::slice::from_ref(&hex));
    assert_eq!(names_in(&bundle_dir), [".decdn-bundles"]);
}

/// Without `-o`, the dry run of a clean bundle plans it into its own `./<hex>`
/// directory, from the manifest cached there.
#[test]
fn a_default_dry_run_plans_into_the_bundles_own_directory() {
    let home = TempDir::new().unwrap();
    let cwd = home.path().join("work");
    fs::create_dir(&cwd).unwrap();
    let hex = blake3::hash(&manifest_json()).to_hex().to_string();
    cache_manifest(&cwd.join(&hex), &manifest_json());

    let run = default_dry_run(home.path(), &cwd, &hex);
    let stdout = String::from_utf8_lossy(&run.stdout);
    let stderr = String::from_utf8_lossy(&run.stderr);
    assert!(run.status.success(), "stderr: {stderr}");
    assert!(
        stdout.contains(&format!("would fetch 2 entr(ies) into ./{hex}:")),
        "stdout: {stdout}"
    );
    assert_eq!(names_in(&cwd), [hex]);
}

/// A file at the bundle's default `./<hex>` (what `decdn get decdn:<hex>`
/// writes) blocks its directory: a default dry run and a `decdn quote` both
/// refuse before any network call, naming the file and the trailing `/` it
/// most likely lacked. The sealed environment has no chain, so a quote that
/// went on would fail on chain resolution instead.
#[test]
fn a_blob_file_at_the_bundles_directory_is_refused_up_front() {
    let home = TempDir::new().unwrap();
    let cwd = home.path().join("work");
    fs::create_dir(&cwd).unwrap();
    let hex = blake3::hash(&manifest_json()).to_hex().to_string();
    fs::write(cwd.join(&hex), manifest_json()).unwrap();

    let dry = default_dry_run(home.path(), &cwd, &hex);
    let quote = common::decdn_command(home.path())
        .current_dir(&cwd)
        .args(["quote", &format!("decdn:{hex}/")])
        .output()
        .expect("run decdn binary");
    for (what, run) in [("get --dry-run", dry), ("quote", quote)] {
        let stderr = String::from_utf8_lossy(&run.stderr);
        assert!(!run.status.success(), "{what}: {stderr}");
        assert!(
            stderr.contains(&format!("./{hex} exists and is not a directory")),
            "{what}: {stderr}"
        );
        assert!(
            stderr.contains("(without the trailing `/`)"),
            "{what}: {stderr}"
        );
    }
    assert_eq!(names_in(&cwd), [hex]);
}
