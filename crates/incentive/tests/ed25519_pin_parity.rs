//! Pin-parity guard for the Ed25519 differential test vectors (issue #1008).
//!
//! `contracts/src/Ed25519Verifier.sol` must stay at least as strict as the
//! `ed25519-dalek::verify_strict` check deCDN nodes run off-chain (issue #669).
//! The Solidity test vectors that pin that parity are produced by
//! `contracts/test/ed25519-vectors`, a *version-isolated* standalone crate with
//! its own `=`-pinned `Cargo.lock`. That isolation is a trap: if the node's
//! `ed25519-dalek` (the one `decdn-incentive` resolves) is bumped without also
//! bumping the generator's pin, the generator keeps emitting vectors for the old
//! dalek. A "regenerate & diff" freshness check still passes, yet the on-chain
//! verifier is now tested against a *different* reference than the network runs.
//!
//! This test closes that gap: it asserts the `ed25519-dalek` / `curve25519-dalek`
//! versions `decdn-incentive` resolves match the versions the generator's
//! `Cargo.lock` pins. Both are read straight from the committed lockfiles — the
//! root lock version-qualifies `decdn-incentive`'s dependency edge (two dalek
//! trees exist, so it reads `"ed25519-dalek 2.2.0"`), so no `cargo metadata`
//! subprocess is needed.

// Test-only: workspace clippy denies these, but a malformed/absent committed
// lockfile is a hard error (not a recoverable condition), so failing loudly is
// exactly right here. Mirrors crates/node/tests/sighup_signal.rs.
#![allow(clippy::expect_used, clippy::panic)]

use std::path::PathBuf;

/// Crates whose versions the on-chain vectors are pinned against.
const ED25519_DALEK: &str = "ed25519-dalek";
const CURVE25519_DALEK: &str = "curve25519-dalek";

fn workspace_root() -> PathBuf {
    // CARGO_MANIFEST_DIR is `<root>/crates/incentive`.
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
}

fn read_lock(path: &PathBuf) -> toml::Table {
    let text = std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("read lockfile {}: {e}", path.display()));
    text.parse::<toml::Table>()
        .unwrap_or_else(|e| panic!("parse lockfile {}: {e}", path.display()))
}

fn packages(lock: &toml::Table) -> &[toml::Value] {
    lock.get("package")
        .and_then(toml::Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default()
}

fn pkg_field<'a>(pkg: &'a toml::Value, key: &str) -> Option<&'a str> {
    pkg.get(key).and_then(toml::Value::as_str)
}

/// Version of `name` as resolved by (optionally version-qualified) `pkg`.
///
/// A Cargo.lock `dependencies` entry is a bare name (`"foo"`) when only one
/// version of `foo` is in the graph, or a qualified `"foo 1.2.3"` when several
/// are. For the bare form we look the version up from the sole matching package.
fn dep_version(lock: &toml::Table, pkg: &toml::Value, name: &str) -> String {
    let deps = pkg
        .get("dependencies")
        .and_then(toml::Value::as_array)
        .expect("package has a dependencies array");
    let entry = deps
        .iter()
        .filter_map(toml::Value::as_str)
        .find(|d| *d == name || d.starts_with(&format!("{name} ")))
        .unwrap_or_else(|| {
            panic!(
                "{} does not depend on {name}",
                pkg_field(pkg, "name").unwrap_or("<?>")
            )
        });
    // A qualified edge is `"name version"` or `"name version (source)"` when a
    // git/path override coexists with the registry copy; take just the version
    // token so a source suffix can't leak in and cause a spurious mismatch.
    match entry.split_once(' ') {
        Some((_, rest)) => rest.split_whitespace().next().unwrap_or(rest).to_owned(),
        None => sole_version(lock, name),
    }
}

/// Version of the single package named `name`; panics if absent or ambiguous.
fn sole_version(lock: &toml::Table, name: &str) -> String {
    let mut hits = packages(lock)
        .iter()
        .filter(|p| pkg_field(p, "name") == Some(name))
        .filter_map(|p| pkg_field(p, "version"));
    let version = hits
        .next()
        .unwrap_or_else(|| panic!("{name} not found in lockfile"))
        .to_owned();
    assert!(
        hits.next().is_none(),
        "{name} resolves to multiple versions; expected one"
    );
    version
}

fn find_pkg<'a>(lock: &'a toml::Table, name: &str, version: &str) -> &'a toml::Value {
    packages(lock)
        .iter()
        .find(|p| pkg_field(p, "name") == Some(name) && pkg_field(p, "version") == Some(version))
        .unwrap_or_else(|| panic!("{name} {version} not found in lockfile"))
}

/// The dalek versions the node (`decdn-incentive`) actually compiles against.
fn node_side_versions(root_lock: &toml::Table) -> (String, String) {
    let incentive = find_pkg_by_name(root_lock, "decdn-incentive");
    let ed = dep_version(root_lock, incentive, ED25519_DALEK);
    // curve25519-dalek is not a direct dep of decdn-incentive; take the version
    // the node's ed25519-dalek pulls, so it tracks that dalek rather than an
    // unrelated copy (e.g. iroh's).
    let ed_pkg = find_pkg(root_lock, ED25519_DALEK, &ed);
    let cv = dep_version(root_lock, ed_pkg, CURVE25519_DALEK);
    (ed, cv)
}

fn find_pkg_by_name<'a>(lock: &'a toml::Table, name: &str) -> &'a toml::Value {
    packages(lock)
        .iter()
        .find(|p| pkg_field(p, "name") == Some(name))
        .unwrap_or_else(|| panic!("{name} not found in lockfile"))
}

#[test]
fn generator_dalek_pins_match_the_node() {
    let root = workspace_root();
    let root_lock = read_lock(&root.join("Cargo.lock"));
    let gen_lock = read_lock(
        &root
            .join("contracts")
            .join("test")
            .join("ed25519-vectors")
            .join("Cargo.lock"),
    );

    let (node_ed, node_cv) = node_side_versions(&root_lock);
    let gen_ed = sole_version(&gen_lock, ED25519_DALEK);
    let gen_cv = sole_version(&gen_lock, CURVE25519_DALEK);

    let hint = "The Ed25519 differential-vector generator's dalek pins have drifted from \
                the version the node runs. Update the `=` pins in \
                contracts/test/ed25519-vectors/Cargo.toml (refresh its Cargo.lock and the \
                Reference:/version literals in src/main.rs), then regenerate with \
                `cargo run -- --write` and re-run `forge fmt`.";

    assert_eq!(
        node_ed, gen_ed,
        "ed25519-dalek: node resolves {node_ed} but the generator pins {gen_ed}. {hint}"
    );
    assert_eq!(
        node_cv, gen_cv,
        "curve25519-dalek: node resolves {node_cv} but the generator pins {gen_cv}. {hint}"
    );
}
