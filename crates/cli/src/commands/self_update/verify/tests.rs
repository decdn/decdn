use minisign_verify::PublicKey;

use super::super::release::Manifest;
use super::*;

const TEST_PUB: &str = include_str!("../testdata/minisign-test.pub");
const SUMS: &[u8] = include_bytes!("../testdata/SHA256SUMS");
const SUMS_SIG: &str = include_str!("../testdata/SHA256SUMS.minisig");

fn test_keys() -> Vec<PublicKey> {
    vec![PublicKey::decode(TEST_PUB).unwrap()]
}

#[test]
fn manifest_signature_verifies_with_the_signing_key() {
    verify_manifest_with(&test_keys(), SUMS, SUMS_SIG).unwrap();
}

/// One flipped byte in the manifest and the signature no longer covers it.
#[test]
fn tampered_manifest_is_refused() {
    let mut sums = SUMS.to_vec();
    if let Some(first) = sums.first_mut() {
        *first ^= 0x01;
    }
    let err = verify_manifest_with(&test_keys(), &sums, SUMS_SIG).unwrap_err();
    assert!(err.to_string().contains("does not verify"), "{err}");
}

/// A signature from a key this build does not embed is refused by name,
/// whatever it signs. The fixtures are signed by the test key, which the
/// binary never trusts, so this also proves the embedded set is enforced.
#[test]
fn signature_from_an_untrusted_key_is_refused() {
    let err = verify_manifest_with(&trusted_keys().unwrap(), SUMS, SUMS_SIG).unwrap_err();
    assert!(
        err.to_string()
            .contains("not signed by a key this decdn build trusts"),
        "{err}"
    );
}

#[test]
fn malformed_signature_is_an_error() {
    let err = verify_manifest_with(&test_keys(), SUMS, "not a signature").unwrap_err();
    assert!(err.to_string().contains("SHA256SUMS.minisig"), "{err}");
}

#[test]
fn sha256_hex_matches_the_known_vector() {
    assert_eq!(
        sha256_hex(b"abc"),
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
}

#[test]
fn verify_archive_checks_the_manifest_digest() {
    let manifest = Manifest::parse(std::str::from_utf8(SUMS).unwrap()).unwrap();
    let name = "decdn-99.0.0-x86_64-unknown-linux-gnu.tar.gz";
    verify_archive(&manifest, name, b"release payload").unwrap();
    let err = verify_archive(&manifest, name, b"release payloaD").unwrap_err();
    assert!(err.to_string().contains("does not match"), "{err}");
    let err =
        verify_archive(&manifest, "decdn-99.0.0-other.tar.gz", b"release payload").unwrap_err();
    assert!(err.to_string().contains("not listed"), "{err}");
}

/// The keys the binary embeds are exactly the `minisign*.pub` files in the
/// crate, so a rotation edits one place in Rust and the release script
/// sees the same set.
#[test]
fn embedded_keys_match_the_files_in_the_crate() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut files: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| {
            let e = e.unwrap();
            let name = e.file_name().to_string_lossy().into_owned();
            (name.starts_with("minisign") && e.path().extension().is_some_and(|x| x == "pub"))
                .then(|| std::fs::read_to_string(e.path()).unwrap())
        })
        .collect();
    files.sort();
    let mut embedded: Vec<String> = TRUSTED_KEYS.iter().map(|k| (*k).to_string()).collect();
    embedded.sort();
    assert_eq!(embedded, files);
    assert!(!trusted_keys().unwrap().is_empty());
}

/// The committed `minisign.pub` is a placeholder until the maintainer key
/// lands; a build that still embeds it must not pass CI.
#[test]
fn embedded_key_is_not_the_placeholder() {
    for key in TRUSTED_KEYS {
        assert!(
            !key.contains("PLACEHOLDER"),
            "crates/cli/minisign.pub is still the placeholder; see RELEASING.md"
        );
    }
}
