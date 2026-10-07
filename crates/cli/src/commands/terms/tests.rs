use super::*;

fn h(byte: u8) -> B256 {
    B256::repeat_byte(byte)
}

#[test]
fn stale_client_beats_every_other_signal() {
    // Even with the flag set on a TTY, a hash mismatch is always stale.
    assert_eq!(decide(true, true, h(0xAA), h(0xBB)), Decision::StaleClient);
    assert_eq!(
        decide(false, false, h(0xAA), h(0xBB)),
        Decision::StaleClient
    );
}

#[test]
fn matching_hash_with_flag_proceeds() {
    assert_eq!(decide(true, true, h(0x11), h(0x11)), Decision::Proceed);
    assert_eq!(decide(true, false, h(0x11), h(0x11)), Decision::Proceed);
}

#[test]
fn matching_hash_on_tty_without_flag_prompts() {
    assert_eq!(decide(false, true, h(0x11), h(0x11)), Decision::Prompt);
}

#[test]
fn matching_hash_headless_without_flag_needs_flag() {
    assert_eq!(decide(false, false, h(0x11), h(0x11)), Decision::NeedFlag);
}

#[test]
fn embedded_terms_hash_is_nonzero_and_versioned() {
    assert_ne!(terms_hash(), B256::ZERO, "TERMS.md must embed and hash");
    assert_ne!(
        terms_version(),
        "unknown",
        "TERMS.md must carry a Version line"
    );
}

/// Locks the embedded terms hash to `keccak256` of the exact `TERMS.md`
/// bytes (verify out-of-band:
/// `cast keccak 0x$(xxd -p -c1000000 crates/cli/TERMS.md)`).
/// Editing TERMS.md deliberately flips this — that is intended: the hash is
/// consensus-critical, so a terms change is a gated event that must be paired
/// with a governance `setCurrentTermsHash` bump and a new deploy value.
#[test]
fn embedded_terms_hash_matches_committed_terms_md() {
    let expected = alloy::primitives::b256!(
        "7da33feb48d0b64ca41a4478ea67aeb13832b56b7598cca803bc2b9f5ec37f4b"
    );
    assert_eq!(
        terms_hash(),
        expected,
        "TERMS.md changed — update this lock AND the network's currentTermsHash \
         (governance setCurrentTermsHash + CURRENT_TERMS_HASH deploy value)"
    );
}

// The test harness has no controlling terminal, so `ensure_accepted`
// exercises the headless branches for real (is_terminal() == false).

#[test]
fn ensure_accepted_headless_requires_the_flag() {
    // Matching terms but no flag and no TTY → must refuse (NeedFlag).
    assert!(ensure_accepted(terms_hash(), false).is_err());
}

#[test]
fn ensure_accepted_with_flag_and_matching_terms_ok() {
    assert!(ensure_accepted(terms_hash(), true).is_ok());
}

#[test]
fn ensure_accepted_stale_terms_refused_even_with_flag() {
    // A network terms hash that differs from the embedded one is stale and
    // must be refused regardless of the flag — never sign unseen terms.
    assert!(ensure_accepted(B256::repeat_byte(0x99), true).is_err());
}
