use super::{DEFAULT_READ_AHEAD_BYTES, PullConfig};

#[test]
fn default_read_ahead_is_the_documented_constant() {
    assert_eq!(
        PullConfig::default().read_ahead_bytes,
        DEFAULT_READ_AHEAD_BYTES
    );
    assert_eq!(DEFAULT_READ_AHEAD_BYTES, 16 * 1024 * 1024);
}

#[test]
fn construction_is_a_pure_const() {
    // A `const` value proves construction runs at compile time — no network
    // or chain access can hide in a `const fn`.
    const CFG: PullConfig = PullConfig::new();
    assert_eq!(CFG.read_ahead_bytes, DEFAULT_READ_AHEAD_BYTES);
}
