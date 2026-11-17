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
fn pay_ahead_defaults_are_the_documented_policy() {
    let policy = PullConfig::new().pay_ahead_policy();
    assert_eq!(policy.cap_micro_usdc, 640, "64 MiB at the default rate");
    assert_eq!(policy.divisor, 2);
    assert_eq!(policy.loss_weight, 20);
}

#[test]
fn construction_is_a_pure_const() {
    // A `const` value proves construction runs at compile time — no network
    // or chain access can hide in a `const fn`.
    const CFG: PullConfig = PullConfig::new();
    assert_eq!(CFG.read_ahead_bytes, DEFAULT_READ_AHEAD_BYTES);
}
