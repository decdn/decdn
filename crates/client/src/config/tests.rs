use super::{DEFAULT_READ_AHEAD_BYTES, PullConfig};

#[test]
fn default_read_ahead_is_the_documented_constant() {
    assert_eq!(
        PullConfig::default().read_ahead_bytes,
        DEFAULT_READ_AHEAD_BYTES
    );
}

#[test]
fn pay_ahead_defaults_are_the_documented_policy() {
    let policy = PullConfig::new().pay_ahead_policy();
    assert_eq!(policy.cap_micro_usdc, 640, "64 MiB at the default rate");
    assert_eq!(policy.divisor, 2);
    assert_eq!(policy.loss_weight, 20);
}
