use super::*;

/// `[self_update]` rejects a key it does not define, like every other section.
#[test]
fn self_update_rejects_unknown_keys() {
    let err = toml::from_str::<FileConfig>(
        r"
            [self_update]
            auto = true
            ",
    )
    .expect_err("unknown key is refused");
    assert!(err.to_string().contains("auto"), "{err}");
}
