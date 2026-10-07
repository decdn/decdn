use super::*;

/// What a `Command` does to one variable. `get_envs` reports a set var as
/// `Some(value)` and an explicitly removed one as `None`; a var it never
/// mentions is absent from the iterator entirely. All three are distinct
/// outcomes here, so name them rather than nesting `Option`s.
#[derive(Debug, PartialEq, Eq)]
enum EnvState {
    Set(OsString),
    Removed,
    Untouched,
}

fn env_of(cmd: &std::process::Command, key: &str) -> EnvState {
    cmd.get_envs()
        .find(|(k, _)| *k == std::ffi::OsStr::new(key))
        .map_or(EnvState::Untouched, |(_, v)| {
            v.map_or(EnvState::Removed, |v| EnvState::Set(v.to_owned()))
        })
}

/// The point of routing every spawn through [`decdn_command`] is that the
/// isolation cannot be omitted. Assert the wiring directly (no chain, no
/// built binary) so dropping it fails a plain
/// `cargo nextest run -p decdn-e2e` rather than silently re-pointing the
/// gated journeys at a developer's real `~/.decdn`.
#[test]
fn hermetic_command_isolates_home_and_carries_the_password() {
    let cmd = hermetic_command(PathBuf::from("decdn"), Path::new("/tmp/decdn-home"), "pw")
        .expect("absolute home accepted");
    assert_eq!(
        env_of(&cmd, "HOME"),
        EnvState::Set("/tmp/decdn-home".into()),
        "HOME must be pinned to the caller's tempdir"
    );
    assert_eq!(
        env_of(&cmd, decdn_incentive::eth_identity::KEYSTORE_PASSWORD_ENV),
        EnvState::Set("pw".into()),
        "the keystore password must reach the child without a prompt"
    );
    assert_eq!(cmd.get_program(), std::ffi::OsStr::new("decdn"));
}

/// An empty or relative `HOME` is not a harmless input: `dirs` treats empty
/// as absent and falls back to `getpwuid`, i.e. straight back to the real
/// home. Reject it loudly instead of silently reproducing #1332.
#[test]
fn hermetic_command_rejects_a_non_absolute_home() {
    for bad in ["", "relative/dir", "node.toml"] {
        let err = hermetic_command(PathBuf::from("decdn"), Path::new(bad), "pw")
            .expect_err("a non-absolute HOME must be rejected");
        assert!(
            format!("{err:#}").contains("must be absolute"),
            "unexpected error for {bad:?}: {err:#}"
        );
    }
}

/// An inherited `DECDN_*` var outranks the fixture's rendered config, so a
/// developer's exported `DECDN_DATA_DIR` would point a journey back at
/// their real data dir — #1332 one variable over.
#[test]
fn strip_decdn_env_removes_the_namespace_and_nothing_else() {
    let mut cmd = std::process::Command::new("decdn");
    cmd.env("PATH", "/usr/bin");
    strip_decdn_env(
        &mut cmd,
        ["DECDN_DATA_DIR", "DECDN_RPC_URL", "PATH", "HOME"]
            .into_iter()
            .map(OsString::from),
    );
    assert_eq!(
        env_of(&cmd, "DECDN_DATA_DIR"),
        EnvState::Removed,
        "an inherited DECDN_* var must be removed"
    );
    assert_eq!(env_of(&cmd, "DECDN_RPC_URL"), EnvState::Removed);
    assert_eq!(
        env_of(&cmd, "PATH"),
        EnvState::Set("/usr/bin".into()),
        "a non-DECDN var must survive untouched"
    );
    assert_eq!(
        env_of(&cmd, "HOME"),
        EnvState::Untouched,
        "the strip must not invent entries for non-DECDN keys"
    );
}
