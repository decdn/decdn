use super::*;

const SAMPLE: &str = r#"
        onramp_url = "https://onramp.example.com"
        decdn_bin = "decdn"
    "#;

/// Compared against the platform's own home directory (`$HOME` on Unix,
/// the profile folder on Windows) rather than a faked `HOME`, which
/// Windows does not consult.
#[test]
fn parses_and_defaults_the_data_dir() {
    let cfg = Config::from_toml_str(SAMPLE).unwrap();
    assert_eq!(cfg.onramp_url, "https://onramp.example.com");
    assert_eq!(cfg.data_dir, home().unwrap().join(".decdn/sponsored"));
}

#[test]
fn expands_a_home_relative_data_dir() {
    let cfg = Config::from_toml_str(&format!("{SAMPLE}\ndata_dir = \"~/elsewhere\"\n")).unwrap();
    assert_eq!(cfg.data_dir, home().unwrap().join("elsewhere"));
}

#[test]
fn ignores_unknown_fields() {
    let with_extra = format!("{SAMPLE}\nrpc_url = \"https://old.example\"\n");
    assert!(Config::from_toml_str(&with_extra).is_ok());
}

/// A lookup over `pairs`, standing in for the process environment.
fn env(pairs: &[(&str, &str)]) -> impl Fn(&str) -> anyhow::Result<Option<String>> {
    let pairs: Vec<(String, String)> = pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
        .collect();
    move |name| {
        Ok(pairs
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.clone()))
    }
}

#[test]
fn env_without_an_onramp_url_configures_nothing() {
    let cfg = Config::from_env(env(&[("DECDN_SPONSOR_DECDN_BIN", "/opt/decdn")])).unwrap();
    assert!(cfg.is_none());
}

#[test]
fn env_defaults_decdn_and_the_data_dir() {
    let cfg = Config::from_env(env(&[(
        "DECDN_SPONSOR_ONRAMP_URL",
        "https://onramp.example.com",
    )]))
    .unwrap()
    .unwrap();
    assert_eq!(cfg.onramp_url, "https://onramp.example.com");
    assert_eq!(cfg.decdn_bin, "decdn");
    assert_eq!(cfg.data_dir, home().unwrap().join(".decdn/sponsored"));
}

#[test]
fn env_overrides_decdn_and_the_data_dir() {
    let cfg = Config::from_env(env(&[
        ("DECDN_SPONSOR_ONRAMP_URL", "https://onramp.example.com"),
        ("DECDN_SPONSOR_DECDN_BIN", "/opt/decdn"),
        ("DECDN_SPONSOR_DATA_DIR", "/var/lib/sponsored"),
    ]))
    .unwrap()
    .unwrap();
    assert_eq!(cfg.decdn_bin, "/opt/decdn");
    assert_eq!(cfg.data_dir, PathBuf::from("/var/lib/sponsored"));
}

#[test]
fn env_expands_a_home_relative_data_dir() {
    let cfg = Config::from_env(env(&[
        ("DECDN_SPONSOR_ONRAMP_URL", "https://onramp.example.com"),
        ("DECDN_SPONSOR_DATA_DIR", "~/elsewhere"),
    ]))
    .unwrap()
    .unwrap();
    assert_eq!(cfg.data_dir, home().unwrap().join("elsewhere"));
}

#[test]
fn env_reads_empty_values_as_unset() {
    let none = Config::from_env(env(&[
        ("DECDN_SPONSOR_ONRAMP_URL", ""),
        ("DECDN_SPONSOR_DECDN_BIN", "/opt/decdn"),
    ]))
    .unwrap();
    assert!(none.is_none());

    let cfg = Config::from_env(env(&[
        ("DECDN_SPONSOR_ONRAMP_URL", "https://onramp.example.com"),
        ("DECDN_SPONSOR_DECDN_BIN", ""),
        ("DECDN_SPONSOR_DATA_DIR", ""),
    ]))
    .unwrap()
    .unwrap();
    assert_eq!(cfg.decdn_bin, "decdn");
    assert_eq!(cfg.data_dir, home().unwrap().join(".decdn/sponsored"));
}

#[test]
fn env_lookup_errors_are_not_read_as_unset() {
    let failing = |name: &str| -> anyhow::Result<Option<String>> {
        anyhow::bail!("{name} is set but is not valid UTF-8")
    };
    let err = Config::from_env(failing).unwrap_err();
    assert!(
        err.to_string().contains("DECDN_SPONSOR_ONRAMP_URL"),
        "{err}"
    );
}

/// The image has no file: with the URL set, a missing one must not matter.
#[test]
fn load_with_an_onramp_url_does_not_read_the_file() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("sponsor.toml");
    let missing = missing.to_str().unwrap();
    let cfg = Config::load_with(env(&[
        ("DECDN_SPONSOR_ONRAMP_URL", "https://env.example.com"),
        ("DECDN_SPONSOR_PROFILE", missing),
    ]))
    .unwrap();
    assert_eq!(cfg.onramp_url, "https://env.example.com");
}

/// Env mode takes nothing from a file that does exist, not even the fields
/// the environment leaves unset.
#[test]
fn load_with_an_onramp_url_ignores_an_existing_file() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("sponsor.toml");
    std::fs::write(
        &file,
        "onramp_url = \"https://file.example.com\"\n\
         decdn_bin = \"/file/decdn\"\n\
         data_dir = \"/file/data\"\n",
    )
    .unwrap();
    let cfg = Config::load_with(env(&[
        ("DECDN_SPONSOR_ONRAMP_URL", "https://env.example.com"),
        ("DECDN_SPONSOR_PROFILE", file.to_str().unwrap()),
    ]))
    .unwrap();
    assert_eq!(cfg.onramp_url, "https://env.example.com");
    assert_eq!(cfg.decdn_bin, "decdn");
    assert_eq!(cfg.data_dir, home().unwrap().join(".decdn/sponsored"));
}

#[test]
fn load_without_an_onramp_url_reads_the_profile() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("sponsor.toml");
    std::fs::write(&file, SAMPLE).unwrap();
    let cfg = Config::load_with(env(&[("DECDN_SPONSOR_PROFILE", file.to_str().unwrap())])).unwrap();
    assert_eq!(cfg.onramp_url, "https://onramp.example.com");
}

#[test]
fn load_without_a_file_names_the_env_alternative() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("sponsor.toml");
    let err = Config::load_with(env(&[("DECDN_SPONSOR_PROFILE", missing.to_str().unwrap())]))
        .unwrap_err();
    assert!(
        err.to_string().contains("DECDN_SPONSOR_ONRAMP_URL"),
        "{err}"
    );
}
