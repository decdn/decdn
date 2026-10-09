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
