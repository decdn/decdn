use super::*;

/// An override value that drops the flag instead.
const UNSET: &str = "<unset>";

/// The required flags, with `overrides` (`[flag, value]` pairs)
/// replacing, adding to or ([`UNSET`]) removing them.
fn args(overrides: &[&str]) -> Result<Args, clap::Error> {
    let ab = "ab".repeat(32);
    let mut flags: Vec<(&str, &str)> = vec![
        ("--public-url", "https://up.example.org/"),
        ("--daemon-token", "tok"),
        ("--rpc-url", "https://rpc.example"),
        (
            "--capacity-bond",
            "0x0000000000000000000000000000000000000002",
        ),
        ("--turnstile-secret", "s"),
        ("--turnstile-sitekey", "0x4AAA-key_1"),
        ("--decdn-release", "v0.3.0"),
        ("--decdn-sums-sha256", &ab),
    ];
    for pair in overrides.chunks(2) {
        let (flag, value) = (pair[0], pair[1]);
        flags.retain(|(f, _)| *f != flag);
        if value != UNSET {
            flags.push((flag, value));
        }
    }
    let argv = std::iter::once("sponsord-onramp").chain(flags.iter().flat_map(|(f, v)| [*f, *v]));
    Args::try_parse_from(argv)
}

#[test]
fn reads_required_and_defaults() {
    let cfg = OnrampConfig::from_args(args(&[]).unwrap()).unwrap();
    assert_eq!(cfg.bind, "127.0.0.1:8080".parse().unwrap());
    assert_eq!(cfg.public_url, "https://up.example.org");
    assert_eq!(cfg.daemon_url, "http://127.0.0.1:8090");
    assert_eq!(cfg.releases_base, "https://github.com/decdn");
    assert_eq!(cfg.spending_cap, None);
    assert_eq!(cfg.ttl_secs, None);
    assert_eq!(cfg.decdn_release.tag, "v0.3.0");
    assert_eq!(cfg.decdn_release.version, "0.3.0");
    assert!(!format!("{cfg:?}").contains("\"tok\""));
}

#[test]
fn public_url_is_required() {
    let argv: Vec<String> = std::iter::once("sponsord-onramp".to_owned()).collect();
    assert!(Args::try_parse_from(argv).is_err());
}

#[test]
fn urls_the_installers_cant_quote_are_refused() {
    for bad in [
        "https://up.example.org/$(id)",
        "https://up.example.org/'x",
        "ftp://up.example.org",
        "not a url",
    ] {
        let parsed = args(&["--public-url", bad]).unwrap();
        assert!(OnrampConfig::from_args(parsed).is_err(), "{bad}");
    }
    let plain_http = args(&["--releases-base", "http://mirror.example"]).unwrap();
    assert!(OnrampConfig::from_args(plain_http).is_err());
    // Paths are appended to these two, so a query or fragment would
    // swallow them; the RPC URL may keep its query (an API key).
    for (flag, bad) in [
        ("--public-url", "https://up.example.org/?x=1"),
        ("--public-url", "https://up.example.org/#x"),
        ("--releases-base", "https://mirror.example/?mirror=x"),
    ] {
        let parsed = args(&[flag, bad]).unwrap();
        assert!(OnrampConfig::from_args(parsed).is_err(), "{flag} {bad}");
    }
    let keyed_rpc = args(&["--rpc-url", "https://rpc.example/v2?key=abc"]).unwrap();
    assert!(OnrampConfig::from_args(keyed_rpc).is_ok());
}

#[test]
fn turnstile_settings_are_required_only_for_the_turnstile_gate() {
    let bare = ["--turnstile-secret", UNSET, "--turnstile-sitekey", UNSET];
    let err = OnrampConfig::from_args(args(&bare).unwrap()).unwrap_err();
    assert!(
        err.to_string().contains("ONRAMP_TURNSTILE_SITEKEY"),
        "{err}"
    );
    let no_secret = ["--turnstile-secret", UNSET];
    let err = OnrampConfig::from_args(args(&no_secret).unwrap()).unwrap_err();
    assert!(err.to_string().contains("ONRAMP_TURNSTILE_SECRET"), "{err}");

    let custom = [bare.as_slice(), &["--gate", "custom"]].concat();
    let cfg = OnrampConfig::from_args(args(&custom).unwrap()).unwrap();
    assert_eq!(cfg.gate, GateKind::Custom);
    assert!(cfg.turnstile.is_none());
}

#[test]
fn turnstile_expected_hostname_is_the_public_url_host() {
    for (url, host) in [
        ("https://up.example.org/", "up.example.org"),
        ("https://up.example.org:8443/onramp", "up.example.org"),
        ("https://UP.Example.ORG", "up.example.org"),
    ] {
        let cfg = OnrampConfig::from_args(args(&["--public-url", url]).unwrap()).unwrap();
        assert_eq!(cfg.turnstile.unwrap().expected_hostname, host, "{url}");
    }
}

#[test]
fn a_turnstile_template_without_the_action_is_refused() {
    let dir = std::env::temp_dir().join(format!("onramp-template-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let without = dir.join("without.html");
    let with = dir.join("with.html");
    std::fs::write(&without, r#"<div data-sitekey="{{SITEKEY}}"></div>"#).unwrap();
    std::fs::write(&with, r#"<div data-action="{{ACTION}}"></div>"#).unwrap();

    let refused = args(&["--gate-template", without.to_str().unwrap()]).unwrap();
    let err = OnrampConfig::from_args(refused).unwrap_err();
    assert!(err.to_string().contains("{{ACTION}}"), "{err}");

    let accepted = args(&["--gate-template", with.to_str().unwrap()]).unwrap();
    assert!(OnrampConfig::from_args(accepted).is_ok());

    // A custom gate renders its own page, so the Turnstile action is not
    // required of a template it is given.
    let custom = args(&[
        "--gate",
        "custom",
        "--gate-template",
        without.to_str().unwrap(),
    ])
    .unwrap();
    assert!(OnrampConfig::from_args(custom).is_ok());
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn sitekey_must_be_a_plain_token() {
    let parsed = args(&["--turnstile-sitekey", "<script>"]).unwrap();
    assert!(OnrampConfig::from_args(parsed).is_err());
}

#[test]
fn release_pin_accepts_semver_tags_and_hex_digests() {
    let digest = "0123456789abcdef".repeat(4);
    for (tag, version) in [
        ("v0.3.0", "0.3.0"),
        ("v0.3.1", "0.3.1"),
        ("v0.4.0-rc.1", "0.4.0-rc.1"),
        ("v10.20.30", "10.20.30"),
        ("v1.0.0-rc.1", "1.0.0-rc.1"),
        ("v1.0.0-beta-2", "1.0.0-beta-2"),
    ] {
        let pin = ReleasePin::new(tag, &digest).unwrap();
        assert_eq!(pin.version, version, "{tag}");
    }
}

/// The installers run `decdn get` without `-o`, which v0.1.x refuses and
/// v0.2.x runs without the hidden-path guard, so a pin below v0.3.0 (a
/// pre-release of it included) is refused at startup rather than served to
/// every download.
#[test]
fn release_pin_refuses_a_decdn_below_the_installers_minimum() {
    let digest = "ab".repeat(32);
    for tag in [
        "v0.1.0",
        "v0.1.9",
        "v0.0.4",
        "v0.2.0",
        "v0.2.9",
        "v0.3.0-rc.1",
        "v0.3.0-beta-2",
    ] {
        let err = ReleasePin::new(tag, &digest).unwrap_err().to_string();
        assert!(err.contains("below v0.3.0"), "{tag}: {err}");
    }
    let parsed = args(&["--decdn-release", "v0.2.0"]).unwrap();
    let err = OnrampConfig::from_args(parsed).unwrap_err().to_string();
    assert!(err.contains("ONRAMP_DECDN_RELEASE"), "{err}");
    assert!(err.contains("below v0.3.0"), "{err}");
}

#[test]
fn release_pin_refuses_per_crate_tags() {
    let digest = "ab".repeat(32);
    assert!(ReleasePin::new("decdn-cli-v0.3.0", &digest).is_err());
    assert!(ReleasePin::new("sponsord-v0.3.0", &digest).is_err());
}

#[test]
fn release_pin_rejects_anything_a_script_could_misread() {
    let digest = "ab".repeat(32);
    for tag in [
        "0.3.0",
        "v0.2",
        "v0.3.0.1",
        "v0..0",
        "v0.3.0-",
        "v0.3.0 ",
        "v0.3.0;rm",
        "v0.3.0$(id)",
        "v0.3.0'",
    ] {
        assert!(ReleasePin::new(tag, &digest).is_err(), "{tag:?}");
    }
    for bad in [&"AB".repeat(32), &"ab".repeat(31), &"zz".repeat(32)] {
        assert!(ReleasePin::new("v0.3.0", bad).is_err(), "{bad}");
    }
}
