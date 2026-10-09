//! Configuration: [`Args`], the `ONRAMP_*` environment variables and flags,
//! checked and resolved into [`OnrampConfig`] at startup.

use std::net::SocketAddr;
use std::path::PathBuf;

use alloy_primitives::Address;
use axum::http::HeaderName;
use clap::{ArgGroup, Parser, ValueEnum};
use sponsord_api::secret::Secret;

use crate::net::ClientIpSource;

use sponsord_api::MicroUsdc;
use sponsord_api::daemon::Info;

/// Tag prefix of a `decdn/decdn` release: the workspace shares one version,
/// so its tags are `vMAJOR.MINOR.PATCH`.
pub const DECDN_TAG_PREFIX: &str = "v";

/// Tag prefix of a `decdn-sponsored` release: sponsord's crates are versioned
/// on their own, so its tags name the crate.
pub const CLI_TAG_PREFIX: &str = "decdn-sponsored-v";

/// A GitHub Release the installers download binaries from, pinned by its tag
/// and by the SHA-256 of its `SHA256SUMS` file. The installer checks the
/// downloaded `SHA256SUMS` against `sums_sha256`, then each archive against
/// `SHA256SUMS`, so a release asset replaced after pinning is rejected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReleasePin {
    /// The release tag: a fixed prefix, then `MAJOR.MINOR.PATCH`, optionally
    /// with a `-pre.release` suffix.
    pub tag: String,
    /// The version in `tag`, which names the release's archives
    /// (`<binary>-<version>-<target>`).
    pub version: String,
    /// 64 lowercase hex characters.
    pub sums_sha256: String,
}

impl ReleasePin {
    /// All three values are interpolated into the POSIX and PowerShell
    /// installer scripts, so they are held to a strict shape rather than
    /// escaped.
    ///
    /// # Errors
    ///
    /// Returns an error if `tag` is not `prefix` followed by a semver version,
    /// or `sums_sha256` is not 64 lowercase hex characters.
    pub fn new(prefix: &str, tag: &str, sums_sha256: &str) -> anyhow::Result<Self> {
        let version = tag
            .strip_prefix(prefix)
            .filter(|v| is_semver(v))
            .ok_or_else(|| {
                anyhow::anyhow!("release tag {tag:?} is not {prefix}MAJOR.MINOR.PATCH[-pre]")
            })?;
        anyhow::ensure!(
            sums_sha256.len() == 64
                && sums_sha256
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
            "SHA256SUMS digest {sums_sha256:?} is not 64 lowercase hex characters"
        );
        Ok(Self {
            tag: tag.to_owned(),
            version: version.to_owned(),
            sums_sha256: sums_sha256.to_owned(),
        })
    }
}

fn is_semver(version: &str) -> bool {
    let (core, pre) = match version.split_once('-') {
        Some((core, pre)) => (core, Some(pre)),
        None => (version, None),
    };
    let parts: Vec<&str> = core.split('.').collect();
    parts.len() == 3
        && parts
            .iter()
            .all(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()))
        && pre.is_none_or(|p| {
            !p.is_empty()
                && p.bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'-')
        })
}

/// sponsord-onramp: the public onramp in front of a sponsord daemon. Serves
/// the installers, the gate page, and the API decdn-sponsored polls.
#[derive(Debug, Parser)]
#[command(name = "sponsord-onramp", version)]
#[command(group(ArgGroup::new("daemon_token_src").required(true).args(["daemon_token", "daemon_token_file"])))]
#[command(group(ArgGroup::new("turnstile_secret_src").args(["turnstile_secret", "turnstile_secret_file"])))]
pub struct Args {
    /// Address the HTTP server listens on.
    #[arg(long, env = "ONRAMP_BIND", default_value = "127.0.0.1:8080")]
    pub bind: SocketAddr,
    /// This onramp's public base URL, as users reach it; baked into the
    /// installers and the gate links.
    #[arg(long, env = "ONRAMP_PUBLIC_URL")]
    pub public_url: String,

    /// Base URL of the sponsord daemon.
    #[arg(
        long,
        env = "ONRAMP_DAEMON_URL",
        default_value = "http://127.0.0.1:8090"
    )]
    pub daemon_url: String,
    /// The daemon's bearer token (its `SPONSORD_API_TOKEN`).
    #[arg(long, env = "ONRAMP_DAEMON_TOKEN", hide_env_values = true)]
    pub daemon_token: Option<String>,
    /// File holding the daemon token, instead of `--daemon-token`.
    #[arg(long, env = "ONRAMP_DAEMON_TOKEN_FILE")]
    pub daemon_token_file: Option<PathBuf>,

    /// Public JSON-RPC endpoint end users' `decdn` reads the chain through.
    #[arg(long, env = "ONRAMP_RPC_URL")]
    pub rpc_url: String,
    /// `CapacityBond` contract address, for node discovery.
    #[arg(long, env = "ONRAMP_CAPACITY_BOND_ADDR")]
    pub capacity_bond: Address,
    /// `SlashJudge` contract address, handed to `decdn` when set.
    #[arg(long, env = "ONRAMP_SLASH_JUDGE_ADDR")]
    pub slash_judge: Option<Address>,
    /// Oldest `decdn-sponsored` this onramp works with; older CLIs are told
    /// to re-run the installer.
    #[arg(long, env = "ONRAMP_MIN_CLI_VERSION")]
    pub min_cli_version: Option<semver::Version>,

    /// Cap requested for each capability, in micro-USDC; unset takes the
    /// daemon's maximum.
    #[arg(long, env = "ONRAMP_SPENDING_CAP_MICRO_USDC")]
    pub spending_cap: Option<MicroUsdc>,
    /// TTL requested for each capability, in seconds; unset takes the
    /// daemon's maximum.
    #[arg(long, env = "ONRAMP_TTL_SECS")]
    pub ttl_secs: Option<u64>,

    /// What a person must pass to get a capability.
    #[arg(long, env = "ONRAMP_GATE", value_enum, default_value_t = GateKind::Turnstile)]
    pub gate: GateKind,
    /// Name shown on the gate page.
    #[arg(long, env = "ONRAMP_BRAND_NAME", default_value = "deCDN")]
    pub brand_name: String,
    /// HTML file to serve as the gate page instead of the built-in one; see
    /// `assets/turnstile.html` for the placeholders it may use.
    #[arg(long, env = "ONRAMP_GATE_TEMPLATE")]
    pub gate_template: Option<PathBuf>,

    /// Cloudflare Turnstile server-side secret (required with
    /// `--gate turnstile`).
    #[arg(long, env = "ONRAMP_TURNSTILE_SECRET", hide_env_values = true)]
    pub turnstile_secret: Option<String>,
    /// File holding the Turnstile secret, instead of `--turnstile-secret`.
    #[arg(long, env = "ONRAMP_TURNSTILE_SECRET_FILE")]
    pub turnstile_secret_file: Option<PathBuf>,
    /// Cloudflare Turnstile sitekey, shown in the gate page (required with
    /// `--gate turnstile`).
    #[arg(long, env = "ONRAMP_TURNSTILE_SITEKEY")]
    pub turnstile_sitekey: Option<String>,

    /// Header the trusted reverse proxy in front of the onramp puts the
    /// client's address in (e.g. `CF-Connecting-IP`, `X-Forwarded-For`; of a
    /// list, the right-most address counts). Unset uses the TCP peer. Set it
    /// only when every request comes through exactly that one proxy.
    #[arg(long, env = "ONRAMP_CLIENT_IP_HEADER")]
    pub client_ip_header: Option<HeaderName>,
    /// `POST /v1/fund` requests allowed per client address per minute; 0
    /// disables the limit.
    #[arg(long, env = "ONRAMP_FUND_RATE_PER_MIN", default_value_t = 10)]
    pub fund_rate_per_min: u32,
    /// `GET /v1/capability` polls allowed per client address per minute (the
    /// CLI polls every 2 s); 0 disables the limit.
    #[arg(long, env = "ONRAMP_POLL_RATE_PER_MIN", default_value_t = 120)]
    pub poll_rate_per_min: u32,

    /// Where the installers download release binaries from:
    /// `<base>/<repo>/releases/download/<tag>/`.
    #[arg(
        long,
        env = "ONRAMP_RELEASES_BASE",
        default_value = "https://github.com/decdn"
    )]
    pub releases_base: String,
    /// `decdn/decdn` release tag the installers install `decdn` from
    /// (`vMAJOR.MINOR.PATCH[-pre]`).
    #[arg(long, env = "ONRAMP_DECDN_RELEASE")]
    pub decdn_release: String,
    /// SHA-256 of that release's `SHA256SUMS` file.
    #[arg(long, env = "ONRAMP_DECDN_SUMS_SHA256")]
    pub decdn_sums_sha256: String,
    /// `decdn/sponsord` release tag the installers install `decdn-sponsored`
    /// from (`decdn-sponsored-vMAJOR.MINOR.PATCH[-pre]`).
    #[arg(long, env = "ONRAMP_CLI_RELEASE")]
    pub cli_release: String,
    /// SHA-256 of that release's `SHA256SUMS` file (printed in its release
    /// notes).
    #[arg(long, env = "ONRAMP_CLI_SUMS_SHA256")]
    pub cli_sums_sha256: String,
}

/// The gates built in. More can be added by implementing `gate::Gate`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum GateKind {
    /// A Cloudflare Turnstile captcha.
    Turnstile,
    /// A gate the program supplies itself, for programs that embed
    /// `sponsord-onramp` with their own `Gate` (docs/integrator.md). The
    /// stock `sponsord-onramp` binary refuses it.
    Custom,
}

/// The Turnstile gate's settings.
#[derive(Debug, Clone)]
pub struct TurnstileConfig {
    /// Server-side secret for Cloudflare's siteverify call, from
    /// `ONRAMP_TURNSTILE_SECRET` or `ONRAMP_TURNSTILE_SECRET_FILE`.
    pub secret: Secret,
    /// Letters, digits, `_` and `-` only: it is interpolated into the page.
    pub sitekey: String,
}

/// Resolved configuration.
#[derive(Debug)]
pub struct OnrampConfig {
    /// Address the HTTP server listens on (`ONRAMP_BIND`).
    pub bind: SocketAddr,
    /// This onramp's own public base URL, baked into the installers.
    pub public_url: String,
    /// Base URL of the sponsord daemon (`ONRAMP_DAEMON_URL`).
    pub daemon_url: String,
    /// Bearer token for the daemon's API, from `ONRAMP_DAEMON_TOKEN` or
    /// `ONRAMP_DAEMON_TOKEN_FILE`.
    pub daemon_token: Secret,
    /// Public RPC URL handed to end users.
    pub rpc_url: String,
    /// `CapacityBond` contract address handed to end users for node
    /// discovery (`ONRAMP_CAPACITY_BOND_ADDR`).
    pub capacity_bond: Address,
    /// `SlashJudge` contract address handed to `decdn`; `None` leaves it out
    /// of the profile (`ONRAMP_SLASH_JUDGE_ADDR`).
    pub slash_judge: Option<Address>,
    /// Oldest `decdn-sponsored` this onramp works with; older CLIs are told
    /// to re-run the installer. `None` accepts any (`ONRAMP_MIN_CLI_VERSION`).
    pub min_cli_version: Option<semver::Version>,
    /// Cap requested for each capability; `None` takes the daemon maximum.
    pub spending_cap: Option<MicroUsdc>,
    /// TTL requested for each capability; `None` takes the daemon maximum.
    pub ttl_secs: Option<u64>,
    /// What a person must pass to get a capability (`ONRAMP_GATE`).
    pub gate: GateKind,
    /// Name shown on the gate page (`ONRAMP_BRAND_NAME`).
    pub brand_name: String,
    /// The gate page template, read at startup; `None` is the built-in one.
    pub gate_template: Option<String>,
    /// Set exactly when `gate` is [`GateKind::Turnstile`].
    pub turnstile: Option<TurnstileConfig>,
    /// Where the requester's address comes from: the TCP peer, or the
    /// `ONRAMP_CLIENT_IP_HEADER` a trusted proxy sets.
    pub client_ip: ClientIpSource,
    /// `POST /v1/fund` requests allowed per client address per minute; 0
    /// disables the limit (`ONRAMP_FUND_RATE_PER_MIN`).
    pub fund_rate_per_min: u32,
    /// `GET /v1/capability` polls allowed per client address per minute; 0
    /// disables the limit (`ONRAMP_POLL_RATE_PER_MIN`).
    pub poll_rate_per_min: u32,
    /// Base URL of the release downloads, without a trailing `/`.
    pub releases_base: String,
    /// The `decdn/decdn` release the installers install `decdn` from.
    pub decdn_release: ReleasePin,
    /// The `decdn/sponsord` release the installers install `decdn-sponsored`
    /// from.
    pub cli_release: ReleasePin,
}

impl OnrampConfig {
    /// Parse the command line and environment.
    ///
    /// # Errors
    ///
    /// See [`OnrampConfig::from_args`].
    pub fn load() -> anyhow::Result<Self> {
        Self::from_args(Args::parse())
    }

    /// # Errors
    ///
    /// A secret or template file cannot be read, a URL is malformed or carries
    /// characters the installer scripts can't hold, the sitekey isn't a
    /// plain token, or a release pin is malformed.
    pub fn from_args(args: Args) -> anyhow::Result<Self> {
        let releases_base = base_url("ONRAMP_RELEASES_BASE", &args.releases_base)?;
        anyhow::ensure!(
            releases_base.starts_with("https://"),
            "ONRAMP_RELEASES_BASE must be an https URL"
        );
        let turnstile = match args.gate {
            GateKind::Turnstile => {
                let sitekey = args.turnstile_sitekey.ok_or_else(|| {
                    anyhow::anyhow!(
                        "ONRAMP_TURNSTILE_SITEKEY is required with ONRAMP_GATE=turnstile"
                    )
                })?;
                anyhow::ensure!(
                    !sitekey.is_empty()
                        && sitekey
                            .bytes()
                            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-'),
                    "ONRAMP_TURNSTILE_SITEKEY must be letters, digits, '_' and '-'"
                );
                Some(TurnstileConfig {
                    secret: Secret::resolve(
                        "ONRAMP_TURNSTILE_SECRET",
                        args.turnstile_secret,
                        args.turnstile_secret_file.as_deref(),
                    )?,
                    sitekey,
                })
            }
            GateKind::Custom => None,
        };
        Ok(Self {
            bind: args.bind,
            public_url: base_url("ONRAMP_PUBLIC_URL", &args.public_url)?,
            daemon_url: args.daemon_url,
            daemon_token: Secret::resolve(
                "ONRAMP_DAEMON_TOKEN",
                args.daemon_token,
                args.daemon_token_file.as_deref(),
            )?,
            rpc_url: script_safe_url("ONRAMP_RPC_URL", &args.rpc_url)?,
            capacity_bond: args.capacity_bond,
            slash_judge: args.slash_judge,
            min_cli_version: args.min_cli_version,
            spending_cap: args.spending_cap,
            ttl_secs: args.ttl_secs,
            turnstile,
            gate: args.gate,
            brand_name: args.brand_name,
            gate_template: args
                .gate_template
                .map(|path| {
                    std::fs::read_to_string(&path).map_err(|e| {
                        anyhow::anyhow!("ONRAMP_GATE_TEMPLATE {}: {e}", path.display())
                    })
                })
                .transpose()?,
            client_ip: args
                .client_ip_header
                .map_or(ClientIpSource::Peer, ClientIpSource::Header),
            fund_rate_per_min: args.fund_rate_per_min,
            poll_rate_per_min: args.poll_rate_per_min,
            releases_base,
            decdn_release: ReleasePin::new(
                DECDN_TAG_PREFIX,
                &args.decdn_release,
                &args.decdn_sums_sha256,
            )
            .map_err(|e| anyhow::anyhow!("ONRAMP_DECDN_RELEASE/ONRAMP_DECDN_SUMS_SHA256: {e}"))?,
            cli_release: ReleasePin::new(CLI_TAG_PREFIX, &args.cli_release, &args.cli_sums_sha256)
                .map_err(|e| anyhow::anyhow!("ONRAMP_CLI_RELEASE/ONRAMP_CLI_SUMS_SHA256: {e}"))?,
        })
    }

    /// Refuse requested terms the daemon would reject on every request.
    ///
    /// # Errors
    ///
    /// A configured cap or TTL is zero or above the daemon's maximum.
    pub fn check_against(&self, info: &Info) -> anyhow::Result<()> {
        check_term(
            "ONRAMP_SPENDING_CAP_MICRO_USDC",
            self.spending_cap.map(|c| c.0),
            info.max_spending_cap.0,
        )?;
        check_term("ONRAMP_TTL_SECS", self.ttl_secs, info.max_ttl_secs)
    }
}

/// `raw` as an http(s) URL without a trailing `/`, refusing anything the
/// POSIX or PowerShell installer could misread inside a quoted string.
fn script_safe_url(name: &str, raw: &str) -> anyhow::Result<String> {
    let url = reqwest::Url::parse(raw).map_err(|e| anyhow::anyhow!("{name}: {e}"))?;
    anyhow::ensure!(
        matches!(url.scheme(), "http" | "https"),
        "{name} must be an http or https URL"
    );
    let s = url.as_str().trim_end_matches('/').to_owned();
    anyhow::ensure!(
        !s.chars()
            .any(|c| c.is_whitespace() || matches!(c, '\'' | '"' | '`' | '$' | '\\' | '{' | '}')),
        "{name} {s:?} holds a character the installers can't quote"
    );
    Ok(s)
}

/// A [`script_safe_url`] that paths are appended to (routes, release
/// downloads), so it may carry neither a query nor a fragment: either would
/// swallow everything appended after it.
fn base_url(name: &str, raw: &str) -> anyhow::Result<String> {
    let url = reqwest::Url::parse(raw).map_err(|e| anyhow::anyhow!("{name}: {e}"))?;
    anyhow::ensure!(
        url.query().is_none() && url.fragment().is_none(),
        "{name} must be a base URL, without a query or fragment"
    );
    script_safe_url(name, raw)
}

fn check_term(name: &str, value: Option<u64>, max: u64) -> anyhow::Result<()> {
    if let Some(v) = value {
        anyhow::ensure!(v > 0, "{name} must be above zero");
        anyhow::ensure!(v <= max, "{name} {v} exceeds the daemon maximum {max}");
    }
    Ok(())
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests {
    use super::*;

    /// An override value that drops the flag instead.
    const UNSET: &str = "<unset>";

    /// The required flags, with `overrides` (`[flag, value]` pairs)
    /// replacing, adding to or ([`UNSET`]) removing them.
    fn args(overrides: &[&str]) -> Result<Args, clap::Error> {
        let ab = "ab".repeat(32);
        let cd = "cd".repeat(32);
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
            ("--decdn-release", "v0.1.0"),
            ("--decdn-sums-sha256", &ab),
            ("--cli-release", "decdn-sponsored-v0.2.0-rc.1"),
            ("--cli-sums-sha256", &cd),
        ];
        for pair in overrides.chunks(2) {
            let (flag, value) = (pair[0], pair[1]);
            flags.retain(|(f, _)| *f != flag);
            if value != UNSET {
                flags.push((flag, value));
            }
        }
        let argv =
            std::iter::once("sponsord-onramp").chain(flags.iter().flat_map(|(f, v)| [*f, *v]));
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
        assert_eq!(cfg.decdn_release.tag, "v0.1.0");
        assert_eq!(cfg.decdn_release.version, "0.1.0");
        assert_eq!(cfg.cli_release.tag, "decdn-sponsored-v0.2.0-rc.1");
        assert_eq!(cfg.cli_release.version, "0.2.0-rc.1");
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
    fn sitekey_must_be_a_plain_token() {
        let parsed = args(&["--turnstile-sitekey", "<script>"]).unwrap();
        assert!(OnrampConfig::from_args(parsed).is_err());
    }

    #[test]
    fn release_pin_accepts_semver_tags_and_hex_digests() {
        let digest = "0123456789abcdef".repeat(4);
        for (tag, version) in [
            ("v0.1.0", "0.1.0"),
            ("v10.20.30", "10.20.30"),
            ("v1.0.0-rc.1", "1.0.0-rc.1"),
            ("v1.0.0-beta-2", "1.0.0-beta-2"),
        ] {
            let pin = ReleasePin::new(DECDN_TAG_PREFIX, tag, &digest).unwrap();
            assert_eq!(pin.version, version, "{tag}");
        }
        let pin = ReleasePin::new(CLI_TAG_PREFIX, "decdn-sponsored-v0.2.0-rc.1", &digest).unwrap();
        assert_eq!(pin.version, "0.2.0-rc.1");
    }

    #[test]
    fn release_pin_requires_its_own_prefix() {
        let digest = "ab".repeat(32);
        // A decdn tag is not a CLI release, and the reverse.
        assert!(ReleasePin::new(CLI_TAG_PREFIX, "v0.2.0", &digest).is_err());
        assert!(ReleasePin::new(CLI_TAG_PREFIX, "sponsord-v0.2.0", &digest).is_err());
        assert!(ReleasePin::new(DECDN_TAG_PREFIX, "decdn-sponsored-v0.2.0", &digest).is_err());
    }

    #[test]
    fn release_pin_rejects_anything_a_script_could_misread() {
        let digest = "ab".repeat(32);
        for tag in [
            "0.1.0",
            "v0.1",
            "v0.1.0.1",
            "v0..0",
            "v0.1.0-",
            "v0.1.0 ",
            "v0.1.0;rm",
            "v0.1.0$(id)",
            "v0.1.0'",
        ] {
            assert!(
                ReleasePin::new(DECDN_TAG_PREFIX, tag, &digest).is_err(),
                "{tag:?}"
            );
            let cli = tag.replacen('v', CLI_TAG_PREFIX, 1);
            assert!(
                ReleasePin::new(CLI_TAG_PREFIX, &cli, &digest).is_err(),
                "{cli:?}"
            );
        }
        for bad in [&"AB".repeat(32), &"ab".repeat(31), &"zz".repeat(32)] {
            assert!(
                ReleasePin::new(DECDN_TAG_PREFIX, "v0.1.0", bad).is_err(),
                "{bad}"
            );
        }
    }
}
