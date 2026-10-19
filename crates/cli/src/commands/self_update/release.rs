//! Release resolution: which archive this build needs, how the signed
//! `SHA256SUMS` manifest reads, and how the latest tag is found.
//!
//! GitHub is an untrusted mirror here. Nothing this module fetches is acted
//! on until [`super::verify`] has checked the manifest signature and the
//! archive digest.

use std::time::Duration;

use anyhow::{Context, bail};
use bytes::{Bytes, BytesMut};
use semver::Version;

/// The repository whose releases every asset is fetched from.
pub(crate) const REPO_BASE: &str = "https://github.com/decdn/decdn";

/// Where the releases live: `DECDN_RELEASES_BASE` when set, else the project
/// repository. The override points a test or a smoke run at a local server;
/// it changes where the client looks, never what it trusts.
pub(crate) fn releases_base() -> String {
    base_from(std::env::var("DECDN_RELEASES_BASE").ok().as_deref())
}

/// `raw` without its trailing slash, or [`REPO_BASE`] when it is blank.
pub(crate) fn base_from(raw: Option<&str>) -> String {
    match raw.map(str::trim).filter(|s| !s.is_empty()) {
        Some(base) => base.trim_end_matches('/').to_string(),
        None => REPO_BASE.to_string(),
    }
}

/// How a release archive for a target is packed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ArchiveKind {
    /// `decdn-<version>-<target>.tar.gz`, holding the bare `decdn` binary.
    TarGz,
    /// `decdn-<version>-<target>.zip`, holding the bare `decdn.exe`.
    Zip,
}

impl ArchiveKind {
    /// The file name the archive holds at its root.
    pub(crate) const fn binary_name(self) -> &'static str {
        match self {
            Self::TarGz => "decdn",
            Self::Zip => "decdn.exe",
        }
    }
}

/// A target the release pipeline builds an archive for: the six triples of
/// the release matrix, and nothing else.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Target {
    /// `x86_64-unknown-linux-gnu`
    X86_64LinuxGnu,
    /// `aarch64-unknown-linux-gnu`
    Aarch64LinuxGnu,
    /// `x86_64-apple-darwin`
    X86_64Darwin,
    /// `aarch64-apple-darwin`
    Aarch64Darwin,
    /// `x86_64-pc-windows-msvc`
    X86_64WindowsMsvc,
    /// `aarch64-pc-windows-msvc`
    Aarch64WindowsMsvc,
}

impl Target {
    /// The target this binary was built for, when the release pipeline
    /// ships an archive for it. `None` on every other platform (a musl or
    /// BSD build, say), which must install from source instead.
    pub(crate) const fn current() -> Option<Self> {
        if cfg!(all(
            target_arch = "x86_64",
            target_os = "linux",
            target_env = "gnu"
        )) {
            Some(Self::X86_64LinuxGnu)
        } else if cfg!(all(
            target_arch = "aarch64",
            target_os = "linux",
            target_env = "gnu"
        )) {
            Some(Self::Aarch64LinuxGnu)
        } else if cfg!(all(target_arch = "x86_64", target_os = "macos")) {
            Some(Self::X86_64Darwin)
        } else if cfg!(all(target_arch = "aarch64", target_os = "macos")) {
            Some(Self::Aarch64Darwin)
        } else if cfg!(all(
            target_arch = "x86_64",
            target_os = "windows",
            target_env = "msvc"
        )) {
            Some(Self::X86_64WindowsMsvc)
        } else if cfg!(all(
            target_arch = "aarch64",
            target_os = "windows",
            target_env = "msvc"
        )) {
            Some(Self::Aarch64WindowsMsvc)
        } else {
            None
        }
    }

    /// The target triple, as the release asset names spell it.
    pub(crate) const fn triple(self) -> &'static str {
        match self {
            Self::X86_64LinuxGnu => "x86_64-unknown-linux-gnu",
            Self::Aarch64LinuxGnu => "aarch64-unknown-linux-gnu",
            Self::X86_64Darwin => "x86_64-apple-darwin",
            Self::Aarch64Darwin => "aarch64-apple-darwin",
            Self::X86_64WindowsMsvc => "x86_64-pc-windows-msvc",
            Self::Aarch64WindowsMsvc => "aarch64-pc-windows-msvc",
        }
    }

    /// How this target's archive is packed: Windows ships zips, the rest tarballs.
    pub(crate) const fn kind(self) -> ArchiveKind {
        match self {
            Self::X86_64WindowsMsvc | Self::Aarch64WindowsMsvc => ArchiveKind::Zip,
            Self::X86_64LinuxGnu
            | Self::Aarch64LinuxGnu
            | Self::X86_64Darwin
            | Self::Aarch64Darwin => ArchiveKind::TarGz,
        }
    }

    /// The release asset name for this target at `version`.
    pub(crate) fn archive_name(self, version: &Version) -> String {
        let ext = match self.kind() {
            ArchiveKind::TarGz => "tar.gz",
            ArchiveKind::Zip => "zip",
        };
        format!("decdn-{version}-{}.{ext}", self.triple())
    }
}

/// A parsed `SHA256SUMS`: one `(digest, name)` entry per line.
#[derive(Debug)]
pub(crate) struct Manifest {
    entries: Vec<(String, String)>,
}

impl Manifest {
    /// Parse GNU `sha256sum` output: 64 lowercase hex digits, then either
    /// two spaces and the file name (text mode) or one space, `*` and the
    /// file name (binary mode). A CRLF line end is tolerated. Any other line
    /// is an error.
    pub(crate) fn parse(text: &str) -> anyhow::Result<Self> {
        let mut entries = Vec::new();
        for (idx, raw) in text.lines().enumerate() {
            let line = raw.trim_end_matches('\r');
            if line.is_empty() {
                continue;
            }
            let (digest, rest) = line
                .split_once(' ')
                .with_context(|| format!("SHA256SUMS line {}: not `<sha256>  <name>`", idx + 1))?;
            if digest.len() != 64
                || !digest
                    .bytes()
                    .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
            {
                bail!(
                    "SHA256SUMS line {}: not a lowercase sha256 hex digest",
                    idx + 1
                );
            }
            let name = rest
                .strip_prefix(' ')
                .or_else(|| rest.strip_prefix('*'))
                .with_context(|| format!("SHA256SUMS line {}: not `<sha256>  <name>`", idx + 1))?;
            if name.is_empty() {
                bail!("SHA256SUMS line {}: empty file name", idx + 1);
            }
            entries.push((digest.to_string(), name.to_string()));
        }
        Ok(Self { entries })
    }

    /// The hex digest the manifest lists for `name`.
    pub(crate) fn digest_for(&self, name: &str) -> Option<&str> {
        self.entries
            .iter()
            .find(|(_, n)| n == name)
            .map(|(d, _)| d.as_str())
    }
}

/// The release tag in a `…/releases/download/<tag>/<asset>` location.
pub(crate) fn tag_from_location(location: &str) -> Option<String> {
    let path = url::Url::parse(location)
        .map(|u| u.path().to_string())
        .or_else(|_| {
            url::Url::parse(REPO_BASE)
                .and_then(|base| base.join(location))
                .map(|u| u.path().to_string())
        })
        .ok()?;
    let mut segments = path.split('/').filter(|s| !s.is_empty());
    while let Some(seg) = segments.next() {
        if seg == "releases" {
            if segments.next() != Some("download") {
                return None;
            }
            let tag = segments.next()?;
            segments.next()?; // the asset name
            return Some(tag.to_string());
        }
    }
    None
}

/// The version a `vX.Y.Z[-pre]` tag names.
pub(crate) fn version_from_tag(tag: &str) -> anyhow::Result<Version> {
    let bare = tag
        .strip_prefix('v')
        .with_context(|| format!("release tag {tag:?} does not start with `v`"))?;
    Version::parse(bare).with_context(|| format!("release tag {tag:?} is not a version"))
}

/// What the latest release is, relative to the running binary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Upgrade {
    /// A newer stable release exists.
    Newer,
    /// The running binary is the latest release.
    UpToDate,
    /// The latest release is older than the running binary; nothing is applied.
    Downgrade,
    /// The latest release is a pre-release; never offered.
    PreRelease,
}

/// Compare the running version with the latest release.
pub(crate) fn decide(running: &Version, latest: &Version) -> Upgrade {
    if !latest.pre.is_empty() {
        Upgrade::PreRelease
    } else if latest > running {
        Upgrade::Newer
    } else if latest == running {
        Upgrade::UpToDate
    } else {
        Upgrade::Downgrade
    }
}

/// The version of this binary, as `decdn --version` prints it.
pub(crate) fn running_version() -> anyhow::Result<Version> {
    Version::parse(env!("CARGO_PKG_VERSION")).context("this build's version is not semver")
}

/// The two HTTP clients a release lookup needs: one that stops at the first
/// redirect, to read the tag out of it, and one that follows redirects to
/// the asset store.
pub(crate) struct Clients {
    follow: reqwest::Client,
    no_redirect: reqwest::Client,
}

impl Clients {
    /// Build both clients with `timeout` as the whole-request bound.
    pub(crate) fn build(timeout: Duration) -> anyhow::Result<Self> {
        let base = || {
            reqwest::Client::builder()
                .user_agent(concat!("decdn/", env!("CARGO_PKG_VERSION")))
                .connect_timeout(timeout.min(Duration::from_secs(10)))
                .timeout(timeout)
        };
        Ok(Self {
            follow: base().build().context("build HTTP client")?,
            no_redirect: base()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .context("build HTTP client")?,
        })
    }
}

/// The tag of the latest release, read from the redirect GitHub answers for
/// `releases/latest/download/SHA256SUMS`. `releases/latest` excludes drafts
/// and releases marked pre-release.
pub(crate) async fn resolve_latest(clients: &Clients, base: &str) -> anyhow::Result<String> {
    let url = format!("{base}/releases/latest/download/SHA256SUMS");
    let resp = clients
        .no_redirect
        .get(&url)
        .send()
        .await
        .with_context(|| format!("GET {url}"))?;
    let status = resp.status();
    if !status.is_redirection() {
        bail!("GET {url}: expected a redirect to the latest release, got {status}");
    }
    let location = resp
        .headers()
        .get(reqwest::header::LOCATION)
        .and_then(|v| v.to_str().ok())
        .with_context(|| format!("GET {url}: {status} without a Location header"))?;
    tag_from_location(location)
        .with_context(|| format!("GET {url}: redirect {location:?} names no release tag"))
}

/// One release asset, at most `max_bytes` long.
pub(crate) async fn fetch_asset(
    clients: &Clients,
    base: &str,
    tag: &str,
    name: &str,
    max_bytes: usize,
) -> anyhow::Result<Bytes> {
    let url = format!("{base}/releases/download/{tag}/{name}");
    let mut resp = clients
        .follow
        .get(&url)
        .send()
        .await
        .with_context(|| format!("GET {url}"))?;
    let status = resp.status();
    if !status.is_success() {
        bail!("GET {url}: {status}");
    }
    if let Some(len) = resp.content_length()
        && usize::try_from(len).is_ok_and(|len| len > max_bytes)
    {
        bail!("GET {url}: declares {len} bytes, larger than the {max_bytes}-byte limit");
    }
    let mut body = BytesMut::new();
    while let Some(chunk) = resp
        .chunk()
        .await
        .with_context(|| format!("GET {url}: read body"))?
    {
        if body.len().saturating_add(chunk.len()) > max_bytes {
            bail!("GET {url}: body is larger than the {max_bytes}-byte limit");
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body.freeze())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests
mod tests;
