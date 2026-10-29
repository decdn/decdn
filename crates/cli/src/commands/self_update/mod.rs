//! `decdn self-update`: replace the running binary with the latest signed
//! release, and the once-a-day notice that a newer one exists.
//!
//! The order is deliberate: ownership first, offline, so a binary this
//! command may not replace costs no download; then the latest tag; then the
//! signed manifest, which must verify against an embedded minisign key
//! before anything else is trusted; then the archive, checked against the
//! manifest and unpacked; and only then the swap, staged beside the running
//! binary and renamed over it.

pub(crate) mod check;
pub(crate) mod install;
pub(crate) mod release;
pub(crate) mod verify;

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, bail};
use decdn_common::cli::SelfUpdateArgs;
use minisign_verify::PublicKey;
use semver::Version;
use tempfile::NamedTempFile;

pub use check::{NudgePolicy, Pending, start as start_nudge};
use install::Ownership;
use release::{Clients, Target, Upgrade};

/// Whole-request bound on each download; an archive is tens of megabytes.
const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(600);
/// `SHA256SUMS` holds one short line per release archive; anything near
/// this is not it.
const MAX_MANIFEST_BYTES: usize = 64 * 1024;
/// A minisign signature is four short lines.
const MAX_SIGNATURE_BYTES: usize = 4 * 1024;
/// Generous ceiling for a release archive and the binary inside it.
const MAX_ARCHIVE_BYTES: usize = 256 * 1024 * 1024;

/// Run `decdn self-update`.
pub async fn self_update(args: &SelfUpdateArgs) -> anyhow::Result<()> {
    let target = Target::current().context(
        "no release archive is built for this platform; build from source or run \
         `cargo install --locked decdn-cli`",
    )?;
    // `~/.decdn` is the data dir; its parent is the home the cargo bin
    // directories hang off.
    let data_dir =
        decdn_common::cli::default_data_dir().context("cannot determine the home directory")?;
    let home = data_dir.parent().unwrap_or(&data_dir).to_path_buf();
    let exe = install::running_exe()?;
    let cargo_home = std::env::var_os("CARGO_HOME").map(PathBuf::from);
    let install_root = std::env::var_os("CARGO_INSTALL_ROOT").map(PathBuf::from);
    let cargo_bins = install::cargo_bin_dirs(&home, cargo_home.as_deref(), install_root.as_deref());
    match install::classify(&exe, &cargo_bins, install::probe_writable) {
        Ownership::Replaceable => {}
        Ownership::CargoBin => bail!(
            "{} was installed by cargo; run `cargo binstall --locked decdn-cli` or \
             `cargo install --locked decdn-cli`, whichever installed it, to update it",
            exe.display()
        ),
        Ownership::NotWritable(reason) => bail!(
            "cannot replace {}: {reason}. Update it the way it was installed (`docker pull`, \
             the package manager, or the fleet tooling that placed it)",
            exe.display()
        ),
    }

    let running = release::running_version()?;
    let request = Request {
        keys: &verify::trusted_keys()?,
        target,
        running: &running,
        exe: &exe,
        clients: &Clients::build(DOWNLOAD_TIMEOUT)?,
        base: &release::releases_base(),
        check: args.check,
    };
    match apply(&request, |staged| install::swap(staged, &exe)).await? {
        Outcome::UpToDate => println!("decdn {running} is up to date"),
        Outcome::Downgrade { latest } => println!(
            "the latest release ({latest}) is older than this build ({running}); nothing to do"
        ),
        Outcome::PreRelease { latest } => {
            println!("the latest release ({latest}) is a pre-release; nothing to do");
        }
        Outcome::Available { latest } => println!(
            "decdn {latest} is available (signed manifest ok); run `decdn self-update` to \
             install it over {}",
            exe.display()
        ),
        Outcome::Updated { latest } => {
            println!("updated {}: {running} -> {latest}", exe.display());
        }
    }
    Ok(())
}

/// Everything [`apply`] needs that the command resolves up front.
pub(crate) struct Request<'a> {
    /// The minisign keys a release manifest may be signed with.
    pub(crate) keys: &'a [PublicKey],
    /// The target whose archive to install.
    pub(crate) target: Target,
    /// The version of the running binary.
    pub(crate) running: &'a Version,
    /// The canonical path of the running binary.
    pub(crate) exe: &'a Path,
    /// The HTTP clients the lookup and downloads use.
    pub(crate) clients: &'a Clients,
    /// The release page, without a trailing slash.
    pub(crate) base: &'a str,
    /// Report only; download no archive and change nothing.
    pub(crate) check: bool,
}

/// What [`apply`] did.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Outcome {
    /// The running binary is the latest release.
    UpToDate,
    /// The latest release is older than the running binary; nothing applied.
    Downgrade {
        /// The release's version.
        latest: Version,
    },
    /// The latest release is a pre-release; nothing applied.
    PreRelease {
        /// The release's version.
        latest: Version,
    },
    /// A newer release exists and its manifest verifies; `check` stopped here.
    Available {
        /// The release's version.
        latest: Version,
    },
    /// The binary was replaced.
    Updated {
        /// The installed version.
        latest: Version,
    },
}

/// Resolve the latest release, verify it, and, unless `request.check`, hand
/// the staged new binary to `swap`. Nothing reaches `swap` that a trusted key
/// did not vouch for, through the manifest signature and the archive digest.
pub(crate) async fn apply(
    request: &Request<'_>,
    swap: impl FnOnce(NamedTempFile) -> anyhow::Result<()>,
) -> anyhow::Result<Outcome> {
    let Request {
        keys,
        target,
        running,
        exe,
        clients,
        base,
        check,
    } = *request;
    let tag = release::resolve_latest(clients, base).await?;
    let latest = release::version_from_tag(&tag)?;
    match release::decide(running, &latest) {
        Upgrade::Newer => {}
        Upgrade::UpToDate => return Ok(Outcome::UpToDate),
        Upgrade::Downgrade => return Ok(Outcome::Downgrade { latest }),
        Upgrade::PreRelease => return Ok(Outcome::PreRelease { latest }),
    }

    let sums = release::fetch_asset(clients, base, &tag, "SHA256SUMS", MAX_MANIFEST_BYTES).await?;
    let minisig = release::fetch_asset(
        clients,
        base,
        &tag,
        "SHA256SUMS.minisig",
        MAX_SIGNATURE_BYTES,
    )
    .await?;
    let minisig = std::str::from_utf8(&minisig).context("SHA256SUMS.minisig is not UTF-8")?;
    let manifest = verify::verify_manifest_with(keys, &sums, minisig)?;
    let name = target.archive_name(&latest);
    if manifest.digest_for(&name).is_none() {
        bail!("release {tag} ships no {name}");
    }
    if check {
        return Ok(Outcome::Available { latest });
    }

    eprintln!("Downloading {name}...");
    let archive = release::fetch_asset(clients, base, &tag, &name, MAX_ARCHIVE_BYTES).await?;
    verify::verify_archive(&manifest, &name, &archive)?;
    let bytes = install::extract_binary(&archive, target.kind(), MAX_ARCHIVE_BYTES as u64)?;
    let dir = exe
        .parent()
        .context("the running binary has no parent directory")?;
    let staged = install::stage(dir, &bytes)?;
    swap(staged)?;
    Ok(Outcome::Updated { latest })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests
mod tests;
