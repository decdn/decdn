"""Regression tests for check-release-signed.sh.

A crates.io version can never be replaced, so the publish-crates workflow
uploads nothing until the release carries every signature a client checks,
and SHA256SUMS.asc verifies against KEYS. These run the real script with a
`gh` shim that serves a scratch release, and a scratch gpg home whose keys
sign it.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT = REPO_ROOT / ".github/scripts/check-release-signed.sh"

pytestmark = pytest.mark.skipif(
    shutil.which("gpg") is None or shutil.which("gpgconf") is None,
    reason="gpg/gpgconf is not installed",
)

TAR = "decdn-0.0.0-x86_64-unknown-linux-gnu.tar.gz"
ZIP = "decdn-0.0.0-x86_64-pc-windows-msvc.zip"
SIGNED = [
    "SHA256SUMS",
    "SHA256SUMS.asc",
    "SHA256SUMS.minisig",
    TAR,
    f"{TAR}.minisig",
    ZIP,
    f"{ZIP}.minisig",
]

# `release view --json isDraft` answers $GH_DRAFT, `--json assets` lists
# $GH_ASSETS (one name per line) and, with $GH_NOISE, writes a warning to
# stderr, and `release download --pattern P` copies $GH_FILES/P into the --dir.
GH_SHIM = r"""#!/bin/sh
case "$1 $2" in
  "release view")
    case "$*" in
      *isDraft*) echo "${GH_DRAFT-false}" ;;
      *assets*)
        [ -n "${GH_NOISE:-}" ] && echo "SHA256SUMS.minisig" >&2
        printf '%s\n' "$GH_ASSETS" ;;
    esac ;;
  "release download")
    pattern=""; dir=""
    while [ $# -gt 0 ]; do
      [ "$1" = "--pattern" ] && pattern="$2"
      [ "$1" = "--dir" ] && dir="$2"
      shift
    done
    cp "$GH_FILES/$pattern" "$dir/" ;;
  *) echo "gh shim: unexpected call: $*" >&2; exit 9 ;;
esac
"""


class Release:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.gnupghome = root / "g"
        self.gnupghome.mkdir(mode=0o700)
        self.bin = root / "bin"
        self.bin.mkdir()
        shim = self.bin / "gh"
        shim.write_text(GH_SHIM)
        shim.chmod(0o755)
        self.files = root / "files"
        self.files.mkdir()
        (self.files / "SHA256SUMS").write_text(f"{'0' * 64}  {TAR}\n{'0' * 64}  {ZIP}\n")

    def env(self, **extra: str) -> dict[str, str]:
        env = {k: v for k, v in os.environ.items() if not k.startswith(("GH_", "GNUPG"))}
        env.update(
            GNUPGHOME=str(self.gnupghome),
            HOME=str(self.root),
            PATH=f"{self.bin}{os.pathsep}{os.environ['PATH']}",
            GH_FILES=str(self.files),
            GH_REPO="decdn/decdn",
        )
        env.update(extra)
        return env

    def gpg(self, *args: str) -> str:
        return subprocess.run(
            ["gpg", "--batch", "--pinentry-mode", "loopback", "--passphrase", "", *args],
            env=self.env(),
            check=True,
            capture_output=True,
            text=True,
        ).stdout

    def key(self, uid: str, *gpg_args: str, expire: str = "never") -> None:
        self.gpg(*gpg_args, "--quick-gen-key", uid, "default", "default", expire)

    def fingerprint(self, uid: str) -> str:
        listing = self.gpg("--list-keys", "--with-colons", uid).splitlines()
        return next(line.split(":")[9] for line in listing if line.startswith("fpr:"))

    def revoke(self, uid: str) -> None:
        """Imports the revocation certificate gpg wrote when it made the key."""
        fpr = self.fingerprint(uid)
        cert = (self.gnupghome / "openpgp-revocs.d" / f"{fpr}.rev").read_text()
        armed = self.root / "rev.asc"
        # gpg prefixes the armor line with `:` so an accidental import does not
        # revoke the key; stripping it is the documented way to arm it.
        armed.write_text(cert.replace(":-----BEGIN", "-----BEGIN"))
        self.gpg("--import", str(armed))

    def manifest(self, *names: str) -> None:
        (self.files / "SHA256SUMS").write_text("".join(f"{'0' * 64}  {n}\n" for n in names))

    def publish_keys(self, *uids: str) -> Path:
        keys = self.root / "KEYS"
        keys.write_text(self.gpg("--armor", "--export", *uids))
        return keys

    def sign_sums(self, uid: str, *gpg_args: str) -> None:
        asc = self.files / "SHA256SUMS.asc"
        asc.unlink(missing_ok=True)
        self.gpg(
            *gpg_args, "--local-user", uid, "--armor", "--detach-sign",
            "--output", str(asc), str(self.files / "SHA256SUMS"),
        )

    def check(self, keys: Path, assets: list[str], **env: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["bash", str(SCRIPT), "v0.0.0", str(keys)],
            env=self.env(GH_ASSETS="\n".join(assets), **env),
            capture_output=True,
            text=True,
            check=False,
        )


@pytest.fixture
def release():
    # A short root, not tmp_path: gpg-agent's socket path has a length limit.
    root = Path(tempfile.mkdtemp(prefix="crs-"))
    rel = Release(root)
    try:
        rel.key("Maintainer <me@decdn.org>")
        yield rel
    finally:
        subprocess.run(["gpgconf", "--kill", "all"], env=rel.env(), check=False)
        shutil.rmtree(root, ignore_errors=True)


def output(result: subprocess.CompletedProcess[str]) -> str:
    return result.stdout + result.stderr


def test_a_fully_signed_release_passes(release):
    keys = release.publish_keys("me@decdn.org")
    release.sign_sums("me@decdn.org")

    result = release.check(keys, SIGNED)

    assert result.returncode == 0, output(result)
    assert "release v0.0.0 is signed" in result.stdout


def test_a_draft_is_refused(release):
    keys = release.publish_keys("me@decdn.org")
    release.sign_sums("me@decdn.org")

    result = release.check(keys, SIGNED, GH_DRAFT="true")

    assert result.returncode != 0
    assert "is still a draft" in result.stderr, output(result)


def test_a_manifest_signed_by_a_key_outside_keys_is_refused(release):
    # The signer holds a key, but KEYS does not publish it.
    release.key("Stranger <x@example.org>")
    keys = release.publish_keys("me@decdn.org")
    release.sign_sums("x@example.org")

    result = release.check(keys, SIGNED)

    assert result.returncode != 0
    assert "not a good signature by a live key in KEYS" in result.stderr, output(result)


@pytest.mark.parametrize("missing", ["SHA256SUMS", "SHA256SUMS.asc", "SHA256SUMS.minisig"])
def test_a_missing_manifest_signature_is_refused(release, missing):
    keys = release.publish_keys("me@decdn.org")
    release.sign_sums("me@decdn.org")

    result = release.check(keys, [a for a in SIGNED if a != missing])

    assert result.returncode != 0
    assert f"carries no {missing}" in result.stderr, output(result)


def test_an_archive_without_its_minisig_is_refused(release):
    keys = release.publish_keys("me@decdn.org")
    release.sign_sums("me@decdn.org")

    result = release.check(keys, [a for a in SIGNED if a != f"{ZIP}.minisig"])

    assert result.returncode != 0
    assert f"carries no .minisig for: {ZIP}" in result.stderr, output(result)
    assert TAR not in result.stderr.split("carries no .minisig for:", 1)[1]


def test_an_archive_the_manifest_names_but_the_release_lacks_is_refused(release):
    keys = release.publish_keys("me@decdn.org")
    release.sign_sums("me@decdn.org")

    result = release.check(keys, [a for a in SIGNED if a != TAR])

    assert result.returncode != 0
    assert f"does not carry {TAR}, which SHA256SUMS names" in result.stderr, output(result)


def test_a_manifest_signed_by_an_expired_key_is_refused(release):
    # Made and used in 2020, expired in 2021: gpg --verify alone exits 0 on it.
    past = ("--faked-system-time", "20200101T000000")
    release.key("Old <old@decdn.org>", *past, expire="1y")
    keys = release.publish_keys("me@decdn.org", "old@decdn.org")
    release.sign_sums("old@decdn.org", *past)

    result = release.check(keys, SIGNED)

    assert result.returncode != 0
    assert "not a good signature by a live key in KEYS" in result.stderr, output(result)


def test_a_manifest_signed_by_a_revoked_key_is_refused(release):
    release.key("Lost <lost@decdn.org>")
    release.sign_sums("lost@decdn.org")
    release.revoke("lost@decdn.org")
    # KEYS publishes the revocation, the way a maintainer retires a key.
    keys = release.publish_keys("me@decdn.org", "lost@decdn.org")

    result = release.check(keys, SIGNED)

    assert result.returncode != 0
    assert "not a good signature by a live key in KEYS" in result.stderr, output(result)


def test_a_tampered_manifest_is_refused(release):
    keys = release.publish_keys("me@decdn.org")
    release.sign_sums("me@decdn.org")
    release.manifest(TAR)

    result = release.check(keys, SIGNED)

    assert result.returncode != 0
    assert "not a good signature by a live key in KEYS" in result.stderr, output(result)


@pytest.mark.parametrize(
    "archive",
    [
        "decdn-0.0.1-x86_64-unknown-linux-gnu.tar.gz",  # another release
        "decdn-0.0.0-rc1-x86_64-unknown-linux-gnu.tar.gz",  # its candidate
    ],
)
def test_another_releases_manifest_is_refused(release, archive):
    release.manifest(archive)
    keys = release.publish_keys("me@decdn.org")
    release.sign_sums("me@decdn.org")

    result = release.check(keys, [*SIGNED, archive, f"{archive}.minisig"])

    assert result.returncode != 0
    assert f"names {archive}, which is not a 0.0.0 archive" in result.stderr, output(result)


def test_an_empty_manifest_is_refused(release):
    release.manifest()
    keys = release.publish_keys("me@decdn.org")
    release.sign_sums("me@decdn.org")

    result = release.check(keys, SIGNED)

    assert result.returncode != 0
    assert "names no archive" in result.stderr, output(result)


@pytest.mark.parametrize("answer", ["", "maybe"])
def test_an_unreadable_draft_state_is_refused(release, answer):
    keys = release.publish_keys("me@decdn.org")
    release.sign_sums("me@decdn.org")

    result = release.check(keys, SIGNED, GH_DRAFT=answer)

    assert result.returncode != 0
    assert f"unexpected isDraft value from gh: '{answer}'" in result.stderr, output(result)


def test_gh_stderr_is_not_read_as_an_asset(release):
    # gh warns on stderr with an asset's name, and the asset is not attached.
    keys = release.publish_keys("me@decdn.org")
    release.sign_sums("me@decdn.org")

    result = release.check(
        keys, [a for a in SIGNED if a != "SHA256SUMS.minisig"], GH_NOISE="1"
    )

    assert result.returncode != 0
    assert "carries no SHA256SUMS.minisig" in result.stderr, output(result)


def test_an_invalid_keys_file_is_refused(release):
    keys = release.root / "KEYS"
    keys.write_text("not a keyring\n")
    release.sign_sums("me@decdn.org")

    result = release.check(keys, SIGNED)

    assert result.returncode != 0
    assert "is not a valid OpenPGP keyring" in result.stderr, output(result)
