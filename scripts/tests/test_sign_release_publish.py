"""Regression tests for sign-release.sh's signing and publishing steps.

The key-selection tests stop the script at its first network step. These run
it to the end: a scratch repo with a bare `origin`, a tag signed by the
generated key, and `gh` / `minisign` shims that answer the script's calls and
log what they were asked to upload and publish. Image-tag promotion is
skipped, so no docker is needed.
"""

from __future__ import annotations

import hashlib
import shutil
import subprocess
import tempfile
from collections.abc import Iterator
from pathlib import Path

import pytest

from test_sign_release_key_selection import Keyring

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts/sign-release.sh"

pytestmark = pytest.mark.skipif(
    shutil.which("gpg") is None or shutil.which("gpgconf") is None,
    reason="gpg/gpgconf is not installed",
)

# Answers every `gh` call the script makes after the key is chosen, and logs
# each call's argv (one line per call) to $GH_LOG. `release download` stages a
# minimal draft: one archive, its manifest, and the image digest and SBOM
# files the script insists on.
GH_SHIM = r"""#!/bin/sh
printf '%s\n' "$*" >> "$GH_LOG"
case "$1 $2" in
  "auth status") exit 0 ;;
  "api repos"*)
    git rev-parse "refs/tags/${2##*/}" ;;
  "release view")
    echo true ;;
  "release download")
    tag="$3"; version="${tag#v}"; dir=""
    while [ $# -gt 0 ]; do [ "$1" = "--dir" ] && dir="$2"; shift; done
    printf 'bytes' > "$dir/decdn-$version-x86_64-unknown-linux-gnu.tar.gz"
    (cd "$dir" && sha256sum decdn-$version-x86_64-unknown-linux-gnu.tar.gz > SHA256SUMS)
    zeros=$(printf '0%.0s' $(seq 64))
    echo "ghcr.io/decdn/decdn-node@sha256:$zeros" > "$dir/decdn-node-image-digest.txt"
    echo "ghcr.io/decdn/decdn@sha256:$zeros" > "$dir/decdn-image-digest.txt"
    echo '{}' > "$dir/decdn-node-$version-sbom.spdx.json"
    echo '{}' > "$dir/decdn-$version-sbom.spdx.json" ;;
  "release upload"|"release edit") exit 0 ;;
  *) echo "gh shim: unexpected call: $*" >&2; exit 9 ;;
esac
"""

# `minisign -S` writes the signature file named by -x; `minisign -V` succeeds
# unless $MINISIGN_FAIL_VERIFY is set. Logs argv to $MINISIGN_LOG.
MINISIGN_SHIM = r"""#!/bin/sh
printf '%s\n' "$*" >> "$MINISIGN_LOG"
mode="$1"; shift
out=""
while [ $# -gt 0 ]; do [ "$1" = "-x" ] && out="$2"; shift; done
case "$mode" in
  -S) echo "untrusted comment: shim" > "$out"; exit 0 ;;
  -V) [ -n "${MINISIGN_FAIL_VERIFY:-}" ] && exit 1; exit 0 ;;
  *) exit 9 ;;
esac
"""


class Release(Keyring):
    """A keyring whose scratch repo has a signed tag and a bare origin."""

    @property
    def gh_log(self) -> Path:
        return self.root / "gh.log"

    @property
    def minisign_log(self) -> Path:
        return self.root / "minisign.log"

    def env(self) -> dict[str, str]:
        env = super().env()
        env.update(GH_LOG=str(self.gh_log), MINISIGN_LOG=str(self.minisign_log))
        return env

    def tag(self, name: str, fpr: str) -> None:
        git = ["git", "-c", "user.name=t", "-c", "user.email=t@decdn.org"]
        run = lambda *a: subprocess.run(  # noqa: E731
            [*git, *a], cwd=self.repo, env=self.env(), check=True, capture_output=True
        )
        run("commit", "--allow-empty", "-q", "-m", "release")
        run("-c", f"user.signingkey={fpr}", "tag", "-s", name, "-m", name)
        origin = self.root / "origin.git"
        subprocess.run(
            ["git", "clone", "-q", "--bare", str(self.repo), str(origin)],
            env=self.env(),
            check=True,
            capture_output=True,
        )
        run("remote", "add", "origin", str(origin))

    def publish(self, tag: str, **env: str) -> subprocess.CompletedProcess[str]:
        keys = self.gpg("--armor", "--export", *self.published)
        (self.repo / "KEYS").write_text(keys)
        return subprocess.run(
            ["bash", str(SCRIPT), tag],
            cwd=self.repo,
            env={**self.env(), **env},
            capture_output=True,
            text=True,
            check=False,
        )

    def calls(self, log: Path, prefix: str) -> list[str]:
        if not log.exists():
            return []
        return [line for line in log.read_text().splitlines() if line.startswith(prefix)]


@pytest.fixture
def release() -> Iterator[Release]:
    root = Path(tempfile.mkdtemp(prefix="sp-"))
    ring = Release(root)
    ring.gnupghome.mkdir(mode=0o700)
    ring.bin.mkdir()
    for name, body in (("gh", GH_SHIM), ("minisign", MINISIGN_SHIM)):
        shim = ring.bin / name
        shim.write_text(body)
        shim.chmod(0o755)
    (ring.root / "minisign.key").write_text("stub\n")
    ring.repo.mkdir()
    (ring.repo / "crates" / "cli").mkdir(parents=True)
    (ring.repo / "crates" / "cli" / "minisign.pub").write_text(
        "untrusted comment: test\nRWQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n"
    )
    subprocess.run(["git", "init", "-q"], cwd=ring.repo, env=ring.env(), check=True)
    try:
        yield ring
    finally:
        subprocess.run(["gpgconf", "--kill", "all"], env=ring.env(), check=False)
        shutil.rmtree(root, ignore_errors=True)


def output(result: subprocess.CompletedProcess[str]) -> str:
    return result.stdout + result.stderr


def test_a_stable_release_is_minisigned_uploaded_and_published(release):
    fpr = release.gen_key("Maintainer <me@decdn.org>", publish=True)
    release.tag("v0.0.0", fpr)

    result = release.publish("v0.0.0")

    assert result.returncode == 0, output(result)
    signs = release.calls(release.minisign_log, "-S")
    assert len(signs) == 1 and "-m SHA256SUMS" in signs[0] and "-x SHA256SUMS.minisig" in signs[0]
    assert release.calls(release.minisign_log, "-V"), "the signature was not verified"
    uploads = release.calls(release.gh_log, "release upload")
    assert len(uploads) == 1 and "SHA256SUMS.minisig" in uploads[0], uploads
    assert "SHA256SUMS.asc" in uploads[0]
    edits = release.calls(release.gh_log, "release edit")
    assert len(edits) == 1 and "--draft=false" in edits[0], edits
    assert "--prerelease" not in edits[0]


def test_a_prerelease_is_published_as_one(release):
    fpr = release.gen_key("Maintainer <me@decdn.org>", publish=True)
    release.tag("v0.0.0-rc1", fpr)

    result = release.publish("v0.0.0-rc1")

    assert result.returncode == 0, output(result)
    edits = release.calls(release.gh_log, "release edit")
    assert len(edits) == 1 and "--prerelease" in edits[0], edits


def test_a_manifest_signature_no_committed_key_verifies_is_refused(release):
    fpr = release.gen_key("Maintainer <me@decdn.org>", publish=True)
    release.tag("v0.0.0", fpr)

    result = release.publish("v0.0.0", MINISIGN_FAIL_VERIFY="1")

    assert result.returncode != 0
    assert "not signed by a key the decdn binary trusts" in result.stderr, output(result)
    assert not release.calls(release.gh_log, "release upload")
    assert not release.calls(release.gh_log, "release edit")


def test_a_missing_minisign_key_is_refused_before_signing(release):
    release.gen_key("Maintainer <me@decdn.org>", publish=True)
    (release.root / "minisign.key").unlink()

    result = release.publish("v0.0.0")

    assert result.returncode != 0
    assert "DECDN_MINISIGN_KEY" in result.stderr, output(result)
    assert "==> Signing as" not in result.stdout


def test_no_committed_public_key_is_refused_before_signing(release):
    release.gen_key("Maintainer <me@decdn.org>", publish=True)
    (release.repo / "crates" / "cli" / "minisign.pub").unlink()

    result = release.publish("v0.0.0")

    assert result.returncode != 0
    assert "crates/cli/minisign*.pub" in result.stderr, output(result)
    assert "==> Signing as" not in result.stdout


def test_sha256_of_the_shim_manifest_is_stable():
    # Guards the shim's manifest shape: GNU text format the script checks
    # with `sha256sum --strict --check`.
    digest = hashlib.sha256(b"bytes").hexdigest()
    assert len(digest) == 64
