"""Regression tests for sign-release.sh's signing and publishing steps.

The key-selection tests stop the script at its first network step. These run
it to the end: a scratch repo with a bare `origin`, a tag signed by the
generated key, and `gh` / `minisign` shims that answer the script's calls and
log what they were asked to upload and publish. Image-tag promotion is
skipped, so no docker is needed.
"""

from __future__ import annotations

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
    echo "ghcr.io/decdn/sponsord@sha256:$zeros" > "$dir/sponsord-image-digest.txt"
    echo "ghcr.io/decdn/sponsord-onramp@sha256:$zeros" > "$dir/sponsord-onramp-image-digest.txt"
    for img in decdn-node decdn sponsord sponsord-onramp; do
      echo '{}' > "$dir/$img-$version-sbom.spdx.json"
    done ;;
  "release upload"|"release edit") exit 0 ;;
  *) echo "gh shim: unexpected call: $*" >&2; exit 9 ;;
esac
"""

# Models key identity with a token: a secret key file holds one token line,
# a public key file mentions the token of the key it belongs to. `minisign -S`
# writes `signed-with:<token>` into the file named by -x unless
# $MINISIGN_FAIL_SIGN is set; `minisign -V` succeeds iff the public key file
# named by -p mentions the signature's token, unless $MINISIGN_FAIL_VERIFY is
# set. Logs argv to $MINISIGN_LOG.
MINISIGN_SHIM = r"""#!/bin/sh
printf '%s\n' "$*" >> "$MINISIGN_LOG"
mode="$1"; shift
out=""; sec=""; pub=""
while [ $# -gt 0 ]; do
  case "$1" in -x) out="$2";; -s) sec="$2";; -p) pub="$2";; esac
  shift
done
case "$mode" in
  -S) [ -n "${MINISIGN_FAIL_SIGN:-}" ] && { echo "shim: bad password" >&2; exit 1; }
      echo "signed-with:$(head -n1 "$sec")" > "$out"; exit 0 ;;
  -V) [ -n "${MINISIGN_FAIL_VERIFY:-}" ] && { echo "shim: signature mismatch" >&2; exit 1; }
      tok=$(sed -n 's/^signed-with://p' "$out")
      grep -q -- "$tok" "$pub" && exit 0
      echo "shim: signature by $tok does not match $pub" >&2; exit 1 ;;
  *) exit 9 ;;
esac
"""

KEY_A = "keyA"
KEY_B = "keyB"


def pub_for(token: str) -> str:
    return f"untrusted comment: test key {token}\nRWQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n"


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

    def git(self, *args: str) -> None:
        subprocess.run(
            ["git", "-c", "user.name=t", "-c", "user.email=t@decdn.org", *args],
            cwd=self.repo,
            env=self.env(),
            check=True,
            capture_output=True,
        )

    def tag(self, name: str, fpr: str) -> None:
        """Commit the working tree, tag it, and make the tag reachable from a
        bare `origin`. The first call creates the origin."""
        self.git("add", "-A")
        self.git("commit", "-q", "--allow-empty", "-m", name)
        self.git("-c", f"user.signingkey={fpr}", "tag", "-s", name, "-m", name)
        origin = self.root / "origin.git"
        if origin.exists():
            self.git("push", "-q", "origin", "HEAD", name)
            return
        subprocess.run(
            ["git", "clone", "-q", "--bare", str(self.repo), str(origin)],
            env=self.env(),
            check=True,
            capture_output=True,
        )
        self.git("remote", "add", "origin", str(origin))

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
    (ring.root / "minisign.key").write_text(f"{KEY_A}\n")
    ring.repo.mkdir()
    (ring.repo / "crates" / "cli").mkdir(parents=True)
    (ring.repo / "crates" / "cli" / "minisign.pub").write_text(pub_for(KEY_A))
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
    verifies = release.calls(release.minisign_log, "-V")
    assert len(verifies) == 1, verifies
    # Verified against the public key read out of the tag's tree, not the
    # secret key's own public half.
    assert "-p " in verifies[0] and "/minisign.pub" in verifies[0], verifies
    assert "signed SHA256SUMS.minisig (crates/cli/minisign.pub)" in result.stdout
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
    assert "minisign said: shim: signature mismatch" in result.stderr, output(result)
    assert not release.calls(release.gh_log, "release upload")
    assert not release.calls(release.gh_log, "release edit")


def test_a_failed_minisign_stops_before_any_upload(release):
    fpr = release.gen_key("Maintainer <me@decdn.org>", publish=True)
    release.tag("v0.0.0", fpr)

    result = release.publish("v0.0.0", MINISIGN_FAIL_SIGN="1")

    assert result.returncode != 0
    assert "failed to minisign SHA256SUMS" in result.stderr, output(result)
    assert not release.calls(release.gh_log, "release upload")


def test_a_placeholder_key_in_the_tag_tree_is_refused(release):
    fpr = release.gen_key("Maintainer <me@decdn.org>", publish=True)
    (release.repo / "crates" / "cli" / "minisign.pub").write_text(
        "untrusted comment: PLACEHOLDER decdn self-update key; replace before a release\n"
        "RWQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n"
    )
    release.tag("v0.0.0", fpr)

    result = release.publish("v0.0.0")

    assert result.returncode != 0
    assert "placeholder" in result.stderr.lower(), output(result)
    assert not release.calls(release.minisign_log, "-S")
    assert not release.calls(release.gh_log, "release upload")


def test_the_keys_come_from_the_tag_tree_not_the_working_tree(release):
    fpr = release.gen_key("Maintainer <me@decdn.org>", publish=True)
    release.tag("v0.0.0", fpr)
    # A second key added after the tag is not one the released binary embeds.
    extra = release.repo / "crates" / "cli" / "minisign-next.pub"
    extra.write_text(pub_for(KEY_B))

    result = release.publish("v0.0.0")

    assert result.returncode == 0, output(result)
    verifies = release.calls(release.minisign_log, "-V")
    assert all("minisign-next.pub" not in line for line in verifies), verifies


def test_a_relative_minisign_key_path_is_resolved_before_signing(release):
    fpr = release.gen_key("Maintainer <me@decdn.org>", publish=True)
    release.tag("v0.0.0", fpr)
    (release.repo / "my.key").write_text(f"{KEY_A}\n")

    # Relative to the directory the script is run from; the script changes
    # directory before it signs.
    result = release.publish("v0.0.0", DECDN_MINISIGN_KEY="my.key")

    assert result.returncode == 0, output(result)
    signs = release.calls(release.minisign_log, "-S")
    assert len(signs) == 1 and f"-s {release.repo}/my.key" in signs[0], signs


def test_a_key_new_in_this_release_may_not_sign_it(release):
    fpr = release.gen_key("Maintainer <me@decdn.org>", publish=True)
    release.tag("v0.0.0", fpr)
    # The next release introduces key B. Installed binaries hold only key A,
    # so B cannot sign the release that introduces it.
    (release.repo / "crates" / "cli" / "minisign-b.pub").write_text(pub_for(KEY_B))
    release.tag("v0.0.1", fpr)
    key_b = release.root / "b.key"
    key_b.write_text(f"{KEY_B}\n")

    result = release.publish("v0.0.1", DECDN_MINISIGN_KEY=str(key_b))

    assert result.returncode != 0
    assert "v0.0.0" in result.stderr and "not trusted by" in result.stderr, output(result)
    assert not release.calls(release.gh_log, "release upload")

    # Key A, which every installed binary trusts, signs it.
    result = release.publish("v0.0.1")

    assert result.returncode == 0, output(result)
    assert "signed SHA256SUMS.minisig (crates/cli/minisign.pub)" in result.stdout


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
