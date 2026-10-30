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
# minimal draft: two archives, their SHA256SUMS, and the image digest and SBOM
# files the script insists on. $GH_BLANK_LINE appends a blank line to the
# SHA256SUMS. $GH_RERUN also stages what an earlier run left on the draft: a
# stale linux archive signature by key B, and a signature for an archive the
# draft no longer carries. `release upload` refuses a signature file that does
# not exist, as gh does, and logs each uploaded .minisig with its first line to
# $GH_UPLOAD_LOG.
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
    printf 'zip' > "$dir/decdn-$version-x86_64-pc-windows-msvc.zip"
    (cd "$dir" && sha256sum decdn-$version-x86_64-unknown-linux-gnu.tar.gz \
      decdn-$version-x86_64-pc-windows-msvc.zip > SHA256SUMS)
    [ -n "${GH_BLANK_LINE:-}" ] && echo >> "$dir/SHA256SUMS"
    if [ -n "${GH_RERUN:-}" ]; then
      echo "signed-with:keyB" > "$dir/decdn-$version-x86_64-unknown-linux-gnu.tar.gz.minisig"
      echo "signed-with:keyA" > "$dir/decdn-$version-aarch64-apple-darwin.tar.gz.minisig"
    fi
    zeros=$(printf '0%.0s' $(seq 64))
    echo "ghcr.io/decdn/decdn-node@sha256:$zeros" > "$dir/decdn-node-image-digest.txt"
    echo "ghcr.io/decdn/decdn@sha256:$zeros" > "$dir/decdn-image-digest.txt"
    echo "ghcr.io/decdn/sponsord@sha256:$zeros" > "$dir/sponsord-image-digest.txt"
    echo "ghcr.io/decdn/sponsord-onramp@sha256:$zeros" > "$dir/sponsord-onramp-image-digest.txt"
    for img in decdn-node decdn sponsord sponsord-onramp; do
      echo '{}' > "$dir/$img-$version-sbom.spdx.json"
    done ;;
  "release upload")
    for f in "$@"; do
      case "$f" in
        *.minisig|*.asc)
          [ -f "$f" ] || { echo "gh shim: no such file: $f" >&2; exit 8; }
          case "$f" in *.minisig) printf '%s %s\n' "$f" "$(head -n1 "$f")" >> "$GH_UPLOAD_LOG" ;; esac ;;
      esac
    done ;;
  "release edit") exit 0 ;;
  *) echo "gh shim: unexpected call: $*" >&2; exit 9 ;;
esac
"""

# Models key identity with a token: a secret key file holds one token line,
# a public key file, or a -P key string, mentions the token of the key it
# belongs to. `minisign -S` writes `signed-with:<token>` for each -m file into
# the file named by -x, else `<file>.minisig`, unless $MINISIGN_FAIL_SIGN is
# set; with $MINISIGN_LEGACY it writes `legacy-signed-with:<token>`, as a
# minisign whose default is the legacy format would. A second line carries the
# trusted comment real minisign writes by default, `file:<name>` among its
# tab-separated fields; $MINISIGN_TRUSTED_COMMENT is a printf format that
# replaces it, with %s the signed file's name. `minisign -V` succeeds
# iff the signature file exists and holds a token, -H is absent or the
# signature is not legacy, and the public key named by -p or -P mentions the
# token, unless $MINISIGN_FAIL_VERIFY is set; with -Q it then prints the
# trusted comment, as minisign does. Logs argv to $MINISIGN_LOG.
MINISIGN_SHIM = r"""#!/bin/sh
printf '%s\n' "$*" >> "$MINISIGN_LOG"
mode="$1"; shift
out=""; sec=""; pub=""; pubkey=""; files=""; prehashed=""; quiet=""
while [ $# -gt 0 ]; do
  case "$1" in
    -H) prehashed=1 ;;
    -Q) quiet=1 ;;
    -x) out="$2"; shift ;;
    -s) sec="$2"; shift ;;
    -p) pub="$2"; shift ;;
    -P) pubkey="$2"; shift ;;
    -m) shift
        while [ $# -gt 0 ] && [ "${1#-}" = "$1" ]; do files="$files $1"; shift; done
        continue ;;
  esac
  shift
done
case "$mode" in
  -S) [ -n "${MINISIGN_FAIL_SIGN:-}" ] && { echo "shim: bad password" >&2; exit 1; }
      prefix="signed-with"; [ -n "${MINISIGN_LEGACY:-}" ] && prefix="legacy-signed-with"
      for f in $files; do
        sig="${out:-$f.minisig}"
        echo "$prefix:$(head -n1 "$sec")" > "$sig"
        # shellcheck disable=SC2059  # the format is the test's input
        printf "trusted comment: ${MINISIGN_TRUSTED_COMMENT:-timestamp:0\tfile:%s\thashed}\n" "$f" >> "$sig"
      done; exit 0 ;;
  -V) [ -n "${MINISIGN_FAIL_VERIFY:-}" ] && { echo "shim: signature mismatch" >&2; exit 1; }
      set -- $files
      sig="${out:-$1.minisig}"
      [ -f "$sig" ] || { echo "shim: no signature file $sig" >&2; exit 1; }
      if [ -n "$prehashed" ] && grep -q '^legacy-signed-with:' "$sig"; then
        echo "Legacy (non-prehashed) signature found" >&2; exit 1
      fi
      tok=$(sed -n 's/^\(legacy-\)\{0,1\}signed-with://p' "$sig")
      [ -n "$tok" ] || { echo "shim: no token in $sig" >&2; exit 1; }
      ok=""
      if [ -n "$pubkey" ]; then
        case "$pubkey" in *"$tok"*) ok=1 ;; esac
      else
        grep -q -- "$tok" "$pub" && ok=1
      fi
      if [ -n "$ok" ]; then
        [ -n "$quiet" ] && sed -n 's/^trusted comment: //p' "$sig"
        exit 0
      fi
      echo "shim: signature by $tok does not match ${pub:-$pubkey}" >&2; exit 1 ;;
  *) exit 9 ;;
esac
"""

KEY_A = "keyA"
KEY_B = "keyB"


def pub_for(token: str) -> str:
    return f"untrusted comment: test key {token}\nRWQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n"


def binstall_manifest(name: str, token: str) -> str:
    """A crate manifest whose binstall signing key is the key with `token`."""
    return (
        f'[package]\nname = "{name}"\n\n'
        "[package.metadata.binstall.signing]\n"
        'algorithm = "minisign"\n'
        f'pubkey = "RWQ-{token}"\n'
        'file = "{ url }.minisig"\n'
    )


class Release(Keyring):
    """A keyring whose scratch repo has a signed tag and a bare origin."""

    @property
    def gh_log(self) -> Path:
        return self.root / "gh.log"

    @property
    def minisign_log(self) -> Path:
        return self.root / "minisign.log"

    @property
    def upload_log(self) -> Path:
        return self.root / "upload.log"

    def env(self) -> dict[str, str]:
        env = super().env()
        env.update(
            GH_LOG=str(self.gh_log),
            MINISIGN_LOG=str(self.minisign_log),
            GH_UPLOAD_LOG=str(self.upload_log),
        )
        return env

    def uploaded_signatures(self) -> dict[str, str]:
        """Each uploaded .minisig's name, mapped to its first line."""
        if not self.upload_log.exists():
            return {}
        return dict(line.split(" ", 1) for line in self.upload_log.read_text().splitlines())

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
    (ring.repo / "crates" / "cli" / "Cargo.toml").write_text(binstall_manifest("decdn-cli", KEY_A))
    (ring.repo / "crates" / "node").mkdir()
    (ring.repo / "crates" / "node" / "Cargo.toml").write_text(
        binstall_manifest("decdn-node", KEY_A)
    )
    # A crate with no binstall key, which the key lookup skips.
    (ring.repo / "crates" / "lib").mkdir()
    (ring.repo / "crates" / "lib" / "Cargo.toml").write_text('[package]\nname = "lib"\n')
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
    # One minisign run, so one password prompt, signs SHA256SUMS and every
    # archive.
    signs = release.calls(release.minisign_log, "-S")
    assert len(signs) == 1, signs
    assert signs[0].endswith(
        "-m SHA256SUMS decdn-0.0.0-x86_64-unknown-linux-gnu.tar.gz "
        "decdn-0.0.0-x86_64-pc-windows-msvc.zip"
    ), signs
    verifies = release.calls(release.minisign_log, "-V")
    assert len(verifies) == 3, verifies
    # Only the prehashed format, the one self-update and binstall accept.
    assert all(" -H " in f" {line} " for line in verifies), verifies
    # Verified against the public key read out of the tag's tree, not the
    # secret key's own public half.
    assert "-p " in verifies[0] and "/minisign.pub" in verifies[0], verifies
    # Each archive against the binstall key the crates name at the tag.
    for archive, line in zip(
        ("x86_64-unknown-linux-gnu.tar.gz", "x86_64-pc-windows-msvc.zip"), verifies[1:]
    ):
        assert f"-P RWQ-{KEY_A}" in line, verifies
        assert f"-m decdn-0.0.0-{archive} -x decdn-0.0.0-{archive}.minisig" in line, verifies
    assert "signed SHA256SUMS.minisig (crates/cli/minisign.pub)" in result.stdout
    assert "signed 2 archives for cargo binstall" in result.stdout
    uploads = release.calls(release.gh_log, "release upload")
    assert len(uploads) == 1 and "SHA256SUMS.minisig" in uploads[0], uploads
    assert "SHA256SUMS.asc" in uploads[0]
    assert "decdn-0.0.0-x86_64-unknown-linux-gnu.tar.gz.minisig" in uploads[0], uploads
    assert "decdn-0.0.0-x86_64-pc-windows-msvc.zip.minisig" in uploads[0], uploads
    # The uploaded archive signatures are the ones this run wrote.
    assert release.uploaded_signatures() == {
        "SHA256SUMS.minisig": f"signed-with:{KEY_A}",
        "decdn-0.0.0-x86_64-unknown-linux-gnu.tar.gz.minisig": f"signed-with:{KEY_A}",
        "decdn-0.0.0-x86_64-pc-windows-msvc.zip.minisig": f"signed-with:{KEY_A}",
    }
    assert "unrecognised signature" not in result.stderr
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


def test_archives_the_binstall_key_cannot_verify_are_refused(release):
    fpr = release.gen_key("Maintainer <me@decdn.org>", publish=True)
    # The crates name key B; key A signs, and passes the SHA256SUMS.minisig checks.
    for crate, name in (("cli", "decdn-cli"), ("node", "decdn-node")):
        (release.repo / "crates" / crate / "Cargo.toml").write_text(binstall_manifest(name, KEY_B))
    release.tag("v0.0.0", fpr)

    result = release.publish("v0.0.0")

    assert result.returncode != 0
    assert "does not verify under the binstall signing key" in result.stderr, output(result)
    assert "cargo binstall would refuse every archive" in result.stderr
    assert not release.calls(release.gh_log, "release upload")
    assert not release.calls(release.gh_log, "release edit")


def test_crates_naming_different_binstall_keys_are_refused_before_signing(release):
    fpr = release.gen_key("Maintainer <me@decdn.org>", publish=True)
    (release.repo / "crates" / "node" / "Cargo.toml").write_text(
        binstall_manifest("decdn-node", KEY_B)
    )
    release.tag("v0.0.0", fpr)

    result = release.publish("v0.0.0")

    assert result.returncode != 0
    assert "name 2 different binstall signing keys" in result.stderr, output(result)
    # Before either passphrase prompt: nothing is GPG-signed or minisigned.
    assert "signed SHA256SUMS" not in result.stdout
    assert not release.calls(release.minisign_log, "-S")
    assert not release.calls(release.gh_log, "release upload")


def test_no_binstall_key_at_the_tag_is_refused_before_signing(release):
    fpr = release.gen_key("Maintainer <me@decdn.org>", publish=True)
    for crate in ("cli", "node"):
        (release.repo / "crates" / crate / "Cargo.toml").write_text('[package]\nname = "x"\n')
    release.tag("v0.0.0", fpr)

    result = release.publish("v0.0.0")

    assert result.returncode != 0
    assert "names a [package.metadata.binstall.signing] pubkey" in result.stderr, output(result)
    assert "signed SHA256SUMS" not in result.stdout
    assert not release.calls(release.minisign_log, "-S")


def test_the_binstall_key_comes_from_the_tag_tree_not_the_working_tree(release):
    fpr = release.gen_key("Maintainer <me@decdn.org>", publish=True)
    release.tag("v0.0.0", fpr)
    # Edited after the tag: crates.io serves the tag's Cargo.toml, not this.
    (release.repo / "crates" / "cli" / "Cargo.toml").write_text(
        binstall_manifest("decdn-cli", KEY_B)
    )

    result = release.publish("v0.0.0")

    assert result.returncode == 0, output(result)
    verifies = release.calls(release.minisign_log, "-V")
    assert all(f"RWQ-{KEY_B}" not in line for line in verifies), verifies


def test_legacy_format_signatures_are_refused(release):
    # A minisign that signs in the legacy format by default: the signatures
    # verify without -H, but neither self-update nor binstall accepts them.
    fpr = release.gen_key("Maintainer <me@decdn.org>", publish=True)
    release.tag("v0.0.0", fpr)

    result = release.publish("v0.0.0", MINISIGN_LEGACY="1")

    assert result.returncode != 0
    assert "Legacy (non-prehashed) signature found" in result.stderr, output(result)
    assert not release.calls(release.gh_log, "release upload")


LINUX = "decdn-0.0.0-x86_64-unknown-linux-gnu.tar.gz"
WINDOWS = "decdn-0.0.0-x86_64-pc-windows-msvc.zip"


@pytest.mark.parametrize(
    "comment",
    [
        # No `file:` field at all, as a custom `-t` comment would write.
        "decdn release",
        # Another archive's name: the signature a rollback would reuse.
        f"timestamp:0\tfile:{WINDOWS}\thashed",
        # A name that only starts with the archive's.
        "timestamp:0\tfile:%s.bak\thashed",
        # A field that only ends with `file:<archive>`.
        "timestamp:0\told-file:%s\thashed",
    ],
)
def test_a_signature_that_does_not_name_its_archive_is_refused(release, comment):
    # binstall accepts a valid signature under any archive name, so the
    # trusted comment is all a person verifying by hand can check it against.
    fpr = release.gen_key("Maintainer <me@decdn.org>", publish=True)
    release.tag("v0.0.0", fpr)

    result = release.publish("v0.0.0", MINISIGN_TRUSTED_COMMENT=comment)

    assert result.returncode != 0
    assert f"{LINUX}.minisig has the trusted comment '" in result.stderr, output(result)
    assert f"which does not name {LINUX}." in result.stderr, output(result)
    assert not release.calls(release.gh_log, "release upload")


def test_a_file_field_alone_names_the_archive(release):
    # The field at both edges of the comment, with no tab on either side.
    fpr = release.gen_key("Maintainer <me@decdn.org>", publish=True)
    release.tag("v0.0.0", fpr)

    result = release.publish("v0.0.0", MINISIGN_TRUSTED_COMMENT="file:%s")

    assert result.returncode == 0, output(result)


def test_a_rerun_uploads_only_the_signatures_it_wrote(release):
    # The draft carries an earlier run's stale signature for the linux archive
    # and a signature for an archive it no longer carries.
    fpr = release.gen_key("Maintainer <me@decdn.org>", publish=True)
    release.tag("v0.0.0", fpr)

    result = release.publish("v0.0.0", GH_RERUN="1")

    assert result.returncode == 0, output(result)
    uploaded = release.uploaded_signatures()
    assert uploaded["decdn-0.0.0-x86_64-unknown-linux-gnu.tar.gz.minisig"] == (
        f"signed-with:{KEY_A}"
    ), uploaded
    assert "decdn-0.0.0-aarch64-apple-darwin.tar.gz.minisig" not in uploaded, uploaded
    assert (
        "unrecognised signature: decdn-0.0.0-aarch64-apple-darwin.tar.gz.minisig" in result.stderr
    ), output(result)
    assert "unrecognised signature: decdn-0.0.0-x86_64" not in result.stderr


def test_a_blank_line_in_sha256sums_names_no_archive(release):
    fpr = release.gen_key("Maintainer <me@decdn.org>", publish=True)
    release.tag("v0.0.0", fpr)

    result = release.publish("v0.0.0", GH_BLANK_LINE="1")

    assert result.returncode == 0, output(result)
    signs = release.calls(release.minisign_log, "-S")
    assert signs[0].endswith("decdn-0.0.0-x86_64-pc-windows-msvc.zip"), signs
