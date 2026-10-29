"""Regression tests for publish-crates.sh's release-asset checks.

A crates.io version can never be replaced, so the script refuses to publish
until the GitHub Release carries every signature a client checks, and until
the binstall metadata at the tag passes check_binstall_metadata.py. These run
the real script against a scratch repo with a signed tag (the `release`
fixture) and shims for `gh`, `cargo`, `curl` and `node`. `cargo` logs and
fails, so a run that clears every check stops at its first `cargo` call.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

from test_sign_release_publish import Release, release  # noqa: F401  (fixture)

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts/publish-crates.sh"
GUARD = ".github/scripts/check_binstall_metadata.py"

pytestmark = pytest.mark.skipif(
    shutil.which("gpg") is None or shutil.which("gpgconf") is None,
    reason="gpg/gpgconf is not installed",
)

TAR = "decdn-0.0.0-x86_64-unknown-linux-gnu.tar.gz"
ZIP = "decdn-0.0.0-x86_64-pc-windows-msvc.zip"
SIGNED = ["SHA256SUMS", "SHA256SUMS.asc", "SHA256SUMS.minisig", TAR, f"{TAR}.minisig", ZIP]

# The release is published. `--json assets` lists $GH_ASSETS (one name per
# line); `release download --pattern SHA256SUMS` writes a manifest naming the
# two archives.
GH_SHIM = r"""#!/bin/sh
printf '%s\n' "$*" >> "$GH_LOG"
case "$1 $2" in
  "auth status") exit 0 ;;
  "api repos"*) git rev-parse "refs/tags/${2##*/}" ;;
  "release view")
    case "$*" in
      *isDraft*) echo false ;;
      *assets*) printf '%s\n' "$GH_ASSETS" ;;
    esac ;;
  "release download")
    dir=""
    while [ $# -gt 0 ]; do [ "$1" = "--dir" ] && dir="$2"; shift; done
    printf '%064d  %s\n%064d  %s\n' 0 decdn-0.0.0-x86_64-unknown-linux-gnu.tar.gz \
      0 decdn-0.0.0-x86_64-pc-windows-msvc.zip > "$dir/SHA256SUMS" ;;
  *) echo "gh shim: unexpected call: $*" >&2; exit 9 ;;
esac
"""

CARGO_SHIM = r"""#!/bin/sh
printf '%s\n' "$*" >> "$CARGO_LOG"
exit 9
"""


def publish(release: Release, assets: list[str]) -> subprocess.CompletedProcess[str]:  # noqa: F811
    for name, body in (
        ("gh", GH_SHIM),
        ("cargo", CARGO_SHIM),
        ("curl", "#!/bin/sh\n"),
        ("node", "#!/bin/sh\n"),
    ):
        shim = release.bin / name
        shim.write_text(body)
        shim.chmod(0o755)
    keys = release.gpg("--armor", "--export", *release.published)
    (release.repo / "KEYS").write_text(keys)
    env = {
        **release.env(),
        "GH_ASSETS": "\n".join(assets),
        "CARGO_LOG": str(release.root / "cargo.log"),
        "CARGO_REGISTRY_TOKEN": "test",
    }
    return subprocess.run(
        ["bash", str(SCRIPT), "v0.0.0"],
        cwd=release.repo,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def tag_with_guard(release: Release) -> None:  # noqa: F811
    """Tag a tree that carries the real guard, and nothing it checks."""
    fpr = release.gen_key("Maintainer <me@decdn.org>", publish=True)
    (release.repo / GUARD).parent.mkdir(parents=True)
    shutil.copy(REPO_ROOT / GUARD, release.repo / GUARD)
    release.tag("v0.0.0", fpr)


def output(result: subprocess.CompletedProcess[str]) -> str:
    return result.stdout + result.stderr


def cargo_calls(release: Release) -> list[str]:  # noqa: F811
    log = release.root / "cargo.log"
    return log.read_text().splitlines() if log.exists() else []


def test_an_archive_without_its_minisig_is_refused(release):  # noqa: F811
    tag_with_guard(release)

    result = publish(release, SIGNED)

    assert result.returncode != 0
    assert f"carries no .minisig for: {ZIP}" in result.stderr, output(result)
    assert TAR not in result.stderr.split("carries no .minisig for:", 1)[1]
    assert not cargo_calls(release)


def test_an_archive_the_manifest_names_but_the_release_lacks_is_refused(release):  # noqa: F811
    tag_with_guard(release)

    result = publish(release, [a for a in SIGNED if a != TAR] + [f"{ZIP}.minisig"])

    assert result.returncode != 0
    assert f"does not carry {TAR}, which SHA256SUMS names" in result.stderr, output(result)
    assert not cargo_calls(release)


def test_a_fully_signed_release_reaches_the_guard_at_the_tag(release):  # noqa: F811
    # Every signature is attached, so the asset checks pass. The guard then runs
    # in the worktree at the tag, finds no release.yml there and stops the run
    # before any `cargo` call.
    tag_with_guard(release)

    result = publish(release, [*SIGNED, f"{ZIP}.minisig"])

    assert result.returncode != 0
    assert "carries no .minisig" not in result.stderr
    assert ".github/workflows/release.yml: not found" in result.stderr, output(result)
    assert f"fails {GUARD}" in result.stderr
    assert not cargo_calls(release)
