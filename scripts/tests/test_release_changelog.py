"""Tests for release-changelog.sh, the cargo-release pre-release-hook.

cargo-release runs the hook once per package, with the package directory as
cwd, and also during a preview with DRY_RUN=true. These tests run the real
script with a controlled PATH: a preview must write nothing and need no
git-cliff at all, and a real run must hand git-cliff absolute paths under
WORKSPACE_ROOT so the twelfth run writes the same file as the first.
"""

from __future__ import annotations

import os
import shutil
import stat
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts/release-changelog.sh"
# Resolved before the tests narrow PATH.
BASH = shutil.which("bash") or "/bin/bash"


def run(tmp_path: Path, path_dir: Path, **env: str) -> subprocess.CompletedProcess[str]:
    cwd = tmp_path / "crates" / "some-crate"
    cwd.mkdir(parents=True, exist_ok=True)
    return subprocess.run(
        [BASH, str(SCRIPT)],
        cwd=cwd,
        env={"HOME": str(tmp_path), "PATH": str(path_dir), **env},
        capture_output=True,
        text=True,
        check=False,
    )


def stub_git_cliff(bin_dir: Path, log: Path) -> None:
    """A `git-cliff` on PATH that records its arguments, one per line, and
    writes a changelog that ends in the trailing blank line the real one
    leaves after the oldest section."""
    bin_dir.mkdir()
    stub = bin_dir / "git-cliff"
    stub.write_text(
        f'#!{BASH}\nprintf "%s\\n" "$@" > "{log}"\n'
        'printf "# Changelog\\n\\n## [1.2.3]\\n\\n- a line\\n\\n" > "${@: -1}"\n'
    )
    stub.chmod(stub.stat().st_mode | stat.S_IXUSR)


def test_dry_run_writes_nothing_and_needs_no_git_cliff(tmp_path):
    r = run(
        tmp_path,
        tmp_path / "no-tools",
        WORKSPACE_ROOT=str(tmp_path),
        NEW_VERSION="1.2.3",
        DRY_RUN="true",
    )
    assert r.returncode == 0, r.stderr
    assert r.stderr == ""
    assert not (tmp_path / "CHANGELOG.md").exists()


def test_missing_cargo_release_env_fails(tmp_path):
    for env in ({"NEW_VERSION": "1.2.3"}, {"WORKSPACE_ROOT": str(tmp_path)}):
        r = run(tmp_path, tmp_path / "no-tools", DRY_RUN="false", **env)
        assert r.returncode != 0
        assert "set by cargo-release" in r.stderr


def test_missing_git_cliff_fails_the_release(tmp_path):
    r = run(
        tmp_path,
        tmp_path / "no-tools",
        WORKSPACE_ROOT=str(tmp_path),
        NEW_VERSION="1.2.3",
        DRY_RUN="false",
    )
    assert r.returncode == 1
    assert "git-cliff not found on PATH" in r.stderr


def test_real_run_hands_git_cliff_absolute_workspace_paths(tmp_path):
    log = tmp_path / "args.log"
    stub_git_cliff(tmp_path / "bin", log)
    r = run(
        tmp_path,
        tmp_path / "bin",
        WORKSPACE_ROOT=str(tmp_path),
        NEW_VERSION="1.2.3",
        DRY_RUN="false",
    )
    assert r.returncode == 0, r.stderr
    args = log.read_text().splitlines()
    assert args == [
        "--repository",
        str(tmp_path),
        "--config",
        str(tmp_path / "cliff.toml"),
        "--tag",
        "v1.2.3",
        "--output",
        str(tmp_path / "CHANGELOG.md"),
    ]
    # Nothing is written relative to the package cwd.
    assert not (tmp_path / "crates" / "some-crate" / "CHANGELOG.md").exists()
    # The file ends in exactly one newline (pre-commit end-of-file-fixer).
    assert (tmp_path / "CHANGELOG.md").read_text() == "# Changelog\n\n## [1.2.3]\n\n- a line\n"
