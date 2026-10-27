"""Regression tests for pnpm.sh, the pnpm the dashboard's tooling runs.

A maintainer's Node 24 may provide pnpm only through corepack. Without the
fallback, publish-crates.sh stops at its tool check on release day, and the
dashboard-lint hook fails every commit that touches the dashboard.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT = REPO_ROOT / ".github/scripts/pnpm.sh"
BASH = shutil.which("bash") or "/bin/bash"


def stub(bin_dir: Path, name: str) -> None:
    path = bin_dir / name
    path.write_text(f'#!/bin/sh\necho "{name} $*"\n')
    path.chmod(0o755)


def run(tmp_path: Path, *tools: str) -> subprocess.CompletedProcess[str]:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    for tool in tools:
        stub(bin_dir, tool)
    return subprocess.run(
        [BASH, str(SCRIPT), "-C", "crates/cli/dashboard", "build"],
        env={"PATH": str(bin_dir)},
        capture_output=True,
        text=True,
        check=False,
    )


def test_pnpm_on_path_runs_directly(tmp_path):
    r = run(tmp_path, "pnpm", "corepack")
    assert r.returncode == 0, r.stderr
    assert r.stdout.strip() == "pnpm -C crates/cli/dashboard build"


def test_corepack_stands_in_when_pnpm_is_missing(tmp_path):
    r = run(tmp_path, "corepack")
    assert r.returncode == 0, r.stderr
    assert r.stdout.strip() == "corepack pnpm -C crates/cli/dashboard build"


def test_neither_fails_and_says_what_to_install(tmp_path):
    r = run(tmp_path)
    assert r.returncode == 127
    assert "corepack enable" in r.stderr
