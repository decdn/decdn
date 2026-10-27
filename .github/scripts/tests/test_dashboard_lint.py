"""Regression tests for dashboard-lint.sh, the dashboard-lint pre-commit hook.

`pre-commit run --all-files` runs every hook whatever its `files:`, so a
contributor who never installed the dashboard's dependencies must not fail on
it; CI's `dashboard` job lints the app on every change to it.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT = REPO_ROOT / ".github/scripts/dashboard-lint.sh"
BASH = shutil.which("bash") or "/bin/bash"


def run(tmp_path: Path, installed: bool) -> subprocess.CompletedProcess[str]:
    dashboard = tmp_path / "crates/cli/dashboard"
    dashboard.mkdir(parents=True)
    if installed:
        (dashboard / "node_modules").mkdir()
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    pnpm = bin_dir / "pnpm"
    pnpm.write_text('#!/bin/sh\necho "pnpm $*"\n')
    pnpm.chmod(0o755)
    for tool in ("dirname", "bash"):
        (bin_dir / tool).symlink_to(shutil.which(tool) or f"/usr/bin/{tool}")
    return subprocess.run(
        [BASH, str(SCRIPT)],
        cwd=tmp_path,
        env={"PATH": str(bin_dir)},
        capture_output=True,
        text=True,
        check=False,
    )


def test_without_dependencies_it_skips_and_says_so(tmp_path):
    r = run(tmp_path, installed=False)
    assert r.returncode == 0, r.stderr
    assert "skipped" in r.stderr
    assert "install --frozen-lockfile" in r.stderr
    assert r.stdout == ""


def test_with_dependencies_it_lints(tmp_path):
    r = run(tmp_path, installed=True)
    assert r.returncode == 0, r.stderr
    assert r.stdout.strip() == "pnpm -C crates/cli/dashboard lint"
