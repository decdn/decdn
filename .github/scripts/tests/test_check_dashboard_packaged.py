"""Regression tests for check-dashboard-packaged.sh.

Every packaging step after the dashboard build passes `--allow-dirty`, since
cargo counts the gitignored build as uncommitted. This script is what makes
that safe: the checkout is otherwise clean, and decdn-cli's .crate carries the
build and no other file git does not track. A miss here ships a stray file to
crates.io, where a version can never be replaced.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT = REPO_ROOT / ".github/scripts/check-dashboard-packaged.sh"
BASH = shutil.which("bash") or "/bin/bash"

DIST = ["dashboard/dist/index.html", "dashboard/dist/app.js", "dashboard/dist/app.css"]
TRACKED = ["Cargo.toml", "README.md", "build.rs", "src/main.rs"]
GENERATED = [".cargo_vcs_info.json", "Cargo.lock", "Cargo.toml.orig"]


def run(
    tmp_path: Path,
    *,
    listed: list[str],
    status: str = "",
    status_fails: bool = False,
) -> subprocess.CompletedProcess[str]:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (tmp_path / "listed").write_text("".join(f"{f}\n" for f in listed))
    (tmp_path / "tracked").write_text("".join(f"crates/cli/{f}\n" for f in TRACKED))
    (tmp_path / "status").write_text(status)
    git = (
        "#!/bin/sh\n"
        'case "$1" in\n'
        f'  status) {"exit 128" if status_fails else f"cat {tmp_path}/status"} ;;\n'
        f"  ls-files) cat {tmp_path}/tracked ;;\n"
        "  *) exit 2 ;;\n"
        "esac\n"
    )
    cargo = f"#!/bin/sh\ncat {tmp_path}/listed\n"
    for name, body in (("git", git), ("cargo", cargo)):
        path = bin_dir / name
        path.write_text(body)
        path.chmod(0o755)
    for tool in ("sort", "comm", "grep", "sed", "cat", "printf"):
        found = shutil.which(tool)
        if found:
            (bin_dir / tool).symlink_to(found)
    return subprocess.run(
        [BASH, str(SCRIPT)],
        env={"PATH": str(bin_dir)},
        capture_output=True,
        text=True,
        check=False,
    )


def test_a_clean_checkout_with_the_build_passes(tmp_path):
    r = run(tmp_path, listed=TRACKED + DIST + GENERATED)
    assert r.returncode == 0, r.stdout + r.stderr


def test_a_missing_build_file_fails(tmp_path):
    r = run(tmp_path, listed=TRACKED + DIST[:2] + GENERATED)
    assert r.returncode == 1
    assert "dashboard/dist/app.css" in r.stdout + r.stderr


def test_a_change_git_sees_fails(tmp_path):
    r = run(tmp_path, listed=TRACKED + DIST + GENERATED, status=" M crates/cli/src/main.rs\n")
    assert r.returncode == 1
    assert "src/main.rs" in r.stdout + r.stderr


def test_a_failing_git_status_fails_instead_of_passing(tmp_path):
    r = run(tmp_path, listed=TRACKED + DIST + GENERATED, status_fails=True)
    assert r.returncode != 0


def test_an_untracked_file_the_crate_would_carry_fails(tmp_path):
    stray = "src/.main.rs.swp"
    r = run(tmp_path, listed=TRACKED + DIST + GENERATED + [stray])
    assert r.returncode == 1
    assert stray in r.stdout + r.stderr
