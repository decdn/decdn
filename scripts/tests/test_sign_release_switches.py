"""Regression tests for sign-release.sh's switches.

A typo in a switch must stop the script before anything is signed. The devops
bump switch matters only after the release is published, when the script no
longer fails, so a typo there has to be caught at the start. These tests run
the real script with a PATH that holds no tool at all: a value the script
accepts gets as far as the first tool check, and one it refuses stops before
it.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts/sign-release.sh"
SWITCHES = ["DECDN_SKIP_IMAGE_TAGS", "DECDN_SKIP_DOCKERHUB", "DECDN_SKIP_DEVOPS_BUMP"]
FIRST_TOOL_CHECK = "gh not found on PATH"
# Resolved before the tests empty PATH.
BASH = shutil.which("bash") or "/bin/bash"


def run(tmp_path: Path, **switches: str) -> subprocess.CompletedProcess[str]:
    env = {"HOME": str(tmp_path), "PATH": str(tmp_path / "no-tools"), **switches}
    return subprocess.run(
        [BASH, str(SCRIPT), "v0.0.0"],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


@pytest.mark.parametrize("switch", SWITCHES)
@pytest.mark.parametrize("value", ["maybe", "2", "on"])
def test_a_typo_stops_before_any_tool_runs(tmp_path, switch, value):
    r = run(tmp_path, **{switch: value})
    assert r.returncode == 1
    assert f"{switch} must be 1/true/yes or 0/false/no, got '{value}'" in r.stderr
    assert FIRST_TOOL_CHECK not in r.stderr


@pytest.mark.parametrize("switch", SWITCHES)
@pytest.mark.parametrize("value", ["", "0", "no", "1", "TRUE", "yes"])
def test_accepted_values_reach_the_tool_checks(tmp_path, switch, value):
    r = run(tmp_path, **{switch: value})
    assert r.returncode == 1
    assert FIRST_TOOL_CHECK in r.stderr, r.stderr
