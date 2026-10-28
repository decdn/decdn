"""Regression tests for the iroh-hold checker.

Cargo.lock is the only pin on the iroh family, and a drift off it shows up
only as an under-pay failure in `anvil e2e (journeys)`. A bug here degrades to
a quiet false pass: the check says the hold stands while client pulls die
mid-leg (#2585).
"""

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
MODULE_PATH = REPO_ROOT / ".github/scripts/check_iroh_hold.py"
WRAPPER = REPO_ROOT / ".github/scripts/check-iroh-hold.sh"

_spec = importlib.util.spec_from_file_location("check_iroh_hold", MODULE_PATH)
assert _spec and _spec.loader
cih = importlib.util.module_from_spec(_spec)
sys.modules["check_iroh_hold"] = cih
_spec.loader.exec_module(cih)

SOURCE = "registry+https://github.com/rust-lang/crates.io-index"


def package(name: str, version: str) -> str:
    return f'[[package]]\nname = "{name}"\nversion = "{version}"\nsource = "{SOURCE}"\n'


def lock(overrides: dict[str, str | None] | None = None, extra: str = "") -> str:
    """A Cargo.lock holding every HELD crate, with `overrides` applied.

    An override of `None` drops that crate from the lock.
    """
    versions: dict[str, str | None] = dict(cih.HELD)
    versions.update(overrides or {})
    body = "version = 4\n\n" + package("tokio", "1.54.0")
    for name, version in versions.items():
        if version is not None:
            body += "\n" + package(name, version)
    return body + extra


def test_held_lock_passes():
    assert cih.check_lock(lock()) == []


def test_held_table_names_the_seven_crates_at_one_version():
    assert set(cih.HELD) == {
        "iroh",
        "iroh-base",
        "iroh-dns",
        "iroh-relay",
        "noq",
        "noq-proto",
        "noq-udp",
    }
    # The family moves together; one version across the table.
    assert len(set(cih.HELD.values())) == 1


def test_each_drifted_crate_is_named_with_both_versions():
    errors = cih.check_lock(lock({"iroh": "1.4.1", "noq-proto": "1.4.0"}))
    assert len(errors) == 2, errors
    assert any("iroh:" in e and "1.4.1" in e and cih.HELD["iroh"] in e for e in errors), errors
    assert any("noq-proto:" in e and "1.4.0" in e for e in errors), errors


def test_a_patch_bump_is_drift_too():
    errors = cih.check_lock(lock({"noq-udp": "1.3.1"}))
    assert errors == [f"noq-udp: Cargo.lock resolves 1.3.1, held at {cih.HELD['noq-udp']}"]


def test_a_downgrade_is_drift_too():
    errors = cih.check_lock(lock({"iroh-dns": "1.2.0"}))
    assert len(errors) == 1 and "iroh-dns" in errors[0], errors


def test_a_second_version_beside_the_held_one_is_drift():
    """Two lock entries of one crate: the held one does not excuse the other."""
    errors = cih.check_lock(lock(extra="\n" + package("noq-proto", "1.4.0")))
    assert len(errors) == 1, errors
    assert "noq-proto" in errors[0] and "1.4.0" in errors[0]


def test_a_missing_held_crate_is_a_failure_not_a_pass():
    errors = cih.check_lock(lock({"iroh-relay": None}))
    assert len(errors) == 1, errors
    assert "iroh-relay" in errors[0] and "not in Cargo.lock" in errors[0]


def test_an_empty_lock_fails_on_every_crate():
    errors = cih.check_lock("version = 4\n")
    assert len(errors) == len(cih.HELD), errors


def test_similar_names_are_not_held_crates():
    """iroh-blobs, iroh-metrics and the like move freely."""
    extra = "\n" + package("iroh-blobs", "0.200.0") + "\n" + package("noq-extra", "9.0.0")
    assert cih.check_lock(lock(extra=extra)) == []


def test_invalid_toml_is_an_error():
    errors = cih.check_lock("[[package]\nname = ")
    assert len(errors) == 1 and "not valid TOML" in errors[0], errors


def test_missing_lock_file_is_an_error(tmp_path):
    assert cih.check(tmp_path) == ["Cargo.lock: not found"]


def test_the_real_repository_holds():
    assert cih.check(REPO_ROOT) == []


def test_wrapper_fails_and_explains_on_drift(tmp_path):
    """The shell entry point exits non-zero and says how to restore or lift."""
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    (tmp_path / "Cargo.lock").write_text(lock({"noq-proto": "1.4.0"}))
    result = subprocess.run(
        [str(WRAPPER)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        env={**os.environ, "GIT_DIR": str(tmp_path / ".git")},
    )
    assert result.returncode == 1, result
    assert "noq-proto" in result.stderr
    assert "#2585" in result.stderr
    assert "noq/pull/828" in result.stderr
    assert "HELD" in result.stderr


def test_wrapper_passes_on_the_held_lock(tmp_path):
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    (tmp_path / "Cargo.lock").write_text(lock())
    result = subprocess.run(
        [str(WRAPPER)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        env={**os.environ, "GIT_DIR": str(tmp_path / ".git")},
    )
    assert result.returncode == 0, result
    assert "iroh hold OK" in result.stdout
