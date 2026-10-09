"""Regression tests for the publish gate's dependency list.

A dependency this misses is a publish that crates.io rejects partway through
the workspace, after the crates before it are already uploaded for good.
"""

from __future__ import annotations

import importlib.util
import sys
import tomllib
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
MODULE_PATH = REPO_ROOT / ".github/scripts/registry_deps.py"

_spec = importlib.util.spec_from_file_location("registry_deps", MODULE_PATH)
assert _spec and _spec.loader
rd = importlib.util.module_from_spec(_spec)
sys.modules["registry_deps"] = rd
_spec.loader.exec_module(rd)

ROOT = tomllib.loads(
    """
[workspace.dependencies]
lib = { path = "crates/lib", version = "0.3.0" }
decdn-client = { git = "https://github.com/decdn/decdn", branch = "main", version = "0.1.0" }
decdn-e2e = { git = "https://github.com/decdn/decdn", branch = "main" }
serde = "1"
"""
)
SIBLINGS = {"lib", "app"}


def deps(crate_toml: str):
    return rd.registry_deps(ROOT, tomllib.loads(crate_toml), SIBLINGS)


def test_aliases_resolve_to_their_versions_and_origin():
    got, errors = deps(
        """
[dependencies]
lib.workspace = true
decdn-client = { workspace = true, features = ["x"] }
serde.workspace = true
tokio = "1"
"""
    )
    assert errors == []
    assert got == [("decdn-client", "0.1.0", "decdn"), ("lib", "0.3.0", "sibling")]


def test_versioned_dev_dependency_counts():
    """It stays in the uploaded manifest, so crates.io must have it."""
    got, _ = deps("[dev-dependencies]\nlib.workspace = true\n")
    assert got == [("lib", "0.3.0", "sibling")]


def test_unversioned_dev_dependency_is_skipped():
    """Path or git: either way it is stripped from the uploaded manifest."""
    for dev in ['decdn-e2e.workspace = true', 'me = { path = ".", features = ["x"] }']:
        got, errors = deps(f"[dev-dependencies]\n{dev}\n")
        assert (got, errors) == ([], []), dev


def test_unversioned_git_normal_dependency_is_an_error():
    got, errors = deps('[dependencies]\ndecdn-e2e.workspace = true\n')
    assert got == []
    assert len(errors) == 1 and "git dependency" in errors[0]


def test_path_only_normal_dependency_is_an_error():
    got, errors = deps('[dependencies]\nlocal = { path = "../local" }\n')
    assert got == []
    assert len(errors) == 1 and "local" in errors[0]


def test_build_and_target_tables_count():
    got, _ = deps(
        "[build-dependencies]\nlib.workspace = true\n"
        "[target.'cfg(unix)'.dependencies]\ndecdn-client.workspace = true\n"
    )
    assert got == [("decdn-client", "0.1.0", "decdn"), ("lib", "0.3.0", "sibling")]


def test_the_real_workspace():
    root = tomllib.loads((REPO_ROOT / "Cargo.toml").read_text())
    members = {}
    for rel in root["workspace"]["members"]:
        m = tomllib.loads((REPO_ROOT / rel / "Cargo.toml").read_text())
        members[m["package"]["name"]] = m
    publishable = {n: m for n, m in members.items() if m["package"].get("publish") is not False}
    needs = {}
    for name, manifest in publishable.items():
        got, errors = rd.registry_deps(root, manifest, set(members))
        assert errors == [], name
        needs[name] = {(n, o) for n, _, o in got}
    # The crates that link decdn need its release on crates.io first.
    for name in ("sponsord-core", "sponsord", "decdn-sponsored"):
        assert any(o == "decdn" for _, o in needs[name]), name
    # The wire types and the onramp stay off decdn entirely.
    for name in ("sponsord-api", "sponsord-onramp"):
        assert not any(o == "decdn" for _, o in needs[name]), name
    siblings = lambda name: {n for n, o in needs[name] if o == "sibling"}  # noqa: E731
    assert siblings("sponsord") == {"sponsord-api", "sponsord-core"}
    assert siblings("sponsord-onramp") == {"sponsord-api"}


def test_workspace_lists_each_decdn_dep_once_and_no_siblings():
    members = {
        "lib": tomllib.loads('[package]\nname = "lib"\n[dependencies]\ndecdn-client.workspace = true\n'),
        "app": tomllib.loads(
            '[package]\nname = "app"\n[dependencies]\nlib.workspace = true\n'
            "decdn-client.workspace = true\n"
        ),
        # Never published, so its dependencies are not crates.io's business.
        "tests": tomllib.loads(
            '[package]\nname = "tests"\npublish = false\n'
            "[dependencies]\ndecdn-e2e.workspace = true\n"
        ),
    }
    got, errors = rd.workspace_decdn_deps(ROOT, members)
    assert (got, errors) == ([("decdn-client", "0.1.0")], [])


def test_workspace_errors_name_the_member():
    members = {"app": tomllib.loads('[package]\nname = "app"\n[dependencies]\ndecdn-e2e.workspace = true\n')}
    got, errors = rd.workspace_decdn_deps(ROOT, members)
    assert got == []
    assert len(errors) == 1 and errors[0].startswith("app: ")


def test_cli_reads_the_tagged_tree():
    import subprocess

    out = subprocess.run(
        [sys.executable, str(MODULE_PATH), "HEAD"],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        check=True,
    ).stdout.split("\n")
    lines = [line for line in out if line]
    assert lines, "the workspace links decdn, so HEAD lists its crates"
    assert all(line.startswith("decdn-") and len(line.split()) == 2 for line in lines), lines


def test_conflicting_decdn_versions_are_both_listed():
    """publish-crates.sh refuses a mismatch only if it sees both versions."""
    root = tomllib.loads(
        "[workspace.dependencies]\n"
        'decdn-client = { git = "https://github.com/decdn/decdn", version = "0.1.0" }\n'
    )
    members = {
        "a": tomllib.loads('[package]\nname = "a"\n[dependencies]\ndecdn-client.workspace = true\n'),
        "b": tomllib.loads(
            '[package]\nname = "b"\n[dependencies]\n'
            'decdn-client = { git = "https://github.com/decdn/decdn", version = "0.2.0" }\n'
        ),
    }
    got, errors = rd.workspace_decdn_deps(root, members)
    assert errors == []
    assert got == [("decdn-client", "0.1.0"), ("decdn-client", "0.2.0")]


def test_the_real_workspace_needs_one_decdn_version():
    """What publish-crates.sh requires of the tagged tree, checked at every PR."""
    root = tomllib.loads((REPO_ROOT / "Cargo.toml").read_text())
    members = {}
    for rel in root["workspace"]["members"]:
        m = tomllib.loads((REPO_ROOT / rel / "Cargo.toml").read_text())
        members[m["package"]["name"]] = m
    got, errors = rd.workspace_decdn_deps(root, members)
    assert errors == []
    assert {n for n, _ in got} == {"decdn-client", "decdn-common", "decdn-incentive"}
    assert len({v for _, v in got}) == 1, got


def test_cli_usage_error():
    import subprocess

    run = subprocess.run([sys.executable, str(MODULE_PATH)], capture_output=True, text=True)
    assert run.returncode == 2 and "usage" in run.stderr
