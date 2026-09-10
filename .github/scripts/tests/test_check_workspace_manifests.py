"""Regression tests for the workspace-manifest checker.

A member that restates `version = "0.1.0"` instead of inheriting it compiles
green and ships the wrong number; a member without `[lints] workspace = true`
compiles green under default lint levels. Neither has a warning, so the only
thing standing between the mistake and a tag is this check — and a bug here is
a quiet false pass.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
MODULE_PATH = REPO_ROOT / ".github/scripts/check_workspace_manifests.py"

_spec = importlib.util.spec_from_file_location("check_workspace_manifests", MODULE_PATH)
assert _spec and _spec.loader
cwm = importlib.util.module_from_spec(_spec)
sys.modules["check_workspace_manifests"] = cwm
_spec.loader.exec_module(cwm)


PUBLISHABLE = """\
[package]
name = "{name}"
version.workspace = true
edition.workspace = true
license.workspace = true
rust-version.workspace = true
description = "a crate"
repository.workspace = true
homepage.workspace = true
readme = "README.md"
keywords.workspace = true
categories.workspace = true

[dependencies]
{deps}
[dev-dependencies]
{dev_deps}
[lints]
workspace = true
"""

PRIVATE = """\
[package]
name = "{name}"
version.workspace = true
edition.workspace = true
license.workspace = true
rust-version.workspace = true
description = "test-only"
publish = false

[dependencies]
{deps}
[dev-dependencies]
{dev_deps}
[lints]
workspace = true
"""


def build_repo(tmp_path: Path, *, version: str = "0.0.0") -> Path:
    """A two-member workspace: `demo-core` (depended upon) and `demo-cli` (a sink)."""
    write_root(
        tmp_path,
        version=version,
        members=["crates/core", "crates/cli"],
        internal={"demo-core": ("crates/core", version)},
    )
    write_member(tmp_path, "crates/core", PUBLISHABLE, name="demo-core")
    write_member(
        tmp_path,
        "crates/cli",
        PUBLISHABLE,
        name="demo-cli",
        deps='demo-core = { workspace = true }\n',
    )
    return tmp_path


def write_root(
    tmp_path: Path,
    *,
    version: str,
    members: list[str],
    internal: dict[str, tuple[str, str]],
) -> None:
    members_toml = ", ".join(f'"{m}"' for m in members)
    internal_toml = "".join(
        f'{name} = {{ path = "{path}", version = "{v}" }}\n'
        for name, (path, v) in internal.items()
    )
    (tmp_path / "Cargo.toml").write_text(
        "[workspace]\n"
        f"members = [{members_toml}]\n\n"
        "[workspace.package]\n"
        f'version = "{version}"\n'
        'edition = "2024"\n'
        'license = "MIT"\n'
        'rust-version = "1.95.0"\n'
        'repository = "https://example.invalid"\n'
        'homepage = "https://example.invalid"\n'
        'keywords = ["x"]\n'
        'categories = ["x"]\n\n'
        "[workspace.dependencies]\n"
        f"{internal_toml}"
        'serde = "1"\n'
    )


def write_member(
    tmp_path: Path, rel: str, template: str, *, name: str, deps: str = "", dev_deps: str = ""
) -> Path:
    crate = tmp_path / rel
    crate.mkdir(parents=True, exist_ok=True)
    manifest = crate / "Cargo.toml"
    manifest.write_text(template.format(name=name, deps=deps, dev_deps=dev_deps))
    return manifest


def test_well_formed_workspace_passes(tmp_path):
    assert cwm.check(build_repo(tmp_path)) == []


def private_cli(repo: Path) -> Path:
    """Rewrite the sink as `publish = false`, the shape `crates/e2e` has."""
    return write_member(
        repo, "crates/cli", PRIVATE, name="demo-cli", deps='demo-core = { workspace = true }\n'
    )


@pytest.mark.parametrize("key", ["version", "edition", "license", "rust-version"])
@pytest.mark.parametrize("private", [False, True], ids=["publishable", "private"])
def test_member_restating_an_inherited_key_is_an_error(tmp_path, key, private):
    repo = build_repo(tmp_path)
    manifest = private_cli(repo) if private else repo / "crates/core/Cargo.toml"
    text = manifest.read_text().replace(f"\n{key}.workspace = true", f'\n{key} = "0.9.9"')
    manifest.write_text(text)
    errors = cwm.check(repo)
    assert len(errors) == 1, errors
    assert str(manifest.relative_to(repo)) in errors[0]
    assert key in errors[0]


@pytest.mark.parametrize(
    "lints",
    ["", "[lints.clippy]\nunwrap_used = \"deny\"\n", "[lints]\nworkspace = false\n"],
    ids=["absent", "own-table", "false"],
)
@pytest.mark.parametrize("private", [False, True], ids=["publishable", "private"])
def test_member_without_workspace_lints_is_an_error(tmp_path, lints, private):
    repo = build_repo(tmp_path)
    manifest = private_cli(repo) if private else repo / "crates/core/Cargo.toml"
    manifest.write_text(manifest.read_text().replace("[lints]\nworkspace = true\n", lints))
    errors = cwm.check(repo)
    assert len(errors) == 1, errors
    assert "[lints]" in errors[0]


@pytest.mark.parametrize("key", ["repository", "homepage", "keywords", "categories"])
def test_publishable_member_must_inherit_registry_metadata(tmp_path, key):
    repo = build_repo(tmp_path)
    manifest = repo / "crates/core/Cargo.toml"
    manifest.write_text(manifest.read_text().replace(f"{key}.workspace = true\n", ""))
    errors = cwm.check(repo)
    assert len(errors) == 1, errors
    assert key in errors[0]


@pytest.mark.parametrize("key", ["description", "readme"])
def test_publishable_member_must_carry_its_own_landing_page_fields(tmp_path, key):
    repo = build_repo(tmp_path)
    manifest = repo / "crates/core/Cargo.toml"
    line = {"description": 'description = "a crate"\n', "readme": 'readme = "README.md"\n'}[key]
    manifest.write_text(manifest.read_text().replace(line, ""))
    errors = cwm.check(repo)
    assert len(errors) == 1, errors
    assert key in errors[0]


def test_private_member_is_exempt_from_registry_metadata(tmp_path):
    repo = build_repo(tmp_path)
    write_member(repo, "crates/cli", PRIVATE, name="demo-cli", deps='demo-core = { workspace = true }\n')
    assert cwm.check(repo) == []


def test_internal_alias_version_must_match_the_workspace_version(tmp_path):
    repo = build_repo(tmp_path)
    write_root(
        repo,
        version="0.0.0",
        members=["crates/core", "crates/cli"],
        internal={"demo-core": ("crates/core", "0.1.1")},
    )
    errors = cwm.check(repo)
    assert len(errors) == 1, errors
    assert "demo-core" in errors[0]
    assert "0.1.1" in errors[0]
    assert "0.0.0" in errors[0]


def test_depended_upon_member_without_an_alias_is_an_error(tmp_path):
    repo = build_repo(tmp_path)
    write_root(repo, version="0.0.0", members=["crates/core", "crates/cli"], internal={})
    # The dependant must then name the path directly, or cargo would refuse to
    # parse the manifest at all — the guard sees the missing alias either way.
    write_member(
        repo,
        "crates/cli",
        PUBLISHABLE,
        name="demo-cli",
        deps='demo-core = { path = "../core" }\n',
    )
    errors = cwm.check(repo)
    assert any("demo-core" in e and "[workspace.dependencies]" in e for e in errors), errors


def test_self_dev_dependency_is_not_a_sibling(tmp_path):
    """`decdn-node` dev-depends on itself with `path = "."` to turn a feature on."""
    repo = build_repo(tmp_path)
    write_member(
        repo,
        "crates/core",
        PUBLISHABLE,
        name="demo-core",
        dev_deps='demo-core = { path = ".", features = ["test-support"] }\n',
    )
    assert cwm.check(repo) == []


def test_dev_dependency_counts_as_depended_upon(tmp_path):
    repo = build_repo(tmp_path)
    write_root(
        repo,
        version="0.0.0",
        members=["crates/core", "crates/cli"],
        internal={"demo-core": ("crates/core", "0.0.0"), "demo-cli": ("crates/cli", "0.0.0")},
    )
    write_member(
        repo,
        "crates/core",
        PUBLISHABLE,
        name="demo-core",
        dev_deps='demo-cli = { workspace = true }\n',
    )
    assert cwm.check(repo) == []


def test_sink_member_with_an_alias_is_an_error(tmp_path):
    """Nothing depends on the alias, so it is a version to forget on release."""
    repo = build_repo(tmp_path)
    write_root(
        repo,
        version="0.0.0",
        members=["crates/core", "crates/cli"],
        internal={"demo-core": ("crates/core", "0.0.0"), "demo-cli": ("crates/cli", "0.0.0")},
    )
    errors = cwm.check(repo)
    assert len(errors) == 1, errors
    assert "demo-cli" in errors[0]
    assert "nothing depends on" in errors[0]


def test_crate_directory_missing_from_members_is_an_error(tmp_path):
    repo = build_repo(tmp_path)
    write_member(repo, "crates/stray", PUBLISHABLE, name="demo-stray")
    errors = cwm.check(repo)
    assert len(errors) == 1, errors
    assert "crates/stray" in errors[0]
    assert "members" in errors[0]


def test_empty_workspace_is_a_failure_not_a_pass(tmp_path):
    write_root(tmp_path, version="0.0.0", members=[], internal={})
    errors = cwm.check(tmp_path)
    assert len(errors) == 1, errors
    assert "inspected nothing" in errors[0]


def test_the_real_repository_passes():
    assert cwm.check(REPO_ROOT) == []


def test_internal_dependency_must_go_through_the_alias(tmp_path):
    """A member naming a sibling by `path` + its own `version` bypasses the alias."""
    repo = build_repo(tmp_path)
    write_member(
        repo,
        "crates/cli",
        PUBLISHABLE,
        name="demo-cli",
        deps='demo-core = { path = "../core", version = "0.1.1" }\n',
    )
    errors = cwm.check(repo)
    assert len(errors) == 1, errors
    assert "crates/cli/Cargo.toml" in errors[0]
    assert "demo-core" in errors[0] and "workspace = true" in errors[0]


@pytest.mark.parametrize(
    "table",
    ["build-dependencies", "target.'cfg(unix)'.dependencies"],
    ids=["build", "target"],
)
def test_other_dependency_tables_count_as_depended_upon(tmp_path, table):
    repo = build_repo(tmp_path)
    write_root(repo, version="0.0.0", members=["crates/core", "crates/cli"], internal={})
    manifest = write_member(repo, "crates/cli", PUBLISHABLE, name="demo-cli")
    manifest.write_text(manifest.read_text() + f"\n[{table}]\ndemo-core = {{ workspace = true }}\n")
    errors = cwm.check(repo)
    assert len(errors) == 1, errors
    assert "demo-core" in errors[0] and "[workspace.dependencies]" in errors[0]


def test_alias_without_a_version_is_an_error(tmp_path):
    repo = build_repo(tmp_path)
    root = repo / "Cargo.toml"
    root.write_text(
        root.read_text().replace(
            'demo-core = { path = "crates/core", version = "0.0.0" }',
            'demo-core = { path = "crates/core" }',
        )
    )
    errors = cwm.check(repo)
    assert len(errors) == 1, errors
    assert "demo-core" in errors[0] and "version" in errors[0]


def test_alias_path_that_is_not_a_member_is_an_error(tmp_path):
    repo = build_repo(tmp_path)
    root = repo / "Cargo.toml"
    root.write_text(
        root.read_text().replace(
            'demo-core = { path = "crates/core", version = "0.0.0" }',
            'demo-core = { path = "crates/kore", version = "0.0.0" }',
        )
    )
    errors = cwm.check(repo)
    assert any("crates/kore" in e and "not a workspace member" in e for e in errors), errors


def test_missing_workspace_version_is_named(tmp_path):
    repo = build_repo(tmp_path)
    root = repo / "Cargo.toml"
    root.write_text(root.read_text().replace('version = "0.0.0"\nedition', "edition"))
    errors = cwm.check(repo)
    assert any("[workspace.package] version" in e and "missing" in e for e in errors), errors


def test_member_without_a_manifest_is_an_error(tmp_path):
    repo = build_repo(tmp_path)
    write_root(
        repo,
        version="0.0.0",
        members=["crates/core", "crates/cli", "crates/ghost"],
        internal={"demo-core": ("crates/core", "0.0.0")},
    )
    errors = cwm.check(repo)
    assert len(errors) == 1, errors
    assert "crates/ghost" in errors[0] and "no Cargo.toml" in errors[0]
