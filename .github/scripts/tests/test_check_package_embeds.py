"""Regression tests for the crate-embed checker.

The masking pass and the `#[cfg(test)]` region tracking are the most intricate
logic in the release tooling, and a bug in either degrades to a *quiet false
pass* — the check reports success while the very mistake it exists to catch
sails through to release day. These tests pin the classification behaviour.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
MODULE_PATH = REPO_ROOT / ".github/scripts/check_package_embeds.py"

_spec = importlib.util.spec_from_file_location("check_package_embeds", MODULE_PATH)
assert _spec and _spec.loader
cpe = importlib.util.module_from_spec(_spec)
sys.modules["check_package_embeds"] = cpe
_spec.loader.exec_module(cpe)


# --- helpers ---------------------------------------------------------------


def build_crate(tmp_path: Path, source: str, *, outside: bool = True) -> tuple[Path, Path, set[Path]]:
    """A crate at <tmp>/crates/demo with `source` as src/lib.rs.

    Also drops a fixture at the workspace root (outside the crate) and one
    inside it, so a test can embed either. Everything is reported as tracked.
    """
    crate_root = tmp_path / "crates" / "demo"
    (crate_root / "src").mkdir(parents=True)
    src = crate_root / "src" / "lib.rs"
    src.write_text(source)
    (tmp_path / "OUTSIDE.md").write_text("x")
    (crate_root / "INSIDE.md").write_text("x")
    tracked = {
        src.resolve(),
        (tmp_path / "OUTSIDE.md").resolve(),
        (crate_root / "INSIDE.md").resolve(),
    }
    if not outside:
        tracked.discard((tmp_path / "OUTSIDE.md").resolve())
    return crate_root, src, tracked


def scan(tmp_path: Path, source: str) -> tuple[list[str], list[tuple[str, str]]]:
    crate_root, src, tracked = build_crate(tmp_path, source)
    return cpe.scan_source(src, crate_root, tmp_path, tracked)


# --- embedding forms -------------------------------------------------------


@pytest.mark.parametrize(
    "call",
    [
        'include_str!("../../../OUTSIDE.md")',
        'include_str!["../../../OUTSIDE.md"]',
        'include_str!{"../../../OUTSIDE.md"}',
        'include_bytes!("../../../OUTSIDE.md")',
        'include!("../../../OUTSIDE.md")',
        'include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../../OUTSIDE.md"))',
    ],
    ids=["paren", "bracket", "brace", "bytes", "plain-include", "manifest-dir"],
)
def test_every_embedding_form_outside_the_crate_is_an_error(tmp_path, call):
    errors, warnings = scan(tmp_path, f"const X: &str = {call};\n")
    assert len(errors) == 1, errors
    assert "OUTSIDE.md" in errors[0]
    assert warnings == []


def test_path_attribute_escaping_the_crate_is_an_error(tmp_path):
    errors, _ = scan(tmp_path, '#[path = "../../../OUTSIDE.md"]\nmod x;\n')
    assert len(errors) == 1, errors


def test_embed_inside_the_crate_is_accepted(tmp_path):
    errors, warnings = scan(tmp_path, 'const X: &str = include_str!("../INSIDE.md");\n')
    assert errors == []
    assert warnings == []


def test_embed_inside_the_crate_but_untracked_is_an_error(tmp_path):
    """cargo package ships tracked files, so a gitignored target is still absent."""
    crate_root, src, tracked = build_crate(tmp_path, 'const X: &str = include_str!("../INSIDE.md");\n')
    tracked.discard((crate_root / "INSIDE.md").resolve())
    errors, _ = cpe.scan_source(src, crate_root, tmp_path, tracked)
    assert len(errors) == 1
    assert "not tracked by git" in errors[0]


def test_unresolvable_argument_is_an_error_not_a_pass(tmp_path):
    errors, _ = scan(tmp_path, "const X: &str = include_str!(some_macro!());\n")
    assert len(errors) == 1
    assert "cannot statically resolve" in errors[0]


# --- cfg(test) classification ----------------------------------------------


def test_cfg_test_module_downgrades_to_a_warning(tmp_path):
    errors, warnings = scan(
        tmp_path,
        "#[cfg(test)]\nmod tests {\n"
        '    const X: &str = include_str!("../../../OUTSIDE.md");\n}\n',
    )
    assert errors == []
    assert warnings == [("crates/demo/src/lib.rs", "OUTSIDE.md")]


def test_production_code_after_a_cfg_test_module_is_still_an_error(tmp_path):
    """The region must end at the module's closing brace, not run to EOF."""
    errors, warnings = scan(
        tmp_path,
        "#[cfg(test)]\nmod tests {\n    fn helper() {}\n}\n"
        'const X: &str = include_str!("../../../OUTSIDE.md");\n',
    )
    assert len(errors) == 1, (errors, warnings)
    assert warnings == []


def test_out_of_line_cfg_test_module_does_not_open_a_region(tmp_path):
    """`#[cfg(test)] mod tests;` has no body — the next `{` belongs to something else."""
    errors, warnings = scan(
        tmp_path,
        "#[cfg(test)]\nmod tests;\n"
        'const X: &str = include_str!("../../../OUTSIDE.md");\n',
    )
    assert len(errors) == 1, (errors, warnings)


def test_cfg_test_with_extra_predicates_is_not_treated_as_test_only(tmp_path):
    """`cfg(any(test, feature = "x"))` is reachable from a published feature."""
    errors, _ = scan(
        tmp_path,
        '#[cfg(any(test, feature = "test-util"))]\nmod tests {\n'
        '    const X: &str = include_str!("../../../OUTSIDE.md");\n}\n',
    )
    assert len(errors) == 1


# --- masking ---------------------------------------------------------------


@pytest.mark.parametrize(
    "source",
    [
        '// const X: &str = include_str!("../../../OUTSIDE.md");\n',
        '/* include_str!("../../../OUTSIDE.md") */\n',
        '/* /* nested */ include_str!("../../../OUTSIDE.md") */\n',
        '//! include_str!("../../../OUTSIDE.md")\n',
        'const S: &str = "include_str!(\\"../../../OUTSIDE.md\\")";\n',
        'const S: &str = r#"include_str!("../../../OUTSIDE.md")"#;\n',
    ],
    ids=["line", "block", "nested-block", "doc", "string", "raw-string"],
)
def test_mentions_in_comments_and_strings_are_not_sites(tmp_path, source):
    errors, warnings = scan(tmp_path, source)
    assert errors == [] and warnings == []


def test_masking_preserves_offsets_and_newlines():
    src = 'let a = "xx"; // c\nlet b = 1;\n'
    masked = cpe.mask(src)
    assert len(masked) == len(src)
    assert masked.count("\n") == src.count("\n")
    assert "xx" not in masked


def test_brace_inside_a_string_does_not_close_a_cfg_test_region(tmp_path):
    """A `}` in a literal must not end the region early and re-arm errors."""
    errors, warnings = scan(
        tmp_path,
        "#[cfg(test)]\nmod tests {\n"
        '    const B: &str = "}";\n'
        '    const X: &str = include_str!("../../../OUTSIDE.md");\n}\n',
    )
    assert errors == [], errors
    assert len(warnings) == 1


# --- the allowlist ratchet, against the real repo ---------------------------


def test_known_test_only_entries_all_still_exist():
    """A stale allowlist entry is as bad as a missing one — it hides a fix."""
    for src, target in cpe.KNOWN_TEST_ONLY.items():
        assert (REPO_ROOT / src).is_file(), f"{src} no longer exists"
        assert (REPO_ROOT / target).is_file(), f"{target} no longer exists"


def test_repository_currently_passes():
    """The checker must be green on the tree it ships with."""
    assert cpe.main() == 0


def test_workspace_members_are_discovered_from_the_manifest():
    members = cpe.workspace_members(REPO_ROOT)
    names = {m.name for m in members}
    assert {"cli", "node", "protocol", "e2e"} <= names
    for m in members:
        assert (m / "Cargo.toml").is_file()
