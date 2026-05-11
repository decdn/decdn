"""Unit tests for coverage-diff.py helpers.

The script's filename has a hyphen, so it isn't directly importable. Load it
via importlib so tests can call its functions as `coverage_diff.<name>`.
"""

import importlib.util
import pathlib
from types import SimpleNamespace

import pytest

_SPEC = importlib.util.spec_from_file_location(
    "coverage_diff",
    pathlib.Path(__file__).resolve().parents[1] / "coverage-diff.py",
)
coverage_diff = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(coverage_diff)


def test_parse_history_empty_body():
    assert coverage_diff.parse_history("") == []


def test_parse_history_no_fence():
    body = "## Coverage report\n\nTotal: 50%\n"
    assert coverage_diff.parse_history(body) == []


def test_parse_history_fence_without_table():
    body = (
        coverage_diff.HISTORY_START
        + "\n<details><summary>History</summary>\n\n</details>\n"
        + coverage_diff.HISTORY_END
    )
    assert coverage_diff.parse_history(body) == []


def test_parse_history_extracts_plain_rows():
    body = "\n".join([
        coverage_diff.HISTORY_START,
        "<details><summary>History</summary>",
        "",
        "| Commit | Total | Δ vs base |",
        "|---|---:|---:|",
        "| `abc1234` | 86.50% | ▲ +0.27% |",
        "| `def5678` | 87.10% | ▲ +0.87% |",
        "",
        "</details>",
        coverage_diff.HISTORY_END,
    ])
    assert coverage_diff.parse_history(body) == [
        ("abc1234", "86.50%", "▲ +0.27%"),
        ("def5678", "87.10%", "▲ +0.87%"),
    ]


def test_parse_history_strips_bold_and_head_marker():
    body = "\n".join([
        coverage_diff.HISTORY_START,
        "| Commit | Total | Δ vs base |",
        "|---|---:|---:|",
        "| **`9ab012c`** (HEAD) | **87.45%** | **▲ +1.22%** |",
        coverage_diff.HISTORY_END,
    ])
    assert coverage_diff.parse_history(body) == [
        ("9ab012c", "87.45%", "▲ +1.22%"),
    ]


def test_parse_history_round_trip():
    rows = [
        ("abc1234", "86.50%", "▲ +0.27%"),
        ("def5678", "87.10%", "▲ +0.87%"),
        ("9ab012c", "87.45%", "▲ +1.22%"),
    ]
    rendered = "\n".join(coverage_diff.render_history(rows, head_sha="9ab012c"))
    assert coverage_diff.parse_history(rendered) == rows


def test_parse_history_skips_truncation_note():
    body = "\n".join([
        coverage_diff.HISTORY_START,
        "<details><summary>History</summary>",
        "",
        "_…2 older entries dropped to fit cap…_",
        "",
        "| Commit | Total | Δ vs base |",
        "|---|---:|---:|",
        "| `abc1234` | 86.50% | ▲ +0.27% |",
        "</details>",
        coverage_diff.HISTORY_END,
    ])
    assert coverage_diff.parse_history(body) == [("abc1234", "86.50%", "▲ +0.27%")]


@pytest.fixture
def no_prune(monkeypatch):
    """Disable prune_unreachable by short-circuiting it to identity."""
    monkeypatch.setattr(coverage_diff, "prune_unreachable", lambda rows: list(rows))


def test_update_history_appends_new_sha(no_prune):
    rows = [("abc1234", "86.50%", "▲ +0.27%")]
    out = coverage_diff.update_history(rows, "def56789cafe", "87.10%", "▲ +0.87%")
    assert out == [
        ("abc1234", "86.50%", "▲ +0.27%"),
        ("def5678", "87.10%", "▲ +0.87%"),
    ]


def test_update_history_replaces_existing_sha(no_prune):
    rows = [
        ("abc1234", "86.50%", "▲ +0.27%"),
        ("def5678", "87.10%", "▲ +0.87%"),
    ]
    out = coverage_diff.update_history(rows, "def56789cafe", "87.50%", "▲ +1.27%")
    assert out == [
        ("abc1234", "86.50%", "▲ +0.27%"),
        ("def5678", "87.50%", "▲ +1.27%"),
    ]


def test_update_history_empty_sha_returns_rows_unchanged():
    rows = [("abc1234", "86.50%", "▲ +0.27%")]
    assert coverage_diff.update_history(rows, "", "x", "y") == rows


def test_update_history_keeps_current_row_even_if_pruned(monkeypatch):
    monkeypatch.setattr(coverage_diff, "prune_unreachable", lambda rows: [])
    out = coverage_diff.update_history([], "feedfacecafe", "50.00%", "—")
    assert out == [("feedfac", "50.00%", "—")]


def test_prune_unreachable_drops_missing(monkeypatch):
    missing = {"badbad1"}

    def fake_run(cmd, **kwargs):
        sha = cmd[-1].split("^")[0]
        rc = 1 if sha in missing else 0
        return SimpleNamespace(returncode=rc, stdout="", stderr="")

    monkeypatch.setattr(coverage_diff.subprocess, "run", fake_run)
    rows = [
        ("abc1234", "86.50%", "▲ +0.27%"),
        ("badbad1", "0%", "—"),
        ("def5678", "87.10%", "▲ +0.87%"),
    ]
    assert coverage_diff.prune_unreachable(rows) == [
        ("abc1234", "86.50%", "▲ +0.27%"),
        ("def5678", "87.10%", "▲ +0.87%"),
    ]


def test_prune_unreachable_keeps_all_when_git_missing(monkeypatch):
    def fake_run(cmd, **kwargs):
        raise FileNotFoundError("git not found")

    monkeypatch.setattr(coverage_diff.subprocess, "run", fake_run)
    rows = [("abc1234", "86.50%", "▲ +0.27%")]
    assert coverage_diff.prune_unreachable(rows) == rows


def test_prune_unreachable_empty_rows_short_circuits(monkeypatch):
    def fail(*a, **k):
        raise AssertionError("subprocess.run should not be called for empty rows")

    monkeypatch.setattr(coverage_diff.subprocess, "run", fail)
    assert coverage_diff.prune_unreachable([]) == []


def test_render_history_empty_rows_returns_empty():
    assert coverage_diff.render_history([], "abc1234") == []


def test_render_history_bolds_head_row():
    rows = [
        ("abc1234", "86.50%", "▲ +0.27%"),
        ("def5678", "87.10%", "▲ +0.87%"),
    ]
    out = "\n".join(coverage_diff.render_history(rows, head_sha="def56789cafe"))
    assert "| `abc1234` | 86.50% | ▲ +0.27% |" in out
    assert "| **`def5678`** (HEAD) | **87.10%** | **▲ +0.87%** |" in out
    assert out.startswith(coverage_diff.HISTORY_START)
    assert out.endswith(coverage_diff.HISTORY_END)


def test_render_history_truncates_to_cap(monkeypatch):
    monkeypatch.setattr(coverage_diff, "MAX_HISTORY_ROWS", 3)
    rows = [(f"sha{i:04d}", "50%", "—") for i in range(5)]
    out_lines = coverage_diff.render_history(rows, head_sha="sha0004")
    out = "\n".join(out_lines)
    assert "_…2 older entries dropped to fit cap…_" in out
    # First two rows should be dropped; rows 2,3,4 kept
    assert "sha0000" not in out
    assert "sha0001" not in out
    assert "sha0002" in out
    assert "sha0004" in out


def test_render_history_truncation_singular(monkeypatch):
    monkeypatch.setattr(coverage_diff, "MAX_HISTORY_ROWS", 2)
    rows = [(f"sha{i:04d}", "50%", "—") for i in range(3)]
    out = "\n".join(coverage_diff.render_history(rows, head_sha="sha0002"))
    assert "_…1 older entry dropped to fit cap…_" in out


def test_compose_history_block_no_sha_returns_empty(monkeypatch):
    monkeypatch.delenv("DECDN_COVERAGE_PRIOR_BODY", raising=False)
    assert coverage_diff._compose_history_block("", "50%", "—") == []


def test_compose_history_block_no_prior_starts_fresh(monkeypatch, tmp_path):
    monkeypatch.delenv("DECDN_COVERAGE_PRIOR_BODY", raising=False)
    monkeypatch.setattr(coverage_diff, "prune_unreachable", lambda rows: list(rows))
    out = "\n".join(coverage_diff._compose_history_block("abc12349cafe", "50.00%", "—"))
    assert "| **`abc1234`** (HEAD) | **50.00%** | **—** |" in out


def test_compose_history_block_appends_to_prior(monkeypatch, tmp_path):
    monkeypatch.setattr(coverage_diff, "prune_unreachable", lambda rows: list(rows))
    prior = tmp_path / "prior.md"
    prior.write_text("\n".join([
        coverage_diff.HISTORY_START,
        "| Commit | Total | Δ vs base |",
        "|---|---:|---:|",
        "| `abc1234` | 86.50% | ▲ +0.27% |",
        coverage_diff.HISTORY_END,
    ]), encoding="utf-8")
    monkeypatch.setenv("DECDN_COVERAGE_PRIOR_BODY", str(prior))

    out = "\n".join(coverage_diff._compose_history_block("def56789cafe", "87.10%", "▲ +0.87%"))
    assert "| `abc1234` | 86.50% | ▲ +0.27% |" in out
    assert "| **`def5678`** (HEAD) | **87.10%** | **▲ +0.87%** |" in out


def test_compose_history_block_missing_prior_file_is_silent(monkeypatch, tmp_path):
    monkeypatch.setattr(coverage_diff, "prune_unreachable", lambda rows: list(rows))
    monkeypatch.setenv("DECDN_COVERAGE_PRIOR_BODY", str(tmp_path / "does-not-exist.md"))
    out = coverage_diff._compose_history_block("abc12349cafe", "50.00%", "—")
    assert out  # still emits a single-row history
    assert "abc1234" in "\n".join(out)


# main() integration: ensure every emit path threads history correctly. The
# build-failure (exit 1) path is the one most likely to silently lose history
# on regression — a maintainer dropping `history_block` from one of the four
# success-path _emit calls would leave the others working and pass review.


def _write_lcov(path, records):
    """records: list of (sf, lf, lh)."""
    lines = []
    for sf, lf, lh in records:
        lines.append(f"SF:{sf}")
        lines.append(f"LF:{lf}")
        lines.append(f"LH:{lh}")
        lines.append("end_of_record")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


@pytest.fixture
def main_env(monkeypatch, tmp_path):
    """Common setup: stub prune + git diff so main() runs hermetically."""
    monkeypatch.setattr(coverage_diff, "prune_unreachable", lambda rows: list(rows))
    monkeypatch.setattr(coverage_diff, "changed_rust_files", lambda base_ref: set())
    prior = tmp_path / "prior.md"
    prior.write_text("\n".join([
        coverage_diff.HISTORY_START,
        "| Commit | Total | Δ vs base |",
        "|---|---:|---:|",
        "| `abc1234` | 80.00% | ▲ +0.50% |",
        coverage_diff.HISTORY_END,
    ]), encoding="utf-8")
    monkeypatch.setenv("DECDN_COVERAGE_PRIOR_BODY", str(prior))
    monkeypatch.setenv("DECDN_HEAD_SHA", "def56789cafefeedface")
    monkeypatch.delenv("GITHUB_STEP_SUMMARY", raising=False)
    return tmp_path


def test_main_build_failure_emits_history_with_placeholder(main_env, monkeypatch, capsys):
    """Exit-1 path (head LCOV missing): error message + prior history + `_build failed_` row."""
    missing = main_env / "does-not-exist.lcov"
    monkeypatch.setattr(coverage_diff.sys, "argv", ["coverage-diff.py", str(missing)])
    rc = coverage_diff.main()
    out = capsys.readouterr().out
    assert rc == 1
    assert "Could not read head coverage file" in out
    assert "| `abc1234` | 80.00% | ▲ +0.50% |" in out
    assert "| **`def5678`** (HEAD) | **_build failed_** | **—** |" in out


def test_main_no_changed_files_emits_history(main_env, monkeypatch, capsys):
    """Happy path with no changed Rust files: snapshot + prior history + new HEAD row."""
    head = main_env / "head.lcov"
    base = main_env / "base.lcov"
    _write_lcov(head, [("/w/crates/cache/src/lib.rs", 100, 90)])
    _write_lcov(base, [("/w/crates/cache/src/lib.rs", 100, 80)])
    monkeypatch.setattr(coverage_diff.sys, "argv", ["coverage-diff.py", str(head), str(base)])
    rc = coverage_diff.main()
    out = capsys.readouterr().out
    assert rc == 0
    assert "_No Rust files changed in this PR._" in out
    assert "| `abc1234` | 80.00% | ▲ +0.50% |" in out
    assert "| **`def5678`** (HEAD) | **90.00%** | **▲ +10.00%** |" in out
