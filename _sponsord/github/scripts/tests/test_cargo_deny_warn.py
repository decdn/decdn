"""Regression tests for the cargo-deny warn-only wrapper.

`cargo deny` exits 1 both for a finding and for a run that checked nothing
(a `deny.toml` error, an advisory-db fetch failure). A bug in telling them
apart degrades to a quiet false pass: a broken audit reads as a finding, and
a finding is non-blocking, so nobody looks.
"""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
MODULE_PATH = REPO_ROOT / ".github/scripts/cargo_deny_warn.py"

_spec = importlib.util.spec_from_file_location("cargo_deny_warn", MODULE_PATH)
assert _spec and _spec.loader
cdw = importlib.util.module_from_spec(_spec)
sys.modules["cargo_deny_warn"] = cdw
_spec.loader.exec_module(cdw)

CLEAN_OUT = "advisories ok, bans ok, licenses ok, sources ok\n"
FINDINGS_OUT = (
    "error[vulnerability]: TLS 1.3 handshake messages incorrectly accepted\n"
    "   ┌─ Cargo.lock:495:1\n"
    "warning[yanked]: detected yanked crate (try `cargo update -p spin`)\n"
    "error[vulnerability]: TLS 1.3 handshake messages incorrectly accepted\n"
    "error[rejected]: failed to satisfy license requirements\n"
    "advisories FAILED, bans ok, licenses FAILED, sources ok\n"
)
CONFIG_ERROR_OUT = (
    "error[parse]: unexpected character\n"
    "  ┌─ deny.toml:3:1\n"
)
FETCH_ERROR_OUT = "error: failed to fetch advisory database: HTTP 401\n"


def test_zero_exit_is_clean():
    assert cdw.classify(0, CLEAN_OUT) == cdw.CLEAN


def test_zero_exit_without_summary_is_not_run():
    # A zero exit alone is not proof of an audit; the summary is.
    assert cdw.classify(0, "") == cdw.NOT_RUN
    assert cdw.classify(0, FETCH_ERROR_OUT) == cdw.NOT_RUN


def test_nonzero_exit_with_summary_is_findings():
    assert cdw.classify(1, FINDINGS_OUT) == cdw.FINDINGS
    assert cdw.classify(5, FINDINGS_OUT) == cdw.FINDINGS


def test_config_error_is_not_run_even_with_error_lines():
    # A deny.toml parse error exits 1 and prints an `error[...]` diagnostic,
    # exactly like a finding; only the missing summary tells it apart.
    assert cdw.classify(1, CONFIG_ERROR_OUT) == cdw.NOT_RUN


def test_fetch_failure_is_not_run():
    assert cdw.classify(1, FETCH_ERROR_OUT) == cdw.NOT_RUN


def test_summary_must_be_a_whole_line():
    quoted = "note: expected `advisories ok, bans ok, licenses ok, sources ok` here\n"
    assert cdw.classify(1, quoted) == cdw.NOT_RUN


def test_findings_annotate_summary_and_each_distinct_error():
    got = cdw.annotations(cdw.FINDINGS, 5, FINDINGS_OUT)
    assert got == [
        "::warning title=cargo deny::advisories FAILED, bans ok, licenses FAILED, sources ok",
        "::warning title=cargo deny::error[rejected]: failed to satisfy license requirements",
        "::warning title=cargo deny::error[vulnerability]: "
        "TLS 1.3 handshake messages incorrectly accepted",
    ]


def test_not_run_annotates_one_error():
    got = cdw.annotations(cdw.NOT_RUN, 1, CONFIG_ERROR_OUT)
    assert len(got) == 1
    assert got[0].startswith("::error title=cargo deny did not run::exit 1")


def test_clean_has_no_annotations():
    assert cdw.annotations(cdw.CLEAN, 0, CLEAN_OUT) == []


def test_annotation_escaping():
    assert cdw.escape_annotation("100%\r\nnext") == "100%25%0D%0Anext"


def fake_run(version_rc: int, deny_rc: int, deny_out: str):
    def run(args, **_kwargs):
        if args == ["cargo", "deny", "--version"]:
            return subprocess.CompletedProcess(args, version_rc, "cargo-deny 0.20.2\n", "")
        assert args == cdw.DENY_ARGS
        return subprocess.CompletedProcess(args, deny_rc, deny_out, None)

    return run


def test_main_exit_codes(monkeypatch, capsys):
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    cases = [
        (fake_run(0, 0, CLEAN_OUT), 0),
        (fake_run(0, 0, ""), 1),
        (fake_run(0, 1, FINDINGS_OUT), 0),
        (fake_run(0, 1, CONFIG_ERROR_OUT), 1),
        (fake_run(0, 1, FETCH_ERROR_OUT), 1),
        (fake_run(101, 0, CLEAN_OUT), 1),
    ]
    for run, expected in cases:
        monkeypatch.setattr(cdw.subprocess, "run", run)
        assert cdw.main() == expected
    capsys.readouterr()


def test_main_missing_cargo_fails(monkeypatch, capsys):
    def run(args, **_kwargs):
        raise FileNotFoundError("cargo")

    monkeypatch.setattr(cdw.subprocess, "run", run)
    assert cdw.main() == 1
    assert "cargo is not on PATH" in capsys.readouterr().err


def test_main_emits_annotations_only_under_actions(monkeypatch, capsys):
    monkeypatch.setattr(cdw.subprocess, "run", fake_run(0, 1, FINDINGS_OUT))
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    cdw.main()
    assert "::warning" not in capsys.readouterr().out

    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    cdw.main()
    assert "::warning title=cargo deny::advisories FAILED" in capsys.readouterr().out
