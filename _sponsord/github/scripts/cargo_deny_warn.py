#!/usr/bin/env python3
"""Run `cargo deny check`: warn on findings, fail when no check ran.

Findings (a vulnerable, yanked, banned, or mis-licensed dependency) never
block a commit or a merge. An upstream advisory lands on every branch at
once, so it is reported, not gated.

A run that did not check anything is a different case. `cargo deny` exits 1
for a finding and also for a `deny.toml` parse error or an advisory-db fetch
failure, so the exit code cannot tell them apart. The summary line
(`advisories ok, bans ok, licenses ok, sources ok`) prints only when the
checks ran, so it is the signal. No summary means no audit happened, and
this script fails so the gap is visible rather than reported as a finding.

`-A duplicate` and `--hide-inclusion-graph` drop the duplicate-version lints
and the reverse-dependency trees, which are most of the output. Every other
warning (yanked crates, stale ignores, unused license entries) still prints.

Under GitHub Actions (`GITHUB_ACTIONS=true`) each outcome also emits a
workflow annotation: the summary and each distinct `error[...]` title as a
warning, or a single error when no check ran.

Run: .github/scripts/cargo-deny-warn.sh
"""

from __future__ import annotations

import os
import re
import subprocess
import sys

DENY_ARGS = [
    "cargo", "deny", "--color", "never", "--all-features",
    "check", "--hide-inclusion-graph", "-A", "duplicate",
]

SUMMARY_RE = re.compile(
    r"^advisories (?:ok|FAILED), bans (?:ok|FAILED), "
    r"licenses (?:ok|FAILED), sources (?:ok|FAILED)$",
    re.MULTILINE,
)
ERROR_RE = re.compile(r"^error\[[^\]]+\]: .*$", re.MULTILINE)

CLEAN = "clean"
FINDINGS = "findings"
NOT_RUN = "not-run"


def classify(returncode: int, output: str) -> str:
    """Name the outcome of one `cargo deny check` run.

    The summary line decides whether the checks ran, whatever the exit code;
    only then does the exit code separate clean from findings.
    """
    if not SUMMARY_RE.search(output):
        return NOT_RUN
    return CLEAN if returncode == 0 else FINDINGS


def escape_annotation(message: str) -> str:
    """Escape a message for a `::warning::` / `::error::` workflow command."""
    return message.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")


def annotations(outcome: str, returncode: int, output: str) -> list[str]:
    """The workflow commands that describe `outcome` on the run summary."""
    if outcome == FINDINGS:
        summary = SUMMARY_RE.search(output)
        lines = [summary.group(0)] if summary else []
        lines += sorted(set(ERROR_RE.findall(output)))
        return [f"::warning title=cargo deny::{escape_annotation(l)}" for l in lines]
    if outcome == NOT_RUN:
        return [
            "::error title=cargo deny did not run::"
            + escape_annotation(f"exit {returncode} with no check summary; see the step log")
        ]
    return []


def main() -> int:
    try:
        probe = subprocess.run(
            ["cargo", "deny", "--version"], capture_output=True, text=True, check=False
        )
    except FileNotFoundError:
        print("error: cargo is not on PATH; install Rust via rustup first", file=sys.stderr)
        return 1
    if probe.returncode != 0:
        print(
            "error: cargo-deny is not installed; run `cargo install --locked cargo-deny`",
            file=sys.stderr,
        )
        return 1

    run = subprocess.run(
        DENY_ARGS, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, check=False
    )
    output = run.stdout
    print(output, end="" if output.endswith("\n") or not output else "\n")

    outcome = classify(run.returncode, output)
    if os.environ.get("GITHUB_ACTIONS") == "true":
        for line in annotations(outcome, run.returncode, output):
            print(line)
    sys.stdout.flush()  # keep the verdict below the output in a merged log

    if outcome == FINDINGS:
        print("warning: cargo deny reported findings (non-blocking)", file=sys.stderr)
        return 0
    if outcome == NOT_RUN:
        print(
            f"error: cargo deny exited {run.returncode} without running its checks: "
            "a deny.toml error, an advisory-db fetch failure "
            "(an anonymous GitHub 401 clears with `gh auth setup-git`), or a tool failure",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
