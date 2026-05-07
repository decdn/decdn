#!/usr/bin/env python3
"""Emit a PR coverage diff comment from two LCOV files.

Usage:
    coverage-diff.py <head.lcov> [<base.lcov>]

If <base.lcov> is missing or unreadable (first-ever PR, expired artifact),
the script falls back to a head-only summary.

Reads `$GITHUB_BASE_REF` to find the diff base for `git diff` (default `main`).
Writes markdown to stdout, prefixed with a hidden HTML marker so subsequent
runs can update the same PR comment in place.

Exit codes:
    0   normal output
    1   head LCOV missing or empty (cargo llvm-cov failed upstream)
    2   bad usage
"""

import os
import subprocess
import sys
from pathlib import Path

# Must match the `marker` constant in .github/workflows/ci.yml's
# Post coverage comment step. Renaming one without the other silently
# breaks the find-or-create logic and starts duplicating PR comments.
MARKER = "<!-- decdn-coverage-comment -->"

# Per-file table is restricted to paths under these roots. Update if the
# workspace gains Rust roots outside crates/ (e.g. top-level examples/).
WORKSPACE_ROOTS = ("crates/",)

# GitHub comments cap at 65,536 characters. Cap the per-file table so
# huge refactor PRs don't 422 the comment-post API.
MAX_TABLE_ROWS = 50


def parse_lcov(path):
    """Return {file: (lines_found, lines_hit)} or None if path is unusable."""
    if not path or not Path(path).is_file():
        return None
    coverage = {}
    current = None
    lf = lh = 0
    try:
        with open(path, encoding="utf-8") as f:
            for line_no, raw in enumerate(f, 1):
                line = raw.rstrip()
                if line.startswith("SF:"):
                    current = line[3:]
                    lf = lh = 0
                elif line.startswith("LF:"):
                    lf = _parse_int(line[3:], "LF", line_no, path)
                elif line.startswith("LH:"):
                    lh = _parse_int(line[3:], "LH", line_no, path)
                elif line == "end_of_record" and current is not None:
                    coverage[current] = (lf, lh)
                    current = None
    except OSError as e:
        print(f"warning: could not read {path}: {e}", file=sys.stderr)
        return None
    return coverage


def _parse_int(s, field, line_no, path):
    try:
        return int(s)
    except ValueError:
        print(
            f"warning: malformed {field} value {s!r} at {path}:{line_no} — treating as 0",
            file=sys.stderr,
        )
        return 0


def changed_rust_files(base_ref):
    """Return set of `.rs` paths changed vs origin/<base_ref>; None on failure.

    On failure, captures git's stderr and surfaces a single-line summary on our
    own stderr (workflow log) so maintainers can diagnose shallow-clone or
    missing-ref problems without having to re-run with verbose flags.
    """
    try:
        result = subprocess.run(
            ["git", "diff", "--name-only", f"origin/{base_ref}...HEAD"],
            capture_output=True,
            text=True,
            check=True,
        )
    except FileNotFoundError as e:
        print(f"git not found: {e}", file=sys.stderr)
        return None
    except subprocess.CalledProcessError as e:
        msg = e.stderr.strip() if e.stderr else "(no stderr)"
        print(f"git diff failed (exit {e.returncode}): {msg}", file=sys.stderr)
        return None
    return {p for p in result.stdout.splitlines() if p.endswith(".rs")}


def pct(hit, total):
    return 100.0 * hit / total if total else 0.0


def fmt_pct(p):
    return f"{p:.2f}%"


def fmt_delta(delta):
    if abs(delta) < 0.005:
        return "● 0.00%"
    if delta > 0:
        return f"▲ +{abs(delta):.2f}%"
    return f"▼ {abs(delta):.2f}%"


def normalize(path):
    """Convert an absolute or relative LCOV path to a workspace-relative path.

    cargo-llvm-cov emits absolute paths under GitHub Actions (e.g.
    `/home/runner/work/decdn/decdn/crates/cache/src/engine.rs`) and relative
    ones in other configs. We prefer `os.path.relpath(path, GITHUB_WORKSPACE)`
    when both are absolute, falling back to a `crates/` substring scan so the
    script remains usable in local testing where GITHUB_WORKSPACE isn't set.
    """
    workspace = os.environ.get("GITHUB_WORKSPACE")
    if workspace and os.path.isabs(path):
        try:
            rel = os.path.relpath(path, workspace)
            if not rel.startswith(".."):
                return rel
        except ValueError:
            pass
    p = path.lstrip("/")
    for root in WORKSPACE_ROOTS:
        idx = p.find(root)
        if idx >= 0:
            return p[idx:]
    return p


def main():
    if len(sys.argv) < 2:
        print("usage: coverage-diff.py <head.lcov> [<base.lcov>]", file=sys.stderr)
        return 2

    head_path = sys.argv[1]
    base_path = sys.argv[2] if len(sys.argv) > 2 else None
    base_ref = os.environ.get("GITHUB_BASE_REF") or "main"

    head = parse_lcov(head_path)
    if head is None or not head:
        if head is None:
            err = (
                f"Could not read head coverage file `{head_path}` — "
                f"the `cargo llvm-cov` step likely failed. See the workflow log."
            )
        else:
            err = (
                f"Head coverage file `{head_path}` parsed but contained no records — "
                f"`cargo llvm-cov` likely produced no output. See the workflow log."
            )
        report = [MARKER, "## Coverage report", "", err]
        _emit(report, report)
        return 1

    base = parse_lcov(base_path) if base_path else None

    head_lf = sum(lf for lf, _ in head.values())
    head_lh = sum(lh for _, lh in head.values())
    head_pct = pct(head_lh, head_lf)

    header = [MARKER, "## Coverage report", ""]

    if base is None:
        header.append(f"Total: **{fmt_pct(head_pct)}** ({head_lh}/{head_lf} lines)")
        header.append("")
        if base_path and Path(base_path).parent.is_dir():
            header.append(
                f"_Baseline `lcov.info` for `origin/{base_ref}` not found "
                f"(first run on this branch, or the baseline artifact has expired)._"
            )
        else:
            header.append(
                f"_No baseline coverage available for `origin/{base_ref}` — "
                f"deltas will appear once the next push to `{base_ref}` completes._"
            )
    else:
        base_lf = sum(lf for lf, _ in base.values())
        base_lh = sum(lh for _, lh in base.values())
        base_pct = pct(base_lh, base_lf)
        header.append(
            f"Total: **{fmt_pct(head_pct)}** "
            f"(was {fmt_pct(base_pct)}, {fmt_delta(head_pct - base_pct)}) — "
            f"{head_lh}/{head_lf} lines"
        )

    changed = changed_rust_files(base_ref)
    if changed is None:
        report = header + ["", f"_Could not resolve `origin/{base_ref}` — per-file diff unavailable._"]
        _emit(report, report)
        return 0
    if not changed:
        report = header + ["", "_No Rust files changed in this PR._"]
        _emit(report, report)
        return 0

    head_norm = {normalize(p): v for p, v in head.items()}
    base_norm = {normalize(p): v for p, v in (base or {}).items()}

    rows = []
    skipped = []
    for path in sorted(changed):
        if not any(path.startswith(r) for r in WORKSPACE_ROOTS):
            skipped.append(path)
            continue
        hv = head_norm.get(path)
        bv = base_norm.get(path)
        if hv is None and bv is None:
            continue
        if hv is None and bv is not None:
            rows.append((path, "—", "—", "_deleted_"))
            continue
        h_lf, h_lh = hv
        h_pct = pct(h_lh, h_lf)
        if bv is None:
            delta_str = "_new_"
        else:
            b_lf, b_lh = bv
            delta_str = fmt_delta(h_pct - pct(b_lh, b_lf))
        rows.append((path, fmt_pct(h_pct), f"{h_lh}/{h_lf}", delta_str))

    if skipped:
        sample = ", ".join(skipped[:5])
        suffix = "…" if len(skipped) > 5 else ""
        print(
            f"note: skipped {len(skipped)} changed .rs file(s) outside {WORKSPACE_ROOTS}: "
            f"{sample}{suffix}",
            file=sys.stderr,
        )

    if not rows:
        report = header + ["", "_No coverage data for changed Rust files._"]
        _emit(report, report)
        return 0

    truncated = len(rows) > MAX_TABLE_ROWS
    truncation_note = (
        f"_Showing top {MAX_TABLE_ROWS} of {len(rows)} changed files — "
        f"see the workflow Step Summary for the full table._"
    )
    stdout_report = header + [""] + list(_table_lines(rows[:MAX_TABLE_ROWS]))
    if truncated:
        stdout_report += ["", truncation_note]
    summary_report = header + [""] + list(_table_lines(rows))
    _emit(stdout_report, summary_report)
    return 0


def _emit(stdout_lines, summary_lines):
    """Write the report to stdout and (when set) to GITHUB_STEP_SUMMARY."""
    sys.stdout.write("\n".join(stdout_lines) + "\n")
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not summary_path:
        return
    try:
        with open(summary_path, "a", encoding="utf-8") as f:
            f.write("\n".join(summary_lines) + "\n")
    except OSError as e:
        print(f"warning: could not write step summary at {summary_path}: {e}",
              file=sys.stderr)


def _table_lines(rows):
    """Yield markdown lines for the coverage table (header + body)."""
    yield "| File | Coverage | Lines | Δ |"
    yield "|---|---:|---:|---:|"
    for path, cov, lines, delta in rows:
        yield f"| `{path}` | {cov} | {lines} | {delta} |"


if __name__ == "__main__":
    sys.exit(main())
