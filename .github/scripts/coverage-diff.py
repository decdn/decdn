#!/usr/bin/env python3
"""Emit a PR coverage diff comment from two LCOV files.

Usage:
    coverage-diff.py <head.lcov> [<base.lcov>]

If <base.lcov> is missing or unreadable (first-ever PR, expired artifact),
the script falls back to a head-only summary.

Reads `$GITHUB_BASE_REF` to find the diff base for `git diff` (default `main`).
Writes markdown to stdout, prefixed with a hidden HTML marker so subsequent
runs can update the same PR comment in place.

Per-commit history (optional):
    DECDN_HEAD_SHA              head commit SHA. On `pull_request` events the
                                workflow injects `pull_request.head.sha` (not
                                GITHUB_SHA, which is the merge SHA).
    DECDN_COVERAGE_PRIOR_BODY   path to the existing PR comment body. The
                                script extracts the prior history block,
                                appends/replaces a row for the current SHA,
                                drops rows whose SHAs are no longer reachable
                                (force-push), and re-renders. Empty/unset =
                                no history block emitted.

Concurrency: two pushes in flight will both read the same prior body and the
later writer wins, dropping the earlier row. Same failure mode as the
pre-history overwrite logic — not worth a lock.

Exit codes:
    0   normal output
    1   head LCOV missing or empty (cargo llvm-cov failed upstream)
    2   bad usage
"""

import os
import re
import subprocess
import sys
from pathlib import Path

# Must match the `marker` constant in .github/workflows/ci.yml's
# Post coverage comment step. Renaming one without the other silently
# breaks the find-or-create logic and starts duplicating PR comments.
MARKER = "<!-- decdn-coverage-comment -->"

# Fence around the per-commit history block. parse_history finds rows
# between these so manual edits to the snapshot above don't corrupt it.
HISTORY_START = "<!-- decdn-coverage-history-start -->"
HISTORY_END = "<!-- decdn-coverage-history-end -->"

# Per-file table is restricted to paths under these roots. Update if the
# workspace gains Rust roots outside crates/ (e.g. top-level examples/).
WORKSPACE_ROOTS = ("crates/",)

# GitHub comments cap at 65,536 characters. Cap the per-file table so
# huge refactor PRs don't 422 the comment-post API.
MAX_TABLE_ROWS = 50

# History rows are tiny (~60 chars) — 30 keeps us far under the 65K cap
# even when stacked with a full per-file table.
MAX_HISTORY_ROWS = 30


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


def short_sha(sha):
    return sha[:7]


_SEPARATOR_CELL = re.compile(r":?-+:?")


def _unbold(cell):
    cell = cell.strip()
    if cell.startswith("**") and cell.endswith("**") and len(cell) > 4:
        return cell[2:-2].strip()
    return cell


def parse_history(prior_body):
    """Extract `(short_sha, total_str, delta_str)` rows from a prior comment.

    Returns [] when the fence is missing, the table is malformed, or the body
    is empty. Tolerates bold markers and `(HEAD)` suffixes from prior renders.
    """
    if not prior_body:
        return []
    start = prior_body.find(HISTORY_START)
    end = prior_body.find(HISTORY_END)
    if start < 0 or end < 0 or end <= start:
        return []
    block = prior_body[start + len(HISTORY_START):end]
    rows = []
    for raw in block.splitlines():
        line = raw.strip()
        if not (line.startswith("|") and line.endswith("|")):
            continue
        cells = [c.strip() for c in line.strip("|").split("|")]
        if len(cells) != 3:
            continue
        if cells[0].lower() == "commit":
            continue
        if all(_SEPARATOR_CELL.fullmatch(c) for c in cells):
            continue
        sha_cell, total_cell, delta_cell = cells
        # Strip `(HEAD)` first: it sits outside the bold markers, so leaving
        # it in place would make _unbold no-op (cell ends with `)`, not `**`)
        # and the SHA would retain the suffix.
        sha_cell = re.sub(r"\s*\(HEAD\)\s*$", "", sha_cell).strip()
        sha_cell = _unbold(sha_cell).strip("`").strip()
        if not sha_cell:
            continue
        rows.append((sha_cell, _unbold(total_cell), _unbold(delta_cell)))
    if not rows:
        # Fence was present but nothing parsed — likely a corrupt or
        # hand-edited prior comment. Surface it; otherwise we'd silently
        # reset the entire history.
        print(
            "warning: history fence present in prior comment but no rows parsed — resetting",
            file=sys.stderr,
        )
    return rows


def prune_unreachable(rows):
    """Drop rows whose SHA fails `git cat-file -e <sha>^{commit}`.

    Keeps every row when git is unavailable — better to leave stale rows than
    nuke history because the runner happens to lack git on PATH.
    """
    if not rows:
        return rows
    kept = []
    for row in rows:
        sha = row[0]
        try:
            r = subprocess.run(
                ["git", "cat-file", "-e", f"{sha}^{{commit}}"],
                capture_output=True,
                text=True,
                check=False,
            )
        except (FileNotFoundError, OSError) as e:
            print(
                f"warning: prune_unreachable could not run git ({e}); keeping all rows",
                file=sys.stderr,
            )
            return list(rows)
        # exit 1 = object missing (legit prune, e.g. force-push).
        # any other non-zero = broken repo state (no .git, bad HEAD, etc.) —
        # keep the row rather than nuke the entire history on infra breakage.
        if r.returncode == 0:
            kept.append(row)
        elif r.returncode == 1:
            print(f"note: dropping unreachable history SHA {sha}", file=sys.stderr)
        else:
            stderr = (r.stderr or "").strip() or "(no stderr)"
            print(
                f"warning: git cat-file unexpected exit {r.returncode} for {sha}: "
                f"{stderr} — keeping row",
                file=sys.stderr,
            )
            kept.append(row)
    return kept


def update_history(rows, sha, total_pct_str, delta_str):
    """Replace the row for `sha` if present (re-run case), else append.

    Then prune unreachable SHAs (force-push cleanup). Always retains the row
    for the current SHA, even if the prune pass would have dropped it (which
    can happen on shallow clones or unusual checkout configs).
    """
    if not sha:
        return list(rows)
    s = short_sha(sha)
    new_row = (s, total_pct_str, delta_str)
    out = [r for r in rows if r[0] != s]
    out.append(new_row)
    out = prune_unreachable(out)
    if not any(r[0] == s for r in out):
        # The only legitimate trigger is a checkout shallow enough that the
        # current PR head SHA isn't reachable to `git cat-file -e`. That's
        # surprising — surface it so it doesn't get masked.
        print(
            f"warning: current SHA {s} was pruned as unreachable; re-adding. "
            f"Check the workflow's checkout depth.",
            file=sys.stderr,
        )
        out.append(new_row)
    return out


def render_history(rows, head_sha):
    """Emit the fenced `<details>` history block. Returns [] if no rows."""
    if not rows:
        return []
    head_short = short_sha(head_sha) if head_sha else ""
    truncated = 0
    if len(rows) > MAX_HISTORY_ROWS:
        truncated = len(rows) - MAX_HISTORY_ROWS
        rows = rows[-MAX_HISTORY_ROWS:]
    out = [HISTORY_START, "<details><summary>History</summary>", ""]
    if truncated:
        plural = "ies" if truncated != 1 else "y"
        out.append(f"_…{truncated} older entr{plural} dropped to fit cap…_")
        out.append("")
    out.append("| Commit | Total | Δ vs base |")
    out.append("|---|---:|---:|")
    for sha, total, delta in rows:
        if sha == head_short:
            out.append(f"| **`{sha}`** (HEAD) | **{total}** | **{delta}** |")
        else:
            out.append(f"| `{sha}` | {total} | {delta} |")
    out += ["", "</details>", HISTORY_END]
    return out


def _compose_history_block(head_sha, total_pct_str, delta_str):
    """Read prior body from `$DECDN_COVERAGE_PRIOR_BODY`, update, render."""
    if not head_sha:
        return []
    prior_path = os.environ.get("DECDN_COVERAGE_PRIOR_BODY")
    prior_body = ""
    if prior_path and Path(prior_path).is_file():
        try:
            prior_body = Path(prior_path).read_text(encoding="utf-8")
        except OSError as e:
            print(
                f"warning: could not read prior comment {prior_path}: {e}",
                file=sys.stderr,
            )
    rows = parse_history(prior_body)
    rows = update_history(rows, head_sha, total_pct_str, delta_str)
    return render_history(rows, head_sha)


def main():
    if len(sys.argv) < 2:
        print("usage: coverage-diff.py <head.lcov> [<base.lcov>]", file=sys.stderr)
        return 2

    head_path = sys.argv[1]
    base_path = sys.argv[2] if len(sys.argv) > 2 else None
    base_ref = os.environ.get("GITHUB_BASE_REF") or "main"
    head_sha = os.environ.get("DECDN_HEAD_SHA", "")

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
        history_block = _compose_history_block(head_sha, "_build failed_", "—")
        _emit(report, report, history_block)
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
        delta_str = "—"
    else:
        base_lf = sum(lf for lf, _ in base.values())
        base_lh = sum(lh for _, lh in base.values())
        base_pct = pct(base_lh, base_lf)
        delta_str = fmt_delta(head_pct - base_pct)
        header.append(
            f"Total: **{fmt_pct(head_pct)}** "
            f"(was {fmt_pct(base_pct)}, {delta_str}) — "
            f"{head_lh}/{head_lf} lines"
        )

    history_block = _compose_history_block(head_sha, fmt_pct(head_pct), delta_str)

    changed = changed_rust_files(base_ref)
    if changed is None:
        report = header + ["", f"_Could not resolve `origin/{base_ref}` — per-file diff unavailable._"]
        _emit(report, report, history_block)
        return 0
    if not changed:
        report = header + ["", "_No Rust files changed in this PR._"]
        _emit(report, report, history_block)
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
            delta_cell = "_new_"
        else:
            b_lf, b_lh = bv
            delta_cell = fmt_delta(h_pct - pct(b_lh, b_lf))
        rows.append((path, fmt_pct(h_pct), f"{h_lh}/{h_lf}", delta_cell))

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
        _emit(report, report, history_block)
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
    _emit(stdout_report, summary_report, history_block)
    return 0


def _emit(stdout_lines, summary_lines, history_block=None):
    """Write the report to stdout and (when set) to GITHUB_STEP_SUMMARY."""
    suffix = ([""] + list(history_block)) if history_block else []
    sys.stdout.write("\n".join(stdout_lines + suffix) + "\n")
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not summary_path:
        return
    try:
        with open(summary_path, "a", encoding="utf-8") as f:
            f.write("\n".join(summary_lines + suffix) + "\n")
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
