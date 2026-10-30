#!/usr/bin/env python3
"""Fail if the release or security workflow widens its token or floats an action.

`release.yml` builds and stages every artifact a user installs, and
`security.yml` scans the published images. A step in either runs with the
job's `GITHUB_TOKEN`, so an action fetched from a moved tag, or a token left
in `.git/config` for a later step to read, hands that token to code nobody
reviewed. With `packages: write` that code can push an image tag; with
`contents: write` it can replace release assets. Four rules keep the token
narrow and the code fixed:

1. Every remote `uses:` names a full 40-hex commit SHA. A tag or branch can be
   moved to other code; a SHA cannot. A `docker://` ref names an
   `@sha256:` digest for the same reason. A local `./…` action is checked
   in, and its own `action.yml` is held to rules 1 and 2.
2. Every `actions/checkout` step sets `persist-credentials: false`, so no
   later step finds the token in the checkout's git config.
3. The workflow-level `permissions:` block exists and grants no `write`, so a
   job that names no permissions of its own can only read.
4. Each job's write grants are exactly its row in WRITE_JOBS, and a job that
   holds `contents: write` runs only GitHub's own `actions/*` actions. The
   table is exact in both directions, so a grant that is dropped from the
   workflow leaves a stale row that fails here until the table moves too.

Text, not YAML: the files are read with the standard library only, line by
line with comments stripped, so a commented-out step is not a site. A file
with no `uses:` at all is an error, so a pattern that matches nothing cannot
report a pass.

Run: .github/scripts/check-release-hardening.sh
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

if sys.version_info < (3, 11):  # the syntax used below
    sys.exit(f"error: python 3.11+ required, found {sys.version.split()[0]}")

# Each workflow in scope, mapped to the jobs allowed a write grant and the
# scopes each may write. A workflow with no row here may grant no write.
WRITE_JOBS: dict[str, dict[str, set[str]]] = {
    ".github/workflows/release.yml": {
        "draft": {"contents"},
        "upload-assets": {"contents"},
        "attach": {"contents"},
        "docker": {"packages"},
    },
    ".github/workflows/security.yml": {},
}

# A `#` at line start or after whitespace opens a YAML comment; one inside a
# value does not, and a `uses:` value never carries one.
COMMENT = re.compile(r"(^|\s)#.*$")
USES = re.compile(r"\buses:\s*['\"]?([^\s'\"]+)")
SHA_PINNED = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+(?:/[^@\s]+)?@[0-9a-f]{40}$")
DIGEST_PINNED = re.compile(r"^docker://[^@\s]+@sha256:[0-9a-f]{64}$")
PERSIST_FALSE = re.compile(r"^\s*persist-credentials:\s*['\"]?false['\"]?\s*$")
TOP_KEY = re.compile(r"^([A-Za-z_][A-Za-z0-9_-]*):(.*)$")
JOB_KEY = re.compile(r"^  ([A-Za-z_][A-Za-z0-9_-]*):\s*$")
JOB_PERMISSIONS = re.compile(r"^    permissions:(.*)$")
GRANT = re.compile(r"^\s+([a-z-]+):\s*['\"]?([a-z-]+)['\"]?\s*$")


def indent_of(line: str) -> int:
    return len(line) - len(line.lstrip(" "))


def stripped_lines(path: Path) -> list[str]:
    return [COMMENT.sub("", raw).rstrip() for raw in path.read_text().splitlines()]


def step_block(lines: list[str], i: int) -> list[str]:
    """The lines of the step whose `uses:` sits on line `i`.

    `uses:` sits at the step's key indent (`- uses:` puts it two past the
    dash). The step opens at the dash line two columns left of that indent,
    which may come before `uses:`, and ends at the next line that is less
    indented or is the next step's dash.
    """
    key = lines[i].index("uses:")
    dash = key - 2
    start = i
    while start > 0 and not lines[start][dash:].startswith("- "):
        start -= 1
    end = i + 1
    while end < len(lines):
        line = lines[end]
        if line.strip():
            if indent_of(line) < key:
                break
        end += 1
    return lines[start:end]


def uses_sites(lines: list[str]) -> list[tuple[int, str]]:
    """Every live `uses:` as (0-based line, ref)."""
    sites = []
    for i, line in enumerate(lines):
        m = USES.search(line)
        if m:
            sites.append((i, m.group(1)))
    return sites


def check_actions(lines: list[str], label: str) -> tuple[list[str], list[str], int]:
    """Rules 1 and 2 over one file: (errors, local action paths, `uses:` count)."""
    errors: list[str] = []
    local: list[str] = []
    sites = uses_sites(lines)
    for i, ref in sites:
        where = f"{label}:{i + 1}"
        if ref.startswith("./"):
            local.append(ref[2:].rstrip("/"))
            continue
        if ref.startswith("docker://"):
            if not DIGEST_PINNED.match(ref):
                errors.append(f"{where}: `{ref}` is not pinned by @sha256: digest")
            continue
        if not SHA_PINNED.match(ref):
            errors.append(
                f"{where}: `{ref}` is not pinned by a 40-hex commit SHA "
                "(write `owner/repo@<sha> # vX.Y.Z`)"
            )
        if ref.split("@", 1)[0] == "actions/checkout" and not any(
            PERSIST_FALSE.match(line) for line in step_block(lines, i)
        ):
            errors.append(f"{where}: actions/checkout does not set `persist-credentials: false`")
    return errors, local, len(sites)


def block_grants(lines: list[str], i: int, inline: str, key_indent: int) -> dict[str, str]:
    """The scopes a `permissions:` key on line `i` grants, as {scope: level}.

    `inline` is the text after the colon. `read-all`/`write-all` map to the
    pseudo-scope `*`, and `{}` grants nothing.
    """
    inline = inline.strip().strip("'\"")
    if inline in ("read-all", "write-all"):
        return {"*": inline.removesuffix("-all")}
    if inline:
        return {}
    grants: dict[str, str] = {}
    for line in lines[i + 1 :]:
        if not line.strip():
            continue
        if indent_of(line) <= key_indent:
            break
        m = GRANT.match(line)
        if m:
            grants[m.group(1)] = m.group(2)
    return grants


def jobs_of(lines: list[str]) -> dict[str, tuple[int, int]]:
    """Each job's name mapped to its [start, end) line range."""
    jobs: dict[str, tuple[int, int]] = {}
    in_jobs = False
    current: str | None = None
    start = 0
    for i, line in enumerate(lines):
        if TOP_KEY.match(line):
            if current is not None:
                jobs[current] = (start, i)
                current = None
            in_jobs = line.startswith("jobs:")
            continue
        if not in_jobs:
            continue
        m = JOB_KEY.match(line)
        if m:
            if current is not None:
                jobs[current] = (start, i)
            current, start = m.group(1), i
    if current is not None:
        jobs[current] = (start, len(lines))
    return jobs


def check_permissions(lines: list[str], label: str, allowed: dict[str, set[str]]) -> list[str]:
    """Rules 3 and 4 over one workflow."""
    errors: list[str] = []

    top = next(
        ((i, m.group(2)) for i, line in enumerate(lines) if (m := TOP_KEY.match(line))
         and m.group(1) == "permissions"),
        None,
    )
    if top is None:
        errors.append(
            f"{label}: no workflow-level `permissions:` block, so a job that names "
            "none gets the repository's default token (write `permissions: contents: read`)"
        )
    else:
        writes = sorted(s for s, level in block_grants(lines, top[0], top[1], 0).items()
                        if level == "write")
        if writes:
            errors.append(
                f"{label}:{top[0] + 1}: the workflow-level `permissions:` grants write on "
                f"{', '.join(writes)}; grant it on the job that needs it"
            )

    jobs = jobs_of(lines)
    for job, (start, end) in jobs.items():
        granted: set[str] = set()
        for i in range(start, end):
            m = JOB_PERMISSIONS.match(lines[i])
            if m:
                granted = {s for s, level in block_grants(lines, i, m.group(1), 4).items()
                           if level == "write"}
                break
        want = allowed.get(job, set())
        if granted != want:
            errors.append(
                f"{label}: job `{job}` grants write on {sorted(granted) or 'nothing'}, but "
                f"WRITE_JOBS in check_release_hardening.py allows {sorted(want) or 'nothing'}"
            )
        if "contents" in granted or "*" in granted:
            for i, ref in uses_sites(lines[start:end]):
                if not ref.startswith("actions/"):
                    errors.append(
                        f"{label}:{start + i + 1}: job `{job}` holds `contents: write` and "
                        f"runs `{ref}`; such a job runs only GitHub's own actions/*"
                    )
    for job in sorted(set(allowed) - set(jobs)):
        errors.append(
            f"{label}: WRITE_JOBS names job `{job}`, which the workflow does not have"
        )
    return errors


def check(repo_root: Path) -> tuple[list[str], int]:
    """(errors, total `uses:` sites checked) across every file in scope."""
    errors: list[str] = []
    total = 0
    seen_local: set[str] = set()
    pending: list[str] = []

    for label, allowed in WRITE_JOBS.items():
        path = repo_root / label
        if not path.is_file():
            errors.append(f"{label}: not found — WRITE_JOBS names it; move the row with the file")
            continue
        lines = stripped_lines(path)
        found, local, count = check_actions(lines, label)
        errors += found
        total += count
        if count == 0:
            errors.append(f"{label}: no `uses:` step found — this check inspected nothing")
        pending += local
        errors += check_permissions(lines, label, allowed)

    # Local composite actions run in the calling job, so their steps are held
    # to the same pinning rules. One may reference another.
    while pending:
        rel = pending.pop()
        if rel in seen_local:
            continue
        seen_local.add(rel)
        candidates = [repo_root / rel / "action.yml", repo_root / rel / "action.yaml"]
        path = next((p for p in candidates if p.is_file()), None)
        if path is None:
            errors.append(f"{rel}: referenced as a local action, but has no action.yml")
            continue
        found, local, count = check_actions(stripped_lines(path), str(path.relative_to(repo_root)))
        errors += found
        total += count
        pending += local

    return errors, total


def main() -> int:
    repo_root = Path(
        subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            check=True,
            stdout=subprocess.PIPE,
            text=True,
        ).stdout.strip()
    )
    errors, total = check(repo_root)
    if errors:
        print("error: the release workflows are not hardened:\n", file=sys.stderr)
        for e in errors:
            print(f"  {e}", file=sys.stderr)
        print(
            "\nEvery action is pinned by commit SHA, no checkout keeps its token, and\n"
            "write access goes only to the jobs WRITE_JOBS names (see the docstring of\n"
            ".github/scripts/check_release_hardening.py).",
            file=sys.stderr,
        )
        return 1
    print(f"release hardening OK ({total} action refs in {len(WRITE_JOBS)} workflows "
          "and their local actions)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
