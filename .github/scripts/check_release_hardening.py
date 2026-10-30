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
2. Every `actions/checkout` step sets `persist-credentials: false` under its
   `with:`, so no later step finds the token in the checkout's git config.
3. The workflow-level `permissions:` exists and grants no `write`, so a job
   that names no permissions of its own can only read.
4. Each job's write grants are exactly its row in WRITE_JOBS, and a job that
   holds `contents: write` uses no action but GitHub's own `actions/*`. The
   table is exact in both directions, so a grant that is dropped from the
   workflow leaves a stale row that fails here until the table moves too.

Out of scope: `run:` steps, which this check does not read, and where a SHA
comes from. Rule 1 proves a ref cannot move, not that the commit belongs to
the named repository or matches its `# vX.Y.Z` comment.

Text, not YAML: the files are read with the standard library only, line by
line with comments stripped, so a commented-out step is not a site. Where the
text takes a shape this reader does not parse — a `permissions:` value it
cannot read, a line under `jobs:` that is not a job, a `uses` key it cannot
read, a step with no dash — it is an error, never a skip. A workflow with no
`uses:` at all is an error too, so a pattern that matches nothing cannot
report a pass.

Run: .github/scripts/check-release-hardening.sh
"""

from __future__ import annotations

import re
import subprocess
import sys
from collections.abc import Iterable
from pathlib import Path

if sys.version_info < (3, 11):  # the floor every guard under .github/scripts sets
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

# A `#` at line start or after whitespace opens a YAML comment. The pattern
# also cuts ` #…` out of quoted strings and `run:` text; that is harmless here
# because no key or value this check reads carries a `#`.
COMMENT = re.compile(r"(^|\s)#.*$")
# A `uses` key, bare or quoted, with or without a space before the colon, on
# a step's dash line or on its own line.
USES = re.compile(r"""^\s*(?:-\s+)?(['"]?)uses\1\s*:\s*['"]?([^\s'"]+)""")
# Anything that looks like a `uses` key; a line that matches this but not USES
# is a shape this reader does not parse.
USES_LIKE = re.compile(r"""\buses['"]?\s*:""")
SHA_PINNED = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+(?:/[^@\s]+)?@[0-9a-f]{40}$")
DIGEST_PINNED = re.compile(r"^docker://[^@\s]+@sha256:[0-9a-f]{64}$")
# A mapping key at the start of a line's content, bare or quoted.
KEY = re.compile(r"""^(['"]?)([A-Za-z_][A-Za-z0-9_-]*)\1\s*:(.*)$""")
GRANT = re.compile(r"""^(['"]?)([a-z-]+)\1\s*:\s*['"]?([a-z-]+)['"]?\s*$""")


def indent_of(line: str) -> int:
    return len(line) - len(line.lstrip(" "))


def key_of(line: str) -> tuple[int, str, str] | None:
    """(column, key, rest) of the mapping key a line opens, past any `- `."""
    col = indent_of(line)
    body = line[col:]
    dash = re.match(r"-\s+", body)
    if dash:
        col += dash.end()
        body = body[dash.end() :]
    m = KEY.match(body)
    return (col, m.group(2), m.group(3)) if m else None


def find_key(
    lines: list[str], rows: Iterable[int], col: int, name: str
) -> tuple[int, str] | None:
    """(line, rest) of the first `name:` key in `rows` that opens its line at
    column `col`, with no dash before it."""
    for n in rows:
        found = key_of(lines[n])
        if found and found[0] == col and indent_of(lines[n]) == col and found[1] == name:
            return n, found[2]
    return None


def stripped_lines(path: Path) -> list[str]:
    return [COMMENT.sub("", raw).rstrip() for raw in path.read_text().splitlines()]


def outdented(line: str, col: int) -> bool:
    """Whether a non-blank line starts left of column `col`."""
    return bool(line.strip()) and indent_of(line) < col


def step_block(lines: list[str], i: int) -> tuple[int, list[str]] | None:
    """(key column, lines) of the step whose `uses` sits on line `i`.

    The step's keys share one column, the one `uses` sits at. The step opens
    at the nearest line at or above `i` that is less indented and starts with
    a dash, which may come before `uses`, and ends at the next line that is
    less indented than the keys. None when no dash line opens it.
    """
    found = key_of(lines[i])
    if found is None:
        return None
    key = found[0]
    start = next((n for n in range(i, -1, -1) if outdented(lines[n], key)), None)
    if start is None or not lines[start].lstrip().startswith("-"):
        return None
    end = next((n for n in range(i + 1, len(lines)) if outdented(lines[n], key)), len(lines))
    return key, lines[start:end]


def persists_false(key: int, block: list[str]) -> bool:
    """Whether the step's `with:` mapping sets `persist-credentials: false`."""
    in_with = False
    child: int | None = None
    for line in block:
        if not line.strip():
            continue
        found = key_of(line)
        if found and found[0] == key:
            in_with = found[1] == "with" and not found[2].strip()
            child = None
            continue
        if not in_with or found is None:
            continue
        if child is None:
            child = found[0]
        if found[0] == child and found[1] == "persist-credentials":
            return found[2].strip().strip("'\"") == "false"
    return False


def uses_sites(lines: list[str]) -> tuple[list[tuple[int, str]], list[int]]:
    """Every live `uses` as (0-based line, ref), plus the lines that look like
    a `uses` key in a shape USES does not read."""
    sites: list[tuple[int, str]] = []
    unread: list[int] = []
    for i, line in enumerate(lines):
        m = USES.match(line)
        if m:
            sites.append((i, m.group(2)))
        elif USES_LIKE.search(line):
            unread.append(i)
    return sites, unread


def check_actions(lines: list[str], label: str) -> tuple[list[str], list[str], int]:
    """Rules 1 and 2 over one file: (errors, local action paths, `uses` count)."""
    errors: list[str] = []
    local: list[str] = []
    sites, unread = uses_sites(lines)
    for i in unread:
        errors.append(f"{label}:{i + 1}: a `uses` key this check cannot read; write `uses: <ref>`")
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
        if ref.split("@", 1)[0].lower() == "actions/checkout":
            step = step_block(lines, i)
            if step is None:
                errors.append(f"{where}: cannot find the step this `uses` belongs to")
            elif not persists_false(*step):
                errors.append(
                    f"{where}: actions/checkout does not set `persist-credentials: false` "
                    "under `with:`"
                )
    return errors, local, len(sites)


def block_grants(
    lines: list[str], i: int, inline: str, key_indent: int
) -> dict[str, str] | None:
    """The scopes a `permissions:` key on line `i` grants, as {scope: level}.

    `inline` is the text after the colon. `read-all`/`write-all` map to the
    pseudo-scope `*`, `{}` grants nothing, and a flow mapping such as
    `{ contents: write }` is read pair by pair. None for any other inline value
    (an alias, say) or block line, which this check cannot read.
    """
    inline = inline.strip()
    unquoted = inline.strip("'\"")
    if unquoted in ("read-all", "write-all"):
        return {"*": unquoted.removesuffix("-all")}
    if inline.startswith("{") and inline.endswith("}"):
        pairs = [p.strip() for p in inline[1:-1].split(",")]
    elif inline:
        return None
    else:
        pairs = []
        for line in lines[i + 1 :]:
            if not line.strip():
                continue
            if indent_of(line) <= key_indent:
                break
            pairs.append(line.strip())
    grants: dict[str, str] = {}
    for pair in filter(None, pairs):
        m = GRANT.match(pair)
        if not m:
            return None
        grants[m.group(2)] = m.group(3)
    return grants


def jobs_of(lines: list[str]) -> tuple[dict[str, tuple[int, int]], list[str]]:
    """Each job's name mapped to its [start, end) line range, plus errors.

    A job key sits at the indent of the first line under `jobs:`, whatever
    that indent is. Any other line at that indent is an error, so a job this
    reader cannot name is never folded into its neighbour.
    """
    jobs: dict[str, tuple[int, int]] = {}
    errors: list[str] = []
    found = find_key(lines, range(len(lines)), 0, "jobs")
    if found is None:
        return jobs, ["no top-level `jobs:`"]
    top = found[0]
    end = next((n for n in range(top + 1, len(lines))
                if lines[n].strip() and indent_of(lines[n]) == 0), len(lines))
    body = [n for n in range(top + 1, end) if lines[n].strip()]
    if not body:
        return jobs, ["`jobs:` holds no job"]
    job_indent = indent_of(lines[body[0]])
    current: str | None = None
    start = top
    for n in body:
        line = lines[n]
        if indent_of(line) > job_indent:
            continue
        found = key_of(line)
        # A dash pushes the key column past the indent: a list item is not a job.
        if found is None or found[0] != job_indent or found[2].strip():
            errors.append(f"line {n + 1}: `{line.strip()}` is not a job key")
            continue
        if current is not None:
            jobs[current] = (start, n)
        current, start = found[1], n
    if current is not None:
        jobs[current] = (start, end)
    return jobs, errors


def job_permissions(lines: list[str], start: int, end: int) -> tuple[int, str] | None:
    """(line, inline value) of a job's own `permissions:` key, if it has one."""
    body = [n for n in range(start + 1, end) if lines[n].strip()]
    if not body:
        return None
    return find_key(lines, body, indent_of(lines[body[0]]), "permissions")


def check_permissions(lines: list[str], label: str, allowed: dict[str, set[str]]) -> list[str]:
    """Rules 3 and 4 over one workflow."""
    errors: list[str] = []
    unreadable = (
        "a `permissions:` value this check cannot read; write the grants as a "
        "block or a `{ scope: level }` mapping"
    )

    top = find_key(lines, range(len(lines)), 0, "permissions")
    if top is None:
        errors.append(
            f"{label}: no workflow-level `permissions:` block, so a job that names "
            "none gets the repository's default token (write `permissions: contents: read`)"
        )
    else:
        grants = block_grants(lines, top[0], top[1], 0)
        if grants is None:
            errors.append(f"{label}:{top[0] + 1}: {unreadable}")
        else:
            writes = sorted(s for s, level in grants.items() if level == "write")
            if writes:
                errors.append(
                    f"{label}:{top[0] + 1}: the workflow-level `permissions:` grants write on "
                    f"{', '.join(writes)}; grant it on the job that needs it"
                )

    jobs, job_errors = jobs_of(lines)
    errors += [f"{label}: {e}" for e in job_errors]
    for job, (start, end) in jobs.items():
        granted: set[str] = set()
        found = job_permissions(lines, start, end)
        if found is not None:
            n, inline = found
            grants = block_grants(lines, n, inline, indent_of(lines[n]))
            if grants is None:
                errors.append(f"{label}:{n + 1}: {unreadable}")
                continue
            granted = {s for s, level in grants.items() if level == "write"}
        want = allowed.get(job, set())
        if granted != want:
            errors.append(
                f"{label}: job `{job}` grants write on {sorted(granted) or 'nothing'}, but "
                f"WRITE_JOBS in check_release_hardening.py allows {sorted(want) or 'nothing'}"
            )
        if "contents" in granted or "*" in granted:
            sites, _ = uses_sites(lines[start:end])
            for i, ref in sites:
                if not ref.lower().startswith("actions/"):
                    errors.append(
                        f"{label}:{start + i + 1}: job `{job}` holds `contents: write` and "
                        f"runs `{ref}`; such a job uses no action but GitHub's own actions/*"
                    )
    for job in sorted(set(allowed) - set(jobs)):
        errors.append(
            f"{label}: WRITE_JOBS names job `{job}`, which the workflow does not have"
        )
    return errors


def check(repo_root: Path) -> tuple[list[str], int]:
    """(errors, total `uses` sites checked) across every file in scope."""
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
    # to the same pinning rules. One may reference another, or itself.
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
