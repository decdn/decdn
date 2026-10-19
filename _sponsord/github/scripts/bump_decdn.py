#!/usr/bin/env python3
"""Move the decdn pin to a decdn release, for open-decdn-bump.sh.

The decdn crates come from crates.io at a caret requirement, and decdn-e2e
from github.com/decdn/decdn at the matching release `tag`
(RELEASING.md § Pinning decdn). A bump PR changes three things: the decdn-e2e
`tag` in the root Cargo.toml (and the requirements, when the release leaves
their caret range), the decdn packages in Cargo.lock (`cargo update`), and a
changelog entry in every published crate the bump reaches.
open-decdn-bump.sh runs `cargo update` and opens the PR; this script does the
rest, one subcommand per step:

    latest            decdn's latest published release tag
    pinned            v<the crates.io decdn version Cargo.lock holds> (empty
                      when it holds none, or several)
    compare <tag>     where the pin stands against <tag>, by SemVer
                      precedence: same, behind, ahead, or untagged when
                      `pinned` is empty
    ready <tag>       exit 0 when the crates.io index cargo reads lists every
                      decdn crate the workspace takes from it at <tag>, 3
                      while one is missing (index lag), 1 when it cannot
                      answer
    rewrite <tag> [--exact]
                      point decdn-e2e at <tag>, and the crates.io
                      requirements at its caret range; with --exact, at
                      `=<version>` instead, so that `cargo update` locks
                      exactly that release (the bump then rewrites again
                      without it)
    changelog <tag>   add the bump entry under [Unreleased], naming the
                      commit Cargo.lock pins decdn-e2e at
    locked-sha        the decdn commit Cargo.lock pins decdn-e2e at
    locked-crates     every decdn crate in Cargo.lock, one per line, for
                      `cargo update -p`
    canary <version>  move the manifest onto decdn's `main` branch, whose
                      workspace version is <version> (ci.yml's decdn main
                      canary): every decdn crate from git `main`, through a
                      [patch.crates-io]
    canary-check      after `canary` and `cargo update`: fail unless every
                      decdn crate in Cargo.lock comes from git `main`

Run: .github/scripts/bump-decdn.sh <subcommand> [args]
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import textwrap
import urllib.error
import urllib.request
from pathlib import Path

if sys.version_info < (3, 11):  # tomllib, and the syntax used below
    sys.exit(f"error: python 3.11+ required, found {sys.version.split()[0]}")

import tomllib  # noqa: E402  (must follow the version guard)

sys.path.insert(0, str(Path(__file__).resolve().parent))
from check_decdn_pin import (  # noqa: E402
    DECDN_GIT,
    DEP_TABLES,
    EXACT_VERSION,
    caret,
    decdn_deps,
    decdn_patch,
    flatten,
    is_decdn_git,
    locked_decdn,
    requirement,
)

RELEASES_API = "https://api.github.com/repos/decdn/decdn/releases/latest"
# The sparse index cargo itself resolves from: the crates.io API can serve a
# release before it does.
CRATES_INDEX = "https://index.crates.io"
USER_AGENT = "decdn-sponsord-bump-decdn (https://github.com/decdn/sponsord)"
NOT_READY = 3

# The lines `rewrite` and `canary` edit, in [workspace.dependencies] and the
# [patch] on the decdn repository: a git alias, a bare requirement, and an
# inline table without a git or path source.
SECTION = re.compile(r"^\[(?P<name>[^\]]+)\]")
GIT_LINE = re.compile(r'^(decdn-[a-z0-9-]+) = \{ git = "' + re.escape(DECDN_GIT) + r'"')
REQ_LINE = re.compile(r'^(?P<head>decdn-[a-z0-9-]+ = ")[^"]*(?P<tail>")')
TABLE_LINE = re.compile(r"^decdn-[a-z0-9-]+ = \{")
REF_FIELD = re.compile(r'\b(?:tag|rev|branch) = "[^"]*"')
VERSION_FIELD = re.compile(r'\bversion = "[^"]*"')


class BumpError(Exception):
    """A precondition the bump cannot continue past."""


def fetch(url: str) -> tuple[int, bytes]:
    """(HTTP status, body) for a GET. Raises OSError on a transport failure."""
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    # Optional: only lifts api.github.com's anonymous rate limit.
    token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
    if token and url.startswith("https://api.github.com/"):
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


def version_of(tag: str) -> str:
    """`X.Y.Z[-pre]` from `vX.Y.Z[-pre]`, or BumpError."""
    version = tag.removeprefix("v")
    if not tag.startswith("v") or not EXACT_VERSION.match(version):
        raise BumpError(f"{tag!r} is not a decdn release tag (vX.Y.Z)")
    return version


def manifest(repo_root: Path) -> dict:
    return tomllib.loads((repo_root / "Cargo.toml").read_text())


def lock(repo_root: Path) -> dict:
    return tomllib.loads((repo_root / "Cargo.lock").read_text())


def pinned_tag(repo_root: Path) -> str | None:
    """v<the crates.io decdn version Cargo.lock holds>, or None for none or several."""
    versions = flatten(locked_decdn(lock(repo_root))[0])
    if len(versions) == 1:
        return f"v{next(iter(versions))}"
    return None


def compare(repo_root: Path, tag: str) -> str:
    """Where the pin stands against <tag>: same, behind, ahead or untagged."""
    target = semver_key(version_of(tag))
    pinned = pinned_tag(repo_root)
    if pinned is None:
        return "untagged"
    current = semver_key(version_of(pinned))
    if current == target:
        return "same"
    return "behind" if current < target else "ahead"


def semver_key(version: str) -> tuple:
    """A sort key in SemVer precedence: a release ranks above its prereleases."""
    core, _, pre = version.partition("-")
    numbers = tuple(int(n) for n in core.split("."))
    if not pre:
        return (numbers, (1,))
    # Numeric identifiers compare as numbers and rank below alphanumeric ones;
    # a shorter run of equal identifiers ranks lower.
    ids = tuple((0, int(i), "") if i.isdigit() else (1, 0, i) for i in pre.split("."))
    return (numbers, (0, ids))


def latest() -> str:
    """decdn's latest published release tag."""
    status, body = fetch(RELEASES_API)
    if status != 200:
        raise BumpError(f"GET {RELEASES_API}: HTTP {status}")
    release = json.loads(body)
    # /releases/latest never returns a draft or a prerelease; checked anyway,
    # because a draft is not on crates.io, and a prerelease would make the
    # caret requirement a prerelease pin.
    if release.get("draft") or release.get("prerelease"):
        raise BumpError(f"{RELEASES_API} returned a draft or prerelease")
    tag = str(release.get("tag_name", ""))
    version_of(tag)
    return tag


def index_url(name: str) -> str:
    """The sparse-index file of crate <name> (crates.io's directory layout)."""
    if len(name) <= 2:
        return f"{CRATES_INDEX}/{len(name)}/{name}"
    if len(name) == 3:
        return f"{CRATES_INDEX}/3/{name[0]}/{name}"
    return f"{CRATES_INDEX}/{name[:2]}/{name[2:4]}/{name}"


def ready(repo_root: Path, tag: str) -> list[str]:
    """The decdn crates the crates.io index does not list at <tag>; empty when ready.

    Those are the crates.io decdn dependencies and the [patch] entries. The
    index is what `cargo update` reads, so once this is empty, a failing
    update is a real failure, not lag.

    Only a missing version (or crate, a 404) counts as missing: that is index
    lag, which a wait outlasts. Any other answer, or none, is a BumpError, so
    that a permanent failure is not reported as "try again later".
    """
    version = version_of(tag)
    root = manifest(repo_root)
    registry, _, _ = decdn_deps(root)
    if not registry:
        raise BumpError("Cargo.toml takes no decdn crate from crates.io")
    missing = []
    for name in sorted(registry.keys() | decdn_patch(root).keys()):
        url = index_url(name)
        try:
            status, body = fetch(url)
        except OSError as e:
            raise BumpError(f"{name} {version}: crates.io index unreachable: {e}") from None
        if status == 404:
            missing.append(f"{name} {version}")
            continue
        if status != 200:
            raise BumpError(f"{name} {version}: GET {url}: HTTP {status}")
        try:
            listed = {json.loads(line)["vers"] for line in body.decode().splitlines() if line.strip()}
        except (ValueError, KeyError, TypeError) as e:
            raise BumpError(f"{name}: GET {url}: not an index file: {e}") from None
        if version not in listed:
            missing.append(f"{name} {version}")
    return missing


def in_decdn_section(header: str) -> bool:
    """Whether a table header is [workspace.dependencies] or the decdn [patch]."""
    if header == "workspace.dependencies":
        return True
    url = header.removeprefix("patch.").strip('"')
    return header.startswith("patch.") and is_decdn_git(url)


def edit_lines(repo_root: Path, edit) -> None:
    """Rewrite Cargo.toml line by line: edit(header, line) -> new line(s)."""
    path = repo_root / "Cargo.toml"
    header = ""
    out = []
    for line in path.read_text().splitlines(keepends=True):
        if section := SECTION.match(line):
            header = section["name"].strip()
        out.append(edit(header, line))
    path.write_text("".join(out))


def set_requirement(line: str, req: str) -> str:
    """<line> with its crates.io requirement set to <req>; others unchanged."""
    if match := REQ_LINE.match(line):
        return f"{match['head']}{req}{match['tail']}{line[match.end():]}"
    if TABLE_LINE.match(line) and "git =" not in line and "path =" not in line:
        return VERSION_FIELD.sub(f'version = "{req}"', line)
    return line


def rewrite(repo_root: Path, tag: str, *, exact: bool = False) -> list[str]:
    """Point decdn-e2e at <tag> and the requirements at its caret range.

    Returns the names of the git aliases moved. The caret requirements change
    only when <tag> leaves their range (0.1 -> 0.2). With <exact>, they become
    `=<version>`: cargo allows one release per caret range and source, so
    `cargo update` then moves every decdn crate to exactly <version>, not to
    crates.io's newest; the bump rewrites again without <exact> afterwards,
    and the lock still satisfies the caret requirements.
    """
    version = version_of(tag)
    req = f"={version}" if exact else caret(version)
    moved: list[str] = []

    def edit(header: str, line: str) -> str:
        if not in_decdn_section(header):
            return line
        if (alias := GIT_LINE.match(line)) and header == "workspace.dependencies":
            line, refs = REF_FIELD.subn(f'tag = "{tag}"', line)
            if refs != 1:
                raise BumpError(f"Cargo.toml: {alias[1]} names {refs} git refs, expected one")
            moved.append(alias[1])
            return VERSION_FIELD.sub(f'version = "{version}"', line)
        return set_requirement(line, req)

    edit_lines(repo_root, edit)

    # The line match is a convenience; the parsed manifest is the proof.
    root = manifest(repo_root)
    registry, git, _ = decdn_deps(root)
    if not registry:
        raise BumpError("Cargo.toml takes no decdn crate from crates.io")
    stale = sorted(
        [n for n, r in registry.items() if r != req]
        + [f"[patch] {n}" for n, spec in decdn_patch(root).items() if requirement(spec) != req]
        + [
            n
            for n, spec in git.items()
            if spec.get("tag") != tag
            or "branch" in spec
            or "rev" in spec
            or spec.get("version", version) != version
        ]
    )
    if stale:
        raise BumpError(
            f"Cargo.toml: {', '.join(stale)} not rewritten to {tag} — each decdn entry "
            'must sit on one line, as `decdn-x = "0.1"` or '
            '`decdn-x = { git = "…", tag = "…", … }`'
        )
    return moved


def canary(repo_root: Path, version: str) -> list[str]:
    """Move every decdn crate onto decdn's `main`, at workspace version <version>.

    decdn-e2e moves to `branch = "main"`; the [patch] that folds its decdn
    dependencies onto crates.io gives way to a [patch.crates-io] that takes
    every decdn crate from `main` instead; the requirements move to the caret
    range of <version>. Returns the crates the [patch.crates-io] names.
    """
    if not EXACT_VERSION.match(version):
        raise BumpError(f"{version!r} is not a version (X.Y.Z)")
    root = manifest(repo_root)
    if "crates-io" in root.get("patch", {}):
        raise BumpError("Cargo.toml already has a [patch.crates-io]")
    registry, git, _ = decdn_deps(root)
    if not registry:
        raise BumpError("Cargo.toml takes no decdn crate from crates.io")
    names = sorted(registry.keys() | decdn_patch(root).keys())
    req = caret(version)

    def edit(header: str, line: str) -> str:
        if header.startswith("patch.") and in_decdn_section(header):
            return ""
        if header != "workspace.dependencies":
            return line
        if GIT_LINE.match(line):
            line = REF_FIELD.sub('branch = "main"', line)
            return VERSION_FIELD.sub(f'version = "{version}"', line)
        return set_requirement(line, req)

    edit_lines(repo_root, edit)
    path = repo_root / "Cargo.toml"
    text = path.read_text().rstrip("\n") + "\n\n[patch.crates-io]\n"
    text += "".join(f'{n} = {{ git = "{DECDN_GIT}", branch = "main" }}\n' for n in names)
    path.write_text(text)

    root = manifest(repo_root)
    registry, git, _ = decdn_deps(root)
    patched = root.get("patch", {}).get("crates-io", {})
    if (
        decdn_patch(root)
        or any(r != req for r in registry.values())
        or any(
            spec.get("branch") != "main" or spec.get("version", version) != version
            for spec in git.values()
        )
        or sorted(patched) != names
    ):
        raise BumpError(f"Cargo.toml: could not move every decdn crate to main ({version})")
    return names


def canary_check(repo_root: Path) -> list[str]:
    """What in Cargo.lock still does not come from decdn `main`; empty when all does.

    A [patch.crates-io] entry whose version does not match main's leaves its
    crate on crates.io, and cargo only warns: the canary would test the
    release, not main, and stay green.
    """
    data = lock(repo_root)
    registry, git = locked_decdn(data)
    problems = [f"{n} {v} from crates.io" for n, vs in sorted(registry.items()) for v in sorted(vs)]
    problems += [
        f"[patch] {p.get('name')} {p.get('version')} unused"
        for p in data.get("patch", {}).get("unused", [])
        if str(p.get("name", "")).startswith("decdn-")
    ]
    for package in data.get("package", []):
        source = str(package.get("source", ""))
        if package.get("name") in git and "branch=main#" not in source:
            problems.append(f"{package['name']} from {source}")
    if not git:
        problems.append("no decdn crate from git main")
    return problems


def locked_sha(repo_root: Path) -> str:
    """The one decdn commit Cargo.lock pins decdn-e2e and its git crates at."""
    commits = flatten(locked_decdn(lock(repo_root))[1])
    if len(commits) != 1:
        raise BumpError(f"Cargo.lock pins decdn at {len(commits)} commits, expected one")
    return next(iter(commits))


def locked_crates(repo_root: Path) -> list[str]:
    """Every decdn crate Cargo.lock holds, from crates.io or git."""
    registry, git = locked_decdn(lock(repo_root))
    return sorted(registry.keys() | git.keys())


def reached_crates(repo_root: Path) -> list[Path]:
    """Directories of the published members that depend on decdn outside dev-deps."""
    root = manifest(repo_root)
    registry, git, _ = decdn_deps(root)
    decdn = registry.keys() | git.keys()
    kept_tables = [t for t in DEP_TABLES if t != "dev-dependencies"]
    reached = []
    for rel in root.get("workspace", {}).get("members", []):
        member = tomllib.loads((repo_root / rel / "Cargo.toml").read_text())
        if member.get("package", {}).get("publish", True) in (False, []):
            continue
        tables = [member.get(t, {}) for t in kept_tables]
        for target in member.get("target", {}).values():
            tables.extend(target.get(t, {}) for t in kept_tables)
        if any(name in decdn for deps in tables for name in deps):
            reached.append(repo_root / rel)
    return reached


def entry(tag: str, sha: str) -> str:
    version = version_of(tag)
    text = (
        f"**decdn {tag}.** The decdn crates are locked at `{version}` from crates.io "
        f"and require `{caret(version)}`, and the anvil fixtures are at decdn's "
        f"`{tag}` release tag (`{sha}`)."
    )
    return textwrap.fill(
        text,
        width=80,
        initial_indent="- ",
        subsequent_indent="  ",
        break_long_words=False,
        break_on_hyphens=False,
    )


ENTRY_PREFIX = "- **decdn v"

# RELEASING.md § Changelogs: the sections after Changed, in their order.
AFTER_CHANGED = ("### Fixed", "### Removed", "### Security")


def add_entry(changelog: str, tag: str, sha: str) -> str:
    """<changelog> with the bump entry first under [Unreleased] → ### Changed.

    An unreleased entry for an earlier bump is replaced, not kept: only the
    last lock before a release is the one it ships.
    """
    lines = changelog.splitlines()
    try:
        start = lines.index("## [Unreleased]")
    except ValueError:
        raise BumpError("no `## [Unreleased]` heading") from None

    def unreleased_end() -> int:
        return next(
            (i for i in range(start + 1, len(lines)) if lines[i].startswith("## ")),
            len(lines),
        )

    # Drop an earlier bump's bullet and its continuation lines, and the blank
    # line after it when one precedes it too.
    old = next(
        (i for i in range(start + 1, unreleased_end()) if lines[i].startswith(ENTRY_PREFIX)),
        None,
    )
    if old is not None:
        stop = old + 1
        while stop < len(lines) and lines[stop].startswith("  "):
            stop += 1
        if lines[old - 1] == "" and stop < len(lines) and lines[stop] == "":
            stop += 1
        del lines[old:stop]

    end = unreleased_end()
    bullet = entry(tag, sha).splitlines()
    changed = next((i for i in range(start + 1, end) if lines[i] == "### Changed"), None)
    if changed is not None:
        insert_at = changed + 2
        if insert_at > len(lines) or lines[insert_at - 1] != "":
            raise BumpError(f"expected a blank line after line {insert_at - 1}")
        # Joins the list a bullet already opens, without a blank line that
        # would split it.
        joins = insert_at < len(lines) and lines[insert_at].startswith("- ")
        block = bullet if joins else [*bullet, ""]
    else:
        insert_at = next(
            (i for i in range(start + 1, end) if lines[i] in AFTER_CHANGED),
            end,
        )
        block = ["### Changed", "", *bullet, ""]
        if lines[insert_at - 1] != "":
            block.insert(0, "")
    lines[insert_at:insert_at] = block
    return "\n".join(lines).rstrip("\n") + "\n"


def changelog(repo_root: Path, tag: str) -> list[Path]:
    """Add the bump entry to every reached crate's CHANGELOG.md."""
    sha = locked_sha(repo_root)
    written = []
    for crate in reached_crates(repo_root):
        path = crate / "CHANGELOG.md"
        if not path.is_file():
            raise BumpError(f"{path.relative_to(repo_root)} is missing")
        try:
            path.write_text(add_entry(path.read_text(), tag, sha))
        except BumpError as e:
            raise BumpError(f"{path.relative_to(repo_root)}: {e}") from None
        written.append(path)
    return written


def main(argv: list[str]) -> int:
    repo_root = Path(
        subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            check=True,
            stdout=subprocess.PIPE,
            text=True,
        ).stdout.strip()
    )
    command, args = (argv[0], argv[1:]) if argv else ("", [])
    try:
        if command == "latest" and not args:
            print(latest())
        elif command == "compare" and len(args) == 1:
            print(compare(repo_root, args[0]))
        elif command == "ready" and len(args) == 1:
            missing = ready(repo_root, args[0])
            if missing:
                print(f"the crates.io index does not list {args[0]} yet:", file=sys.stderr)
                for m in missing:
                    print(f"  {m}", file=sys.stderr)
                return NOT_READY
            print(f"the crates.io index lists every decdn crate at {args[0]}")
        elif command == "rewrite" and args and args[1:] in ([], ["--exact"]):
            exact = args[1:] == ["--exact"]
            names = rewrite(repo_root, args[0], exact=exact)
            req = f"={version_of(args[0])}" if exact else caret(version_of(args[0]))
            print(f"pinned the decdn crates to {req}, {', '.join(names) or 'no git alias'} to {args[0]}")
        elif command == "changelog" and len(args) == 1:
            for path in changelog(repo_root, args[0]):
                print(f"updated {path.relative_to(repo_root)}")
        elif command == "pinned" and not args:
            print(pinned_tag(repo_root) or "")
        elif command == "locked-sha" and not args:
            print(locked_sha(repo_root))
        elif command == "locked-crates" and not args:
            print("\n".join(locked_crates(repo_root)))
        elif command == "canary" and len(args) == 1:
            names = canary(repo_root, args[0])
            print(f"took {', '.join(names)} from decdn main ({args[0]})")
        elif command == "canary-check" and not args:
            problems = canary_check(repo_root)
            if problems:
                print("the lock is not on decdn main:", file=sys.stderr)
                for p in problems:
                    print(f"  {p}", file=sys.stderr)
                return 1
            print("every decdn crate comes from decdn main")
        else:
            print(__doc__, file=sys.stderr)
            return 2
    except BumpError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
