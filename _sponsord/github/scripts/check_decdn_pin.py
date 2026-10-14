#!/usr/bin/env python3
"""Fail if the decdn dependencies, the [patch] and the lock disagree.

The decdn crates come from crates.io, at a caret requirement, and Cargo.lock
decides which release builds. decdn-e2e, the anvil fixture, is the exception:
decdn does not publish it, so it is a git dependency on github.com/decdn/decdn
at a release tag, used only as a dev-dependency. The decdn crates it shares
with the crates.io dependencies (decdn-client and what it pulls in) are
folded back onto crates.io by a [patch] on that repository, so that each is
built once; the ones only it uses (decdn-node, ...) stay on git:

    [workspace.dependencies]      decdn-x = "0.1"           (crates.io)
                                  decdn-e2e = { git, tag }  (dev-only)
    [patch."<decdn git>"]         decdn-x = "0.1"           (git -> crates.io)
    Cargo.lock                    the crates.io decdn version, and the decdn
                                  commit decdn-e2e and its git-only crates sit at

Checked here:

* At least one decdn dependency comes from crates.io, all at one plain caret
  requirement (`X`, `X.Y` or `X.Y.Z[-pre]`): the caret range of the version
  Cargo.lock holds. None is a leftover path into a sibling checkout, and
  members take every decdn crate from [workspace.dependencies]
  (`workspace = true`), so nothing here escapes these checks; every member
  listed has a Cargo.toml (globs are not expanded).
* A decdn git dependency is used only as a dev-dependency, and names decdn's
  `v<version>` tag for the crates.io version Cargo.lock holds: its contracts
  must match the ABI those crates were built from.
* Every [patch] entry on the decdn repository is a crates.io requirement equal
  to the one above. Cargo.lock locks every crates.io decdn dependency from
  crates.io, all at one version; no decdn crate from both git and crates.io
  (a [patch] entry is missing) or from a path (a local override); every git
  decdn crate at one commit, which a [workspace.dependencies] git alias
  names; and records no unused [patch] entry.

Run: .github/scripts/check-decdn-pin.sh
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

if sys.version_info < (3, 11):  # tomllib, and the syntax used below
    sys.exit(f"error: python 3.11+ required, found {sys.version.split()[0]}")

import tomllib  # noqa: E402  (must follow the version guard)

DECDN_GIT = "https://github.com/decdn/decdn"
CRATES_IO = "registry+https://github.com/rust-lang/crates.io-index"
EXACT_VERSION = re.compile(r"^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$")
# A bare caret requirement, as cargo reads `"0.1"`: no operator, no wildcard.
CARET = re.compile(r"^\d+(?:\.\d+(?:\.\d+(?:-[0-9A-Za-z.-]+)?)?)?$")
LOCK_SOURCE = re.compile(r"^git\+(?P<url>[^?#]+)(?:\?[^#]*)?#(?P<sha>[0-9a-f]{40})$")
DEP_TABLES = ("dependencies", "build-dependencies", "dev-dependencies")


def is_decdn_git(url: object) -> bool:
    return str(url).rstrip("/").removesuffix(".git") == DECDN_GIT


def caret(version: str) -> str:
    """The caret requirement that takes <version> and its compatible releases.

    `1.4.2` -> `1`, `0.3.1` -> `0.3`, `0.0.7` -> `0.0.7`. A prerelease is kept
    whole, because cargo matches a prerelease only when the requirement names
    one.
    """
    if "-" in version:
        return version
    major, minor, _ = version.split(".")
    if major != "0":
        return major
    if minor != "0":
        return f"0.{minor}"
    return version


def requirement(spec: object) -> str | None:
    """The crates.io requirement of a dependency spec, or None for git/path."""
    if isinstance(spec, str):
        return spec
    if isinstance(spec, dict) and not {"git", "path", "registry"} & spec.keys():
        version = spec.get("version")
        return version if isinstance(version, str) else None
    return None


def decdn_deps(manifest: dict) -> tuple[dict[str, str], dict[str, dict], list[str]]:
    """(crates.io requirements, git specs, errors) of the decdn workspace deps."""
    deps = manifest.get("workspace", {}).get("dependencies", {})
    registry: dict[str, str] = {}
    git: dict[str, dict] = {}
    errors: list[str] = []
    for name, spec in deps.items():
        package = str(spec.get("package", name)) if isinstance(spec, dict) else name
        if package != name and (package.startswith("decdn-") or name.startswith("decdn-")):
            # Classified by key below, a renamed alias would escape every check.
            errors.append(
                f"Cargo.toml: {name} renames {package} — name decdn crates by their own "
                "name in [workspace.dependencies]"
            )
        elif isinstance(spec, dict) and is_decdn_git(spec.get("git", "")):
            git[name] = spec
        elif isinstance(spec, dict) and str(spec.get("path", "")).startswith("../decdn/"):
            errors.append(
                f"Cargo.toml: {name} is a path into a sibling decdn checkout — the decdn "
                "crates come from crates.io (CONTRIBUTING.md shows the local [patch] "
                "override instead)"
            )
        elif name.startswith("decdn-"):
            req = requirement(spec)
            if req is None:
                errors.append(f"Cargo.toml: {name} is neither a crates.io nor a {DECDN_GIT} dependency")
            else:
                registry[name] = req
    return registry, git, errors


def decdn_patch(manifest: dict) -> dict:
    """The [patch] table on the decdn repository, empty when there is none."""
    for url, table in manifest.get("patch", {}).items():
        if is_decdn_git(url):
            return table
    return {}


def member_uses(repo_root: Path, manifest: dict) -> dict[str, set[str]]:
    """For each dependency name, the dependency tables members use it in."""
    uses: dict[str, set[str]] = {}
    for rel in manifest.get("workspace", {}).get("members", []):
        path = repo_root / rel / "Cargo.toml"
        if not path.is_file():
            continue
        member = tomllib.loads(path.read_text())
        tables = [(t, member.get(t, {})) for t in DEP_TABLES]
        for target in member.get("target", {}).values():
            tables.extend((t, target.get(t, {})) for t in DEP_TABLES)
        for table, deps in tables:
            for dep in deps:
                uses.setdefault(dep, set()).add(table)
    return uses


def member_errors(repo_root: Path, manifest: dict) -> list[str]:
    """Members that are missing, or declare a decdn crate themselves.

    A member's own `decdn-x = { git = … }` or `{ path = … }` would escape
    every check on [workspace.dependencies]; so would a member path that does
    not resolve (a glob), which member_uses() cannot read.
    """
    errors: list[str] = []
    for rel in manifest.get("workspace", {}).get("members", []):
        path = repo_root / rel / "Cargo.toml"
        if not path.is_file():
            errors.append(
                f"Cargo.toml: workspace member {rel!r} has no Cargo.toml — list members "
                "one by one (this check does not expand globs)"
            )
            continue
        member = tomllib.loads(path.read_text())
        tables = [member.get(t, {}) for t in DEP_TABLES]
        for target in member.get("target", {}).values():
            tables.extend(target.get(t, {}) for t in DEP_TABLES)
        for deps in tables:
            for key, spec in deps.items():
                package = spec.get("package", key) if isinstance(spec, dict) else key
                inherited = isinstance(spec, dict) and spec.get("workspace") is True
                if str(package).startswith("decdn-") and not inherited:
                    errors.append(
                        f"{rel}/Cargo.toml: {key} declares decdn's {package} itself — take "
                        "it from [workspace.dependencies] (`workspace = true`)"
                    )
    return errors


def locked_decdn(lock: dict) -> tuple[dict[str, set[str]], dict[str, set[str]]]:
    """({name: versions} from crates.io, {name: commits} from the decdn repo).

    Sets, because a lock may hold one name more than once (0.1.x beside
    0.2.x, or two commits), and a check must see every entry.
    """
    registry: dict[str, set[str]] = {}
    git: dict[str, set[str]] = {}
    for package in lock.get("package", []):
        name = str(package.get("name", ""))
        source = str(package.get("source", ""))
        if source == CRATES_IO and name.startswith("decdn-"):
            registry.setdefault(name, set()).add(str(package.get("version", "")))
        elif (match := LOCK_SOURCE.match(source)) and is_decdn_git(match["url"]):
            git.setdefault(name, set()).add(match["sha"])
    return registry, git


def flatten(locked: dict[str, set[str]]) -> set[str]:
    """Every version (or commit) in a locked_decdn() map."""
    return set().union(*locked.values())


def check(repo_root: Path) -> tuple[list[str], list[str]]:
    """Returns (errors, notes)."""
    notes: list[str] = []
    manifest = tomllib.loads((repo_root / "Cargo.toml").read_text())
    registry, git, errors = decdn_deps(manifest)
    if not registry:
        errors.append(
            "Cargo.toml: no [workspace.dependencies] decdn-* entry comes from crates.io "
            "— this check inspected nothing"
        )
        return errors, notes

    reqs = set(registry.values())
    if len(reqs) > 1:
        errors.append(
            "Cargo.toml: the decdn dependencies disagree on the requirement: "
            + ", ".join(f"{n} = {v!r}" for n, v in sorted(registry.items()))
            + " — decdn has one workspace version"
        )
    for name, req in sorted(registry.items()):
        if not CARET.match(req):
            errors.append(
                f"Cargo.toml: {name} requires {req!r}, not a plain caret requirement "
                "like \"0.1\" — published crates take every compatible decdn release"
            )
    req = min(reqs)

    errors += member_errors(repo_root, manifest)
    uses = member_uses(repo_root, manifest)
    for name, spec in sorted(git.items()):
        kept = uses.get(name, set()) - {"dev-dependencies"}
        if kept:
            errors.append(
                f"Cargo.toml: {name} is a git dependency used in [{', '.join(sorted(kept))}] "
                "— only a dev-dependency may come from git; take it from crates.io"
            )
        if "tag" not in spec:
            errors.append(f"Cargo.toml: {name} must name a decdn release `tag`")

    patch = decdn_patch(manifest)
    for name, spec in sorted(patch.items()):
        if requirement(spec) != req:
            errors.append(
                f"Cargo.toml: [patch.\"{DECDN_GIT}\"] {name} = {spec!r} must be the "
                f"crates.io requirement {req!r}"
            )

    lock_path = repo_root / "Cargo.lock"
    if not lock_path.is_file():
        errors.append("Cargo.lock is missing — it is what pins the decdn release")
        return errors, notes
    lock = tomllib.loads(lock_path.read_text())
    locked, commits = locked_decdn(lock)

    versions = flatten(locked)
    if len(versions) > 1:
        errors.append(
            "Cargo.lock: the crates.io decdn crates are at different versions: "
            + ", ".join(f"{n} {v}" for n, vs in sorted(locked.items()) for v in sorted(vs))
            + " — `cargo update` them together"
        )
    for name in sorted(registry):
        if name not in locked:
            errors.append(f"Cargo.lock: {name} is not locked from crates.io")
    for name in sorted(locked.keys() & commits.keys()):
        errors.append(
            f"Cargo.lock: {name} is locked from both crates.io and {DECDN_GIT}, so it "
            f"builds twice — add it to [patch.\"{DECDN_GIT}\"]"
        )
    for package in lock.get("package", []):
        name = str(package.get("name", ""))
        if name.startswith("decdn-") and "source" not in package:
            errors.append(
                f"Cargo.lock: {name} is locked from a local path — a `[patch]` override "
                "(CONTRIBUTING.md) leaked into the lock; `git checkout -- Cargo.lock`"
            )
    shas = flatten(commits)
    if shas and not git:
        errors.append(
            f"Cargo.lock: decdn git crates ({', '.join(sorted(commits))}) but no "
            "[workspace.dependencies] git alias names their tag"
        )
    if len(shas) > 1:
        errors.append(
            f"Cargo.lock: decdn is locked at {len(shas)} commits "
            f"({', '.join(sorted(shas))}) — move every git alias to one tag"
        )
    unused = [p for p in lock.get("patch", {}).get("unused", []) if str(p.get("name", "")).startswith("decdn-")]
    for package in unused:
        errors.append(
            f"Cargo.lock: [patch] entry {package.get('name')} {package.get('version')} is "
            "unused — a decdn git crate requires a version it does not match, or nothing "
            "needs it"
        )

    if len(versions) == 1:
        version = next(iter(versions))
        if len(reqs) == 1 and req != caret(version):
            errors.append(
                f"Cargo.toml: the decdn crates require {req!r} but Cargo.lock holds "
                f"{version} — set every decdn-* requirement, and the [patch], to "
                f"{caret(version)!r} (`bump-decdn.sh rewrite v{version}`)"
            )
        for name, spec in sorted(git.items()):
            if "tag" in spec and spec["tag"] != f"v{version}":
                errors.append(
                    f"Cargo.toml: {name} is at tag {spec['tag']} but Cargo.lock holds decdn "
                    f"{version} from crates.io — set its tag to v{version}, so its contracts "
                    "match the crates' ABI"
                )
        if not errors:
            notes.append(f"crates.io {version} at {req!r}")
            if commits:
                tags = sorted({spec["tag"] for spec in git.values()})
                notes.append(
                    f"{', '.join(sorted(git))} at {', '.join(tags)}, "
                    f"decdn@{next(iter(shas))}"
                )
    return errors, notes


def main(argv: list[str] | None = None) -> int:
    del argv
    repo_root = Path(
        subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            check=True,
            stdout=subprocess.PIPE,
            text=True,
        ).stdout.strip()
    )
    errors, notes = check(repo_root)
    if errors:
        print("error: the decdn pin is inconsistent:\n", file=sys.stderr)
        for e in errors:
            print(f"  {e}", file=sys.stderr)
        print(
            "\nThe decdn-* entries in Cargo.toml's [workspace.dependencies] and\n"
            "[patch], and the decdn crates in Cargo.lock, must agree. See RELEASING.md.",
            file=sys.stderr,
        )
        return 1
    print(f"decdn pin OK ({'; '.join(notes)})")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
