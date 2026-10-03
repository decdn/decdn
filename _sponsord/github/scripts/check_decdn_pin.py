#!/usr/bin/env python3
"""Fail if the decdn dependencies, their versions and the lock disagree.

The decdn crates are git dependencies on github.com/decdn/decdn, and
Cargo.lock pins the one commit every build compiles. Each decdn alias in the
root `[workspace.dependencies]` that a published crate keeps also carries a
`version`, because a published manifest drops the `git` source: crates.io
resolves the version instead. So:

    [workspace.dependencies]      the source, and the version the published
      decdn-* git + version         crates require
    Cargo.lock                    the decdn commit, and the version cargo
      decdn-* source + version      found at it

Checked here:

* There is at least one decdn alias, every one names the same git source
  (repository and branch), and none is a leftover path into a sibling
  checkout.
* Every alias with a `version` carries an exact `X.Y.Z[-pre]`, and all carry
  the same one. An alias without one is used only as a dev-dependency (cargo
  strips those from the published manifest).
* Cargo.lock locks every decdn package at one commit of that repository, and
  each versioned alias at exactly its version.

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
EXACT_VERSION = re.compile(r"^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$")
LOCK_SOURCE = re.compile(r"^git\+(?P<url>[^?#]+)(?:\?[^#]*)?#(?P<sha>[0-9a-f]{40})$")
DEP_TABLES = ("dependencies", "build-dependencies", "dev-dependencies")


def decdn_aliases(manifest: dict) -> tuple[dict[str, dict], list[str]]:
    """(aliases, errors): the git aliases on decdn, and stale path aliases."""
    deps = manifest.get("workspace", {}).get("dependencies", {})
    aliases: dict[str, dict] = {}
    errors: list[str] = []
    for name, spec in deps.items():
        if not isinstance(spec, dict):
            continue
        if str(spec.get("git", "")).rstrip("/").removesuffix(".git") == DECDN_GIT:
            aliases[name] = spec
        elif str(spec.get("path", "")).startswith("../decdn/"):
            errors.append(
                f"Cargo.toml: {name} is a path into a sibling decdn checkout — the decdn "
                f"crates are git dependencies on {DECDN_GIT} (CONTRIBUTING.md shows the "
                "local [patch] override instead)"
            )
    return aliases, errors


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


def check(repo_root: Path) -> tuple[list[str], list[str]]:
    """Returns (errors, notes)."""
    notes: list[str] = []
    manifest = tomllib.loads((repo_root / "Cargo.toml").read_text())
    aliases, errors = decdn_aliases(manifest)
    if not aliases:
        errors.append(
            f"Cargo.toml: no [workspace.dependencies] entry is a git dependency on "
            f"{DECDN_GIT} — this check inspected nothing"
        )
        return errors, notes

    sources = {(spec.get("branch"), spec.get("tag"), spec.get("rev")) for spec in aliases.values()}
    if len(sources) > 1:
        errors.append(
            "Cargo.toml: the decdn aliases name different refs of "
            f"{DECDN_GIT} — they must resolve to one commit"
        )

    uses = member_uses(repo_root, manifest)
    versions: dict[str, str] = {}
    for name, spec in sorted(aliases.items()):
        version = spec.get("version")
        if version is None:
            kept = uses.get(name, set()) - {"dev-dependencies"}
            if kept:
                errors.append(
                    f"Cargo.toml: {name} has no `version` but a member uses it in "
                    f"[{', '.join(sorted(kept))}] — the published manifest needs one"
                )
        elif not isinstance(version, str) or not EXACT_VERSION.match(version):
            errors.append(
                f"Cargo.toml: {name} version {version!r} is not an exact X.Y.Z — it must "
                "name the decdn release the lock builds against"
            )
        else:
            versions[name] = version
    if len(set(versions.values())) > 1:
        errors.append(
            "Cargo.toml: the decdn aliases disagree on the version: "
            + ", ".join(f"{n} = {v}" for n, v in sorted(versions.items()))
            + " — decdn has one workspace version"
        )

    lock_path = repo_root / "Cargo.lock"
    if not lock_path.is_file():
        errors.append("Cargo.lock is missing — it is what pins the decdn commit")
        return errors, notes
    lock = tomllib.loads(lock_path.read_text())
    commits: set[str] = set()
    locked: dict[str, str] = {}
    for package in lock.get("package", []):
        source = LOCK_SOURCE.match(str(package.get("source", "")))
        if not source or source["url"].rstrip("/").removesuffix(".git") != DECDN_GIT:
            continue
        commits.add(source["sha"])
        locked[package["name"]] = package["version"]
    if len(commits) > 1:
        errors.append(
            f"Cargo.lock: decdn is locked at {len(commits)} commits ({', '.join(sorted(commits))}) "
            "— `cargo update -p decdn-incentive` moves them together"
        )
    for name in sorted(aliases):
        if name not in locked:
            errors.append(f"Cargo.lock: {name} is not locked from {DECDN_GIT}")
        elif name in versions and locked[name] != versions[name]:
            errors.append(
                f"Cargo.toml: {name} requires {versions[name]} but the locked decdn commit "
                f"is at {locked[name]} — set it to {locked[name]}"
            )
    if commits and not errors:
        notes.append(f"decdn@{next(iter(commits))}")
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
            "\nThe decdn-* versions in Cargo.toml's [workspace.dependencies] and the\n"
            "decdn commit Cargo.lock pins must agree. See RELEASING.md.",
            file=sys.stderr,
        )
        return 1
    manifest = tomllib.loads((repo_root / "Cargo.toml").read_text())
    aliases, _ = decdn_aliases(manifest)
    versions = sorted({v for v in (s.get("version") for s in aliases.values()) if v})
    print(f"decdn pin OK ({len(aliases)} aliases at {', '.join(versions)}, {', '.join(notes)})")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
