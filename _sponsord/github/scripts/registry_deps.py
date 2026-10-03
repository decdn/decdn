#!/usr/bin/env python3
"""The path and git dependencies of one crate that crates.io must already serve.

Locally a `path` or `git` source wins; in the manifest `cargo publish` uploads
it becomes its `version`, resolved from crates.io. So before a crate is
published, every such dependency that survives into that manifest must already
be there at the version it requires:

* sibling crates (sponsord-api and sponsord-core for sponsord, sponsord-api for
  the onramp and the CLI), each released on its own tag;
* the decdn crates (git dependencies on github.com/decdn/decdn), from the decdn
  release at the commit Cargo.lock pins.

A dev-dependency without a `version` is stripped from the uploaded manifest,
so it is skipped. A normal or build dependency on a path or git source without
one cannot be published at all, and is an error.

Usage:  registry_deps.py <git-rev> <crate>
Prints one `<name> <version> <sibling|decdn>` line per dependency, read from the
manifests at <git-rev>, so it describes the tagged tree rather than the working
copy.
"""

from __future__ import annotations

import subprocess
import sys

if sys.version_info < (3, 11):  # tomllib
    sys.exit(f"error: python 3.11+ required, found {sys.version.split()[0]}")

import tomllib  # noqa: E402  (must follow the version guard)

DEP_TABLES = ("dependencies", "build-dependencies", "dev-dependencies")


def registry_deps(
    root: dict, crate: dict, sibling_names: set[str]
) -> tuple[list[tuple[str, str, str]], list[str]]:
    """(deps, errors): each dep as (name, version, "sibling" | "decdn")."""
    aliases = root.get("workspace", {}).get("dependencies", {})
    tables: list[tuple[str, dict]] = [(t, crate.get(t, {})) for t in DEP_TABLES]
    for target in crate.get("target", {}).values():
        tables.extend((t, target.get(t, {})) for t in DEP_TABLES)

    found: dict[str, tuple[str, str, str]] = {}
    errors: list[str] = []
    for table, deps in tables:
        for name, spec in deps.items():
            if isinstance(spec, dict) and spec.get("workspace") is True:
                spec = aliases.get(name, {})
            if not isinstance(spec, dict) or not ("path" in spec or "git" in spec):
                continue  # a registry dependency already
            package = spec.get("package", name)
            version = spec.get("version")
            if version is None:
                if table == "dev-dependencies":
                    continue  # stripped from the uploaded manifest
                source = "path" if "path" in spec else "git"
                errors.append(
                    f"[{table}] {name} is a {source} dependency with no `version`; the "
                    "published manifest would have nothing to resolve"
                )
                continue
            origin = "sibling" if package in sibling_names else "decdn"
            found[package] = (package, str(version), origin)
    return sorted(found.values()), errors


def _git_show(rev: str, path: str) -> dict:
    out = subprocess.run(
        ["git", "show", f"{rev}:{path}"], check=True, capture_output=True, text=True
    ).stdout
    return tomllib.loads(out)


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("usage: registry_deps.py <git-rev> <crate>", file=sys.stderr)
        return 2
    rev, crate_name = argv
    root = _git_show(rev, "Cargo.toml")
    members = {}
    for rel in root.get("workspace", {}).get("members", []):
        manifest = _git_show(rev, f"{rel}/Cargo.toml")
        members[manifest["package"]["name"]] = manifest
    if crate_name not in members:
        print(f"error: {crate_name} is not a workspace member at {rev}", file=sys.stderr)
        return 1
    deps, errors = registry_deps(root, members[crate_name], set(members))
    for e in errors:
        print(f"error: {crate_name}: {e}", file=sys.stderr)
    if errors:
        return 1
    for name, version, origin in deps:
        print(name, version, origin)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
