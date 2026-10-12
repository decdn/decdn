#!/usr/bin/env python3
"""The dependencies crates.io must already serve before the workspace publishes.

Locally a `path` or `git` source wins; in the manifest `cargo publish` uploads
it becomes its `version`, resolved from crates.io. Every such dependency that
survives into an uploaded manifest falls in one of two groups:

* sibling crates (sponsord-api, sponsord-core): released in the same run, at
  the same version, so `cargo publish --workspace` uploads them first;
* the decdn crates (git dependencies on github.com/decdn/decdn), which must
  already be on crates.io, from the decdn release at the commit Cargo.lock
  pins.

A dev-dependency without a `version` is stripped from the uploaded manifest,
so it is skipped. A normal or build dependency on a path or git source without
one cannot be published at all, and is an error.

Usage:  registry_deps.py <git-rev>
Prints one `<name> <version>` line per decdn dependency of any publishable
member, read from the manifests at <git-rev>, so it describes the tagged tree
rather than the working copy.
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


def workspace_decdn_deps(
    root: dict, members: dict[str, dict]
) -> tuple[list[tuple[str, str]], list[str]]:
    """(deps, errors) across every publishable member: each decdn dep once, as
    (name, version). Two members requiring different versions of one decdn
    crate are both listed; publish-crates.sh refuses that."""
    found: set[tuple[str, str]] = set()
    errors: list[str] = []
    for name, manifest in sorted(members.items()):
        if manifest.get("package", {}).get("publish") is False:
            continue
        deps, errs = registry_deps(root, manifest, set(members))
        errors.extend(f"{name}: {e}" for e in errs)
        found.update((n, v) for n, v, origin in deps if origin == "decdn")
    return sorted(found), errors


def _git_show(rev: str, path: str) -> dict:
    out = subprocess.run(
        ["git", "show", f"{rev}:{path}"], check=True, capture_output=True, text=True
    ).stdout
    return tomllib.loads(out)


def main(argv: list[str]) -> int:
    if len(argv) != 1:
        print("usage: registry_deps.py <git-rev>", file=sys.stderr)
        return 2
    (rev,) = argv
    root = _git_show(rev, "Cargo.toml")
    members = {}
    for rel in root.get("workspace", {}).get("members", []):
        manifest = _git_show(rev, f"{rel}/Cargo.toml")
        members[manifest["package"]["name"]] = manifest
    if not members:
        print(f"error: no workspace members at {rev}", file=sys.stderr)
        return 1
    deps, errors = workspace_decdn_deps(root, members)
    for e in errors:
        print(f"error: {e}", file=sys.stderr)
    if errors:
        return 1
    for name, version in deps:
        print(name, version)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
