#!/usr/bin/env python3
"""Fail if a workspace member opts out of what the workspace decided.

`[workspace.package]` holds one edition, one licence and one MSRV for every
crate, and `[workspace.lints]` holds the lint table the anti-panic policy
depends on. Both reach a member only when that member asks for them, and a
member that does not ask gets no warning: a crate restating `edition` compiles
green under its own value, and a crate without `[lints] workspace = true`
compiles green under default lint levels.

The version is the exception: each crate is versioned and released on its own
(`cargo release -p <crate>`, tag `<crate>-v<version>`), so every member carries
its own literal `version`, and the workspace has none to inherit.

Per member:

* `version` is the member's own string, never `{ workspace = true }`.
* `edition`, `license`, `rust-version` are `{ workspace = true }`.
* `[lints] workspace = true` is present.
* A publishable member (no `publish = false`) also inherits `repository`,
  `homepage`, `keywords`, `categories`, and carries its own `description` and
  `readme` — the crates.io landing page is per crate, so those two are never
  inherited (the root manifest says why beside `[workspace.package]`).
* A dependency on a sibling member is `{ workspace = true }`, so the alias
  below carries the version. A `path` + `version` written in the member
  packages fine and then requires a sibling version that was never released.
  A crate's dev-dependency on itself (`path = "."`, to turn a feature on for
  its own integration tests) is the one exception: it is not a sibling.

Across the workspace:

* Every internal `[workspace.dependencies]` alias (a `path` into a member)
  resolves to a member and carries `version` equal to that member's own
  version. `cargo release -p <member>` keeps them in step (`dependent-version =
  "upgrade"`); a hand edit does not.
* `[workspace.package]` has no `version` for a member to inherit by mistake.
* A member some other member depends on has an alias; a member nothing depends
  on has none — an unread alias is one more version to forget on release.
* Every `crates/*/Cargo.toml` on disk is listed in `members`. The list is
  explicit rather than a glob, so a new crate directory is otherwise invisible
  to every workspace command until someone notices.

Run: .github/scripts/check-workspace-manifests.sh
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

if sys.version_info < (3, 11):  # tomllib, and the syntax used below
    sys.exit(f"error: python 3.11+ required, found {sys.version.split()[0]}")

import tomllib  # noqa: E402  (must follow the version guard)

INHERITED_ALWAYS = ("edition", "license", "rust-version")
INHERITED_IF_PUBLISHABLE = ("repository", "homepage", "keywords", "categories")
LOCAL_IF_PUBLISHABLE = ("description", "readme")
DEP_TABLES = ("dependencies", "dev-dependencies", "build-dependencies")


def workspace_members(repo_root: Path, manifest: dict) -> list[Path]:
    """Every member directory, from the root manifest.

    Read from `members` rather than globbed as `crates/*`: a member added
    outside `crates/` is exactly the case a hardcoded glob would skip.
    """
    members: list[Path] = []
    for pattern in manifest.get("workspace", {}).get("members", []):
        if any(ch in pattern for ch in "*?["):
            members.extend(sorted(p for p in repo_root.glob(pattern) if p.is_dir()))
        else:
            members.append(repo_root / pattern)
    return members


def inherits(table: dict, key: str) -> bool:
    return table.get(key) == {"workspace": True}


def check(repo_root: Path) -> list[str]:
    errors: list[str] = []
    root = tomllib.loads((repo_root / "Cargo.toml").read_text())
    workspace = root.get("workspace", {})
    ws_version = workspace.get("package", {}).get("version")
    members = workspace_members(repo_root, root)

    if not members:
        return ["Cargo.toml: [workspace] members is empty — this check inspected nothing"]
    if ws_version is not None:
        errors.append(
            "Cargo.toml: [workspace.package] has a `version` — crates are versioned "
            "independently, so each member carries its own and there is nothing to inherit"
        )

    # --- per-member shape --------------------------------------------------
    name_by_dir: dict[Path, str] = {}
    version_by_dir: dict[Path, object] = {}
    depended_upon: set[str] = set()
    internal_deps: list[tuple[Path, str, object]] = []
    for crate_root in members:
        rel = crate_root.relative_to(repo_root) / "Cargo.toml"
        manifest_path = crate_root / "Cargo.toml"
        if not manifest_path.is_file():
            errors.append(f"{rel}: workspace member has no Cargo.toml")
            continue
        manifest = tomllib.loads(manifest_path.read_text())
        package = manifest.get("package", {})
        name = package.get("name", str(rel))
        name_by_dir[crate_root.resolve()] = name
        version_by_dir[crate_root.resolve()] = package.get("version")

        if not isinstance(package.get("version"), str):
            errors.append(
                f"{rel}: `version` must be the crate's own string (`version = \"X.Y.Z\"`) — "
                "each crate is versioned and released on its own"
            )
        for key in INHERITED_ALWAYS:
            if not inherits(package, key):
                errors.append(
                    f"{rel}: `{key}` must be `{key}.workspace = true` — a member that "
                    "restates it compiles green and ships its own value"
                )
        if manifest.get("lints", {}).get("workspace") is not True:
            errors.append(
                f"{rel}: missing `[lints] workspace = true` — the workspace lint table "
                "(anti-panic, cast safety, print lints) does not reach this crate"
            )
        if package.get("publish") is not False:
            for key in INHERITED_IF_PUBLISHABLE:
                if not inherits(package, key):
                    errors.append(f"{rel}: publishable crate must set `{key}.workspace = true`")
            for key in LOCAL_IF_PUBLISHABLE:
                if not isinstance(package.get(key), str):
                    errors.append(
                        f"{rel}: publishable crate must carry its own `{key}` — the "
                        "crates.io landing page is per crate, never inherited"
                    )

        dep_tables = [manifest.get(table, {}) for table in DEP_TABLES]
        for target in manifest.get("target", {}).values():
            dep_tables.extend(target.get(table, {}) for table in DEP_TABLES)
        for deps in dep_tables:
            depended_upon.update(deps.keys())
            internal_deps.extend((rel, dep, spec) for dep, spec in deps.items() if dep != name)

    member_names = set(name_by_dir.values())
    for rel, dep, spec in internal_deps:
        if dep in member_names and not (isinstance(spec, dict) and spec.get("workspace") is True):
            errors.append(
                f"{rel}: internal dependency {dep} must be `{{ workspace = true }}` — the "
                "[workspace.dependencies] alias carries the version; a path + version "
                "written here packages fine and then requires a sibling version that was "
                "never released"
            )

    # --- internal aliases --------------------------------------------------
    aliases: dict[str, dict] = {}
    alias_member_version: dict[str, object] = {}
    for name, spec in workspace.get("dependencies", {}).items():
        if isinstance(spec, dict) and "path" in spec:
            target = (repo_root / spec["path"]).resolve()
            if target in name_by_dir:
                aliases[name] = spec
                alias_member_version[name] = version_by_dir[target]
            elif not target.is_relative_to(repo_root.resolve()):
                # A sibling checkout (the decdn crates). Its version is decdn's
                # to set, so only its presence is checked here; check_decdn_pin.py
                # compares it with the checkout.
                if not isinstance(spec.get("version"), str):
                    errors.append(
                        f"Cargo.toml: [workspace.dependencies] {name} has path = "
                        f"{spec['path']!r} outside the repository but no `version` — the "
                        "published manifest has no path to follow"
                    )
            else:
                errors.append(
                    f"Cargo.toml: [workspace.dependencies] {name} has path = {spec['path']!r}, "
                    "which is not a workspace member"
                )

    for name, spec in sorted(aliases.items()):
        version = spec.get("version")
        member_version = alias_member_version[name]
        if version != member_version:
            errors.append(
                f"Cargo.toml: [workspace.dependencies] {name} says version = {version!r} "
                f"but the crate is at {member_version!r} — `cargo release -p {name}` "
                "moves both; a hand edit must too"
            )
        if name not in depended_upon:
            errors.append(
                f"Cargo.toml: [workspace.dependencies] {name} exists but nothing depends on "
                "it — remove the alias until a member does (see the note beside the "
                "internal entries in Cargo.toml)"
            )
    for name in sorted(member_names & depended_upon):
        if name not in aliases:
            errors.append(
                f"Cargo.toml: {name} is depended on by another member but has no "
                "[workspace.dependencies] alias — add one with `path` and `version` so the "
                "published manifest resolves"
            )

    # --- directories not in members ----------------------------------------
    listed = set(name_by_dir)
    for manifest_path in sorted((repo_root / "crates").glob("*/Cargo.toml")):
        if manifest_path.parent.resolve() not in listed:
            errors.append(
                f"{manifest_path.parent.relative_to(repo_root)}: has a Cargo.toml but is not "
                "in [workspace] members"
            )

    return errors


def main() -> int:
    repo_root = Path(
        subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            check=True,
            stdout=subprocess.PIPE,
            text=True,
        ).stdout.strip()
    )
    errors = check(repo_root)
    if errors:
        print("error: workspace manifests drift from the workspace's decisions:\n", file=sys.stderr)
        for e in errors:
            print(f"  {e}", file=sys.stderr)
        print(
            "\nEvery member carries its own version and inherits edition/license/\n"
            "rust-version and the lint table from the root Cargo.toml; publishable\n"
            "crates also inherit the registry metadata.\n"
            "See .github/scripts/check_workspace_manifests.py.",
            file=sys.stderr,
        )
        return 1
    root = tomllib.loads((repo_root / "Cargo.toml").read_text())
    n = len(workspace_members(repo_root, root))
    print(f"workspace manifests OK ({n} members, each with its own version)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
