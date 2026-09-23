#!/usr/bin/env python3
"""Fail if a crate depends on one the design says it must not.

Cargo enforces acyclicity and nothing more. The direction the workspace's
crates depend on each other is a design decision — three leaves that pull in
no sibling, `cli` and `e2e` as sinks nothing depends on, a publisher CLI that
links no blob store or AWS SDK — and CLAUDE.md § Crate Structure states it.
Without this check nothing enforces it: an edge added the wrong way compiles
green, and the first thing to notice is a `decdn` binary that has grown an S3
client.

Two passes over `cargo metadata --all-features` (all features, because an
optional dependency is absent from the resolve graph until its feature is on,
and `cargo install decdn-cli --features x` would still link it):

* **Direct edges.** Each member's normal and build dependencies on other
  members must equal its ALLOWED row — equal, not a subset: the table is the
  graph transcribed, so an edge the graph lacks is rot in the other direction.
  Dev-dependencies are not constrained; the design accepts dev-only cycles
  (`cli` tests the daemon it is shipped beside). A member with no row fails,
  and a row for a member that does not exist fails too.
* **Closure.** The transitive normal and build dependencies of each
  FOOTPRINT_GUARDED crate — the `decdn-cli` binary and the `decdn-client` SDK
  it builds on — must not include the blob store or the AWS SDK
  (MUST_NOT_LINK). The direct table cannot see this: `client → common` is
  allowed, so `common` quietly growing an `iroh-blobs` edge would pass the
  first pass and still put the blob store in the SDK and the CLI. The SDK is
  guarded on its own, not only through the CLI, because a third party links it
  without the CLI. A walk that visits nothing is a failure, not a clean
  closure.

The table is the actual graph, transcribed. Changing it is a design change and
CLAUDE.md's Dependency flow paragraph moves with it.

Run: .github/scripts/check-crate-edges.sh
"""

from __future__ import annotations

import json
import subprocess
import sys
from collections import deque

# Member -> members it may depend on (normal dependencies only). `e2e` may
# depend on anything but a sink, so its row is the sentinel ANY.
ANY = frozenset({"*"})
ALLOWED: dict[str, frozenset[str]] = {
    "decdn-protocol": frozenset(),
    "decdn-config-types": frozenset(),
    "decdn-bao-range": frozenset(),
    "decdn-common": frozenset({"decdn-config-types", "decdn-protocol"}),
    "decdn-reputation": frozenset({"decdn-protocol"}),
    "decdn-incentive": frozenset({"decdn-common", "decdn-protocol"}),
    "decdn-cache": frozenset({"decdn-bao-range", "decdn-config-types", "decdn-protocol"}),
    "decdn-client": frozenset(
        {"decdn-bao-range", "decdn-common", "decdn-incentive", "decdn-protocol"}
    ),
    "decdn-node": frozenset(
        {
            "decdn-bao-range",
            "decdn-cache",
            "decdn-client",
            "decdn-common",
            "decdn-incentive",
            "decdn-protocol",
            "decdn-reputation",
        }
    ),
    "decdn-cli": frozenset(
        {
            "decdn-bao-range",
            "decdn-client",
            "decdn-common",
            "decdn-config-types",
            "decdn-incentive",
            "decdn-protocol",
        }
    ),
    "decdn-e2e": ANY,
}
# Nothing may depend on these, dev-deps aside.
SINKS = frozenset({"decdn-cli", "decdn-e2e"})
# The publisher CLI and the client SDK are iroh-blobs-free and AWS-free (#578,
# #1150): neither links a blob store or an S3 SDK, so `cargo install decdn-cli`
# builds no such thing and a crate built on `decdn-client` inherits none.
FOOTPRINT_GUARDED = ("decdn-cli", "decdn-client")
MUST_NOT_LINK = frozenset(
    {"iroh-blobs", "aws-config", "aws-credential-types", "aws-runtime", "aws-types", "aws-sigv4"}
)
# The SDK's service and smithy crates by prefix. Not a bare `aws-` prefix:
# `aws-lc-rs` is rustls's crypto provider and legitimately in the CLI tree.
MUST_NOT_LINK_PREFIXES = ("aws-sdk-", "aws-smithy-")
# A build script compiles the crate too, so a build-dependency is an edge the
# design has to allow; only dev-dependencies are exempt.
EDGE_KINDS = frozenset({None, "build"})
METADATA_ARGS = ("cargo", "metadata", "--format-version", "1", "--locked", "--all-features")


def is_forbidden(name: str) -> bool:
    return name in MUST_NOT_LINK or name.startswith(MUST_NOT_LINK_PREFIXES)


def is_edge(dep_kinds: list[dict]) -> bool:
    return any(k.get("kind") in EDGE_KINDS for k in dep_kinds)


def check(metadata: dict) -> list[str]:
    errors: list[str] = []
    by_id = {p["id"]: p for p in metadata["packages"]}
    members = {by_id[i]["name"]: by_id[i] for i in metadata["workspace_members"]}

    # --- the table matches the workspace ------------------------------------
    for name in sorted(set(ALLOWED) - set(members)):
        errors.append(
            f"ALLOWED in check_crate_edges.py lists {name}, which is no longer a workspace "
            "member — remove the row (and update CLAUDE.md § Crate Structure)"
        )
    for name in sorted(set(members) - set(ALLOWED)):
        errors.append(
            f"{name} is a workspace member with no row in ALLOWED (check_crate_edges.py) — "
            "decide its edges and record them there and in CLAUDE.md § Crate Structure"
        )

    # --- direct edges -------------------------------------------------------
    for name, pkg in sorted(members.items()):
        allowed = ALLOWED.get(name)
        if allowed is None:
            continue
        internal = sorted(
            {d["name"] for d in pkg["dependencies"] if d.get("kind") in EDGE_KINDS and d["name"] in members}
        )
        for dep in internal:
            if dep in SINKS:
                errors.append(
                    f"{name} → {dep}: nothing may depend on {dep}; it is a sink "
                    "(only a dev-dependency may point at it)"
                )
            elif allowed is not ANY and dep not in allowed:
                errors.append(
                    f"{name} → {dep}: not in the allowed dependency flow (CLAUDE.md § Crate "
                    "Structure). Move the shared code down to a crate both may reach, or "
                    "change the design and the table together"
                )
        if allowed is not ANY:
            for dep in sorted(allowed - set(internal)):
                errors.append(
                    f"{name} does not have the edge → {dep} that its ALLOWED row lists — the "
                    "table is the graph transcribed, so drop the entry (and update CLAUDE.md "
                    "§ Crate Structure)"
                )

    # --- footprint closures -------------------------------------------------
    for root in FOOTPRINT_GUARDED:
        pkg = members.get(root)
        if pkg is not None:
            errors.extend(check_closure(metadata, by_id, root, pkg["id"]))

    return errors


def check_closure(metadata: dict, by_id: dict, root: str, root_id: str) -> list[str]:
    """Walk `root`'s normal and build closure; report every forbidden crate in it."""
    errors: list[str] = []
    nodes = {n["id"]: n for n in metadata["resolve"]["nodes"]}
    parent: dict[str, str | None] = {root_id: None}
    queue = deque([root_id])
    while queue:
        current = queue.popleft()
        for dep in nodes.get(current, {}).get("deps", []):
            if not is_edge(dep["dep_kinds"]) or dep["pkg"] in parent:
                continue
            parent[dep["pkg"]] = current
            queue.append(dep["pkg"])
    if len(parent) <= 1:
        errors.append(
            f"{root} has no dependencies in the resolve graph — the closure walk inspected nothing"
        )
    for pkg_id, via in sorted(parent.items()):
        name = by_id[pkg_id]["name"]
        if is_forbidden(name):
            path = [name]
            while via is not None:
                path.append(by_id[via]["name"])
                via = parent[via]
            errors.append(
                f"{root} links {name} via " + " ← ".join(path) + " — the publisher CLI and "
                "the client SDK are iroh-blobs-free and AWS-free (#578); the edge that "
                "introduced it must go"
            )
    return errors


def main() -> int:
    repo_root = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"], check=True, stdout=subprocess.PIPE, text=True
    ).stdout.strip()
    # stderr passes through: a stale lock under `--locked` is the common local
    # failure, and cargo's own line says how to fix it.
    out = subprocess.run(
        [*METADATA_ARGS, "--manifest-path", f"{repo_root}/Cargo.toml"],
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    ).stdout
    errors = check(json.loads(out))
    if errors:
        print("error: crate dependency direction violates the design:\n", file=sys.stderr)
        for e in errors:
            print(f"  {e}", file=sys.stderr)
        print(
            "\nThe allowed flow is the table in .github/scripts/check_crate_edges.py and\n"
            "the Dependency flow paragraph in CLAUDE.md § Crate Structure. They move\n"
            "together, by design decision, not to make a build pass.",
            file=sys.stderr,
        )
        return 1
    guarded = ", ".join(FOOTPRINT_GUARDED)
    print(f"crate edges OK ({len(ALLOWED)} members; {guarded} closures clean)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
