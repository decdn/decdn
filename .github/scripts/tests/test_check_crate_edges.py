"""Regression tests for the crate dependency-direction checker.

Cargo enforces acyclicity and nothing more. The direction the crates depend on
each other — three leaves, `cli`/`e2e` as sinks, a CLI that links no blob
store — is a design decision only this check holds, and a bug here degrades to
a quiet false pass. The tests hand it `cargo metadata`-shaped JSON built by
hand so they need no toolchain.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
MODULE_PATH = REPO_ROOT / ".github/scripts/check_crate_edges.py"

_spec = importlib.util.spec_from_file_location("check_crate_edges", MODULE_PATH)
assert _spec and _spec.loader
cce = importlib.util.module_from_spec(_spec)
sys.modules["check_crate_edges"] = cce
_spec.loader.exec_module(cce)


# The real graph today, as `cargo metadata --no-deps` reports it (normal deps
# only). Tests mutate a copy of this rather than inventing a toy graph, so the
# allowed table under test is the one that ships.
GRAPH: dict[str, list[str]] = {
    "decdn-protocol": [],
    "decdn-config-types": [],
    "decdn-bao-range": [],
    "decdn-common": ["decdn-config-types", "decdn-protocol"],
    "decdn-reputation": ["decdn-protocol"],
    "decdn-incentive": ["decdn-common", "decdn-protocol"],
    "decdn-cache": ["decdn-bao-range", "decdn-config-types", "decdn-protocol"],
    "decdn-client-pull": [
        "decdn-bao-range",
        "decdn-common",
        "decdn-incentive",
        "decdn-protocol",
    ],
    "decdn-node": [
        "decdn-bao-range",
        "decdn-cache",
        "decdn-client-pull",
        "decdn-common",
        "decdn-incentive",
        "decdn-protocol",
        "decdn-reputation",
    ],
    "decdn-cli": [
        "decdn-bao-range",
        "decdn-client-pull",
        "decdn-common",
        "decdn-config-types",
        "decdn-incentive",
        "decdn-protocol",
    ],
    "decdn-e2e": [
        "decdn-bao-range",
        "decdn-cache",
        "decdn-client-pull",
        "decdn-common",
        "decdn-config-types",
        "decdn-incentive",
        "decdn-node",
        "decdn-protocol",
    ],
}

DEV: dict[str, list[str]] = {
    "decdn-cli": ["decdn-cache", "decdn-incentive", "decdn-node"],
    "decdn-node": ["decdn-client-pull", "decdn-incentive"],
}

# External crates and what they pull, enough to exercise the closure walk.
EXTERNAL: dict[str, list[str]] = {
    "iroh": [],
    "iroh-blobs": ["iroh"],
    "aws-sdk-s3": [],
}
EXTERNAL_EDGES: dict[str, list[str]] = {
    "decdn-cache": ["iroh-blobs", "aws-sdk-s3"],
    "decdn-node": ["iroh-blobs", "iroh"],
    "decdn-cli": ["iroh"],
    "decdn-common": ["iroh"],
}


def metadata(
    graph: dict[str, list[str]] = GRAPH,
    dev: dict[str, list[str]] = DEV,
    external_edges: dict[str, list[str]] = EXTERNAL_EDGES,
) -> dict:
    """`cargo metadata --format-version 1` shaped output for the given graph."""

    def pkg_id(name: str) -> str:
        return f"path+file:///w/{name}#0.0.0" if name in graph else f"registry+{name}#1.0.0"

    packages = []
    nodes = []
    for name, internal in graph.items():
        normal = internal + external_edges.get(name, [])
        deps = [{"name": d, "kind": None} for d in normal]
        deps += [{"name": d, "kind": "dev"} for d in dev.get(name, [])]
        packages.append({"name": name, "id": pkg_id(name), "dependencies": deps})
        node_deps = [{"pkg": pkg_id(d), "dep_kinds": [{"kind": None}]} for d in normal]
        node_deps += [{"pkg": pkg_id(d), "dep_kinds": [{"kind": "dev"}]} for d in dev.get(name, [])]
        nodes.append({"id": pkg_id(name), "deps": node_deps})
    for name, internal in EXTERNAL.items():
        packages.append(
            {"name": name, "id": pkg_id(name), "dependencies": [{"name": d, "kind": None} for d in internal]}
        )
        nodes.append(
            {"id": pkg_id(name), "deps": [{"pkg": pkg_id(d), "dep_kinds": [{"kind": None}]} for d in internal]}
        )
    return {
        "packages": packages,
        "workspace_members": [pkg_id(n) for n in graph],
        "resolve": {"nodes": nodes},
    }


def test_the_current_graph_passes():
    assert cce.check(metadata()) == []


def test_reverse_edge_is_an_error_naming_both_ends():
    graph = {k: list(v) for k, v in GRAPH.items()}
    graph["decdn-protocol"].append("decdn-common")
    errors = cce.check(metadata(graph))
    assert len(errors) == 1, errors
    assert "decdn-protocol" in errors[0] and "decdn-common" in errors[0]


def test_leaf_gaining_an_external_looking_internal_edge_is_an_error():
    graph = {k: list(v) for k, v in GRAPH.items()}
    graph["decdn-bao-range"].append("decdn-protocol")
    errors = cce.check(metadata(graph))
    assert len(errors) == 1, errors
    assert "decdn-bao-range" in errors[0]


def test_nothing_may_depend_on_a_sink():
    graph = {k: list(v) for k, v in GRAPH.items()}
    graph["decdn-node"].append("decdn-cli")
    errors = cce.check(metadata(graph))
    assert len(errors) == 1, errors
    assert "decdn-cli" in errors[0]


def test_e2e_may_depend_on_anything_but_cli():
    graph = {k: list(v) for k, v in GRAPH.items()}
    graph["decdn-e2e"].append("decdn-reputation")
    assert cce.check(metadata(graph)) == []
    graph["decdn-e2e"].append("decdn-cli")
    errors = cce.check(metadata(graph))
    assert len(errors) == 1, errors


def test_dev_dependencies_are_not_edges():
    """`cli` dev-depends on `node`; that cycle-shaped edge is allowed."""
    dev = {k: list(v) for k, v in DEV.items()}
    dev["decdn-protocol"] = ["decdn-node"]
    assert cce.check(metadata(dev=dev)) == []


def test_cli_closure_must_not_reach_the_blob_store():
    ext = {k: list(v) for k, v in EXTERNAL_EDGES.items()}
    ext["decdn-common"].append("iroh-blobs")
    errors = cce.check(metadata(external_edges=ext))
    assert len(errors) == 1, errors
    assert "decdn-cli" in errors[0]
    assert "iroh-blobs" in errors[0]
    assert "decdn-common" in errors[0], "the path should name the crate that introduced it"


def test_cli_closure_ignores_dev_only_routes():
    """`cli` dev-depends on `cache`, which links iroh-blobs; that is not a leak."""
    assert cce.check(metadata()) == []


def test_member_missing_from_the_table_is_an_error():
    graph = {k: list(v) for k, v in GRAPH.items()}
    graph["decdn-new"] = ["decdn-protocol"]
    errors = cce.check(metadata(graph))
    assert len(errors) == 1, errors
    assert "decdn-new" in errors[0]
    assert "ALLOWED" in errors[0]


def test_table_row_for_a_vanished_crate_is_an_error():
    graph = {k: list(v) for k, v in GRAPH.items()}
    del graph["decdn-reputation"]
    graph["decdn-node"].remove("decdn-reputation")
    errors = cce.check(metadata(graph))
    assert len(errors) == 1, errors
    assert "decdn-reputation" in errors[0]
    assert "no longer" in errors[0]
