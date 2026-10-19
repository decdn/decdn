"""Regression tests for the crate dependency-direction checker.

Cargo enforces acyclicity and nothing more. The direction the crates depend on
each other — three leaves, `cli`/`e2e` as sinks, a CLI that links no blob
store — is a design decision only this check holds, and a bug here degrades to
a quiet false pass. The tests hand it `cargo metadata`-shaped JSON and
`cargo tree`-shaped text built by hand, so they need no toolchain.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

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
    "decdn-client": [
        "decdn-bao-range",
        "decdn-common",
        "decdn-incentive",
        "decdn-protocol",
    ],
    "decdn-node": [
        "decdn-bao-range",
        "decdn-cache",
        "decdn-client",
        "decdn-common",
        "decdn-incentive",
        "decdn-protocol",
        "decdn-reputation",
    ],
    "decdn-cli": [
        "decdn-bao-range",
        "decdn-client",
        "decdn-common",
        "decdn-config-types",
        "decdn-incentive",
        "decdn-protocol",
    ],
    "decdn-e2e": [
        "decdn-bao-range",
        "decdn-cache",
        "decdn-client",
        "decdn-common",
        "decdn-config-types",
        "decdn-incentive",
        "decdn-node",
        "decdn-protocol",
    ],
    "sponsord-api": [],
    "sponsord-core": ["decdn-client", "decdn-common", "decdn-incentive", "sponsord-api"],
    "sponsord": ["decdn-common", "decdn-incentive", "sponsord-api", "sponsord-core"],
    "sponsord-onramp": ["sponsord-api"],
    "sponsord-e2e": [],
}

DEV: dict[str, list[str]] = {
    "decdn-cli": ["decdn-cache", "decdn-incentive", "decdn-node"],
    "decdn-node": ["decdn-client", "decdn-incentive"],
    "sponsord": ["decdn-client", "decdn-e2e", "sponsord-core"],
    "sponsord-core": ["decdn-e2e"],
    "sponsord-e2e": ["sponsord", "sponsord-api", "sponsord-core", "sponsord-onramp", "decdn-incentive"],
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
    build: dict[str, list[str]] | None = None,
    external: dict[str, list[str]] = EXTERNAL,
) -> dict:
    """`cargo metadata --format-version 1 --no-deps` shaped output for the given graph."""
    build = build or {}

    def pkg_id(name: str) -> str:
        return f"path+file:///w/{name}#0.0.0"

    packages = []
    for name, internal in graph.items():
        deps = [{"name": d, "kind": None} for d in internal + external_edges.get(name, [])]
        deps += [{"name": d, "kind": "dev"} for d in dev.get(name, [])]
        deps += [{"name": d, "kind": "build"} for d in build.get(name, [])]
        packages.append({"name": name, "id": pkg_id(name), "dependencies": deps})
    return {"packages": packages, "workspace_members": [pkg_id(n) for n in graph]}


def tree(
    root: str,
    graph: dict[str, list[str]] = GRAPH,
    dev: dict[str, list[str]] = DEV,
    external_edges: dict[str, list[str]] = EXTERNAL_EDGES,
    build: dict[str, list[str]] | None = None,
    external: dict[str, list[str]] = EXTERNAL,
) -> str:
    """`cargo tree -p root -e normal,build --prefix depth --format {p}` for the graph.

    Like cargo: depth-first, each crate's dependencies printed once, a repeat
    marked ` (*)`, dev-dependencies left out.
    """
    build = build or {}

    def deps(name: str) -> list[str]:
        out = graph.get(name, []) + external_edges.get(name, []) + build.get(name, [])
        return out + external.get(name, [])

    lines: list[str] = []
    seen: set[str] = set()

    def walk(name: str, depth: int) -> None:
        suffix = " (/w)" if name in graph else ""
        if name in seen:
            lines.append(f"{depth}{name} v1.0.0{suffix} (*)")
            return
        seen.add(name)
        lines.append(f"{depth}{name} v1.0.0{suffix}")
        for d in deps(name):
            walk(d, depth + 1)

    walk(root, 0)
    return "\n".join(lines) + "\n"


def run(**kw) -> list[str]:
    """Both passes over one hand-built graph."""
    graph = kw.get("graph", GRAPH)
    trees = {r: tree(r, **kw) for r in cce.FOOTPRINT_GUARDED if r in graph}
    return cce.check(metadata(**kw), trees)


def test_the_current_graph_passes():
    assert run() == []


def test_reverse_edge_is_an_error_naming_both_ends():
    graph = {k: list(v) for k, v in GRAPH.items()}
    graph["decdn-protocol"].append("decdn-common")
    errors = run(graph=graph)
    assert len(errors) == 1, errors
    assert "decdn-protocol" in errors[0] and "decdn-common" in errors[0]


def test_leaf_gaining_an_external_looking_internal_edge_is_an_error():
    graph = {k: list(v) for k, v in GRAPH.items()}
    graph["decdn-bao-range"].append("decdn-protocol")
    errors = run(graph=graph)
    assert len(errors) == 1, errors
    assert "decdn-bao-range" in errors[0]


@pytest.mark.parametrize(
    ("dependant", "sink"),
    [
        ("decdn-node", "decdn-cli"),
        ("decdn-node", "decdn-e2e"),
        ("sponsord-onramp", "sponsord"),
        ("sponsord-core", "sponsord-e2e"),
    ],
)
def test_nothing_may_depend_on_a_sink(dependant, sink):
    assert sink in cce.SINKS
    graph = {k: list(v) for k, v in GRAPH.items()}
    graph[dependant].append(sink)
    errors = run(graph=graph)
    assert len(errors) == 1, errors
    assert sink in errors[0] and "sink" in errors[0], errors[0]


def test_the_onramp_is_not_a_sink():
    """The onramp is an embeddable library; an edge into it is an ordinary table miss."""
    assert "sponsord-onramp" not in cce.SINKS
    graph = {k: list(v) for k, v in GRAPH.items()}
    graph["sponsord"].append("sponsord-onramp")
    errors = run(graph=graph)
    assert len(errors) == 1, errors
    assert "sink" not in errors[0], errors[0]


def test_sponsord_e2e_may_depend_on_anything_but_a_sink():
    graph = {k: list(v) for k, v in GRAPH.items()}
    graph["sponsord-e2e"].append("decdn-reputation")
    assert run(graph=graph) == []
    graph["sponsord-e2e"].append("sponsord")
    errors = run(graph=graph)
    assert len(errors) == 1, errors


def test_leak_under_the_wire_types_reaches_only_the_sponsord_crates():
    """`sponsord-api` is under both sponsord servers and under nothing else guarded."""
    ext = {k: list(v) for k, v in EXTERNAL_EDGES.items()}
    ext["sponsord-api"] = ["iroh-blobs"]
    errors = run(external_edges=ext)
    assert [e.split()[0] for e in errors] == ["sponsord", "sponsord-onramp"], errors
    assert all("← sponsord-api ←" in e for e in errors), errors


def test_a_guarded_root_that_is_not_a_member_is_an_error(monkeypatch):
    monkeypatch.setattr(cce, "FOOTPRINT_GUARDED", (*cce.FOOTPRINT_GUARDED, "sponsord-typo"))
    errors = run()
    assert len(errors) == 1, errors
    assert "sponsord-typo" in errors[0] and "not a workspace member" in errors[0], errors[0]


def test_e2e_may_depend_on_anything_but_cli():
    graph = {k: list(v) for k, v in GRAPH.items()}
    graph["decdn-e2e"].append("decdn-reputation")
    assert run(graph=graph) == []
    graph["decdn-e2e"].append("decdn-cli")
    errors = run(graph=graph)
    assert len(errors) == 1, errors


def test_dev_dependencies_are_not_edges():
    """`cli` dev-depends on `node`; that cycle-shaped edge is allowed."""
    dev = {k: list(v) for k, v in DEV.items()}
    dev["decdn-protocol"] = ["decdn-node"]
    assert run(dev=dev) == []


def test_closure_must_not_reach_the_blob_store():
    """A leak below the SDK reaches every guarded crate built on it; each gets its own error.

    The onramp sits on the wire types alone, so it is the one guarded crate
    the leak misses.
    """
    ext = {k: list(v) for k, v in EXTERNAL_EDGES.items()}
    ext["decdn-common"].append("iroh-blobs")
    errors = run(external_edges=ext)
    assert len(errors) == 3, errors
    assert [e.split()[0] for e in errors] == ["decdn-cli", "decdn-client", "sponsord"], errors
    for e in errors:
        assert "iroh-blobs" in e
        assert "decdn-common" in e, "the path should name the crate that introduced it"


def test_client_sdk_edge_to_the_blob_store_is_an_error():
    ext = {k: list(v) for k, v in EXTERNAL_EDGES.items()}
    ext["decdn-client"] = ["iroh-blobs"]
    errors = run(external_edges=ext)
    assert any(
        e.startswith("decdn-client links iroh-blobs via iroh-blobs ← decdn-client") for e in errors
    ), errors


def test_client_sdk_is_guarded_without_the_cli():
    """A third party links the SDK without the CLI, so its closure is walked on its own."""
    graph = {k: list(v) for k, v in GRAPH.items()}
    graph["decdn-cli"].remove("decdn-client")
    ext = {k: list(v) for k, v in EXTERNAL_EDGES.items()}
    ext["decdn-client"] = ["aws-sdk-s3"]
    errors = [e for e in run(graph=graph, external_edges=ext) if " links " in e]
    # The SDK's own error, and the sponsord daemon's, which links the SDK
    # through sponsord-core.
    assert len(errors) == 2, errors
    assert errors[0].startswith("decdn-client links aws-sdk-s3"), errors[0]
    assert errors[1].startswith("sponsord links aws-sdk-s3"), errors[1]


def test_cli_only_leak_does_not_blame_the_sdk():
    ext = {k: list(v) for k, v in EXTERNAL_EDGES.items()}
    ext["decdn-cli"].append("iroh-blobs")
    errors = run(external_edges=ext)
    assert len(errors) == 1, errors
    assert errors[0].startswith("decdn-cli links iroh-blobs"), errors[0]


def test_cli_closure_ignores_dev_only_routes():
    """`cli` dev-depends on `cache`, which links iroh-blobs; that is not a leak."""
    dev = {"decdn-cli": ["decdn-cache"]}
    assert run(dev=dev) == []


@pytest.mark.parametrize(
    "forbidden",
    [
        "iroh-blobs",
        "aws-sdk-s3",
        "aws-config",
        "aws-smithy-types",
        "aws-credential-types",
        "aws-lc-rs",
        "aws-lc-sys",
        "jsonrpsee-server",
    ],
)
def test_cli_closure_leak_two_hops_through_an_external_crate(forbidden):
    external = {**EXTERNAL, "origin-sdk": [forbidden], forbidden: []}
    ext = {k: list(v) for k, v in EXTERNAL_EDGES.items()}
    ext["decdn-incentive"] = ["origin-sdk"]
    errors = run(external_edges=ext, external=external)
    assert len(errors) == 3, errors
    assert f"{forbidden} ← origin-sdk ← decdn-incentive ← decdn-cli" in errors[0], errors[0]
    assert f"{forbidden} ← origin-sdk ← decdn-incentive ← decdn-client" in errors[1], errors[1]
    assert f"{forbidden} ← origin-sdk ← decdn-incentive ← sponsord" in errors[2], errors[2]


def test_aws_lc_reached_through_rustls_is_an_error():
    """The #2454 shape: a reqwest/rustls feature turning aws-lc on under the SDK."""
    external = {**EXTERNAL, "rustls": ["aws-lc-rs"], "aws-lc-rs": ["aws-lc-sys"], "aws-lc-sys": []}
    ext = {k: list(v) for k, v in EXTERNAL_EDGES.items()}
    ext["decdn-common"].append("rustls")
    errors = run(external_edges=ext, external=external)
    # aws-lc-rs and aws-lc-sys, for each of the CLI, the SDK and the daemon.
    assert len(errors) == 6, errors
    assert "aws-lc-sys ← aws-lc-rs ← rustls ← decdn-common ← decdn-client" in "\n".join(errors)


def test_closure_reads_the_per_crate_tree_not_the_workspace_resolve():
    """The daemon links aws-lc and the JSON-RPC server; the CLI's own tree does not.

    `cargo metadata` resolves features once for the workspace and would put
    both in every closure. The check reads only the per-crate trees, so a
    crate the metadata names but the tree does not is no error.
    """
    m = metadata(external_edges={**EXTERNAL_EDGES, "decdn-node": ["jsonrpsee-server", "aws-lc-sys"]})
    trees = {r: tree(r) for r in cce.FOOTPRINT_GUARDED}
    assert cce.check(m, trees) == []


def test_each_guarded_crate_gets_its_own_tree_invocation():
    """`cargo tree -p a -p b` unifies their features, so one root per command."""
    for root in cce.FOOTPRINT_GUARDED:
        cmd = cce.tree_command(root, "/w/Cargo.toml")
        assert cmd.count("-p") == 1 and cmd[cmd.index("-p") + 1] == root, cmd


def test_tree_is_read_with_all_features_all_targets_and_no_dev_deps():
    """An optional dep or a mobile-only dep still links; a dev-dep does not."""
    args = cce.TREE_ARGS
    assert "--all-features" in args
    assert args[args.index("--target") + 1] == "all"
    assert args[args.index("-e") + 1] == "normal,build"


def test_tree_with_repeats_keeps_every_edge():
    """A ` (*)` line is still an edge; its subtree was printed where first seen."""
    text = "0decdn-client v1 (/w)\n1a v1\n2leaf v1\n1b v1\n2a v1 (*)\n"
    root, deps = cce.parse_tree(text)
    assert root == "decdn-client"
    assert deps == {"decdn-client": ["a", "b"], "a": ["leaf"], "b": ["a"]}


def test_unparseable_tree_line_is_an_error_not_a_skip():
    with pytest.raises(ValueError):
        cce.parse_tree("0decdn-client v1 (/w)\n[build-dependencies]\n1cc v1\n")
    with pytest.raises(ValueError):
        cce.parse_tree("0decdn-client v1 (/w)\n2cc v1\n")


def test_empty_tree_is_a_failure_not_a_pass():
    errors = cce.check(metadata(), {r: "" for r in cce.FOOTPRINT_GUARDED})
    assert len(errors) == len(cce.FOOTPRINT_GUARDED), errors
    assert all("inspected nothing" in e for e in errors), errors


def test_tree_for_the_wrong_root_is_an_error():
    trees = {r: tree("decdn-protocol") for r in cce.FOOTPRINT_GUARDED}
    errors = cce.check(metadata(), trees)
    assert len(errors) == len(cce.FOOTPRINT_GUARDED), errors
    assert all("rooted at decdn-protocol" in e for e in errors), errors


def test_table_row_with_slack_is_an_error():
    """The table is the graph transcribed; an edge the graph lacks is rot too."""
    graph = {k: list(v) for k, v in GRAPH.items()}
    graph["decdn-node"].remove("decdn-reputation")
    errors = run(graph=graph)
    assert len(errors) == 1, errors
    assert "decdn-node" in errors[0] and "decdn-reputation" in errors[0]
    assert "does not have" in errors[0]


def test_build_dependency_is_an_edge():
    """A build script compiles the crate too, so it is not exempt like a dev-dep."""
    errors = run(build={"decdn-node": ["decdn-cli"]})
    assert len(errors) == 1, errors
    assert "decdn-cli" in errors[0]


def test_member_missing_from_the_table_is_an_error():
    graph = {k: list(v) for k, v in GRAPH.items()}
    graph["decdn-new"] = ["decdn-protocol"]
    errors = run(graph=graph)
    assert len(errors) == 1, errors
    assert "decdn-new" in errors[0]
    assert "ALLOWED" in errors[0]


def test_table_row_for_a_vanished_crate_is_an_error():
    graph = {k: list(v) for k, v in GRAPH.items()}
    del graph["decdn-reputation"]
    graph["decdn-node"].remove("decdn-reputation")
    errors = run(graph=graph)
    assert any("decdn-reputation" in e and "no longer" in e for e in errors), errors
