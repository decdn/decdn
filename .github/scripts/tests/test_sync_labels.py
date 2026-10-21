"""Tests for the label sync planner.

The planner decides which live labels get renamed, created, updated or
deleted. A wrong plan is applied to the real repository, and a wrong delete
strips a label from every closed issue that carried it, so the decision
logic is pure and tested here against hand-written live states. The real
`.github/labels.toml` is parsed too, so a malformed entry fails CI before
anyone runs the script.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
MODULE_PATH = REPO_ROOT / ".github/scripts/sync_labels.py"
SPEC_PATH = REPO_ROOT / ".github/labels.toml"

_spec = importlib.util.spec_from_file_location("sync_labels", MODULE_PATH)
assert _spec and _spec.loader
sl = importlib.util.module_from_spec(_spec)
sys.modules["sync_labels"] = sl
_spec.loader.exec_module(sl)

SPEC_TEXT = """
[[label]]
name = "kind:bug"
color = "D73A4A"
description = "Something does not behave as documented"
previously = ["bug"]

[[label]]
name = "area:cache"
color = "1d76db"
description = "Cache engine"

[[label]]
name = "status:mainnet-blocking"
color = "5319e7"
description = "Must land before mainnet"
previously = ["pre-launch", "prod-blocking"]

[[retired]]
name = "cuj"
"""


def live(*rows: tuple[str, str, str]) -> list:
    return [sl.Live(n, c, d) for n, c, d in rows]


# ---- spec parsing -----------------------------------------------------------


def test_parse_spec_lowercases_color_and_keeps_order():
    spec = sl.parse_spec(SPEC_TEXT)
    assert [l.name for l in spec.labels] == ["kind:bug", "area:cache", "status:mainnet-blocking"]
    assert spec.labels[0].color == "d73a4a"
    assert spec.labels[0].previously == ("bug",)
    assert spec.retired == ("cuj",)


@pytest.mark.parametrize(
    "text, fragment",
    [
        ('[[label]]\nname = "a"\ncolor = "12345"\ndescription = "x"\n', "not 6 hex digits"),
        ('[[label]]\nname = "a"\ncolor = "123456"\n', "`description` must be"),
        ('[[label]]\nname = ""\ncolor = "123456"\ndescription = "x"\n', "`name` must be"),
        (
            '[[label]]\nname = "a"\ncolor = "123456"\ndescription = "x"\n'
            '[[label]]\nname = "a"\ncolor = "123456"\ndescription = "y"\n',
            "already appears",
        ),
        (
            '[[label]]\nname = "a"\ncolor = "123456"\ndescription = "x"\n'
            '[[label]]\nname = "b"\ncolor = "123456"\ndescription = "y"\npreviously = ["a"]\n',
            "already appears",
        ),
        (
            '[[label]]\nname = "a"\ncolor = "123456"\ndescription = "x"\npreviously = "old"\n',
            "list of non-empty strings",
        ),
        (
            '[[label]]\nname = "a"\ncolor = "123456"\ndescription = "x"\n[[retired]]\nname = "a"\n',
            "also appears",
        ),
        (
            '[[label]]\nname = "a"\ncolor = "123456"\ndescription = "x"\n'
            '[[retired]]\nname = "z"\n[[retired]]\nname = "z"\n',
            "listed twice",
        ),
        ("", "names no labels"),
        ("[[label]\n", "labels.toml:"),
    ],
)
def test_parse_spec_rejects(text: str, fragment: str):
    with pytest.raises(sl.SpecError, match=fragment):
        sl.parse_spec(text)


# ---- planning ---------------------------------------------------------------


def test_identical_live_state_is_a_no_op():
    spec = sl.parse_spec(SPEC_TEXT)
    state = live(
        ("kind:bug", "d73a4a", "Something does not behave as documented"),
        ("area:cache", "1d76db", "Cache engine"),
        ("status:mainnet-blocking", "5319e7", "Must land before mainnet"),
    )
    result = sl.plan(spec, state)
    assert result.ops == []
    assert result.unmanaged == []


def test_rename_wins_over_create_and_then_updates_metadata():
    spec = sl.parse_spec(SPEC_TEXT)
    state = live(("bug", "ff0000", "Something isn't working"))
    result = sl.plan(spec, state)
    assert result.ops[0] == sl.Rename("bug", "kind:bug")
    update = result.ops[1]
    assert isinstance(update, sl.Update)
    assert update.label.name == "kind:bug"
    assert update.live == sl.Live("kind:bug", "ff0000", "Something isn't working")
    # The other two are plain creates; nothing is deleted.
    assert [type(op) for op in result.ops[2:]] == [sl.Create, sl.Create]


def test_rename_with_matching_metadata_emits_no_update():
    spec = sl.parse_spec(SPEC_TEXT)
    state = live(("bug", "d73a4a", "Something does not behave as documented"))
    ops = sl.plan(spec, state).ops
    assert ops[0] == sl.Rename("bug", "kind:bug")
    assert not any(isinstance(op, sl.Update) for op in ops)


def test_update_only_when_color_or_description_differ():
    spec = sl.parse_spec(SPEC_TEXT)
    state = live(
        ("kind:bug", "D73A4A", "Something does not behave as documented"),  # case only
        ("area:cache", "1d76db", "stale description"),
        ("status:mainnet-blocking", "000000", "Must land before mainnet"),
    )
    # parse_live lowercases; emulate that here.
    state = [sl.Live(l.name, l.color.lower(), l.description) for l in state]
    ops = sl.plan(spec, state).ops
    assert ops == [
        sl.Update(spec.labels[1], state[1]),
        sl.Update(spec.labels[2], state[2]),
    ]


def test_retired_label_is_deleted_only_when_live():
    spec = sl.parse_spec(SPEC_TEXT)
    with_cuj = live(("cuj", "000000", "")) + live(
        ("kind:bug", "d73a4a", "Something does not behave as documented"),
        ("area:cache", "1d76db", "Cache engine"),
        ("status:mainnet-blocking", "5319e7", "Must land before mainnet"),
    )
    assert sl.plan(spec, with_cuj).ops == [sl.Delete("cuj")]
    assert sl.plan(spec, with_cuj[1:]).ops == []


def test_unmanaged_live_label_is_reported_not_deleted():
    spec = sl.parse_spec(SPEC_TEXT)
    state = live(
        ("kind:bug", "d73a4a", "Something does not behave as documented"),
        ("area:cache", "1d76db", "Cache engine"),
        ("status:mainnet-blocking", "5319e7", "Must land before mainnet"),
        ("dependencies", "0366d6", "Pull requests that update a dependency file"),
    )
    result = sl.plan(spec, state)
    assert result.ops == []
    assert result.unmanaged == ["dependencies"]


def test_two_live_previous_names_is_an_error():
    spec = sl.parse_spec(SPEC_TEXT)
    state = live(("pre-launch", "000000", ""), ("prod-blocking", "000000", ""))
    with pytest.raises(sl.SpecError, match="pre-launch, prod-blocking"):
        sl.plan(spec, state)


def test_previous_and_target_both_live_is_an_error():
    spec = sl.parse_spec(SPEC_TEXT)
    state = live(("bug", "000000", ""), ("kind:bug", "000000", ""))
    with pytest.raises(sl.SpecError, match="'kind:bug' and its previous name 'bug'"):
        sl.plan(spec, state)


def test_ops_run_in_rename_create_update_delete_order():
    spec = sl.parse_spec(SPEC_TEXT)
    state = live(
        ("cuj", "000000", ""),
        ("area:cache", "1d76db", "old"),
        ("bug", "d73a4a", "Something does not behave as documented"),
    )
    kinds = [type(op) for op in sl.plan(spec, state).ops]
    # Renames and creates/updates interleave per label in spec order; deletes last.
    assert kinds == [sl.Rename, sl.Update, sl.Create, sl.Delete]


# ---- gh command lines and apply --------------------------------------------


def test_gh_args_per_op():
    label = sl.Label("area:cache", "1d76db", "Cache engine")
    repo = "decdn/decdn"
    assert sl.gh_args(sl.Rename("bug", "kind:bug"), repo) == [
        "gh", "label", "edit", "bug", "--name", "kind:bug", "--repo", repo,
    ]  # fmt: skip
    assert sl.gh_args(sl.Create(label), repo) == [
        "gh", "label", "create", "area:cache",
        "--color", "1d76db", "--description", "Cache engine", "--repo", repo,
    ]  # fmt: skip
    assert sl.gh_args(sl.Update(label, sl.Live("area:cache", "000000", "x")), repo) == [
        "gh", "label", "edit", "area:cache",
        "--color", "1d76db", "--description", "Cache engine", "--repo", repo,
    ]  # fmt: skip
    assert sl.gh_args(sl.Delete("cuj"), repo) == [
        "gh", "label", "delete", "cuj", "--yes", "--repo", repo,
    ]  # fmt: skip


def test_apply_runs_every_op_through_the_runner_in_order():
    calls: list[list[str]] = []
    ops = [sl.Rename("bug", "kind:bug"), sl.Delete("cuj")]
    sl.apply(ops, "o/r", runner=calls.append)
    assert [c[2] for c in calls] == ["edit", "delete"]


def test_parse_live_tolerates_null_description_and_uppercase_color():
    rows = sl.parse_live('[{"name":"a","color":"ABCDEF","description":null}]')
    assert rows == [sl.Live("a", "abcdef", "")]


# ---- the real spec ----------------------------------------------------------

FLAT_ALLOWED = {
    "good first issue", "help wanted",  # GitHub discovery features
    "duplicate", "invalid", "wontfix",  # closure reasons
    "dependencies", "rust", "github_actions", "docker",  # Dependabot
}  # fmt: skip
FAMILIES = ("kind:", "area:", "prio:", "status:", "design:")


def test_real_spec_parses_and_every_name_is_prefixed_or_allowed_flat():
    spec = sl.load_spec(SPEC_PATH)
    for label in spec.labels:
        assert label.name in FLAT_ALLOWED or label.name.startswith(FAMILIES), label.name
    assert FLAT_ALLOWED <= spec.names, "a kept flat label went missing"


def test_real_spec_uses_one_color_per_family():
    spec = sl.load_spec(SPEC_PATH)
    by_family: dict[str, set[str]] = {}
    for label in spec.labels:
        fam = label.name.split(":", 1)[0] if ":" in label.name else None
        if fam in ("area", "design"):
            by_family.setdefault(fam, set()).add(label.color)
    assert all(len(colors) == 1 for colors in by_family.values()), by_family


def test_real_spec_retires_nothing_that_the_issue_forms_still_apply():
    """A form's `labels:` must name a live label, or GitHub drops it silently."""
    spec = sl.load_spec(SPEC_PATH)
    forms = (REPO_ROOT / ".github/ISSUE_TEMPLATE").glob("*.yml")
    import re

    applied: set[str] = set()
    for form in forms:
        text = form.read_text(encoding="utf-8")
        m = re.search(r"^labels:\s*\[(.*?)\]", text, re.MULTILINE)
        if m:
            applied |= {s.strip().strip("\"'") for s in m.group(1).split(",") if s.strip()}
    missing = applied - spec.names
    assert not missing, f"issue forms apply labels the spec does not define: {missing}"
