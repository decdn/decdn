#!/usr/bin/env python3
"""Reconcile the repository's GitHub labels to `.github/labels.toml`.

The label set is held in a file so that a rename, a recolor or a new area
label is a reviewed change, not a click in the web UI that nobody else sees.
This script computes the operations that bring the live set in line with the
file, prints them, and applies them only when asked.

    [[label]]   name, color (6 hex digits), description, optional
                `previously = ["old name", …]`: live labels this one replaces.
    [[retired]] name: a live label to delete.

Operations, in the order they run:

    rename   a `previously` name is live and the new name is not
             (`gh label edit OLD --name NEW`, which keeps the label on every
             issue and PR that carries it)
    create   the name is not live under any spelling
    update   the name is live and its color or description differs
    delete   a `[[retired]]` name is live

A live label the file neither names nor retires is reported as "unmanaged"
and left alone, so a label that Dependabot or a GitHub feature creates is
never deleted by accident. Two `previously` names both live, or a
`previously` name and its target both live, is an error that names them: a
merge moves issues between labels, which is a human's job.

Default is a dry run. `--apply` executes through `gh`, which must be
authenticated for the repository (`gh auth status`).

Run: .github/scripts/sync-labels.sh [--apply] [--repo OWNER/NAME] [--spec PATH]
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

if sys.version_info < (3, 11):  # tomllib, and the syntax used below
    sys.exit(f"error: python 3.11+ required, found {sys.version.split()[0]}")

import tomllib  # noqa: E402  (must follow the version guard)

DEFAULT_REPO = "decdn/decdn"
DEFAULT_SPEC = Path(__file__).resolve().parents[1] / "labels.toml"
COLOR = re.compile(r"^[0-9a-f]{6}$")


class SpecError(ValueError):
    """`labels.toml` is malformed; the message says where."""


@dataclass(frozen=True)
class Label:
    name: str
    color: str
    description: str
    previously: tuple[str, ...] = ()


@dataclass(frozen=True)
class Spec:
    labels: tuple[Label, ...]
    retired: tuple[str, ...]

    @property
    def names(self) -> set[str]:
        return {label.name for label in self.labels}


@dataclass(frozen=True)
class Live:
    """One label as `gh label list --json name,color,description` reports it."""

    name: str
    color: str
    description: str


@dataclass(frozen=True)
class Rename:
    old: str
    new: str


@dataclass(frozen=True)
class Create:
    label: Label


@dataclass(frozen=True)
class Update:
    label: Label
    live: Live


@dataclass(frozen=True)
class Delete:
    name: str


Op = Rename | Create | Update | Delete


@dataclass
class Plan:
    ops: list[Op] = field(default_factory=list)
    unmanaged: list[str] = field(default_factory=list)


def _str_field(table: dict, key: str, where: str) -> str:
    value = table.get(key)
    if not isinstance(value, str) or not value:
        raise SpecError(f"{where}: `{key}` must be a non-empty string")
    return value


def parse_spec(text: str) -> Spec:
    """Parse and validate the TOML text of a label spec."""
    try:
        doc = tomllib.loads(text)
    except tomllib.TOMLDecodeError as exc:
        raise SpecError(f"labels.toml: {exc}") from exc

    labels: list[Label] = []
    seen: dict[str, str] = {}  # every name, live or previous -> the entry it belongs to
    for i, table in enumerate(doc.get("label", []), start=1):
        where = f"[[label]] #{i}"
        name = _str_field(table, "name", where)
        where = f"[[label]] {name!r}"
        color = _str_field(table, "color", where).lower()
        if not COLOR.match(color):
            raise SpecError(f"{where}: color {color!r} is not 6 hex digits")
        description = _str_field(table, "description", where)
        previously = table.get("previously", [])
        if not isinstance(previously, list) or not all(
            isinstance(p, str) and p for p in previously
        ):
            raise SpecError(f"{where}: `previously` must be a list of non-empty strings")
        for n in (name, *previously):
            if n in seen:
                raise SpecError(f"{where}: {n!r} already appears in {seen[n]}")
            seen[n] = where
        labels.append(Label(name, color, description, tuple(previously)))

    retired: list[str] = []
    for i, table in enumerate(doc.get("retired", []), start=1):
        name = _str_field(table, "name", f"[[retired]] #{i}")
        if name in seen:
            raise SpecError(f"[[retired]] {name!r} also appears in {seen[name]}")
        if name in retired:
            raise SpecError(f"[[retired]] {name!r} is listed twice")
        retired.append(name)

    if not labels:
        raise SpecError("labels.toml names no labels")
    return Spec(tuple(labels), tuple(retired))


def load_spec(path: Path) -> Spec:
    return parse_spec(path.read_text(encoding="utf-8"))


def parse_live(payload: str) -> list[Live]:
    """The JSON that `gh label list --json name,color,description` prints."""
    raw = json.loads(payload)
    return [Live(r["name"], str(r.get("color", "")).lower(), r.get("description") or "") for r in raw]


def plan(spec: Spec, live: list[Live]) -> Plan:
    """The operations that bring `live` in line with `spec`.

    Pure: no I/O. Raises `SpecError` when a rename is ambiguous.
    """
    by_name = {entry.name: entry for entry in live}
    out = Plan()
    managed: set[str] = set()

    for label in spec.labels:
        present_old = [p for p in label.previously if p in by_name]
        target_live = by_name.get(label.name)
        if len(present_old) > 1:
            raise SpecError(
                f"{label.name!r}: more than one previous name is live "
                f"({', '.join(present_old)}); merge them by hand first"
            )
        if present_old and target_live is not None:
            raise SpecError(
                f"{label.name!r} and its previous name {present_old[0]!r} are both live; "
                "move the issues off one of them by hand first"
            )
        if present_old:
            old = present_old[0]
            managed.add(old)
            out.ops.append(Rename(old, label.name))
            # After the rename the label carries the old color and description.
            target_live = Live(label.name, by_name[old].color, by_name[old].description)
        managed.add(label.name)
        if target_live is None:
            out.ops.append(Create(label))
        elif (target_live.color, target_live.description) != (label.color, label.description):
            out.ops.append(Update(label, target_live))

    for name in spec.retired:
        managed.add(name)
        if name in by_name:
            out.ops.append(Delete(name))

    out.unmanaged = sorted(n for n in by_name if n not in managed)
    return out


def describe(op: Op) -> str:
    match op:
        case Rename(old, new):
            return f"rename  {old!r} -> {new!r}"
        case Create(label):
            return f"create  {label.name!r}  #{label.color}  {label.description}"
        case Update(label, live):
            parts = []
            if live.color != label.color:
                parts.append(f"color #{live.color} -> #{label.color}")
            if live.description != label.description:
                parts.append(f"description {live.description!r} -> {label.description!r}")
            return f"update  {label.name!r}  " + "; ".join(parts)
        case Delete(name):
            return f"delete  {name!r}"
    raise AssertionError(f"unreachable: {op!r}")


def gh_args(op: Op, repo: str) -> list[str]:
    """The `gh` command line for one operation."""
    match op:
        case Rename(old, new):
            return ["gh", "label", "edit", old, "--name", new, "--repo", repo]
        case Create(label):
            return [
                "gh", "label", "create", label.name,
                "--color", label.color,
                "--description", label.description,
                "--repo", repo,
            ]  # fmt: skip
        case Update(label, _):
            return [
                "gh", "label", "edit", label.name,
                "--color", label.color,
                "--description", label.description,
                "--repo", repo,
            ]  # fmt: skip
        case Delete(name):
            return ["gh", "label", "delete", name, "--yes", "--repo", repo]
    raise AssertionError(f"unreachable: {op!r}")


Runner = Callable[[list[str]], None]


def run_checked(args: list[str]) -> None:
    subprocess.run(args, check=True)


def apply(ops: list[Op], repo: str, runner: Runner = run_checked) -> None:
    for op in ops:
        runner(gh_args(op, repo))


def fetch_live(repo: str) -> list[Live]:
    result = subprocess.run(
        [
            "gh", "label", "list",
            "--repo", repo,
            "--limit", "500",
            "--json", "name,color,description",
        ],  # fmt: skip
        check=True,
        capture_output=True,
        text=True,
    )
    return parse_live(result.stdout)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument("--apply", action="store_true", help="execute the operations through gh")
    parser.add_argument("--repo", default=DEFAULT_REPO, help=f"OWNER/NAME (default {DEFAULT_REPO})")
    parser.add_argument("--spec", type=Path, default=DEFAULT_SPEC, help="path to labels.toml")
    args = parser.parse_args(argv)

    try:
        spec = load_spec(args.spec)
        result = plan(spec, fetch_live(args.repo))
    except SpecError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except subprocess.CalledProcessError as exc:
        print(f"error: gh failed: {exc.stderr or exc}", file=sys.stderr)
        return 1

    for op in result.ops:
        print(describe(op))
    for name in result.unmanaged:
        print(f"unmanaged  {name!r}  (not in labels.toml; left alone)")
    if not result.ops:
        print(f"{args.repo}: labels match {args.spec.name}; nothing to do")
        return 0
    if not args.apply:
        print(f"\n{len(result.ops)} operation(s) planned for {args.repo}; re-run with --apply")
        return 0

    try:
        apply(result.ops, args.repo)
    except subprocess.CalledProcessError as exc:
        print(f"error: gh failed: {exc}", file=sys.stderr)
        return 1
    print(f"\napplied {len(result.ops)} operation(s) to {args.repo}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
