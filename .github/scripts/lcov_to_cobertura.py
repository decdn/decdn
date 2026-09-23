#!/usr/bin/env python3
"""Convert an LCOV coverage report to Cobertura XML.

GitHub Code Quality accepts coverage only as Cobertura XML, and `forge
coverage` writes LCOV only. This script is the bridge that lets the
`solidity-coverage` CI job upload contract coverage beside the Rust report
(`cargo llvm-cov` writes Cobertura natively).

Forge runs from `contracts/`, so its `SF:` paths are relative to that
directory. Code Quality matches files by their repo-relative path, and a
report whose paths match nothing is accepted without error but annotates no
file. `--path-prefix` joins the prefix onto every `SF:` path so the report
lands on the real files.

Only `SF`, `DA` and `BRDA` records are read. A line that appears in more than
one `DA` record keeps the highest hit count. A `BRDA` taken count of `-`
(the block never ran) counts as zero, and a `BRDA` line with no `DA` record
takes its hit count from its most-taken branch. An input with no `SF` record is an
error: an empty report must fail the step, not upload as "0 of 0 lines".
"""

from __future__ import annotations

import argparse
import sys
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import PurePosixPath


@dataclass
class FileCoverage:
    """Line hits and per-line branch outcomes for one source file."""

    lines: dict[int, int] = field(default_factory=dict)
    # line -> [taken count per branch]
    branches: dict[int, list[int]] = field(default_factory=dict)


def parse_lcov(text: str) -> dict[str, FileCoverage]:
    """Parse LCOV text into per-file coverage, keyed by the `SF:` path."""
    files: dict[str, FileCoverage] = {}
    current: FileCoverage | None = None
    for raw in text.splitlines():
        line = raw.strip()
        if line.startswith("SF:"):
            current = files.setdefault(line[3:], FileCoverage())
        elif line == "end_of_record":
            current = None
        elif current is None:
            continue
        elif line.startswith("DA:"):
            fields = line[3:].split(",")
            lineno, hits = int(fields[0]), int(fields[1])
            current.lines[lineno] = max(current.lines.get(lineno, 0), hits)
        elif line.startswith("BRDA:"):
            fields = line[5:].split(",")
            lineno, taken = int(fields[0]), fields[3]
            current.branches.setdefault(lineno, []).append(0 if taken == "-" else int(taken))
    return files


def _rate(covered: int, valid: int) -> str:
    return str(covered / valid) if valid else "1.0"


def _set_totals(elem: ET.Element, lc: int, lv: int, bc: int, bv: int) -> None:
    elem.set("line-rate", _rate(lc, lv))
    elem.set("branch-rate", _rate(bc, bv))
    elem.set("complexity", "0")


def convert(text: str, path_prefix: str = "") -> ET.ElementTree:
    """Build a Cobertura document from LCOV text.

    Raises ValueError when the input holds no `SF:` record.
    """
    files = parse_lcov(text)
    if not files:
        raise ValueError("LCOV input has no SF: records")

    packages: dict[str, list[tuple[str, FileCoverage]]] = {}
    for path, cov in sorted(files.items()):
        filename = str(PurePosixPath(path_prefix) / path) if path_prefix else path
        package = str(PurePosixPath(filename).parent).replace("/", ".")
        packages.setdefault(package, []).append((filename, cov))

    root = ET.Element("coverage")
    ET.SubElement(ET.SubElement(root, "sources"), "source").text = "."
    packages_el = ET.SubElement(root, "packages")

    total = [0, 0, 0, 0]  # lines covered, lines valid, branches covered, branches valid
    for package, members in packages.items():
        pkg_el = ET.SubElement(packages_el, "package", name=package)
        classes_el = ET.SubElement(pkg_el, "classes")
        pkg_total = [0, 0, 0, 0]
        for filename, cov in members:
            cls_el = ET.SubElement(
                classes_el, "class", name=PurePosixPath(filename).name, filename=filename
            )
            ET.SubElement(cls_el, "methods")
            lines_el = ET.SubElement(cls_el, "lines")
            cls_total = [0, 0, 0, 0]
            for lineno in sorted(cov.lines.keys() | cov.branches.keys()):
                # A branch line with no DA record ran if any of its branches did.
                hits = cov.lines.get(lineno, max(cov.branches.get(lineno, [0])))
                line_el = ET.SubElement(lines_el, "line", number=str(lineno), hits=str(hits))
                cls_total[0] += hits > 0
                cls_total[1] += 1
                taken = cov.branches.get(lineno)
                if taken:
                    covered = sum(t > 0 for t in taken)
                    pct = covered * 100 // len(taken)
                    line_el.set("branch", "true")
                    line_el.set("condition-coverage", f"{pct}% ({covered}/{len(taken)})")
                    cls_total[2] += covered
                    cls_total[3] += len(taken)
                else:
                    line_el.set("branch", "false")
            _set_totals(cls_el, *cls_total)
            pkg_total = [a + b for a, b in zip(pkg_total, cls_total)]
        _set_totals(pkg_el, *pkg_total)
        total = [a + b for a, b in zip(total, pkg_total)]

    _set_totals(root, *total)
    root.set("lines-covered", str(total[0]))
    root.set("lines-valid", str(total[1]))
    root.set("branches-covered", str(total[2]))
    root.set("branches-valid", str(total[3]))
    root.set("timestamp", str(int(time.time())))
    root.set("version", "lcov_to_cobertura")
    return ET.ElementTree(root)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("lcov", help="input LCOV file")
    parser.add_argument("output", help="output Cobertura XML file")
    parser.add_argument(
        "--path-prefix",
        default="",
        help="directory joined onto every SF: path to make it repo-relative",
    )
    args = parser.parse_args(argv)

    with open(args.lcov, encoding="utf-8") as f:
        text = f.read()
    try:
        tree = convert(text, args.path_prefix)
    except ValueError as e:
        print(f"error: {args.lcov}: {e}", file=sys.stderr)
        return 1
    ET.indent(tree)
    tree.write(args.output, encoding="utf-8", xml_declaration=True)
    root = tree.getroot()
    print(
        f"wrote {args.output}: {root.get('lines-covered')}/{root.get('lines-valid')} lines, "
        f"{root.get('branches-covered')}/{root.get('branches-valid')} branches"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
