"""Regression tests for the LCOV → Cobertura converter.

Code Quality accepts any well-formed Cobertura document, so a converter bug
does not fail the upload: it shows the wrong coverage, or coverage on no file
at all. The tests pin the counts and the repo-relative filenames against
hand-written LCOV in the shape `forge coverage` emits.
"""

from __future__ import annotations

import importlib.util
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
MODULE_PATH = REPO_ROOT / ".github/scripts/lcov_to_cobertura.py"

_spec = importlib.util.spec_from_file_location("lcov_to_cobertura", MODULE_PATH)
assert _spec and _spec.loader
ltc = importlib.util.module_from_spec(_spec)
sys.modules["lcov_to_cobertura"] = ltc
_spec.loader.exec_module(ltc)


LCOV = """\
TN:
SF:src/Token.sol
FN:10,Token.mint
FNDA:4,Token.mint
DA:10,4
DA:11,4
DA:12,0
BRDA:11,0,0,4
BRDA:11,0,1,-
BRF:2
BRH:1
LF:3
LH:2
end_of_record
TN:
SF:src/lib/Math.sol
DA:5,1
DA:5,7
DA:6,0
LF:2
LH:1
end_of_record
TN:
SF:test/Token.t.sol
DA:3,1
end_of_record
"""


def _lines(root: ET.Element, filename: str) -> dict[int, ET.Element]:
    cls = root.find(f".//class[@filename='{filename}']")
    assert cls is not None, filename
    return {int(e.get("number", "0")): e for e in cls.iter("line")}


def test_totals_match_the_records() -> None:
    root = ltc.convert(LCOV).getroot()
    assert root.get("lines-covered") == "4"
    assert root.get("lines-valid") == "6"
    assert root.get("branches-covered") == "1"
    assert root.get("branches-valid") == "2"
    assert float(root.get("line-rate", "")) == pytest.approx(4 / 6)
    assert float(root.get("branch-rate", "")) == pytest.approx(0.5)


def test_duplicate_da_keeps_the_highest_hit_count() -> None:
    root = ltc.convert(LCOV).getroot()
    lines = _lines(root, "src/lib/Math.sol")
    assert sorted(lines) == [5, 6]
    assert lines[5].get("hits") == "7"


def test_brda_dash_counts_as_not_taken() -> None:
    root = ltc.convert(LCOV).getroot()
    lines = _lines(root, "src/Token.sol")
    assert lines[11].get("branch") == "true"
    assert lines[11].get("condition-coverage") == "50% (1/2)"
    assert lines[10].get("branch") == "false"


def test_branch_line_without_da_takes_its_most_taken_branch() -> None:
    lcov = "SF:a.sol\nBRDA:4,0,0,-\nBRDA:4,0,1,3\nend_of_record\n"
    root = ltc.convert(lcov).getroot()
    assert _lines(root, "a.sol")[4].get("hits") == "3"
    assert root.get("lines-covered") == "1"


def test_path_prefix_makes_filenames_repo_relative() -> None:
    root = ltc.convert(LCOV, "contracts").getroot()
    filenames = sorted(e.get("filename", "") for e in root.iter("class"))
    assert filenames == [
        "contracts/src/Token.sol",
        "contracts/src/lib/Math.sol",
        "contracts/test/Token.t.sol",
    ]


def test_files_group_into_one_package_per_directory() -> None:
    root = ltc.convert(LCOV, "contracts").getroot()
    packages = {
        p.get("name"): [c.get("filename") for c in p.iter("class")]
        for p in root.iter("package")
    }
    assert packages == {
        "contracts.src": ["contracts/src/Token.sol"],
        "contracts.src.lib": ["contracts/src/lib/Math.sol"],
        "contracts.test": ["contracts/test/Token.t.sol"],
    }


def test_records_outside_sf_are_ignored() -> None:
    lcov = "DA:1,1\nTN:\nSF:a.sol\nDA:2,0\nend_of_record\nDA:3,1\n"
    root = ltc.convert(lcov).getroot()
    assert sorted(_lines(root, "a.sol")) == [2]


@pytest.mark.parametrize("text", ["", "TN:\nend_of_record\n"], ids=["empty", "no-sf"])
def test_input_without_sf_records_is_an_error(text: str) -> None:
    with pytest.raises(ValueError, match="no SF"):
        ltc.convert(text)


def test_main_writes_parseable_xml(tmp_path: Path) -> None:
    src = tmp_path / "lcov.info"
    out = tmp_path / "cobertura.xml"
    src.write_text(LCOV)
    assert ltc.main([str(src), str(out), "--path-prefix", "contracts"]) == 0
    root = ET.parse(out).getroot()
    assert root.tag == "coverage"
    assert root.find("sources/source") is not None
    assert root.get("lines-valid") == "6"


def test_main_fails_on_empty_report(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    src = tmp_path / "lcov.info"
    src.write_text("")
    assert ltc.main([str(src), str(tmp_path / "out.xml")]) == 1
    assert "no SF" in capsys.readouterr().err
    assert not (tmp_path / "out.xml").exists()
