"""Regression tests for release.yml's per-artifact archive check.

The `upload-assets` step "Each archive comes from its own target's artifact"
runs only on a tag, so a regression would otherwise show up as a broken
release or not at all. These tests read the step's `run:` from release.yml,
run it the way `shell: bash` does, and feed it the layouts download-artifact
can produce: the real one, built from the build-binaries matrix, and each
layout the step must refuse.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
WORKFLOW = yaml.safe_load((REPO_ROOT / ".github/workflows/release.yml").read_text())
STEPS = {s.get("name"): s for s in WORKFLOW["jobs"]["upload-assets"]["steps"]}
CHECK = STEPS["Each archive comes from its own target's artifact"]
EXPECTED_ARCHIVES = int(STEPS["Generate checksum manifest"]["env"]["EXPECTED_ARCHIVES"])
VERSION = "0.0.0-test"


def real_layout() -> dict[str, list[str]]:
    """Each build leg's artifact directory mapped to the archives it uploads."""
    layout: dict[str, list[str]] = {}
    for leg in WORKFLOW["jobs"]["build-binaries"]["strategy"]["matrix"]["include"]:
        target, ext = leg["target"], leg["archive"]
        bins = [build.split(":", 1)[1] for build in leg["builds"].split()]
        layout[f"binaries-{target}"] = [f"{b}-{VERSION}-{target}.{ext}" for b in bins]
    return layout


def materialize(root: Path, layout: dict[str, list[str]]) -> Path:
    artifacts = root / "artifacts"
    artifacts.mkdir()
    for directory, names in layout.items():
        (artifacts / directory).mkdir()
        for name in names:
            (artifacts / directory / name).write_text(name)
    return artifacts


def run_check(artifacts: Path, tmp_path: Path) -> subprocess.CompletedProcess[str]:
    assert CHECK["shell"] == "bash"
    script = tmp_path / "step.sh"
    script.write_text(CHECK["run"])
    # GitHub runs `shell: bash` as `bash --noprofile --norc -eo pipefail {0}`.
    return subprocess.run(
        ["bash", "--noprofile", "--norc", "-eo", "pipefail", str(script)],
        cwd=artifacts,
        env={"PATH": "/usr/bin:/bin", "VERSION": VERSION},
        capture_output=True,
        text=True,
        check=False,
    )


def assert_refused(result: subprocess.CompletedProcess[str], message: str) -> None:
    assert result.returncode != 0, result.stdout
    assert message in result.stdout, result.stdout


def test_the_matrix_builds_the_expected_archive_count() -> None:
    assert sum(len(names) for names in real_layout().values()) == EXPECTED_ARCHIVES


def test_the_real_layout_passes_and_ends_flat(tmp_path: Path) -> None:
    layout = real_layout()
    artifacts = materialize(tmp_path, layout)
    result = run_check(artifacts, tmp_path)
    assert result.returncode == 0, result.stdout + result.stderr
    expected = sorted(name for names in layout.values() for name in names)
    assert sorted(p.name for p in artifacts.iterdir()) == expected
    assert all(p.is_file() for p in artifacts.iterdir())


def test_an_impostor_artifact_is_refused(tmp_path: Path) -> None:
    layout = real_layout()
    layout["binaries-evil"] = [f"decdn-{VERSION}-x86_64-unknown-linux-gnu.tar.gz"]
    result = run_check(materialize(tmp_path, layout), tmp_path)
    assert_refused(result, f"does not name version {VERSION} and target evil")


def test_a_file_naming_another_target_is_refused(tmp_path: Path) -> None:
    layout = real_layout()
    layout["binaries-x86_64-apple-darwin"].append(
        f"decdn-{VERSION}-aarch64-pc-windows-msvc.zip"
    )
    result = run_check(materialize(tmp_path, layout), tmp_path)
    assert_refused(result, "in artifact binaries-x86_64-apple-darwin does not name version")


def test_a_wrong_version_is_named_in_the_message(tmp_path: Path) -> None:
    layout = real_layout()
    layout["binaries-x86_64-apple-darwin"] = ["decdn-9.9.9-x86_64-apple-darwin.tar.gz"]
    result = run_check(materialize(tmp_path, layout), tmp_path)
    assert_refused(
        result,
        f"decdn-9.9.9-x86_64-apple-darwin.tar.gz in artifact binaries-x86_64-apple-darwin "
        f"does not name version {VERSION} and target x86_64-apple-darwin",
    )


def test_a_directory_not_named_binaries_is_refused(tmp_path: Path) -> None:
    layout = real_layout()
    layout["other"] = [f"decdn-{VERSION}-x86_64-unknown-linux-gnu.tar.gz"]
    result = run_check(materialize(tmp_path, layout), tmp_path)
    assert_refused(result, "artifact other is not named binaries-<target>")


def test_a_hidden_file_in_an_artifact_is_refused(tmp_path: Path) -> None:
    layout = real_layout()
    layout["binaries-x86_64-apple-darwin"].append(".hidden")
    result = run_check(materialize(tmp_path, layout), tmp_path)
    assert_refused(result, ".hidden in artifact binaries-x86_64-apple-darwin is hidden")


def test_a_hidden_archive_with_a_matching_suffix_is_refused(tmp_path: Path) -> None:
    # It would pass the suffix match, and the count and upload globs skip it.
    layout = real_layout()
    name = f".extra-{VERSION}-x86_64-apple-darwin.tar.gz"
    layout["binaries-x86_64-apple-darwin"].append(name)
    result = run_check(materialize(tmp_path, layout), tmp_path)
    assert_refused(result, f"{name} in artifact binaries-x86_64-apple-darwin is hidden")


def test_a_hidden_top_level_file_is_refused(tmp_path: Path) -> None:
    artifacts = materialize(tmp_path, real_layout())
    (artifacts / ".hidden").write_text("")
    assert_refused(run_check(artifacts, tmp_path), ".hidden is not an artifact directory")


def test_a_hidden_top_level_directory_is_refused(tmp_path: Path) -> None:
    layout = real_layout()
    layout[".binaries-x86_64-apple-darwin"] = []
    result = run_check(materialize(tmp_path, layout), tmp_path)
    assert_refused(result, "artifact .binaries-x86_64-apple-darwin is not named")


def test_a_flat_layout_is_refused(tmp_path: Path) -> None:
    # download-artifact extracts flat when one artifact matches the pattern,
    # or with `merge-multiple`.
    artifacts = tmp_path / "artifacts"
    artifacts.mkdir()
    for name in real_layout()["binaries-aarch64-pc-windows-msvc"]:
        (artifacts / name).write_text(name)
    assert_refused(run_check(artifacts, tmp_path), "is not an artifact directory")


def test_no_download_at_all_is_refused(tmp_path: Path) -> None:
    artifacts = materialize(tmp_path, {})
    assert_refused(run_check(artifacts, tmp_path), "no artifact directory was downloaded")


def test_an_empty_artifact_is_refused(tmp_path: Path) -> None:
    # With an extra, correctly targeted archive elsewhere, the count of 15
    # would still hold without the Windows ARM64 CLI.
    layout = real_layout()
    layout["binaries-aarch64-pc-windows-msvc"] = []
    layout["binaries-x86_64-pc-windows-msvc"].append(
        f"extra-{VERSION}-x86_64-pc-windows-msvc.zip"
    )
    result = run_check(materialize(tmp_path, layout), tmp_path)
    assert_refused(result, "artifact binaries-aarch64-pc-windows-msvc is empty")


def test_two_artifacts_carrying_one_name_are_refused(tmp_path: Path) -> None:
    # Reachable only through a target that ends in `-<version>-<other target>`,
    # but it is what stops `mv` overwriting a real archive if the name match is
    # ever loosened.
    impostor = f"a-{VERSION}-x86_64-apple-darwin"
    name = f"z-{VERSION}-{impostor}.tar.gz"
    layout = real_layout()
    layout["binaries-x86_64-apple-darwin"].append(name)
    layout[f"binaries-{impostor}"] = [name]
    result = run_check(materialize(tmp_path, layout), tmp_path)
    assert_refused(result, f"two artifacts carry {name}")


def test_an_artifact_with_an_empty_target_is_refused(tmp_path: Path) -> None:
    layout = real_layout()
    layout["binaries-"] = [f"decdn-{VERSION}-.zip"]
    result = run_check(materialize(tmp_path, layout), tmp_path)
    assert_refused(result, "artifact binaries- is not named binaries-<target>")


def test_a_symlinked_archive_is_refused(tmp_path: Path) -> None:
    # Once moved up, it would resolve to the x86_64 archive and be checksummed
    # under the aarch64 name.
    artifacts = materialize(tmp_path, real_layout())
    link = artifacts / "binaries-aarch64-apple-darwin" / f"decdn-{VERSION}-aarch64-apple-darwin.tar.gz"
    link.unlink()
    link.symlink_to(f"decdn-{VERSION}-x86_64-apple-darwin.tar.gz")
    assert_refused(run_check(artifacts, tmp_path), f"{link.name} in artifact "
                   "binaries-aarch64-apple-darwin is not a regular file")


def test_a_directory_named_like_an_archive_is_refused(tmp_path: Path) -> None:
    artifacts = materialize(tmp_path, real_layout())
    (artifacts / "binaries-x86_64-apple-darwin" / f"x-{VERSION}-x86_64-apple-darwin.zip").mkdir()
    assert_refused(run_check(artifacts, tmp_path), "is not a regular file")
