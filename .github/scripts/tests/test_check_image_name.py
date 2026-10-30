"""check-image-name.sh fails when an image name drifts between the three places it lives.

The script reads the real files, so each test copies them into a throwaway git
repository (it `cd`s to `git rev-parse --show-toplevel`), breaks one thing, and
expects the named error. A green run on the untouched copy pins the fixture.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT = REPO_ROOT / ".github/scripts/check-image-name.sh"
FILES = (
    ".github/workflows/release.yml",
    ".github/workflows/security.yml",
    "scripts/sign-release.sh",
)


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    for rel in FILES:
        dst = tmp_path / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(REPO_ROOT / rel, dst)
    (tmp_path / "docker").mkdir()
    for overview in (REPO_ROOT / "docker").glob("README.*.md"):
        shutil.copy(overview, tmp_path / "docker" / overview.name)
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    return tmp_path


def run(repo: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", str(SCRIPT)], cwd=repo, capture_output=True, text=True, check=False
    )


def rewrite(repo: Path, rel: str, old: str, new: str) -> None:
    path = repo / rel
    text = path.read_text()
    assert text.count(old) == 1, (rel, old, text.count(old))
    path.write_text(text.replace(old, new))


def test_the_untouched_copy_passes(repo: Path) -> None:
    result = run(repo)
    assert result.returncode == 0, result.stderr
    assert "every image has an overview file" in result.stdout


def test_swapped_sponsord_images_in_the_release_matrix_are_an_error(repo: Path) -> None:
    rewrite(
        repo,
        ".github/workflows/release.yml",
        "            image: ${{ github.repository_owner }}/sponsord\n",
        "            image: ${{ github.repository_owner }}/sponsord-SWAP\n",
    )
    rewrite(
        repo,
        ".github/workflows/release.yml",
        "            image: ${{ github.repository_owner }}/sponsord-onramp\n",
        "            image: ${{ github.repository_owner }}/sponsord\n",
    )
    rewrite(
        repo,
        ".github/workflows/release.yml",
        "            image: ${{ github.repository_owner }}/sponsord-SWAP\n",
        "            image: ${{ github.repository_owner }}/sponsord-onramp\n",
    )
    result = run(repo)
    assert result.returncode == 1
    assert "target sponsord pushes to" in result.stderr, result.stderr
    assert "target sponsord-onramp pushes to" in result.stderr, result.stderr


def test_an_image_missing_from_the_signing_list_is_an_error(repo: Path) -> None:
    rewrite(
        repo,
        "scripts/sign-release.sh",
        'IMAGES=( "decdn-node" "decdn" "sponsord" "sponsord-onramp" )',
        'IMAGES=( "decdn-node" "decdn" "sponsord" )',
    )
    result = run(repo)
    assert result.returncode == 1
    assert "scripts/sign-release.sh has no line: IMAGES=(" in result.stderr, result.stderr


def test_an_image_missing_from_the_sbom_matrix_is_an_error(repo: Path) -> None:
    rewrite(
        repo,
        ".github/workflows/release.yml",
        "target: [decdn-node, decdn, sponsord, sponsord-onramp]",
        "target: [decdn-node, decdn, sponsord]",
    )
    result = run(repo)
    assert result.returncode == 1
    assert "release.yml has no line: target: [decdn-node" in result.stderr, result.stderr


def test_an_image_missing_from_the_attach_loop_is_an_error(repo: Path) -> None:
    rewrite(
        repo,
        ".github/workflows/release.yml",
        "for t in decdn-node decdn sponsord sponsord-onramp; do",
        "for t in decdn-node decdn sponsord; do",
    )
    result = run(repo)
    assert result.returncode == 1
    assert "release.yml has no line: for t in decdn-node" in result.stderr, result.stderr


def test_a_missing_overview_file_is_an_error(repo: Path) -> None:
    (repo / "docker/README.sponsord-onramp.md").unlink()
    result = run(repo)
    assert result.returncode == 1
    assert "stage sponsord-onramp has no Docker Hub overview" in result.stderr, result.stderr


def test_an_image_missing_from_the_scan_matrix_is_an_error(repo: Path) -> None:
    rewrite(
        repo,
        ".github/workflows/security.yml",
        "          - ghcr.io/${{ github.repository_owner }}/sponsord\n",
        "",
    )
    result = run(repo)
    assert result.returncode == 1
    assert "security.yml has no line: - ghcr.io/${{ github.repository_owner }}/sponsord" in result.stderr, result.stderr


def test_a_missing_workflow_file_is_named(repo: Path) -> None:
    (repo / ".github/workflows/security.yml").unlink()
    result = run(repo)
    assert result.returncode == 1
    assert "security.yml not found" in result.stderr, result.stderr
