"""Regression tests for the release-hardening checker.

The checker holds `release.yml` and `security.yml` to SHA-pinned actions,
checkouts that keep no token, and write grants only on the jobs its table
names. Each rule is read from text, not YAML, so a bug degrades to a quiet
false pass: the check reports a hardened workflow while a step runs a moved
tag under a write token.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
MODULE_PATH = REPO_ROOT / ".github/scripts/check_release_hardening.py"

_spec = importlib.util.spec_from_file_location("check_release_hardening", MODULE_PATH)
assert _spec and _spec.loader
crh = importlib.util.module_from_spec(_spec)
sys.modules["check_release_hardening"] = crh
_spec.loader.exec_module(crh)

SHA = "3d3c42e5aac5ba805825da76410c181273ba90b1"
OTHER_SHA = "c3c9e263c25d99ce0380d002d59b67737d91b0dc"

RELEASE = f"""\
name: Release
permissions:
  contents: read

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@{SHA} # v7.0.1
        with:
          persist-credentials: false
      - uses: ./.github/actions/setup
      - name: Build
        run: cargo build

  draft:
    needs: build
    permissions:
      contents: write
    steps:
      - uses: actions/download-artifact@{SHA} # v8.0.2
      - run: gh release create

  docker:
    needs: draft
    permissions:
      contents: read
      packages: write
    steps:
      - name: Check out
        uses: actions/checkout@{SHA}
        with:
          ref: main
          persist-credentials: false
      - uses: docker/build-push-action@{OTHER_SHA} # v7.4.0
"""

SECURITY = f"""\
name: Security
permissions:
  contents: read
jobs:
  scan:
    permissions:
      contents: read
      packages: read
    steps:
      - uses: actions/checkout@{SHA}
        with:
          persist-credentials: false
      - uses: aquasecurity/trivy-action@{OTHER_SHA} # v0.36.0
"""

ACTION = f"""\
name: Setup
runs:
  using: composite
  steps:
    - uses: actions/setup-node@{SHA} # v7.1.0
"""

TABLE = {
    ".github/workflows/release.yml": {"draft": {"contents"}, "docker": {"packages"}},
    ".github/workflows/security.yml": {},
}


def swap(text: str, old: str, new: str, count: int = -1) -> str:
    """`text.replace`, but a fixture edit that matches nothing fails the test
    rather than leaving the hardened fixture to pass vacuously."""
    assert old in text, f"fixture edit matched nothing: {old!r}"
    return text.replace(old, new, count)


@pytest.fixture(autouse=True)
def table(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(crh, "WRITE_JOBS", TABLE)


def build_repo(
    tmp_path: Path,
    *,
    release: str = RELEASE,
    security: str | None = SECURITY,
    action: str | None = ACTION,
) -> Path:
    workflows = tmp_path / ".github" / "workflows"
    workflows.mkdir(parents=True)
    (workflows / "release.yml").write_text(release)
    if security is not None:
        (workflows / "security.yml").write_text(security)
    if action is not None:
        path = tmp_path / ".github" / "actions" / "setup" / "action.yml"
        path.parent.mkdir(parents=True)
        path.write_text(action)
    return tmp_path


def write_action(repo: Path, name: str, body: str, filename: str = "action.yml") -> None:
    path = repo / ".github" / "actions" / name / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body)


def errors_for(tmp_path: Path, **kw: str | None) -> list[str]:
    errors, _ = crh.check(build_repo(tmp_path, **kw))
    return errors


def test_hardened_fixture_passes(tmp_path: Path) -> None:
    errors, total = crh.check(build_repo(tmp_path))
    assert errors == []
    # Five refs in release.yml, two in security.yml, one in the local action.
    assert total == 8


def test_real_repository_passes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.undo()
    errors, total = crh.check(REPO_ROOT)
    assert errors == []
    assert total > 0


# ---- rule 1: SHA pins ------------------------------------------------------


@pytest.mark.parametrize("ref", ["v7", "v7.0.1", "main", SHA[:39], SHA.upper()])
def test_tag_or_short_ref_fails(tmp_path: Path, ref: str) -> None:
    release = swap(RELEASE, f"docker/build-push-action@{OTHER_SHA}", f"docker/build-push-action@{ref}")
    errors = errors_for(tmp_path, release=release)
    assert len(errors) == 1
    assert "not pinned by a 40-hex commit SHA" in errors[0]
    assert f"docker/build-push-action@{ref}" in errors[0]


def test_quoted_tag_ref_fails(tmp_path: Path) -> None:
    release = swap(
        RELEASE,
        f"uses: docker/build-push-action@{OTHER_SHA}",
        "uses: 'docker/build-push-action@v7'",
    )
    errors = errors_for(tmp_path, release=release)
    assert any("docker/build-push-action@v7" in e for e in errors)


def test_subpath_action_pinned_by_sha_passes(tmp_path: Path) -> None:
    release = swap(
        RELEASE,
        f"docker/build-push-action@{OTHER_SHA}",
        f"github/codeql-action/init@{OTHER_SHA}",
    )
    assert errors_for(tmp_path, release=release) == []


def test_docker_ref_needs_a_digest(tmp_path: Path) -> None:
    tagged = swap(RELEASE, f"docker/build-push-action@{OTHER_SHA}", "docker://alpine:3.20")
    assert any("not pinned by @sha256: digest" in e for e in errors_for(tmp_path / "a", release=tagged))
    digested = swap(
        RELEASE,
        f"docker/build-push-action@{OTHER_SHA}",
        "docker://alpine@sha256:" + "a" * 64,
    )
    assert errors_for(tmp_path / "b", release=digested) == []


def test_commented_out_step_is_not_a_site(tmp_path: Path) -> None:
    release = swap(
        RELEASE,
        "      - run: gh release create\n",
        "      - run: gh release create\n      # - uses: someone/action@v1\n",
    )
    assert errors_for(tmp_path, release=release) == []


def test_unpinned_ref_in_local_action_fails(tmp_path: Path) -> None:
    action = swap(ACTION, f"@{SHA} # v7.1.0", "@v7.1.0")
    errors = errors_for(tmp_path, action=action)
    assert len(errors) == 1
    assert errors[0].startswith(".github/actions/setup/action.yml:5:")


def test_missing_local_action_fails(tmp_path: Path) -> None:
    errors = errors_for(tmp_path, action=None)
    assert errors == [".github/actions/setup: referenced as a local action, but has no action.yml"]


def test_file_with_no_uses_fails(tmp_path: Path) -> None:
    security = "permissions:\n  contents: read\njobs:\n  scan:\n    steps:\n      - run: true\n"
    errors = errors_for(tmp_path, security=security)
    assert errors == [".github/workflows/security.yml: no `uses:` step found — this check inspected nothing"]


def test_missing_workflow_fails(tmp_path: Path) -> None:
    errors = errors_for(tmp_path, security=None)
    assert errors == [".github/workflows/security.yml: not found — WRITE_JOBS names it; move the row with the file"]


# ---- rule 2: persist-credentials -------------------------------------------


def test_checkout_without_persist_false_fails(tmp_path: Path) -> None:
    security = swap(SECURITY, "        with:\n          persist-credentials: false\n", "")
    errors = errors_for(tmp_path, security=security)
    assert errors == [
        ".github/workflows/security.yml:10: actions/checkout does not set "
        "`persist-credentials: false` under `with:`"
    ]


def test_persist_true_fails(tmp_path: Path) -> None:
    security = swap(SECURITY, "persist-credentials: false", "persist-credentials: true")
    assert len(errors_for(tmp_path, security=security)) == 1


def test_persist_false_in_the_next_step_does_not_count(tmp_path: Path) -> None:
    # The first checkout has none; the second step's input must not satisfy it.
    security = swap(
        SECURITY,
        "        with:\n          persist-credentials: false\n"
        f"      - uses: aquasecurity/trivy-action@{OTHER_SHA} # v0.36.0\n",
        f"      - uses: aquasecurity/trivy-action@{OTHER_SHA} # v0.36.0\n"
        "        with:\n          persist-credentials: false\n",
    )
    errors = errors_for(tmp_path, security=security)
    assert len(errors) == 1
    assert "does not set `persist-credentials: false`" in errors[0]


def test_with_before_uses_in_the_same_step_counts(tmp_path: Path) -> None:
    release = swap(
        RELEASE,
        f"      - name: Check out\n        uses: actions/checkout@{SHA}\n"
        "        with:\n          ref: main\n          persist-credentials: false\n",
        "      - name: Check out\n        with:\n          persist-credentials: false\n"
        f"        uses: actions/checkout@{SHA}\n",
    )
    assert errors_for(tmp_path, release=release) == []


# ---- rule 3: workflow-level permissions ------------------------------------


def test_workflow_level_write_fails(tmp_path: Path) -> None:
    release = swap(RELEASE, "permissions:\n  contents: read\n", "permissions:\n  contents: write\n", 1)
    errors = errors_for(tmp_path, release=release)
    assert any("workflow-level `permissions:` grants write on contents" in e for e in errors)


def test_workflow_level_write_all_fails(tmp_path: Path) -> None:
    release = swap(RELEASE, "permissions:\n  contents: read\n", "permissions: write-all\n", 1)
    errors = errors_for(tmp_path, release=release)
    assert any("grants write on *" in e for e in errors)


def test_missing_workflow_level_permissions_fails(tmp_path: Path) -> None:
    release = swap(RELEASE, "permissions:\n  contents: read\n\n", "", 1)
    errors = errors_for(tmp_path, release=release)
    assert len(errors) == 1
    assert "no workflow-level `permissions:` block" in errors[0]


# ---- rule 4: job write grants ----------------------------------------------


def test_write_on_a_job_outside_the_table_fails(tmp_path: Path) -> None:
    release = swap(
        RELEASE,
        "  build:\n    runs-on: ubuntu-latest\n",
        "  build:\n    runs-on: ubuntu-latest\n    permissions:\n      packages: write\n",
    )
    errors = errors_for(tmp_path, release=release)
    assert errors == [
        ".github/workflows/release.yml: job `build` grants write on ['packages'], but "
        "WRITE_JOBS in check_release_hardening.py allows nothing"
    ]


def test_extra_scope_on_a_table_job_fails(tmp_path: Path) -> None:
    release = swap(
        RELEASE,
        "      contents: read\n      packages: write\n",
        "      contents: write\n      packages: write\n",
    )
    errors = errors_for(tmp_path, release=release)
    assert any("job `docker` grants write on ['contents', 'packages']" in e for e in errors)
    # And the contents-write job now runs a third-party action.
    assert any("job `docker` holds `contents: write` and runs `docker/build-push-action" in e
               for e in errors)


def test_stale_table_row_fails(tmp_path: Path) -> None:
    release = swap(RELEASE, "    permissions:\n      contents: write\n", "")
    errors = errors_for(tmp_path, release=release)
    assert errors == [
        ".github/workflows/release.yml: job `draft` grants write on nothing, but "
        "WRITE_JOBS in check_release_hardening.py allows ['contents']"
    ]


def test_table_job_missing_from_workflow_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(crh, "WRITE_JOBS", {
        **TABLE,
        ".github/workflows/release.yml": {**TABLE[".github/workflows/release.yml"], "attach": {"contents"}},
    })
    errors = errors_for(tmp_path)
    assert errors == [
        ".github/workflows/release.yml: WRITE_JOBS names job `attach`, which the workflow does not have"
    ]


def test_contents_write_job_running_third_party_action_fails(tmp_path: Path) -> None:
    release = swap(
        RELEASE,
        "      - run: gh release create\n",
        f"      - uses: someone/tool@{OTHER_SHA}\n      - run: gh release create\n",
    )
    errors = errors_for(tmp_path, release=release)
    assert len(errors) == 1
    assert "job `draft` holds `contents: write` and runs `someone/tool@" in errors[0]


def test_contents_write_job_running_local_action_fails(tmp_path: Path) -> None:
    release = swap(
        RELEASE,
        "      - run: gh release create\n",
        "      - uses: ./.github/actions/setup\n      - run: gh release create\n",
    )
    errors = errors_for(tmp_path, release=release)
    assert len(errors) == 1
    assert "runs `./.github/actions/setup`" in errors[0]


def test_write_in_security_workflow_fails(tmp_path: Path) -> None:
    security = swap(SECURITY, "      packages: read\n", "      packages: write\n")
    errors = errors_for(tmp_path, security=security)
    assert errors == [
        ".github/workflows/security.yml: job `scan` grants write on ['packages'], but "
        "WRITE_JOBS in check_release_hardening.py allows nothing"
    ]


# ---- shapes the text reader must not skip -------------------------------


def test_uses_key_with_a_space_before_the_colon_is_read(tmp_path: Path) -> None:
    release = swap(RELEASE, f"      - uses: docker/build-push-action@{OTHER_SHA} # v7.4.0\n",
                   "      - uses : docker/build-push-action@v7\n")
    errors = errors_for(tmp_path, release=release)
    assert any("`docker/build-push-action@v7` is not pinned" in e for e in errors), errors


def test_quoted_uses_key_is_read(tmp_path: Path) -> None:
    release = swap(RELEASE, f"      - uses: docker/build-push-action@{OTHER_SHA} # v7.4.0\n",
                   "      - \"uses\": docker/build-push-action@v7\n")
    errors = errors_for(tmp_path, release=release)
    assert any("`docker/build-push-action@v7` is not pinned" in e for e in errors), errors


def test_flow_style_step_is_an_error_not_a_skip(tmp_path: Path) -> None:
    release = swap(RELEASE, f"      - uses: docker/build-push-action@{OTHER_SHA} # v7.4.0\n",
                   "      - { uses: docker/build-push-action@v7 }\n")
    errors = errors_for(tmp_path, release=release)
    assert any("a `uses` key this check cannot read" in e for e in errors), errors


def test_four_space_job_indent_is_read(tmp_path: Path) -> None:
    security = (
        "permissions:\n    contents: read\njobs:\n    scan:\n        permissions:\n"
        "            packages: write\n        steps:\n"
        f"            - uses: aquasecurity/trivy-action@{OTHER_SHA}\n"
    )
    errors = errors_for(tmp_path, security=security)
    assert errors == [
        ".github/workflows/security.yml: job `scan` grants write on ['packages'], but "
        "WRITE_JOBS in check_release_hardening.py allows nothing"
    ]


def test_quoted_job_key_is_read(tmp_path: Path) -> None:
    security = swap(SECURITY, "  scan:\n", "  'scan':\n")
    security = swap(security, "      packages: read\n", "      packages: write\n")
    errors = errors_for(tmp_path, security=security)
    assert any("job `scan` grants write on ['packages']" in e for e in errors), errors


def test_line_under_jobs_that_is_not_a_job_is_an_error(tmp_path: Path) -> None:
    security = swap(SECURITY, "  scan:\n", "  scan: &anchor\n")
    errors = errors_for(tmp_path, security=security)
    assert any("is not a job key" in e for e in errors), errors


def test_empty_jobs_is_an_error(tmp_path: Path) -> None:
    security = (
        "permissions:\n  contents: read\njobs:\n"
        f"# - uses: aquasecurity/trivy-action@{OTHER_SHA}\n"
        f"steps:\n  - uses: aquasecurity/trivy-action@{OTHER_SHA}\n"
    )
    errors = errors_for(tmp_path, security=security)
    assert ".github/workflows/security.yml: `jobs:` holds no job" in errors, errors


def test_dash_offset_step_is_bounded(tmp_path: Path) -> None:
    # An earlier step's `persist-credentials: false` must not cover a later
    # checkout written with extra space after the dash.
    security = swap(
        SECURITY,
        f"      - uses: aquasecurity/trivy-action@{OTHER_SHA} # v0.36.0\n",
        f"      -   uses: actions/checkout@{SHA}\n",
    )
    errors = errors_for(tmp_path, security=security)
    assert errors == [
        ".github/workflows/security.yml:13: actions/checkout does not set "
        "`persist-credentials: false` under `with:`"
    ]


def test_dash_offset_step_with_its_own_persist_passes(tmp_path: Path) -> None:
    security = swap(
        SECURITY,
        f"      - uses: aquasecurity/trivy-action@{OTHER_SHA} # v0.36.0\n",
        f"      -   uses: actions/checkout@{SHA}\n          with:\n"
        "            persist-credentials: false\n",
    )
    assert errors_for(tmp_path, security=security) == []


def test_persist_false_outside_with_does_not_count(tmp_path: Path) -> None:
    security = swap(SECURITY, "        with:\n          persist-credentials: false\n",
                    "        env:\n          persist-credentials: false\n", 1)
    errors = errors_for(tmp_path, security=security)
    assert len(errors) == 1 and "under `with:`" in errors[0], errors


def test_quoted_false_counts(tmp_path: Path) -> None:
    security = swap(SECURITY, "persist-credentials: false", "persist-credentials: 'false'")
    assert errors_for(tmp_path, security=security) == []


def test_checkout_ref_case_does_not_skip_rule_2(tmp_path: Path) -> None:
    security = swap(SECURITY, f"      - uses: actions/checkout@{SHA}\n        with:\n"
                    "          persist-credentials: false\n",
                    f"      - uses: Actions/Checkout@{SHA}\n")
    errors = errors_for(tmp_path, security=security)
    assert len(errors) == 1 and "does not set `persist-credentials: false`" in errors[0], errors


def test_flow_mapping_write_on_a_job_fails(tmp_path: Path) -> None:
    security = swap(SECURITY, "    permissions:\n      contents: read\n      packages: read\n",
                    "    permissions: { contents: read, packages: write }\n")
    errors = errors_for(tmp_path, security=security)
    assert errors == [
        ".github/workflows/security.yml: job `scan` grants write on ['packages'], but "
        "WRITE_JOBS in check_release_hardening.py allows nothing"
    ]


def test_flow_mapping_write_at_workflow_level_fails(tmp_path: Path) -> None:
    security = swap(SECURITY, "permissions:\n  contents: read\njobs:",
                    "permissions: {contents: write}\njobs:")
    errors = errors_for(tmp_path, security=security)
    assert any("workflow-level `permissions:` grants write on contents" in e for e in errors), errors


def test_flow_mapping_read_passes(tmp_path: Path) -> None:
    security = swap(SECURITY, "permissions:\n  contents: read\njobs:",
                    "permissions: { contents: read }\njobs:")
    assert errors_for(tmp_path, security=security) == []


def test_empty_flow_mapping_grants_nothing(tmp_path: Path) -> None:
    security = swap(SECURITY, "    permissions:\n      contents: read\n      packages: read\n",
                    "    permissions: {}\n")
    assert errors_for(tmp_path, security=security) == []


@pytest.mark.parametrize("value", ["*grants", "${{ inputs.perms }}", "write"])
def test_unreadable_permissions_value_is_an_error(tmp_path: Path, value: str) -> None:
    security = swap(SECURITY, "    permissions:\n      contents: read\n      packages: read\n",
                    f"    permissions: {value}\n")
    errors = errors_for(tmp_path, security=security)
    assert errors == [
        ".github/workflows/security.yml:6: a `permissions:` value this check cannot read; "
        "write the grants as a block or a `{ scope: level }` mapping"
    ]


def test_job_level_write_all_fails(tmp_path: Path) -> None:
    security = swap(SECURITY, "    permissions:\n      contents: read\n      packages: read\n",
                    "    permissions: write-all\n")
    errors = errors_for(tmp_path, security=security)
    assert any("job `scan` grants write on ['*']" in e for e in errors), errors
    # write-all includes contents, so the actions-only rule applies too.
    assert any("job `scan` holds `contents: write` and runs `aquasecurity/" in e
               for e in errors), errors


# ---- local actions --------------------------------------------------------


def test_nested_local_action_is_checked(tmp_path: Path) -> None:
    repo = build_repo(tmp_path, action=ACTION + "    - uses: ./.github/actions/inner\n")
    write_action(repo, "inner", "runs:\n  steps:\n    - uses: someone/tool@v1\n")
    errors, _ = crh.check(repo)
    assert errors == [
        ".github/actions/inner/action.yml:3: `someone/tool@v1` is not pinned by a 40-hex "
        "commit SHA (write `owner/repo@<sha> # vX.Y.Z`)"
    ]


def test_self_referencing_local_action_terminates(tmp_path: Path) -> None:
    repo = build_repo(tmp_path, action=ACTION + "    - uses: ./.github/actions/setup/\n")
    errors, total = crh.check(repo)
    assert errors == []
    assert total == 9


def test_local_action_named_action_yaml_is_checked(tmp_path: Path) -> None:
    repo = build_repo(tmp_path, action=None)
    write_action(repo, "setup", swap(ACTION, f"@{SHA} # v7.1.0", "@v7.1.0"), "action.yaml")
    errors, _ = crh.check(repo)
    assert len(errors) == 1
    assert errors[0].startswith(".github/actions/setup/action.yaml:5:")


def test_checkout_in_local_action_needs_persist_false(tmp_path: Path) -> None:
    action = ACTION + f"    - uses: actions/checkout@{SHA}\n"
    errors = errors_for(tmp_path, action=action)
    assert errors == [
        ".github/actions/setup/action.yml:6: actions/checkout does not set "
        "`persist-credentials: false` under `with:`"
    ]


def test_checkout_outside_a_step_list_is_an_error(tmp_path: Path) -> None:
    # A `uses` that no step dash opens: its `with:` cannot be told from the
    # enclosing mapping's, so the check refuses it rather than guess.
    security = swap(
        SECURITY,
        "    steps:\n",
        f"    with:\n      persist-credentials: false\n    uses: actions/checkout@{SHA}\n    steps:\n",
    )
    errors = errors_for(tmp_path, security=security)
    assert errors == [
        ".github/workflows/security.yml:11: cannot find the step this `uses` belongs to"
    ]


def test_list_item_under_jobs_is_not_a_job(tmp_path: Path) -> None:
    security = swap(SECURITY, "  scan:\n", "  - scan:\n")
    errors = errors_for(tmp_path, security=security)
    assert any("`- scan:` is not a job key" in e for e in errors), errors


# ---- rule 5: images ----------------------------------------------------------

DIGEST = "sha256:" + "a" * 64


def with_docker_step(step: str) -> str:
    """RELEASE with one more step in the docker job, after its build step."""
    return swap(
        RELEASE,
        f"      - uses: docker/build-push-action@{OTHER_SHA} # v7.4.0\n",
        f"      - uses: docker/build-push-action@{OTHER_SHA} # v7.4.0\n{step}",
    )


QEMU = f"      - uses: docker/setup-qemu-action@{OTHER_SHA} # v4.4.0\n"
BUILDX = f"      - uses: docker/setup-buildx-action@{OTHER_SHA} # v4.4.1\n"


@pytest.mark.parametrize(
    "step",
    [
        QEMU,
        QEMU + "        with:\n          cache-image: false\n",
        QEMU + "        with:\n          image: tonistiigi/binfmt:qemu-v10.2.3\n",
        BUILDX,
        BUILDX + "        with:\n          driver-opts: image=moby/buildkit:v0.33.1\n",
        BUILDX + "        with:\n          driver-opts: network=host\n",
    ],
)
def test_image_action_without_a_digest_fails(tmp_path: Path, step: str) -> None:
    errors = errors_for(tmp_path, release=with_docker_step(step))
    assert len(errors) == 1, errors
    assert "is not pinned by @sha256: digest" in errors[0], errors


@pytest.mark.parametrize(
    "step",
    [
        QEMU + f"        with:\n          image: docker.io/tonistiigi/binfmt:qemu-v10.2.3@{DIGEST}\n",
        QEMU + f"        with:\n          image: 'tonistiigi/binfmt@{DIGEST}'\n",
        BUILDX + f"        with:\n          driver-opts: image=moby/buildkit:v0.33.1@{DIGEST}\n",
        BUILDX
        + "        with:\n          driver-opts: |\n            network=host\n"
        + f"            image=moby/buildkit@{DIGEST}\n",
    ],
)
def test_image_action_pinned_by_digest_passes(tmp_path: Path, step: str) -> None:
    assert errors_for(tmp_path, release=with_docker_step(step)) == []


def test_digest_in_another_input_does_not_count(tmp_path: Path) -> None:
    step = BUILDX + f"        with:\n          endpoint: image=moby/buildkit@{DIGEST}\n"
    errors = errors_for(tmp_path, release=with_docker_step(step))
    assert len(errors) == 1 and "`driver-opts` input" in errors[0], errors


@pytest.mark.parametrize("key", ["container", "services"])
def test_job_container_or_services_fails(tmp_path: Path, key: str) -> None:
    value = "redis:7" if key == "container" else "\n      db:\n        image: redis:7"
    security = swap(SECURITY, "  scan:\n", f"  scan:\n    {key}: {value}\n")
    errors = errors_for(tmp_path, security=security)
    assert errors == [
        f".github/workflows/security.yml:6: job `scan` sets `{key}:`, whose images this "
        "check does not read; run the image from a digest-pinned step instead"
    ]
