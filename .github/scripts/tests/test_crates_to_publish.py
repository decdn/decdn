"""Regression tests for crates_to_publish.py.

The publish-crates workflow uploads what this plan says is pending and
excludes the rest. A wrong plan either uploads a crate twice, which cargo
refuses partway through a run, or leaves one out of the release for good.
crates.io is replaced by a table of HTTP statuses.
"""

from __future__ import annotations

import email.message
import http.client
import importlib.util
import io
import json
import sys
import urllib.error
from pathlib import Path
from types import SimpleNamespace

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
MODULE_PATH = REPO_ROOT / ".github/scripts/crates_to_publish.py"

_spec = importlib.util.spec_from_file_location("crates_to_publish", MODULE_PATH)
assert _spec and _spec.loader
ctp = importlib.util.module_from_spec(_spec)
sys.modules["crates_to_publish"] = ctp
_spec.loader.exec_module(ctp)

API = ctp.API


def metadata(*crates: tuple[str, str], unpublished: tuple[str, ...] = ()) -> dict:
    packages = [{"name": n, "version": v, "publish": None} for n, v in crates]
    packages += [{"name": n, "version": "0.0.0", "publish": []} for n in unpublished]
    return {"packages": packages}


def body_for(url: str) -> bytes:
    """The JSON crates.io serves for a crate or version URL."""
    parts = url.removeprefix(f"{API}/").split("/")
    if len(parts) == 1:
        return json.dumps({"crate": {"name": parts[0]}}).encode()
    return json.dumps({"version": {"crate": parts[0], "num": parts[1]}}).encode()


@pytest.fixture
def registry(monkeypatch):
    """crates.io as a dict from URL to status. A URL it does not hold is 404.
    A 200 carries the JSON crates.io serves for that URL, unless `bodies`
    holds another one."""
    statuses: dict[str, int] = {}
    asked: list[str] = []
    bodies: dict[str, bytes] = {}

    def http_get(url: str) -> tuple[int, bytes]:
        asked.append(url)
        status = statuses.get(url, 404)
        return status, (bodies.get(url, body_for(url)) if status == 200 else b"")

    monkeypatch.setattr(ctp, "http_get", http_get)
    monkeypatch.setattr(ctp.time, "sleep", lambda _s: None)
    return SimpleNamespace(statuses=statuses, asked=asked, bodies=bodies)


def exists(statuses: dict[str, int], name: str, *versions: str) -> None:
    statuses[f"{API}/{name}"] = 200
    for v in versions:
        statuses[f"{API}/{name}/{v}"] = 200


def test_crates_with_publish_false_are_not_planned():
    crates = ctp.publishable(metadata(("b", "1.0.0"), ("a", "1.0.0"), unpublished=("e2e",)))
    assert [c.name for c in crates] == ["a", "b"]


def test_a_crate_name_the_shell_would_split_is_refused():
    with pytest.raises(ctp.PlanError, match="unexpected crate name"):
        ctp.publishable(metadata(("a b", "1.0.0")))


def test_no_publishable_crate_is_an_error():
    with pytest.raises(ctp.PlanError, match="no publishable crate"):
        ctp.publishable(metadata(unpublished=("e2e",)))


def test_a_version_other_than_the_tags_is_refused_before_crates_io_is_asked(registry):
    asked = registry.asked
    crates = ctp.publishable(metadata(("a", "1.0.0"), ("b", "0.9.0")))

    with pytest.raises(ctp.PlanError, match="b=0.9.0"):
        ctp.plan(crates, "1.0.0")
    assert asked == []


def test_a_crate_crates_io_does_not_hold_is_refused(registry):
    statuses = registry.statuses
    exists(statuses, "a")
    crates = ctp.publishable(metadata(("a", "1.0.0"), ("new", "1.0.0")))

    with pytest.raises(ctp.PlanError, match="do not exist on crates.io.*: new"):
        ctp.plan(crates, "1.0.0")


def test_published_crates_are_excluded_and_the_rest_pending(registry):
    statuses = registry.statuses
    exists(statuses, "a", "1.0.0")
    exists(statuses, "b")
    exists(statuses, "c", "0.9.0")
    crates = ctp.publishable(metadata(("a", "1.0.0"), ("b", "1.0.0"), ("c", "1.0.0")))

    pending, published = ctp.plan(crates, "1.0.0")

    assert pending == ["b", "c"]
    assert published == ["a"]


def test_every_crate_published_leaves_nothing_pending(registry):
    statuses = registry.statuses
    exists(statuses, "a", "1.0.0")
    exists(statuses, "b", "1.0.0")
    crates = ctp.publishable(metadata(("a", "1.0.0"), ("b", "1.0.0")))

    assert ctp.plan(crates, "1.0.0") == ([], ["a", "b"])


@pytest.mark.parametrize("url_suffix", ["a", "a/1.0.0"])
def test_an_unexpected_status_is_refused(registry, url_suffix):
    statuses = registry.statuses
    exists(statuses, "a")
    statuses[f"{API}/{url_suffix}"] = 403
    crates = ctp.publishable(metadata(("a", "1.0.0")))

    with pytest.raises(ctp.PlanError, match="HTTP 403"):
        ctp.plan(crates, "1.0.0")


def test_outputs_name_the_excluded_crates(tmp_path, monkeypatch):
    out = tmp_path / "out"
    monkeypatch.setenv("GITHUB_OUTPUT", str(out))

    ctp.write_outputs(["b"], ["a", "c"])

    assert out.read_text() == "pending=true\nexclude=a c\n"


def test_nothing_pending_writes_pending_false(tmp_path, monkeypatch):
    out = tmp_path / "out"
    monkeypatch.setenv("GITHUB_OUTPUT", str(out))

    ctp.write_outputs([], ["a"])

    assert out.read_text().startswith("pending=false\n")


def test_main_plans_from_cargo_metadata(registry, monkeypatch, tmp_path, capsys):
    statuses = registry.statuses
    exists(statuses, "a", "1.0.0")
    exists(statuses, "b")
    out = tmp_path / "out"
    monkeypatch.setenv("GITHUB_OUTPUT", str(out))
    monkeypatch.setattr(ctp, "cargo_metadata", lambda: metadata(("a", "1.0.0"), ("b", "1.0.0")))

    assert ctp.main(["--version", "1.0.0"]) == 0
    assert out.read_text() == "pending=true\nexclude=a\n"
    assert "to publish at 1.0.0: b" in capsys.readouterr().out


def test_main_fails_on_a_plan_error(registry, monkeypatch, capsys):
    monkeypatch.setattr(ctp, "cargo_metadata", lambda: metadata(("a", "0.1.0")))

    assert ctp.main(["--version", "1.0.0"]) == 1
    assert "these crates disagree: a=0.1.0" in capsys.readouterr().err


def test_confirm_warns_about_unserved_crates_and_exits_zero(registry, monkeypatch, capsys):
    statuses = registry.statuses
    exists(statuses, "a", "1.0.0")
    monkeypatch.setattr(ctp, "cargo_metadata", lambda: metadata(("a", "1.0.0"), ("b", "1.0.0")))

    assert ctp.main(["--version", "1.0.0", "--confirm"]) == 0
    out = capsys.readouterr().out
    assert "https://crates.io/crates/a/1.0.0" in out
    assert "::warning::crates.io does not serve b 1.0.0 yet" in out
    assert "serve a " not in out


def test_confirm_says_when_crates_io_could_not_be_asked(monkeypatch, capsys):
    def unreachable(url: str) -> tuple[int, bytes]:
        raise ctp.PlanError(f"cannot reach crates.io for {url}: timed out")

    monkeypatch.setattr(ctp, "http_get", unreachable)
    monkeypatch.setattr(ctp.time, "sleep", lambda _s: None)
    monkeypatch.setattr(ctp, "cargo_metadata", lambda: metadata(("a", "1.0.0")))

    assert ctp.main(["--version", "1.0.0", "--confirm"]) == 0
    out = capsys.readouterr().out
    assert "::warning::could not ask crates.io about a 1.0.0: cannot reach" in out
    assert "index lag" not in out


def test_confirm_retries_a_crate_the_index_does_not_serve_yet(registry, monkeypatch, capsys):
    statuses, asked = registry.statuses, registry.asked
    url = f"{API}/a/1.0.0"
    monkeypatch.setattr(ctp, "cargo_metadata", lambda: metadata(("a", "1.0.0")))

    real = ctp.http_get

    def lagging(u: str) -> tuple[int, bytes]:
        result = real(u)
        statuses[url] = 200  # served from the second ask on
        return result

    monkeypatch.setattr(ctp, "http_get", lagging)

    assert ctp.main(["--version", "1.0.0", "--confirm"]) == 0
    out = capsys.readouterr().out
    assert "https://crates.io/crates/a/1.0.0" in out
    assert "::warning::" not in out
    assert asked.count(url) == 2


def test_confirm_exits_zero_when_cargo_metadata_fails(monkeypatch, capsys):
    def broken() -> dict:
        raise RuntimeError("cargo metadata exploded")

    monkeypatch.setattr(ctp, "cargo_metadata", broken)

    assert ctp.main(["--version", "1.0.0", "--confirm"]) == 0
    assert "::warning::could not confirm the upload on crates.io" in capsys.readouterr().out


def test_confirm_stops_at_its_time_budget(registry, monkeypatch, capsys):
    clock = iter([0.0, 0.0, ctp.CONFIRM_BUDGET + 1, ctp.CONFIRM_BUDGET + 1])
    monkeypatch.setattr(ctp.time, "monotonic", lambda: next(clock))
    monkeypatch.setattr(ctp, "cargo_metadata", lambda: metadata(("a", "1.0.0"), ("b", "1.0.0")))
    asked = registry.asked

    assert ctp.main(["--version", "1.0.0", "--confirm"]) == 0
    out = capsys.readouterr().out
    assert "crates.io does not serve a 1.0.0 yet" in out
    assert "crates.io does not serve b 1.0.0 yet" in out
    assert asked == [f"{API}/a/1.0.0"]


def test_main_fails_when_cargo_metadata_fails(monkeypatch, capsys):
    def broken() -> dict:
        raise json.JSONDecodeError("bad", "", 0)

    monkeypatch.setattr(ctp, "cargo_metadata", broken)

    assert ctp.main(["--version", "1.0.0"]) == 1
    assert "cannot read the workspace crates" in capsys.readouterr().err


@pytest.mark.parametrize("name", ["a*", "$(x)", "-x", "a;b", "a b", ""])
def test_a_crate_name_the_shell_could_misread_is_refused(name):
    with pytest.raises(ctp.PlanError, match="unexpected crate name"):
        ctp.publishable(metadata((name, "1.0.0")))


@pytest.mark.parametrize(
    "body",
    [
        b"<html>captive portal</html>",
        b'{"errors": [{"detail": "Not Found"}]}',
        b'{"version": {"crate": "a", "num": "0.9.0"}}',
        b'{"version": {"crate": "other", "num": "1.0.0"}}',
    ],
)
def test_a_200_that_does_not_describe_the_version_is_refused(registry, body):
    statuses = registry.statuses
    exists(statuses, "a", "1.0.0")
    registry.bodies[f"{API}/a/1.0.0"] = body

    with pytest.raises(ctp.PlanError, match="body that does not describe it"):
        ctp.plan(ctp.publishable(metadata(("a", "1.0.0"))), "1.0.0")


def test_a_200_that_does_not_describe_the_crate_is_refused(registry):
    statuses = registry.statuses
    exists(statuses, "a")
    registry.bodies[f"{API}/a"] = b'{"crate": {"name": "b"}}'

    with pytest.raises(ctp.PlanError, match="body that does not describe it"):
        ctp.plan(ctp.publishable(metadata(("a", "1.0.0"))), "1.0.0")


# ---- http_get, against a scripted urlopen ---------------------------------


class Ok:
    def __init__(self, body: bytes = b"{}") -> None:
        self.status = 200
        self._body = io.BytesIO(body)

    def read(self) -> bytes:
        return self._body.read()

    def __enter__(self) -> "Ok":
        return self

    def __exit__(self, *_exc: object) -> None:
        return None


def http_error(code: int) -> urllib.error.HTTPError:
    return urllib.error.HTTPError("u", code, "x", email.message.Message(), None)


@pytest.fixture
def urlopen(monkeypatch):
    """Scripts urlopen: each call takes the next answer (an Ok to return, or an
    exception to raise) and records the request."""
    answers: list[object] = []
    requests: list[object] = []
    sleeps: list[float] = []

    def fake(request, timeout):
        requests.append(request)
        answer = answers.pop(0)
        if isinstance(answer, BaseException):
            raise answer
        return answer

    monkeypatch.setattr(ctp.urllib.request, "urlopen", fake)
    monkeypatch.setattr(ctp.time, "sleep", sleeps.append)
    return answers, requests, sleeps


def test_a_404_is_returned_at_once(urlopen):
    answers, requests, sleeps = urlopen
    answers.append(http_error(404))

    assert ctp.http_get("https://x") == (404, b"")
    assert len(requests) == 1 and sleeps == []


def test_a_429_is_returned_without_a_retry(urlopen):
    # plan() then refuses on it: a rate-limited crates.io ends the run.
    answers, requests, _ = urlopen
    answers.append(http_error(429))

    assert ctp.http_get("https://x") == (429, b"")
    assert len(requests) == 1


def test_a_5xx_is_retried(urlopen):
    answers, requests, sleeps = urlopen
    answers.extend([http_error(503), Ok(b"body")])

    assert ctp.http_get("https://x") == (200, b"body")
    assert len(requests) == 2 and len(sleeps) == 1


@pytest.mark.parametrize(
    "failure",
    [
        http_error(503),
        urllib.error.URLError("no route"),
        TimeoutError("timed out"),
        http.client.IncompleteRead(b""),
    ],
)
def test_a_failure_that_persists_raises_after_every_attempt(urlopen, failure):
    answers, requests, _ = urlopen
    answers.extend([failure] * ctp.ATTEMPTS)

    with pytest.raises(ctp.PlanError, match="cannot reach crates.io for https://x"):
        ctp.http_get("https://x")
    assert len(requests) == ctp.ATTEMPTS


def test_every_request_names_its_caller(urlopen):
    answers, requests, _ = urlopen
    answers.append(Ok())

    ctp.http_get("https://x")

    assert requests[0].get_header("User-agent") == ctp.USER_AGENT
