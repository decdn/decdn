"""Tests for dockerhub-description.py, the Docker Hub overview push.

Most tests run the script as a subprocess, from a copy inside a scratch git
repository, so `--ref` reads that repository's commits. Its Hub API is a fake
on localhost, reached through DECDN_DOCKERHUB_API, and its docker config is a
scratch DOCKER_CONFIG with fake credential helpers on PATH. The read-back and
helper-timeout tests load the real script in-process instead, to shorten its
delays, and the last test checks the checked-out docker/README.*.md files. No
test touches the real Docker Hub.
"""

from __future__ import annotations

import base64
import importlib.util
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import threading
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import ModuleType

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts/dockerhub-description.py"
# The fixture's own stages. The committed overviews are found by glob in the
# last test, so a new image's file is checked without an edit here.
STAGES = ["decdn-node", "decdn"]
USER = "hubuser"
SECRET = "dckr_pat_secret"
TOKEN = "access-token-1"
HUB_SERVER = "https://index.docker.io/v1/"


def overview(description: str, body: str = "# Image\n\nOverview text.\n") -> str:
    return f"<!-- description: {description} -->\n{body}"


def basic_auth(user: str, secret: str) -> str:
    return base64.b64encode(f"{user}:{secret}".encode()).decode()


def auths(user: str = USER, secret: str = SECRET) -> dict[str, object]:
    return {HUB_SERVER: {"auth": basic_auth(user, secret)}}


@dataclass
class FakeHub:
    """A Docker Hub API that keeps each repository's two fields in memory."""

    repos: dict[str, dict[str, str]] = field(default_factory=dict)
    requests: list[tuple[str, str, dict[str, str], dict[str, object]]] = field(
        default_factory=list
    )
    login_status: int = 200
    login_reply: dict[str, object] = field(default_factory=lambda: {"access_token": TOKEN})
    patch_status: int = 200
    # How many GETs fail with 503 before the fake answers normally.
    get_failures: int = 0

    def handler(self) -> type[BaseHTTPRequestHandler]:
        hub = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args: object) -> None:
                pass

            def reply(self, status: int, body: dict[str, object]) -> None:
                data = json.dumps(body).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def record(self) -> dict[str, object]:
                length = int(self.headers.get("Content-Length") or 0)
                body = json.loads(self.rfile.read(length)) if length else {}
                hub.requests.append((self.command, self.path, dict(self.headers), body))
                return body

            def repo(self) -> str | None:
                prefix = "/v2/repositories/"
                return self.path[len(prefix) :] if self.path.startswith(prefix) else None

            def do_POST(self) -> None:
                body = self.record()
                if self.path != "/v2/auth/token":
                    self.reply(404, {"message": "not found"})
                elif hub.login_status != 200:
                    self.reply(hub.login_status, {"message": "login failed"})
                elif body != {"identifier": USER, "secret": SECRET}:
                    self.reply(401, {"message": "incorrect authentication credentials"})
                else:
                    self.reply(200, hub.login_reply)

            def do_PATCH(self) -> None:
                body = self.record()
                repo = self.repo()
                if self.headers.get("Authorization") != f"Bearer {TOKEN}":
                    self.reply(401, {"message": "unauthorized"})
                elif hub.patch_status != 200:
                    self.reply(hub.patch_status, {"message": "patch failed"})
                elif repo not in hub.repos:
                    self.reply(404, {"message": "object not found"})
                else:
                    hub.repos[repo].update({k: str(v) for k, v in body.items()})
                    self.reply(200, hub.repos[repo])

            def do_GET(self) -> None:
                self.record()
                repo = self.repo()
                if hub.get_failures:
                    hub.get_failures -= 1
                    self.reply(503, {"message": "unavailable"})
                elif repo not in hub.repos:
                    self.reply(404, {"message": "object not found"})
                else:
                    self.reply(200, hub.repos[repo])

        return Handler


@dataclass
class Env:
    """A scratch repository, docker config and PATH around one fake Hub."""

    root: Path
    hub: FakeHub
    api: str

    @property
    def repo(self) -> Path:
        return self.root / "repo"

    @property
    def docker_config(self) -> Path:
        return self.root / "docker"

    @property
    def bin(self) -> Path:
        return self.root / "bin"

    def git(self, *args: str) -> None:
        subprocess.run(
            ["git", "-C", str(self.repo), *args],
            check=True,
            capture_output=True,
            env=self.env(),
        )

    def commit(self, files: dict[str, str]) -> None:
        for name, text in files.items():
            path = self.repo / "docker" / name
            path.parent.mkdir(parents=True, exist_ok=True)
            # newline="": write CRLF test input byte for byte.
            path.write_text(text, newline="")
        self.git("add", "-A")
        self.git("commit", "-q", "-m", "overviews")

    def config(self, config: object) -> None:
        (self.docker_config / "config.json").write_text(json.dumps(config))

    def helper(self, name: str, body: str) -> None:
        """Puts docker-credential-<name> on PATH, running the shell `body`
        after it has read the server address into $server."""
        script = self.bin / f"docker-credential-{name}"
        script.write_text(f'#!/bin/sh\n[ "$1" = get ] || exit 1\nread -r server\n{body}\n')
        script.chmod(0o755)

    def answering_helper(self, name: str, reply: dict[str, str] | None) -> None:
        """A helper that answers `get` for the Hub server address with `reply`,
        and reports a miss for anything else, the way docker's helpers do."""
        answer = json.dumps(reply) if reply else ""
        self.helper(
            name,
            f'[ "$server" = "{HUB_SERVER}" ] && [ -n \'{answer}\' ] || '
            "{ echo 'credentials not found in native keychain'; exit 1; }\n"
            f"printf '%s' '{answer}'",
        )

    def env(self, **extra: str) -> dict[str, str]:
        env = {
            k: v
            for k, v in os.environ.items()
            if not k.startswith(("DECDN_", "GIT_", "DOCKER_"))
        }
        env.update(
            HOME=str(self.root),
            GIT_CONFIG_NOSYSTEM="1",
            GIT_AUTHOR_NAME="t",
            GIT_AUTHOR_EMAIL="t@example.com",
            GIT_COMMITTER_NAME="t",
            GIT_COMMITTER_EMAIL="t@example.com",
            DOCKER_CONFIG=str(self.docker_config),
            DECDN_DOCKERHUB_API=self.api,
            PATH=f"{self.bin}{os.pathsep}{os.environ['PATH']}",
        )
        env.update(extra)
        return env

    def run(self, *args: str, **extra: str) -> subprocess.CompletedProcess[str]:
        targets = list(args) or [f"decdn/{s}={s}" for s in STAGES]
        # From the scratch root, not the repository: the script must find its
        # own checkout, because sign-release.sh calls it from a temp directory.
        return subprocess.run(
            [sys.executable, str(self.repo / "scripts/dockerhub-description.py"), *targets],
            cwd=self.root,
            env=self.env(**extra),
            capture_output=True,
            text=True,
            check=False,
        )


@pytest.fixture
def hub_env(tmp_path: Path) -> Iterator[Env]:
    hub = FakeHub(repos={f"decdn/{s}": {"description": "", "full_description": ""} for s in STAGES})
    server = ThreadingHTTPServer(("127.0.0.1", 0), hub.handler())
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    env = Env(tmp_path, hub, f"http://127.0.0.1:{server.server_address[1]}")
    for d in (env.repo / "scripts", env.docker_config, env.bin):
        d.mkdir(parents=True)
    shutil.copy(SCRIPT, env.repo / "scripts")
    env.git("init", "-q")
    env.commit({f"README.{s}.md": overview(f"The {s} image.") for s in STAGES})
    env.config({"auths": auths()})
    try:
        yield env
    finally:
        server.shutdown()
        server.server_close()


def calls(hub: FakeHub) -> list[tuple[str, str]]:
    return [(method, path) for method, path, _, _ in hub.requests]


def assert_clean_failure(result: subprocess.CompletedProcess[str], message: str) -> None:
    """A failure the operator can act on: exit 1, an `error:` line naming the
    cause, no traceback, and no secret."""
    assert result.returncode == 1, result.stdout + result.stderr
    assert "error: " in result.stderr
    assert message in result.stderr
    assert "Traceback" not in result.stderr
    assert SECRET not in result.stdout + result.stderr


# ---- the push --------------------------------------------------------------


def test_pushes_both_fields_and_reads_them_back_anonymously(hub_env: Env) -> None:
    result = hub_env.run()
    assert result.returncode == 0, result.stderr
    for s in STAGES:
        assert hub_env.hub.repos[f"decdn/{s}"] == {
            "description": f"The {s} image.",
            "full_description": "# Image\n\nOverview text.\n",
        }
    assert calls(hub_env.hub) == [
        ("POST", "/v2/auth/token"),
        ("PATCH", "/v2/repositories/decdn/decdn-node"),
        ("GET", "/v2/repositories/decdn/decdn-node"),
        ("PATCH", "/v2/repositories/decdn/decdn"),
        ("GET", "/v2/repositories/decdn/decdn"),
    ]
    for method, _, headers, _ in hub_env.hub.requests:
        if method == "GET":
            assert "Authorization" not in headers


def test_reads_the_overview_at_the_ref_not_the_working_tree(hub_env: Env) -> None:
    hub_env.git("tag", "v1")
    hub_env.commit({"README.decdn.md": overview("Committed after the tag.")})
    (hub_env.repo / "docker/README.decdn.md").write_text(overview("Uncommitted."))

    assert hub_env.run("--ref", "v1", "decdn/decdn=decdn").returncode == 0
    assert hub_env.hub.repos["decdn/decdn"]["description"] == "The decdn image."

    assert hub_env.run("decdn/decdn=decdn").returncode == 0
    assert hub_env.hub.repos["decdn/decdn"]["description"] == "Committed after the tag."


def test_one_failed_repository_does_not_stop_the_next(hub_env: Env) -> None:
    result = hub_env.run("decdn/missing=decdn", "decdn/decdn=decdn")
    assert_clean_failure(result, "HTTP 404")
    assert "not confirmed on Docker Hub: decdn/missing" in result.stderr
    assert "decdn/decdn: overview pushed" in result.stdout
    assert hub_env.hub.repos["decdn/decdn"]["description"] == "The decdn image."


def test_a_refused_patch_fails_with_the_status(hub_env: Env) -> None:
    hub_env.hub.patch_status = 403
    result = hub_env.run("decdn/decdn=decdn")
    assert_clean_failure(result, "PATCH")
    assert "HTTP 403" in result.stderr
    assert "written" not in result.stderr


# ---- the overview file -----------------------------------------------------


@pytest.mark.parametrize(
    ("text", "error"),
    [
        ("# No description line\n\nBody.\n", "line 1 must be"),
        (overview(""), "description on line 1 is empty"),
        (overview("   "), "description on line 1 is empty"),
        (overview("x" * 101), "101 characters"),
        (overview("Fine.", body="\n\n"), "overview after line 1 is empty"),
        (overview("Fine.", body="x" * 25_001), "25001 bytes"),
        # 12 501 characters but 25 002 bytes: the limit counts bytes.
        (overview("Fine.", body="é" * 12_501), "25002 bytes"),
    ],
)
def test_a_bad_file_stops_before_any_request(hub_env: Env, text: str, error: str) -> None:
    # The bad file is the second target, so the first one must stay untouched.
    hub_env.commit({"README.decdn.md": text})
    result = hub_env.run()
    assert_clean_failure(result, error)
    assert hub_env.hub.requests == []


@pytest.mark.parametrize(
    "text",
    [
        overview("x" * 100),
        overview("Fine.", body="é" * 12_500),  # exactly 25 000 bytes
    ],
)
def test_a_file_at_the_limits_is_accepted(hub_env: Env, text: str) -> None:
    hub_env.commit({"README.decdn.md": text})
    result = hub_env.run()
    assert result.returncode == 0, result.stderr


def test_line_1_and_the_blank_lines_after_it_are_stripped(hub_env: Env) -> None:
    hub_env.commit({"README.decdn.md": "<!-- description: CRLF file. -->\r\n\r\n\n# Image\r\n"})
    assert hub_env.run("decdn/decdn=decdn").returncode == 0
    # The file is read in text mode, so CRLF reaches Hub as LF.
    assert hub_env.hub.repos["decdn/decdn"] == {
        "description": "CRLF file.",
        "full_description": "# Image\n",
    }


def test_a_file_that_is_not_utf8_fails_cleanly(hub_env: Env) -> None:
    (hub_env.repo / "docker/README.decdn.md").write_bytes(b"<!-- description: x -->\n\xff\xfe\n")
    hub_env.git("add", "-A")
    hub_env.git("commit", "-q", "-m", "binary")
    result = hub_env.run("decdn/decdn=decdn")
    assert_clean_failure(result, "docker/README.decdn.md at HEAD is not UTF-8")
    assert hub_env.hub.requests == []


def test_a_missing_file_names_it(hub_env: Env) -> None:
    result = hub_env.run("decdn/other=other")
    assert_clean_failure(result, "docker/README.other.md")
    assert hub_env.hub.requests == []


@pytest.mark.parametrize("arg", ["decdn/decdn", "decdn=decdn", "decdn/decdn=", "Decdn/x=decdn"])
def test_a_malformed_target_is_rejected(hub_env: Env, arg: str) -> None:
    result = hub_env.run(arg)
    assert result.returncode == 2
    assert "<namespace>/<repo>=<stage>" in result.stderr


# ---- credentials -----------------------------------------------------------


def test_the_env_token_wins_over_docker_config(hub_env: Env) -> None:
    hub_env.config({"auths": auths(secret="stale-password")})
    result = hub_env.run(DECDN_DOCKERHUB_USERNAME=USER, DECDN_DOCKERHUB_TOKEN=SECRET)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("var", ["DECDN_DOCKERHUB_USERNAME", "DECDN_DOCKERHUB_TOKEN"])
def test_half_an_env_token_is_rejected(hub_env: Env, var: str) -> None:
    result = hub_env.run(**{var: "x"})
    assert_clean_failure(result, "set both")
    assert hub_env.hub.requests == []


@pytest.mark.parametrize("key", [HUB_SERVER, "index.docker.io"])
def test_a_cred_helpers_entry_wins_over_auths(hub_env: Env, key: str) -> None:
    hub_env.answering_helper("hub", {"Username": USER, "Secret": SECRET})
    hub_env.config({"credHelpers": {key: "hub"}, "auths": auths(secret="stale-password")})
    result = hub_env.run()
    assert result.returncode == 0, result.stderr


def test_a_creds_store_wins_over_auths(hub_env: Env) -> None:
    hub_env.answering_helper("store", {"ServerURL": "x", "Username": USER, "Secret": SECRET})
    hub_env.config({"credsStore": "store", "auths": auths(secret="stale-password")})
    result = hub_env.run()
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("config_key", ["credHelpers", "credsStore"])
def test_a_helper_miss_is_final(hub_env: Env, config_key: str) -> None:
    # A valid `auths` entry beside the helper is never used, as in docker.
    hub_env.answering_helper("empty", None)
    named = {HUB_SERVER: "empty"} if config_key == "credHelpers" else "empty"
    hub_env.config({config_key: named, "auths": auths()})
    result = hub_env.run()
    assert_clean_failure(result, "no Docker Hub credential found")
    assert "docker login docker.io" in result.stderr
    assert hub_env.hub.requests == []


def test_a_broken_helper_reports_its_own_error(hub_env: Env) -> None:
    hub_env.helper("broken", "echo 'Cannot autolaunch D-Bus without X11 $DISPLAY' >&2; exit 1")
    hub_env.config({"credsStore": "broken", "auths": auths()})
    result = hub_env.run()
    assert_clean_failure(result, "docker-credential-broken get failed (exit 1)")
    assert "Cannot autolaunch D-Bus" in result.stderr
    assert "no Docker Hub credential found" not in result.stderr


def test_another_registrys_credential_is_never_used(hub_env: Env) -> None:
    hub_env.config({"auths": {"ghcr.io": {"auth": basic_auth(USER, SECRET)}}})
    result = hub_env.run()
    assert_clean_failure(result, "no Docker Hub credential found")


def test_no_docker_config_at_all(hub_env: Env) -> None:
    (hub_env.docker_config / "config.json").unlink()
    result = hub_env.run()
    assert_clean_failure(result, "no Docker Hub credential found")


@pytest.mark.parametrize(
    ("setup", "message"),
    [
        (lambda e: e.config({"credsStore": "absent"}), "docker-credential-absent is not on PATH"),
        (
            lambda e: (e.docker_config / "config.json").write_text("{not json"),
            "is not valid JSON",
        ),
        (lambda e: e.config(["a", "list"]), "is not a JSON object"),
        (lambda e: e.config({"auths": ["a", "list"]}), "`auths` is not a JSON object"),
        (lambda e: e.config({"auths": {HUB_SERVER: {"auth": "!!!"}}}), "is not base64"),
        (
            lambda e: e.config({"auths": {HUB_SERVER: {"auth": base64.b64encode(b"x").decode()}}}),
            "is not `user:secret`",
        ),
        (
            lambda e: e.config({"auths": {HUB_SERVER: {"identitytoken": "t"}}}),
            "identity token",
        ),
        (
            lambda e: (
                e.answering_helper("tok", {"Username": "<token>", "Secret": "t"}),
                e.config({"credsStore": "tok"}),
            ),
            "identity token",
        ),
        (
            lambda e: (e.helper("garbled", "echo 'not json'"), e.config({"credsStore": "garbled"})),
            "not JSON",
        ),
        (
            lambda e: (
                e.helper("binary", "printf '\\377\\376'"),
                e.config({"credsStore": "binary"}),
            ),
            "docker-credential-binary printed something that is not UTF-8",
        ),
        (
            lambda e: (
                e.answering_helper("blank", {"Username": "", "Secret": SECRET}),
                e.config({"credsStore": "blank"}),
            ),
            "empty username or secret",
        ),
    ],
)
def test_a_bad_credential_source_fails_cleanly(
    hub_env: Env, setup: Callable[[Env], object], message: str
) -> None:
    setup(hub_env)
    result = hub_env.run()
    assert_clean_failure(result, message)
    assert hub_env.hub.requests == []


def test_a_hung_helper_times_out_cleanly(
    hub_env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = load_module()
    monkeypatch.setattr(module, "TIMEOUT", 0.5)
    hub_env.helper("hung", "sleep 10")
    hub_env.config({"credsStore": "hung"})
    # Env.env() drops the caller's DECDN_/DOCKER_ variables; drop them here too,
    # or an exported token would bypass the helper this test hangs.
    for key in list(os.environ):
        if key.startswith(("DECDN_", "DOCKER_")):
            monkeypatch.delenv(key)
    for key, value in hub_env.env().items():
        monkeypatch.setenv(key, value)
    with pytest.raises(module.Failure, match="gave no answer"):
        module.credential()


# ---- the login -------------------------------------------------------------


@pytest.mark.parametrize("status", [401, 403])
def test_a_refused_login_points_at_the_token_override(hub_env: Env, status: int) -> None:
    hub_env.hub.login_status = status
    result = hub_env.run()
    assert_clean_failure(result, "refused the login for hubuser")
    assert "DECDN_DOCKERHUB_TOKEN" in result.stderr
    assert calls(hub_env.hub) == [("POST", "/v2/auth/token")]


def test_a_failing_login_is_not_called_a_refusal(hub_env: Env) -> None:
    hub_env.hub.login_status = 503
    result = hub_env.run()
    assert_clean_failure(result, "HTTP 503")
    assert "refused" not in result.stderr
    assert "personal access token" not in result.stderr


def test_a_login_reply_without_a_token(hub_env: Env) -> None:
    hub_env.hub.login_reply = {"detail": "ok"}
    result = hub_env.run()
    assert_clean_failure(result, "carries no access_token")


def test_an_unreachable_hub_fails_cleanly(hub_env: Env) -> None:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    result = hub_env.run(DECDN_DOCKERHUB_API=f"http://127.0.0.1:{port}")
    assert_clean_failure(result, "POST http://127.0.0.1")
    assert "refused the login" not in result.stderr


@pytest.mark.parametrize(
    "api",
    [
        "https://hub.example.com",
        "https://hub.docker.com.example.com",
        "http://hub.docker.com",
        "http://hub.example.com",
        "ftp://127.0.0.1",
        "hub.docker.com",
    ],
)
def test_the_api_override_accepts_only_docker_hub_or_loopback(hub_env: Env, api: str) -> None:
    # The login sends the secret to this origin, so no other host may receive it.
    result = hub_env.run(DECDN_DOCKERHUB_API=api)
    assert_clean_failure(result, "accepts only https://hub.docker.com or a loopback")
    assert hub_env.hub.requests == []


def outputs(hub_env: Env) -> Iterator[str]:
    """The script's output on the success path and on two failure paths."""
    result = hub_env.run()
    yield result.stdout + result.stderr
    hub_env.hub.login_status = 401
    result = hub_env.run()
    yield result.stdout + result.stderr
    # Malformed JSON that still carries the secret: the parse error must not quote it.
    hub_env.helper("leaky", f"printf '%s' '{{\"Username\":\"{USER}\",\"Secret\":\"{SECRET}\"'")
    hub_env.config({"credsStore": "leaky"})
    result = hub_env.run()
    yield result.stdout + result.stderr


def test_no_secret_or_token_reaches_the_output(hub_env: Env) -> None:
    for out in outputs(hub_env):
        assert SECRET not in out
        assert TOKEN not in out


# ---- the read-back ---------------------------------------------------------


def load_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("dockerhub_description", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    # @dataclass resolves the module's annotations through sys.modules.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def stale_request(real, field: str, stale_reads: int | None = None):  # type: ignore[no-untyped-def]
    """Wraps `request` so the first `stale_reads` GETs (all, if None) serve an
    old value of `field`, the way a cached read-back would."""
    seen = 0

    def wrapped(method, url, body=None, token=None):  # type: ignore[no-untyped-def]
        nonlocal seen
        reply = real(method, url, body, token)
        if method == "GET" and (stale_reads is None or seen < stale_reads):
            seen += 1
            return {**reply, field: "old"}
        return reply

    return wrapped


@pytest.fixture
def module(monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    loaded = load_module()
    monkeypatch.setattr(loaded, "READBACK_DELAY", 0)
    return loaded


@pytest.mark.parametrize("field", ["description", "full_description"])
def test_a_read_back_that_never_matches_fails(
    hub_env: Env, module: ModuleType, monkeypatch: pytest.MonkeyPatch, field: str
) -> None:
    monkeypatch.setattr(module, "request", stale_request(module.request, field))
    with pytest.raises(module.Failure) as err:
        module.push(hub_env.api, TOKEN, "decdn/decdn", module.Overview("desc", "body text"))
    assert "written, but the anonymous read-back does not confirm it" in str(err.value)
    assert re.search(rf"\b{field} is 3 characters, \d+ written; first difference at offset 0", str(err.value))
    gets = [c for c in calls(hub_env.hub) if c[0] == "GET"]
    assert len(gets) == module.READBACK_ATTEMPTS


def test_a_read_back_that_catches_up_passes(
    hub_env: Env, module: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(module, "request", stale_request(module.request, "full_description", 2))
    module.push(hub_env.api, TOKEN, "decdn/decdn", module.Overview("d", "body"))
    gets = [c for c in calls(hub_env.hub) if c[0] == "GET"]
    assert len(gets) == 3


def test_a_read_back_that_errors_is_retried(hub_env: Env, module: ModuleType) -> None:
    hub_env.hub.get_failures = 2
    module.push(hub_env.api, TOKEN, "decdn/decdn", module.Overview("d", "body"))
    assert hub_env.hub.repos["decdn/decdn"]["description"] == "d"


def test_a_read_back_that_keeps_erroring_says_the_write_landed(
    hub_env: Env, module: ModuleType
) -> None:
    hub_env.hub.get_failures = 1_000
    with pytest.raises(module.Failure, match=r"written, but .*HTTP 503"):
        module.push(hub_env.api, TOKEN, "decdn/decdn", module.Overview("d", "body"))


# ---- the checked-out overviews ---------------------------------------------

# Every link form Hub's renderer resolves: inline, reference, and HTML.
LINK = re.compile(r"\]\(([^)]*)\)|^\s*\[[^\]]+\]:\s*(\S+)|(?:href|src)=\"([^\"]*)\"", re.M)


@pytest.mark.parametrize(
    "path", sorted(p.relative_to(REPO_ROOT) for p in (REPO_ROOT / "docker").glob("README.*.md"))
)
def test_the_checked_out_overviews_are_valid(path: Path) -> None:
    module = load_module()
    parsed = module.parse_overview((REPO_ROOT / path).read_text(), str(path))
    # Docker Hub resolves no relative link, so every link must be absolute.
    for match in LINK.finditer(parsed.full_description):
        target = next(g for g in match.groups() if g is not None)
        assert target.startswith("https://"), f"{path}: relative link {target!r}"


def test_the_link_check_sees_every_form() -> None:
    text = "[a](rel.md)\n[b]: ./ref.md\n<img src=\"x.png\">\n[c](https://ok)\n"
    found = [next(g for g in m.groups() if g is not None) for m in LINK.finditer(text)]
    assert found == ["rel.md", "./ref.md", "x.png", "https://ok"]
