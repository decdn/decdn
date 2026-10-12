#!/usr/bin/env python3
"""Pushes each image's Docker Hub overview from docker/README.<stage>.md.

Docker Hub does not read the OCI labels on an image, and the release mirror
(`docker buildx imagetools create` in sign-release.sh) copies only the
manifest. A Hub repository's short description and overview are therefore
separate metadata, written here through the Hub API.

Usage:  scripts/dockerhub-description.py [--ref <git-ref>] <hub-repo>=<stage> ...
e.g.    scripts/dockerhub-description.py --ref v0.1.2 \\
            decdn/decdn-node=decdn-node decdn/decdn=decdn

Each overview is read from `docker/README.<stage>.md` at <git-ref> (default
HEAD), not from the working tree, so a release publishes the text as it stood
at its tag. Line 1 of the file is the Hub short description, written as
`<!-- description: ... -->`; the rest is the overview. Line 1, and any blank
lines right after it, are stripped before upload.

Credentials:
  DECDN_DOCKERHUB_USERNAME + DECDN_DOCKERHUB_TOKEN, when both are set: a
  personal access token with the Read, Write, Delete scope.
  Else the `docker login docker.io` credential in
  ${DOCKER_CONFIG:-~/.docker}/config.json, from the first of these that the
  config names: a `credHelpers` entry for Docker Hub, the `credsStore`, the
  `auths` entry. As in docker, a configured helper or store is final: a miss
  there does not fall through to `auths`.

After each write the repository is read back WITHOUT credentials, as a user's
browser would, and both fields must match what was sent. Every repository is
tried, and the exit status is non-zero if any one fails.

For a stable release, sign-release.sh runs this after the Docker Hub mirror.
It is also the command to run by hand: to fill a new repository, to push an
edit between releases, or to recover after sign-release.sh reports a failure
here.
"""

from __future__ import annotations

import argparse
import base64
import http.client
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path

# The checkout this script lives in. sign-release.sh calls it from a scratch
# directory, so git must not run in the current one.
REPO_ROOT = Path(__file__).resolve().parents[1]
# Docker's server address for Docker Hub: the `auths` key, the `credHelpers`
# key, and what a credential helper is asked for. A `credHelpers` entry under
# the bare hostname is accepted too.
HUB_SERVER = "https://index.docker.io/v1/"
HUB_HOSTS = (HUB_SERVER, "index.docker.io")
# What every docker credential helper prints when it holds nothing for the
# server. Any other failure is a broken helper, not a missing credential.
HELPER_MISS = "credentials not found"
# Docker's marker for an identity (OAuth) token in place of a username.
IDENTITY_TOKEN_USER = "<token>"
DEFAULT_API = "https://hub.docker.com"
# The test seam: tests point the script at a fake Hub on localhost. Anything
# else must be https, because the login carries the secret.
API_ENV = "DECDN_DOCKERHUB_API"
LOCAL_HOSTS = ("127.0.0.1", "localhost", "::1")
# Hub's own limits on the two fields.
MAX_DESCRIPTION = 100
MAX_FULL_DESCRIPTION = 25_000
DESCRIPTION_LINE = re.compile(r"<!-- description: (.*) -->")
REPO_NAME = re.compile(r"[a-z0-9][a-z0-9._-]*/[a-z0-9][a-z0-9._-]*")
STAGE_NAME = re.compile(r"[a-z0-9][a-z0-9._-]*")
TIMEOUT = 30
# The anonymous read-back can briefly serve the previous value or fail.
READBACK_ATTEMPTS = 5
READBACK_DELAY = 2.0
# How much of an error reply or helper stderr to show.
DETAIL_LIMIT = 300


class Failure(Exception):
    """An error the operator has to act on; printed without a traceback."""


class HttpFailure(Failure):
    """A request that got an HTTP error status back."""

    def __init__(self, message: str, status: int) -> None:
        super().__init__(message)
        self.status = status


@dataclass(frozen=True)
class Overview:
    """One repository's two Hub fields."""

    description: str
    full_description: str


def overview_path(stage: str) -> str:
    """The repo-relative overview file of a Dockerfile stage."""
    return f"docker/README.{stage}.md"


def parse_overview(text: str, source: str) -> Overview:
    """Splits an overview file into its short description and its body."""
    first, _, rest = text.partition("\n")
    match = DESCRIPTION_LINE.fullmatch(first.rstrip("\r"))
    if match is None:
        raise Failure(f"{source}: line 1 must be `<!-- description: ... -->`")
    description = match.group(1).strip()
    if not description:
        raise Failure(f"{source}: the description on line 1 is empty")
    if len(description) > MAX_DESCRIPTION:
        raise Failure(
            f"{source}: the description is {len(description)} characters; "
            f"Docker Hub allows {MAX_DESCRIPTION}"
        )
    body = rest.lstrip("\r\n")
    if not body.strip():
        raise Failure(f"{source}: the overview after line 1 is empty")
    size = len(body.encode())
    if size > MAX_FULL_DESCRIPTION:
        raise Failure(
            f"{source}: the overview is {size} bytes; Docker Hub allows {MAX_FULL_DESCRIPTION}"
        )
    return Overview(description, body)


def read_at_ref(ref: str, path: str) -> str:
    """The file at `path` as committed at `ref`."""
    result = subprocess.run(
        ["git", "-C", str(REPO_ROOT), "show", f"{ref}:{path}"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise Failure(f"cannot read {path} at {ref}: {result.stderr.strip()}")
    return result.stdout


def identity_token(source: str) -> Failure:
    """The error for a stored identity token, which the Hub login cannot use."""
    return Failure(
        f"{source} holds a docker identity token, which the Hub API does not "
        "accept. Set DECDN_DOCKERHUB_USERNAME and DECDN_DOCKERHUB_TOKEN to a "
        "personal access token with the Read, Write, Delete scope"
    )


def checked(username: object, secret: object, source: str) -> tuple[str, str]:
    """Rejects a stored credential the Hub login cannot use."""
    if username == IDENTITY_TOKEN_USER:
        raise identity_token(source)
    if not isinstance(username, str) or not username or not isinstance(secret, str) or not secret:
        raise Failure(f"{source} holds an empty username or secret")
    return username, secret


def from_helper(helper: str) -> tuple[str, str] | None:
    """Asks `docker-credential-<helper>` for the Hub credential. None means the
    helper holds nothing for Docker Hub."""
    program = f"docker-credential-{helper}"
    try:
        result = subprocess.run(
            [program, "get"],
            input=HUB_SERVER,
            capture_output=True,
            text=True,
            check=False,
            timeout=TIMEOUT,
        )
    except FileNotFoundError:
        raise Failure(
            f"docker config names the credential helper `{helper}`, but {program} is not on PATH"
        ) from None
    except subprocess.TimeoutExpired:
        raise Failure(
            f"{program} gave no answer in {TIMEOUT}s (is the keychain locked, "
            "or waiting for a passphrase?)"
        ) from None
    except OSError as err:
        raise Failure(f"cannot run {program}: {err}") from None
    if result.returncode != 0:
        if HELPER_MISS in result.stdout or HELPER_MISS in result.stderr:
            return None
        detail = (result.stderr.strip() or result.stdout.strip())[:DETAIL_LIMIT]
        raise Failure(f"{program} get failed (exit {result.returncode}): {detail}")
    # Helper stdout carries the secret, so it is never quoted in an error.
    try:
        reply = json.loads(result.stdout)
    except ValueError:
        raise Failure(f"{program} returned something that is not JSON") from None
    if not isinstance(reply, dict):
        raise Failure(f"{program} returned something that is not a JSON object")
    return checked(reply.get("Username"), reply.get("Secret"), program)


def docker_credential() -> tuple[str, str] | None:
    """The Hub credential `docker login docker.io` stored, if any."""
    path = Path(os.environ.get("DOCKER_CONFIG") or Path.home() / ".docker") / "config.json"
    try:
        config = json.loads(path.read_text())
    except FileNotFoundError:
        return None
    except OSError as err:
        raise Failure(f"cannot read {path}: {err}") from None
    except ValueError as err:
        raise Failure(f"{path} is not valid JSON: {err}") from None
    if not isinstance(config, dict):
        raise Failure(f"{path} is not a JSON object")

    def section(name: str) -> dict[str, object]:
        value = config.get(name) or {}
        if not isinstance(value, dict):
            raise Failure(f"{path}: `{name}` is not a JSON object")
        return value

    helpers = section("credHelpers")
    helper = next((helpers[h] for h in HUB_HOSTS if helpers.get(h)), None)
    if helper:
        return from_helper(str(helper))
    if config.get("credsStore"):
        return from_helper(str(config["credsStore"]))
    entry = section("auths").get(HUB_SERVER) or {}
    if not isinstance(entry, dict):
        raise Failure(f"{path}: the `auths` entry for {HUB_SERVER} is not a JSON object")
    encoded = entry.get("auth")
    if not encoded:
        if entry.get("identitytoken"):
            raise identity_token(path.name)
        return None
    try:
        username, sep, secret = base64.b64decode(str(encoded), validate=True).decode().partition(":")
    except ValueError:
        raise Failure(f"{path}: the `auths` entry for {HUB_SERVER} is not base64") from None
    if not sep:
        raise Failure(f"{path}: the `auths` entry for {HUB_SERVER} is not `user:secret`")
    return checked(username, secret, path.name)


def credential() -> tuple[str, str]:
    """The username and secret to log in to the Hub API with."""
    username = os.environ.get("DECDN_DOCKERHUB_USERNAME", "")
    token = os.environ.get("DECDN_DOCKERHUB_TOKEN", "")
    if username and token:
        return username, token
    if username or token:
        raise Failure("set both DECDN_DOCKERHUB_USERNAME and DECDN_DOCKERHUB_TOKEN, or neither")
    found = docker_credential()
    if found is None:
        raise Failure(
            "no Docker Hub credential found. Run `docker login docker.io`, or set "
            "DECDN_DOCKERHUB_USERNAME and DECDN_DOCKERHUB_TOKEN (a personal access "
            "token with the Read, Write, Delete scope)"
        )
    return found


def api_base() -> str:
    """The Hub API origin: DEFAULT_API, or the test seam's fake."""
    api = (os.environ.get(API_ENV) or DEFAULT_API).rstrip("/")
    parsed = urllib.parse.urlsplit(api)
    if parsed.scheme != "https" and not (
        parsed.scheme == "http" and parsed.hostname in LOCAL_HOSTS
    ):
        raise Failure(f"{API_ENV} must be an https:// URL (or http:// on localhost): {api!r}")
    return api


def request(
    method: str, url: str, body: dict[str, str] | None = None, token: str | None = None
) -> dict[str, object]:
    """Sends one JSON request and returns the JSON reply."""
    headers = {"Accept": "application/json"}
    data = None
    if body is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(body).encode()
    if token is not None:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
            reply = json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as err:
        detail = err.read().decode(errors="replace").strip()[:DETAIL_LIMIT]
        raise HttpFailure(f"{method} {url}: HTTP {err.code}: {detail}", err.code) from None
    except urllib.error.URLError as err:
        raise Failure(f"{method} {url}: {err.reason}") from None
    except (OSError, http.client.HTTPException) as err:
        # A stall or a dropped connection after the request was sent.
        raise Failure(f"{method} {url}: {type(err).__name__}: {err}") from None
    except ValueError:
        raise Failure(f"{method} {url}: the reply is not JSON") from None
    if not isinstance(reply, dict):
        raise Failure(f"{method} {url}: the reply is not a JSON object")
    return reply


def login(api: str, username: str, secret: str) -> str:
    """Exchanges the credential for a Hub API access token."""
    try:
        reply = request("POST", f"{api}/v2/auth/token", {"identifier": username, "secret": secret})
    except HttpFailure as err:
        if err.status not in (401, 403):
            raise
        raise Failure(
            f"Docker Hub refused the login for {username}: {err}\n"
            "A browser-based `docker login` can store a token the Hub API does not "
            "accept. Set DECDN_DOCKERHUB_USERNAME and DECDN_DOCKERHUB_TOKEN to a "
            "personal access token with the Read, Write, Delete scope."
        ) from None
    token = reply.get("access_token")
    if not isinstance(token, str) or not token:
        raise Failure("the Docker Hub login reply carries no access_token")
    return token


def mismatch(field: str, want: str, got: object) -> str:
    """Says where a read-back field departs from what was written."""
    if not isinstance(got, str):
        return f"{field} is {type(got).__name__}, not the {len(want)} characters written"
    at = next((i for i, (a, b) in enumerate(zip(want, got)) if a != b), min(len(want), len(got)))
    return (
        f"{field} is {len(got)} characters, {len(want)} written; first difference at "
        f"offset {at}: {got[at : at + 20]!r} where {want[at : at + 20]!r} was written"
    )


def push(api: str, token: str, repo: str, overview: Overview) -> None:
    """Writes both fields to `repo`, then reads them back anonymously."""
    url = f"{api}/v2/repositories/{repo}"
    sent = {
        "description": overview.description,
        "full_description": overview.full_description,
    }
    request("PATCH", url, sent, token)
    problem = ""
    for attempt in range(READBACK_ATTEMPTS):
        if attempt:
            time.sleep(READBACK_DELAY)
        try:
            got = request("GET", url)
        except Failure as err:
            problem = str(err)
            continue
        stale = [mismatch(f, want, got.get(f)) for f, want in sent.items() if got.get(f) != want]
        if not stale:
            return
        problem = "; ".join(stale)
    raise Failure(f"{repo}: written, but the anonymous read-back does not confirm it: {problem}")


def parse_target(arg: str) -> tuple[str, str]:
    """Parses one `<hub-repo>=<stage>` argument."""
    repo, sep, stage = arg.partition("=")
    if not sep or not REPO_NAME.fullmatch(repo) or not STAGE_NAME.fullmatch(stage):
        raise argparse.ArgumentTypeError(
            f"expected <namespace>/<repo>=<stage>, e.g. decdn/decdn-node=decdn-node: {arg!r}"
        )
    return repo, stage


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Push docker/README.<stage>.md to each Docker Hub repository."
    )
    parser.add_argument("--ref", default="HEAD", help="git ref to read the overviews at")
    parser.add_argument("targets", nargs="+", type=parse_target, metavar="<hub-repo>=<stage>")
    args = parser.parse_args(argv)

    try:
        # Every file is read and checked before any request, so a bad file never
        # leaves one repository updated and the other not.
        overviews = [
            (repo, parse_overview(read_at_ref(args.ref, overview_path(stage)), overview_path(stage)))
            for repo, stage in args.targets
        ]
        api = api_base()
        token = login(api, *credential())
    except Failure as err:
        print(f"error: {err}", file=sys.stderr)
        return 1

    failed = []
    for repo, overview in overviews:
        try:
            push(api, token, repo, overview)
        except Failure as err:
            print(f"error: {err}", file=sys.stderr)
            failed.append(repo)
        else:
            print(f"    {repo}: overview pushed")
    if failed:
        print(f"error: not confirmed on Docker Hub: {' '.join(failed)}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
