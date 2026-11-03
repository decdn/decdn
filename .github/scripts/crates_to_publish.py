#!/usr/bin/env python3
"""Decide which workspace crates the publish-crates workflow still uploads.

A crates.io version can never be replaced, so the workflow publishes only the
crates that crates.io does not yet serve at the release's version and
excludes the rest. A run that failed partway is therefore finished by
running it again: the crates it already uploaded are excluded, and the
remainder goes up.

The check reads `cargo metadata` and skips every crate with `publish = false`.
It stops before anything is uploaded when:

- a crate's version is not the release's version. The tag names the version,
  and `cargo publish` would otherwise upload whatever the manifests say.
- a crate does not exist on crates.io. Trusted publishing cannot create a
  crate: its first version goes up by hand, with an API token, and the crate
  then needs its own trusted-publisher entry (RELEASING.md).
- crates.io answers anything but 200 or 404, or a 200 whose JSON does not
  describe the crate or version asked about. A guess here either uploads a
  crate twice, which cargo refuses partway through the run, or skips one.

Modes:

  --version X            print the plan. With $GITHUB_OUTPUT set, write
                         `pending=true|false` and `exclude=<names>` there,
                         the names space-separated.
  --version X --confirm  after an upload: ask crates.io for every crate at X,
                         a few times within a few minutes, and warn about any
                         it does not serve yet or could not be asked about.
                         Exits 0 whatever happens, because the upload has
                         already happened and a red job would invite a retry.

Run: python3 .github/scripts/crates_to_publish.py --version 0.3.1
"""

from __future__ import annotations

import argparse
import http.client
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass

if sys.version_info < (3, 11):  # the floor every guard under .github/scripts sets
    sys.exit(f"error: python 3.11+ required, found {sys.version.split()[0]}")

API = "https://crates.io/api/v1/crates"
# crates.io refuses requests without a User-Agent that names the caller.
USER_AGENT = "decdn-publish-crates (https://github.com/decdn/decdn)"
# A crate name as crates.io accepts it. The names reach a shell as `--exclude`
# arguments, so anything else is refused rather than quoted.
CRATE_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_-]{0,63}$")
ATTEMPTS = 3
TIMEOUT = 30.0
CONFIRM_ATTEMPTS = 10
CONFIRM_DELAY = 3.0
# --confirm gives up after this long, however many crates are left. A slow or
# unreachable crates.io must not hold the job for hours after the upload.
CONFIRM_BUDGET = 300.0


class PlanError(Exception):
    """A reason to publish nothing."""


@dataclass(frozen=True)
class Crate:
    name: str
    version: str


def http_get(url: str) -> tuple[int, bytes]:
    """(status, body) of crates.io's answer for `url`. A 4xx is returned with
    an empty body. A transport error or a 5xx is retried; one that persists
    raises PlanError."""
    last = ""
    for attempt in range(ATTEMPTS):
        request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        try:
            with urllib.request.urlopen(request, timeout=TIMEOUT) as response:
                return int(response.status), response.read()
        # HTTPError is a URLError, so it is caught first.
        except urllib.error.HTTPError as err:
            if err.code < 500:
                return int(err.code), b""
            last = f"HTTP {err.code}"
        except (urllib.error.URLError, http.client.HTTPException, OSError) as err:
            last = str(err) or type(err).__name__
        if attempt + 1 < ATTEMPTS:
            time.sleep(2.0 * (attempt + 1))
    raise PlanError(f"cannot reach crates.io for {url}: {last}")


def exists(name: str, version: str | None = None) -> bool:
    """Whether crates.io serves crate `name`, or its `version` when given.

    A 200 counts only when its JSON describes the crate or version asked
    about: a proxy or an error page answering 200 must not mark a crate
    published, which would leave it out of the upload for good. Any status
    but 200 or 404 raises PlanError.
    """
    url = f"{API}/{name}" if version is None else f"{API}/{name}/{version}"
    what = name if version is None else f"{name} {version}"
    status, body = http_get(url)
    if status == 404:
        return False
    if status != 200:
        raise PlanError(f"crates.io answered HTTP {status} for {what}")
    try:
        data = json.loads(body)
        if version is None:
            ok = data["crate"]["name"] == name
        else:
            ok = data["version"]["crate"] == name and data["version"]["num"] == version
    except (ValueError, TypeError, KeyError):
        ok = False
    if not ok:
        raise PlanError(f"crates.io answered HTTP 200 for {what} with a body that does not describe it")
    return True


def publishable(metadata: dict) -> list[Crate]:
    """Every crate `cargo publish --workspace` uploads, sorted by name."""
    crates = []
    for package in metadata.get("packages", []):
        # `publish = false` reaches cargo metadata as `"publish": []`.
        if package.get("publish") == []:
            continue
        name = package["name"]
        if not CRATE_NAME.match(name):
            raise PlanError(f"unexpected crate name {name!r}")
        crates.append(Crate(name, package["version"]))
    if not crates:
        raise PlanError("cargo metadata lists no publishable crate")
    return sorted(crates, key=lambda c: c.name)


def plan(crates: list[Crate], version: str) -> tuple[list[str], list[str]]:
    """(pending, published): the crates still to upload at `version`, and the
    ones crates.io already serves at it."""
    wrong = [f"{c.name}={c.version}" for c in crates if c.version != version]
    if wrong:
        raise PlanError(f"the tag says {version}, but these crates disagree: {', '.join(wrong)}")

    pending: list[str] = []
    published: list[str] = []
    missing: list[str] = []
    for crate in crates:
        if not exists(crate.name):
            missing.append(crate.name)
        elif exists(crate.name, version):
            published.append(crate.name)
        else:
            pending.append(crate.name)
    if missing:
        raise PlanError(
            "these crates do not exist on crates.io, and trusted publishing cannot "
            f"create a crate: {', '.join(missing)}. Publish each one's first version by "
            "hand with an API token, then add its trusted publisher (RELEASING.md "
            "§ Trusted publishing)."
        )
    return pending, published


def confirm(crates: list[Crate], version: str) -> dict[str, str]:
    """Each crate crates.io still does not serve at `version` after a few
    tries, mapped to why: not served yet, or the error that stopped the ask."""
    waiting = {c.name: "" for c in crates}
    deadline = time.monotonic() + CONFIRM_BUDGET
    for attempt in range(CONFIRM_ATTEMPTS):
        for name in list(waiting):
            if time.monotonic() > deadline:
                return waiting
            try:
                served = exists(name, version)
                reason = ""
            except PlanError as err:
                served, reason = False, str(err)
            if served:
                print(f"https://crates.io/crates/{name}/{version}")
                del waiting[name]
            else:
                waiting[name] = reason
        if not waiting:
            break
        if attempt + 1 < CONFIRM_ATTEMPTS:
            time.sleep(CONFIRM_DELAY)
    return waiting


def cargo_metadata() -> dict:
    out = subprocess.run(
        ["cargo", "metadata", "--no-deps", "--format-version", "1", "--locked"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    return json.loads(out)


def write_outputs(pending: list[str], published: list[str]) -> None:
    path = os.environ.get("GITHUB_OUTPUT")
    if not path:
        return
    with open(path, "a", encoding="utf-8") as out:
        out.write(f"pending={'true' if pending else 'false'}\n")
        out.write(f"exclude={' '.join(published)}\n")


def run_confirm(version: str) -> int:
    """--confirm: warn about every crate crates.io does not serve yet. Never
    fails, because the upload has already happened and a red job would invite
    a retry."""
    try:
        waiting = confirm(publishable(cargo_metadata()), version)
    except Exception as err:  # noqa: BLE001 — any failure here is a warning
        print(f"::warning::could not confirm the upload on crates.io: {err}")
        return 0
    for name, reason in waiting.items():
        if reason:
            print(
                f"::warning::could not ask crates.io about {name} {version}: {reason}. "
                f"The upload itself succeeded; check https://crates.io/crates/{name}/{version}."
            )
        else:
            print(
                f"::warning::crates.io does not serve {name} {version} yet. This is "
                "normally index lag; the upload itself succeeded. Check "
                f"https://crates.io/crates/{name}/{version} again in a minute."
            )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--version", required=True)
    parser.add_argument("--confirm", action="store_true")
    args = parser.parse_args(argv)

    if args.confirm:
        return run_confirm(args.version)

    try:
        crates = publishable(cargo_metadata())
    except (subprocess.CalledProcessError, json.JSONDecodeError, KeyError) as err:
        print(f"error: cannot read the workspace crates from cargo metadata: {err}", file=sys.stderr)
        return 1
    except PlanError as err:
        print(f"error: {err}", file=sys.stderr)
        return 1

    try:
        pending, published = plan(crates, args.version)
    except PlanError as err:
        print(f"error: {err}", file=sys.stderr)
        return 1

    for name in published:
        print(f"already on crates.io at {args.version}: {name}")
    for name in pending:
        print(f"to publish at {args.version}: {name}")
    if not pending:
        print(f"every crate is already on crates.io at {args.version}; nothing to publish")
    write_outputs(pending, published)
    return 0


if __name__ == "__main__":
    sys.exit(main())
