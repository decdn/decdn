#!/usr/bin/env python3
"""Fail if Cargo.lock moves the iroh family off its held version.

noq-proto 1.4 caps a stream's retained receive buffers at 1024 and closes the
whole connection with INTERNAL_ERROR "too many gaps in stream buffer" past it.
The cap counts unread buffers, not gaps, so a client that falls ~2.4 MiB
behind the node trips it on a gap-free stream. The paid leg then dies mid-pull
with the node's credit-window bytes delivered and unpaid (#2585). This is
quinn#2809, which quinn fixes in quinn#2814; noq has not ported the fix yet
(n0-computer/noq#828).

iroh and its QUIC crates therefore stay at the baseline ADR 000 records. The
committed Cargo.lock is the only pin: `Cargo.toml` asks for `iroh = "1"`, and
Dependabot ignores `iroh*` and `noq*`. Nothing else stops a local
`cargo update`, and only `anvil e2e (journeys)` notices the result, as a
flaky-looking under-pay failure. This check makes the drift a plain error on
every PR.

`HELD` is the whole policy: every crate in it must appear in Cargo.lock, and
every entry of that name must carry exactly the held version. A crate absent
from the lock is an error rather than a pass, so a renamed or dropped crate
moves the table with it.

Run: .github/scripts/check-iroh-hold.sh
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

if sys.version_info < (3, 11):  # tomllib, and the syntax used below
    sys.exit(f"error: python 3.11+ required, found {sys.version.split()[0]}")

import tomllib  # noqa: E402  (must follow the version guard)

# Crate -> the exact version Cargo.lock must resolve it to. The seven move
# together: iroh pulls the other six, and `cargo update -p iroh` moves them all.
HELD: dict[str, str] = {
    "iroh": "1.3.0",
    "iroh-base": "1.3.0",
    "iroh-dns": "1.3.0",
    "iroh-relay": "1.3.0",
    "noq": "1.3.0",
    "noq-proto": "1.3.0",
    "noq-udp": "1.3.0",
}

HOW_TO_FIX = """\
iroh and its QUIC crates are held because noq-proto 1.4 closes client pulls
mid-leg with "too many gaps in stream buffer" (#2585,
https://github.com/decdn/decdn/issues/2585). The upstream fix is quinn#2814;
noq has not released its port yet (https://github.com/n0-computer/noq/pull/828).

To restore the hold, put back only the held crates' `version` and `checksum`
lines in Cargo.lock (plus any dependency line their bump added or removed).
`cargo update -p <crate> --precise <version>` also re-resolves unrelated
crates, so check `git diff Cargo.lock` after it.

To lift the hold, once a noq release carries the quinn#2814 port: move the
family together (`cargo update -p iroh`), rerun `anvil e2e (journeys)`
(cli_fetch_topup in particular), record the new baseline in
adr/000-language.md, and update HELD in .github/scripts/check_iroh_hold.py in
the same PR."""


def lock_versions(lock_text: str) -> dict[str, list[str]]:
    """Every version Cargo.lock resolves each crate name to, in file order."""
    lock = tomllib.loads(lock_text)
    versions: dict[str, list[str]] = {}
    for package in lock.get("package", []):
        name = package.get("name")
        version = package.get("version")
        if isinstance(name, str) and isinstance(version, str):
            versions.setdefault(name, []).append(version)
    return versions


def check_lock(lock_text: str, held: dict[str, str] = HELD) -> list[str]:
    """One error per held crate that is missing or resolves off its version."""
    try:
        versions = lock_versions(lock_text)
    except tomllib.TOMLDecodeError as e:
        return [f"Cargo.lock: not valid TOML ({e})"]
    errors: list[str] = []
    for name, want in held.items():
        found = versions.get(name)
        if not found:
            errors.append(
                f"{name}: not in Cargo.lock; HELD names a crate the lock no longer has"
            )
            continue
        drifted = sorted({v for v in found if v != want})
        if drifted:
            errors.append(f"{name}: Cargo.lock resolves {', '.join(drifted)}, held at {want}")
    return errors


def check(repo_root: Path) -> list[str]:
    lock = repo_root / "Cargo.lock"
    if not lock.is_file():
        return ["Cargo.lock: not found"]
    return check_lock(lock.read_text())


def main() -> int:
    repo_root = Path(
        subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            check=True,
            stdout=subprocess.PIPE,
            text=True,
        ).stdout.strip()
    )
    errors = check(repo_root)
    if errors:
        print("error: Cargo.lock moves the iroh family off its held version:\n", file=sys.stderr)
        for e in errors:
            print(f"  {e}", file=sys.stderr)
        print(f"\n{HOW_TO_FIX}", file=sys.stderr)
        return 1
    print(f"iroh hold OK ({len(HELD)} crates at their held versions)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
