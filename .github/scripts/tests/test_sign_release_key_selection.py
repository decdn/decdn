"""Regression tests for sign-release.sh's choice of signing key.

A maintainer keyring usually holds a personal key as well as the `@decdn.org`
key published in KEYS. Picking whichever key gpg lists first makes the script
die at the KEYS check, or sign with the wrong maintainer's key, depending on
keyring order. These tests run the real script against a throwaway keyring and
stop it right after it announces the key: the scratch repo has no `origin`, so
the tag fetch that follows always fails.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT = REPO_ROOT / ".github/scripts/sign-release.sh"

pytestmark = pytest.mark.skipif(
    shutil.which("gpg") is None or shutil.which("gpgconf") is None,
    reason="gpg is not installed",
)

DECDN_RULE = "decdn.org key from KEYS"
FALLBACK_RULE = "first secret key in the keyring"
ENV_RULE = "DECDN_SIGNING_KEY"
NO_ORIGIN = "cannot reach origin to confirm the tag"


@dataclass
class Keyring:
    """A scratch GNUPGHOME plus a scratch git repo whose KEYS it controls."""

    root: Path
    published: list[str] = field(default_factory=list)

    @property
    def gnupghome(self) -> Path:
        return self.root / "g"

    @property
    def repo(self) -> Path:
        return self.root / "repo"

    def env(self) -> dict[str, str]:
        env = {
            k: v
            for k, v in os.environ.items()
            if not k.startswith(("DECDN_", "GIT_", "GNUPG"))
        }
        env.update(
            GNUPGHOME=str(self.gnupghome),
            HOME=str(self.root),
            GIT_CONFIG_NOSYSTEM="1",
            PATH=f"{self.root / 'bin'}{os.pathsep}{os.environ['PATH']}",
            DECDN_SKIP_IMAGE_TAGS="1",
        )
        return env

    def gpg(self, *args: str) -> str:
        return subprocess.run(
            ["gpg", "--batch", "--pinentry-mode", "loopback", "--passphrase", "", *args],
            env=self.env(),
            check=True,
            capture_output=True,
            text=True,
        ).stdout

    def gen_key(self, uid: str, *, publish: bool) -> str:
        """Creates a passphrase-less secret key and returns its fingerprint."""
        self.gpg("--quick-gen-key", uid, "ed25519", "sign", "never")
        listing = self.gpg("--list-secret-keys", "--with-colons", uid)
        fprs = [line.split(":")[9] for line in listing.splitlines() if line.startswith("fpr:")]
        assert len(fprs) == 1, listing
        if publish:
            self.published.append(fprs[0])
        return fprs[0]

    def run(self, **env: str) -> subprocess.CompletedProcess[str]:
        keys = self.gpg("--armor", "--export", *self.published) if self.published else ""
        (self.repo / "KEYS").write_text(keys)
        return subprocess.run(
            ["bash", str(SCRIPT), "v0.0.0"],
            cwd=self.repo,
            env={**self.env(), **env},
            capture_output=True,
            text=True,
            check=False,
        )


@pytest.fixture
def keyring() -> Iterator[Keyring]:
    # A short path under the system temp dir, not pytest's tmp_path: gpg-agent
    # puts its socket in GNUPGHOME when /run/user is absent (CI), and a socket
    # path over ~108 bytes fails to bind.
    root = Path(tempfile.mkdtemp(prefix="sr-"))
    ring = Keyring(root)
    ring.gnupghome.mkdir(mode=0o700)
    (root / "bin").mkdir()
    gh = root / "bin" / "gh"
    gh.write_text("#!/bin/sh\nexit 0\n")
    gh.chmod(0o755)
    ring.repo.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=ring.repo, env=ring.env(), check=True)
    try:
        yield ring
    finally:
        subprocess.run(["gpgconf", "--kill", "all"], env=ring.env(), check=False)
        shutil.rmtree(root, ignore_errors=True)


def signed_as(result: subprocess.CompletedProcess[str], fpr: str, rule: str) -> bool:
    return f"==> Signing as {fpr} ({rule})" in result.stdout


@pytest.mark.parametrize("personal_first", [True, False])
def test_prefers_published_decdn_key_regardless_of_order(keyring, personal_first):
    if personal_first:
        keyring.gen_key("Personal <me@example.com>", publish=False)
        decdn = keyring.gen_key("Maintainer <me@decdn.org>", publish=True)
    else:
        decdn = keyring.gen_key("Maintainer <me@decdn.org>", publish=True)
        keyring.gen_key("Personal <me@example.com>", publish=False)

    result = keyring.run()

    assert signed_as(result, decdn, DECDN_RULE), result.stdout + result.stderr
    assert NO_ORIGIN in result.stderr


def test_two_published_decdn_keys_are_refused(keyring):
    a = keyring.gen_key("A <a@decdn.org>", publish=True)
    b = keyring.gen_key("B <b@decdn.org>", publish=True)

    result = keyring.run()

    assert result.returncode != 0
    assert "==> Signing as" not in result.stdout
    assert "found 2 @decdn.org secret keys published in KEYS" in result.stderr
    assert a in result.stderr and b in result.stderr


def test_no_decdn_key_falls_back_to_first_secret_key(keyring):
    personal = keyring.gen_key("Personal <me@example.com>", publish=True)

    result = keyring.run()

    assert signed_as(result, personal, FALLBACK_RULE), result.stdout + result.stderr


def test_fallback_key_still_has_to_be_published(keyring):
    personal = keyring.gen_key("Personal <me@example.com>", publish=False)
    keyring.gen_key("Other <other@example.com>", publish=True)

    result = keyring.run()

    assert result.returncode != 0
    assert f"signing key {personal} is not published in KEYS" in result.stderr


def test_unpublished_decdn_key_is_not_chosen(keyring):
    personal = keyring.gen_key("Personal <me@example.com>", publish=True)
    keyring.gen_key("Maintainer <me@decdn.org>", publish=False)

    result = keyring.run()

    assert signed_as(result, personal, FALLBACK_RULE), result.stdout + result.stderr


@pytest.mark.parametrize(
    "uid",
    [
        "Lookalike <me@decdn.org.example>",
        "Lookalike <me@notdecdn.org>",
        "Lookalike <me@eu.decdn.org>",
        "Mentions me@decdn.org <me@example.com>",
    ],
)
def test_lookalike_domains_do_not_match(keyring, uid):
    personal = keyring.gen_key("Personal <me@example.com>", publish=True)
    keyring.gen_key(uid, publish=True)

    result = keyring.run()

    assert signed_as(result, personal, FALLBACK_RULE), result.stdout + result.stderr


def test_bare_address_uid_matches(keyring):
    keyring.gen_key("Personal <me@example.com>", publish=False)
    decdn = keyring.gen_key("me@DECDN.org", publish=True)

    result = keyring.run()

    assert signed_as(result, decdn, DECDN_RULE), result.stdout + result.stderr


def test_revoked_decdn_uid_does_not_match(keyring):
    personal = keyring.gen_key("Personal <me@example.com>", publish=True)
    other = keyring.gen_key("Other <other@example.com>", publish=True)
    keyring.gpg("--quick-add-uid", other, "Maintainer <other@decdn.org>")
    keyring.gpg("--quick-revoke-uid", other, "Maintainer <other@decdn.org>")

    result = keyring.run()

    assert signed_as(result, personal, FALLBACK_RULE), result.stdout + result.stderr


def test_signing_key_env_overrides_decdn_key(keyring):
    personal = keyring.gen_key("Personal <me@example.com>", publish=True)
    keyring.gen_key("Maintainer <me@decdn.org>", publish=True)

    result = keyring.run(DECDN_SIGNING_KEY=personal)

    assert signed_as(result, personal, ENV_RULE), result.stdout + result.stderr


def test_ambiguous_signing_key_env_is_refused(keyring):
    keyring.gen_key("A <a@decdn.org>", publish=True)
    keyring.gen_key("B <b@decdn.org>", publish=True)

    result = keyring.run(DECDN_SIGNING_KEY="decdn.org")

    assert result.returncode != 0
    assert "DECDN_SIGNING_KEY 'decdn.org' is ambiguous" in result.stderr
