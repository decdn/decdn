"""Regression tests for sign-release.sh's choice of signing key.

A maintainer keyring usually holds a personal key as well as the `@decdn.org`
key published in KEYS, so the script must not let keyring order choose
between them. These tests run the real script against a throwaway keyring and
stop it right after it announces the key: the scratch repo has no `origin`, so
the tag fetch that follows always fails.

Every generated key has an encryption subkey, like the real keys in KEYS, so
a subkey fingerprint mistaken for a primary one shows up here.
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
    reason="gpg/gpgconf is not installed",
)

DECDN_RULE = "decdn.org key from KEYS"
ONLY_PUBLISHED_RULE = "only secret key in KEYS"
ENV_RULE = "SPONSORD_SIGNING_KEY"
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

    @property
    def bin(self) -> Path:
        return self.root / "bin"

    def env(self) -> dict[str, str]:
        env = {
            k: v
            for k, v in os.environ.items()
            if not k.startswith(("SPONSORD_", "GIT_", "GNUPG", "XDG_"))
        }
        env.update(
            GNUPGHOME=str(self.gnupghome),
            HOME=str(self.root),
            GIT_CONFIG_NOSYSTEM="1",
            PATH=f"{self.bin}{os.pathsep}{os.environ['PATH']}",
            SPONSORD_SKIP_IMAGE_TAGS="1",
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
        """Creates a passphrase-less key with an encryption subkey and returns
        its primary fingerprint."""
        self.gpg("--quick-gen-key", uid, "default", "default", "never")
        listing = self.gpg("--list-secret-keys", "--with-colons", uid).splitlines()
        sec = [i for i, line in enumerate(listing) if line.startswith("sec:")]
        assert len(sec) == 1, listing
        fpr = listing[sec[0] + 1].split(":")[9]
        if publish:
            self.published.append(fpr)
        return fpr

    def co_maintainer(self) -> str:
        """Publishes a key whose secret half this keyring does not hold, the
        way a second maintainer's key sits in KEYS."""
        fpr = self.gen_key("Co-maintainer <co@decdn.org>", publish=True)
        self.gpg("--yes", "--delete-secret-keys", fpr)
        return fpr

    def revocation(self, fpr: str) -> str:
        """The revocation certificate gpg wrote when it created the key."""
        cert = (self.gnupghome / "openpgp-revocs.d" / f"{fpr}.rev").read_text()
        # gpg prefixes the armor line with `:` so an accidental import does not
        # revoke the key; stripping it is the documented way to arm it.
        return cert.replace(":-----BEGIN", "-----BEGIN")

    def revoke(self, fpr: str) -> None:
        """Revokes the key in the local keyring, and so in KEYS too."""
        cert = self.root / "rev.asc"
        cert.write_text(self.revocation(fpr))
        self.gpg("--import", str(cert))

    def fail_gpg_on(self, arg: str) -> None:
        """Puts a gpg on PATH that fails whenever `arg` is on its command line."""
        real = shutil.which("gpg")
        shim = self.bin / "gpg"
        shim.write_text(
            "#!/bin/sh\n"
            f'for a in "$@"; do [ "$a" = "{arg}" ] && '
            "{ echo 'gpg: injected failure' >&2; exit 2; }; done\n"
            f'exec "{real}" "$@"\n'
        )
        shim.chmod(0o755)

    def run(self, *, extra_keys: str = "", **env: str) -> subprocess.CompletedProcess[str]:
        keys = self.gpg("--armor", "--export", *self.published) if self.published else ""
        (self.repo / "KEYS").write_text(keys + extra_keys)
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
    # A short path under the system temp dir, not pytest's tmp_path: when
    # /run/user/<uid> does not exist (macOS, containers, some CI runners)
    # gpg-agent puts its sockets in GNUPGHOME, and a socket path over 108 bytes
    # (104 on macOS) fails to bind. tmp_path under a long $TMPDIR exceeds that.
    root = Path(tempfile.mkdtemp(prefix="sr-"))
    ring = Keyring(root)
    ring.gnupghome.mkdir(mode=0o700)
    ring.bin.mkdir()
    gh = ring.bin / "gh"
    gh.write_text("#!/bin/sh\nexit 0\n")
    gh.chmod(0o755)
    ring.repo.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=ring.repo, env=ring.env(), check=True)
    try:
        yield ring
    finally:
        subprocess.run(["gpgconf", "--kill", "all"], env=ring.env(), check=False)
        shutil.rmtree(root, ignore_errors=True)


def output(result: subprocess.CompletedProcess[str]) -> str:
    return result.stdout + result.stderr


def signed_as(result: subprocess.CompletedProcess[str], fpr: str, rule: str) -> bool:
    return f"==> Signing as {fpr} ({rule})" in result.stdout


def refused(result: subprocess.CompletedProcess[str], message: str) -> bool:
    return (
        result.returncode != 0
        and "==> Signing as" not in result.stdout
        and message in result.stderr
    )


# ---- the @decdn.org rule ---------------------------------------------------


@pytest.mark.parametrize("personal_first", [True, False])
def test_prefers_published_decdn_key_regardless_of_order(keyring, personal_first):
    # The personal key is published too, so only the @decdn.org rule (not the
    # "only published key" fallback) can make this choice.
    if personal_first:
        keyring.gen_key("Personal <me@example.com>", publish=True)
        decdn = keyring.gen_key("Maintainer <me@decdn.org>", publish=True)
    else:
        decdn = keyring.gen_key("Maintainer <me@decdn.org>", publish=True)
        keyring.gen_key("Personal <me@example.com>", publish=True)

    result = keyring.run()

    assert signed_as(result, decdn, DECDN_RULE), output(result)
    assert NO_ORIGIN in result.stderr


def test_two_published_decdn_keys_are_refused(keyring):
    a = keyring.gen_key("A <a@decdn.org>", publish=True)
    b = keyring.gen_key("B <b@decdn.org>", publish=True)

    result = keyring.run()

    assert refused(result, "found 2 @decdn.org secret keys published in KEYS"), output(result)
    assert a in result.stderr and b in result.stderr


def test_one_key_with_two_decdn_uids_is_one_candidate(keyring):
    keyring.gen_key("Personal <me@example.com>", publish=True)
    decdn = keyring.gen_key("Maintainer <me@decdn.org>", publish=True)
    keyring.gpg("--quick-add-uid", decdn, "Security <security@decdn.org>")

    result = keyring.run()

    assert signed_as(result, decdn, DECDN_RULE), output(result)


def test_rotation_skips_the_revoked_old_key(keyring):
    old = keyring.gen_key("Maintainer <me@decdn.org>", publish=True)
    new = keyring.gen_key("Maintainer (2026) <me@decdn.org>", publish=True)
    keyring.revoke(old)

    result = keyring.run()

    assert signed_as(result, new, DECDN_RULE), output(result)


def test_key_revoked_in_keys_but_live_locally_is_skipped(keyring):
    # The maintainer published a revocation but never imported it locally.
    # Consumers import KEYS, so KEYS's view is the one that counts.
    old = keyring.gen_key("Maintainer <me@decdn.org>", publish=True)
    new = keyring.gen_key("Maintainer (2026) <me@decdn.org>", publish=True)

    result = keyring.run(extra_keys=keyring.revocation(old))

    assert signed_as(result, new, DECDN_RULE), output(result)


def test_unpublished_decdn_key_is_not_chosen(keyring):
    personal = keyring.gen_key("Personal <me@example.com>", publish=True)
    keyring.gen_key("Maintainer <me@decdn.org>", publish=False)

    result = keyring.run()

    assert signed_as(result, personal, ONLY_PUBLISHED_RULE), output(result)


@pytest.mark.parametrize(
    "uid",
    [
        "Lookalike <me@decdn.org.example>",
        "Lookalike <me@notdecdn.org>",
        "Lookalike <me@eu.decdn.org>",
        "Mentions me@decdn.org <me@example.com>",
        "Name me@decdn.org",
        # gpg's mailbox is the FIRST <…>, so a trailing one is not the email.
        "Two <me@example.com> <me@decdn.org>",
    ],
)
def test_lookalike_uids_do_not_match(keyring, uid):
    # The look-alike key is the only published one, so it is still chosen — by
    # the fallback, which the label tells apart from the @decdn.org rule.
    keyring.gen_key("Personal <me@example.com>", publish=False)
    lookalike = keyring.gen_key(uid, publish=True)

    result = keyring.run()

    assert signed_as(result, lookalike, ONLY_PUBLISHED_RULE), output(result)


def test_bare_address_uid_matches_case_insensitively(keyring):
    keyring.gen_key("Personal <me@example.com>", publish=True)
    decdn = keyring.gen_key("me@DECDN.org", publish=True)

    result = keyring.run()

    assert signed_as(result, decdn, DECDN_RULE), output(result)


def test_text_after_the_address_still_matches(keyring):
    # gpg's own `<me@decdn.org>` lookup finds this uid, so the script agrees.
    keyring.gen_key("Personal <me@example.com>", publish=True)
    decdn = keyring.gen_key("Maintainer <me@decdn.org> (release key)", publish=True)

    result = keyring.run()

    assert signed_as(result, decdn, DECDN_RULE), output(result)


def test_revoked_decdn_uid_does_not_match(keyring):
    keyring.gen_key("Personal <me@example.com>", publish=False)
    other = keyring.gen_key("Other <other@example.com>", publish=True)
    keyring.gpg("--quick-add-uid", other, "Maintainer <other@decdn.org>")
    keyring.gpg("--quick-revoke-uid", other, "Maintainer <other@decdn.org>")

    result = keyring.run()

    assert signed_as(result, other, ONLY_PUBLISHED_RULE), output(result)


# ---- the fallback: the one published secret key -----------------------------


@pytest.mark.parametrize("published_first", [True, False])
def test_fallback_takes_the_only_published_key_regardless_of_order(keyring, published_first):
    if published_first:
        published = keyring.gen_key("Published <p@example.com>", publish=True)
        keyring.gen_key("Personal <me@example.com>", publish=False)
    else:
        keyring.gen_key("Personal <me@example.com>", publish=False)
        published = keyring.gen_key("Published <p@example.com>", publish=True)

    result = keyring.run()

    assert signed_as(result, published, ONLY_PUBLISHED_RULE), output(result)


def test_fallback_refuses_two_published_keys(keyring):
    a = keyring.gen_key("A <a@example.com>", publish=True)
    b = keyring.gen_key("B <b@example.com>", publish=True)

    result = keyring.run()

    assert refused(result, "found 2 secret keys published in KEYS"), output(result)
    assert a in result.stderr and b in result.stderr


def test_no_published_secret_key_is_refused(keyring):
    keyring.co_maintainer()
    personal = keyring.gen_key("Personal <me@example.com>", publish=False)
    keyring.gen_key("Other <other@example.com>", publish=False)

    result = keyring.run()

    assert refused(result, "none of your secret keys is published in KEYS"), output(result)
    assert personal in result.stderr


def test_co_maintainer_key_without_its_secret_is_not_a_candidate(keyring):
    keyring.co_maintainer()
    decdn = keyring.gen_key("Maintainer <me@decdn.org>", publish=True)

    result = keyring.run()

    assert signed_as(result, decdn, DECDN_RULE), output(result)


def test_empty_keyring_is_refused(keyring):
    keyring.co_maintainer()

    result = keyring.run()

    assert refused(result, "gpg lists no secret keys"), output(result)


def test_gpg_failure_is_reported_not_swallowed(keyring):
    keyring.gen_key("Maintainer <me@decdn.org>", publish=True)
    keyring.fail_gpg_on("--list-secret-keys")

    result = keyring.run()

    assert refused(result, "gpg could not list your secret keys"), output(result)
    assert "gpg: injected failure" in result.stderr


# ---- SPONSORD_SIGNING_KEY -------------------------------------------------------


def test_signing_key_env_overrides_decdn_key(keyring):
    personal = keyring.gen_key("Personal <me@example.com>", publish=True)
    keyring.gen_key("Maintainer <me@decdn.org>", publish=True)

    result = keyring.run(SPONSORD_SIGNING_KEY=personal)

    assert signed_as(result, personal, ENV_RULE), output(result)


def test_ambiguous_signing_key_env_is_refused(keyring):
    keyring.gen_key("A <a@decdn.org>", publish=True)
    keyring.gen_key("B <b@decdn.org>", publish=True)

    result = keyring.run(SPONSORD_SIGNING_KEY="decdn.org")

    assert refused(result, "SPONSORD_SIGNING_KEY 'decdn.org' is ambiguous"), output(result)


def test_signing_key_env_matching_nothing_is_refused(keyring):
    keyring.gen_key("Maintainer <me@decdn.org>", publish=True)

    result = keyring.run(SPONSORD_SIGNING_KEY="nobody@example.com")

    assert refused(result, "no gpg secret key matches SPONSORD_SIGNING_KEY"), output(result)


def test_signing_key_env_must_be_published(keyring):
    personal = keyring.gen_key("Personal <me@example.com>", publish=False)
    keyring.gen_key("Maintainer <me@decdn.org>", publish=True)

    result = keyring.run(SPONSORD_SIGNING_KEY=personal)

    assert refused(result, f"signing key {personal} is not published in KEYS"), output(result)


def test_signing_key_env_revoked_in_keys_is_refused(keyring):
    old = keyring.gen_key("Maintainer <me@decdn.org>", publish=True)

    result = keyring.run(extra_keys=keyring.revocation(old), SPONSORD_SIGNING_KEY=old)

    assert refused(result, "KEYS marks it revoked or expired"), output(result)
