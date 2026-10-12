# The installer

What happens on a user's machine. One command installs `decdn` and
downloads a bundle, paid for by the onramp's sponsor:

```bash
curl -fsSL https://<onramp>/decdn.sh | sh -s -- pull b3:<hash> [-o <dir>] [--namespace <id>]
```

```powershell
& ([scriptblock]::Create((irm https://<onramp>/decdn.ps1))) pull b3:<hash> [-o <dir>] [--namespace <id>]
```

The onramp serves each script with its own URL, the pinned `decdn` release
and its download profile (chain, RPC, contracts) baked in. Without `pull`,
the script only installs `decdn`.

## Install

The script downloads `decdn` from its pinned GitHub Release into
`~/.local/bin` (Windows: `%LOCALAPPDATA%\decdn\bin`), after checking the
release's `SHA256SUMS` against the pinned digest and the archive against
`SHA256SUMS`. A marker in `~/.decdn/installer/` records what is installed,
so a later run with the same pin downloads nothing.

## `pull <hash> [-o <dir>] [--namespace <id>]`

1. Opens `~/.decdn/sponsored/downloads/<hash>/` and makes a throwaway key
   there with `decdn key-gen`, under a random password. Nothing to manage.
   One run per download at a time: a second run of the same download stops
   at once.
2. Asks the onramp for a capability for that key. If there is none yet, it
   prints a link (and opens it in the browser) to the gate page, then waits
   up to 10 minutes for the person to pass it. The capability and its expiry
   are saved beside the key.
3. Runs `decdn bundle pull` with the capability and key. `decdn`'s own
   progress and errors show as they are. `decdn` keeps its peer cache in
   `~/.decdn/sponsored/decdn/`, which downloads share, so a later download
   finds nodes faster. Downloads run one at a time in that directory; one
   started while another runs gets a directory of its own.
4. On success, deletes the download's state (the shared peer cache stays).
   On failure, keeps it: running the same command again resumes with the
   same key and capability, without the gate, and `decdn` resumes from its
   `.partial` files.

A capability within about an hour of expiring is replaced by a fresh key
and a new trip through the gate, so a download never starts on one that
nodes will refuse midway.

`--namespace` is passed to `decdn` as-is: it lets a node that hasn't cached
the bundle fetch it from that namespace's origins. It never changes which
bytes are accepted.
