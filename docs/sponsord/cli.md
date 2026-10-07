# The `decdn-sponsored` CLI

What happens on a user's machine.

## Install

The onramp serves the installers with its own URL and the pinned releases
baked in:

```bash
curl -fsSL https://<onramp>/decdn.sh | sh -s -- pull b3:<hash> --namespace <id>
```

```powershell
irm https://<onramp>/decdn.ps1 | iex; decdn-sponsored pull b3:<hash> --namespace <id>
```

The installer downloads `decdn` and `decdn-sponsored` from GitHub Releases
into `~/.local/bin` (Windows: `%LOCALAPPDATA%\decdn\bin`), checks each
against the pinned digests, and writes `~/.decdn/sponsor.toml`:

```toml
onramp_url = "https://<onramp>"
decdn_bin = "/home/you/.local/bin/decdn"
# data_dir = "~/.decdn/sponsored"   (optional; this is the default)
```

Arguments after the installer are passed to `decdn-sponsored`. Re-running it
is harmless and upgrades to whatever release the onramp pins.

## `decdn-sponsored pull <hash> [-o <dir>] [--namespace <id>]`

1. Reads the onramp's `/v1/profile` (chain, RPC, contracts). If the onramp
   needs a newer CLI, it says so and stops.
2. Opens `~/.decdn/sponsored/downloads/<hash>/` and makes a throwaway key
   there, with a random password. Nothing to manage.
3. Asks the onramp for a capability for that key. If there is none yet, it
   prints a link (and opens it in the browser) to the gate page, then waits
   up to 10 minutes for the person to pass it.
4. Runs `decdn bundle pull` with the capability and key. `decdn`'s own
   progress and errors show as they are. `decdn` keeps its peer cache in
   `~/.decdn/sponsored/decdn/`, which downloads share, so a later download
   finds nodes faster. Downloads run one at a time in that directory; one
   started while another runs gets a directory of its own.
5. On success, deletes the download's state (the shared peer cache stays).
   On failure, keeps it: running
   the same command again resumes with the same key and capability, and
   `decdn` resumes from its `.partial` files. A resumed download works even
   if the onramp is down, using the profile it saved.

A capability within about an hour of expiring is replaced by a fresh key
and a new trip through the gate, so a download never starts on one that
nodes will refuse midway.

`--namespace` is passed to `decdn` as-is: it lets a node that hasn't cached
the bundle fetch it from that namespace's origins. It never changes which
bytes are accepted.
