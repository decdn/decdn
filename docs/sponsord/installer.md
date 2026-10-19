# The installer

What happens on a user's machine. One command installs `decdn` and
downloads a bundle, one file of a bundle, or a blob, paid for by the
onramp's sponsor:

```bash
curl -fsSL https://<onramp>/decdn.sh | sh -s -- get 'decdn:<hash>/[<path>][?ns=<id>]' [-o <dir>] [<decdn get flags>]
curl -fsSL https://<onramp>/decdn.sh | sh -s -- get 'decdn:<hash>[?ns=<id>]' -o <file> [<decdn get flags>]
```

```powershell
& ([scriptblock]::Create((irm https://<onramp>/decdn.ps1))) get 'decdn:<hash>/[<path>][?ns=<id>]' [-o <dir>] [<decdn get flags>]
& ([scriptblock]::Create((irm https://<onramp>/decdn.ps1))) get 'decdn:<hash>[?ns=<id>]' -o <file> [<decdn get flags>]
```

The onramp serves each script with its own URL, the pinned `decdn` release
and its download profile (chain, RPC, contracts) baked in. Without `get`,
the script only installs `decdn`.

## Install

The script downloads `decdn` from its pinned GitHub Release into
`~/.local/bin` (Windows: `%LOCALAPPDATA%\decdn\bin`), after checking the
release's `SHA256SUMS` against the pinned digest and the archive against
`SHA256SUMS`. A marker in `~/.decdn/installer/` records what is installed,
so a later run with the same pin downloads nothing.

To install `decdn` without downloading a bundle:

```bash
curl -fsSL https://<onramp>/decdn.sh | sh
```

```powershell
irm https://<onramp>/decdn.ps1 | iex
```

On Windows the script adds `%LOCALAPPDATA%\decdn\bin` to the user's `Path`
on every run. On macOS and Linux it leaves shell startup files alone: when
`~/.local/bin` is not on `PATH`, it prints the line that adds it for the
user's shell (`~/.zshrc`, `~/.bashrc` or `~/.bash_profile`, `fish_add_path`,
or `~/.profile`).

## `get <decdn: URI> [-o <dir or file>] [<decdn get flags>]`

The URI names what to download, in the `decdn:` form `decdn get` takes
(decdn's `adr/appendix-bundles.md` § `decdn:` URIs):

| URI | Downloads |
|---|---|
| `decdn:<hash>/` | every file of the bundle whose manifest has this hash |
| `decdn:<hash>/a/b.txt` | that one file of the bundle, to `<dir>/a/b.txt` |
| `decdn:<hash>` | the blob with this hash, to the file `-o` names |
| any with `?ns=<id>` | the same, routed to the namespace's origins |

A blob URI needs `-o <file>`: a blob carries no file name. Quote the URI
when it has a `?`, so the shell does not read it as a pattern.

1. Opens `~/.decdn/sponsored/downloads/<hash>/` and makes a throwaway key
   there with `decdn key-gen`, under a random password. Nothing to manage.
   One run per download at a time: a second run of the same download stops
   at once.
2. Asks the onramp for a capability for that key. If there is none yet, it
   prints a link (and opens it in the browser) to the gate page, then waits
   up to 10 minutes for the person to pass it. The capability and its expiry
   are saved beside the key.
3. Runs `decdn get` with the capability and key. `decdn`'s own progress and
   errors show as they are. `decdn` keeps its peer cache in
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

The download's state is keyed on the hash, so every URI of one bundle or
blob shares its key and capability. The URI and every argument after it go to
`decdn get` unchanged, and `decdn` checks them:

```bash
curl -fsSL https://<onramp>/decdn.sh | sh -s -- get 'decdn:<hash>/?ns=7' -o out --jobs 8 --include '*.txt'
```

For a bundle URI without `-o` (or `--output`), the script adds `-o .`; a
blob URI without one stops before installing anything. `?ns=<id>` lets a
node that hasn't cached the content fetch it from that namespace's origins;
it never changes which bytes are accepted. A bad URI path or namespace is
reported by `decdn` after the gate, and running the fixed command again
skips the gate. A flag the script already passes (`--capability`,
`--keystore`, `--data-dir`, the chain and contract flags) can be given
again this way, and what `decdn` then does is up to `decdn`.
