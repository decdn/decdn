# decdn dashboard

The page `decdn dashboard` serves, on `127.0.0.1:8485` by default. It polls
the dashboard's `GET /_status` every two seconds while visible, and shows
three sources, each only as far as it answers: the `decdn get` runs on the
host, the gateway at `--gateway`, and the node at `--admin-url` and
`--metrics-url`. The dashboard reads every source itself, so the page reads
nothing from another origin. `crates/cli/build.rs` embeds the build in the
`decdn` binary; a build with no `dist/` embeds a page that says how to make
one.

Node 24 and pnpm 10 (`corepack enable` provides it):

```bash
pnpm -C crates/cli/dashboard install --frozen-lockfile
pnpm -C crates/cli/dashboard build    # writes dist/index.html, app.js, app.css
cargo build -p decdn-cli              # embeds dist/
```

`pnpm dev` serves the page with hot reload and proxies `/_status` to
`decdn dashboard` on `127.0.0.1:8485`. Open the dev server's `/_dashboard/`
with `#token=<token>` appended, the token `decdn dashboard` printed (also in
`~/.decdn/dashboard/token`); the link it printed opens the embedded page on
8485 instead.

Rules the dashboard's content security policy sets: no inline script, no
inline style attribute, and nothing loaded from another origin. Every style
lives in the stylesheet: `eslint` refuses a `style` prop. The build emits
exactly three files with fixed names; CI checks that.

`src/api.ts` mirrors two bodies, and `src/contract.test.ts` checks it
against the fixtures the Rust tests pin:

- the dashboard's `/_status`, in `src/fixtures/dashboard-*.json`, pinned by
  `crates/cli/src/commands/dashboard/status/tests.rs`. Its `gateway.status`
  there is a stand-in that names the gateway fixture to splice in;
- the gateway's own `/_status`, which the dashboard passes through, in
  `src/fixtures/status-*.json`, pinned by
  `crates/cli/src/commands/gateway/status/tests.rs`.

After a change to either body, rewrite both sets with
`UPDATE_STATUS_FIXTURES=1 cargo nextest run -p decdn-cli status::` and make
`api.ts` match.
