# decdn gateway dashboard

The page `decdn gateway` serves at `/` on a loopback gateway, and on
`--metrics-listen`. It polls `GET /_status` every two seconds while visible.
`crates/cli/build.rs` embeds the build in the `decdn` binary; a build with no
`dist/` embeds a page that says how to make one.

Node 24 and pnpm 10 (`corepack enable` provides it):

```bash
pnpm -C crates/cli/dashboard install --frozen-lockfile
pnpm -C crates/cli/dashboard build    # writes dist/index.html, app.js, app.css
cargo build -p decdn-cli              # embeds dist/
```

`pnpm dev` serves the page with hot reload and proxies `/_status` to a
gateway on `127.0.0.1:8484`. Paste the gateway token into the page when it
asks.

Rules the gateway's content security policy sets: no inline script, no
inline style (no `style` prop; `eslint` refuses one), and nothing loaded from
another origin. The build emits exactly three files with fixed names; CI
checks that.

`src/api.ts` mirrors `crates/cli/src/commands/gateway/status.rs` field for
field: change both together.
