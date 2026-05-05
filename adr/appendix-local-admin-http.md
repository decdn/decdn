# Appendix: Local Admin HTTP Surface

> **This is an appendix, not a core protocol ADR.** The local admin HTTP surface is a binary-internal interface for operator runbook automation — other nodes/clients on the network do not interact with it. Alternative implementations could expose admin via gRPC, Unix socket, signal handlers, or a dedicated CLI without breaking interop. This appendix specifies the loopback-bound HTTP API used by the reference implementation.

## Context

Operators running a deCDN node have two observation points today:

- A loopback-only `/metrics` HTTP endpoint (hyper on `observability.metrics_port`, default `9090`) that emits OpenMetrics text.
- The `cdn/probe/v1` ALPN, which any remote iroh peer can hit for latency and availability checks.

Neither is a good fit when an operator on the same host needs to **read live internal state** (e.g. the gossip peer table — issue #247) or **trigger a local control action** (e.g. a graceful drain — issue #244). `/metrics` is text-only, aggregate, and read-only by design; `cdn/probe/v1` is not intended as a local control channel and carries no auth beyond "anyone with an iroh connection".

A control-plane surface is therefore needed. This ADR pins down the shape of that surface so the first method (`admin_v1_peersList`, added for #247) and future ones (drain, health, config reload, …) share a common transport.

## Decision

A running deCDN node exposes a **loopback-only JSON-RPC 2.0 server** on a configurable port (`observability.admin_port`, default `9191`). Methods are dispatched over HTTP `POST /`, framed by JSON-RPC 2.0 per the spec at <https://www.jsonrpc.org/specification>.

Method names carry the surface version in the namespace prefix (`admin_v1_...`) rather than in a URL path segment, because JSON-RPC dispatches on the envelope's `method` field — there is no URL path to version. `v1` means the same thing it did under the old `/v1/...` scheme: routes may accrete fields backwards-compatibly within `v1`, and a breaking change cuts over to `admin_v2_...`.

Initial method set:

| Method                 | Params | Result                         |
| ---------------------- | ------ | ------------------------------ |
| `admin_v1_peersList`   | none   | `{ peers: PeerView[] }`        |

Future methods that are **expected** to use this surface (not designed here):

- `admin_v1_drain` — graceful drain (#244).
- `admin_v1_health` — readiness/liveness probe.
- `admin_v1_configReload` — reload mutable config sections.

Response format:

- Per JSON-RPC 2.0: successful results live under `"result"`, errors under `"error"` with a numeric `code`, a `message`, and optional structured `data`. HTTP status is `200` for valid JSON-RPC envelopes regardless of success/failure at the method level. Parse failures and malformed envelopes use the standard JSON-RPC error codes (`-32700 Parse error`, `-32600 Invalid Request`, `-32601 Method not found`, `-32602 Invalid params`, `-32603 Internal error`).

Transport shape:

- Binds on `127.0.0.1:<port>` only. Never on a public interface.
- Uses [`jsonrpsee`](https://github.com/paritytech/jsonrpsee) (server + http-client + macros) so the trait is the single source of truth for both sides of the wire — no hand-rolled routing or JSON parsing.
- Per-connection concurrency capped via `ServerBuilder::max_connections`.
- Shutdown via `ServerHandle::stop()` fired from the runtime's existing shutdown sequence.

Config shape:

- New field `observability.admin_port: Option<u16>`.
- Missing resolves to the default admin port `9191` (enabled); explicit `0` means "admin server disabled". The "default-on" choice keeps the operator tooling working out of the box — an operator who wants the surface off has to opt out deliberately.
- Resolution rejects `admin_port == metrics_port` up front to avoid a silent bind failure.
- Clap flag `--admin-port`, env `DECDN_ADMIN_PORT`.

CLI shape:

- New subcommand group `decdn node`, whose children talk to the admin server: initially `decdn node peers`, with `decdn node drain` to follow under #244.
- Admin URL resolution (`decdn node ...`) precedence:
  1. `--admin-url` flag, or `DECDN_ADMIN_URL` env (folded into the flag by clap's `env =`).
  2. `observability.admin_port` from the TOML config file — the subcommand-level `--config`, then the top-level `decdn --config`, then the default `~/.decdn/node.toml`. An explicit path that doesn't exist is an error; the default path missing is fine (just falls through). `admin_port = 0` in the file errors rather than silently probing the default port.
  3. Built-in default `http://127.0.0.1:9191`.

## Rationale

### Why HTTP on loopback, not a Unix domain socket?

- The node already runs a loopback HTTP server for metrics; keeping one transport shape reduces operator surface area (one kind of port to document, one kind of client to script).
- `curl` + `jq` still works for ad-hoc debugging — a JSON-RPC envelope is a single `POST` with a short JSON body.
- Portability: the current deployment target is Linux/macOS, but a UDS would make any future Windows support awkward. Localhost TCP is portable.
- Security posture: a UDS's filesystem permissions are a real improvement over `127.0.0.1` only when multiple local users are untrusted. For a PoC single-operator deployment, that gap is small and can be closed in a later ADR by adding a shared-secret header without changing the transport.

### Why JSON-RPC, not REST-style routes?

- The surface exists to dispatch named operations, not to manipulate resource state. REST's verb/URI model is a bad fit — operator actions like `drain` or `configReload` are neither `GET` nor `PUT` on a resource, and forcing them into that shape adds friction without benefit.
- jsonrpsee's `#[rpc(server, client)]` macro makes the Rust trait the single source of truth: server impl and client bindings are generated together, so the two sides cannot drift on method name, params, or return shape. The old hyper route table required paired hand-written `handle(&req)` branches and reqwest JSON parsing, both maintained separately.
- Error modeling is cleaner. JSON-RPC carries errors inside a structured `{ code, message, data }` object on a `200` response; route-based HTTP conflates transport failures (`404` because the route is missing) with application failures (`404` because a peer wasn't found) unless the server is careful. We don't have to be careful about that distinction.
- Cost is modest: jsonrpsee is a larger dependency than a hand-rolled hyper service, but the admin surface is expected to accrete methods over time (drain, health, config reload, and more), and the per- method cost with jsonrpsee is one trait method vs. one handler plus a routing entry.

### Why versioned method names (`admin_v1_...`)?

- Admin surfaces accrete methods over time. A version prefix lets us evolve the shape of existing methods (add fields, rename) within `v1`, and cut over to `admin_v2_...` without breaking operator scripts when a breaking change is necessary.
- The version lives in the method name, not the URL, because JSON-RPC routes by the envelope's `method` string — there is no URL path segment to place a version in.

### Why no authentication (for now)?

- The server binds on `127.0.0.1` only; any caller is already on the same host as the node process.
- On a single-operator host, filesystem permissions on the node's data dir are already the effective trust boundary.
- Multi-tenant hosts are explicitly **not** a PoC target. If they become one, a follow-up ADR can layer a shared-secret `Authorization: Bearer <token>` header on the same transport — no wire-shape change required.

## Consequences

- The node opens one additional TCP socket by default. Operators who want zero extra listeners can set `observability.admin_port = 0`.
- `decdn node <sub>` commands are the operator-preferred way to read live state; the `/metrics` endpoint stays focused on time-series aggregates (Prometheus scrapes), and the `cdn/probe/v1` ALPN stays focused on remote peer-health checks.
- Future operational routes (drain, reload) ship here by default — resisting the "new surface for each new command" drift that produces a half-dozen ops-tooling sockets per node.
- A careless operator who binds `admin_port` to a non-loopback address by editing the source would expose internal state; the binding is hardcoded to `127.0.0.1` in `crates/node/src/runtime/mod.rs` to make that a code change rather than a config mistake.

## Implementation Notes

- `crates/node/src/admin.rs` defines the `AdminRpc` trait with `#[rpc(server, client, namespace = "admin_v1")]` and the concrete server impl backed by the gossip `PeerTable`.
- `AdminState` carries `Arc<RwLock<PeerTable>>` (and will grow more handles as new methods land).
- JSON DTOs (`PeerView`, `PeersResponse`) are defined in `admin.rs` rather than derived from internal types so the wire format can stay stable even when internal structs change.
- `decdn node peers` lives in `crates/node/src/commands/node.rs` and uses `jsonrpsee::http_client::HttpClient` with the generated `AdminRpcClient` trait — no hand-rolled JSON or HTTP logic on the client side.

## Alternatives Considered

The five admin-surface alternatives evaluated against jsonrpsee-on-loopback (hand-rolled hyper REST, jsonrpsee + OpenRPC, Unix domain socket, new iroh ALPN, extending `/metrics`) are recorded in [`_history/alternatives-pre-launch.md` § Local Admin HTTP Surface (appendix)](_history/alternatives-pre-launch.md#local-admin-http-surface-appendix).
