# Appendix: Local Admin HTTP Surface

> **This is an appendix, not a core protocol ADR.** The local admin HTTP surface is a binary-internal interface for operator runbook automation — other nodes/clients do not interact with it. Alternative implementations could expose admin via gRPC, Unix socket, signal handlers, or a dedicated CLI without breaking interop. This appendix specifies the loopback-bound HTTP API of the reference implementation.

## Context

A deCDN node has two observation points today:

- A loopback-only `/metrics` HTTP endpoint (hyper on `observability.metrics_port`, default `9090`) emitting OpenMetrics text — text-only, aggregate, read-only by design.
- The `cdn/probe/v1` ALPN, hittable by any remote iroh peer for latency/availability checks — not a local control channel, no auth beyond "anyone with an iroh connection".

Neither fits when a same-host operator must **read live internal state** (readiness, lane state) or **trigger a local control action** (graceful drain). A control-plane surface is therefore needed; this appendix pins its shape so every `admin_v1_…` method shares a common transport.

## Decision

A running deCDN node exposes a **loopback-only JSON-RPC 2.0 server** on a configurable port (`observability.admin_port`, default `9191`), dispatched over HTTP `POST /`, framed per <https://www.jsonrpc.org/specification>.

The surface version lives in the namespace prefix (`admin_v1_...`), not a URL path segment, since JSON-RPC dispatches on the envelope's `method` field. Within `v1`, routes accrete fields backwards-compatibly; a breaking change cuts over to `admin_v2_...`.

Method set (the `AdminRpc` trait in `decdn-common` is the canonical surface):

| Method             | Params                  | Result            |
| ------------------ | ----------------------- | ----------------- |
| `admin_v1_health`  | none                    | `HealthResponse`  |
| `admin_v1_status`  | none                    | `StatusResponse`  |
| `admin_v1_drain`   | `DrainRequest` (optional) | `DrainResponse`   |
| `admin_v1_evict`   | `EvictRequest`          | `EvictResponse`   |
| `admin_v1_reload`  | none                    | `ReloadResponse`  |
| `admin_v1_lanes`   | none                    | `LanesResponse`   |
| `admin_v1_slashes` | none                    | `SlashesResponse` |

Response format:

- Per JSON-RPC 2.0: results under `"result"`, errors under `"error"` with numeric `code`, `message`, optional structured `data`. HTTP status `200` for valid envelopes regardless of method-level success/failure. Parse/malformed envelopes use standard codes (`-32700 Parse error`, `-32600 Invalid Request`, `-32601 Method not found`, `-32602 Invalid params`, `-32603 Internal error`).

Transport shape:

- Binds on `127.0.0.1:<port>` only. Never on a public interface.
- Uses [`jsonrpsee`](https://github.com/paritytech/jsonrpsee) (server + http-client + macros) so the trait is the single source of truth for both wire sides — no hand-rolled routing or JSON parsing.
- Per-connection concurrency capped via `ServerBuilder::max_connections`; shutdown via `ServerHandle::stop()` from the existing runtime shutdown sequence.

Config shape:

- New field `observability.admin_port: Option<u16>`. Missing resolves to default `9191` (enabled); explicit `0` disables the admin server. Default-on keeps operator tooling working out of the box; disabling is a deliberate opt-out.
- Resolution rejects `admin_port == metrics_port` up front to avoid a silent bind failure. Clap flag `--admin-port`, env `DECDN_ADMIN_PORT`.

CLI shape:

- New subcommand group `decdn node` whose children talk to the admin server: initially `decdn node health`, with `decdn node drain` to follow.
- Admin URL resolution (`decdn node ...`) precedence:
  1. `--admin-url` flag, or `DECDN_ADMIN_URL` env (folded into the flag by clap's `env =`).
  2. `observability.admin_port` from the TOML config file — subcommand-level `--config`, then top-level `decdn --config`, then default `~/.decdn/node.toml`. An explicit path that doesn't exist errors; the default path missing falls through. `admin_port = 0` in the file errors rather than silently probing the default port.
  3. Built-in default `http://127.0.0.1:9191`.

## Rationale

### Why HTTP on loopback, not a Unix domain socket?

- The node already runs a loopback HTTP server for metrics; one transport shape reduces operator surface area (one port kind to document, one client kind to script), and `curl` + `jq` still works for ad-hoc debugging.
- Localhost TCP is portable; a UDS would make future Windows support awkward (target is Linux/macOS).
- A UDS's filesystem permissions only improve on `127.0.0.1` when multiple local users are untrusted — a small gap for a PoC single-operator deployment, closable later via a shared-secret header without changing the transport.

### Why JSON-RPC, not REST-style routes?

- The surface dispatches named operations, not resource state. Actions like `drain` or `reload` are neither `GET` nor `PUT` on a resource; REST's verb/URI model adds friction without benefit.
- jsonrpsee's `#[rpc(server, client)]` macro makes the Rust trait the single source of truth: server impl and client bindings generate together, so the two sides cannot drift on method name, params, or return shape. A hand-written hyper route table would need separately maintained `handle(&req)` branches and reqwest JSON parsing.
- Cleaner error modeling: JSON-RPC carries errors in a structured `{ code, message, data }` object on a `200`; route-based HTTP conflates transport `404` (missing route) with application `404` (peer not found) unless the server is careful.
- Modest cost: jsonrpsee is a larger dependency than a hand-rolled hyper service, but as the surface accretes methods the per-method cost is one trait method vs. one handler plus a routing entry.

### Why versioned method names (`admin_v1_...`)?

- A version prefix evolves existing methods (add fields, rename) within `v1` and cuts over to `admin_v2_...` for a breaking change without breaking operator scripts. The version lives in the method name because JSON-RPC routes by the envelope's `method` string — there is no URL path segment for a version.

### Why no authentication (for now)?

- The server binds on `127.0.0.1` only, so any caller is already on the same host as the node process; on a single-operator host, filesystem permissions on the node's data dir are the effective trust boundary.
- Multi-tenant hosts are explicitly **not** a PoC target. If they become one, a follow-up ADR can layer a shared-secret `Authorization: Bearer <token>` header on the same transport — no wire-shape change required.

## Consequences

- The node opens one additional TCP socket by default; operators wanting zero extra listeners set `observability.admin_port = 0`.
- `decdn node <sub>` commands are the operator-preferred way to read live state; `/metrics` stays focused on time-series aggregates (Prometheus scrapes) and `cdn/probe/v1` on remote peer-health checks.
- Future operational routes (drain, reload) ship here by default — resisting the "new surface per command" drift that produces a half-dozen ops-tooling sockets per node.
- The bind is hardcoded to `127.0.0.1` in `crates/node/src/runtime/mod.rs`, so exposing internal state on a non-loopback address is a code change rather than a config mistake.

## Implementation Notes

- `crates/common/src/admin.rs` defines the `AdminRpc` trait with `#[rpc(server, client, namespace = "admin_v1")]`, the JSON DTOs, error code constants, and the `parse_hash_arg` helper; both binaries import from there.
- `crates/node/src/admin.rs` carries the daemon-side server impl: `AdminState` (the cache, reload hook, drain trigger), `AdminRpcImpl`, and the `bind` / `serve` helpers.
- JSON DTOs (`HealthResponse`, `StatusResponse`, `DrainResponse`, etc.) live in the shared crate rather than derived from internal types, so the wire format stays stable when internal structs change.
- `decdn node health` lives in `crates/cli/src/commands/node.rs` and uses `jsonrpsee::http_client::HttpClient` with the generated `AdminRpcClient` trait — no hand-rolled JSON or HTTP on the client side.
