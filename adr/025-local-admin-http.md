# ADR 025: Local Admin HTTP Surface

**Date:** 2026-04-17
**Status:** Draft

---

## Context

Operators running a deCDN node have two observation points today:

- A loopback-only `/metrics` HTTP endpoint (hyper on
  `observability.metrics_port`, default `9090`) that emits OpenMetrics text.
- The `cdn/probe/v1` ALPN, which any remote iroh peer can hit for latency
  and availability checks.

Neither is a good fit when an operator on the same host needs to **read
live internal state** (e.g. the gossip peer table — issue #247) or **trigger
a local control action** (e.g. a graceful drain — issue #244). `/metrics` is
text-only, aggregate, and read-only by design; `cdn/probe/v1` is not intended
as a local control channel and carries no auth beyond "anyone with an iroh
connection".

A control-plane surface is therefore needed. This ADR pins down the shape
of that surface so the first route (`GET /v1/peers`, added for #247) and
future ones (drain, health, config reload, …) share a common transport.

---

## Decision

A running deCDN node exposes a **loopback-only HTTP admin server** on a
configurable port (`observability.admin_port`, default `9191`). It speaks
JSON on versioned `/v1/...` routes.

Initial route set:

| Method | Path         | Purpose                                   |
| ------ | ------------ | ----------------------------------------- |
| `GET`  | `/v1/peers`  | Dump the current gossip peer table.       |

Future routes that are **expected** to use this surface (not designed here):

- `POST /v1/drain` — graceful drain (#244).
- `GET  /v1/health` — readiness/liveness probe.
- `POST /v1/config/reload` — reload mutable config sections.

Response format:

- `200` responses are `application/json`.
- Errors are JSON too: `{"error":"<slug>"}` with semantic HTTP status
  codes (`404 not_found`, `405 method_not_allowed`, `500 internal`).

Transport shape:

- Binds on `127.0.0.1:<port>` only. Never on a public interface.
- Hyper `service_fn` over a pre-bound `TcpListener`, identical pattern to
  the metrics server in `crates/node/src/metrics.rs`.
- Per-connection concurrency capped by a semaphore.
- Shutdown via a `oneshot::Receiver<()>` fired from the runtime's existing
  shutdown sequence.

Config shape:

- New field `observability.admin_port: Option<u16>`.
- `0` or missing means "admin server disabled".
- Resolution rejects `admin_port == metrics_port` up front to avoid a
  silent bind failure.
- Clap flag `--admin-port`, env `DECDN_ADMIN_PORT`.

CLI shape:

- New subcommand group `decdn node`, whose children talk to the admin
  server: initially `decdn node peers`, with `decdn node drain` to follow
  under #244.
- Admin URL resolution (`decdn node ...`): `--admin-url` flag, then
  `DECDN_ADMIN_URL` env var, then default `http://127.0.0.1:9191`.

---

## Rationale

### Why HTTP on loopback, not a Unix domain socket?

- The node already runs a loopback HTTP server for metrics; keeping one
  transport shape reduces operator surface area (one kind of port to
  document, one kind of client to script).
- `curl` + `jq` is the universal admin-debug toolchain; no extra client
  glue needed.
- Portability: the current deployment target is Linux/macOS, but a UDS
  would make any future Windows support awkward. Localhost TCP is
  portable.
- Security posture: a UDS's filesystem permissions are a real
  improvement over `127.0.0.1` only when multiple local users are
  untrusted. For a PoC single-operator deployment, that gap is small
  and can be closed in a later ADR by adding a shared-secret header
  without changing the transport.

### Why not a new iroh ALPN (e.g. `cdn/admin/v1`)?

- The admin surface is a **local operator** tool, not a node-to-node
  protocol. Running it over iroh would give it NodeId-based auth — useful
  — but also pull in relay traffic, QUIC handshakes, and the iroh
  connection lifecycle for what needs to be a zero-dependency,
  always-on-localhost debug channel.
- Separating admin from node-to-node protocol boundaries means a buggy
  admin route cannot affect the CDN wire protocol ALPNs.

### Why versioned routes (`/v1/...`)?

- Admin surfaces accrete routes over time. A version prefix lets us
  evolve the shape of existing routes (add fields, rename) within `v1`,
  and cut over to `v2` without breaking operator scripts when a breaking
  change is necessary.

### Why no authentication (for now)?

- The server binds on `127.0.0.1` only; any caller is already on the
  same host as the node process.
- On a single-operator host, filesystem permissions on the node's data
  dir are already the effective trust boundary.
- Multi-tenant hosts are explicitly **not** a PoC target. If they become
  one, a follow-up ADR can layer a shared-secret `Authorization: Bearer
  <token>` header on the same transport — no wire-shape change required.

---

## Consequences

- The node opens one additional TCP socket by default. Operators who want
  zero extra listeners can set `observability.admin_port = 0`.
- `decdn node <sub>` commands are the operator-preferred way to read live
  state; the `/metrics` endpoint stays focused on time-series aggregates
  (Prometheus scrapes), and the `cdn/probe/v1` ALPN stays focused on
  remote peer-health checks.
- Future operational routes (drain, reload) ship here by default —
  resisting the "new surface for each new command" drift that produces
  a half-dozen ops-tooling sockets per node.
- A careless operator who binds `admin_port` to a non-loopback address by
  editing the source would expose internal state; the binding is
  hardcoded to `127.0.0.1` in `crates/node/src/runtime/mod.rs` to make
  that a code change rather than a config mistake.

---

## Implementation Notes

- `crates/node/src/admin.rs` implements the hyper server and mirrors
  `crates/node/src/metrics.rs` module-for-module.
- `AdminState` carries `Arc<RwLock<PeerTable>>` (and will grow more
  handles as new routes land).
- JSON DTOs (`PeerView`) are defined in `admin.rs` rather than derived
  from internal types so the wire format can stay stable even when
  internal structs change.
- `decdn node peers` lives in `crates/node/src/commands/node.rs` and
  uses `reqwest` for the HTTP call.

---

## Alternatives considered

- **Unix domain socket.** Better multi-user isolation; worse portability
  and higher client-side friction. Revisit if multi-tenant hosts enter
  scope.
- **New iroh ALPN.** NodeId-based auth is attractive for remote admin,
  but this surface is specifically not remote.
- **Extending `/metrics` with non-Prometheus routes.** Mixes a scraped
  time-series surface with mutating operations; operators would have to
  lock down the metrics endpoint more aggressively than they do today.
