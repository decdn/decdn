# ADR 027 — Content Discovery: Namespace Registry and Origin Search Indexes

**Status:** Accepted
**Date:** 2026-04-13
**Deciders:** Core team
**Issue:** [#210](https://github.com/decdn/decdn/issues/210)

---

## Context

The BLAKE3 manifest hash is the entry point for every download in deCDN. ADR 012
calls manifest hashes "distributed out-of-band" without defining what that means in
practice. This leaves a gap in the core user journey:

> I want to download a file from deCDN — how do I find its hash?

For **encrypted content** (ADR 006 streaming use case) the app server already acts
as a hash oracle: the client authenticates, requests content by platform ID, and the
app server returns the manifest hash alongside the wrapped epoch key. That path is
complete.

For **public/unencrypted content** — the `decdn pull <hash>` CLI use case — there
is no mechanism. Content providers publish BLAKE3 hashes on their own websites or
APIs, but deCDN provides no standard way to discover them. This is the gap ADR 027
addresses.

---

## Decision

Introduce a two-layer content discovery system:

1. **On-chain namespace registry** — origins register a short namespace and an HTTP
   search endpoint URL alongside their node registration (ADR 019). The smart
   contract is the authoritative, tamper-evident mapping of namespace to endpoint.

2. **Origin-run HTTP search indexes** — each origin exposes an HTTP endpoint at the
   registered URL that accepts search queries and returns manifest hashes with
   metadata. The search implementation is entirely up to the origin operator; deCDN
   only defines the response schema.

The client CLI exposes a `decdn search` command to query these indexes.

```
# Search within a specific origin namespace
decdn search --origin decdn-ml google/gemma-4-3B-it

# Search across all registered origins (client fans out to all from the contract)
decdn search google/gemma-4-3B-it

# Typical pipeline
decdn search --origin crates-rs serde | decdn pull -o serde.tar.zst
```

---

## Namespace Registration

Namespace registration is added to the origin staking transaction (ADR 019). An
origin that wants to be discoverable includes these extra fields when calling
`registerOrigin(...)` on the registry contract:

```solidity
struct OriginSearchInfo {
    string  namespace;       // e.g. "decdn-ml" — unique, lowercase, [a-z0-9-]+
    string  searchEndpoint;  // HTTPS URL, max 256 bytes
}
```

Constraints enforced on-chain:

- `namespace` must match `^[a-z0-9][a-z0-9-]{1,30}[a-z0-9]$`.
- Namespace uniqueness: claim fails if already registered to a different address.
- Namespace ownership transfers with the staked node registration (same owner key).
- Namespace and endpoint can be updated via a separate `updateOriginSearch(...)`
  transaction (requires the registration owner key).

Origins that do not serve public content may omit both fields; they simply won't
appear in search results.

---

## HTTP Search Endpoint Wire Format

The search endpoint must support a single HTTP GET request:

```
GET <searchEndpoint>?q=<query>[&origin=<namespace>][&limit=<n>][&offset=<n>]
Accept: application/json
```

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `q` | string | yes | Free-text or structured query (format up to origin) |
| `limit` | uint | no | Max results to return (default 20, max 100) |
| `offset` | uint | no | Pagination offset |

Response (HTTP 200):

```json
{
  "results": [
    {
      "hash":      "blake3:aabbcc...",
      "name":      "google/gemma-4-3B-it",
      "namespace": "decdn-ml",
      "size_bytes": 2684354560,
      "description": "Gemma 4 3B instruction-tuned",
      "tags":      ["llm", "gemma", "google"],
      "version":   "v1.0.0"
    }
  ],
  "total":  42,
  "offset": 0
}
```

Fields:

| Field | Required | Notes |
|-------|----------|-------|
| `hash` | yes | `blake3:<hex>` — this is the value to pass to `decdn pull` |
| `name` | yes | Human-readable identifier within the namespace |
| `namespace` | yes | Must match the registered namespace |
| `size_bytes` | no | Total reconstructed size (sum of all chunks) |
| `description` | no | Free text, max 512 chars |
| `tags` | no | String array for filtering |
| `version` | no | Origin-defined version string |

Error responses use standard HTTP status codes (400 bad request, 500 server error).
No authentication is required; search indexes are public.

---

## Client CLI

### `decdn search`

```
decdn search [--origin <namespace>] [--limit <n>] [--json] <query>
```

Behaviour:

1. Fetch the registered origin list from the on-chain registry (or local cache, see
   below).
2. If `--origin` is given, query only that namespace's endpoint.
3. If `--origin` is omitted, fan out to **all** registered origins in parallel and
   merge results, ranked by relevance score returned by each origin (or insertion
   order if no score is given).
4. Display results as a table (default) or JSON (`--json`).

Example output:

```
HASH                          NAME                         ORIGIN      SIZE
blake3:aabbcc...              google/gemma-4-3B-it         decdn-ml    2.5 GiB
blake3:ddeeff...              google/gemma-4-3B-it-q4      decdn-ml    1.3 GiB
```

### Registry cache

Fetching the full origin list from the contract on every search is slow. The client
caches the registry locally at `~/.decdn/registry-cache.json` and refreshes it:

- On first use.
- Whenever a search returns an empty result set (possible stale cache).
- Explicitly via `decdn registry refresh`.
- After a configurable TTL (default 1 hour).

### `decdn registry`

```
decdn registry list              # print all registered origins and their endpoints
decdn registry refresh           # force-refresh local cache from contract
decdn registry show <namespace>  # show details for one namespace
```

---

## Trust Model

Search results affect **discoverability** only. The manifest hash returned by a
search endpoint is BLAKE3-verified when the client fetches the manifest blob, and
every chunk is BLAKE3-verified during download (ADR 002, ADR 012). A malicious or
compromised search endpoint can return:

- A wrong hash → `decdn pull` fails verification immediately.
- A hash that does not exist → `decdn pull` times out on DHT lookup.
- No results → content is undiscoverable via that origin, but the delivery network
  is unaffected.

The worst-case attack is a griefing denial-of-service against discoverability, not
a content substitution attack. On-chain namespace staking creates economic friction
for registering malicious origins.

---

## Alternatives Considered

### P2P search via `cdn/search/v1` ALPN

Add a `cdn/search/v1` ALPN to the iroh protocol so clients query origin nodes
directly over QUIC rather than HTTP.

**Pros:** protocol consistency (no HTTP dependency), NAT traversal built-in, always
encrypted, origins run one binary instead of two.

**Cons:** requires designing a new wire format and pagination protocol; no standard
caching layer (Cloudflare / nginx can front an HTTP endpoint trivially); richer query
semantics (full-text, filters, facets) are built-in for HTTP, custom for QUIC; higher
implementation cost for a search use case that is not performance-critical.

**Disposition:** deferred as the natural production evolution once the delivery
protocol is stable. The namespace registry design is transport-agnostic — the
`searchEndpoint` field can be either an HTTPS URL (PoC) or an iroh NodeId + ALPN
string (production). Both resolve from the same on-chain record. This ADR specifies
HTTP for the PoC; `cdn/search/v1` is reserved for a future amendment.

### DHT-based content catalog

Store `hash → metadata` directly in the DHT alongside `hash → node_list` (ADR 022).

**Pros:** fully P2P, no origin server dependency for discovery.

**Cons:** DHT is designed for point lookups (fetch by known key), not full-text
search; Kademlia prefix queries are expensive; origins would need to push all their
content hashes into the DHT; storage burden on non-origin nodes is unbounded.

**Disposition:** rejected for search. DHT remains the right mechanism for
`hash → node_list` lookups once the hash is already known.

### CRDT content catalog (from architecture.md Future Considerations)

A KV-CRDT namespace per content provider replicates `hash → metadata` across cache
nodes via gossip.

**Pros:** search survives origin downtime; distributed read path.

**Cons:** significant protocol complexity; merge semantics for search metadata are
non-trivial; unbounded storage growth on all nodes for all origins' catalogs.

**Disposition:** deferred post-mainnet. Complements rather than replaces the HTTP
index; the CRDT would be seeded by origin search indexes.

### Purely out-of-band (status quo)

Document that hash distribution is the content provider's responsibility and deCDN
is delivery-only, analogous to BitTorrent tracker sites being separate from the
protocol.

**Disposition:** rejected as the primary answer. Valid as a fallback (origins can
still share hashes via their own websites), but "out-of-band" is not a user-facing
CLI feature. The namespace registry provides an in-protocol, standardised discovery
path without mandating how origins implement their search indexes.

---

## PoC Simplification

```rust
#[cfg(feature = "poc")]
// Registry cache TTL is 1 hour (hardcoded). No `decdn registry` subcommand.
// Cross-origin fan-out sends requests sequentially rather than in parallel.
// `cdn/search/v1` ALPN not implemented; HTTP only.

#[cfg(not(feature = "poc"))]
// Parallel fan-out with configurable concurrency.
// `cdn/search/v1` ALPN supported as alternative to HTTP endpoint.
// Registry cache refresh triggered by block-log subscription.
```

---

## ADRs Affected

| ADR | Change |
|-----|--------|
| [ADR 012](012-client.md) | `decdn search` and `decdn registry` added to client CLI section |
| [ADR 016](016-contract-interactions.md) | `OriginSearchInfo` fields added to origin registration; `updateOriginSearch` function |
| [ADR 019](019-node-onboarding.md) | Namespace + endpoint included in optional origin registration fields |
| [ADR 022](022-content-discovery.md) | Clarify scope: DHT resolves hash → nodes; ADR 027 resolves name → hash |
