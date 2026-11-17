# Appendix: Public HTTP Gateway

> **This is an appendix, not a core protocol ADR.** The public gateway is a
> paying client that other clients reach over HTTP. Nodes, pools and
> contracts see ordinary paid pulls and nothing else. An operator can run a
> gateway with a different policy, or none.

**Date:** 2026-10-24 — **Status:** Accepted (`decdn gateway --public-url`
ships). **Touches:** `decdn` CLI (`gateway`, `publish point`), `decdn-client`
(byte windows), `decdn-incentive` (the buyer-pool snapshot), `decdn-cache`
(the gateway cache).

## Context

A client that holds no key, no USDC and no `decdn` binary cannot fetch from
the network. A publisher has no URL to put in a release note. The public
gateway gives them one. It is the loopback gateway of
[Directory Bundles](appendix-bundles.md#appendix-directory-bundles) with no
token. Both gateways pay only with the [capabilities](#capabilities) that
pool owners register. A gateway holds no keystore, no pool and no payment
state on disk. The caps of its capabilities bound what it spends.

## Capabilities

A capability ([ADR 003 § Capability delegation](003-payments.md#capability-delegation))
lets a delegate key that the gateway holds spend from a pool. Thus a pool
owner funds a fleet of gateways with no funded wallet on each machine. A
machine can stop, and a new one takes its place, with nothing to tear down.

The gateway holds at most one pending key. No capability claims a pending
key yet. `GET /_delegate` returns the address of the pending key, and makes
the key when there is none. Thus two calls return the same address until a
capability claims the key. The next call then makes a new pending key.

`PUT /_capability` takes `{"capability": "dcap1:…", "scope": …}`. The
`scope` is required. It is `"any"` for a capability that pays for any
content, or a signed [namespace scope](#scope). Thus a capability pays for
any content only when its registrant says so. The capability names a key
that the gateway holds. Its owner signature must recover the on-chain owner
of its pool on the gateway's deployment. A key holds at most one capability
per pool. The gateway answers:

| Status | When |
|--------|------|
| 201 | The capability is registered. The body names `expiry`, `pool` and `signer`, and `namespaces` for a scoped capability. |
| 409 | The gateway does not hold the key, or the key holds a capability from this pool. |
| 422 | The body is not `{"capability", "scope"}`, the scope is not `"any"` or a namespace scope, the scope names no namespace, the cap is zero, the expiry is inside the expiry margin, or a signature does not recover the pool's owner. |
| 502 | The pool could not be read on-chain. |

The gateway makes all checks that need no chain read before it reads the
pool. Thus a capability that cannot register costs the operator's RPC
nothing.

`GET /_capability` lists the capabilities in the order of registration. Each
entry has `cap`, `expiry`, `pool`, `signer` and `spent`. A scoped entry also
has `namespaces`. The gateway pays each pull with the first capability in
that order that can pay, after the [scope](#scope) rule. A capability can
pay when no funding stop retired it and when it is outside the expiry
margin of the nodes: one redeem interval plus 120 s before its `expiry`
([ADR 003 § Revocation](003-payments.md#revocation)). A capability whose
pull stops for funding before its first byte is retired, and the next one
takes the request. A retired capability stays in the list, with `spent`
true, until it expires. The gateway holds many capabilities at one time.
Thus an operator registers the next capability before the current one runs
low, and the gateway does not stop.

These routes need no credential. Nothing they do spends money without a
pool owner's signature. An operator who wants authentication puts a proxy
in front of the gateway. `GET /_capability` and
[`/_status`](#state) carry public data: pools, caps, expiries and the spend
of each key. An operator's controller reads them to see what is left.

The keys and the capabilities live in memory only. A restart drops all of
them. The operator's controller then gets the new pending key and registers
new capabilities.

### Scope

A publisher can fund a shared public gateway for its own content only. The
pool owner signs a scope. The scope limits one capability to a list of
namespaces. It is an EIP-712 record:

```text
CapabilityScope(address signer,bytes32 poolId,uint256[] namespaces)
```

The domain is `decdn-capability-scope`, version `1`, with the chain id and
the `PaymentPool` address as `verifyingContract`. `signer` and `poolId` name
the capability, so a scope does not move to another capability. The scope
signature must recover the pool's on-chain owner. `PUT /_capability` carries
it as `"scope": {"namespaces": [7, 9], "signature": "0x…"}`.
`decdn pool assign --namespace <id>` signs one, and prints the whole
`PUT /_capability` body. `decdn pool assign --any-namespace` prints the body
with `"scope": "any"`.

On a public gateway, a scoped capability pays only for a request that a
namespace pointer serves: a request on `<ns>.<host>` for the bundle that the
pointer names, with `ns` in the scope. The namespace's owner signs the
pointer. Thus another client can make the capability pay only for content
that the owner chose to serve. A request names a namespace in two more ways:
a `?ns=` query and a `<ns>.<hash32>.` label. Anyone can write these, so on a
public gateway they select no scoped capability.

A loopback gateway holds no pointers. Only the clients of the user and the
pages that the [browser check](#cross-origin-reads) lets through reach it. There, the
namespace that a request's `decdn:` URI names (`?ns=`, or the site label)
selects a scoped capability.

For a request of a namespace, the gateway pays with a capability scoped to
that namespace first, and then with an `"any"` capability. For every other
request, it pays with an `"any"` capability. The gateway cache serves a repeated request
with no pull, so repeated requests spend no more of the cap.

The scope controls which content a capability pays for. It does not control
which website asks. An `Origin` header proves nothing outside a browser, so
a public gateway stays readable by every origin.

## Hosts

The gateway serves one public origin, `--public-url`, behind a proxy that
terminates TLS. A request names what it wants by its `Host`:

| Host | Serves |
|------|--------|
| `<host>` | `GET /<hash>[/<path>][?ns=<id>]`: a blob streams; a bundle or an entry redirects to its site. |
| `<hash32>.<host>` | The bundle whose manifest hash is `<hash32>` (lowercase unpadded base32), as a website: `/` is its `index.html` or a listing, `/<path>` an entry. |
| `<ns>.<hash32>.<host>` | The same site, with the namespace routing hint `<ns>`. |
| `<ns>.<host>` | The bundle the namespace's [pointer](#namespace-pointer) names, as a website. Without a pointer the answer is 404. |

Every other `Host` is refused. The gateway's own listen address reaches the
bare host, for a health check.

A site is a separate browser origin. It is a separate site, with its own
cookies, only when `<host>` is a public suffix. The operator registers the
host on the Public Suffix List.

The API (`/_delegate`, `/_capability`, `/_pointer` and
[`/_status`](#state)) answers on the bare host only. On every other host it
answers 404. `/` keeps its short page.
The browser's `Sec-Fetch-*` headers decide nothing: the gateway pays. A
`/_decdn/` request with no `Sec-Fetch-Mode` is a navigation.

## Payment

Each pull pays with the first [capability](#capabilities) that can pay. The
pulls under one capability that run at one time share one payer. The buyer
store lives in a directory that the gateway removes when it stops, because
the keys whose lanes it records live in memory. A pull keeps a small
read-ahead, so a client that drops a response leaves little paid and not
delivered.

An entry's pull is capped at the size its manifest declares. A manifest
cannot make the gateway pay for more than it says. An entry whose pull, by
that size, passes `--max-blob-mb` answers 413 before any pull of the entry.

A raw blob has no manifest. The gateway uses the verified size of the blob.
Every node that states a size sends its size proof, and the fetch verifies
the proof against the hash before it uses the size
([ADR 005 § Bounded byte ranges](005-protocol.md#bounded-byte-ranges)). So
the server can stop reading a body at its `Content-Length`, and a short pull
never ends as a whole response. The gateway caps the pull of the blob at that
size, as it caps the pull of an entry. The empty root has a size of zero and
needs no pull.

Before it answers a raw blob of unknown size, the gateway learns its size. It
resolves the holders of the blob and takes the first size that one of them
proves: a probe answer, or the header of an open of the blob's first byte.
That open pays for one chunk group at most. The gateway keeps the size as a
verified size. A
response whose pull passes `--max-blob-mb` answers 413 before any pull. The
pull is the whole blob, or the chunk-group span of the one `Range`, so a
range of a large blob is served. A
funding stop ends the request as it ends a pull. A fault before any holder
proves a size, such as no holder, also ends the request as it ends a pull.

A blob that is not a manifest does not become one. Its hash fixes its bytes.
The manifest cap stops a pull only on verified bytes or a proven size. The
gateway keeps up to 1024 such hashes. It drops the least recently used hash
first. A repeat request for a kept hash as a bundle answers 404 and starts no
pull. The gateway does not keep a pull that fails upstream or on funding.

The answers:

| Status | When | Carries |
|--------|------|---------|
| 503 | No capability that the gateway holds can pay, and the cache does not hold the response. | `Retry-After: 60`, `Link: <decdn:…>; rel="alternate"`, JSON `{"error", "uri", "retry_after_secs"}` |
| 502 | The upstream pull failed. | A bare body. The detail is in the log. |

A paying client continues from the `decdn:` URI on its own. A loopback
gateway answers 402 with a text body when no capability can pay.

## Namespace pointer

A namespace id is stable. A bundle hash changes with each publish. The
pointer is the record that makes `<ns>.<host>` name the current bundle. A
publisher has one URL to give out.
[ADR 002](002-content-addressing.md#adr-002-content-addressing) puts a
mutable name above the protocol. The pointer is gateway-local: nodes, the
wire and the contracts never see it.

The pointer is an EIP-712 record:

```text
Pointer(uint256 namespaceId,bytes32 root,uint64 seq)
```

The domain is `decdn-pointer`, version `1`, with the chain id and the
`PublisherRegistry` address as `verifyingContract`. A pointer signed for one
deployment means nothing on another: namespace ids repeat across chains and
redeploys. `root` is the bundle's manifest hash. `seq` orders the pointers
of one signer: the gateway takes a pointer only above every `seq` it has
stored from that signer, so a replayed or reordered pointer changes nothing,
and a namespace that comes back to a former owner cannot be rolled back to a
pointer they signed before.

`decdn publish point <ns> decdn:<hash>/ --gateway <origin>` signs a pointer
with the namespace owner's keystore and sends it as `PUT /_pointer` on the
bare host, a JSON body `{"namespace", "root", "seq", "signature"}`. `seq`
defaults to the Unix time, so each publish advances it. The CLI makes no RPC
call. The gateway answers:

| Status | When |
|--------|------|
| 201 | The pointer is stored. The body names `namespace`, `root`, `seq` and the namespace's `url`. |
| 409 | The gateway stored a pointer from this signer at or above this `seq`. The body gives that `seq`. No chain read. |
| 422 | The body is not the record, or the signature is malformed. |
| 403 | The signature recovers an address that does not own the namespace. |
| 404 | The namespace does not exist on this deployment. On a loopback gateway every `PUT /_pointer` is 404. |
| 429 | The gateway's owner lookups for this minute are spent. `Retry-After` says when. |
| 502 | The owner could not be read on-chain. |

The gateway resolves the registry from `--publisher-registry-address`, else
`blockchain.publisher_registry_address` in the config file, else the built-in
testnet's when it runs on that chain. It recovers the signer and reads
`PublisherRegistry.ownerOf` for each pointer that passes the `seq` check.
The reads come out of a budget per minute, so an anonymous flood of
well-formed pointers costs the operator's RPC a bounded amount. A signer the
gateway stored a pointer from before was the owner once; it draws on a
smaller budget of its own per namespace, which a stranger's keys cannot
spend, so the flood does not keep a publisher from updating. A first pointer,
or a new owner's, waits for the shared budget. Limits per client belong on
the proxy, which knows the client. The owner read and the
store are one step, so two pointers for one namespace resolve in the order
their owners were read.

A `GET` never reads the owner. After a transfer on-chain the gateway serves
the former owner's bundle until the new owner points the namespace, on each
gateway separately. The new owner's first pointer replaces the held one
whatever the held `seq`. A transfer away from a compromised key ends that
key's hold with one `decdn publish point`.

Pointers live in `<data-dir>/pointers/<ns>.json`, one file per namespace,
with the signer each recovered to, the highest `seq` stored from each
signer, and the chain id and registry the owner was read on. The gateway
reads them at start and fails to start on a damaged file, or on a file
stored for another deployment: the same data dir on another chain or
registry does not serve pointers nobody there signed. A restart changes no
pointer.

## Ranges

A sized answer takes one `Range`: an entry whose manifest declares its size,
or a raw blob whose size a holder proved. The forms
are `bytes=a-b`, `bytes=a-` or `bytes=-n`. The answer is 206 over exactly that window, with
`Content-Range`. The pull pays for the chunk-group-aligned span around the
window ([ADR 038](038-bao-verified-range-streaming.md#adr-038-bao-verified-range-streaming-on-cdnclientv1)). A range past the end
is 416. A range the gateway cannot honour is ignored: several ranges, another
unit, a malformed range, a page rewritten as it streams, a raw blob of
unknown size. A sized answer carries `Accept-Ranges: bytes`, on `HEAD` too. A
BEP 19 webseed is a plain URL with ranges, so it needs nothing more.

`HEAD` on an entry answers from the bundle manifest: the entry's length and
hash, at no pull of the entry. `HEAD` on a raw blob, on the bare host or under
a site's `/_decdn/`, answers from the verified size, at no pull. `HEAD` on a
raw blob of unknown size learns its size first, as its `GET` does. A fault
that ends the size lookup ends the `HEAD` with the status that it gives a
`GET`. `HEAD`
of a bundle or an entry under a site's `/_decdn/` redirects to the site of
that bundle.

## Cross-origin reads

A public gateway asks for no credentials. It holds no token. Thus every
origin can read it. A page on any origin fetches a blob from `<host>` or an
entry from a site host, and reads the answer.

Every answer carries `Access-Control-Allow-Origin: *`.
`Access-Control-Expose-Headers` names `Content-Length`, `Content-Encoding`,
`Content-Range`, `Accept-Ranges`, `Content-Type`, `Content-Disposition`,
`ETag`, `Retry-After` and `Link`. The gateway does not encode a body. A proxy
in front of it can. `Content-Encoding` then tells a page that
`Content-Length` counts the encoded bytes. A page can also read a refusal. A
503 answer names the `decdn:` URI to fetch elsewhere, in its body and in
`Link`.

`OPTIONS` on a known host is a preflight. A preflight pulls nothing. It
counts as an ok answer in the metrics. On a content path the answer is 204,
with `Access-Control-Allow-Methods: GET, HEAD` and
`Access-Control-Allow-Headers: range`. On `/_pointer` of the bare host the
answer is 204, with `Access-Control-Allow-Methods: PUT` and
`Access-Control-Allow-Headers: content-type`. Thus a page can sign a pointer
with a wallet and send it. On `/_capability` of the bare host the methods
are `GET, PUT`, and on `/_delegate` and `/_status` the method is `GET`. All
these answers carry `Access-Control-Max-Age: 600`. A preflight on an API
path of a site or namespace host answers 404.

A site page has the policy `connect-src 'self'`. A page on another origin can
read a site. A site page cannot read another origin.

A loopback gateway takes no token. It checks the method and the browser
headers of every request, on every route, after the `Host` check. A
top-level `GET` navigation (`Sec-Fetch-Mode: navigate` and
`Sec-Fetch-Dest: document`) passes from any site, so a link on any page
opens content on the gateway. The gateway refuses every other request that
matches a row:

| Request | Answer |
|---------|--------|
| `Sec-Fetch-Site: cross-site` | 403 `a page on another site may not use the gateway` |
| An `Origin` that is not the bare host or one of its site origins | 403 `a page on another origin may not use the gateway` |
| No `Sec-Fetch-Site`, and an `Origin` or a `User-Agent` that starts `Mozilla/` | 403 `a browser that sends no Sec-Fetch-Site may not use the gateway` |

A client outside a browser (the SDKs, the CLI, `curl`) sends none of these
headers, and the gateway serves it. A browser sends them, and a page script
cannot change them. Thus a page that the user visits can make the gateway
pay only for content that the browser opens as a whole page, and the page
reads no answer. A browser that sends no `Sec-Fetch-Site` is refused. On a site origin,
a request from a page also must be the site's own (`same-origin`), the
user's (`none`), or a top-level navigation. On the bare host, a browser's
navigation (`Sec-Fetch-Mode: navigate`) to a bundle or an entry redirects to
the bundle's site. A loopback gateway sends no CORS headers, so a page on
another origin reads no answer.

## Cache

The gateway keeps what its pulls deliver. Each pull streams into a cache
under `<data-dir>/cache`. The cache is the node's cache engine with no origin
([ADR 040](040-cache-policy.md#adr-040-pluggable-cache-admission-and-eviction-policies)).
It verifies each chunk group against the
hash of the blob before it keeps the group. It keeps the blob under the size
that a holder proved.

A request is a cache hit when the cache holds every byte of the response:
the whole blob, or the chunk groups of its one `Range`. The cache must also
hold the blob under the size that the response states. The gateway serves a
hit from the cache. A hit starts no pull and spends no capability. Thus a hit
is served when no capability can pay. A blob that the
cache holds whole answers `HEAD` with its length, and needs no size lookup.

A bundle manifest that the cache holds whole is read from the cache, with no
pull. Thus a site entry, or the `index.html` at a site's `/`, is a hit when
the cache holds the manifest and the entry.

A request that the cache holds only in part is not a hit. It streams as a
pull. The pull fetches only the chunk groups that the cache does not hold.

The eviction loop of the node runs on the cache. It keeps the cache under
`--cache-mb` (10240 by default). It also keeps the free-disk margin of the
node on the volume. It releases the least recently used blobs first. A hit
counts as a use. The loop releases no blob that a pull is filling or a hit is
reading: a release lets GC remove the blob, and GC that removes a blob under a
fill loses the writes that follow.
`--cache-mb 0` turns the cache off. Each pull then fills a scratch directory,
and the gateway removes the directory when the pull ends.

Concurrent pulls of one blob fill one cache entry. Each pull fetches the
chunk groups that are missing when it plans a range. Thus two concurrent
pulls can pay for the same chunk group once each.

## Metrics

Every gateway counts what it serves, the loopback gateway too.
`--metrics-listen <loopback address>` serves two routes:

| Route | Answers |
|-------|---------|
| `GET /metrics` | Prometheus text. |
| `GET /_status` | The state of the gateway as one JSON document (see [State](#state)), with no token. |

The listener answers only its own loopback names in `Host`
(`<listen address>`, `localhost:<port>`, `127.0.0.1:<port>`, `[::1]:<port>`).
It refuses every other `Host` with 403. Thus a web page that rebinds its name
to the loopback address cannot read `/_status`.

The series follow
[Observability and Metrics](appendix-observability.md#appendix-observability-and-metrics):
one unlabeled sibling counter per outcome, every series exported from the
start.

| Series | Type | Counts |
|--------|------|--------|
| `decdn_gateway_bytes_served_total` | counter | Content bytes handed to responses. A node counts bao wire bytes in `decdn_bytes_served_total`, for every requester. For one whole-blob pull, the node counts about 0.4% more than this counter. For a range, the node counts the wire bytes of the chunk-group-aligned span. The node also counts manifest pulls. |
| `decdn_gateway_requests_ok_total` | counter | Answers with success or a redirect. |
| `decdn_gateway_requests_not_found_total` | counter | Answers 404. |
| `decdn_gateway_requests_refused_total` | counter | Answers refused before a pull: an unknown host, a refused browser request, a bad URI, a method, a range past the end. |
| `decdn_gateway_requests_payment_required_total` | counter | Answers 402: a loopback gateway holds no capability that can pay. |
| `decdn_gateway_pull_failures_funding_total` | counter | Answers 503: a public gateway holds no capability that can pay. |
| `decdn_gateway_pull_failures_other_total` | counter | Answers 502. |
| `decdn_gateway_pulls_active` | gauge | Pulls handed to the worker and not yet ended. |
| `decdn_gateway_pointer_updates_total` | counter | Namespace pointers stored through `PUT /_pointer`. |
| `decdn_gateway_cache_hits_total` | counter | Responses served from the cache, with no pull (see [Cache](#cache)). |

The exposition is for the operator's scraper. The listener takes a loopback
address only.

## State

`GET /_status` gives the state of the gateway as one JSON document. The
operator reads it with `decdn dashboard`
([Binaries § Dashboard](appendix-binaries.md#dashboard)), which polls it
every 2 seconds. The gateway serves no dashboard. The counters do not
count `/_status` requests on a loopback gateway's own host. A publisher who
wants to see how a site is served runs a loopback gateway and
`decdn dashboard`.

`/_status` answers on the gateway's bare host, and on `--metrics-listen`
when the operator sets it. It needs no token in either place.

`/_status` holds these fields:

| Field | Holds |
|-------|-------|
| `mode` | `loopback` or `public`. |
| `version` | The `decdn` version. |
| `started_unix`, `uptime_secs`, `listen` | When the gateway started, and its main listen address. |
| `chain_id`, `payment_pool` | The deployment the gateway pays on. |
| `counters` | The [metrics](#metrics) series, each by its name without the `decdn_gateway_` prefix and the `_total` suffix. |
| `active_pulls` | Each pull in flight: blob hash, namespace, byte window, payer (the capability, by its key and pool), age, bytes delivered. |
| `capabilities` | The registered capabilities, in the shape and order of `GET /_capability`. |
| `pointers` | A public gateway's namespace pointers: namespace, root, `seq`, signer. `null` on loopback. |
| `pools` | The buyer-pool store: the lanes of each registered capability's key, with the key's spend and bytes. These include the pulls in flight. |
| `pool_error` | Why `pools` is `null`. The text has its secrets removed. |

Each kind of value has one spelling. An address is EIP-55 checksummed hex, as
`GET /_capability` writes it. A pool id is `0x` hex. A content hash is bare
hex, as a gateway URL carries it.

A key's spend is what it signed for on this gateway. Other signers of the
same pool spend from it too, so the pool can hold less than the cap less the
spend. The capability's `cap` bounds the key's spend over every gateway.

The pull worker owns the store and reads it. The store holds what each pull
wrote when it ended. Thus the lanes in `pools` lag the pulls in flight. A key's spend and bytes include the pulls in flight. For
each lane, the gateway uses the larger of two values: the stored value, and
the value in the live voucher ledger of a pull in flight. The gateway reads
the live ledgers before the store, so a pull that ends during the read is
not lost. A read that takes more than 5 seconds fills `pool_error`.

A loopback gateway's `/_status` grants no cross-origin read: its preflight
answers 403. A public gateway's `/_status` is readable by every origin.
`decdn dashboard` reads `/_status` from its own process, not from a browser.

## Non-goals

- A name on chain. A namespace pointer is a gateway-local record above the
  protocol ([ADR 002](002-content-addressing.md#adr-002-content-addressing)).
  Two gateways may hold different pointers for one namespace; the publisher
  points each.
- A token on a gateway. Browser headers keep pages from spending on a
  loopback gateway, and a public gateway serves anyone.
- A keystore, a pool or a daily allowance on the gateway. An operator funds
  a gateway from outside, with capabilities, and the caps bound its spend.
- A capability chosen per page or per host. The gateway pays every pull with
  the capabilities it holds, in the order of registration.
- Keys that outlive a restart. The operator's controller provisions them
  again.
- Authentication of the API. A proxy in front of the gateway gives it.
