# Appendix: Public HTTP Gateway

> **This is an appendix, not a core protocol ADR.** The public gateway is a
> paying client that other clients reach over HTTP. Nodes, pools and
> contracts see ordinary paid pulls and nothing else. An operator can run a
> gateway with a different policy, or none.

**Date:** 2026-10-24 — **Status:** Accepted (`decdn gateway --public-url`
ships). **Touches:** `decdn` CLI (`gateway`, `publish point`), `decdn-client`
(byte windows), `decdn-incentive` (the buyer-pool snapshot).

## Context

A client that holds no key, no USDC and no `decdn` binary cannot fetch from
the network. A publisher has no URL to put in a release note. The public
gateway gives them one. It is the loopback gateway of
[Directory Bundles](appendix-bundles.md#appendix-directory-bundles) with its
token and capability layer replaced by one pool the operator funds, and an
allowance that bounds what that pool spends.

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

The API of the loopback gateway (`/_delegate`, `/_capability`) answers 404.
`/_status` and the [dashboard](#dashboard) answer 404 too.
The browser's `Sec-Fetch-*` headers decide nothing: the operator pays, within
the allowance. A `/_decdn/` request with no `Sec-Fetch-Mode` is a navigation.

## Payment and allowance

Every pull pays from the operator's pool. The pull worker builds one payer
from the keystore when the gateway starts, and every pull shares it. Pool
open and top-up run inside the pull path ([ADR 003](003-payments.md#adr-003-payment-model)).

`--allowance-bytes-per-day` bounds the bytes the gateway delivers per UTC
day, over every request. The namespace hint buys no bucket of its own: a
client sets it. `--allowance <ns>=<bytes>` gives one namespace a bucket of
its own. That bucket replaces the shared one for the requests the
namespace's pointer serves, on `<ns>.<host>`, and for those alone. A pointer
is signed by the namespace's owner, so a request on that host is the
namespace's. A `?ns=` query or a `<ns>.<hash32>.` label is anyone's to
write, so it spends the shared bucket. The gateway counts:

- every byte a pull lands, a manifest that does not parse included;
- an entry's declared size before its pull, and refuses one that does not
  fit;
- the chunk-group-aligned span of a range, not the window alone;
- a bound of what a pull may have paid past the frames it delivered, when a
  response does not end whole.

An entry's pull is capped at the size its manifest declares. A manifest
cannot make the gateway pay for more than it says.

The answers:

| Status | When | Carries |
|--------|------|---------|
| 402 | The allowance is spent, or the request does not fit what is left. | `Retry-After`, `Link: <decdn:…>; rel="alternate"`, JSON `{"error", "uri", "retry_after_secs"}` |
| 503 | The gateway's pool cannot pay. | The same as 402. |
| 502 | The upstream pull failed. | A bare body. The detail is in the log. |

A paying client continues from the `decdn:` URI on its own.

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

An entry whose manifest declares its size takes one `Range`: `bytes=a-b`,
`bytes=a-` or `bytes=-n`. The answer is 206 over exactly that window, with
`Content-Range`. The pull pays for the chunk-group-aligned span around the
window ([ADR 038](038-bao-verified-range-streaming.md#adr-038-bao-verified-range-streaming-on-cdnclientv1)). A range past the end
is 416. A range the gateway cannot honour is ignored: several ranges, another
unit, a malformed range, a page rewritten as it streams, a blob of unknown
size. A sized answer carries `Accept-Ranges: bytes`, on `HEAD` too. A BEP 19
webseed is a plain URL with ranges, so it needs nothing more.

`HEAD` on an entry answers from the bundle manifest: the entry's length and
hash, at no pull of the entry.

## Cross-origin reads

A public gateway asks for no credentials. It holds no token. Thus every
origin can read it. A page on any origin fetches a blob from `<host>` or an
entry from a site host, and reads the answer.

Every answer carries `Access-Control-Allow-Origin: *`.
`Access-Control-Expose-Headers` names `Content-Length`, `Content-Range`,
`Accept-Ranges`, `Content-Type`, `Content-Disposition`, `ETag`, `Retry-After`
and `Link`. A page can also read a refusal. A 402 or 503 answer names the
`decdn:` URI to fetch elsewhere, in its body and in `Link`.

`OPTIONS` on a known host is a preflight. A preflight pulls nothing. It
counts as an ok answer in the metrics. On a content path the answer is 204,
with `Access-Control-Allow-Methods: GET, HEAD` and
`Access-Control-Allow-Headers: range`. On `/_pointer` of the bare host the
answer is 204, with `Access-Control-Allow-Methods: PUT` and
`Access-Control-Allow-Headers: content-type`. Thus a page can sign a pointer
with a wallet and send it. Both answers carry `Access-Control-Max-Age: 600`.
A preflight on `/_delegate` or `/_capability`, or on `/_pointer` of a site or
namespace host, answers 404.

A site page has the policy `connect-src 'self'`. A page on another origin can
read a site. A site page cannot read another origin. A loopback gateway sends
CORS headers on its API only. A page on another origin cannot send the token
header to content, and it cannot read content.

## Metrics

Every gateway counts what it serves, the loopback gateway too.
`--metrics-listen <loopback address>` serves four routes, with no token:

| Route | Answers |
|-------|---------|
| `GET /metrics` | Prometheus text. |
| `GET /_status` | The state of the gateway as one JSON document (see [Dashboard](#dashboard)). |
| `GET /` | The dashboard. |
| `GET /_dashboard/app.js`, `GET /_dashboard/app.css` | The dashboard's script and style. |

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
| `decdn_gateway_requests_refused_total` | counter | Answers refused before a pull: an unknown host, a bad URI, a method, a range past the end. |
| `decdn_gateway_requests_allowance_exhausted_total` | counter | Answers 402. |
| `decdn_gateway_pull_failures_funding_total` | counter | Answers 503. |
| `decdn_gateway_pull_failures_other_total` | counter | Answers 502. |
| `decdn_gateway_pulls_active` | gauge | Pulls handed to the worker and not yet ended. |
| `decdn_gateway_pointer_updates_total` | counter | Namespace pointers stored through `PUT /_pointer`. |

The exposition is for the operator's scraper. The listener takes a loopback
address only.

## Dashboard

The dashboard is a web page for the operator. It reads `GET /_status` every
2 seconds while the page is visible. A publisher who wants to see how a site
is served runs a loopback gateway and opens its dashboard.

| Gateway | Where the dashboard and `/_status` answer |
|---------|-------------------------------------------|
| Loopback | On the gateway's own host. `/_status` needs the gateway token in `x-decdn-token`, as `GET /_capability` does. |
| Any, with `--metrics-listen` | On that listener, with no token. |
| Public | Not on the public host. `/_status` and `/_dashboard/*` answer 404 there, and `/` keeps its short page. |

`/_status` holds these fields:

| Field | Holds |
|-------|-------|
| `mode` | `loopback` or `public`. |
| `version`, `dashboard_built` | The `decdn` version. `true` when the binary embeds the dashboard's build. |
| `started_unix`, `uptime_secs`, `listen` | When the gateway started, and its main listen address. |
| `chain_id`, `payment_pool` | The deployment the gateway pays on. |
| `counters` | The [metrics](#metrics) series, each by its name without the `decdn_gateway_` prefix and the `_total` suffix. |
| `active_pulls` | Each pull in flight: blob hash, namespace, byte window, payer, age, bytes delivered. |
| `capabilities` | The registered capabilities, in the shape of `GET /_capability`. A public gateway holds none. |
| `allowance` | A public gateway's buckets: the shared bucket first, then each namespace. Each has its bytes per day (`null` for no bound), the bytes delivered today, the bytes left (`null` for no bound) and the seconds until the UTC day turns. `null` on loopback. |
| `pointers` | A public gateway's namespace pointers: namespace, root, `seq`, signer. `null` on loopback. |
| `pools` | The buyer-pool store: each pool with its deposit, its spend and its lanes, and the lanes of each registered capability's key. |
| `pool_error` | Why `pools` is `null`. The text has its secrets removed. |

The pool spend is the pool's spend as the store knows it: the lanes of the
store plus the spend that other signers redeemed. The deposit less the spend
is what all signers of the pool can still spend. A loopback gateway pays with
capabilities. The store keeps their lanes apart from the pool rows, so
`pools` lists them per key.

The pull worker owns the store and reads it. The store holds what each pull
wrote when it ended. Thus `pools` lags the pulls in flight. A read that takes
more than 5 seconds fills `pool_error`.

The gateway token reaches the page in the fragment of the link that
`decdn gateway` prints: `http://<listen>/#token=<token>`. A browser does not
send a fragment to the server. The page moves the token to session storage
and removes the fragment from the address. A site origin of the gateway is a
different origin and cannot read that storage. A cross-origin page cannot
send the token header: the preflight of `/_status` answers 403.

The page loads its script and its style from the gateway only. It holds no
inline script and no inline style. Its content security policy is
`default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self';
img-src 'self' data:; base-uri 'none'; form-action 'none';
frame-ancestors 'none'`.

The page is a Vite app in `crates/cli/dashboard/`. Its build is three files:
`index.html`, `app.js` and `app.css`. The build script of `decdn-cli` embeds
them in the binary. A build with no Node toolchain embeds a short page that
tells how to build the dashboard. Thus `cargo build` works without Node. A
release never ships that page: the release workflow builds the dashboard
first and refuses a `decdn` binary that holds it.

## Non-goals

- A name on chain. A namespace pointer is a gateway-local record above the
  protocol ([ADR 002](002-content-addressing.md#adr-002-content-addressing)).
  Two gateways may hold different pointers for one namespace; the publisher
  points each.
- A token or capability on the public gateway. The operator pays.
- A bucket per client-supplied namespace hint. The hint is not a trust
  anchor.
- A dashboard on the public origin. The state of the gateway is for its
  operator.
