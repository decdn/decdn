# Appendix: Public HTTP Gateway

> **This is an appendix, not a core protocol ADR.** The public gateway is a
> paying client that other clients reach over HTTP. Nodes, pools and
> contracts see ordinary paid pulls and nothing else. An operator can run a
> gateway with a different policy, or none.

**Date:** 2026-10-24 — **Status:** Accepted (`decdn gateway --public-url`
ships). **Touches:** `decdn` CLI (`gateway`, `publish point`), `decdn-client`
(byte windows).

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
with the signer each recovered to and the highest `seq` stored from each
signer. The gateway reads them at start and fails to start on a damaged
file. A restart changes no pointer.

## `HF_ENDPOINT`

A namespace host is an endpoint for `huggingface_hub`. With
`HF_ENDPOINT=https://<ns>.<host>` the client asks for
`/<repo>/resolve/<revision>/<file>`, where `<repo>` is `org/name`, with
`datasets/` or `spaces/` before it for those. A single-segment id
such as `gpt2` is not a resolve path. `resolve` is looked for after the repo
id, so a repo named `resolve` is a repo. On a namespace host that path is
the entry `<repo>/<file>` of the bundle the pointer names. The revision is
`main` or the bundle's commit: the first forty hex characters of its
manifest hash. Another revision is 404.

`HEAD` and `GET` carry the headers the client reads: `X-Repo-Commit` (the
commit), `ETag` and `X-Linked-Etag` (the entry hash, quoted), `X-Linked-Size`
(the entry's size, the whole of it on a partial answer) and
`Accept-Ranges: bytes`. The body is the entry byte for byte, whatever its
type: a page is not rewritten here, since the client stores the body under
the entry hash. A 404 carries `X-Error-Code`: `RevisionNotFound`, or
`EntryNotFound` with the commit. A `HEAD` under `main` answers 302 to the
same path under the commit, and the client downloads from there: a pointer
that moves between the two requests makes the download 404 instead of
caching another bundle's bytes under the first bundle's commit. `GET` under
`main` serves directly. Nothing redirects off the host. A resume with a
`Range` is a range as in [Ranges](#ranges).

Any `/<repo>/resolve/<rev>/<file>` path on a namespace host is this
shim's. An entry at such a literal path is not reachable on the namespace
host; it is on the bundle's own site. A later `/api/` or `/v2/` surface
shadows literal paths the same way. The `/api/` surface the client's
`snapshot_download` needs is not served.

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

## Metrics

`--metrics-listen <loopback address>` serves Prometheus text at
`GET /metrics`. The series follow
[Observability and Metrics](appendix-observability.md#appendix-observability-and-metrics):
one unlabeled sibling counter per outcome, every series exported from the
start.

| Series | Type | Counts |
|--------|------|--------|
| `decdn_gateway_bytes_served_total` | counter | Content bytes handed to responses. |
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

## Non-goals

- A name on chain. A namespace pointer is a gateway-local record above the
  protocol ([ADR 002](002-content-addressing.md#adr-002-content-addressing)).
  Two gateways may hold different pointers for one namespace; the publisher
  points each.
- A token or capability on the public gateway. The operator pays.
- A bucket per client-supplied namespace hint. The hint is not a trust
  anchor.
