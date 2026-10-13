# Architecture

## Components

| Component | Runs | Holds | Talks to |
|---|---|---|---|
| `sponsord` | your server, private | the treasury key (owns the pool), the API token | the chain; callers with the token |
| `sponsord-onramp` | your server, public | the daemon token, the gate's secret | `sponsord`; browsers; the installers |
| `decdn.sh` / `decdn.ps1` | the user's machine | one throwaway key per download | the onramp; runs `decdn` |
| `PaymentPool` | the chain | your pool's deposit | nodes redeem vouchers against it |

`sponsord-core` is `sponsord` as a library, and `sponsord-api` holds the wire
types both servers share.

## Capabilities

A *capability* is an EIP-712 grant signed by the pool owner: "key `S` may
spend up to `cap` from pool `P` until `expiry`". It is serialized as a
`dcap1:` token. A node accepts vouchers from `S` against `P` once it holds
the capability; the first time one is redeemed, the contract records `S`'s
cap and expiry (`PaymentPool._registerCapability`), and those terms are
fixed from then on.

That is why:

- issuing a capability costs nothing on-chain and locks no per-user deposit;
- the installer uses a fresh key per download: a key's terms can't be
  extended;
- for a key already registered on-chain, `sponsord` re-signs the registered
  terms (and, with a local key, returns the very same token), and refuses
  once that registration has expired (`409 signer_expired`).

## The download flow

1. **Install.** `curl …/decdn.sh | sh -s -- pull b3:<hash>` (or `decdn.ps1`)
   runs the script the onramp serves, with the onramp URL, the pinned
   `decdn` release and the download profile (chain id, RPC URL,
   `PaymentPool`, `CapacityBond`) baked in. The onramp takes the chain id and
   pool from the daemon's `GET /v1/info` at startup. The script downloads
   `decdn` from its pinned GitHub Release, checking the release's
   `SHA256SUMS` against a pinned digest and the archive against `SHA256SUMS`.
2. **Key.** It makes a throwaway key with `decdn key-gen` under
   `~/.decdn/sponsored/downloads/<hash>/`, with a random password beside it.
3. **Gate.** It polls `GET /v1/capability?client=<key>`. While that answers
   `204`, it prints and opens `GET /fund?client=<key>`, the gate page. Once
   the person passes, the page posts `POST /v1/fund`; the onramp checks the
   proof with the gate, then calls the daemon's `POST /v1/capabilities` and
   holds the token and its expiry for the script, which saves both beside
   the key.
4. **Pull.** The script runs `decdn bundle pull --capability <token>` with
   the key. `decdn` finds nodes, pays them with vouchers signed by the
   throwaway key, and verifies every byte against the hash. Its `--data-dir`
   is `~/.decdn/sponsored/decdn/`, shared across downloads, so peers cached
   by one download spare the next its registry read. A download started
   while another holds that dir runs in its own directory instead.
5. **Done.** On success the download's state is deleted. On failure it is
   kept, so re-running the same command resumes with the same key and
   capability, without the gate.

## Contracts with deployed software

Published commands and downloads in progress read these, so a change to any
of them is called out as contract-breaking in the changelog:

- the installers' arguments (`pull <hash> [-o <dir>] [--namespace <id>]
  [<bundle pull flags>]`);
- the layout under `~/.decdn/sponsored/` (`downloads/<hash>/` with
  `keystore.json`, `password`, `address`, `capability` and `expiry`), which
  an interrupted download resumes from;
- both HTTP APIs (`sponsord-api`).

## Trust boundaries

- The **daemon** trusts every caller holding its token. It decides nothing
  about who should get a capability, only enforces its maximum cap and TTL.
- The **onramp** is the only thing exposed to the internet. Its gate decides
  who gets a capability; its rate limits bound how fast.
- The **installer** trusts the onramp that serves it for the chain and
  contract addresses and for the pinned `decdn` release digest. It trusts no
  node: `decdn` verifies content against the hash.

See [security.md](security.md) for what each piece can lose.
