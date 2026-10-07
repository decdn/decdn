# Security model

Report vulnerabilities as described in [SECURITY.md](../SECURITY.md).

## What holds what

| Secret | Where | Can do |
|---|---|---|
| Treasury key (keystore + password) | `sponsord` host | Sign capabilities against your pool; move the wallet's funds |
| `SPONSORD_API_TOKEN` | `sponsord`, the onramp, your gate | Get a capability for any key, up to the daemon's maximum terms |
| Turnstile secret | the onramp | Verify captcha tokens |
| A download key | the user's machine, one per download | Spend up to its capability's cap, until it expires |

The treasury key both owns the pool and pays for top-ups
(`PaymentPool.topUp` is owner-only), so a compromised daemon host loses the
wallet's funds as well as the pool. Keep only what the next few top-ups need
in that wallet. Splitting the two is tracked in
[#25](https://github.com/decdn/sponsord/issues/25).

## What bounds a loss

Anyone who gets capabilities can spend, at most:

- **per capability:** its cap (`SPONSORD_MAX_SPENDING_CAP_MICRO_USDC`, or
  the onramp's lower `ONRAMP_SPENDING_CAP_MICRO_USDC`), until its expiry;
- **in total:** the pool's remaining balance **plus** the treasury wallet's
  USDC. The keeper tops the pool up by `SPONSORD_POOL_REFILL_MICRO_USDC`
  every time it falls below the low-water mark, so a drained pool is
  refilled from the wallet until the wallet runs out (or its gas does). Fund
  the wallet with what you are willing to lose, and watch
  `sponsord_pool_topups_total`. While the daemon runs, a top-up whose
  receipt could not be read holds the next one until it mines or can no
  longer mine, so one refill is not sent twice. The hold is kept in memory:
  a restart while that transaction may still mine allows a second refill.
  A top-up whose submission was accepted but whose response was lost is not
  held at all (see [the operator guide, §5](operator.md#5-monitor)).

How many capabilities they can get is bounded by the gate, and on the onramp
by the per-address rate limits (`ONRAMP_FUND_RATE_PER_MIN`).

## Hardening checklist

- Keep `sponsord` unreachable from the internet; only the onramp (or your
  gate) talks to it. It binds to localhost by default.
- Give secrets as files (`*_FILE`, systemd credentials, Docker secrets), not
  environment variables. `Debug` output never prints them.
- Behind a reverse proxy, set `ONRAMP_CLIENT_IP_HEADER` only if the proxy is
  the only way to reach the onramp; otherwise clients choose their own
  address. With `X-Forwarded-For` the right-most address counts.
- Watch `sponsord_pool_topups_total` and the treasury balance: a spike in
  top-ups means capabilities are being spent faster than expected.
- Pin releases: the installers check each release's `SHA256SUMS` against a
  digest you set (`ONRAMP_*_SUMS_SHA256`), so a release asset replaced after
  you pinned it is refused.
- Every value baked into the installers is validated at startup to hold no
  character a quoted shell or PowerShell string could misread.

## The CLI

`decdn-sponsored` keeps each download's key, password, capability and
profile under `~/.decdn/sponsored/downloads/<hash>/`, created `0700` with
files `0600` on Unix. The key holds no funds and is deleted once the
download succeeds. Every download signs with a fresh key, and `decdn`
generates a fresh iroh node key per run, so nodes can't link one download to
another by identity. `decdn`'s data dir, `~/.decdn/sponsored/decdn/`, is
shared across downloads and holds only public facts about nodes (addresses,
latency, recent failures); sponsored pulls write no buyer-channel rows there.
`decdn` verifies every byte against the content hash, so
a malicious node or onramp can't change what is downloaded; a malicious
onramp could only point the CLI at a different pool or chain.
