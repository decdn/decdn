# issue #1252 runtime micro-deduplications design

## Goal

Remove three mechanical duplication clusters from the node runtime without
changing pruning, EIP-712 signing, provider construction, polling, or startup
failure behavior.

## Scope

The work has three independent parts: a shared node-internal `PruneGuard`,
single construction of the runtime's signing domains, and a private HTTP
provider factory for `run()`. Contract address parsing remains on the existing
`parse_nonzero_address` path; unrelated address or watcher refactors are out of
scope.

## Shared `PruneGuard`

Create `crates/node/src/prune_guard.rs` and expose it only within the node
crate. The guard borrows an `AtomicBool` and resets it to `false` with
`Ordering::Release` on drop, including panic unwind. Both
`ConnectionLimiter` and `ThreeLayerRateLimiter` import this type and retain
their existing compare-exchange and sweep logic. No generalized limiter or
shared sweep abstraction is introduced.

## EIP-712 domains

After the relevant non-zero contract addresses have been parsed, `run()`
constructs exactly one `slash_judge_domain`, one `voucher_domain`, and one
`bind_node_id_domain`. Later consumers receive clones of those immutable domain
values where ownership requires it. The probe handler, client handler, buyer
bootstrap, and node-origin configuration therefore use byte-identical domains
derived from the same chain ID and verifying addresses.

## Provider factory

A private runtime provider factory single-sources Alloy HTTP provider creation.
It owns the configured pending-transaction poll interval and supplies four
semantic constructors used by `run()`:

- read-only providers with the configured poll interval;
- the seller wallet provider with simple nonce management and the poll interval;
- the buyer wallet provider with the same nonce and polling policy but a
  separately constructed provider instance;
- the plain shared-head provider without `with_poll_interval`.

The factory accepts an explicit URL for optional watcher/indexer providers, so
the existing cloned blacklist and reputation URLs keep their ownership and
redaction behavior. Seller and buyer providers remain separate instances even
though their builder policy is identical. No provider is cached or shared
across consumers that currently own separate clients.

## Failure behavior

URL and address parsing errors remain fatal at the same startup points and keep
their current context strings. The factory performs no I/O and introduces no
new fallback. Wallet providers retain simple nonce management so failed sends
cannot leave a cached nonce gap. Read-only and head providers remain wallet-free.

## Verification

Existing prune-unwind tests must continue proving that both single-flight flags
are released after a panic. Existing runtime poll-interval tests must exercise
the factory-backed read-only provider, and focused node tests must compile every
provider consumer. Formatting and Clippy must pass after rebasing onto current
`main`. A source audit must show one production construction of each signing
domain and no direct production `ProviderBuilder::new().connect_http(...)`
sites in `run()` outside the factory.

## Acceptance criteria

- Both keyed limiters use one crate-private `PruneGuard` implementation.
- Each of the three production EIP-712 domains is constructed once in `run()`.
- Every production HTTP provider in `run()` comes from the private factory.
- Poll intervals, simple nonce management, provider separation, error context,
  and watcher startup behavior are unchanged.
- Focused tests, formatting, and Clippy pass on current `main`.
