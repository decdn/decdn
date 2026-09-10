# deCDN over-engineering precedents

Concrete mechanisms that were added, then cut, and the exact reason each died. Use these to
justify keeping a *new* mechanism out of a spec: "this is the same shape as X, cut in #NNNN."
Every reason here traces to a real PR/issue/ADR. Quotes are from the commit/PR/ADR bodies.

## Table of contents

- [Enforcement theater (gate 1)](#enforcement-theater-gate-1)
- [No attacker binding / trustless-axiom failures (gate 2)](#no-attacker-binding-gate-2)
- [Built ahead of its consumer (gate 3)](#built-ahead-of-its-consumer-gate-3)
- [Wrong altitude — on-chain / protocol / negotiation when lower works (gates 4, 6)](#wrong-altitude-gates-4-6)
- [Redundant signal (gate 5)](#redundant-signal-gate-5)
- [Self-inflicted cascade (gate 7)](#self-inflicted-cascade-gate-7)
- [Liability by purpose (gate 8)](#liability-by-purpose-gate-8)
- [Speculative work with no live waiter](#speculative-work-with-no-live-waiter)
- [Incentive-free-P2P tricks the payment layer obviates](#incentive-free-p2p-tricks)
- [On not keeping a "kept" list](#on-not-keeping-a-kept-list)

---

## Enforcement theater (gate 1)

- **Advisory `deliveryCeiling` rate bound** (#1441 / #1472). "The ceiling enforced nothing — it
  appeared in no require/revert on the settlement path — and asked a seller to self-clamp its own
  advertised rate downward, which buys no on-chain safety." Removing it also made a bug class
  unrepresentable (see gate 6).
- **On-chain `maxVoucherIntervalMb`** (#1435). "advisory, unenforceable, untested, no Rust reader."
  The load-bearing off-chain wire field stayed.
- **Residual authorized-origin content-authorization gate** (#1525). Never authorized per-content:
  "it admitted a serve for any hash in any namespace with any registered origin." A husk left after
  the per-hash claim layer was removed.
- **Min-reputation floor** (#1438 / #1456). See gate 3 — also enforced nothing in production.

## No attacker binding (gate 2)

- **Phantom-announcement slashing** (#1512). "its second half was a signed `StreamResponse{ok:false}`,
  coerced by nothing — a malicious fork just drops the stream — so the offense only ever slashed
  honest operators running the reference binary." Availability → local reputation (ADR 008).
- **Unenforceable client-reputation ledger** (#1552 / #1606). "Reputation keyed to `channel.client`
  accumulates against a wallet address; wallets are free, so a client at its reject threshold opens a
  fresh channel from a new key and resets." The per-channel credit window already bounds the threat on
  the first byte. 1,603 lines, no consumer.
- **Per-signer abandonment leaky-bucket** (#1957). A node-local refilling throttle keyed to the
  capability signer — durable `PoolFloorLossStore` (a whole redb `floor-loss.redb` database), two config
  knobs, debit-on-drop, forget-tombstones, a metric + alert, an e2e test — to rate-limit "connect, take
  one free floor, abandon, repeat." Same shape as the client-reputation ledger above: a signer is a
  free-to-mint keypair, so the attacker who cares rotates and the tally resets to empty; it only ever bit
  the honest reused-key client the design was tuned to spare. Concurrent fan-out was already bounded by
  the pool ceiling (`remaining − M`) and the per-signer live cap; the sequential trickle it nominally
  addressed is real egress the attacker pays for byte-by-byte — a residual ADR 003 already accepts that
  "grows with neither the pool nor the network," so it is not scale infra either. Net −3,855 lines. Kept:
  the in-memory pool ceiling + per-signer live cap.
- **Client NodeId rotation** (#1437, demoted). "the serving node already learns your on-chain address,
  so rotation does nothing against the two adversaries who matter." Its own Limitation paragraph gutted
  the claim.
- **Trusted-IP rate-limit exemption** (#1440 / #1456). "weakest exactly where it was most likely to be
  used" — moved a ceiling from 50/s to 100/s, "a number the operator could have set directly," while
  leaving a standing allow-list to go stale.
- **Self-reported `total_bytes` as a pull gate.** A node lying only hurts honest nodes that respect the
  field; it centralizes toward the liar. The field informs the reporter's own choices, never anyone
  else's trust.

## Built ahead of its consumer (gate 3)

- **Min-reputation floor** (#1438). "The sole production call site passed `DEFAULT_MIN_REPUTATION = 0.0`;
  every `_with_floor` entry point was reached only from unit tests."
- **Off-chain ERC-1271 + Safe-as-wallet** (#1431). The node-side ERC-1271 check "was never built"; a
  1-of-1 Safe is "the same trust posture as today's eth_keystore — zero security gain, added setup burden."
- **Dead `StreamResponse.redirect` field** (#1838 / #1842). "no node ever populates it, no client ever
  follows it, yet it is carried on the wire and folded into the EIP-712 signed set." Must go pre-launch:
  "post-launch it becomes a frozen signed field."
- **Mechanism-inventory sweep** (#1850). Net −3,900 lines of code "with no production caller, along with
  the tests that existed only to exercise it": client rtt_map, fused progressive pull, `Percent` type,
  `discovery::rank`, `CapacityBond.getBondMultiple`/`getNode`, and more. Notably it also lists what was
  KEPT to prevent over-deletion.

## Wrong altitude (gates 4, 6)

- **Multi-token / multi-stablecoin allowlist + price oracle** (ADR 010, dropped). Replaced by "USDC, with
  its address fixed at contract deployment (immutable constructor argument)." No `addToken`/`removeToken`,
  no per-token rate bounds, no oracle. One token erases the whole fee-on-transfer / rebase / reentrancy
  edge-case surface (gate 6). Other assets swap to USDC off-protocol.
- **Encrypted-content publishing `cdn/keys/v1`** (retired appendix, #1430). A full DRM system — epoch keys,
  envelope encryption, device-bound offline leases, a companion app server. "an application, not a
  protocol… Nothing in the CDN protocol depends on any of it. The appendix says so itself, twice." →
  one paragraph in ADR 002.
- **Protocol version-negotiation runtime** (#1433). Multi-ALPN negotiation and dual-version handlers "with
  no v2 to run against and no code behind it." Kept a reserved version sentinel.
- **On-chain event-log replay** (#1504). The slash watcher scanned ~10.4M blocks (~1,037 `eth_getLogs`)
  every boot; once contract-side enumeration views existed, replay was dead. → enumerate-at-head /
  follow-the-tail / re-read-periodically.
- **On-chain Genesis Bond Credit** (#799). → off-chain TGE unlocks, no contract surface. `CapacityBond`
  runtime size dropped ~3KB, easing the EIP-170 ceiling.
- **Node-side tokenomics/governance metrics** (#1439). "Node re-exporting authoritative on-chain state for
  off-node dashboards is a layering violation; the chain is the source of truth."
- **Deliverable of `Arc<ArcSwap<Bounds>>` → `Arc<AtomicU64>`** (#1472, gate 6). The pair-consistency
  machinery existed only to stop a reader seeing a new floor beside an old ceiling and tripping a clamp
  assert. Remove the ceiling → bug class unrepresentable.

## Redundant signal (gate 5)

- **Settlement-weighted bootstrap ranking** (#1434). `FeeRouter.recordSettlement` on every settlement so
  clients could rank by recency — "Nothing ever indexed it: the real bootstrap ranker reads
  `getActiveNodes`, orders region-first and ranks by probe result, which is a strictly fresher signal."
- **Aggregated / gossip reputation tier** (#1419 / #1418). A shared global score "fights load balancing…
  amplifies incumbency… adds a permanent gossip attack surface… to buy 30% weight on a signal the ADR
  itself calls subjective and non-converging. BitTorrent shows a P2P content system needs none of this:
  local tit-for-tat scales fine." Net +327 / −6,316.
- **NodeAnnounce gossip** (#1712). Node region now sourced from the on-chain registry; the DHT covers
  discovery in O(C). Gossip crate and wire deleted.
- **DECDNMAN file-manifest format** (#1505 / #1507). "obao4 is a strictly finer, single-hash superset.
  Ranged access, resume, bounded origin egress, verifiable file structure — obao4 does all of it against
  one root hash." The producer was never built. −2,092 lines.

## Self-inflicted cascade (gate 7)

- **SafetyReserve appeal surface** (ADR 032, retired). Per-appeal lien accounting, `MAX_APPEAL_RESTITUTION`
  caps, TWAP machinery, six pinned events — all existed only because restitution flowed through a USDC
  reserve pool. Escrow slashed TOKEN in `CapacityBond`, refund on successful appeal → the entire dependent
  surface is gone. Re-homed to a lean `SlashAppeal`: "open/fastTrack/reject/grant/uphold/cleanup; TOKEN
  appeal bonds only, no USDC/pool/swap/queue."
- **Safety / insurance reserve** (ADR 033, retired). A 5% FeeRouter bucket, a standing USDC insurance pool
  with a payout queue and a TOKEN→USDC keeper swap, for incorrect-slash / downtime / "bad-data" incidents.
  → escrow-on-slash refund; "no pool needed." Recourse for undemonstrated harm belongs in DAO governance,
  not a standing contract. Freed 5% folded into buyback-burn.
- **Blacklist-entry appeals / fast-track** (#1432). A second appeal state machine on top of blacklisting —
  six entry points, `StandingPath`, Sybil-porous per-filer knobs, `entry.suspended` end to end. Deleted;
  `ContentBlacklist` bytecode 24,576 → 11,193 bytes. Core enforcement untouched.

## Liability by purpose (gate 8)

- **Delegator pool / `DelegatorBuyer`** (ADR 035, deleted entirely). A 7% bucket routed through a
  MEV-protected USDC→TOKEN buy-and-distribute pipeline paying passive yield to ve-lockers. "passive yield
  to ve-lockers is eliminated to break Howey prong 4." Not refined — deleted.
- **Gauge-boost + voting escrow / veTOKEN** (ADR 034, retired). A Curve veCRV-style lock/checkpoint/
  delegation contract plus a boost formula with divide-by-zero fallbacks and a wash-trading share cap. →
  the closed-form `bond = k × Mbps^α` capacity curve. Yield differentiation flows from capital cost, not a
  yield haircut.
- **Per-hash on-chain content claims** (#1394). "per-hash claims push the chain toward an on-chain index of
  all served content" — a content-surveillance ledger the design explicitly rejects. Namespace is "a
  routing hint, not a trust anchor" since BLAKE3 verification is independent.
- **Capacity-shortfall slashing** (#682, retired). Probe-and-slash declared-vs-actual capacity, to stop
  wash-trading for governance weight. → ADR 036 sources vote weight from `FeeRouter.bytesInWindow` — proven
  delivered bytes — making the attack moot and deleting the whole enforcement path.

## Speculative work with no live waiter

- **Speculative prefetch** (#1396 / #1399). "a second, parallel acquisition path whose only job is to guess
  at demand the paid path handles on its own"; "DHT prefetch does not improve locality." ~2,900 lines.
- **Client-less background cache-warm** (#1610 / #1611). "unpaid egress forces paid ingress… leaves the node
  paying an upstream from zero for a blob nobody wants; repeated, it drains payment-channel deposits."
  Invariant: "A node ingests only while a live client is waiting on the bytes. It never initiates a from-zero
  pull speculatively." Explicitly "not a scale-deferral cut — the economics are wrong at any node count."
- **Opt-in remote-origin prewarm** (#1522, reverted). Same speculative-warm family; never shipped tagged.
- **QUIC 0-RTT probe establishment** (ADR 015, #1465). "one round trip that never compounds and only lands
  on a warm reconnect; the cost is a permanent footgun on the probe path" — a replay surface guarded only by
  a client-side convention no server could enforce (iroh sets `max_early_data_size = u32::MAX`).

## Incentive-free-P2P tricks

- **Endgame hedging in multi-source fetch** (#1442). "BitTorrent needs endgame hedging because it has no
  incentive layer; we do. Paying for speculative duplicate units to shave tail latency is exactly the kind of
  thing our payment/scheduling layer should make unnecessary."
- **Aggregated reputation** (#1419, above) — the same "borrowed from incentive-free P2P" argument.

## On not keeping a "kept" list

There is deliberately no roster of "mechanisms we reviewed and decided to keep" here. Such a list is a trap:
it goes stale (the Kademlia DHT's *scale case* held, but "regional gossip topics" and `gossip.max_peer_entries`
were later dropped with the NodeAnnounce gossip removal in #1712 — a list that named them as permanently "kept"
would now be wrong), and a standing "protected" list biases the next reviewer against re-examining those
mechanisms, which is the exact opposite of putting the burden on every addition.

Keep only the **rule**: do not cut proven, load-bearing scale infrastructure purely because it is not needed
*yet* at the current node count. Everything else re-faces the gates whenever a real change touches it. A past
keep is a snapshot, not a shield.

If you need to know whether a specific mechanism is currently live, read the ADRs and the code — not a cached
verdict. The source of truth is the current tree, never a frozen list of past decisions.
