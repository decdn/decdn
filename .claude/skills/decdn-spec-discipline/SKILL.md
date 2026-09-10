---
name: decdn-spec-discipline
description: >-
  deCDN spec-authoring discipline. Puts the burden of proof on ADDING any mechanism to an
  ADR, protocol, contract, config knob, or on-chain surface — the opposite of the usual
  default. Use this whenever writing or editing an ADR, proposing or reviewing a protocol,
  contract, slashing/reputation/penalty/enforcement rule, a config knob, an on-chain record,
  or any "should we add X to protect clients/nodes/the network" question — even when the user
  never says "over-engineering" or "simplify". Runs every proposed mechanism through the
  trustless axiom (nodes run open-source code they can patch, so protection must be
  self-enforced by the counterparty or bound to a signed benefit-taking action) and eight
  disproof gates before it earns a place in a spec. Also guards the reverse: "unnecessary at
  small scale but earns its keep at scale" is never a reason to cut. Reach for this BEFORE
  drafting protocol / tokenomics / contract / economic design, not after it is implemented.
---

# deCDN Spec Discipline

## Why this exists

deCDN spent 50–100 PRs and tens of thousands of deleted lines removing protection
mechanisms, on-chain records, punishment tiers, and config knobs that never should have
been written. The cost was not the deletion — it was a month of building, testing, and
reasoning about machinery that a single question would have killed at spec time.

This skill front-loads that question. It is a filter you run at **authoring time**, so the
over-engineering never lands in a spec, an ADR, a contract, or the wire in the first place.

The failure mode it fights is subtle: each mechanism looked *responsible* when proposed.
"Punish bad nodes." "Cap the rate." "Track client reputation." "Insure against bad slashes."
"Let governance pick the token." Every one felt like diligence. Every one was cut. The tell
is that they were all **additions justified by a story about a threat**, and nobody made the
addition carry its own weight.

## The prime directive: burden of proof is on addition

**The default is no mechanism.** Adding one is the thing that must be justified, in detail,
against the tests below. Deletion carries no burden — you do not steelman a cut, you do not
owe a mechanism its survival. Spend the argument budget on *whether to add*, never on
*whether to keep*.

When you draft or review a spec and reach for a new mechanism, stop and make it prove itself.
If it cannot pass the trustless axiom and the eight gates, it does not go in the document —
not as "nice to have", not as "we can gate it off", not as "future-proofing". Leave it out.

**The one-sentence test (apply this before the gates — it is the fastest kill).** You must be
able to explain, in a single concrete sentence, *the specific failure this mechanism prevents
and what breaks without it*. If the best you can do is a generic virtue — "robustness",
"safety", "defense in depth", "resilience", "future-proofing", "just in case" — that is a
**failed** justification: it names no failure, so it defends against nothing in particular,
so it is dead weight. Every line of code and every clause of a spec is a standing cost on
performance and maintainability; deletion is the default win, and the *addition* must beat it
by pointing at a real thing that goes wrong in its absence. "I can't explain exactly why this
is here" is not a reason to keep it — it is the reason to cut it.

There is exactly one carve-out on the deletion side, at the very end of this file. It is
narrow. It does not soften the prime directive.

## The trustless axiom (apply this first, it kills the most)

**A deCDN node runs open-source code its operator can modify.** So can a client. So any
protection whose enforcement lives *inside the honest execution of the party it constrains*
is voidable by exactly that party. They patch it out and keep getting paid.

Ask of every mechanism: **can the party it targets delete this code and still get paid /
still participate?** If yes, it protects no one — and it is usually *worse* than nothing,
because it **asymmetrically penalizes the honest**. The compliant node self-limits; the
patched node does not; so the mechanism hands share to the cheater and calls it safety.

Durable protection has only two shapes. If a proposed mechanism is neither, it is theater:

1. **The counterparty protects itself.** Client-side BLAKE3 verification of received bytes.
   No-pay-on-failed-delivery. Per-channel exposure bounds the client sets. These hold no
   matter what binary the other side runs, because the protected party enforces them
   locally. Trustless. This is the *preferred* shape — push protection to the side that
   benefits from it.
2. **A penalty bound to a signed, benefit-taking action the attacker must emit to get paid.**
   Slashing works only when every signature in the evidence sits on an action the attacker
   *cannot avoid* producing to take the benefit. If the incriminating half is a signature the
   attacker can simply not send (a `StreamResponse{ok:false}`, a refusal, silence), the
   offense binds nobody — a hostile fork just drops it, and the only operators it ever slashes
   are the honest ones running the reference binary.

Worked consequences from the actual codebase:

- **Phantom-announcement slashing was deleted** — "advertised a blob then refused to serve"
  needed a signed refusal the attacker never sends. It could only slash honest nodes.
  Availability failures live in *local* reputation, not on-chain punishment.
- **Self-reported `total_bytes` cannot gate a pull** — a node lying about its capacity/load
  only hurts honest nodes that respect the field. The field can inform *the reporter's own*
  choices; it cannot be a trust anchor for anyone else.
- **Client-reputation ledger was deleted** — reputation keyed to a wallet address is
  unenforceable: wallets are free, a client at its reject threshold opens a fresh channel
  from a new key and resets. The real bound (per-channel credit window) already holds on the
  first byte, trustlessly.

The one-line version: **"clients protect themselves; it's trustless" is the load-bearing
sentence.** Design toward it.

## The eight disproof gates

A proposed mechanism must survive **all** of these. Read each as a question you ask the
mechanism, with the reasoning for why a "no" is fatal.

1. **Enforcement.** Does it appear in a reject / revert / refuse path? If it is advisory —
   a self-clamp, a limit nothing checks, a field no code reads to deny anything — it enforces
   nothing. *(The advisory `deliveryCeiling` asked a seller to clamp its own quote downward;
   it lived in no `require`. Cut. The `maxVoucherIntervalMb` on-chain knob had no Rust reader.
   Cut.)*

2. **Attacker binding (the trustless axiom, restated as a gate).** Can the target evade it for
   free — rotate a wallet or NodeId, drop the incriminating signature, serve out-of-band,
   patch the binary? If yes, it only taxes the honest.

3. **Live consumer.** Is there a production caller *today* that reads or constructs this, or is
   it built ahead of a consumer that may never arrive? Code built ahead of its consumer is dead
   code wearing a plan. *(The min-reputation floor's only caller passed `0.0`; every real path
   was reached only by unit tests. The node-side ERC-1271 check was never built. Both cut.)*

4. **Right home / right altitude.** Push every choice to the lowest tier that works:
   **constant over negotiation**, **off-chain over on-chain**, **application over protocol**,
   **config value over governance surface.** Move up only when something concrete forces it.
   *(Multi-token allowlist + price oracle → one immutable USDC address. Encrypted-publishing
   DRM with epoch keys and offline leases → one paragraph: "we deliver ciphertext like any
   other blob." Version-negotiation runtime → a reserved version sentinel, because there is no
   v2 to negotiate against.)*

5. **Redundant signal.** Does something already cover this with a *fresher* or *simpler*
   signal? A new mechanism that re-derives what a better signal already gives you is pure
   surface. *(Settlement-recency ranking → live probe RTT is strictly fresher. Aggregated
   gossip reputation → local tit-for-tat already scales, per BitTorrent. NodeAnnounce gossip →
   the on-chain registry already has it.)*

6. **Unrepresentable beats defended.** Can the bad state be made *impossible by construction*
   instead of guarded by machinery? Defensive machinery is a standing liability; an
   unrepresentable state costs nothing forever. *(Removing the ceiling collapsed
   `Arc<ArcSwap<Bounds>>` → `Arc<AtomicU64>` and made a whole clamp-assert bug class
   unrepresentable rather than defended against. One token erases every fee-on-transfer /
   rebase / reentrancy edge case a token allowlist has to handle.)*

7. **Cascade origin.** Does this complexity exist *only* to prop up an earlier elaborate
   choice? If so, the fix is upstream: remove the base and the dependent surface evaporates.
   *(The slash-appeal lien / TWAP / MAX_RESTITUTION apparatus existed only because restitution
   flowed through a USDC insurance pool. Escrow the slashed TOKEN in the bond and refund on
   successful appeal — the whole dependent surface is gone.)*

8. **Liability.** Is the mechanism's *whole purpose* a legal or security liability? Then the
   move is deletion, not refinement. *(The delegator pool delivered passive yield to lockers —
   the exact "profit from others' efforts" that makes a token a security. Deleted to break
   Howey, not tuned. Per-hash on-chain content claims pushed the chain toward a surveillance
   index of everything served — a privacy regression, cut on sight.)*

For the concrete precedent behind every example above — mechanism, what it did, the exact
reason it died, PR/ADR number — see `references/decdn-precedents.md`. Cite it when you want a
prior cut to justify keeping a new mechanism out.

## How to apply it

**When drafting an ADR or spec:** as each mechanism enters the draft, run it through the
axiom and gates *in your reasoning*, not on the page. What lands in the document is the lean
result plus, where a reader would wonder "why isn't there an X here?", a one-line note that X
was considered and why it stays out. Do not narrate the whole filter in the ADR — ADRs are
present-tense canon of what *is*, not a changelog of what you rejected.

**When brainstorming a protocol / tokenomics / economic feature:** propose the *smallest*
thing that could work, then let the user add pressure toward more — not the reverse. If you
catch yourself reaching for a punishment tier, a new participant role, a new on-chain record,
or a governance knob, name which gate it has to pass before you write it down.

**When reviewing an existing spec or PR for over-engineering:** go mechanism by mechanism.
For each, state the gate it fails and the disproof in one line. Give every flagged mechanism a
concrete replacement or a "delete outright". Mark genuinely close calls "[your call]" rather
than manufacturing certainty.

**Verdict format** (for a review, or a "should we add X" question):

```
MECHANISM: <name>
VERDICT: keep out / delete / keep (rare) / [your call]
FAILS: gate <n> — <one-line disproof>
INSTEAD: <the trustless / lower-altitude / unrepresentable alternative, or "nothing needed">
```

## The one deletion carve-out: do not cut scale infrastructure

The prime directive makes deletion cheap on purpose. There is a single guard against it
eating real infrastructure:

**"Unnecessary at tens of nodes but earns its keep at scale" is NOT a valid reason to cut.**
The network is meant to scale as well as possible; anything that earns its keep as it grows
stays. This has real precedent — mechanisms have been steelmanned for removal and kept
precisely because their case reduced to scale-deferral — but resist writing down a roster of
"kept" mechanisms: that list goes stale (things kept once get dropped later for other
reasons) and it biases the next pass against re-examining them, which is the opposite of the
burden-on-addition posture. Apply the rule; don't fossilize the verdicts.

So the two sides do not contradict:

- **Adding** a mechanism ⇒ it carries the full burden (axiom + eight gates).
- **Cutting** a mechanism ⇒ free, *except* you may not cut proven, load-bearing scale
  infrastructure purely because it is not needed *yet* at the current node count.

Distinguish three arguments that get smuggled together, and name which one you are making:

- **Scale-deferral** ("not needed yet at N nodes") — an *invalid* reason to cut. Reject it.
- **Adoption / maturity** ("smart-account clients don't exist yet") — can be valid; name it.
- **Governance rollout** ("no regional bodies registered at launch") — can be valid; name it.

Within a *single* review pass, it's fine to note "considered and kept, because —" so you don't
thrash over the same call twice in one sitting. But a keep is never permanent: it holds only
until the next real change touches that mechanism, at which point it faces the gates again from
scratch. Don't carry a standing "protected" list forward between passes.
