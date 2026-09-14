# ADR 040: Pluggable Cache Admission and Eviction Policies

**Status:** Accepted

## Context

A node's local blob cache decides two things: which pulled content it keeps
(admission) and which cached content it drops under size pressure (eviction).
The protocol does not mandate a specific policy. Two nodes with different
policies still interoperate: policy is a node-local implementation choice, not
a wire concern.

Admission and eviction are pluggable behind clean trait boundaries in
`crates/cache`. The `node` crate selects and wires concrete implementations;
`cache` defines no mode branching and holds no policy semantics itself. This
follows the leaf/wiring seam pattern in
[appendix-poc-production-seams.md](appendix-poc-production-seams.md#appendix-pocproduction-seam-architecture-rust-implementation).

Ingest stays bounded to paid demand: a node ingests content only behind a
live, paying client. Admission policy narrows what a node keeps after ingest;
it never widens what a node pulls.

## Decision

### Signals in, decisions at the sweep

The engine emits signals to the policy layer in real time and exposes generic
store primitives. The policies own every semantic. The engine holds no
segment meaning, reads no frequency estimate, and runs no promotion logic.

`AdmissionPolicy` acts instantly. At store time the engine asks it for a
target segment and tags the blob with `set_segment`. This is the only
real-time policy action that mutates the store.

`EvictionPolicy` buffers signals and decides once, at the sweep. Its `plan`
method returns what to evict and what to promote. The eviction driver is a
dumb executor: it applies the plan through engine primitives
(`set_segment`, `segment_of`, `segment_bytes`) and enforces nothing itself. No
segment tag moves between sweeps, other than the admission-time tag.

```rust
pub trait AdmissionPolicy: Send + Sync + std::fmt::Debug {
    fn admit(&self, ctx: &AdmissionContext) -> AdmissionDecision;
}

pub enum Segment { Probation, Main }

pub enum AdmissionDecision {
    Store { segment: Segment },
    PassThrough,
}

pub trait EvictionPolicy: Send + Sync + std::fmt::Debug {
    fn plan(&self, ctx: &EvictionContext) -> EvictionPlan;
    fn on_access(&self, _hash: Hash) {}
}

pub struct EvictionPlan {
    pub evict: Vec<Hash>,
    pub promote: Vec<(Hash, Segment)>,
}
```

`Segment` is an opaque label to the engine. It carries no meaning inside
`cache`; only the policies interpret it.

Three boundary rules keep the split clean:

- The engine owns every safety exemption: pins, probe-triggered holds, and
  the deny/takedown set. Eviction candidates arrive to the policy already
  filtered of them, and the engine refuses to act on a pinned, held, or
  denied hash regardless of what a plan says.
- The engine owns the store and reclaim path. It exposes generic segment
  primitives and assigns them no meaning of its own; it stores the label
  admission chose and moves it when a plan says to. Policies hold no store
  handles.
- The two selectors are chosen independently, but `tinylfu` admission depends
  on `tinylfu` eviction. Promotion out of probation and the probation cap both
  live in `TinyLfuEviction::plan`; `lru` eviction ignores segment membership and
  never promotes. So `tinylfu` admission paired with `lru` eviction sets
  probation labels that are never promoted or capped — the node logs a warning
  at bring-up for this inert combination. `lru` eviction with `always` admission
  is the fully independent, supported default pairing.

Reclaim is whole-blob only. `EvictionPlan.evict` is a list of hashes, not
byte ranges. See [§ Whole-blob reclaim](#whole-blob-reclaim-range-eviction-is-upstream-gated).

### The hit signal and its shared estimator

```rust
pub trait FrequencyEstimator: Send + Sync + std::fmt::Debug {
    fn observe(&self, hash: Hash);
    fn estimate(&self, hash: Hash) -> u32;
}
```

The estimator is the hit-signal sink. The engine holds an optional
`Arc<dyn FrequencyEstimator>` as an output port and exposes one entry point,
`observe_hit`, that emits a single `observe` for a hash. Every path that
serves bytes to a client calls `observe_hit` exactly once per served request:
the paid `deliver` and `serve_leg` serve chokepoints, and `get` on its own
path. A multi-range or multi-interval serve is still one sighting. The fill
paths — `populate` and `admit_bao_stream` — do not emit the signal; they only
record recency, so a miss that fills and then serves counts as one sighting,
not two. The engine never calls `estimate`; reading the estimate is a policy
act. When no estimator is configured, the engine skips the call at zero cost.

When either the admission selector or the eviction selector is `tinylfu`, the
`node` wiring layer constructs one estimator and injects the same `Arc` into
both. One sketch feeds two readers: admission's instant segment choice and
eviction's sweep-time ranking and promotion decision both read the same
buffered frequency signal.

The shipped estimator is W-TinyLFU, implemented in-tree with no external
dependency: a count-min sketch that ages by halving its counters, sized to a
fixed in-process memory budget (`sketch_bytes`). It is not durable. State is
lost on restart, matching the empty-on-boot behavior of `lru` recency
tracking.

The sketch keys on the content hash. Cache keys are already BLAKE3 hashes, so
the sketch derives its row indices from disjoint slices of the 32-byte key
and needs no separate hash functions. The estimator omits the classic
doorkeeper bloom filter: a doorkeeper exists to keep one-hit-wonders out of
the sketch, and the probationary segment (see below) already serves that
role. Frequency tracking stays whole-blob; the sketch keys on hash, not on
byte range.

The sketch is sharded. The counter array is split into independently locked
shards. A fifth disjoint slice of the key selects the shard. An increment or an
estimate locks one shard only. Two readers that touch different hashes usually
do not contend. This matters because the serve path increments the sketch once
per completed request, and the fill path reads an estimate on each admission
decision.

Aging uses one node-wide observation clock, not a clock per shard. The node
counts all observations. Each shard halves its own counters when it is next
touched, by the number of windows that passed since it last aged. The work stays
per shard. The cadence stays global. This is necessary because the policies
compare estimates on one scale: admission tests an estimate against a node-wide
threshold, and eviction ranks candidates from different shards against each
other. A shard clocked on its own traffic would age at a rate set by key skew,
and two blobs of equal true frequency would then get different estimates.

The node catches a counter up to the clock when it reads the counter. It does
not age counters continuously. A ranking pass reads each candidate separately.
The pass carries one halving of skew for each window boundary it crosses.

A halving that applies evenly across a pass cannot invert the ranking. It only
creates ties. Two candidates read on opposite sides of a boundary can misorder.
Estimates within a factor of two can swap. The window is `cols * 10`
observations, so a sweep crosses a boundary rarely. The next sweep reads both
candidates on one side of the boundary and corrects the order.

### Probationary admission (mechanism C)

First sighting of a cache miss admits to the probationary segment. The engine
calls `set_segment(hash, Probation)`. Stream-while-store is unchanged: the
same pull that fills a waiting client also fills the cache. The default
`AlwaysAdmit` policy always returns `Main`; probation stays inert unless the
`tinylfu` admission selector is active.

The miss is the admission trigger. There is no separate on-miss signal; the
engine calls `admit` only on a miss-fill, so the call itself is the miss
handler. A missed blob still accumulates frequency, because every miss is
paired with the serve that fills it, and that serve emits the one sighting; so
a repeatedly-missed-and-served blob eventually admits straight to `Main`.

**Ordering invariant.** On a miss-fill, the engine reads the frequency
estimate for the admission decision before the paired serve emits that
request's own `observe`. The fill path never emits the signal itself, and the
serve chokepoint emits it only after the fill completes, so the admission read
always precedes the request's own sighting. A first-ever request therefore
sees an estimate of zero and admits to `Probation`; a request is never
evidence for its own promotion. `promotion_threshold = N` means "admit to
`Main` after N prior sightings," not "after N total sightings including this
one." Emitting the sighting from the fill path, ahead of the admission read,
would send every one-hit-wonder straight to `Main` at `threshold = 1`,
defeating admission.

Promotion happens at the sweep, decided by the policy. The engine does not
promote and does not read the estimator directly. On each sweep,
`EvictionPolicy::plan` returns `promote: Vec<(Hash, Segment)>`: the
probationary members whose buffered frequency has reached
`promotion_threshold`. The driver applies each promotion through
`move_segment`. Promotion matters only under cap pressure — a hot
probationary blob otherwise ranks high and is never evicted — so deciding it
at sweep time, rather than per-serve, is sufficient and avoids retag churn.
No tag moves between sweeps.

The probationary cap lives in the policy, not the driver. `TinyLfuEviction`
holds `probation_target_pct` and enforces it inside `plan`, using segment
membership and blob sizes from `EvictionContext`. It evicts the
least-frequent probationary members first, so a scan of cold one-hit-wonders
cannot push the hot working set out of `Main`. The driver only supplies
context and executes the plan; the engine only measures, through
`segment_bytes`.

### Whole-blob reclaim; range eviction is upstream-gated

Shipped reclaim removes whole blobs, through the existing tag-drop-then-GC
path. `iroh-blobs` exposes no range-removal or partial-truncation primitive:
it offers `import_bao` to add ranges, `export_ranges` and `observe` to read
them, and a whole-hash `delete`. Reclaim granularity is therefore the whole
hash.

Partial blobs already occupy only their present ranges on disk, at
chunk-group granularity, because the store writes at offsets and persists
only present ranges. Punching holes in a partial blob outside the store's
own bookkeeping would desynchronize its bitfield and break verification, so
a node cannot reclaim ranges behind the store's back.

Range-carrying eviction stays in the `EvictionPolicy` trait shape as
forward-compatible surface, but no shipped policy emits it. Range reclaim is
blocked on an upstream `iroh-blobs` primitive that atomically forgets a byte
range across the bitfield, outboard, and data file. Until that primitive
exists, range reclaim is future work, external to this ADR.

### Configuration surface

```toml
[cache]
cache_size_mb    = 102400       # UPPER bound on the ceiling; free disk clamps it down (see § Free-disk-aware ceiling)
disk_headroom_mb = 8192         # free disk kept on the cache_dir volume (default 8 GiB); 0 opts out
admission_policy = "always"     # "always" | "tinylfu"   (default "always")
eviction_policy  = "lru"        # "lru"    | "tinylfu"   (default "lru")

[cache.tinylfu]                 # sketch_bytes is live on a default node; see below
sketch_bytes         = 262144   # minimum 16384
promotion_threshold  = 2
probation_target_pct = 10
aging_halflife_sec   = 600
```

`node` owns the name-to-implementation mapping and validates the selectors.
`cache` exports the traits and implementations and makes no selection
itself. An unknown selector name is a config error at load; there is no
silent fallback. `promotion_threshold` and `probation_target_pct` resolve but
stay unused when neither selector names `tinylfu`.

`sketch_bytes` is different. The node builds the frequency estimator when a
selector names `tinylfu`, and also when `cache.serve_economics.policy` is
`margin`. That policy is the default. So `sketch_bytes` sizes a live sketch on
a node that keeps the default selectors, and
[ADR 041 § The buy ceiling](041-refuse-to-serve.md#the-buy-ceiling) reads that sketch for the heat input to its
price gate.

The node validates every one of these parameters whether or not a selector
names `tinylfu`. A parameter that is out of range stays out of range when an
operator switches the selector later. The node rejects it at load. It never
clamps it to a working value, because a clamp hides the mistake.

`sketch_bytes` carries a floor of 16384 bytes. The sketch holds one counter
per byte, over four rows, so the floor buys 4096 columns.

The floor comes from an over-report target. The sketch reports a blob hotter
than it is when every one of its four row counters also holds some other
blob's count. Different blobs can pollute different rows. One blob that
collides in all four rows is therefore sufficient but not necessary, and its
rate does not bound the error. For `N` live blobs over `cols` columns the
over-report rate is `(1 - e^(-N / cols))^4`. Sharding does not change that
rate. A shard divides the columns and the blobs in the same proportion, so
the shard count cancels out of the expression.

The rate depends only on `N / cols`. A target rate therefore fixes a blob
count that grows in proportion to the width. A sketch holds the rate below
1 percent for about `0.38 * cols` live blobs. On that basis the floor serves
about 1500 blobs, and the default of 262144 bytes serves about 25000. A node
that holds more blobs needs a proportionally wider sketch. The sketch counts
every blob it observes between halvings, not only the resident ones, so read
these counts as an upper bound.

Cache policy is node-local. A node's disk is its own resource. Policy
selection is operator configuration, not a governance or consensus
parameter. This is a deliberate exception to deCDN's default instinct of
making economic parameters governance-tunable: cache policy carries no
economic weight and has no cross-node interoperability requirement.

The defaults, `always` admission and `lru` eviction, reproduce today's
recency-only behavior exactly. Adopting `tinylfu` is an explicit operator
opt-in.

### Free-disk-aware ceiling

`cache_size_mb` is an upper bound, not the ceiling. The eviction driver clamps
it down each tick to keep `disk_headroom_mb` of the `cache_dir` volume free. It
defends that margin against every process on the volume, not only this cache.

The driver probes free disk once per tick with one `statvfs` call, beside the
store walk it already runs. It then recomputes the effective ceiling:

```
effective_cap = min(cache_size_mb, footprint + max(0, free_disk - disk_headroom_mb))
```

The cache may grow into whatever is free beyond the headroom. Below the
headroom the ceiling collapses to the current footprint, so the driver evicts to
claw disk back. The high-water and target percentages apply to this dynamic
ceiling, so the sweep tracks real free space rather than a static number.

The clamp is continuous, so a node with a large `cache_size_mb` sizes itself to
its volume. An operator drops the same config onto any machine and the cache
fills the disk down to the headroom, whatever the disk holds.

A `statvfs` failure disables the clamp for that tick. The configured
`cache_size_mb` binds, and the driver never evicts blind on a probe error. The
driver logs the clamp transition and the probe-failure transition once each, not
every tick.

`disk_headroom_mb` defaults to 8192 (8 GiB). `0` opts out, leaving only
`cache_size_mb`. Because reclaim is soft (tag-drop then GC), the driver defends
the margin reactively, so a headroom smaller than one GC interval's writes can
be crossed briefly under a heavy write burst. `decdn node doctor` reads the same
free-disk helper to flag a budget or headroom that does not fit the volume
before boot.

### Pinning, durable operator-evict, and the probe-hold stay engine-enforced

Three exemption layers sit above every admission and eviction policy, and the
cache engine enforces all three regardless of the active policy.

**Operator pinning overrides eviction.** A pinned hash never appears among
eviction candidates. The pin set reloads atomically on `SIGHUP`. Pinning does
not affect segment membership or frequency tracking; an unpinned hash
re-enters the eviction pool at whatever segment and frequency it already
carries.

**Durable operator-evict is orthogonal to eviction policy.**
`CacheEngine::evict` is the operator takedown path. It records the
hash in a durable, `fsync`-backed log and makes the engine treat the hash as
absent for every subsequent lookup, independent of any cache-pressure
eviction. Disk reclaim for an evicted hash follows on the next GC sweep, when
periodic GC is enabled and the protecting tag deletion succeeds. Pinning
protects a hash against eviction-policy pressure but not against
`CacheEngine::evict`: a durable operator directive always wins.

**A serve that detects stored corruption quarantines the hash.** Every serve
export validates the exported chunk groups and their proof nodes against the
content root. A hash mismatch or a short read over held content means that the
stored copy changed after admission, from disk rot or tampering. The engine
then quarantines the hash. The node stops serving, announcing, and
re-acquiring the hash. A stream request for it answers `EvictedSinceProbe`.
The engine drops the protecting tags, also for a pinned hash. The pin does not
keep corrupt bytes from GC. The next GC sweep reclaims the entry. The next
lookup or origin rescan that finds the store no longer holds the hash lifts
the quarantine. A later pull-through then admits a verified copy. A fill that
protects the entry during the quarantine does not block this: each origin
rescan drops the tags of a quarantined entry again. The quarantine is in
memory only. After a restart, the next serve of the corrupt bytes quarantines
the hash again. `CacheEngine::evict` is not the recovery path, because a
durable takedown withholds legitimate content permanently. Reclaim needs
periodic GC. When GC is off, the hash stays quarantined and the bytes stay on
disk.

**The probe-triggered hold composes above policy.** A hash a node has just
advertised as present, per [ADR 005 § Probe-Triggered Eviction
Hold](005-protocol.md#probe-triggered-eviction-hold), is exempt from eviction
for `probe_hold_duration` regardless of segment or frequency. The hold budget
(`max_probe_holds`) and its exhaustion behavior are unchanged by this ADR.

Reputation does not factor into admission or eviction. The unified
reputation score governs peer *selection*
([ADR 001](001-network.md#adr-001-network-topology-and-peer-mesh),
[ADR 008](008-reputation.md#adr-008-reputation-system)), not local cache
retention.

## Consequences

### Positive

- New admission and eviction policies plug in without touching the cache
  engine.
- Policies are pure functions over their context structs, so they are
  unit-testable without a live store.
- W-TinyLFU raises hit rate under a fixed disk budget and resists scan-driven
  eviction of the hot working set.
- The probationary segment bounds one-hit-wonder disk usage to a fixed
  budget.
- Safety invariants — pins, probe-holds, and the deny set — stay
  engine-enforced across every policy combination.
- The default configuration preserves `lru`/`always` recency-only behavior
  exactly; adopting the new policies is opt-in.

### Negative

- Range-aware eviction is not achievable until an upstream `iroh-blobs`
  range-forget primitive exists. Whole-blob reclaim ships in its place.
- W-TinyLFU state is not durable. Cold-start starvation on a full disk with
  no warmed signal persists under `lru`'s recency-only design too.
- Probationary admission still writes first-hit bytes; it is not a
  pass-through. Write amplification stays bounded by paid demand.

### Known limitations carried forward

- A `gc_interval_sec` of zero disables periodic GC, so the size ceiling is
  unenforceable and the boot path only warns.
- The write path applies no disk-full backpressure. Footprint overshoot is
  bounded only by the reactive eviction driver and the per-blob
  `max_blob_size` limit. The free-disk-aware ceiling defends `disk_headroom_mb`
  of the volume, but reactively: soft-evict plus GC lag means a heavy write
  burst can cross the margin briefly before the driver claws it back.
- Segment membership lives in memory only. A restart loses it, so every
  cached blob returns to an uncapped state until traffic re-observes it and
  the estimator rebuilds its signal.
- The probation cap measures its footprint over the eviction candidates —
  blobs touched since process start — so a probation blob not yet touched
  since boot is excluded from the cap's overage math until it is observed.

## Acceptance Criteria

1. `AdmissionPolicy`, `EvictionPolicy`, and `FrequencyEstimator` are defined
   in `crates/cache`. The engine holds them as trait objects and contains no
   inline policy-decision logic.
2. The `lru` and `always` defaults reproduce today's recency-only behavior;
   the existing eviction-driver test suite passes unchanged under them.
3. `tinylfu` eviction and probationary admission are available behind
   `cache.eviction_policy` and `cache.admission_policy`; both default off.
4. When either selector names `tinylfu`, one shared `FrequencyEstimator`
   feeds both the admission and the eviction policy.
5. Probationary footprint is capped by `probation_target_pct`. Promotion
   happens on a member's `promotion_threshold`-th sighting. The hot working
   set survives a cold scan.
6. Shipped policies emit only whole-blob eviction targets. Range reclaim is
   documented as blocked on an upstream `iroh-blobs` primitive.
7. Pins, probe-holds, and the deny set are never evicted, regardless of the
   active policy; the engine enforces this independent of any plan.
8. An unknown policy name is a config error at load, with no silent
   fallback.
9. The workspace builds clean under the anti-panic clippy lints
   (`unwrap_used`, `expect_used`, `panic`, `indexing_slicing`, `todo`, `unimplemented` denied).
