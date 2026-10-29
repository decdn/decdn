// The `/_status` contracts. dashboard-*.json is what `decdn dashboard`
// serializes, pinned by crates/cli/src/commands/dashboard/status/tests.rs;
// its gateway section carries the gateway's own body, which status-*.json
// pins (crates/cli/src/commands/gateway/status/tests.rs). Each key table
// below is exhaustive over its `api.ts` type at compile time, and each
// fixture object must carry exactly those keys at run time, so a field
// renamed or added on either side fails here.
import { describe, expect, it } from "vitest";
import type {
  ActivePull,
  BucketStat,
  Bucket,
  BuyerLaneSnapshot,
  BuyerPoolSnapshot,
  BuyerPoolsResponse,
  BundleCounts,
  Capability,
  Counters,
  DelegateLanes,
  EarningsPeriod,
  EarningsResponse,
  GatewaySection,
  GatewayStatus,
  HealthResponse,
  Lane,
  LaneSnapshot,
  LanesResponse,
  LastSettlement,
  NodeMetrics,
  NodePaid,
  NodeRates,
  NodeSection,
  Paid,
  Part,
  PendingRedeem,
  Pointer,
  Pool,
  PoolSnapshot,
  PullFields,
  PullRecord,
  PullThroughStatus,
  Pulls,
  RecordStoreHealth,
  RepublishHealth,
  RoutingHealth,
  SlashesResponse,
  SlashRecordDto,
  Status,
  StatusResponse,
  UnreadableRecord,
} from "./api";
import alone from "./fixtures/dashboard-alone.json";
import failing from "./fixtures/dashboard-failing.json";
import full from "./fixtures/dashboard-full.json";
import loopback from "./fixtures/status-loopback.json";
import poolError from "./fixtures/status-pool-error.json";
import publicStatus from "./fixtures/status-public.json";
import {
  buckets,
  capabilities,
  earningsRows,
  gatewayLine,
  gatewayShapeProblem,
  livePulls,
  nodeView,
  partNote,
  poolRows,
  pools,
  recentPulls,
  unreadableView,
} from "./view";

type Keys<T> = Record<keyof T, true>;

/** Each variant of a `state`-tagged union, with its exact keys. */
type Variants<T extends { state: string }> = {
  [S in T["state"]]: Keys<Extract<T, { state: S }>>;
};

/**
 * Each arm of a `kind`-tagged union, with only the keys that arm adds beyond
 * those every arm shares. An arm that adds none is `{}`, which the compiler
 * cannot hold to no keys: `expectShape` checks it at run time, against
 * fixtures that hold every kind.
 */
type KindVariants<T extends { kind: string }> = {
  [K in T["kind"]]: Keys<Omit<Extract<T, { kind: K }>, keyof T>>;
};

const STATUS: Keys<GatewayStatus> = {
  mode: true,
  version: true,
  started_unix: true,
  uptime_secs: true,
  listen: true,
  chain_id: true,
  payment_pool: true,
  counters: true,
  active_pulls: true,
  capabilities: true,
  allowance: true,
  pointers: true,
  pools: true,
  pool_error: true,
};
const COUNTERS: Keys<Counters> = {
  bytes_served: true,
  requests_ok: true,
  requests_not_found: true,
  requests_refused: true,
  requests_allowance_exhausted: true,
  pull_failures_funding: true,
  pull_failures_other: true,
  pulls_active: true,
  pointer_updates: true,
};
const PULL: Keys<ActivePull> = {
  hash: true,
  namespace: true,
  range: true,
  payer: true,
  since_secs: true,
  delivered_bytes: true,
};
const CAPABILITY: Keys<Capability> = {
  cap: true,
  expiry: true,
  origin: true,
  pool: true,
  scope: true,
  signer: true,
  spent: true,
};
const BUCKET: Keys<Bucket> = {
  namespace: true,
  per_day: true,
  used: true,
  remaining: true,
  resets_in_secs: true,
};
const POINTER: Keys<Pointer> = {
  namespace: true,
  root: true,
  seq: true,
  signer: true,
};
const LANE: Keys<Lane> = {
  voucher_signer: true,
  provider: true,
  last_amount_micro_usdc: true,
  last_bytes_delivered: true,
};
const POOL: Keys<Pool> = {
  pool_id: true,
  chain_id: true,
  payment_pool: true,
  owner: true,
  token: true,
  deposit_micro_usdc: true,
  spent_micro_usdc: true,
  lanes: true,
};
const DELEGATE: Keys<DelegateLanes> = {
  signer: true,
  pool_id: true,
  lanes: true,
  spent_micro_usdc: true,
  bytes_delivered: true,
};
const SNAPSHOT: Keys<PoolSnapshot> = { pools: true, delegate_lanes: true };

const DASHBOARD: Keys<Status> = {
  version: true,
  now_unix_ms: true,
  dashboard_built: true,
  started_unix: true,
  pulls: true,
  gateway: true,
  node: true,
};
const PULLS: Keys<Pulls> = {
  dir: true,
  updated_unix_ms: true,
  active: true,
  recent: true,
  unreadable: true,
  error: true,
};
const PULL_FIELDS: Keys<PullFields> = {
  uri: true,
  kind: true,
  output: true,
  pid: true,
  started_unix_ms: true,
  updated_unix_ms: true,
  received_bytes: true,
  downloaded_bytes: true,
  resumed_bytes: true,
  reused_bytes: true,
  expected_bytes: true,
  paid: true,
  nodes: true,
  stale: true,
};
/** Only a bundle run carries bundle fields. */
const PULL_KIND: KindVariants<PullFields> = {
  blob: {},
  entry: {},
  bundle: { files_started: true, files_landed: true, counts: true },
};
const PULL_RECORD: Variants<PullRecord> = {
  running: { ...PULL_FIELDS, state: true },
  done: { ...PULL_FIELDS, state: true, finished_unix_ms: true },
  interrupted: { ...PULL_FIELDS, state: true, finished_unix_ms: true },
  failed: { ...PULL_FIELDS, state: true, finished_unix_ms: true, error: true },
  gave_up: { ...PULL_FIELDS, state: true, finished_unix_ms: true, error: true },
};
const PAID: Keys<Paid> = { amount_micro_usdc: true, bytes: true };
const NODE_PAID: Keys<NodePaid> = {
  provider: true,
  amount_micro_usdc: true,
  bytes: true,
};
const BUNDLE_COUNTS: Keys<BundleCounts> = {
  fetched: true,
  linked: true,
  skipped: true,
  reused: true,
  excluded: true,
  failed: true,
  not_run: true,
};
const UNREADABLE: Keys<UnreadableRecord> = { path: true, error: true };
const GATEWAY_BASE = { url: true, updated_unix_ms: true, state: true } as const;
const GATEWAY: Variants<GatewaySection> = {
  waiting: GATEWAY_BASE,
  absent: GATEWAY_BASE,
  unauthorized: GATEWAY_BASE,
  error: { ...GATEWAY_BASE, message: true },
  up: { ...GATEWAY_BASE, status: true },
};
const NODE_BASE = {
  admin_url: true,
  metrics_url: true,
  state: true,
  health: true,
  status: true,
  metrics: true,
  lanes: true,
  earnings: true,
  pools: true,
  slashes: true,
} as const;
const NODE: Variants<NodeSection> = {
  waiting: NODE_BASE,
  absent: NODE_BASE,
  up: NODE_BASE,
  error: { ...NODE_BASE, message: true },
};
const PART_BASE = { updated_unix_ms: true, state: true } as const;
const PART: Variants<Part<unknown>> = {
  waiting: PART_BASE,
  ok: { ...PART_BASE, value: true },
  unconfigured: { ...PART_BASE, message: true },
  error: { ...PART_BASE, message: true },
};
const HEALTH: Keys<HealthResponse> = {
  node_id: true,
  uptime_s: true,
  in_flight_streams: true,
  binding: true,
  bound_node_id: true,
  registry_active: true,
};
const NODE_STATUS: Keys<StatusResponse> = {
  node_id: true,
  routing: true,
  known_stakers: true,
  record_store: true,
  republish: true,
  chain_denied_origins: true,
  operator_address: true,
  pull_through: true,
};
const PULL_THROUGH: Keys<PullThroughStatus> = {
  enabled: true,
  buyer_wallet_micro_usdc: true,
};
const ROUTING: Keys<RoutingHealth> = {
  total_peers: true,
  non_empty_buckets: true,
  buckets: true,
  bucket_capacity: true,
  refresh_interval_s: true,
  last_refresh_us: true,
};
const BUCKET_STAT: Keys<BucketStat> = { index: true, fill: true };
const RECORD_STORE: Keys<RecordStoreHealth> = { records: true, capacity: true };
const REPUBLISH: Keys<RepublishHealth> = { scheduled_records: true };
const METRICS: Keys<NodeMetrics> = {
  uptime_seconds: true,
  active_connections: true,
  dispatch_in_flight: true,
  cache_hits: true,
  cache_misses: true,
  cache_bytes_returned: true,
  unredeemed_usdc: true,
  rpc_healthy: true,
  rates: true,
  hit_rate: true,
};
const RATES: Keys<NodeRates> = { hits: true, misses: true, bytes: true };
const LANES: Keys<LanesResponse> = {
  lanes: true,
  redeem_threshold_micro_usdc: true,
};
const LANE_SNAPSHOT: Keys<LaneSnapshot> = {
  pool_id: true,
  counterparty: true,
  voucher_signer: true,
  last_nonce: true,
  outstanding_micro_usdc: true,
  unredeemed_micro_usdc: true,
  deposit_micro_usdc: true,
  seconds_since_last_voucher: true,
  settlement_eligible: true,
};
const EARNINGS: Keys<EarningsResponse> = {
  operator_address: true,
  covered_from_unix: true,
  last_24h: true,
  last_7d: true,
  since_ledger_start: true,
  since: true,
  last_settlement: true,
  pending: true,
};
const PERIOD: Keys<EarningsPeriod> = {
  from_unix: true,
  net_micro_usdc: true,
  gross_micro_usdc: true,
  bytes_settled: true,
  settlements: true,
};
const LAST_SETTLEMENT: Keys<LastSettlement> = {
  at_unix: true,
  tx_hash: true,
  block_number: true,
  net_micro_usdc: true,
};
const PENDING: Keys<PendingRedeem> = {
  micro_usdc: true,
  lanes: true,
  stranded_micro_usdc: true,
  stranded_lanes: true,
  last_sweep_at_unix: true,
  next_sweep_at_unix: true,
};
const BUYER_POOLS: Keys<BuyerPoolsResponse> = { pools: true, skipped: true };
const BUYER_POOL: Keys<BuyerPoolSnapshot> = {
  pool_id: true,
  chain_id: true,
  payment_pool: true,
  owner: true,
  token: true,
  deposit_micro_usdc: true,
  spent_micro_usdc: true,
  lanes: true,
};
const BUYER_LANE: Keys<BuyerLaneSnapshot> = {
  voucher_signer: true,
  provider: true,
  last_amount_micro_usdc: true,
  last_bytes_delivered: true,
};
const SLASHES: Keys<SlashesResponse> = { slashes: true };
const SLASH: Keys<SlashRecordDto> = {
  slash_id: true,
  offense_type: true,
  amount: true,
  evidence_hash: true,
  block_number: true,
  appeal_window_close: true,
};

function keysOf(value: unknown): string[] {
  expect(value, "an object").toBeTypeOf("object");
  return Object.keys(value as object).sort();
}

function expectShape(value: unknown, keys: object, where: string) {
  expect(keysOf(value), where).toEqual(Object.keys(keys).sort());
}

function expectStatus(raw: unknown) {
  expectShape(raw, STATUS, "status");
  const s = raw as GatewayStatus;
  expect(["loopback", "public"]).toContain(s.mode);
  expectShape(s.counters, COUNTERS, "counters");
  for (const p of s.active_pulls) {
    expectShape(p, PULL, "active_pulls[]");
    expect(["pool", "capability"]).toContain(p.payer.kind);
    expectShape(
      p.payer,
      p.payer.kind === "pool" ? { kind: 1 } : { kind: 1, signer: 1 },
      "payer",
    );
  }
  s.capabilities.forEach((c) => expectShape(c, CAPABILITY, "capabilities[]"));
  s.allowance?.forEach((b) => expectShape(b, BUCKET, "allowance[]"));
  s.pointers?.forEach((p) => expectShape(p, POINTER, "pointers[]"));
  if (s.pools) {
    expectShape(s.pools, SNAPSHOT, "pools");
    expectShape(s.pools.pools, { pools: 1, skipped: 1 }, "pools.pools");
    for (const p of s.pools.pools.pools) {
      expectShape(p, POOL, "pools.pools.pools[]");
      p.lanes.forEach((l) => expectShape(l, LANE, "lanes[]"));
    }
    for (const d of s.pools.delegate_lanes) {
      expectShape(d, DELEGATE, "delegate_lanes[]");
      d.lanes.forEach((l) => expectShape(l, LANE, "delegate lanes[]"));
    }
  }
  return s;
}

/** A run's record: the keys of its state's and its kind's arms, and only those. */
function expectPull(raw: PullRecord, where: string) {
  expect(Object.keys(PULL_RECORD)).toContain(raw.state);
  expect(Object.keys(PULL_KIND)).toContain(raw.kind);
  expectShape(
    raw,
    { ...PULL_RECORD[raw.state], ...PULL_KIND[raw.kind] },
    where,
  );
  const p = raw;
  expectShape(p.paid, PAID, `${where}.paid`);
  p.nodes.forEach((n) => expectShape(n, NODE_PAID, `${where}.nodes[]`));
  if (p.kind === "bundle" && p.counts) {
    expectShape(p.counts, BUNDLE_COUNTS, `${where}.counts`);
  }
}

/** A node read's envelope, and `check` over its value when it has one. */
function expectPart<T>(raw: Part<T>, where: string, check: (value: T) => void) {
  expect(Object.keys(PART)).toContain(raw.state);
  expectShape(raw, PART[raw.state], where);
  if (raw.state === "ok") check(raw.value);
}

function expectNode(n: NodeSection) {
  expect(Object.keys(NODE)).toContain(n.state);
  expectShape(n, NODE[n.state], "node");
  expectPart(n.health, "node.health", (h) => {
    expectShape(h, HEALTH, "health");
    expect(["bound", "mismatch", "unbound", "unknown"]).toContain(h.binding);
  });
  expectPart(n.status, "node.status", (s) => {
    expectShape(s, NODE_STATUS, "status");
    expectShape(s.routing, ROUTING, "status.routing");
    s.routing.buckets.forEach((b) => expectShape(b, BUCKET_STAT, "buckets[]"));
    expectShape(s.record_store, RECORD_STORE, "status.record_store");
    expectShape(s.republish, REPUBLISH, "status.republish");
    if (s.pull_through) {
      expectShape(s.pull_through, PULL_THROUGH, "status.pull_through");
    }
  });
  expectPart(n.metrics, "node.metrics", (m) => {
    expectShape(m, METRICS, "metrics");
    if (m.rates) expectShape(m.rates, RATES, "metrics.rates");
  });
  expectPart(n.lanes, "node.lanes", (l) => {
    expectShape(l, LANES, "lanes");
    l.lanes.forEach((x) => expectShape(x, LANE_SNAPSHOT, "lanes.lanes[]"));
  });
  expectPart(n.earnings, "node.earnings", (e) => {
    expectShape(e, EARNINGS, "earnings");
    for (const p of [e.last_24h, e.last_7d, e.since_ledger_start, e.since]) {
      if (p) expectShape(p, PERIOD, "earnings period");
    }
    if (e.last_settlement) {
      expectShape(e.last_settlement, LAST_SETTLEMENT, "last_settlement");
    }
    if (e.pending) expectShape(e.pending, PENDING, "pending");
  });
  expectPart(n.pools, "node.pools", (p) => {
    expectShape(p, BUYER_POOLS, "pools");
    for (const pool of p.pools) {
      expectShape(pool, BUYER_POOL, "pools.pools[]");
      pool.lanes.forEach((l) => expectShape(l, BUYER_LANE, "pool lanes[]"));
    }
  });
  expectPart(n.slashes, "node.slashes", (s) => {
    expectShape(s, SLASHES, "slashes");
    s.slashes.forEach((x) => expectShape(x, SLASH, "slashes[]"));
  });
}

/**
 * The dashboard's body. The inside of `gateway.status` is the gateway's own
 * contract, checked against status-*.json; here it is a stand-in.
 */
function expectDashboard(raw: unknown) {
  expectShape(raw, DASHBOARD, "dashboard");
  const s = raw as Status;
  expectShape(s.pulls, PULLS, "pulls");
  for (const u of s.pulls.unreadable) {
    expectShape(u, UNREADABLE, "pulls.unreadable[]");
  }
  for (const p of s.pulls.active) {
    expectPull(p, "pulls.active[]");
    expect(p.state, "an active run").toBe("running");
    // Counts are set once a run ended.
    if (p.kind === "bundle") expect(p.counts).toBeNull();
  }
  for (const p of s.pulls.recent) {
    expectPull(p, "pulls.recent[]");
    expect(p.state, "a recent run").not.toBe("running");
  }
  expect(Object.keys(GATEWAY)).toContain(s.gateway.state);
  expectShape(s.gateway, GATEWAY[s.gateway.state], "gateway");
  expectNode(s.node);
  return s;
}

describe("the dashboard's /_status fixtures", () => {
  it("match api.ts with every source up, and read into the panels", () => {
    const s = expectDashboard(full);
    expect(
      new Set([...s.pulls.active, ...s.pulls.recent].map((p) => p.kind)),
      "every kind's arm is pinned",
    ).toEqual(new Set(Object.keys(PULL_KIND)));
    expect(s.gateway.state).toBe("up");
    if (s.gateway.state === "up") {
      expect(s.gateway.status).toEqual({ pinned_by: "status-loopback.json" });
    }
    expect(livePulls(s.pulls).map((p) => [p.files, p.stale])).toEqual([
      ["9 of 12 files", false],
      [null, true],
    ]);
    expect(
      recentPulls(s.pulls).map((p) => [p.outcome, p.durationSecs, p.error]),
    ).toEqual([
      ["✓ Done", 42, null],
      ["✓ Done", 42, null],
      ["Interrupted", 42, null],
      ["⛔ Failed", 42, "no node holds the blob"],
    ]);
    expect(
      recentPulls(s.pulls).map((p) => [
        p.counts?.map((c) => c.text) ?? null,
        p.downloaded,
        p.resumed,
        p.reused,
      ]),
    ).toEqual([
      [["11 skipped", "9 excluded"], 0, 0, 0],
      [
        ["9 fetched", "1 linked", "2 skipped", "1 reused"],
        734_003_200,
        0,
        4_096,
      ],
      [null, 734_003_200, 0, 0],
      [null, 465_567_744, 268_435_456, 0],
    ]);
    expect(livePulls(s.pulls).map((p) => p.resumed)).toEqual([0, 268_435_456]);
    expect(unreadableView(s.pulls)).toMatchObject({ count: 1, more: 0 });
    expect(s.pulls.unreadable[0]?.error).toContain("missing field");
    expect(s.pulls.error).toBeNull();
    expect(nodeView(s.node).line).toBeNull();
    expect(partNote(s.node.slashes)).toEqual({
      text: "slash detection is not configured",
      alert: false,
    });
    expect(
      s.node.earnings.state === "ok" &&
        earningsRows(s.node.earnings.value).map((r) => r.label),
    ).toEqual(["Last 24 h", "Last 7 days", "Since the ledger started"]);
    expect(
      s.node.pools.state === "ok" && poolRows(s.node.pools.value.pools)[0],
    ).toMatchObject({ headroom: 1_690_000 });
  });

  it("match api.ts with no gateway and no node", () => {
    const s = expectDashboard(alone);
    expect(gatewayLine(s.gateway)).toBe(
      "No gateway at http://127.0.0.1:8484/.",
    );
    expect(nodeView(s.node)).toMatchObject({
      line: "No node at http://127.0.0.1:9191.",
      panels: false,
    });
    expect(livePulls(s.pulls)).toEqual([]);
    expect(recentPulls(s.pulls)[0]?.outcome).toBe("⚠ Gave up");
  });

  it("match api.ts with every source failing, and keep the node reads that answered", () => {
    const s = expectDashboard(failing);
    expect(s.pulls.error).toContain("Permission denied");
    expect(s.pulls.active).toEqual([]);
    expect(gatewayLine(s.gateway)).toContain("connection reset by peer");
    expect(nodeView(s.node)).toEqual({
      line: "The node's health read failed: admin at http://127.0.0.1:9191 did not respond within 3000ms",
      alert: true,
      panels: true,
    });
    expect(s.node.status.state).toBe("ok");
    expect(s.node.metrics.state).toBe("ok");
  });

  it("carry a gateway body the gateway's own contract accepts", () => {
    const spliced = { ...full, gateway: { ...full.gateway, status: loopback } };
    const s = expectDashboard(spliced);
    expect(
      s.gateway.state === "up" && expectStatus(s.gateway.status).mode,
    ).toBe("loopback");
    for (const body of [loopback, publicStatus, poolError]) {
      expect(gatewayShapeProblem(body)).toBeNull();
    }
  });
});

describe("the gateway's /_status fixtures", () => {
  it("match api.ts on a public gateway, and read into its panels", () => {
    const s = expectStatus(publicStatus);
    expect(s.mode).toBe("public");
    expect(s.active_pulls[0]?.range).toEqual([1_048_576, 4_194_304]);
    expect(pools(s)[0]).toMatchObject({ headroom: 6_750_000 });
    expect(buckets(s).map((b) => b.label)).toEqual(["Shared", "Namespace 9"]);
  });

  it("match api.ts on a loopback gateway, and tie a capability to its key's lanes", () => {
    const s = expectStatus(loopback);
    expect(s.mode).toBe("loopback");
    expect(s.allowance).toBeNull();
    expect(s.pointers).toBeNull();
    expect(capabilities(s)[0]).toMatchObject({
      spentMicro: 1_250_000,
      used: 0.3125,
    });
  });

  it("match api.ts when the store could not be read", () => {
    const s = expectStatus(poolError);
    expect(s.pools).toBeNull();
    expect(s.pool_error).toBeTypeOf("string");
    expect(capabilities(s)).toEqual([]);
  });
});
