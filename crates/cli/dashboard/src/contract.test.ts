// The `/_status` contract: the fixtures are what the gateway serializes,
// pinned by crates/cli/src/commands/gateway/status/tests.rs. Each key table
// below is exhaustive over its `api.ts` type at compile time, and each
// fixture object must carry exactly those keys at run time, so a field
// renamed or added on either side fails here.
import { describe, expect, it } from "vitest";
import type {
  ActivePull,
  Bucket,
  Capability,
  Counters,
  DelegateLanes,
  Lane,
  Pointer,
  Pool,
  PoolSnapshot,
  Status,
} from "./api";
import loopback from "./fixtures/status-loopback.json";
import poolError from "./fixtures/status-pool-error.json";
import publicStatus from "./fixtures/status-public.json";
import { buckets, capabilities, pools } from "./view";

type Keys<T> = Record<keyof T, true>;

const STATUS: Keys<Status> = {
  mode: true,
  version: true,
  dashboard_built: true,
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
};
const SNAPSHOT: Keys<PoolSnapshot> = { pools: true, delegate_lanes: true };

function keysOf(value: unknown): string[] {
  expect(value, "an object").toBeTypeOf("object");
  return Object.keys(value as object).sort();
}

function expectShape(value: unknown, keys: object, where: string) {
  expect(keysOf(value), where).toEqual(Object.keys(keys).sort());
}

function expectStatus(raw: unknown) {
  expectShape(raw, STATUS, "status");
  const s = raw as Status;
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

describe("the /_status fixtures", () => {
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
