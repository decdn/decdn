import { describe, expect, it } from "vitest";
import type {
  EarningsPeriod,
  GatewayStatus,
  EndedPull,
  NodeSection,
  NodeState,
  PullFields,
  PullTarget,
  Pulls,
  RunningPull,
} from "./api";
import {
  buckets,
  capabilities,
  earningsRows,
  gatewayLine,
  livePulls,
  gatewayShapeProblem,
  nodeView,
  offense,
  partNote,
  partValue,
  pools,
  recentPulls,
  severity,
  staleSecs,
  stalestSecs,
} from "./view";

const signer = "0xAbCdEf0000000000000000000000000000000001";
const pool = "0x" + "01".repeat(32);

function status(overrides: Partial<GatewayStatus> = {}): GatewayStatus {
  return {
    mode: "loopback",
    version: "0.3.0",
    started_unix: 0,
    uptime_secs: 0,
    listen: "127.0.0.1:8484",
    chain_id: 421614,
    payment_pool: "0x00",
    counters: {
      bytes_served: 0,
      requests_ok: 0,
      requests_not_found: 0,
      requests_refused: 0,
      requests_allowance_exhausted: 0,
      pull_failures_funding: 0,
      pull_failures_other: 0,
      pulls_active: 0,
      pointer_updates: 0,
    },
    active_pulls: [],
    capabilities: [],
    allowance: null,
    pointers: null,
    pools: null,
    pool_error: null,
    ...overrides,
  };
}

const lane = (amount: number, bytes: number, provider = "0xbb") => ({
  voucher_signer: signer,
  provider,
  last_amount_micro_usdc: amount,
  last_bytes_delivered: bytes,
});

describe("pools", () => {
  it("takes headroom as the deposit less the whole pool's spend", () => {
    const rows = pools(
      status({
        pools: {
          pools: {
            pools: [
              {
                pool_id: pool,
                chain_id: 421614,
                payment_pool: "0x00",
                owner: signer,
                token: "0xcc",
                deposit_micro_usdc: 10_000_000,
                // More than the lanes' 1_000: another signer redeemed too.
                spent_micro_usdc: 9_000_000,
                lanes: [lane(1_000, 10)],
              },
            ],
            skipped: [],
          },
          delegate_lanes: [],
        },
      }),
    );
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({
      headroom: 1_000_000,
      used: 0.9,
      severity: "warning",
    });
  });

  it("never reads a negative headroom", () => {
    const rows = pools(
      status({
        pools: {
          pools: {
            pools: [
              {
                pool_id: pool,
                chain_id: 1,
                payment_pool: "0x00",
                owner: signer,
                token: "0xcc",
                deposit_micro_usdc: 5,
                spent_micro_usdc: 9,
                lanes: [],
              },
            ],
            skipped: [],
          },
          delegate_lanes: [],
        },
      }),
    );
    expect(rows[0]?.headroom).toBe(0);
    expect(rows[0]?.used).toBe(1);
  });
});

describe("capabilities", () => {
  it("takes the spend of the capability's signer and pool against its cap", () => {
    const rows = capabilities(
      status({
        active_pulls: [
          {
            hash: "ab",
            namespace: null,
            range: null,
            payer: { kind: "capability", signer: signer.toLowerCase() },
            since_secs: 1,
            delivered_bytes: 0,
          },
          {
            hash: "cd",
            namespace: null,
            range: null,
            payer: { kind: "pool" },
            since_secs: 1,
            delivered_bytes: 0,
          },
        ],
        capabilities: [
          {
            cap: 4_000,
            expiry: 2_000_000_000,
            origin: null,
            pool,
            scope: "*",
            signer,
            spent: false,
          },
        ],
        pools: {
          pools: { pools: [], skipped: [] },
          delegate_lanes: [
            // Addresses compare whatever their case.
            {
              signer: signer.toLowerCase(),
              pool_id: pool,
              // spent_micro_usdc (3_500) runs ahead of the stored lanes
              // (3_000): it counts the pull in flight.
              lanes: [lane(1_000, 100), lane(2_000, 200, "0xcc")],
              spent_micro_usdc: 3_500,
              bytes_delivered: 350,
            },
            {
              signer: "0x99",
              pool_id: pool,
              lanes: [lane(500, 5)],
              spent_micro_usdc: 500,
              bytes_delivered: 5,
            },
          ],
        },
      }),
    );
    expect(rows[0]).toMatchObject({
      spentMicro: 3_500,
      bytes: 350,
      used: 0.875,
      inFlight: 1,
    });
  });

  it("knows nothing of the spend when the store could not be read", () => {
    const rows = capabilities(
      status({
        capabilities: [
          {
            cap: 1,
            expiry: 0,
            origin: null,
            pool,
            scope: "*",
            signer,
            spent: true,
          },
        ],
      }),
    );
    expect(rows[0]?.spentMicro).toBeNull();
    expect(rows[0]?.used).toBeNull();
  });
});

describe("buckets", () => {
  it("labels the shared bucket and each namespace, and leaves an unbounded one unmetered", () => {
    const rows = buckets(
      status({
        mode: "public",
        allowance: [
          {
            namespace: null,
            per_day: 1_000,
            used: 250,
            remaining: 750,
            resets_in_secs: 60,
          },
          {
            namespace: 7,
            per_day: null,
            used: 0,
            remaining: null,
            resets_in_secs: 60,
          },
        ],
      }),
    );
    expect(rows.map((r) => [r.label, r.used])).toEqual([
      ["Shared", 0.25],
      ["Namespace 7", null],
    ]);
  });

  it("is empty on a loopback gateway", () => {
    expect(buckets(status())).toEqual([]);
  });
});

describe("severity", () => {
  it("steps up at 80% and 95%", () => {
    expect(severity(0.5)).toBe("ok");
    expect(severity(0.8)).toBe("warning");
    expect(severity(0.95)).toBe("critical");
    expect(severity(null)).toBe("ok");
  });
});

type Overrides = Partial<Omit<PullFields, "kind">>;

function fields(
  overrides: Overrides = {},
  target: PullTarget = { kind: "blob" },
): PullFields {
  return {
    uri: "decdn:aa",
    output: "out",
    pid: 7,
    started_unix_ms: 1_000_000,
    updated_unix_ms: 1_030_000,
    received_bytes: 0,
    expected_bytes: null,
    paid: { amount_micro_usdc: 0, bytes: 0 },
    nodes: [],
    stale: false,
    ...overrides,
    ...target,
  };
}

const record = (
  overrides: Overrides = {},
  target?: PullTarget,
): RunningPull => ({
  ...fields(overrides, target),
  state: "running",
});

function pulls(overrides: Partial<Pulls> = {}): Pulls {
  return {
    dir: "/pulls",
    updated_unix_ms: 1_060_000,
    active: [],
    recent: [],
    unreadable: 0,
    error: null,
    ...overrides,
  };
}

describe("livePulls", () => {
  it("reads progress against the expected size, and ages a run from the last read", () => {
    const rows = livePulls(
      pulls({
        active: [
          record(
            {
              received_bytes: 250,
              expected_bytes: 1_000,
              nodes: [
                { provider: "0xbb", amount_micro_usdc: 5, bytes: 9 },
                { provider: "0xcc", amount_micro_usdc: 5, bytes: 9 },
              ],
              paid: { amount_micro_usdc: 10, bytes: 18 },
            },
            {
              kind: "bundle",
              files_started: 4,
              files_landed: 3,
              counts: null,
            },
          ),
        ],
      }),
    );
    expect(rows[0]).toMatchObject({
      done: 0.25,
      files: "3 of 4 files",
      paidMicro: 10,
      nodes: 2,
      ageSecs: 60,
      stale: false,
    });
  });

  it("leaves a run of unknown size unmetered, and counts no files for one blob", () => {
    const rows = livePulls(pulls({ active: [record({ stale: true })] }));
    expect(rows[0]).toMatchObject({ done: null, files: null, stale: true });
  });
});

describe("recentPulls", () => {
  it("names each outcome, and times a run to its end", () => {
    const rows = recentPulls(
      pulls({
        recent: [
          { ...fields(), state: "done", finished_unix_ms: 1_042_000 },
          {
            ...fields(),
            state: "failed",
            finished_unix_ms: 1_005_000,
            error: "no node holds the blob",
          },
          {
            ...fields(),
            state: "gave_up",
            finished_unix_ms: 1_090_000,
            error: "no progress",
          },
          { ...fields(), state: "interrupted", finished_unix_ms: 1_001_000 },
        ] satisfies EndedPull[],
      }),
    );
    expect(rows.map((r) => [r.outcome, r.tone, r.error])).toEqual([
      ["✓ Done", "ok", null],
      ["⛔ Failed", "critical", "no node holds the blob"],
      ["⚠ Gave up", "critical", "no progress"],
      ["Interrupted", "muted", null],
    ]);
    expect(rows.map((r) => r.durationSecs)).toEqual([42, 5, 90, 1]);
  });
});

describe("gatewayLine", () => {
  const url = "http://127.0.0.1:8484/";
  it("says why no gateway panel shows, and nothing once it is up", () => {
    const base = { url, updated_unix_ms: 1 };
    expect(gatewayLine({ ...base, state: "absent" })).toBe(
      `No gateway at ${url}.`,
    );
    expect(gatewayLine({ ...base, state: "unauthorized" })).toContain(
      "--gateway-token-file",
    );
    expect(gatewayLine({ ...base, state: "error", message: "502" })).toContain(
      "502",
    );
    expect(gatewayLine({ ...base, state: "up", status: status() })).toBeNull();
  });
});

const waiting = { updated_unix_ms: null, state: "waiting" } as const;

function node(
  state: NodeState,
  overrides: { health?: NodeSection["health"] } = {},
): NodeSection {
  return {
    admin_url: "http://127.0.0.1:9191",
    metrics_url: "http://127.0.0.1:9090/metrics",
    ...state,
    health: waiting,
    status: waiting,
    metrics: waiting,
    lanes: waiting,
    earnings: waiting,
    pools: waiting,
    slashes: waiting,
    ...overrides,
  };
}

describe("nodeView", () => {
  it("shows no panels while no node answers", () => {
    expect(nodeView(node({ state: "absent" }))).toEqual({
      line: "No node at http://127.0.0.1:9191.",
      alert: false,
      panels: false,
    });
    expect(nodeView(node({ state: "waiting" })).panels).toBe(false);
  });

  it("keeps the panels on a failed health read, with its message above them", () => {
    const view = nodeView(
      node(
        { state: "error", message: "did not respond within 3000ms" },
        // Its own message, which the view does not read.
        { health: { updated_unix_ms: 1, state: "error", message: "other" } },
      ),
    );
    expect(view).toEqual({
      line: "The node's health read failed: did not respond within 3000ms",
      alert: true,
      panels: true,
    });
  });

  it("says nothing above the panels of a node that answers", () => {
    expect(nodeView(node({ state: "up" }))).toEqual({
      line: null,
      alert: false,
      panels: true,
    });
  });
});

describe("staleSecs and stalestSecs", () => {
  const now = 1_000_000;

  it("flag a read older than three of its polls, in seconds", () => {
    expect(staleSecs(now - 6_000, now, 2_000)).toBeNull();
    expect(staleSecs(now - 6_001, now, 2_000)).toBe(6.001);
    expect(staleSecs(now - 40_000, now, 15_000)).toBeNull();
    expect(staleSecs(now - 50_000, now, 15_000)).toBe(50);
  });

  it("flag nothing before the first read", () => {
    expect(staleSecs(null, now, 1_000)).toBeNull();
  });

  it("take the stalest of several reads", () => {
    const at = (ms: number | null) => ({ updated_unix_ms: ms });
    expect(stalestSecs([at(now), at(null)], now, 2_000)).toBeNull();
    expect(
      stalestSecs([at(now - 10_000), at(now - 20_000), at(now)], now, 2_000),
    ).toBe(20);
  });
});

describe("gatewayShapeProblem", () => {
  it("passes a gateway body the panels can read", () => {
    expect(gatewayShapeProblem(status())).toBeNull();
  });

  it("names what a body of another shape lacks", () => {
    expect(gatewayShapeProblem("busy")).toBe("the body is not an object");
    expect(gatewayShapeProblem({ ...status(), mode: "other" })).toBe(
      "it names no mode",
    );
    expect(gatewayShapeProblem({ ...status(), counters: null })).toBe(
      "it has no counters",
    );
    expect(gatewayShapeProblem({ ...status(), capabilities: undefined })).toBe(
      "capabilities is not a list",
    );
    expect(gatewayShapeProblem({ ...status(), allowance: {} })).toBe(
      "allowance is not a list",
    );
    expect(gatewayShapeProblem({ ...status(), pools: { pools: [] } })).toBe(
      "pools is not a pool snapshot",
    );
  });
});

describe("partNote and partValue", () => {
  it("tell a feature the node runs without from a failed read", () => {
    expect(
      partNote({ updated_unix_ms: 1, state: "unconfigured", message: "off" }),
    ).toEqual({ text: "off", alert: false });
    expect(
      partNote({ updated_unix_ms: 1, state: "error", message: "timeout" }),
    ).toEqual({ text: "timeout", alert: true });
    expect(partNote(waiting)?.alert).toBe(false);
    expect(partNote({ updated_unix_ms: 1, state: "ok", value: 3 })).toBeNull();
  });

  it("give a read's value only once it has one", () => {
    expect(partValue({ updated_unix_ms: 1, state: "ok", value: 3 })).toBe(3);
    expect(partValue(waiting)).toBeNull();
  });
});

describe("earningsRows", () => {
  const period = (from_unix: number): EarningsPeriod => ({
    from_unix,
    net_micro_usdc: 0,
    gross_micro_usdc: 0,
    bytes_settled: 0,
    settlements: 0,
  });

  it("lists the standard periods, then the one a request asked for", () => {
    const rows = earningsRows({
      operator_address: null,
      ledger_since_unix: null,
      last_24h: period(1),
      last_7d: period(2),
      since_ledger_start: period(0),
      since: period(1_793_000_000),
      last_settlement: null,
      pending: null,
    });
    expect(rows.map((r) => r.label)).toEqual([
      "Last 24 h",
      "Last 7 days",
      "Since the ledger started",
      "Since 2026-10-26",
    ]);
  });
});

describe("offense", () => {
  it("names the known offenses and numbers the rest", () => {
    expect(offense(0)).toBe("Rate manipulation");
    expect(offense(1)).toBe("Blacklist");
    expect(offense(4)).toBe("Offense 4");
  });
});
