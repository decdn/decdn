import { describe, expect, it } from "vitest";
import type {
  EarningsPeriod,
  GatewayStatus,
  NodeSection,
  PullRecord,
  Pulls,
} from "./api";
import {
  buckets,
  capabilities,
  earningsRows,
  gatewayLine,
  livePulls,
  nodeLine,
  offense,
  partNote,
  partValue,
  pools,
  recentPulls,
  severity,
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
  it("sums the delegate lanes of the capability's signer and pool against its cap", () => {
    const rows = capabilities(
      status({
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
              lanes: [lane(1_000, 100), lane(2_000, 200, "0xcc")],
            },
            { signer: "0x99", pool_id: pool, lanes: [lane(500, 5)] },
          ],
        },
      }),
    );
    expect(rows[0]).toMatchObject({
      spentMicro: 3_000,
      bytes: 300,
      used: 0.75,
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

function record(overrides: Partial<PullRecord> = {}): PullRecord {
  return {
    uri: "decdn:aa",
    kind: "blob",
    output: "out",
    pid: 7,
    started_unix_ms: 1_000_000,
    updated_unix_ms: 1_030_000,
    finished_unix_ms: null,
    state: "running",
    received_bytes: 0,
    expected_bytes: null,
    files_started: 0,
    files_landed: 0,
    paid: { amount_micro_usdc: 0, bytes: 0 },
    nodes: [],
    bundle: null,
    error: null,
    stale: false,
    ...overrides,
  };
}

function pulls(overrides: Partial<Pulls> = {}): Pulls {
  return {
    dir: "/pulls",
    updated_unix_ms: 1_060_000,
    active: [],
    recent: [],
    unreadable: 0,
    ...overrides,
  };
}

describe("livePulls", () => {
  it("reads progress against the expected size, and ages a run from the last read", () => {
    const rows = livePulls(
      pulls({
        active: [
          record({
            kind: "bundle",
            received_bytes: 250,
            expected_bytes: 1_000,
            files_started: 4,
            files_landed: 3,
            nodes: [
              { provider: "0xbb", amount_micro_usdc: 5, bytes: 9 },
              { provider: "0xcc", amount_micro_usdc: 5, bytes: 9 },
            ],
            paid: { amount_micro_usdc: 10, bytes: 18 },
          }),
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
          record({ state: "done", finished_unix_ms: 1_042_000 }),
          record({ state: "failed", error: "no node holds the blob" }),
          record({ state: "gave_up" }),
        ],
      }),
    );
    expect(rows.map((r) => [r.outcome, r.tone])).toEqual([
      ["✓ Done", "ok"],
      ["⛔ Failed", "critical"],
      ["⚠ Gave up", "critical"],
    ]);
    expect(rows[0]?.durationSecs).toBe(42);
    // No end recorded: the last heartbeat.
    expect(rows[1]?.durationSecs).toBe(30);
    expect(rows[1]?.error).toBe("no node holds the blob");
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

function node(overrides: Partial<NodeSection> = {}): NodeSection {
  return {
    admin_url: "http://127.0.0.1:9191",
    metrics_url: "http://127.0.0.1:9090/metrics",
    state: "waiting",
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

describe("nodeLine", () => {
  it("says no node is there, or what the admin URL answered instead", () => {
    expect(nodeLine(node({ state: "absent" }))).toBe(
      "No node at http://127.0.0.1:9191.",
    );
    expect(
      nodeLine(
        node({
          state: "error",
          health: { updated_unix_ms: 1, state: "error", message: "404" },
        }),
      ),
    ).toBe("http://127.0.0.1:9191 does not answer as a node: 404");
    expect(nodeLine(node({ state: "up" }))).toBeNull();
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
