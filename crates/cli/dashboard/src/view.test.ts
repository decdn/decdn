import { describe, expect, it } from "vitest";
import type { Status } from "./api";
import { buckets, capabilities, pools, severity } from "./view";

const signer = "0xAbCdEf0000000000000000000000000000000001";
const pool = "0x" + "01".repeat(32);

function status(overrides: Partial<Status> = {}): Status {
  return {
    mode: "loopback",
    version: "0.3.0",
    dashboard_built: true,
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
