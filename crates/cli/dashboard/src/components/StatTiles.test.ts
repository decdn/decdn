import { describe, expect, it } from "vitest";
import type { Counters } from "../api";
import { tiles } from "./StatTiles";

const counters: Counters = {
  bytes_served: 0,
  requests_ok: 0,
  requests_not_found: 0,
  requests_refused: 0,
  requests_payment_required: 4,
  pull_failures_funding: 5,
  pull_failures_other: 0,
  pulls_active: 0,
  pointer_updates: 2,
  cache_hits: 3,
};

const couldNotPay = (publicMode: boolean) =>
  tiles(counters, publicMode).find((t) => t.label === "No capability could pay")
    ?.value;

describe("tiles", () => {
  it("count a loopback gateway's 402s as no capability that could pay", () => {
    expect(couldNotPay(false)).toBe("4");
    expect(
      tiles(counters, false).some((t) => t.label === "Pointer updates"),
    ).toBe(false);
  });

  it("show the cache hits in both modes", () => {
    for (const publicMode of [false, true]) {
      expect(
        tiles(counters, publicMode).find((t) => t.label === "Cache hits")
          ?.value,
      ).toBe("3");
    }
  });

  it("count a public gateway's 503s as no capability that could pay, beside its pointer updates", () => {
    expect(couldNotPay(true)).toBe("5");
    expect(
      tiles(counters, true).find((t) => t.label === "Pointer updates")?.value,
    ).toBe("2");
  });
});
