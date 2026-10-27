import { describe, expect, it } from "vitest";
import type { Counters } from "../api";
import { tiles } from "./StatTiles";

const counters: Counters = {
  bytes_served: 0,
  requests_ok: 0,
  requests_not_found: 0,
  requests_refused: 0,
  requests_allowance_exhausted: 4,
  pull_failures_funding: 0,
  pull_failures_other: 0,
  pulls_active: 0,
  pointer_updates: 2,
};

describe("tiles", () => {
  it("show the 402s of a loopback gateway, as pages no capability covers", () => {
    const shown = tiles(counters, false);
    expect(
      shown.find((t) => t.label === "No capability for the page")?.value,
    ).toBe("4");
    expect(shown.some((t) => t.label === "Allowance spent")).toBe(false);
    expect(shown.some((t) => t.label === "Pointer updates")).toBe(false);
  });

  it("show the 402s of a public gateway as the spent allowance, beside its pointer updates", () => {
    const shown = tiles(counters, true);
    expect(shown.find((t) => t.label === "Allowance spent")?.value).toBe("4");
    expect(shown.find((t) => t.label === "Pointer updates")?.value).toBe("2");
    expect(shown.some((t) => t.label === "No capability for the page")).toBe(
      false,
    );
  });
});
