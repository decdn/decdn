import { describe, expect, it } from "vitest";
import type { HealthResponse, NodeMetrics } from "../api";
import { nodeTiles } from "./NodeHealth";

const health: HealthResponse = {
  node_id: "5f",
  uptime_s: 90,
  in_flight_streams: 3,
  binding: "mismatch",
  bound_node_id: "6a",
  registry_active: false,
};

const metrics: NodeMetrics = {
  uptime_seconds: 90,
  active_connections: 5,
  dispatch_in_flight: 2,
  cache_hits: 9,
  cache_misses: 1,
  cache_bytes_returned: 0,
  unredeemed_usdc: 1_250_000,
  rpc_healthy: true,
  rates: null,
  hit_rate: 0.9,
};

const value = (tiles: { label: string; value: string }[], label: string) =>
  tiles.find((t) => t.label === label)?.value;

describe("nodeTiles", () => {
  it("flag a key the operator is not bound to, and a node outside the active set", () => {
    const shown = nodeTiles(health, null, null);
    expect(value(shown, "Binding")).toBe("⛔ Mismatch");
    expect(value(shown, "Active staker")).toBe("⚠ No");
    expect(value(shown, "Cache hit rate")).toBeUndefined();
  });

  it("read the metrics alone when the admin reads fail, with no rate on the first scrape", () => {
    const shown = nodeTiles(null, metrics, null);
    expect(value(shown, "Binding")).toBeUndefined();
    expect(value(shown, "Cache hit rate")).toBe("90%");
    expect(value(shown, "Serving")).toBe("—");
    expect(value(shown, "Unredeemed")).toBe("1.25 USDC");
  });
});
