import type {
  BindingStatus,
  HealthResponse,
  NodeMetrics,
  NodeSection,
  StatusResponse,
} from "../api";
import {
  formatCount,
  formatDuration,
  formatRate,
  formatShare,
  formatUsdc,
} from "../format";
import { partValue } from "../view";
import { PartLine, Stale } from "./Panel";
import { TileGrid, type Tile } from "./StatTiles";

const BINDING: Record<BindingStatus, { value: string; hint: string }> = {
  bound: {
    value: "Bound",
    hint: "The operator's on-chain binding names this key",
  },
  mismatch: {
    value: "⛔ Mismatch",
    hint: "The operator is bound to another key: a slash cannot reach this node. decdn node rotate-key repairs it",
  },
  unbound: {
    value: "⚠ Unbound",
    hint: "The operator has no binding: decdn node register makes one",
  },
  unknown: {
    value: "Unknown",
    hint: "Not checked: no chain configured, or the read failed",
  },
};

/**
 * The node's health tiles, from whichever of its reads have a value: a
 * metrics URL that does not answer leaves the admin tiles, and the other
 * way round.
 */
export function nodeTiles(
  health: HealthResponse | null,
  metrics: NodeMetrics | null,
  status: StatusResponse | null,
): Tile[] {
  const list: Tile[] = [];
  if (health) {
    list.push(
      { label: "Binding", ...BINDING[health.binding] },
      {
        label: "Active staker",
        value: health.registry_active ? "Yes" : "⚠ No",
        hint: "In the on-chain active set: peers route only to a node they can slash",
      },
      {
        label: "Uptime",
        value: formatDuration(health.uptime_s),
        hint: "Since the daemon started",
      },
      {
        label: "Streams in flight",
        value: formatCount(health.in_flight_streams),
        hint: "Handler tasks holding a dispatch permit",
      },
    );
  }
  if (metrics) {
    list.push(
      {
        label: "Connections",
        value: formatCount(metrics.active_connections),
        hint: "Open QUIC connections",
      },
      {
        label: "Cache hit rate",
        value: metrics.hit_rate === null ? "—" : formatShare(metrics.hit_rate),
        hint: "Hits over hits and misses since the node started",
      },
      {
        label: "Serving",
        value: metrics.rates ? formatRate(metrics.rates.bytes) : "—",
        hint: "Cache bytes returned per second since the scrape before",
      },
      {
        label: "Unredeemed",
        value: `${formatUsdc(metrics.unredeemed_usdc)} USDC`,
        hint: "Held in accepted vouchers not yet redeemed on-chain",
      },
      {
        label: "Chain RPC",
        value: metrics.rpc_healthy ? "Healthy" : "⛔ Failing",
        hint: "Whether the node's chain RPC answers",
      },
    );
  }
  if (status) {
    list.push(
      {
        label: "DHT peers",
        value: formatCount(status.routing.total_peers),
        hint: `In ${status.routing.non_empty_buckets} routing buckets`,
      },
      {
        label: "Known stakers",
        value: formatCount(status.known_stakers),
        hint: "The staker set the DHT admits records from",
      },
    );
  }
  return list;
}

/**
 * The node's health, connections, cache and chain, as tiles. A failed
 * health read is the node section's own line, so it is not repeated here.
 */
export function NodeHealth(props: { node: NodeSection; stale: number | null }) {
  const n = props.node;
  const shown = nodeTiles(
    partValue(n.health),
    partValue(n.metrics),
    partValue(n.status),
  );
  return (
    <div className="flex flex-col gap-2">
      <Stale secs={props.stale} />
      {shown.length > 0 ? <TileGrid label="Node health" tiles={shown} /> : null}
      <PartLine what={`Metrics at ${n.metrics_url}`} part={n.metrics} />
      <PartLine what="DHT status" part={n.status} />
    </div>
  );
}
