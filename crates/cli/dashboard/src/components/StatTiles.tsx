import type { Counters } from "../api";
import { formatBytes, formatCount } from "../format";

export interface Tile {
  label: string;
  value: string;
  hint: string;
}

/**
 * The tiles for `c`. A gateway with no capability that can pay answers 402
 * on loopback and 503 on a public gateway: both modes show that count under
 * one name.
 */
export function tiles(c: Counters, publicMode: boolean): Tile[] {
  const list: Tile[] = [
    {
      label: "Bytes served",
      value: formatBytes(c.bytes_served),
      hint: "Content bytes handed to responses",
    },
    {
      label: "Requests served",
      value: formatCount(c.requests_ok),
      hint: "Answered with success or a redirect",
    },
    {
      label: "Pulls in flight",
      value: formatCount(c.pulls_active),
      hint: "Pulls handed to the worker and not yet ended",
    },
    {
      label: "Cache hits",
      value: formatCount(c.cache_hits),
      hint: "Read whole out of the cache, with no pull",
    },
    {
      label: "Not found",
      value: formatCount(c.requests_not_found),
      hint: "A path, entry or manifest that does not exist",
    },
    {
      label: "Refused",
      value: formatCount(c.requests_refused),
      hint: "Refused before any pull",
    },
    {
      label: "No capability could pay",
      value: formatCount(
        publicMode ? c.pull_failures_funding : c.requests_payment_required,
      ),
      hint: publicMode
        ? "Answered 503: no registered capability could pay"
        : "Answered 402: no registered capability could pay",
    },
    {
      label: "Upstream failed",
      value: formatCount(c.pull_failures_other),
      hint: "Answered 502",
    },
  ];
  if (publicMode) {
    list.push({
      label: "Pointer updates",
      value: formatCount(c.pointer_updates),
      hint: "Stored through PUT /_pointer",
    });
  }
  return list;
}

/** The counters since the gateway started, as a row of tiles. */
export function StatTiles(props: { counters: Counters; publicMode: boolean }) {
  return (
    <TileGrid
      label="Gateway counters since start"
      tiles={tiles(props.counters, props.publicMode)}
    />
  );
}

/** `tiles` as a grid, each with its hint on hover. */
export function TileGrid(props: { label: string; tiles: Tile[] }) {
  return (
    <section aria-label={props.label}>
      <dl className="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-5">
        {props.tiles.map((t) => (
          <div
            key={t.label}
            className="rounded-lg border border-line bg-surface p-3"
            title={t.hint}
          >
            <dt className="text-sm text-ink-2">{t.label}</dt>
            <dd className="mt-1 text-2xl font-semibold">{t.value}</dd>
          </div>
        ))}
      </dl>
    </section>
  );
}
