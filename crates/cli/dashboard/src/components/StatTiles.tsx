import type { Counters } from "../api";
import { formatBytes, formatCount } from "../format";

interface Tile {
  label: string;
  value: string;
  hint: string;
}

function tiles(c: Counters, publicMode: boolean): Tile[] {
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
      label: "Pool could not pay",
      value: formatCount(c.pull_failures_funding),
      hint: "Answered 503",
    },
    {
      label: "Upstream failed",
      value: formatCount(c.pull_failures_other),
      hint: "Answered 502",
    },
  ];
  if (publicMode) {
    list.push(
      {
        label: "Allowance spent",
        value: formatCount(c.requests_allowance_exhausted),
        hint: "Answered 402",
      },
      {
        label: "Pointer updates",
        value: formatCount(c.pointer_updates),
        hint: "Stored through PUT /_pointer",
      },
    );
  }
  return list;
}

/** The counters since the gateway started, as a row of tiles. */
export function StatTiles(props: { counters: Counters; publicMode: boolean }) {
  return (
    <section aria-label="Counters since start">
      <dl className="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-5">
        {tiles(props.counters, props.publicMode).map((t) => (
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
