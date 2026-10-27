import { formatBytes, formatDuration, formatUsdc, shortUri } from "../format";
import type { LivePullRow } from "../view";
import { Empty, Panel, Progress } from "./Panel";

function received(p: LivePullRow): string {
  if (p.expected === null) return `${formatBytes(p.received)} received`;
  return `${formatBytes(p.received)} of ${formatBytes(p.expected)}`;
}

/** The `decdn get` runs on this host that have not ended. */
export function LivePulls(props: { rows: LivePullRow[] }) {
  return (
    <Panel title="decdn get, running" note={`${props.rows.length} now`}>
      {props.rows.length === 0 ? (
        <Empty>
          No <code>decdn get</code> is running.
        </Empty>
      ) : (
        <ul className="flex flex-col gap-4">
          {props.rows.map((p) => (
            <li
              key={p.key}
              className="flex flex-wrap items-center justify-between gap-3"
            >
              <div className="min-w-0">
                <p className="font-mono text-xs break-all" title={p.uri}>
                  {shortUri(p.uri)}
                </p>
                <p className="text-sm text-ink-2 tabular-nums">
                  {p.kind} to <span className="font-mono">{p.output}</span>
                  {p.files ? ` · ${p.files}` : ""} · paid{" "}
                  {formatUsdc(p.paidMicro)} USDC to {p.nodes}{" "}
                  {p.nodes === 1 ? "node" : "nodes"} · for{" "}
                  {formatDuration(p.ageSecs)}
                </p>
                {p.stale ? (
                  <p className="text-sm text-critical-ink">
                    ⚠ Stopped without a summary: the run exited before its last
                    write
                  </p>
                ) : null}
              </div>
              <div className="flex w-full flex-col gap-1 sm:w-64">
                {p.done === null ? null : (
                  <Progress done={p.done} label={`Progress of ${p.uri}`} />
                )}
                <span className="text-xs text-ink-2 tabular-nums">
                  {received(p)}
                </span>
              </div>
            </li>
          ))}
        </ul>
      )}
    </Panel>
  );
}
