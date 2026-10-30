import { formatBytes, formatDuration } from "../format";
import type { BucketRow } from "../view";
import { Gauge, Panel } from "./Panel";

export function Allowance(props: { rows: BucketRow[] }) {
  const resets = props.rows[0]?.resetsIn;
  return (
    <Panel
      title="Daily allowance"
      note={
        resets === undefined
          ? undefined
          : `Resets in ${formatDuration(resets)} (UTC day)`
      }
    >
      <ul className="flex flex-col gap-3">
        {props.rows.map((b) => (
          <li
            key={b.label}
            className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between"
          >
            <div className="min-w-0 sm:flex-1">
              <p className="text-sm font-medium">{b.label}</p>
              <p className="text-sm text-ink-2 tabular-nums">
                {formatBytes(b.usedBytes)} delivered
                {b.perDay === null
                  ? " · no bound"
                  : ` of ${formatBytes(b.perDay)}`}
              </p>
            </div>
            {b.used === null ? null : (
              <div className="w-full sm:w-64 sm:shrink-0">
                <Gauge
                  used={b.used}
                  severity={b.severity}
                  label={`${b.label} allowance`}
                />
              </div>
            )}
          </li>
        ))}
      </ul>
    </Panel>
  );
}
