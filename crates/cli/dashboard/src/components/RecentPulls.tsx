import {
  formatBytes,
  formatDuration,
  formatUsdc,
  shortPath,
  shortUri,
} from "../format";
import type { RecentPullRow, Tone } from "../view";
import { Cell, Empty, Panel, Table } from "./Panel";

const TONE: Record<Tone, string> = {
  ok: "",
  muted: "text-ink-3",
  critical: "text-critical-ink",
};

/** The table's column count, which an error line spans. */
const COLUMNS = 7;

/** The `decdn get` runs on this host that ended, newest first. */
export function RecentPulls(props: { rows: RecentPullRow[] }) {
  return (
    <Panel title="decdn get, ended">
      {props.rows.length === 0 ? (
        <Empty>
          No <code>decdn get</code> has ended on this host yet.
        </Empty>
      ) : (
        <Table
          head={[
            "Outcome",
            "URI",
            "Written to",
            "Received",
            "Took",
            "Paid",
            "Nodes",
          ]}
        >
          {props.rows.flatMap((p) => [
            <tr key={p.key}>
              <td
                className={`border-b border-line py-2 pr-4 whitespace-nowrap ${TONE[p.tone]}`}
              >
                {p.outcome}
              </td>
              <Cell mono title={p.uri}>
                {shortUri(p.uri)}
              </Cell>
              <Cell mono title={p.output}>
                {shortPath(p.output)}
              </Cell>
              <Cell>{formatBytes(p.received)}</Cell>
              <Cell>{formatDuration(p.durationSecs)}</Cell>
              <Cell>{formatUsdc(p.paidMicro)} USDC</Cell>
              <Cell>{p.nodes}</Cell>
            </tr>,
            // Why a run failed reads under it, across the row, so a long
            // error neither widens the table nor squeezes into one column.
            p.error ? (
              <tr key={`${p.key}-error`}>
                <td
                  colSpan={COLUMNS}
                  className="border-b border-line pb-2 text-sm break-words text-critical-ink"
                >
                  {p.error}
                </td>
              </tr>
            ) : null,
          ])}
        </Table>
      )}
    </Panel>
  );
}
