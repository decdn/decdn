import { formatBytes, formatDuration, formatUsdc, shortUri } from "../format";
import type { RecentPullRow, Tone } from "../view";
import { Cell, Empty, Panel, Table } from "./Panel";

const TONE: Record<Tone, string> = {
  ok: "",
  muted: "text-ink-3",
  critical: "text-critical-ink",
};

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
            "Error",
          ]}
        >
          {props.rows.map((p) => (
            <tr key={p.key}>
              <td
                className={`border-b border-line py-2 pr-4 whitespace-nowrap ${TONE[p.tone]}`}
              >
                {p.outcome}
              </td>
              <Cell mono title={p.uri}>
                {shortUri(p.uri)}
              </Cell>
              <Cell mono>{p.output}</Cell>
              <Cell>{formatBytes(p.received)}</Cell>
              <Cell>{formatDuration(p.durationSecs)}</Cell>
              <Cell>{formatUsdc(p.paidMicro)} USDC</Cell>
              <Cell>{p.nodes}</Cell>
              <td className="border-b border-line py-2 pr-4 text-critical-ink">
                {p.error ?? ""}
              </td>
            </tr>
          ))}
        </Table>
      )}
    </Panel>
  );
}
