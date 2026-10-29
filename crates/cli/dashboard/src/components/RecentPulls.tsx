import {
  formatBytes,
  formatDuration,
  formatUsdc,
  shortPath,
  shortUri,
} from "../format";
import type { CountPart, RecentPullRow, Tone } from "../view";
import { Cell, Empty, Panel, Table } from "./Panel";

const TONE: Record<Tone, string> = {
  ok: "",
  muted: "text-ink-3",
  critical: "text-critical-ink",
};

/** The table's column count, which a counts or error line spans. */
const COLUMNS = 7;

/** What else the run's bytes came from: resumed and reused, when any. */
function Besides(props: { row: RecentPullRow }) {
  const p = props.row;
  const parts = [
    p.resumed > 0 ? `${formatBytes(p.resumed)} resumed` : null,
    p.reused > 0 ? `${formatBytes(p.reused)} reused` : null,
  ].filter((part) => part !== null);
  if (parts.length === 0) return null;
  return (
    <span className="block text-xs text-ink-3">{`+ ${parts.join(" · ")}`}</span>
  );
}

/** A bundle or entry run's counts, the failed and not-run ones in the critical tone. */
function Counts(props: { parts: CountPart[] }) {
  return props.parts.map((part, i) => (
    <span key={part.text}>
      {i > 0 ? ", " : ""}
      <span className={part.critical ? "text-critical-ink" : undefined}>
        {part.text}
      </span>
    </span>
  ));
}

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
            "Downloaded",
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
              <Cell>
                {formatBytes(p.downloaded)}
                <Besides row={p} />
              </Cell>
              <Cell>{formatDuration(p.durationSecs)}</Cell>
              <Cell>{formatUsdc(p.paidMicro)} USDC</Cell>
              <Cell>{p.nodes}</Cell>
            </tr>,
            // A bundle or entry run's counts read under it, as its summary
            // line does, so a run that skipped every file does not read as
            // one that did nothing.
            p.counts ? (
              <tr key={`${p.key}-counts`}>
                <td
                  colSpan={COLUMNS}
                  className="border-b border-line pb-2 text-sm text-ink-2 tabular-nums"
                >
                  <Counts parts={p.counts} />
                </td>
              </tr>
            ) : null,
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
