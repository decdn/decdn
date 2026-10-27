import type { ActivePull } from "../api";
import { formatBytes, formatDuration, shortHex } from "../format";
import { Cell, Empty, Panel, Table } from "./Panel";

export function ActivePulls(props: { pulls: ActivePull[] }) {
  return (
    <Panel title="Pulls in flight" note={`${props.pulls.length} now`}>
      {props.pulls.length === 0 ? (
        <Empty>No pull is running.</Empty>
      ) : (
        <Table
          head={["Blob", "Namespace", "Range", "Paid by", "Age", "Delivered"]}
        >
          {/* Listed oldest first, so an index stays with its pull until one ends. */}
          {props.pulls.map((p, i) => (
            <tr key={`${p.hash}-${i}`}>
              <Cell mono title={p.hash}>
                {shortHex(p.hash)}
              </Cell>
              <Cell>{p.namespace ?? "—"}</Cell>
              <Cell>
                {p.range
                  ? `${formatBytes(p.range[1])} at ${formatBytes(p.range[0])}`
                  : "whole"}
              </Cell>
              <Cell
                mono
                title={
                  p.payer.kind === "capability" ? p.payer.signer : undefined
                }
              >
                {p.payer.kind === "pool"
                  ? "gateway pool"
                  : shortHex(p.payer.signer)}
              </Cell>
              <Cell>{formatDuration(p.since_secs)}</Cell>
              <Cell>{formatBytes(p.delivered_bytes)}</Cell>
            </tr>
          ))}
        </Table>
      )}
    </Panel>
  );
}
