import type { EarningsResponse, Part } from "../api";
import { formatBytes, formatUsdc, formatWhen, shortHex } from "../format";
import { earningsRows } from "../view";
import { Cell, PartPanel, Table } from "./Panel";

function Pending(props: { e: EarningsResponse }) {
  const p = props.e.pending;
  if (!p) return null;
  return (
    <p className="text-sm text-ink-2 tabular-nums">
      Pending: up to {formatUsdc(p.micro_usdc)} USDC on {p.lanes}{" "}
      {p.lanes === 1 ? "lane" : "lanes"}
      {p.next_sweep_at_unix === null
        ? ""
        : ` · next sweep ${formatWhen(p.next_sweep_at_unix)}`}
      {p.stranded_lanes > 0 ? (
        <span className="text-critical-ink">
          {" "}
          · ⚠ {formatUsdc(p.stranded_micro_usdc)} USDC on {p.stranded_lanes}{" "}
          {p.stranded_lanes === 1 ? "lane" : "lanes"} no sweep redeems
        </span>
      ) : null}
    </p>
  );
}

function Last(props: { e: EarningsResponse }) {
  const l = props.e.last_settlement;
  if (!l) {
    return <p className="text-sm text-ink-3">No settlement has landed yet.</p>;
  }
  return (
    <p className="text-sm text-ink-2 tabular-nums">
      Last settlement: {formatUsdc(l.net_micro_usdc)} USDC at{" "}
      {formatWhen(l.at_unix)}, block {l.block_number}, tx{" "}
      <span className="font-mono text-xs" title={l.tx_hash}>
        {shortHex(l.tx_hash)}
      </span>
    </p>
  );
}

/** What the node was paid, per period, and what it is still owed. */
export function NodeEarnings(props: { part: Part<EarningsResponse> }) {
  return (
    <PartPanel
      title="Earnings"
      note="Net of the fee split, from the node's settlement ledger"
      part={props.part}
    >
      {(e) => (
        <div className="flex flex-col gap-3">
          <Table
            head={["Period", "Net", "Gross", "Bytes settled", "Settlements"]}
          >
            {earningsRows(e).map((r) => (
              <tr key={r.label}>
                <Cell>{r.label}</Cell>
                <Cell>{formatUsdc(r.period.net_micro_usdc)} USDC</Cell>
                <Cell>{formatUsdc(r.period.gross_micro_usdc)} USDC</Cell>
                <Cell>{formatBytes(r.period.bytes_settled)}</Cell>
                <Cell>{r.period.settlements}</Cell>
              </tr>
            ))}
          </Table>
          {e.ledger_since_unix === null ? null : (
            <p className="text-sm text-ink-3">
              The ledger holds settlements from{" "}
              {formatWhen(e.ledger_since_unix)} on.
            </p>
          )}
          <Pending e={e} />
          <Last e={e} />
        </div>
      )}
    </PartPanel>
  );
}
