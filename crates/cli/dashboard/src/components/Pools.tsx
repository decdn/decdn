import type { DelegateLanes, Lane } from "../api";
import { formatBytes, formatUsdc, shortHex } from "../format";
import type { PoolRow } from "../view";
import { Cell, Empty, Gauge, Panel, Table } from "./Panel";

export function Lanes(props: { lanes: Lane[] }) {
  return (
    <Table head={["Signer", "Node", "Paid", "Bytes"]}>
      {props.lanes.map((l) => (
        <tr key={`${l.voucher_signer}-${l.provider}`}>
          <Cell mono title={l.voucher_signer}>
            {shortHex(l.voucher_signer)}
          </Cell>
          <Cell mono title={l.provider}>
            {shortHex(l.provider)}
          </Cell>
          <Cell>{formatUsdc(l.last_amount_micro_usdc)} USDC</Cell>
          <Cell>{formatBytes(l.last_bytes_delivered)}</Cell>
        </tr>
      ))}
    </Table>
  );
}

const GATEWAY_NOTE =
  "From the buyer store. Spend counts what nodes redeemed and this gateway signed; another signer's unredeemed vouchers and the pulls in flight are not in it yet";

export function Pools(props: {
  title?: string;
  note?: string;
  stale?: number | null;
  rows: PoolRow[];
  delegates: DelegateLanes[];
  skipped: string[];
  error: string | null;
}) {
  const delegates = props.delegates.filter((d) => d.lanes.length > 0);
  return (
    <Panel
      title={props.title ?? "Pools and lanes"}
      note={props.note ?? GATEWAY_NOTE}
      stale={props.stale}
    >
      {props.error ? (
        <p role="alert" className="mb-3 text-sm text-critical-ink">
          ⛔ The store could not be read: {props.error}
        </p>
      ) : null}
      {props.skipped.length > 0 ? (
        <p role="alert" className="mb-3 text-sm text-critical-ink">
          ⚠ {props.skipped.length} pool record(s) would not decode; their
          deposits stay escrowed but untracked:{" "}
          {props.skipped.map(shortHex).join(", ")}
        </p>
      ) : null}
      {props.rows.length === 0 && delegates.length === 0 && !props.error ? (
        <Empty>No pool or lane is recorded yet.</Empty>
      ) : null}
      <div className="flex flex-col gap-4">
        {props.rows.map((p) => (
          <article key={p.id} className="flex flex-col gap-2">
            <div className="flex flex-col gap-3 sm:flex-row sm:items-end sm:justify-between">
              <div className="min-w-0 sm:flex-1">
                <h4 className="font-mono text-sm" title={p.id}>
                  Pool {shortHex(p.id)}
                </h4>
                <p className="text-sm text-ink-2">
                  Up to {formatUsdc(p.headroom)} USDC left of{" "}
                  {formatUsdc(p.deposit)} · spent {formatUsdc(p.spent)} or more
                </p>
              </div>
              <div className="w-full sm:w-64 sm:shrink-0">
                <Gauge
                  used={p.used}
                  severity={p.severity}
                  label={`Spend of pool ${shortHex(p.id)}`}
                />
              </div>
            </div>
            {p.lanes.length > 0 ? <Lanes lanes={p.lanes} /> : null}
          </article>
        ))}
        {delegates.map((d) => (
          <article
            key={`${d.signer}-${d.pool_id}`}
            className="flex flex-col gap-2"
          >
            <h4 className="text-sm">
              Lanes of key{" "}
              <span className="font-mono">{shortHex(d.signer)}</span> on pool{" "}
              <span className="font-mono">{shortHex(d.pool_id)}</span>
            </h4>
            <Lanes lanes={d.lanes} />
          </article>
        ))}
      </div>
    </Panel>
  );
}
