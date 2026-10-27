import { formatBytes, formatUsdc, formatWhen, shortHex } from "../format";
import type { CapabilityRow } from "../view";
import { Cell, Empty, Gauge, Panel, Table } from "./Panel";

export function Capabilities(props: { rows: CapabilityRow[] }) {
  return (
    <Panel title="Capabilities" note="Held in memory; a restart drops them">
      {props.rows.length === 0 ? (
        <Empty>
          No capability is registered. Pages and the browser extension register
          one with <code>PUT /_capability</code>.
        </Empty>
      ) : (
        <Table
          head={[
            "Key",
            "Pool",
            "Scope",
            "Registered by",
            "Spent of cap",
            "Bytes",
            "Expires",
          ]}
        >
          {props.rows.map((c) => (
            <tr key={`${c.signer}-${c.pool}`}>
              <Cell mono title={c.signer}>
                {shortHex(c.signer)}
              </Cell>
              <Cell mono title={c.pool}>
                {shortHex(c.pool)}
              </Cell>
              <Cell>{c.scope}</Cell>
              <Cell>{c.origin ?? "token holder"}</Cell>
              <td className="border-b border-line py-2 pr-4">
                {c.used === null || c.spentMicro === null ? (
                  <span className="text-ink-3">
                    {formatUsdc(c.cap)} cap · spend unknown
                  </span>
                ) : (
                  <div className="flex flex-col gap-1">
                    <span>
                      {formatUsdc(c.spentMicro)} of {formatUsdc(c.cap)} USDC
                    </span>
                    {c.inFlight > 0 ? (
                      <span className="text-xs text-ink-3">
                        Live: {c.inFlight} {c.inFlight === 1 ? "pull" : "pulls"}{" "}
                        in flight
                      </span>
                    ) : null}
                    <Gauge
                      used={c.used}
                      severity={c.severity}
                      label={`Spend of ${shortHex(c.signer)}`}
                    />
                  </div>
                )}
                {c.exhausted ? (
                  <span className="text-xs text-critical-ink">
                    ⛔ Ran out of its cap
                  </span>
                ) : null}
              </td>
              <Cell>{c.bytes === null ? "—" : formatBytes(c.bytes)}</Cell>
              <Cell>{formatWhen(c.expiry)}</Cell>
            </tr>
          ))}
        </Table>
      )}
    </Panel>
  );
}
