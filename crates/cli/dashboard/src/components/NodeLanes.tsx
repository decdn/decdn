import type { LanesResponse, Part } from "../api";
import { formatDuration, formatUsdc, shortHex } from "../format";
import { Cell, Empty, PartPanel, Table } from "./Panel";

/** The lanes the node serves against, as it accrued them. */
export function NodeLanes(props: {
  part: Part<LanesResponse>;
  stale: number | null;
}) {
  return (
    <PartPanel
      title="Lanes the node is paid on"
      note="Redeems once the lanes owe more than their gas"
      part={props.part}
      stale={props.stale}
    >
      {(l) =>
        l.lanes.length === 0 ? (
          <Empty>No lane is open against this node.</Empty>
        ) : (
          <Table
            head={[
              "Pool",
              "Pool owner",
              "Signer",
              "Owed",
              "Unredeemed",
              "Last voucher",
            ]}
          >
            {l.lanes.map((lane) => (
              <tr key={`${lane.pool_id}-${lane.voucher_signer}`}>
                <Cell mono title={lane.pool_id}>
                  {shortHex(lane.pool_id)}
                </Cell>
                <Cell mono title={lane.counterparty ?? undefined}>
                  {lane.counterparty ? shortHex(lane.counterparty) : "unknown"}
                </Cell>
                <Cell mono title={lane.voucher_signer}>
                  {shortHex(lane.voucher_signer)}
                </Cell>
                <Cell>{formatUsdc(lane.outstanding_micro_usdc)} USDC</Cell>
                <Cell>{formatUsdc(lane.unredeemed_micro_usdc)} USDC</Cell>
                <Cell>
                  {lane.seconds_since_last_voucher === null
                    ? "not since start"
                    : `${formatDuration(lane.seconds_since_last_voucher)} ago`}
                </Cell>
              </tr>
            ))}
          </Table>
        )
      }
    </PartPanel>
  );
}
