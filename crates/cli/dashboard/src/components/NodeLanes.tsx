import type { LanesResponse, Part } from "../api";
import { formatDuration, formatUsdc, shortHex } from "../format";
import { partValue } from "../view";
import { Cell, Empty, PartPanel, Table } from "./Panel";

/** The lanes the node serves against, as it accrued them. */
export function NodeLanes(props: {
  part: Part<LanesResponse>;
  stale: number | null;
}) {
  const read = partValue(props.part);
  return (
    <PartPanel
      title="Lanes the node is paid on"
      note={
        read
          ? `Redeems once the lanes owe ${formatUsdc(read.redeem_threshold_micro_usdc)} USDC`
          : null
      }
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
              "Over threshold",
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
                <Cell>{lane.settlement_eligible ? "Yes" : "No"}</Cell>
              </tr>
            ))}
          </Table>
        )
      }
    </PartPanel>
  );
}
