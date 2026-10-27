import type { Part, SlashesResponse } from "../api";
import { formatWhen, shortHex } from "../format";
import { offense } from "../view";
import { Cell, Empty, PartPanel, Table } from "./Panel";

/** Slashes against the node's operator, each with its appeal window. */
export function NodeSlashes(props: { part: Part<SlashesResponse> }) {
  return (
    <PartPanel
      title="Slashes"
      note={
        <>
          Appeal with <code>decdn appeal slash</code> before the window closes
        </>
      }
      part={props.part}
    >
      {(s) =>
        s.slashes.length === 0 ? (
          <Empty>No slash is recorded against this operator.</Empty>
        ) : (
          <Table
            head={[
              "Slash",
              "Offense",
              "Amount (TOKEN units)",
              "Evidence",
              "Block",
              "Appeal window closes",
            ]}
          >
            {s.slashes.map((x) => (
              <tr key={x.slash_id}>
                <Cell>{x.slash_id}</Cell>
                <Cell>{offense(x.offense_type)}</Cell>
                <Cell>{x.amount}</Cell>
                <Cell mono title={x.evidence_hash}>
                  {shortHex(x.evidence_hash)}
                </Cell>
                <Cell>{x.block_number ?? "pending"}</Cell>
                <Cell>
                  {x.appeal_window_close === null
                    ? "unknown"
                    : `about ${formatWhen(x.appeal_window_close)}`}
                </Cell>
              </tr>
            ))}
          </Table>
        )
      }
    </PartPanel>
  );
}
