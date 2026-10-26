import type { Pointer } from "../api";
import { shortHex } from "../format";
import { Cell, Empty, Panel, Table } from "./Panel";

export function Pointers(props: { pointers: Pointer[] }) {
  return (
    <Panel title="Namespace pointers" note={`${props.pointers.length} held`}>
      {props.pointers.length === 0 ? (
        <Empty>
          No pointer is stored. <code>decdn publish point</code> sends one.
        </Empty>
      ) : (
        <Table head={["Namespace", "Bundle", "Seq", "Signed by"]}>
          {props.pointers.map((p) => (
            <tr key={p.namespace}>
              <Cell>{p.namespace}</Cell>
              <Cell mono title={p.root}>
                {shortHex(p.root)}
              </Cell>
              <Cell>{p.seq}</Cell>
              <Cell mono title={p.signer}>
                {shortHex(p.signer)}
              </Cell>
            </tr>
          ))}
        </Table>
      )}
    </Panel>
  );
}
