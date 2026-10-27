import type { BuyerPoolsResponse, Part } from "../api";
import { poolRows } from "../view";
import { Panel, PartLine } from "./Panel";
import { Pools } from "./Pools";

const TITLE = "Pools the node pays from";

/** The node's own deposits, which pay for its cache-miss pulls. */
export function NodePools(props: {
  part: Part<BuyerPoolsResponse>;
  stale: number | null;
}) {
  const part = props.part;
  if (part.state !== "ok") {
    return (
      <Panel title={TITLE} stale={props.stale}>
        <PartLine part={part} />
      </Panel>
    );
  }
  return (
    <Pools
      title={TITLE}
      stale={props.stale}
      note="From the node's buyer store: what its cache-miss pulls spent. Unredeemed vouchers of another signer on a pool are not in it yet"
      rows={poolRows(part.value.pools)}
      delegates={[]}
      skipped={part.value.skipped}
      error={null}
    />
  );
}
