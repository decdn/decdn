import type { UnreadableView } from "../view";

/** The files in the records directory that did not read as a record, and why. */
export function UnreadableRecords(props: { view: UnreadableView }) {
  const v = props.view;
  if (v.count === 0) return null;
  return (
    <div role="alert" className="text-sm text-critical-ink">
      <p>
        ⚠ {v.count} file(s) in the directory did not read as a record. The
        dashboard only reads them; deleting one is safe.
      </p>
      <ul className="mt-1 flex flex-col gap-1">
        {v.shown.map((u) => (
          <li key={u.path} className="break-words">
            <span className="font-mono text-xs">{u.path}</span>: {u.error}
          </li>
        ))}
        {v.more > 0 ? <li>…and {v.more} more</li> : null}
      </ul>
    </div>
  );
}
