import type { ReactNode } from "react";
import type { Part } from "../api";
import { formatDuration } from "../format";
import { partNote, type Severity } from "../view";

/** Says a source was last read `secs` ago, when that is long; else nothing. */
export function Stale(props: { secs: number | null | undefined }) {
  if (props.secs === null || props.secs === undefined) return null;
  return (
    <span role="status" className="text-xs font-medium text-critical-ink">
      ⚠ last read {formatDuration(props.secs)} ago
    </span>
  );
}

/** One source's panels under its name: pulls, the gateway, the node. */
export function Group(props: {
  title: string;
  note?: ReactNode;
  stale?: number | null;
  children: ReactNode;
}) {
  return (
    <section aria-label={props.title} className="flex flex-col gap-4">
      <header className="flex flex-wrap items-baseline justify-between gap-2 border-b border-line pb-1">
        <h2 className="flex items-baseline gap-2 text-lg font-semibold">
          {props.title} <Stale secs={props.stale} />
        </h2>
        {props.note ? <p className="text-sm text-ink-2">{props.note}</p> : null}
      </header>
      {props.children}
    </section>
  );
}

/** A titled card. */
export function Panel(props: {
  title: string;
  note?: ReactNode;
  stale?: number | null;
  children: ReactNode;
}) {
  return (
    <section className="rounded-lg border border-line bg-surface p-4">
      <header className="mb-3 flex flex-wrap items-baseline justify-between gap-2">
        <h3 className="flex items-baseline gap-2 text-base font-semibold">
          {props.title} <Stale secs={props.stale} />
        </h3>
        {props.note ? <p className="text-sm text-ink-3">{props.note}</p> : null}
      </header>
      {props.children}
    </section>
  );
}

/** A table that scrolls inside its card, never the page. */
export function Table(props: { head: string[]; children: ReactNode }) {
  return (
    <div className="-mx-4 overflow-x-auto px-4">
      <table className="w-full text-left text-sm tabular-nums">
        <thead className="text-ink-3">
          <tr>
            {props.head.map((h) => (
              <th
                key={h}
                scope="col"
                className="border-b border-line py-2 pr-4 font-medium"
              >
                {h}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>{props.children}</tbody>
      </table>
    </div>
  );
}

export function Cell(props: {
  children: ReactNode;
  title?: string;
  mono?: boolean;
}) {
  return (
    <td
      title={props.title}
      className={`border-b border-line py-2 pr-4 whitespace-nowrap ${props.mono ? "font-mono text-xs" : ""}`}
    >
      {props.children}
    </td>
  );
}

export function Empty(props: { children: ReactNode }) {
  return <p className="text-sm text-ink-3">{props.children}</p>;
}

const SEVERITY_LABEL: Record<Severity, string | null> = {
  ok: null,
  warning: "⚠ Low",
  critical: "⛔ Nearly out",
};

/** A share of a limit, as a meter and the words that say the same. */
export function Gauge(props: {
  used: number;
  severity: Severity;
  label: string;
}) {
  const flag = SEVERITY_LABEL[props.severity];
  return (
    <div className="flex min-w-40 flex-col gap-1">
      <meter
        className="gauge"
        min={0}
        max={1}
        value={props.used}
        data-severity={props.severity}
        aria-label={props.label}
      />
      <span className="text-xs text-ink-2">
        {Math.round(props.used * 100)}% used{flag ? ` · ${flag}` : ""}
      </span>
    </div>
  );
}

/** How far a run is, as a meter; the text beside it says the same. */
export function Progress(props: { done: number; label: string }) {
  return (
    <meter
      className="gauge"
      min={0}
      max={1}
      value={props.done}
      aria-label={props.label}
    />
  );
}

/** What a node panel shows for a read it has no value for, if `what`. */
export function PartLine(props: { part: Part<unknown>; what?: string }) {
  const note = partNote(props.part);
  if (!note) return null;
  const what = props.what ? `${props.what}: ` : "";
  return note.alert ? (
    <p role="alert" className="text-sm text-critical-ink">
      ⛔ {what}
      {note.text}
    </p>
  ) : (
    <Empty>
      {what}
      {note.text}
    </Empty>
  );
}

/** A panel over one node read: its body once the read has a value. */
export function PartPanel<T>(props: {
  title: string;
  note?: ReactNode;
  stale?: number | null;
  part: Part<T>;
  children: (value: T) => ReactNode;
}) {
  const part = props.part;
  return (
    <Panel
      title={props.title}
      note={part.state === "ok" ? props.note : null}
      stale={props.stale}
    >
      {part.state === "ok" ? (
        props.children(part.value)
      ) : (
        <PartLine part={part} />
      )}
    </Panel>
  );
}
