import type { ReactNode } from "react";
import type { Severity } from "../view";

/** A titled card. */
export function Panel(props: {
  title: string;
  note?: ReactNode;
  children: ReactNode;
}) {
  return (
    <section className="rounded-lg border border-line bg-surface p-4">
      <header className="mb-3 flex flex-wrap items-baseline justify-between gap-2">
        <h2 className="text-base font-semibold">{props.title}</h2>
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
