import type { Status } from "../api";
import { formatWhen } from "../format";

export function Header(props: {
  status: Status | null;
  live: boolean;
  onForget?: () => void;
}) {
  const s = props.status;
  return (
    <header className="flex flex-wrap items-end justify-between gap-3">
      <div>
        <h1 className="text-xl font-semibold">decdn dashboard</h1>
        {s ? (
          <p className="mt-1 text-sm text-ink-2">
            v{s.version} · up since {formatWhen(s.started_unix)}
          </p>
        ) : null}
      </div>
      <div className="flex items-center gap-3 text-sm text-ink-2">
        <span>
          <span
            aria-hidden="true"
            className={props.live ? "text-good" : "text-ink-3"}
          >
            ●
          </span>{" "}
          {props.live ? "Live, every 2 s" : "Not connected"}
        </span>
        {props.onForget ? (
          <button
            type="button"
            onClick={props.onForget}
            className="rounded border border-line px-2 py-1 hover:bg-page"
          >
            Forget token
          </button>
        ) : null}
      </div>
    </header>
  );
}
