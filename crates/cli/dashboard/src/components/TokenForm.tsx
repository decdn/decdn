import { useState, type FormEvent } from "react";

/** Asks for the gateway token a loopback gateway's `/_status` needs. */
export function TokenForm(props: {
  rejected: boolean;
  onToken: (token: string) => void;
}) {
  const [value, setValue] = useState("");
  const submit = (e: FormEvent) => {
    e.preventDefault();
    const token = value.trim();
    if (token) props.onToken(token);
  };
  return (
    <form
      onSubmit={submit}
      className="flex max-w-xl flex-col gap-3 rounded-lg border border-line bg-surface p-4"
    >
      <label htmlFor="token" className="font-semibold">
        Gateway token
      </label>
      <p className="text-sm text-ink-2">
        {props.rejected ? "That token was not accepted. " : null}
        Open the <code>dashboard:</code> link <code>decdn gateway</code> printed
        when it started, or paste the token it printed (also in{" "}
        <code>~/.decdn/gateway/token</code>).
      </p>
      <input
        id="token"
        type="password"
        autoComplete="off"
        spellCheck={false}
        value={value}
        onChange={(e) => setValue(e.target.value)}
        className="rounded border border-line bg-page px-3 py-2 font-mono text-sm"
      />
      <button
        type="submit"
        className="self-start rounded bg-accent px-3 py-1.5 text-sm font-medium text-white"
      >
        Connect
      </button>
    </form>
  );
}
