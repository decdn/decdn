import { describe, expect, it } from "vitest";
import { adoptToken, loadToken, saveToken, TOKEN_KEY } from "./token";

/** A Storage stand-in, optionally one that throws on every call. */
function memory(throws = false): Storage {
  const items = new Map<string, string>();
  const guard = () => {
    if (throws) throw new Error("storage is blocked");
  };
  return {
    get length() {
      return items.size;
    },
    clear: () => items.clear(),
    key: (i) => [...items.keys()][i] ?? null,
    getItem: (k) => (guard(), items.get(k) ?? null),
    setItem: (k, v) => (guard(), void items.set(k, v)),
    removeItem: (k) => (guard(), void items.delete(k)),
  };
}

function place(hash: string) {
  const replaced: string[] = [];
  return {
    location: { hash, pathname: "/", search: "?x=1" },
    history: {
      replaceState: (_: unknown, __: string, url: string) =>
        void replaced.push(url),
    },
    replaced,
  };
}

describe("adoptToken", () => {
  it("takes the fragment's token, stores it, and strips the fragment", () => {
    const storage = memory();
    const p = place("#token=abc123");
    expect(adoptToken(p.location, p.history, storage)).toBe("abc123");
    expect(storage.getItem(TOKEN_KEY)).toBe("abc123");
    expect(p.replaced).toEqual(["/?x=1"]);
  });

  it("falls back to the stored token when the fragment has none", () => {
    const storage = memory();
    storage.setItem(TOKEN_KEY, "stored");
    const p = place("");
    expect(adoptToken(p.location, p.history, storage)).toBe("stored");
    expect(p.replaced).toEqual([]);
  });

  it("strips a fragment that names no token, and keeps the stored one", () => {
    const storage = memory();
    storage.setItem(TOKEN_KEY, "stored");
    const p = place("#token=");
    expect(adoptToken(p.location, p.history, storage)).toBe("stored");
    expect(p.replaced).toEqual(["/?x=1"]);
  });

  it("still hands over the fragment's token when storage throws", () => {
    const p = place("#token=abc");
    expect(adoptToken(p.location, p.history, memory(true))).toBe("abc");
    expect(p.replaced).toEqual(["/?x=1"]);
  });
});

describe("saveToken and loadToken", () => {
  it("round-trip a trimmed token, and read nothing from blocked storage", () => {
    const storage = memory();
    saveToken(storage, "  t0k  ");
    expect(loadToken(storage)).toBe("t0k");
    expect(loadToken(memory(true))).toBeNull();
    expect(() => saveToken(memory(true), "x")).not.toThrow();
  });
});
