import { describe, expect, it } from "vitest";
import {
  adoptToken,
  clearToken,
  loadToken,
  saveToken,
  session,
  TOKEN_KEY,
} from "./token";

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

describe("session", () => {
  it("is the window's session storage", () => {
    const storage = memory();
    expect(session({ sessionStorage: storage })).toBe(storage);
  });

  it("is null where reading it throws, as with site data blocked", () => {
    const blocked = {
      get sessionStorage(): Storage {
        throw new DOMException("denied", "SecurityError");
      },
    };
    expect(session(blocked)).toBeNull();
  });
});

describe("adoptToken", () => {
  it("takes the fragment's token, stores it, and strips the fragment", () => {
    const storage = memory();
    const p = place("#token=abc123");
    expect(adoptToken(p.location, p.history, storage)).toEqual({
      token: "abc123",
      kept: true,
    });
    expect(storage.getItem(TOKEN_KEY)).toBe("abc123");
    expect(p.replaced).toEqual(["/?x=1"]);
  });

  it("falls back to the stored token when the fragment has none", () => {
    const storage = memory();
    storage.setItem(TOKEN_KEY, "stored");
    const p = place("");
    expect(adoptToken(p.location, p.history, storage)).toEqual({
      token: "stored",
      kept: true,
    });
    expect(p.replaced).toEqual([]);
  });

  it("strips a fragment that names no token, and keeps the stored one", () => {
    const storage = memory();
    storage.setItem(TOKEN_KEY, "stored");
    const p = place("#token=");
    expect(adoptToken(p.location, p.history, storage).token).toBe("stored");
    expect(p.replaced).toEqual(["/?x=1"]);
  });

  it("hands over the fragment's token, and says it is not kept, when storage throws", () => {
    const p = place("#token=abc");
    expect(adoptToken(p.location, p.history, memory(true))).toEqual({
      token: "abc",
      kept: false,
    });
    expect(p.replaced).toEqual(["/?x=1"]);
  });

  it("hands over the fragment's token, and says it is not kept, with no storage at all", () => {
    const p = place("#token=abc");
    expect(adoptToken(p.location, p.history, null)).toEqual({
      token: "abc",
      kept: false,
    });
    expect(p.replaced).toEqual(["/?x=1"]);
  });
});

describe("saveToken, loadToken and clearToken", () => {
  it("round-trip a trimmed token", () => {
    const storage = memory();
    expect(saveToken(storage, "  t0k  ")).toBe(true);
    expect(loadToken(storage)).toBe("t0k");
    clearToken(storage);
    expect(loadToken(storage)).toBeNull();
  });

  it("never throw on blocked or missing storage, and say nothing was kept", () => {
    for (const storage of [memory(true), null]) {
      expect(loadToken(storage)).toBeNull();
      expect(saveToken(storage, "x")).toBe(false);
      expect(() => clearToken(storage)).not.toThrow();
    }
  });
});
