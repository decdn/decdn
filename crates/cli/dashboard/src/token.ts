// The gateway token's hand-off: `decdn gateway` prints a link whose
// fragment carries the token. The fragment never reaches the server or a
// log; the page moves it to session storage and strips it from the address.
// Where the browser blocks storage, the page keeps the token in memory until
// it reloads, and says so.

export const TOKEN_KEY = "decdn-gateway-token";

interface Place {
  hash: string;
  pathname: string;
  search: string;
}

interface History {
  replaceState(data: unknown, unused: string, url: string): void;
}

/** The token to use, and whether storage keeps it across a reload. */
export interface Adopted {
  token: string | null;
  kept: boolean;
}

/**
 * The window's session storage, or null where reading it throws: a browser
 * that blocks site data throws on the property read itself.
 */
export function session(win: {
  readonly sessionStorage: Storage;
}): Storage | null {
  try {
    return win.sessionStorage;
  } catch {
    return null;
  }
}

/** The token a `#token=` fragment names, or null. */
function fragmentToken(hash: string): string | null {
  if (!hash.startsWith("#")) return null;
  const token = new URLSearchParams(hash.slice(1)).get("token");
  return token === null ? null : token.trim();
}

/**
 * The token to use: the fragment's, which is then stored and stripped from
 * the address, or else the stored one.
 */
export function adoptToken(
  place: Place,
  history: History,
  storage: Storage | null,
): Adopted {
  const given = fragmentToken(place.hash);
  if (given === null) return { token: loadToken(storage), kept: true };
  history.replaceState(null, "", place.pathname + place.search);
  if (given === "") return { token: loadToken(storage), kept: true };
  return { token: given, kept: saveToken(storage, given) };
}

/** The stored token, or null when there is none or storage is blocked. */
export function loadToken(storage: Storage | null): string | null {
  try {
    return storage?.getItem(TOKEN_KEY) ?? null;
  } catch {
    return null;
  }
}

/** Store `token`. False when storage is blocked: the caller holds it. */
export function saveToken(storage: Storage | null, token: string): boolean {
  try {
    if (!storage) return false;
    storage.setItem(TOKEN_KEY, token.trim());
    return true;
  } catch {
    return false;
  }
}

/** Forget the stored token. */
export function clearToken(storage: Storage | null): void {
  try {
    storage?.removeItem(TOKEN_KEY);
  } catch {
    // Nothing stored that could be read back.
  }
}
