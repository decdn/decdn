// The gateway token's hand-off: `decdn gateway` prints a link whose
// fragment carries the token. The fragment never reaches the server or a
// log; the page moves it to session storage and strips it from the address.

export const TOKEN_KEY = "decdn-gateway-token";

interface Place {
  hash: string;
  pathname: string;
  search: string;
}

interface History {
  replaceState(data: unknown, unused: string, url: string): void;
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
  storage: Storage,
): string | null {
  const given = fragmentToken(place.hash);
  if (given === null) return loadToken(storage);
  history.replaceState(null, "", place.pathname + place.search);
  if (given === "") return loadToken(storage);
  saveToken(storage, given);
  return given;
}

/** The stored token, or null when there is none or storage is blocked. */
export function loadToken(storage: Storage): string | null {
  try {
    return storage.getItem(TOKEN_KEY);
  } catch {
    return null;
  }
}

/** Store `token`; a blocked storage keeps it for this page view only. */
export function saveToken(storage: Storage, token: string): void {
  try {
    storage.setItem(TOKEN_KEY, token.trim());
  } catch {
    // The caller holds the token in memory.
  }
}

/** Forget the stored token. */
export function clearToken(storage: Storage): void {
  try {
    storage.removeItem(TOKEN_KEY);
  } catch {
    // Nothing stored that could be read back.
  }
}
