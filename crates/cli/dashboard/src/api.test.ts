import { describe, expect, it } from "vitest";
import { fetchStatus, TOKEN_HEADER } from "./api";

interface Seen {
  url?: string;
  init?: RequestInit;
}

function respond(status: number, body: string, seen: Seen = {}): typeof fetch {
  return (async (url: string, init?: RequestInit) => {
    seen.url = url;
    seen.init = init;
    return new Response(body, { status });
  }) as typeof fetch;
}

const status = JSON.stringify({ version: "0.3.0", pulls: {}, node: {} });

describe("fetchStatus", () => {
  it("reads /_status with the token, uncached, and returns the body", async () => {
    const seen: Seen = {};
    const got = await fetchStatus("t0k", respond(200, status, seen));
    expect(got.kind).toBe("ok");
    expect(seen.url).toBe("/_status");
    expect(seen.init?.cache).toBe("no-store");
    expect((seen.init?.headers as Record<string, string>)[TOKEN_HEADER]).toBe(
      "t0k",
    );
  });

  it("sends no token header without a token", async () => {
    const seen: Seen = {};
    await fetchStatus(null, respond(200, status, seen));
    expect(seen.init?.headers).toEqual({});
  });

  it("tells a refused token from an error", async () => {
    expect(
      (await fetchStatus("x", respond(401, "token required\n"))).kind,
    ).toBe("unauthorized");
    expect(await fetchStatus("x", respond(500, " busy\n"))).toEqual({
      kind: "error",
      message: "500 busy",
    });
  });

  it("takes a body that is not a status as an error", async () => {
    expect((await fetchStatus(null, respond(200, "null"))).kind).toBe("error");
  });

  it("takes the gateway's own status as an error: the page reads the dashboard", async () => {
    const gateway = JSON.stringify({ mode: "loopback", counters: {} });
    expect(await fetchStatus(null, respond(200, gateway))).toEqual({
      kind: "error",
      message: "the dashboard's answer is not its status",
    });
  });

  it("gives up on a dashboard that does not answer", async () => {
    const hang = ((_: string, init?: RequestInit) =>
      new Promise((_resolve, reject) => {
        init?.signal?.addEventListener("abort", () =>
          reject(init.signal?.reason),
        );
      })) as typeof fetch;
    const got = await fetchStatus(null, hang, 20);
    expect(got.kind).toBe("error");
    expect(got.kind === "error" && got.message).toContain("did not answer");
  });

  it("reports a network failure", async () => {
    const down = (async () => {
      throw new TypeError("Failed to fetch");
    }) as typeof fetch;
    expect(await fetchStatus(null, down)).toEqual({
      kind: "error",
      message: "Failed to fetch",
    });
  });
});
