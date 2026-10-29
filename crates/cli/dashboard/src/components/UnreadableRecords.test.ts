import { createElement } from "react";
import { renderToString } from "react-dom/server";
import { describe, expect, it } from "vitest";
import type { Pulls } from "../api";
import full from "../fixtures/dashboard-full.json";
import { unreadableView } from "../view";
import { UnreadableRecords } from "./UnreadableRecords";

const pulls = (full as { pulls: Pulls }).pulls;

const draw = (p: Pulls) =>
  renderToString(createElement(UnreadableRecords, { view: unreadableView(p) }));

describe("UnreadableRecords", () => {
  it("names each unreadable file and why", () => {
    const html = draw(pulls);
    expect(html).toContain("1<!-- --> file(s) in the directory");
    const [u] = pulls.unreadable;
    expect(html).toContain(u?.path);
    expect(html).toContain("missing field `resumed_bytes`");
    expect(html).not.toContain("more");
  });

  it("says how many more files there are past the ones it names", () => {
    const unreadable = Array.from({ length: 8 }, (_, i) => ({
      path: `/pulls/${i}.json`,
      error: "EOF while parsing",
    }));
    const html = draw({ ...pulls, unreadable });
    expect(html).toContain("/pulls/4.json");
    expect(html).not.toContain("/pulls/5.json");
    expect(html).toContain("and <!-- -->3<!-- --> more");
  });

  it("draws nothing when every file reads", () => {
    expect(draw({ ...pulls, unreadable: [] })).toBe("");
  });
});
