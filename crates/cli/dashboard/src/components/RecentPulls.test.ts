import { createElement } from "react";
import { renderToString } from "react-dom/server";
import { describe, expect, it } from "vitest";
import type { EndedPull, Pulls } from "../api";
import full from "../fixtures/dashboard-full.json";
import { recentPulls } from "../view";
import { RecentPulls } from "./RecentPulls";

const pulls = (full as { pulls: Pulls }).pulls;

const draw = (p: Pulls) =>
  renderToString(createElement(RecentPulls, { rows: recentPulls(p) }));

describe("RecentPulls", () => {
  it("draws a bundle run's counts and what its bytes came from", () => {
    const html = draw(pulls);
    expect(html).toContain("Downloaded");
    expect(html).toContain("11 skipped");
    expect(html).toContain("9 excluded");
    expect(html).toContain("256.0 MiB resumed");
    expect(html).toContain("4.0 KiB reused");
  });

  it("draws failed and not-run counts in the critical tone", () => {
    const [bundle] = pulls.recent;
    const failing = {
      ...bundle,
      kind: "bundle",
      files_started: 2,
      files_landed: 1,
      counts: {
        fetched: 1,
        linked: 0,
        skipped: 0,
        reused: 0,
        excluded: 0,
        failed: 1,
        not_run: 0,
      },
    } as EndedPull;
    const html = draw({ ...pulls, recent: [failing] });
    expect(html).toMatch(/<span class="text-critical-ink">1 failed<\/span>/);
    expect(html).not.toMatch(/text-critical-ink">1 fetched/);
  });
});
