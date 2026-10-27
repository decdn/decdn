// The boundary's catch runs only in a browser renderer (a server render
// throws past it), so these check what it derives from a throw and that it
// passes a section that draws through untouched.
import { createElement } from "react";
import { renderToString } from "react-dom/server";
import { describe, expect, it } from "vitest";
import type { GatewayStatus } from "../api";
import loopback from "../fixtures/status-loopback.json";
import poolError from "../fixtures/status-pool-error.json";
import publicStatus from "../fixtures/status-public.json";
import { GatewayPanels } from "./GatewayPanels";
import { SectionBoundary, thrownMessage } from "./SectionBoundary";

describe("SectionBoundary", () => {
  it("keeps what a render threw, as one line", () => {
    expect(
      SectionBoundary.getDerivedStateFromError(
        new TypeError("Cannot read properties of undefined (reading 'map')"),
      ),
    ).toEqual({
      message: "Cannot read properties of undefined (reading 'map')",
    });
    expect(thrownMessage("bare string")).toBe("bare string");
  });

  it("draws a section that renders, untouched", () => {
    const html = renderToString(
      createElement(SectionBoundary, {
        title: "Gateway",
        resetKey: 1,
        children: createElement("p", null, "drawn"),
      }),
    );
    expect(html).toBe("<p>drawn</p>");
  });
});

describe("GatewayPanels", () => {
  it("draws every gateway fixture", () => {
    for (const body of [loopback, publicStatus, poolError]) {
      const html = renderToString(
        createElement(GatewayPanels, { status: body as GatewayStatus }),
      );
      expect(html).toContain("Pools and lanes");
    }
  });
});
