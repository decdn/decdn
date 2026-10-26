import { describe, expect, it } from "vitest";
import {
  formatBytes,
  formatCount,
  formatDuration,
  formatUsdc,
  shortHex,
} from "./format";

describe("formatBytes", () => {
  it("uses binary units with one decimal past bytes", () => {
    expect(formatBytes(0)).toBe("0 B");
    expect(formatBytes(1023)).toBe("1023 B");
    expect(formatBytes(1536)).toBe("1.5 KiB");
    expect(formatBytes(20 * 1024 ** 3)).toBe("20.0 GiB");
  });
});

describe("formatUsdc", () => {
  it("shows at least two decimals and keeps the micro digits that matter", () => {
    expect(formatUsdc(0)).toBe("0.00");
    expect(formatUsdc(1_500_000)).toBe("1.50");
    expect(formatUsdc(1_234)).toBe("0.001234");
    expect(formatUsdc(10_000_000)).toBe("10.00");
    expect(formatUsdc(191_205)).toBe("0.191205");
  });
});

describe("formatDuration", () => {
  it("names the two largest units", () => {
    expect(formatDuration(45)).toBe("45s");
    expect(formatDuration(192)).toBe("3m 12s");
    expect(formatDuration(7_500)).toBe("2h 05m");
    expect(formatDuration(3 * 86_400 + 4 * 3_600)).toBe("3d 4h");
  });
});

describe("formatCount", () => {
  it("groups below ten thousand and compacts above", () => {
    expect(formatCount(1_284)).toBe("1,284");
    expect(formatCount(12_900)).toBe("12.9K");
    expect(formatCount(4_200_000)).toBe("4.2M");
  });
});

describe("shortHex", () => {
  it("keeps the head and tail of a long hex string", () => {
    expect(shortHex("0x1234567890abcdef1234")).toBe("0x1234…1234");
    expect(shortHex("0x12")).toBe("0x12");
  });
});
