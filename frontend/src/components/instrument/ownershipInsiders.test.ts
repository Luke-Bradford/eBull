import { describe, expect, it } from "vitest";

import {
  type InsiderBaselineRowShape,
  type InsiderRowShape,
  isBaselineHoldingRow,
  isInsiderHoldingRow,
} from "./ownershipInsiders";

function row(overrides: Partial<InsiderRowShape> = {}): InsiderRowShape {
  return {
    filer_cik: "0000000001",
    filer_name: "Test Filer",
    txn_date: "2026-04-15",
    post_transaction_shares: "1000",
    is_derivative: false,
    ...overrides,
  };
}

describe("isInsiderHoldingRow", () => {
  it("includes a non-derivative row with a parseable share count", () => {
    expect(isInsiderHoldingRow(row())).toBe(true);
  });

  it("excludes derivative rows even when the share count parses", () => {
    expect(isInsiderHoldingRow(row({ is_derivative: true }))).toBe(false);
  });

  it("excludes a non-derivative row whose share count is null", () => {
    // A Form 4 row with no usable post-transaction balance can't
    // contribute to the snapshot — counting its txn_date as fresh
    // would advance the freshness chip ahead of the actual ring data
    // (the original Codex / review-bot finding on PR #770).
    expect(isInsiderHoldingRow(row({ post_transaction_shares: null }))).toBe(false);
  });

  it("excludes a non-derivative row whose share count is unparseable", () => {
    expect(isInsiderHoldingRow(row({ post_transaction_shares: "not-a-number" }))).toBe(
      false,
    );
  });
});


function baselineRow(
  overrides: Partial<InsiderBaselineRowShape> = {},
): InsiderBaselineRowShape {
  return {
    filer_cik: "0000000099",
    filer_name: "Test Officer",
    is_derivative: false,
    shares: "5000",
    as_of_date: "2026-02-15",
    ...overrides,
  };
}


describe("isBaselineHoldingRow", () => {
  it("includes a row with a positive share count", () => {
    expect(isBaselineHoldingRow(baselineRow())).toBe(true);
  });

  it("excludes a row with null shares (value-branch holding without share count)", () => {
    // Codex / bot review of #768 PR4: a baseline row with null
    // shares (value-branch holding) must not advance the freshness
    // chip — the wedge it would correspond to is filtered out of
    // the rendered ring.
    expect(isBaselineHoldingRow(baselineRow({ shares: null }))).toBe(false);
  });

  it("excludes a row with zero shares", () => {
    // A zero-shares baseline row is also non-rendering — same drift
    // class.
    expect(isBaselineHoldingRow(baselineRow({ shares: "0" }))).toBe(false);
  });

  it("excludes a row with unparseable shares", () => {
    expect(isBaselineHoldingRow(baselineRow({ shares: "garbage" }))).toBe(false);
  });

  it("includes a derivative row with positive shares", () => {
    // is_derivative is informational on baseline rows (it splits the
    // SunburstHolder.key namespace) — but doesn't gate inclusion.
    // Derivative-equity-grant baselines DO render as ring 3 wedges.
    expect(isBaselineHoldingRow(baselineRow({ is_derivative: true }))).toBe(true);
  });
});
