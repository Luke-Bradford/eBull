import { describe, expect, it } from "vitest";

import { type InsiderRowShape, isInsiderHoldingRow } from "./ownershipInsiders";

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
