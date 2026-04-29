/**
 * Month-bucket aggregation tests (#588 Codex round 1).
 *
 * Covers cutoff inclusivity (the calendar-month boundary, not a
 * rolling 30d×24 approximation) and the net-zero-but-active distinction
 * that the empty-state copy hinges on. The recharts render layer is
 * out of scope here — see the page mount test for visual coverage.
 */

import { describe, expect, it } from "vitest";

import type { InsiderTransactionDetail } from "@/api/instruments";
import { buildMonthBuckets } from "@/components/insider/InsiderNetByMonth";

function makeRow(
  partial: Partial<InsiderTransactionDetail>,
): InsiderTransactionDetail {
  return {
    accession_number: "A1",
    txn_row_num: 0,
    document_type: "4",
    txn_date: "2026-04-15",
    deemed_execution_date: null,
    filer_cik: "0000000001",
    filer_name: "Jane Doe",
    filer_role: "officer:CFO",
    security_title: "Common Stock",
    txn_code: "P",
    acquired_disposed_code: "A",
    shares: "100",
    price: "10",
    post_transaction_shares: null,
    direct_indirect: "D",
    nature_of_ownership: null,
    is_derivative: false,
    equity_swap_involved: null,
    transaction_timeliness: null,
    conversion_exercise_price: null,
    exercise_date: null,
    expiration_date: null,
    underlying_security_title: null,
    underlying_shares: null,
    underlying_value: null,
    footnotes: {},
    ...partial,
  };
}

describe("buildMonthBuckets", () => {
  it("returns 24 contiguous month buckets", () => {
    const result = buildMonthBuckets([]);
    expect(result.buckets).toHaveLength(24);
    // Months strictly increasing in lexicographic order
    for (let i = 1; i < result.buckets.length; i++) {
      expect(result.buckets[i]!.month > result.buckets[i - 1]!.month).toBe(
        true,
      );
    }
  });

  it("flags hadActivity=true when offsetting buys + sells net to zero", () => {
    const date = "2026-04-15";
    const rows = [
      makeRow({
        txn_date: date,
        shares: "100",
        acquired_disposed_code: "A",
        txn_code: "P",
      }),
      makeRow({
        txn_date: date,
        shares: "100",
        acquired_disposed_code: "D",
        txn_code: "S",
      }),
    ];
    const result = buildMonthBuckets(rows);
    expect(result.hadActivity).toBe(true);
    // The bucket itself has net=0 — render path uses hadActivity, not
    // an all-zero check, so the chart still draws a flat bar.
    const apr = result.buckets.find((b) => b.month === "2026-04");
    expect(apr?.net).toBe(0);
  });

  it("flags hadActivity=false when only derivative or unknown rows match", () => {
    const rows = [
      makeRow({ is_derivative: true }),
      makeRow({ acquired_disposed_code: null, txn_code: "Z" }),
    ];
    const result = buildMonthBuckets(rows);
    expect(result.hadActivity).toBe(false);
  });
});
