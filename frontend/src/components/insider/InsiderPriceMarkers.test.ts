/**
 * Marker-bucket aggregation tests (#588 Codex round 2).
 *
 * The chart layer itself is exercised in the page mount test (where
 * the component is mocked); these unit tests pin the data shape so a
 * regression in cutoff handling, direction classification, or
 * same-day bucketing is caught at the spec level rather than via a
 * visual diff on the rendered scatter.
 */

import { describe, expect, it } from "vitest";

import type { InsiderTransactionDetail } from "@/api/instruments";
import { bucketTransactionsForMarkers } from "@/components/insider/InsiderPriceMarkers";

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

const APR15_UTC_SEC = Math.floor(Date.UTC(2026, 3, 15) / 1000);

describe("bucketTransactionsForMarkers", () => {
  it("collapses same-day acquireds into one bucket and same-day disposeds into another", () => {
    const rows = [
      makeRow({
        txn_date: "2026-04-15",
        shares: "100",
        price: "10",
        acquired_disposed_code: "A",
        txn_code: "P",
      }),
      makeRow({
        txn_date: "2026-04-15",
        shares: "200",
        price: "11",
        acquired_disposed_code: "A",
        txn_code: "P",
      }),
      makeRow({
        txn_date: "2026-04-15",
        shares: "50",
        price: "10",
        acquired_disposed_code: "D",
        txn_code: "S",
      }),
    ];
    const buckets = bucketTransactionsForMarkers(rows, 0);
    expect(buckets).toHaveLength(2);
    const acquired = buckets.find((b) => b.direction === "acquired");
    const disposed = buckets.find((b) => b.direction === "disposed");
    expect(acquired?.shares).toBe(300);
    expect(acquired?.notional).toBe(100 * 10 + 200 * 11);
    expect(acquired?.count).toBe(2);
    expect(disposed?.shares).toBe(50);
    expect(disposed?.count).toBe(1);
  });

  it("includes a transaction filed on the cutoff date (UTC midnight semantics)", () => {
    const cutoff = Date.UTC(2026, 3, 15); // 2026-04-15 UTC
    const rows = [
      makeRow({ txn_date: "2026-04-15", filer_name: "On Cutoff" }),
      makeRow({ txn_date: "2026-04-14", filer_name: "Just Before" }),
    ];
    const buckets = bucketTransactionsForMarkers(rows, cutoff);
    expect(buckets).toHaveLength(1);
    expect(buckets[0]!.time).toBe(APR15_UTC_SEC);
  });

  it("falls back to share count when notional cannot be computed", () => {
    const rows = [
      makeRow({
        txn_date: "2026-04-15",
        shares: "100",
        price: null,
        acquired_disposed_code: "A",
        txn_code: "P",
      }),
    ];
    const buckets = bucketTransactionsForMarkers(rows, 0);
    expect(buckets).toHaveLength(1);
    expect(buckets[0]!.notional).toBe(100); // share-count fallback
    expect(buckets[0]!.shares).toBe(100);
  });

  it("drops derivative and unknown-direction rows", () => {
    const rows = [
      makeRow({ is_derivative: true, txn_date: "2026-04-15" }),
      makeRow({
        txn_date: "2026-04-15",
        acquired_disposed_code: null,
        txn_code: "Z",
      }),
    ];
    expect(bucketTransactionsForMarkers(rows, 0)).toHaveLength(0);
  });

  it("returns buckets sorted ascending by time so lightweight-charts accepts them", () => {
    const rows = [
      makeRow({ txn_date: "2026-04-10" }),
      makeRow({ txn_date: "2026-03-15" }),
      makeRow({ txn_date: "2026-04-05" }),
    ];
    const buckets = bucketTransactionsForMarkers(rows, 0);
    for (let i = 1; i < buckets.length; i++) {
      expect((buckets[i]!.time as number) >= (buckets[i - 1]!.time as number)).toBe(
        true,
      );
    }
  });
});
