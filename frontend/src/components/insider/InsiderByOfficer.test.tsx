/**
 * Officer-bucket aggregation tests (#588 Codex round 1).
 *
 * Targets the pure aggregator rather than the recharts component:
 * recharts' ResponsiveContainer needs a real layout pipeline that
 * jsdom does not simulate, and the visual layer is exercised in
 * the page mount test. The data-shaping logic — cutoff inclusivity,
 * CIK vs name dedupe, net-zero-but-active retention, and unknown-
 * direction filtering — is what the bug-hunting tests should cover.
 */

import { describe, expect, it } from "vitest";

import type { InsiderTransactionDetail } from "@/api/instruments";
import { buildOfficerBuckets } from "@/components/insider/InsiderByOfficer";

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

describe("buildOfficerBuckets", () => {
  it("includes a transaction filed on the cutoff date", () => {
    const cutoff = Date.UTC(2026, 3, 1); // 2026-04-01 UTC
    const rows = [
      makeRow({ filer_name: "On Cutoff", txn_date: "2026-04-01" }),
      makeRow({ filer_name: "Just Before", txn_date: "2026-03-31" }),
    ];
    const buckets = buildOfficerBuckets(rows, cutoff);
    const names = buckets.map((b) => b.officer);
    expect(names.some((n) => n.startsWith("On Cutoff"))).toBe(true);
    expect(names.some((n) => n.startsWith("Just Before"))).toBe(false);
  });

  it("aggregates by CIK so a renamed filer collapses to one row", () => {
    const cutoff = 0;
    const rows = [
      makeRow({
        filer_cik: "0000123",
        filer_name: "Smith, John",
        shares: "100",
        acquired_disposed_code: "A",
        txn_code: "P",
      }),
      makeRow({
        filer_cik: "0000123",
        filer_name: "John Smith",
        shares: "50",
        acquired_disposed_code: "A",
        txn_code: "P",
      }),
    ];
    const buckets = buildOfficerBuckets(rows, cutoff);
    expect(buckets).toHaveLength(1);
    expect(buckets[0]!.net).toBe(150);
  });

  it("falls back to filer_name when CIK is null", () => {
    const cutoff = 0;
    const rows = [
      makeRow({
        filer_cik: null,
        filer_name: "Anon Trader",
        shares: "200",
        acquired_disposed_code: "A",
        txn_code: "P",
      }),
      makeRow({
        filer_cik: null,
        filer_name: "Anon Trader",
        shares: "100",
        acquired_disposed_code: "D",
        txn_code: "S",
      }),
    ];
    const buckets = buildOfficerBuckets(rows, cutoff);
    expect(buckets).toHaveLength(1);
    expect(buckets[0]!.net).toBe(100);
    expect(buckets[0]!.txnCount).toBe(2);
  });

  it("retains officers whose net is zero from offsetting buys + sells", () => {
    const cutoff = 0;
    const rows = [
      makeRow({
        filer_cik: "1",
        filer_name: "Wash",
        shares: "100",
        acquired_disposed_code: "A",
        txn_code: "P",
      }),
      makeRow({
        filer_cik: "1",
        filer_name: "Wash",
        shares: "100",
        acquired_disposed_code: "D",
        txn_code: "S",
      }),
    ];
    const buckets = buildOfficerBuckets(rows, cutoff);
    expect(buckets).toHaveLength(1);
    expect(buckets[0]!.net).toBe(0);
    expect(buckets[0]!.txnCount).toBe(2);
  });

  it("drops derivative and unknown-direction rows", () => {
    const cutoff = 0;
    const rows = [
      makeRow({
        filer_cik: "1",
        filer_name: "Deriv",
        is_derivative: true,
      }),
      makeRow({
        filer_cik: "2",
        filer_name: "Mystery",
        acquired_disposed_code: null,
        txn_code: "Z", // unknown
      }),
      makeRow({
        filer_cik: "3",
        filer_name: "Real",
      }),
    ];
    const buckets = buildOfficerBuckets(rows, cutoff);
    expect(buckets.map((b) => b.officer)[0]).toMatch(/Real/);
    expect(buckets).toHaveLength(1);
  });
});
