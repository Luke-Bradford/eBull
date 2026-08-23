import type { BenchmarkRefusal } from "@/api/types";

/**
 * #2602 item 5 — mirrors `app/api/strategies.py::BENCHMARK_REFUSALS`.
 *
 * ⚠ One copy, because two would drift from the server independently and a test
 * asserting a refusal the server no longer sends is worse than no test (review
 * NITPICK on PR #2883). The backend constant is the source of truth; the codes
 * here are the same tokens, and `detail` is abridged — the tests assert the
 * codes and one substring, never the full prose.
 */
export const BENCHMARK_REFUSALS: BenchmarkRefusal[] = [
  {
    benchmark: "sp500_total_return",
    label: "S&P 500 total return",
    reasons: [
      {
        code: "benchmark_source_unlicensed",
        detail: "We have reviewed no free S&P 500 total-return source and found it legally usable.",
      },
      { code: "benchmark_identity_unverified", detail: "Every S&P series we hold is a tracking ETF's PRICE." },
    ],
  },
  {
    benchmark: "cpih_real_return",
    label: "CPIH real return",
    reasons: [{ code: "benchmark_series_not_ingested", detail: "No CPI/CPIH series is ingested." }],
  },
];
