/**
 * v1 (pre-#1596, no schema_version) snapshot fixtures pinning the
 * corrected legacy branch (spec §3.2). Shapes follow the documented v1
 * key inventory in docs/proposals/ui/2026-06-12-report-ia.md §2 — the
 * keys the OLD builders actually wrote (`realized_pnl`, not the §2
 * phantom `realised_pnl`; `gross_return_pct`, not `return_pct`).
 *
 * These are deliberately handwritten: the v1 builders no longer exist
 * to emit them (the no-handwrite rule binds the V2 fixtures, which
 * stay backend-emitted).
 */
import type { ReportSnapshot } from "@/api/reports";

export const V1_WEEKLY: ReportSnapshot = {
  snapshot_id: 101,
  report_type: "weekly",
  period_start: "2026-04-20",
  period_end: "2026-04-26",
  computed_at: "2026-04-27T06:00:00Z",
  snapshot_json: {
    pnl: {
      realized_pnl: "150.000000",
      unrealized_pnl: "320.500000",
      total_pnl: "470.500000",
      note: "current-state snapshot, not period delta",
    },
    top_performers: [
      { instrument_id: 1, symbol: "AAPL", company_name: "Apple", unrealized_pnl: "200.000000" },
    ],
    bottom_performers: [
      { instrument_id: 2, symbol: "GME", company_name: "GameStop", unrealized_pnl: "-50.000000" },
    ],
    positions_opened: [],
    positions_closed: [],
    upcoming_earnings: [],
    score_changes: [],
    budget: {},
    positions: [],
    period_contribution: {
      contributors: [
        { instrument_id: 1, symbol: "AAPL", pnl_delta: "120.000000", pnl_pct: "0.060000" },
      ],
      drags: [{ instrument_id: 2, symbol: "GME", pnl_delta: "-30.000000", pnl_pct: "-0.030000" }],
    },
  },
};

export const V1_MONTHLY: ReportSnapshot = {
  snapshot_id: 102,
  report_type: "monthly",
  period_start: "2026-04-01",
  period_end: "2026-04-30",
  computed_at: "2026-05-01T06:00:00Z",
  snapshot_json: {
    ...V1_WEEKLY.snapshot_json,
    position_pnl: [],
    win_rate: 66.67,
    avg_holding_days: 12.4,
    best_trade: {
      instrument_id: 1,
      symbol: "AAPL",
      gross_return_pct: "0.150000",
      hold_days: 20,
      model_alpha_pct: null,
    },
    worst_trade: {
      instrument_id: 2,
      symbol: "GME",
      gross_return_pct: "-0.080000",
      hold_days: 5,
      model_alpha_pct: null,
    },
    attribution_summary: {},
    thesis_accuracy: [
      { stance: "buy", symbol: "AAPL", gross_return_pct: "0.150000", target_hit: true },
      { stance: "avoid", symbol: "GME", gross_return_pct: "-0.080000", target_hit: false },
    ],
    tax_provision: {},
  },
};
