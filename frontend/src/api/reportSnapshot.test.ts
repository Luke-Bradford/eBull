/**
 * SnapshotV2 type-test against the BACKEND-EMITTED fixtures (#1592
 * child 2, spec §8 step 2): the interfaces must accept the exact JSON
 * the builders write. Regenerate fixtures with
 * `REPORT_FIXTURE_WRITE=1 uv run pytest tests/test_reporting_v2_db.py`
 * — never handwrite them.
 *
 * The `...fixture` spreads are the type assertion: a missing, renamed,
 * or re-typed field in either interface or fixture fails `pnpm
 * typecheck` (the §2 phantom-key class, caught at compile time).
 * `schema_version` / `report_type` are re-pinned after the spread only
 * because JSON imports widen literals to number/string.
 */
import { describe, expect, it } from "vitest";

import monthlyFixture from "../../../tests/fixtures/report_snapshot_v2/monthly.json";
import weeklyFixture from "../../../tests/fixtures/report_snapshot_v2/weekly.json";

import {
  isMonthlyV2,
  isSnapshotV2,
  type MonthlySnapshotV2,
  type WeeklySnapshotV2,
} from "@/api/reportSnapshot";

const monthly: MonthlySnapshotV2 = {
  ...monthlyFixture,
  schema_version: 2,
  report_type: "monthly",
};

const weekly: WeeklySnapshotV2 = {
  ...weeklyFixture,
  schema_version: 2,
  report_type: "weekly",
};

/**
 * Exact top-level key inventories. The `...fixture` spreads above are
 * assignability-only (spreads bypass excess-property checks), so a
 * backend field the interfaces don't model would slip through — these
 * literal key sets close that hole (Codex ckpt-2 P3): an added,
 * renamed, or dropped builder key fails HERE, forcing the interface
 * update in the same diff. The backend db test pins fixture == builder
 * keys, so the chain is builder == fixture == this list == interface.
 */
const WEEKLY_KEYS = [
  "bottom_performers",
  "budget",
  "cover",
  "generated_at",
  "holdings",
  "performance",
  "period_contribution",
  "period_end",
  "period_start",
  "pnl",
  "positions",
  "positions_closed",
  "positions_opened",
  "report_type",
  "schema_version",
  "score_changes",
  "top_performers",
  "upcoming_earnings",
].sort();

const MONTHLY_KEYS = [
  "attribution_summary",
  "avg_holding_days",
  "best_trade",
  "costs",
  "cover",
  "generated_at",
  "holdings",
  "income",
  "performance",
  "period_contribution",
  "period_end",
  "period_start",
  "pnl",
  "position_pnl",
  "positions",
  "positions_closed",
  "positions_opened",
  "report_type",
  "risk",
  "rolling_returns",
  "schema_version",
  "score_changes",
  "tax_provision",
  "thesis_accuracy",
  "thesis_summary",
  "trade_stats",
  "win_rate",
  "worst_trade",
].sort();

describe("SnapshotV2 fixtures", () => {
  it("fixture top-level key sets mirror the modelled interfaces exactly", () => {
    expect(Object.keys(weeklyFixture).sort()).toEqual(WEEKLY_KEYS);
    expect(Object.keys(monthlyFixture).sort()).toEqual(MONTHLY_KEYS);
  });

  it("backend-emitted monthly fixture satisfies MonthlySnapshotV2", () => {
    expect(monthly.schema_version).toBe(2);
    expect(monthly.cover.closing_value).toBeTypeOf("string");
    expect(monthly.risk.sector_exposure).toBeTypeOf("object");
    // Fraction-basis pin: weight 0.52… is a fraction, not 52.38.
    const weight = Number(monthly.holdings[0]?.weight_pct);
    expect(weight).toBeGreaterThan(0);
    expect(weight).toBeLessThan(1);
  });

  it("backend-emitted weekly fixture satisfies WeeklySnapshotV2", () => {
    expect(weekly.schema_version).toBe(2);
    expect(Array.isArray(weekly.positions_opened)).toBe(true);
    expect(weekly.budget).toBeTypeOf("object");
  });

  it("isSnapshotV2 discriminates on schema_version", () => {
    expect(isSnapshotV2(monthlyFixture as Record<string, unknown>)).toBe(true);
    expect(isSnapshotV2({ pnl: {} })).toBe(false);
    expect(isSnapshotV2({ schema_version: 1 })).toBe(false);
  });

  it("isMonthlyV2 discriminates on report_type", () => {
    expect(isMonthlyV2(monthly)).toBe(true);
    expect(isMonthlyV2(weekly)).toBe(false);
  });

  it("benchmark legend label never leaks the internal symbol", () => {
    // Spec §5: legends say "S&P 500 (price index)", never SPX500.
    expect(monthly.performance.benchmark?.label).toBe("S&P 500 (price index)");
    expect(monthly.performance.benchmark?.label).not.toContain("SPX500");
  });
});
