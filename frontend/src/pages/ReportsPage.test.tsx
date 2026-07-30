/**
 * ReportsPage — period-statement shell (#1592 child 2).
 *
 * Pins the §6.4 one-fetch model (page skeleton / page ErrorBanner /
 * EmptyState), the v2 statement chrome (masthead, benchmark label,
 * nil lines, notes), the fraction-basis formatting contract, and the
 * v1 → legacy-branch routing. Chart internals are covered by the pure
 * aggregator tests (snapshotMath, buildAttributionRows) per the
 * InsiderByOfficer convention — jsdom can't lay out
 * ResponsiveContainer.
 */
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type { ReportSnapshot } from "@/api/reports";
import monthlyFixture from "../../../tests/fixtures/report_snapshot_v2/monthly.json";
import weeklyFixture from "../../../tests/fixtures/report_snapshot_v2/weekly.json";
import { V1_WEEKLY } from "@/components/reports/__fixtures__/v1Snapshots";

vi.mock("@/api/reports", () => ({
  fetchWeeklyReports: vi.fn(),
  fetchMonthlyReports: vi.fn(),
}));

import { fetchMonthlyReports, fetchWeeklyReports } from "@/api/reports";
import { ReportsPage } from "@/pages/ReportsPage";

const mockedWeekly = vi.mocked(fetchWeeklyReports);
const mockedMonthly = vi.mocked(fetchMonthlyReports);

function row(
  json: Record<string, unknown>,
  overrides: Partial<ReportSnapshot> = {},
): ReportSnapshot {
  return {
    snapshot_id: 1,
    report_type: (json["report_type"] as "weekly" | "monthly") ?? "weekly",
    period_start: (json["period_start"] as string) ?? "2026-05-01",
    period_end: (json["period_end"] as string) ?? "2026-05-31",
    computed_at: "2026-06-12T14:36:07Z",
    snapshot_json: json,
    ...overrides,
  };
}

function renderPage(initialEntry = "/reports?type=monthly") {
  return render(
    <MemoryRouter initialEntries={[initialEntry]}>
      <ReportsPage />
    </MemoryRouter>,
  );
}

beforeEach(() => {
  mockedWeekly.mockReset();
  mockedMonthly.mockReset();
});

afterEach(cleanup);

describe("ReportsPage — v2 monthly statement", () => {
  beforeEach(() => {
    mockedMonthly.mockResolvedValue([row(monthlyFixture as Record<string, unknown>)]);
  });

  it("renders the masthead with currency and period range", async () => {
    renderPage();
    await waitFor(() => {
      expect(screen.getByText(/eToro demo account/)).toBeInTheDocument();
    });
    expect(screen.getByText(/Reporting currency USD/)).toBeInTheDocument();
    expect(screen.getByText(/Statement period/)).toBeInTheDocument();
  });

  it("renders the full monthly section set with statement names", async () => {
    renderPage();
    await waitFor(() => {
      expect(screen.getByText("Account summary")).toBeInTheDocument();
    });
    for (const title of [
      "Performance vs benchmark",
      "Rolling returns",
      "Attribution",
      "Holdings & exposure",
      "Period activity",
      "Dividends & income",
      "Charges",
      "Risk & trade statistics",
      "Model & thesis review",
      "Notes & disclosures",
      "Appendix: snapshot data",
    ]) {
      expect(screen.getByText(title)).toBeInTheDocument();
    }
  });

  it("renders fraction-basis percent fields through formatPct (no ×100 bug)", async () => {
    renderPage();
    await waitFor(() => {
      expect(screen.getByText("Holdings & exposure")).toBeInTheDocument();
    });
    // weight_pct "0.523810" → 52.38%, not 0.52%.
    expect(screen.getAllByText("52.38%").length).toBeGreaterThan(0);
    // since_entry_return_pct "0.100000" → +10.00% (signed return).
    expect(screen.getByText("+10.00%")).toBeInTheDocument();
  });

  it("renders nil lines for present-but-empty sections, not EmptyState", async () => {
    renderPage();
    await waitFor(() => {
      expect(screen.getByText("No dividends declared this period.")).toBeInTheDocument();
    });
    expect(screen.getByText(/No closed trades during this period/)).toBeInTheDocument();
  });

  it("monthly activity renders the nil line (keys present, §4.6 W+M)", async () => {
    renderPage();
    await waitFor(() => {
      expect(screen.getByText("No transactions during this period.")).toBeInTheDocument();
    });
  });

  it("pre-activity-contract monthly snapshots get the missing-key EmptyState", async () => {
    const old = { ...(monthlyFixture as Record<string, unknown>) };
    delete old["positions_opened"];
    delete old["positions_closed"];
    mockedMonthly.mockResolvedValue([row(old)]);
    renderPage();
    await waitFor(() => {
      expect(screen.getByText("Not included in this snapshot")).toBeInTheDocument();
    });
  });

  it("benchmark legend says the display label, never SPX500", async () => {
    renderPage();
    await waitFor(() => {
      expect(screen.getByText("Notes & disclosures")).toBeInTheDocument();
    });
    // The internal symbol may only appear inside the raw-JSON appendix
    // <pre>; any other occurrence is a legend/label leak (spec §5).
    const leaks = screen.queryAllByText(/SPX500/).filter((el) => el.closest("pre") === null);
    expect(leaks).toHaveLength(0);
  });

  it("renders the named sector exposure bars from the snapshot", async () => {
    renderPage();
    await waitFor(() => {
      expect(screen.getByText("Sector exposure")).toBeInTheDocument();
    });
    // Named sector appears in both the holdings table and the exposure
    // bars — both render the resolved name, never a numeric id. The
    // name is the canonical SIC-derived GICS label (#1634/#1851/#1951),
    // not eToro's coarse "Technology"; the fixture carried the stale
    // coarse value until it was regenerated (#2178).
    expect(
      screen.getAllByText("Information Technology").length,
    ).toBeGreaterThanOrEqual(2);
  });

  it("footer carries generated_at and the benchmark basis", async () => {
    renderPage();
    await waitFor(() => {
      expect(screen.getByText(/^Generated /)).toBeInTheDocument();
    });
    expect(screen.getByText(/Benchmark: S&P 500 \(price index\)/)).toBeInTheDocument();
  });
});

describe("ReportsPage — weekly digest", () => {
  beforeEach(() => {
    mockedWeekly.mockResolvedValue([row(weeklyFixture as Record<string, unknown>)]);
  });

  it("renders sections 1, 2, 4, 5, 6 + notes/appendix and no monthly-only sections", async () => {
    renderPage("/reports?type=weekly");
    await waitFor(() => {
      expect(screen.getByText("Account summary")).toBeInTheDocument();
    });
    expect(screen.getByText("Performance vs benchmark")).toBeInTheDocument();
    expect(screen.getByText("Attribution")).toBeInTheDocument();
    expect(screen.getByText("Holdings & exposure")).toBeInTheDocument();
    expect(screen.getByText("Period activity")).toBeInTheDocument();
    expect(screen.getByText("Notes & disclosures")).toBeInTheDocument();
    expect(screen.queryByText("Rolling returns")).not.toBeInTheDocument();
    expect(screen.queryByText("Dividends & income")).not.toBeInTheDocument();
    expect(screen.queryByText("Risk & trade statistics")).not.toBeInTheDocument();
  });

  it("weekly empty activity renders the nil line (key present, list empty)", async () => {
    renderPage("/reports?type=weekly");
    await waitFor(() => {
      expect(screen.getByText("No transactions during this period.")).toBeInTheDocument();
    });
  });
});

describe("ReportsPage — states and legacy routing", () => {
  it("error renders the page-level ErrorBanner with a fixed phrase and retry", async () => {
    mockedMonthly.mockRejectedValue(new Error("boom — internal detail"));
    renderPage();
    await waitFor(() => {
      expect(
        screen.getByText(/Failed to load report snapshots\. Check the browser console/),
      ).toBeInTheDocument();
    });
    // Never render exception text in the DOM.
    expect(screen.queryByText(/internal detail/)).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Retry" })).toBeInTheDocument();
  });

  it("empty list renders the EmptyState with the generating job named", async () => {
    mockedMonthly.mockResolvedValue([]);
    renderPage();
    await waitFor(() => {
      expect(screen.getByText("No monthly statements yet")).toBeInTheDocument();
    });
    expect(screen.getByText(/monthly_report/)).toBeInTheDocument();
  });

  it("v1 snapshots route to the corrected legacy branch", async () => {
    mockedWeekly.mockResolvedValue([V1_WEEKLY]);
    renderPage("/reports?type=weekly");
    await waitFor(() => {
      expect(screen.getByText(/Legacy snapshot \(v1 schema\)/)).toBeInTheDocument();
    });
    // v2 chrome must not leak into the legacy branch.
    expect(screen.queryByText(/eToro demo account/)).not.toBeInTheDocument();
  });

  it("fetches with limit=100 (period archive reachable, §6.6)", async () => {
    mockedMonthly.mockResolvedValue([row(monthlyFixture as Record<string, unknown>)]);
    renderPage();
    await waitFor(() => {
      expect(mockedMonthly).toHaveBeenCalledWith(100);
    });
  });
});

describe("ReportsPage — unbounded-payload defences (#2178)", () => {
  /** A pre-#2178 snapshot: the builder shipped every scored row in the
   *  period, so the stored monthly for June 2026 carries 29,281. Those
   *  rows are already persisted, so the builder fix does not rescue
   *  them — the page must survive them on its own. */
  function monthlyWithMovers(count: number): Record<string, unknown> {
    return {
      ...(monthlyFixture as Record<string, unknown>),
      // Override the fixture's own total (0) so the header stays
      // coherent with the array this helper builds.
      score_changes_total: count,
      score_changes: Array.from({ length: count }, (_, i) => ({
        instrument_id: i + 1,
        symbol: `S${i + 1}`,
        total_score: "50",
        rank: i + 1,
        rank_delta: count - i,
        scored_at: "2026-06-15T00:00:00Z",
      })),
    };
  }

  it("caps the rendered mover list regardless of payload size", async () => {
    mockedMonthly.mockResolvedValue([row(monthlyWithMovers(500))]);
    renderPage();
    await waitFor(() => {
      expect(screen.getByText("Model & thesis review")).toBeInTheDocument();
    });
    // Symbols are S1..S500 in rank_delta-DESC order; only the first 50
    // may reach the DOM. S1 is in, S51 is not.
    expect(screen.getByText("S1")).toBeInTheDocument();
    expect(screen.queryByText("S51")).not.toBeInTheDocument();
  });

  it("discloses the pre-cap total so the exhibit never reads as the full set", async () => {
    mockedMonthly.mockResolvedValue([
      row({ ...monthlyWithMovers(20), score_changes_total: 3898 }),
    ]);
    renderPage();
    await waitFor(() => {
      expect(screen.getByText(/20 of 3898 shown/)).toBeInTheDocument();
    });
  });

  it("falls back to the array length when a pre-#2178 snapshot has no total", async () => {
    // A genuinely pre-#2178 snapshot has no score_changes_total at all —
    // strip the key the current builder now always writes. The array IS
    // the full set there, so claiming a different number invents data.
    const legacy = { ...monthlyWithMovers(12) };
    delete legacy["score_changes_total"];
    mockedMonthly.mockResolvedValue([row(legacy)]);
    renderPage();
    await waitFor(() => {
      expect(screen.getByText(/12 of 12 shown/)).toBeInTheDocument();
    });
  });

  it("does not build the raw-JSON appendix until the operator opens it", async () => {
    mockedMonthly.mockResolvedValue([row(monthlyFixture as Record<string, unknown>)]);
    const { container } = renderPage();
    await waitFor(() => {
      expect(screen.getByText("Appendix: snapshot data")).toBeInTheDocument();
    });
    // `<details>` would have committed the <pre> while merely collapsed;
    // state-gating means there is no <pre> at all.
    expect(container.querySelector("pre")).toBeNull();

    fireEvent.click(screen.getByRole("button", { name: "Raw JSON" }));
    expect(container.querySelector("pre")).not.toBeNull();
  });
});
