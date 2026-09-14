/**
 * `suggested_size_pct` is "target % of AUM" (`app/services/portfolio.py:95`) —
 * a composition, not a return. Rendering it through `formatPct` printed
 * "+5.00%", a portfolio weight wearing a gain's sign. Same defect class as
 * #3032's win rate, found by auditing every `@/lib/format` percent call site.
 */
import { cleanup, render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, describe, expect, it } from "vitest";

import type { RecommendationListItem } from "@/api/types";
import { RecommendationsTable } from "@/components/recommendations/RecommendationsTable";

const ITEM: RecommendationListItem = {
  recommendation_id: 1,
  instrument_id: 42,
  symbol: "AAPL",
  company_name: "Apple Inc.",
  action: "BUY",
  status: "pending",
  rationale: "test",
  score_id: null,
  model_version: null,
  suggested_size_pct: 0.05,
  target_entry: null,
  cash_balance_known: null,
  data_completeness: null,
  completeness_tier: null,
  created_at: "2026-09-14T09:00:00Z",
};

afterEach(cleanup);

describe("RecommendationsTable percent signs", () => {
  it("renders the suggested size unsigned — a share of AUM has no direction", () => {
    render(
      <MemoryRouter>
        <RecommendationsTable view={{ kind: "data", items: [ITEM] }} />
      </MemoryRouter>,
    );
    expect(screen.getByText("5.00%")).toBeInTheDocument();
    expect(screen.queryByText("+5.00%")).not.toBeInTheDocument();
  });
});
