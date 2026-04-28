import { describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { RankingsPage } from "@/pages/RankingsPage";
import * as rankingsApi from "@/api/rankings";
import type { RankingItem, RankingsListResponse } from "@/api/types";

function _item(overrides: Partial<RankingItem>): RankingItem {
  return {
    instrument_id: 1,
    symbol: "AAA",
    company_name: "Alpha Co",
    sector: "Tech",
    coverage_tier: 1,
    rank: 1,
    rank_delta: 0,
    total_score: 80,
    raw_total: 80,
    quality_score: 70,
    value_score: 60,
    turnaround_score: 50,
    momentum_score: 40,
    sentiment_score: 30,
    confidence_score: 20,
    penalties_json: null,
    explanation: null,
    model_version: "v1-balanced",
    scored_at: "2026-04-28T12:00:00Z",
    ...overrides,
  };
}

describe("RankingsPage — #194 search", () => {
  it("filters rows by symbol substring (debounced)", async () => {
    const response: RankingsListResponse = {
      items: [
        _item({ instrument_id: 1, symbol: "AAA", company_name: "Alpha Co" }),
        _item({ instrument_id: 2, symbol: "BBB", company_name: "Beta Inc" }),
        _item({ instrument_id: 3, symbol: "CCC", company_name: "Charlie Ltd" }),
      ],
      total: 3,
      offset: 0,
      limit: 200,
      model_version: "v1-balanced",
      scored_at: "2026-04-28T12:00:00Z",
    };
    vi.spyOn(rankingsApi, "fetchRankings").mockResolvedValue(response);

    render(
      <MemoryRouter>
        <RankingsPage />
      </MemoryRouter>,
    );

    // Initial render shows all three rows.
    expect(await screen.findByText("AAA")).toBeInTheDocument();
    expect(screen.getByText("BBB")).toBeInTheDocument();
    expect(screen.getByText("CCC")).toBeInTheDocument();

    const searchInput = screen.getByLabelText(/search/i);
    await userEvent.type(searchInput, "BBB");

    // 300ms debounce — wait for filter to apply.
    await waitFor(
      () => {
        expect(screen.queryByText("AAA")).not.toBeInTheDocument();
      },
      { timeout: 1000 },
    );
    expect(screen.getByText("BBB")).toBeInTheDocument();
    expect(screen.queryByText("CCC")).not.toBeInTheDocument();
  });

  it("warns when search runs over a truncated page (#194 Codex)", async () => {
    const response: RankingsListResponse = {
      items: [_item({ instrument_id: 1, symbol: "AAA", company_name: "Alpha Co" })],
      // total > items.length triggers the truncation banner once the
      // user starts searching. Otherwise an out-of-page match would
      // silently appear as "No instruments match the current filters".
      total: 250,
      offset: 0,
      limit: 200,
      model_version: "v1-balanced",
      scored_at: "2026-04-28T12:00:00Z",
    };
    vi.spyOn(rankingsApi, "fetchRankings").mockResolvedValue(response);

    render(
      <MemoryRouter>
        <RankingsPage />
      </MemoryRouter>,
    );

    expect(await screen.findByText("AAA")).toBeInTheDocument();
    // No banner before search.
    expect(screen.queryByText(/matches outside the page/i)).not.toBeInTheDocument();

    await userEvent.type(screen.getByLabelText(/search/i), "foo");
    await waitFor(
      () => {
        expect(screen.getByText(/matches outside the page/i)).toBeInTheDocument();
      },
      { timeout: 1000 },
    );
  });

  it("filters by company-name substring (case-insensitive)", async () => {
    const response: RankingsListResponse = {
      items: [
        _item({ instrument_id: 1, symbol: "AAA", company_name: "Alpha Co" }),
        _item({ instrument_id: 2, symbol: "BBB", company_name: "Beta Inc" }),
      ],
      total: 2,
      offset: 0,
      limit: 200,
      model_version: "v1-balanced",
      scored_at: "2026-04-28T12:00:00Z",
    };
    vi.spyOn(rankingsApi, "fetchRankings").mockResolvedValue(response);

    render(
      <MemoryRouter>
        <RankingsPage />
      </MemoryRouter>,
    );

    expect(await screen.findByText("AAA")).toBeInTheDocument();

    const searchInput = screen.getByLabelText(/search/i);
    await userEvent.type(searchInput, "alpha");

    await waitFor(
      () => {
        expect(screen.queryByText("BBB")).not.toBeInTheDocument();
      },
      { timeout: 1000 },
    );
    expect(screen.getByText("AAA")).toBeInTheDocument();
  });
});
