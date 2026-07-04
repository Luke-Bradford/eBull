import { describe, expect, it, vi, beforeEach } from "vitest";
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
    gics_sector: "Information Technology",
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
    data_completeness: 0.91,
    completeness_tier: "full",
    penalties_json: null,
    explanation: null,
    model_version: "v1-balanced",
    scored_at: "2026-04-28T12:00:00Z",
    ...overrides,
  };
}

function _response(
  items: RankingItem[],
  total: number,
  offset = 0,
): RankingsListResponse {
  return {
    items,
    total,
    offset,
    limit: 50,
    model_version: "v1-balanced",
    scored_at: "2026-04-28T12:00:00Z",
  };
}

/** Last (query, limit, offset) the page passed to fetchRankings. */
function lastCall(spy: ReturnType<typeof vi.spyOn>) {
  const calls = spy.mock.calls;
  return calls[calls.length - 1] as unknown as [rankingsApi.RankingsQuery, number, number];
}

describe("RankingsPage — server-authoritative (#1825)", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
    // #1918: the header's independent coverage fetch — stub so it never hits
    // the network. Its failure/absence must not affect the table assertions.
    vi.spyOn(rankingsApi, "fetchRankingsCoverage").mockResolvedValue({
      model_version: "v1.3-balanced",
      scored_at: null,
      universe: 12597,
      ranked: 1,
      not_ranked: [{ reason: "no_primary_sec_cik", label: "No SEC filer (non-US listing)", count: 12596 }],
    });
  });

  it("renders rows + the completeness chip", async () => {
    const spy = vi
      .spyOn(rankingsApi, "fetchRankings")
      .mockResolvedValue(_response([_item({ completeness_tier: "thin_data" })], 1));
    render(
      <MemoryRouter>
        <RankingsPage />
      </MemoryRouter>,
    );
    expect(await screen.findByText("AAA")).toBeInTheDocument();
    expect(screen.getByText(/thin data/i)).toBeInTheDocument();
    expect(spy).toHaveBeenCalled();
  });

  it("search drives a server query param (q) and resets to offset 0", async () => {
    const spy = vi
      .spyOn(rankingsApi, "fetchRankings")
      .mockResolvedValue(_response([_item({})], 1));
    render(
      <MemoryRouter>
        <RankingsPage />
      </MemoryRouter>,
    );
    await screen.findByText("AAA");

    await userEvent.type(screen.getByLabelText(/search/i), "BBB");
    await waitFor(
      () => {
        expect(lastCall(spy)[0].q).toBe("BBB");
      },
      { timeout: 1000 },
    );
    // offset (3rd positional arg) is 0 after a search.
    expect(lastCall(spy)[2]).toBe(0);
  });

  it("pagination Next advances the server offset", async () => {
    // 120 total, page size 50 → Next is enabled.
    const spy = vi
      .spyOn(rankingsApi, "fetchRankings")
      .mockResolvedValue(_response([_item({})], 120));
    render(
      <MemoryRouter>
        <RankingsPage />
      </MemoryRouter>,
    );
    await screen.findByText("AAA");
    expect(screen.getByText(/showing 1–1 of 120/i)).toBeInTheDocument();

    await userEvent.click(screen.getByRole("button", { name: /next/i }));
    await waitFor(() => {
      expect(lastCall(spy)[2]).toBe(50);
    });
  });

  it("Prev is disabled on the first page", async () => {
    vi.spyOn(rankingsApi, "fetchRankings").mockResolvedValue(
      _response([_item({})], 120),
    );
    render(
      <MemoryRouter>
        <RankingsPage />
      </MemoryRouter>,
    );
    await screen.findByText("AAA");
    expect(screen.getByRole("button", { name: /prev/i })).toBeDisabled();
  });

  it("sort header click drives the server sort param", async () => {
    const spy = vi
      .spyOn(rankingsApi, "fetchRankings")
      .mockResolvedValue(_response([_item({})], 1));
    render(
      <MemoryRouter>
        <RankingsPage />
      </MemoryRouter>,
    );
    await screen.findByText("AAA");

    await userEvent.click(screen.getByRole("button", { name: /value/i }));
    await waitFor(() => {
      expect(lastCall(spy)[0].sort).toBe("value_score");
      expect(lastCall(spy)[0].sort_dir).toBe("desc");
    });
  });

  it("shows the dirty-filter empty state when a search returns nothing", async () => {
    vi.spyOn(rankingsApi, "fetchRankings").mockResolvedValue(
      _response([], 0),
    );
    render(
      <MemoryRouter>
        <RankingsPage />
      </MemoryRouter>,
    );
    // No dirty filter yet → "produced no ranked instruments".
    expect(
      await screen.findByText(/produced no ranked instruments/i),
    ).toBeInTheDocument();

    await userEvent.type(screen.getByLabelText(/search/i), "zzz");
    await waitFor(
      () => {
        expect(
          screen.getByText(/no instruments match the current filters/i),
        ).toBeInTheDocument();
      },
      { timeout: 1000 },
    );
  });
});
