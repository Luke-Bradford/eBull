/**
 * Tests for RightRail (Slice 2 of per-stock research page,
 * docs/superpowers/specs/2026-04-20-per-stock-research-page.md).
 *
 * Pins the rules the spec actually cares about:
 *   - All three section headers render.
 *   - Recent filings rows show `filing_type` + date + a link when the
 *     primary document URL is present.
 *   - Peer snapshot short-circuits when sector is null (no pointless
 *     rankings fetch).
 *   - Peer snapshot filters the current instrument out + links peers
 *     to `/instrument/:symbol`.
 *   - `fetchRankings` receives the sector + limit=6 contract.
 *
 * Explicitly NOT covered here: null-URL filing fallback + sentiment
 * tone palette. Those are presentation details; the spec ships their
 * correctness via code review, not behavioural tests.
 */
import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";

import { RightRail } from "@/components/instrument/RightRail";
import type {
  CopyTradingResponse,
  FilingsListResponse,
  NewsListResponse,
  RankingsListResponse,
} from "@/api/types";

vi.mock("@/api/filings", () => ({ fetchFilings: vi.fn() }));
vi.mock("@/api/news", () => ({ fetchNews: vi.fn() }));
vi.mock("@/api/rankings", () => ({
  fetchRankings: vi.fn(),
  RANKINGS_PAGE_LIMIT: 200,
}));
vi.mock("@/api/copyTrading", () => ({ fetchCopyTrading: vi.fn() }));

import { fetchFilings } from "@/api/filings";
import { fetchNews } from "@/api/news";
import { fetchRankings } from "@/api/rankings";
import { fetchCopyTrading } from "@/api/copyTrading";

const mockedFilings = vi.mocked(fetchFilings);
const mockedNews = vi.mocked(fetchNews);
const mockedRankings = vi.mocked(fetchRankings);
const mockedCopyTrading = vi.mocked(fetchCopyTrading);

function filingsEmpty(instrumentId: number): FilingsListResponse {
  return {
    instrument_id: instrumentId,
    symbol: null,
    items: [],
    total: 0,
    offset: 0,
    limit: 3,
  };
}

function filingsWith(instrumentId: number): FilingsListResponse {
  return {
    instrument_id: instrumentId,
    symbol: "AAPL",
    items: [
      {
        filing_event_id: 1,
        instrument_id: instrumentId,
        filing_date: "2026-04-18",
        filing_type: "10-Q",
        provider: "sec",
        source_url: null,
        primary_document_url: "https://sec.gov/10q",
        extracted_summary: null,
        red_flag_score: null,
        created_at: "2026-04-18T12:00:00Z",
      },
    ],
    total: 1,
    offset: 0,
    limit: 3,
  };
}

function newsEmpty(instrumentId: number): NewsListResponse {
  return {
    instrument_id: instrumentId,
    symbol: null,
    items: [],
    total: 0,
    offset: 0,
    limit: 3,
  };
}

function rankingsEmpty(): RankingsListResponse {
  return {
    items: [],
    total: 0,
    offset: 0,
    limit: 6,
    model_version: "v1",
    scored_at: null,
  };
}

function rankingsWith(currentSymbol: string): RankingsListResponse {
  return {
    items: [
      {
        instrument_id: 1,
        symbol: currentSymbol, // the current instrument appears in its own sector list
        company_name: `${currentSymbol} Inc.`,
        sector: "Technology",
        coverage_tier: 1,
        rank: 1,
        rank_delta: null,
        total_score: 9.1,
        raw_total: null,
        quality_score: null,
        value_score: null,
        turnaround_score: null,
        momentum_score: null,
        sentiment_score: null,
        confidence_score: null,
        penalties_json: null,
        explanation: null,
        model_version: "v1",
        scored_at: "2026-04-18T00:00:00Z",
      },
      {
        instrument_id: 2,
        symbol: "MSFT",
        company_name: "Microsoft Corp.",
        sector: "Technology",
        coverage_tier: 1,
        rank: 2,
        rank_delta: null,
        total_score: 8.4,
        raw_total: null,
        quality_score: null,
        value_score: null,
        turnaround_score: null,
        momentum_score: null,
        sentiment_score: null,
        confidence_score: null,
        penalties_json: null,
        explanation: null,
        model_version: "v1",
        scored_at: "2026-04-18T00:00:00Z",
      },
    ],
    total: 2,
    offset: 0,
    limit: 6,
    model_version: "v1",
    scored_at: "2026-04-18T00:00:00Z",
  };
}

function copyTradingEmpty(): CopyTradingResponse {
  return {
    traders: [],
    total_mirror_equity: 0,
    display_currency: "GBP",
  };
}

beforeEach(() => {
  mockedFilings.mockReset();
  mockedNews.mockReset();
  mockedRankings.mockReset();
  mockedCopyTrading.mockReset();
  mockedFilings.mockResolvedValue(filingsEmpty(42));
  mockedNews.mockResolvedValue(newsEmpty(42));
  mockedRankings.mockResolvedValue(rankingsEmpty());
  mockedCopyTrading.mockResolvedValue(copyTradingEmpty());
});

function renderRail(props: {
  instrumentId?: number;
  sector?: string | null;
  currentSymbol?: string;
} = {}) {
  const {
    instrumentId = 42,
    sector = "Technology",
    currentSymbol = "AAPL",
  } = props;
  return render(
    <MemoryRouter>
      <RightRail
        instrumentId={instrumentId}
        sector={sector}
        currentSymbol={currentSymbol}
        hasFilingsCoverage={true}
      />
    </MemoryRouter>,
  );
}

describe("RightRail", () => {
  it("renders all three section headers", async () => {
    renderRail();
    expect(
      await screen.findByText(/Recent filings/i),
    ).toBeInTheDocument();
    expect(screen.getByText(/Peer snapshot/i)).toBeInTheDocument();
    expect(screen.getByText(/Recent news/i)).toBeInTheDocument();
  });

  it("renders filing rows when data arrives", async () => {
    mockedFilings.mockResolvedValue(filingsWith(42));
    renderRail();
    await waitFor(() => {
      expect(screen.getByText("10-Q")).toBeInTheDocument();
    });
    const link = screen.getByText(/open →/);
    expect(link.getAttribute("href")).toBe("https://sec.gov/10q");
  });

  it("short-circuits peer fetch when sector is null", async () => {
    renderRail({ sector: null });
    await waitFor(() => {
      expect(
        screen.getByText(/Sector unknown — no peer set available/i),
      ).toBeInTheDocument();
    });
    expect(mockedRankings).not.toHaveBeenCalled();
  });

  it("filters the current instrument out of peer list + links peers to /instrument/:symbol", async () => {
    mockedRankings.mockResolvedValue(rankingsWith("AAPL"));
    renderRail({ currentSymbol: "AAPL" });

    await waitFor(() => {
      expect(screen.getByText("MSFT")).toBeInTheDocument();
    });
    // AAPL (the current instrument) must not appear as a peer link.
    // Scoping via the link href avoids false positives against any
    // future heading / breadcrumb that also renders "AAPL" text.
    expect(
      screen.queryByRole("link", { name: /^AAPL$/ }),
    ).not.toBeInTheDocument();

    const msftLink = screen
      .getByText("MSFT")
      .closest("a") as HTMLAnchorElement;
    expect(msftLink.getAttribute("href")).toBe("/instrument/MSFT");
  });

  it("renders 'No other ranked peers' when filtering leaves the peer list empty", async () => {
    // Rankings return only the current instrument — filter removes it,
    // leaving zero peers. Narrow fixture: one-row rankings containing
    // just "AAPL".
    mockedRankings.mockResolvedValue({
      items: [
        {
          instrument_id: 1,
          symbol: "AAPL",
          company_name: "Apple Inc.",
          sector: "Technology",
          coverage_tier: 1,
          rank: 1,
          rank_delta: null,
          total_score: 9.1,
          raw_total: null,
          quality_score: null,
          value_score: null,
          turnaround_score: null,
          momentum_score: null,
          sentiment_score: null,
          confidence_score: null,
          penalties_json: null,
          explanation: null,
          model_version: "v1",
          scored_at: "2026-04-18T00:00:00Z",
        },
      ],
      total: 1,
      offset: 0,
      limit: 6,
      model_version: "v1",
      scored_at: "2026-04-18T00:00:00Z",
    });
    renderRail({ currentSymbol: "AAPL" });
    await waitFor(() => {
      expect(
        screen.getByText(/No other ranked peers in this sector/i),
      ).toBeInTheDocument();
    });
  });

  it("passes sector + limit=6 to fetchRankings", async () => {
    mockedRankings.mockResolvedValue(rankingsEmpty());
    renderRail({ sector: "Healthcare" });
    await waitFor(() => {
      expect(mockedRankings).toHaveBeenCalledTimes(1);
    });
    const [query, limit] = mockedRankings.mock.calls[0]!;
    expect(query).toMatchObject({ sector: "Healthcare" });
    expect(limit).toBe(6);
  });
});

describe("RightRail — copy-trader exposure (Slice 6)", () => {
  it("hides exposure section when no copy traders hold the instrument", async () => {
    renderRail();
    // Wait for the copy-trading fetch to settle before asserting
    // absence — otherwise the section's loading-skeleton would still
    // be in the DOM and the assertion would falsely pass on a
    // later render.
    await waitFor(() => {
      expect(mockedCopyTrading).toHaveBeenCalled();
    });
    await waitFor(() => {
      expect(
        screen.queryByText(/Copy-trader exposure/i),
      ).not.toBeInTheDocument();
    });
  });

  it("lists each mirror holding the instrument + links to /copy-trading/:mirror_id", async () => {
    mockedCopyTrading.mockResolvedValue({
      traders: [
        {
          parent_cid: 1,
          parent_username: "@gurutrader",
          total_equity: 10000,
          mirrors: [
            {
              mirror_id: 42,
              active: true,
              initial_investment: 5000,
              deposit_summary: 5000,
              withdrawal_summary: 0,
              available_amount: 200,
              closed_positions_net_profit: 0,
              mirror_equity: 5500,
              position_count: 3,
              positions: [
                {
                  position_id: 1,
                  instrument_id: 42,
                  symbol: "AAPL",
                  company_name: "Apple Inc.",
                  is_buy: true,
                  units: 4,
                  amount: 800,
                  open_rate: 200,
                  open_conversion_rate: 1.0,
                  open_date_time: "2026-03-01T10:00:00Z",
                  current_price: 210,
                  market_value: 840,
                  unrealized_pnl: 40,
                },
              ],
              started_copy_date: "2026-02-01",
              closed_at: null,
            },
          ],
        },
      ],
      total_mirror_equity: 5500,
      display_currency: "GBP",
    });
    renderRail({ instrumentId: 42 });

    await waitFor(() => {
      expect(screen.getByText(/Copy-trader exposure/i)).toBeInTheDocument();
    });
    const link = screen.getByText("@gurutrader").closest("a") as HTMLAnchorElement;
    expect(link.getAttribute("href")).toBe("/copy-trading/42");
  });

  it("filters mirrors that don't hold the current instrument", async () => {
    mockedCopyTrading.mockResolvedValue({
      traders: [
        {
          parent_cid: 1,
          parent_username: "@other",
          total_equity: 1000,
          mirrors: [
            {
              mirror_id: 99,
              active: true,
              initial_investment: 500,
              deposit_summary: 500,
              withdrawal_summary: 0,
              available_amount: 0,
              closed_positions_net_profit: 0,
              mirror_equity: 500,
              position_count: 1,
              positions: [
                {
                  position_id: 7,
                  instrument_id: 999, // different instrument
                  symbol: "TSLA",
                  company_name: "Tesla",
                  is_buy: true,
                  units: 1,
                  amount: 100,
                  open_rate: 100,
                  open_conversion_rate: 1.0,
                  open_date_time: "2026-03-01T10:00:00Z",
                  current_price: 100,
                  market_value: 100,
                  unrealized_pnl: 0,
                },
              ],
              started_copy_date: "2026-02-01",
              closed_at: null,
            },
          ],
        },
      ],
      total_mirror_equity: 500,
      display_currency: "GBP",
    });
    renderRail({ instrumentId: 42 });

    await waitFor(() => {
      expect(mockedCopyTrading).toHaveBeenCalled();
    });
    await waitFor(() => {
      // @other holds TSLA, not AAPL — exposure section stays hidden.
      expect(
        screen.queryByText(/Copy-trader exposure/i),
      ).not.toBeInTheDocument();
    });
  });

  it("ignores closed mirrors even when they retain the position", async () => {
    mockedCopyTrading.mockResolvedValue({
      traders: [
        {
          parent_cid: 1,
          parent_username: "@closedone",
          total_equity: 0,
          mirrors: [
            {
              mirror_id: 77,
              active: false, // closed — must not contribute
              initial_investment: 500,
              deposit_summary: 500,
              withdrawal_summary: 500,
              available_amount: 0,
              closed_positions_net_profit: 0,
              mirror_equity: 0,
              position_count: 1,
              positions: [
                {
                  position_id: 42,
                  instrument_id: 42,
                  symbol: "AAPL",
                  company_name: "Apple",
                  is_buy: true,
                  units: 1,
                  amount: 200,
                  open_rate: 200,
                  open_conversion_rate: 1.0,
                  open_date_time: "2026-01-01T10:00:00Z",
                  current_price: 210,
                  market_value: 210,
                  unrealized_pnl: 10,
                },
              ],
              started_copy_date: "2025-11-01",
              closed_at: "2026-02-01",
            },
          ],
        },
      ],
      total_mirror_equity: 0,
      display_currency: "GBP",
    });
    renderRail({ instrumentId: 42 });

    await waitFor(() => {
      expect(mockedCopyTrading).toHaveBeenCalled();
    });
    await waitFor(() => {
      expect(
        screen.queryByText(/Copy-trader exposure/i),
      ).not.toBeInTheDocument();
    });
  });
});
