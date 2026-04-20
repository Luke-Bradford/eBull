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

import { fetchFilings } from "@/api/filings";
import { fetchNews } from "@/api/news";
import { fetchRankings } from "@/api/rankings";

const mockedFilings = vi.mocked(fetchFilings);
const mockedNews = vi.mocked(fetchNews);
const mockedRankings = vi.mocked(fetchRankings);

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

beforeEach(() => {
  mockedFilings.mockReset();
  mockedNews.mockReset();
  mockedRankings.mockReset();
  mockedFilings.mockResolvedValue(filingsEmpty(42));
  mockedNews.mockResolvedValue(newsEmpty(42));
  mockedRankings.mockResolvedValue(rankingsEmpty());
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
    // AAPL (the current instrument) must not appear in the peer list.
    expect(screen.queryByText(/^AAPL$/)).not.toBeInTheDocument();

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
