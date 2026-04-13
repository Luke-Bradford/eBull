/**
 * Tests for InstrumentDetailPage (#62).
 *
 * Scope:
 *   - Header renders instrument metadata and quote
 *   - 404 instrument shows "not found" empty state
 *   - Thesis section: renders latest thesis, empty when 404
 *   - Score history: renders table, empty when no data
 *   - Filings: renders table, empty when no data
 *   - News: renders table, empty when no data
 *   - Recommendations: renders table, empty when no data
 *   - Position: shown when held, hidden when not
 *   - Per-section error isolation (one failing section doesn't blank others)
 *
 * API clients are mocked at the module boundary.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, within } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";

import { InstrumentDetailPage } from "@/pages/InstrumentDetailPage";
import { fetchInstrumentDetail } from "@/api/instruments";
import { fetchLatestThesis } from "@/api/theses";
import { fetchScoreHistory } from "@/api/scoreHistory";
import { fetchFilings } from "@/api/filings";
import { fetchNews } from "@/api/news";
import { fetchRecommendations } from "@/api/recommendations";
import { fetchPortfolio } from "@/api/portfolio";
import { ApiError } from "@/api/client";
import type {
  InstrumentDetail,
  ThesisDetail,
  ScoreHistoryResponse,
  FilingsListResponse,
  NewsListResponse,
  RecommendationsListResponse,
  PortfolioResponse,
} from "@/api/types";

vi.mock("@/api/instruments", () => ({
  fetchInstrumentDetail: vi.fn(),
  fetchInstruments: vi.fn(),
}));
vi.mock("@/api/theses", () => ({ fetchLatestThesis: vi.fn() }));
vi.mock("@/api/scoreHistory", () => ({ fetchScoreHistory: vi.fn() }));
vi.mock("@/api/filings", () => ({ fetchFilings: vi.fn() }));
vi.mock("@/api/news", () => ({ fetchNews: vi.fn() }));
vi.mock("@/api/recommendations", () => ({
  fetchRecommendations: vi.fn(),
  fetchRecommendation: vi.fn(),
}));
vi.mock("@/api/portfolio", () => ({ fetchPortfolio: vi.fn() }));

const mockedInstrument = vi.mocked(fetchInstrumentDetail);
const mockedThesis = vi.mocked(fetchLatestThesis);
const mockedScores = vi.mocked(fetchScoreHistory);
const mockedFilings = vi.mocked(fetchFilings);
const mockedNews = vi.mocked(fetchNews);
const mockedRecs = vi.mocked(fetchRecommendations);
const mockedPortfolio = vi.mocked(fetchPortfolio);

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

function makeInstrument(overrides: Partial<InstrumentDetail> = {}): InstrumentDetail {
  return {
    instrument_id: 42,
    symbol: "AAPL",
    company_name: "Apple Inc.",
    exchange: "NASDAQ",
    currency: "USD",
    sector: "Technology",
    industry: "Consumer Electronics",
    country: "US",
    is_tradable: true,
    first_seen_at: "2024-01-01T00:00:00Z",
    last_seen_at: "2024-06-01T00:00:00Z",
    coverage_tier: 1,
    latest_quote: {
      bid: 190.5,
      ask: 191.0,
      last: 190.75,
      spread_pct: 0.0026,
      quoted_at: "2024-06-01T12:00:00Z",
    },
    external_identifiers: [],
    ...overrides,
  };
}

function makeThesis(overrides: Partial<ThesisDetail> = {}): ThesisDetail {
  return {
    thesis_id: 1,
    instrument_id: 42,
    thesis_version: 1,
    thesis_type: "compounder",
    stance: "buy",
    confidence_score: 0.85,
    buy_zone_low: 180,
    buy_zone_high: 195,
    base_value: 220,
    bull_value: 260,
    bear_value: 160,
    break_conditions_json: ["Revenue growth < 5%"],
    memo_markdown: "Strong fundamentals and growing margins.",
    critic_json: { risk: "Valuation stretched" },
    created_at: "2024-06-01T10:00:00Z",
    ...overrides,
  };
}

const emptyScores: ScoreHistoryResponse = { instrument_id: 42, items: [] };
const emptyFilings: FilingsListResponse = {
  instrument_id: 42,
  symbol: "AAPL",
  items: [],
  total: 0,
  offset: 0,
  limit: 10,
};
const emptyNews: NewsListResponse = {
  instrument_id: 42,
  symbol: "AAPL",
  items: [],
  total: 0,
  offset: 0,
  limit: 10,
};
const emptyRecs: RecommendationsListResponse = {
  items: [],
  total: 0,
  offset: 0,
  limit: 50,
};
const emptyPortfolio: PortfolioResponse = {
  positions: [],
  position_count: 0,
  total_aum: 0,
  cash_balance: null,
  mirror_equity: 0,
  display_currency: "USD",
  fx_rates_used: {},
};

function renderPage(instrumentId = "42") {
  return render(
    <MemoryRouter initialEntries={[`/instruments/${instrumentId}`]}>
      <Routes>
        <Route path="/instruments/:instrumentId" element={<InstrumentDetailPage />} />
      </Routes>
    </MemoryRouter>,
  );
}

// ---------------------------------------------------------------------------
// Setup
// ---------------------------------------------------------------------------

beforeEach(() => {
  mockedInstrument.mockResolvedValue(makeInstrument());
  mockedThesis.mockResolvedValue(makeThesis());
  mockedScores.mockResolvedValue(emptyScores);
  mockedFilings.mockResolvedValue(emptyFilings);
  mockedNews.mockResolvedValue(emptyNews);
  mockedRecs.mockResolvedValue(emptyRecs);
  mockedPortfolio.mockResolvedValue(emptyPortfolio);
});

afterEach(() => {
  vi.restoreAllMocks();
});

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("InstrumentDetailPage — header", () => {
  it("renders instrument symbol, name, and metadata", async () => {
    renderPage();
    expect(await screen.findByText("AAPL")).toBeInTheDocument();
    expect(screen.getByText("Apple Inc.")).toBeInTheDocument();
    expect(screen.getByText(/Technology/)).toBeInTheDocument();
    expect(screen.getByText(/NASDAQ/)).toBeInTheDocument();
    expect(screen.getByText("Tier 1")).toBeInTheDocument();
  });

  it("renders latest quote with bid/ask and spread", async () => {
    renderPage();
    await screen.findByText("AAPL");
    expect(screen.getByText(/Bid/)).toBeInTheDocument();
    expect(screen.getByText(/Ask/)).toBeInTheDocument();
    expect(screen.getByText(/Spread/)).toBeInTheDocument();
  });

  it("shows not-tradable badge when instrument is not tradable", async () => {
    mockedInstrument.mockResolvedValueOnce(makeInstrument({ is_tradable: false }));
    renderPage();
    expect(await screen.findByText("Not tradable")).toBeInTheDocument();
  });
});

describe("InstrumentDetailPage — 404", () => {
  it("shows not-found state when instrument returns 404", async () => {
    mockedInstrument.mockRejectedValueOnce(
      new ApiError(404, "Not found"),
    );
    renderPage();
    expect(await screen.findByText("Instrument not found")).toBeInTheDocument();
    expect(screen.getByText(/Back to instruments/)).toBeInTheDocument();
  });

  it("shows invalid instrument for non-numeric ID", async () => {
    renderPage("abc");
    expect(await screen.findByText("Invalid instrument")).toBeInTheDocument();
  });
});

describe("InstrumentDetailPage — thesis section", () => {
  it("renders thesis with stance, type, memo, and critic output", async () => {
    renderPage();
    await screen.findByText("AAPL");
    expect(screen.getByText("buy")).toBeInTheDocument();
    expect(screen.getByText(/compounder/)).toBeInTheDocument();
    expect(screen.getByText(/Strong fundamentals/)).toBeInTheDocument();
    expect(screen.getByText("Critic")).toBeInTheDocument();
    expect(screen.getByText(/Valuation stretched/)).toBeInTheDocument();
  });

  it("renders break conditions", async () => {
    renderPage();
    await screen.findByText("AAPL");
    expect(screen.getByText("Revenue growth < 5%")).toBeInTheDocument();
  });

  it("shows empty state when thesis returns 404", async () => {
    mockedThesis.mockRejectedValueOnce(new ApiError(404, "No thesis"));
    renderPage();
    await screen.findByText("AAPL");
    expect(screen.getByText("No thesis generated yet")).toBeInTheDocument();
  });
});

describe("InstrumentDetailPage — score history", () => {
  it("renders score table when data exists", async () => {
    mockedScores.mockResolvedValueOnce({
      instrument_id: 42,
      items: [
        {
          scored_at: "2024-06-01T00:00:00Z",
          total_score: 78.5,
          raw_total: 80,
          quality_score: 85,
          value_score: 70,
          turnaround_score: null,
          momentum_score: 60,
          sentiment_score: 55,
          confidence_score: 0.9,
          penalties_json: null,
          explanation: null,
          rank: 3,
          rank_delta: -1,
          model_version: "v1-balanced",
        },
      ],
    });
    renderPage();
    await screen.findByText("AAPL");
    // Find score data in the score history section
    const section = screen.getByText("Score history").closest("section")!;
    expect(within(section).getByText("78.5")).toBeInTheDocument();
    expect(within(section).getByText("3")).toBeInTheDocument();
    expect(within(section).getByText("-1")).toBeInTheDocument();
  });

  it("shows empty state when no scores", async () => {
    renderPage();
    await screen.findByText("AAPL");
    expect(screen.getByText("No scoring data available")).toBeInTheDocument();
  });
});

describe("InstrumentDetailPage — filings", () => {
  it("shows empty state when no filings", async () => {
    renderPage();
    await screen.findByText("AAPL");
    expect(screen.getByText("No filing events recorded")).toBeInTheDocument();
  });

  it("renders filings table when data exists", async () => {
    mockedFilings.mockResolvedValueOnce({
      instrument_id: 42,
      symbol: "AAPL",
      items: [
        {
          filing_event_id: 1,
          instrument_id: 42,
          filing_date: "2024-05-15",
          filing_type: "10-Q",
          provider: "sec_edgar",
          source_url: "https://sec.gov/filing/123",
          primary_document_url: null,
          extracted_summary: "Revenue up 8% YoY",
          red_flag_score: 0.2,
          created_at: "2024-05-16T00:00:00Z",
        },
      ],
      total: 1,
      offset: 0,
      limit: 10,
    });
    renderPage();
    await screen.findByText("AAPL");
    expect(screen.getByText("10-Q")).toBeInTheDocument();
    expect(screen.getByText("Revenue up 8% YoY")).toBeInTheDocument();
  });
});

describe("InstrumentDetailPage — news", () => {
  it("shows empty state when no news", async () => {
    renderPage();
    await screen.findByText("AAPL");
    expect(screen.getByText("No news events in the last 30 days")).toBeInTheDocument();
  });

  it("renders news table when data exists", async () => {
    mockedNews.mockResolvedValueOnce({
      instrument_id: 42,
      symbol: "AAPL",
      items: [
        {
          news_event_id: 1,
          instrument_id: 42,
          event_time: "2024-06-01T08:00:00Z",
          source: "Reuters",
          headline: "Apple launches new product",
          category: "product",
          sentiment_score: 0.7,
          importance_score: 0.8,
          snippet: "Apple announced...",
          url: "https://reuters.com/article/1",
        },
      ],
      total: 1,
      offset: 0,
      limit: 10,
    });
    renderPage();
    await screen.findByText("AAPL");
    expect(screen.getByText("Apple launches new product")).toBeInTheDocument();
    expect(screen.getByText("Reuters")).toBeInTheDocument();
  });
});

describe("InstrumentDetailPage — recommendations", () => {
  it("shows empty state when no recommendations", async () => {
    renderPage();
    await screen.findByText("AAPL");
    expect(screen.getByText("No recommendations yet")).toBeInTheDocument();
  });

  it("renders recommendation table with action and status pills", async () => {
    mockedRecs.mockResolvedValueOnce({
      items: [
        {
          recommendation_id: 1,
          instrument_id: 42,
          symbol: "AAPL",
          company_name: "Apple Inc.",
          action: "BUY",
          status: "executed",
          rationale: "Strong thesis",
          score_id: 1,
          model_version: "v1-balanced",
          suggested_size_pct: 0.05,
          target_entry: 190,
          cash_balance_known: true,
          created_at: "2024-05-20T00:00:00Z",
        },
      ],
      total: 1,
      offset: 0,
      limit: 50,
    });
    renderPage();
    await screen.findByText("AAPL");
    const section = screen.getByText("Recommendation history").closest("section")!;
    expect(within(section).getByText("BUY")).toBeInTheDocument();
    expect(within(section).getByText("executed")).toBeInTheDocument();
    expect(within(section).getByText("Strong thesis")).toBeInTheDocument();
  });
});

describe("InstrumentDetailPage — position", () => {
  it("shows position section when instrument is held", async () => {
    mockedPortfolio.mockResolvedValueOnce({
      positions: [
        {
          instrument_id: 42,
          symbol: "AAPL",
          company_name: "Apple Inc.",
          open_date: "2024-03-01",
          avg_cost: 185,
          current_units: 10,
          cost_basis: 1850,
          market_value: 1910,
          unrealized_pnl: 60,
          valuation_source: "quote" as const,
          source: "broker_sync",
          updated_at: "2024-06-01T00:00:00Z",
        },
      ],
      position_count: 1,
      total_aum: 1910,
      cash_balance: 500,
      mirror_equity: 0,
      display_currency: "USD",
      fx_rates_used: {},
    });
    renderPage();
    await screen.findByText("AAPL");
    expect(screen.getByText("Position")).toBeInTheDocument();
    // Check that market value is rendered
    const section = screen.getByText("Position").closest("section")!;
    expect(within(section).getByText("Market value")).toBeInTheDocument();
  });

  it("hides position section when instrument is not held", async () => {
    renderPage();
    await screen.findByText("AAPL");
    expect(screen.queryByText("Position")).toBeNull();
  });
});

describe("InstrumentDetailPage — section error isolation", () => {
  it("shows header even when thesis fails with non-404 error", async () => {
    mockedThesis.mockRejectedValueOnce(new Error("network error"));
    renderPage();
    // Header renders fine
    expect(await screen.findByText("AAPL")).toBeInTheDocument();
    // Thesis section shows retry button
    expect(screen.getByText(/Failed to load/)).toBeInTheDocument();
    // Other sections still render their empty states
    expect(screen.getByText("No scoring data available")).toBeInTheDocument();
  });
});
