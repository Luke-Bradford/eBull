/**
 * Tests for SummaryStrip (Slice 1 of per-stock research page,
 * docs/superpowers/specs/2026-04-20-per-stock-research-page.md).
 *
 * Pins the action-button gating spec:
 *   - Close visible only when `position.total_units > 0`.
 *   - Generate thesis visible when no thesis OR thesis > 30d old.
 *   - Add visible when `summary.is_tradable === true`.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { SummaryStrip } from "@/components/instrument/SummaryStrip";
import type {
  InstrumentPositionDetail,
  InstrumentSummary,
  ThesisDetail,
} from "@/api/types";

function summary(overrides: Partial<InstrumentSummary> = {}): InstrumentSummary {
  return {
    instrument_id: 42,
    is_tradable: true,
    coverage_tier: 1,
    session_profile: "us_equity",
    identity: {
      symbol: "AAPL",
      display_name: "Apple Inc.",
      sector: "8",
      sector_name: "Technology",
      industry: "Consumer Electronics",
      gics_sector: null,
      sector_spdr: null,
      exchange: "NMS",
      country: "United States",
      currency: "USD",
      market_cap: "3000000000000",
      class_market_value: null,
      canonical_symbol: null,
    },
    price: {
      current: "200.50",
      day_change: "1.50",
      day_change_pct: "0.00753",
      week_52_high: "250.00",
      week_52_low: "140.00",
      currency: "USD",
      display_current: null,
      display_currency: null,
    },
    key_stats: null,
    source: { identity: "local_db", price: "quotes", key_stats: "unavailable" },
    has_sec_cik: true,
    has_filings_coverage: true,
    capabilities: {
      filings: { providers: ["sec_edgar"], data_present: { sec_edgar: true } },
      fundamentals: { providers: [], data_present: {} },
      dividends: { providers: [], data_present: {} },
      insider: { providers: [], data_present: {} },
      analyst: { providers: [], data_present: {} },
      ratings: { providers: [], data_present: {} },
      esg: { providers: [], data_present: {} },
      ownership: { providers: [], data_present: {} },
      corporate_events: { providers: [], data_present: {} },
      business_summary: { providers: [], data_present: {} },
      officers: { providers: [], data_present: {} },
    },
    ...overrides,
  };
}

function freshThesis(): ThesisDetail {
  const now = new Date();
  return {
    thesis_id: 1,
    instrument_id: 42,
    thesis_version: 3,
    thesis_type: "compounder",
    stance: "buy",
    confidence_score: 0.72,
    buy_zone_low: 180,
    buy_zone_high: 210,
    base_value: 230,
    bull_value: 260,
    bear_value: 170,
    break_conditions_json: null,
    memo_markdown: "Fresh thesis memo.",
    critic_json: null,
    created_at: now.toISOString(),
  };
}

function staleThesis(): ThesisDetail {
  const old = new Date();
  old.setDate(old.getDate() - 45);
  return { ...freshThesis(), created_at: old.toISOString() };
}

function heldPosition(): InstrumentPositionDetail {
  return {
    instrument_id: 42,
    symbol: "AAPL",
    company_name: "Apple Inc.",
    currency: "USD",
    current_price: 200.5,
    total_units: 10,
    avg_entry: 180,
    total_invested: 1800,
    total_value: 2005,
    total_pnl: 205,
    trades: [
      {
        position_id: 101,
        is_buy: true,
        units: 10,
        amount: 1800,
        open_rate: 180,
        open_date_time: "2026-01-01T10:00:00Z",
        current_price: 200.5,
        market_value: 2005,
        unrealized_pnl: 205,
        stop_loss_rate: null,
        take_profit_rate: null,
        is_tsl_enabled: false,
        leverage: 1,
        total_fees: 0,
      },
    ],
  };
}

function noopProps() {
  return {
    thesisLoaded: true,
    thesisError: false,
    positionLoaded: true,
    positionError: false,
    onAdd: vi.fn(),
    onClose: vi.fn(),
    onGenerateThesis: vi.fn(),
    generatingThesis: false,
  };
}

describe("SummaryStrip — action gating", () => {
  it("renders identity + price + sector strip from summary", () => {
    render(
      <SummaryStrip
        summary={summary()}
        thesis={null}
        position={null}
        {...noopProps()}
      />,
    );
    expect(screen.getByText("AAPL")).toBeInTheDocument();
    expect(screen.getByText("Apple Inc.")).toBeInTheDocument();
    expect(screen.getByText(/Technology/)).toBeInTheDocument();
  });

  it("shows native price primary + display-currency companion (#1906)", () => {
    render(
      <SummaryStrip
        summary={summary({
          price: {
            current: "200.50",
            day_change: "1.50",
            day_change_pct: "0.00753",
            week_52_high: "250.00",
            week_52_low: "140.00",
            currency: "USD",
            display_current: "156.39",
            display_currency: "GBP",
          },
        })}
        thesis={null}
        position={null}
        {...noopProps()}
      />,
    );
    // Native is the primary (tradable) number.
    expect(screen.getByText("USD 200.50")).toBeInTheDocument();
    // Display-currency worth rides along as the muted companion.
    expect(screen.getByTestId("price-companion")).toHaveTextContent(
      "≈ GBP 156.39",
    );
  });

  it("omits the companion when there is no display-currency conversion (#1906)", () => {
    render(
      <SummaryStrip
        summary={summary()}
        thesis={null}
        position={null}
        {...noopProps()}
      />,
    );
    expect(screen.getByText("USD 200.50")).toBeInTheDocument();
    expect(screen.queryByTestId("price-companion")).not.toBeInTheDocument();
  });

  it("prefers the real GICS sector + SPDR over the opaque sector code (#1634)", () => {
    render(
      <SummaryStrip
        summary={summary({
          identity: {
            ...summary().identity,
            sector: "3",
            gics_sector: "Information Technology",
            sector_spdr: "XLK",
          },
        })}
        thesis={null}
        position={null}
        {...noopProps()}
      />,
    );
    expect(
      screen.getByText(/Information Technology \(XLK\)/),
    ).toBeInTheDocument();
    // The opaque code is not surfaced when a real sector resolves.
    expect(screen.queryByText(/^3 ·/)).not.toBeInTheDocument();
  });

  it("falls back to the resolved eToro industry name, never the raw id (#1599)", () => {
    render(
      <SummaryStrip
        summary={summary({
          identity: {
            ...summary().identity,
            sector: "5",
            sector_name: "Healthcare",
            gics_sector: null,
            sector_spdr: null,
          },
        })}
        thesis={null}
        position={null}
        {...noopProps()}
      />,
    );
    expect(screen.getByText(/Healthcare/)).toBeInTheDocument();
    // The opaque eToro numeric id must never be surfaced.
    expect(screen.queryByText(/^5 ·/)).not.toBeInTheDocument();
  });

  it("hides Close when not held", () => {
    render(
      <SummaryStrip
        summary={summary()}
        thesis={freshThesis()}
        position={null}
        {...noopProps()}
      />,
    );
    expect(screen.queryByTestId("action-close")).not.toBeInTheDocument();
    expect(screen.queryByTestId("held-badge")).not.toBeInTheDocument();
  });

  it("shows Close + Held badge when position.total_units > 0", () => {
    render(
      <SummaryStrip
        summary={summary()}
        thesis={freshThesis()}
        position={heldPosition()}
        {...noopProps()}
      />,
    );
    expect(screen.getByTestId("action-close")).toBeInTheDocument();
    expect(screen.getByTestId("held-badge").textContent).toContain("10u");
  });

  it("hides Generate thesis when thesis is fresh (< 30d)", () => {
    render(
      <SummaryStrip
        summary={summary()}
        thesis={freshThesis()}
        position={null}
        {...noopProps()}
      />,
    );
    expect(
      screen.queryByTestId("action-generate-thesis"),
    ).not.toBeInTheDocument();
    expect(screen.getByTestId("thesis-badge").textContent).not.toContain(
      "stale",
    );
  });

  it("shows Generate thesis when thesis is missing", () => {
    render(
      <SummaryStrip
        summary={summary()}
        thesis={null}
        position={null}
        {...noopProps()}
      />,
    );
    expect(screen.getByTestId("action-generate-thesis")).toBeInTheDocument();
    expect(screen.getByTestId("thesis-badge-missing")).toBeInTheDocument();
  });

  it("shows Generate thesis + (stale) marker when thesis is > 30d old", () => {
    render(
      <SummaryStrip
        summary={summary()}
        thesis={staleThesis()}
        position={null}
        {...noopProps()}
      />,
    );
    expect(screen.getByTestId("action-generate-thesis")).toBeInTheDocument();
    expect(screen.getByTestId("thesis-badge").textContent).toContain("stale");
  });

  it("labels the action Buy when unheld and Add when held (#316)", () => {
    const { rerender } = render(
      <SummaryStrip
        summary={summary()}
        thesis={freshThesis()}
        position={null}
        {...noopProps()}
      />,
    );
    expect(screen.getByTestId("action-add").textContent).toBe("Buy");

    rerender(
      <SummaryStrip
        summary={summary()}
        thesis={freshThesis()}
        position={heldPosition()}
        {...noopProps()}
      />,
    );
    expect(screen.getByTestId("action-add").textContent).toBe("Add");
  });

  it("defaults the action label to Add (not Buy) while position state is loading or errored (#316)", () => {
    const { rerender } = render(
      <SummaryStrip
        summary={summary()}
        thesis={freshThesis()}
        position={null}
        {...noopProps()}
        positionLoaded={false}
      />,
    );
    expect(screen.getByTestId("action-add").textContent).toBe("Add");

    rerender(
      <SummaryStrip
        summary={summary()}
        thesis={freshThesis()}
        position={null}
        {...noopProps()}
        positionError={true}
      />,
    );
    expect(screen.getByTestId("action-add").textContent).toBe("Add");
  });

  it("hides Add when instrument is not tradable", () => {
    render(
      <SummaryStrip
        summary={summary({ is_tradable: false })}
        thesis={freshThesis()}
        position={null}
        {...noopProps()}
      />,
    );
    expect(screen.queryByTestId("action-add")).not.toBeInTheDocument();
  });

  it("calls onAdd / onClose / onGenerateThesis on click", async () => {
    const props = noopProps();
    render(
      <SummaryStrip
        summary={summary()}
        thesis={staleThesis()}
        position={heldPosition()}
        {...props}
      />,
    );
    const user = userEvent.setup();
    await user.click(screen.getByTestId("action-add"));
    await user.click(screen.getByTestId("action-close"));
    await user.click(screen.getByTestId("action-generate-thesis"));
    expect(props.onAdd).toHaveBeenCalledTimes(1);
    expect(props.onClose).toHaveBeenCalledTimes(1);
    expect(props.onGenerateThesis).toHaveBeenCalledTimes(1);
  });

  it("disables Generate thesis button while generatingThesis=true", () => {
    render(
      <SummaryStrip
        summary={summary()}
        thesis={null}
        position={null}
        {...noopProps()}
        generatingThesis
      />,
    );
    const btn = screen.getByTestId("action-generate-thesis");
    expect(btn).toBeDisabled();
    expect(btn.textContent).toContain("Generating");
  });

  it("hides thesis badge + Generate thesis button while fetch is unsettled (thesisLoaded=false)", () => {
    // Prevents a pre-resolution null from flashing "Generate thesis"
    // and triggering a needless generation (Codex slice-1 feedback).
    render(
      <SummaryStrip
        summary={summary()}
        thesis={null}
        position={null}
        {...noopProps()}
        thesisLoaded={false}
      />,
    );
    expect(screen.queryByTestId("thesis-badge")).not.toBeInTheDocument();
    expect(
      screen.queryByTestId("thesis-badge-missing"),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByTestId("action-generate-thesis"),
    ).not.toBeInTheDocument();
  });

  it("hides held badge + Close button while position fetch is unsettled", () => {
    render(
      <SummaryStrip
        summary={summary()}
        thesis={freshThesis()}
        position={heldPosition()}
        {...noopProps()}
        positionLoaded={false}
      />,
    );
    expect(screen.queryByTestId("held-badge")).not.toBeInTheDocument();
    expect(screen.queryByTestId("action-close")).not.toBeInTheDocument();
  });

  it("keeps stance badge visible when thesisError=true but thesis data is non-null", () => {
    // Previously-fetched thesis data must not be silently dropped
    // during a sticky-error window (Codex slice-1 round-4 finding).
    render(
      <SummaryStrip
        summary={summary()}
        thesis={freshThesis()}
        position={null}
        {...noopProps()}
        thesisError
        thesisLoaded={false}
      />,
    );
    expect(screen.getByTestId("thesis-badge").textContent).toContain("BUY");
    // Error signal is additive, not a replacement.
    expect(screen.getByTestId("thesis-badge-error")).toBeInTheDocument();
  });

  it("shows error badge + keeps Generate thesis reachable on thesisError", () => {
    // Non-404 thesis errors used to silently blank the strip. Now
    // the error is surfaced and the retry affordance remains visible
    // (Codex slice-1 round-3 finding).
    render(
      <SummaryStrip
        summary={summary()}
        thesis={null}
        position={null}
        {...noopProps()}
        thesisError
        thesisLoaded={false}
      />,
    );
    expect(screen.getByTestId("thesis-badge-error")).toBeInTheDocument();
    expect(screen.getByTestId("action-generate-thesis")).toBeInTheDocument();
  });

  it("shows position error badge + hides Close on positionError", () => {
    render(
      <SummaryStrip
        summary={summary()}
        thesis={freshThesis()}
        position={null}
        {...noopProps()}
        positionError
        positionLoaded={false}
      />,
    );
    expect(screen.getByTestId("position-badge-error")).toBeInTheDocument();
    // Close button must stay hidden when holdings are unknown —
    // offering Close against unresolved position data is unsafe.
    expect(screen.queryByTestId("action-close")).not.toBeInTheDocument();
  });

  it("hides Close button for multi-trade positions (can't close from strip)", () => {
    render(
      <SummaryStrip
        summary={summary()}
        thesis={freshThesis()}
        position={{
          ...heldPosition(),
          trades: [
            { position_id: 1 } as unknown as InstrumentPositionDetail["trades"][0],
            { position_id: 2 } as unknown as InstrumentPositionDetail["trades"][0],
          ],
        }}
        {...noopProps()}
      />,
    );
    expect(screen.queryByTestId("action-close")).not.toBeInTheDocument();
  });
});

// A minimal EventSource stub so we can drive the useLiveQuote SSE overlay
// and verify the header's native-primary + companion sourcing (#1906).
class FakeEventSource {
  static instances: FakeEventSource[] = [];
  static CONNECTING = 0;
  static OPEN = 1;
  static CLOSED = 2;
  url: string;
  readyState = FakeEventSource.CONNECTING;
  onopen: ((ev: Event) => void) | null = null;
  onmessage: ((ev: MessageEvent) => void) | null = null;
  onerror: ((ev: Event) => void) | null = null;
  constructor(url: string) {
    this.url = url;
    FakeEventSource.instances.push(this);
  }
  close(): void {
    this.readyState = FakeEventSource.CLOSED;
  }
  fireMessage(data: string): void {
    this.onmessage?.(new MessageEvent("message", { data }));
  }
}

describe("SummaryStrip — live-tick currency sourcing (#1906)", () => {
  beforeEach(() => {
    FakeEventSource.instances = [];
    vi.stubGlobal("EventSource", FakeEventSource);
  });
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("overrides REST native with the live tick and drops a stale REST companion when the tick has no display block", () => {
    render(
      <SummaryStrip
        summary={summary({
          price: {
            current: "100.00",
            day_change: null,
            day_change_pct: null,
            week_52_high: null,
            week_52_low: null,
            currency: "USD",
            // Stale companion computed for the 100.00 REST snapshot.
            display_current: "78.00",
            display_currency: "GBP",
          },
        })}
        thesis={null}
        position={null}
        {...noopProps()}
      />,
    );
    // Live tick: new native price, NO display block (no FX rate this tick).
    act(() => {
      FakeEventSource.instances[0]!.fireMessage(
        JSON.stringify({
          instrument_id: 42,
          native_currency: "USD",
          bid: "109",
          ask: "111",
          last: "110",
          quoted_at: "2026-07-04T12:00:00+00:00",
          display: null,
        }),
      );
    });
    // Native primary follows the live tick…
    expect(screen.getByText("USD 110.00")).toBeInTheDocument();
    // …and the stale REST companion (GBP 78, for the old 100.00) is NOT shown.
    expect(screen.queryByTestId("price-companion")).not.toBeInTheDocument();
  });

  it("shows the live tick's own display companion beside the live native price", () => {
    render(
      <SummaryStrip
        summary={summary({
          price: {
            current: "100.00",
            day_change: null,
            day_change_pct: null,
            week_52_high: null,
            week_52_low: null,
            currency: "USD",
            display_current: null,
            display_currency: null,
          },
        })}
        thesis={null}
        position={null}
        {...noopProps()}
      />,
    );
    act(() => {
      FakeEventSource.instances[0]!.fireMessage(
        JSON.stringify({
          instrument_id: 42,
          native_currency: "USD",
          bid: "109",
          ask: "111",
          last: "110",
          quoted_at: "2026-07-04T12:00:00+00:00",
          display: { currency: "GBP", bid: "85", ask: "87", last: "86" },
        }),
      );
    });
    expect(screen.getByText("USD 110.00")).toBeInTheDocument();
    expect(screen.getByTestId("price-companion")).toHaveTextContent("≈ GBP 86.00");
  });
});
