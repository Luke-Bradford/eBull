/**
 * Tests for SummaryStrip (Slice 1 of per-stock research page,
 * docs/superpowers/specs/2026-04-20-per-stock-research-page.md).
 *
 * Pins the action-button gating spec:
 *   - Close visible only when `position.total_units > 0`.
 *   - Generate thesis visible when no thesis OR thesis > 30d old.
 *   - Add visible when `summary.is_tradable === true`.
 */
import { describe, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";
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
    identity: {
      symbol: "AAPL",
      display_name: "Apple Inc.",
      sector: "Technology",
      industry: "Consumer Electronics",
      exchange: "NMS",
      country: "United States",
      currency: "USD",
      market_cap: "3000000000000",
    },
    price: {
      current: "200.50",
      day_change: "1.50",
      day_change_pct: "0.00753",
      week_52_high: "250.00",
      week_52_low: "140.00",
      currency: "USD",
    },
    key_stats: null,
    source: { identity: "local_db+yfinance", price: "yfinance", key_stats: "yfinance" },
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
