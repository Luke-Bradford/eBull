/**
 * InsiderActivityPanel rendering tests (#458).
 *
 * Exists primarily to pin the summary-strip rendering against the
 * misleading-zero regression: a window with no acquisitions / no
 * disposals must render "0", not "+0" / "-0". Also pins the
 * two-lens label wording so a later refactor can't quietly drop the
 * "Open-market only (discretionary P/S)" qualifier and re-regress
 * the operator-facing clarity.
 */

import { render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import type {
  InsiderSummary,
  InsiderTransactionsList,
} from "@/api/instruments";

import { InsiderActivityPanel } from "./InsiderActivityPanel";

vi.mock("@/api/instruments", () => ({
  fetchInsiderSummary: vi.fn(),
  fetchInsiderTransactions: vi.fn(),
}));

import {
  fetchInsiderSummary,
  fetchInsiderTransactions,
} from "@/api/instruments";

const mockSummary = vi.mocked(fetchInsiderSummary);
const mockTransactions = vi.mocked(fetchInsiderTransactions);

function makeSummary(overrides: Partial<InsiderSummary> = {}): InsiderSummary {
  return {
    symbol: "GME",
    open_market_net_shares_90d: "-18331",
    open_market_buy_count_90d: 0,
    open_market_sell_count_90d: 3,
    total_acquired_shares_90d: "42392",
    total_disposed_shares_90d: "18331",
    acquisition_count_90d: 2,
    disposition_count_90d: 3,
    unique_filers_90d: 2,
    latest_txn_date: "2026-04-13",
    net_shares_90d: "-18331",
    buy_count_90d: 0,
    sell_count_90d: 3,
    ...overrides,
  };
}

function emptyTxns(): InsiderTransactionsList {
  return { symbol: "GME", rows: [] };
}

afterEach(() => vi.clearAllMocks());


describe("InsiderActivityPanel — summary strip", () => {
  it("leads with the all-codes Net change, not the open-market P/S count", async () => {
    mockSummary.mockResolvedValue(makeSummary());
    mockTransactions.mockResolvedValue(emptyTxns());

    render(<InsiderActivityPanel symbol="GME" />);

    await waitFor(() => {
      expect(screen.getByText(/Net change/i)).toBeInTheDocument();
    });
    // All-codes net: +42,392 - 18,331 = +24,061
    expect(screen.getByText(/\+24,061 shares/)).toBeInTheDocument();
    // Acquired / Disposed counters present (both values may appear
    // more than once — open-market breakdown reuses the -18,331).
    expect(screen.getByText(/\+42,392/)).toBeInTheDocument();
    expect(screen.getAllByText(/-18,331/).length).toBeGreaterThanOrEqual(1);
    // Open-market qualifier present but clearly secondary
    expect(
      screen.getByText(/Open-market only \(discretionary P\/S\)/i),
    ).toBeInTheDocument();
  });

  it("renders 0 (not +0 or -0) when acquisitions or disposals are empty", async () => {
    mockSummary.mockResolvedValue(
      makeSummary({
        total_acquired_shares_90d: "0",
        total_disposed_shares_90d: "0",
        acquisition_count_90d: 0,
        disposition_count_90d: 0,
        open_market_net_shares_90d: "0",
        net_shares_90d: "0",
      }),
    );
    mockTransactions.mockResolvedValue(emptyTxns());

    render(<InsiderActivityPanel symbol="GME" />);

    await waitFor(() => {
      expect(screen.getByText(/Net change/i)).toBeInTheDocument();
    });
    // No rendered "+0" or "-0" anywhere.
    expect(screen.queryByText(/^\+0$/)).toBeNull();
    expect(screen.queryByText(/^-0$/)).toBeNull();
  });
});
