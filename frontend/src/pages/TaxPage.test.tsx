/**
 * Tests for TaxPage (#1905 PR2 — Tax & CGT view).
 *
 * Scope:
 *   - Summary metrics render (net gain, gains, losses, CGT estimates)
 *   - Exempt gauge shows "remaining" vs "above the allowance" copy
 *   - Disposals table renders rows; CSV button appears only when non-empty
 *   - Empty / error states per section
 *   - Year selector lists current + years-with-data and refetches on change
 *   - Money is GBP regardless of any display-currency context
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { TaxPage } from "@/pages/TaxPage";
import {
  fetchTaxSummary,
  fetchTaxDisposals,
  fetchTaxPools,
  fetchTaxYears,
  type TaxSummary,
  type TaxDisposal,
  type S104Pool,
  type TaxYears,
} from "@/api/tax";

vi.mock("@/api/tax", () => ({
  fetchTaxSummary: vi.fn(),
  fetchTaxDisposals: vi.fn(),
  fetchTaxPools: vi.fn(),
  fetchTaxYears: vi.fn(),
}));

const mockedSummary = vi.mocked(fetchTaxSummary);
const mockedDisposals = vi.mocked(fetchTaxDisposals);
const mockedPools = vi.mocked(fetchTaxPools);
const mockedYears = vi.mocked(fetchTaxYears);

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

function makeSummary(overrides: Partial<TaxSummary> = {}): TaxSummary {
  return {
    tax_year: "2026/27",
    total_gains_gbp: 5000,
    total_losses_gbp: -1200,
    net_gain_gbp: 3800,
    dividend_total_gbp: 450,
    disposals_same_day: 1,
    disposals_bed_and_breakfast: 2,
    disposals_s104: 3,
    annual_exempt_gbp: 3000,
    exempt_remaining_gbp: 0,
    estimated_cgt_basic_scenario: 160,
    estimated_cgt_higher_scenario: 320,
    ...overrides,
  };
}

function makeDisposal(overrides: Partial<TaxDisposal> = {}): TaxDisposal {
  return {
    match_id: 1,
    instrument_id: 42,
    symbol: "AAPL",
    matching_rule: "s104_pool",
    matched_units: 10,
    acquisition_cost_gbp: 1000,
    disposal_proceeds_gbp: 1500,
    gain_or_loss_gbp: 500,
    disposal_uk_date: "2026-06-01",
    tax_year: "2026/27",
    disposal_tax_lot_id: 900,
    acquisition_tax_lot_id: 800,
    matched_at: "2026-06-02T09:00:00Z",
    ...overrides,
  };
}

function makePool(overrides: Partial<S104Pool> = {}): S104Pool {
  return {
    instrument_id: 42,
    symbol: "AAPL",
    pool_units: 25,
    pool_cost_gbp: 2500,
    pool_avg_cost_gbp: 100,
    updated_at: "2026-06-02T09:00:00Z",
    ...overrides,
  };
}

const YEARS: TaxYears = { current: "2026/27", available: ["2026/27", "2025/26"] };

// ---------------------------------------------------------------------------
// Setup
// ---------------------------------------------------------------------------

beforeEach(() => {
  mockedYears.mockResolvedValue(YEARS);
  mockedSummary.mockResolvedValue(makeSummary());
  mockedDisposals.mockResolvedValue([makeDisposal()]);
  mockedPools.mockResolvedValue([makePool()]);
});

afterEach(() => {
  vi.restoreAllMocks();
});

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("TaxPage — summary", () => {
  it("renders headline metrics in GBP", async () => {
    render(<TaxPage />);
    expect(await screen.findByText("Net gain")).toBeInTheDocument();
    expect(screen.getByText("£3,800.00")).toBeInTheDocument(); // net gain, GBP
    expect(screen.getByText("£5,000.00")).toBeInTheDocument(); // gains
    expect(screen.getByText("-£1,200.00")).toBeInTheDocument(); // losses
    expect(screen.getByText("£160.00")).toBeInTheDocument(); // est CGT basic
  });

  it("shows 'above the allowance' when net gain exceeds the exemption", async () => {
    render(<TaxPage />);
    // net 3800 > 3000 allowance → taxable 800 over
    expect(await screen.findByText(/above the allowance/)).toBeInTheDocument();
    expect(screen.getByText(/£800\.00 above the allowance/)).toBeInTheDocument();
  });

  it("shows remaining allowance when net gain is below the exemption", async () => {
    mockedSummary.mockResolvedValue(
      makeSummary({ net_gain_gbp: 1000, total_gains_gbp: 1000, exempt_remaining_gbp: 2000 }),
    );
    render(<TaxPage />);
    expect(await screen.findByText(/£2,000\.00 of allowance remaining/)).toBeInTheDocument();
  });
});

describe("TaxPage — disposals", () => {
  it("renders a disposal row with a human rule label", async () => {
    render(<TaxPage />);
    expect(await screen.findByText("Section 104 pool")).toBeInTheDocument();
    const rows = screen.getAllByRole("row");
    const aaplRow = rows.find((r) => within(r).queryByText("AAPL") !== null)!;
    expect(within(aaplRow).getByText("£500.00")).toBeInTheDocument();
  });

  it("offers a CSV export when disposals exist", async () => {
    render(<TaxPage />);
    expect(await screen.findByRole("button", { name: /export csv/i })).toBeInTheDocument();
  });

  it("shows an empty state and no CSV button when there are no disposals", async () => {
    mockedDisposals.mockResolvedValue([]);
    render(<TaxPage />);
    expect(await screen.findByText("No disposals in this tax year")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /export csv/i })).toBeNull();
  });

  it("shows a retry on disposals fetch failure", async () => {
    mockedDisposals.mockRejectedValue(new Error("boom"));
    render(<TaxPage />);
    expect(await screen.findByRole("button", { name: /retry/i })).toBeInTheDocument();
  });
});

describe("TaxPage — pools", () => {
  it("renders a Section 104 pool row", async () => {
    render(<TaxPage />);
    await screen.findByText("Section 104 pools");
    const rows = screen.getAllByRole("row");
    const poolRow = rows.find((r) => within(r).queryByText("£2,500.00") !== null)!;
    expect(poolRow).toBeTruthy();
  });

  it("shows an empty state when there are no open pools", async () => {
    mockedPools.mockResolvedValue([]);
    render(<TaxPage />);
    expect(await screen.findByText("No open pools")).toBeInTheDocument();
  });
});

describe("TaxPage — year selector", () => {
  it("lists the current year plus years with data (de-duplicated)", async () => {
    render(<TaxPage />);
    await screen.findByText("Net gain");
    const select = screen.getByRole("combobox");
    const options = within(select).getAllByRole("option").map((o) => o.textContent);
    expect(options).toEqual(["2026/27", "2025/26"]);
  });

  it("surfaces an inline retry (not the top banner) when only tax-years fails", async () => {
    mockedYears.mockRejectedValue(new Error("years down"));
    render(<TaxPage />);
    // Summary still renders off the current-year default...
    expect(await screen.findByText("Net gain")).toBeInTheDocument();
    // ...and the selector owns its own retry, without the page-level banner.
    expect(
      screen.getByRole("button", { name: /tax years unavailable/i }),
    ).toBeInTheDocument();
    expect(screen.queryByText(/Failed to load tax data/)).toBeNull();
  });

  it("refetches summary + disposals for the chosen year", async () => {
    const user = userEvent.setup();
    render(<TaxPage />);
    await screen.findByText("Net gain");
    mockedSummary.mockClear();
    mockedDisposals.mockClear();

    await user.selectOptions(screen.getByRole("combobox"), "2025/26");

    expect(mockedSummary).toHaveBeenCalledWith("2025/26");
    expect(mockedDisposals).toHaveBeenCalledWith("2025/26");
  });
});

describe("TaxPage — total failure", () => {
  it("shows the page-level banner only when every source fails", async () => {
    mockedYears.mockRejectedValue(new Error("x"));
    mockedSummary.mockRejectedValue(new Error("x"));
    mockedDisposals.mockRejectedValue(new Error("x"));
    mockedPools.mockRejectedValue(new Error("x"));
    render(<TaxPage />);
    expect(await screen.findByText(/Failed to load tax data/)).toBeInTheDocument();
  });
});
