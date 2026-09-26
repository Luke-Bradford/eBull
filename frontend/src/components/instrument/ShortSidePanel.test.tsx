import { describe, expect, it, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";

import * as instrumentsApi from "@/api/instruments";
import type { InstrumentShortVolume } from "@/api/types";
import {
  ShortSidePanel,
  shortInterestUnavailableText,
} from "@/components/instrument/ShortSidePanel";

function volume(overrides: Partial<InstrumentShortVolume> = {}): InstrumentShortVolume {
  return {
    symbol: "GME",
    definition: "Short-sale volume as a share of reported volume.",
    caveats: ["Covers only trades reported to FINRA facilities."],
    latest_trade_date: "2026-09-25",
    days: [
      {
        trade_date: "2026-09-25",
        short_volume: "566400.000000",
        short_exempt_volume: "1200.000000",
        total_volume: "1000000.000000",
        short_volume_share: "0.5664",
        facilities: "B,Q,N",
      },
      {
        trade_date: "2026-09-24",
        short_volume: "400000.000000",
        short_exempt_volume: "0.000000",
        total_volume: "1000000.000000",
        short_volume_share: "0.4",
        facilities: "Q,N",
      },
    ],
    withheld_reason: null,
    ...overrides,
  };
}

describe("ShortSidePanel", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  it("shows both measures, each with its own definition", async () => {
    vi.spyOn(instrumentsApi, "fetchInstrumentShortVolume").mockResolvedValue(volume());
    render(
      <ShortSidePanel
        symbol="GME"
        shortInterest={{ signal: 0.5, short_pct: 0.2012, days_to_cover: 3.25, asof: "2026-09-15" }}
      />,
    );
    expect(screen.getByText("20.12%")).toBeInTheDocument();
    expect(screen.getByText("3.3")).toBeInTheDocument();
    expect(screen.getByText(/FINRA Rule 4560/)).toBeInTheDocument();
    expect(await screen.findByText("56.64%")).toBeInTheDocument();
    expect(screen.getByText("566,400 / 1,000,000")).toBeInTheDocument();
    expect(screen.getByText("B,Q,N")).toBeInTheDocument();
    expect(screen.getByText("Short-sale volume as a share of reported volume.")).toBeInTheDocument();
    expect(screen.getByText("Covers only trades reported to FINRA facilities.")).toBeInTheDocument();
    expect(screen.getByText(/2 trade dates/)).toBeInTheDocument();
  });

  it("shows the withheld reason instead of figures (#3437)", async () => {
    vi.spyOn(instrumentsApi, "fetchInstrumentShortVolume").mockResolvedValue(
      volume({ days: [], latest_trade_date: null, withheld_reason: "Withheld: collision." }),
    );
    render(<ShortSidePanel symbol="TPC" shortInterest={null} />);
    expect(await screen.findByText("Withheld: collision.")).toBeInTheDocument();
    expect(screen.queryByText("No short-sale volume held")).not.toBeInTheDocument();
  });

  it("renders an empty state when FINRA reports nothing", async () => {
    vi.spyOn(instrumentsApi, "fetchInstrumentShortVolume").mockResolvedValue(
      volume({ days: [], latest_trade_date: null }),
    );
    render(<ShortSidePanel symbol="VOD.L" shortInterest={null} />);
    expect(await screen.findByText("No short-sale volume held")).toBeInTheDocument();
  });

  it("renders a retryable error without the exception text", async () => {
    vi.spyOn(instrumentsApi, "fetchInstrumentShortVolume").mockRejectedValue(
      new Error("boom internal"),
    );
    render(<ShortSidePanel symbol="GME" shortInterest={null} />);
    expect(await screen.findByRole("alert")).toBeInTheDocument();
    expect(screen.queryByText(/boom internal/)).not.toBeInTheDocument();
  });

  it("never shows a gated short-interest figure", async () => {
    vi.spyOn(instrumentsApi, "fetchInstrumentShortVolume").mockResolvedValue(volume());
    render(
      <ShortSidePanel
        symbol="GME"
        shortInterest={{ signal: null, reason: "stale_settlement", max_age_days: 20, asof: "2025-01-15" }}
      />,
    );
    expect(screen.getByText(/latest FINRA settlement on file is/)).toBeInTheDocument();
    expect(screen.getByText(/limit 20 days/)).toBeInTheDocument();
    expect(screen.queryByText("days to cover")).not.toBeInTheDocument();
    await screen.findByText("56.64%");
  });
});

describe("shortInterestUnavailableText", () => {
  it("names each gate", () => {
    expect(shortInterestUnavailableText(null)).toMatch(/scoring run/);
    expect(
      shortInterestUnavailableText({ reason: "stale_share_count", shares_outstanding_asof: "2024-02-01" }),
    ).toMatch(/share count it divides by was filed/);
    expect(shortInterestUnavailableText({ reason: "no_short_interest_or_shares" })).toMatch(
      /no short interest/,
    );
  });
});
