import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { MemoryRouter } from "react-router-dom";

import { CalendarPage } from "./CalendarPage";
import { fetchCalendarEvents } from "@/api/calendar";
import type { CalendarEvents } from "@/api/types";

const sample: CalendarEvents = {
  scope: "portfolio",
  as_of: "2026-06-29",
  market_status: [
    {
      profile: "us_equity",
      label: "US equity",
      timezone: "America/New_York",
      holidays_modelled: true,
      week: [
        { date: "2026-06-29", day_type: "open", reason: null },
        { date: "2026-07-03", day_type: "closed", reason: "Independence Day" },
        // half_day + not_modelled render the other two DAY_TYPE_LABEL entries. Without them
        // the #3176 guard below cannot fire: a drift of an unrendered entry to "Open" would
        // leave every assertion passing (measured — probing `not_modelled: "Open"` against the
        // two-day fixture passed 2/2).
        { date: "2026-07-24", day_type: "half_day", reason: null },
        { date: "2026-07-25", day_type: "not_modelled", reason: null },
      ],
    },
  ],
  ex_dividends: [{ symbol: "FOO", instrument_id: 1, ex_date: "2026-07-01", pay_date: "2026-07-15" }],
  expected_filings: [
    {
      symbol: "GME",
      instrument_id: 2,
      filing_type: "10-Q",
      window_start: "2026-07-30",
      window_end: "2026-08-24",
    },
  ],
};

vi.mock("@/api/calendar", () => ({
  fetchCalendarEvents: vi.fn(() => Promise.resolve(sample)),
}));
// useMarketSpecials hits the network for US years; stub it.
vi.mock("@/lib/useMarketSpecials", () => ({
  useMarketSpecials: () => ({ fullClosures: new Set<string>(), halfDays: new Set<string>() }),
}));

describe("CalendarPage", () => {
  it("renders market status + upcoming ex-dividends", async () => {
    render(
      <MemoryRouter>
        <CalendarPage />
      </MemoryRouter>,
    );
    await waitFor(() => expect(screen.getByText("US equity")).toBeInTheDocument());
    // Every DAY_TYPE_LABEL entry the fixture can render. These three carry the guard for
    // their own entries: any of them drifting onto another word fails its own assertion.
    expect(screen.getAllByText("Trading").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Closed").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Half day").length).toBeGreaterThan(0);
    // #3176. The week strip carries two vocabularies three lines apart — the instant one
    // ("Closed now" / "Open · regular hours") and the day one — and they used to share the
    // token "Open", so outside RTH the block contradicted itself about today.
    //
    // ⚠ This line's job is NOT the three entries above; each of those is already guarded by
    // its own presence assertion, which fails FIRST on a drift (revert-probed: reverting
    // `open` to "Open" fails at the `getAllByText("Trading")` line, not here). It catches the
    // case no presence assertion covers — "Open" reappearing as a standalone token anywhere
    // else in the strip, including the unlabelled `not_modelled` entry, which is why the
    // fixture now renders one.
    expect(screen.queryByText("Open")).toBeNull();
    // upcoming ex-dividend row.
    expect(screen.getByText("FOO")).toBeInTheDocument();
    expect(screen.getByText(/ex 2026-07-01/)).toBeInTheDocument();
    // expected-filing row (#1907) renders as an "expected" date range.
    expect(screen.getByText("Expected filings")).toBeInTheDocument();
    expect(screen.getByText("GME")).toBeInTheDocument();
    expect(screen.getByText(/expected .*Jul.*–.*Aug/)).toBeInTheDocument();
    // the honest note: earnings still not ingested.
    expect(screen.getByText(/ingests no forward earnings calendar/i)).toBeInTheDocument();
    // closure reason (#1766) renders on the closed tile.
    expect(screen.getByText("Independence Day")).toBeInTheDocument();
  });

  it("requests the default 1-week horizon, then widens to 4 weeks", async () => {
    render(
      <MemoryRouter>
        <CalendarPage />
      </MemoryRouter>,
    );
    await waitFor(() => expect(screen.getByText("US equity")).toBeInTheDocument());
    expect(fetchCalendarEvents).toHaveBeenCalledWith("portfolio", 7);
    // Default window phrasing tracks the 1-week horizon.
    expect(screen.getByText("Market status — this week")).toBeInTheDocument();

    await userEvent.click(screen.getByRole("button", { name: "4 weeks" }));
    await waitFor(() => expect(fetchCalendarEvents).toHaveBeenCalledWith("portfolio", 28));
    // Section title + intro now reflect the wider horizon, not a stale "this week".
    await waitFor(() => expect(screen.getByText("Market status — next 4 weeks")).toBeInTheDocument());
  });
});
