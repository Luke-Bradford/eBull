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
      ],
    },
  ],
  ex_dividends: [{ symbol: "FOO", instrument_id: 1, ex_date: "2026-07-01", pay_date: "2026-07-15" }],
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
    // day-type labels render (open + closed in the week strip).
    expect(screen.getAllByText("Open").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Closed").length).toBeGreaterThan(0);
    // upcoming ex-dividend row.
    expect(screen.getByText("FOO")).toBeInTheDocument();
    expect(screen.getByText(/ex 2026-07-01/)).toBeInTheDocument();
    // the honest "not ingested" note about earnings/filings.
    expect(screen.getByText(/does not ingest forward earnings/i)).toBeInTheDocument();
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

    await userEvent.click(screen.getByRole("button", { name: "4 weeks" }));
    await waitFor(() => expect(fetchCalendarEvents).toHaveBeenCalledWith("portfolio", 28));
  });
});
