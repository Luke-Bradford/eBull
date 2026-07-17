import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { describe, expect, it, vi, beforeEach } from "vitest";

import { ThesesPage } from "@/pages/ThesesPage";
import type { ThesisLibraryItem, ThesisLibraryResponse } from "@/api/types";

vi.mock("@/api/theses", () => ({
  fetchThesesLibrary: vi.fn(),
  generateInstrumentThesis: vi.fn(),
}));

import * as thesesApi from "@/api/theses";

const mockedFetch = vi.mocked(thesesApi.fetchThesesLibrary);
const mockedGenerate = vi.mocked(thesesApi.generateInstrumentThesis);

function makeItem(overrides: Partial<ThesisLibraryItem> = {}): ThesisLibraryItem {
  return {
    instrument_id: 42,
    symbol: "AAPL",
    company_name: "Apple Inc.",
    thesis_id: 1,
    thesis_version: 2,
    thesis_type: "compounder",
    stance: "buy",
    confidence_score: 0.9,
    buy_zone_low: 180,
    buy_zone_high: 210,
    created_at: new Date(Date.now() - 3 * 60 * 60 * 1000).toISOString(),
    critic_verdict: "Weak challenge",
    stale_reason: null,
    stale_detail: null,
    is_held: true,
    latest_score: 0.61,
    latest_rank: 7,
    run_status: "ok",
    run_error: null,
    run_trigger: "manual",
    run_started_at: null,
    last_change_summary: null,
    last_change_material: false,
    ...overrides,
  };
}

function respond(items: ThesisLibraryItem[], total = items.length): ThesisLibraryResponse {
  return { items, total, offset: 0, limit: 50 };
}

function renderPage(initialEntry = "/theses") {
  return render(
    <MemoryRouter initialEntries={[initialEntry]}>
      <ThesesPage />
    </MemoryRouter>,
  );
}

beforeEach(() => {
  vi.clearAllMocks();
  mockedGenerate.mockResolvedValue({
    cached: false,
    thesis: {} as never,
  });
});

describe("ThesesPage", () => {
  it("renders library rows with stance, critic verdict, held pill and score", async () => {
    mockedFetch.mockResolvedValue(
      respond([
        makeItem(),
        makeItem({
          instrument_id: 43,
          symbol: "GME",
          company_name: "GameStop",
          stance: "watch",
          critic_verdict: null,
          is_held: false,
          stale_reason: "event_new_10k",
          run_status: "failed",
          run_error: "writer schema error",
          latest_score: null,
          latest_rank: null,
        }),
      ]),
    );
    renderPage();

    expect(await screen.findByText("AAPL")).toBeInTheDocument();
    expect(screen.getByText("Apple Inc.")).toBeInTheDocument();
    expect(screen.getByText("Weak challenge")).toBeInTheDocument();
    expect(screen.getByText("held")).toBeInTheDocument();
    expect(screen.getByText("#7")).toBeInTheDocument();
    // Second row: no critic, stale from a new filing, failed run.
    expect(screen.getByText("no critic")).toBeInTheDocument();
    expect(screen.getByText("new 10-K")).toBeInTheDocument();
    expect(screen.getByText("failed")).toBeInTheDocument();
    // Symbol links to the instrument's Verdict tab.
    expect(screen.getByRole("link", { name: "AAPL" })).toHaveAttribute(
      "href",
      "/instrument/AAPL?tab=verdict",
    );
  });

  it("shows the guiding empty state when no theses exist", async () => {
    mockedFetch.mockResolvedValue(respond([]));
    renderPage();
    expect(await screen.findByText("No theses")).toBeInTheDocument();
    expect(screen.getByText(/thesis_refresh/)).toBeInTheDocument();
  });

  it("reads filters from the URL and passes them to the API", async () => {
    mockedFetch.mockResolvedValue(respond([]));
    renderPage("/theses?held=true&stale=true&stance=buy");
    await screen.findByText("No theses match these filters.");
    expect(mockedFetch).toHaveBeenCalledWith({
      heldOnly: true,
      stale: true,
      stance: "buy",
      offset: 0,
      limit: 50,
    });
  });

  it("fires a forced regeneration from the per-row Refresh button", async () => {
    mockedFetch.mockResolvedValue(respond([makeItem()]));
    renderPage();
    const btn = await screen.findByRole("button", { name: "Refresh" });
    await userEvent.click(btn);
    expect(mockedGenerate).toHaveBeenCalledWith("AAPL", true);
  });

  it("shows a fixed-phrase notice when the regeneration request fails", async () => {
    mockedFetch.mockResolvedValue(respond([makeItem()]));
    mockedGenerate.mockRejectedValue(new Error("boom"));
    const errSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    renderPage();
    await userEvent.click(await screen.findByRole("button", { name: "Refresh" }));
    expect(
      await screen.findByText(/Generation request for AAPL failed/),
    ).toBeInTheDocument();
    // Fixed phrase only — never exception text in the DOM.
    expect(screen.queryByText(/boom/)).not.toBeInTheDocument();
    errSpy.mockRestore();
  });

  it("disables the row action while a run is in flight", async () => {
    mockedFetch.mockResolvedValue(
      respond([makeItem({ run_status: "running" })]),
    );
    renderPage();
    const btn = await screen.findByRole("button", { name: "Generating…" });
    expect(btn).toBeDisabled();
  });

  it("renders a held-no-thesis gap row with a Generate action", async () => {
    mockedFetch.mockResolvedValue(
      respond([
        makeItem({
          thesis_id: null,
          thesis_version: null,
          thesis_type: null,
          stance: null,
          confidence_score: null,
          buy_zone_low: null,
          buy_zone_high: null,
          created_at: null,
          critic_verdict: null,
          stale_reason: "no_thesis",
          latest_score: null,
          latest_rank: null,
          run_status: null,
        }),
      ]),
    );
    renderPage();
    expect(await screen.findByText("no thesis yet")).toBeInTheDocument();
    expect(screen.getByText("missing")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Generate" })).toBeEnabled();
  });

  it("clamps an out-of-range offset back to page one instead of stranding an empty state", async () => {
    // offset=9999 with total=1: server returns an empty page — the page
    // must reset offset and refetch rather than show a dead empty state.
    mockedFetch
      .mockResolvedValueOnce({ items: [], total: 1, offset: 9999, limit: 50 })
      .mockResolvedValue(respond([makeItem()]));
    renderPage("/theses?offset=9999");
    expect(await screen.findByText("AAPL")).toBeInTheDocument();
    expect(mockedFetch).toHaveBeenLastCalledWith(
      expect.objectContaining({ offset: 0 }),
    );
  });
});
