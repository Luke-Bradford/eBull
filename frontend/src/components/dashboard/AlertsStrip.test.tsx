import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { describe, expect, it, vi, beforeEach } from "vitest";

import { AlertsStrip } from "@/components/dashboard/AlertsStrip";
import type { GuardRejectionsResponse } from "@/api/types";

vi.mock("@/api/alerts", () => ({
  fetchGuardRejections: vi.fn(),
  markAlertsSeen: vi.fn(),
  dismissAllAlerts: vi.fn(),
}));

import * as alertsApi from "@/api/alerts";

const baseRow = {
  decision_id: 501,
  decision_time: new Date(Date.now() - 5 * 60 * 1000).toISOString(),
  instrument_id: 42,
  symbol: "AAPL",
  action: "BUY" as const,
  explanation: "FAIL — cash_available: need £200, have £50",
};

const mockedFetch = vi.mocked(alertsApi.fetchGuardRejections);

function stubFetch(data: GuardRejectionsResponse) {
  mockedFetch.mockResolvedValue(data);
}

function stubFetchError() {
  mockedFetch.mockRejectedValue(new Error("boom"));
}

function renderStrip() {
  return render(
    <MemoryRouter>
      <AlertsStrip />
    </MemoryRouter>,
  );
}

beforeEach(() => {
  vi.clearAllMocks();
});

describe("AlertsStrip", () => {
  it("renders nothing when rejections list is empty", async () => {
    stubFetch({
      alerts_last_seen_decision_id: null,
      unseen_count: 0,
      rejections: [],
    });
    const { container } = renderStrip();
    await vi.waitFor(() => {
      expect(alertsApi.fetchGuardRejections).toHaveBeenCalled();
    });
    expect(container).toBeEmptyDOMElement();
  });

  it("renders nothing on fetch error (silent-on-error)", async () => {
    stubFetchError();
    const { container } = renderStrip();
    await vi.waitFor(() => {
      expect(alertsApi.fetchGuardRejections).toHaveBeenCalled();
    });
    expect(container).toBeEmptyDOMElement();
  });

  it("renders row symbol / action / explanation", async () => {
    stubFetch({
      alerts_last_seen_decision_id: null,
      unseen_count: 1,
      rejections: [baseRow],
    });
    renderStrip();
    expect(await screen.findByText("AAPL")).toBeInTheDocument();
    expect(screen.getByText("BUY")).toBeInTheDocument();
    expect(
      screen.getByText(/cash_available: need £200/),
    ).toBeInTheDocument();
  });

  it("wraps row in a Link when instrument_id is non-null", async () => {
    stubFetch({
      alerts_last_seen_decision_id: null,
      unseen_count: 1,
      rejections: [baseRow],
    });
    renderStrip();
    const link = await screen.findByRole("link");
    expect(link.getAttribute("href")).toBe("/instruments/42");
  });

  it("renders plain row (no link) when instrument_id is null", async () => {
    stubFetch({
      alerts_last_seen_decision_id: null,
      unseen_count: 1,
      rejections: [{ ...baseRow, instrument_id: null, symbol: null }],
    });
    renderStrip();
    await screen.findByText(/cash_available/);
    expect(screen.queryByRole("link")).toBeNull();
  });

  it("applies amber border for unseen rows, slate for seen", async () => {
    stubFetch({
      alerts_last_seen_decision_id: 500,
      unseen_count: 1,
      rejections: [
        { ...baseRow, decision_id: 501 },  // unseen (501 > 500)
        { ...baseRow, decision_id: 499 },  // seen (499 <= 500)
      ],
    });
    renderStrip();
    const rows = await screen.findAllByTestId("alerts-row");
    expect(rows[0]!.className).toMatch(/border-amber/);
    expect(rows[1]!.className).toMatch(/border-slate/);
  });

  it("shows unseen_count pill when unseen_count > 0", async () => {
    stubFetch({
      alerts_last_seen_decision_id: null,
      unseen_count: 3,
      rejections: [baseRow],
    });
    renderStrip();
    expect(await screen.findByText(/3 new/)).toBeInTheDocument();
  });

  it("omits unseen_count pill when unseen_count === 0", async () => {
    stubFetch({
      alerts_last_seen_decision_id: 999,
      unseen_count: 0,
      rejections: [baseRow],  // still shown, just all seen
    });
    renderStrip();
    await screen.findByText("AAPL");
    expect(screen.queryByText(/new$/)).toBeNull();
  });

  it("truncates explanation visually but preserves full text in title", async () => {
    const long = "FAIL — cash_available: need £200; thesis_stale: 14 days old; spread_wide: 0.12%";
    stubFetch({
      alerts_last_seen_decision_id: null,
      unseen_count: 1,
      rejections: [{ ...baseRow, explanation: long }],
    });
    renderStrip();
    const node = await screen.findByText(long);
    expect(node.getAttribute("title")).toBe(long);
    expect(node.className).toMatch(/truncate/);
  });
});
