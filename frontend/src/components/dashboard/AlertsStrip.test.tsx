import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
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

describe("AlertsStrip — Mark all read (normal path)", () => {
  it("renders 'Mark all read' when unseen_count > 0 and <= rejections.length", async () => {
    stubFetch({
      alerts_last_seen_decision_id: 499,
      unseen_count: 2,
      rejections: [
        { ...baseRow, decision_id: 501 },
        { ...baseRow, decision_id: 500 },
      ],
    });
    renderStrip();
    expect(await screen.findByRole("button", { name: /mark all read/i })).toBeInTheDocument();
  });

  it("hides 'Mark all read' when unseen_count === 0 (all rows already seen)", async () => {
    stubFetch({
      alerts_last_seen_decision_id: 999,
      unseen_count: 0,
      rejections: [{ ...baseRow, decision_id: 500 }],  // seen (500 < 999)
    });
    renderStrip();
    await screen.findByText("AAPL");
    expect(screen.queryByRole("button", { name: /mark all read/i })).toBeNull();
  });

  it("stays visible at the 500-row cap when unseen_count === rejections.length === 500", async () => {
    const rejections = Array.from({ length: 500 }, (_, i) => ({
      ...baseRow,
      decision_id: 500 - i,
    }));
    stubFetch({
      alerts_last_seen_decision_id: null,
      unseen_count: 500,
      rejections,
    });
    renderStrip();
    expect(await screen.findByRole("button", { name: /mark all read/i })).toBeInTheDocument();
  });

  it("click posts rejections[0].decision_id and refetches", async () => {
    stubFetch({
      alerts_last_seen_decision_id: 499,
      unseen_count: 2,
      rejections: [
        { ...baseRow, decision_id: 501 },
        { ...baseRow, decision_id: 500 },
      ],
    });
    vi.mocked(alertsApi.markAlertsSeen).mockResolvedValue(undefined);
    renderStrip();

    const btn = await screen.findByRole("button", { name: /mark all read/i });
    await userEvent.click(btn);

    expect(alertsApi.markAlertsSeen).toHaveBeenCalledWith(501);  // MAX(decision_id) in payload
    await vi.waitFor(() => {
      expect(vi.mocked(alertsApi.fetchGuardRejections).mock.calls.length).toBeGreaterThanOrEqual(2);
    });
  });
});

describe("AlertsStrip — Dismiss all (overflow path)", () => {
  function overflowStub() {
    stubFetch({
      alerts_last_seen_decision_id: null,
      unseen_count: 600,
      rejections: Array.from({ length: 500 }, (_, i) => ({
        ...baseRow,
        decision_id: 600 - i,
      })),
    });
  }

  it("renders 'Dismiss all (600) as acknowledged' and a /recommendations link when unseen_count > rejections.length", async () => {
    overflowStub();
    renderStrip();
    expect(
      await screen.findByRole("button", { name: /dismiss all \(600\)/i }),
    ).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /mark all read/i })).toBeNull();
    const recLink = screen.getByRole("link", { name: /recommendations/i });
    expect(recLink.getAttribute("href")).toBe("/recommendations");
  });

  it("confirm dialog: confirm calls dismissAllAlerts + refetch", async () => {
    overflowStub();
    vi.mocked(alertsApi.dismissAllAlerts).mockResolvedValue(undefined);
    const confirmSpy = vi.spyOn(window, "confirm").mockReturnValue(true);
    renderStrip();

    const btn = await screen.findByRole("button", { name: /dismiss all \(600\)/i });
    await userEvent.click(btn);

    expect(confirmSpy).toHaveBeenCalled();
    expect(alertsApi.dismissAllAlerts).toHaveBeenCalled();
    await vi.waitFor(() => {
      expect(vi.mocked(alertsApi.fetchGuardRejections).mock.calls.length).toBeGreaterThanOrEqual(2);
    });
    confirmSpy.mockRestore();
  });

  it("confirm dialog: cancel does NOT call dismissAllAlerts or refetch", async () => {
    overflowStub();
    vi.mocked(alertsApi.dismissAllAlerts).mockResolvedValue(undefined);
    const confirmSpy = vi.spyOn(window, "confirm").mockReturnValue(false);
    renderStrip();

    const fetchCallsBefore = vi.mocked(alertsApi.fetchGuardRejections).mock.calls.length;
    const btn = await screen.findByRole("button", { name: /dismiss all \(600\)/i });
    await userEvent.click(btn);

    expect(confirmSpy).toHaveBeenCalled();
    expect(alertsApi.dismissAllAlerts).not.toHaveBeenCalled();
    expect(vi.mocked(alertsApi.fetchGuardRejections).mock.calls.length).toBe(fetchCallsBefore);
    confirmSpy.mockRestore();
  });
});
