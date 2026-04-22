import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { describe, expect, it, vi, beforeEach } from "vitest";

import { AlertsStrip } from "@/components/dashboard/AlertsStrip";
import type {
  CoverageStatusDrop,
  CoverageStatusDropsResponse,
  GuardRejection,
  GuardRejectionsResponse,
  PositionAlert,
  PositionAlertsResponse,
} from "@/api/types";

vi.mock("@/api/alerts", () => ({
  fetchGuardRejections: vi.fn(),
  markAlertsSeen: vi.fn(),
  dismissAllAlerts: vi.fn(),
  fetchPositionAlerts: vi.fn(),
  markPositionAlertsSeen: vi.fn(),
  dismissAllPositionAlerts: vi.fn(),
  fetchCoverageStatusDrops: vi.fn(),
  markCoverageStatusDropsSeen: vi.fn(),
  dismissAllCoverageStatusDrops: vi.fn(),
}));

import * as alertsApi from "@/api/alerts";

const mockedGuardFetch = vi.mocked(alertsApi.fetchGuardRejections);
const mockedPositionFetch = vi.mocked(alertsApi.fetchPositionAlerts);
const mockedCoverageFetch = vi.mocked(alertsApi.fetchCoverageStatusDrops);

const mockedMarkGuard = vi.mocked(alertsApi.markAlertsSeen);
const mockedMarkPosition = vi.mocked(alertsApi.markPositionAlertsSeen);
const mockedMarkCoverage = vi.mocked(alertsApi.markCoverageStatusDropsSeen);

const mockedDismissGuard = vi.mocked(alertsApi.dismissAllAlerts);
const mockedDismissPosition = vi.mocked(alertsApi.dismissAllPositionAlerts);
const mockedDismissCoverage = vi.mocked(alertsApi.dismissAllCoverageStatusDrops);

const EMPTY_GUARD: GuardRejectionsResponse = {
  alerts_last_seen_decision_id: null,
  unseen_count: 0,
  rejections: [],
};
const EMPTY_POSITION: PositionAlertsResponse = {
  alerts_last_seen_position_alert_id: null,
  unseen_count: 0,
  alerts: [],
};
const EMPTY_COVERAGE: CoverageStatusDropsResponse = {
  alerts_last_seen_coverage_event_id: null,
  unseen_count: 0,
  drops: [],
};

function stubAll(
  overrides: {
    guard?: Partial<GuardRejectionsResponse> | Error;
    position?: Partial<PositionAlertsResponse> | Error;
    coverage?: Partial<CoverageStatusDropsResponse> | Error;
  } = {},
) {
  if (overrides.guard instanceof Error) {
    mockedGuardFetch.mockRejectedValue(overrides.guard);
  } else {
    mockedGuardFetch.mockResolvedValue({ ...EMPTY_GUARD, ...overrides.guard });
  }
  if (overrides.position instanceof Error) {
    mockedPositionFetch.mockRejectedValue(overrides.position);
  } else {
    mockedPositionFetch.mockResolvedValue({ ...EMPTY_POSITION, ...overrides.position });
  }
  if (overrides.coverage instanceof Error) {
    mockedCoverageFetch.mockRejectedValue(overrides.coverage);
  } else {
    mockedCoverageFetch.mockResolvedValue({ ...EMPTY_COVERAGE, ...overrides.coverage });
  }
}

function makeGuard(overrides: Partial<GuardRejection> = {}): GuardRejection {
  return {
    decision_id: 501,
    decision_time: new Date(Date.now() - 5 * 60 * 1000).toISOString(),
    instrument_id: 42,
    symbol: "AAPL",
    action: "BUY",
    explanation: "FAIL — cash_available: need £200, have £50",
    ...overrides,
  };
}

function makePosition(overrides: Partial<PositionAlert> = {}): PositionAlert {
  return {
    alert_id: 701,
    alert_type: "sl_breach",
    instrument_id: 43,
    symbol: "MSFT",
    opened_at: new Date(Date.now() - 10 * 60 * 1000).toISOString(),
    resolved_at: null,
    detail: "bid=320 < sl=330",
    current_bid: "320",
    ...overrides,
  };
}

function makeCoverage(overrides: Partial<CoverageStatusDrop> = {}): CoverageStatusDrop {
  return {
    event_id: 301,
    instrument_id: 44,
    symbol: "TSLA",
    changed_at: new Date(Date.now() - 30 * 60 * 1000).toISOString(),
    old_status: "analysable",
    new_status: "insufficient",
    ...overrides,
  };
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

// ---------------------------------------------------------------------------
// Basic rendering + hide policy
// ---------------------------------------------------------------------------

describe("AlertsStrip — basic rendering", () => {
  it("renders nothing when all feeds are empty", async () => {
    stubAll();
    const { container } = renderStrip();
    await vi.waitFor(() => {
      expect(mockedGuardFetch).toHaveBeenCalled();
      expect(mockedPositionFetch).toHaveBeenCalled();
      expect(mockedCoverageFetch).toHaveBeenCalled();
    });
    expect(container).toBeEmptyDOMElement();
  });

  it("renders nothing when all feeds error (silent-on-error)", async () => {
    stubAll({
      guard: new Error("boom"),
      position: new Error("boom"),
      coverage: new Error("boom"),
    });
    const errSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    const { container } = renderStrip();
    await vi.waitFor(() => {
      expect(mockedGuardFetch).toHaveBeenCalled();
    });
    expect(container).toBeEmptyDOMElement();
    errSpy.mockRestore();
  });

  it("renders nothing while any feed is still loading (no flash)", () => {
    mockedGuardFetch.mockResolvedValue(EMPTY_GUARD);
    mockedPositionFetch.mockImplementation(() => new Promise(() => {})); // never resolves
    mockedCoverageFetch.mockResolvedValue(EMPTY_COVERAGE);
    const { container } = renderStrip();
    expect(container).toBeEmptyDOMElement();
  });

  it("renders ok feeds when one feed errored", async () => {
    stubAll({
      guard: new Error("boom"),
      position: { alerts: [makePosition({ symbol: "MSFT" })], unseen_count: 1 },
      coverage: { drops: [makeCoverage({ symbol: "TSLA" })], unseen_count: 1 },
    });
    const errSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    renderStrip();
    expect(await screen.findByText("MSFT")).toBeInTheDocument();
    expect(screen.getByText("TSLA")).toBeInTheDocument();
    errSpy.mockRestore();
  });
});

// ---------------------------------------------------------------------------
// Merge + ordering
// ---------------------------------------------------------------------------

describe("AlertsStrip — merge + ordering", () => {
  it("merges three feeds into a single list DESC by timestamp", async () => {
    const coverage = makeCoverage({
      symbol: "TSLA",
      changed_at: new Date(Date.now() - 30 * 60 * 1000).toISOString(),
    });
    const guard = makeGuard({
      symbol: "AAPL",
      decision_time: new Date(Date.now() - 5 * 60 * 1000).toISOString(),
    });
    const position = makePosition({
      symbol: "MSFT",
      opened_at: new Date(Date.now() - 15 * 60 * 1000).toISOString(),
    });
    stubAll({
      guard: { rejections: [guard], unseen_count: 1 },
      position: { alerts: [position], unseen_count: 1 },
      coverage: { drops: [coverage], unseen_count: 1 },
    });
    renderStrip();
    const rows = await screen.findAllByTestId("alerts-row");
    expect(rows).toHaveLength(3);
    expect(rows[0]!.textContent).toContain("AAPL"); // 5 min ago (newest)
    expect(rows[1]!.textContent).toContain("MSFT"); // 15 min ago
    expect(rows[2]!.textContent).toContain("TSLA"); // 30 min ago
  });
});

// ---------------------------------------------------------------------------
// Per-kind rendering + click-through
// ---------------------------------------------------------------------------

describe("AlertsStrip — per-kind rendering", () => {
  it("renders kind pill badge for each row type", async () => {
    stubAll({
      guard: { rejections: [makeGuard()], unseen_count: 1 },
      position: { alerts: [makePosition()], unseen_count: 1 },
      coverage: { drops: [makeCoverage()], unseen_count: 1 },
    });
    renderStrip();
    expect(await screen.findByText("GUARD")).toBeInTheDocument();
    expect(screen.getByText("POSITION")).toBeInTheDocument();
    expect(screen.getByText("COVERAGE")).toBeInTheDocument();
  });

  it("links rows with non-null instrument_id to /instruments/<id>", async () => {
    stubAll({
      guard: { rejections: [makeGuard({ instrument_id: 42 })], unseen_count: 1 },
      position: { alerts: [makePosition({ instrument_id: 43 })], unseen_count: 1 },
      coverage: { drops: [makeCoverage({ instrument_id: 44 })], unseen_count: 1 },
    });
    renderStrip();
    await screen.findAllByTestId("alerts-row");
    const links = screen
      .getAllByRole("link")
      .map((l) => l.getAttribute("href"));
    expect(links).toEqual(
      expect.arrayContaining(["/instruments/42", "/instruments/43", "/instruments/44"]),
    );
  });

  it("renders guard row with null instrument_id inline (no link)", async () => {
    stubAll({
      guard: {
        rejections: [makeGuard({ instrument_id: null, symbol: null })],
        unseen_count: 1,
      },
    });
    renderStrip();
    const row = await screen.findByTestId("alerts-row");
    expect(row.closest("a")).toBeNull();
  });

  it("guard row shows symbol / action / explanation", async () => {
    stubAll({
      guard: { rejections: [makeGuard()], unseen_count: 1 },
    });
    renderStrip();
    expect(await screen.findByText("AAPL")).toBeInTheDocument();
    expect(screen.getByText("BUY")).toBeInTheDocument();
    expect(screen.getByText(/cash_available: need £200/)).toBeInTheDocument();
  });

  it("position row shows symbol / alert_type label / detail", async () => {
    stubAll({
      position: {
        alerts: [makePosition({ alert_type: "sl_breach", detail: "bid=320 < sl=330" })],
        unseen_count: 1,
      },
    });
    renderStrip();
    expect(await screen.findByText("MSFT")).toBeInTheDocument();
    expect(screen.getByText("SL")).toBeInTheDocument();
    expect(screen.getByText("bid=320 < sl=330")).toBeInTheDocument();
  });

  it("coverage row shows symbol and old → new transition", async () => {
    stubAll({
      coverage: {
        drops: [makeCoverage({ old_status: "analysable", new_status: "insufficient" })],
        unseen_count: 1,
      },
    });
    renderStrip();
    expect(await screen.findByText("TSLA")).toBeInTheDocument();
    expect(screen.getByText("analysable → insufficient")).toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// Unseen cursor math (per-kind)
// ---------------------------------------------------------------------------

describe("AlertsStrip — per-kind unseen cursor", () => {
  it("applies amber border for unseen guard rows and slate for seen", async () => {
    stubAll({
      guard: {
        alerts_last_seen_decision_id: 500,
        unseen_count: 1,
        rejections: [
          makeGuard({ decision_id: 501 }), // unseen
          makeGuard({ decision_id: 499 }), // seen
        ],
      },
    });
    renderStrip();
    const rows = await screen.findAllByTestId("alerts-row");
    expect(rows[0]!.className).toMatch(/border-amber/);
    expect(rows[1]!.className).toMatch(/border-slate/);
  });

  it("per-kind cursor: position cursor only affects position rows", async () => {
    stubAll({
      position: {
        alerts_last_seen_position_alert_id: 700,
        unseen_count: 1,
        alerts: [
          makePosition({ alert_id: 701 }), // unseen (701 > 700)
        ],
      },
      coverage: {
        alerts_last_seen_coverage_event_id: null,
        unseen_count: 1,
        drops: [makeCoverage({ event_id: 301 })], // unseen (cursor null)
      },
    });
    renderStrip();
    const rows = await screen.findAllByTestId("alerts-row");
    for (const r of rows) {
      expect(r.className).toMatch(/border-amber/);
    }
  });
});

// ---------------------------------------------------------------------------
// Header badge + totals
// ---------------------------------------------------------------------------

describe("AlertsStrip — header totals", () => {
  it("header badge is sum of per-feed unseen_count", async () => {
    stubAll({
      guard: { rejections: [makeGuard()], unseen_count: 3 },
      position: { alerts: [makePosition()], unseen_count: 2 },
      coverage: { drops: [makeCoverage()], unseen_count: 1 },
    });
    renderStrip();
    expect(await screen.findByText("6 new")).toBeInTheDocument();
  });

  it("omits badge when no unseen across any feed", async () => {
    stubAll({
      guard: {
        alerts_last_seen_decision_id: 999,
        unseen_count: 0,
        rejections: [makeGuard({ decision_id: 500 })],
      },
    });
    renderStrip();
    await screen.findByText("AAPL");
    expect(screen.queryByText(/\bnew\b/)).toBeNull();
  });

  it("heading text is 'Alerts'", async () => {
    stubAll({
      guard: { rejections: [makeGuard()], unseen_count: 1 },
    });
    renderStrip();
    expect(await screen.findByRole("heading", { name: "Alerts" })).toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// Per-feed overflow + ack-button gating
// ---------------------------------------------------------------------------

describe("AlertsStrip — per-feed overflow", () => {
  it("shows Dismiss-all when any feed has unseen > rendered (per-feed overflow)", async () => {
    stubAll({
      guard: { rejections: [makeGuard()], unseen_count: 2 }, // overflow (2 > 1)
    });
    renderStrip();
    expect(await screen.findByRole("button", { name: /Dismiss all/i })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /Mark all read/i })).toBeNull();
  });

  it("shows Mark-all-read when no feed overflows and totalUnseen > 0", async () => {
    stubAll({
      guard: { rejections: [makeGuard()], unseen_count: 1 },
      position: { alerts: [makePosition()], unseen_count: 1 },
    });
    renderStrip();
    expect(await screen.findByRole("button", { name: /Mark all read/i })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /Dismiss all/i })).toBeNull();
  });

  it("hides both buttons when totalUnseen === 0", async () => {
    stubAll({
      guard: {
        alerts_last_seen_decision_id: 999,
        unseen_count: 0,
        rejections: [makeGuard({ decision_id: 500 })],
      },
    });
    renderStrip();
    await screen.findByText("AAPL");
    expect(screen.queryByRole("button", { name: /Mark all read/i })).toBeNull();
    expect(screen.queryByRole("button", { name: /Dismiss all/i })).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// Mark-all-read fan-out
// ---------------------------------------------------------------------------

describe("AlertsStrip — Mark all read", () => {
  it("fans out to each non-empty feed with its MAX id", async () => {
    stubAll({
      guard: { rejections: [makeGuard({ decision_id: 510 })], unseen_count: 1 },
      position: { alerts: [makePosition({ alert_id: 710 })], unseen_count: 1 },
    });
    mockedMarkGuard.mockResolvedValue(undefined);
    mockedMarkPosition.mockResolvedValue(undefined);
    renderStrip();
    const btn = await screen.findByRole("button", { name: /Mark all read/i });
    await userEvent.click(btn);
    await vi.waitFor(() => {
      expect(mockedMarkGuard).toHaveBeenCalledWith(510);
      expect(mockedMarkPosition).toHaveBeenCalledWith(710);
    });
    expect(mockedMarkCoverage).not.toHaveBeenCalled();
  });

  it("tolerates a single POST failure and still calls others", async () => {
    stubAll({
      guard: { rejections: [makeGuard({ decision_id: 510 })], unseen_count: 1 },
      position: { alerts: [makePosition({ alert_id: 710 })], unseen_count: 1 },
    });
    mockedMarkGuard.mockRejectedValue(new Error("guard seen boom"));
    mockedMarkPosition.mockResolvedValue(undefined);
    const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    renderStrip();
    await userEvent.click(await screen.findByRole("button", { name: /Mark all read/i }));
    await vi.waitFor(() => {
      expect(mockedMarkGuard).toHaveBeenCalled();
      expect(mockedMarkPosition).toHaveBeenCalled();
    });
    expect(consoleSpy).toHaveBeenCalled();
    consoleSpy.mockRestore();
  });
});

// ---------------------------------------------------------------------------
// Dismiss-all fan-out
// ---------------------------------------------------------------------------

describe("AlertsStrip — Dismiss all", () => {
  it("skips POST for any errored feed and fans out to ok feeds", async () => {
    stubAll({
      guard: new Error("boom"),
      position: { alerts: [makePosition()], unseen_count: 5 }, // overflow (5 > 1)
    });
    mockedDismissPosition.mockResolvedValue(undefined);
    mockedDismissCoverage.mockResolvedValue(undefined);
    const confirmSpy = vi.spyOn(window, "confirm").mockReturnValue(true);
    const errSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    renderStrip();
    const btn = await screen.findByRole("button", { name: /Dismiss all/i });
    await userEvent.click(btn);
    await vi.waitFor(() => {
      expect(mockedDismissPosition).toHaveBeenCalled();
    });
    expect(mockedDismissGuard).not.toHaveBeenCalled();
    expect(mockedDismissCoverage).toHaveBeenCalled();
    confirmSpy.mockRestore();
    errSpy.mockRestore();
  });

  it("no-op when operator cancels confirm", async () => {
    stubAll({
      guard: { rejections: [makeGuard()], unseen_count: 5 },
    });
    const confirmSpy = vi.spyOn(window, "confirm").mockReturnValue(false);
    renderStrip();
    await userEvent.click(await screen.findByRole("button", { name: /Dismiss all/i }));
    expect(mockedDismissGuard).not.toHaveBeenCalled();
    confirmSpy.mockRestore();
  });

  it("tolerates a single dismiss POST failure silently", async () => {
    stubAll({
      guard: { rejections: [makeGuard()], unseen_count: 5 },
      position: { alerts: [makePosition()], unseen_count: 1 },
    });
    mockedDismissGuard.mockRejectedValue(new Error("guard dismiss boom"));
    mockedDismissPosition.mockResolvedValue(undefined);
    mockedDismissCoverage.mockResolvedValue(undefined);
    const confirmSpy = vi.spyOn(window, "confirm").mockReturnValue(true);
    const errSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    renderStrip();
    await userEvent.click(await screen.findByRole("button", { name: /Dismiss all/i }));
    await vi.waitFor(() => {
      expect(mockedDismissGuard).toHaveBeenCalled();
      expect(mockedDismissPosition).toHaveBeenCalled();
    });
    expect(errSpy).toHaveBeenCalled();
    confirmSpy.mockRestore();
    errSpy.mockRestore();
  });
});
