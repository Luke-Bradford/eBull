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
    action: "HOLD",
    explanation: "FAIL — kill_switch: kill switch active since 2026-06-28",
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
// #1898 grouping — collapse the flood
// ---------------------------------------------------------------------------

describe("AlertsStrip — grouping (#1898)", () => {
  it("collapses many same-reason guard rejections into ONE card with a symbol summary", async () => {
    // The real dev flood: kill_switch across 4 symbols on several days.
    const symbols = ["BBBY", "IEP", "GME", "VOO"];
    const rejections: GuardRejection[] = [];
    let id = 600;
    for (let day = 0; day < 3; day += 1) {
      for (const s of symbols) {
        rejections.push(
          makeGuard({
            decision_id: id++,
            symbol: s,
            explanation: "FAIL — kill_switch: kill switch active since 2026-06-28",
          }),
        );
      }
    }
    stubAll({ guard: { rejections, unseen_count: rejections.length } });
    renderStrip();
    const rows = await screen.findAllByTestId("alerts-row");
    // 12 emissions → a single grouped card.
    expect(rows).toHaveLength(1);
    expect(rows[0]!.textContent).toContain("Kill switch active");
    expect(rows[0]!.textContent).toContain("×12");
    // All four affected symbols surfaced once each.
    expect(rows[0]!.textContent).toContain("4 symbols:");
    for (const s of symbols) expect(rows[0]!.textContent).toContain(s);
  });

  it("splits guard rejections into one card per distinct reason code", async () => {
    stubAll({
      guard: {
        rejections: [
          makeGuard({ decision_id: 611, symbol: "GME", explanation: "FAIL — kill_switch: active" }),
          makeGuard({ decision_id: 610, symbol: "IEP", explanation: "FAIL — kill_switch: active" }),
          makeGuard({
            decision_id: 609,
            symbol: "GME",
            explanation: "FAIL — auto_trading: enable_auto_trading is False",
          }),
        ],
        unseen_count: 3,
      },
    });
    renderStrip();
    const rows = await screen.findAllByTestId("alerts-row");
    expect(rows).toHaveLength(2);
    expect(screen.getByText("Kill switch active")).toBeInTheDocument();
    expect(screen.getByText("Auto-trading disabled")).toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// Severity-tier ordering
// ---------------------------------------------------------------------------

describe("AlertsStrip — severity ordering", () => {
  it("orders actionable (position) above informational (guard) above housekeeping (coverage), regardless of timestamp", async () => {
    // Give the LOWEST-priority feed the NEWEST timestamp to prove tier wins over time.
    const coverage = makeCoverage({
      symbol: "TSLA",
      changed_at: new Date(Date.now() - 1 * 60 * 1000).toISOString(), // newest
    });
    const guard = makeGuard({
      symbol: "AAPL",
      decision_time: new Date(Date.now() - 5 * 60 * 1000).toISOString(),
    });
    const position = makePosition({
      symbol: "MSFT",
      opened_at: new Date(Date.now() - 30 * 60 * 1000).toISOString(), // oldest
    });
    stubAll({
      guard: { rejections: [guard], unseen_count: 1 },
      position: { alerts: [position], unseen_count: 1 },
      coverage: { drops: [coverage], unseen_count: 1 },
    });
    renderStrip();
    const rows = await screen.findAllByTestId("alerts-row");
    expect(rows).toHaveLength(3);
    expect(rows[0]!.textContent).toContain("MSFT"); // actionable
    expect(rows[1]!.textContent).toContain("Kill switch active"); // informational
    expect(rows[2]!.textContent).toContain("Coverage analysable → insufficient"); // housekeeping
  });
});

// ---------------------------------------------------------------------------
// Per-kind rendering + click-through
// ---------------------------------------------------------------------------

describe("AlertsStrip — per-kind rendering", () => {
  it("renders the tier pill for each kind", async () => {
    stubAll({
      guard: { rejections: [makeGuard()], unseen_count: 1 },
      position: { alerts: [makePosition()], unseen_count: 1 },
      coverage: { drops: [makeCoverage()], unseen_count: 1 },
    });
    renderStrip();
    expect(await screen.findByText("ACTION")).toBeInTheDocument();
    expect(screen.getByText("GUARD")).toBeInTheDocument();
    expect(screen.getByText("COVERAGE")).toBeInTheDocument();
  });

  it("position card links to /instruments/<id>", async () => {
    stubAll({
      position: { alerts: [makePosition({ instrument_id: 43 })], unseen_count: 1 },
    });
    renderStrip();
    await screen.findAllByTestId("alerts-row");
    const links = screen.getAllByRole("link").map((l) => l.getAttribute("href"));
    expect(links).toContain("/instruments/43");
  });

  it("guard card links to its remediation action (kill-switch → /admin, not deactivate)", async () => {
    stubAll({
      guard: {
        rejections: [makeGuard({ explanation: "FAIL — kill_switch: active" })],
        unseen_count: 1,
      },
    });
    renderStrip();
    await screen.findAllByTestId("alerts-row");
    const link = screen.getByRole("link", { name: /Manage in Admin/i });
    expect(link.getAttribute("href")).toBe("/admin");
  });

  it("guard card shows the human label + consequence, not the raw rule string", async () => {
    stubAll({
      guard: {
        rejections: [makeGuard({ explanation: "FAIL — kill_switch: active since 2026-06-28" })],
        unseen_count: 1,
      },
    });
    renderStrip();
    expect(await screen.findByText("Kill switch active")).toBeInTheDocument();
    expect(screen.getByText(/All order paths blocked/)).toBeInTheDocument();
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

  it("coverage card shows the transition and affected symbols", async () => {
    stubAll({
      coverage: {
        drops: [
          makeCoverage({ symbol: "TSLA", old_status: "analysable", new_status: "insufficient" }),
          makeCoverage({
            event_id: 302,
            symbol: "NIO",
            old_status: "analysable",
            new_status: "insufficient",
          }),
        ],
        unseen_count: 2,
      },
    });
    renderStrip();
    expect(await screen.findByText("Coverage analysable → insufficient")).toBeInTheDocument();
    const row = screen.getByTestId("alerts-row");
    expect(row.textContent).toContain("TSLA");
    expect(row.textContent).toContain("NIO");
  });

  it("coverage card renders a null new_status as a dash", async () => {
    stubAll({
      coverage: {
        drops: [makeCoverage({ old_status: "analysable", new_status: null })],
        unseen_count: 1,
      },
    });
    renderStrip();
    expect(await screen.findByText("Coverage analysable → —")).toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// Unseen cursor math (group-level)
// ---------------------------------------------------------------------------

describe("AlertsStrip — unseen cursor", () => {
  it("guard group is unseen when its MAX member id exceeds the cursor, seen otherwise", async () => {
    stubAll({
      guard: {
        alerts_last_seen_decision_id: 500,
        unseen_count: 1,
        rejections: [
          // unseen group: max id 501 > 500
          makeGuard({ decision_id: 501, explanation: "FAIL — kill_switch: active" }),
          // seen group: max id 499 < 500 (distinct reason so it forms its own card)
          makeGuard({ decision_id: 499, explanation: "FAIL — auto_trading: off" }),
        ],
      },
    });
    renderStrip();
    const rows = await screen.findAllByTestId("alerts-row");
    const killRow = rows.find((r) => r.textContent?.includes("Kill switch active"))!;
    const autoRow = rows.find((r) => r.textContent?.includes("Auto-trading disabled"))!;
    expect(killRow.className).toMatch(/border-amber/);
    expect(autoRow.className).toMatch(/border-slate/);
  });

  it("per-kind cursor: position + coverage cursors drive their own rows", async () => {
    stubAll({
      position: {
        alerts_last_seen_position_alert_id: 700,
        unseen_count: 1,
        alerts: [makePosition({ alert_id: 701 })], // unseen (701 > 700)
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
      // unseen position → red accent; unseen coverage (null cursor) → slate accent.
      expect(r.className).toMatch(/border-(red|slate)-/);
    }
  });
});

// ---------------------------------------------------------------------------
// Header badge + totals
// ---------------------------------------------------------------------------

describe("AlertsStrip — header totals", () => {
  it("header badge is sum of per-feed unseen_count (honest emission count)", async () => {
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
    await screen.findByText("Kill switch active");
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
// Per-feed overflow + ack-button gating (raw counts, not grouped)
// ---------------------------------------------------------------------------

describe("AlertsStrip — per-feed overflow", () => {
  it("shows Dismiss-all when a feed's backend unseen_count exceeds its RAW fetched rows", async () => {
    // unseen_count 2 > rejections.length 1 → overflow, even though the card collapses.
    stubAll({
      guard: { rejections: [makeGuard()], unseen_count: 2 },
    });
    renderStrip();
    expect(await screen.findByRole("button", { name: /Dismiss all/i })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /Mark all read/i })).toBeNull();
  });

  it("grouping alone (unseen_count == raw rows) does NOT trigger overflow", async () => {
    // 12 collapsed emissions, all fetched (unseen_count == 12 == rejections.length): normal ack.
    const rejections = Array.from({ length: 12 }, (_, i) =>
      makeGuard({ decision_id: 600 + i, explanation: "FAIL — kill_switch: active" }),
    );
    stubAll({ guard: { rejections, unseen_count: 12 } });
    renderStrip();
    expect(await screen.findByRole("button", { name: /Mark all read/i })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /Dismiss all/i })).toBeNull();
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
    await screen.findByText("Kill switch active");
    expect(screen.queryByRole("button", { name: /Mark all read/i })).toBeNull();
    expect(screen.queryByRole("button", { name: /Dismiss all/i })).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// Mark-all-read fan-out (raw feed MAX id, not group latestTs)
// ---------------------------------------------------------------------------

describe("AlertsStrip — Mark all read", () => {
  it("fans out to each non-empty feed with its MAX raw id", async () => {
    stubAll({
      guard: {
        rejections: [
          makeGuard({ decision_id: 505, explanation: "FAIL — kill_switch: active" }),
          makeGuard({ decision_id: 510, explanation: "FAIL — kill_switch: active" }),
        ],
        unseen_count: 2,
      },
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
