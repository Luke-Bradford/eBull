/**
 * Tests for RecommendationsPage (#63).
 *
 * Scope:
 *   - recommendations section: loading, empty, error, data states
 *   - audit section: loading, empty, error, data states
 *   - sections are independent (one failing does not blank the other)
 *   - filter changes trigger refetch
 *   - row expansion fetches detail
 *   - evidence panel renders checklist for execution_guard, JSON for order_client
 *
 * API mocked at module boundary — tests exercise the page state machine,
 * not the network layer.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";

import { RecommendationsPage } from "@/pages/RecommendationsPage";
import { fetchRecommendations, fetchRecommendation } from "@/api/recommendations";
import { fetchAuditList, fetchAuditDetail } from "@/api/audit";
import { ApiError } from "@/api/client";
import type {
  RecommendationsListResponse,
  RecommendationDetail,
  AuditListResponse,
  AuditDetail,
} from "@/api/types";

vi.mock("@/api/recommendations", () => ({
  fetchRecommendations: vi.fn(),
  fetchRecommendation: vi.fn(),
  RECOMMENDATIONS_PAGE_LIMIT: 50,
}));

vi.mock("@/api/audit", () => ({
  fetchAuditList: vi.fn(),
  fetchAuditDetail: vi.fn(),
  AUDIT_PAGE_LIMIT: 50,
}));

const mockedFetchRecs = vi.mocked(fetchRecommendations);
const mockedFetchRec = vi.mocked(fetchRecommendation);
const mockedFetchAudit = vi.mocked(fetchAuditList);
const mockedFetchAuditDetail = vi.mocked(fetchAuditDetail);

function recsResponse(): RecommendationsListResponse {
  return {
    items: [
      {
        recommendation_id: 1,
        instrument_id: 10,
        symbol: "AAPL",
        company_name: "Apple Inc.",
        action: "BUY",
        status: "proposed",
        rationale: "Strong compounder thesis with improving margins.",
        score_id: 100,
        model_version: "v1-balanced",
        suggested_size_pct: 0.05,
        target_entry: 180.5,
        cash_balance_known: true,
        created_at: "2026-04-08T10:00:00Z",
      },
      {
        recommendation_id: 2,
        instrument_id: 20,
        symbol: "MSFT",
        company_name: "Microsoft Corp.",
        action: "HOLD",
        status: "executed",
        rationale: "Maintaining position. No material change.",
        score_id: 101,
        model_version: "v1-balanced",
        suggested_size_pct: null,
        target_entry: null,
        cash_balance_known: true,
        created_at: "2026-04-08T09:00:00Z",
      },
    ],
    total: 2,
    offset: 0,
    limit: 50,
  };
}

function recDetailResponse(): RecommendationDetail {
  return {
    recommendation_id: 1,
    instrument_id: 10,
    symbol: "AAPL",
    company_name: "Apple Inc.",
    action: "BUY",
    status: "proposed",
    rationale: "Strong compounder thesis with improving margins.",
    score_id: 100,
    model_version: "v1-balanced",
    suggested_size_pct: 0.05,
    target_entry: 180.5,
    cash_balance_known: true,
    total_score: 78.5,
    created_at: "2026-04-08T10:00:00Z",
  };
}

function auditResponse(): AuditListResponse {
  return {
    items: [
      {
        decision_id: 1,
        decision_time: "2026-04-08T10:01:00Z",
        instrument_id: 10,
        symbol: "AAPL",
        company_name: "Apple Inc.",
        recommendation_id: 1,
        stage: "execution_guard",
        model_version: "v1-balanced",
        pass_fail: "PASS",
        explanation: "All guard rules passed.",
      },
      {
        decision_id: 2,
        decision_time: "2026-04-08T10:02:00Z",
        instrument_id: 10,
        symbol: "AAPL",
        company_name: "Apple Inc.",
        recommendation_id: 1,
        stage: "order_client",
        model_version: null,
        pass_fail: "PASS",
        explanation: "Order placed successfully.",
      },
    ],
    total: 2,
    offset: 0,
    limit: 50,
  };
}

function guardAuditDetail(): AuditDetail {
  return {
    decision_id: 1,
    decision_time: "2026-04-08T10:01:00Z",
    instrument_id: 10,
    symbol: "AAPL",
    company_name: "Apple Inc.",
    recommendation_id: 1,
    stage: "execution_guard",
    model_version: "v1-balanced",
    pass_fail: "PASS",
    explanation: "All guard rules passed.",
    evidence_json: [
      { rule: "kill_switch", passed: true, detail: "Kill switch inactive" },
      { rule: "fresh_thesis", passed: true, detail: "Thesis is 2 days old" },
      { rule: "spread_limit", passed: false, detail: "Spread 0.8% exceeds 0.5% cap" },
    ],
  };
}

function orderAuditDetail(): AuditDetail {
  return {
    decision_id: 2,
    decision_time: "2026-04-08T10:02:00Z",
    instrument_id: 10,
    symbol: "AAPL",
    company_name: "Apple Inc.",
    recommendation_id: 1,
    stage: "order_client",
    model_version: null,
    pass_fail: "PASS",
    explanation: "Order placed successfully.",
    evidence_json: { order_id: "abc-123", raw_payload: { status: "filled" } },
  };
}

function renderPage() {
  return render(
    <MemoryRouter>
      <RecommendationsPage />
    </MemoryRouter>,
  );
}

beforeEach(() => {
  mockedFetchRecs.mockReset();
  mockedFetchRec.mockReset();
  mockedFetchAudit.mockReset();
  mockedFetchAuditDetail.mockReset();
  mockedFetchRecs.mockResolvedValue(recsResponse());
  mockedFetchAudit.mockResolvedValue(auditResponse());
  mockedFetchRec.mockResolvedValue(recDetailResponse());
  mockedFetchAuditDetail.mockResolvedValue(guardAuditDetail());
});

afterEach(() => {
  vi.clearAllMocks();
});

// ---------------------------------------------------------------------------
// Recommendations section
// ---------------------------------------------------------------------------

describe("RecommendationsPage — recommendations section", () => {
  it("renders recommendation rows with action and status badges", async () => {
    renderPage();
    // Wait for data to load — section title is unique
    await waitFor(() => {
      expect(screen.getByText("Recommendation history")).toBeInTheDocument();
    });
    // Both symbols rendered somewhere on the page
    expect(screen.getAllByText("AAPL").length).toBeGreaterThanOrEqual(1);
    expect(screen.getAllByText("MSFT").length).toBeGreaterThanOrEqual(1);
    // Rationale text is unique to the recommendations table
    expect(screen.getByText(/Strong compounder thesis/)).toBeInTheDocument();
    expect(screen.getByText(/Maintaining position/)).toBeInTheDocument();
  });

  it("shows empty state when no recommendations exist", async () => {
    mockedFetchRecs.mockResolvedValue({ items: [], total: 0, offset: 0, limit: 50 });
    renderPage();
    await waitFor(() => {
      expect(screen.getByText("No recommendations yet")).toBeInTheDocument();
    });
  });

  it("shows error state on recommendations fetch failure", async () => {
    mockedFetchRecs.mockRejectedValue(new Error("network"));
    renderPage();
    await waitFor(() => {
      expect(screen.getByText(/Failed to load/)).toBeInTheDocument();
    });
  });

  it("shows 401 state on authentication error", async () => {
    mockedFetchRecs.mockRejectedValue(new ApiError(401, "Unauthorized"));
    mockedFetchAudit.mockRejectedValue(new ApiError(401, "Unauthorized"));
    renderPage();
    await waitFor(() => {
      expect(screen.getAllByText("Authentication required")).toHaveLength(2);
    });
  });

  it("expands a row to show full detail with total_score", async () => {
    renderPage();
    await waitFor(() => {
      expect(screen.getByText(/Strong compounder thesis/)).toBeInTheDocument();
    });

    const user = userEvent.setup();
    // Find the row by its unique rationale text
    const rationaleCell = screen.getByText(/Strong compounder thesis/);
    const row = rationaleCell.closest("tr")!;
    await user.click(row);

    await waitFor(() => {
      expect(screen.getByText(/Total score:/)).toBeInTheDocument();
    });
  });
});

// ---------------------------------------------------------------------------
// Audit section
// ---------------------------------------------------------------------------

describe("RecommendationsPage — audit section", () => {
  it("renders audit rows with stage labels", async () => {
    renderPage();
    await waitFor(() => {
      expect(screen.getByText("Guard")).toBeInTheDocument();
    });
    expect(screen.getByText("Order")).toBeInTheDocument();
  });

  it("shows empty state when no audit entries exist", async () => {
    mockedFetchAudit.mockResolvedValue({ items: [], total: 0, offset: 0, limit: 50 });
    renderPage();
    await waitFor(() => {
      expect(screen.getByText("No execution guard decisions recorded yet")).toBeInTheDocument();
    });
  });

  it("shows error state on audit fetch failure without affecting recommendations", async () => {
    mockedFetchAudit.mockRejectedValue(new Error("network"));
    renderPage();
    // Recommendations should still render
    await waitFor(() => {
      expect(screen.getByText(/Strong compounder thesis/)).toBeInTheDocument();
    });
    // Audit section should show error
    const alerts = screen.getAllByRole("alert");
    expect(alerts.length).toBeGreaterThanOrEqual(1);
  });
});

// ---------------------------------------------------------------------------
// Evidence panel (stage-aware rendering)
// ---------------------------------------------------------------------------

describe("RecommendationsPage — evidence panel", () => {
  it("renders guard checklist for execution_guard stage", async () => {
    mockedFetchAuditDetail.mockResolvedValue(guardAuditDetail());
    renderPage();
    await waitFor(() => {
      expect(screen.getByText("Guard")).toBeInTheDocument();
    });

    const user = userEvent.setup();
    // Click the first audit row (execution_guard)
    const guardRow = screen.getByText("Guard").closest("tr")!;
    await user.click(guardRow);

    await waitFor(() => {
      expect(screen.getByText("kill_switch")).toBeInTheDocument();
    });
    expect(screen.getByText("fresh_thesis")).toBeInTheDocument();
    expect(screen.getByText("spread_limit")).toBeInTheDocument();
    // Check pass/fail indicators — 2 passed rules + 1 failed
    expect(screen.getAllByLabelText("Passed")).toHaveLength(2);
    expect(screen.getAllByLabelText("Failed")).toHaveLength(1);
  });

  it("renders generic JSON for order_client stage", async () => {
    mockedFetchAuditDetail.mockResolvedValue(orderAuditDetail());
    renderPage();
    await waitFor(() => {
      expect(screen.getByText("Order")).toBeInTheDocument();
    });

    const user = userEvent.setup();
    const orderRow = screen.getByText("Order").closest("tr")!;
    await user.click(orderRow);

    await waitFor(() => {
      expect(screen.getByText(/abc-123/)).toBeInTheDocument();
    });
  });
});

// ---------------------------------------------------------------------------
// Independent section failure
// ---------------------------------------------------------------------------

describe("RecommendationsPage — section independence", () => {
  it("both sections failing shows top-level error banner", async () => {
    mockedFetchRecs.mockRejectedValue(new Error("network"));
    mockedFetchAudit.mockRejectedValue(new Error("network"));
    renderPage();
    await waitFor(() => {
      expect(screen.getByText(/API is unreachable/)).toBeInTheDocument();
    });
  });

  it("only recs failing does not show top-level error banner", async () => {
    mockedFetchRecs.mockRejectedValue(new Error("network"));
    // audit succeeds
    renderPage();
    // Assert audit rendered successfully AND banner is absent in the same
    // waitFor — proves section independence, not vacuous absence.
    await waitFor(() => {
      expect(screen.getByText("Guard")).toBeInTheDocument();
      expect(screen.queryByText(/API is unreachable/)).not.toBeInTheDocument();
    });
  });
});

// ---------------------------------------------------------------------------
// Filter interactions
// ---------------------------------------------------------------------------

describe("RecommendationsPage — filters", () => {
  it("changing action filter triggers refetch with the filter param", async () => {
    renderPage();
    await waitFor(() => {
      expect(screen.getByText(/Strong compounder thesis/)).toBeInTheDocument();
    });

    const user = userEvent.setup();
    const actionSelect = screen.getByLabelText("Action");
    await user.selectOptions(actionSelect, "BUY");

    await waitFor(() => {
      // Second call should include action=BUY in the query
      const calls = mockedFetchRecs.mock.calls;
      const lastCall = calls[calls.length - 1]!;
      expect(lastCall[0]).toMatchObject({ action: "BUY" });
    });
  });

  it("clear filters resets to unfiltered state", async () => {
    renderPage();
    await waitFor(() => {
      expect(screen.getByText(/Strong compounder thesis/)).toBeInTheDocument();
    });

    const user = userEvent.setup();
    // First set a filter
    const actionSelect = screen.getByLabelText("Action");
    await user.selectOptions(actionSelect, "EXIT");

    // Then clear — target the recommendations filter bar specifically
    const recFilterBar = screen.getByRole("group", { name: "Recommendations filters" });
    const clearBtn = within(recFilterBar).getByRole("button", { name: "Clear filters" });
    await user.click(clearBtn);

    await waitFor(() => {
      const calls = mockedFetchRecs.mock.calls;
      const lastCall = calls[calls.length - 1]!;
      expect(lastCall[0]).toMatchObject({ action: null, status: null, instrument_id: null });
    });
  });
});
