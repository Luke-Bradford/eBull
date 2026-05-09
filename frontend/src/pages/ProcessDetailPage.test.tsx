import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { ApiError } from "@/api/client";
import {
  cancelProcess,
  fetchOrchestratorDag,
  fetchProcess,
  fetchProcessRuns,
  triggerProcess,
} from "@/api/processes";
import type { OrchestratorDagResponse } from "@/api/types";
import {
  makeProcessRow,
  makeError,
} from "@/components/admin/__fixtures__/processes";
import { ProcessDetailPage } from "@/pages/ProcessDetailPage";

vi.mock("@/api/processes", async () => {
  const actual =
    await vi.importActual<typeof import("@/api/processes")>("@/api/processes");
  return {
    ...actual,
    fetchProcess: vi.fn(),
    fetchProcessRuns: vi.fn(),
    triggerProcess: vi.fn(),
    cancelProcess: vi.fn(),
    fetchOrchestratorDag: vi.fn(),
  };
});

const mockedDetail = vi.mocked(fetchProcess);
const mockedRuns = vi.mocked(fetchProcessRuns);
const mockedTrigger = vi.mocked(triggerProcess);
const mockedCancel = vi.mocked(cancelProcess);
const mockedDag = vi.mocked(fetchOrchestratorDag);

beforeEach(() => {
  mockedDetail.mockReset();
  mockedRuns.mockReset();
  mockedTrigger.mockReset();
  mockedCancel.mockReset();
  mockedDag.mockReset();
});

function renderAt(path = "/admin/processes/sec_form4_ingest") {
  return render(
    <MemoryRouter initialEntries={[path]}>
      <Routes>
        <Route path="admin/processes/:id" element={<ProcessDetailPage />} />
      </Routes>
    </MemoryRouter>,
  );
}

describe("ProcessDetailPage", () => {
  it("renders three tabs", async () => {
    mockedDetail.mockResolvedValue(makeProcessRow());
    mockedRuns.mockResolvedValue([]);
    renderAt();
    await waitFor(() => expect(mockedDetail).toHaveBeenCalled());
    expect(screen.getByRole("tab", { name: "Overview" })).toBeTruthy();
    expect(screen.getByRole("tab", { name: "History" })).toBeTruthy();
    expect(screen.getByRole("tab", { name: "Errors" })).toBeTruthy();
  });

  it("Overview tab surfaces watermark.human", async () => {
    mockedDetail.mockResolvedValue(
      makeProcessRow({
        watermark: {
          cursor_kind: "filed_at",
          cursor_value: "x",
          human: "Resume from filings filed after 2026-05-08T13:00Z",
          last_advanced_at: "2026-05-08T13:00:00+00:00",
        },
      }),
    );
    mockedRuns.mockResolvedValue([]);
    renderAt();
    await waitFor(() =>
      expect(
        screen.getByText("Resume from filings filed after 2026-05-08T13:00Z"),
      ).toBeTruthy(),
    );
  });

  it("History tab renders runs from the API", async () => {
    mockedDetail.mockResolvedValue(makeProcessRow());
    mockedRuns.mockResolvedValue([
      {
        run_id: 7,
        started_at: "2026-05-08T13:00:00+00:00",
        finished_at: "2026-05-08T13:03:00+00:00",
        duration_seconds: 180,
        rows_processed: 4520,
        rows_skipped_by_reason: {},
        rows_errored: 0,
        status: "success",
        cancelled_by_operator_id: null,
      },
    ]);
    renderAt();
    await waitFor(() =>
      expect(screen.getByRole("tab", { name: "History" })).toBeTruthy(),
    );
    fireEvent.click(screen.getByRole("tab", { name: "History" }));
    await waitFor(() => expect(mockedRuns).toHaveBeenCalled());
    expect(screen.getByText(/4520/)).toBeTruthy();
  });

  it("Errors tab renders grouped error classes", async () => {
    mockedDetail.mockResolvedValue(
      makeProcessRow({
        status: "failed",
        last_n_errors: [
          makeError({ error_class: "Form4ParseError", count: 5 }),
        ],
      }),
    );
    mockedRuns.mockResolvedValue([]);
    renderAt();
    await waitFor(() =>
      expect(screen.getByRole("tab", { name: "Errors" })).toBeTruthy(),
    );
    fireEvent.click(screen.getByRole("tab", { name: "Errors" }));
    expect(await screen.findByText("Form4ParseError")).toBeTruthy();
  });

  it("Iterate POSTs mode=iterate and surfaces 409 reason on failure", async () => {
    mockedDetail.mockResolvedValue(
      makeProcessRow({ can_iterate: true, can_full_wash: false, can_cancel: false }),
    );
    mockedRuns.mockResolvedValue([]);
    mockedTrigger.mockRejectedValueOnce(
      new ApiError(409, "kill switch active", { reason: "kill_switch_active" }),
    );
    renderAt();
    await waitFor(() =>
      expect(screen.getByRole("button", { name: "Iterate" })).toBeTruthy(),
    );
    fireEvent.click(screen.getByRole("button", { name: "Iterate" }));
    await waitFor(() => expect(mockedTrigger).toHaveBeenCalled());
    expect(mockedTrigger).toHaveBeenCalledWith("sec_form4_ingest", {
      mode: "iterate",
    });
    const note = await screen.findByText("trigger rejected");
    expect(note.getAttribute("title")).toContain("Kill switch is active");
  });

  it("cancel cooperative posts mode=cooperative", async () => {
    mockedDetail.mockResolvedValue(
      makeProcessRow({
        can_iterate: false,
        can_full_wash: false,
        can_cancel: true,
      }),
    );
    mockedRuns.mockResolvedValue([]);
    mockedCancel.mockResolvedValueOnce({
      target_run_kind: "job_run",
      target_run_id: 7,
    });
    renderAt();
    await waitFor(() =>
      expect(screen.getByRole("button", { name: "Cancel" })).toBeTruthy(),
    );
    fireEvent.click(screen.getByRole("button", { name: "Cancel" }));
    const dialog = await screen.findByRole("dialog");
    const confirmBtn = Array.from(dialog.querySelectorAll("button")).find(
      (b) => b.textContent === "Cancel cooperatively",
    ) as HTMLButtonElement;
    fireEvent.click(confirmBtn);
    await waitFor(() => expect(mockedCancel).toHaveBeenCalled());
    expect(mockedCancel).toHaveBeenCalledWith("sec_form4_ingest", {
      mode: "cooperative",
    });
  });
});


// ---------------------------------------------------------------------------
// PR6 (#1078) — DAG drill-in tab on /admin/processes/orchestrator_full_sync
// ---------------------------------------------------------------------------

function makeDagPayload(): OrchestratorDagResponse {
  return {
    sync_run: {
      sync_run_id: 42,
      scope: "full",
      scope_detail: null,
      trigger: "manual",
      started_at: "2026-05-09T13:00:00Z",
      finished_at: null,
      status: "running",
      layers_planned: 3,
      layers_done: 1,
      layers_failed: 0,
      layers_skipped: 0,
      error_category: null,
      cancel_requested_at: null,
    },
    layers: [
      {
        name: "universe",
        display_name: "Tradable Universe",
        tier: 0,
        status: "complete",
        started_at: "2026-05-09T13:00:01Z",
        finished_at: "2026-05-09T13:00:30Z",
        items_total: 100,
        items_done: 100,
        row_count: 100,
        error_category: null,
        skip_reason: null,
        error_message: null,
      },
      {
        name: "candles",
        display_name: "Daily Price Candles",
        tier: 1,
        status: "running",
        started_at: "2026-05-09T13:00:30Z",
        finished_at: null,
        items_total: 50,
        items_done: 12,
        row_count: null,
        error_category: null,
        skip_reason: null,
        error_message: null,
      },
      {
        name: "fundamentals",
        display_name: "Fundamentals Snapshot",
        tier: 1,
        status: "pending",
        started_at: null,
        finished_at: null,
        items_total: null,
        items_done: null,
        row_count: null,
        error_category: null,
        skip_reason: null,
        error_message: null,
      },
    ],
  };
}

function renderOrchestrator() {
  return render(
    <MemoryRouter initialEntries={["/admin/processes/orchestrator_full_sync"]}>
      <Routes>
        <Route path="admin/processes/:id" element={<ProcessDetailPage />} />
      </Routes>
    </MemoryRouter>,
  );
}

describe("ProcessDetailPage — DAG tab (orchestrator_full_sync)", () => {
  it("renders the DAG tab on orchestrator_full_sync detail page", async () => {
    mockedDetail.mockResolvedValueOnce(
      makeProcessRow({ process_id: "orchestrator_full_sync", display_name: "Orchestrator full sync" }),
    );
    mockedRuns.mockResolvedValueOnce([]);
    renderOrchestrator();
    await waitFor(() =>
      expect(screen.getByRole("tab", { name: "DAG" })).toBeTruthy(),
    );
  });

  it("does NOT render the DAG tab on a non-orchestrator detail page", async () => {
    mockedDetail.mockResolvedValueOnce(
      makeProcessRow({ process_id: "sec_form4_ingest", display_name: "Form 4 ingest" }),
    );
    mockedRuns.mockResolvedValueOnce([]);
    renderAt();
    await waitFor(() =>
      expect(screen.getByRole("tab", { name: "Overview" })).toBeTruthy(),
    );
    expect(screen.queryByRole("tab", { name: "DAG" })).toBeNull();
    // Non-orchestrator pages must NEVER call /dag — regression guard
    // for Codex M1.
    expect(mockedDag).not.toHaveBeenCalled();
  });

  it("does NOT fetch /dag on initial load when tab is overview", async () => {
    mockedDetail.mockResolvedValueOnce(
      makeProcessRow({ process_id: "orchestrator_full_sync", display_name: "Orchestrator full sync" }),
    );
    mockedRuns.mockResolvedValueOnce([]);
    renderOrchestrator();
    await waitFor(() =>
      expect(screen.getByRole("tab", { name: "DAG" })).toBeTruthy(),
    );
    // Codex M-r2-2: fetch is gated on (tab === "dag") AND orchestrator id.
    expect(mockedDag).not.toHaveBeenCalled();
  });

  it("fetches /dag and renders layer rows when DAG tab is opened", async () => {
    mockedDetail.mockResolvedValueOnce(
      makeProcessRow({ process_id: "orchestrator_full_sync", display_name: "Orchestrator full sync" }),
    );
    mockedRuns.mockResolvedValueOnce([]);
    mockedDag.mockResolvedValueOnce(makeDagPayload());
    renderOrchestrator();
    fireEvent.click(await screen.findByRole("tab", { name: "DAG" }));
    await waitFor(() => expect(mockedDag).toHaveBeenCalledWith("orchestrator_full_sync"));
    expect(await screen.findByText("Tradable Universe")).toBeTruthy();
    expect(screen.getByText("Daily Price Candles")).toBeTruthy();
    expect(screen.getByText("Fundamentals Snapshot")).toBeTruthy();
  });

  it("renders 'no recent run' when /dag returns null sync_run", async () => {
    mockedDetail.mockResolvedValueOnce(
      makeProcessRow({ process_id: "orchestrator_full_sync", display_name: "Orchestrator full sync" }),
    );
    mockedRuns.mockResolvedValueOnce([]);
    mockedDag.mockResolvedValueOnce({ sync_run: null, layers: [] });
    renderOrchestrator();
    fireEvent.click(await screen.findByRole("tab", { name: "DAG" }));
    expect(await screen.findByText(/No recent sync run/i)).toBeTruthy();
  });
});
