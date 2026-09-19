import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { ApiError } from "@/api/client";
import { runJob } from "@/api/jobs";
import {
  cancelProcess,
  fetchBootstrapTimeline,
  fetchOrchestratorDag,
  fetchProcess,
  fetchProcessRuns,
  triggerProcess,
} from "@/api/processes";
import type {
  BootstrapTimelineResponse,
  OrchestratorDagResponse,
  ParamMetadata,
} from "@/api/types";
import {
  makeProcessRow,
  makeError,
} from "@/components/admin/__fixtures__/processes";
import { ProcessRow, processRowSignature } from "@/components/admin/ProcessRow";
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
    fetchBootstrapTimeline: vi.fn(),
  };
});

vi.mock("@/api/jobs", async () => {
  const actual = await vi.importActual<typeof import("@/api/jobs")>(
    "@/api/jobs",
  );
  return { ...actual, runJob: vi.fn() };
});

const mockedDetail = vi.mocked(fetchProcess);
const mockedRuns = vi.mocked(fetchProcessRuns);
const mockedTrigger = vi.mocked(triggerProcess);
const mockedCancel = vi.mocked(cancelProcess);
const mockedDag = vi.mocked(fetchOrchestratorDag);
const mockedTimeline = vi.mocked(fetchBootstrapTimeline);
const mockedRunJob = vi.mocked(runJob);

beforeEach(() => {
  mockedDetail.mockReset();
  mockedRuns.mockReset();
  mockedTrigger.mockReset();
  mockedCancel.mockReset();
  mockedDag.mockReset();
  mockedTimeline.mockReset();
  mockedRunJob.mockReset();
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
        progress_errors: null,
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

  it("History tab distinguishes a degraded run that errored from a clean one", async () => {
    // #3111 — `rows_processed` COUNTS the failures (`scheduler.py:7860`
    // assigned `parsed + tombstoned + failed` to row_count), so both rows
    // below report 5 rows. Without the errored column and the status tone,
    // the degraded one is indistinguishable from the successful one.
    mockedDetail.mockResolvedValue(makeProcessRow());
    mockedRuns.mockResolvedValue([
      {
        run_id: 9,
        started_at: "2026-05-08T13:10:00+00:00",
        finished_at: "2026-05-08T13:10:30+00:00",
        duration_seconds: 30,
        rows_processed: 5,
        rows_skipped_by_reason: {},
        rows_errored: 5,
        progress_errors: null,
        status: "degraded",
        cancelled_by_operator_id: null,
      },
      {
        run_id: 8,
        started_at: "2026-05-08T13:00:00+00:00",
        finished_at: "2026-05-08T13:00:30+00:00",
        duration_seconds: 30,
        rows_processed: 5,
        rows_skipped_by_reason: {},
        rows_errored: 0,
        progress_errors: null,
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

    const errored = screen.getAllByTestId("run-rows-errored");
    expect(errored).toHaveLength(2);
    expect(errored.map((cell) => cell.textContent)).toEqual([
      "5",
      // Zero renders as an em-dash, not "0" — a column of zeroes trains the
      // eye to skip it, which is how the one non-zero gets missed.
      "—",
    ]);

    // Rendered as a TONED BADGE, not bare text — a degraded run sitting at the
    // same visual weight as a clean one is half the defect. Asserted through
    // the testid rather than a colour class: the tone→class map belongs to
    // `Badge`, and pinning raw Tailwind in a test is how `eightKSeverity.ts`
    // shipped light-only chips past the dark gate.
    const statuses = screen.getAllByTestId("run-status");
    expect(statuses.map((el) => el.textContent)).toEqual([
      // The stored value verbatim, so the operator can query `job_runs.status`
      // with what they read. `STATUS_VISUAL.degraded` says "no progress",
      // which on a run that errored on every row would be false.
      "degraded",
      "success",
    ]);
  });

  it("History tab shows the JobProgress error buckets the scalar cannot see", async () => {
    // #3111 slice 5 — TWO disjoint error counters reach this cell and they are
    // NOT summable. The only `degraded` run in the corpus
    // (`daily_candle_refresh` 133470) has `rows_errored=0` and
    // `progress_json.errors={"failed":1}`, so slice 4's column rendered `—` on
    // the one run it was shipped to flag. All four branches asserted here:
    // buckets present, scalar only, both (buckets win), neither.
    mockedDetail.mockResolvedValue(makeProcessRow());
    mockedRuns.mockResolvedValue([
      {
        // The real 133470 shape.
        run_id: 13,
        started_at: "2026-05-08T13:40:00+00:00",
        finished_at: "2026-05-08T13:40:30+00:00",
        duration_seconds: 30,
        rows_processed: 11524,
        rows_skipped_by_reason: {},
        rows_errored: 0,
        progress_errors: { failed: 1 },
        status: "degraded",
        cancelled_by_operator_id: null,
      },
      {
        // Buckets win over the scalar, and they render UNSUMMED — `5` would
        // be the fold this slice exists to avoid, and sorted order matches
        // `degradation_reason`'s own sorted reason string.
        run_id: 12,
        started_at: "2026-05-08T13:30:00+00:00",
        finished_at: "2026-05-08T13:30:30+00:00",
        duration_seconds: 30,
        rows_processed: 9,
        rows_skipped_by_reason: {},
        rows_errored: 5,
        progress_errors: { failed: 3, dispatch_errors: 2 },
        status: "degraded",
        cancelled_by_operator_id: null,
      },
      {
        // Scalar only — the pre-slice-5 behaviour, unchanged.
        run_id: 11,
        started_at: "2026-05-08T13:20:00+00:00",
        finished_at: "2026-05-08T13:20:30+00:00",
        duration_seconds: 30,
        rows_processed: 5,
        rows_skipped_by_reason: {},
        rows_errored: 5,
        progress_errors: null,
        status: "failure",
        cancelled_by_operator_id: null,
      },
      {
        // Reported, none positive. `{}` and `null` are DIFFERENT answers on
        // the API (sql/254: NULL is not a measured zero) and deliberately the
        // same em-dash here — the cell does not need the distinction, the
        // audit trail does.
        run_id: 10,
        started_at: "2026-05-08T13:15:00+00:00",
        finished_at: "2026-05-08T13:15:30+00:00",
        duration_seconds: 30,
        rows_processed: 5,
        rows_skipped_by_reason: {},
        rows_errored: 0,
        progress_errors: {},
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

    const errored = screen.getAllByTestId("run-rows-errored");
    expect(errored.map((cell) => cell.textContent)).toEqual([
      "failed 1",
      "dispatch_errors 2 · failed 3",
      "5",
      "—",
    ]);

    // The bucket branch HIDES the scalar, so the tooltip must state its actual
    // value rather than assert one. Codex ckpt-2 caught an earlier draft
    // claiming "rows_errored is 0 or absent" — false for run 12 below, which
    // is exactly the shape the manifest worker produces.
    const titles = errored.map((cell) => cell.getAttribute("title") ?? "");
    expect(titles[0]).toContain("rows_errored on this run is 0");
    expect(titles[1]).toContain("rows_errored on this run is 5");
  });

  it("Errors tab states which counter it reads instead of denying all errors", async () => {
    // #3111 slice 5 — this tab reads ONLY `job_runs.error_classes`. "No errors
    // on the latest terminal run" was a universal denial it cannot support,
    // and it would sit beside a History row reading `failed 1`.
    mockedDetail.mockResolvedValue(makeProcessRow({ last_n_errors: [] }));
    mockedRuns.mockResolvedValue([]);
    renderAt();
    await waitFor(() =>
      expect(screen.getByRole("tab", { name: "Errors" })).toBeTruthy(),
    );
    fireEvent.click(screen.getByRole("tab", { name: "Errors" }));
    expect(await screen.findByText(/job_runs.error_classes/)).toBeTruthy();
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

  // #2274 — the two things that make offering Cancel on an orchestrator
  // wrapper row safe and legible.
  it("pins the displayed run id on the cancel request", async () => {
    mockedDetail.mockResolvedValue(
      makeProcessRow({
        can_iterate: false,
        can_full_wash: false,
        can_cancel: true,
        active_run: {
          run_id: 4242,
          run_kind: "sync_run",
          started_at: "2026-05-08T13:00:00+00:00",
          rows_processed_so_far: null,
          progress_units_done: null,
          progress_units_total: null,
          last_progress_at: null,
          is_cancelling: false,
        },
      }),
    );
    mockedRuns.mockResolvedValue([]);
    mockedCancel.mockResolvedValueOnce({
      target_run_kind: "sync_run",
      target_run_id: 4242,
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
    // Without the pin the server cancels whatever holds the singleton slot
    // when the POST lands — which, given the dialog can stay open
    // indefinitely, need not be the run the operator was looking at.
    expect(mockedCancel).toHaveBeenCalledWith("sec_form4_ingest", {
      mode: "cooperative",
      target_run_id: 4242,
    });
  });

  it("refetches when the pinned run has simply ended", async () => {
    // #2274 / Codex ckpt-2 P2 — a pinned run that was REPLACED returns
    // `run_changed`, but one that just FINISHED with nothing behind it returns
    // `no_active_run`. This page does not poll, so refetching on only the first
    // leaves it retrying a dead pin forever.
    mockedDetail.mockResolvedValue(
      makeProcessRow({
        can_iterate: false,
        can_full_wash: false,
        can_cancel: true,
        active_run: {
          run_id: 77,
          run_kind: "sync_run",
          started_at: "2026-05-08T13:00:00+00:00",
          rows_processed_so_far: null,
          progress_units_done: null,
          progress_units_total: null,
          last_progress_at: null,
          is_cancelling: false,
        },
      }),
    );
    mockedRuns.mockResolvedValue([]);
    mockedCancel.mockRejectedValueOnce(
      new ApiError(409, "conflict", { reason: "no_active_run" }),
    );
    renderAt();
    await waitFor(() =>
      expect(screen.getByRole("button", { name: "Cancel" })).toBeTruthy(),
    );
    const callsBefore = mockedDetail.mock.calls.length;
    fireEvent.click(screen.getByRole("button", { name: "Cancel" }));
    const dialog = await screen.findByRole("dialog");
    const confirmBtn = Array.from(dialog.querySelectorAll("button")).find(
      (b) => b.textContent === "Cancel cooperatively",
    ) as HTMLButtonElement;
    fireEvent.click(confirmBtn);
    await waitFor(() =>
      expect(mockedDetail.mock.calls.length).toBeGreaterThan(callsBefore),
    );
  });

  it("names the table a sync run id came from", async () => {
    mockedDetail.mockResolvedValue(
      makeProcessRow({
        active_run: {
          run_id: 418,
          run_kind: "sync_run",
          started_at: "2026-05-08T13:00:00+00:00",
          rows_processed_so_far: null,
          progress_units_done: null,
          progress_units_total: null,
          last_progress_at: null,
          is_cancelling: false,
        },
      }),
    );
    mockedRuns.mockResolvedValue([]);
    renderAt();
    // `sync_runs` and `job_runs` ids overlap freely, so a bare "run #418"
    // sends an operator grepping the wrong table.
    await waitFor(() =>
      expect(screen.getByText(/sync run #418/)).toBeTruthy(),
    );
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

  // PR3a #1064 — bootstrap mechanism uses different action verbs.
  it("bootstrap mechanism labels Iterate as 'Re-run failed' and Full-wash as 'Re-run all'", async () => {
    mockedDetail.mockResolvedValue(
      makeProcessRow({
        process_id: "bootstrap",
        mechanism: "bootstrap",
        display_name: "First-install bootstrap",
        can_iterate: true,
        can_full_wash: true,
      }),
    );
    mockedRuns.mockResolvedValue([]);
    render(
      <MemoryRouter initialEntries={["/admin/processes/bootstrap"]}>
        <Routes>
          <Route path="admin/processes/:id" element={<ProcessDetailPage />} />
        </Routes>
      </MemoryRouter>,
    );
    await waitFor(() =>
      expect(
        screen.getByRole("button", { name: "Re-run failed" }),
      ).toBeTruthy(),
    );
    expect(screen.getByRole("button", { name: "Re-run all" })).toBeTruthy();
    expect(screen.queryByRole("button", { name: "Iterate" })).toBeNull();
    expect(screen.queryByRole("button", { name: "Full-wash" })).toBeNull();
  });

  it("clean-complete bootstrap de-emphasises 'Re-run all' to a neutral tone (#1432)", async () => {
    mockedDetail.mockResolvedValue(
      makeProcessRow({
        process_id: "bootstrap",
        mechanism: "bootstrap",
        display_name: "First-install bootstrap",
        status: "ok",
        can_iterate: false,
        can_full_wash: true,
      }),
    );
    mockedRuns.mockResolvedValue([]);
    render(
      <MemoryRouter initialEntries={["/admin/processes/bootstrap"]}>
        <Routes>
          <Route path="admin/processes/:id" element={<ProcessDetailPage />} />
        </Routes>
      </MemoryRouter>,
    );
    const btn = (await screen.findByRole("button", {
      name: "Re-run all",
    })) as HTMLButtonElement;
    // Still available, but no red destructive styling — nothing failed.
    expect(btn.disabled).toBe(false);
    expect(btn.className).not.toContain("text-red-700");
    expect(btn.className).toContain("text-slate-700");
    expect(btn.title).toContain("completed cleanly");
  });

  it("first-install bootstrap shows only 'Run bootstrap' + 'Start bootstrap' modal", async () => {
    mockedDetail.mockResolvedValue(
      makeProcessRow({
        process_id: "bootstrap",
        mechanism: "bootstrap",
        display_name: "First-install bootstrap",
        status: "pending_first_run",
        can_iterate: false,
        can_full_wash: true,
        can_cancel: false,
      }),
    );
    mockedRuns.mockResolvedValue([]);
    render(
      <MemoryRouter initialEntries={["/admin/processes/bootstrap"]}>
        <Routes>
          <Route path="admin/processes/:id" element={<ProcessDetailPage />} />
        </Routes>
      </MemoryRouter>,
    );
    await waitFor(() =>
      expect(
        screen.getByRole("button", { name: "Run bootstrap" }),
      ).toBeTruthy(),
    );
    // Inapplicable affordances hidden on a never-run row.
    expect(screen.queryByRole("button", { name: "Re-run failed" })).toBeNull();
    expect(screen.queryByRole("button", { name: "Re-run all" })).toBeNull();
    expect(screen.queryByRole("button", { name: "Cancel" })).toBeNull();
    // Confirm modal uses non-destructive start framing.
    fireEvent.click(screen.getByRole("button", { name: "Run bootstrap" }));
    expect(
      await screen.findByRole("heading", { name: /Start bootstrap/i }),
    ).toBeTruthy();
    expect(screen.getByText(/Safe to run/i)).toBeTruthy();
    expect(screen.queryByText(/resets every stage/i)).toBeNull();
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


// ---------------------------------------------------------------------------
// PR7 (#1080) — Timeline drill-in tab on /admin/processes/bootstrap
// ---------------------------------------------------------------------------

function makeTimelinePayload(): BootstrapTimelineResponse {
  return {
    run: {
      run_id: 7,
      status: "running",
      triggered_at: "2026-05-09T10:00:00Z",
      completed_at: null,
      cancel_requested_at: null,
      has_warnings: false,
    },
    stages: [
      {
        stage_key: "universe_sync",
        display_name: "Universe Sync",
        stage_order: 1,
        lane: "init",
        job_name: "nightly_universe_sync",
        status: "success",
        started_at: "2026-05-09T10:00:01Z",
        completed_at: "2026-05-09T10:00:30Z",
        last_error: null,
        rows_processed: 4520,
        processed_count: 4520,
        target_count: null,
        last_progress_at: "2026-05-09T10:00:30Z",
        rate: null,
        eta_seconds: null,
        heartbeat_age_seconds: null,
        is_stale: false,
        target_cohort_fingerprint: null,
        archives: [
          {
            archive_name: "__job__",
            rows_written: 0,
            rows_skipped_by_reason: {},
            completed_at: "2026-05-09T10:00:30Z",
          },
        ],
        warning: null,
      },
      {
        stage_key: "cik_refresh",
        display_name: "Cik Refresh",
        stage_order: 6,
        lane: "sec_rate",
        job_name: "daily_cik_refresh",
        status: "running",
        started_at: "2026-05-09T10:00:31Z",
        completed_at: null,
        last_error: null,
        rows_processed: null,
        processed_count: 12,
        target_count: null,
        // #1409 P5 — live signals on the running stage: rate set,
        // eta null (no target), fresh heartbeat, not stale.
        last_progress_at: "2026-05-09T10:05:00Z",
        rate: 15600,
        eta_seconds: null,
        heartbeat_age_seconds: 8,
        is_stale: false,
        // #1273 PR2 — fingerprint set on a running stage so the
        // tooltip-rendering assertion below can verify the bar
        // wrapper carries the title attribute. The shape mirrors
        // the §4 streaming-style fingerprint.
        target_cohort_fingerprint:
          "max_subjects=unbounded;follow_pagination=true;fast_path_seeded=false",
        warning: null,
        archives: [
          {
            archive_name: "cik_index_2026Q2.zip",
            rows_written: 420,
            rows_skipped_by_reason: { unresolved_cik: 12 },
            completed_at: "2026-05-09T10:00:45Z",
          },
        ],
      },
    ],
  };
}

function renderBootstrap(path = "/admin/processes/bootstrap") {
  return render(
    <MemoryRouter initialEntries={[path]}>
      <Routes>
        <Route path="admin/processes/:id" element={<ProcessDetailPage />} />
      </Routes>
    </MemoryRouter>,
  );
}

describe("ProcessDetailPage — Timeline tab (bootstrap)", () => {
  it("renders the Timeline tab on the bootstrap detail page", async () => {
    mockedDetail.mockResolvedValueOnce(
      makeProcessRow({ process_id: "bootstrap", display_name: "First-install bootstrap" }),
    );
    mockedRuns.mockResolvedValueOnce([]);
    renderBootstrap();
    await waitFor(() =>
      expect(screen.getByRole("tab", { name: "Timeline" })).toBeTruthy(),
    );
  });

  it("does NOT render the Timeline tab on a non-bootstrap detail page", async () => {
    mockedDetail.mockResolvedValueOnce(
      makeProcessRow({ process_id: "sec_form4_ingest", display_name: "Form 4 ingest" }),
    );
    mockedRuns.mockResolvedValueOnce([]);
    renderAt();
    await waitFor(() =>
      expect(screen.getByRole("tab", { name: "Overview" })).toBeTruthy(),
    );
    expect(screen.queryByRole("tab", { name: "Timeline" })).toBeNull();
    // Non-bootstrap pages must NEVER call /timeline — restricted endpoint.
    expect(mockedTimeline).not.toHaveBeenCalled();
  });

  it("does NOT fetch /timeline on initial load when tab is overview", async () => {
    mockedDetail.mockResolvedValueOnce(
      makeProcessRow({ process_id: "bootstrap", display_name: "First-install bootstrap" }),
    );
    mockedRuns.mockResolvedValueOnce([]);
    renderBootstrap();
    await waitFor(() =>
      expect(screen.getByRole("tab", { name: "Timeline" })).toBeTruthy(),
    );
    // Fetch is gated on (tab === "timeline") AND bootstrap id.
    expect(mockedTimeline).not.toHaveBeenCalled();
  });

  // #1267 — the bootstrap nudge banner deep-links here with ?tab=timeline.
  it("preselects the Timeline tab and fetches /timeline when deep-linked via ?tab=timeline", async () => {
    mockedDetail.mockResolvedValueOnce(
      makeProcessRow({ process_id: "bootstrap", display_name: "First-install bootstrap" }),
    );
    mockedRuns.mockResolvedValueOnce([]);
    mockedTimeline.mockResolvedValueOnce(makeTimelinePayload());
    renderBootstrap("/admin/processes/bootstrap?tab=timeline");
    // No tab click — the deep link alone must open Timeline.
    await waitFor(() => expect(mockedTimeline).toHaveBeenCalledWith("bootstrap"));
    expect(await screen.findByText("Universe Sync")).toBeTruthy();
  });

  // #1267 — an unknown ?tab= value must fall back to overview, and a
  // timeline deep link on a NON-bootstrap process must reset via the
  // existing per-process guard (restricted endpoint never called).
  it("falls back to Overview for unknown ?tab= and for ?tab=timeline on a non-bootstrap process", async () => {
    mockedDetail.mockResolvedValue(
      makeProcessRow({ process_id: "sec_form4_ingest", display_name: "Form 4 ingest" }),
    );
    mockedRuns.mockResolvedValue([]);
    renderAt("/admin/processes/sec_form4_ingest?tab=bogus");
    await waitFor(() =>
      expect(screen.getByRole("tab", { name: "Overview" })).toBeTruthy(),
    );
    expect(mockedTimeline).not.toHaveBeenCalled();
    cleanup();
    renderAt("/admin/processes/sec_form4_ingest?tab=timeline");
    await waitFor(() =>
      expect(screen.getByRole("tab", { name: "Overview" })).toBeTruthy(),
    );
    expect(mockedTimeline).not.toHaveBeenCalled();
  });

  it("fetches /timeline and renders stages grouped by lane when Timeline tab is opened", async () => {
    mockedDetail.mockResolvedValueOnce(
      makeProcessRow({ process_id: "bootstrap", display_name: "First-install bootstrap" }),
    );
    mockedRuns.mockResolvedValueOnce([]);
    mockedTimeline.mockResolvedValueOnce(makeTimelinePayload());
    renderBootstrap();
    fireEvent.click(await screen.findByRole("tab", { name: "Timeline" }));
    await waitFor(() => expect(mockedTimeline).toHaveBeenCalledWith("bootstrap"));
    expect(await screen.findByText("Universe Sync")).toBeTruthy();
    expect(screen.getByText("Cik Refresh")).toBeTruthy();
    // Lane headers render in declared order.
    expect(screen.getByText(/^init$/)).toBeTruthy();
    expect(screen.getByText(/^sec_rate$/)).toBeTruthy();
    // #1273 PR2 — cohort fingerprint surfaces as a native title= tooltip
    // on the running stage's progress-bar wrapper. The cik_refresh
    // fixture above carries target_count=null + processed_count=12,
    // so the wrapper renders via the no-target branch; the new title
    // attribute should equal the fixture's fingerprint string.
    const fingerprintEl = await screen.findByText(/12 processed \(no target set\)/);
    const barWrapper = fingerprintEl.parentElement;
    expect(barWrapper).not.toBeNull();
    expect(barWrapper!.getAttribute("title")).toBe(
      "max_subjects=unbounded;follow_pagination=true;fast_path_seeded=false",
    );
  });

  it("renders 'no bootstrap run yet' when /timeline returns null run", async () => {
    mockedDetail.mockResolvedValueOnce(
      makeProcessRow({ process_id: "bootstrap", display_name: "First-install bootstrap" }),
    );
    mockedRuns.mockResolvedValueOnce([]);
    mockedTimeline.mockResolvedValueOnce({ run: null, stages: [] });
    renderBootstrap();
    fireEvent.click(await screen.findByRole("tab", { name: "Timeline" }));
    expect(await screen.findByText(/No bootstrap run yet/i)).toBeTruthy();
  });

  // #1140 Task C — warning chip on success+zero-rows strict-cap stage.
  it("renders an amber warning chip on a success+rows=0 strict-cap stage", async () => {
    mockedDetail.mockResolvedValueOnce(
      makeProcessRow({ process_id: "bootstrap", display_name: "First-install bootstrap" }),
    );
    mockedRuns.mockResolvedValueOnce([]);
    const payload = makeTimelinePayload();
    // Override the universe_sync stage to carry a warning.
    payload.stages[0]!.warning =
      "stage succeeded but rows_processed=0; strict-gate capability fundamentals_raw_seeded cannot be satisfied";
    mockedTimeline.mockResolvedValueOnce(payload);
    renderBootstrap();
    fireEvent.click(await screen.findByRole("tab", { name: "Timeline" }));
    const chip = await screen.findByTestId("stage-warning-chip");
    expect(chip).toBeTruthy();
    expect(chip.getAttribute("title")).toContain("fundamentals_raw_seeded");
  });

  // #1266 — a deliberate skip (slow-connection fallback) must render its
  // reason in neutral slate, not error red; a genuine error stays red.
  it("tones the detail line neutral for a skipped stage and red for an error stage", async () => {
    mockedDetail.mockResolvedValueOnce(
      makeProcessRow({ process_id: "bootstrap", display_name: "First-install bootstrap" }),
    );
    mockedRuns.mockResolvedValueOnce([]);
    const payload = makeTimelinePayload();
    payload.stages[0]!.status = "skipped";
    payload.stages[0]!.last_error =
      "skipped: slow-connection fallback (mbps=9.514); fallback manifest written, bulk archives bypassed";
    payload.stages[1]!.status = "error";
    payload.stages[1]!.last_error = "TimeoutError: archive fetch timed out";
    mockedTimeline.mockResolvedValueOnce(payload);
    renderBootstrap();
    fireEvent.click(await screen.findByRole("tab", { name: "Timeline" }));
    const details = await screen.findAllByTestId("stage-detail-text");
    expect(details.length).toBe(2);
    const skippedDetail = details.find((d) =>
      d.textContent?.includes("slow-connection fallback"),
    )!;
    const errorDetail = details.find((d) =>
      d.textContent?.includes("TimeoutError"),
    )!;
    expect(skippedDetail.className).toContain("text-slate-500");
    expect(skippedDetail.className).not.toContain("text-red");
    expect(errorDetail.className).toContain("text-red-700");
  });

  // #1409 P5 — live signals on a running stage: rate + heartbeat age.
  it("renders rate and heartbeat age on a running stage", async () => {
    mockedDetail.mockResolvedValueOnce(
      makeProcessRow({ process_id: "bootstrap", display_name: "First-install bootstrap" }),
    );
    mockedRuns.mockResolvedValueOnce([]);
    mockedTimeline.mockResolvedValueOnce(makeTimelinePayload());
    renderBootstrap();
    fireEvent.click(await screen.findByRole("tab", { name: "Timeline" }));
    const meta = await screen.findByTestId("stage-live-meta");
    // rate=15600 → "15.6k rows/s"; heartbeat_age=8 → "updated 8s ago".
    expect(meta.textContent).toContain("15.6k rows/s");
    expect(meta.textContent).toContain("updated 8s ago");
  });

  // #1409 P5 — stale chip when the server flags a wedged running stage.
  it("renders the stale chip when a running stage is flagged is_stale", async () => {
    mockedDetail.mockResolvedValueOnce(
      makeProcessRow({ process_id: "bootstrap", display_name: "First-install bootstrap" }),
    );
    mockedRuns.mockResolvedValueOnce([]);
    const payload = makeTimelinePayload();
    payload.stages[1]!.is_stale = true;
    payload.stages[1]!.heartbeat_age_seconds = 2400;
    payload.stages[1]!.rate = null;
    mockedTimeline.mockResolvedValueOnce(payload);
    renderBootstrap();
    fireEvent.click(await screen.findByRole("tab", { name: "Timeline" }));
    expect(await screen.findByTestId("stage-stale-chip")).toBeTruthy();
  });

  // #1409 P5 — no stale chip when a non-running stage has an old heartbeat.
  it("does NOT render the stale chip for non-running stages", async () => {
    mockedDetail.mockResolvedValueOnce(
      makeProcessRow({ process_id: "bootstrap", display_name: "First-install bootstrap" }),
    );
    mockedRuns.mockResolvedValueOnce([]);
    mockedTimeline.mockResolvedValueOnce(makeTimelinePayload());
    renderBootstrap();
    fireEvent.click(await screen.findByRole("tab", { name: "Timeline" }));
    await screen.findByText("Universe Sync");
    // is_stale is false on both fixture stages.
    expect(screen.queryByTestId("stage-stale-chip")).toBeNull();
  });

  // #1409 P5 §5.5 — last-refreshed caption renders once a payload lands.
  it("renders the last-refreshed caption", async () => {
    mockedDetail.mockResolvedValueOnce(
      makeProcessRow({ process_id: "bootstrap", display_name: "First-install bootstrap" }),
    );
    mockedRuns.mockResolvedValueOnce([]);
    mockedTimeline.mockResolvedValueOnce(makeTimelinePayload());
    renderBootstrap();
    fireEvent.click(await screen.findByRole("tab", { name: "Timeline" }));
    const caption = await screen.findByTestId("timeline-refreshed-at");
    // Running fixture → "Auto-refreshing · last refreshed …".
    expect(caption.textContent).toContain("Auto-refreshing");
    expect(caption.textContent).toContain("last refreshed");
  });

  // #1140 Task C — run-level amber dot on complete + has_warnings.
  it("renders the run amber dot when run is complete AND has_warnings", async () => {
    mockedDetail.mockResolvedValueOnce(
      makeProcessRow({ process_id: "bootstrap", display_name: "First-install bootstrap" }),
    );
    mockedRuns.mockResolvedValueOnce([]);
    const payload = makeTimelinePayload();
    payload.run!.status = "complete";
    payload.run!.has_warnings = true;
    mockedTimeline.mockResolvedValueOnce(payload);
    renderBootstrap();
    fireEvent.click(await screen.findByRole("tab", { name: "Timeline" }));
    expect(await screen.findByTestId("run-warning-dot")).toBeTruthy();
  });

  // Control: run-level dot suppressed when run is partial_error (red signal
  // already louder than amber warning).
  it("does NOT render the run amber dot for partial_error runs even when has_warnings", async () => {
    mockedDetail.mockResolvedValueOnce(
      makeProcessRow({ process_id: "bootstrap", display_name: "First-install bootstrap" }),
    );
    mockedRuns.mockResolvedValueOnce([]);
    const payload = makeTimelinePayload();
    payload.run!.status = "partial_error";
    payload.run!.has_warnings = true;
    mockedTimeline.mockResolvedValueOnce(payload);
    renderBootstrap();
    fireEvent.click(await screen.findByRole("tab", { name: "Timeline" }));
    await screen.findByText("Universe Sync");
    expect(screen.queryByTestId("run-warning-dot")).toBeNull();
  });

  it("opens the archive-detail modal when an archive square is clicked", async () => {
    mockedDetail.mockResolvedValueOnce(
      makeProcessRow({ process_id: "bootstrap", display_name: "First-install bootstrap" }),
    );
    mockedRuns.mockResolvedValueOnce([]);
    mockedTimeline.mockResolvedValueOnce(makeTimelinePayload());
    renderBootstrap();
    fireEvent.click(await screen.findByRole("tab", { name: "Timeline" }));
    const button = await screen.findByRole("button", {
      name: /Open archive detail: cik_index_2026Q2.zip/,
    });
    fireEvent.click(button);
    // Nested drawer carries the archive name as the modal heading.
    const dialog = await screen.findByRole("dialog");
    expect(dialog.textContent).toContain("cik_index_2026Q2.zip");
    expect(dialog.textContent).toContain("unresolved_cik");
  });
});

// ---------------------------------------------------------------------------
// Advanced tab — params disclosure renderer (#1064 PR2)
// ---------------------------------------------------------------------------

const SAMPLE_DATE_METADATA: ParamMetadata = {
  name: "min_period_of_report",
  label: "Recency floor",
  help_text: "Skip 13F accessions whose period_of_report is older than this date.",
  field_type: "date",
  default: null,
  advanced_group: true,
  enum_values: null,
  min_value: null,
  max_value: null,
};

describe("ProcessDetailPage — Advanced tab", () => {
  function renderAtAdvanced(processId = "sec_13f_quarterly_sweep") {
    return render(
      <MemoryRouter initialEntries={[`/admin/processes/${processId}`]}>
        <Routes>
          <Route path="admin/processes/:id" element={<ProcessDetailPage />} />
        </Routes>
      </MemoryRouter>,
    );
  }

  it("hidden when row is bootstrap mechanism (no operator-exposable params)", async () => {
    mockedDetail.mockResolvedValue(
      makeProcessRow({
        process_id: "bootstrap",
        mechanism: "bootstrap",
        params_metadata: [],
      }),
    );
    mockedRuns.mockResolvedValue([]);
    renderAtAdvanced("bootstrap");
    await waitFor(() => expect(mockedDetail).toHaveBeenCalled());
    expect(screen.queryByRole("tab", { name: "Advanced" })).toBeNull();
  });

  it("hidden when scheduled job has empty params_metadata", async () => {
    mockedDetail.mockResolvedValue(
      makeProcessRow({
        process_id: "daily_cik_refresh",
        mechanism: "scheduled_job",
        params_metadata: [],
      }),
    );
    mockedRuns.mockResolvedValue([]);
    renderAtAdvanced("daily_cik_refresh");
    await waitFor(() => expect(mockedDetail).toHaveBeenCalled());
    expect(screen.queryByRole("tab", { name: "Advanced" })).toBeNull();
  });

  it("visible when scheduled job has at least one param declaration", async () => {
    mockedDetail.mockResolvedValue(
      makeProcessRow({
        process_id: "sec_13f_quarterly_sweep",
        mechanism: "scheduled_job",
        params_metadata: [SAMPLE_DATE_METADATA],
      }),
    );
    mockedRuns.mockResolvedValue([]);
    renderAtAdvanced();
    expect(
      await screen.findByRole("tab", { name: "Advanced" }),
    ).toBeTruthy();
  });

  it("submit calls runJob with {params} envelope and surfaces request_id", async () => {
    mockedDetail.mockResolvedValue(
      makeProcessRow({
        process_id: "sec_13f_quarterly_sweep",
        mechanism: "scheduled_job",
        params_metadata: [SAMPLE_DATE_METADATA],
      }),
    );
    mockedRuns.mockResolvedValue([]);
    mockedRunJob.mockResolvedValue({ request_id: 7 });
    renderAtAdvanced();
    fireEvent.click(await screen.findByRole("tab", { name: "Advanced" }));
    fireEvent.change(screen.getByLabelText("Recency floor"), {
      target: { value: "2024-01-01" },
    });
    fireEvent.click(
      await screen.findByRole("button", { name: /run with these params/i }),
    );
    await waitFor(() =>
      expect(mockedRunJob).toHaveBeenCalledWith("sec_13f_quarterly_sweep", {
        params: { min_period_of_report: "2024-01-01" },
      }),
    );
    await screen.findByText(/queued as request #7/i);
  });

  it("surfaces 400 ApiError detail inline", async () => {
    mockedDetail.mockResolvedValue(
      makeProcessRow({
        process_id: "sec_13f_quarterly_sweep",
        mechanism: "scheduled_job",
        params_metadata: [SAMPLE_DATE_METADATA],
      }),
    );
    mockedRuns.mockResolvedValue([]);
    mockedRunJob.mockRejectedValue(
      new ApiError(400, "param 'min_period_of_report': cannot coerce ...", "param 'min_period_of_report': cannot coerce ..."),
    );
    renderAtAdvanced();
    fireEvent.click(await screen.findByRole("tab", { name: "Advanced" }));
    fireEvent.change(screen.getByLabelText("Recency floor"), {
      target: { value: "2024-01-01" },
    });
    fireEvent.click(
      await screen.findByRole("button", { name: /run with these params/i }),
    );
    const alert = await screen.findByRole("alert");
    expect(alert.textContent).toMatch(/trigger rejected/);
    expect(alert.textContent).toMatch(/cannot coerce/);
  });
});

/**
 * #2274 — the drill-in must state the SAME health claim as the table that
 * linked to it.
 *
 * Before this the page rendered `STATUS_VISUAL[row.status]` inside the
 * Overview tab. Measured against the live snapshot (72 rows, `partial=False`,
 * 2026-09-19): the two surfaces printed a different health word on **every**
 * row, and on 2 of 72 they printed them in different alarm colours — a
 * `schedule_missed` row that the table pins red showed a calm grey `idle` on
 * the page the operator reached by clicking the red.
 *
 * ⚠ Every fixture here sets `health_verdict` / `verdict_reason` /
 * `stale_reasons` EXPLICITLY. `__fixtures__/processes.ts::deriveVerdict`
 * auto-derives a verdict from `{status, stale_reasons}`, but it is a
 * hand-maintained mirror of `compute_verdict`, not that function — it already
 * differs from the backend on disabled-row precedence. Deriving the value
 * under test from a mirror of the thing under test proves nothing.
 */
describe("ProcessDetailPage — health verdict (#2274)", () => {
  const ALL_VERDICTS = [
    ["current", "current"],
    ["working", "working"],
    ["self_healing", "retrying"],
    ["attention", "needs attention"],
    ["stale_manual", "stale"],
    ["paused", "paused"],
  ] as const;

  it.each(ALL_VERDICTS)(
    "renders health_verdict=%s in the page header as %s",
    async (verdict, label) => {
      mockedDetail.mockResolvedValue(
        makeProcessRow({ health_verdict: verdict, verdict_reason: "" }),
      );
      mockedRuns.mockResolvedValue([]);
      renderAt();
      const pill = await screen.findByTestId("status-pill");
      expect(pill.getAttribute("data-verdict")).toBe(verdict);
      expect(pill.textContent).toBe(label);
      expect(pill.getAttribute("aria-label")).toBe(`Health: ${label}`);
    },
  );

  it("keeps the verdict visible on every tab, not just Overview", async () => {
    // The pill lives in the page header for exactly this reason: a health
    // headline that vanishes when the operator opens History is not a
    // headline. ProblemsPanel / StaleBanner / the bootstrap timeline all
    // deep-link into this route.
    mockedDetail.mockResolvedValue(
      makeProcessRow({
        health_verdict: "attention",
        verdict_reason: "schedule missed",
        stale_reasons: ["schedule_missed"],
      }),
    );
    mockedRuns.mockResolvedValue([]);
    renderAt();
    await screen.findByTestId("status-pill");
    for (const tabName of ["History", "Errors", "Overview"]) {
      fireEvent.click(screen.getByRole("tab", { name: tabName }));
      expect(screen.getByTestId("status-pill").getAttribute("data-verdict")).toBe(
        "attention",
      );
    }
  });

  it("shows the verdict AND the raw process state when they diverge", async () => {
    // The exact live shape: `core_rebalance_observation`, status `idle`,
    // verdict `attention`. Both facts belong on the page — the verdict is the
    // health claim, `status` is the adapter-normalised state it was computed
    // from. Dropping either is how the two surfaces disagreed in the first
    // place.
    mockedDetail.mockResolvedValue(
      makeProcessRow({
        process_id: "core_rebalance_observation",
        status: "idle",
        health_verdict: "attention",
        verdict_reason: "schedule missed",
        stale_reasons: ["schedule_missed"],
      }),
    );
    mockedRuns.mockResolvedValue([]);
    renderAt();
    const pill = await screen.findByTestId("status-pill");
    expect(pill.getAttribute("data-verdict")).toBe("attention");
    // `Process state` is plain text, NOT a second toned pill: two toned pills
    // side by side re-create the two-cells-that-disagree defect.
    expect(screen.getByText("Process state")).toBeTruthy();
    expect(screen.getByText("idle")).toBeTruthy();
    expect(screen.getAllByTestId("status-pill")).toHaveLength(1);
  });

  it("renders every stale reason as a chip, in payload order", async () => {
    mockedDetail.mockResolvedValue(
      makeProcessRow({
        status: "running",
        health_verdict: "attention",
        verdict_reason: "running past its runtime ceiling",
        // Two reasons fire; `verdict_reason` can only show one. The full list
        // is why `stale_reasons` is on the payload at all.
        stale_reasons: ["runtime_ceiling", "mid_flight_stuck"],
      }),
    );
    mockedRuns.mockResolvedValue([]);
    renderAt();
    await screen.findByTestId("stale-reasons");
    const chips = screen.getAllByTestId("stale-reason-chip");
    expect(chips.map((c) => c.getAttribute("data-reason"))).toEqual([
      "runtime_ceiling",
      "mid_flight_stuck",
    ]);
    expect(chips.map((c) => c.textContent)).toEqual([
      "past runtime ceiling",
      "no progress",
    ]);
    // ⚠ No elapsed suffix on any chip. `ProcessRow` appends one to its reason
    // line, but that advances only because the TABLE polls and folds the
    // elapsed string into `processRowSignature`. This page's envelope has no
    // interval, so a duration here would freeze at fetch time while reading
    // as live.
    for (const chip of chips) expect(chip.textContent).not.toMatch(/\d+[smhd]/);
  });

  it("renders no chip container when there are no stale reasons", async () => {
    mockedDetail.mockResolvedValue(
      makeProcessRow({ health_verdict: "current", stale_reasons: [] }),
    );
    mockedRuns.mockResolvedValue([]);
    renderAt();
    await screen.findByTestId("status-pill");
    expect(screen.queryByTestId("stale-reasons")).toBeNull();
  });

  it("a paused row keeps its neutral verdict while still showing the reason", async () => {
    // ⚠⚠ The suppression test. `compute_verdict` returns neutral `paused` for
    // a halted row that still carries `schedule_missed` — the kill switch is
    // the loop's normal state and #1831 measured what false-red flooding costs
    // (~42 halted jobs painted red, burying the real failures). The chip must
    // report the reason WITHOUT re-alarming the row the backend calmed.
    mockedDetail.mockResolvedValue(
      makeProcessRow({
        status: "disabled",
        health_verdict: "paused",
        verdict_reason: "",
        stale_reasons: ["schedule_missed"],
      }),
    );
    mockedRuns.mockResolvedValue([]);
    renderAt();
    const pill = await screen.findByTestId("status-pill");
    expect(pill.getAttribute("data-verdict")).toBe("paused");
    const chip = screen.getByTestId("stale-reason-chip");
    expect(chip.textContent).toBe("schedule missed");
    // "Carries no tone", machine-checkable: every `Badge` TONE_CLASSES entry
    // sets a `bg-*` utility, so the absence of one is the absence of a tone.
    // Asserted as a class FAMILY rather than a specific colour — pinning raw
    // Tailwind is how `eightKSeverity.ts` shipped light-only chips past the
    // dark gate.
    expect(chip.className).not.toMatch(/\bbg-/);
    // ...and no pill GEOMETRY either. The same rule `ProcessRow::RecentReaps`
    // follows: a fact that must not read as a second status is muted text, not
    // a bordered chip. `check-hand-rolled-pills` guards the class pattern; this
    // guards the intent at the one place it is rendered.
    expect(chip.className).not.toMatch(/\bborder\b/);
    // And the verdict pill stays the only toned health claim on the page.
    expect(screen.getAllByTestId("status-pill")).toHaveLength(1);
  });

  it("renders verdict_reason when non-empty and nothing when empty", async () => {
    mockedDetail.mockResolvedValue(
      makeProcessRow({
        health_verdict: "attention",
        verdict_reason: "schedule missed",
        stale_reasons: ["schedule_missed"],
      }),
    );
    mockedRuns.mockResolvedValue([]);
    renderAt();
    expect((await screen.findByTestId("verdict-reason")).textContent).toBe(
      "schedule missed",
    );

    cleanup();
    mockedDetail.mockResolvedValue(
      makeProcessRow({ health_verdict: "current", verdict_reason: "" }),
    );
    renderAt();
    await screen.findByTestId("status-pill");
    expect(screen.queryByTestId("verdict-reason")).toBeNull();
  });
});

/**
 * #2274 — the cross-surface test. The table and the drill-in render the same
 * `VerdictPill`, so given ONE payload row they must make an identical claim.
 *
 * ⚠ This does not — and cannot — assert that the two surfaces agree at any
 * given instant: the table polls and the drill-in does not, so they can be
 * looking at different payloads. The guarantee under test is narrower and is
 * the one the defect was: GIVEN THE SAME ROW, the same claim.
 */
describe("table vs drill-in — same row, same health claim (#2274)", () => {
  it.each([
    ["attention", "idle"],
    ["paused", "disabled"],
    ["self_healing", "pending_retry"],
    ["current", "ok"],
  ] as const)(
    "verdict=%s with status=%s renders identically on both surfaces",
    async (verdict, status) => {
      const row = makeProcessRow({
        status,
        health_verdict: verdict,
        verdict_reason: "",
        stale_reasons: [],
      });

      const table = render(
        <MemoryRouter>
          <table>
            <tbody>
              <ProcessRow
                row={row}
                signature={processRowSignature(row)}
                triggerError={undefined}
                cancelError={undefined}
                busy={false}
                onIterate={vi.fn()}
                onFullWash={vi.fn()}
                onCancel={vi.fn()}
              />
            </tbody>
          </table>
        </MemoryRouter>,
      );
      const tablePill = table.getByTestId("status-pill");
      const fromTable = {
        label: tablePill.textContent,
        verdict: tablePill.getAttribute("data-verdict"),
        aria: tablePill.getAttribute("aria-label"),
      };
      cleanup();

      mockedDetail.mockResolvedValue(row);
      mockedRuns.mockResolvedValue([]);
      renderAt();
      const detailPill = await screen.findByTestId("status-pill");
      expect({
        label: detailPill.textContent,
        verdict: detailPill.getAttribute("data-verdict"),
        aria: detailPill.getAttribute("aria-label"),
      }).toEqual(fromTable);
    },
  );
});
