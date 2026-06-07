import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { ApiError } from "@/api/client";
import { cancelProcess, triggerProcess } from "@/api/processes";
import { ProcessesTable } from "@/components/admin/ProcessesTable";
import {
  makeProcessRow,
  makeProcessList,
} from "@/components/admin/__fixtures__/processes";

vi.mock("@/api/processes", async () => {
  const actual =
    await vi.importActual<typeof import("@/api/processes")>("@/api/processes");
  return {
    ...actual,
    triggerProcess: vi.fn(),
    cancelProcess: vi.fn(),
  };
});

const mockTrigger = vi.mocked(triggerProcess);
const mockCancel = vi.mocked(cancelProcess);

beforeEach(() => {
  mockTrigger.mockReset();
  mockCancel.mockReset();
});

function renderTable(
  rows = [makeProcessRow()],
  partial = false,
  onMutationSuccess = vi.fn(),
) {
  const result = render(
    <MemoryRouter>
      <ProcessesTable
        snapshot={makeProcessList(rows, partial)}
        onMutationSuccess={onMutationSuccess}
      />
    </MemoryRouter>,
  );
  // #1513 — non-actionable rows collapse by default. Expand so the
  // existing row-level assertions (actions, sort, filter) see every row;
  // the default-collapsed behaviour has dedicated tests below.
  const toggle = screen
    .queryByTestId("collapsed-disclosure")
    ?.querySelector("button");
  if (toggle) fireEvent.click(toggle);
  return result;
}

describe("ProcessesTable", () => {
  it("renders a row per process with correct status pill", () => {
    renderTable([
      makeProcessRow({ process_id: "a", status: "ok", display_name: "A" }),
      makeProcessRow({ process_id: "b", status: "running", display_name: "B" }),
      makeProcessRow({ process_id: "c", status: "failed", display_name: "C" }),
    ]);
    expect(screen.getByRole("link", { name: "A" })).toBeTruthy();
    expect(screen.getByRole("link", { name: "B" })).toBeTruthy();
    expect(screen.getByRole("link", { name: "C" })).toBeTruthy();
  });

  it("sorts failed rows above ok rows", () => {
    const { container } = renderTable([
      makeProcessRow({ process_id: "ok_one", status: "ok", display_name: "A_ok" }),
      makeProcessRow({ process_id: "fail_one", status: "failed", display_name: "B_failed" }),
    ]);
    const links = Array.from(container.querySelectorAll("tbody a")).map(
      (a) => a.textContent ?? "",
    );
    expect(links[0]).toBe("B_failed");
    expect(links[1]).toBe("A_ok");
  });

  it("filters by selected lane", () => {
    renderTable([
      makeProcessRow({ process_id: "x", lane: "sec", display_name: "X_sec" }),
      // Pinned verdict (attention) so the row stays visible after the lane
      // switch re-collapses the default-quiet rows (#1513) — this test is
      // about lane filtering, not the collapse.
      makeProcessRow({
        process_id: "y",
        lane: "ownership",
        display_name: "Y_own",
        stale_reasons: ["queue_stuck"],
      }),
    ]);
    fireEvent.click(screen.getByRole("button", { name: /^Ownership/ }));
    expect(screen.queryByRole("link", { name: "X_sec" })).toBeNull();
    expect(screen.getByRole("link", { name: "Y_own" })).toBeTruthy();
  });

  it("renders the partial banner when envelope.partial is true", () => {
    renderTable([makeProcessRow()], true);
    expect(screen.getByText(/adapter is unavailable/i)).toBeTruthy();
  });

  it("does NOT render the partial banner when envelope.partial is false", () => {
    renderTable([makeProcessRow()], false);
    expect(screen.queryByText(/adapter is unavailable/i)).toBeNull();
  });

  it("posts iterate on click and calls onMutationSuccess", async () => {
    mockTrigger.mockResolvedValueOnce({ request_id: 1, mode: "iterate" });
    const onSuccess = vi.fn();
    renderTable([makeProcessRow()], false, onSuccess);
    fireEvent.click(screen.getByRole("button", { name: "Iterate" }));
    await waitFor(() => expect(mockTrigger).toHaveBeenCalledTimes(1));
    expect(mockTrigger).toHaveBeenCalledWith("sec_form4_ingest", {
      mode: "iterate",
    });
    await waitFor(() => expect(onSuccess).toHaveBeenCalled());
  });

  it("renders structured tooltip on 409 iterate_already_pending", async () => {
    mockTrigger.mockRejectedValueOnce(
      new ApiError(409, "already in flight", {
        reason: "iterate_already_pending",
      }),
    );
    renderTable();
    fireEvent.click(screen.getByRole("button", { name: "Iterate" }));
    await waitFor(() => expect(screen.getByText("trigger rejected")).toBeTruthy());
    const note = screen.getByText("trigger rejected");
    expect(note.getAttribute("title")).toContain("already in flight");
  });

  it("full-wash confirm button is enabled immediately (no type-to-confirm)", async () => {
    // Operator decision 2026-05-22 (#1264): type-to-confirm dropped —
    // process names are internal identifiers, double-confirm with a
    // single click on the red verb button is the chosen UX. The modal
    // no longer renders the 'Process name confirmation' input.
    renderTable([
      makeProcessRow({ display_name: "Insider Form 4 ingest", can_full_wash: true }),
    ]);
    fireEvent.click(screen.getByRole("button", { name: "Full-wash" }));
    const dialog = await screen.findByRole("dialog");
    expect(
      dialog.querySelector("input[aria-label='Process name confirmation']"),
    ).toBeNull();
    const confirmBtn = Array.from(dialog.querySelectorAll("button")).find(
      (b) => b.textContent === "Full-wash",
    ) as HTMLButtonElement;
    expect(confirmBtn.disabled).toBe(false);
  });

  it("cancel modal exposes terminate behind 'More' disclosure with honest copy", async () => {
    renderTable([makeProcessRow({ can_cancel: true })]);
    fireEvent.click(screen.getByRole("button", { name: "Cancel" }));
    const dialog = await screen.findByRole("dialog");
    expect(dialog.textContent).toContain("Cooperative cancel");
    // Honest copy is gated behind the disclosure — terminate must NOT
    // be tabbable until the operator explicitly opens the More section.
    expect(dialog.textContent).not.toContain("Terminate marks for cleanup");
    const moreToggle = Array.from(dialog.querySelectorAll("button")).find(
      (b) => b.textContent?.includes("More — terminate"),
    ) as HTMLButtonElement;
    expect(moreToggle.getAttribute("aria-expanded")).toBe("false");
    fireEvent.click(moreToggle);
    expect(dialog.textContent).toContain("Terminate marks for cleanup");
    expect(dialog.textContent).toContain("Active SEC fetches continue");
    expect(moreToggle.getAttribute("aria-expanded")).toBe("true");
  });

  it("cooperative cancel POSTs mode=cooperative", async () => {
    mockCancel.mockResolvedValueOnce({
      target_run_kind: "job_run",
      target_run_id: 42,
    });
    renderTable([makeProcessRow({ can_cancel: true })]);
    fireEvent.click(screen.getByRole("button", { name: "Cancel" }));
    const dialog = await screen.findByRole("dialog");
    const confirmBtn = Array.from(dialog.querySelectorAll("button")).find(
      (b) => b.textContent === "Cancel cooperatively",
    ) as HTMLButtonElement;
    fireEvent.click(confirmBtn);
    await waitFor(() => expect(mockCancel).toHaveBeenCalledTimes(1));
    expect(mockCancel).toHaveBeenCalledWith("sec_form4_ingest", {
      mode: "cooperative",
    });
  });

  it("terminate POSTs mode=terminate", async () => {
    mockCancel.mockResolvedValueOnce({
      target_run_kind: "job_run",
      target_run_id: 42,
    });
    renderTable([makeProcessRow({ can_cancel: true })]);
    fireEvent.click(screen.getByRole("button", { name: "Cancel" }));
    const dialog = await screen.findByRole("dialog");
    // Open the More disclosure so the terminate button mounts (Codex
    // pre-push BLOCKING — terminate is otherwise hidden but tabbable).
    const moreToggle = Array.from(dialog.querySelectorAll("button")).find(
      (b) => b.textContent?.includes("More — terminate"),
    ) as HTMLButtonElement;
    fireEvent.click(moreToggle);
    const terminateBtn = Array.from(dialog.querySelectorAll("button")).find(
      (b) => b.textContent === "Terminate (mark for cleanup)",
    ) as HTMLButtonElement;
    fireEvent.click(terminateBtn);
    await waitFor(() => expect(mockCancel).toHaveBeenCalledTimes(1));
    expect(mockCancel).toHaveBeenCalledWith("sec_form4_ingest", {
      mode: "terminate",
    });
  });

  it("renders empty-state copy when lane filter excludes all rows", () => {
    renderTable([
      makeProcessRow({ lane: "sec", display_name: "X" }),
    ]);
    fireEvent.click(screen.getByRole("button", { name: /^Ownership/ }));
    expect(screen.getByText(/No processes match/)).toBeTruthy();
  });

  // ---------------------------------------------------------------------
  // #1513 — clean-bill health header integration. Header unit-behaviour
  // lives in StaleBanner.test.tsx; here we just confirm the table mounts it
  // with the positive all-clear when healthy and the summary otherwise.
  // ---------------------------------------------------------------------

  it("renders the all-clear header when all rows are healthy", () => {
    renderTable([makeProcessRow({ stale_reasons: [] })]);
    const header = screen.getByTestId("health-header");
    expect(header.textContent).toContain("All systems current");
  });

  it("renders the attention summary header when at least one row needs attention", () => {
    renderTable([
      makeProcessRow({ process_id: "a", stale_reasons: [] }),
      makeProcessRow({
        process_id: "b",
        stale_reasons: ["watermark_gap"],
      }),
    ]);
    expect(screen.getByTestId("health-header").textContent).toContain(
      "need attention",
    );
  });

  it("sorts stale rows above non-stale ok rows (status='ok' + stale_reasons populated)", () => {
    const { container } = renderTable([
      makeProcessRow({
        process_id: "ok_one",
        status: "ok",
        display_name: "A_ok",
      }),
      makeProcessRow({
        process_id: "ok_stale",
        status: "ok",
        display_name: "B_stale",
        stale_reasons: ["watermark_gap"],
      }),
    ]);
    const links = Array.from(container.querySelectorAll("tbody a")).map(
      (a) => a.textContent ?? "",
    );
    // Stale row floats up above ok row even though both have status='ok'.
    expect(links[0]).toBe("B_stale");
    expect(links[1]).toBe("A_ok");
  });

  it("#1512: failed and overdue rows are peers (both attention), tie-broken deterministically", () => {
    // Under the single verdict a failed row and an ok+overdue row both
    // collapse to `attention` — there is no failed>stale hierarchy any
    // more. With equal verdict + equal next_fire_at the tiebreak is
    // display_name, so A_overdue precedes B_failed.
    const { container } = renderTable([
      makeProcessRow({
        process_id: "overdue",
        status: "ok",
        display_name: "A_overdue",
        stale_reasons: ["queue_stuck"],
      }),
      makeProcessRow({
        process_id: "fail",
        status: "failed",
        display_name: "B_failed",
      }),
    ]);
    const pills = Array.from(
      container.querySelectorAll("[data-testid='status-pill']"),
    ).map((p) => p.getAttribute("data-verdict"));
    expect(pills).toEqual(["attention", "attention"]);
    const links = Array.from(container.querySelectorAll("tbody a")).map(
      (a) => a.textContent ?? "",
    );
    expect(links[0]).toBe("A_overdue");
    expect(links[1]).toBe("B_failed");
  });

  // ---------------------------------------------------------------------
  // PR9 (#1085) — keyboard nav. Within an enabled row, the operator can
  // tab from the drill-in link through Iterate → Full-wash → Cancel
  // without picking up any unexpected focus stop. The lane-filter
  // buttons render above the table and are NOT in scope here — the
  // test starts focus on the row link to scope the assertion to the
  // row's own DOM order.
  // ---------------------------------------------------------------------

  it("row keyboard order: link → ⓘ tooltip → Iterate → Full-wash → Cancel (all enabled)", async () => {
    // PR4 #1082: the ⓘ description tooltip is a focusable button so
    // keyboard-only operators can reach it; it slots in between the
    // row link and the Iterate button. The order pin protects the
    // operator's expected focus path through the row.
    const user = userEvent.setup();
    renderTable([
      makeProcessRow({
        process_id: "kbd",
        display_name: "Keyboard Row",
        can_iterate: true,
        can_full_wash: true,
        can_cancel: true,
        description: "kbd row description",
      }),
    ]);
    const link = screen.getByRole("link", { name: "Keyboard Row" });
    link.focus();
    expect(document.activeElement).toBe(link);

    await user.tab();
    expect(document.activeElement).toBe(
      screen.getByTestId("process-description-tooltip"),
    );

    await user.tab();
    expect(document.activeElement).toBe(
      screen.getByRole("button", { name: "Iterate" }),
    );

    await user.tab();
    expect(document.activeElement).toBe(
      screen.getByRole("button", { name: "Full-wash" }),
    );

    await user.tab();
    expect(document.activeElement).toBe(
      screen.getByRole("button", { name: "Cancel" }),
    );
  });

  // ---------------------------------------------------------------------
  // PR3a #1064 — bootstrap-only render mode
  // ---------------------------------------------------------------------
  // Per data-engineer skill §7.1, when bootstrap_state.status !=
  // 'complete' the ProcessesTable hides every non-bootstrap row so the
  // operator's only path forward is the bootstrap row. Lane filter
  // hidden in this mode (one-row list).

  function renderTableWithStatus(
    rows: Parameters<typeof makeProcessList>[0],
    bootstrapStatus:
      | "pending"
      | "running"
      | "complete"
      | "partial_error"
      | null,
  ) {
    const result = render(
      <MemoryRouter>
        <ProcessesTable
          snapshot={makeProcessList(rows, false)}
          onMutationSuccess={vi.fn()}
          bootstrapStatus={bootstrapStatus}
        />
      </MemoryRouter>,
    );
    // #1513 — expand the steady-state collapse so non-attention rows are
    // assertable (no-op in bootstrap-only mode, where collapse is disabled).
    const toggle = screen
      .queryByTestId("collapsed-disclosure")
      ?.querySelector("button");
    if (toggle) fireEvent.click(toggle);
    return result;
  }

  it("hides non-bootstrap rows + lane filter when bootstrap is partial_error", () => {
    renderTableWithStatus(
      [
        makeProcessRow({
          process_id: "bootstrap",
          mechanism: "bootstrap",
          display_name: "First-install bootstrap",
        }),
        makeProcessRow({
          process_id: "daily_cik_refresh",
          mechanism: "scheduled_job",
          display_name: "CIK refresh",
        }),
        makeProcessRow({
          process_id: "form4_sweep",
          mechanism: "ingest_sweep",
          display_name: "Form 4 sweep",
        }),
      ],
      "partial_error",
    );
    expect(
      screen.getByRole("link", { name: "First-install bootstrap" }),
    ).toBeTruthy();
    expect(screen.queryByRole("link", { name: "CIK refresh" })).toBeNull();
    expect(screen.queryByRole("link", { name: "Form 4 sweep" })).toBeNull();
    expect(
      screen.getByText(/Other categories are gated until bootstrap reaches/i),
    ).toBeTruthy();
    // Lane chip filter (LaneFilter, role="toolbar") is suppressed in this mode.
    expect(
      screen.queryByRole("toolbar", { name: /Filter processes by lane/i }),
    ).toBeNull();
  });

  it("gate banner uses 'run the bootstrap' copy (not 're-run') when pending", () => {
    renderTableWithStatus(
      [
        makeProcessRow({
          process_id: "bootstrap",
          mechanism: "bootstrap",
          display_name: "First-install bootstrap",
          status: "pending_first_run",
        }),
      ],
      "pending",
    );
    expect(
      screen.getByText(/run the bootstrap from the bootstrap row/i),
    ).toBeTruthy();
    expect(screen.queryByText(/re-run failed stages/i)).toBeNull();
  });

  it("hides the completed bootstrap row but shows steady-state rows when complete", () => {
    // #1508 — once bootstrap is complete the first-install row is done and is
    // removed from the steady-state ops list; the other categories appear.
    renderTableWithStatus(
      [
        makeProcessRow({
          process_id: "bootstrap",
          mechanism: "bootstrap",
          display_name: "First-install bootstrap",
        }),
        makeProcessRow({
          process_id: "daily_cik_refresh",
          mechanism: "scheduled_job",
          display_name: "CIK refresh",
        }),
      ],
      "complete",
    );
    expect(
      screen.queryByRole("link", { name: "First-install bootstrap" }),
    ).toBeNull();
    expect(screen.getByRole("link", { name: "CIK refresh" })).toBeTruthy();
    expect(
      screen.queryByText(/Other categories are gated/i),
    ).toBeNull();
  });

  it("keeps the bootstrap row when status is unknown (fail-open)", () => {
    // null = bootstrap-status fetch pending/errored: don't hide information
    // on uncertainty — render the full table including the bootstrap row.
    renderTableWithStatus(
      [
        makeProcessRow({
          process_id: "bootstrap",
          mechanism: "bootstrap",
          display_name: "First-install bootstrap",
        }),
        makeProcessRow({
          process_id: "daily_cik_refresh",
          mechanism: "scheduled_job",
          display_name: "CIK refresh",
        }),
      ],
      null,
    );
    expect(
      screen.getByRole("link", { name: "First-install bootstrap" }),
    ).toBeTruthy();
    expect(screen.getByRole("link", { name: "CIK refresh" })).toBeTruthy();
  });

  it("bootstrap full-wash modal uses 'Re-run all' heading + replay copy", async () => {
    renderTableWithStatus(
      [
        makeProcessRow({
          process_id: "bootstrap",
          mechanism: "bootstrap",
          display_name: "First-install bootstrap",
          can_iterate: true,
          can_full_wash: true,
        }),
      ],
      "partial_error",
    );
    fireEvent.click(screen.getByRole("button", { name: "Re-run all" }));
    expect(
      await screen.findByRole("heading", { name: /Confirm Re-run all/i }),
    ).toBeTruthy();
    expect(
      screen.getByText(/replays the full first-install bootstrap/i),
    ).toBeTruthy();
    expect(screen.queryByText(/resets the watermark/i)).toBeNull();
  });

  it("first-install bootstrap modal uses 'Start bootstrap' heading + non-destructive copy", async () => {
    renderTableWithStatus(
      [
        makeProcessRow({
          process_id: "bootstrap",
          mechanism: "bootstrap",
          display_name: "First-install bootstrap",
          status: "pending_first_run",
          can_iterate: false,
          can_full_wash: true,
          can_cancel: false,
        }),
      ],
      "pending",
    );
    fireEvent.click(screen.getByRole("button", { name: "Run bootstrap" }));
    expect(
      await screen.findByRole("heading", { name: /Start bootstrap/i }),
    ).toBeTruthy();
    expect(screen.getByText(/Safe to run/i)).toBeTruthy();
    // Destructive re-run copy must NOT appear on a never-run row.
    expect(screen.queryByText(/resets every stage/i)).toBeNull();
    expect(screen.queryByRole("heading", { name: /Re-run all/i })).toBeNull();
  });

  it("scheduled_job full-wash modal keeps original watermark copy", async () => {
    renderTableWithStatus(
      [
        makeProcessRow({
          process_id: "daily_cik_refresh",
          mechanism: "scheduled_job",
          display_name: "CIK refresh",
          can_iterate: true,
          can_full_wash: true,
        }),
      ],
      "complete",
    );
    fireEvent.click(screen.getByRole("button", { name: "Full-wash" }));
    expect(
      await screen.findByRole("heading", { name: /Confirm full-wash/i }),
    ).toBeTruthy();
    expect(screen.getByText(/resets the watermark/i)).toBeTruthy();
  });

  it("fail-open: renders all rows when bootstrapStatus is null (fetch pending/errored)", () => {
    renderTableWithStatus(
      [
        makeProcessRow({
          process_id: "bootstrap",
          mechanism: "bootstrap",
          display_name: "First-install bootstrap",
        }),
        makeProcessRow({
          process_id: "daily_cik_refresh",
          mechanism: "scheduled_job",
          display_name: "CIK refresh",
        }),
      ],
      null,
    );
    expect(
      screen.getByRole("link", { name: "First-install bootstrap" }),
    ).toBeTruthy();
    expect(screen.getByRole("link", { name: "CIK refresh" })).toBeTruthy();
  });

  // ---------------------------------------------------------------------
  // #1513 — non-actionable rows collapse by default behind an inline
  // disclosure; needs-attention rows are always visible.
  // ---------------------------------------------------------------------

  // Raw render (the shared renderTable auto-expands; here we want the
  // default-collapsed state).
  function renderTableRaw(rows: ReturnType<typeof makeProcessRow>[]) {
    return render(
      <MemoryRouter>
        <ProcessesTable
          snapshot={makeProcessList(rows, false)}
          onMutationSuccess={vi.fn()}
        />
      </MemoryRouter>,
    );
  }

  it("#1508 C3: pins only attention; collapses current/working/self-healing", () => {
    renderTableRaw([
      makeProcessRow({ process_id: "att", display_name: "Attn", stale_reasons: ["queue_stuck"] }),
      makeProcessRow({ process_id: "run", display_name: "Runn", status: "running", stale_reasons: [] }),
      makeProcessRow({ process_id: "cur", display_name: "Curr", status: "ok", stale_reasons: [] }),
      makeProcessRow({ process_id: "heal", display_name: "Heal", status: "pending_retry", stale_reasons: [] }),
    ]);
    // Only attention pins (the operator must act).
    expect(screen.getByRole("link", { name: "Attn" })).toBeTruthy();
    // The three calm verdicts all hide behind the disclosure. A live run
    // ("Runn") keeps its Cancel affordance once expanded — it just no
    // longer screams when nothing is wrong.
    expect(screen.queryByRole("link", { name: "Runn" })).toBeNull();
    expect(screen.queryByRole("link", { name: "Curr" })).toBeNull();
    expect(screen.queryByRole("link", { name: "Heal" })).toBeNull();
    const disclosure = screen.getByTestId("collapsed-disclosure");
    expect(disclosure.textContent).toContain("1 current");
    expect(disclosure.textContent).toContain("1 working");
    expect(disclosure.textContent).toContain("1 self-healing");
  });

  it("reveals the collapsed rows when the disclosure is clicked", () => {
    renderTableRaw([
      makeProcessRow({ process_id: "cur", display_name: "Curr", status: "ok", stale_reasons: [] }),
    ]);
    expect(screen.queryByRole("link", { name: "Curr" })).toBeNull();
    fireEvent.click(
      screen.getByTestId("collapsed-disclosure").querySelector("button")!,
    );
    expect(screen.getByRole("link", { name: "Curr" })).toBeTruthy();
  });

  it("renders no disclosure when every row needs attention", () => {
    renderTableRaw([
      makeProcessRow({ process_id: "a", display_name: "A", stale_reasons: ["queue_stuck"] }),
      makeProcessRow({ process_id: "b", display_name: "B", status: "failed", stale_reasons: [] }),
    ]);
    expect(screen.queryByTestId("collapsed-disclosure")).toBeNull();
    expect(screen.getByRole("link", { name: "A" })).toBeTruthy();
    expect(screen.getByRole("link", { name: "B" })).toBeTruthy();
  });
});
