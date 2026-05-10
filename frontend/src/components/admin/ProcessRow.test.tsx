import { fireEvent, render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { describe, expect, it, vi } from "vitest";

import { ApiError } from "@/api/client";
import { makeProcessRow, makeError } from "@/components/admin/__fixtures__/processes";
import { ProcessRow } from "@/components/admin/ProcessRow";

function renderRow(props: Partial<Parameters<typeof ProcessRow>[0]> = {}) {
  const row = props.row ?? makeProcessRow();
  return render(
    <MemoryRouter>
      <table>
        <tbody>
          <ProcessRow
            row={row}
            triggerError={undefined}
            cancelError={undefined}
            busy={false}
            onIterate={vi.fn()}
            onFullWash={vi.fn()}
            onCancel={vi.fn()}
            {...props}
          />
        </tbody>
      </table>
    </MemoryRouter>,
  );
}

describe("ProcessRow", () => {
  it("renders display_name as a link to the drill-in route", () => {
    renderRow({ row: makeProcessRow({ display_name: "Insider Form 4" }) });
    const link = screen.getByRole("link", { name: /Insider Form 4/ });
    expect(link.getAttribute("href")).toContain("/admin/processes/");
  });

  it("renders watermark.human as the Iterate tooltip when can_iterate=true", () => {
    renderRow({
      row: makeProcessRow({
        can_iterate: true,
        watermark: {
          cursor_kind: "filed_at",
          cursor_value: "2026-05-08T13:00:00+00:00",
          human: "Resume from filings filed after 2026-05-08T13:00Z",
          last_advanced_at: "2026-05-08T13:00:00+00:00",
        },
      }),
    });
    const btn = screen.getByRole("button", { name: "Iterate" });
    expect(btn.getAttribute("title")).toBe(
      "Resume from filings filed after 2026-05-08T13:00Z",
    );
  });

  it("falls back to 'no resume cursor' when watermark is null", () => {
    renderRow({ row: makeProcessRow({ watermark: null }) });
    const btn = screen.getByRole("button", { name: "Iterate" });
    expect(btn.getAttribute("title")).toBe("no resume cursor");
  });

  it("disables Iterate / Full-wash / Cancel per envelope flags", () => {
    renderRow({
      row: makeProcessRow({
        can_iterate: false,
        can_full_wash: false,
        can_cancel: false,
      }),
    });
    expect(
      (screen.getByRole("button", { name: "Iterate" }) as HTMLButtonElement)
        .disabled,
    ).toBe(true);
    expect(
      (screen.getByRole("button", { name: "Full-wash" }) as HTMLButtonElement)
        .disabled,
    ).toBe(true);
    expect(
      (screen.getByRole("button", { name: "Cancel" }) as HTMLButtonElement)
        .disabled,
    ).toBe(true);
  });

  it("renders pulsing left border on running rows (motion-reduce respected)", () => {
    const { container } = renderRow({
      row: makeProcessRow({ status: "running" }),
    });
    const tr = container.querySelector("tr[data-status='running']") as HTMLElement;
    expect(tr.className).toContain("animate-pulse");
    expect(tr.className).toContain("motion-reduce:animate-none");
    expect(tr.className).toContain("border-l-sky-500");
  });

  it("does not pulse on terminal statuses", () => {
    const { container } = renderRow({ row: makeProcessRow({ status: "ok" }) });
    const tr = container.querySelector("tr[data-status='ok']") as HTMLElement;
    expect(tr.className).not.toContain("animate-pulse");
  });

  it("renders inline error preview when status=failed and last_n_errors non-empty", () => {
    renderRow({
      row: makeProcessRow({
        status: "failed",
        last_n_errors: [
          makeError({ error_class: "ConnectionTimeout", count: 3 }),
          makeError({ error_class: "MissingCIK", count: 1 }),
        ],
      }),
    });
    expect(screen.getByText("ConnectionTimeout")).toBeTruthy();
    expect(screen.getByText("MissingCIK")).toBeTruthy();
  });

  it("does NOT render error preview on status=running (auto-hide-on-retry already empty BE-side)", () => {
    renderRow({
      row: makeProcessRow({ status: "running", last_n_errors: [] }),
    });
    expect(screen.queryByText("ConnectionTimeout")).toBeNull();
  });

  it("renders pending_retry tooltip on the status pill", () => {
    renderRow({ row: makeProcessRow({ status: "pending_retry" }) });
    const pill = screen.getByText(/pending retry/i);
    expect(pill.getAttribute("title")).toContain("hiding");
    expect(pill.getAttribute("title")).toContain("retry");
  });

  it("renders structured 409 reason tooltip when triggerError is an ApiError", () => {
    renderRow({
      triggerError: new ApiError(409, "iterate already in flight", {
        reason: "iterate_already_pending",
      }),
    });
    const note = screen.getByText("trigger rejected");
    expect(note.getAttribute("title")).toContain("already in flight");
  });

  it("falls back to a fixed phrase when error has no known reason", () => {
    renderRow({
      triggerError: new ApiError(500, "Internal Server Error"),
    });
    const note = screen.getByText("trigger rejected");
    expect(note.getAttribute("title")).toContain("browser console");
  });

  // ---------------------------------------------------------------------
  // PR8 (#1083) — four-case stale model chips + pulsing border.
  // ---------------------------------------------------------------------

  it("renders no stale chips when stale_reasons is empty", () => {
    const { container } = renderRow({
      row: makeProcessRow({ status: "ok", stale_reasons: [] }),
    });
    expect(
      container.querySelector("[data-testid='stale-chips']"),
    ).toBeNull();
  });

  it("renders one chip per stale reason (multiple can fire)", () => {
    const { container } = renderRow({
      row: makeProcessRow({
        status: "ok",
        stale_reasons: ["schedule_missed", "watermark_gap"],
      }),
    });
    const chips = container.querySelector(
      "[data-testid='stale-chips']",
    ) as HTMLElement;
    expect(chips).toBeTruthy();
    expect(
      chips.querySelectorAll("[data-stale-reason='schedule_missed']").length,
    ).toBe(1);
    expect(
      chips.querySelectorAll("[data-stale-reason='watermark_gap']").length,
    ).toBe(1);
    expect(chips.textContent).toContain("schedule missed");
    expect(chips.textContent).toContain("source has fresh data");
  });

  it("mid_flight_stuck chip suffixes elapsed-since-heartbeat", () => {
    const fiveMinutesAgo = new Date(Date.now() - 5 * 60 * 1000).toISOString();
    const { container } = renderRow({
      row: makeProcessRow({
        status: "running",
        stale_reasons: ["mid_flight_stuck"],
        active_run: {
          run_id: 99,
          started_at: fiveMinutesAgo,
          rows_processed_so_far: 42,
          progress_units_done: null,
          progress_units_total: null,
          last_progress_at: fiveMinutesAgo,
          is_cancelling: false,
        },
      }),
    });
    const chip = container.querySelector(
      "[data-stale-reason='mid_flight_stuck']",
    ) as HTMLElement;
    expect(chip).toBeTruthy();
    expect(chip.textContent).toMatch(/no progress\s+\d+m/);
  });

  it("pulsing amber border applies when stale_reasons non-empty", () => {
    const { container } = renderRow({
      row: makeProcessRow({
        status: "ok",
        stale_reasons: ["watermark_gap"],
      }),
    });
    const tr = container.querySelector("tr") as HTMLElement;
    expect(tr.className).toContain("animate-pulse");
    expect(tr.className).toContain("motion-reduce:animate-none");
    expect(tr.className).toContain("border-l-amber-500");
  });

  it("amber stale border outranks sky running border on overlap", () => {
    const { container } = renderRow({
      row: makeProcessRow({
        status: "running",
        stale_reasons: ["mid_flight_stuck"],
      }),
    });
    const tr = container.querySelector("tr") as HTMLElement;
    expect(tr.className).toContain("border-l-amber-500");
    expect(tr.className).not.toContain("border-l-sky-500");
  });

  // ---------------------------------------------------------------------
  // PR9 (#1085) — a11y: lane chip + status pill + stale chips carry
  // an `aria-label` so screen readers announce a labelled phrase
  // ("Lane: sec", "Status: running", "Stale reason: schedule missed")
  // rather than a bare token. Tests assert via `toHaveAccessibleName`
  // — the jest-dom matcher resolves the WAI-ARIA accessible-name
  // algorithm and short-circuits on `aria-label`, which is closer to
  // assistive-tech behaviour than reading the raw attribute.
  // ---------------------------------------------------------------------

  it("lane chip's accessible name includes the lane key with screen-reader prefix", () => {
    const { container } = renderRow({ row: makeProcessRow({ lane: "sec" }) });
    const chip = container.querySelector(
      "[data-testid='lane-chip']",
    ) as HTMLElement;
    expect(chip).toHaveAccessibleName("Lane: sec");
  });

  it("status pill's accessible name includes the human label with screen-reader prefix", () => {
    const { container } = renderRow({
      row: makeProcessRow({ status: "running" }),
    });
    const pill = container.querySelector(
      "[data-testid='status-pill']",
    ) as HTMLElement;
    expect(pill).toHaveAccessibleName("Status: running");
  });

  it("stale chip's accessible name carries the reason with screen-reader prefix", () => {
    const { container } = renderRow({
      row: makeProcessRow({
        status: "ok",
        stale_reasons: ["schedule_missed", "queue_stuck"],
      }),
    });
    const scheduleChip = container.querySelector(
      "[data-stale-reason='schedule_missed']",
    ) as HTMLElement;
    const queueChip = container.querySelector(
      "[data-stale-reason='queue_stuck']",
    ) as HTMLElement;
    expect(scheduleChip).toHaveAccessibleName("Stale reason: schedule missed");
    expect(queueChip).toHaveAccessibleName("Stale reason: queue stuck");
  });

  it("mid_flight_stuck stale chip's accessible name includes the elapsed suffix", () => {
    const fiveMinutesAgo = new Date(Date.now() - 5 * 60 * 1000).toISOString();
    const { container } = renderRow({
      row: makeProcessRow({
        status: "running",
        stale_reasons: ["mid_flight_stuck"],
        active_run: {
          run_id: 99,
          started_at: fiveMinutesAgo,
          rows_processed_so_far: 0,
          progress_units_done: null,
          progress_units_total: null,
          last_progress_at: fiveMinutesAgo,
          is_cancelling: false,
        },
      }),
    });
    const chip = container.querySelector(
      "[data-stale-reason='mid_flight_stuck']",
    ) as HTMLElement;
    // Visible text is `no progress 5m`; the accessible name layers the
    // `Stale reason:` prefix on top so a screen reader announces the
    // full meaning rather than the bare phrase.
    expect(chip).toHaveAccessibleName(/^Stale reason: no progress\s+\d+m$/);
  });

  // ---------------------------------------------------------------------
  // PR3a #1064 — bootstrap mechanism action verbs + no cadence
  // ---------------------------------------------------------------------

  it("bootstrap row labels Iterate as 'Re-run failed' and Full-wash as 'Re-run all'", () => {
    renderRow({
      row: makeProcessRow({
        process_id: "bootstrap",
        mechanism: "bootstrap",
        display_name: "First-install bootstrap",
        can_iterate: true,
        can_full_wash: true,
      }),
    });
    expect(
      screen.getByRole("button", { name: "Re-run failed" }),
    ).toBeTruthy();
    expect(
      screen.getByRole("button", { name: "Re-run all" }),
    ).toBeTruthy();
    expect(screen.queryByRole("button", { name: "Iterate" })).toBeNull();
    expect(screen.queryByRole("button", { name: "Full-wash" })).toBeNull();
  });

  it("scheduled_job row keeps Iterate / Full-wash labels", () => {
    renderRow({
      row: makeProcessRow({
        process_id: "daily_cik_refresh",
        mechanism: "scheduled_job",
      }),
    });
    expect(screen.getByRole("button", { name: "Iterate" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "Full-wash" })).toBeTruthy();
  });

  it("bootstrap row omits cadence (stages are a fixed sequence, not scheduled)", () => {
    const { container } = renderRow({
      row: makeProcessRow({
        process_id: "bootstrap",
        mechanism: "bootstrap",
        cadence_human: "every 5m",
        next_fire_at: "2026-05-10T14:00:00+00:00",
      }),
    });
    expect(container.textContent).not.toContain("every 5m");
    expect(container.textContent).not.toContain("next:");
  });

  it("scheduled_job row keeps cadence visible", () => {
    const { container } = renderRow({
      row: makeProcessRow({
        process_id: "daily_cik_refresh",
        mechanism: "scheduled_job",
        cadence_human: "every 5m",
      }),
    });
    expect(container.textContent).toContain("every 5m");
  });

  // ---------------------------------------------------------------------
  // PR4 #1082 — ⓘ tooltip rendering description
  // ---------------------------------------------------------------------

  it("renders ⓘ tooltip button when description is non-empty", () => {
    renderRow({
      row: makeProcessRow({
        description: "Refreshes SEC CIK mappings nightly.",
      }),
    });
    const tooltip = screen.getByTestId("process-description-tooltip");
    expect(tooltip).toBeTruthy();
    // Accessible name carries the description for screen readers
    // (replaces the prior native ``title`` after operator feedback
    // that the native delay + click-to-hide were poor UX).
    expect(tooltip).toHaveAccessibleName(
      "Refreshes SEC CIK mappings nightly.",
    );
    // Popover starts collapsed; aria-expanded reflects it.
    expect(tooltip.getAttribute("aria-expanded")).toBe("false");
  });

  it("clicking the ⓘ pins the tooltip open; second click collapses", () => {
    renderRow({
      row: makeProcessRow({ description: "pinned popover content." }),
    });
    const tooltip = screen.getByTestId("process-description-tooltip");
    expect(screen.queryByRole("tooltip")).toBeNull();

    fireEvent.click(tooltip);
    expect(tooltip.getAttribute("aria-expanded")).toBe("true");
    const pop = screen.getByRole("tooltip");
    expect(pop.textContent).toBe("pinned popover content.");

    fireEvent.click(tooltip);
    expect(tooltip.getAttribute("aria-expanded")).toBe("false");
    expect(screen.queryByRole("tooltip")).toBeNull();
  });

  it("hovering the ⓘ surfaces the popover immediately (no native title delay)", () => {
    renderRow({
      row: makeProcessRow({ description: "hover content." }),
    });
    const tooltip = screen.getByTestId("process-description-tooltip");
    expect(screen.queryByRole("tooltip")).toBeNull();

    fireEvent.pointerEnter(tooltip.parentElement!);
    expect(screen.getByRole("tooltip").textContent).toBe("hover content.");

    fireEvent.pointerLeave(tooltip.parentElement!);
    expect(screen.queryByRole("tooltip")).toBeNull();
  });

  it("hides ⓘ tooltip when description is empty", () => {
    renderRow({
      row: makeProcessRow({ description: "" }),
    });
    expect(
      screen.queryByTestId("process-description-tooltip"),
    ).toBeNull();
  });
});
