import { fireEvent, render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { describe, expect, it, vi } from "vitest";

import { ApiError } from "@/api/client";
import { makeProcessRow, makeError } from "@/components/admin/__fixtures__/processes";
import { ProcessRow, processRowSignature } from "@/components/admin/ProcessRow";
import type { ProcessRowResponse } from "@/api/types";

function renderRow(props: Partial<Parameters<typeof ProcessRow>[0]> = {}) {
  const row = props.row ?? makeProcessRow();
  return render(
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
            {...props}
          />
        </tbody>
      </table>
    </MemoryRouter>,
  );
}

// Stable handler refs shared across rerenders — the memo comparator
// reference-compares the callbacks, so fresh `vi.fn()`s per render would
// (correctly) force a repaint and mask the signature-gating behaviour
// under test.
const STABLE_HANDLERS = {
  onIterate: vi.fn(),
  onFullWash: vi.fn(),
  onCancel: vi.fn(),
};

function rowMarkup(row: ProcessRowResponse, signature: string) {
  return (
    <MemoryRouter>
      <table>
        <tbody>
          <ProcessRow
            row={row}
            signature={signature}
            triggerError={undefined}
            cancelError={undefined}
            busy={false}
            onIterate={STABLE_HANDLERS.onIterate}
            onFullWash={STABLE_HANDLERS.onFullWash}
            onCancel={STABLE_HANDLERS.onCancel}
          />
        </tbody>
      </table>
    </MemoryRouter>
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

  it("renders auto-hide tooltip on the self-healing (pending_retry) pill", () => {
    renderRow({ row: makeProcessRow({ status: "pending_retry", stale_reasons: [] }) });
    const pill = screen.getByText(/self-healing/i);
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
  // #1512 — single computed verdict pill + one inline reason line.
  // The two-axis stale chips are gone; an overdue row is one red
  // "needs attention" pill with an inline reason.
  // ---------------------------------------------------------------------

  it("renders no verdict-reason line for a clean (current) row", () => {
    const { container } = renderRow({
      row: makeProcessRow({ status: "ok", stale_reasons: [] }),
    });
    expect(container.querySelector("[data-testid='verdict-reason']")).toBeNull();
    expect(container.querySelector("[data-testid='stale-chips']")).toBeNull();
  });

  it("renders ONE verdict pill (not two contradictory axes) for ok+overdue", () => {
    const { container } = renderRow({
      row: makeProcessRow({
        status: "ok",
        stale_reasons: ["schedule_missed", "watermark_gap"],
      }),
    });
    const pill = container.querySelector(
      "[data-testid='status-pill']",
    ) as HTMLElement;
    expect(pill.getAttribute("data-verdict")).toBe("attention");
    expect(pill.textContent?.toLowerCase()).toContain("needs attention");
    // No second axis — the old stale-chips cluster is gone.
    expect(container.querySelector("[data-testid='stale-chips']")).toBeNull();
    const reason = container.querySelector(
      "[data-testid='verdict-reason']",
    ) as HTMLElement;
    // First reason in fixed order wins the headline.
    expect(reason.textContent).toContain("schedule missed");
  });

  it("mid_flight_stuck verdict-reason suffixes elapsed-since-heartbeat", () => {
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
    const reason = container.querySelector(
      "[data-testid='verdict-reason']",
    ) as HTMLElement;
    expect(reason).toBeTruthy();
    expect(reason.textContent).toMatch(/running but no progress\s+\d+m/);
  });

  it("attention verdict paints a red left border", () => {
    const { container } = renderRow({
      row: makeProcessRow({ status: "ok", stale_reasons: ["watermark_gap"] }),
    });
    const tr = container.querySelector("tr") as HTMLElement;
    expect(tr.className).toContain("border-l-red-500");
  });

  it("self_healing verdict pulses amber", () => {
    const { container } = renderRow({
      row: makeProcessRow({ status: "pending_retry", stale_reasons: [] }),
    });
    const tr = container.querySelector("tr") as HTMLElement;
    expect(tr.className).toContain("border-l-amber-500");
    expect(tr.className).toContain("animate-pulse");
  });

  it("running (not stuck) pulses sky, not amber", () => {
    const { container } = renderRow({
      row: makeProcessRow({ status: "running", stale_reasons: [] }),
    });
    const tr = container.querySelector("tr") as HTMLElement;
    expect(tr.className).toContain("border-l-sky-500");
    expect(tr.className).not.toContain("border-l-amber-500");
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

  it("verdict pill's accessible name includes the verdict label with screen-reader prefix", () => {
    const { container } = renderRow({
      row: makeProcessRow({ status: "running", stale_reasons: [] }),
    });
    const pill = container.querySelector(
      "[data-testid='status-pill']",
    ) as HTMLElement;
    expect(pill).toHaveAccessibleName("Health: working");
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

  it("first-install bootstrap shows only 'Run bootstrap' (no Re-run/Cancel)", () => {
    renderRow({
      row: makeProcessRow({
        process_id: "bootstrap",
        mechanism: "bootstrap",
        display_name: "First-install bootstrap",
        status: "pending_first_run",
        can_iterate: false,
        can_full_wash: true,
        can_cancel: false,
      }),
    });
    const runBtn = screen.getByRole("button", {
      name: "Run bootstrap",
    }) as HTMLButtonElement;
    expect(runBtn).toBeTruthy();
    expect(runBtn.disabled).toBe(false);
    // Inapplicable affordances are hidden, not greyed, on a never-run row.
    expect(screen.queryByRole("button", { name: "Re-run failed" })).toBeNull();
    expect(screen.queryByRole("button", { name: "Re-run all" })).toBeNull();
    expect(screen.queryByRole("button", { name: "Cancel" })).toBeNull();
  });

  it("clean-complete bootstrap de-emphasises 'Re-run all' to a neutral tone (#1432)", () => {
    renderRow({
      row: makeProcessRow({
        process_id: "bootstrap",
        mechanism: "bootstrap",
        status: "ok",
        can_iterate: false,
        can_full_wash: true,
      }),
    });
    const btn = screen.getByRole("button", {
      name: "Re-run all",
    }) as HTMLButtonElement;
    // Still available — full re-bootstrap is a legal action.
    expect(btn.disabled).toBe(false);
    // ...but no red destructive styling, since nothing failed.
    expect(btn.className).not.toContain("text-red-700");
    expect(btn.className).toContain("text-slate-700");
    expect(btn.title).toContain("completed cleanly");
  });

  it("failed bootstrap keeps the red destructive 'Re-run all' tone", () => {
    renderRow({
      row: makeProcessRow({
        process_id: "bootstrap",
        mechanism: "bootstrap",
        status: "failed",
        can_iterate: true,
        can_full_wash: true,
      }),
    });
    const btn = screen.getByRole("button", {
      name: "Re-run all",
    }) as HTMLButtonElement;
    expect(btn.className).toContain("text-red-700");
    expect(btn.className).not.toContain("text-slate-700");
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

  it("popover's role='tooltip' is linked to the trigger via aria-describedby", () => {
    // Round 3 review WARNING: ARIA spec requires explicit linkage
    // between the trigger and the popover for AT to announce the
    // expanded content. Pin: when the tooltip is visible, the
    // trigger's aria-describedby points at the tooltip's id.
    renderRow({
      row: makeProcessRow({ description: "linkage content." }),
    });
    const tooltip = screen.getByTestId("process-description-tooltip");
    expect(tooltip.getAttribute("aria-describedby")).toBeNull();

    fireEvent.click(tooltip);
    const describedBy = tooltip.getAttribute("aria-describedby");
    expect(describedBy).toBeTruthy();
    const popover = screen.getByRole("tooltip");
    expect(popover.id).toBe(describedBy);
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

// ---------------------------------------------------------------------
// #1480 — content signature + React.memo anti-flicker contract.
// ---------------------------------------------------------------------

describe("processRowSignature", () => {
  it("is identical for a deep-cloned row — the unchanged-poll case", () => {
    const a = makeProcessRow({ status: "ok" });
    // A real poll returns a fresh JSON parse: new object identity at
    // every level (nested last_run / watermark too), same content. The
    // signature must collapse that back to equality so memo skips.
    const b: ProcessRowResponse = JSON.parse(JSON.stringify(a));
    expect(a).not.toBe(b);
    expect(a.last_run).not.toBe(b.last_run);
    expect(processRowSignature(a)).toBe(processRowSignature(b));
  });

  it("changes when a rendered field changes", () => {
    const base = makeProcessRow({ status: "ok" });
    const changed: ProcessRowResponse = { ...base, status: "failed" };
    expect(processRowSignature(base)).not.toBe(processRowSignature(changed));
  });

  // The stuck-row elapsed suffix is the one part of the signature NOT
  // already implied by JSON.stringify(row): last_progress_at is frozen
  // while a process is wedged, so without a wall-clock term the chip
  // ("no progress Nm") would freeze too and the operator loses the
  // wedge signal (#1474 / #1478). These two tests prove the suffix
  // earns its place — comparing stuck-vs-non-stuck would NOT, since
  // stale_reasons already lands in the JSON.
  it("advances a stuck row's signature as wall-clock elapses, with frozen data", () => {
    const frozen = "2026-05-08T13:00:00+00:00";
    const stuck = makeProcessRow({
      status: "running",
      stale_reasons: ["mid_flight_stuck"],
      active_run: {
        run_id: 99,
        started_at: frozen,
        rows_processed_so_far: 0,
        progress_units_done: null,
        progress_units_total: null,
        last_progress_at: frozen, // never moves — the process is wedged
        is_cancelling: false,
      },
    });
    const base = Date.parse(frozen);
    const nowSpy = vi.spyOn(Date, "now");

    nowSpy.mockReturnValue(base + 60_000); // 1m wedged
    const at1m = processRowSignature(stuck);
    nowSpy.mockReturnValue(base + 10 * 60_000); // 10m wedged, same row data
    const at10m = processRowSignature(stuck);

    expect(at1m).not.toBe(at10m);
    nowSpy.mockRestore();
  });

  it("does not add a time term for quiescent rows — stable across wall-clock", () => {
    const ok = makeProcessRow({ status: "ok", stale_reasons: [] });
    const nowSpy = vi.spyOn(Date, "now");

    nowSpy.mockReturnValue(1_000_000);
    const early = processRowSignature(ok);
    nowSpy.mockReturnValue(500_000_000);
    const late = processRowSignature(ok);

    // A healthy row must never repaint merely because time passed.
    expect(early).toBe(late);
    nowSpy.mockRestore();
  });
});

describe("ProcessRow memoisation (#1480)", () => {
  it("skips repaint when the signature is unchanged despite a new row object", () => {
    const a = makeProcessRow({ display_name: "Alpha" });
    const { rerender } = render(rowMarkup(a, "SIG-1"));
    expect(screen.getByRole("link", { name: "Alpha" })).toBeTruthy();

    // New row object, DIFFERENT content, but SAME signature → memo must
    // skip the repaint and keep showing the prior content. This is the
    // unchanged-poll case: snapshot identity churns, content does not.
    const b = makeProcessRow({ display_name: "Beta" });
    rerender(rowMarkup(b, "SIG-1"));
    expect(screen.queryByRole("link", { name: "Beta" })).toBeNull();
    expect(screen.getByRole("link", { name: "Alpha" })).toBeTruthy();

    // Signature changes → memo lets the repaint through.
    rerender(rowMarkup(b, "SIG-2"));
    expect(screen.getByRole("link", { name: "Beta" })).toBeTruthy();
  });
});
