/**
 * Admin-surface a11y suite (#1086).
 *
 * Replacement for the originally-scoped manual Lighthouse + axe runs
 * called for at line 966 of the admin control hub spec. Manual external
 * tools required a logged-in browser session and screenshots — neither
 * compatible with this repo's "must work naturally with no third-party
 * service overhead" posture, and neither catches future regressions.
 *
 * This suite runs axe-core (the same engine Lighthouse + the @axe-core
 * CLI wrap) against the rendered DOM tree of each admin surface. Wired
 * into the existing vitest pipeline + pre-push hook → every change
 * gets a fresh a11y check.
 *
 * Coverage:
 *   - ProcessesTable empty / populated / partial / stale + banner
 *   - ProcessRow stale chips + 409 trigger error
 *   - StaleBanner (single + multi-cause)
 *
 * Out of scope (jsdom limitation): rules that depend on layout or
 * computed CSS (color-contrast, focus-order-when-layout-dependent).
 * Contrast is covered separately by `frontend/scripts/check-dark-classes.mjs`;
 * focus order is pinned by the keyboard-nav assertion in
 * `ProcessesTable.test.tsx`.
 */

import { fireEvent, render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { describe, expect, it, vi } from "vitest";
import { axe } from "vitest-axe";

import { ApiError } from "@/api/client";
import {
  makeError,
  makeProcessList,
  makeProcessRow,
} from "@/components/admin/__fixtures__/processes";
import { ProcessRow } from "@/components/admin/ProcessRow";
import { ProcessesTable } from "@/components/admin/ProcessesTable";
import { StaleBanner } from "@/components/admin/StaleBanner";

// jsdom does not implement HTMLCanvasElement.getContext, which axe-core
// uses for icon-ligature detection inside the color-contrast rule.
// CSS contrast cannot be computed in jsdom anyway (no layout / no
// computed styles), so we disable the rule globally for this suite —
// `frontend/scripts/check-dark-classes.mjs` is the canonical contrast
// gate. Without this, every test logs a noisy "Not implemented" stderr
// without changing the assertion outcome.
const AXE_OPTIONS = {
  rules: {
    "color-contrast": { enabled: false },
  },
} as const;

function renderTable(rows = [makeProcessRow()], partial = false) {
  return render(
    <MemoryRouter>
      <ProcessesTable
        snapshot={makeProcessList(rows, partial)}
        onMutationSuccess={vi.fn()}
      />
    </MemoryRouter>,
  );
}

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

describe("admin a11y — ProcessesTable", () => {
  it("empty snapshot has no axe violations", async () => {
    const { container } = renderTable([]);
    expect(await axe(container, AXE_OPTIONS)).toHaveNoViolations();
  });

  it("populated snapshot has no axe violations", async () => {
    const { container } = renderTable([
      makeProcessRow({ process_id: "a", status: "ok", display_name: "A" }),
      makeProcessRow({
        process_id: "b",
        status: "running",
        display_name: "B",
      }),
      makeProcessRow({
        process_id: "c",
        status: "failed",
        display_name: "C",
        last_n_errors: [makeError({ error_class: "ConnectionTimeout" })],
      }),
    ]);
    expect(await axe(container, AXE_OPTIONS)).toHaveNoViolations();
  });

  it("partial-banner mode has no axe violations", async () => {
    const { container } = renderTable([makeProcessRow()], true);
    expect(await axe(container, AXE_OPTIONS)).toHaveNoViolations();
  });

  it("with at least one stale row + banner has no axe violations", async () => {
    const { container } = renderTable([
      makeProcessRow({ process_id: "fresh" }),
      makeProcessRow({
        process_id: "stale_one",
        status: "ok",
        stale_reasons: ["watermark_gap"],
      }),
    ]);
    expect(await axe(container, AXE_OPTIONS)).toHaveNoViolations();
  });
});

describe("admin a11y — ProcessRow", () => {
  it("idle row has no axe violations", async () => {
    const { container } = renderRow({ row: makeProcessRow({ status: "ok" }) });
    expect(await axe(container, AXE_OPTIONS)).toHaveNoViolations();
  });

  it("running row with active_run has no axe violations", async () => {
    const { container } = renderRow({
      row: makeProcessRow({
        status: "running",
        active_run: {
          run_id: 1,
          started_at: "2026-05-09T13:00:00+00:00",
          rows_processed_so_far: 100,
          progress_units_done: null,
          progress_units_total: null,
          last_progress_at: "2026-05-09T13:00:00+00:00",
          is_cancelling: false,
        },
      }),
    });
    expect(await axe(container, AXE_OPTIONS)).toHaveNoViolations();
  });

  it("failed row with inline error preview has no axe violations", async () => {
    const { container } = renderRow({
      row: makeProcessRow({
        status: "failed",
        last_n_errors: [
          makeError({ error_class: "ConnectionTimeout", count: 3 }),
          makeError({ error_class: "MissingCIK", count: 1 }),
        ],
      }),
    });
    expect(await axe(container, AXE_OPTIONS)).toHaveNoViolations();
  });

  it("stale row with multiple chips has no axe violations", async () => {
    const { container } = renderRow({
      row: makeProcessRow({
        status: "ok",
        stale_reasons: ["schedule_missed", "queue_stuck", "watermark_gap"],
      }),
    });
    expect(await axe(container, AXE_OPTIONS)).toHaveNoViolations();
  });

  it("row with structured 409 trigger error has no axe violations", async () => {
    const { container } = renderRow({
      triggerError: new ApiError(409, "iterate already in flight", {
        reason: "iterate_already_pending",
      }),
    });
    expect(await axe(container, AXE_OPTIONS)).toHaveNoViolations();
  });

  it("row with structured 409 cancel error has no axe violations", async () => {
    const { container } = renderRow({
      cancelError: new ApiError(409, "no active run", {
        reason: "no_active_run",
      }),
    });
    expect(await axe(container, AXE_OPTIONS)).toHaveNoViolations();
  });
});

describe("admin a11y — confirm modals", () => {
  // Codex pre-push P2: dialogs are higher-risk a11y surfaces than the
  // table rows (focus-trap, aria-labelledby on heading, disabled-state
  // confirm button, controlled disclosure for terminate). Open each
  // modal explicitly and re-run axe so the suite guards them too.

  it("Full-wash confirm dialog has no axe violations", async () => {
    const { container } = renderTable([
      makeProcessRow({
        display_name: "Insider Form 4 ingest",
        can_full_wash: true,
      }),
    ]);
    fireEvent.click(screen.getByRole("button", { name: "Full-wash" }));
    await screen.findByRole("dialog");
    expect(await axe(container, AXE_OPTIONS)).toHaveNoViolations();
  });

  it("Cancel confirm dialog (collapsed disclosure) has no axe violations", async () => {
    const { container } = renderTable([
      makeProcessRow({ can_cancel: true }),
    ]);
    fireEvent.click(screen.getByRole("button", { name: "Cancel" }));
    await screen.findByRole("dialog");
    expect(await axe(container, AXE_OPTIONS)).toHaveNoViolations();
  });

  it("Cancel confirm dialog (terminate disclosure expanded) has no axe violations", async () => {
    const { container } = renderTable([
      makeProcessRow({ can_cancel: true }),
    ]);
    fireEvent.click(screen.getByRole("button", { name: "Cancel" }));
    await screen.findByRole("dialog");
    // Bot WARNING: previously used `Array.from(...).find(...) as HTMLButtonElement`
    // which crashes with TypeError if the disclosure copy ever changes,
    // instead of a clean assertion failure. `getByRole` throws a
    // descriptive error pointing at the missing accessible name.
    const moreToggle = screen.getByRole("button", {
      name: /More — terminate/,
    });
    fireEvent.click(moreToggle);
    expect(await axe(container, AXE_OPTIONS)).toHaveNoViolations();
  });
});

describe("admin a11y — StaleBanner", () => {
  it("single-cause banner has no axe violations", async () => {
    const { container } = render(
      <MemoryRouter>
        <StaleBanner
          rows={[
            makeProcessRow({
              process_id: "a",
              stale_reasons: ["queue_stuck"],
            }),
            makeProcessRow({
              process_id: "b",
              stale_reasons: ["queue_stuck"],
            }),
          ]}
        />
      </MemoryRouter>,
    );
    expect(await axe(container, AXE_OPTIONS)).toHaveNoViolations();
  });

  it("multi-cause banner has no axe violations", async () => {
    const { container } = render(
      <MemoryRouter>
        <StaleBanner
          rows={[
            makeProcessRow({
              process_id: "a",
              stale_reasons: ["watermark_gap"],
            }),
            makeProcessRow({
              process_id: "b",
              stale_reasons: ["queue_stuck"],
            }),
            makeProcessRow({
              process_id: "c",
              stale_reasons: ["mid_flight_stuck", "schedule_missed"],
            }),
          ]}
        />
      </MemoryRouter>,
    );
    expect(await axe(container, AXE_OPTIONS)).toHaveNoViolations();
  });
});
