import { render, screen } from "@testing-library/react";
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
});
