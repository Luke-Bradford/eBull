import { fireEvent, render, screen, waitFor } from "@testing-library/react";
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
  return render(
    <MemoryRouter>
      <ProcessesTable
        snapshot={makeProcessList(rows, partial)}
        onMutationSuccess={onMutationSuccess}
      />
    </MemoryRouter>,
  );
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
      makeProcessRow({ process_id: "y", lane: "ownership", display_name: "Y_own" }),
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

  it("full-wash requires typed-name match before confirm enables", async () => {
    renderTable([
      makeProcessRow({ display_name: "Insider Form 4 ingest", can_full_wash: true }),
    ]);
    fireEvent.click(screen.getByRole("button", { name: "Full-wash" }));
    const dialog = await screen.findByRole("dialog");
    const input = dialog.querySelector(
      "input[aria-label='Process name confirmation']",
    ) as HTMLInputElement;
    const confirmBtn = dialog.querySelector(
      "button[disabled]",
    ) as HTMLButtonElement;
    expect(confirmBtn.disabled).toBe(true);
    fireEvent.change(input, { target: { value: "wrong" } });
    expect(
      (
        Array.from(dialog.querySelectorAll("button")).find(
          (b) => b.textContent === "Full-wash",
        ) as HTMLButtonElement
      ).disabled,
    ).toBe(true);
    fireEvent.change(input, { target: { value: "Insider Form 4 ingest" } });
    const confirmEnabled = Array.from(dialog.querySelectorAll("button")).find(
      (b) => b.textContent === "Full-wash",
    ) as HTMLButtonElement;
    expect(confirmEnabled.disabled).toBe(false);
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
  // PR8 (#1083) — stale banner integration. Banner unit-behaviour lives
  // in StaleBanner.test.tsx; here we just confirm the table mounts it
  // when at least one row is stale and hides it otherwise.
  // ---------------------------------------------------------------------

  it("does NOT render stale banner when all rows have empty stale_reasons", () => {
    renderTable([makeProcessRow({ stale_reasons: [] })]);
    expect(screen.queryByTestId("stale-banner")).toBeNull();
  });

  it("renders stale banner when at least one row has stale_reasons", () => {
    renderTable([
      makeProcessRow({ process_id: "a", stale_reasons: [] }),
      makeProcessRow({
        process_id: "b",
        stale_reasons: ["watermark_gap"],
      }),
    ]);
    expect(screen.getByTestId("stale-banner")).toBeTruthy();
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

  it("failed rows still outrank stale rows", () => {
    const { container } = renderTable([
      makeProcessRow({
        process_id: "stale",
        status: "ok",
        display_name: "A_stale",
        stale_reasons: ["queue_stuck"],
      }),
      makeProcessRow({
        process_id: "fail",
        status: "failed",
        display_name: "B_failed",
      }),
    ]);
    const links = Array.from(container.querySelectorAll("tbody a")).map(
      (a) => a.textContent ?? "",
    );
    expect(links[0]).toBe("B_failed");
    expect(links[1]).toBe("A_stale");
  });
});
