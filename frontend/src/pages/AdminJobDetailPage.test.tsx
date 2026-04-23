/**
 * Tests for AdminJobDetailPage (#415 drill-through).
 *
 * Scope:
 *   - Loading state renders skeleton
 *   - Error state renders retry button
 *   - Empty state renders neutral "No recent runs" copy + Back link
 *   - Populated list renders rows newest-first
 *   - Only failure rows with non-null error_msg are expandable
 *   - Successful rows are not interactive
 *   - Component passes the router-decoded :name straight to fetchJobRuns
 *     (no double decode; no re-encode) — the helper re-encodes internally.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes } from "react-router-dom";

import { AdminJobDetailPage } from "@/pages/AdminJobDetailPage";
import { fetchJobRuns } from "@/api/jobs";
import type { JobRunResponse, JobRunsListResponse } from "@/api/types";

vi.mock("@/api/jobs", () => ({ fetchJobRuns: vi.fn() }));

const mockedFetch = vi.mocked(fetchJobRuns);

function makeRun(overrides: Partial<JobRunResponse> = {}): JobRunResponse {
  return {
    run_id: 1,
    job_name: "fundamentals_sync",
    started_at: "2026-04-22T10:00:00Z",
    finished_at: "2026-04-22T10:01:30Z",
    status: "success",
    row_count: 100,
    error_msg: null,
    ...overrides,
  };
}

function makeList(items: JobRunResponse[]): JobRunsListResponse {
  return {
    items,
    count: items.length,
    limit: 50,
    job_name: "fundamentals_sync",
  };
}

function renderPage(routeParam: string = "fundamentals_sync") {
  return render(
    <MemoryRouter initialEntries={[`/admin/jobs/${routeParam}`]}>
      <Routes>
        <Route path="/admin/jobs/:name" element={<AdminJobDetailPage />} />
      </Routes>
    </MemoryRouter>,
  );
}

beforeEach(() => {
  mockedFetch.mockReset();
});

afterEach(() => {
  vi.restoreAllMocks();
});

describe("AdminJobDetailPage", () => {
  it("renders a loading skeleton while the first fetch is in-flight", () => {
    mockedFetch.mockReturnValue(new Promise(() => {})); // never resolves
    renderPage();
    expect(screen.getByRole("status")).toBeInTheDocument();
  });

  it("renders an error + retry when the fetch fails", async () => {
    mockedFetch.mockRejectedValueOnce(new Error("boom"));
    renderPage();
    expect(await screen.findByRole("alert")).toHaveTextContent(/Failed to load/i);
    expect(
      screen.getByRole("button", { name: /Retry/i }),
    ).toBeInTheDocument();
  });

  it("renders a neutral 'No recent runs' message with the header Back link when list is empty", async () => {
    mockedFetch.mockResolvedValueOnce(makeList([]));
    renderPage();
    expect(
      await screen.findByText(/No recent runs for this job/i),
    ).toBeInTheDocument();
    // Header back link (every state carries it).
    expect(
      screen.getByRole("link", { name: /Back to Admin/i }),
    ).toHaveAttribute("href", "/admin");
  });

  it("renders runs in the exact order delivered by the backend (newest-first)", async () => {
    // Backend returns ORDER BY started_at DESC. Pin the order on the
    // Started timestamps each row rendered so a regression that
    // reverses / re-sorts the list is caught (the raw length check
    // would pass either way).
    mockedFetch.mockResolvedValueOnce(
      makeList([
        makeRun({ run_id: 3, started_at: "2026-04-22T12:00:00Z", finished_at: null, status: "running", row_count: null }),
        makeRun({ run_id: 2, started_at: "2026-04-22T11:00:00Z", status: "success" }),
        makeRun({ run_id: 1, started_at: "2026-04-22T10:00:00Z", status: "success" }),
      ]),
    );
    renderPage();
    const rows = await screen.findAllByRole("row");
    // header + 3 data rows.
    expect(rows).toHaveLength(4);
    // Row 0 is the <th> header. Data rows expose the raw ISO
    // timestamp via data-started-at so the assertion does not depend
    // on the host timezone (formatDateTime localizes; DST would make
    // a locale-text assertion flaky in CI).
    const dataRows = rows.slice(1);
    const startedAt = dataRows.map((r) =>
      r.getAttribute("data-started-at"),
    );
    expect(startedAt).toEqual([
      "2026-04-22T12:00:00Z",
      "2026-04-22T11:00:00Z",
      "2026-04-22T10:00:00Z",
    ]);
  });

  it("expands a failed run to show its error_msg", async () => {
    mockedFetch.mockResolvedValueOnce(
      makeList([
        makeRun({
          run_id: 42,
          status: "failure",
          finished_at: "2026-04-22T10:05:00Z",
          error_msg: "psycopg.errors.UndefinedColumn: column \"red_flag_score\" does not exist",
          row_count: null,
        }),
      ]),
    );
    renderPage();
    // Failure row is clickable.
    const expand = await screen.findByRole("button", {
      name: /Show error for run 42/i,
    });
    await userEvent.click(expand);
    expect(
      screen.getByText(/column "red_flag_score" does not exist/i),
    ).toBeInTheDocument();
  });

  it("does not render an expand affordance on successful runs", async () => {
    mockedFetch.mockResolvedValueOnce(
      makeList([
        makeRun({ run_id: 7, status: "success", error_msg: null }),
      ]),
    );
    renderPage();
    // Wait for the status cell to confirm the row has rendered, then
    // assert no expand button exists for this run.
    await screen.findByText("success");
    expect(
      screen.queryByRole("button", { name: /Show error for run 7/i }),
    ).toBeNull();
  });

  it("does not render an expand affordance on a failure with a null error_msg", async () => {
    // Spec-critical edge: only (failure AND error_msg !== null) is
    // expandable. A failure row with no captured error would otherwise
    // expand to an empty <pre>, which the spec explicitly rejects.
    mockedFetch.mockResolvedValueOnce(
      makeList([
        makeRun({
          run_id: 8,
          status: "failure",
          finished_at: "2026-04-22T10:05:00Z",
          error_msg: null,
          row_count: null,
        }),
      ]),
    );
    renderPage();
    await screen.findByText("failure");
    expect(
      screen.queryByRole("button", { name: /Show error for run 8/i }),
    ).toBeNull();
  });

  it("passes the router-decoded :name through to fetchJobRuns without re-encoding or double-decoding", async () => {
    // React Router 6 already URL-decodes path params. If the page also
    // decoded, "etl%2Ffundamentals_sync" → "etl/fundamentals_sync"
    // the first time (by router), and a second decode would leave it
    // as-is (harmless here) but a name containing "%" itself would
    // corrupt (e.g. "%25" → "%"). The defence is: do nothing.
    mockedFetch.mockResolvedValueOnce(makeList([]));
    renderPage(encodeURIComponent("etl/fundamentals_sync"));
    // The assertion the implementation must satisfy: the decoded name
    // reaches fetchJobRuns verbatim. fetchJobRuns re-encodes internally
    // when building the URLSearchParams.
    await screen.findByText(/No recent runs/i);
    expect(mockedFetch).toHaveBeenCalledWith("etl/fundamentals_sync", 50);
  });
});
