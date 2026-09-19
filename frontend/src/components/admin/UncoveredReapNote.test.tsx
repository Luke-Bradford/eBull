import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import type { UncoveredReapResponse } from "@/api/types";

import { UncoveredReapNote, uncoveredReapSummary } from "./UncoveredReapNote";

const TWO: UncoveredReapResponse[] = [
  { job_name: "daily_candle_refresh", events: 11, runs: 14 },
  { job_name: "daily_portfolio_sync", events: 4, runs: 4 },
];

describe("UncoveredReapNote", () => {
  it("names every job and carries BOTH numbers", () => {
    // ⚠ events !== runs on the first entry deliberately: on the real corpus
    // they are usually equal, so a fixture copied from production could not
    // tell a transposed render from a correct one.
    render(<UncoveredReapNote entries={TWO} partial={false} />);

    const note = screen.getByTestId("uncovered-reap-note");
    expect(note.textContent).toContain("daily_candle_refresh");
    expect(note.textContent).toContain("11 reaps");
    expect(note.textContent).toContain("14 runs");
    expect(note.textContent).toContain("daily_portfolio_sync");
    expect(note.textContent).toContain("4 reaps");
  });

  it("says 'not in this table', never 'nowhere else'", () => {
    // These jobs ARE reachable through the sync layers and the orchestrator
    // DAG — those surfaces carry data freshness, which is a different axis
    // from reap recurrence. Overclaiming would be the defect.
    render(<UncoveredReapNote entries={TWO} partial={false} />);

    const text = screen.getByTestId("uncovered-reap-note").textContent ?? "";
    expect(text).toContain("Not in this table");
    expect(text).toContain("reachable elsewhere");
  });

  it("does not claim lost work", () => {
    // A reaped row may have committed its writes, and the reaper's auto-retry
    // may have re-run it. The reaper detects an orphaned row; it records no
    // cause.
    render(<UncoveredReapNote entries={TWO} partial={false} />);

    const text = screen.getByTestId("uncovered-reap-note").textContent ?? "";
    expect(text).toContain("written off by the orphan reaper");
    expect(text).not.toMatch(/lost work|data loss/i);
  });

  it("renders nothing when there is nothing to disclose", () => {
    render(<UncoveredReapNote entries={[]} partial={false} />);
    expect(screen.queryByTestId("uncovered-reap-note")).toBeNull();
  });

  it("renders nothing on a PARTIAL snapshot even with entries", () => {
    // The backend does not compute the residual on a partial snapshot, so an
    // empty list there means "not evaluated". Rendering an all-clear would be
    // a lie; the partial banner is what the operator reads.
    render(<UncoveredReapNote entries={TWO} partial={true} />);
    expect(screen.queryByTestId("uncovered-reap-note")).toBeNull();
  });

  it("tolerates a payload with the field absent", () => {
    render(<UncoveredReapNote entries={undefined} partial={false} />);
    expect(screen.queryByTestId("uncovered-reap-note")).toBeNull();
  });
});

describe("uncoveredReapSummary", () => {
  it("carries the count for the collapsed disclosure label", () => {
    // ⚠ CollapsibleSection UNMOUNTS its body when closed, so the note above is
    // only reachable while the section happens to be open. This is the same
    // reachability defect PR #3211 fixed for the collapsed `current` group.
    expect(uncoveredReapSummary(TWO, false)).toBe("2 jobs not listed");
  });

  it("is singular for one job", () => {
    expect(uncoveredReapSummary(TWO.slice(0, 1), false)).toBe("1 job not listed");
  });

  it("is null when empty or partial, so the label stays unchanged", () => {
    expect(uncoveredReapSummary([], false)).toBeNull();
    expect(uncoveredReapSummary(TWO, true)).toBeNull();
    expect(uncoveredReapSummary(undefined, false)).toBeNull();
  });
});
