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
    render(<UncoveredReapNote entries={TWO} />);

    const note = screen.getByTestId("uncovered-reap-note");
    expect(note.textContent).toContain("daily_candle_refresh");
    expect(note.textContent).toContain("11 reaps");
    expect(note.textContent).toContain("14 runs");
    expect(note.textContent).toContain("daily_portfolio_sync");
    expect(note.textContent).toContain("4 reaps");
  });

  it("claims only 'not in this table', and names no other surface", () => {
    // ⚠ BOTH directions are overclaims and the first two drafts shipped one
    // each. "Nowhere else" is false for the sync-orchestrator layer jobs,
    // which do have a layers/DAG surface. "Some of these have other surfaces"
    // is false whenever the residual holds only an outside-DAG job like
    // `strategy_backtest_run` — the backend filter proves only that a job has
    // no process row.
    render(<UncoveredReapNote entries={TWO} />);

    const text = screen.getByTestId("uncovered-reap-note").textContent ?? "";
    expect(text).toContain("Not in this table");
    expect(text).not.toMatch(/sync layer|orchestrator|DAG|other surface|nowhere else/i);
  });

  it("does not claim lost work", () => {
    // A reaped row may have committed its writes, and the reaper's auto-retry
    // may have re-run it. The reaper detects an orphaned row; it records no
    // cause.
    render(<UncoveredReapNote entries={TWO} />);

    const text = screen.getByTestId("uncovered-reap-note").textContent ?? "";
    expect(text).toContain("written off by the orphan reaper");
    expect(text).not.toMatch(/lost work|data loss/i);
  });

  it("caps what it renders and says so, per the array-size rule", () => {
    // `.claude/skills/frontend/api-shape-and-types.md` — #2178 froze the tab
    // with a `.map()` over an uncapped API array. The backend applies NO limit
    // here, so the component cap is the only bound. `job_name` is
    // unconstrained TEXT, so a malformed producer is how this gets long.
    const many: UncoveredReapResponse[] = Array.from({ length: 40 }, (_, i) => ({
      job_name: `job_${String(i).padStart(2, "0")}`,
      events: 100 - i,
      runs: 100 - i,
    }));

    render(<UncoveredReapNote entries={many} />);

    const text = screen.getByTestId("uncovered-reap-note").textContent ?? "";
    expect(text).toContain("25 of 40 shown");
    expect(text).toContain("job_00"); // loudest kept
    expect(text).not.toContain("job_39"); // quietest dropped
    // ⚠ The COUNT is the pre-cap total — capping the render must not silently
    // shrink what the operator is told exists.
    expect(text).toContain("40 jobs have");
    expect(uncoveredReapSummary(many)).toBe("40 jobs not listed");
  });

  it("does not say 'shown' when nothing was dropped", () => {
    render(<UncoveredReapNote entries={TWO} />);
    expect(screen.getByTestId("uncovered-reap-note").textContent).not.toContain("shown");
  });

  it("renders nothing when measured and empty", () => {
    render(<UncoveredReapNote entries={[]} />);
    expect(screen.queryByTestId("uncovered-reap-note")).toBeNull();
  });

  it("renders nothing when NOT EVALUATED, which is null and not []", () => {
    // The backend sends null when an adapter raised (a short covered set would
    // make well-covered jobs look uncovered) or when the residual read failed.
    // Rendering an all-clear for that would be a lie — and note the backend
    // deliberately does NOT set `partial` for it, because no lanes are omitted.
    render(<UncoveredReapNote entries={null} />);
    expect(screen.queryByTestId("uncovered-reap-note")).toBeNull();
  });

  it("tolerates a cached payload with the field absent", () => {
    render(<UncoveredReapNote entries={undefined} />);
    expect(screen.queryByTestId("uncovered-reap-note")).toBeNull();
  });
});

describe("uncoveredReapSummary", () => {
  it("carries the count for the collapsed disclosure label", () => {
    // ⚠ CollapsibleSection UNMOUNTS its body when closed, so the note is only
    // reachable while the section happens to be open. This is the same
    // reachability defect PR #3211 fixed for the collapsed `current` group.
    expect(uncoveredReapSummary(TWO)).toBe("2 jobs not listed");
  });

  it("is singular for one job", () => {
    expect(uncoveredReapSummary(TWO.slice(0, 1))).toBe("1 job not listed");
  });

  it("is null when empty or not evaluated, so the label stays unchanged", () => {
    expect(uncoveredReapSummary([])).toBeNull();
    expect(uncoveredReapSummary(null)).toBeNull();
    expect(uncoveredReapSummary(undefined)).toBeNull();
  });
});
