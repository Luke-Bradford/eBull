import { describe, expect, it } from "vitest";

import {
  makeProcessList,
  makeProcessRow,
} from "@/components/admin/__fixtures__/processes";
import {
  isSteadyStateProcess,
  steadyStateAttentionRows,
} from "@/lib/processHealth";

describe("isSteadyStateProcess", () => {
  it("accepts a steady-state scheduled job", () => {
    expect(
      isSteadyStateProcess(makeProcessRow({ role: "steady_state", mechanism: "scheduled_job" })),
    ).toBe(true);
  });

  it("accepts a steady-state ingest_sweep (the #1959 case)", () => {
    expect(
      isSteadyStateProcess(
        makeProcessRow({ process_id: "nport_sweep", role: "steady_state", mechanism: "ingest_sweep" }),
      ),
    ).toBe(true);
  });

  it("rejects backfill / bootstrap roles", () => {
    expect(isSteadyStateProcess(makeProcessRow({ role: "backfill" }))).toBe(false);
    expect(isSteadyStateProcess(makeProcessRow({ role: "bootstrap" }))).toBe(false);
  });

  it("rejects the one-time bootstrap-mechanism row", () => {
    expect(
      isSteadyStateProcess(makeProcessRow({ role: "steady_state", mechanism: "bootstrap" })),
    ).toBe(false);
  });
});

describe("steadyStateAttentionRows", () => {
  it("returns only steady-state rows whose verdict is attention", () => {
    const rows = makeProcessList([
      makeProcessRow({ process_id: "fundamentals_sync", status: "failed" }), // attention
      makeProcessRow({ process_id: "nport_sweep", mechanism: "ingest_sweep", status: "failed" }), // attention
      makeProcessRow({ process_id: "healthy", status: "ok" }), // current
      makeProcessRow({ process_id: "retrying", status: "pending_retry" }), // self_healing
      makeProcessRow({ process_id: "paused", status: "disabled" }), // paused (kill switch)
      makeProcessRow({ process_id: "backfill_fail", role: "backfill", status: "failed" }), // excluded by scope
    ]).rows;

    const attention = steadyStateAttentionRows(rows).map((r) => r.process_id);
    expect(attention).toEqual(["fundamentals_sync", "nport_sweep"]);
  });

  it("returns an empty array when nothing needs attention", () => {
    const rows = makeProcessList([makeProcessRow({ status: "ok" })]).rows;
    expect(steadyStateAttentionRows(rows)).toEqual([]);
  });
});
