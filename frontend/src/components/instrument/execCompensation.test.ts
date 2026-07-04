import { describe, expect, it } from "vitest";

import type { ExecCompRow } from "@/api/instruments";
import { groupExecComp, parseComp } from "./execCompensation";

function row(over: Partial<ExecCompRow>): ExecCompRow {
  return {
    executive_name: "Jane Doe",
    principal_position: "Chief Executive Officer",
    fiscal_year: 2025,
    salary: "1000000.00",
    bonus: null,
    stock_awards: null,
    option_awards: null,
    non_equity_incentive: null,
    pension_nqdc: null,
    other_comp: null,
    total_comp: "1000000.00",
    ...over,
  };
}

describe("parseComp", () => {
  it("returns null for a null cell", () => {
    expect(parseComp(null)).toBeNull();
  });
  it("parses a Decimal-as-string cell", () => {
    expect(parseComp("74294811.00")).toBe(74294811);
  });
  it("returns null for a non-numeric string (never NaN)", () => {
    expect(parseComp("n/a")).toBeNull();
  });
});

describe("groupExecComp", () => {
  it("groups rows by executive with years ordered fiscal_year DESC", () => {
    const groups = groupExecComp([
      row({ executive_name: "A", fiscal_year: 2024, total_comp: "50" }),
      row({ executive_name: "A", fiscal_year: 2025, total_comp: "60" }),
      row({ executive_name: "A", fiscal_year: 2023, total_comp: "40" }),
    ]);
    expect(groups).toHaveLength(1);
    expect(groups[0]!.years.map((y) => y.fiscal_year)).toEqual([
      2025, 2024, 2023,
    ]);
  });

  it("takes the position from the executive's most-recent year", () => {
    const groups = groupExecComp([
      row({
        executive_name: "A",
        fiscal_year: 2024,
        principal_position: "SVP",
      }),
      row({
        executive_name: "A",
        fiscal_year: 2025,
        principal_position: "CEO",
      }),
    ]);
    expect(groups[0]!.principal_position).toBe("CEO");
  });

  it("orders executives by latest-FY total_comp DESC (highest-paid first)", () => {
    const groups = groupExecComp([
      row({ executive_name: "CFO", fiscal_year: 2025, total_comp: "20000000" }),
      row({ executive_name: "CEO", fiscal_year: 2025, total_comp: "74000000" }),
      row({ executive_name: "COO", fiscal_year: 2025, total_comp: "27000000" }),
    ]);
    expect(groups.map((g) => g.executive_name)).toEqual(["CEO", "COO", "CFO"]);
  });

  it("sorts an executive whose latest total is null to the end", () => {
    const groups = groupExecComp([
      row({ executive_name: "NoTotal", fiscal_year: 2025, total_comp: null }),
      row({ executive_name: "Paid", fiscal_year: 2025, total_comp: "5000000" }),
    ]);
    expect(groups.map((g) => g.executive_name)).toEqual(["Paid", "NoTotal"]);
  });

  it("breaks ties on first-appearance order (stable, deterministic)", () => {
    const groups = groupExecComp([
      row({ executive_name: "First", fiscal_year: 2025, total_comp: "1000" }),
      row({ executive_name: "Second", fiscal_year: 2025, total_comp: "1000" }),
    ]);
    expect(groups.map((g) => g.executive_name)).toEqual(["First", "Second"]);
  });

  it("returns [] for no rows", () => {
    expect(groupExecComp([])).toEqual([]);
  });
});
