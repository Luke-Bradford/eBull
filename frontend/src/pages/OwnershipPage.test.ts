import { describe, expect, it } from "vitest";

import { buildCsv } from "./OwnershipPage";
import type { FilerRow } from "./OwnershipPage";

function row(overrides: Partial<FilerRow> = {}): FilerRow {
  return {
    key: "0000102909",
    label: "VANGUARD GROUP",
    category: "etfs",
    category_label: "ETFs",
    shares: 1_234_567,
    value_usd: 230_000_000,
    voting: "SOLE",
    is_put_call: null,
    accession: "0000102909-25-000001",
    period_of_report: "2024-12-31",
    ...overrides,
  };
}

describe("buildCsv", () => {
  it("emits a header line matching the column order", () => {
    const csv = buildCsv([]);
    const [header] = csv.split("\n");
    expect(header).toBe(
      "filer_key,filer_label,category,shares,value_usd,voting_authority,put_call,accession,period_of_report",
    );
  });

  it("renders typical rows without quoting safe values", () => {
    const csv = buildCsv([row()]);
    const [, dataLine] = csv.split("\n");
    expect(dataLine).toContain("0000102909,VANGUARD GROUP,etfs,1234567,230000000,SOLE,,0000102909-25-000001,2024-12-31");
  });

  it("escapes commas, quotes, and newlines via RFC 4180 quoting", () => {
    const csv = buildCsv([
      row({ label: 'Vanguard "Group", LLC' }),
      row({ label: "Two\nLine\nName" }),
    ]);
    expect(csv).toContain('"Vanguard ""Group"", LLC"');
    expect(csv).toContain('"Two\nLine\nName"');
  });

  it("formula-injection guard prefixes leading =/+/-/@ with a single quote", () => {
    // Excel / Sheets / Numbers interpret =CMD() as a formula on
    // import, which is a known CSV smuggling vector. Mirrors the
    // backend guard in app/api/instruments insider transactions.
    const csv = buildCsv([
      row({ label: "=cmd|' /C calc'!A0" }),
      row({ label: "+SUM(A:A)" }),
      row({ label: "-1234" }),
      row({ label: "@user" }),
    ]);
    expect(csv).toContain("'=cmd");
    expect(csv).toContain("'+SUM(A:A)");
    expect(csv).toContain("'-1234");
    expect(csv).toContain("'@user");
  });

  it("renders null value_usd as empty string, not 'null'", () => {
    const csv = buildCsv([row({ value_usd: null })]);
    expect(csv).not.toContain("null");
    // Empty token between two commas in the value_usd position.
    expect(csv).toMatch(/,1234567,,SOLE,/);
  });
});
