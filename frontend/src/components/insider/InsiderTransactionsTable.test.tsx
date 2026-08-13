import { fireEvent, render, screen, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import {
  InsiderTransactionsTable,
  buildCsv,
} from "@/components/insider/InsiderTransactionsTable";
import type { InsiderTransactionDetail } from "@/api/instruments";

function makeRow(
  partial: Partial<InsiderTransactionDetail>,
): InsiderTransactionDetail {
  return {
    accession_number: "0000000000-00-000001",
    txn_row_num: 0,
    document_type: "4",
    txn_date: "2026-04-15",
    deemed_execution_date: null,
    filer_cik: "0000000001",
    filer_name: "Jane Doe",
    filer_role: "officer:CFO",
    security_title: "Common Stock",
    txn_code: "P",
    acquired_disposed_code: "A",
    shares: "100",
    price: "10",
    post_transaction_shares: "1000",
    direct_indirect: "D",
    nature_of_ownership: null,
    is_derivative: false,
    equity_swap_involved: null,
    transaction_timeliness: null,
    conversion_exercise_price: null,
    exercise_date: null,
    expiration_date: null,
    underlying_security_title: null,
    underlying_shares: null,
    underlying_value: null,
    footnotes: {},
    ...partial,
  };
}

describe("InsiderTransactionsTable", () => {
  const rows: InsiderTransactionDetail[] = [
    makeRow({
      accession_number: "A1",
      txn_row_num: 0,
      txn_date: "2026-04-15",
      filer_name: "Jane Doe",
      txn_code: "P",
      acquired_disposed_code: "A",
      shares: "100",
      price: "10",
    }),
    makeRow({
      accession_number: "A2",
      txn_row_num: 0,
      txn_date: "2026-03-01",
      filer_name: "John Smith",
      filer_role: "director",
      txn_code: "S",
      acquired_disposed_code: "D",
      shares: "500",
      price: "20",
    }),
    makeRow({
      accession_number: "A3",
      txn_row_num: 0,
      txn_date: "2026-02-10",
      filer_name: "Alice Wong",
      filer_role: "officer:CEO",
      txn_code: "M",
      acquired_disposed_code: "A",
      shares: "50",
      price: null,
    }),
  ];

  it("sorts by date descending by default", () => {
    render(<InsiderTransactionsTable symbol="GME" transactions={rows} />);
    const tableRows = screen.getAllByRole("row");
    // Header + 3 data rows
    expect(tableRows).toHaveLength(4);
    expect(within(tableRows[1]!).getByText("2026-04-15")).toBeInTheDocument();
    expect(within(tableRows[3]!).getByText("2026-02-10")).toBeInTheDocument();
  });

  it("toggles sort direction on repeat header click", () => {
    render(<InsiderTransactionsTable symbol="GME" transactions={rows} />);
    fireEvent.click(screen.getByTestId("insider-table-sort-txn_date"));
    // Now ascending: oldest first
    let tableRows = screen.getAllByRole("row");
    expect(within(tableRows[1]!).getByText("2026-02-10")).toBeInTheDocument();
    fireEvent.click(screen.getByTestId("insider-table-sort-txn_date"));
    tableRows = screen.getAllByRole("row");
    expect(within(tableRows[1]!).getByText("2026-04-15")).toBeInTheDocument();
  });

  it("sorts numerics descending by default when picking a numeric column", () => {
    render(<InsiderTransactionsTable symbol="GME" transactions={rows} />);
    fireEvent.click(screen.getByTestId("insider-table-sort-shares"));
    // Largest first → 500, 100, 50
    const tableRows = screen.getAllByRole("row");
    expect(within(tableRows[1]!).getByText("500")).toBeInTheDocument();
    expect(within(tableRows[3]!).getByText("50")).toBeInTheDocument();
  });

  it("filters case-insensitively across name, role and security title", () => {
    render(<InsiderTransactionsTable symbol="GME" transactions={rows} />);
    fireEvent.change(screen.getByTestId("insider-table-filter"), {
      target: { value: "ceo" },
    });
    expect(screen.getByText("Alice Wong")).toBeInTheDocument();
    expect(screen.queryByText("Jane Doe")).not.toBeInTheDocument();
    expect(screen.queryByText("John Smith")).not.toBeInTheDocument();
    expect(screen.getByText(/Showing 1 of 3/)).toBeInTheDocument();
  });

  it("disables Export CSV button when filter empties the table", () => {
    render(<InsiderTransactionsTable symbol="GME" transactions={rows} />);
    fireEvent.change(screen.getByTestId("insider-table-filter"), {
      target: { value: "no-such-officer" },
    });
    expect(
      screen.getByTestId("insider-table-export-csv"),
    ).toBeDisabled();
  });

  it("clicking Export CSV triggers a Blob download with the filtered + sorted view", () => {
    const createObjectURL = vi.fn<(blob: Blob) => string>(() => "blob:mock");
    const revokeObjectURL = vi.fn<(url: string) => void>();
    Object.defineProperty(URL, "createObjectURL", {
      configurable: true,
      writable: true,
      value: createObjectURL,
    });
    Object.defineProperty(URL, "revokeObjectURL", {
      configurable: true,
      writable: true,
      value: revokeObjectURL,
    });
    render(<InsiderTransactionsTable symbol="GME" transactions={rows} />);
    fireEvent.click(screen.getByTestId("insider-table-export-csv"));
    expect(createObjectURL).toHaveBeenCalledTimes(1);
    const firstCall = createObjectURL.mock.calls[0];
    expect(firstCall).toBeDefined();
    const blob = firstCall![0];
    expect(blob).toBeInstanceOf(Blob);
    expect(blob.type).toContain("text/csv");
    expect(revokeObjectURL).toHaveBeenCalledWith("blob:mock");
  });
});

describe("buildCsv", () => {
  it("emits header + one row per transaction with classified direction", () => {
    const csv = buildCsv([
      makeRow({
        txn_date: "2026-04-15",
        filer_name: "Jane Doe",
        txn_code: "P",
        acquired_disposed_code: "A",
        shares: "100",
        price: "10",
      }),
    ]);
    const lines = csv.split("\r\n");
    expect(lines[0]).toContain("txn_date,filer_name");
    expect(lines[1]).toContain("Jane Doe");
    expect(lines[1]).toContain(",acquired,");
    expect(lines[1]).toContain(",1000,"); // value column = 100*10
  });

  it("escapes commas, quotes and newlines per RFC 4180", () => {
    const csv = buildCsv([
      makeRow({
        filer_name: 'Smith, "Junior"',
        security_title: "Common\nStock",
      }),
    ]);
    const dataLine = csv.split("\r\n")[1] ?? "";
    expect(dataLine).toContain('"Smith, ""Junior"""');
    expect(dataLine).toContain('"Common\nStock"');
  });

  it("sanitises Excel-formula prefixes to defeat CSV injection", () => {
    const csv = buildCsv([
      makeRow({
        filer_name: "=cmd|'/c calc'!A1",
        security_title: "+SUM(A1:A2)",
      }),
    ]);
    const dataLine = csv.split("\r\n")[1] ?? "";
    // Apostrophe prefix neutralises the formula trigger. RFC 4180
    // quoting only kicks in when the value also has comma/quote/CRLF;
    // these test inputs don't, so the bare prefixed form is correct.
    expect(dataLine).toContain("'=cmd|'/c calc'!A1");
    expect(dataLine).toContain("'+SUM(A1:A2)");
    // No raw cell may begin with a formula-trigger character. Walk
    // each comma-separated field and assert it starts with something
    // safe. Quoting is fine because the leading char is then `"`.
    for (const cell of dataLine.split(",")) {
      expect(cell.charAt(0)).not.toMatch(/[=+\-@\t\r]/);
    }
  });

  it("also sanitises the - and @ prefixes (full OWASP set)", () => {
    const csv = buildCsv([
      makeRow({
        filer_name: "-2+3",
        filer_role: "@SUM(A1)",
      }),
    ]);
    const line = csv.split("\r\n")[1] ?? "";
    expect(line).toContain("'-2+3");
    expect(line).toContain("'@SUM(A1)");
  });
});
