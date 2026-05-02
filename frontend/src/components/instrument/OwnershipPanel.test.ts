/**
 * Unit tests for the ownership panel's data extraction.
 *
 * Focused on the Form 3 baseline merging behaviour added in #768 PR4.
 * The full panel is React+ResponsiveContainer-heavy and not amenable
 * to a fast unit test; we test the pure ``extractData`` helper that
 * decides which insider holders surface on the per-officer ring.
 */

import { describe, expect, it } from "vitest";

import type { BlockholdersResponse } from "@/api/blockholders";
import type {
  InsiderBaselineList,
  InsiderTransactionsList,
} from "@/api/instruments";
import type { InstitutionalHoldingsResponse } from "@/api/institutionalHoldings";
import type { InstrumentFinancials } from "@/api/types";

import { extractData } from "./OwnershipPanel";

const _BALANCE: InstrumentFinancials = {
  symbol: "AAPL",
  statement: "balance",
  period: "quarterly",
  currency: "USD",
  source: "sec_xbrl",
  rows: [
    {
      period_end: "2026-03-28",
      values: {
        shares_outstanding: "14000000000",
        treasury_shares: "100000000",
      },
    },
  ],
} as unknown as InstrumentFinancials;

const _EMPTY_INSTITUTIONAL: InstitutionalHoldingsResponse = {
  symbol: "AAPL",
  totals: null,
  filers: [],
};

const _EMPTY_INSIDERS: InsiderTransactionsList = {
  symbol: "AAPL",
  rows: [],
};

const _EMPTY_BASELINE: InsiderBaselineList = {
  symbol: "AAPL",
  rows: [],
};

const _EMPTY_BLOCKHOLDERS: BlockholdersResponse = {
  symbol: "AAPL",
  totals: null,
  blockholders: [],
};

describe("extractData — Form 3 baseline merging (#768 PR4)", () => {
  it("returns no insider holders when both Form 4 and baseline are empty", () => {
    const data = extractData(_BALANCE, _EMPTY_INSTITUTIONAL, _EMPTY_INSIDERS, _EMPTY_BASELINE, _EMPTY_BLOCKHOLDERS);
    expect(data.insider_holders).toHaveLength(0);
    expect(data.insiders_total).toBeNull();
  });

  it("surfaces baseline-only filers on the insider holders list", () => {
    const baseline: InsiderBaselineList = {
      symbol: "AAPL",
      rows: [
        {
          filer_cik: "0001000099",
          filer_name: "Doe, Jane",
          filer_role: "officer:CFO",
          security_title: "Common Stock",
          is_derivative: false,
          direct_indirect: "D",
          shares: "12500",
          value_owned: null,
          as_of_date: "2026-01-15",
        },
      ],
    };
    const data = extractData(_BALANCE, _EMPTY_INSTITUTIONAL, _EMPTY_INSIDERS, baseline, _EMPTY_BLOCKHOLDERS);
    expect(data.insider_holders).toHaveLength(1);
    expect(data.insider_holders[0]!.label).toBe("Doe, Jane");
    expect(data.insider_holders[0]!.shares).toBe(12500);
    expect(data.insiders_total).toBe(12500);
  });

  it("merges Form 4 holders with baseline filers without overlap", () => {
    // Backend's NOT EXISTS gate guarantees no CIK overlap between
    // the two sets, but the frontend must additionally key the
    // baseline rows under a distinct ``baseline:`` prefix so a
    // future bug in the gate doesn't cause two same-CIK rows to
    // collide on the same SunburstHolder.key (Recharts would
    // silently drop one).
    const insiders: InsiderTransactionsList = {
      symbol: "AAPL",
      rows: [
        {
          filer_cik: "0001000001",
          filer_name: "Smith, John",
          txn_date: "2026-04-15",
          post_transaction_shares: "50000",
          is_derivative: false,
        },
      ] as InsiderTransactionsList["rows"],
    };
    const baseline: InsiderBaselineList = {
      symbol: "AAPL",
      rows: [
        {
          filer_cik: "0001000099",
          filer_name: "Doe, Jane",
          filer_role: "officer:CFO",
          security_title: "Common Stock",
          is_derivative: false,
          direct_indirect: "D",
          shares: "12500",
          value_owned: null,
          as_of_date: "2026-01-15",
        },
      ],
    };
    const data = extractData(_BALANCE, _EMPTY_INSTITUTIONAL, insiders, baseline, _EMPTY_BLOCKHOLDERS);
    expect(data.insider_holders).toHaveLength(2);
    const keys = data.insider_holders.map((h) => h.key);
    expect(new Set(keys).size).toBe(2); // no key collision
    expect(keys.some((k) => k.startsWith("baseline:"))).toBe(true);
    // Total sums Form 4 + baseline.
    expect(data.insiders_total).toBe(62500);
  });

  it("drops baseline rows with null or non-positive shares", () => {
    // A baseline row with a null share count (e.g. value-branch
    // holding without a share count, or a malformed ingest) must
    // not produce a phantom wedge.
    const baseline: InsiderBaselineList = {
      symbol: "AAPL",
      rows: [
        {
          filer_cik: "0001000099",
          filer_name: "Null Filer",
          filer_role: null,
          security_title: "Series A Units",
          is_derivative: false,
          direct_indirect: "D",
          shares: null,
          value_owned: "250000",
          as_of_date: "2026-01-15",
        },
        {
          filer_cik: "0001000100",
          filer_name: "Zero Filer",
          filer_role: null,
          security_title: "Common Stock",
          is_derivative: false,
          direct_indirect: "D",
          shares: "0",
          value_owned: null,
          as_of_date: "2026-01-15",
        },
      ],
    };
    const data = extractData(_BALANCE, _EMPTY_INSTITUTIONAL, _EMPTY_INSIDERS, baseline, _EMPTY_BLOCKHOLDERS);
    expect(data.insider_holders).toHaveLength(0);
    // Codex / bot review of #768 PR4: the freshness chip's
    // ``insiders_as_of`` must use the same eligibility predicate as
    // the holders builder. A null/zero-shares baseline row that
    // never renders must not advance the chip past the actual ring.
    expect(data.insiders_as_of).toBeNull();
  });

  it("insiders_as_of takes the max of latest Form 4 txn_date and baseline as_of_date", () => {
    // Issuer with one Form 4 row from 2026-02-10 and one baseline
    // row from 2026-04-01 — the baseline date is more recent so
    // the chip's "as of" reflects the baseline.
    const insiders: InsiderTransactionsList = {
      symbol: "AAPL",
      rows: [
        {
          filer_cik: "0001000001",
          filer_name: "Smith, John",
          txn_date: "2026-02-10",
          post_transaction_shares: "100",
          is_derivative: false,
        },
      ] as InsiderTransactionsList["rows"],
    };
    const baseline: InsiderBaselineList = {
      symbol: "AAPL",
      rows: [
        {
          filer_cik: "0001000099",
          filer_name: "Doe, Jane",
          filer_role: null,
          security_title: "Common Stock",
          is_derivative: false,
          direct_indirect: "D",
          shares: "5000",
          value_owned: null,
          as_of_date: "2026-04-01",
        },
      ],
    };
    const data = extractData(_BALANCE, _EMPTY_INSTITUTIONAL, insiders, baseline, _EMPTY_BLOCKHOLDERS);
    expect(data.insiders_as_of).toBe("2026-04-01");
  });

  it("when Form 4 is newer than baseline, insiders_as_of takes the Form 4 date (Codex coverage gap)", () => {
    const insiders: InsiderTransactionsList = {
      symbol: "AAPL",
      rows: [
        {
          filer_cik: "0001000001",
          filer_name: "Smith, John",
          txn_date: "2026-04-15",
          post_transaction_shares: "50000",
          is_derivative: false,
        },
      ] as InsiderTransactionsList["rows"],
    };
    const baseline: InsiderBaselineList = {
      symbol: "AAPL",
      rows: [
        {
          filer_cik: "0001000099",
          filer_name: "Doe, Jane",
          filer_role: null,
          security_title: "Common Stock",
          is_derivative: false,
          direct_indirect: "D",
          shares: "5000",
          value_owned: null,
          as_of_date: "2025-11-30",
        },
      ],
    };
    const data = extractData(_BALANCE, _EMPTY_INSTITUTIONAL, insiders, baseline, _EMPTY_BLOCKHOLDERS);
    // Form 4 (2026-04-15) is newer than baseline (2025-11-30) — chip
    // takes the Form 4 date so the chip stays in lockstep with the
    // most recent insider activity. An older baseline never silently
    // overrides a newer trade.
    expect(data.insiders_as_of).toBe("2026-04-15");
  });

  it("baseline-only issuer (no Form 4 rows) still produces an Insiders as_of_date", () => {
    const baseline: InsiderBaselineList = {
      symbol: "AAPL",
      rows: [
        {
          filer_cik: "0001000099",
          filer_name: "Doe, Jane",
          filer_role: null,
          security_title: "Common Stock",
          is_derivative: false,
          direct_indirect: "D",
          shares: "5000",
          value_owned: null,
          as_of_date: "2026-03-10",
        },
      ],
    };
    const data = extractData(_BALANCE, _EMPTY_INSTITUTIONAL, _EMPTY_INSIDERS, baseline, _EMPTY_BLOCKHOLDERS);
    expect(data.insiders_as_of).toBe("2026-03-10");
  });
});

describe("extractData — blockholders 5th category (#766 PR3)", () => {
  it("returns no blockholder holders when response totals are null", () => {
    const data = extractData(_BALANCE, _EMPTY_INSTITUTIONAL, _EMPTY_INSIDERS, _EMPTY_BASELINE, _EMPTY_BLOCKHOLDERS);
    expect(data.blockholder_holders).toHaveLength(0);
    expect(data.blockholders_total).toBeNull();
    expect(data.blockholders_as_of).toBeNull();
  });

  it("maps each block to one holder keyed on filer_cik", () => {
    const blockholders: BlockholdersResponse = {
      symbol: "AAPL",
      totals: {
        blockholders_shares: "4500000",
        active_shares: "1500000",
        passive_shares: "3000000",
        total_filers: 2,
        as_of_date: "2025-11-06",
      },
      blockholders: [
        {
          filer_cik: "0001234567",
          filer_name: "Test Activist Fund LP",
          reporter_cik: "0001234567",
          reporter_name: "Test Activist Fund LP",
          submission_type: "SCHEDULE 13D",
          status: "active",
          accession_number: "0001234567-25-000001",
          aggregate_amount_owned: "1500000",
          percent_of_class: "5.5",
          additional_reporters: 0,
          date_of_event: "2025-11-03",
          filed_at: "2025-11-06T00:00:00Z",
        },
        {
          filer_cik: "0007654321",
          filer_name: "Index Fund",
          reporter_cik: "0007654321",
          reporter_name: "Index Fund",
          submission_type: "SCHEDULE 13G",
          status: "passive",
          accession_number: "0007654321-25-000001",
          aggregate_amount_owned: "3000000",
          percent_of_class: "11.0",
          additional_reporters: 0,
          date_of_event: "2025-09-30",
          filed_at: "2025-10-01T00:00:00Z",
        },
      ],
    };
    const data = extractData(_BALANCE, _EMPTY_INSTITUTIONAL, _EMPTY_INSIDERS, _EMPTY_BASELINE, blockholders);
    expect(data.blockholder_holders).toHaveLength(2);
    expect(data.blockholder_holders.map((h) => h.key)).toEqual([
      "block:0001234567",
      "block:0007654321",
    ]);
    expect(data.blockholder_holders[0]!.shares).toBe(1500000);
    expect(data.blockholder_holders[0]!.category).toBe("blockholders");
    expect(data.blockholders_total).toBe(4500000);
    expect(data.blockholders_as_of).toBe("2025-11-06");
  });

  it("collapses joint-filing reporters to one wedge per accession", () => {
    // A joint 13D filing surfaces as 2 per-reporter rows from the
    // backend, both pointing at the same accession with the same
    // 1.5M-share aggregate. The frontend must dedupe by accession
    // so the wedge count matches the backend's per-accession-block
    // count — without this dedupe, the snapshot-lag leaf-sum bump
    // in buildSunburstRings would inflate the category total to 3M.
    const blockholders: BlockholdersResponse = {
      symbol: "AAPL",
      totals: {
        blockholders_shares: "1500000",
        active_shares: "1500000",
        passive_shares: "0",
        total_filers: 1,
        as_of_date: "2025-11-06",
      },
      blockholders: [
        {
          filer_cik: "0001234567",
          filer_name: "Test Activist Fund LP",
          reporter_cik: "0001234567",
          reporter_name: "Test Activist Fund LP",
          submission_type: "SCHEDULE 13D",
          status: "active",
          accession_number: "0001234567-25-000010",
          aggregate_amount_owned: "1500000",
          percent_of_class: "5.5",
          additional_reporters: 1,
          date_of_event: "2025-11-03",
          filed_at: "2025-11-06T00:00:00Z",
        },
        {
          filer_cik: "0001234567",
          filer_name: "Test Activist Fund LP",
          reporter_cik: null,
          reporter_name: "Jane Doe (managing member)",
          submission_type: "SCHEDULE 13D",
          status: "active",
          accession_number: "0001234567-25-000010", // same accession!
          aggregate_amount_owned: "1500000",
          percent_of_class: "5.5",
          additional_reporters: 1,
          date_of_event: "2025-11-03",
          filed_at: "2025-11-06T00:00:00Z",
        },
      ],
    };
    const data = extractData(_BALANCE, _EMPTY_INSTITUTIONAL, _EMPTY_INSIDERS, _EMPTY_BASELINE, blockholders);
    expect(data.blockholder_holders).toHaveLength(1);
    expect(data.blockholder_holders[0]!.shares).toBe(1500000);
  });

  it("does not collide wedge keys when one submitter has two distinct holders", () => {
    // One EDGAR submitter (same filer_cik) files for two different
    // beneficial owners (different reporter_cik). The wedge keys
    // must be distinct so Recharts does not silently drop one.
    const blockholders: BlockholdersResponse = {
      symbol: "AAPL",
      totals: {
        blockholders_shares: "3000000",
        active_shares: "3000000",
        passive_shares: "0",
        total_filers: 2,
        as_of_date: "2025-11-06",
      },
      blockholders: [
        {
          filer_cik: "0003333333", // shared submitter
          filer_name: "Shared Adviser LLC",
          reporter_cik: "0008000001",
          reporter_name: "Beneficial Owner A",
          submission_type: "SCHEDULE 13D",
          status: "active",
          accession_number: "0003333333-25-000001",
          aggregate_amount_owned: "1000000",
          percent_of_class: "3.5",
          additional_reporters: 0,
          date_of_event: "2025-11-01",
          filed_at: "2025-11-01T00:00:00Z",
        },
        {
          filer_cik: "0003333333", // shared submitter
          filer_name: "Shared Adviser LLC",
          reporter_cik: "0008000002",
          reporter_name: "Beneficial Owner B",
          submission_type: "SCHEDULE 13D",
          status: "active",
          accession_number: "0003333333-25-000002",
          aggregate_amount_owned: "2000000",
          percent_of_class: "7.0",
          additional_reporters: 0,
          date_of_event: "2025-11-06",
          filed_at: "2025-11-06T00:00:00Z",
        },
      ],
    };
    const data = extractData(_BALANCE, _EMPTY_INSTITUTIONAL, _EMPTY_INSIDERS, _EMPTY_BASELINE, blockholders);
    expect(data.blockholder_holders).toHaveLength(2);
    const keys = data.blockholder_holders.map((h) => h.key);
    expect(new Set(keys).size).toBe(2);
    expect(keys).toContain("block:0008000001");
    expect(keys).toContain("block:0008000002");
  });

  it("drops blockholder rows with null or non-positive aggregate_amount_owned", () => {
    // A defer-to-prior-cover-page filing carries null aggregate
    // numbers — those rows surface in the drilldown table but
    // cannot size a wedge. Blockholders_total still reflects the
    // backend totals (sum across blocks with real numbers).
    const blockholders: BlockholdersResponse = {
      symbol: "AAPL",
      totals: {
        blockholders_shares: "1500000",
        active_shares: "1500000",
        passive_shares: "0",
        total_filers: 1,
        as_of_date: "2025-11-06",
      },
      blockholders: [
        {
          filer_cik: "0001234567",
          filer_name: "Real Block",
          reporter_cik: "0001234567",
          reporter_name: "Real Block",
          submission_type: "SCHEDULE 13D",
          status: "active",
          accession_number: "0001234567-25-000001",
          aggregate_amount_owned: "1500000",
          percent_of_class: "5.5",
          additional_reporters: 0,
          date_of_event: "2025-11-03",
          filed_at: "2025-11-06T00:00:00Z",
        },
        {
          filer_cik: "0009999999",
          filer_name: "Defer-to-Prior Block",
          reporter_cik: null,
          reporter_name: "Defer-to-Prior Block",
          submission_type: "SCHEDULE 13D/A",
          status: "active",
          accession_number: "0009999999-25-000001",
          aggregate_amount_owned: null,
          percent_of_class: null,
          additional_reporters: 0,
          date_of_event: "2025-11-03",
          filed_at: "2025-11-06T00:00:00Z",
        },
      ],
    };
    const data = extractData(_BALANCE, _EMPTY_INSTITUTIONAL, _EMPTY_INSIDERS, _EMPTY_BASELINE, blockholders);
    expect(data.blockholder_holders).toHaveLength(1);
    expect(data.blockholder_holders[0]!.label).toBe("Real Block");
  });
});
