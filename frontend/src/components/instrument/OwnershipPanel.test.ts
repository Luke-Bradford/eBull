/**
 * Unit tests for the ownership panel's data extraction.
 *
 * Focused on the Form 3 baseline merging behaviour added in #768 PR4.
 * The full panel is React+ResponsiveContainer-heavy and not amenable
 * to a fast unit test; we test the pure ``extractData`` helper that
 * decides which insider holders surface on the per-officer ring.
 */

import { describe, expect, it } from "vitest";

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

describe("extractData — Form 3 baseline merging (#768 PR4)", () => {
  it("returns no insider holders when both Form 4 and baseline are empty", () => {
    const data = extractData(_BALANCE, _EMPTY_INSTITUTIONAL, _EMPTY_INSIDERS, _EMPTY_BASELINE);
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
    const data = extractData(_BALANCE, _EMPTY_INSTITUTIONAL, _EMPTY_INSIDERS, baseline);
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
    const data = extractData(_BALANCE, _EMPTY_INSTITUTIONAL, insiders, baseline);
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
    const data = extractData(_BALANCE, _EMPTY_INSTITUTIONAL, _EMPTY_INSIDERS, baseline);
    expect(data.insider_holders).toHaveLength(0);
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
    const data = extractData(_BALANCE, _EMPTY_INSTITUTIONAL, insiders, baseline);
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
    const data = extractData(_BALANCE, _EMPTY_INSTITUTIONAL, insiders, baseline);
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
    const data = extractData(_BALANCE, _EMPTY_INSTITUTIONAL, _EMPTY_INSIDERS, baseline);
    expect(data.insiders_as_of).toBe("2026-03-10");
  });
});
