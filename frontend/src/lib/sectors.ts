/**
 * GICS sector-SPDR options for the operator-facing sector filter (#1675).
 *
 * Mirror of `app/services/sector_classification.py::SPDR_SECTORS` (the backend
 * single source of truth). The filter value is the SPDR symbol (e.g. `XLK`);
 * the label is the GICS sector name. Kept in sync by hand — the set is the 11
 * fixed GICS sectors and does not change.
 */
export interface SectorOption {
  /** SPDR symbol — the `sector_spdr` filter value sent to the API. */
  value: string;
  /** GICS sector name — the human-readable dropdown label. */
  label: string;
}

export const SECTOR_OPTIONS: readonly SectorOption[] = [
  { value: "XLC", label: "Communication Services" },
  { value: "XLY", label: "Consumer Discretionary" },
  { value: "XLP", label: "Consumer Staples" },
  { value: "XLE", label: "Energy" },
  { value: "XLF", label: "Financials" },
  { value: "XLV", label: "Health Care" },
  { value: "XLI", label: "Industrials" },
  { value: "XLK", label: "Information Technology" },
  { value: "XLB", label: "Materials" },
  { value: "XLRE", label: "Real Estate" },
  { value: "XLU", label: "Utilities" },
] as const;
