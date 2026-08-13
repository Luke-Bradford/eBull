import { apiFetch } from "@/api/client";

/**
 * Tax & CGT read API (#1905 PR2).
 *
 * Passive read surface over the UK tax engine (`app/api/tax.py`, wrapping
 * `app/services/tax_ledger.py`). The engine owns every CGT treatment; these
 * types only mirror its JSON shapes for the Tax page.
 *
 * All money figures are statutory GBP — the disposal engine denominates in
 * GBP by law (settled-decisions: the tax `fx_rates` table, sql/013). The page
 * formats them with a hardcoded "GBP", never the operator's display currency.
 */

export interface TaxSummary {
  tax_year: string;
  total_gains_gbp: number;
  total_losses_gbp: number;
  net_gain_gbp: number;
  dividend_total_gbp: number;
  disposals_same_day: number;
  disposals_bed_and_breakfast: number;
  disposals_s104: number;
  annual_exempt_gbp: number;
  exempt_remaining_gbp: number;
  estimated_cgt_basic_scenario: number;
  estimated_cgt_higher_scenario: number;
}

export interface TaxDisposal {
  match_id: number;
  instrument_id: number;
  symbol: string;
  matching_rule: string;
  matched_units: number;
  acquisition_cost_gbp: number;
  disposal_proceeds_gbp: number;
  gain_or_loss_gbp: number;
  disposal_uk_date: string;
  tax_year: string;
  disposal_tax_lot_id: number;
  acquisition_tax_lot_id: number | null;
  matched_at: string;
}

export interface S104Pool {
  instrument_id: number;
  symbol: string;
  pool_units: number;
  pool_cost_gbp: number;
  pool_avg_cost_gbp: number;
  updated_at: string;
}

export interface TaxYears {
  current: string;
  available: string[];
}

/** `tax_year` is a UK label like "2026/27"; omit to default to the current year. */
export function fetchTaxSummary(taxYear?: string): Promise<TaxSummary> {
  const q = taxYear ? `?tax_year=${encodeURIComponent(taxYear)}` : "";
  return apiFetch<TaxSummary>(`/tax/summary${q}`);
}

export function fetchTaxDisposals(taxYear?: string): Promise<TaxDisposal[]> {
  const q = taxYear ? `?tax_year=${encodeURIComponent(taxYear)}` : "";
  return apiFetch<TaxDisposal[]>(`/tax/disposals${q}`);
}

export function fetchTaxPools(): Promise<S104Pool[]> {
  return apiFetch<S104Pool[]>(`/tax/pools`);
}

export function fetchTaxYears(): Promise<TaxYears> {
  return apiFetch<TaxYears>(`/tax/tax-years`);
}
