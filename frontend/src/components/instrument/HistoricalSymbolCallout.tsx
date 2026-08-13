/**
 * Historical-symbol callout (#794 frontend finish, Batch 7 of #788).
 *
 * The user's 2026-05-03 question:
 *
 *   "I see things like in BBBY, we don't have all the financial
 *   information available, because they changed tickers, but assume
 *   the cik was still the same, filings should still be represented
 *   by the etoro id provided?"
 *
 * Renders a small inline callout when the instrument's symbol chain
 * (from ``instrument_symbol_history``) includes any symbol other
 * than the current one. Tells the operator "filings before YYYY-MM-DD
 * appear on EDGAR under SYMBOL_X" so they can verify the historical
 * coverage path on the source side.
 *
 * Hidden when the chain is just the current ``imported`` row (no
 * historical context to surface).
 */

import type { OwnershipHistoricalSymbol } from "@/api/ownership";

export interface HistoricalSymbolCalloutProps {
  readonly currentSymbol: string;
  readonly historicalSymbols: readonly OwnershipHistoricalSymbol[];
}

export function HistoricalSymbolCallout({
  currentSymbol,
  historicalSymbols,
}: HistoricalSymbolCalloutProps): JSX.Element | null {
  const priorSymbols = historicalSymbols.filter(
    (h) => h.symbol.toUpperCase() !== currentSymbol.toUpperCase(),
  );
  if (priorSymbols.length === 0) return null;

  // Render the most-recent prior symbol prominently (the typical
  // BBBY → BBBYQ case has exactly one prior). When >1 prior, list
  // all in chronological order so the operator can trace the
  // chain.
  return (
    <p
      className="rounded-md border border-blue-200 bg-blue-50 px-3 py-2 text-xs text-blue-900 dark:border-blue-900/60 dark:bg-blue-900/20 dark:text-blue-200"
      data-test="historical-symbol-callout"
      role="note"
    >
      <span className="font-medium">Symbol history:</span> filings before
      {" "}
      <span className="font-mono">
        {priorSymbols.map((h, i) => (
          <span key={h.symbol + h.effective_from}>
            {i > 0 && ", "}
            {h.symbol}
            {h.effective_to !== null && ` (until ${h.effective_to})`}
          </span>
        ))}
      </span>{" "}
      are aggregated under the current{" "}
      <span className="font-mono">{currentSymbol}</span> instrument via the
      stable SEC CIK.
    </p>
  );
}
