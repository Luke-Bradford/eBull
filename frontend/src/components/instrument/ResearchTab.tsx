/**
 * ResearchTab — default tab of the per-stock research page (Slice 1 of
 * docs/superpowers/specs/2026-04-20-per-stock-research-page.md).
 *
 * Composes existing data into one operator view: key stats with
 * field_source provenance, thesis memo if present, break conditions.
 * Red-flag surfacing and peer context come in Slice 2 (right rail).
 *
 * Capability panels (Dividends, Insider activity, Corporate events)
 * iterate ``summary.capabilities[type]`` and render one shell per
 * active (data-present) provider — so a cross-listed instrument with
 * multiple providers renders multiple panels labelled with each
 * provider tag (#515 PR 3b).
 */
import { Section } from "@/components/dashboard/Section";
import { BusinessSectionsTeaser } from "@/components/instrument/BusinessSectionsTeaser";
import { DividendsPanel } from "@/components/instrument/DividendsPanel";
import { EightKEventsPanel } from "@/components/instrument/EightKEventsPanel";
import { InsiderActivityPanel } from "@/components/instrument/InsiderActivityPanel";
import { SecProfilePanel } from "@/components/instrument/SecProfilePanel";
import { EmptyState } from "@/components/states/EmptyState";
import type { CapabilityCell, InstrumentSummary, ThesisDetail } from "@/api/types";
import { activeProviders } from "@/lib/capabilityProviders";

function formatDecimal(
  value: string | null | undefined,
  opts: { percent?: boolean } = {},
): string {
  if (value === null || value === undefined) return "—";
  const num = Number(value);
  if (!Number.isFinite(num)) return "—";
  if (opts.percent) return `${(num * 100).toFixed(2)}%`;
  return num.toLocaleString(undefined, { maximumFractionDigits: 2 });
}

function formatMarketCap(value: string | null): string {
  if (value === null) return "—";
  const num = Number(value);
  if (!Number.isFinite(num)) return "—";
  if (num >= 1e12) return `${(num / 1e12).toFixed(2)}T`;
  if (num >= 1e9) return `${(num / 1e9).toFixed(2)}B`;
  if (num >= 1e6) return `${(num / 1e6).toFixed(2)}M`;
  return num.toLocaleString();
}

function FieldSourceTag({ source }: { source: string | undefined }) {
  if (!source) return null;
  // Colour-code the provenance so the operator sees at-a-glance where
  // each figure came from. Matches the KeyStatsFieldSource union from
  // frontend/src/api/types.ts.
  let tone = "bg-slate-100 text-slate-600";
  let label = source;
  switch (source) {
    case "sec_xbrl":
      tone = "bg-emerald-50 text-emerald-700";
      label = "SEC";
      break;
    case "sec_dividend_summary":
      tone = "bg-emerald-50 text-emerald-700";
      label = "SEC · div";
      break;
    case "sec_xbrl_price_missing":
      tone = "bg-amber-50 text-amber-700";
      label = "SEC · price?";
      break;
    case "unavailable":
      tone = "bg-slate-100 text-slate-500";
      label = "—";
      break;
  }
  return (
    <span className={`ml-2 rounded px-1.5 py-0.5 text-[10px] uppercase ${tone}`}>
      {label}
    </span>
  );
}

function KeyStat({
  label,
  value,
  source,
}: {
  label: string;
  value: string;
  source?: string;
}) {
  return (
    <>
      <dt className="text-slate-500">{label}</dt>
      <dd className="flex items-center tabular-nums">
        <span>{value}</span>
        <FieldSourceTag source={source} />
      </dd>
    </>
  );
}

function ThesisPanel({
  thesis,
  errored,
}: {
  thesis: ThesisDetail | null;
  errored: boolean;
}) {
  if (errored) {
    return (
      <EmptyState
        title="Thesis temporarily unavailable"
        description="Failed to fetch the latest thesis. Retry via the Generate thesis button in the strip above."
      />
    );
  }
  if (thesis === null) {
    return (
      <EmptyState
        title="No thesis yet"
        description="Generate one from the strip above — the AI will pull the latest filings, news, and fundamentals to draft a buy/hold/exit memo."
      />
    );
  }
  const breaks = thesis.break_conditions_json ?? [];
  return (
    <div className="space-y-3 text-sm">
      <div className="whitespace-pre-wrap text-slate-700">
        {thesis.memo_markdown}
      </div>
      {(thesis.base_value !== null ||
        thesis.bull_value !== null ||
        thesis.bear_value !== null) && (
        <dl className="grid grid-cols-3 gap-2 rounded bg-slate-50 p-3 text-xs">
          <div>
            <dt className="text-slate-500">Bear</dt>
            <dd className="font-medium tabular-nums">
              {thesis.bear_value !== null ? thesis.bear_value : "—"}
            </dd>
          </div>
          <div>
            <dt className="text-slate-500">Base</dt>
            <dd className="font-medium tabular-nums">
              {thesis.base_value !== null ? thesis.base_value : "—"}
            </dd>
          </div>
          <div>
            <dt className="text-slate-500">Bull</dt>
            <dd className="font-medium tabular-nums">
              {thesis.bull_value !== null ? thesis.bull_value : "—"}
            </dd>
          </div>
        </dl>
      )}
      {breaks.length > 0 && (
        <div>
          <div className="mb-1 text-xs font-medium uppercase tracking-wider text-slate-500">
            Break conditions
          </div>
          <ul className="list-inside list-disc space-y-0.5 text-xs text-slate-600">
            {breaks.map((b, i) => (
              <li key={i}>{b}</li>
            ))}
          </ul>
        </div>
      )}
    </div>
  );
}

export interface ResearchTabProps {
  summary: InstrumentSummary;
  thesis: ThesisDetail | null;
  thesisErrored?: boolean;
}

const EMPTY_CELL: CapabilityCell = { providers: [], data_present: {} };

export function ResearchTab({
  summary,
  thesis,
  thesisErrored = false,
}: ResearchTabProps): JSX.Element {
  const stats = summary.key_stats;
  const fs = stats?.field_source ?? undefined;

  // SEC profile + 10-K Item 1 panels are still SEC-specific (not yet
  // refactored into provider-agnostic shells); gate on the SEC
  // identifier signal directly. The three capability panels below
  // (Dividends, Insider, Corporate events) iterate
  // `summary.capabilities[type]` and render one shell per active
  // provider — provider-agnostic by construction.
  const hasSec = summary.has_sec_cik;
  const dividends = summary.capabilities.dividends ?? EMPTY_CELL;
  const insider = summary.capabilities.insider ?? EMPTY_CELL;
  const events = summary.capabilities.corporate_events ?? EMPTY_CELL;
  const dividendProviders = activeProviders(dividends);
  const insiderProviders = activeProviders(insider);
  const eventProviders = activeProviders(events);

  return (
    <div className="grid gap-4 md:grid-cols-2">
      {hasSec ? <SecProfilePanel symbol={summary.identity.symbol} /> : null}
      {dividendProviders.map((p) => (
        <DividendsPanel
          key={`dividends-${p}`}
          symbol={summary.identity.symbol}
          provider={p}
        />
      ))}
      {hasSec ? (
        <div className="md:col-span-2">
          <BusinessSectionsTeaser symbol={summary.identity.symbol} />
        </div>
      ) : null}
      {insiderProviders.map((p) => (
        <div key={`insider-${p}`} className="md:col-span-2">
          <InsiderActivityPanel
            symbol={summary.identity.symbol}
            provider={p}
          />
        </div>
      ))}
      {eventProviders.map((p) => (
        <div key={`events-${p}`} className="md:col-span-2">
          <EightKEventsPanel
            symbol={summary.identity.symbol}
            provider={p}
          />
        </div>
      ))}

      <Section title="Key statistics">
        {stats === null ? (
          <EmptyState
            title="No key stats"
            description="No provider returned key stats for this ticker."
          />
        ) : (
          <dl className="grid grid-cols-[auto_1fr] gap-x-4 gap-y-2 text-sm">
            <KeyStat label="Market cap" value={formatMarketCap(summary.identity.market_cap)} />
            <KeyStat label="P/E ratio" value={formatDecimal(stats.pe_ratio)} source={fs?.pe_ratio} />
            <KeyStat label="P/B ratio" value={formatDecimal(stats.pb_ratio)} source={fs?.pb_ratio} />
            <KeyStat label="Dividend yield" value={formatDecimal(stats.dividend_yield, { percent: true })} source={fs?.dividend_yield} />
            <KeyStat label="Payout ratio" value={formatDecimal(stats.payout_ratio, { percent: true })} source={fs?.payout_ratio} />
            <KeyStat label="ROE" value={formatDecimal(stats.roe, { percent: true })} source={fs?.roe} />
            <KeyStat label="ROA" value={formatDecimal(stats.roa, { percent: true })} source={fs?.roa} />
            <KeyStat label="Debt / Equity" value={formatDecimal(stats.debt_to_equity)} source={fs?.debt_to_equity} />
            <KeyStat label="Revenue growth (YoY)" value={formatDecimal(stats.revenue_growth_yoy, { percent: true })} source={fs?.revenue_growth_yoy} />
            <KeyStat label="Earnings growth (YoY)" value={formatDecimal(stats.earnings_growth_yoy, { percent: true })} source={fs?.earnings_growth_yoy} />
          </dl>
        )}
      </Section>

      <Section title="Thesis">
        <ThesisPanel thesis={thesis} errored={thesisErrored} />
      </Section>
    </div>
  );
}
