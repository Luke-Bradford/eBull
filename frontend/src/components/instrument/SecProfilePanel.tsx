/**
 * SecProfilePanel — SEC-sourced entity metadata for the instrument
 * page. Backed by GET /instruments/{symbol}/sec_profile (#427).
 *
 * Surfaces authentic business description, SIC sector, exchange
 * listings, former names. Replaces the yfinance long_business_summary
 * blurb as the primary description source for US-mapped tickers.
 * Form-4 insider activity lives in the sibling InsiderActivityPanel
 * (backed by #429 ingestion) and is no longer summarised here.
 */

import {
  fetchInstrumentEmployees,
  fetchInstrumentSecProfile,
} from "@/api/instruments";
import type {
  InstrumentHeadcount,
  InstrumentSecProfile,
} from "@/api/instruments";
import {
  Section,
  SectionError,
  SectionSkeleton,
} from "@/components/dashboard/Section";
import { EmptyState } from "@/components/states/EmptyState";
import { useAsync } from "@/lib/useAsync";
import { useCallback } from "react";

export interface SecProfilePanelProps {
  readonly symbol: string;
}

export function SecProfilePanel({ symbol }: SecProfilePanelProps) {
  const state = useAsync<InstrumentSecProfile | null>(
    useCallback(() => fetchInstrumentSecProfile(symbol), [symbol]),
    [symbol],
  );
  const headcount = useAsync<InstrumentHeadcount | null>(
    useCallback(() => fetchInstrumentEmployees(symbol), [symbol]),
    [symbol],
  );

  return (
    <Section title="Company profile (SEC)">
      {state.loading ? (
        <SectionSkeleton rows={4} />
      ) : state.error !== null ? (
        <SectionError onRetry={state.refetch} />
      ) : state.data === null ? (
        <EmptyState
          title="No SEC profile yet"
          description="This instrument has no primary CIK mapping, or the daily SEC ingest has not yet seeded its entity row. Non-US tickers will not have one."
        />
      ) : (
        <Body profile={state.data} headcount={headcount.data} />
      )}
    </Section>
  );
}

function Body({
  profile,
  headcount,
}: {
  profile: InstrumentSecProfile;
  headcount: InstrumentHeadcount | null;
}) {
  return (
    <div className="space-y-3 text-sm">
      {profile.description !== null && profile.description.length > 0 && (
        <p className="leading-relaxed text-slate-700">{profile.description}</p>
      )}

      <dl className="grid grid-cols-[auto_1fr] gap-x-4 gap-y-1">
        {profile.sic_description !== null && (
          <>
            <dt className="text-slate-500">Industry (SIC)</dt>
            <dd>
              {profile.sic_description}
              {profile.sic !== null && (
                <span className="ml-1 text-xs text-slate-400">({profile.sic})</span>
              )}
            </dd>
          </>
        )}
        {profile.owner_org !== null && (
          <>
            <dt className="text-slate-500">SEC sector</dt>
            <dd>{profile.owner_org}</dd>
          </>
        )}
        {profile.exchanges.length > 0 && (
          <>
            <dt className="text-slate-500">Exchanges</dt>
            <dd>{profile.exchanges.join(", ")}</dd>
          </>
        )}
        {profile.category !== null && (
          <>
            <dt className="text-slate-500">Filer category</dt>
            <dd>{profile.category}</dd>
          </>
        )}
        {profile.fiscal_year_end !== null && (
          <>
            <dt className="text-slate-500">Fiscal year end</dt>
            <dd>{formatFiscalYearEnd(profile.fiscal_year_end)}</dd>
          </>
        )}
        {profile.state_of_incorporation_desc !== null && (
          <>
            <dt className="text-slate-500">Incorporated</dt>
            <dd>{profile.state_of_incorporation_desc}</dd>
          </>
        )}
        {headcount !== null && (
          <>
            <dt className="text-slate-500">Employees</dt>
            <dd>
              {headcount.employees.toLocaleString()}
              <span className="ml-1 text-xs text-slate-400">
                (as of {headcount.period_end_date})
              </span>
            </dd>
          </>
        )}
        <dt className="text-slate-500">CIK</dt>
        <dd className="font-mono text-xs text-slate-600">{profile.cik}</dd>
      </dl>

      {profile.website !== null && (
        <div>
          <a
            href={profile.website}
            className="text-xs text-sky-600 hover:underline"
            target="_blank"
            rel="noreferrer noopener"
          >
            {profile.website}
          </a>
        </div>
      )}

      {profile.former_names.length > 0 && (
        <div>
          <div className="mb-1 text-xs font-medium uppercase tracking-wider text-slate-500">
            Former names
          </div>
          <ul className="list-inside list-disc text-xs text-slate-600">
            {profile.former_names.map((fn, i) => (
              <li key={`${fn.name}-${i}`}>
                {fn.name}
                {fn.from_ !== null && fn.to !== null && (
                  <span className="ml-1 text-slate-400">
                    ({fn.from_.slice(0, 10)} → {fn.to.slice(0, 10)})
                  </span>
                )}
              </li>
            ))}
          </ul>
        </div>
      )}

    </div>
  );
}

/**
 * SEC publishes fiscalYearEnd as "MMDD" (e.g. "0930" = Sept 30).
 * Render as a month/day string; fall back to the raw value if parse
 * fails.
 */
function formatFiscalYearEnd(raw: string): string {
  if (raw.length !== 4 || !/^\d{4}$/.test(raw)) return raw;
  const month = Number.parseInt(raw.slice(0, 2), 10);
  const day = Number.parseInt(raw.slice(2, 4), 10);
  if (month < 1 || month > 12 || day < 1 || day > 31) return raw;
  const months = [
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
  ];
  return `${months[month - 1]} ${day}`;
}
