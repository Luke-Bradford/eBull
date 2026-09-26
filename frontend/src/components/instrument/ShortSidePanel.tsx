/**
 * ShortSidePanel — the instrument report's short side (#3390 slice 2).
 *
 * Two different FINRA measures, each defined on its own card, never blended:
 *   - Short interest: open short POSITIONS as of a settlement date, twice a
 *     month. Read by the scoring run into the IAR (`positioning.short_interest`)
 *     under its settlement + share-count freshness gates; a gated read shows
 *     its reason and dates, never a number.
 *   - Reg SHO short-sale volume: daily TRADE FLOW reported to FINRA's
 *     off-exchange facilities (`/instruments/{symbol}/short-volume`). Its
 *     definition and caveats come from the API so the copy has one source.
 */

import { type JSX, type ReactNode } from "react";

import { fetchInstrumentShortVolume } from "@/api/instruments";
import type { IarPositioningSignal, InstrumentShortVolume } from "@/api/types";
import { SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { Sparkline } from "@/components/instrument/Sparkline";
import { EmptyState } from "@/components/states/EmptyState";
import { formatDate, formatNumber, formatUnsignedPct } from "@/lib/format";
import { useAsync } from "@/lib/useAsync";

// Server default is 20; the route caps at 250.
const SHORT_VOLUME_DAYS = 20;

const SHORT_INTEREST_DEFINITION =
  "Open short positions reported by broker-dealers to FINRA as of the settlement date (FINRA Rule 4560), published twice a month. Shown as a share of shares outstanding: public float is not ingested.";
const DAYS_TO_COVER_DEFINITION =
  "FINRA's published figure: the short position divided by average daily share volume.";

export interface ShortSidePanelProps {
  readonly symbol: string;
  /** `null` when the scoring run produced no analytics for this instrument. */
  readonly shortInterest: IarPositioningSignal | null | undefined;
}

export function ShortSidePanel({
  symbol,
  shortInterest,
}: ShortSidePanelProps): JSX.Element {
  return (
    <div className="grid gap-3 sm:grid-cols-2">
      <ShortInterestCard sig={shortInterest} />
      <ShortVolumeCard symbol={symbol} />
    </div>
  );
}

function Card({
  title,
  children,
}: {
  title: string;
  children: ReactNode;
}): JSX.Element {
  return (
    <div className="rounded border border-slate-200 p-3 dark:border-slate-800">
      <div className="mb-1 text-[11px] font-medium uppercase tracking-wide text-slate-500">
        {title}
      </div>
      {children}
    </div>
  );
}

function Row({ label, value }: { label: string; value: string }): JSX.Element {
  return (
    <div className="flex items-baseline justify-between gap-2 text-xs">
      <span className="text-slate-500">{label}</span>
      <span className="font-medium tabular-nums text-slate-700 dark:text-slate-300">
        {value}
      </span>
    </div>
  );
}

function Note({ children }: { children: ReactNode }): JSX.Element {
  return <p className="mt-1.5 text-[10px] text-slate-400">{children}</p>;
}

/** Why a short-interest read carries no figure, in the reader's own terms. */
export function shortInterestUnavailableText(
  sig: IarPositioningSignal | null | undefined,
): string {
  if (sig === null || sig === undefined) {
    return "Not available: short interest is read by the scoring run, which has no analytics for this instrument yet.";
  }
  const maxAge = sig.max_age_days !== undefined ? ` (limit ${sig.max_age_days} days)` : "";
  switch (sig.reason) {
    case "stale_settlement":
      return sig.asof !== undefined
        ? `Withheld: the latest FINRA settlement on file is ${formatDate(sig.asof)}, too old to read as current${maxAge}.`
        : `Withheld: no FINRA settlement date on file${maxAge}.`;
    case "stale_share_count":
      return sig.shares_outstanding_asof !== undefined
        ? `Withheld: the share count it divides by was filed ${formatDate(sig.shares_outstanding_asof)}, too old to read as current${maxAge}.`
        : `Withheld: the share count it divides by has no filing date${maxAge}.`;
    default:
      return "Not available: FINRA reports no short interest for this instrument, or no share count is on file.";
  }
}

function ShortInterestCard({
  sig,
}: {
  sig: IarPositioningSignal | null | undefined;
}): JSX.Element {
  const available = sig?.short_pct !== undefined;
  return (
    <Card title="Short interest (FINRA, twice monthly)">
      {sig === null || sig === undefined || !available ? (
        <p className="text-xs text-slate-500">{shortInterestUnavailableText(sig)}</p>
      ) : (
        <>
          <div className="text-xl font-semibold tabular-nums text-slate-800 dark:text-slate-100">
            {formatUnsignedPct(sig.short_pct)}
          </div>
          <div className="text-[10px] text-slate-400">of shares outstanding</div>
          <div className="mt-1 space-y-0.5">
            <Row
              label="days to cover"
              value={sig.days_to_cover !== undefined ? formatNumber(sig.days_to_cover, 1) : "—"}
            />
            <Row label="settlement date" value={formatDate(sig.asof)} />
          </div>
        </>
      )}
      <Note>{SHORT_INTEREST_DEFINITION}</Note>
      <Note>Days to cover: {DAYS_TO_COVER_DEFINITION}</Note>
    </Card>
  );
}

function ShortVolumeCard({ symbol }: { symbol: string }): JSX.Element {
  // useAsync captures fn via a ref — fresh arrow per render is fine.
  const volume = useAsync<InstrumentShortVolume>(
    () => fetchInstrumentShortVolume(symbol, SHORT_VOLUME_DAYS),
    [symbol],
  );
  return (
    <Card title="Short-sale volume (Reg SHO, daily)">
      {volume.loading ? (
        <SectionSkeleton rows={3} />
      ) : volume.error !== null || volume.data === null ? (
        <SectionError onRetry={volume.refetch} />
      ) : (
        <ShortVolumeBody data={volume.data} />
      )}
    </Card>
  );
}

function ShortVolumeBody({ data }: { data: InstrumentShortVolume }): JSX.Element {
  const latest = data.days[0];
  // Sparkline wants oldest→newest; the API returns newest first.
  const shares = data.days
    .map((d) => (d.short_volume_share === null ? null : Number(d.short_volume_share)))
    .filter((v): v is number => v !== null)
    .reverse();
  return (
    <>
      {data.withheld_reason !== null ? (
        <p className="text-xs text-slate-500">{data.withheld_reason}</p>
      ) : latest === undefined ? (
        <EmptyState
          title="No short-sale volume held"
          description="FINRA's daily file covers securities traded in the US; none is held for this instrument. The FINRA Reg SHO daily refresh job fills it after each trading day."
        />
      ) : (
        <>
          <div className="text-xl font-semibold tabular-nums text-slate-800 dark:text-slate-100">
            {latest.short_volume_share === null
              ? "—"
              : formatUnsignedPct(Number(latest.short_volume_share))}
          </div>
          <div className="text-[10px] text-slate-400">
            of reported volume on {formatDate(latest.trade_date)}
          </div>
          <div className="mt-1 space-y-0.5">
            <Row
              label="short / total volume"
              value={`${formatNumber(Number(latest.short_volume), 0)} / ${formatNumber(Number(latest.total_volume), 0)}`}
            />
            <Row label="facilities" value={latest.facilities} />
          </div>
          {shares.length >= 2 && (
            <div className="mt-2 flex items-center gap-3">
              <Sparkline
                values={shares}
                width={160}
                height={32}
                formatValue={(v) => formatUnsignedPct(v)}
                className="text-blue-600 dark:text-blue-400"
              />
              <span className="text-[10px] text-slate-400">
                {shares.length} trade dates · daily share
              </span>
            </div>
          )}
        </>
      )}
      <Note>{data.definition}</Note>
      <ul className="mt-1 list-disc space-y-0.5 pl-4 text-[10px] text-slate-400">
        {data.caveats.map((c) => (
          <li key={c}>{c}</li>
        ))}
      </ul>
    </>
  );
}
