/**
 * FairValueBandPanel — the instrument report's valuation band (#3390 slice 4).
 *
 * Renders `/instruments/{symbol}/fair-value-band`: the deterministic peer band
 * (#2009, `fair_value_band_current` at the live method version). It is
 * comparables evidence, labelled as not validated fair value — the label and
 * definition come from the API so the copy has one source. An absent band
 * shows its stored reason; a band older than its own price-freshness rule is
 * flagged, never silently shown as current.
 */

import { type JSX } from "react";

import { fetchInstrumentFairValueBand } from "@/api/instruments";
import type { FairValueBandLeg, InstrumentFairValueBand } from "@/api/types";
import { SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import {
  ReportCard,
  ReportNote,
  ReportRow,
} from "@/components/instrument/ReportCard";
import { Badge } from "@/components/ui/Badge";
import { formatDate, formatMoney, formatNumber } from "@/lib/format";
import { parseDecimal } from "@/lib/riskView";
import { useAsync } from "@/lib/useAsync";

const MULTIPLE_LABEL: Record<string, string> = {
  pe: "P/E",
  ps: "P/S",
  pb: "P/B",
  ev_ebitda: "EV/EBITDA",
};

// select_multiples returns no multiple for an FPI ADR/ADS target whatever its
// fundamentals: the band's price bases are not ADS-adjusted (#1939/#2117).
const ADR_BASES = new Set(["fpi_adr_unavailable", "fpi_adr_ratio"]);

/** Plain-English copy for a stored absent-band reason (fair_value_band.py). */
export function bandReasonText(reason: string, targetBasis: string | null): string {
  switch (reason) {
    case "no_band":
      return "No band has been computed for this instrument.";
    case "no_multiple":
      if (targetBasis !== null && ADR_BASES.has(targetBasis)) {
        return "Depositary shares (ADR/ADS): the band's price basis is not adjusted for the ADS ratio, so no multiple is applied.";
      }
      return "No multiple applies: the trailing earnings, revenue or book value it needs are missing or not positive.";
    case "thin_cohort":
      return "Too few comparable peers or own-history points to build a band.";
    case "stale_price":
      return "The price the band would anchor on was too old when it was computed.";
    case "currency_mismatch":
      return "The company reports in a different currency from the one it trades in, so no band is built.";
    case "multiclass_unavailable":
      return "Dual-class shares with no resolvable per-class share count.";
    case "earnings_nonrepresentative":
      return "P/E was the only usable multiple, and recent net income is not representative (never profitable, depressed or spiked).";
    default:
      return `No band (${reason}).`;
  }
}

/** Why a leg did not enter the blend, or null when it did. */
export function legExclusionText(leg: FairValueBandLeg): string | null {
  if (leg.contributed) return null;
  if (leg.earnings_nonrep !== null) return "earnings not representative";
  if (leg.dropped_nonpositive) return "net debt exceeds implied value";
  return "no comparator";
}

export function FairValueBandPanel({ symbol }: { readonly symbol: string }): JSX.Element {
  // useAsync captures fn via a ref — fresh arrow per render is fine.
  const band = useAsync<InstrumentFairValueBand>(
    () => fetchInstrumentFairValueBand(symbol),
    [symbol],
  );
  return (
    <ReportCard title="Valuation band (peer comparables)">
      {band.loading ? (
        <SectionSkeleton rows={4} />
      ) : band.error !== null || band.data === null ? (
        <SectionError onRetry={band.refetch} />
      ) : (
        <BandBody data={band.data} />
      )}
    </ReportCard>
  );
}

/** Per-share figure in the instrument's currency; bare number when unknown. */
function perShare(v: number | null, currency: string | null): string {
  return currency === null ? formatNumber(v, 2) : formatMoney(v, currency);
}

function BandBody({ data }: { data: InstrumentFairValueBand }): JSX.Element {
  const money = (v: string | null) => perShare(parseDecimal(v), data.currency);
  return (
    <>
      <div className="mb-1">
        <Badge tone="neutral" title={data.definition}>
          {data.label}
        </Badge>
      </div>
      {!data.available ? (
        <p className="text-xs text-slate-500">{bandReasonText(data.reason, data.target_basis)}</p>
      ) : (
        <>
          <div className="space-y-0.5">
            <ReportRow label="bear" value={money(data.bear_value)} />
            <ReportRow label="base" value={money(data.base_value)} />
            <ReportRow label="bull" value={money(data.bull_value)} />
            <ReportRow label="quality" value={data.quality_status ?? "—"} />
          </div>
          {data.stale && (
            <p className="mt-1 text-[10px] text-amber-700 dark:text-amber-400">
              Computed for {formatDate(data.as_of_date)}, older than the band&apos;s own{" "}
              {data.stale_after_days}-day price-freshness rule — read it as history.
            </p>
          )}
          <table className="mt-2 w-full text-[11px]">
            <thead>
              <tr className="text-left text-slate-400">
                <th className="font-normal">multiple</th>
                <th className="font-normal">per-share base</th>
                <th className="font-normal">peers</th>
                <th className="font-normal">own points</th>
              </tr>
            </thead>
            <tbody className="tabular-nums text-slate-600 dark:text-slate-300">
              {data.legs.map((leg) => {
                const excluded = legExclusionText(leg);
                return (
                  <tr key={leg.multiple}>
                    <td>{MULTIPLE_LABEL[leg.multiple] ?? leg.multiple}</td>
                    <td>
                      {excluded === null
                        ? perShare(leg.base_value, data.currency)
                        : `not used: ${excluded}`}
                    </td>
                    <td>
                      {leg.cohort_n ?? "—"}
                      {leg.cohort_screened === false ? " (unscreened)" : ""}
                    </td>
                    <td>{leg.own_points ?? "—"}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
          {data.cross_leg_base_ratio !== null && (
            <ReportNote>
              Highest ÷ lowest contributing base: {formatNumber(data.cross_leg_base_ratio, 2)}×
            </ReportNote>
          )}
        </>
      )}
      <ReportNote>
        As of {formatDate(data.as_of_date)} · TTM to {formatDate(data.ttm_end)} · price{" "}
        {formatDate(data.price_as_of)} · {data.method_version}
      </ReportNote>
      <ReportNote>{data.definition}</ReportNote>
    </>
  );
}
