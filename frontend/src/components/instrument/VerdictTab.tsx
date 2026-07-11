/**
 * VerdictTab — the per-instrument Verdict surface (#1824, P3 of #1815).
 *
 * Renders the Instrument Analytical Record (IAR, `scores.analytics_json`,
 * shipped evidence-only by #1823) plus the score-history sparkline and the
 * existing thesis verdict/valuation. Read-only.
 *
 * Honesty rules (per #1815 §7 + the "no cohort-relative normalization" settled
 * decision):
 *   - The headline family score is the ABSOLUTE sub-score. The hybrid/percentile
 *     peer grade is shown beside it but labelled EVIDENCE-ONLY — it is weight 0
 *     in the live score and must never read as the headline.
 *   - Absent / null / suppressed signals render their reason ("not available",
 *     "n/a — financials", "evidence not yet computed"), never a neutral 0.00.
 *   - `scored_at` is surfaced so a stale verdict (instrument dropped from the
 *     latest run) is visibly stale, not silently old.
 */

import { type JSX } from "react";

import { fetchScoreHistory } from "@/api/scoreHistory";
import { fetchScoreVerdict } from "@/api/verdict";
import type {
  IarAltmanZ,
  IarPiotroski,
  IarPositioningSignal,
  ScoreHistoryResponse,
  ThesisDetail,
  VerdictResponse,
} from "@/api/types";
import { Section, SectionSkeleton } from "@/components/dashboard/Section";
import { Sparkline } from "@/components/instrument/Sparkline";
import { ThesisPane } from "@/components/instrument/ThesisPane";
import { EmptyState } from "@/components/states/EmptyState";
import { useAsync } from "@/lib/useAsync";

export interface VerdictTabProps {
  readonly instrumentId: number;
  readonly thesis: ThesisDetail | null;
  readonly thesisErrored?: boolean;
  /** Native header price + currency, forwarded to the ThesisPane value
   *  band (#2000). Optional — older callers degrade to "—". */
  readonly currentPrice?: string | null;
  readonly currency?: string | null;
}

const FAMILIES: { key: string; label: string; scoreKey: keyof FamilyScores }[] = [
  { key: "quality", label: "Quality", scoreKey: "quality_score" },
  { key: "value", label: "Value", scoreKey: "value_score" },
  { key: "turnaround", label: "Turnaround", scoreKey: "turnaround_score" },
  { key: "momentum", label: "Momentum", scoreKey: "momentum_score" },
  { key: "sentiment", label: "Sentiment", scoreKey: "sentiment_score" },
  { key: "confidence", label: "Confidence", scoreKey: "confidence_score" },
];

interface FamilyScores {
  quality_score: number | null;
  value_score: number | null;
  turnaround_score: number | null;
  momentum_score: number | null;
  sentiment_score: number | null;
  confidence_score: number | null;
}

function fmt2(v: number | null | undefined): string {
  return v === null || v === undefined ? "—" : v.toFixed(2);
}

function ErrorBox({ message }: { message: string }): JSX.Element {
  return (
    <div className="rounded border border-red-200 bg-red-50 p-3 text-sm text-red-700 dark:border-red-900/60 dark:bg-red-950/40 dark:text-red-300">
      {message}
    </div>
  );
}

/** Evidence-only tag — peer percentile is weight 0 in the live score. */
function EvidenceTag(): JSX.Element {
  return (
    <span
      className="rounded bg-slate-100 px-1 py-0.5 text-[9px] uppercase tracking-wide text-slate-500 dark:bg-slate-800 dark:text-slate-400"
      title="Evidence-only — weight 0 in the live score (no cohort-relative normalization)"
    >
      evidence-only
    </span>
  );
}

export function VerdictTab({
  instrumentId,
  thesis,
  thesisErrored = false,
  currentPrice = null,
  currency = null,
}: VerdictTabProps): JSX.Element {
  const verdict = useAsync<VerdictResponse>(
    () => fetchScoreVerdict(instrumentId),
    [instrumentId],
  );
  const history = useAsync<ScoreHistoryResponse>(
    () => fetchScoreHistory(instrumentId, 30),
    [instrumentId],
  );

  if (verdict.loading) return <SectionSkeleton rows={5} />;
  if (verdict.error !== null)
    return (
      <ErrorBox
        message={
          verdict.error instanceof Error
            ? verdict.error.message
            : "Failed to load the verdict."
        }
      />
    );

  const score = verdict.data?.score ?? null;
  if (score === null) {
    // Thesis still leads even when unscored (#2003) — a generated memo
    // must not vanish behind the "Not yet scored" empty state.
    return (
      <div className="space-y-4">
        <ThesisPane
          thesis={thesis}
          errored={thesisErrored}
          currentPrice={currentPrice}
          currency={currency}
        />
        <Section title="Verdict">
          <EmptyState
            title="Not yet scored"
            description="This instrument has no scoring run yet. The verdict appears once the deterministic engine has scored it."
          />
        </Section>
      </div>
    );
  }

  const iar = score.analytics_json;
  const peer = iar?.peer_grade;

  // Score-history sparkline values, oldest→newest (the API returns newest first).
  const historyValues = (history.data?.items ?? [])
    .map((it) => it.total_score)
    .filter((v): v is number => v !== null)
    .reverse();

  return (
    <div className="space-y-4">
      {/* 1. Thesis narrative + valuation leads the tab (#2003) — the memo
          is the page's payoff; the deterministic score block reads as
          supporting evidence below it. Renders nothing when no thesis
          exists (and no fetch error), so unthesised names degrade to the
          score-first layout. */}
      <ThesisPane
        thesis={thesis}
        errored={thesisErrored}
        currentPrice={currentPrice}
        currency={currency}
      />

      {/* 2. Score headline. The stance chip that used to sit here is
          gone (#2003 dedupe) — the ThesisPane directly above already
          leads with the StanceBadge. */}
      <Section title="Verdict">
        <div className="flex flex-wrap items-center gap-3">
          <div className="flex items-baseline gap-1">
            <span className="text-2xl font-semibold tabular-nums text-slate-800 dark:text-slate-100">
              {fmt2(score.total_score)}
            </span>
            <span className="text-xs text-slate-500">total score</span>
          </div>
          {score.rank !== null && (
            <span className="text-xs text-slate-500">
              rank #{score.rank}
              {score.rank_delta !== null && score.rank_delta !== 0 && (
                <span
                  className={
                    score.rank_delta < 0
                      ? "ml-1 text-emerald-600 dark:text-emerald-400"
                      : "ml-1 text-red-600 dark:text-red-400"
                  }
                >
                  {score.rank_delta < 0 ? "▲" : "▼"}
                  {Math.abs(score.rank_delta)}
                </span>
              )}
            </span>
          )}
          {score.completeness_tier !== null && (
            <span className="rounded bg-slate-100 px-1.5 py-0.5 text-[10px] text-slate-600 dark:bg-slate-800 dark:text-slate-400">
              completeness: {score.completeness_tier}
              {score.data_completeness !== null &&
                ` (${(score.data_completeness * 100).toFixed(0)}%)`}
            </span>
          )}
          <span className="ml-auto text-[10px] text-slate-400">
            as of {score.scored_at.slice(0, 10)} · {score.model_version}
          </span>
        </div>
        {score.explanation !== null && (
          <p className="mt-2 max-w-prose text-xs text-slate-600 dark:text-slate-400">
            {score.explanation}
          </p>
        )}
        {thesis !== null && new Date(thesis.created_at) > new Date(score.scored_at) && (
          <p className="mt-2 text-[11px] text-amber-700 dark:text-amber-300">
            The latest thesis (v{thesis.thesis_version}) postdates this score — thesis-fed
            families (value, confidence) refresh on the next ranking run.
          </p>
        )}
      </Section>

      {/* 3. Six graded families */}
      <Section title="Graded families">
        <table className="min-w-full text-sm">
          <thead>
            <tr className="text-left text-[11px] uppercase tracking-wide text-slate-500">
              <th className="py-1 pr-4 font-medium">Family</th>
              <th className="py-1 pr-4 font-medium">Score</th>
              <th className="py-1 pr-2 font-medium">
                Peer percentile <EvidenceTag />
              </th>
              <th className="py-1 font-medium">
                Hybrid <EvidenceTag />
              </th>
            </tr>
          </thead>
          <tbody>
            {FAMILIES.map((f) => {
              const absolute = score[f.scoreKey];
              const pf = peer?.families?.[f.key];
              return (
                <tr
                  key={f.key}
                  className="border-t border-slate-100 dark:border-slate-800"
                >
                  <td className="py-1 pr-4 text-slate-700 dark:text-slate-300">
                    {f.label}
                  </td>
                  <td className="py-1 pr-4 font-medium tabular-nums">
                    {fmt2(absolute)}
                  </td>
                  <td className="py-1 pr-2 tabular-nums text-slate-500">
                    {pf?.percentile !== null && pf?.percentile !== undefined
                      ? `${(pf.percentile * 100).toFixed(0)}%`
                      : "—"}
                  </td>
                  <td className="py-1 tabular-nums text-slate-500">
                    {fmt2(pf?.hybrid)}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
        {peer?.peer_key !== undefined ? (
          <p className="mt-1.5 text-[10px] text-slate-400">
            Peer cohort: {peer.peer_key} (n={peer.peer_n ?? "—"}, {peer.basis})
          </p>
        ) : (
          <p className="mt-1.5 text-[10px] text-slate-400">
            Peer percentile pending — populates on the next ranking run with run
            context.
          </p>
        )}
      </Section>

      {/* 4. Quality signals */}
      <Section title="Quality signals">
        {iar === null ? (
          <EvidencePending />
        ) : (
          <div className="grid gap-3 sm:grid-cols-2">
            <PiotroskiCard p={iar.piotroski} />
            <AltmanCard z={iar.altman_z} />
          </div>
        )}
      </Section>

      {/* 5. Positioning */}
      <Section title="Positioning">
        {iar === null ? (
          <EvidencePending />
        ) : (
          <div className="grid gap-3 sm:grid-cols-3">
            <PositioningCard
              label="Insider net 90d"
              sig={iar.positioning?.insider_net_90d}
            />
            <PositioningCard
              label="13F QoQ"
              sig={iar.positioning?.inst_13f_qoq}
            />
            <PositioningCard
              label="Short interest"
              sig={iar.positioning?.short_interest}
            />
          </div>
        )}
      </Section>

      {/* 6. Score history */}
      <Section title="Score history">
        {history.loading ? (
          <SectionSkeleton rows={1} />
        ) : history.error !== null ? (
          <ErrorBox message="Failed to load score history." />
        ) : historyValues.length < 2 ? (
          <p className="text-xs text-slate-500">
            Not enough runs yet to chart a trend.
          </p>
        ) : (
          <div className="flex items-center gap-3">
            <Sparkline
              values={historyValues}
              width={240}
              height={48}
              className="text-blue-600 dark:text-blue-400"
            />
            <span className="text-[10px] text-slate-400">
              {historyValues.length} runs · total score
            </span>
          </div>
        )}
      </Section>

    </div>
  );
}

function EvidencePending(): JSX.Element {
  return (
    <p className="text-xs text-slate-500">
      Evidence not yet computed — populates on the next scoring run.
    </p>
  );
}

function SignalRow({
  label,
  value,
}: {
  label: string;
  value: string;
}): JSX.Element {
  return (
    <div className="flex items-baseline justify-between gap-2 text-xs">
      <span className="text-slate-500">{label}</span>
      <span className="font-medium tabular-nums text-slate-700 dark:text-slate-300">
        {value}
      </span>
    </div>
  );
}

function PiotroskiCard({ p }: { p: IarPiotroski | undefined }): JSX.Element {
  return (
    <div className="rounded border border-slate-200 p-3 dark:border-slate-800">
      <div className="mb-1 text-[11px] font-medium uppercase tracking-wide text-slate-500">
        Piotroski F
      </div>
      {p === undefined ? (
        <span className="text-xs text-slate-400">—</span>
      ) : p.suppressed ? (
        <span className="text-xs text-slate-400">n/a — financials</span>
      ) : p.score === null || p.score === undefined ? (
        <span className="text-xs text-slate-400">
          unavailable{p.reason ? ` (${p.reason})` : ""}
        </span>
      ) : (
        <div className="flex items-baseline gap-2">
          <span className="text-xl font-semibold tabular-nums text-slate-800 dark:text-slate-100">
            {p.score}
            {p.components_available !== undefined && (
              <span className="text-sm text-slate-400">
                /{p.components_available}
              </span>
            )}
          </span>
          {p.band && <Band band={p.band} />}
        </div>
      )}
      {p?.components_available !== undefined &&
        p.components_available < 9 &&
        p.score !== null &&
        p.score !== undefined && (
          <p className="mt-1 text-[10px] text-slate-400">
            {p.components_available}/9 components evaluable (rest lack prior-year
            data)
          </p>
        )}
    </div>
  );
}

function AltmanCard({ z }: { z: IarAltmanZ | undefined }): JSX.Element {
  return (
    <div className="rounded border border-slate-200 p-3 dark:border-slate-800">
      <div className="mb-1 text-[11px] font-medium uppercase tracking-wide text-slate-500">
        Altman Z″
      </div>
      {z === undefined ? (
        <span className="text-xs text-slate-400">—</span>
      ) : z.suppressed ? (
        <span className="text-xs text-slate-400">n/a — financials</span>
      ) : z.z === null || z.z === undefined ? (
        <span className="text-xs text-slate-400">
          unavailable{z.reason ? ` (${z.reason})` : ""}
        </span>
      ) : (
        <div className="flex items-baseline gap-2">
          <span className="text-xl font-semibold tabular-nums text-slate-800 dark:text-slate-100">
            {z.z.toFixed(2)}
          </span>
          {z.band && <Band band={z.band} />}
        </div>
      )}
    </div>
  );
}

function Band({ band }: { band: string }): JSX.Element {
  const cls =
    band === "strong" || band === "safe"
      ? "bg-emerald-100 text-emerald-700 dark:bg-emerald-900/40 dark:text-emerald-300"
      : band === "weak" || band === "distress"
        ? "bg-red-100 text-red-700 dark:bg-red-900/40 dark:text-red-300"
        : "bg-amber-100 text-amber-700 dark:bg-amber-900/40 dark:text-amber-300";
  return (
    <span className={`rounded px-1.5 py-0.5 text-[10px] font-medium ${cls}`}>
      {band}
    </span>
  );
}

function PositioningCard({
  label,
  sig,
}: {
  label: string;
  sig: IarPositioningSignal | undefined;
}): JSX.Element {
  return (
    <div className="rounded border border-slate-200 p-3 dark:border-slate-800">
      <div className="mb-1 text-[11px] font-medium uppercase tracking-wide text-slate-500">
        {label}
      </div>
      {sig === undefined || sig.signal === null || sig.signal === undefined ? (
        <span className="text-xs text-slate-400">
          unavailable{sig?.reason ? ` (${sig.reason})` : ""}
        </span>
      ) : (
        <>
          <div className="text-xl font-semibold tabular-nums text-slate-800 dark:text-slate-100">
            {sig.signal.toFixed(2)}
          </div>
          <div className="mt-1 space-y-0.5">
            {sig.delta_shares_pct !== undefined && (
              <SignalRow
                label="Δ shares QoQ"
                value={`${(sig.delta_shares_pct * 100).toFixed(1)}%`}
              />
            )}
            {sig.short_pct !== undefined && (
              <SignalRow
                label="short %"
                value={`${(sig.short_pct * 100).toFixed(1)}%`}
              />
            )}
            {sig.days_to_cover !== undefined && (
              <SignalRow
                label="days to cover"
                value={sig.days_to_cover.toFixed(1)}
              />
            )}
            {sig.net_shares !== null && sig.net_shares !== undefined && (
              <SignalRow
                label="net shares"
                value={sig.net_shares.toLocaleString()}
              />
            )}
          </div>
          {sig.caveat && (
            <p className="mt-1 text-[10px] text-slate-400">{sig.caveat}</p>
          )}
        </>
      )}
    </div>
  );
}
