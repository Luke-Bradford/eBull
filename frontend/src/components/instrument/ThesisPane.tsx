import { Pane } from "@/components/instrument/Pane";
import { CriticVerdictBadge } from "@/components/theses/CriticVerdictBadge";
import { MemoMarkdown } from "@/components/theses/MemoMarkdown";
import { StanceBadge } from "@/components/theses/StanceBadge";
import { EmptyState } from "@/components/states/EmptyState";
import type { ThesisDetail } from "@/api/types";

export interface ThesisPaneProps {
  readonly thesis: ThesisDetail | null;
  readonly errored: boolean;
  /** Native listing price from the instrument header (#1906 primary) —
   *  anchors the value band so a zone far from the market reads as
   *  obviously off, not merely confusing (#2000). */
  readonly currentPrice?: string | null;
  readonly currency?: string | null;
}

export function ThesisPane({
  thesis,
  errored,
  currentPrice = null,
  currency = null,
}: ThesisPaneProps): JSX.Element | null {
  if (thesis === null && !errored) return null;

  return (
    <Pane title="Thesis">
      {errored ? (
        <EmptyState
          title="Thesis temporarily unavailable"
          description="Failed to fetch the latest thesis. Retry via the Generate thesis button in the strip above."
        />
      ) : (
        <ThesisBody
          thesis={thesis as ThesisDetail}
          currentPrice={currentPrice}
          currency={currency}
        />
      )}
    </Pane>
  );
}

/** Safe string extraction from the open critic_json payload. */
function criticString(critic: Record<string, unknown>, key: string): string | null {
  const v = critic[key];
  return typeof v === "string" && v.length > 0 ? v : null;
}

/** Safe string-array extraction from the open critic_json payload. */
function criticList(critic: Record<string, unknown>, key: string): string[] {
  const v = critic[key];
  if (!Array.isArray(v)) return [];
  return v.filter((x): x is string => typeof x === "string");
}

function fmt(value: number | null, currency: string | null): string {
  if (value === null) return "—";
  const num = value.toFixed(2);
  return currency !== null && currency !== "" ? `${num} ${currency}` : num;
}

function formatDate(iso: string): string {
  const d = new Date(iso);
  // en-GB pinned — matches the app money/date convention (formatBigMoney,
  // Calendar) and keeps tests locale-independent.
  return Number.isNaN(d.getTime())
    ? iso
    : d.toLocaleDateString("en-GB", { day: "numeric", month: "short", year: "numeric" });
}

interface BodyProps {
  readonly thesis: ThesisDetail;
  readonly currentPrice: string | null;
  readonly currency: string | null;
}

function ThesisBody({ thesis, currentPrice, currency }: BodyProps): JSX.Element {
  const breaks = thesis.break_conditions_json ?? [];
  const critic = thesis.critic_json;
  const criticSummary = critic ? criticString(critic, "summary") : null;
  const criticRisks = critic ? criticList(critic, "key_risks") : [];
  const hasBuyZone = thesis.buy_zone_low !== null || thesis.buy_zone_high !== null;
  const hasBand =
    thesis.base_value !== null ||
    thesis.bull_value !== null ||
    thesis.bear_value !== null ||
    hasBuyZone;

  const price = currentPrice !== null && currentPrice !== "" ? Number(currentPrice) : null;
  const priceValid = price !== null && Number.isFinite(price) && price > 0;
  const upsideToBase =
    priceValid && thesis.base_value !== null
      ? ((thesis.base_value - (price as number)) / (price as number)) * 100
      : null;
  const outsideZone =
    priceValid &&
    thesis.stance === "buy" &&
    thesis.buy_zone_low !== null &&
    thesis.buy_zone_high !== null &&
    ((price as number) < thesis.buy_zone_low || (price as number) > thesis.buy_zone_high);

  // Explicit blind-list: prompt "v1" and unstamped pre-#1919 rows priced
  // targets with no current price in context (#1987) — flag those, and only
  // those. Future prompt versions (v3+) inherit the anchor; an allowlist of
  // "anchored" versions would mis-flag them (review WARNING, PR #2001).
  const blindPricing =
    thesis.prompt_version === null ||
    thesis.prompt_version === undefined ||
    thesis.prompt_version === "v1";

  return (
    <div className="space-y-3 text-sm">
      <div className="flex flex-wrap items-center gap-x-2 gap-y-1">
        <StanceBadge stance={thesis.stance} />
        <span className="text-xs capitalize text-slate-600 dark:text-slate-400">
          {thesis.thesis_type}
        </span>
        {thesis.confidence_score !== null && (
          <span className="text-xs tabular-nums text-slate-500">
            conf {(thesis.confidence_score * 100).toFixed(0)}%
          </span>
        )}
        <span
          className="ml-auto text-[10px] text-slate-400 dark:text-slate-500"
          title={
            thesis.provider !== null && thesis.provider !== undefined
              ? `provider: ${thesis.provider}`
              : undefined
          }
        >
          v{thesis.thesis_version} · {formatDate(thesis.created_at)}
          {thesis.model !== null && thesis.model !== undefined ? ` · ${thesis.model}` : ""}
          {thesis.prompt_version !== null && thesis.prompt_version !== undefined
            ? ` · prompt ${thesis.prompt_version}`
            : ""}
        </span>
        {blindPricing && (
          <span
            className="rounded border border-amber-300 dark:border-amber-700 bg-amber-50 dark:bg-amber-950/40 px-1.5 py-0.5 text-[10px] font-medium text-amber-700 dark:text-amber-300"
            title="Generated before the price anchor (#1987): targets were written without the current market price in context. Regenerate for anchored numbers."
          >
            pre-anchor memo
          </span>
        )}
      </div>

      {hasBand && (
        <div className="rounded bg-slate-50 dark:bg-slate-900/40 p-3">
          <dl className="grid grid-cols-2 gap-2 text-xs sm:grid-cols-5">
            <div>
              <dt className="text-slate-500">Bear</dt>
              <dd className="font-medium tabular-nums">{fmt(thesis.bear_value, currency)}</dd>
            </div>
            <div>
              <dt className="text-slate-500">Base</dt>
              <dd className="font-medium tabular-nums">
                {fmt(thesis.base_value, currency)}
                {upsideToBase !== null && (
                  <span
                    className={`ml-1 ${upsideToBase >= 0 ? "text-emerald-600 dark:text-emerald-400" : "text-red-600 dark:text-red-400"}`}
                  >
                    {upsideToBase >= 0 ? "+" : ""}
                    {upsideToBase.toFixed(1)}%
                  </span>
                )}
              </dd>
            </div>
            <div>
              <dt className="text-slate-500">Bull</dt>
              <dd className="font-medium tabular-nums">{fmt(thesis.bull_value, currency)}</dd>
            </div>
            <div>
              <dt className="text-slate-500">Buy zone</dt>
              <dd className="font-medium tabular-nums">
                {hasBuyZone
                  ? `${thesis.buy_zone_low !== null ? thesis.buy_zone_low.toFixed(2) : "—"} – ${
                      thesis.buy_zone_high !== null ? thesis.buy_zone_high.toFixed(2) : "—"
                    }`
                  : "—"}
              </dd>
            </div>
            <div>
              <dt className="text-slate-500">Price now</dt>
              <dd className="font-medium tabular-nums">
                {priceValid ? fmt(price as number, currency) : "—"}
              </dd>
            </div>
          </dl>
          {outsideZone && (
            <p className="mt-2 text-[11px] text-amber-700 dark:text-amber-300">
              Current price is outside the buy zone — entry conditions not met at market.
            </p>
          )}
        </div>
      )}

      <MemoMarkdown memo={thesis.memo_markdown} />

      {breaks.length > 0 && (
        <div>
          <div className="mb-1 text-xs font-medium uppercase tracking-wider text-slate-500">
            Break conditions
          </div>
          <ul className="list-inside list-disc space-y-0.5 text-xs text-slate-600 dark:text-slate-400">
            {breaks.map((b, i) => (
              <li key={i}>{b}</li>
            ))}
          </ul>
        </div>
      )}
      {critic !== null && critic !== undefined && (
        <div className="rounded border border-slate-200 dark:border-slate-800 p-3">
          <div className="mb-1 flex items-center gap-2">
            <span className="text-xs font-medium uppercase tracking-wider text-slate-500">
              Critic
            </span>
            <CriticVerdictBadge verdict={criticString(critic, "verdict")} />
          </div>
          {criticSummary !== null && (
            <p className="max-w-prose text-xs text-slate-600 dark:text-slate-300">
              {criticSummary}
            </p>
          )}
          {criticRisks.length > 0 && (
            <ul className="mt-1 list-inside list-disc space-y-0.5 text-xs text-slate-600 dark:text-slate-400">
              {criticRisks.map((r, i) => (
                <li key={i}>{r}</li>
              ))}
            </ul>
          )}
        </div>
      )}
    </div>
  );
}
