import { fetchEngineBookRisk } from "@/api/strategies";
import type { EngineBookRiskCheck, EngineBookRiskLatest, EngineBookRiskResponse } from "@/api/types";
import { SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { JobRow } from "@/components/strategies/AiTrialPanel";
import { Badge, type BadgeTone } from "@/components/ui/Badge";
import { formatDate } from "@/lib/format";
import { money, number, pctPoints, pctPointsUnsigned } from "@/lib/strategyFormat";
import { useAsync } from "@/lib/useAsync";

type CheckKind = "vol" | "stress" | "count";

/** Stored check keys in reading order (`app/services/engine_book_risk.py::compute_snapshot`). */
const CHECKS: { key: string; label: string; kind: CheckKind }[] = [
  { key: "forecast_vol_vs_target", label: "Forecast volatility vs target", kind: "vol" },
  { key: "stress_2020_vs_max_drawdown", label: "2020 crash stress vs max drawdown", kind: "stress" },
  { key: "stress_2022_vs_max_drawdown", label: "2022 bear stress vs max drawdown", kind: "stress" },
  { key: "positions_vs_max_concurrent", label: "Open trades vs cap", kind: "count" },
  { key: "stale_marks", label: "Stale marks", kind: "count" },
];

const HISTORY_NOTE: Record<string, string> = {
  insufficient_history: "Fewer than 60 shared daily returns, so volatility and beta are not stated yet.",
  degenerate: "SPY returns have zero variance over the window, so beta is undefined.",
  empty_book: "The engine holds nothing, so every exposure figure is zero.",
};

function checkValue(kind: CheckKind, value: string | null): string {
  if (value === null) return "—";
  if (kind === "vol") return pctPointsUnsigned(value);
  if (kind === "stress") return pctPoints(value);
  return value;
}

/** The drawdown limit is stored unsigned; the stress it is compared with is a signed loss. */
function checkLimit(kind: CheckKind, limit: string | null): string {
  if (limit === null) return "—";
  if (kind === "vol") return pctPointsUnsigned(limit);
  if (kind === "stress") return pctPoints(`-${limit}`);
  return limit;
}

function checkBadge(check: EngineBookRiskCheck): { text: string; tone: BadgeTone } {
  if (check.status === "no_limit") return { text: "no limit set", tone: "neutral" };
  if (check.status === "unknown") return { text: "not measured", tone: "neutral" };
  return check.flagged ? { text: "over", tone: "warn" } : { text: "within", tone: "ok" };
}

function CheckList({ checks }: { checks: Record<string, EngineBookRiskCheck> }) {
  const known = new Set(CHECKS.map((c) => c.key));
  const rows = [
    ...CHECKS.flatMap((c) => {
      const check = checks[c.key];
      return check === undefined ? [] : [{ ...c, check }];
    }),
    ...Object.entries(checks)
      .filter(([key]) => !known.has(key))
      .map(([key, check]) => ({ key, label: key, kind: "count" as CheckKind, check })),
  ];
  return (
    <ul className="mt-2 divide-y divide-slate-200 dark:divide-slate-800" aria-label="Mandate checks">
      {rows.map(({ key, label, kind, check }) => {
        const badge = checkBadge(check);
        return (
          <li key={key} className="flex flex-wrap items-center justify-between gap-2 py-1.5 text-sm">
            <span>{label}</span>
            <span className="flex items-center gap-2 tabular-nums text-slate-700 dark:text-slate-200">
              {checkValue(kind, check.value)} vs {checkLimit(kind, check.limit)}
              <Badge tone={badge.tone}>{badge.text}</Badge>
            </span>
          </li>
        );
      })}
    </ul>
  );
}

function Figures({ snap }: { snap: EngineBookRiskLatest }) {
  const capital = number(snap.capital_usd);
  const gross = number(snap.gross_usd);
  const invested = capital && gross !== null ? pctPointsUnsigned(String((gross / capital) * 100)) : "—";
  return (
    <dl className="mt-3 grid grid-cols-[max-content_1fr] gap-x-3 gap-y-1 text-sm tabular-nums">
      <dt className="text-slate-500">Invested</dt>
      <dd>
        {money(snap.gross_usd)} of {money(snap.capital_usd)} ({invested}) · {snap.instrument_count} instruments,{" "}
        {snap.position_count} positions
      </dd>
      <dt className="text-slate-500">Volatility</dt>
      <dd>
        {snap.hist_vol_pct === null
          ? "—"
          : `${pctPointsUnsigned(snap.hist_vol_pct)} historical · ${pctPointsUnsigned(snap.ewma_vol_pct)} forecast (EWMA)`}{" "}
        <span className="text-slate-500">({snap.vol_n_obs} daily returns)</span>
      </dd>
      <dt className="text-slate-500">Beta to SPY</dt>
      <dd>{snap.beta === null ? "—" : (number(snap.beta)?.toFixed(2) ?? "—")}</dd>
      <dt className="text-slate-500">Concentration</dt>
      <dd>
        {snap.largest_share_pct === null
          ? "—"
          : `largest ${pctPointsUnsigned(snap.largest_share_pct)} · top 5 ${pctPointsUnsigned(snap.top5_share_pct)} · HHI ${Math.round(number(snap.hhi) ?? 0)}`}
      </dd>
      <dt className="text-slate-500">Stress (approx.)</dt>
      <dd>
        2020 crash {pctPoints(snap.stress_2020_pct)} · 2022 bear {pctPoints(snap.stress_2022_pct)}
        {snap.beta_defaulted_count > 0
          ? ` · ${snap.beta_defaulted_count} instrument(s), ${pctPointsUnsigned(snap.beta_defaulted_weight_pct)} of capital, assumed beta 1`
          : ""}
      </dd>
    </dl>
  );
}

function Positions({ snap }: { snap: EngineBookRiskLatest }) {
  if (snap.positions.length === 0) return null;
  return (
    <details className="mt-3 text-sm">
      <summary className="cursor-pointer select-none text-xs text-slate-500">Positions measured</summary>
      <ul className="mt-1 divide-y divide-slate-200 dark:divide-slate-800" aria-label="Positions measured">
        {snap.positions.map((p) => (
          <li key={p.position_id} className="flex flex-wrap justify-between gap-2 py-1 tabular-nums">
            <span className="font-medium">{p.symbol ?? `#${p.instrument_id}`}</span>
            <span className="text-slate-700 dark:text-slate-200">
              {money(p.market_value_usd)} · {pctPointsUnsigned(p.weight_of_capital_pct)} of capital · mark{" "}
              {p.mark_date === "cost" ? "cost basis (no close)" : formatDate(p.mark_date)} · beta{" "}
              {p.beta === "defaulted" ? "1 (assumed)" : (number(p.beta)?.toFixed(2) ?? "—")}
            </span>
          </li>
        ))}
      </ul>
    </details>
  );
}

function Recent({ view }: { view: EngineBookRiskResponse }) {
  if (view.recent.length < 2) return null;
  return (
    <>
      <h3 className="mt-4 text-xs font-semibold uppercase text-slate-500">Recent sessions</h3>
      <ul className="mt-1 divide-y divide-slate-200 dark:divide-slate-800" aria-label="Recent risk snapshots">
        {view.recent.map((r) => (
          <li key={r.session_date} className="flex flex-wrap items-center gap-2 py-1 text-sm tabular-nums">
            <span className="w-28">{formatDate(r.session_date)}</span>
            <span className="text-slate-700 dark:text-slate-200">
              {money(r.gross_usd)} · vol {r.ewma_vol_pct === null ? "—" : pctPointsUnsigned(r.ewma_vol_pct)} · 2020
              stress {pctPoints(r.stress_2020_pct)}
            </span>
            {r.flagged.length > 0 ? <Badge tone="warn">{r.flagged.length} over</Badge> : null}
          </li>
        ))}
      </ul>
    </>
  );
}

function Body({ view }: { view: EngineBookRiskResponse }) {
  const snap = view.latest;
  return (
    <>
      {snap === null ? (
        <p className="mt-2 text-sm text-slate-600 dark:text-slate-300">
          No snapshot yet. The daily job measures the engine book after each completed NYSE session; a refusal
          reason, if any, shows on the job line below.
        </p>
      ) : (
        <>
          <p className="mt-1 text-xs text-slate-500">
            Session {formatDate(snap.session_date)} · measurement only — nothing here refuses or resizes a trade.
          </p>
          {HISTORY_NOTE[snap.history_status] ? (
            <p className="mt-1 text-sm text-slate-600 dark:text-slate-300">{HISTORY_NOTE[snap.history_status]}</p>
          ) : null}
          <CheckList checks={snap.checks} />
          <Figures snap={snap} />
          <Positions snap={snap} />
          <Recent view={view} />
        </>
      )}
      <ul className="mt-3 divide-y divide-slate-200 dark:divide-slate-800" aria-label="Risk job">
        <JobRow label="Daily risk snapshot" fire={view.job} />
      </ul>
    </>
  );
}

/**
 * #3543: the engine book's daily risk vs its mandate — volatility, beta,
 * concentration, beta-mapped stress and stale marks. Read-only; the snapshot
 * job owns the measurement.
 */
export function EngineBookRiskPanel() {
  const risk = useAsync(fetchEngineBookRisk, []);
  const checks = risk.data?.latest ? Object.values(risk.data.latest.checks) : null;
  const flagged = checks?.filter((c) => c.flagged).length ?? 0;
  return (
    <section
      aria-labelledby="engine-book-risk"
      className="border border-slate-200 bg-white px-5 py-4 dark:border-slate-800 dark:bg-slate-900"
    >
      <h2 id="engine-book-risk" className="flex items-center gap-2 text-sm font-semibold">
        Book risk
        {checks === null ? null : flagged > 0 ? (
          <Badge tone="warn">{flagged} over limit</Badge>
        ) : checks.every((c) => c.status === "evaluated") ? (
          <Badge tone="ok">Within mandate</Badge>
        ) : (
          // An unset limit or an unmeasured figure is not compliance.
          <Badge tone="neutral">Partly checked</Badge>
        )}
      </h2>
      {risk.loading ? <SectionSkeleton rows={3} /> : null}
      {risk.error ? <SectionError onRetry={risk.refetch} /> : null}
      {risk.data ? <Body view={risk.data} /> : null}
    </section>
  );
}
