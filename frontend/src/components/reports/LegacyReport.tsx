/**
 * LegacyReport — corrected v1 rendering (spec §3.2).
 *
 * Renders snapshots WITHOUT `schema_version` (pre-#1596). Retires the
 * three §2 phantom-key bugs:
 *   1. P&L reads `realized_pnl` / `unrealized_pnl` / `total_pnl` (the
 *      keys the builder actually wrote) — not `realised_pnl` /
 *      `portfolio_value` / `cash` (never existed).
 *   2. Performers read `unrealized_pnl` (currency), best/worst trades
 *      read `gross_return_pct` (FRACTION-basis) — not `return_pct`.
 *   3. Thesis accuracy is the per-trade LIST the builder emitted — no
 *      aggregate `buy_hit_rate_pct` / `avoid_hit_rate_pct` keys exist.
 * Dead sections dropped: `upcoming_earnings` (retired #539, always []).
 *
 * EXIT CONDITION: this branch renders OLD snapshots only. No new
 * features land here, ever — pinned by the v1 fixture tests
 * (LegacyReport.test.tsx). v1 snapshots predate display-currency
 * stamping; values render in USD as the v1 page always did.
 */
import { Link } from "react-router-dom";

import type { ReportSnapshot } from "@/api/reports";
import { dec } from "@/components/reports/snapshotMath";
import { formatDate, formatMoney, formatPct } from "@/lib/format";

interface LegacyContributorRow {
  instrument_id: number;
  symbol: string;
  pnl_delta: string | null;
  pnl_pct: string | null;
}

function asRecord(v: unknown): Record<string, unknown> | null {
  return typeof v === "object" && v !== null && !Array.isArray(v) ? (v as Record<string, unknown>) : null;
}

function asArray(v: unknown): unknown[] {
  return Array.isArray(v) ? v : [];
}

function str(v: unknown): string | null {
  return typeof v === "string" ? v : typeof v === "number" ? String(v) : null;
}

function Eyebrow({ children }: { children: React.ReactNode }) {
  return (
    <h3 className="mb-1 text-xs font-semibold uppercase tracking-wide text-slate-500">{children}</h3>
  );
}

export function LegacyReport({ report }: { report: ReportSnapshot }) {
  const json = report.snapshot_json;
  const pnl = asRecord(json["pnl"]);
  const periodContribution = asRecord(json["period_contribution"]);
  const contributors = asArray(periodContribution?.["contributors"]) as LegacyContributorRow[];
  const drags = asArray(periodContribution?.["drags"]) as LegacyContributorRow[];
  const topPerformers = asArray(json["top_performers"]).map(asRecord);
  const bottomPerformers = asArray(json["bottom_performers"]).map(asRecord);
  const bestTrade = asRecord(json["best_trade"]);
  const worstTrade = asRecord(json["worst_trade"]);
  const thesisRows = asArray(json["thesis_accuracy"]).map(asRecord);
  const winRate = json["win_rate"];
  const avgHoldingDays = json["avg_holding_days"];
  const isMonthly = report.report_type === "monthly";

  return (
    <div className="space-y-4">
      <p className="text-xs text-slate-500">
        Legacy snapshot (v1 schema) · {formatDate(report.period_start)} –{" "}
        {formatDate(report.period_end)} · generated {formatDate(report.computed_at)}
      </p>

      {pnl !== null ? (
        <div>
          <Eyebrow>P&amp;L (since inception)</Eyebrow>
          <dl className="grid grid-cols-2 gap-y-1 text-sm md:grid-cols-3">
            <dt className="text-slate-500">Net realised gains</dt>
            <dd className="tabular-nums">{formatMoney(dec(str(pnl["realized_pnl"])), "USD")}</dd>
            <dt className="text-slate-500">Net unrealised appreciation</dt>
            <dd className="tabular-nums">{formatMoney(dec(str(pnl["unrealized_pnl"])), "USD")}</dd>
            <dt className="text-slate-500">Total P&amp;L</dt>
            <dd className="tabular-nums">{formatMoney(dec(str(pnl["total_pnl"])), "USD")}</dd>
          </dl>
        </div>
      ) : null}

      {(contributors.length > 0 || drags.length > 0) && (
        <div>
          <Eyebrow>Period contribution (vs prior snapshot)</Eyebrow>
          <div className="grid gap-4 md:grid-cols-2">
            <LegacyContributorList title="Top contributors" rows={contributors} tone="positive" />
            <LegacyContributorList title="Top detractors" rows={drags} tone="negative" />
          </div>
        </div>
      )}

      {topPerformers.length > 0 && (
        <LegacyPerformerList title="Top performers (unrealised P&L)" rows={topPerformers} />
      )}
      {bottomPerformers.length > 0 && (
        <LegacyPerformerList title="Bottom performers (unrealised P&L)" rows={bottomPerformers} />
      )}

      {isMonthly && (winRate !== undefined || avgHoldingDays !== undefined) && (
        <div>
          <Eyebrow>Closed-trade review</Eyebrow>
          <dl className="grid grid-cols-2 gap-y-1 text-sm md:grid-cols-4">
            <dt className="text-slate-500">Win rate</dt>
            {/* v1 win_rate is PERCENT-basis (number or "66.67" string). */}
            <dd className="tabular-nums">
              {dec(str(winRate)) !== null ? `${dec(str(winRate))?.toFixed(2)}%` : "—"}
            </dd>
            <dt className="text-slate-500">Average holding period</dt>
            <dd className="tabular-nums">
              {dec(str(avgHoldingDays)) !== null ? `${Math.round(dec(str(avgHoldingDays)) ?? 0)} days` : "—"}
            </dd>
          </dl>
        </div>
      )}

      {isMonthly && (bestTrade !== null || worstTrade !== null) && (
        <div>
          <Eyebrow>Best &amp; worst closed trades (gross return)</Eyebrow>
          {bestTrade !== null && (
            <p className="text-sm">
              Best: <span className="font-medium">{str(bestTrade["symbol"]) ?? "?"}</span>{" "}
              <span className="text-emerald-600">
                {formatPct(dec(str(bestTrade["gross_return_pct"])))}
              </span>
            </p>
          )}
          {worstTrade !== null && (
            <p className="text-sm">
              Worst: <span className="font-medium">{str(worstTrade["symbol"]) ?? "?"}</span>{" "}
              <span className="text-red-600">
                {formatPct(dec(str(worstTrade["gross_return_pct"])))}
              </span>
            </p>
          )}
        </div>
      )}

      {isMonthly && thesisRows.length > 0 && (
        <div>
          <Eyebrow>Thesis outcomes ({thesisRows.length} closed trades)</Eyebrow>
          <ul className="space-y-1 text-sm">
            {thesisRows.map((row, i) =>
              row !== null ? (
                <li key={i} className="flex items-baseline gap-3">
                  <span className="font-medium">{str(row["symbol"]) ?? "?"}</span>
                  <span className="text-xs uppercase text-slate-500">{str(row["stance"]) ?? ""}</span>
                  <span className="tabular-nums text-slate-600 dark:text-slate-300">
                    {formatPct(dec(str(row["gross_return_pct"])))}
                  </span>
                  <span className="text-xs text-slate-500">
                    {row["target_hit"] === true ? "hit" : row["target_hit"] === false ? "miss" : ""}
                  </span>
                </li>
              ) : null,
            )}
          </ul>
        </div>
      )}

      <details className="text-xs">
        <summary className="cursor-pointer text-slate-500">Snapshot data (raw JSON)</summary>
        <pre className="mt-2 overflow-x-auto rounded bg-slate-50 p-2 dark:bg-slate-900/40">
          {JSON.stringify(json, null, 2)}
        </pre>
      </details>
    </div>
  );
}

function LegacyContributorList({
  title,
  rows,
  tone,
}: {
  title: string;
  rows: LegacyContributorRow[];
  tone: "positive" | "negative";
}) {
  const toneClass = tone === "positive" ? "text-emerald-600" : "text-red-600";
  return (
    <div>
      <div
        className={`mb-1 text-[11px] font-semibold uppercase ${tone === "positive" ? "text-emerald-700" : "text-red-700"}`}
      >
        {title}
      </div>
      {rows.length === 0 ? (
        <div className="text-xs text-slate-500">—</div>
      ) : (
        <ul className="space-y-1 text-sm">
          {rows.map((r) => (
            <li key={r.instrument_id} className="flex items-baseline justify-between gap-3">
              <Link
                to={`/instrument/${encodeURIComponent(r.symbol)}`}
                className="font-medium text-blue-600 hover:underline dark:text-blue-400"
              >
                {r.symbol}
              </Link>
              <span className={`tabular-nums ${toneClass}`}>
                {formatMoney(dec(r.pnl_delta), "USD")}
                {r.pnl_pct !== null ? (
                  <span className="ml-2 text-xs">({formatPct(dec(r.pnl_pct))})</span>
                ) : null}
              </span>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

function LegacyPerformerList({
  title,
  rows,
}: {
  title: string;
  rows: Array<Record<string, unknown> | null>;
}) {
  return (
    <div>
      <Eyebrow>{title}</Eyebrow>
      <ul className="space-y-1 text-sm">
        {rows.map((p, idx) =>
          p !== null ? (
            <li key={idx}>
              <span className="font-medium">{str(p["symbol"]) ?? "?"}</span>{" "}
              <span className="tabular-nums text-slate-500">
                {formatMoney(dec(str(p["unrealized_pnl"])), "USD")}
              </span>
            </li>
          ) : null,
        )}
      </ul>
    </div>
  );
}
