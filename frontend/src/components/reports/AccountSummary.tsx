/**
 * §4.1 Account summary — stat tiles over the v2 cover + the value
 * bridge + YTD / since-inception rows. All figures from the snapshot's
 * own `cover` key; nothing recomputed at view time.
 */
import { StatTile } from "@/components/dashboard/StatTile";
import { Fn, type NoteIndex } from "@/components/reports/StatementChrome";
import { dec } from "@/components/reports/snapshotMath";
import { formatMoney, formatPct } from "@/lib/format";
import type { CoverV2 } from "@/api/reportSnapshot";

function money(raw: string | null, currency: string): string {
  return formatMoney(dec(raw), currency);
}

function pct(raw: string | null): string {
  return formatPct(dec(raw));
}

function toneOf(raw: string | null): "positive" | "negative" | undefined {
  const n = dec(raw);
  if (n === null) return undefined;
  return n >= 0 ? "positive" : "negative";
}

const BRIDGE_ROWS: ReadonlyArray<readonly [keyof CoverV2["bridge"], string]> = [
  ["opening_value", "Opening value"],
  ["net_external_flows", "Net external flows"],
  ["realized_delta", "Net realised gains"],
  ["unrealized_delta", "Change in net unrealised appreciation"],
  ["broker_adjustments_residual", "Broker adjustments (unitemised)"],
  ["closing_value", "Closing value"],
];

export function AccountSummary({ cover, marker }: { cover: CoverV2; marker: NoteIndex }) {
  const ccy = cover.display_currency;
  const excess = dec(cover.excess_return);
  return (
    <div className="space-y-4">
      <div className="grid grid-cols-1 gap-x-6 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-6">
        <StatTile label="Closing value" value={money(cover.closing_value, ccy)} />
        <StatTile
          label="Period return"
          value={pct(cover.period_return)}
          tone={toneOf(cover.period_return)}
          hint={
            <span>
              flow-adjusted
              <Fn n={marker.dietz} />
            </span>
          }
        />
        <StatTile
          label="Benchmark return"
          value={pct(cover.benchmark_return)}
          hint={
            excess !== null ? (
              <span>
                excess {formatPct(excess)}
                <Fn n={marker.benchmark} />
              </span>
            ) : (
              <span>
                S&amp;P 500 (price index)
                <Fn n={marker.benchmark} />
              </span>
            )
          }
        />
        <StatTile
          label="Net realised gains (period)"
          value={money(cover.realized_delta, ccy)}
          tone={toneOf(cover.realized_delta)}
        />
        <StatTile
          label="Change in net unrealised appreciation (period)"
          value={money(cover.unrealized_delta, ccy)}
          tone={toneOf(cover.unrealized_delta)}
        />
        <StatTile label="Cash" value={money(cover.cash, ccy)} />
      </div>

      <div>
        <h3 className="mb-1 text-xs font-semibold uppercase tracking-wide text-slate-500">
          Value bridge
        </h3>
        <dl className="max-w-md text-sm">
          {BRIDGE_ROWS.map(([key, label]) => {
            const isClosing = key === "closing_value";
            return (
              <div
                key={key}
                className={`flex items-baseline justify-between gap-4 py-0.5 ${
                  isClosing
                    ? "border-t border-slate-200 font-medium dark:border-slate-800"
                    : ""
                }`}
              >
                <dt className="text-slate-500">{label}</dt>
                <dd className="tabular-nums">{money(cover.bridge[key], ccy)}</dd>
              </div>
            );
          })}
        </dl>
      </div>

      <dl className="max-w-md space-y-0.5 text-sm">
        <div className="flex items-baseline justify-between gap-4">
          <dt className="text-slate-500">YTD return (portfolio vs benchmark)</dt>
          <dd className="tabular-nums">
            {pct(cover.ytd_return)} vs {pct(cover.benchmark_ytd_return)}
          </dd>
        </div>
        <div className="flex items-baseline justify-between gap-4">
          <dt className="text-slate-500">Since-inception return (portfolio vs benchmark)</dt>
          <dd className="tabular-nums">
            {pct(cover.si_return)} vs {pct(cover.benchmark_si_return)}
          </dd>
        </div>
      </dl>
    </div>
  );
}
