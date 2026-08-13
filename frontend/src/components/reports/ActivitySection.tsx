/**
 * §4.6 Period activity — own-platform trades in the period, capped at
 * ~20 rows with "show all (N)" in the Section action slot (§6.8).
 * Permanent scope caveat until the #1593 trade ledger.
 */
import { useState } from "react";
import { Link } from "react-router-dom";

import type { ActivityRowV2 } from "@/api/reportSnapshot";
import { Fn, NilLine, ScopeCaveat, type NoteIndex } from "@/components/reports/StatementChrome";
import { dec } from "@/components/reports/snapshotMath";
import { formatDateTime, formatMoney, formatNumber } from "@/lib/format";
import { actionTone } from "@/lib/badgeTone";
import { Badge } from "@/components/ui/Badge";

const CAP = 20;

export function ActivitySection({
  opened,
  closed,
  currency,
  marker,
}: {
  opened: ActivityRowV2[];
  closed: ActivityRowV2[];
  currency: string;
  marker: NoteIndex;
}) {
  const [showAll, setShowAll] = useState(false);
  const rows = [...opened, ...closed].sort((a, b) =>
    (a.filled_at ?? "") < (b.filled_at ?? "") ? -1 : 1,
  );

  if (rows.length === 0) {
    return (
      <div>
        <NilLine>No transactions during this period.</NilLine>
        <ScopeCaveat>
          Own-platform orders only — broker-side trade history lands with the trade ledger (#1593)
          <Fn n={marker.scope1593} />
        </ScopeCaveat>
      </div>
    );
  }

  const visible = showAll ? rows : rows.slice(0, CAP);

  return (
    <div className="space-y-2">
      <table className="w-full text-sm">
        <thead>
          <tr className="text-left text-xs uppercase tracking-wide text-slate-500">
            <th className="px-2 py-2 font-medium">Date</th>
            <th className="px-2 py-2 font-medium">Side</th>
            <th className="px-2 py-2 font-medium">Symbol</th>
            <th className="px-2 py-2 text-right font-medium">Units</th>
            <th className="px-2 py-2 text-right font-medium">Price</th>
            <th className="px-2 py-2 text-right font-medium">Fees</th>
          </tr>
        </thead>
        <tbody>
          {visible.map((r, i) => (
            <tr key={`${r.instrument_id}-${r.filled_at}-${i}`} className="border-t border-slate-100 dark:border-slate-800/60">
              <td className="px-2 py-2 text-slate-500">{formatDateTime(r.filled_at)}</td>
              <td className="px-2 py-2">
                <Badge tone={actionTone(r.action)}>{r.action}</Badge>
              </td>
              <td className="px-2 py-2">
                <Link
                  to={`/instrument/${encodeURIComponent(r.symbol)}`}
                  className="font-medium text-blue-600 hover:underline dark:text-blue-400"
                >
                  {r.symbol}
                </Link>
              </td>
              <td className="px-2 py-2 text-right tabular-nums">{formatNumber(dec(r.units))}</td>
              <td className="px-2 py-2 text-right tabular-nums">{formatMoney(dec(r.price), currency)}</td>
              <td className="px-2 py-2 text-right tabular-nums">{formatMoney(dec(r.fees), currency)}</td>
            </tr>
          ))}
        </tbody>
      </table>
      {rows.length > CAP && !showAll ? (
        <button
          type="button"
          onClick={() => setShowAll(true)}
          className="rounded border border-slate-200 px-2 py-1 text-xs font-medium text-slate-600 hover:bg-slate-50 dark:border-slate-700 dark:text-slate-300 dark:hover:bg-slate-800"
        >
          Show all ({rows.length})
        </button>
      ) : null}
      <ScopeCaveat>
        Own-platform orders only — broker-side trade history lands with the trade ledger (#1593)
        <Fn n={marker.scope1593} />
      </ScopeCaveat>
    </div>
  );
}
