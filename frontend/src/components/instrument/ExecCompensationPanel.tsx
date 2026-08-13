/**
 * ExecCompensationPanel — DEF 14A Item 402(c) Summary Compensation Table
 * on the instrument Research grid (#1969, backend #1945).
 *
 * Renders the latest proxy's SCT grouped by executive (name + position
 * once, up to three fiscal-year rows beneath, latest first), highest-paid
 * NEO first. Figures are the filing's USD amounts, shown compact
 * (`formatBigNumber`) so the eight SCT columns fit a full-width tile; the
 * Total column is emphasised. Self-gates to an honest empty state when the
 * issuer has no parsed SCT yet (many issuers file no comp-voting proxy),
 * so the tile is harmless before the #1945 production backfill lands.
 *
 * Source rule: 17 CFR §229.402(c). Backend table def14a_exec_compensation.
 */

import { Fragment } from "react";
import { useCallback } from "react";

import { fetchExecCompensation } from "@/api/instruments";
import type { ExecCompensationResponse } from "@/api/instruments";
import { SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { Pane } from "@/components/instrument/Pane";
import { EmptyState } from "@/components/states/EmptyState";
import { groupExecComp, parseComp } from "@/components/instrument/execCompensation";
import { formatBigNumber } from "@/lib/format";
import { useAsync } from "@/lib/useAsync";

export interface ExecCompensationPanelProps {
  readonly symbol: string;
}

/** Compact USD magnitude for a Decimal-as-string comp cell; "—" when
 *  the column is absent/null (matches the SCT convention of blank cells). */
function money(value: string | null): string {
  return formatBigNumber(parseComp(value));
}

export function ExecCompensationPanel({
  symbol,
}: ExecCompensationPanelProps): JSX.Element {
  const state = useAsync<ExecCompensationResponse>(
    // useAsync captures fn via a ref — fresh arrow per render is fine.
    useCallback(() => fetchExecCompensation(symbol), [symbol]),
    [symbol],
  );

  return (
    <Pane title="Executive compensation" source={{ providers: ["sec_def14a"] }}>
      {state.loading ? (
        <SectionSkeleton rows={4} />
      ) : state.error !== null || state.data === null ? (
        <SectionError onRetry={state.refetch} />
      ) : state.data.rows.length === 0 ? (
        <EmptyState
          title="No proxy compensation on file"
          description="Populated from DEF 14A proxy Summary Compensation Tables (Item 402(c)). Not every issuer files a compensation-voting proxy."
        />
      ) : (
        <PanelBody data={state.data} />
      )}
    </Pane>
  );
}

function PanelBody({ data }: { data: ExecCompensationResponse }): JSX.Element {
  const groups = groupExecComp(data.rows);
  return (
    <div>
      <table className="w-full text-sm">
        <thead className="text-xs uppercase tracking-wide text-slate-500 dark:text-slate-400">
          <tr>
            <th className="pb-1 pr-2 text-left font-medium">Fiscal year</th>
            <th className="pb-1 px-2 text-right font-medium">Salary</th>
            <th className="pb-1 px-2 text-right font-medium">Bonus</th>
            <th className="pb-1 px-2 text-right font-medium">Stock</th>
            <th className="pb-1 px-2 text-right font-medium">Option</th>
            <th className="pb-1 px-2 text-right font-medium">Non-equity</th>
            <th className="pb-1 px-2 text-right font-medium">Pension/NQDC</th>
            <th className="pb-1 px-2 text-right font-medium">Other</th>
            <th className="pb-1 pl-2 text-right font-medium">Total</th>
          </tr>
        </thead>
        <tbody>
          {groups.map((group) => (
            <Fragment key={group.executive_name}>
              <tr className="border-t border-t-slate-200 dark:border-t-slate-700">
                <td
                  colSpan={9}
                  className="pt-2 pb-1 text-slate-800 dark:text-slate-100"
                >
                  <span className="font-semibold">{group.executive_name}</span>
                  {group.principal_position !== null ? (
                    <span className="ml-2 text-xs text-slate-500 dark:text-slate-400">
                      {group.principal_position}
                    </span>
                  ) : null}
                </td>
              </tr>
              {group.years.map((year) => (
                <tr
                  key={year.fiscal_year}
                  className="text-slate-700 dark:text-slate-200"
                >
                  <td className="py-1 pr-2 tabular-nums">
                    FY {year.fiscal_year}
                  </td>
                  <td className="py-1 px-2 text-right tabular-nums">
                    {money(year.salary)}
                  </td>
                  <td className="py-1 px-2 text-right tabular-nums">
                    {money(year.bonus)}
                  </td>
                  <td className="py-1 px-2 text-right tabular-nums">
                    {money(year.stock_awards)}
                  </td>
                  <td className="py-1 px-2 text-right tabular-nums">
                    {money(year.option_awards)}
                  </td>
                  <td className="py-1 px-2 text-right tabular-nums">
                    {money(year.non_equity_incentive)}
                  </td>
                  <td className="py-1 px-2 text-right tabular-nums">
                    {money(year.pension_nqdc)}
                  </td>
                  <td className="py-1 px-2 text-right tabular-nums">
                    {money(year.other_comp)}
                  </td>
                  <td className="py-1 pl-2 text-right font-semibold tabular-nums text-slate-900 dark:text-slate-100">
                    {money(year.total_comp)}
                  </td>
                </tr>
              ))}
            </Fragment>
          ))}
        </tbody>
      </table>
      <p className="mt-3 text-xs text-slate-500 dark:text-slate-400">
        Summary Compensation Table · 17 CFR §229.402(c) · figures in USD
        {data.accession_number !== null
          ? ` · latest proxy ${data.accession_number}`
          : ""}
      </p>
    </div>
  );
}
