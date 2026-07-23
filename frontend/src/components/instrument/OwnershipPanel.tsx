/**
 * Ownership card (#789, parent #788).
 *
 * Cross-channel deduped ownership snapshot rendered as a three-ring
 * sunburst against the canonical ``shares_outstanding`` denominator
 * (XBRL DEI). Treasury renders as an additive top wedge — NOT in the
 * denominator. Treasury-in-denominator math (the prior version's
 * ``shares_outstanding + treasury_shares``) systematically wedged
 * every other category down by the treasury fraction; codex audit
 * 2026-05-03 flagged this as a ship-blocker.
 *
 * The panel issues exactly one fetch (``fetchOwnershipRollup``) so
 * every slice, the residual, and the coverage banner all reconcile
 * against a single server-side ``snapshot_read`` snapshot. The five-
 * fetch race in the prior version is gone.
 *
 * Banner state machine (server-driven):
 *
 *   * ``no_data`` — XBRL ``shares_outstanding`` not on file. Red
 *     banner with a "trigger fundamentals sync" CTA copy.
 *   * ``unknown_universe`` — outstanding present but per-category
 *     universe estimates aren't yet seeded (Tier 0 default; #790
 *     closed no-code, estimates seed per-instrument as 13F filer
 *     counts land). Yellow banner with explicit "estimate not
 *     available" copy.
 *   * ``red`` / ``amber`` / ``green`` — universe coverage thresholds.
 *     Red ranks worst, then ``unknown_universe``, then amber, then
 *     green; the worst-of fold across the four tracked categories
 *     decides the banner. Codex v2 review pinned this ordering.
 *
 * Concentration (sum of slices / outstanding) ships as a separate
 * info chip — Codex v2 review caught the prior conflation that
 * would have permanently red-banned retail-heavy names.
 */

import { useCallback, useMemo } from "react";
import { useNavigate } from "react-router-dom";

import { fetchOwnershipRollup } from "@/api/ownership";
import type {
  OwnershipDef14ADrift,
  OwnershipNonvestedAwards,
  OwnershipDrs,
  OwnershipRollupResponse,
  OwnershipSlice,
  OwnershipSliceCategory,
} from "@/api/ownership";
import { SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { HistoricalSymbolCallout } from "@/components/instrument/HistoricalSymbolCallout";
import { OwnershipCoverageBanner } from "@/components/instrument/OwnershipCoverageBanner";
import { DualClassDenominatorCallout } from "@/components/instrument/DualClassDenominatorCallout";
import { PerClassDenominatorCallout } from "@/components/instrument/PerClassDenominatorCallout";
import {
  OwnershipLegend,
  OwnershipSunburst,
  openWedgeSource,
} from "@/components/instrument/OwnershipSunburst";
import type { WedgeClick } from "@/components/instrument/OwnershipSunburst";
import { Pane } from "@/components/instrument/Pane";
import {
  formatPct,
  formatShares,
  ownershipStaleDenominatorCopy,
  parseShareCount,
  topHoldersByShares,
} from "@/components/instrument/ownershipMetrics";
import type {
  SunburstHolder,
  SunburstInputs,
} from "@/components/instrument/ownershipRings";
import { buildSunburstRings } from "@/components/instrument/ownershipRings";
import { EmptyState } from "@/components/states/EmptyState";
import { useAsync } from "@/lib/useAsync";

export interface OwnershipPanelProps {
  readonly symbol: string;
}

export function OwnershipPanel({ symbol }: OwnershipPanelProps): JSX.Element {
  const rollupState = useAsync<OwnershipRollupResponse>(
    useCallback(() => fetchOwnershipRollup(symbol), [symbol]),
    [symbol],
  );

  const navigate = useNavigate();
  const handleWedgeClick = useCallback(
    (target: WedgeClick) => {
      // #921 split model (operator decision 2026-06-11): a per-filer
      // leaf wedge with a known source filing opens SEC EDGAR in a
      // new tab; everything else (categories, center, URL-less
      // leaves, popup-blocked opens) falls through to the in-app L2
      // drill below.
      if (openWedgeSource(target)) return;
      const params = new URLSearchParams();
      if (target.kind === "category") params.set("category", target.category_key);
      if (target.kind === "leaf") {
        params.set("category", target.category_key);
        params.set("filer", target.leaf_key);
      }
      const qs = params.toString();
      const suffix = qs.length > 0 ? `?${qs}` : "";
      navigate(`/instrument/${encodeURIComponent(symbol)}/ownership${suffix}`);
    },
    [navigate, symbol],
  );

  return (
    <Pane
      title="Ownership"
      source={{
        providers: [
          "sec_13f",
          "sec_form3",
          "sec_form4",
          "sec_13dg",
          "sec_def14a",
          "sec_xbrl",
        ],
      }}
    >
      {rollupState.loading ? (
        <SectionSkeleton rows={4} />
      ) : rollupState.error !== null || rollupState.data === null ? (
        // ``data === null`` should only be reachable as a defense-
        // in-depth path: the rollup endpoint always returns at least
        // the ``no_data`` payload shape (200 OK + empty slices), so a
        // null body would only arise from a future middleware that
        // unwraps the response. Surface it as an error rather than
        // hanging on a perma-skeleton. Claude PR review (PR 798)
        // round 2 caught the prior skeleton fallback.
        <SectionError onRetry={rollupState.refetch} />
      ) : (
        <PanelBody rollup={rollupState.data} onWedgeClick={handleWedgeClick} />
      )}
    </Pane>
  );
}

/**
 * Map the rollup response to the existing ``SunburstInputs`` shape so
 * the chart code can render unchanged. ``shares_outstanding`` is the
 * denominator (treasury-on-top model); per-slice totals come from
 * the deduped rollup; per-filer holders feed the outer ring.
 *
 * ``def14a_unmatched`` is a NON-ADDITIVE memo overlay (#1659,
 * ``denominator_basis=proxy_disclosure``) — DEF 14A beneficial ownership is a
 * Rule 13d-3 deemed/overlapping disclosure (SEC Item 403), already counted via
 * 13D/G, 13F, Form 4 — so it is NOT a sunburst wedge (it is rendered in the memo
 * overlay below the pie, like ``funds``). Both overlays are deliberately NOT
 * flattened into the sunburst here. (Reverses #1627's additive treatment.)
 */
export function rollupToSunburstInputs(
  rollup: OwnershipRollupResponse,
): SunburstInputs | null {
  const outstanding = parseShareCount(rollup.shares_outstanding);
  if (outstanding === null || outstanding <= 0) return null;
  const sliceTotal = (cat: OwnershipSliceCategory): number | null => {
    const found = rollup.slices.find((s) => s.category === cat);
    return found === undefined ? null : parseShareCount(found.total_shares);
  };
  const sliceAsOf = (cat: OwnershipSliceCategory): string | null => {
    const found = rollup.slices.find((s) => s.category === cat);
    if (found === undefined || found.holders.length === 0) return null;
    let latest: string | null = null;
    for (const h of found.holders) {
      if (h.as_of_date === null) continue;
      if (latest === null || h.as_of_date > latest) latest = h.as_of_date;
    }
    return latest;
  };
  const flattenHolders = (
    cat: OwnershipSliceCategory,
    target: SunburstHolder["category"],
  ): SunburstHolder[] => {
    const found = rollup.slices.find((s) => s.category === cat);
    if (found === undefined) return [];
    const out: SunburstHolder[] = [];
    for (const h of found.holders) {
      const shares = parseShareCount(h.shares);
      if (shares === null || shares <= 0) continue;
      const key = h.filer_cik ?? `name:${h.filer_name}`;
      out.push({
        key,
        label: h.filer_name,
        shares,
        category: target,
        source_url: h.winning_edgar_url,
      });
    }
    return out;
  };

  const treasury = parseShareCount(rollup.treasury_shares);

  return {
    // Canonical denominator: shares_outstanding only. Treasury is
    // additive on top, NOT part of the denominator.
    total_shares: outstanding,
    holders: [
      ...flattenHolders("institutions", "institutions"),
      ...flattenHolders("etfs", "etfs"),
      ...flattenHolders("insiders", "insiders"),
      // def14a_unmatched (proxy_disclosure) and funds (institution_subset) are
      // NON-ADDITIVE memo overlays — never sunburst wedges — so both are
      // deliberately absent from this list (#1659 / #919).
      ...flattenHolders("blockholders", "blockholders"),
    ],
    institutions_total: sliceTotal("institutions"),
    etfs_total: sliceTotal("etfs"),
    insiders_total: sliceTotal("insiders"),
    // Non-additive overlay (#1659): no DEF 14A sunburst wedge. The proxy holders
    // render only in the memo overlay below the pie.
    def14a_total: null,
    blockholders_total: sliceTotal("blockholders"),
    treasury_shares: treasury,
    institutions_as_of: sliceAsOf("institutions"),
    etfs_as_of: sliceAsOf("etfs"),
    insiders_as_of: sliceAsOf("insiders"),
    def14a_as_of: null,
    treasury_as_of: rollup.treasury_as_of,
    blockholders_as_of: sliceAsOf("blockholders"),
  };
}

interface PanelBodyProps {
  readonly rollup: OwnershipRollupResponse;
  readonly onWedgeClick: (target: WedgeClick) => void;
}

function PanelBody({ rollup, onWedgeClick }: PanelBodyProps): JSX.Element {
  const inputs = useMemo(() => rollupToSunburstInputs(rollup), [rollup]);
  if (rollup.banner.state === "no_data" || inputs === null) {
    return (
      <div className="flex flex-col gap-3">
        <OwnershipCoverageBanner banner={rollup.banner} />
        {/* Drift is denominator-independent (#966) — keep the chip
            visible even when the rollup itself is degraded. */}
        <Def14ADriftChip drift={rollup.def14a_drift ?? null} />
        <HistoricalSymbolCallout
          currentSymbol={rollup.symbol}
          historicalSymbols={rollup.historical_symbols}
        />
        <EmptyState
          title="No ownership data"
          description={
            ownershipStaleDenominatorCopy(
              rollup.banner.state,
              rollup.shares_outstanding_as_of,
            ) ??
            "XBRL shares-outstanding not yet on file for this instrument. Trigger a fundamentals sync, or wait for the next scheduled run."
          }
        />
      </div>
    );
  }
  const rings = buildSunburstRings(inputs);
  if (rings === null) {
    return (
      <div className="flex flex-col gap-3">
        <OwnershipCoverageBanner banner={rollup.banner} />
        <Def14ADriftChip drift={rollup.def14a_drift ?? null} />
        <HistoricalSymbolCallout
          currentSymbol={rollup.symbol}
          historicalSymbols={rollup.historical_symbols}
        />
        <EmptyState
          title="No ownership data"
          description="Sunburst rings could not be derived — shares outstanding resolved to zero or the input snapshot is malformed."
        />
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-3">
      <OwnershipCoverageBanner banner={rollup.banner} />
      <HistoricalSymbolCallout
        currentSymbol={rollup.symbol}
        historicalSymbols={rollup.historical_symbols}
      />
      <ConcentrationChip rollup={rollup} />
      <Def14ADriftChip drift={rollup.def14a_drift ?? null} />
      {rollup.dual_class_denominator !== null && (
        <DualClassDenominatorCallout note={rollup.dual_class_denominator.note} />
      )}
      {rollup.per_class_denominator != null && (
        <PerClassDenominatorCallout note={rollup.per_class_denominator.note} />
      )}
      {rollup.residual.oversubscribed && <OversubscribedWarning />}
      <div className="flex flex-col gap-4 lg:flex-row lg:items-start">
        <div className="flex flex-col items-center gap-3">
          <OwnershipSunburst inputs={inputs} onWedgeClick={onWedgeClick} />
          <OwnershipLegend rings={rings} />
        </div>
        <div className="min-w-0 flex-1">
          <p className="mb-1 text-xs text-slate-500 dark:text-slate-400">
            {rollup.shares_outstanding !== null
              ? `${formatShares(parseShareCount(rollup.shares_outstanding) ?? 0)} outstanding`
              : "outstanding unknown"}
            {rollup.treasury_shares !== null
              && parseShareCount(rollup.treasury_shares) !== null
              && (parseShareCount(rollup.treasury_shares) ?? 0) > 0 && (
                <> + {formatShares(parseShareCount(rollup.treasury_shares) ?? 0)} treasury</>
              )}
            {rollup.shares_outstanding_as_of !== null && (
              <> · as of {rollup.shares_outstanding_as_of}</>
            )}
            {rollup.shares_outstanding_source.edgar_url !== null && (
              <>
                {" · "}
                <a
                  className="underline decoration-dotted hover:text-slate-700 dark:hover:text-slate-300"
                  href={rollup.shares_outstanding_source.edgar_url}
                  target="_blank"
                  rel="noopener noreferrer"
                  data-test="shares-outstanding-source"
                >
                  {rollup.shares_outstanding_source.form_type ?? "SEC filing"}
                </a>
              </>
            )}
            .
          </p>
          <SliceTable rollup={rollup} />
          <ResidualLine rollup={rollup} />
          <NonvestedAwardsMemo memo={rollup.nonvested_awards ?? null} />
          <DrsMemo drs={rollup.drs ?? null} />
          <p className="mt-2 text-xs text-slate-400 dark:text-slate-500">
            Click any colored wedge for the per-filer drilldown.
          </p>
        </div>
      </div>
    </div>
  );
}

interface ConcentrationChipProps {
  readonly rollup: OwnershipRollupResponse;
}

function ConcentrationChip({ rollup }: ConcentrationChipProps): JSX.Element {
  return (
    <p className="text-xs text-slate-500 dark:text-slate-400" data-test="concentration-chip">
      {rollup.concentration.info_chip}
    </p>
  );
}

function Def14ADriftChip({
  drift,
}: {
  readonly drift: OwnershipDef14ADrift | null;
}): JSX.Element | null {
  // #966 — DEF 14A vs Form 4 drift. Server-owned copy; no client-side
  // threshold logic (operator-ui convention). Amber = warning, red =
  // critical (color semantics table).
  if (drift === null) {
    return null;
  }
  const palette =
    drift.worst_severity === "critical"
      ? "border-red-200 bg-red-50 text-red-700 dark:border-red-900/60 dark:bg-red-900/20 dark:text-red-200"
      : "border-amber-200 bg-amber-50 text-amber-900 dark:border-amber-900/60 dark:bg-amber-900/20 dark:text-amber-200";
  return (
    <div
      className={`rounded-md border px-3 py-2 text-xs ${palette}`}
      role="status"
      data-test="def14a-drift-chip"
    >
      {drift.chip}
      {drift.holders.length > 0 && (
        <span className="opacity-80"> Worst: {drift.holders.join(", ")}.</span>
      )}
    </div>
  );
}

function NonvestedAwardsMemo({
  memo,
}: {
  readonly memo: OwnershipNonvestedAwards | null;
}): JSX.Element | null {
  // #844 — unvested RSU/PSU memo. Absolute count only (RSUs are not
  // outstanding until vested — never a wedge); server-owned label.
  if (memo === null) {
    return null;
  }
  const shares = parseShareCount(memo.shares);
  if (shares === null || shares <= 0) {
    return null;
  }
  return (
    <p
      className="mt-1 text-xs text-slate-500 dark:text-slate-400"
      data-test="nonvested-awards-memo"
    >
      +{formatShares(shares)} {memo.label} (not outstanding until vested; as of{" "}
      {memo.period_end}).
    </p>
  );
}

function DrsMemo({ drs }: { readonly drs: OwnershipDrs | null }): JSX.Element | null {
  // #844 PR-2 — issuer-disclosed registered/street split. Server owns
  // cohort + staleness; absence renders nothing (no fake "0 DRS" state).
  if (drs === null) {
    return null;
  }
  const registered = parseShareCount(drs.registered_shares);
  if (registered === null || registered <= 0) {
    return null;
  }
  // #2124 — the server sends a raw NUMERIC(8,4) string ("14.0000"); parse
  // to trim trailing zeros ("14", "99.6", "0.4"). formatPct is not reused:
  // it expects a fraction and force-fixes 2 decimals ("14.00%").
  const pctNum =
    drs.registered_pct !== null ? Number.parseFloat(drs.registered_pct) : NaN;
  const pct = Number.isFinite(pctNum) ? ` (${pctNum}%)` : "";
  const holders =
    drs.holders_of_record !== null
      ? `; ${drs.holders_of_record.toLocaleString()} holders of record`
      : "";
  return (
    <p
      className="mt-1 text-xs text-slate-500 dark:text-slate-400"
      data-test="drs-memo"
    >
      {formatShares(registered)}{pct} registered with transfer agent (DRS/book
      form){holders} · as of {drs.as_of_date}.
    </p>
  );
}

function OversubscribedWarning(): JSX.Element {
  // The server clamped the residual to 0; surface the diagnostic so
  // the operator knows a stale 13F + fresh Form 4/13D mix is in play.
  return (
    <div
      className="rounded-md border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-900 dark:border-amber-900/60 dark:bg-amber-900/20 dark:text-amber-200"
      role="status"
      data-test="oversubscribed-warning"
    >
      Category totals exceed shares outstanding (likely cause: stale 13F
      quarter combined with fresh Form 4 / 13D filings). Awaiting next
      13F cycle for the snapshots to align.
    </div>
  );
}

function ResidualLine({ rollup }: { rollup: OwnershipRollupResponse }): JSX.Element {
  const pct = parseShareCount(rollup.residual.pct_outstanding) ?? 0;
  return (
    <p className="mt-2 text-xs text-slate-500 dark:text-slate-400" data-test="residual-line">
      {rollup.residual.label}: {formatPct(pct)}
    </p>
  );
}

interface SliceTableProps {
  readonly rollup: OwnershipRollupResponse;
}

// Known pie-wedge categories, in render order. Pie-wedge slices
// (``denominator_basis="pie_wedge"``) sum to ≤ shares_outstanding and
// feed the chart + server residual; ``institution_subset`` slices
// (funds) render below as a non-additive memo overlay. The table is
// driven off ``denominator_basis`` (NOT this list alone) so a future
// additive category is never silently dropped while the residual still
// subtracts it — this list only fixes the ORDER of the categories we
// already know (Codex ckpt-1).
const _CATEGORY_ORDER_TABLE: readonly OwnershipSliceCategory[] = [
  "insiders",
  "blockholders",
  "institutions",
  "etfs",
  // def14a_unmatched is a non-additive overlay (#1659), not a pie-wedge row.
];

function _denominatorBasis(slice: OwnershipSlice) {
  return slice.denominator_basis ?? "pie_wedge";
}

function SliceTable({ rollup }: SliceTableProps): JSX.Element {
  const pieSlices = rollup.slices.filter((s) => _denominatorBasis(s) === "pie_wedge");
  // Any non-pie_wedge slice is a non-additive memo overlay (funds
  // institution_subset #919, DEF 14A proxy_disclosure #1659). Basis-driven so a
  // future overlay surfaces here automatically.
  const overlaySlices = rollup.slices.filter(
    (s) => _denominatorBasis(s) !== "pie_wedge",
  );
  // Order the known categories, then append any unknown pie-wedge slice
  // the FE has no explicit slot for — so a new additive category can't
  // vanish from the table while the server residual still counts it.
  const orderedPie: OwnershipSlice[] = [
    ..._CATEGORY_ORDER_TABLE.map((cat) =>
      pieSlices.find((s) => s.category === cat),
    ).filter((s): s is OwnershipSlice => s !== undefined),
    ...pieSlices.filter((s) => !_CATEGORY_ORDER_TABLE.includes(s.category)),
  ];

  if (orderedPie.length === 0 && overlaySlices.length === 0) {
    return (
      <p className="text-xs text-slate-500 dark:text-slate-400">
        No filings ingested yet.
      </p>
    );
  }
  return (
    <>
      {orderedPie.length > 0 && (
        <table className="w-full text-sm">
          <thead className="text-xs uppercase tracking-wide text-slate-500 dark:text-slate-400">
            <tr>
              <th className="pb-1 text-left">Category</th>
              <th className="pb-1 text-right">Shares</th>
              <th className="pb-1 text-right">% of outstanding</th>
              <th className="pb-1 text-right">Filers</th>
            </tr>
          </thead>
          <tbody>
            {orderedPie.map((slc) => {
              const shares = parseShareCount(slc.total_shares) ?? 0;
              const pct = parseShareCount(slc.pct_outstanding) ?? 0;
              return (
                <tr
                  key={slc.category}
                  className="border-t border-t-slate-100 dark:border-t-slate-800"
                >
                  <td className="py-1.5 text-slate-700 dark:text-slate-200">
                    {slc.label}
                  </td>
                  <td className="py-1.5 text-right font-mono text-slate-700 dark:text-slate-200">
                    {formatShares(shares)}
                  </td>
                  <td className="py-1.5 text-right font-mono text-slate-900 dark:text-slate-100">
                    {formatPct(pct)}
                  </td>
                  <td className="py-1.5 text-right font-mono text-slate-500 dark:text-slate-400">
                    {slc.filer_count}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      )}
      {overlaySlices.map((slc) => (
        <OverlaySection key={slc.category} slice={slc} />
      ))}
    </>
  );
}

interface OverlaySectionProps {
  readonly slice: OwnershipSlice;
}

const _OVERLAY_TOP_N = 8;

/**
 * Memo overlay for an ``institution_subset`` slice — funds today (#919),
 * with top-N per-holder detail (#1627). These holdings are NON-additive
 * (fund-level N-PORT detail already counted inside the 13F-HR
 * institutional aggregate), so they render in a separate, visually
 * distinct panel and never sum into the pie or residual. Italic memo +
 * muted styling signal the non-additive semantic. Shows the top
 * ``_OVERLAY_TOP_N`` holders by shares, a "+N more" line, and the
 * aggregate total. Overlay selection is basis-driven, so a future
 * ``institution_subset`` slice surfaces here automatically; only the
 * funds-specific double-count copy is keyed on the category.
 */
function OverlaySection({ slice }: OverlaySectionProps): JSX.Element {
  const total = parseShareCount(slice.total_shares) ?? 0;
  const totalPct = parseShareCount(slice.pct_outstanding) ?? 0;
  const { shown, remaining } = topHoldersByShares(slice.holders, _OVERLAY_TOP_N);
  const isFunds = slice.category === "funds";
  const isProxy = slice.category === "def14a_unmatched";
  const isEsop = slice.category === "esop";
  const unit = isFunds ? "fund series" : "holders";
  return (
    <div
      className="mt-3 rounded-md border border-slate-200 bg-slate-50 p-2 text-xs dark:border-slate-800 dark:bg-slate-900/40"
      data-test="ownership-overlay"
    >
      <p className="mb-1 italic text-slate-600 dark:text-slate-300">
        Memo: {slice.label} —{" "}
        {isFunds
          ? "fund-level detail of positions already counted in Institutions via 13F-HR. "
          : isProxy
            ? "DEF 14A beneficial ownership (Rule 13d-3): the same shares may be listed under multiple owners (control groups, parent/sub, “all officers as a group”), so it is shown as a cross-check, not added to the pie. The real holders are counted via 13D/G, 13F and Form 4. "
            : isEsop
              ? "ESOP / employee-benefit-plan holdings disclosed in DEF 14A (Rule 13d-3, SEC Item 403) — a proxy cross-check, not added to the pie. "
              : "non-additive overlay. "}
        Does not contribute to the pie or residual math.
      </p>
      <table className="w-full">
        <thead className="text-[0.625rem] uppercase tracking-wide text-slate-500 dark:text-slate-400">
          <tr>
            <th className="pb-1 text-left">
              {isFunds ? "Fund series (N-PORT)" : "Holder"}
            </th>
            <th className="pb-1 text-right">Shares</th>
            <th className="pb-1 text-right">% of outstanding</th>
          </tr>
        </thead>
        <tbody>
          {shown.map((h, i) => {
            const hShares = parseShareCount(h.shares) ?? 0;
            const hPct = parseShareCount(h.pct_outstanding) ?? 0;
            // filer_cik is NOT unique — a registrant CIK fronts many N-PORT
            // series funds (#1800: Vanguard 0000036405 = 43 rows on AAPL). This
            // overlay table never navigates, so the row key is a pure React
            // reconciliation id: compose with the array index, the only
            // guaranteed-unique discriminator (full-population check found
            // byte-identical holder rows that no data field disambiguates).
            return (
              <tr key={`${h.filer_cik ?? `name:${h.filer_name}`}:${i}`}>
                <td className="py-1 text-slate-600 dark:text-slate-300">
                  {h.winning_edgar_url !== null ? (
                    <a
                      className="underline decoration-dotted hover:text-slate-800 dark:hover:text-slate-100"
                      href={h.winning_edgar_url}
                      target="_blank"
                      rel="noopener noreferrer"
                    >
                      {h.filer_name}
                    </a>
                  ) : (
                    h.filer_name
                  )}
                </td>
                <td className="py-1 text-right font-mono text-slate-600 dark:text-slate-300">
                  {formatShares(hShares)}
                </td>
                <td className="py-1 text-right font-mono text-slate-700 dark:text-slate-200">
                  {formatPct(hPct)}
                </td>
              </tr>
            );
          })}
          {remaining > 0 && (
            <tr>
              <td colSpan={3} className="py-1 italic text-slate-500 dark:text-slate-400">
                + {formatShares(remaining)} more {unit}
              </td>
            </tr>
          )}
        </tbody>
        <tfoot>
          <tr className="border-t border-t-slate-200 dark:border-t-slate-700">
            <td className="py-1 font-medium text-slate-700 dark:text-slate-200">
              Total · {slice.filer_count} {unit}
            </td>
            <td className="py-1 text-right font-mono font-medium text-slate-700 dark:text-slate-200">
              {formatShares(total)}
            </td>
            <td className="py-1 text-right font-mono font-medium text-slate-700 dark:text-slate-200">
              {formatPct(totalPct)}
            </td>
          </tr>
        </tfoot>
      </table>
    </div>
  );
}
