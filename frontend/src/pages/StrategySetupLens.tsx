import { useState } from "react";

import {
  fetchCoreSleeve,
  fetchStrategyOverview,
  rebalanceCoreSleeve,
  updateCoreMandate,
} from "@/api/strategies";
import type { CoreSleeveResponse, StrategyOverviewResponse } from "@/api/types";
import { ApiError } from "@/api/client";
import { SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { StatTile } from "@/components/dashboard/StatTile";
import { AutomationControl, BlockerRow } from "@/components/strategies/StrategyPortfolioPanels";
import { Badge } from "@/components/ui/Badge";
import { Modal } from "@/components/ui/Modal";
import { formatDate } from "@/lib/format";
import { useAsync } from "@/lib/useAsync";

/**
 * Setup lens of `/strategies` (#3334) — every WRITE the pot has, and nothing
 * the operator only watches.
 *
 * Operator feedback 2026-09-23: configuration sat above results, so the page
 * opened on a form. Monitor and configure are two jobs, so they are two lenses
 * (the settled lens-hub pattern, `information-architecture.md`): Portfolio is
 * read-only apart from closing positions, and this lens holds funding, the
 * risk profile, the core sleeve's mandate and — read-only, each with its reason
 * — the rules policy fixes and no form can change.
 */

// The server currently returns exactly the three preregistered #2833
// candidates. Keep a defensive render cap anyway: API array types carry no
// size bound, and a malformed response must not create an unbounded DOM.
const CORE_CANDIDATE_RENDER_CAP = 10;

/**
 * One badge per sleeve state (#3037).
 *
 * ⚠⚠ The badge this replaced was `state === "ready" ? "Ready" : "Cash"`, so every
 * non-ready state rendered as **Cash** — and `cash` is not "not ready", it is one of the
 * two TERMINAL answers the sealed #2833 verifier emits. At 1 of 5 common dates the card
 * was asserting the study's result before the study could be opened.
 *
 * Derived state is the trap here: a verdict must be REPORTED, never inferred from the
 * absence of readiness.
 */
const CORE_SLEEVE_BADGE: Record<CoreSleeveResponse["state"], { label: string; tone: "ok" | "warn" }> = {
  ready: { label: "Ready", tone: "ok" },
  cash: { label: "Cash", tone: "warn" },
  awaiting_verdict: { label: "Verdict due", tone: "warn" },
  evidence_collecting: { label: "Collecting evidence", tone: "warn" },
  unavailable: { label: "Unavailable", tone: "warn" },
};

/**
 * Whether #2833's declared window has CLOSED — a fact about coverage, deliberately
 * independent of `state`.
 *
 * ⚠ Keying the window copy on `state !== "evidence_collecting"` is wrong (caught at Codex
 * checkpoint 2): `unavailable` is reachable with the window still OPEN — a missing
 * candidate row, or inconsistent verdict constants — and would then render "Window closed"
 * over a bound in the future. Server-side the window is closed iff the REQUIRED-th common
 * date exists, which is exactly `observed_trading_days >= required_trading_days`: both
 * derive from the same `common_dates` CTE, and both are zeroed when a candidate is missing.
 */
function coreWindowClosed(sleeve: CoreSleeveResponse): boolean {
  return sleeve.observed_trading_days >= sleeve.required_trading_days;
}

/** The `Instrument` tile says "Cash" only when the verdict actually said cash. */
function coreInstrumentTile(sleeve: CoreSleeveResponse): { value: string; hint: string } {
  if (sleeve.selected_symbol) return { value: sleeve.selected_symbol, hint: "Evidence-selected" };
  if (sleeve.state === "cash") return { value: "Cash", hint: "#2833 adopted no sleeve" };
  if (sleeve.state === "awaiting_verdict") return { value: "—", hint: "Verdict can be opened now" };
  if (sleeve.state === "unavailable") return { value: "—", hint: "Selection cannot be used" };
  return { value: "—", hint: "Decided when the verdict opens" };
}

function CoreCandidateCoverageTable({ sleeve }: { sleeve: CoreSleeveResponse }) {
  return (
    <div>
      <h3 className="text-xs font-semibold uppercase tracking-wide text-slate-500">Candidate coverage</h3>
      <p className="mt-1 text-xs text-slate-500">Overall evidence counts only dates shared by every candidate.</p>
      <div className="mt-2 overflow-x-auto">
        <table className="w-full text-left text-sm" aria-label="Core candidate evidence coverage">
          <thead className="text-xs text-slate-500">
            <tr>
              <th scope="col" className="py-1 pr-4 font-medium">Instrument</th>
              <th scope="col" className="py-1 pr-4 font-medium">Dates seen</th>
              <th scope="col" className="py-1 font-medium">Prospective dates</th>
            </tr>
          </thead>
          <tbody>
            {sleeve.candidates.slice(0, CORE_CANDIDATE_RENDER_CAP).map((candidate) => (
              <tr key={candidate.instrument_id} className="border-t border-slate-100 dark:border-slate-800">
                <th scope="row" className="py-2 pr-4 font-semibold">{candidate.symbol}</th>
                <td className="py-2 pr-4 tabular-nums">
                  {candidate.observed_trading_days} / {sleeve.required_trading_days}
                </td>
                <td className="py-2 text-slate-500">
                  {candidate.first_observed_date && candidate.last_observed_date
                    ? `${formatDate(candidate.first_observed_date)} – ${formatDate(candidate.last_observed_date)}`
                    : "Awaiting first date"}
                </td>
              </tr>
            ))}
            {sleeve.candidates.length === 0 ? (
              <tr className="border-t border-slate-100 dark:border-slate-800">
                <td colSpan={3} className="py-2 text-slate-500">
                  No candidate coverage is available; the core sleeve&apos;s blockers name what must be restored.
                </td>
              </tr>
            ) : null}
          </tbody>
        </table>
      </div>
      {sleeve.candidates.length > CORE_CANDIDATE_RENDER_CAP ? (
        <p className="mt-2 text-xs text-slate-500">
          Showing {CORE_CANDIDATE_RENDER_CAP} of {sleeve.candidates.length} candidates.
        </p>
      ) : null}
    </div>
  );
}

function CoreSleeveControl({
  sleeve,
  busy,
  setBusy,
  onError,
  onUpdated,
}: {
  sleeve: CoreSleeveResponse;
  busy: boolean;
  setBusy: (busy: boolean) => void;
  onError: (message: string | null) => void;
  onUpdated: () => void;
}) {
  const mandate = sleeve.mandate;
  const [enabled, setEnabled] = useState(mandate.enabled ?? false);
  const [target, setTarget] = useState(mandate.core_target_pct ?? "80");
  const [reserve, setReserve] = useState(mandate.liquidity_reserve_pct ?? "10");
  const [band, setBand] = useState(mandate.rebalance_band_pct ?? "5");
  const [minimum, setMinimum] = useState(mandate.min_rebalance_amount ?? "25");
  const [reason, setReason] = useState("");
  const [confirmRebalance, setConfirmRebalance] = useState(false);
  const [outcome, setOutcome] = useState<string | null>(null);
  const policyUpgradeRequired = sleeve.blockers.some(
    (blocker) => blocker.code === "core_mandate_policy_unsupported",
  );
  const mandateDirty =
    policyUpgradeRequired ||
    enabled !== (mandate.enabled ?? false) ||
    Number(target) !== Number(mandate.core_target_pct ?? "80") ||
    Number(reserve) !== Number(mandate.liquidity_reserve_pct ?? "10") ||
    Number(band) !== Number(mandate.rebalance_band_pct ?? "5") ||
    Number(minimum) !== Number(mandate.min_rebalance_amount ?? "25");

  async function saveMandate() {
    setBusy(true);
    onError(null);
    setOutcome(null);
    try {
      await updateCoreMandate({
        enabled,
        core_instrument_id: sleeve.selected_instrument_id ?? mandate.core_instrument_id ?? null,
        core_target_pct: target,
        liquidity_reserve_pct: reserve,
        rebalance_band_pct: band,
        min_rebalance_amount: minimum,
        reason: reason.trim(),
        provider: "etoro",
        environment: "demo",
      });
      setReason("");
      onUpdated();
    } catch (error) {
      onError(error instanceof ApiError ? error.message : "The core mandate could not be saved.");
    } finally {
      setBusy(false);
    }
  }

  async function rebalance() {
    setBusy(true);
    onError(null);
    setOutcome(null);
    try {
      const result = await rebalanceCoreSleeve();
      const label =
        // #2603 sell leg: a sell executes as a WHOLE close of the core position, and a
        // later rebalance buys back to the band's lower edge.
        result.reason_code === "core_rebalance_close_submitted"
          ? "Broker accepted the whole close of the core position; the next rebalance resolves it."
          : result.state === "closed"
            ? "The core position was closed whole; rebalance again to buy back to the band's lower edge."
            : result.state === "reconcile_required"
              ? `The core close needs reconciliation (${result.reason_code}); no core buy is admitted until it resolves.`
              : result.state === "submitted"
          ? "Broker accepted; fill reconciliation is pending."
          : result.state === "submission_uncertain"
            ? "Submission outcome is uncertain; reconciliation is required before retrying."
            : result.reason_code === "core_order_reconciled"
              ? "The existing broker order was reconciled; holdings and fill state have been refreshed."
              : // ⚠ `core_resume_already_resolved` is `held`, and it does NOT mean the
                // band was evaluated — the click loaded an unresolved order and
                // something else resolved it first, so no sleeve observation ran at
                // all. It fell through to the band sentence below, which is a claim
                // about evidence this response does not carry. #2962 turned that from
                // a two-operator race into the ordinary case: the scheduled cycle now
                // reconciles core orders every five minutes.
                result.reason_code === "core_resume_already_resolved"
                ? "That order was already reconciled by the scheduled cycle. The sleeve was not re-evaluated — rebalance again to check the band."
                : // ⚠ `below_min_rebalance_amount` is also `held`, and it means the OPPOSITE
                  // of the band sentence below: the sleeve is OUTSIDE the band and the gap
                  // is smaller than the mandate's `min_rebalance_amount`, so the floor wins
                  // and the breach is reported rather than traded through
                  // (`strategy_core_allocator.py`: "The floor wins and the breach, if any,
                  // is reported"). Reachable since #3123 re-enabled the affordance while
                  // the sleeve holds a position, which is the state a small drift lives in.
                  //
                  // ⚠⚠ `state === "held"` is load-bearing, not defensive. The SAME reason
                  // code arrives as `refused` from `assess_core_broker_preflight`, where
                  // it is the BROKER's minimum biting, not the operator's. Naming the
                  // mandate there would send them to lower a setting that cannot resolve
                  // it. The executor evaluates the allocator without `broker_minimum` on
                  // the held path, so a held one is unambiguously the mandate floor; a
                  // refused one falls through to the generic line, because this response
                  // carries no `floor_source` to say which minimum bound.
                  result.state === "held" && result.reason_code === "below_min_rebalance_amount"
                  ? "The sleeve is outside its band, but the gap is below the mandate's minimum rebalance amount, so no trade was placed."
                  : result.state === "held"
                    ? "No trade required; the sleeve remains inside its band."
                    : `Rebalance refused: ${result.reason_code}.`;
      setOutcome(label);
      onUpdated();
    } catch (error) {
      onError(error instanceof ApiError ? error.message : "The demo rebalance could not be evaluated.");
    } finally {
      setBusy(false);
      setConfirmRebalance(false);
    }
  }

  return (
    <>
      <div className="mt-5 border-t border-slate-200 pt-4 dark:border-slate-800">
        {!sleeve.can_configure ? (
          <p className="mb-3 text-xs text-slate-500">
            Save these values as a disabled draft now.{" "}
            {sleeve.state === "cash"
              ? "#2833 returned cash, so no core instrument is adopted and enabling stays locked."
              : "Enabling and demo rebalancing remain locked until a core instrument passes #2833."}
          </p>
        ) : null}
        {/* #3334: the four sizing numbers are ADVANCED — sensible defaults exist
            and most operators never change them — so they sit behind a
            disclosure. `open` whenever a save is pending an upgrade, so a
            policy-upgrade save is never made against fields nobody can see. */}
        <details open={policyUpgradeRequired || undefined} className="group">
          <summary className="min-h-11 cursor-pointer py-2 text-sm font-medium text-slate-700 dark:text-slate-200">
            Advanced: core target, cash reserve, band, minimum trade
          </summary>
        <div className="mt-2 grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
          {[
            ["Core target %", target, setTarget],
            ["Minimum cash reserve %", reserve, setReserve],
            ["Rebalance band (pp)", band, setBand],
            ["Minimum amount (USD)", minimum, setMinimum],
          ].map(([label, value, setter]) => (
            <label key={label as string} className="text-xs font-medium text-slate-600 dark:text-slate-300">
              {label as string}
              <input
                type="number"
                min="0"
                step="0.01"
                value={value as string}
                onChange={(event) => (setter as (value: string) => void)(event.target.value)}
                className="mt-1 min-h-11 w-full rounded-md border border-slate-300 bg-white px-3 text-sm dark:border-slate-700 dark:bg-slate-950"
              />
            </label>
          ))}
        </div>
        </details>
        <label className="mt-3 flex min-h-11 items-center gap-2 text-sm">
          <input
            type="checkbox"
            checked={enabled}
            disabled={!sleeve.can_configure && !enabled}
            onChange={(event) => setEnabled(event.target.checked)}
            className="disabled:cursor-not-allowed disabled:opacity-50"
          />
          Enable demo core sleeve
        </label>
        <label className="mt-3 block text-xs font-medium text-slate-600 dark:text-slate-300">
          Audit reason
          <input
            value={reason}
            onChange={(event) => setReason(event.target.value)}
            className="mt-1 min-h-11 w-full rounded-md border border-slate-300 bg-white px-3 text-sm dark:border-slate-700 dark:bg-slate-950"
          />
        </label>
        <div className="mt-3 flex flex-wrap gap-2">
          <button
            type="button"
            disabled={busy || reason.trim().length === 0 || (enabled && !sleeve.can_configure) || (mandate.configured && !mandateDirty)}
            onClick={() => void saveMandate()}
            className="min-h-11 rounded-md border border-slate-300 px-3 text-sm font-medium disabled:cursor-not-allowed disabled:opacity-50 dark:border-slate-700"
          >
            {busy ? "Saving…" : "Save mandate"}
          </button>
          <button
            type="button"
            disabled={busy || (!sleeve.can_rebalance && !sleeve.can_resume) || (!sleeve.can_resume && mandateDirty)}
            onClick={() => setConfirmRebalance(true)}
            className="min-h-11 rounded-md bg-sky-700 px-3 text-sm font-medium text-white disabled:cursor-not-allowed disabled:opacity-50"
          >
            {/*
              ⚠⚠ #3222 residual 1. This said "Resume demo order", which reads as a
              trading action and so contradicted a "Not trading" headline beside it.
              Both sentences were true; the label was the lie. The backend proves the
              action cannot trade -- `resume_core_submission` is "Reconcile one
              committed authority WITHOUT EVER RETRYING ITS MUTATION", a broker lookup
              -- see the comment on `can_resume` in `app/api/strategies.py`. "Settle"
              is not a new word either: `strategyPortfolioStatus` already renders
              "Settling a core order" for this exact state.
            */}
            {sleeve.can_resume ? "Settle demo order" : "Rebalance demo now"}
          </button>
        </div>
      </div>
      {outcome ? <p role="status" className="mt-3 text-sm text-slate-700 dark:text-slate-200">{outcome}</p> : null}
      <Modal isOpen={confirmRebalance} onRequestClose={() => setConfirmRebalance(false)} labelledBy="core-rebalance-title">
        <h2 id="core-rebalance-title" className="text-base font-semibold">
          {sleeve.can_resume ? `Settle demo order ${sleeve.pending_order_id}?` : `Rebalance ${sleeve.selected_symbol} in demo?`}
        </h2>
        <p className="mt-2 text-sm text-slate-600 dark:text-slate-300">
          {sleeve.can_resume
            ? "The server will look up and reconcile the already-authorised order using its original request ID and account credentials. It cannot create a second order."
            : `The server will size within the ${target}% core mandate and every live guard. This path can buy only; it cannot sell or use alpha signals.`}
        </p>
        <div className="mt-4 flex justify-end gap-2">
          <button type="button" onClick={() => setConfirmRebalance(false)} className="min-h-11 rounded-md border border-slate-300 px-3 text-sm dark:border-slate-700">Cancel</button>
          <button type="button" disabled={busy} onClick={() => void rebalance()} className="min-h-11 rounded-md bg-sky-700 px-3 text-sm font-medium text-white disabled:opacity-50">{busy ? "Evaluating…" : sleeve.can_resume ? "Check with broker" : "Confirm demo rebalance"}</button>
        </div>
      </Modal>
    </>
  );
}

/** One read-only policy row: what is fixed, its current value, and why. */
function PolicyRow({ label, value, why }: { label: string; value: string; why: string }) {
  return (
    <div className="grid gap-1 border-t border-slate-200 py-3 text-sm sm:grid-cols-[12rem_10rem_1fr] sm:gap-4 dark:border-slate-800">
      <dt className="font-medium text-slate-700 dark:text-slate-200">{label}</dt>
      <dd className="font-semibold tabular-nums">{value}</dd>
      <dd className="text-xs text-slate-500">{why}</dd>
    </div>
  );
}

function PolicyRules({
  overview,
  coreSleeve,
}: {
  overview: StrategyOverviewResponse;
  coreSleeve: CoreSleeveResponse | null;
}) {
  const mandate = overview.paper_pool.mandate;
  return (
    <section aria-labelledby="setup-policy" className="border border-slate-200 bg-white p-5 dark:border-slate-800 dark:bg-slate-900">
      <h2 id="setup-policy" className="text-sm font-semibold">Set by policy</h2>
      <p className="mt-1 text-xs text-slate-500">Read-only. These are rules, not settings; no form on this page changes them.</p>
      <dl className="mt-3">
        <PolicyRow
          label="Stop loss / take profit"
          value="Always on"
          why="Every engine position carries a broker-side stop and target from the moment it opens (#3284). The levels are shown per position on the Portfolio lens."
        />
        <PolicyRow
          label="Kill switch"
          value={overview.entry_block.global_kill_active ? "On" : "Off"}
          why="When on, no new entry is placed anywhere in the engine. Open positions stay managed and can still be closed. When it is on, the Portfolio lens shows it with its Clear control."
        />
        <PolicyRow
          label="Account"
          value={overview.demo_connection ? "Demo" : "No demo connection"}
          why="Real-money strategy activation stays locked until the broker contract is validated and the capital boundary holds (#2843, #2844)."
        />
        <PolicyRow
          label="Direction and leverage"
          value={`${mandate.shorts_allowed ? "Long and short" : "Long only"} · ${mandate.leverage_allowed ? "Leverage" : "No leverage"}`}
          why="Fixed by the portfolio mandate policy for every risk profile in this version."
        />
      </dl>
      {coreSleeve ? (
        <div className="mt-4 border-t border-slate-200 pt-4 dark:border-slate-800">
          <CoreCandidateCoverageTable sleeve={coreSleeve} />
        </div>
      ) : null}
    </section>
  );
}

export function StrategySetupLens() {
  const overview = useAsync(fetchStrategyOverview, []);
  const coreSleeve = useAsync(fetchCoreSleeve, [], { preserveOnRefetch: true });
  const [busy, setBusy] = useState(false);
  const [actionError, setActionError] = useState<string | null>(null);

  if (overview.loading) return <SectionSkeleton rows={6} />;
  if (overview.error || !overview.data) return <SectionError onRetry={overview.refetch} />;
  const data: StrategyOverviewResponse = overview.data;

  return (
    <div className="space-y-6">
      {actionError ? (
        <p role="alert" className="text-sm text-rose-700 dark:text-rose-300">
          {actionError}
        </p>
      ) : null}
      <AutomationControl
        overview={data}
        // A preserved core snapshot is visibly stale during revalidation
        // and must not authorise the independent core activation lane.
        // Alpha readiness remains available from `overview` on its own.
        coreSleeve={coreSleeve.isRevalidating ? null : coreSleeve.data}
        onUpdated={() => {
          void overview.refetch();
          void coreSleeve.refetch();
        }}
      />
      {coreSleeve.loading ? <SectionSkeleton rows={3} /> : null}
      {coreSleeve.error ? <SectionError onRetry={coreSleeve.refetch} /> : null}
      {coreSleeve.data ? (
        <section className="border border-slate-200 bg-white p-5 dark:border-slate-800 dark:bg-slate-900">
          <div className="flex flex-wrap items-start justify-between gap-3">
            <div>
              <h2 className="text-sm font-semibold">Core &amp; cash</h2>
              <p className="mt-1 text-xs text-slate-500">
                Deterministic fallback when no strategy has earned capital.
              </p>
            </div>
            <div className="flex items-center gap-2">
              {coreSleeve.isRevalidating ? (
                <span className="text-xs text-amber-700 dark:text-amber-300">Status is stale — refreshing</span>
              ) : null}
              <button
                type="button"
                disabled={coreSleeve.isRevalidating}
                onClick={coreSleeve.refetch}
                className="min-h-11 rounded-md border border-slate-300 px-3 text-xs font-medium disabled:cursor-not-allowed disabled:opacity-50 dark:border-slate-700"
              >
                {coreSleeve.isRevalidating ? "Refreshing…" : "Refresh status"}
              </button>
              <Badge tone={CORE_SLEEVE_BADGE[coreSleeve.data.state].tone}>
                {CORE_SLEEVE_BADGE[coreSleeve.data.state].label}
              </Badge>
            </div>
          </div>
          <div className="mt-5 grid grid-cols-2 gap-x-6 lg:grid-cols-4">
            <StatTile
              size="md"
              label="Common dates seen"
              value={`${coreSleeve.data.observed_trading_days} / ${coreSleeve.data.required_trading_days}`}
              hint={
                coreWindowClosed(coreSleeve.data)
                  ? "The declared window is complete"
                  : "Provisional until the sealed verifier opens"
              }
            />
            <StatTile size="md" label="Instrument" {...coreInstrumentTile(coreSleeve.data)} />
            <StatTile
              size="md"
              label={coreWindowClosed(coreSleeve.data) ? "Window closed" : "Earliest verdict"}
              value={formatDate(coreSleeve.data.earliest_possible_verdict_at)}
              hint={
                coreWindowClosed(coreSleeve.data)
                  ? "The fifth common session closed here"
                  : "Lower bound if all five common sessions complete"
              }
            />
            <StatTile
              size="md"
              label="Cost ceiling"
              value={`${(coreSleeve.data.max_cost_bps / 100).toFixed(2)}%`}
              hint="Preregistered #2833 bar"
            />
          </div>
          <div className="mt-4">
            {coreSleeve.data.blockers.map((blocker) => (
              <BlockerRow key={blocker.code} tone="warn" label={blocker.detail} />
            ))}
          </div>
          <p className="mt-4 border-t border-slate-200 pt-3 text-xs text-slate-500 dark:border-slate-800">
            Demo only · buy only · no alpha signal. {coreSleeve.data.household_tax_caveat}{" "}
            {coreSleeve.data.household_currency_caveat}
          </p>
          <CoreSleeveControl
            sleeve={coreSleeve.data}
            busy={busy || coreSleeve.isRevalidating}
            setBusy={setBusy}
            onError={setActionError}
            onUpdated={() => {
              void coreSleeve.refetch();
              void overview.refetch();
            }}
          />
        </section>
      ) : null}
      <PolicyRules overview={data} coreSleeve={coreSleeve.data ?? null} />
    </div>
  );
}
