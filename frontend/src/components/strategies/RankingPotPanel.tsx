import { fetchRankingPotReadout, fetchRankingPotStatus } from "@/api/rankingPot";
import type {
  RankingPotPosition,
  RankingPotReadoutDoc,
  RankingPotReadoutResponse,
  RankingPotStatusResponse,
} from "@/api/types";
import { SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { JobRow } from "@/components/strategies/AiTrialPanel";
import { Badge, type BadgeTone } from "@/components/ui/Badge";
import { formatDate, formatDateTime, formatPct } from "@/lib/format";
import { money, number } from "@/lib/strategyFormat";
import { useAsync } from "@/lib/useAsync";

const STATE: Record<string, { badge: string; tone: BadgeTone }> = {
  shadow_only: { badge: "Shadow only — no orders", tone: "neutral" },
  executing: { badge: "Executing", tone: "ok" },
  halted_loss: { badge: "Halted — loss limit", tone: "risk" },
  halted_operator: { badge: "Halted — needs supervisor", tone: "risk" },
  winding_down: { badge: "Winding down", tone: "warn" },
  completed: { badge: "Completed", tone: "neutral" },
};

const JOB_LABEL: Record<string, string> = {
  ranking_pot_rebalance: "Monthly rebalance",
  ranking_pot_step: "Daily step (shadow + controls)",
  ranking_pot_execute: "Execute (in session)",
};

const POSITION_TONE: Record<RankingPotPosition["status"], BadgeTone> = {
  entry_pending: "info",
  open: "ok",
  closed: "neutral",
  expired: "neutral",
  refused: "warn",
  failed: "risk",
};

/** A readout ratio (`"0.0123"`) as a signed percent; null stays a dash. */
function ratio(value: string | null | undefined): string {
  return formatPct(number(value ?? null));
}

function scoreText(value: string | number | null): string {
  if (value === null) return "—";
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed.toFixed(3) : String(value);
}

function PnlText({ value, label }: { value: string | null; label: string }) {
  const pnl = number(value);
  const tone =
    pnl === null || pnl === 0
      ? "text-slate-500"
      : pnl > 0
        ? "text-emerald-700 dark:text-emerald-400"
        : "text-red-700 dark:text-red-400";
  return (
    <span className={tone}>
      {label} {money(value)}
    </span>
  );
}

function levelsText(position: RankingPotPosition): string {
  if (position.held_observed_at !== null) {
    const sl = position.held_no_stop_loss ? "none" : money(position.held_stop_loss);
    const tp = position.held_no_take_profit ? "none" : money(position.held_take_profit);
    return `broker SL ${sl} / TP ${tp} (seen ${formatDateTime(position.held_observed_at)})`;
  }
  if (position.sent_stop_loss !== null) {
    return `sent SL ${money(position.sent_stop_loss)} / TP ${money(position.sent_take_profit)} (broker levels not observed)`;
  }
  return "no order sent";
}

/** The §6 ticket: why this name, from what was known before the order. */
function Ticket({ position }: { position: RankingPotPosition }) {
  const t = position.ticket;
  const families = Object.entries(t.score.families);
  return (
    <details className="mt-1 text-xs text-slate-600 dark:text-slate-300">
      <summary className="cursor-pointer select-none">Why this trade</summary>
      {!position.ticket_verified ? (
        <p className="mt-1 font-medium text-red-700 dark:text-red-400">
          The stored ticket does not match its recorded hash — treat it as unverified.
        </p>
      ) : null}
      <dl className="mt-1 grid grid-cols-[max-content_1fr] gap-x-3 gap-y-0.5">
        <dt>Rank</dt>
        <dd>
          R-rank {t.r_rank ?? "—"} · F-rank {t.f_rank ?? "—"} (rule {t.rule_id})
        </dd>
        <dt>Score</dt>
        <dd className="tabular-nums">
          total {scoreText(t.score.total_score)} (raw {scoreText(t.score.raw_total)}, {t.score.penalties.length}{" "}
          penalties, {t.score.rewards.length} rewards{t.score.reconciles ? "" : ", does NOT reconcile"}) ·{" "}
          {t.score.model_version}
        </dd>
        <dt>Factors</dt>
        <dd className="tabular-nums">
          {families.length === 0 ? "—" : families.map(([name, value]) => `${name} ${scoreText(value)}`).join(" · ")}
        </dd>
        <dt>Thesis</dt>
        <dd>
          {t.thesis === null
            ? "none used"
            : `#${t.thesis.thesis_id}, ${t.thesis.age_days} days old (${t.thesis.model}, ${t.thesis.prompt_version})`}
        </dd>
        <dt>Planned</dt>
        <dd className="tabular-nums">
          {t.planned_levels.refused
            ? `no valid levels (${t.planned_levels.refused})`
            : `SL ${money(t.planned_levels.stop_loss ?? null)} / TP ${money(t.planned_levels.take_profit ?? null)}`}{" "}
          · ATR14 {scoreText(t.planned_levels.atr14)}
        </dd>
        <dt>Exit</dt>
        <dd>{String(t.exit_rule)}</dd>
      </dl>
    </details>
  );
}

function PositionRow({ position }: { position: RankingPotPosition }) {
  return (
    <li className="py-2 text-sm">
      <div className="flex flex-wrap items-baseline justify-between gap-2">
        <span className="flex items-baseline gap-2">
          <span className="w-12 text-xs text-slate-500">slot {position.slot}</span>
          <span className="font-semibold">{position.symbol ?? `#${position.instrument_id}`}</span>
          <Badge tone={POSITION_TONE[position.status]}>{position.status.replace("_", " ")}</Badge>
          {position.exit_reason ? (
            <span className="text-xs text-amber-700 dark:text-amber-400">
              exit {position.exit_session ? formatDate(position.exit_session) : ""}: {position.exit_reason}
            </span>
          ) : null}
        </span>
        <span className="tabular-nums text-slate-700 dark:text-slate-200">
          {position.amount !== null ? `${money(position.amount)} at ask ${money(position.ask)} · ` : ""}
          {levelsText(position)}
          {position.unrealized_pnl_usd !== null ? (
            <>
              {" · "}
              <PnlText value={position.unrealized_pnl_usd} label="open" />
              {position.marked_on ? ` (mark ${formatDate(position.marked_on)})` : ""}
            </>
          ) : null}
          {position.realized_pnl_usd !== null ? (
            <>
              {" · "}
              <PnlText value={position.realized_pnl_usd} label="booked" />
            </>
          ) : null}
          {position.funding_reason && position.status === "refused" ? ` · ${position.funding_reason}` : ""}
        </span>
      </div>
      <Ticket position={position} />
    </li>
  );
}

function Readout({ view }: { view: RankingPotReadoutResponse }) {
  if (view.readout === null) {
    const text =
      view.reason === "invariant_violation"
        ? "The stored rows violate an invariant, so the figures are withheld (details in the server log)."
        : view.reason === "not_declared"
          ? "No declaration, so nothing to read out."
          : "No session has been stepped yet; the first readout follows the first rebalance's target session.";
    return (
      <p
        className={`mt-1 text-sm ${view.reason === "invariant_violation" ? "text-red-700 dark:text-red-400" : "text-slate-500"}`}
      >
        {text}
      </p>
    );
  }
  const r: RankingPotReadoutDoc = view.readout;
  const pf = r.lifecycles.profit_factor;
  const w = r.turnover_occupancy.window;
  const last = r.executed.activated ? r.executed.nav_vs_spy.last : null;
  return (
    <div className="mt-1 text-sm">
      {r.policy_drift ? (
        <p className="text-red-700 dark:text-red-400">
          The running code&apos;s policy hash differs from the declaration&apos;s (policy drift).
        </p>
      ) : null}
      <dl className="grid grid-cols-[max-content_1fr] gap-x-3 gap-y-1 tabular-nums">
        <dt className="text-slate-500">As of</dt>
        <dd>
          {formatDate(r.endpoint)} · {r.sessions} sessions (interim, not a look)
        </dd>
        <dt className="text-slate-500">Shadow trades</dt>
        <dd>
          {r.lifecycles.count} · mean {ratio(r.lifecycles.mean)} · median {ratio(r.lifecycles.median)} · profit factor{" "}
          {pf.value === null ? "— (no loser)" : scoreText(pf.value)} ({pf.wins}W / {pf.losses}L / {pf.flat} flat)
        </dd>
        <dt className="text-slate-500">Shadow NAV</dt>
        <dd>
          {ratio(r.variant.nav_return.shadow)} · without SL/TP {ratio(r.variant.nav_return.variant)}
        </dd>
        <dt className="text-slate-500">Turnover / month</dt>
        <dd>
          {ratio(w.shadow_turnover_mean)} · controls median {ratio(w.controls_median_turnover_mean)}
        </dd>
        <dt className="text-slate-500">Occupancy</dt>
        <dd>
          {ratio(w.shadow_occupancy)} · controls median {ratio(w.controls_median_occupancy)}
        </dd>
        <dt className="text-slate-500">Deflated Sharpe</dt>
        <dd>
          {r.deflated_sharpe.dsr !== null
            ? scoreText(r.deflated_sharpe.dsr)
            : `— (${r.deflated_sharpe.reason ?? "unavailable"})`}{" "}
          (descriptive)
        </dd>
        <dt className="text-slate-500">Executed vs SPY</dt>
        <dd>
          {!r.executed.activated
            ? "not activated"
            : last === null
              ? "no reconciled point yet"
              : `NAV ${scoreText(last.nav)} vs SPY ${scoreText(last.spy)} (${ratio(last.difference)}) at ${formatDate(last.snapshot_date)}`}
        </dd>
      </dl>
      <details className="mt-2 text-xs">
        <summary className="cursor-pointer select-none text-slate-500">Full readout</summary>
        <pre className="mt-1 max-h-96 overflow-auto bg-slate-50 p-2 dark:bg-slate-950">
          {JSON.stringify(r, null, 2)}
        </pre>
      </details>
    </div>
  );
}

/** Its own fetch, mounted only once a declaration exists: the readout walks every step row. */
function ReadoutSection() {
  const readout = useAsync(fetchRankingPotReadout, []);
  return (
    <>
      <h3 className="mt-4 text-xs font-semibold uppercase text-slate-500">Readout</h3>
      {readout.loading ? <SectionSkeleton rows={3} /> : null}
      {readout.error ? <SectionError onRetry={readout.refetch} /> : null}
      {readout.data ? <Readout view={readout.data} /> : null}
    </>
  );
}

function PotBody({ status }: { status: RankingPotStatusResponse }) {
  const decl = status.declaration;
  return (
    <>
      {decl === null ? (
        <p className="mt-2 text-sm text-slate-600 dark:text-slate-300">
          {status.build_complete
            ? "Built, not frozen yet. The supervisor freezes the declaration (shadow only), then activates it; until then the jobs below run and do nothing."
            : "Still being built: a declaration cannot be frozen until the build is complete."}
        </p>
      ) : decl.state_reason && (decl.state?.startsWith("halted_") || decl.state === "winding_down") ? (
        <p className="mt-2 text-sm text-red-700 dark:text-red-400">{decl.state_reason}</p>
      ) : null}
      {decl?.pot_capital ? (
        <p className="mt-1 text-sm tabular-nums text-slate-600 dark:text-slate-300">
          Pot capital {money(decl.pot_capital)}
        </p>
      ) : null}

      <ul className="mt-3 divide-y divide-slate-200 dark:divide-slate-800" aria-label="Pot jobs">
        {status.jobs.map((fire) => (
          <JobRow key={fire.job_name} label={JOB_LABEL[fire.job_name] ?? fire.job_name} fire={fire} />
        ))}
      </ul>

      {decl !== null ? (
        <>
          <h3 className="mt-4 text-xs font-semibold uppercase text-slate-500">Rebalances</h3>
          {status.rebalances.length === 0 ? (
            <p className="mt-1 text-sm text-slate-500">No rebalance has fired yet.</p>
          ) : (
            <ul className="mt-1 divide-y divide-slate-200 dark:divide-slate-800" aria-label="Pot rebalances">
              {status.rebalances.map((r) => (
                <li key={`${r.month}-${r.fired_at}`} className="flex flex-wrap items-center gap-2 py-1.5 text-sm">
                  <span className="w-28 tabular-nums">{formatDate(r.target_session)}</span>
                  <Badge tone={r.outcome === "decided" ? "ok" : r.outcome === "refused" ? "warn" : "neutral"}>
                    {r.outcome}
                    {r.refusal ? `: ${r.refusal}` : ""}
                  </Badge>
                  {r.entries_allowed === false ? (
                    <span className="text-xs text-slate-500">
                      no entries ({r.v1_active ? "AI trial v1 active" : (r.executed_state ?? "state")})
                    </span>
                  ) : null}
                </li>
              ))}
            </ul>
          )}

          {status.step?.refusal_reason ? (
            <p className="mt-2 text-xs text-slate-500">
              Last step refusal: {status.step.refusal_reason} for{" "}
              {status.step.refusal_session ? formatDate(status.step.refusal_session) : "—"}
              {status.step.latest_session ? ` · stepped through ${formatDate(status.step.latest_session)}` : ""}
            </p>
          ) : null}

          {status.looks.length > 0 ? (
            <>
              <h3 className="mt-4 text-xs font-semibold uppercase text-slate-500">Looks</h3>
              <ul className="mt-1 space-y-1 text-sm">
                {status.looks.map((look) => (
                  <li key={`${look.look_months}-${look.kind}`}>
                    {look.look_months} months ({formatDate(look.endpoint_session)}): {look.verdict}
                    {look.harm ? " · harm stop" : ""}
                    {look.reasons.length > 0 ? ` · ${look.reasons.join(", ")}` : ""}
                  </li>
                ))}
              </ul>
            </>
          ) : null}

          <h3 className="mt-4 text-xs font-semibold uppercase text-slate-500">Positions</h3>
          {status.held.length === 0 ? (
            <p className="mt-1 text-sm text-slate-500">No position is held or pending.</p>
          ) : (
            <ul className="mt-1 divide-y divide-slate-200 dark:divide-slate-800" aria-label="Pot positions">
              {status.held.map((p) => (
                <PositionRow key={p.lifecycle_id} position={p} />
              ))}
            </ul>
          )}

          {status.recent.length > 0 ? (
            <>
              <h3 className="mt-4 text-xs font-semibold uppercase text-slate-500">Recent (closed, refused, expired)</h3>
              <ul className="mt-1 divide-y divide-slate-200 dark:divide-slate-800" aria-label="Pot recent positions">
                {status.recent.map((p) => (
                  <PositionRow key={p.lifecycle_id} position={p} />
                ))}
              </ul>
            </>
          ) : null}

          <ReadoutSection />
        </>
      ) : null}
    </>
  );
}

/**
 * ranking-pot-v1 (#2842): the multi-factor ranking traded as a monthly 25-name
 * demo portfolio. Every position carries the ticket written before its order —
 * ranks, factor scores, thesis, planned levels — so each trade shows why.
 */
export function RankingPotPanel() {
  const status = useAsync(fetchRankingPotStatus, []);
  const state = status.data?.declaration?.state ? STATE[status.data.declaration.state] : null;
  return (
    <section
      aria-labelledby="ranking-pot"
      className="border border-slate-200 bg-white px-5 py-4 dark:border-slate-800 dark:bg-slate-900"
    >
      <h2 id="ranking-pot" className="flex items-center gap-2 text-sm font-semibold">
        Ranking pot (v1)
        {status.data && status.data.declaration === null ? <Badge tone="neutral">Not frozen</Badge> : null}
        {state ? <Badge tone={state.tone}>{state.badge}</Badge> : null}
        {status.data?.declaration ? (
          <span className="text-xs font-normal text-slate-500">
            declaration {status.data.declaration.declaration_id}
          </span>
        ) : null}
      </h2>
      {status.loading ? <SectionSkeleton rows={3} /> : null}
      {status.error ? <SectionError onRetry={status.refetch} /> : null}
      {status.data ? <PotBody status={status.data} /> : null}
    </section>
  );
}
