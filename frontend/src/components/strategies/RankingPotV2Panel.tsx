import { fetchRankingPotV2Readout, fetchRankingPotV2Status } from "@/api/rankingPot";
import type {
  RankingPotV2EntryReasons,
  RankingPotV2Holding,
  RankingPotV2Look,
  RankingPotV2ReadoutResponse,
  RankingPotV2StatusResponse,
} from "@/api/types";
import { SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { JobRow } from "@/components/strategies/AiTrialPanel";
import { Badge, type BadgeTone } from "@/components/ui/Badge";
import { formatDate, formatPct } from "@/lib/format";
import { money, number } from "@/lib/strategyFormat";
import { useAsync } from "@/lib/useAsync";

const STATE: Record<string, { badge: string; tone: BadgeTone }> = {
  shadow_only: { badge: "Shadow only — no orders", tone: "neutral" },
  winding_down: { badge: "Winding down", tone: "warn" },
  completed: { badge: "Completed", tone: "neutral" },
};

const JOB_LABEL: Record<string, string> = {
  ranking_pot_v2_rebalance: "Monthly rebalance",
  ranking_pot_v2_step: "Daily step (shadow, controls, v1 reference)",
};

const VERDICT: Record<string, { text: string; tone: BadgeTone }> = {
  research_pass: { text: "Research pass", tone: "ok" },
  not_passed: { text: "Not passed", tone: "warn" },
  unevaluable: { text: "Unevaluable", tone: "neutral" },
};

/** An exact `p/q` (or decimal) string as a 3-dp number; anything unparseable is shown as stored. */
function exact(value: string | null): string {
  if (value === null) return "—";
  const [p, q] = value.split("/");
  const parsed = q === undefined ? Number(p) : Number(p) / Number(q);
  return Number.isFinite(parsed) ? parsed.toFixed(3) : value;
}

function ratio(value: string | null | undefined): string {
  return formatPct(number(value ?? null));
}

function condition(label: string, value: boolean | null): string {
  return `${label} ${value === null ? "unevaluable" : value ? "met" : "not met"}`;
}

/** §5: why the shadow entered this name — its three percentiles, the composite, DTC and insider evidence. */
function Reasons({ reasons }: { reasons: RankingPotV2EntryReasons | null }) {
  if (reasons === null) {
    return <p className="mt-1 text-xs text-slate-500">No entry reasons stored for this name.</p>;
  }
  const opportunistic = reasons.pairs.length;
  return (
    <details className="mt-1 text-xs text-slate-600 dark:text-slate-300">
      <summary className="cursor-pointer select-none">Why this name</summary>
      <dl className="mt-1 grid grid-cols-[max-content_1fr] gap-x-3 gap-y-0.5 tabular-nums">
        <dt>Composite</dt>
        <dd>
          {exact(reasons.composite)} = mean of score {exact(reasons.u_score)} · days-to-cover {exact(reasons.u_dtc)} ·
          insider {exact(reasons.u_ins)} (percentiles within the ranked universe)
        </dd>
        <dt>Days to cover</dt>
        <dd>
          {reasons.dtc !== null
            ? `${exact(reasons.dtc)} days`
            : `missing (${reasons.dtc_missing ?? "no FINRA row"})`}
          {reasons.dtc_settlement_date ? ` · FINRA settlement ${formatDate(reasons.dtc_settlement_date)}` : ""}
        </dd>
        <dt>Insider buying</dt>
        <dd>
          {opportunistic === 0
            ? "no opportunistic purchase in the look-back"
            : `${opportunistic} opportunistic insider ${opportunistic === 1 ? "pair" : "pairs"} · Form 4 ${reasons.accessions.join(", ")}`}
        </dd>
      </dl>
    </details>
  );
}

function HoldingRow({ holding }: { holding: RankingPotV2Holding }) {
  const invested = number(holding.invested);
  const value = number(holding.value);
  const ret = invested !== null && value !== null && invested !== 0 ? value / invested - 1 : null;
  return (
    <li className="py-2 text-sm">
      <div className="flex flex-wrap items-baseline justify-between gap-2">
        <span className="flex items-baseline gap-2">
          <span className="w-12 text-xs text-slate-500">{holding.slot === null ? "" : `slot ${holding.slot}`}</span>
          <span className="font-semibold">{holding.symbol ?? `#${holding.instrument_id}`}</span>
          <Badge tone={holding.state === "held" ? "ok" : "info"}>{holding.state}</Badge>
        </span>
        <span className="tabular-nums text-slate-700 dark:text-slate-200">
          {holding.state === "pending"
            ? `enters ${formatDate(holding.entry_session)}`
            : `entered ${formatDate(holding.entry_session)} at ${money(holding.entry_fill)} · last ${money(holding.last_close)}` +
              ` (${formatPct(ret)}) · SL ${money(holding.stop_loss)} / TP ${money(holding.take_profit)}`}
        </span>
      </div>
      <Reasons reasons={holding.reasons} />
    </li>
  );
}

function LookRow({ look }: { look: RankingPotV2Look }) {
  const verdict = VERDICT[look.v2_verdict] ?? { text: look.v2_verdict, tone: "neutral" as BadgeTone };
  return (
    <li className="flex flex-wrap items-center gap-2">
      <span>
        {look.look_months} months ({formatDate(look.endpoint)})
      </span>
      {look.invalidated_by !== null ? (
        <Badge tone="risk">Invalidated{look.invalidated_note ? `: ${look.invalidated_note}` : ""}</Badge>
      ) : (
        <Badge tone={verdict.tone}>{verdict.text}</Badge>
      )}
      <span className="text-xs text-slate-500">
        {condition("v1 reference", look.reference_condition)} · {condition("turnover", look.turnover_condition)}
        {look.harm ? " · harm stop" : ""}
        {look.reasons.length > 0 ? ` · ${look.reasons.join(", ")}` : ""}
      </span>
    </li>
  );
}

function Readout({ view }: { view: RankingPotV2ReadoutResponse }) {
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
  const r = view.readout;
  const pf = r.lifecycles.profit_factor;
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
          {pf.value === null ? "— (no loser)" : exact(pf.value)} ({pf.wins}W / {pf.losses}L / {pf.flat} flat)
        </dd>
        <dt className="text-slate-500">vs v1 reference</dt>
        <dd>
          NAV {ratio(r.reference.nav_return.shadow)} vs {ratio(r.reference.nav_return.reference)} (
          {ratio(r.reference.nav_return.shadow_minus_reference)}) · T {exact(r.reference.t.shadow)} vs{" "}
          {exact(r.reference.t.reference)}
        </dd>
        <dt className="text-slate-500">Deflated Sharpe</dt>
        <dd>
          {r.deflated_sharpe.dsr !== null
            ? exact(r.deflated_sharpe.dsr)
            : `— (${r.deflated_sharpe.reason ?? "unavailable"})`}{" "}
          (descriptive)
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
  const readout = useAsync(fetchRankingPotV2Readout, []);
  return (
    <>
      <h3 className="mt-4 text-xs font-semibold uppercase text-slate-500">Readout</h3>
      {readout.loading ? <SectionSkeleton rows={3} /> : null}
      {readout.error ? <SectionError onRetry={readout.refetch} /> : null}
      {readout.data ? <Readout view={readout.data} /> : null}
    </>
  );
}

function PotBody({ status }: { status: RankingPotV2StatusResponse }) {
  const decl = status.declaration;
  return (
    <>
      <p className="mt-2 text-sm text-slate-600 dark:text-slate-300">
        v1&apos;s ranking with opportunistic insider buying and FINRA days-to-cover added to the order, run as a
        shadow book beside random-attachment controls and the plain v1 order. It places no orders.
      </p>
      {decl === null ? (
        <p className="mt-1 text-sm text-slate-600 dark:text-slate-300">
          {status.build_complete
            ? "Built, not frozen yet. The supervisor freezes the declaration; until then the jobs below run and do nothing."
            : "Still being built: a declaration cannot be frozen until the build is complete."}
        </p>
      ) : decl.state_reason && decl.state === "winding_down" ? (
        <p className="mt-1 text-sm text-amber-700 dark:text-amber-400">{decl.state_reason}</p>
      ) : null}

      <ul className="mt-3 divide-y divide-slate-200 dark:divide-slate-800" aria-label="Pot v2 jobs">
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
            <ul className="mt-1 divide-y divide-slate-200 dark:divide-slate-800" aria-label="Pot v2 rebalances">
              {status.rebalances.map((r) => (
                <li key={`${r.month}-${r.fired_at}`} className="flex flex-wrap items-center gap-2 py-1.5 text-sm">
                  <span className="w-28 tabular-nums">{formatDate(r.target_session)}</span>
                  <Badge tone={r.outcome === "decided" ? "ok" : r.outcome === "refused" ? "warn" : "neutral"}>
                    {r.outcome}
                    {r.refusal ? `: ${r.refusal}` : ""}
                  </Badge>
                </li>
              ))}
            </ul>
          )}

          {status.step?.refusal_reason ? (
            <p className="mt-2 text-xs text-slate-500">
              Last step refusal: {status.step.refusal_reason} for{" "}
              {status.step.refusal_session ? formatDate(status.step.refusal_session) : "—"}
            </p>
          ) : null}

          <h3 className="mt-4 text-xs font-semibold uppercase text-slate-500">Looks</h3>
          {status.looks_withheld ? (
            <p className="mt-1 text-sm text-red-700 dark:text-red-400">
              A stored look does not decode, so the looks are withheld (details in the server log).
            </p>
          ) : status.looks.length === 0 ? (
            <p className="mt-1 text-sm text-slate-500">No look is due yet.</p>
          ) : (
            <ul className="mt-1 space-y-1 text-sm">
              {status.looks.map((look) => (
                <LookRow key={look.look_id} look={look} />
              ))}
            </ul>
          )}

          <h3 className="mt-4 text-xs font-semibold uppercase text-slate-500">
            Shadow book
            {status.shadow_nav !== null && status.step?.latest_session ? (
              <span className="ml-2 font-normal normal-case tabular-nums">
                NAV {exact(status.shadow_nav)} at {formatDate(status.step.latest_session)}
              </span>
            ) : null}
          </h3>
          {status.holdings.length === 0 ? (
            <p className="mt-1 text-sm text-slate-500">The shadow book holds nothing yet.</p>
          ) : (
            <ul className="mt-1 divide-y divide-slate-200 dark:divide-slate-800" aria-label="Pot v2 shadow holdings">
              {status.holdings.map((h) => (
                <HoldingRow key={`${h.state}-${h.instrument_id}`} holding={h} />
              ))}
            </ul>
          )}

          <ReadoutSection />
        </>
      ) : null}
    </>
  );
}

/**
 * ranking-pot-v2 (#3592): v1's ranking plus opportunistic insider purchases and
 * days-to-cover, measured shadow-only against random attachment and against v1
 * itself. Every shadow holding shows why it was entered.
 */
export function RankingPotV2Panel() {
  const status = useAsync(fetchRankingPotV2Status, []);
  const state = status.data?.declaration?.state ? STATE[status.data.declaration.state] : null;
  return (
    <section
      aria-labelledby="ranking-pot-v2"
      className="border border-slate-200 bg-white px-5 py-4 dark:border-slate-800 dark:bg-slate-900"
    >
      <h2 id="ranking-pot-v2" className="flex items-center gap-2 text-sm font-semibold">
        Ranking pot (v2, shadow)
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
