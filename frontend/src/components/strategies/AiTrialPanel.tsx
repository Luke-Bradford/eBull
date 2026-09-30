import { fetchAiTrialStatus } from "@/api/aiTrial";
import type {
  AiTrialDecision,
  AiTrialExecution,
  AiTrialJobFire,
  AiTrialOpenLeg,
  AiTrialState,
  AiTrialStatusResponse,
} from "@/api/types";
import { SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { Badge, type BadgeTone } from "@/components/ui/Badge";
import { formatDate, formatDateTime } from "@/lib/format";
import { money, number } from "@/lib/strategyFormat";
import { useAsync } from "@/lib/useAsync";

const STATE: Record<AiTrialState, { badge: string; tone: BadgeTone }> = {
  not_declared: { badge: "Not started", tone: "neutral" },
  not_started: { badge: "Declared, not started", tone: "neutral" },
  active: { badge: "Running", tone: "ok" },
  halted_harm: { badge: "Halted — harm stop", tone: "risk" },
  halted_loss: { badge: "Halted — loss limit", tone: "risk" },
  halted_mandate: { badge: "Halted — mandate", tone: "risk" },
  halted_operator: { badge: "Halted — needs supervisor", tone: "risk" },
};

function codes(counts: Record<string, number>): string {
  return Object.entries(counts)
    .map(([code, n]) => `${code} ×${n}`)
    .join(", ");
}

/** Every decision state reads differently, and only `not_run` reads as a failure. */
function decisionText(decision: AiTrialDecision): { text: string; tone: BadgeTone } {
  switch (decision.state) {
    case "not_run":
      return { text: "Decision job did not run", tone: "risk" };
    case "deciding":
      return { text: "Deciding", tone: "info" };
    case "refused":
      return { text: `Run refused: ${decision.refusal_reason ?? "unknown"}`, tone: "warn" };
    case "abstained":
      return { text: "Model chose no trade", tone: "neutral" };
    case "no_valid_plan": {
      const why = codes(decision.decision_refusals);
      return { text: `Every decision refused${why ? ` (${why})` : ""}`, tone: "neutral" };
    }
    case "legs_published":
      return { text: `${decision.legs} legs published`, tone: "ok" };
  }
}

function executionText(item: AiTrialExecution): { text: string; tone: BadgeTone } {
  switch (item.state) {
    case "submitted":
      return { text: `${item.count} submitted`, tone: "ok" };
    case "refused":
      return { text: `${item.count} refused: ${item.reason ?? "unknown"}`, tone: "warn" };
    case "awaiting_execution":
      return { text: `${item.count} awaiting the execute job`, tone: "info" };
    case "not_run":
      return { text: `${item.count} never executed`, tone: "risk" };
  }
}

function JobRow({ label, fire }: { label: string; fire: AiTrialJobFire }) {
  return (
    <li className="py-1.5 text-sm">
      <span className="font-medium">{label}</span>
      <span className="text-slate-600 dark:text-slate-300">
        {" · last "}
        {fire.last_started_at ? (
          <>
            {formatDateTime(fire.last_started_at)} ({fire.last_status ?? "unknown"}
            {fire.last_note ? `: ${fire.last_note}` : ""})
          </>
        ) : (
          "never run"
        )}
        {" · next "}
        {formatDateTime(fire.next_fire_at)}
      </span>
    </li>
  );
}

function levels(stop: string | null, target: string | null, absent = "—"): string {
  return `SL ${stop === null ? absent : money(stop)} / TP ${target === null ? absent : money(target)}`;
}

function LegRow({ leg }: { leg: AiTrialOpenLeg }) {
  const pnl = number(leg.pnl_usd);
  return (
    <li className="flex flex-wrap items-baseline justify-between gap-2 py-2 text-sm">
      <span>
        <span className="font-semibold">{leg.symbol}</span>
        <span className="text-slate-500">
          {" "}
          · {leg.leg} · pair {leg.pair_seq} · {leg.trade_status}
        </span>
      </span>
      <span className="tabular-nums text-slate-700 dark:text-slate-200">
        entry {money(leg.entry_price)} ·{" "}
        {leg.broker_observed
          ? `broker ${levels(leg.broker_stop, leg.broker_target, "none")}`
          : `requested ${levels(leg.requested_stop, leg.requested_target)} (broker levels not observed)`}{" "}
        ·{" "}
        <span
          className={
            pnl === null || pnl === 0
              ? "text-slate-500"
              : pnl > 0
                ? "text-emerald-700 dark:text-emerald-400"
                : "text-red-700 dark:text-red-400"
          }
        >
          {pnl === null ? "P&L unmeasured" : money(leg.pnl_usd)}
        </span>
      </span>
    </li>
  );
}

function TrialBody({ status }: { status: AiTrialStatusResponse }) {
  const declared = status.declaration_id !== null;
  return (
    <>
      {status.state.startsWith("halted_") && status.state_reason ? (
        <p className="mt-2 text-sm text-red-700 dark:text-red-400">{status.state_reason}</p>
      ) : null}
      {!declared ? (
        <p className="mt-2 text-sm text-slate-600 dark:text-slate-300">
          No frozen declaration yet, so both jobs run and do nothing until the trial is started.
        </p>
      ) : null}

      <ul className="mt-3 divide-y divide-slate-200 dark:divide-slate-800" aria-label="Trial jobs">
        <JobRow label="Decision job (after the close)" fire={status.decision_job} />
        <JobRow label="Execute job (in session)" fire={status.execute_job} />
      </ul>

      {declared ? (
        <>
          <h3 className="mt-4 text-xs font-semibold uppercase text-slate-500">Sessions</h3>
          {status.sessions.length === 0 ? (
            <p className="mt-1 text-sm text-slate-500">No session has been decided yet; the first decision runs at the decision job's next fire above.</p>
          ) : (
            <ul className="mt-1 divide-y divide-slate-200 dark:divide-slate-800" aria-label="Trial sessions">
              {status.sessions.map((session) => {
                const decision = decisionText(session.decision);
                return (
                  <li key={session.session_date} className="flex flex-wrap items-center gap-2 py-1.5 text-sm">
                    <span className="w-28 tabular-nums">{formatDate(session.session_date)}</span>
                    <Badge tone={decision.tone}>{decision.text}</Badge>
                    {session.execution.map((item) => {
                      const execution = executionText(item);
                      return (
                        <Badge key={item.label} tone={execution.tone}>
                          {execution.text}
                        </Badge>
                      );
                    })}
                  </li>
                );
              })}
            </ul>
          )}

          <h3 className="mt-4 text-xs font-semibold uppercase text-slate-500">Open legs</h3>
          {status.open_legs.length === 0 ? (
            <p className="mt-1 text-sm text-slate-500">No trial leg is open.</p>
          ) : (
            <ul className="mt-1 divide-y divide-slate-200 dark:divide-slate-800">
              {status.open_legs.map((leg) => (
                <LegRow key={leg.strategy_trade_id} leg={leg} />
              ))}
            </ul>
          )}

          <h3 className="mt-4 text-xs font-semibold uppercase text-slate-500">Loss halt</h3>
          <ul className="mt-1 space-y-1 text-sm">
            {status.loss.map((item) => (
              <li key={item.leg} className="tabular-nums">
                <span className="font-medium">{item.leg}</span>: P&amp;L {money(item.pnl_usd)} · {money(item.headroom_usd)}{" "}
                left before the {money(item.limit_usd)} halt
                {item.unmeasured > 0 ? ` · ${item.unmeasured} trade(s) unmeasured` : ""}
              </li>
            ))}
          </ul>
        </>
      ) : null}
    </>
  );
}

/**
 * The AI-discretionary trial's readiness on `/invest` (#3514). A day without a
 * trial trade has many causes — not started, halted, the job did not run, the
 * model abstained, every decision or leg refused, legs awaiting the execute
 * fire — and each is named here from stored rows so none reads like another.
 */
export function AiTrialPanel() {
  const status = useAsync(fetchAiTrialStatus, []);
  const state = status.data ? STATE[status.data.state] : null;
  return (
    <section
      aria-labelledby="invest-ai-trial"
      className="border border-slate-200 bg-white px-5 py-4 dark:border-slate-800 dark:bg-slate-900"
    >
      <h2 id="invest-ai-trial" className="flex items-center gap-2 text-sm font-semibold">
        AI trial
        {state ? <Badge tone={state.tone}>{state.badge}</Badge> : null}
        {status.data?.declaration_id != null ? (
          <span className="text-xs font-normal text-slate-500">declaration {status.data.declaration_id}</span>
        ) : null}
      </h2>
      {status.loading ? <SectionSkeleton rows={3} /> : null}
      {status.error ? <SectionError onRetry={status.refetch} /> : null}
      {status.data ? <TrialBody status={status.data} /> : null}
    </section>
  );
}
