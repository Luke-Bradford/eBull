import { Pane } from "@/components/instrument/Pane";
import { CriticVerdictBadge } from "@/components/theses/CriticVerdictBadge";
import { EmptyState } from "@/components/states/EmptyState";
import type { ThesisDetail } from "@/api/types";

export interface ThesisPaneProps {
  readonly thesis: ThesisDetail | null;
  readonly errored: boolean;
}

export function ThesisPane({
  thesis,
  errored,
}: ThesisPaneProps): JSX.Element | null {
  if (thesis === null && !errored) return null;

  return (
    <Pane title="Thesis">
      {errored ? (
        <EmptyState
          title="Thesis temporarily unavailable"
          description="Failed to fetch the latest thesis. Retry via the Generate thesis button in the strip above."
        />
      ) : (
        <ThesisBody thesis={thesis as ThesisDetail} />
      )}
    </Pane>
  );
}

/** Safe string extraction from the open critic_json payload. */
function criticString(critic: Record<string, unknown>, key: string): string | null {
  const v = critic[key];
  return typeof v === "string" && v.length > 0 ? v : null;
}

/** Safe string-array extraction from the open critic_json payload. */
function criticList(critic: Record<string, unknown>, key: string): string[] {
  const v = critic[key];
  if (!Array.isArray(v)) return [];
  return v.filter((x): x is string => typeof x === "string");
}

function ThesisBody({ thesis }: { thesis: ThesisDetail }): JSX.Element {
  const breaks = thesis.break_conditions_json ?? [];
  const critic = thesis.critic_json;
  const criticSummary = critic ? criticString(critic, "summary") : null;
  const criticRisks = critic ? criticList(critic, "key_risks") : [];
  const hasBuyZone =
    thesis.buy_zone_low !== null || thesis.buy_zone_high !== null;
  return (
    <div className="space-y-3 text-sm">
      <div className="max-w-prose whitespace-pre-wrap text-slate-700">
        {thesis.memo_markdown}
      </div>
      {(thesis.base_value !== null ||
        thesis.bull_value !== null ||
        thesis.bear_value !== null ||
        hasBuyZone) && (
        <dl className="grid grid-cols-4 gap-2 rounded bg-slate-50 dark:bg-slate-900/40 p-3 text-xs">
          <div>
            <dt className="text-slate-500">Bear</dt>
            <dd className="font-medium tabular-nums">
              {thesis.bear_value !== null ? thesis.bear_value : "—"}
            </dd>
          </div>
          <div>
            <dt className="text-slate-500">Base</dt>
            <dd className="font-medium tabular-nums">
              {thesis.base_value !== null ? thesis.base_value : "—"}
            </dd>
          </div>
          <div>
            <dt className="text-slate-500">Bull</dt>
            <dd className="font-medium tabular-nums">
              {thesis.bull_value !== null ? thesis.bull_value : "—"}
            </dd>
          </div>
          <div>
            <dt className="text-slate-500">Buy zone</dt>
            <dd className="font-medium tabular-nums">
              {hasBuyZone
                ? `${thesis.buy_zone_low ?? "—"} – ${thesis.buy_zone_high ?? "—"}`
                : "—"}
            </dd>
          </div>
        </dl>
      )}
      {breaks.length > 0 && (
        <div>
          <div className="mb-1 text-xs font-medium uppercase tracking-wider text-slate-500">
            Break conditions
          </div>
          <ul className="list-inside list-disc space-y-0.5 text-xs text-slate-600">
            {breaks.map((b, i) => (
              <li key={i}>{b}</li>
            ))}
          </ul>
        </div>
      )}
      {critic !== null && critic !== undefined && (
        <div className="rounded border border-slate-200 dark:border-slate-800 p-3">
          <div className="mb-1 flex items-center gap-2">
            <span className="text-xs font-medium uppercase tracking-wider text-slate-500">
              Critic
            </span>
            <CriticVerdictBadge
              verdict={criticString(critic, "verdict")}
            />
          </div>
          {criticSummary !== null && (
            <p className="max-w-prose text-xs text-slate-600 dark:text-slate-300">
              {criticSummary}
            </p>
          )}
          {criticRisks.length > 0 && (
            <ul className="mt-1 list-inside list-disc space-y-0.5 text-xs text-slate-600 dark:text-slate-400">
              {criticRisks.map((r, i) => (
                <li key={i}>{r}</li>
              ))}
            </ul>
          )}
        </div>
      )}
    </div>
  );
}
