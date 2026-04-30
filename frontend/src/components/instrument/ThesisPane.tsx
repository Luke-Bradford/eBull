import { Pane } from "@/components/instrument/Pane";
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

function ThesisBody({ thesis }: { thesis: ThesisDetail }): JSX.Element {
  const breaks = thesis.break_conditions_json ?? [];
  return (
    <div className="space-y-3 text-sm">
      <div className="max-w-prose whitespace-pre-wrap text-slate-700">
        {thesis.memo_markdown}
      </div>
      {(thesis.base_value !== null ||
        thesis.bull_value !== null ||
        thesis.bear_value !== null) && (
        <dl className="grid grid-cols-3 gap-2 rounded bg-slate-50 dark:bg-slate-900/40 p-3 text-xs">
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
    </div>
  );
}
