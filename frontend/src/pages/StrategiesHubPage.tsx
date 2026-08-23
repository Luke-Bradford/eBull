import { useSearchParams } from "react-router-dom";

import { StrategiesPage } from "@/pages/StrategiesPage";
import { StrategyPortfolioLens } from "@/pages/StrategyPortfolioLens";

/**
 * Strategies hub (#2868) — the fenced-off pot and the candidate pipeline are
 * two jobs on two datasets, and `/strategies` was rendering both at once: 2,134
 * lines, ~25 panels, a 667 KB payload, and the two facts that decide everything
 * (kill switch, assigned capital) buried among zeroed metrics.
 *
 * ⚠ This is the INVERSE of the `information-architecture` skill's consolidation
 * case, and deliberately so. That rule merges N routes rendering N lenses on ONE
 * dataset; here one route rendered two datasets that answer different questions.
 * Same principle read the other way — one surface per dataset — so the preset
 * mechanics (segmented control, `?view=` in the URL, deep-linkable) are reused
 * exactly as `ResearchHubPage` established them.
 *
 * `/portfolio` (the broker account) and `/research` (the instrument universe)
 * are taken by unrelated surfaces, hence presets under `/strategies` rather than
 * two new top-level routes. The sidebar stays one item.
 *
 * Landing lens = Portfolio: it is what the operator opens the page to see, and
 * the research lens is where they go deliberately.
 */
type ViewKey = "portfolio" | "research";

const VIEW_ORDER: ViewKey[] = ["portfolio", "research"];
const DEFAULT_VIEW: ViewKey = "portfolio";

const PRESETS: Record<ViewKey, { label: string; hint: string; Component: () => JSX.Element }> = {
  portfolio: {
    label: "Portfolio",
    hint: "The fenced-off pot: status, capital, positions",
    Component: StrategyPortfolioLens,
  },
  research: {
    label: "Research",
    hint: "Candidate strategies and their evidence",
    Component: StrategiesPage,
  },
};

function isViewKey(value: string | null): value is ViewKey {
  return value === "portfolio" || value === "research";
}

/**
 * Switch preset without dropping params the hub does not own.
 *
 * `setParams({ view: key })` REPLACES the whole query string. Neither lens owns
 * URL state today, so nothing is lost yet — but the mirror-image trap is a
 * settled rule already (`.claude/skills/frontend/information-architecture.md`,
 * "Lens components must PRESERVE the hub's `view` param", from #1917), and a
 * hub that clears a lens's filters on tab-switch is the same defect pointing
 * the other way. Set only our own key; leave the rest untouched.
 */
function selectView(
  setParams: ReturnType<typeof useSearchParams>[1],
  key: ViewKey,
): void {
  setParams((prev) => {
    const next = new URLSearchParams(prev);
    next.set("view", key);
    return next;
  });
}

export function StrategiesHubPage(): JSX.Element {
  const [params, setParams] = useSearchParams();
  const raw = params.get("view");
  const view: ViewKey = isViewKey(raw) ? raw : DEFAULT_VIEW;
  const ActiveLens = PRESETS[view].Component;

  return (
    <div className="space-y-6 pt-6">
      <header className="flex flex-wrap items-center justify-between gap-3">
        <h1 className="text-xl font-semibold">Automated strategies</h1>
        <div
          role="tablist"
          aria-label="Strategies view"
          className="inline-flex gap-0.5 rounded-lg border border-slate-200 bg-slate-50 p-0.5 dark:border-slate-800 dark:bg-slate-900/40"
        >
          {VIEW_ORDER.map((key) => {
            const preset = PRESETS[key];
            const selected = key === view;
            return (
              <button
                key={key}
                type="button"
                role="tab"
                aria-selected={selected}
                title={preset.hint}
                onClick={() => selectView(setParams, key)}
                className={[
                  "rounded-md px-3 py-1.5 text-sm font-medium transition-colors",
                  selected
                    ? "bg-white text-slate-900 dark:bg-slate-800 dark:text-slate-100"
                    : "text-slate-500 hover:text-slate-800 dark:text-slate-400 dark:hover:text-slate-200",
                ].join(" ")}
              >
                {preset.label}
              </button>
            );
          })}
        </div>
      </header>

      <ActiveLens />
    </div>
  );
}
