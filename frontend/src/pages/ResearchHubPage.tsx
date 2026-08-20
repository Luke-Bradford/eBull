import { useSearchParams } from "react-router-dom";

import { RankingsPage } from "@/pages/RankingsPage";
import { InstrumentsPage } from "@/pages/InstrumentsPage";
import { ThesesPage } from "@/pages/ThesesPage";
import { RecommendationsPage } from "@/pages/RecommendationsPage";

/**
 * Research hub (#1917) — one surface for the four lenses on the instrument
 * universe (Instruments / Rankings / Theses / Recommendations were four
 * top-level pages differing only in which columns + filter they show).
 *
 * A view preset is a MODE, not an additive filter, so it renders as a segmented
 * control, not chips (see `frontend/information-architecture` skill). The preset
 * lives in the URL (`/research?view=…`) so it is deep-linkable and back-button
 * correct; the old routes redirect here preserving their query strings, so a
 * `/theses?held&stale` bookmark lands on `?view=theses&held&stale` and the
 * Theses lens reads those params unchanged.
 *
 * Each preset renders the existing page component verbatim — behaviour-preserving
 * per lens. The Theses lens is a generation queue (status/staleness), NOT a second
 * ranked table. The Map render mode is decoupled from #1912 (its endpoint is
 * unmerged): the toggle is present-but-disabled on the table lenses, table-only.
 */
type ViewKey = "ranked" | "universe" | "theses" | "actioned";

interface Preset {
  label: string;
  hint: string;
  Component: () => JSX.Element;
  mappable: boolean;
}

const PRESETS: Record<ViewKey, Preset> = {
  ranked: { label: "Ranked", hint: "Universe by score", Component: RankingsPage, mappable: true },
  universe: { label: "Universe", hint: "All tradable instruments", Component: InstrumentsPage, mappable: true },
  theses: { label: "Theses", hint: "Thesis generation queue + staleness", Component: ThesesPage, mappable: false },
  actioned: { label: "Actioned", hint: "Portfolio-manager recommendations", Component: RecommendationsPage, mappable: false },
};

// Landing lens = Ranked (the scored universe is the daily driver).
const VIEW_ORDER: ViewKey[] = ["ranked", "universe", "theses", "actioned"];
const DEFAULT_VIEW: ViewKey = "ranked";

function isViewKey(v: string | null): v is ViewKey {
  return v === "ranked" || v === "universe" || v === "theses" || v === "actioned";
}

export function ResearchHubPage(): JSX.Element {
  const [params, setParams] = useSearchParams();
  const raw = params.get("view");
  const view: ViewKey = isViewKey(raw) ? raw : DEFAULT_VIEW;
  const active = PRESETS[view];
  const ActiveLens = active.Component;

  return (
    <div className="space-y-4 pt-6">
      <div className="flex flex-wrap items-center justify-between gap-3">
        {/* Segmented control — one lens at a time. Switching a preset drops the
            previous lens's own params (they are lens-specific); a deep link that
            arrives with them still works because the target lens reads the URL. */}
        <div
          role="tablist"
          aria-label="Research view"
          className="inline-flex gap-0.5 rounded-lg border border-slate-200 bg-slate-50 p-0.5 dark:border-slate-800 dark:bg-slate-900/40"
        >
          {VIEW_ORDER.map((key) => {
            const p = PRESETS[key];
            const selected = key === view;
            return (
              <button
                key={key}
                type="button"
                role="tab"
                aria-selected={selected}
                title={p.hint}
                onClick={() => setParams({ view: key })}
                className={[
                  "rounded-md px-3 py-1.5 text-sm font-medium transition-colors",
                  selected
                    ? "bg-white text-slate-900 dark:bg-slate-800 dark:text-slate-100"
                    : "text-slate-500 hover:text-slate-800 dark:text-slate-400 dark:hover:text-slate-200",
                ].join(" ")}
              >
                {p.label}
              </button>
            );
          })}
        </div>
        {active.mappable ? <RenderModeToggle /> : null}
      </div>

      <ActiveLens />
    </div>
  );
}

/**
 * Table | Map render-mode affordance. Map is decoupled from #1912 (endpoint
 * unmerged) — present but disabled so the hub ships table-only now and the map
 * drops in later with no re-architecture (#1917 / information-architecture skill).
 */
function RenderModeToggle(): JSX.Element {
  return (
    <div
      role="group"
      aria-label="Render mode"
      className="inline-flex gap-0.5 rounded-lg border border-slate-200 bg-slate-50 p-0.5 text-sm dark:border-slate-800 dark:bg-slate-900/40"
    >
      <button
        type="button"
        aria-pressed="true"
        className="rounded-md bg-white px-3 py-1.5 font-medium text-slate-900 dark:bg-slate-800 dark:text-slate-100"
      >
        Table
      </button>
      <button
        type="button"
        disabled
        title="Ownership / geography map — coming soon (#1912)"
        className="cursor-not-allowed rounded-md px-3 py-1.5 font-medium text-slate-400 disabled:opacity-100 dark:text-slate-600"
      >
        Map
      </button>
    </div>
  );
}
