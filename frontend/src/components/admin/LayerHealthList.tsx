/**
 * Per-layer health list for the Admin page orchestrator details.
 *
 * Reads `v2.layers` (canonical per-layer list added in A.5 chunk 0).
 * Shows state pill (Healthy / Catching up / Needs attention / Disabled),
 * relative last_updated timestamp, plain-language SLA, and a ⋯ menu
 * whose Enable / Disable item emits `onToggle(name, enabled)` for the
 * parent to wire to the backend endpoint (chunk 2).
 */
import { useState } from "react";

import type { LayerEntry, LayerStateStr } from "@/api/types";


export interface LayerHealthListProps {
  readonly layers: readonly LayerEntry[];
  readonly onToggle: (layer: string, enabled: boolean) => void;
}


type Pill = "healthy" | "catching_up" | "needs_attention" | "disabled";


function pillFor(state: LayerStateStr): Pill {
  if (state === "healthy") return "healthy";
  if (state === "disabled") return "disabled";
  if (state === "action_needed" || state === "secret_missing") return "needs_attention";
  return "catching_up";
}


const PILL_LABEL: Record<Pill, string> = {
  healthy: "Healthy",
  catching_up: "Catching up",
  needs_attention: "Needs attention",
  disabled: "Disabled",
};


const PILL_CLASS: Record<Pill, string> = {
  healthy: "bg-emerald-100 text-emerald-800",
  catching_up: "bg-amber-100 text-amber-800",
  needs_attention: "bg-red-100 text-red-800",
  disabled: "bg-slate-200 text-slate-600",
};


function relativeAgo(iso: string | null): string {
  if (iso === null) return "never";
  const diff = Date.now() - new Date(iso).getTime();
  const minutes = Math.floor(diff / 60_000);
  if (minutes < 1) return "just now";
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}


export function LayerHealthList({ layers, onToggle }: LayerHealthListProps): JSX.Element {
  const [menuOpen, setMenuOpen] = useState<string | null>(null);

  return (
    <ul className="divide-y divide-slate-100">
      {layers.map((entry) => {
        const pill = pillFor(entry.state);
        const isDisabled = entry.state === "disabled";
        return (
          <li
            key={entry.layer}
            id={`admin-layer-${entry.layer}`}
            className={`flex items-start justify-between py-2 ${isDisabled ? "opacity-50" : ""}`}
          >
            <div className="flex-1">
              <div className="flex items-center gap-2">
                <span className="font-medium text-slate-800">{entry.display_name}</span>
                <span
                  aria-label={`${entry.layer} state`}
                  className={`inline-block rounded-full px-2 py-0.5 text-xs font-medium ${PILL_CLASS[pill]}`}
                >
                  {PILL_LABEL[pill]}
                </span>
                <span className="text-xs text-slate-500">Updated {relativeAgo(entry.last_updated)}</span>
              </div>
              <div className="mt-1 text-xs text-slate-600">{entry.plain_language_sla}</div>
            </div>
            <div className="relative ml-4">
              <button
                type="button"
                aria-label={`${entry.layer} actions`}
                onClick={() => setMenuOpen(menuOpen === entry.layer ? null : entry.layer)}
                className="rounded border border-slate-200 bg-white px-2 py-1 text-xs text-slate-700 hover:bg-slate-50"
              >
                ⋯
              </button>
              {menuOpen === entry.layer ? (
                <div className="absolute right-0 top-full z-10 mt-1 w-40 rounded border border-slate-200 bg-white shadow">
                  <button
                    type="button"
                    onClick={() => {
                      onToggle(entry.layer, isDisabled);
                      setMenuOpen(null);
                    }}
                    className="block w-full px-3 py-1 text-left text-xs text-slate-700 hover:bg-slate-50"
                  >
                    {isDisabled ? "Enable layer" : "Disable layer"}
                  </button>
                </div>
              ) : null}
            </div>
          </li>
        );
      })}
    </ul>
  );
}
