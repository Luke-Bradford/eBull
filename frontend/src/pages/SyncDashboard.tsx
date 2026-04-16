/**
 * Sync orchestrator dashboard (issue #260 Phase 3).
 *
 * Three sections:
 *   1. Status banner — "all current" / "N layers stale" / "running N/M"
 *      with a "Sync Now" button (POST /sync, scope='full', trigger='manual').
 *   2. Data layer grid — 15 layers grouped by tier (0=sources, 1=raw,
 *      2=computed, 3=decisions). Each card shows freshness, last
 *      success, last duration, dependencies.
 *   3. Recent sync runs — most-recent N (default 20).
 *
 * Auto-refresh: polls every 10s when a sync is running, every 60s idle.
 */

import { useCallback, useEffect, useMemo, useState } from "react";

import { ApiError } from "@/api/client";
import {
  fetchSyncLayers,
  fetchSyncRuns,
  fetchSyncStatus,
  triggerSync,
} from "@/api/sync";
import type { SyncLayer, SyncRun } from "@/api/sync";
import {
  Section,
  SectionError,
  SectionSkeleton,
} from "@/components/dashboard/Section";
import { formatDateTime } from "@/lib/format";
import { useAsync } from "@/lib/useAsync";

const TIER_LABEL: Record<number, string> = {
  0: "Tier 0 · Sources",
  1: "Tier 1 · Raw data",
  2: "Tier 2 · Computed",
  3: "Tier 3 · Decisions",
};

const STATUS_TONE: Record<string, string> = {
  complete: "text-emerald-600",
  partial: "text-amber-600",
  failed: "text-red-600",
  running: "text-sky-600",
};

function formatDuration(seconds: number | null): string {
  if (seconds === null) return "—";
  if (seconds < 60) return `${seconds}s`;
  const m = Math.floor(seconds / 60);
  const s = seconds % 60;
  return `${m}m ${s}s`;
}

export function SyncDashboard() {
  const layers = useAsync(fetchSyncLayers, []);
  const status = useAsync(fetchSyncStatus, []);
  const runs = useAsync(() => fetchSyncRuns(20), []);

  const refetchAll = useCallback(() => {
    layers.refetch();
    status.refetch();
    runs.refetch();
    // Deliberate: useAsync refetch callbacks are stable refs.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [layers.refetch, status.refetch, runs.refetch]);

  const isRunning = status.data?.is_running ?? false;
  const interval = isRunning ? 10_000 : 60_000;
  useEffect(() => {
    const id = window.setInterval(refetchAll, interval);
    return () => window.clearInterval(id);
  }, [refetchAll, interval]);

  const [triggerState, setTriggerState] = useState<
    | { kind: "idle" }
    | { kind: "running" }
    | { kind: "error"; message: string }
    | { kind: "queued"; syncRunId: number }
  >({ kind: "idle" });

  // Clear `queued` banner once the status poll confirms the trigger
  // took effect (is_running=true). Otherwise the banner would stick
  // around indefinitely if the sync ran fast and finished before the
  // next poll tick.
  useEffect(() => {
    if (triggerState.kind === "queued" && isRunning) {
      setTriggerState({ kind: "idle" });
    }
  }, [triggerState, isRunning]);

  const handleSyncNow = useCallback(async () => {
    setTriggerState({ kind: "running" });
    try {
      const result = await triggerSync({ scope: "full" });
      setTriggerState({ kind: "queued", syncRunId: result.sync_run_id });
      refetchAll();
    } catch (err) {
      const message =
        err instanceof ApiError
          ? err.status === 409
            ? "Sync already running"
            : err.status === 503
              ? "Sync orchestrator disabled"
              : `Failed (HTTP ${err.status})`
          : "Failed";
      setTriggerState({ kind: "error", message });
    }
  }, [refetchAll]);

  const layerList: SyncLayer[] = layers.data?.layers ?? [];
  const stale = layerList.filter((l) => !l.is_fresh).length;
  const runList: SyncRun[] = runs.data?.runs ?? [];

  const banner = useMemo(() => {
    if (isRunning && status.data?.current_run) {
      const r = status.data.current_run;
      return {
        text: `Sync in progress — ${r.layers_done}/${r.layers_planned} layers complete`,
        tone: "text-sky-600",
      };
    }
    if (stale === 0 && layerList.length > 0) {
      return { text: "All data current", tone: "text-emerald-600" };
    }
    if (stale > 0) {
      return {
        text: `${stale} of ${layerList.length} layers stale — sync recommended`,
        tone: "text-amber-600",
      };
    }
    return { text: "—", tone: "text-slate-500" };
  }, [isRunning, status.data, stale, layerList.length]);

  const layersByTier = useMemo(() => {
    const groups: Record<number, SyncLayer[]> = { 0: [], 1: [], 2: [], 3: [] };
    for (const layer of layerList) {
      groups[layer.tier]?.push(layer);
    }
    return groups;
  }, [layerList]);

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-semibold text-slate-800">Data sync</h1>
      </div>

      {/* --- Status banner --- */}
      <Section
        title="Status"
        action={
          <button
            type="button"
            onClick={handleSyncNow}
            disabled={
              // Disabled from click until either (a) the backend confirms
              // is_running=true via the next /sync/status poll, or (b)
              // the trigger returned an error and we're back in idle.
              // Without the "queued" guard, a second click between the
              // POST returning 202 and the status poll could fire a
              // second concurrent trigger.
              triggerState.kind === "running" ||
              triggerState.kind === "queued" ||
              isRunning
            }
            className="rounded bg-sky-600 px-3 py-1 text-sm font-medium text-white hover:bg-sky-700 disabled:bg-slate-300"
          >
            {triggerState.kind === "running"
              ? "Triggering…"
              : isRunning
                ? "Running"
                : "Sync now"}
          </button>
        }
      >
        <div className="flex items-center gap-3">
          <span className={`text-sm font-medium ${banner.tone}`}>
            {banner.text}
          </span>
          {triggerState.kind === "error" && (
            <span className="text-sm text-red-600">
              {triggerState.message}
            </span>
          )}
          {triggerState.kind === "queued" && (
            <span className="text-sm text-slate-500">
              Queued as run #{triggerState.syncRunId}
            </span>
          )}
        </div>
      </Section>

      {/* --- Data layer grid --- */}
      <Section title="Data layers">
        {layers.loading ? (
          <SectionSkeleton rows={15} />
        ) : layers.error !== null ? (
          <SectionError onRetry={layers.refetch} />
        ) : (
          <div className="space-y-4">
            {[0, 1, 2, 3].map((tier) => {
              const group = layersByTier[tier] ?? [];
              if (group.length === 0) return null;
              return (
                <div key={tier}>
                  <div className="mb-2 text-xs font-semibold uppercase tracking-wide text-slate-500">
                    {TIER_LABEL[tier]}
                  </div>
                  <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-3">
                    {group.map((layer) => (
                      <LayerCard key={layer.name} layer={layer} />
                    ))}
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </Section>

      {/* --- Recent sync runs --- */}
      <Section title="Recent sync runs">
        {runs.loading ? (
          <SectionSkeleton rows={5} />
        ) : runs.error !== null ? (
          <SectionError onRetry={runs.refetch} />
        ) : runList.length === 0 ? (
          <p className="text-sm text-slate-500">No sync runs yet.</p>
        ) : (
          <table className="w-full text-sm">
            <thead className="text-left text-xs font-semibold uppercase text-slate-500">
              <tr>
                <th className="py-2">Started</th>
                <th>Scope</th>
                <th>Trigger</th>
                <th>Status</th>
                <th>Layers</th>
                <th>Duration</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100">
              {runList.map((r) => (
                <tr key={r.sync_run_id}>
                  <td className="py-2 text-slate-600">
                    {formatDateTime(r.started_at)}
                  </td>
                  <td className="text-slate-700">
                    {r.scope}
                    {r.scope_detail ? ` · ${r.scope_detail}` : ""}
                  </td>
                  <td className="text-slate-500">{r.trigger}</td>
                  <td>
                    <span
                      className={`font-medium ${STATUS_TONE[r.status] ?? "text-slate-700"}`}
                    >
                      {r.status}
                    </span>
                  </td>
                  <td className="text-slate-600">
                    {r.layers_done}✓ {r.layers_failed}✗ {r.layers_skipped}⊘
                    <span className="text-slate-400">
                      {" "}
                      / {r.layers_planned}
                    </span>
                  </td>
                  <td className="text-slate-500">
                    {r.finished_at
                      ? formatDuration(
                          Math.round(
                            (new Date(r.finished_at).getTime() -
                              new Date(r.started_at).getTime()) /
                              1000,
                          ),
                        )
                      : "running…"}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </Section>
    </div>
  );
}

function LayerCard({ layer }: { layer: SyncLayer }) {
  const border = layer.is_fresh ? "border-emerald-200" : "border-amber-200";
  const dot = layer.is_fresh ? "bg-emerald-500" : "bg-amber-500";
  return (
    <div
      className={`rounded border ${border} bg-white p-3 text-sm shadow-sm`}
    >
      <div className="flex items-center justify-between">
        <span className="font-medium text-slate-800">{layer.display_name}</span>
        <span className="flex items-center gap-1">
          <span className="sr-only">
            {layer.is_fresh ? "fresh" : "stale"}
          </span>
          <span className={`h-2 w-2 rounded-full ${dot}`} aria-hidden />
        </span>
      </div>
      <p
        className="mt-1 truncate text-xs text-slate-500"
        title={layer.freshness_detail}
      >
        {layer.freshness_detail}
      </p>
      <div className="mt-2 flex flex-wrap gap-2 text-xs text-slate-400">
        {layer.last_success_at && (
          <span title={layer.last_success_at}>
            last {formatDateTime(layer.last_success_at)}
          </span>
        )}
        {layer.last_duration_seconds !== null && (
          <span>· {formatDuration(layer.last_duration_seconds)}</span>
        )}
        {!layer.is_blocking && (
          <span className="rounded bg-slate-100 px-1.5 py-0.5 text-slate-500">
            non-blocking
          </span>
        )}
      </div>
      {layer.dependencies.length > 0 && (
        <p className="mt-1 text-xs text-slate-400">
          deps: {layer.dependencies.join(", ")}
        </p>
      )}
    </div>
  );
}
