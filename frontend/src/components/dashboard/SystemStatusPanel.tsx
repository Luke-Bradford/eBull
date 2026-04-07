import { useEffect, useState } from "react";
import type {
  ConfigResponse,
  LayerHealthResponse,
  OverallStatus,
  SystemStatusResponse,
} from "@/api/types";
import { formatDateTime } from "@/lib/format";

interface KillSwitchSnapshot {
  active: boolean;
  reason: string | null;
  /** True iff the snapshot was taken from a fresh successful response. */
  fresh: boolean;
}

const OVERALL_TONE: Record<OverallStatus, string> = {
  ok: "bg-emerald-100 text-emerald-700",
  degraded: "bg-amber-100 text-amber-700",
  down: "bg-red-100 text-red-700",
};

const LAYER_TONE: Record<string, string> = {
  ok: "text-emerald-600",
  stale: "text-amber-600",
  empty: "text-slate-500",
  error: "text-red-600",
};

/**
 * Read-only system status panel.
 *
 * Surfaces overall health, kill-switch state, layer freshness, and a
 * compact view of trading flags. The kill switch toggle lives on the admin
 * page (#64), per the issue — this panel never mutates state.
 *
 * Both `system` and `config` are optional so a partial failure on either
 * endpoint still renders whatever the other provided.
 */
/**
 * Per-side render contract: each of the two endpoints (system / config) is
 * one of {loading, errored, resolved}. Slow or retrying endpoints must
 * never block the resolved side from rendering — see the round-3 review
 * finding on PR #89 about combined loading gates.
 */
export function SystemStatusPanel({
  system,
  config,
  systemLoading,
  configLoading,
  systemError,
  configError,
  onRetrySystem,
  onRetryConfig,
}: {
  system: SystemStatusResponse | null;
  config: ConfigResponse | null;
  systemLoading: boolean;
  configLoading: boolean;
  systemError: boolean;
  configError: boolean;
  onRetrySystem: () => void;
  onRetryConfig: () => void;
}) {
  // Safety-state visibility: the kill switch banner must not disappear
  // while one of its source endpoints is in flight or has errored. We
  // remember the last *confirmed* kill-switch snapshot from either source
  // and keep showing it (with a "stale" marker) until a fresh successful
  // response says otherwise. Surfacing the banner is fail-safe — if both
  // sources have ever reported `active=true`, the operator must keep
  // seeing it until a fresh `active=false` clears it.
  const [cachedKillSwitch, setCachedKillSwitch] = useState<KillSwitchSnapshot | null>(null);
  useEffect(() => {
    // Prefer system as the canonical source; fall back to config.
    const fresh =
      (system?.kill_switch ?? null) ?? (config?.kill_switch ?? null);
    if (fresh !== null) {
      setCachedKillSwitch({
        active: fresh.active,
        reason: fresh.reason,
        fresh: true,
      });
    }
  }, [system, config]);

  // Live or cached, whichever exists. The cached copy is marked stale
  // because the underlying endpoint may have errored or be retrying.
  const liveKillSwitch =
    system?.kill_switch ?? config?.kill_switch ?? null;
  const displayedKillSwitch: KillSwitchSnapshot | null = liveKillSwitch
    ? { active: liveKillSwitch.active, reason: liveKillSwitch.reason, fresh: true }
    : cachedKillSwitch !== null
      ? { ...cachedKillSwitch, fresh: false }
      : null;

  return (
    <div className="space-y-4">
      {systemError ? (
        <PartialError label="/system/status" onRetry={onRetrySystem} />
      ) : null}
      {configError ? <PartialError label="/config" onRetry={onRetryConfig} /> : null}
      <div className="flex flex-wrap items-center gap-2">
        {systemLoading ? (
          <span className="inline-block h-4 w-16 animate-pulse rounded bg-slate-100" />
        ) : system ? (
          <span
            className={`inline-block rounded px-2 py-0.5 text-xs font-medium ${
              OVERALL_TONE[system.overall_status]
            }`}
          >
            {system.overall_status.toUpperCase()}
          </span>
        ) : (
          <span className="text-xs text-slate-400">status unavailable</span>
        )}
        {configLoading ? (
          <span className="inline-block h-4 w-32 animate-pulse rounded bg-slate-100" />
        ) : config ? (
          <>
            <FlagPill label="auto trading" on={config.runtime.enable_auto_trading} />
            <FlagPill label="live trading" on={config.runtime.enable_live_trading} />
            <span className="text-xs text-slate-500">
              {config.app_env} · {config.etoro_env}
            </span>
          </>
        ) : null}
      </div>

      {displayedKillSwitch?.active && (
        <div
          role="alert"
          className="rounded-md border border-red-300 bg-red-50 px-3 py-2 text-sm text-red-700"
        >
          <strong>Kill switch active.</strong>{" "}
          {displayedKillSwitch.reason ?? "No reason recorded."}{" "}
          <span className="text-xs text-red-600">
            Toggle from the admin page when ready to resume.
          </span>
          {!displayedKillSwitch.fresh && (
            <span className="ml-1 text-[10px] uppercase text-red-500">(stale — refreshing)</span>
          )}
        </div>
      )}

      {systemLoading ? (
        <div className="space-y-2">
          <div className="h-3 w-24 animate-pulse rounded bg-slate-100" />
          <div className="h-3 w-full animate-pulse rounded bg-slate-100" />
          <div className="h-3 w-full animate-pulse rounded bg-slate-100" />
        </div>
      ) : system ? (
        <>
          <div>
            <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">
              Data layers
            </div>
            <ul className="mt-2 space-y-1 text-sm">
              {system.layers.length === 0 ? (
                <li className="text-xs text-slate-400">No layers reported.</li>
              ) : (
                system.layers.map((l) => <LayerRow key={l.layer} layer={l} />)
              )}
            </ul>
          </div>
          <div>
            <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">
              Recent jobs
            </div>
            <ul className="mt-2 space-y-1 text-sm">
              {system.jobs.length === 0 ? (
                <li className="text-xs text-slate-400">No jobs reported.</li>
              ) : (
                system.jobs.map((j) => (
                  <li key={j.name} className="flex justify-between gap-4">
                    <span className="truncate text-slate-700">{j.name}</span>
                    <span className="shrink-0 text-xs text-slate-500">
                      {j.last_status ?? "never run"} · {formatDateTime(j.last_finished_at)}
                    </span>
                  </li>
                ))
              )}
            </ul>
          </div>
          <div className="text-[10px] text-slate-400">
            Checked {formatDateTime(system.checked_at)}
          </div>
        </>
      ) : null}
    </div>
  );
}

function LayerRow({ layer }: { layer: LayerHealthResponse }) {
  const tone = LAYER_TONE[layer.status] ?? "text-slate-600";
  return (
    <li className="flex justify-between gap-4">
      <span className="text-slate-700">{layer.layer}</span>
      <span className={`shrink-0 text-xs ${tone}`}>
        {layer.status}
        {layer.age_seconds !== null ? ` · ${Math.round(layer.age_seconds)}s old` : ""}
      </span>
    </li>
  );
}

function PartialError({ label, onRetry }: { label: string; onRetry: () => void }) {
  return (
    <div
      role="alert"
      className="flex items-center justify-between rounded border border-red-200 bg-red-50 px-2 py-1 text-xs text-red-700"
    >
      <span>{label} failed to load.</span>
      <button
        type="button"
        onClick={onRetry}
        className="rounded border border-red-300 bg-white px-2 py-0.5 text-[10px] font-medium text-red-700 hover:bg-red-100"
      >
        Retry
      </button>
    </div>
  );
}

function FlagPill({ label, on }: { label: string; on: boolean }) {
  return (
    <span
      className={`inline-block rounded px-1.5 py-0.5 text-[10px] font-medium ${
        on ? "bg-emerald-100 text-emerald-700" : "bg-slate-100 text-slate-500"
      }`}
    >
      {label}: {on ? "on" : "off"}
    </span>
  );
}
