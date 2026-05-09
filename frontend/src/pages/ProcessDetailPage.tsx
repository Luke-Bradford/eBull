/**
 * ProcessDetailPage — drill-in route /admin/processes/:id (#1076 / #1064).
 *
 * Three tabs per spec §"Information architecture":
 *   - Overview: current row state, cadence, watermark, action buttons.
 *   - History: last 7 days of runs (`GET /runs?days=7`).
 *   - Errors:  grouped error classes of the latest terminal run.
 *
 * Trigger / cancel buttons reuse the same envelope contracts as the
 * ProcessesTable; modal state is owned here so a 409 from the detail
 * page surfaces inline rather than escaping back to the table view.
 */

import { useCallback, useEffect, useState } from "react";
import { Link, useParams } from "react-router-dom";

import { ApiError } from "@/api/client";
import {
  cancelProcess,
  fetchOrchestratorDag,
  fetchProcess,
  fetchProcessRuns,
  triggerProcess,
} from "@/api/processes";
import type {
  CancelMode,
  OrchestratorDagLayerResponse,
  OrchestratorDagResponse,
  OrchestratorDagSyncRunResponse,
  ProcessRowResponse,
  ProcessRunSummaryResponse,
} from "@/api/types";
import {
  Section,
  SectionError,
  SectionSkeleton,
} from "@/components/dashboard/Section";
import { Modal } from "@/components/ui/Modal";
import {
  REASON_TOOLTIP,
  STATUS_VISUAL,
  reasonTooltip,
} from "@/components/admin/processStatus";
import { useAsync } from "@/lib/useAsync";
import { formatDateTime } from "@/lib/format";

type TabKey = "overview" | "history" | "errors" | "dag";

const ORCHESTRATOR_FULL_SYNC_ID = "orchestrator_full_sync";

export function ProcessDetailPage() {
  const params = useParams<{ id: string }>();
  const id = params.id ?? "";
  const [tab, setTab] = useState<TabKey>("overview");
  const [busy, setBusy] = useState(false);
  const [triggerError, setTriggerError] = useState<unknown>(null);
  const [cancelError, setCancelError] = useState<unknown>(null);
  const [showFullWash, setShowFullWash] = useState(false);
  const [showCancel, setShowCancel] = useState(false);

  // useAsync captures fn via a ref — fresh arrow per render is fine.
  const detail = useAsync(() => fetchProcess(id), [id]);
  const runs = useAsync(() => fetchProcessRuns(id, 7), [id]);
  // DAG drill-in fetcher (#1078). Gated on BOTH (process_id is the
  // orchestrator) AND (active tab is DAG) so non-orchestrator detail
  // pages never call /dag and the orchestrator page does not fetch
  // the DAG until the operator opens the tab. `preserveOnRefetch`
  // keeps the rendered grid stable while a tick re-fetches (the
  // useAsync default is false — Codex pre-impl review M2).
  const isOrchestrator = id === ORCHESTRATOR_FULL_SYNC_ID;
  const dag = useAsync<OrchestratorDagResponse | null>(
    () =>
      tab === "dag" && isOrchestrator
        ? fetchOrchestratorDag(id)
        : Promise.resolve(null),
    [id, tab, isOrchestrator],
    { preserveOnRefetch: true },
  );

  // Reset tab to overview if currently on DAG and the route param
  // changes to a non-orchestrator id (operator clicked a different
  // process row from the table). Codex pre-impl review M-r2-2.
  useEffect(() => {
    if (tab === "dag" && !isOrchestrator) {
      setTab("overview");
    }
  }, [tab, isOrchestrator]);

  // Extract the refetch refs as local const bindings so ESLint can
  // see their identity and verify the dep array — `useAsync` wraps
  // refetch in `useCallback([], [])` (see useAsync.test.ts which
  // pins that invariant) so these references are stable across
  // renders. Listing the full `detail` / `runs` / `dag` hook-return
  // objects in deps would re-derive `refetchAll` every render and
  // propagate the identity churn through every `handleX` below —
  // PR #1077 review WARNING / PREVENTION-log #1209.
  const refetchDetail = detail.refetch;
  const refetchRuns = runs.refetch;
  const refetchDag = dag.refetch;

  const refetchAll = useCallback(() => {
    refetchDetail();
    refetchRuns();
    refetchDag();
  }, [refetchDetail, refetchRuns, refetchDag]);

  const handleIterate = useCallback(async () => {
    setTriggerError(null);
    setBusy(true);
    try {
      await triggerProcess(id, { mode: "iterate" });
      refetchAll();
    } catch (err) {
      setTriggerError(err);
      if (!(err instanceof ApiError))
        console.error("triggerProcess(iterate) failed", err);
    } finally {
      setBusy(false);
    }
  }, [id, refetchAll]);

  const handleFullWashConfirmed = useCallback(async () => {
    setTriggerError(null);
    setBusy(true);
    try {
      await triggerProcess(id, { mode: "full_wash" });
      setShowFullWash(false);
      refetchAll();
    } catch (err) {
      // On error, dismiss the modal and surface the structured 409
      // reason in the ActionBar — keeping the modal up alongside an
      // out-of-context error pill would be confusing. Operator
      // remediation lives on the row (see ApiError.detail.reason →
      // reasonTooltip mapping in ActionBar).
      setTriggerError(err);
      setShowFullWash(false);
      if (!(err instanceof ApiError))
        console.error("triggerProcess(full_wash) failed", err);
    } finally {
      setBusy(false);
    }
  }, [id, refetchAll]);

  const handleCancelConfirmed = useCallback(
    async (mode: CancelMode) => {
      setCancelError(null);
      setBusy(true);
      try {
        await cancelProcess(id, { mode });
        setShowCancel(false);
        refetchAll();
      } catch (err) {
        // Same pattern as full-wash above — dismiss modal, surface
        // the reason in the ActionBar tooltip via reasonTooltip.
        setCancelError(err);
        setShowCancel(false);
        if (!(err instanceof ApiError))
          console.error("cancelProcess failed", err);
      } finally {
        setBusy(false);
      }
    },
    [id, refetchAll],
  );

  return (
    <div className="space-y-4 pt-6">
      <div className="flex items-center justify-between">
        <div>
          <Link
            to="/admin"
            className="text-xs text-blue-700 hover:underline dark:text-blue-300"
          >
            ← Admin
          </Link>
          <h1 className="text-xl font-semibold text-slate-800 dark:text-slate-100">
            {detail.data?.display_name ?? id}
          </h1>
          {detail.data ? (
            <p className="text-xs text-slate-500 dark:text-slate-400">
              {detail.data.process_id} · {detail.data.mechanism} ·{" "}
              {detail.data.lane}
            </p>
          ) : null}
        </div>
        {detail.data ? (
          <ActionBar
            row={detail.data}
            busy={busy}
            triggerError={triggerError}
            cancelError={cancelError}
            onIterate={handleIterate}
            onFullWash={() => setShowFullWash(true)}
            onCancel={() => setShowCancel(true)}
          />
        ) : null}
      </div>

      <TabBar tab={tab} setTab={setTab} showDag={isOrchestrator} />

      <Section title={tabTitle(tab)}>
        {tab === "overview" ? (
          <OverviewTab
            row={detail.data}
            loading={detail.loading}
            error={detail.error}
            onRetry={detail.refetch}
          />
        ) : tab === "history" ? (
          <HistoryTab
            runs={runs.data}
            loading={runs.loading}
            error={runs.error}
            onRetry={runs.refetch}
          />
        ) : tab === "errors" ? (
          <ErrorsTab
            row={detail.data}
            loading={detail.loading}
            error={detail.error}
            onRetry={detail.refetch}
          />
        ) : (
          <DagTab
            payload={dag.data}
            loading={dag.loading}
            error={dag.error}
            onRetry={dag.refetch}
          />
        )}
      </Section>

      {showFullWash && detail.data ? (
        <FullWashConfirmDialog
          row={detail.data}
          busy={busy}
          onCancel={() => setShowFullWash(false)}
          onConfirm={handleFullWashConfirmed}
        />
      ) : null}

      {showCancel && detail.data ? (
        <CancelConfirmDialog
          row={detail.data}
          busy={busy}
          onCancel={() => setShowCancel(false)}
          onConfirm={handleCancelConfirmed}
        />
      ) : null}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Action bar (Iterate / Full-wash / Cancel)
// ---------------------------------------------------------------------------

function ActionBar({
  row,
  busy,
  triggerError,
  cancelError,
  onIterate,
  onFullWash,
  onCancel,
}: {
  row: ProcessRowResponse;
  busy: boolean;
  triggerError: unknown;
  cancelError: unknown;
  onIterate: () => void;
  onFullWash: () => void;
  onCancel: () => void;
}) {
  const watermarkTooltip = row.watermark?.human ?? "no resume cursor";
  return (
    <div className="flex flex-col items-end gap-1">
      <div className="flex items-center gap-2">
        <button
          type="button"
          onClick={onIterate}
          disabled={!row.can_iterate || busy}
          title={row.can_iterate ? watermarkTooltip : "Iterate is not available right now."}
          className="rounded border border-slate-300 bg-white px-3 py-1 text-sm font-medium text-slate-700 hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-50 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-200 dark:hover:bg-slate-800/40"
        >
          Iterate
        </button>
        <button
          type="button"
          onClick={onFullWash}
          disabled={!row.can_full_wash || busy}
          title={
            row.can_full_wash
              ? "Reset watermark and re-fetch from epoch (typed-name confirm required)."
              : "Full-wash is not available right now."
          }
          className="rounded border border-red-300 bg-white px-3 py-1 text-sm font-medium text-red-700 hover:bg-red-50 disabled:cursor-not-allowed disabled:opacity-50 dark:border-red-900 dark:bg-slate-900 dark:text-red-300 dark:hover:bg-red-950/40"
        >
          Full-wash
        </button>
        <button
          type="button"
          onClick={onCancel}
          disabled={!row.can_cancel || busy}
          title={row.can_cancel ? "Cooperative cancel — the worker stops at its next checkpoint." : "No active run to cancel."}
          className="rounded border border-amber-300 bg-white px-3 py-1 text-sm font-medium text-amber-700 hover:bg-amber-50 disabled:cursor-not-allowed disabled:opacity-50 dark:border-amber-900 dark:bg-slate-900 dark:text-amber-300 dark:hover:bg-amber-950/40"
        >
          Cancel
        </button>
      </div>
      {triggerError ? (
        <div
          role="status"
          className="text-xs text-red-700 dark:text-red-300"
          title={reasonTooltip(triggerError)}
        >
          trigger rejected
        </div>
      ) : null}
      {cancelError ? (
        <div
          role="status"
          className="text-xs text-red-700 dark:text-red-300"
          title={reasonTooltip(cancelError)}
        >
          cancel rejected
        </div>
      ) : null}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Tabs
// ---------------------------------------------------------------------------

function TabBar({
  tab,
  setTab,
  showDag,
}: {
  tab: TabKey;
  setTab: (t: TabKey) => void;
  showDag: boolean;
}) {
  const tabs: { key: TabKey; label: string }[] = [
    { key: "overview", label: "Overview" },
    { key: "history", label: "History" },
    { key: "errors", label: "Errors" },
    ...(showDag ? [{ key: "dag" as TabKey, label: "DAG" }] : []),
  ];
  return (
    <div role="tablist" className="flex gap-1 border-b border-slate-200 dark:border-slate-800">
      {tabs.map((t) => {
        const active = tab === t.key;
        return (
          <button
            key={t.key}
            type="button"
            role="tab"
            aria-selected={active}
            onClick={() => setTab(t.key)}
            className={`-mb-px rounded-t border border-b-0 px-3 py-1 text-sm font-medium ${
              active
                ? "border-slate-300 bg-white text-slate-800 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100"
                : "border-transparent bg-transparent text-slate-500 hover:bg-slate-50 dark:text-slate-400 dark:hover:bg-slate-800/40"
            }`}
          >
            {t.label}
          </button>
        );
      })}
    </div>
  );
}

function tabTitle(tab: TabKey): string {
  return tab === "overview"
    ? "Overview"
    : tab === "history"
      ? "Run history (last 7 days)"
      : tab === "errors"
        ? "Errors (latest terminal run)"
        : "DAG (latest sync run)";
}

function OverviewTab({
  row,
  loading,
  error,
  onRetry,
}: {
  row: ProcessRowResponse | null;
  loading: boolean;
  error: unknown;
  onRetry: () => void;
}) {
  if (loading) return <SectionSkeleton rows={4} />;
  if (error) return <SectionError onRetry={onRetry} />;
  if (!row) return <p className="text-sm text-slate-500">No detail available.</p>;
  const visual = STATUS_VISUAL[row.status];
  return (
    <div className="space-y-3 text-sm">
      <div className="flex items-center gap-2">
        <span className="text-xs uppercase tracking-wide text-slate-500 dark:text-slate-400">
          Status
        </span>
        <span
          className={`inline-flex items-center rounded-full border px-1.5 py-0.5 text-[10px] font-medium uppercase tracking-wide ${visual.toneClass}`}
        >
          {visual.label}
        </span>
      </div>
      <KeyValueRow label="Cadence" value={row.cadence_human} />
      <KeyValueRow
        label="Next fire"
        value={row.next_fire_at ? formatDateTime(row.next_fire_at) : "—"}
      />
      <KeyValueRow
        label="Watermark"
        value={row.watermark ? row.watermark.human : "no resume cursor"}
      />
      {row.last_run ? (
        <KeyValueRow
          label="Last run"
          value={`${formatDateTime(row.last_run.finished_at)} · ${row.last_run.status} · ${row.last_run.rows_processed ?? "?"} rows`}
        />
      ) : (
        <KeyValueRow label="Last run" value="never" />
      )}
      {row.active_run ? (
        <KeyValueRow
          label="Active run"
          value={`run #${row.active_run.run_id} · started ${formatDateTime(row.active_run.started_at)}`}
        />
      ) : null}
    </div>
  );
}

function KeyValueRow({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-baseline gap-2">
      <span className="w-28 text-xs uppercase tracking-wide text-slate-500 dark:text-slate-400">
        {label}
      </span>
      <span className="text-sm text-slate-700 dark:text-slate-200">{value}</span>
    </div>
  );
}

function HistoryTab({
  runs,
  loading,
  error,
  onRetry,
}: {
  runs: ProcessRunSummaryResponse[] | null;
  loading: boolean;
  error: unknown;
  onRetry: () => void;
}) {
  if (loading) return <SectionSkeleton rows={5} />;
  if (error) return <SectionError onRetry={onRetry} />;
  if (!runs || runs.length === 0) {
    return (
      <p className="text-sm text-slate-500 dark:text-slate-400">
        No runs in the last 7 days.
      </p>
    );
  }
  return (
    <table className="w-full text-left text-sm">
      <thead className="text-xs uppercase tracking-wide text-slate-500 dark:text-slate-400">
        <tr>
          <th className="px-2 py-2">Started</th>
          <th className="px-2 py-2">Finished</th>
          <th className="px-2 py-2">Duration</th>
          <th className="px-2 py-2">Rows</th>
          <th className="px-2 py-2">Status</th>
        </tr>
      </thead>
      <tbody className="divide-y divide-slate-100 dark:divide-slate-800">
        {runs.map((r) => (
          <tr key={r.run_id} className="text-sm">
            <td className="px-2 py-2 text-slate-700 dark:text-slate-200">
              {formatDateTime(r.started_at)}
            </td>
            <td className="px-2 py-2 text-slate-600 dark:text-slate-400">
              {formatDateTime(r.finished_at)}
            </td>
            <td className="px-2 py-2 tabular-nums text-slate-600 dark:text-slate-400">
              {r.duration_seconds.toFixed(1)}s
            </td>
            <td className="px-2 py-2 tabular-nums text-slate-600 dark:text-slate-400">
              {r.rows_processed ?? "—"}
            </td>
            <td className="px-2 py-2 text-xs text-slate-700 dark:text-slate-200">
              {r.status}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function ErrorsTab({
  row,
  loading,
  error,
  onRetry,
}: {
  row: ProcessRowResponse | null;
  loading: boolean;
  error: unknown;
  onRetry: () => void;
}) {
  if (loading) return <SectionSkeleton rows={3} />;
  if (error) return <SectionError onRetry={onRetry} />;
  if (!row) return <p className="text-sm text-slate-500">No detail available.</p>;
  if (row.last_n_errors.length === 0) {
    return (
      <p className="text-sm text-slate-500 dark:text-slate-400">
        No errors on the latest terminal run.
      </p>
    );
  }
  return (
    <ul className="space-y-2">
      {row.last_n_errors.map((e) => (
        <li
          key={e.error_class}
          className="rounded border border-red-200 bg-red-50 p-2 text-sm dark:border-red-900 dark:bg-red-950/40"
        >
          <div className="flex items-baseline justify-between">
            <span className="font-medium text-red-800 dark:text-red-200">
              {e.error_class}
            </span>
            <span className="text-xs text-red-700 dark:text-red-300">
              ×{e.count} · last seen {formatDateTime(e.last_seen_at)}
            </span>
          </div>
          {e.sample_subject ? (
            <div className="mt-1 text-xs text-red-700 dark:text-red-300">
              {e.sample_subject}
            </div>
          ) : null}
          <pre className="mt-1 whitespace-pre-wrap break-words text-xs text-red-900 dark:text-red-100">
            {e.sample_message}
          </pre>
        </li>
      ))}
    </ul>
  );
}

// ---------------------------------------------------------------------------
// DAG drill-in tab (#1078 — orchestrator_full_sync only)
// ---------------------------------------------------------------------------

const LAYER_STATUS_TONE: Record<string, string> = {
  pending: "border-slate-300 bg-slate-50 text-slate-600 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-300",
  running: "border-sky-300 bg-sky-50 text-sky-700 dark:border-sky-900 dark:bg-sky-950/40 dark:text-sky-300",
  complete: "border-emerald-300 bg-emerald-50 text-emerald-700 dark:border-emerald-900 dark:bg-emerald-950/40 dark:text-emerald-300",
  failed: "border-red-300 bg-red-50 text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300",
  skipped: "border-slate-300 bg-slate-50 text-slate-500 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-400",
  partial: "border-amber-300 bg-amber-50 text-amber-700 dark:border-amber-900 dark:bg-amber-950/40 dark:text-amber-300",
  cancelled: "border-slate-300 bg-slate-50 text-slate-500 line-through dark:border-slate-700 dark:bg-slate-900 dark:text-slate-400",
};

function DagTab({
  payload,
  loading,
  error,
  onRetry,
}: {
  payload: OrchestratorDagResponse | null;
  loading: boolean;
  error: unknown;
  onRetry: () => void;
}) {
  if (loading) return <SectionSkeleton rows={6} />;
  if (error) return <SectionError onRetry={onRetry} />;
  if (!payload || payload.sync_run === null) {
    return (
      <p className="text-sm text-slate-500 dark:text-slate-400">
        No recent sync run. Trigger a sync from the action bar above to populate the DAG.
      </p>
    );
  }
  return (
    <div className="space-y-4">
      <DagRunSummary run={payload.sync_run} />
      <DagLayerTable layers={payload.layers} />
    </div>
  );
}

function DagRunSummary({ run }: { run: OrchestratorDagSyncRunResponse }) {
  return (
    <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
      <Cell label="Run id" value={`#${run.sync_run_id}`} />
      <Cell
        label="Scope"
        value={`${run.scope}${run.scope_detail ? ` · ${run.scope_detail}` : ""}`}
      />
      <Cell label="Trigger" value={run.trigger} />
      <Cell label="Status" value={run.status} />
      <Cell label="Started" value={formatDateTime(run.started_at)} />
      <Cell
        label="Finished"
        value={run.finished_at ? formatDateTime(run.finished_at) : "—"}
      />
      <Cell
        label="Layers"
        value={`${run.layers_done}/${run.layers_planned} done · ${run.layers_failed} failed · ${run.layers_skipped} skipped`}
      />
      <Cell
        label="Cancel signal"
        value={run.cancel_requested_at ? formatDateTime(run.cancel_requested_at) : "—"}
      />
    </div>
  );
}

function Cell({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded border border-slate-200 bg-slate-50 p-2 text-sm dark:border-slate-800 dark:bg-slate-900/40">
      <div className="text-xs uppercase tracking-wide text-slate-500 dark:text-slate-400">
        {label}
      </div>
      <div className="mt-0.5 truncate text-slate-700 dark:text-slate-200" title={value}>
        {value}
      </div>
    </div>
  );
}

function DagLayerTable({ layers }: { layers: OrchestratorDagLayerResponse[] }) {
  if (layers.length === 0) {
    return (
      <p className="text-sm text-slate-500 dark:text-slate-400">
        No layer rows recorded for this run.
      </p>
    );
  }
  // Group by tier so the operator sees source / raw / computed /
  // decisions ordering. ``null`` tier (defensive, registry drift)
  // sinks to the end.
  const sorted = [...layers].sort((a, b) => {
    const at = a.tier ?? 99;
    const bt = b.tier ?? 99;
    if (at !== bt) return at - bt;
    return a.display_name.localeCompare(b.display_name);
  });
  return (
    <table className="w-full text-left text-sm">
      <thead className="text-xs uppercase tracking-wide text-slate-500 dark:text-slate-400">
        <tr>
          <th className="px-2 py-2">Layer</th>
          <th className="px-2 py-2">Tier</th>
          <th className="px-2 py-2">Status</th>
          <th className="px-2 py-2">Items</th>
          <th className="px-2 py-2">Finished</th>
          <th className="px-2 py-2">Detail</th>
        </tr>
      </thead>
      <tbody className="divide-y divide-slate-100 dark:divide-slate-800">
        {sorted.map((layer) => {
          const tone =
            LAYER_STATUS_TONE[layer.status] ??
            "border-slate-300 bg-slate-50 text-slate-600 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-300";
          const items =
            layer.items_total === null && layer.items_done === null
              ? "—"
              : `${layer.items_done ?? 0}/${layer.items_total ?? "?"}`;
          const detail =
            layer.error_message ??
            layer.skip_reason ??
            (layer.error_category ? `category: ${layer.error_category}` : "—");
          return (
            <tr key={layer.name} className="text-sm align-top">
              <td className="px-2 py-2 text-slate-700 dark:text-slate-200">
                <div className="font-medium">{layer.display_name}</div>
                <div className="text-xs text-slate-500 dark:text-slate-400">
                  {layer.name}
                </div>
              </td>
              <td className="px-2 py-2 text-slate-600 dark:text-slate-400">
                {layer.tier === null ? "—" : `T${layer.tier}`}
              </td>
              <td className="px-2 py-2">
                <span
                  className={`inline-flex items-center rounded-full border px-1.5 py-0.5 text-[10px] font-medium uppercase tracking-wide ${tone}`}
                >
                  {layer.status}
                </span>
              </td>
              <td className="px-2 py-2 tabular-nums text-slate-600 dark:text-slate-400">
                {items}
              </td>
              <td className="px-2 py-2 text-xs text-slate-500 dark:text-slate-400">
                {layer.finished_at ? formatDateTime(layer.finished_at) : "—"}
              </td>
              <td className="px-2 py-2 text-xs text-slate-600 dark:text-slate-400">
                <span className="break-words" title={detail}>
                  {detail}
                </span>
              </td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

// ---------------------------------------------------------------------------
// Confirm dialogs (mirror ProcessesTable's contracts)
// ---------------------------------------------------------------------------

function FullWashConfirmDialog({
  row,
  busy,
  onCancel,
  onConfirm,
}: {
  row: ProcessRowResponse;
  busy: boolean;
  onCancel: () => void;
  onConfirm: () => void;
}) {
  const [typed, setTyped] = useState("");
  const matches = typed === row.display_name;
  return (
    <Modal isOpen={true} onRequestClose={onCancel} labelledBy="detail-fw-title">
      <h2
        id="detail-fw-title"
        className="text-sm font-semibold text-slate-800 dark:text-slate-100"
      >
        Confirm full-wash
      </h2>
      <p className="mt-2 text-sm text-slate-700 dark:text-slate-300">
        Full-wash resets the watermark for{" "}
        <span className="font-medium">{row.display_name}</span> and re-fetches
        from epoch.
      </p>
      <p className="mt-2 text-xs text-slate-500 dark:text-slate-400">
        Type the process name exactly to enable the confirm button.
      </p>
      <label className="mt-3 block text-xs font-medium text-slate-700 dark:text-slate-200">
        Process name
        <input
          type="text"
          value={typed}
          onChange={(e) => setTyped(e.target.value)}
          autoFocus
          aria-label="Process name confirmation"
          placeholder={row.display_name}
          className="mt-1 w-full rounded border border-slate-300 bg-white px-2 py-1 text-sm font-mono text-slate-800 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-100"
        />
      </label>
      <div className="mt-4 flex justify-end gap-2">
        <button
          type="button"
          onClick={onCancel}
          className="rounded border border-slate-300 bg-white px-3 py-1 text-xs font-medium text-slate-700 hover:bg-slate-50 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-200 dark:hover:bg-slate-800/40"
        >
          Cancel
        </button>
        <button
          type="button"
          onClick={onConfirm}
          disabled={!matches || busy}
          className="rounded border border-red-400 bg-red-600 px-3 py-1 text-xs font-medium text-white hover:bg-red-700 disabled:cursor-not-allowed disabled:opacity-50 dark:border-red-700 dark:bg-red-700 dark:hover:bg-red-800"
        >
          {busy ? "Triggering…" : "Full-wash"}
        </button>
      </div>
    </Modal>
  );
}

function CancelConfirmDialog({
  row,
  busy,
  onCancel,
  onConfirm,
}: {
  row: ProcessRowResponse;
  busy: boolean;
  onCancel: () => void;
  onConfirm: (mode: CancelMode) => void;
}) {
  // Codex pre-push BLOCKING: a closed `<details>` keeps the terminate
  // `<button>` tabbable in the DOM. The Modal focus trap walks the
  // dialog subtree on open and lands on the first tabbable, which
  // would be the hidden destructive button. Render terminate only
  // when the operator has explicitly opened the More disclosure.
  const [moreOpen, setMoreOpen] = useState(false);
  return (
    <Modal isOpen={true} onRequestClose={onCancel} labelledBy="detail-cancel-title">
      <h2
        id="detail-cancel-title"
        className="text-sm font-semibold text-slate-800 dark:text-slate-100"
      >
        Cancel {row.display_name}?
      </h2>
      <p className="mt-2 text-sm text-slate-700 dark:text-slate-300">
        Cooperative cancel signals the worker to stop at its next checkpoint.
        The active checkpoint completes (writes are idempotent); the run
        transitions to <span className="font-mono">cancelled</span> once the
        worker observes the flag.
      </p>
      <div className="mt-3 text-xs text-slate-600 dark:text-slate-300">
        <button
          type="button"
          onClick={() => setMoreOpen((v) => !v)}
          aria-expanded={moreOpen}
          className="text-slate-500 hover:text-slate-700 dark:text-slate-400 dark:hover:text-slate-200"
        >
          {moreOpen ? "▾" : "▸"} More — terminate (escape hatch)
        </button>
        {moreOpen ? (
          <div className="mt-2">
            <p className="leading-relaxed">
              Terminate marks for cleanup. Active SEC fetches continue. To
              force a stop, use cooperative cancel and wait, or restart the
              jobs process.
            </p>
            <button
              type="button"
              onClick={() => onConfirm("terminate")}
              disabled={busy}
              className="mt-2 rounded border border-red-300 bg-red-50 px-2 py-1 text-xs font-medium text-red-700 hover:bg-red-100 disabled:cursor-not-allowed disabled:opacity-50 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300 dark:hover:bg-red-950/60"
            >
              Terminate (mark for cleanup)
            </button>
          </div>
        ) : null}
      </div>
      <div className="mt-4 flex justify-end gap-2">
        <button
          type="button"
          onClick={onCancel}
          className="rounded border border-slate-300 bg-white px-3 py-1 text-xs font-medium text-slate-700 hover:bg-slate-50 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-200 dark:hover:bg-slate-800/40"
        >
          Keep running
        </button>
        <button
          type="button"
          onClick={() => onConfirm("cooperative")}
          disabled={busy}
          className="rounded border border-amber-400 bg-amber-500 px-3 py-1 text-xs font-medium text-white hover:bg-amber-600 disabled:cursor-not-allowed disabled:opacity-50 dark:border-amber-700 dark:bg-amber-700 dark:hover:bg-amber-800"
        >
          {busy ? "Cancelling…" : "Cancel cooperatively"}
        </button>
      </div>
    </Modal>
  );
}

// Re-export so tests reuse the canonical mapping.
export { REASON_TOOLTIP };
