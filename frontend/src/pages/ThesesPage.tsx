/**
 * /theses — Theses library (#1902): the global surface for thesis reports.
 *
 * Latest thesis per instrument with stance / confidence / buy zone / critic
 * verdict / staleness / held flag / latest score, plus the latest
 * generation-run status from thesis_runs (#1919). Filter state lives in the
 * URL query string (held / stale / stance / offset) so deep-links work —
 * the dashboard AlertsStrip links straight to ?held=true&stale=true.
 *
 * Detail view = the instrument page's Verdict tab (existing rendering);
 * this page deliberately adds no second memo renderer (and no markdown
 * dependency — memo_markdown stays plain-text there, decision on #1902).
 *
 * Status column: server truth from thesis_runs. While any row is
 * 'running' the page polls (POLL_MS) so completion/failure lands without
 * a manual reload — the transient skeleton on each poll tick is the
 * documented useAsync default (preserveOnRefetch would leak stale rows
 * into filter changes after the first poll tick; see useAsync contract).
 */

import { useEffect, useRef, useState } from "react";
import { Link, useSearchParams } from "react-router-dom";

import { fetchThesesLibrary, generateInstrumentThesis } from "@/api/theses";
import type { ThesisLibraryItem, ThesisLibraryResponse } from "@/api/types";
import {
  Section,
  SectionError,
  SectionSkeleton,
} from "@/components/dashboard/Section";
import { EmptyState } from "@/components/states/EmptyState";
import { CriticVerdictBadge } from "@/components/theses/CriticVerdictBadge";
import { StanceBadge } from "@/components/theses/StanceBadge";
import { formatNumber, formatRelativeTime } from "@/lib/format";
import { useAsync } from "@/lib/useAsync";

const PAGE_SIZE = 50;
const POLL_MS = 20_000;

const STANCES = ["buy", "hold", "watch", "avoid"] as const;

interface LibraryFilters {
  held: boolean;
  stale: boolean;
  stance: string;
  offset: number;
}

function readFilters(p: URLSearchParams): LibraryFilters {
  const stance = p.get("stance") ?? "";
  const offset = Number(p.get("offset") ?? "0");
  return {
    held: p.get("held") === "true",
    stale: p.get("stale") === "true",
    stance: (STANCES as readonly string[]).includes(stance) ? stance : "",
    offset: Number.isInteger(offset) && offset > 0 ? offset : 0,
  };
}

function writeFilters(f: LibraryFilters): URLSearchParams {
  const out = new URLSearchParams();
  if (f.held) out.set("held", "true");
  if (f.stale) out.set("stale", "true");
  if (f.stance !== "") out.set("stance", f.stance);
  if (f.offset > 0) out.set("offset", String(f.offset));
  return out;
}

/** Human label for a find_stale_instruments reason code. */
function staleLabel(reason: string): string {
  if (reason.startsWith("event_new_")) {
    return `new ${reason.slice("event_new_".length).replace("10k", "10-K").replace("10q", "10-Q").replace("8k", "8-K")}`;
  }
  if (reason === "missing_frequency") return "no cadence";
  if (reason === "no_thesis") return "missing";
  if (reason === "break_fired") return "break";
  return "stale";
}

/** #2013 — compact what-changed chip next to the run status. Amber when the
 *  change is material (stance/type/target move per thesis_diff); muted for
 *  a non-material tweak. Absent for v1 / gap rows / unchanged regens. */
function ChangeChip({ row }: { row: ThesisLibraryItem }) {
  if (row.last_change_summary === null) return null;
  const cls = row.last_change_material
    ? "border-amber-300 dark:border-amber-700 bg-amber-50 dark:bg-amber-950/40 text-amber-700 dark:text-amber-300"
    : "border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-900/40 text-slate-500 dark:text-slate-400";
  return (
    <span
      data-testid="thesis-change-chip"
      className={`inline-block max-w-48 truncate rounded border px-1.5 py-0.5 align-bottom text-[10px] font-medium ${cls}`}
      title={row.last_change_summary}
    >
      Δ {row.last_change_summary}
    </span>
  );
}

function RunStatusCell({ row }: { row: ThesisLibraryItem }) {
  if (row.run_status === "running") {
    return (
      <span
        className="inline-block rounded border border-blue-300 dark:border-blue-700 bg-blue-50 dark:bg-blue-950/40 px-1.5 py-0.5 text-[10px] font-medium text-blue-700 dark:text-blue-300"
        title={`trigger: ${row.run_trigger ?? "—"}, started ${formatRelativeTime(row.run_started_at)}`}
      >
        running
      </span>
    );
  }
  if (row.run_status === "failed") {
    return (
      <span
        className="inline-block rounded border border-red-300 dark:border-red-700 bg-red-50 dark:bg-red-950/40 px-1.5 py-0.5 text-[10px] font-medium text-red-700 dark:text-red-300"
        title={row.run_error ?? "generation failed"}
      >
        failed
      </span>
    );
  }
  if (row.run_status === "ok") {
    return (
      <span className="text-xs text-slate-400 dark:text-slate-500">ok</span>
    );
  }
  return <span className="text-xs text-slate-400 dark:text-slate-500">—</span>;
}

export function ThesesPage(): JSX.Element {
  const [searchParams, setSearchParams] = useSearchParams();
  const filters = readFilters(searchParams);

  // useAsync captures fn via a ref — fresh arrow per render is fine.
  const state = useAsync<ThesisLibraryResponse>(
    () =>
      fetchThesesLibrary({
        heldOnly: filters.held,
        stale: filters.stale,
        stance: filters.stance || undefined,
        offset: filters.offset,
        limit: PAGE_SIZE,
      }),
    [filters.held, filters.stale, filters.stance, filters.offset],
  );

  // Per-row "request fresh" in-flight ids. Transient button state only —
  // the durable status is the server's thesis_runs row (#1902 scope note:
  // FE-local generation state dies on navigation, so nothing beyond the
  // button disable relies on this).
  const [busyIds, setBusyIds] = useState<ReadonlySet<number>>(new Set());
  // Fixed-phrase notice for a failed regeneration REQUEST (transport /
  // HTTP error) — the durable failure state still comes from thesis_runs,
  // but that can lag a poll cycle behind the click (review NITPICK).
  const [actionError, setActionError] = useState<string | null>(null);
  const mountedRef = useRef(true);
  useEffect(() => {
    mountedRef.current = true;
    return () => {
      mountedRef.current = false;
    };
  }, []);

  const refetch = state.refetch;

  // Poll while any visible run is in-flight so completion/failure lands
  // without a manual reload. Local generation takes minutes (qwen3:14b
  // ~5-8 min/thesis) — 20s keeps the page honest without hammering.
  const anyRunning =
    state.data?.items.some((r) => r.run_status === "running") ?? false;
  useEffect(() => {
    if (!anyRunning) return;
    const t = setInterval(() => refetch(), POLL_MS);
    return () => clearInterval(t);
  }, [anyRunning, refetch]);

  // A stale deep-link (?offset beyond the filtered total) lands on an empty
  // page whose controls live in the table branch — clamp back to page one
  // instead of stranding the operator on a misleading empty state (Codex
  // ckpt-2). setSearchParams is stable; data settles before this fires.
  const emptyBeyondTotal =
    state.data !== null &&
    state.data.items.length === 0 &&
    state.data.total > 0 &&
    filters.offset > 0;
  useEffect(() => {
    if (!emptyBeyondTotal) return;
    setSearchParams(
      (prev) => {
        const out = new URLSearchParams(prev);
        out.delete("offset");
        return out;
      },
      { replace: true },
    );
  }, [emptyBeyondTotal, setSearchParams]);

  function setFilters(next: Partial<LibraryFilters>): void {
    // Any filter change resets pagination — offset only survives paging.
    const merged: LibraryFilters = {
      ...filters,
      offset: 0,
      ...next,
    };
    setSearchParams(writeFilters(merged), { replace: true });
  }

  async function onRequestFresh(row: ThesisLibraryItem): Promise<void> {
    setBusyIds((prev) => new Set(prev).add(row.instrument_id));
    setActionError(null);
    // Surface the server's 'running' row shortly after firing — the POST
    // itself only resolves when generation completes (minutes on a local
    // model), and the poll loop only starts once a running row is visible.
    setTimeout(() => {
      if (mountedRef.current) refetch();
    }, 1_500);
    try {
      await generateInstrumentThesis(row.symbol, true);
    } catch (err) {
      // Fixed phrase to the surface, full detail to the console (never
      // exception text in the DOM); the durable per-row failure state
      // arrives via thesis_runs on the next poll.
      console.error("[ThesesPage] force generation failed", err);
      if (mountedRef.current) {
        setActionError(
          `Generation request for ${row.symbol} failed — check the browser console; the row's status column carries the server-side failure detail.`,
        );
      }
    } finally {
      if (mountedRef.current) {
        setBusyIds((prev) => {
          const next = new Set(prev);
          next.delete(row.instrument_id);
          return next;
        });
        refetch();
      }
    }
  }

  const data = state.data;
  const items = data?.items ?? [];
  const total = data?.total ?? 0;
  const pageStart = total === 0 ? 0 : filters.offset + 1;
  const pageEnd = filters.offset + items.length;

  return (
    <div className="mx-auto max-w-screen-2xl space-y-3 p-4 pt-6">
      <Section title="Theses">
        <div className="flex flex-wrap items-center gap-4 text-xs text-slate-600 dark:text-slate-300">
          <label className="flex items-center gap-1.5">
            <input
              type="checkbox"
              checked={filters.held}
              onChange={(e) => setFilters({ held: e.target.checked })}
            />
            Held only
          </label>
          <label className="flex items-center gap-1.5">
            <input
              type="checkbox"
              checked={filters.stale}
              onChange={(e) => setFilters({ stale: e.target.checked })}
            />
            Stale only
          </label>
          <label className="flex items-center gap-1.5">
            Stance
            <select
              className="rounded border border-slate-300 dark:border-slate-700 bg-white dark:bg-slate-900 px-1.5 py-0.5"
              value={filters.stance}
              onChange={(e) => setFilters({ stance: e.target.value })}
            >
              <option value="">all</option>
              {STANCES.map((s) => (
                <option key={s} value={s}>
                  {s}
                </option>
              ))}
            </select>
          </label>
        </div>

        {actionError !== null ? (
          <div
            role="status"
            className="mt-3 rounded border border-red-200 dark:border-red-900/60 bg-red-50 dark:bg-red-950/40 px-3 py-1.5 text-xs text-red-700 dark:text-red-300"
          >
            {actionError}
          </div>
        ) : null}
        {state.loading ? (
          <div className="mt-3">
            <SectionSkeleton rows={6} />
          </div>
        ) : state.error !== null ? (
          <div className="mt-3">
            <SectionError onRetry={state.refetch} />
          </div>
        ) : items.length === 0 ? (
          <div className="mt-3">
            <EmptyState
              title="No theses"
              description={
                total === 0 && !filters.held && !filters.stale && filters.stance === ""
                  ? "Theses are generated hourly by the thesis_refresh job (held + top-ranked instruments), or on demand from an instrument page via Generate thesis."
                  : "No theses match these filters."
              }
            />
          </div>
        ) : (
          <>
            <div className="mt-3 overflow-x-auto">
              <table className="min-w-full text-sm">
                <thead>
                  <tr className="border-b border-slate-200 dark:border-slate-800 text-left text-xs text-slate-500">
                    <th className="px-2 py-2">Symbol</th>
                    <th className="px-2 py-2">Stance</th>
                    <th className="px-2 py-2">Type</th>
                    <th className="px-2 py-2 text-right">Confidence</th>
                    <th className="px-2 py-2 text-right">Buy zone</th>
                    <th className="px-2 py-2 text-right">Score</th>
                    <th className="px-2 py-2">Critic</th>
                    <th className="px-2 py-2">Age</th>
                    <th className="px-2 py-2">Status</th>
                    <th className="px-2 py-2"></th>
                  </tr>
                </thead>
                <tbody>
                  {items.map((row) => (
                    <tr
                      key={row.instrument_id}
                      className="border-b border-slate-100 dark:border-slate-800/60 hover:bg-slate-50 dark:hover:bg-slate-800/40"
                    >
                      <td className="px-2 py-2">
                        <Link
                          to={`/instrument/${encodeURIComponent(row.symbol)}?tab=verdict`}
                          className="font-medium text-sky-700 dark:text-sky-400 hover:underline"
                        >
                          {row.symbol}
                        </Link>
                        {row.is_held ? (
                          <span
                            title="Held position"
                            className="ml-1.5 rounded bg-blue-100 dark:bg-blue-900/40 px-1 py-0.5 text-[10px] font-medium text-blue-700 dark:text-blue-300"
                          >
                            held
                          </span>
                        ) : null}
                        <div className="max-w-[16rem] truncate text-xs text-slate-500 dark:text-slate-400">
                          {row.company_name}
                        </div>
                      </td>
                      <td className="px-2 py-2">
                        {row.stance !== null ? (
                          <StanceBadge stance={row.stance} />
                        ) : (
                          <span className="text-xs text-slate-400 dark:text-slate-500">
                            —
                          </span>
                        )}
                      </td>
                      <td className="px-2 py-2 text-xs text-slate-600 dark:text-slate-300">
                        {row.thesis_type ?? "—"}
                      </td>
                      <td className="px-2 py-2 text-right tabular-nums">
                        {row.confidence_score !== null
                          ? `${Math.round(row.confidence_score * 100)}%`
                          : "—"}
                      </td>
                      <td className="px-2 py-2 text-right tabular-nums text-xs">
                        {row.buy_zone_low !== null || row.buy_zone_high !== null
                          ? `${formatNumber(row.buy_zone_low, 2)} – ${formatNumber(row.buy_zone_high, 2)}`
                          : "—"}
                      </td>
                      <td className="px-2 py-2 text-right tabular-nums text-xs">
                        {row.latest_score !== null ? (
                          <>
                            {formatNumber(row.latest_score, 3)}
                            {row.latest_rank !== null ? (
                              <span className="ml-1 text-slate-400 dark:text-slate-500">
                                #{row.latest_rank}
                              </span>
                            ) : null}
                          </>
                        ) : (
                          "—"
                        )}
                      </td>
                      <td className="px-2 py-2">
                        {row.thesis_id !== null ? (
                          <CriticVerdictBadge verdict={row.critic_verdict} />
                        ) : (
                          <span className="text-xs text-slate-400 dark:text-slate-500">
                            —
                          </span>
                        )}
                      </td>
                      <td className="px-2 py-2 text-xs text-slate-600 dark:text-slate-300">
                        {row.created_at !== null
                          ? formatRelativeTime(row.created_at)
                          : "no thesis yet"}
                        {row.stale_reason !== null ? (
                          <span
                            title={`Stale: ${row.stale_reason}`}
                            className="ml-1.5 rounded border border-amber-300 dark:border-amber-700 bg-amber-50 dark:bg-amber-950/40 px-1 py-0.5 text-[10px] font-medium text-amber-700 dark:text-amber-300"
                          >
                            {staleLabel(row.stale_reason)}
                          </span>
                        ) : null}
                      </td>
                      <td className="px-2 py-2">
                        <div className="flex items-center gap-1.5">
                          <RunStatusCell row={row} />
                          <ChangeChip row={row} />
                        </div>
                      </td>
                      <td className="px-2 py-2 text-right">
                        <button
                          type="button"
                          disabled={
                            busyIds.has(row.instrument_id) ||
                            row.run_status === "running"
                          }
                          onClick={() => void onRequestFresh(row)}
                          className="rounded border border-slate-300 dark:border-slate-700 bg-white dark:bg-slate-900 px-2 py-1 text-xs font-medium text-slate-700 dark:text-slate-200 hover:bg-slate-50 dark:hover:bg-slate-800 disabled:cursor-not-allowed disabled:opacity-50"
                          title="Regenerate this thesis now (bypasses the 24h cache)"
                        >
                          {busyIds.has(row.instrument_id) ||
                          row.run_status === "running"
                            ? "Generating…"
                            : row.thesis_id === null
                              ? "Generate"
                              : "Refresh"}
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <div className="mt-3 flex items-center justify-between text-xs text-slate-500 dark:text-slate-400">
              <span className="tabular-nums">
                {pageStart}–{pageEnd} of {total}
              </span>
              <div className="flex gap-2">
                <button
                  type="button"
                  disabled={filters.offset === 0}
                  onClick={() =>
                    setFilters({
                      ...filters,
                      offset: Math.max(0, filters.offset - PAGE_SIZE),
                    })
                  }
                  className="rounded border border-slate-300 dark:border-slate-700 px-2 py-1 font-medium disabled:cursor-not-allowed disabled:opacity-50"
                >
                  ← Prev
                </button>
                <button
                  type="button"
                  disabled={pageEnd >= total}
                  onClick={() =>
                    setFilters({
                      ...filters,
                      offset: filters.offset + PAGE_SIZE,
                    })
                  }
                  className="rounded border border-slate-300 dark:border-slate-700 px-2 py-1 font-medium disabled:cursor-not-allowed disabled:opacity-50"
                >
                  Next →
                </button>
              </div>
            </div>
          </>
        )}
      </Section>
    </div>
  );
}
