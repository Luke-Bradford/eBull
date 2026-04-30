import { useState } from "react";
import { ApiError } from "@/api/client";
import {
  fetchRecommendations,
  RECOMMENDATIONS_PAGE_LIMIT,
  type RecommendationsQuery,
} from "@/api/recommendations";
import { fetchAuditList, AUDIT_PAGE_LIMIT, type AuditQuery } from "@/api/audit";
import { useAsync } from "@/lib/useAsync";
import { ErrorBanner } from "@/components/states/ErrorBanner";
import { Section } from "@/components/dashboard/Section";
import { RecommendationsFilters } from "@/components/recommendations/RecommendationsFilters";
import {
  RecommendationsTable,
  type RecommendationsView,
} from "@/components/recommendations/RecommendationsTable";
import { AuditFilters } from "@/components/recommendations/AuditFilters";
import { AuditTrail, type AuditView } from "@/components/recommendations/AuditTrail";
import type { RecommendationListItem } from "@/api/types";
import type { AuditListItem } from "@/api/types";

/**
 * Recommendations & execution review page (#63).
 *
 * Two independent async sources:
 *   1. GET /recommendations — filterable by action, status, instrument_id
 *   2. GET /audit — filterable by pass_fail, stage, instrument_id, date range
 *
 * Each section owns its own {loading, error, data} lifecycle per the
 * async-data-loading skill. A failing /audit must not blank /recommendations.
 *
 * Strictly read-only: no mutations, no write actions.
 * Auth: both backend routers require session/service token; 401 → per-section
 * "Authentication required" state.
 */
export function RecommendationsPage() {
  // -- Recommendations state ------------------------------------------------
  const [recQuery, setRecQuery] = useState<RecommendationsQuery>({
    action: null,
    status: null,
    instrument_id: null,
  });
  const [recOffset, setRecOffset] = useState(0);

  // useAsync captures fn via a ref — fresh arrow per render is fine.
  const recs = useAsync(
    () => fetchRecommendations(recQuery, recOffset),
    [recQuery.action, recQuery.status, recQuery.instrument_id, recOffset],
  );

  const recFiltersDirty =
    recQuery.action !== null || recQuery.status !== null || recQuery.instrument_id !== null;

  const onClearRecFilters = () => {
    setRecQuery({ action: null, status: null, instrument_id: null });
    setRecOffset(0);
  };

  const onRecQueryChange = (next: RecommendationsQuery) => {
    setRecQuery(next);
    setRecOffset(0);
  };

  // -- Audit state ----------------------------------------------------------
  const [auditQuery, setAuditQuery] = useState<AuditQuery>({
    instrument_id: null,
    pass_fail: null,
    stage: null,
    date_from: null,
    date_to: null,
  });
  const [auditOffset, setAuditOffset] = useState(0);

  // useAsync captures fn via a ref — fresh arrow per render is fine.
  const audit = useAsync(
    () => fetchAuditList(auditQuery, auditOffset),
    [
      auditQuery.instrument_id,
      auditQuery.pass_fail,
      auditQuery.stage,
      auditQuery.date_from,
      auditQuery.date_to,
      auditOffset,
    ],
  );

  const auditFiltersDirty =
    auditQuery.instrument_id !== null ||
    auditQuery.pass_fail !== null ||
    auditQuery.stage !== null ||
    auditQuery.date_from !== null ||
    auditQuery.date_to !== null;

  const onClearAuditFilters = () => {
    setAuditQuery({
      instrument_id: null,
      pass_fail: null,
      stage: null,
      date_from: null,
      date_to: null,
    });
    setAuditOffset(0);
  };

  const onAuditQueryChange = (next: AuditQuery) => {
    setAuditQuery(next);
    setAuditOffset(0);
  };

  // -- Top-level error banner -----------------------------------------------
  const allFailed = recs.error !== null && audit.error !== null;

  // -- View computation -----------------------------------------------------
  const recView = computeRecView({
    loading: recs.loading,
    error: recs.error,
    items: recs.data?.items ?? null,
    filtersDirty: recFiltersDirty,
    onRetry: recs.refetch,
    onClearFilters: onClearRecFilters,
  });

  const auditView = computeAuditView({
    loading: audit.loading,
    error: audit.error,
    items: audit.data?.items ?? null,
    filtersDirty: auditFiltersDirty,
    onRetry: audit.refetch,
    onClearFilters: onClearAuditFilters,
  });

  const recTotal = recs.data?.total ?? 0;
  const recHasPrev = recOffset > 0;
  const recHasNext = recOffset + RECOMMENDATIONS_PAGE_LIMIT < recTotal;

  const auditTotal = audit.data?.total ?? 0;
  const auditHasPrev = auditOffset > 0;
  const auditHasNext = auditOffset + AUDIT_PAGE_LIMIT < auditTotal;

  return (
    <div className="space-y-6">
      <h1 className="text-xl font-semibold text-slate-800 dark:text-slate-100">Recommendations</h1>

      {allFailed ? (
        <ErrorBanner message="The API is unreachable. Check that the backend is running and the auth token is configured." />
      ) : null}

      <RecommendationsFilters
        query={recQuery}
        onQueryChange={onRecQueryChange}
        onClearAll={onClearRecFilters}
        filtersDirty={recFiltersDirty}
      />

      <Section
        title="Recommendation history"
        action={
          !recs.loading && recTotal > 0 ? (
            <Pagination
              offset={recOffset}
              limit={RECOMMENDATIONS_PAGE_LIMIT}
              total={recTotal}
              hasPrev={recHasPrev}
              hasNext={recHasNext}
              onPrev={() => setRecOffset((o) => Math.max(0, o - RECOMMENDATIONS_PAGE_LIMIT))}
              onNext={() => setRecOffset((o) => o + RECOMMENDATIONS_PAGE_LIMIT)}
            />
          ) : undefined
        }
      >
        <RecommendationsTable view={recView} />
      </Section>

      <AuditFilters
        query={auditQuery}
        onQueryChange={onAuditQueryChange}
        onClearAll={onClearAuditFilters}
        filtersDirty={auditFiltersDirty}
      />

      <Section
        title="Execution audit trail"
        action={
          !audit.loading && auditTotal > 0 ? (
            <Pagination
              offset={auditOffset}
              limit={AUDIT_PAGE_LIMIT}
              total={auditTotal}
              hasPrev={auditHasPrev}
              hasNext={auditHasNext}
              onPrev={() => setAuditOffset((o) => Math.max(0, o - AUDIT_PAGE_LIMIT))}
              onNext={() => setAuditOffset((o) => o + AUDIT_PAGE_LIMIT)}
            />
          ) : undefined
        }
      >
        <AuditTrail view={auditView} />
      </Section>
    </div>
  );
}

// ---------------------------------------------------------------------------
// View computation
// ---------------------------------------------------------------------------

interface ComputeRecViewArgs {
  loading: boolean;
  error: unknown;
  items: ReadonlyArray<RecommendationListItem> | null;
  filtersDirty: boolean;
  onRetry: () => void;
  onClearFilters: () => void;
}

function computeRecView(args: ComputeRecViewArgs): RecommendationsView {
  const { loading, error, items, filtersDirty, onRetry, onClearFilters } = args;

  if (loading) return { kind: "loading" };

  if (error !== null) {
    if (error instanceof ApiError && error.status === 401) {
      return { kind: "error401" };
    }
    return { kind: "error", onRetry };
  }

  // useAsync contract: after loading=false, exactly one of data or error
  // is non-null. This branch is unreachable at runtime but required for
  // TypeScript narrowing — surface as a generic error if it ever fires.
  if (items === null) return { kind: "error", onRetry };

  if (items.length === 0) {
    if (filtersDirty) {
      return {
        kind: "empty",
        title: "No results match the current filters",
        description: "Loosen the filters or clear them to see all recommendations.",
        action: <ClearFiltersButton onClick={onClearFilters} />,
      };
    }
    return {
      kind: "empty",
      title: "No recommendations yet",
      description:
        "Recommendations will appear here once the portfolio manager has run.",
    };
  }

  return { kind: "data", items: items.slice() };
}

interface ComputeAuditViewArgs {
  loading: boolean;
  error: unknown;
  items: ReadonlyArray<AuditListItem> | null;
  filtersDirty: boolean;
  onRetry: () => void;
  onClearFilters: () => void;
}

function computeAuditView(args: ComputeAuditViewArgs): AuditView {
  const { loading, error, items, filtersDirty, onRetry, onClearFilters } = args;

  if (loading) return { kind: "loading" };

  if (error !== null) {
    if (error instanceof ApiError && error.status === 401) {
      return { kind: "error401" };
    }
    return { kind: "error", onRetry };
  }

  // useAsync contract: after loading=false, exactly one of data or error
  // is non-null. This branch is unreachable at runtime but required for
  // TypeScript narrowing — surface as a generic error if it ever fires.
  if (items === null) return { kind: "error", onRetry };

  if (items.length === 0) {
    if (filtersDirty) {
      return {
        kind: "empty",
        title: "No results match the current filters",
        description: "Loosen the filters or clear them to see all audit entries.",
        action: <ClearFiltersButton onClick={onClearFilters} />,
      };
    }
    return {
      kind: "empty",
      title: "No execution guard decisions recorded yet",
      description:
        "Audit entries will appear here once the execution guard has evaluated a recommendation.",
    };
  }

  return { kind: "data", items: items.slice() };
}

// ---------------------------------------------------------------------------
// Shared small components
// ---------------------------------------------------------------------------

function ClearFiltersButton({ onClick }: { onClick: () => void }) {
  return (
    <button
      type="button"
      onClick={onClick}
      className="rounded border border-slate-300 dark:border-slate-700 bg-white px-3 py-1.5 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:hover:bg-slate-800"
    >
      Clear filters
    </button>
  );
}

function Pagination({
  offset,
  limit,
  total,
  hasPrev,
  hasNext,
  onPrev,
  onNext,
}: {
  offset: number;
  limit: number;
  total: number;
  hasPrev: boolean;
  hasNext: boolean;
  onPrev: () => void;
  onNext: () => void;
}) {
  const from = offset + 1;
  const to = Math.min(offset + limit, total);
  return (
    <div className="flex items-center gap-2 text-xs text-slate-500">
      <span>
        {from}–{to} of {total}
      </span>
      <button
        type="button"
        disabled={!hasPrev}
        onClick={onPrev}
        className="rounded border border-slate-300 dark:border-slate-700 px-1.5 py-0.5 text-xs hover:bg-slate-100 dark:hover:bg-slate-800 disabled:cursor-not-allowed disabled:opacity-50"
      >
        Prev
      </button>
      <button
        type="button"
        disabled={!hasNext}
        onClick={onNext}
        className="rounded border border-slate-300 dark:border-slate-700 px-1.5 py-0.5 text-xs hover:bg-slate-100 dark:hover:bg-slate-800 disabled:cursor-not-allowed disabled:opacity-50"
      >
        Next
      </button>
    </div>
  );
}
