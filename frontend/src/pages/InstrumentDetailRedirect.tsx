/**
 * Route shim at `/instruments/:instrumentId` (Slice 3 of per-stock
 * research page spec).
 *
 * Fetches the instrument's symbol by id, then `Navigate`s to the
 * canonical `/instrument/:symbol` research page. Keeps legacy
 * bookmarks warm for one release after the old InstrumentDetailPage
 * retires; delete in Slice 5 once operator bookmarks have migrated.
 */
import { Navigate, useParams } from "react-router-dom";

import { ApiError } from "@/api/client";
import { fetchInstrumentDetail } from "@/api/instruments";
import { SectionSkeleton } from "@/components/dashboard/Section";
import { EmptyState } from "@/components/states/EmptyState";
import { useAsync } from "@/lib/useAsync";

export interface InstrumentDetailRedirectProps {
  /** Query string appended to the target path, e.g. `?tab=positions`. */
  search?: string;
}

export function InstrumentDetailRedirect({
  search = "",
}: InstrumentDetailRedirectProps): JSX.Element {
  const { instrumentId } = useParams<{ instrumentId: string }>();
  const parsedId = instrumentId ? Number(instrumentId) : NaN;

  const { data, error, loading } = useAsync(
    async () => {
      if (!Number.isFinite(parsedId)) return null;
      try {
        return await fetchInstrumentDetail(parsedId);
      } catch (err) {
        if (err instanceof ApiError && err.status === 404) return null;
        throw err;
      }
    },
    [parsedId],
  );

  if (!Number.isFinite(parsedId)) {
    return (
      <EmptyState
        title="Invalid instrument id"
        description={`"${instrumentId}" is not a valid id.`}
      />
    );
  }
  if (loading) return <SectionSkeleton rows={2} />;
  if (error !== null) {
    return (
      <EmptyState
        title="Failed to resolve instrument"
        description="Retry from the /instrument/:symbol URL directly."
      />
    );
  }
  if (data === null) {
    return (
      <EmptyState
        title="Instrument not found"
        description={`No instrument with id ${parsedId}.`}
      />
    );
  }
  const qs = search.startsWith("?") || search === "" ? search : `?${search}`;
  return (
    <Navigate
      to={`/instrument/${encodeURIComponent(data.symbol)}${qs}`}
      replace
    />
  );
}
