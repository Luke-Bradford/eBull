import { useCallback } from "react";
import { useNavigate } from "react-router-dom";

import { fetchNews } from "@/api/news";
import type { NewsListResponse } from "@/api/types";
import { Pane } from "@/components/instrument/Pane";
import { SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { useAsync } from "@/lib/useAsync";

const ROW_LIMIT = 5;

export interface RecentNewsPaneProps {
  readonly instrumentId: number;
  readonly symbol: string;
}

export function RecentNewsPane({
  instrumentId,
  symbol,
}: RecentNewsPaneProps): JSX.Element | null {
  const state = useAsync<NewsListResponse>(
    useCallback(() => fetchNews(instrumentId, 0, ROW_LIMIT), [instrumentId]),
    [instrumentId],
  );
  const navigate = useNavigate();

  if (state.loading) {
    return (
      <Pane title="Recent news">
        <SectionSkeleton rows={4} />
      </Pane>
    );
  }
  if (state.error !== null) {
    return (
      <Pane title="Recent news">
        <SectionError onRetry={state.refetch} />
      </Pane>
    );
  }
  if (state.data === null || state.data.items.length === 0) {
    return null;
  }

  const items = state.data.items.slice(0, ROW_LIMIT);
  return (
    <Pane
      title="Recent news"
      onExpand={() =>
        navigate(`/instrument/${encodeURIComponent(symbol)}?tab=news`)
      }
    >
      <ul className="space-y-1.5 text-xs">
        {items.map((n) => (
          <li key={n.news_event_id} className="flex items-baseline gap-2">
            <span className="text-slate-500">
              {n.event_time.slice(0, 10)}
            </span>
            <span className="truncate text-slate-700">{n.headline}</span>
          </li>
        ))}
      </ul>
    </Pane>
  );
}
