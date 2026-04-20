/**
 * RightRail — peripheral-vision column of the per-stock research page.
 * Slices 2 + 6 of docs/superpowers/specs/2026-04-20-per-stock-research-page.md.
 *
 * Sections (independent fetches; one failure does not blank others):
 *   1. Recent filings (last 3) — link out to filings tab + documents
 *   2. Peer snapshot (top 5 ranked within same sector) — clickable
 *      rows drill into each peer's research page
 *   3. Copy-trader exposure (Slice 6) — parent traders the operator
 *      mirrors who hold this instrument. Hidden when there's no
 *      exposure. Placeholder scaffold; deeper design deferred to a
 *      follow-up spec if scope grows.
 *   4. Recent news (last 3) — headline + sentiment badge
 */
import { Link } from "react-router-dom";

import { fetchCopyTrading } from "@/api/copyTrading";
import { fetchFilings } from "@/api/filings";
import { fetchNews } from "@/api/news";
import { fetchRankings } from "@/api/rankings";
import type {
  CopyTraderSummary,
  FilingItem,
  NewsItem,
  RankingItem,
} from "@/api/types";
import { Section, SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { useAsync } from "@/lib/useAsync";

export interface RightRailProps {
  instrumentId: number;
  sector: string | null;
  currentSymbol: string;
}

export function RightRail({
  instrumentId,
  sector,
  currentSymbol,
}: RightRailProps): JSX.Element {
  return (
    <aside className="space-y-4">
      <RecentFilings instrumentId={instrumentId} />
      {/* `key={sector}` forces a full remount when the sector changes
          so `useAsync` re-initialises with `loading=true, data=null`
          on the first render for the new sector — otherwise one frame
          would show the prior sector's peers under the new sector's
          heading (Codex slice-2 round-2 stale-data finding). */}
      <PeerSnapshot
        key={sector ?? "__no_sector__"}
        sector={sector}
        currentSymbol={currentSymbol}
      />
      <CopyExposure instrumentId={instrumentId} />
      <RecentNews instrumentId={instrumentId} />
    </aside>
  );
}

// ---------------------------------------------------------------------------
// Recent filings
// ---------------------------------------------------------------------------

function RecentFilings({ instrumentId }: { instrumentId: number }) {
  const { data, error, loading, refetch } = useAsync(
    () => fetchFilings(instrumentId, 0, 3),
    [instrumentId],
  );
  return (
    <Section title="Recent filings">
      {loading && <SectionSkeleton rows={3} />}
      {error !== null && <SectionError onRetry={refetch} />}
      {!loading && error === null && (data?.items.length ?? 0) === 0 && (
        <div className="text-xs text-slate-500">No filings ingested yet.</div>
      )}
      {!loading && error === null && (data?.items.length ?? 0) > 0 && (
        <ul className="space-y-1.5 text-xs">
          {(data?.items ?? []).map((f) => (
            <FilingRow key={f.filing_event_id} f={f} />
          ))}
        </ul>
      )}
    </Section>
  );
}

function FilingRow({ f }: { f: FilingItem }) {
  const link = f.primary_document_url ?? f.source_url;
  return (
    <li className="flex items-baseline justify-between gap-2">
      <span className="flex items-baseline gap-2 truncate">
        <span className="inline-block min-w-[40px] rounded bg-slate-100 px-1 py-0.5 text-center text-[10px] font-semibold uppercase text-slate-600">
          {f.filing_type ?? "—"}
        </span>
        <span className="truncate text-slate-700">{f.filing_date}</span>
      </span>
      {link ? (
        <a
          href={link}
          target="_blank"
          rel="noopener noreferrer"
          className="shrink-0 text-[10px] text-blue-700 hover:underline"
        >
          open →
        </a>
      ) : null}
    </li>
  );
}

// ---------------------------------------------------------------------------
// Peer snapshot
// ---------------------------------------------------------------------------

function PeerSnapshot({
  sector,
  currentSymbol,
}: {
  sector: string | null;
  currentSymbol: string;
}) {
  // `sector=null` short-circuits the fetch to avoid a pointless 200-row
  // rankings call on unknown-sector instruments.
  const { data, error, loading, refetch } = useAsync(
    async () => {
      if (sector === null) return null;
      return await fetchRankings(
        { coverage_tier: null, sector, stance: null },
        6, // top 5 + room to filter out the current instrument
      );
    },
    [sector],
  );

  if (sector === null) {
    return (
      <Section title="Peer snapshot">
        <div className="text-xs text-slate-500">
          Sector unknown — no peer set available.
        </div>
      </Section>
    );
  }

  // Gate the derivation on `data !== null` rather than `?? []` so
  // empty state only flashes when we've actually fetched an empty
  // ranking set, not during a sector-change transition where
  // `useAsync`'s effect hasn't flipped loading=true yet (Codex
  // slice-2 round-1 caveat).
  const peers: RankingItem[] | null =
    data === null
      ? null
      : data.items.filter((r) => r.symbol !== currentSymbol).slice(0, 5);

  return (
    <Section title={`Peer snapshot · ${sector}`}>
      {loading && <SectionSkeleton rows={3} />}
      {error !== null && <SectionError onRetry={refetch} />}
      {!loading && error === null && peers !== null && peers.length === 0 && (
        <div className="text-xs text-slate-500">
          No other ranked peers in this sector.
        </div>
      )}
      {!loading && error === null && peers !== null && peers.length > 0 && (
        <ul className="space-y-1.5 text-xs">
          {peers.map((p) => (
            <li
              key={p.instrument_id}
              className="flex items-baseline justify-between gap-2"
            >
              <Link
                to={`/instrument/${encodeURIComponent(p.symbol)}`}
                className="flex items-baseline gap-2 truncate text-blue-700 hover:underline"
              >
                <span className="inline-block min-w-[32px] rounded bg-slate-100 px-1 py-0.5 text-center text-[10px] font-semibold tabular-nums text-slate-600">
                  #{p.rank ?? "—"}
                </span>
                <span className="truncate font-medium">{p.symbol}</span>
              </Link>
              <span className="shrink-0 tabular-nums text-slate-500">
                {p.total_score !== null ? p.total_score.toFixed(1) : "—"}
              </span>
            </li>
          ))}
        </ul>
      )}
    </Section>
  );
}

// ---------------------------------------------------------------------------
// Recent news
// ---------------------------------------------------------------------------

function RecentNews({ instrumentId }: { instrumentId: number }) {
  const { data, error, loading, refetch } = useAsync(
    () => fetchNews(instrumentId, 0, 3),
    [instrumentId],
  );
  return (
    <Section title="Recent news">
      {loading && <SectionSkeleton rows={3} />}
      {error !== null && <SectionError onRetry={refetch} />}
      {!loading && error === null && (data?.items.length ?? 0) === 0 && (
        <div className="text-xs text-slate-500">No news ingested yet.</div>
      )}
      {!loading && error === null && (data?.items.length ?? 0) > 0 && (
        <ul className="space-y-2 text-xs">
          {(data?.items ?? []).map((n) => (
            <NewsRow key={n.news_event_id} n={n} />
          ))}
        </ul>
      )}
    </Section>
  );
}

function sentimentTone(score: number | null): string {
  if (score === null) return "bg-slate-100 text-slate-500";
  if (score >= 0.3) return "bg-emerald-50 text-emerald-700";
  if (score <= -0.3) return "bg-red-50 text-red-700";
  return "bg-slate-100 text-slate-600";
}

// ---------------------------------------------------------------------------
// Copy-trader exposure (Slice 6)
// ---------------------------------------------------------------------------

interface MirrorHolding {
  mirrorId: number;
  parentUsername: string;
  units: number;
  marketValue: number;
}

/** Aggregate `CopyTradingResponse` into one row per mirror holding
 *  the instrument. Filters on `active === true` so closed mirrors
 *  (which can retain historical positions in the response) don't
 *  surface stale exposure. If a parent-trader has multiple active
 *  mirrors both holding it, each mirror surfaces as its own row —
 *  the operator cares about mirror-level actions (pause / stop),
 *  not parent-level aggregation. */
function extractHoldings(
  traders: CopyTraderSummary[],
  instrumentId: number,
): MirrorHolding[] {
  const out: MirrorHolding[] = [];
  for (const trader of traders) {
    for (const mirror of trader.mirrors) {
      if (!mirror.active) continue;
      const match = mirror.positions.filter(
        (p) => p.instrument_id === instrumentId,
      );
      if (match.length === 0) continue;
      out.push({
        mirrorId: mirror.mirror_id,
        parentUsername: trader.parent_username,
        units: match.reduce((s, p) => s + p.units, 0),
        marketValue: match.reduce((s, p) => s + p.market_value, 0),
      });
    }
  }
  return out;
}

function CopyExposure({ instrumentId }: { instrumentId: number }) {
  const { data, error, loading, refetch } = useAsync(
    () => fetchCopyTrading(),
    [],
  );
  // Hide the section entirely on loading/error/no-exposure so the rail
  // doesn't nag about empty state when no copy traders hold the
  // instrument. The mirrors endpoint is already exercised on
  // /copy-trading surfaces; we opt for silent-when-empty here.
  if (loading) {
    // Still render a skeleton on cold start so a slow fetch isn't a
    // jarring content-shift when it resolves.
    return (
      <Section title="Copy-trader exposure">
        <SectionSkeleton rows={1} />
      </Section>
    );
  }
  if (error !== null) {
    return (
      <Section title="Copy-trader exposure">
        <SectionError onRetry={refetch} />
      </Section>
    );
  }
  const holdings = data ? extractHoldings(data.traders, instrumentId) : [];
  if (holdings.length === 0) return null;

  return (
    <Section title={`Copy-trader exposure · ${holdings.length}`}>
      <ul className="space-y-1.5 text-xs">
        {holdings.map((h) => (
          <li
            key={h.mirrorId}
            className="flex items-baseline justify-between gap-2"
          >
            <Link
              to={`/copy-trading/${h.mirrorId}`}
              className="truncate font-medium text-blue-700 hover:underline"
            >
              {h.parentUsername}
            </Link>
            <span className="shrink-0 tabular-nums text-slate-500">
              {h.units.toLocaleString(undefined, {
                maximumFractionDigits: 2,
              })}
              u
            </span>
          </li>
        ))}
      </ul>
    </Section>
  );
}

function NewsRow({ n }: { n: NewsItem }) {
  const tone = sentimentTone(n.sentiment_score);
  return (
    <li>
      <div className="flex items-baseline justify-between gap-2">
        {n.url ? (
          <a
            href={n.url}
            target="_blank"
            rel="noopener noreferrer"
            className="line-clamp-2 text-slate-700 hover:text-blue-700 hover:underline"
          >
            {n.headline}
          </a>
        ) : (
          <span className="line-clamp-2 text-slate-700">{n.headline}</span>
        )}
        {n.sentiment_score !== null ? (
          <span
            className={`shrink-0 rounded px-1 py-0.5 text-[9px] font-semibold tabular-nums uppercase ${tone}`}
          >
            {n.sentiment_score > 0 ? "+" : ""}
            {n.sentiment_score.toFixed(2)}
          </span>
        ) : null}
      </div>
      <div className="mt-0.5 flex gap-2 text-[10px] text-slate-500">
        <span>{new Date(n.event_time).toLocaleDateString()}</span>
        {n.source ? <span>· {n.source}</span> : null}
      </div>
    </li>
  );
}
