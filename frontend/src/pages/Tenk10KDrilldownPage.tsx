/**
 * /instrument/:symbol/filings/10-k[?accession=...] — full SEC 10-K
 * Item 1 drilldown (#559).
 *
 * Three-pane layout:
 *   - Left rail (180 px): TOC built from section_order + label
 *   - Center reader: full-width body with continuous vertical
 *     left rail (CSS ::before, not section borders, so multi-block
 *     children don't break the line)
 *   - Right rail (200 px): filing accession, prior 10-Ks list,
 *     cross-related items
 *
 * The body renders prose with embedded <table> blocks at sentinel
 * positions and cross-ref chips that pop a 240-char preview popover.
 *
 * `?accession=` deep-links to a specific historical 10-K. Default
 * (no query string) renders the latest filing.
 */

import {
  fetchBusinessSections,
  fetchTenKHistory,
  type BusinessCrossReference,
  type BusinessSection,
  type BusinessSectionsResponse,
  type TenKHistoryResponse,
} from "@/api/instruments";
import { CrossRefPopover } from "@/components/instrument/CrossRefPopover";
import { Section, SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { EmbeddedTable } from "@/components/instrument/EmbeddedTable";
import { EmptyState } from "@/components/states/EmptyState";
import { TenKMetadataRail } from "@/components/instrument/TenKMetadataRail";
import { useAsync } from "@/lib/useAsync";
import { useCallback } from "react";
import { Link, useParams, useSearchParams } from "react-router-dom";

// Sentinel kept in sync with app/services/business_summary.py
const TABLE_SENTINEL_RE = /␞TABLE_(\d+)␞/g;

function sectionAnchorId(s: BusinessSection): string {
  return `s-${s.section_order}-${s.section_key}`;
}

interface BodyPart {
  type: "prose" | "table";
  prose?: string;
  tableOrder?: number;
}

function splitBodyByTables(body: string): BodyPart[] {
  const parts: BodyPart[] = [];
  let cursor = 0;
  for (const m of body.matchAll(TABLE_SENTINEL_RE)) {
    const before = body.slice(cursor, m.index);
    if (before.trim().length > 0) parts.push({ type: "prose", prose: before });
    parts.push({ type: "table", tableOrder: Number(m[1]) });
    cursor = (m.index ?? 0) + m[0].length;
  }
  const tail = body.slice(cursor);
  if (tail.trim().length > 0) parts.push({ type: "prose", prose: tail });
  return parts;
}

function renderProseWithCrossRefs(
  prose: string,
  crefs: ReadonlyArray<BusinessCrossReference>,
  sections: ReadonlyArray<BusinessSection>,
  secSearchUrl: string | null,
): JSX.Element {
  // Build a single regex matching every cref.target, longest-first to
  // avoid "Item 1" eating "Item 1A".
  const targets = [...new Set(crefs.map((c) => c.target))].sort(
    (a, b) => b.length - a.length,
  );
  if (targets.length === 0) {
    return <p className="whitespace-pre-wrap leading-relaxed text-slate-700">{prose}</p>;
  }
  const escaped = targets.map((t) => t.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"));
  const re = new RegExp(`\\b(${escaped.join("|")})\\b`, "g");
  const parts: (string | JSX.Element)[] = [];
  let cursor = 0;
  let key = 0;
  for (const m of prose.matchAll(re)) {
    const idx = m.index ?? 0;
    if (idx > cursor) parts.push(prose.slice(cursor, idx));
    const cref = crefs.find((c) => c.target === m[1]);
    if (cref !== undefined) {
      parts.push(
        <CrossRefPopover
          key={`cref-${key++}`}
          cref={cref}
          sections={sections}
          secSearchUrl={secSearchUrl}
        />,
      );
    } else {
      parts.push(m[0]);
    }
    cursor = idx + m[0].length;
  }
  if (cursor < prose.length) parts.push(prose.slice(cursor));
  return <p className="whitespace-pre-wrap leading-relaxed text-slate-700">{parts}</p>;
}

function SectionBody({
  section,
  allSections,
  secSearchUrl,
}: {
  readonly section: BusinessSection;
  readonly allSections: ReadonlyArray<BusinessSection>;
  readonly secSearchUrl: string | null;
}) {
  const parts = splitBodyByTables(section.body);
  return (
    <article
      id={sectionAnchorId(section)}
      className="relative pl-6 before:absolute before:bottom-0 before:left-0 before:top-0 before:w-0.5 before:bg-slate-200"
    >
      <h3 className="text-base font-semibold text-slate-900 dark:text-slate-100">{section.section_label}</h3>
      <div className="mt-2 space-y-3 text-sm">
        {parts.map((p, i) => {
          if (p.type === "prose" && p.prose !== undefined) {
            return (
              <div key={i}>
                {renderProseWithCrossRefs(
                  p.prose,
                  section.cross_references,
                  allSections,
                  secSearchUrl,
                )}
              </div>
            );
          }
          if (p.type === "table" && p.tableOrder !== undefined) {
            const t = section.tables[p.tableOrder];
            if (t === undefined) return null;
            return <EmbeddedTable key={i} table={t} />;
          }
          return null;
        })}
      </div>
    </article>
  );
}

function TOCRail({ sections }: { readonly sections: ReadonlyArray<BusinessSection> }) {
  return (
    <nav className="sticky top-4 max-h-[calc(100vh-2rem)] overflow-y-auto text-xs">
      <div className="mb-2 text-[10px] font-medium uppercase tracking-wider text-slate-500">
        Sections
      </div>
      <ul className="space-y-1">
        {sections.map((s) => (
          <li key={sectionAnchorId(s)}>
            <a
              href={`#${sectionAnchorId(s)}`}
              className="block truncate text-slate-700 hover:text-sky-700 hover:underline"
              title={s.section_label}
            >
              {s.section_label}
            </a>
          </li>
        ))}
      </ul>
    </nav>
  );
}

function secSearchUrlFor(
  accession: string | null,
  cik: string | null,
): string | null {
  if (accession === null) return null;
  // #563: prefer the iXBRL viewer when both CIK + accession are
  // available — it lands on the specific filing's iXBRL document
  // rather than an EDGAR full-text result list. Strip the leading
  // zero padding from CIK so the viewer URL matches EDGAR's expected
  // format (the API column stores the zero-padded form).
  if (cik !== null) {
    const cikTrimmed = cik.replace(/^0+/, "") || "0";
    return (
      `https://www.sec.gov/cgi-bin/viewer?action=view` +
      `&cik=${encodeURIComponent(cikTrimmed)}` +
      `&accession_number=${encodeURIComponent(accession)}` +
      `&type=10-K`
    );
  }
  // Fallback: EDGAR full-text search by accession. Used when the
  // instrument has no primary SEC CIK link (non-US tickers etc.).
  return `https://efts.sec.gov/LATEST/search-index?q=%22${encodeURIComponent(accession)}%22&forms=10-K,10-K%2FA`;
}

function Body({
  data,
  history,
  historyLoading,
  historyError,
  symbol,
}: {
  readonly data: BusinessSectionsResponse;
  readonly history: TenKHistoryResponse;
  readonly historyLoading: boolean;
  readonly historyError: unknown;
  readonly symbol: string;
}) {
  if (data.sections.length === 0) {
    return (
      <EmptyState
        title="No 10-K Item 1 on file"
        description="No 10-K business description has been parsed for this instrument yet."
      />
    );
  }
  const allCrefs = data.sections.flatMap((s) => s.cross_references);
  const relatedItems = [
    ...new Set(
      allCrefs
        .filter((c) => c.reference_type === "item")
        .map((c) => c.target),
    ),
  ];
  const secSearchUrl = secSearchUrlFor(data.source_accession, data.cik);

  return (
    <div className="grid gap-6 lg:grid-cols-[180px_minmax(0,1fr)_200px]">
      <aside className="hidden lg:block">
        <TOCRail sections={data.sections} />
      </aside>
      <div className="min-w-0 space-y-6">
        <header className="border-b border-slate-200 dark:border-slate-800 pb-3">
          <Link
            to={`/instrument/${encodeURIComponent(symbol)}`}
            className="text-xs text-sky-700 hover:underline"
          >
            ← Back to {symbol}
          </Link>
          <h2 className="mt-1 text-lg font-semibold text-slate-900 dark:text-slate-100">
            Form 10-K · Item 1 Business
          </h2>
        </header>
        {data.sections.map((s) => (
          <SectionBody
            key={sectionAnchorId(s)}
            section={s}
            allSections={data.sections}
            secSearchUrl={secSearchUrl}
          />
        ))}
      </div>
      <div className="hidden lg:block">
        {historyLoading ? (
          <SectionSkeleton rows={4} />
        ) : historyError !== null ? (
          <p className="text-xs text-amber-700">
            Filing history unavailable — couldn't load prior 10-Ks.
          </p>
        ) : (
          <TenKMetadataRail
            symbol={symbol}
            currentAccession={data.source_accession}
            history={history.filings}
            relatedItems={relatedItems}
          />
        )}
      </div>
    </div>
  );
}

export function Tenk10KDrilldownPage() {
  const { symbol = "" } = useParams<{ symbol: string }>();
  const [searchParams] = useSearchParams();
  const accession = searchParams.get("accession") ?? undefined;

  const sectionsState = useAsync<BusinessSectionsResponse>(
    useCallback(() => fetchBusinessSections(symbol, accession), [symbol, accession]),
    [symbol, accession],
  );
  const historyState = useAsync<TenKHistoryResponse>(
    useCallback(() => fetchTenKHistory(symbol), [symbol]),
    [symbol],
  );

  return (
    <div className="mx-auto max-w-screen-2xl p-4">
      <Section title={`${symbol} — 10-K narrative`}>
        {sectionsState.loading ? (
          <SectionSkeleton rows={6} />
        ) : sectionsState.error !== null ? (
          <SectionError onRetry={sectionsState.refetch} />
        ) : sectionsState.data === null ? (
          <EmptyState
            title="Business narrative unavailable"
            description="Could not load 10-K Item 1 sections for this instrument."
          />
        ) : (
          <Body
            data={sectionsState.data}
            history={historyState.data ?? { symbol, filings: [] }}
            historyLoading={historyState.loading}
            historyError={historyState.error}
            symbol={symbol}
          />
        )}
      </Section>
    </div>
  );
}
