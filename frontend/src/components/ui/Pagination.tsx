/**
 * Reusable numbered pagination bar.
 *
 * Renders Previous / page numbers / Next with ellipsis for large ranges.
 * Designed for operator surfaces — compact, slate palette, tabular layout.
 */

// ---------------------------------------------------------------------------
// Page range with ellipsis
// ---------------------------------------------------------------------------

/** Visible page slots: always show first, last, and a window around current. */
function pageRange(current: number, total: number): (number | "ellipsis")[] {
  if (total <= 7) {
    return Array.from({ length: total }, (_, i) => i);
  }

  const pages = new Set<number>();
  // Always include first and last
  pages.add(0);
  pages.add(total - 1);
  // Window of 1 around current
  for (let i = current - 1; i <= current + 1; i++) {
    if (i >= 0 && i < total) pages.add(i);
  }

  const sorted = [...pages].sort((a, b) => a - b);
  const result: (number | "ellipsis")[] = [];
  for (let i = 0; i < sorted.length; i++) {
    if (i > 0 && sorted[i]! - sorted[i - 1]! > 1) {
      result.push("ellipsis");
    }
    result.push(sorted[i]!);
  }
  return result;
}

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

export interface PaginationProps {
  /** Zero-based current page index. */
  page: number;
  /** Total number of pages. */
  totalPages: number;
  /** Called with the new zero-based page index. */
  onPageChange: (page: number) => void;
}

const BTN_BASE =
  "rounded border px-2 py-1 text-xs font-medium transition-colors";
const BTN_IDLE =
  "border-slate-200 bg-white text-slate-600 hover:bg-slate-50";
const BTN_ACTIVE =
  "border-blue-400 bg-blue-50 text-blue-700";
const BTN_DISABLED =
  "border-slate-200 bg-white text-slate-300 cursor-not-allowed";

export function Pagination({ page, totalPages, onPageChange }: PaginationProps) {
  if (totalPages <= 1) return null;

  const slots = pageRange(page, totalPages);

  return (
    <div className="mt-3 flex items-center justify-between text-xs text-slate-500">
      <span>
        Page {page + 1} of {totalPages}
      </span>
      <div className="flex gap-1">
        <button
          type="button"
          disabled={page === 0}
          onClick={() => onPageChange(page - 1)}
          className={`${BTN_BASE} ${page === 0 ? BTN_DISABLED : BTN_IDLE}`}
        >
          Previous
        </button>

        {slots.map((slot, i) =>
          slot === "ellipsis" ? (
            <span key={`e${i}`} className="px-1 text-slate-400">
              ...
            </span>
          ) : (
            <button
              key={slot}
              type="button"
              onClick={() => onPageChange(slot)}
              className={`${BTN_BASE} ${slot === page ? BTN_ACTIVE : BTN_IDLE}`}
              aria-current={slot === page ? "page" : undefined}
            >
              {slot + 1}
            </button>
          ),
        )}

        <button
          type="button"
          disabled={page >= totalPages - 1}
          onClick={() => onPageChange(page + 1)}
          className={`${BTN_BASE} ${page >= totalPages - 1 ? BTN_DISABLED : BTN_IDLE}`}
        >
          Next
        </button>
      </div>
    </div>
  );
}
