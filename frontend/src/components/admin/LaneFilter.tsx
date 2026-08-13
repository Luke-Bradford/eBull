/**
 * LaneFilter — chip row for the ProcessesTable (#1076 / #1064).
 *
 * Spec §Information architecture: chips are
 * `[All] [Setup] [Universe] [Candles] [SEC] [Ownership] [Fundamentals] [Ops] [AI]`.
 *
 * `null` selection = All. Selecting a lane chip filters the table to
 * that lane only; clicking it again deselects. Single-select; the
 * spec does not call for multi-lane composition in v1.
 */

import type { ProcessLane } from "@/api/types";

const LANES: { value: ProcessLane; label: string }[] = [
  { value: "setup", label: "Setup" },
  { value: "universe", label: "Universe" },
  { value: "candles", label: "Candles" },
  { value: "sec", label: "SEC" },
  { value: "ownership", label: "Ownership" },
  { value: "fundamentals", label: "Fundamentals" },
  { value: "ops", label: "Ops" },
  { value: "ai", label: "AI" },
];

export function LaneFilter({
  selected,
  counts,
  onSelect,
}: {
  selected: ProcessLane | null;
  counts: Partial<Record<ProcessLane, number>>;
  onSelect: (lane: ProcessLane | null) => void;
}) {
  return (
    <div
      className="flex flex-wrap items-center gap-1.5"
      role="toolbar"
      aria-label="Filter processes by lane"
    >
      <Chip
        label="All"
        selected={selected === null}
        onClick={() => onSelect(null)}
      />
      {LANES.map((lane) => {
        const count = counts[lane.value] ?? 0;
        return (
          <Chip
            key={lane.value}
            label={lane.label}
            count={count}
            selected={selected === lane.value}
            onClick={() => onSelect(selected === lane.value ? null : lane.value)}
          />
        );
      })}
    </div>
  );
}

function Chip({
  label,
  count,
  selected,
  onClick,
}: {
  label: string;
  count?: number;
  selected: boolean;
  onClick: () => void;
}) {
  const tone = selected
    ? "border-blue-300 bg-blue-50 text-blue-800 dark:border-blue-700 dark:bg-blue-950/50 dark:text-blue-200"
    : "border-slate-200 bg-white text-slate-700 hover:bg-slate-50 dark:border-slate-800 dark:bg-slate-900 dark:text-slate-200 dark:hover:bg-slate-800/40";
  return (
    <button
      type="button"
      onClick={onClick}
      aria-pressed={selected}
      className={`rounded-full border px-2.5 py-0.5 text-xs font-medium transition-colors ${tone}`}
    >
      {label}
      {count !== undefined ? (
        <span className="ml-1 text-[10px] text-slate-500 dark:text-slate-400">
          {count}
        </span>
      ) : null}
    </button>
  );
}
