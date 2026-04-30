export function LoadingSkeleton({ label = "Loading…" }: { label?: string }) {
  return (
    <div
      role="status"
      aria-live="polite"
      className="flex animate-pulse items-center justify-center rounded-md border border-dashed border-slate-200 dark:border-slate-800 bg-white p-12 text-sm text-slate-400"
    >
      {label}
    </div>
  );
}
