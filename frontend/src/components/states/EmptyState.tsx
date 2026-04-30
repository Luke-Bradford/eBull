import type { ReactNode } from "react";

export function EmptyState({
  title,
  description,
  children,
}: {
  title: string;
  description?: string;
  children?: ReactNode;
}) {
  return (
    <div className="flex flex-col items-center justify-center rounded-md border border-dashed border-slate-200 dark:border-slate-800 bg-white p-12 text-center">
      <h2 className="text-base font-semibold text-slate-700">{title}</h2>
      {description ? (
        <p className="mt-1 max-w-md text-sm text-slate-500">{description}</p>
      ) : null}
      {children ? <div className="mt-4">{children}</div> : null}
    </div>
  );
}
