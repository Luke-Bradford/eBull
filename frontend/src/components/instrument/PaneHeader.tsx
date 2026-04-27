import { providerLabel } from "@/lib/capabilityProviders";

export interface PaneHeaderProps {
  readonly title: string;
  readonly scope?: string;
  readonly source?: {
    readonly providers: ReadonlyArray<string>;
    readonly lastSync?: string;
  };
  /**
   * Renders an "Open →" button. The handler is invoked on click; the
   * click event's propagation is stopped so an enclosing Pane with
   * `onCardClick` doesn't also fire.
   */
  readonly onExpand?: () => void;
}

export function PaneHeader({
  title,
  scope,
  source,
  onExpand,
}: PaneHeaderProps): JSX.Element {
  const sourceText =
    source && source.providers.length > 0
      ? source.providers.map(providerLabel).join(" · ") +
        (source.lastSync ? ` · ${source.lastSync}` : "")
      : null;
  return (
    <header className="flex items-baseline justify-between gap-2 border-b border-slate-100 pb-1.5">
      <div className="flex min-w-0 items-baseline gap-2">
        <h2 className="text-xs font-semibold uppercase tracking-wider text-slate-600">
          {title}
        </h2>
        {scope ? (
          <span className="text-[10px] text-slate-500">{scope}</span>
        ) : null}
      </div>
      <div className="flex flex-shrink-0 items-center gap-2">
        {sourceText !== null ? (
          <span
            className="truncate rounded bg-slate-100 px-1.5 py-0.5 text-[10px] text-slate-600"
            title={sourceText}
          >
            {sourceText}
          </span>
        ) : null}
        {onExpand !== undefined ? (
          <button
            type="button"
            onClick={(e) => {
              e.stopPropagation();
              onExpand();
            }}
            className="text-[11px] text-sky-700 hover:underline focus-visible:rounded focus-visible:outline-2 focus-visible:outline-sky-500"
          >
            Open →
          </button>
        ) : null}
      </div>
    </header>
  );
}
