import { providerLabel } from "@/lib/capabilityProviders";

export interface PaneHeaderProps {
  readonly title: string;
  readonly scope?: string;
  readonly source?: {
    readonly providers: ReadonlyArray<string>;
    readonly lastSync?: string;
  };
  /**
   * Renders an "Open →" button. Click handler is invoked on click;
   * propagation is stopped so an enclosing Pane with `onCardClick`
   * doesn't also fire.
   */
  readonly onExpand?: () => void;
}

/**
 * PaneHeader — small-caps editorial title row (design-system v1).
 *
 * Lives directly under the Pane's hairline top-rule with no extra
 * border-bottom — the rule above the title is the visual section
 * marker. Tracking-[0.08em] (slightly tighter than tracking-widest)
 * keeps the small-caps feeling intentional without going into
 * decorative-label territory.
 *
 * Open button uses amber-500 hover (Bloomberg-echo accent reserved
 * for interactivity) instead of sky — keeps blue free for charts.
 */
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
    <header className="flex items-baseline justify-between gap-2">
      <div className="flex min-w-0 items-baseline gap-2">
        <h2 className="text-[11px] font-semibold uppercase tracking-[0.08em] text-slate-700">
          {title}
        </h2>
        {scope ? (
          <span className="text-[10px] text-slate-500">{scope}</span>
        ) : null}
      </div>
      <div className="flex flex-shrink-0 items-center gap-2">
        {sourceText !== null ? (
          <span
            className="truncate text-[10px] uppercase tracking-wide text-slate-400"
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
            className="text-[11px] font-medium text-slate-600 transition-colors hover:text-amber-600 focus-visible:rounded focus-visible:outline-2 focus-visible:outline-amber-500"
          >
            Open →
          </button>
        ) : null}
      </div>
    </header>
  );
}
