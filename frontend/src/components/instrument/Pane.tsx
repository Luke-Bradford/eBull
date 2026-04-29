import type { ReactNode } from "react";

import { PaneHeader } from "./PaneHeader";
import type { PaneHeaderProps } from "./PaneHeader";

export interface PaneProps extends PaneHeaderProps {
  readonly children: ReactNode;
  /** Optional className overrides on the outer article. */
  readonly className?: string;
  /**
   * Optional whole-card click handler. When provided, clicking anywhere
   * on the card invokes the handler. Internal interactive controls
   * (range pickers, etc.) must call `e.stopPropagation()` so they don't
   * also fire this handler. PaneHeader's "Open →" button stops
   * propagation automatically.
   *
   * Accessibility: the article does NOT receive `role="button"` or
   * `tabIndex` because it contains real `<button>` descendants. Nesting
   * interactive elements inside a custom button is an ARIA violation
   * and assistive tech may flatten the inner controls. Keyboard users
   * navigate via the inner Open button instead.
   */
  readonly onCardClick?: () => void;
  /**
   * Stretch the pane (and its single child) to fill the parent grid
   * cell vertically. Use when the cell uses `lg:row-span-N` to span
   * multiple rows.
   */
  readonly fillHeight?: boolean;
}

/**
 * Pane — borderless editorial chrome (design-system v1).
 *
 * Replaces the prior rounded card (border + shadow + bg-white) with a
 * single hairline rule on top + a small-caps title row. The grid reads
 * as one continuous editorial spread instead of a Trello-board of
 * tiles, which suits financial data (operators scan across panes
 * looking for cross-pane signal: revenue trend → dividend yield →
 * insider buying — that's one document, not separate cards).
 *
 * Visual grouping comes from the top rule + title pair, not from a
 * bordered box. Hover affordance for clickable panes is a subtle
 * slate-50 tint, not a shadow lift (no layout shift).
 *
 * Horizontal padding is zero so the rule extends edge-to-edge of the
 * grid cell — the parent grid container owns horizontal rhythm.
 */
export function Pane({
  title,
  scope,
  source,
  onExpand,
  className,
  onCardClick,
  fillHeight = false,
  children,
}: PaneProps): JSX.Element {
  const clickable = onCardClick !== undefined;
  // Atomic className segments — null entries are filtered so optional
  // pieces do not leave double spaces when omitted.
  const articleCls = [
    "border-t border-slate-200 pt-3 pb-1",
    fillHeight ? "flex h-full flex-col" : null,
    clickable ? "cursor-pointer transition-colors hover:bg-slate-50/60" : null,
    className ?? null,
  ]
    .filter((x): x is string => x !== null)
    .join(" ");
  const childCls = fillHeight ? "mt-3 flex min-h-0 flex-1 flex-col" : "mt-3";
  return (
    <article
      className={articleCls}
      onClick={clickable ? onCardClick : undefined}
      data-clickable={clickable ? "true" : undefined}
    >
      <PaneHeader
        title={title}
        scope={scope}
        source={source}
        onExpand={onExpand}
      />
      <div className={childCls}>{children}</div>
    </article>
  );
}
