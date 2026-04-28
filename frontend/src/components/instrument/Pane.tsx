import type { ReactNode } from "react";

import { PaneHeader } from "./PaneHeader";
import type { PaneHeaderProps } from "./PaneHeader";

export interface PaneProps extends PaneHeaderProps {
  readonly children: ReactNode;
  /** Optional className overrides on the outer article. */
  readonly className?: string;
  /**
   * Optional whole-card click handler. When provided, the Pane gets a
   * cursor-pointer + hover-elevate affordance and clicking anywhere on
   * the card invokes the handler. Internal interactive controls (e.g.
   * range pickers) must call `e.stopPropagation()` so they don't also
   * trigger this handler. The PaneHeader's "Open →" button stops
   * propagation automatically — see PaneHeader.tsx.
   *
   * Accessibility: the article does NOT receive `role="button"` or
   * `tabIndex` because it contains real `<button>` descendants
   * (PaneHeader Open button, in-pane controls). Nesting interactive
   * elements inside a custom button is an ARIA violation and can
   * cause assistive tech to flatten the inner controls. Keyboard
   * users navigate via the inner Open button instead — that button
   * is always rendered when `onExpand` is provided, so the drill is
   * keyboard-reachable without needing a card-level handler.
   */
  readonly onCardClick?: () => void;
  /**
   * Stretch the pane (and its single child) to fill the parent grid
   * cell vertically. Use when the cell uses `lg:row-span-N` to span
   * multiple rows — without this, the child sits at its intrinsic
   * height and leaves whitespace below when the right rail is taller.
   */
  readonly fillHeight?: boolean;
}

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
  const childCls = fillHeight ? "mt-2 flex min-h-0 flex-1 flex-col" : "mt-2";
  // Build the article className from atomic segments so optional
  // segments do not leave double spaces when omitted (e.g.
  // `shadow-sm  ` when fillHeight is off + non-clickable + no
  // `className` override). Cheap to read, matches the conditional
  // class pattern used elsewhere in this file's siblings.
  const articleCls = [
    "rounded-md border border-slate-200 bg-white px-3 py-2.5 shadow-sm",
    fillHeight ? "flex h-full flex-col" : null,
    clickable ? "cursor-pointer transition hover:border-slate-300 hover:shadow-md" : null,
    className ?? null,
  ]
    .filter((x): x is string => x !== null)
    .join(" ");
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
