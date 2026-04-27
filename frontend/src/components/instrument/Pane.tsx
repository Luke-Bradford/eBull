import type { ReactNode } from "react";

import { PaneHeader } from "./PaneHeader";
import type { PaneHeaderProps } from "./PaneHeader";

export interface PaneProps extends PaneHeaderProps {
  readonly children: ReactNode;
  /** Optional className overrides on the outer article. */
  readonly className?: string;
}

export function Pane({
  title,
  scope,
  source,
  onExpand,
  className,
  children,
}: PaneProps): JSX.Element {
  return (
    <article
      className={`rounded-md border border-slate-200 bg-white px-3 py-2.5 shadow-sm ${className ?? ""}`}
    >
      <PaneHeader
        title={title}
        scope={scope}
        source={source}
        onExpand={onExpand}
      />
      <div className="mt-2">{children}</div>
    </article>
  );
}
