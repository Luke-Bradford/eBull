import { DensityGrid } from "@/components/instrument/DensityGrid";
import type { InstrumentSummary, ThesisDetail } from "@/api/types";

export interface ResearchTabProps {
  readonly summary: InstrumentSummary;
  readonly thesis: ThesisDetail | null;
  readonly thesisErrored?: boolean;
}

export function ResearchTab({
  summary,
  thesis,
  thesisErrored = false,
}: ResearchTabProps): JSX.Element {
  return (
    <DensityGrid
      summary={summary}
      thesis={thesis}
      thesisErrored={thesisErrored}
    />
  );
}
