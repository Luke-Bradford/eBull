import type { InstrumentSummary } from "@/api/types";
import { activeProviders } from "@/lib/capabilityProviders";

export type DensityProfile = "full-sec" | "partial-filings" | "minimal";

export const EMPTY_CELL: { providers: string[]; data_present: Record<string, boolean> } = {
  providers: [],
  data_present: {},
};

export function selectProfile(summary: InstrumentSummary): DensityProfile {
  const cap = summary.capabilities;
  const fundCell = cap.fundamentals ?? EMPTY_CELL;
  const hasFundamentals =
    fundCell.providers.includes("sec_xbrl") &&
    fundCell.data_present["sec_xbrl"] === true;
  const hasFilings = activeProviders(cap.filings ?? EMPTY_CELL).length > 0;

  if (hasFundamentals && hasFilings) return "full-sec";
  if (hasFilings) return "partial-filings";
  return "minimal";
}
