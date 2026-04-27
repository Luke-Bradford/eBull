import type { InstrumentSummary } from "@/api/types";
import { activeProviders } from "@/lib/capabilityProviders";

export type DensityProfile = "full-sec" | "partial-filings" | "minimal";

export const EMPTY_CELL: { providers: string[]; data_present: Record<string, boolean> } = {
  providers: [],
  data_present: {},
};

export function hasFundamentalsActive(summary: InstrumentSummary): boolean {
  const fundCell = summary.capabilities.fundamentals ?? EMPTY_CELL;
  return (
    fundCell.providers.includes("sec_xbrl") &&
    fundCell.data_present["sec_xbrl"] === true
  );
}

export function selectProfile(summary: InstrumentSummary): DensityProfile {
  const hasFunds = hasFundamentalsActive(summary);
  const hasFilings =
    activeProviders(summary.capabilities.filings ?? EMPTY_CELL).length > 0;

  if (hasFunds && hasFilings) return "full-sec";
  if (hasFunds || hasFilings || summary.has_sec_cik) return "partial-filings";
  return "minimal";
}
