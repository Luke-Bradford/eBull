/**
 * OfferingBlock — parsed 424B cover offering (Reg S-K Item 501(b)(3))
 * rendered inline on a Filings-tab row (#1978, data path #1816).
 *
 * Money fields are all-nullable by contract: NULL means the cover
 * presentation was not resolvable (resale shelves, percent-of-principal
 * notes) — never a guessed value. When no money field is present the
 * block renders an honest one-liner instead of an empty grid.
 */

import type { OfferingSummary } from "@/api/types";
import { formatBigNumber, formatMoney } from "@/lib/format";

function offeringKindLabel(offering: OfferingSummary): string {
  if (offering.is_issuer_offering === true) return "issuer offering";
  if (offering.is_issuer_offering === false) return "resale by holders";
  return "offering";
}

export interface OfferingBlockProps {
  readonly offering: OfferingSummary;
}

export function OfferingBlock({ offering }: OfferingBlockProps): JSX.Element {
  const moneyRows: Array<{ label: string; value: string }> = [];
  if (offering.price_per_unit !== null) {
    moneyRows.push({
      label: offering.unit_label ?? "Per unit",
      value: formatMoney(offering.price_per_unit, offering.currency),
    });
  }
  if (offering.aggregate_offering_amount !== null) {
    moneyRows.push({
      label: "Aggregate",
      value: formatBigNumber(offering.aggregate_offering_amount),
    });
  }
  if (offering.underwriting_discount !== null) {
    moneyRows.push({
      label: "Underwriting discount",
      value: formatBigNumber(offering.underwriting_discount),
    });
  }
  if (offering.net_proceeds_to_issuer !== null) {
    moneyRows.push({
      label: "Net to issuer",
      value: formatBigNumber(offering.net_proceeds_to_issuer),
    });
  }
  if (offering.proceeds_to_selling_holders !== null) {
    moneyRows.push({
      label: "To selling holders",
      value: formatBigNumber(offering.proceeds_to_selling_holders),
    });
  }

  return (
    <div className="mt-1 rounded border border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-800/50 px-2 py-1.5 text-xs">
      <div className="flex items-baseline gap-2">
        <span className="font-medium text-slate-700 dark:text-slate-200">
          {offering.security_type ?? "Security"}
        </span>
        <span className="text-slate-500 dark:text-slate-400">{offeringKindLabel(offering)}</span>
        <span className="text-slate-400 dark:text-slate-500">
          {offering.currency}
        </span>
      </div>
      {moneyRows.length > 0 ? (
        <dl className="mt-1 flex flex-wrap gap-x-4 gap-y-0.5">
          {moneyRows.map((r) => (
            <div key={r.label} className="flex items-baseline gap-1">
              <dt className="text-slate-500 dark:text-slate-400">{r.label}</dt>
              <dd className="font-medium tabular-nums text-slate-700 dark:text-slate-200">
                {r.value}
              </dd>
            </div>
          ))}
        </dl>
      ) : (
        <p className="mt-0.5 text-slate-500 dark:text-slate-400">
          No priced cover table in this prospectus (typical for resale or
          percent-of-principal covers).
        </p>
      )}
    </div>
  );
}
