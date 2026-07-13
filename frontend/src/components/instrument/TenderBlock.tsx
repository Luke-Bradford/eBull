/**
 * TenderBlock — parsed tender-offer / going-private event (Reg M-A: Schedule TO /
 * 14D-9 / 13E-3) rendered inline on a Filings-tab row (#1982, data path PR #2027).
 *
 * The four transaction-type checkbox flags are orthogonal and stored uncollapsed by
 * design — a third-party tender can also be going-private / 13D-amending. Every flag
 * that is `true` renders; null/false does not (nullable-checkbox discipline: null is
 * an unresolved cover box, never a guessed value). `role` and `subject_company_name`
 * are non-null by contract, so the block always has content — no empty-grid fallback
 * (unlike OfferingBlock, whose every field is nullable).
 */

import type { TenderEventSummary } from "@/api/types";
import { formatDate, formatMoney, formatNumber } from "@/lib/format";

// Orthogonal tri-state flags — render ALL that are true, never collapse to one label.
function typeLabels(t: TenderEventSummary): string[] {
  const out: string[] = [];
  if (t.is_third_party_tender === true) out.push("Third-party tender");
  if (t.is_issuer_tender === true) out.push("Issuer self-tender");
  if (t.is_going_private === true) out.push("Going-private");
  if (t.amends_13d === true) out.push("13D amendment");
  return out.length > 0 ? out : ["Tender offer"];
}

// Source-faithful role term (SGML SUBJECT-COMPANY vs FILED-BY block) — "offeror", not
// "acquirer". Lowercase to match OfferingBlock's muted secondary-label convention and to
// avoid colliding with the capitalised "Subject"/"Offeror" KV row labels below.
function roleLabel(role: "subject" | "offeror"): string {
  return role === "offeror" ? "offeror" : "subject";
}

// SEC Item 1012(a) board position (DB CHECK enum accept/reject/neutral/unable).
function boardRecLabel(v: string): string {
  switch (v) {
    case "accept":
      return "Recommends accepting";
    case "reject":
      return "Recommends rejecting";
    case "neutral":
      return "Neutral / no position";
    case "unable":
      return "Unable to take position";
    default:
      return v; // defensive — DB CHECK bounds this to the 4 above
  }
}

// Amendment STATUS (orthogonal to the transaction-type flags).
function amendmentSuffix(t: TenderEventSummary): string | null {
  if (t.is_final_amendment === true) return "final amendment";
  if (t.amendment_no !== null) return `amendment ${t.amendment_no}`;
  return null;
}

export interface TenderBlockProps {
  readonly tender: TenderEventSummary;
}

export function TenderBlock({ tender }: TenderBlockProps): JSX.Element {
  const rows: Array<{ label: string; value: string }> = [];
  rows.push({ label: "Subject", value: tender.subject_company_name });
  if (tender.offeror_names !== null && tender.offeror_names.length > 0) {
    rows.push({ label: "Offeror", value: tender.offeror_names.join(", ") });
  }
  if (tender.offer_price_per_unit !== null) {
    // Never fabricate a currency: formatMoney's default is GBP, so fall back to a bare
    // number when currency is null rather than pass `currency ?? undefined` (Codex ckpt-1).
    const price =
      tender.currency !== null
        ? formatMoney(tender.offer_price_per_unit, tender.currency)
        : formatNumber(tender.offer_price_per_unit, 2);
    rows.push({
      label: "Price",
      value: tender.unit_label !== null ? `${price} / ${tender.unit_label}` : price,
    });
  }
  if (tender.expiration_date !== null) {
    rows.push({ label: "Expires", value: formatDate(tender.expiration_date) });
  }
  if (tender.board_recommendation !== null) {
    rows.push({
      label: "Board",
      value: boardRecLabel(tender.board_recommendation),
    });
  }

  const suffix = amendmentSuffix(tender);

  return (
    <div className="mt-1 rounded border border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-800/50 px-2 py-1.5 text-xs">
      <div className="flex items-baseline gap-2">
        <span className="font-medium text-slate-700 dark:text-slate-200">
          {typeLabels(tender).join(" · ")}
        </span>
        <span className="text-slate-500 dark:text-slate-400">
          {roleLabel(tender.role)}
          {suffix !== null ? ` · ${suffix}` : ""}
        </span>
      </div>
      <dl className="mt-1 flex flex-wrap gap-x-4 gap-y-0.5">
        {rows.map((r) => (
          <div key={r.label} className="flex items-baseline gap-1">
            <dt className="text-slate-500 dark:text-slate-400">{r.label}</dt>
            <dd className="font-medium tabular-nums text-slate-700 dark:text-slate-200">
              {r.value}
            </dd>
          </div>
        ))}
      </dl>
    </div>
  );
}
