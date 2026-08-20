# Tender/going-private FE render — TenderBlock on the Filings tab

Issue: #2028 (follow-up child of #1982). Branch: `feature/2028-tender-fe-render`.

## Goal

Render the already-shipped `tender` object inline on the Filings tab, one block per
filing row that carries a non-null `tender`, mirroring the OfferingBlock (424B, #1978)
precedent. FE-only; the backend contract, serializer, and data path shipped in #1982 / PR #2027.

## Source rule

No data-treatment decision here — the parser already classified role, transaction type, and
board recommendation; the FE is a passive render of the shipped `TenderEventSummary` contract.
The one source-anchored *display* choice is `board_recommendation`: its DB enum
(`accept`/`reject`/`neutral`/`unable`, `sql/224:76-77`) encodes the SEC **Item 1012(a)**
position vocabulary (parser spec `docs/specs/filings/2026-07-05-tender-going-private-parser.md:116-118`),
so the operator-facing phrasing must read as accept/reject/neutral/unable, not invented wording.

## Settled decisions

None apply — `grep -niE 'filings tab|tender|offering block|frontend render|per-form|filing detail' docs/settled-decisions.md` returns nothing. This render introduces no new settled decision (scoring-neutral, no `model_version` bump — parser spec `:313`).

## Prevention-log entries that apply

- **FE↔BE type parity, same PR** (`review-prevention-log.md:473-474`, `.claude/skills/frontend/api-shape-and-types.md`): `types.ts` mirrors the Pydantic class field-for-field, nullability exact, snake_case on the wire. `apiFetch<T>` does no runtime validation, so a wrong type deserializes silently to `undefined`.
- **Nullable-checkbox discipline** (parser spec `:138-139`): the transaction-type flags are `bool | None`; `null ≠ false`. The type-label helper picks the first flag that is `=== true` and falls to a neutral label when all are null — never inferred from a null/false.
- **Loading/empty/error owned by the parent** (`.claude/skills/frontend/loading-error-empty-states.md`): `FilingsTab` already answers all three (`InstrumentPage.tsx:448-459`). The block is a pure presentational leaf mounted only behind a `!== null` guard — no own skeleton/error surface.

## Backend contract (already shipped — no change)

`GET /filings/{instrument_id}` → `FilingsListResponse.items[].tender: TenderEventSummary | None`
(`app/api/filings.py:90-113,148`, serializer `_parse_tender` `:254-279`, LEFT JOIN on the table PK
`(accession_number, instrument_id)` `:412-415` → at most one tender row per item). `TenderEventSummary`,
14 fields, only `role` + `subject_company_name` non-null:

| # | field | TS type |
|---|-------|---------|
| 1 | `role` | `"subject" \| "offeror"` |
| 2 | `subject_company_name` | `string` |
| 3 | `offeror_names` | `string[] \| null` |
| 4 | `is_third_party_tender` | `boolean \| null` |
| 5 | `is_issuer_tender` | `boolean \| null` |
| 6 | `is_going_private` | `boolean \| null` |
| 7 | `amends_13d` | `boolean \| null` |
| 8 | `is_final_amendment` | `boolean \| null` |
| 9 | `amendment_no` | `number \| null` |
| 10 | `offer_price_per_unit` | `number \| null` |
| 11 | `unit_label` | `string \| null` |
| 12 | `currency` | `string \| null` |
| 13 | `expiration_date` | `string \| null` (ISO `YYYY-MM-DD`) |
| 14 | `board_recommendation` | `string \| null` (`accept`/`reject`/`neutral`/`unable`) |

## FE changes

### 1. `frontend/src/api/types.ts`
Add `TenderEventSummary` in the `/filings/{instrument_id}` section (beside `OfferingSummary`, `:1335`),
mirroring the table above in declaration order (name matches the Pydantic class exactly, as
`OfferingSummary` does). Add `tender: TenderEventSummary | null` on `FilingItem` after `offering` (`:1372`),
with the standard `null otherwise` comment. **Divergence from OfferingSummary to mirror exactly:**
`currency` is `string | null` here (OfferingSummary's is non-null `string`).

`board_recommendation` stays `string | null` (Codex ckpt-1 NIT REBUTTED): the Pydantic field is
`str | None`, not a `Literal`, and `api-shape-and-types.md` mandates an exact mirror (narrowing the FE
past the BE type is drift). `boardRecLabel`'s `default` branch handles any value outside the 4-state
enum defensively.

### 2. `frontend/src/components/instrument/TenderBlock.tsx` (new)
Pure presentational leaf, prop `readonly tender: TenderEventSummary`. Copy OfferingBlock's markup
verbatim — outer bordered `div` (`OfferingBlock.tsx:58`), header row (`:59-66`), `dl`/`dt`/`dd` KV grid
(`:68-77`). Reuse `formatMoney` + `formatDate` from `@/lib/format` (no new formatter).

Null-safe label helpers (mirror `offeringKindLabel`):
```ts
// Codex ckpt-1 BLOCKING: the 4 transaction-type checkboxes are ORTHOGONAL and stored
// uncollapsed by design (parser spec :21-25 — a TO-T can also be going-private / 13D-amending).
// Render ALL true flags — never collapse to one priority label. null/false → no flag
// (nullable-checkbox discipline). All-null/false → neutral "Tender offer".
function typeLabels(t: TenderEventSummary): string[] {
  const out: string[] = [];
  if (t.is_third_party_tender === true) out.push("Third-party tender");
  if (t.is_issuer_tender === true) out.push("Issuer self-tender");
  if (t.is_going_private === true) out.push("Going-private");
  if (t.amends_13d === true) out.push("13D amendment");
  return out.length > 0 ? out : ["Tender offer"];
}
// Source-faithful role term: `role` is the SGML SUBJECT-COMPANY vs FILED-BY block the
// instrument's CIK matched (parser spec :133-134) — "offeror", NOT "acquirer" (a mini-tender
// offeror is not necessarily an acquirer). Use the reg vocabulary.
function roleLabel(role: "subject" | "offeror"): string {
  return role === "offeror" ? "Offeror" : "Subject";
}
function boardRecLabel(v: string): string {
  switch (v) {
    case "accept": return "Recommends accepting";
    case "reject": return "Recommends rejecting";
    case "neutral": return "Neutral / no position";
    case "unable": return "Unable to take position";
    default: return v; // defensive — DB CHECK bounds this to the 4 above
  }
}
```

Header: `{typeLabels(tender).join(" · ")}` (font-medium) + `{roleLabel(tender.role)}` (muted),
mirroring OfferingBlock's two-span header. If `is_final_amendment === true` append muted
"· final amendment"; else if `amendment_no !== null` append muted `· amendment ${amendment_no}`.
(`is_final_amendment` / `amendment_no` are amendment STATUS, orthogonal to the type flags — kept
separate, not folded into `typeLabels`.)

KV rows (push only when present; `subject_company_name` is always present so the grid is never empty —
**no all-null one-liner branch is needed, unlike OfferingBlock**, because `subject_company_name` is
non-null by contract):
- `Subject` → `tender.subject_company_name` (always)
- `Offeror` → `tender.offeror_names.join(", ")` — only if `offeror_names !== null && length > 0`
- `Price` → only if `offer_price_per_unit !== null`. Value: `tender.currency !== null ? formatMoney(price, tender.currency) : formatNumber(price, 2)` — **never `currency ?? undefined`** (Codex ckpt-1 WARNING: that fabricates a GBP symbol when currency is null; render the bare number instead). Append ` / ${unit_label}` when `unit_label !== null`.
- `Expires` → `formatDate(tender.expiration_date)` — only if `expiration_date !== null`
- `Board` → `boardRecLabel(tender.board_recommendation)` — only if `board_recommendation !== null`

Color: keep OfferingBlock's neutral slate palette (no new color job per `operator-ui-conventions.md` — the "Going-private transaction" label carries the signal in text; KISS).

### 3. `frontend/src/pages/InstrumentPage.tsx`
Import `TenderBlock` (beside the OfferingBlock import, `:44`). Slot
`{f.tender !== null && <TenderBlock tender={f.tender} />}` immediately after the OfferingBlock slot
(`:484`), same guard shape.

### 4. `frontend/src/components/instrument/TenderBlock.test.tsx` (new)
Mirror `OfferingBlock.test.tsx` (real verified dev fixtures). Cases:
- (a) third-party offeror with price + target + offeror + expiry → "Third-party tender", role "Offeror", Subject "NETSUITE INC", Offeror "ORACLE CORP", Price "US$109.00 / Share" render. Fixture: ORCL/NetSuite `0001193125-16-684804`.
- (b) subject 14D9 with `board_recommendation: "reject"`, `offer_price_per_unit: 8.61`, all checkbox flags null → neutral label "Tender offer", role "Subject", Board "Recommends rejecting", Expires, no Offeror row. Fixture: NHP/Comrit `0001104659-20-031431`.
- (c) **combo** (Codex ckpt-1): `is_third_party_tender: true` AND `is_going_private: true` → BOTH "Third-party tender" AND "Going-private" render (no collapse).
- (d) `is_final_amendment: true` → "· final amendment" suffix.
- (e) price present + `currency: null` → bare number (no fabricated currency symbol).

### 5. Existing FilingItem fixtures (Codex ckpt-1 WARNING)
`FilingItem.tender` is a new **required** field, so every existing `FilingItem` literal must add
`tender: null` or `pnpm typecheck` breaks. Affected fixtures (grep `offering:` under `frontend/src`):
`frontend/src/components/instrument/FilingsPane.test.tsx`,
`frontend/src/components/instrument/RightRail.test.tsx`.

## Out of scope

No backend/serializer change; no new fetcher (`tender` flows on `FilingItem` through the existing
`fetchFilings`); no scoring / `model_version` impact.

## Verification / DoD

- `pnpm --dir frontend typecheck` + `pnpm --dir frontend test:unit` green.
- FE-QA eyeball the Filings tab on **ORCL** (offeror, multi-event: NetSuite $109 / Textura $26 /
  Opower $10.30) and **NHP** (subject, SC TO-T + SC 14D9 board recommendation). Requires the
  `sec_tender` re-drain (in progress) complete first.
