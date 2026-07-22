# DEF 14A PvP iXBRL NEO-name oracle (#2099)

Status: proposal (unshipped). Research ticket #2099; follows #2097 (role-boundary
name/title split, parser def14a-v5).

## Problem

The SCT (Item 402(c)(2)(i)) "Name and Principal Position" cell is free-form HTML;
every scraper (ours and edgartools', skill G16) shares its ambiguity, producing
the recurring per-case patch thread #2088 → #2094 → #2097 → #2100. The
standard-filing reuse check demands a structured source be tested before patch N.
Item 402(v) Pay-versus-Performance is that source: its disclosure — including
the footnote that NAMES every NEO — is Inline-XBRL tagged inside the same DEF 14A
HTML body we already store and parse.

## Source rule

- **17 CFR § 229.402(v)(3)** (Reg S-K Item 402(v)(3), verbatim from eCFR
  2026-07-01): footnotes to the PvP table must disclose "*the name of each named
  executive officer included as a PEO or in the calculation of the average
  remaining named executive officer compensation, and the fiscal years in which
  such persons are included*". NEO names are a **documented disclosure
  requirement**, not an inference.
- **§ 229.402(v)(7)**: the paragraph (v) disclosure "*including, but not limited
  to, any disclosure provided pursuant to paragraphs (v)(3) and (6)*" "*must be
  provided in an Interactive Data File in accordance with § 232.405*" (Reg S-T
  Rule 405 = Inline XBRL) — the (v)(3) names are **explicitly inside the tagging
  mandate**.
- **§ 229.402(v)(8) + SCT instruction**: smaller reporting companies file scaled
  PvP (3 years; 2 in the first filing; no peer-group TSR / Company-Selected
  Measure) and "*[a] smaller reporting company is required to comply with
  paragraph (v)(7) … in the third filing in which it provides the disclosure*"
  — SRC tagging lags two proxy seasons.
- **Exemptions** (SEC rule page for release 34-95607, 87 FR 55193): emerging
  growth companies, registered investment companies, foreign private issuers do
  NOT file PvP at all. First applies to fiscal years ending on/after 2022-12-16
  (2023 proxy season onward).
- **Taxonomy**: the ECD element is `PeoName`, taxonomy-labelled "PEO Name"
  (ECD taxonomy, `xbrl.sec.gov/ecd`); the taxonomy's documented home for
  non-PEO NEO names is the prose `NamedExecutiveOfficersFnTextBlock`. Facts sit
  in contexts dimensioned by `ExecutiveCategoryAxis` (`PeoMember` /
  `NonPeoNeoMember`) × `IndividualAxis` (one custom extension member per
  person), one fact per person per covered fiscal year. **Filer practice
  (empirical)**: a minority extend `PeoName` to non-PEO NEOs — AAPL tags all 7
  NEOs with explicit `NonPeoNeoMember` × per-person contexts (verified,
  `From2023-10-01…_custom_LucaMaestriMember_ecd_NonPeoNeoMember`); most tag
  PEOs only (JPM; SPNT's three names Egan/Malloy/Sankaran and CNTY's two are
  all PEOs/co-PEOs across the 5-yr window, NOT non-PEO NEOs). So the element
  is structurally a **PEO-name oracle with an optional all-NEO extension**;
  the coverage numbers below measure the blend.
- **Matching must be namespace-URI-resolved, not prefix-literal**: iXBRL fact
  `name` attributes carry the document's declared prefix; the ECD namespace is
  versioned yearly (`http://xbrl.sec.gov/ecd/YYYY…`). Resolve prefix → URI from
  the document's xmlns declarations and match on URI-starts-with
  `http://xbrl.sec.gov/ecd` + localname `PeoName` / `IndividualAxis` /
  `ExecutiveCategoryAxis` (Codex ckpt-1 M1).

## Empirical verification (stored `def14a_body`, dev DB, 2026-07-22)

Mechanism confirmed on real filings — AAPL `0001308179-26-000008` tags **all 7
NEOs** (PEO + 6 non-PEO) with per-person members; JPM `0000019617-26-000096`
tags **PEO names only** ("James Dimon"; members `jpm:DimonMember` /
`jpm:PintoMember`, surname-only); HD `0000354950-26-000090` and MSFT
`0001193125-25-245150` tag **no names structurally** (prose
`ecd:NamedExecutiveOfficersFnTextBlock` only). Value-quality hazards, all
observed:

| hazard | example |
|---|---|
| honorific form, not legal name | AAPL PEO fact = "Mr. Cook"; CNTY "Dr. Erwin Haitzmann" |
| render wrap INSIDE the tagged fact value | AAPL "Kevan\n Parekh"; SPNT "Scott\n Egan" |
| HTML entities | "Deirdre O&#8217;Brien" |
| filer typo in the tagged fact | TechnipFMC `0001140361-24-019443` oracle "Douglas **P.** Pferdehirt" vs SCT (and reality) "Douglas **J.** Pferdehirt" |
| member QName junk / surname-only | `cnty:Peo1Member`, `spnt:MrEganMember`, `jpm:DimonMember` |
| PEO-only tagging | JPM: 0 of 5 non-PEO NEOs named |

**The oracle is corroboration, never ground truth** — it can itself be wrong
(typo row above), partial (JPM), or absent (MSFT).

## Full-population coverage (all 42,491 stored `def14a_body`, by filed year)

| filed | bodies | any `ecd` ns | `PeoName` | `IndividualAxis` |
|---|---|---|---|---|
| ≤2022 | 17,814 | 0 | 0 | 0 |
| 2023 | 4,075 | 50 | 43 | 16 |
| 2024 | 4,992 | 371 | 327 | 147 |
| 2025 | 8,253 | 3,299 | 2,641 | 1,302 |
| 2026 | 7,251 | 3,095 | 2,817 | 1,957 |

Scoped to accessions with parsed SCT rows (`def14a_exec_compensation`) — the rows
the oracle would cross-check:

| filed | SCT accessions | `PeoName` marker | `IndividualAxis` |
|---|---|---|---|
| 2025 | 2,833 | 2,204 (77.8%) | 1,123 (39.6%) |
| 2026 | 2,613 | 2,336 (**89.4%**) | 1,646 (63.0%) |

(The ~10% 2026 gap = EGC/FPI-exempt filers + PvP-in-non-primary-document splits;
we store the primary document only. edgartools' own docstring estimates "~60% of
companies" dimensionally tag — matches our 63%.)

## Full-population verification — oracle vs stored SCT names

Scan of ALL 4,851 SCT-bearing accessions whose body carries `PeoName`
(extraction identical to the proposed parser; token-subset matcher with
honorific/initial stripping; run 2026-07-22 against the live table mid-v5-rewash):

| signal | value |
|---|---|
| accessions scanned | 4,851 |
| accessions with ≥1 oracle name | 4,614 (95.1%) |
| oracle names extracted | 6,810 (≈1.4/accession — most filers tag the PEO only) |
| SCT names matched by an oracle name | 5,269 / 22,784 (**23.1%**) |
| oracle names with no SCT counterpart | 1,544 (PvP 5-yr window ≥ SCT 3-yr; former PEOs — absence ≠ error in EITHER direction) |
| single-token SCT names (suspicious class) | 70 |
| … uniquely repairable (token-subset of exactly ONE oracle name) | **8** |
| … ambiguous (subset of >1) | 0 |
| … no oracle hit (person untagged) | 62 |

**Falsification verdict on the ticket's strategic premise**: the oracle does NOT
"stop the per-case HTML patch thread". Row-level coverage is 23% (PEO-heavy);
scan repair power on the suspicious class is 8/70 (11%), and the post-v5
eyeball (D3) cuts the oracle-only remainder to ≈ **2 rows** — the residual
single-tokens concentrate exactly where tagging is thinnest (PEO-only filers,
exempt filers). The shippable value is the D3 sibling rule (same-document,
oracle-free), the FY-gated oracle as PEO-class insurance, and a per-accession
corroboration signal — ~100 LOC, no new dependency. If the operator prefers,
the oracle leg can be dropped entirely and only the sibling rule shipped; the
spec keeps both because the oracle also covers the all-single-token table case
(CXW) where no sibling exists.

## Premise refinements vs ticket #2099

1. **No CIK+FY join needed.** The ticket proposed a CIK+FY+fuzzy-name join;
   empirically the PvP iXBRL lives INSIDE the same `def14a_body` we already
   parse — the join key collapses to *same accession* (+ an FY-membership gate
   on repair, D3). Scope caveat (Codex ckpt-1 M2): this holds for the **stored
   primary document** — a filer whose PvP section rides a non-primary exhibit
   simply yields an empty oracle there (safe no-op), and that gap is part of
   the measured coverage numbers above.
2. **Component gap confirmed** (ticket point 3): PvP tags totals/CAP only
   (`ecd:PeoTotalCompAmt` etc.) — no per-year salary/bonus/stock components.
   The oracle cross-checks the NAME on an HTML-parsed row; it cannot replace
   the row.
3. **Coverage is real but bounded**: ~89% of current-season SCT accessions have
   PeoName facts, but PEO-only tagging (JPM) and prose-only filers (MSFT) mean
   an SCT name with NO oracle match is NEVER evidence of error.

## Design

### D1 — extraction (provider, pure)

`app/providers/implementations/sec_def14a.py::parse_pvp_neo_names(html_text)
-> tuple[Def14APvpNeoName, ...]` — self-contained (providers do not import
services), lxml `etree.HTMLParser` walk mirroring the research probe.
**HTML-mode handling is explicit (Codex round-2 #1)**: lxml's HTML parser does
NOT namespace-expand — `ix:nonNumeric` survives as the literal lowercased tag
`ix:nonnumeric`, `nsmap` is not populated, and attribute names are lowercased
(`contextRef` → `contextref`). The extractor therefore: harvests `xmlns:*`
declarations as literal attributes to build the prefix→URI map; splits
prefixed tag/attribute QNames manually; reads `contextref` (lowercased). No
`QName(el.tag)` / `el.get("contextRef")` patterns. (The research probe hit
exactly these three traps; its working form is the reference.) Collect
`ix:nonnumeric` facts whose `name` QName resolves (via the document's xmlns
declarations) to namespace-URI starting `http://xbrl.sec.gov/ecd` + localname
`PeoName` — NOT the literal prefix `ecd:`; full-pop measured **300 of 5,841
(5.1%)** PeoName-bearing bodies declare a different prefix. Resolve
`contextRef` → explicit members + period end-dates on the URI-resolved
`IndividualAxis` / `ExecutiveCategoryAxis` from the context definitions.
Normalise the fact value: entity-decode, flatten ALL whitespace (the #2097
lesson applies to fact values too), strip trailing footnote digits. Dedupe by
(normalised value, individual member); carry covered-FY set per person for the
D3 gate. lxml is already an app dependency (`xbrl_instance.py`,
`dimensional_facts.py`).

### D2 — NO persistence table (deliberate cut)

An earlier draft added `def14a_pvp_neo_names`. Cut: the oracle is fully
re-derivable from the retained `def14a_body` raw payload (that is what the
raw-store + rewash architecture is for, §13.F REWASH bucket), it has no
consumer beyond the parse-time repair, and the full-pop scan shows 23% row
coverage — too thin to be a read-path cross-check anyone should query. If a
concrete consumer appears (thesis DQ audit, #2100 verification tooling), add
the table then; the extraction function is the reusable part.

### D3 — repair rule (parse-time, conservative, v1 = single-token extension only)

At SCT write time, for each parsed `executive_name` in the **suspicious trigger
class** — single-token name (`^[A-Za-z'’-]+$`); empirically these are
truncations, though the class is a *trigger*, not a proof (legal mononyms
exist, Codex ckpt-1 L1) — repair in strict priority order:

1. **Intra-SCT sibling first (pure HTML, no oracle).** If EXACTLY ONE other
   parsed row in the SAME accession has a token-superset name
   (`Pferdehirt` ⊂ `Douglas J. Pferdehirt`), adopt the sibling's name. This is
   the person's own other-FY row rendered unwrapped — same-document evidence,
   immune to oracle typos. Decisive case: FTI `0001140361-24-019443`, where v5
   emits BOTH forms and the oracle's spelling ("Douglas **P.**") is a filer
   typo — sibling repair yields the correct "Douglas J.".
2. **Oracle second, FY-gated.** Else replace iff the SCT token set is a subset
   of EXACTLY ONE oracle name's token set, AND the FY gate passes, AND the
   replacement has strictly more tokens after honorific stripping ("Mr. Cook"
   can never shorten/no-op a name). **FY gate** (Codex ckpt-1 H2, round-2 #2):
   the SCT `fiscal_year` is a label; PvP context period-END dates are calendar
   dates, and a non-calendar fiscal year's label ≠ its calendar end-year
   (a Jan-ending FY2024 ends in calendar 2025). Gate passes when any of the
   oracle person's covered period-end years ∈ {`fiscal_year`, `fiscal_year`+1}
   — tolerant by construction (FY label ≤ calendar end-year ≤ label+1). The
   gate exists to exclude PEOs whose coverage ended YEARS before the SCT
   window, not to do exact FY accounting; the ±1 slack cannot re-admit them.

Normalisation both sides: entity-decode, NFKD, whitespace-flatten (fact values
carry render wraps too), lowercase, strip honorifics `Mr/Ms/Mrs/Dr`, drop
1-char initials for the subset test (keep them in the replacement text).

- **Never**: delete a row, touch a multi-token name (the FTI oracle-typo case
  proves full names must not be "corrected"), repair on ambiguity, treat oracle
  absence as error. `principal_position` untouched.
- Title-leak trimming via oracle (`David E. Govrin Group`) is deliberately NOT
  in v1 — that class stays with #2100 Class 3 (regex modifier extension); the
  oracle direction (oracle ⊂ name) has a harder false-positive surface.

**Post-v5 eyeball of the scan's 8 repair candidates** (reparse through the live
v5 parser, not the stale table): 4 of 7 accessions were stale-row artifacts
(v5 already clean — Soluna, Liberty, SPNT-2025, East West); FTI routes to
sibling repair; oracle-only repairs remaining ≈ **2 rows** (CXW "Hininger",
`0001193125-26-153469` "Charles"). The honest steady-state value is the
sibling rule + PEO-class insurance + corroboration, not bulk repair.

### D4 — versioning + backfill

Parser `def14a-v5` → `def14a-v6`, **one bump SHARED with the #2100 residual
fixes** (an 8-row repair alone does not justify a standalone ~13 h 42k-body
rewash; bundling gives one local rewash for both). `scripts/rewash.py --kind
def14a_body`, no SEC fetches. Operator-visible figure:
`/instruments/{symbol}/exec-compensation` panel.

### D5 — DQ visibility

Parse logs per accession: `pvp_names=N sct=M matched=K repaired=R`. The #2100
verification queries re-run post-rewash quantify residual shrink.

## What this kills in #2100 (measured on the residual filers)

| #2100 class | oracle effect |
|---|---|
| Class 1 cross-row wrap (CoreCivic CXW `0001140361-26-012100`) | partial: PEO "Hininger" → "Damon T. Hininger"; Garfinkle/Mayberry/Swindle/Grande untagged → remain HTML-only |
| Class 2 CJK glued (NXTT/ITP `0001213900-*`) | none — filers are PvP-exempt (no `ecd` at all; EGC-profile) |
| Class 3 modifier leak (SPNT Govrin "Group", CNTY "Managing") | none — those individuals untagged (SPNT tags Egan/Malloy/Sankaran; CNTY tags PEOs as "Dr./Mr." honorifics) |
| Class 4 non-lexicon title over-glue (SPNT Leonardo) | none — untagged |

Full-pop equivalent: 8 of 70 single-token rows repairable. The oracle is a
conservative single-token repairer + corroboration signal, NOT a replacement
for the #2100 HTML residual work. (Ticket correction while here: #2100 Class 1
attributes "Pferdehirt" to CoreCivic — `0001140361-24-019443` is TechnipFMC
(FTI); the shared prefix is the filing agent.)

## Rejected alternatives

- **edgartools `ProxyStatement.named_executives`** (`edgar/proxy/core.py:727`):
  same concept/axis logic, but (a) requires network XBRL retrieval per accession
  — non-viable for a 42k-body full-pop rewash at 10 req/s; (b) drops
  undimensioned `ecd:PeoName` facts (JPM's bare PEO name would be lost); our
  stored-body offline extraction keeps both. Compared per the standard-filing
  reuse check; skill G16 updated with the outcome.
- **XBRL as the primary name source** (drop HTML names where oracle exists):
  rejected — PEO-only/partial tagging (JPM) + honorific values ("Mr. Cook")
  make the HTML-scraped name the better primary for most rows; the oracle wins
  only where HTML demonstrably failed (single-token class).
- **Prose fallback** (`ecd:NamedExecutiveOfficersFnTextBlock` name extraction):
  deferred — free-text NER-ish parsing, exactly the fragility this ticket
  exists to avoid. Revisit only if the structured-tag coverage proves
  insufficient for a concrete consumer.

## Do NOT

- Do not swap the SCT HTML parser for edgartools (G16: comma-first split leaks
  titles).
- Do not delete/nuke SCT rows on oracle mismatch (`ingest_status='tombstoned'`
  rows are the #2086 402/403-decouple, not bogus).
- Do not extend repair to multi-token names without a new full-pop scan.

## Prevention-log entries honoured

- "verify the signal on the FULL population, never a sample" (#1659) — the
  4,851-accession scan above.
- `_POSITION_ROLE_RE` bounded-modifier lesson (L2121) — no regex changes here.
- L1914 (name-clustering FP) — repair requires UNIQUE token-subset, never
  clusters.

## Fixtures (unit)

- AAPL: all-NEO tagging, honorific PEO ("Mr. Cook" must NOT overwrite
  "Tim Cook"), entity-decode (O'Brien), wrapped fact value ("Kevan\n Parekh").
- JPM: PEO-only, undimensioned fact retained.
- CoreCivic: single-token "Hininger" + oracle "Damon T. Hininger" + matching FY
  → repaired; "Garfinkle" (no oracle hit) untouched.
- FTI: "Pferdehirt" + sibling "Douglas J. Pferdehirt" → sibling repair wins
  over the typo'd oracle ("Douglas P."); the full-name sibling row untouched.
- FY gate: oracle person covering only prior FYs must NOT repair a current-FY
  single-token row.
- CNTY: "Dr. Erwin Haitzmann" honorific strip; junk members `Peo1Member`.
- Non-`ecd:` prefix body (5.1% class): URI-resolved extraction still finds
  PeoName.
- No-PvP body (CJK filer): extraction returns (), parse unchanged.

## ETL clauses 8–11 plan

Smoke panel AAPL/GME/MSFT/JPM/HD exec-comp endpoint pre/post; cross-source
fixture: AAPL NEO set vs proxy PDF (or gurufocus exec-comp page); backfill =
local rewash (D4) run to completion; operator-visible verify on
`/instruments/AAPL/exec-compensation` + CoreCivic symbol post-rewash.
