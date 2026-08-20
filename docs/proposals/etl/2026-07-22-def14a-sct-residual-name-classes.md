# DEF 14A SCT residual name classes — same-document recovery (#2100)

Status: proposal (unshipped). Follows #2097 (def14a-v5) + #2099 (PvP oracle
spec, merged PR #2102). One parser bump `def14a-v5 → def14a-v6` ships this AND
the #2099 D1/D3 oracle (one rewash).

## Source rule

- **17 CFR § 229.402(c)(2)(i)** — the SCT column (i) is "Name and Principal
  Position". The reg fixes the column's CONTENT, not its rendering; how a filer
  renders the name (full, wrapped, surname-only) is filing style.
- **17 CFR § 229.402(v)(3) + (v)(7)** — PvP footnote NEO names, Inline-XBRL
  tagged (the #2099 oracle; source-rule detail in
  `2026-07-22-def14a-pvp-neo-name-oracle.md`).
- **Recovery posture (parser policy, stated plainly)**: the reg does not
  mandate any repair. Policy: a truncated/glued `executive_name` is repaired
  ONLY from deterministic evidence elsewhere in the SAME filing (sibling SCT
  row, document prose, PvP iXBRL facts) — never invented, never cross-filing.

## Premise falsification (#2100 ticket vs full-pop reality)

1. **Class 1 "cross-physical-row name wrap" is FALSIFIED for the flagship
   case.** CoreCivic CXW `0001140361-26-012100`: the SCT `<table>` as filed
   contains ONLY surnames — "Damon" appears NOWHERE in the 75 KB table element
   (verified on the raw winner table). There is no wrap to merge; the filer
   labels SCT rows by surname. No row-lookahead fix can recover the first name
   — it must come from elsewhere in the document ("David M. Garfinkle",
   "Patrick D. Swindle", "Anthony L. Grande", "Lucibeth N. Mayberry" all occur
   in prose; PEO "Damon T. Hininger" also in the PvP facts).
2. **Ticket mislabel**: "Pferdehirt (CoreCivic `0001140361-24-019443`)" — that
   accession is TechnipFMC (FTI); `0001140361` is the filing agent prefix.
3. **FTI is a DIFFERENT shape**: v5 emits BOTH 'Douglas J. Pferdehirt'
   (FY2021-23) AND a spurious 'Pferdehirt' FY2023 row whose total (393,737)
   CONFLICTS with the real FY2023 row (17,062,495) — a sibling-rename would
   key-collide and clobber a real figure. Hence the collision guard below.
4. **Class 2 CJK glued**: NXTT "HechunWei" / ITP "LushaNiu" — the SPACED form
   occurs in the same document's prose (1× / 11×), so a camel-split can be
   VALIDATED against the document instead of guessed (kills the
   McDonald/LaBelle false-positive class a blind splitter would have).

## Change (def14a-v6)

### C1 — `_POSITION_ROLE_RE`: add `group|managing` to the leading-modifier alternation

Fixes Class 3 — scope precisely (Codex ckpt-1 L1): `group`/`managing` become
LEADING MODIFIERS of an existing core role, so "Group **President**" /
"Managing **Director**" / "Senior Managing **Director**" split before the
modifier; "Managing"/"Group" before a NON-role word still does not split
(no broad managing-title coverage). Bare "Managing Director" rows classify
position-only instead of minting a bogus "Managing" NEO. Bound stays `{0,3}`
(prevention-log L2121 — quadratic backtracking); the existing fuzz test
extends to the new modifiers.

### C2 — `_repair_truncated_names(rows, html_text)` post-pass in `parse_summary_compensation_table`

Trigger class: distinct `executive_name` that is single-token
(`^[A-Za-z'’\-]+$`). For each such name, collect candidate replacements from
THREE independent same-document evidence sources — two structured (parsed
rows; reg-mandated iXBRL facts) plus one EXACT-STRING document check (camel,
below — document-text evidence, adopted as parser policy since no source rule
governs glued rendering; it is equality against one derived string, never
pattern harvesting):

1. **Intra-SCT sibling**: other distinct parsed names whose token set is a
   strict superset (same person's other-FY row rendered unwrapped).
2. **Camel-verbatim**: when the name matches `^[A-Z][a-z]{2,}[A-Z][a-z]+$`
   (first run ≥3 chars — excludes Mc/La/De/Di surnames), the space-split form
   IF it occurs VERBATIM in the flattened document text — exact string
   equality, not a pattern harvest.
3. **PvP oracle** (#2099 D3): `PeoName` token-subset matches, FY-gated,
   must-lengthen.

**Repair iff every pair of candidates AGREES and the union names ONE person.**
Two candidates agree iff, after honorific-strip + lowercase, one FULL token
set (initials included) is a STRICT subset of the other ("Cook" ⊆ "Tim Cook";
"Damon Hininger" ⊆ "Damon T. Hininger" — the one-side-initials case is
covered by the subset branch), or the token sets are equal AND the token
ORDER matches. Token order is identity-bearing: a permutation
("Hechun Wei" vs "Wei Hechun") is two different people and DISAGREES
(fresh-agent review — an order-blind set comparison would have let a shared
single token repair onto either). Conflicting initials are a DISAGREEMENT:
"Douglas J. Pferdehirt" vs "Douglas P. Pferdehirt" repairs nothing (Codex
ckpt-1 M1). Any disagreement or within-source ambiguity → no repair.
Replacement = the most token-complete agreeing form; tie → sibling > camel >
oracle (prefers the same-table HTML spelling over a possibly-typo'd fact
value). The camel-verbatim occurrence check is WORD-BOUNDED, not substring —
"Jon Smithson" in prose must not validate a "JonSmith" split (fresh-agent
review).

**Per-name atomicity of the oracle gate (Codex ckpt-1 H1)**: repair renames
ALL rows carrying the suspicious name, so the oracle FY gate must hold for
EVERY such row's `fiscal_year` (each within the oracle person's covered
period-end years ±1). A partially-covered name is NOT repaired — no partial
renames, ever. (CXW "Hininger" rows FY2023-25 ⊆ oracle 2021-2025 → passes.)

**Rejected: free-prose full-name harvesting** (`Leading-caps + surname`
patterns over the document text). Full-pop validation on the 22 flagged names
falsified it: candidates are junk-dominated by adjacent-list renderings
("Garfinkle Patrick D.", "Grande Lucibeth N."), table-header adjacency
("Hininger Accelerated Vesting", "Charles Family Trust"), and every filter
iteration demanded a bigger stoplist (the L1914 heuristic-clustering failure
mode); it also produced a live FALSE repair — 'Employee' →
"Employee Retirement Income" (ERISA boilerplate). Cost of dropping it: CXW's
four non-PEO surnames (Garfinkle/Grande/Mayberry/Swindle) stay as-filed —
name present, comp correctly attributed; documented residual.

**Collision guard**: if any (replacement-name, fiscal_year) pair already
exists among parsed rows, the repair for that name is skipped ENTIRELY (no
partial renames — a person must not split across two spellings; FTI above is
the live case, its conflicting FY2023 totals stay under distinct names for
the operator to see). After rename, exact-duplicate (name, fy) rows cannot
arise by construction of the guard.

Never: delete a row, touch a multi-token name (except the C1 regex split
change), repair without unanimity, invent a name.

### C3 — `parse_pvp_neo_names` (#2099 D1)

As specced in `2026-07-22-def14a-pvp-neo-name-oracle.md`: URI-resolved ECD
matching (fact-level prefix drift measured 0 full-pop — the earlier "5.1%"
was MetaLinks-JSON artifacts, corrected in the oracle spec; URI resolution
kept for yearly-ns correctness), HTML-mode lxml handling (literal lowercased
tags/attrs), per-person covered-FY sets.

### C4 — Class 4 accepted

Non-lexicon-title over-glue ("Thomas Leonardo Global Head of Accident and
Health", SPNT) stays as-is: name present, comp correctly attributed;
lexicon-expanding "Global Head" would over-trigger on real surnames ("Head").

## Full-population verification

Scan of ALL 6,042 SCT-bearing accessions with stored bodies (0 parse errors):
parse under live v5 and under the C1 candidate regex; diff every name;
simulate C2 on every flagged name; EVERY diff and repair eyeballed, not
sampled.

| signal | v5 | v6 candidate |
|---|---|---|
| distinct single-token names | 24 | 22 |
| title-tail leak names (incl. group/managing) | 182 | 41 |
| accessions changed by C1 regex | — | 88 (141 gone / 140 new, all eyeballed: every one a clean "Name Group/Managing" → "Name" split or a junk-row drop; CNTY's bogus "Managing" NEO replaced by the real "Andreas Terler"/"Nikolaus Strohriegel"; ZERO real-surname regressions) |
| C2 repairs (final rules) | — | 3: HechunWei→"Hechun Wei", LushaNiu→"Lusha Niu" (camel), Hininger→"Damon T. Hininger" (oracle; 5 per-FY facts cover 2021-25 ⊇ rows 2023-25) |
| C2 correctly blocked | — | 2: FTI Pferdehirt (fy-collision guard); "Charles"→"Dirkson Charles" (oracle fact covers FY2025 only, rows span 2023-25 — per-name atomic gate blocks; safety over yield) |
| unrepaired residual | — | ~17 = bogus fragment rows ("Global", "Executive", "Non", "Trans" — not names, repair would be wrong) + prose-only surnames (CXW Garfinkle/Grande/Mayberry/Swindle class) |

Remaining 41 leak names (eyeballed): cross-row wrapped titles ("X Executive
Vice" + next-row "President" — bare `vice` core token is UNSAFE, real surname
"David Vice" in-pop), non-lexicon "Head of …" titles, junk aggregate rows
("Officers as a Group"). All pre-existing classes, none introduced by C1;
non-harmful (name present, comp attributed). Accepted with Class 4.

## Fixtures (unit)

- CXW surname-only: "Hininger" → oracle "Damon T. Hininger" (FY-gated);
  "Garfinkle" stays as-filed (no structured evidence — prose harvesting
  rejected).
- FTI collision: 'Pferdehirt' FY2023 conflicting with real FY2023 row → NO
  repair (guard fires); 'Douglas J. Pferdehirt' rows untouched.
- NXTT/ITP camel: "HechunWei" → "Hechun Wei" (verbatim-validated); "LushaNiu"
  → "Lusha Niu"; a McDonald-style surname NOT split (regex excludes 2-char
  first run); a camel name whose spaced form is ABSENT from the document NOT
  split.
- Fragment rows ("Global", "Executive") never repaired (no unanimous
  candidate).
- SPNT Class 3: "David E. Govrin Group President…" splits at "Group";
  "Robert A. Bank" surname-safety unchanged; regex fuzz with group/managing
  runs stays bounded (L2121 test extended).
- CNTY: bare "Managing Director, Finance…" row is position-only (no "Managing"
  NEO; real NEO names emerge).
- Oracle disagreement: sibling spelling wins replacement text when
  normalised-equal; non-equal → no repair.

## Backfill + ETL clauses 8-11

Parser v6 bump → local rewash `scripts/rewash.py --kind def14a_body` (no SEC
fetches; ~13 h full drain observed for v5). Smoke panel AAPL/GME/MSFT/JPM/HD
`/instruments/{symbol}/exec-compensation` pre/post; cross-source: CXW NEO full
names vs the proxy itself + one independent source; operator-visible verify on
CXW + NXTT post-rewash.

## Prevention-log entries honoured

- L2121 bounded modifier run (+ fuzz extension).
- L1914 full-pop before heuristic (the 6,042-accession scan; every C1 diff and
  C2 repair eyeballed, not sampled).
- #2097 "question the model, not the case" — this spec replaces the per-case
  lookahead premise with a same-document recovery model.
