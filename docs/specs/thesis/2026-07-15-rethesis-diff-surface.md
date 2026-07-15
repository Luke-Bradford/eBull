# Re-thesis diff surface — structured what-changed vs prior version + alert (#2013)

**Status:** spec (pre-implementation). **Issue:** #2013. **Related:** #1902 (theses library,
shipped), #1988 (regen triggers, open — orthogonal, see Source rule), #2010 (prompt v3 — will
mint many N>1 versions; this surface pays immediately when it lands), #2017 (context audit,
merged — unblocked this).

## Source rule

No SEC/external data treatment — every input is an already-persisted internal surface. Governing
internal contracts:

- **settled-decisions "Thesis semantics" (:143-185):** theses are append-only, versioned by
  insert, never overwritten. Therefore `diff(N, N-1)` is a **pure function of two immutable
  rows** → computed on read, never stored. No second copy of truth, no backfill, and it works
  retroactively for every existing N>1 pair (43 on dev today).
- **#2007 field semantics (PR #2008):** `_to_float` keeps NULL as None — a NULL target is an
  *abstention*, never 0. Diff must treat null↔value transitions as first-class events
  (`added`/`removed`), not as numeric moves from/to zero.
- **Alert-feed precedent (sql/044/045/047/213 + `app/api/alerts.py`):** the repo has exactly two
  alert shapes — (a) *standing condition*, computed on read, clears when the condition resolves
  (`/alerts/thesis-staleness`); (b) *event feed*, cursored on a BIGSERIAL id with
  `operators.alerts_last_seen_<x>_id` mark-seen (decisions, position alerts, coverage events,
  rank-moves). A thesis change is a discrete post-generation **fact** — it does not "resolve" —
  so it is shape (b), cursored on `theses.thesis_id` (BIGSERIAL PK, insert-ordered). This
  answers the issue's "decide in spec vs #1988": #1988 is about *pre-generation regen triggers*
  (when data changes, regenerate); #2013 alerts are *post-generation facts* (regen happened,
  here is what moved). Orthogonal; no shared semantics to reconcile.
- **Version pairing:** `_insert_thesis_atomic` computes `thesis_version` as
  `COALESCE(MAX(thesis_version),0)+1` under `UNIQUE(instrument_id, thesis_version)`. The design
  does NOT assume gaplessness or timestamp ordering: every predecessor lookup is an explicit
  join on `(instrument_id, thesis_version - 1)` per row; a missing predecessor yields
  `diff = None`, never an error.

## Full-population verification (dev, 2026-07-15)

All 43 existing N>1 pairs scanned (join on `thesis_version - 1`):

| signal                                  | count                                               |
| --------------------------------------- | --------------------------------------------------- |
| stance change                           | 14                                                  |
| thesis_type change                      | 3                                                   |
| any target/zone change                  | 17                                                  |
| null-transition (target added/removed)  | 12                                                  |
| numeric moves (n=21)                    | **all ≥5%**; median 50%, p25 20%, p75 70%, max 180% |

The EXACT design predicate — **stance change OR thesis_type change OR null-transition OR
relative move ≥5%** — was run verbatim on all 43 pairs: **22 material, identical to the 22
pairs with ANY field change**. Zero events lost; the 5% threshold guards against future
small-jitter regens once #2010 v3 starts minting versions. thesis_type is in the predicate by
design (a type reclassification is material per se); it changes alone in 0 current pairs.

Memo survey, FULL population (all 325 memos): writer emits `###`-level headings only
(942 headings, no other level), **0 memos with duplicate headings**, 15 memos with NO headings
at all. Heading TEXT drifts between generations ("Valuation & Fair Value Context" vs
"Valuation & Technical Analysis") → memo section diff is **informational only**, never part of
the material predicate.

## Design

### 1. Pure diff module — `app/services/thesis_diff.py` (no DB, no LLM)

Pattern sibling of `thesis_context_audit.py` (#2017). One entry point:

```python
compute_thesis_diff(prev: Mapping, curr: Mapping) -> ThesisDiff
```

`ThesisDiff` (dataclass → dict for JSON):

- `stance: {from, to} | None` — set when changed.
- `thesis_type: {from, to} | None`.
- `confidence: {from, to, delta} | None` — when changed (either side may be None).
- `targets: list[{field, from, to, rel_move | None, kind}]` — one entry per changed field among
  `buy_zone_low/high`, `base_value`, `bull_value`, `bear_value`; `kind ∈ added|removed|moved`
  (null-transition vs numeric move); `rel_move = |new−old|/|old|` (None when old is 0/None).
- `break_conditions: {added: [str], removed: [str]}` — set diff on whitespace-normalized,
  case-folded condition strings from `break_conditions_json`.
- `memo_sections: {added: [str], removed: [str], changed: [str]}` — split `memo_markdown` on
  `###`-prefixed heading lines; `changed` = same heading text, different whitespace-normalized
  body.
  Duplicate headings within one memo are collapsed (bodies concatenated) so heading text is a
  unique key — 0 duplicates exist in the full population, this is a guard, not a feature. A
  memo with no headings (15/325 exist) is treated as one pseudo-section `"(body)"`.
  Informational only.
- `provenance: {prompt_version: {from,to} | None, model: {from,to} | None}`.
- `material: bool` — stance changed OR thesis_type changed OR any target `kind != moved` OR any
  `rel_move ≥ _MATERIAL_REL_MOVE` (module constant, `0.05`). Confidence/memo/provenance never
  make a diff material.
- `summary: str` — deterministic compact one-liner built from the material fields
  (e.g. `"stance buy→hold · base 120→98 (−18%) · bear target removed"`); empty-string when
  nothing changed. This is the v1 "one-liner"; the issue's optional LLM one-liner is
  **descoped** (deterministic string is sufficient and free; revisit post-#2010 if operator
  wants prose).

The material predicate lives ONLY here — SQL pairs rows, Python decides. No dual predicate to
drift.

### 2. API

- **`GET /theses/{instrument_id}`** (`ThesisDetail`): new `diff: ThesisDiffModel | None` — None
  when `thesis_version == 1`. Implementation: when version > 1, fetch the `version - 1` row and
  call the module.
- **`GET /theses/{instrument_id}/history`**: each item gains the same `diff` field, paired
  against its predecessor via an explicit `(instrument_id, thesis_version - 1)` join per row
  (LATERAL) — NOT page adjacency, which the `created_at DESC` ordering does not guarantee.
- **`GET /theses`** (library, `ThesisLibraryItem`): new `last_change_summary: str | None` +
  `last_change_material: bool` — field-level summary only (no memo section compare on the list
  path). Prior row via `LEFT JOIN LATERAL` on `(instrument_id, thesis_version - 1)`; NULL →
  summary None, material false.
- **`GET /alerts/thesis-changes`** (rank-moves semantics exactly): window = ALL pairs with
  `curr.thesis_version > 1 AND curr.created_at >= now() - interval '14 days'`; every windowed
  pair is diffed in Python and filtered to `material` — the cap (50) applies to the RESPONSE
  list only, AFTER materiality, newest first; `unseen_count` = count of material windowed
  changes with `thesis_id > COALESCE(alerts_last_seen_thesis_change_id, 0)` (never truncated
  by the list cap). Scanning the full window is cheap: `theses` holds 325 rows total today and
  regen throughput is ≤5/hour. Response: `{alerts_last_seen_thesis_change_id, unseen_count,
  changes: [{instrument_id, symbol, thesis_id, thesis_version, created_at, summary,
  stance_from, stance_to}]}`.
- **`POST /alerts/thesis-changes/seen`** body `{seen_through_thesis_id}` — advances the cursor
  via the standard `GREATEST(COALESCE(...))` idiom.

### 3. Migration — `sql/227_operators_alerts_thesis_change_cursor.sql`

```sql
ALTER TABLE operators
    ADD COLUMN IF NOT EXISTS alerts_last_seen_thesis_change_id BIGINT;
```

Mirror of sql/213 (rank-move cursor). NULL = never acknowledged. No new tables; diffs are not
stored (Source rule).

### 4. Frontend

- **ThesisPane provenance area** (`ThesisPane.tsx:132-135`): when `diff` present and material,
  a `Δ vs v{n-1}` line rendering `summary`; expandable detail (targets table, break-condition
  add/removes, memo section names). Non-material diff → muted "minor changes vs v{n-1}".
  Version 1 → nothing.
- **ThesesPage run-status column**: compact change chip (`Δ stance buy→hold`) when
  `last_change_material`; tooltip = `last_change_summary`.
- **AlertsStrip**: thesis-changes group card following the rank-moves card pattern (unseen
  count badge, per-row summary, mark-seen on dismiss).

### 5. Out of scope

- LLM one-liner (descoped, see §1).
- Regen triggers / staleness predicates (#1988).
- Meta-thesis aggregation (#2002).
- Any change to writer/critic prompts, scoring, or portfolio consumption — this is a pure
  read-surface + alert feature; `theses` writes are untouched.

## Tests

Pure-logic table tests for `thesis_diff.py` (fast tier, no DB): every field class, null
transitions both directions, materiality boundary (4.9% vs 5.1% move), zero/None `old` rel_move,
break-condition normalization, memo split edges (no headings, duplicate headings, heading-only
rename). ONE db-tier test for the alerts cursor semantics (unseen → seen → new insert reopens),
mirroring the existing rank-moves test shape. API field presence covered by extending existing
theses API tests (TestClient, auto-db-marked).

## Verification plan (definition of done)

Not an ETL/parser/schema-affecting-data change (operators cursor column only), so clauses 8-11
apply in spirit: exercise `GET /theses/{id}` + `/theses` + `/alerts/thesis-changes` on dev
against known multi-version instruments (31 exist), eyeball ThesisPane + ThesesPage + AlertsStrip
per change-coupled FE-QA, and record the observed figures in the PR description.
