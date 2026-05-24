# PR4 — Form 4 3-year ingest cap (#1233 retention rubric)

> Created: **2026-05-20**. Revised after Codex 1a (9 findings) + Codex 1b
> (4 residual gaps) + Codex 1c (3 stale references + lint-guard
> portability fix).
> Spec: `docs/specs/etl/retention-rubric.md` §4.3 + §7.
> PR3 (filing_events 10y cap) merged as #1239 (commit `85c9de0`).
> Umbrella: **#1233** (reopened after premature auto-close).

## 1. Scope

Apply the canonical Form 4 / 4-A depth cap (3 calendar years) at every writer
chokepoint. **Ingest-side only** — no `DELETE FROM insider_*` on pre-cap rows;
existing rows survive until the operator-driven pre-wipe (spec §6.3).

Three chokepoint families:

- **Legacy `filing_events` → `insider_transactions` path** (per-filing XML fetch).
- **Manifest-worker `_parse_form4` path** (parser-version-routed re-ingest).
- **Bulk dataset path** (`sec_insider_dataset_ingest.py`, Form 3/4/5 TSV archives,
  bootstrap stage S11) — Codex 1a BLOCKING #1 caught.

Tighten the existing `INSIDER_FORM4_BACKFILL_FLOOR_YEARS = 5` to a 3-year
window, rename to `INSIDER_FORM4_RETENTION_YEARS`, and:

- Close two legacy chokepoint gaps the 5-year incarnation did not cover
  (inner per-instrument SELECTs lacking the floor predicate).
- Add a manifest-worker pre-fetch gate.
- Add a bulk-dataset Form-4-only filter (Form 5 / Form 3 untouched per spec §4.4).

Out of scope:

- Form 3 / Form 5 — PR10 (latest-only cap).
- Row deletion of any insider table — none.
- `ownership_insiders_current` schema change — none. Behavior unchanged
  (deterministic recompute via `refresh_insiders_current()`).

## 2. Spec amendment (resolves Codex 1a BLOCKING #2)

Spec §4.3 currently contains contradictory clauses:

> "if it would recompute from observations, the parser writes a synthetic
> opening-balance row at the 3y boundary as a write-through anchor"

versus the immediately-following paragraph:

> "**Post-wipe semantics for cumulative state**: a whole-DB wipe deliberately
> resets `ownership_insiders_current`… Pre-3y cumulative position is lost by
> design. Operator accepts this as the trade-off…"

The "synthetic-anchor" clause survived from an earlier spec draft; the
"loss accepted" clause is the final position. PR4 amends the spec to
delete the synthetic-anchor sentence and the conditional in the
verification step. The remaining post-wipe-loss-accepted clause becomes
the canonical contract.

Without this amendment, the PR4 contract is ambiguous and the reviewer
could reasonably ask for the anchor row. With it, the rollup invariant
is: `refresh_insiders_current` continues to recompute deterministically
from observations + Form 3 baselines, and post-wipe state reflects only
the 3-year window. **PR4 ships the spec edit alongside the code.**

## 3. Constants + helper (mirrors PR3 §4.2 shape)

`app/services/insider_transactions.py`:

```python
INSIDER_FORM4_RETENTION_YEARS: int = 3


def form4_retention_cutoff(now: datetime | None = None) -> date:
    """Earliest filing_date accepted for Form 4 / 4-A ingest.

    Calendar-year subtraction (Codex 1a PR3 lesson — `365 * N` drifts).
    Feb 29 anchor → Feb 28 of target year.
    """
    if now is None:
        now = datetime.now(tz=UTC)
    today = now.date()
    target_year = today.year - INSIDER_FORM4_RETENTION_YEARS
    try:
        return today.replace(year=target_year)
    except ValueError:
        return date(target_year, 2, 28)


def form4_within_retention(filing_date: date, now: datetime | None = None) -> bool:
    return filing_date >= form4_retention_cutoff(now)
```

Why a Form-4-specific helper, not the PR3 `filing_events_retention_cutoff`:
each form has a per-source half-life (filing_events 10y, Form 4 3y,
DEF 14A latest-2, 13F 8q). Separate helper per form keeps each cap
independently tunable and greppable.

## 4. Chokepoint coverage

### 4.1 Legacy filing_events path

`app/services/insider_transactions.py`. **Resolves Codex 1a HIGH #3
(Python-side cutoff, not `NOW() - make_interval`).** All four SQL sites
take `retention_cutoff = form4_retention_cutoff()` as a Python `date`
param and use `fe.filing_date >= %(retention_cutoff)s`. No SQL-side
`make_interval`, no DB-session timezone ambiguity, single source of
truth for the Feb 29 logic.

| File:line | Site | Current state | Fix |
| --- | --- | --- | --- |
| `:1540-1551` | `ingest_insider_transactions_for_instrument` inner SELECT | No floor predicate | Add `AND fe.filing_date >= %(retention_cutoff)s` |
| `:1605` | `ingest_insider_transactions` universe SELECT | Floor via `make_interval(years => 5)` | Replace with Python-cutoff param |
| `:1806` | `ingest_insider_transactions_backfill` outer aggregate | Floor via `make_interval(years => 5)` | Replace with Python-cutoff param |
| `:1832-1846` | `ingest_insider_transactions_backfill` inner per-instrument SELECT | **No floor predicate — gap** | Add `AND fe.filing_date >= %(retention_cutoff)s` |

### 4.2 Manifest-worker path

`app/services/manifest_parsers/insider_345.py:_parse_form4`. Pre-fetch
gate. The ManifestRow carries `filed_at: datetime`. Before
the network fetch, reject rows older than the retention cutoff.

**Resolves Codex 1a HIGH #2 (tombstone-rewash claim was wrong).** A
pre-fetch retention skip writes NO `filing_raw_documents` row, so
`scripts/rewash.py` parser-version bump cannot revive these accessions.
The recovery path if the operator widens the cap is the
**`POST /jobs/sec_rebuild/run`** scope-reset endpoint (or `process =
"sec_form4"` full-wash) which clears tombstones for re-evaluation. PR
description states this explicitly; no claim about parser-version
rewash.

**Resolves Codex 1a MED #3 + Codex 1b MED rationale fix.** The
`filed_at` column is NOT NULL at the DB level, but the parser-side type
is `Any`. Defensive: if `row.filed_at is None`, return
`status="tombstoned"` with `error="missing filed_at"`. This **matches
the existing `_parse_form4` pattern** (lines 122-140 already tombstone
on `missing instrument_id` + `missing primary_document_url`); the
earlier plan draft incorrectly said `failed`. `tombstoned` is correct
because the row's manifest metadata is deterministically bad — retry
would not change anything.

Status choice for retention-skip: **`tombstoned`** with
`error="retention floor"`. `failed` is wrong (would schedule retry).

### 4.3 Bulk dataset path — Codex 1a BLOCKING #1

`app/services/sec_insider_dataset_ingest.py`. Two write sites per
archive: NONDERIV_TRANS loop (lines 312-345) + NONDERIV_HOLDING loop
(lines 371-405). Both call `record_insider_observation` directly with
`filed_at` derived from the SUBMISSION TSV row.

**Resolves Codex 1a MED #1 (Form 5 must NOT be capped here).**
`_map_form_to_source(form)` collapses Form 4 + Form 5 into the
`"form4"` observations enum. Spec §4.4 reserves Form 5 for PR10
(latest-only). The retention check fires only when the raw form code
starts with `"4"` (i.e. Form 4 or 4-A), NOT when it maps to
`source="form4"`. Form 5 / 5-A bulk rows continue to ingest unbounded
until PR10.

Implementation: in both loops, after reading `form` (line 311 / 370),
extract `form_upper = (form or "").strip().upper()`. If
`form_upper.startswith("4")` AND `filed_at.date() < retention_cutoff`,
bump `result.rows_skipped_retention` (new counter on
`InsiderIngestResult`) and `continue`.

`retention_cutoff` is computed ONCE per `ingest_insider_dataset_archive`
call, not per-row.

**Telemetry (Codex 1b LOW gap)**: `rows_skipped_retention` is added to
`InsiderIngestResult` AND wired into the archive-completion logging /
stage-summary aggregator so the operator-visible counter accounts for
deliberate retention skips. Without this, retention-skipped rows look
like silent drops.

## 5. Cumulative-rollup invariant verification

Spec amendment (§2 above) resolves the ambiguity. PR4 verification step:

- **Steady-state**: pin a test that `refresh_insiders_current` continues
  to aggregate pre-existing observations with post-cap rows. Required
  to confirm no regression — current dev DB still holds pre-3y rows
  until the operator wipe.
- **Post-wipe behavior**: NO test. Codex 1a HIGH #1 caught the PR10
  dependency. The post-wipe state involves Form 3 baseline behavior
  that PR10 owns. PR4 covers Form 4 only; it must not invent test
  coverage that overlaps PR10 scope.

The synthetic-opening-balance anchor row is NOT implemented (spec
amendment removes the conditional that would require it).

## 6. Lint guard — revised after Codex 1a MED #2 + Codex 1b residual gap

`scripts/check_form4_retention.sh`. The Codex 1b residual gap is that
"AT LEAST N" thresholds cannot catch a new SAME-FILE chokepoint that
adds a SELECT but omits the predicate — count stays at N even though
coverage dropped. Two complementary guards:

**A. Block-aware parity (Codex 1c portability fix)** — `grep -c` can't
do multiline parsing portably (BSD vs GNU grep diverge on `-P`) and
counts every occurrence including the helper definition + docstring,
which would inflate the helper count beyond actual call sites. Switch
to a portable `awk` block parser invoked by the shell guard:

- Helper call-sites: count lines matching
  `form4_retention_cutoff(` OR `form4_within_retention(` while
  EXCLUDING lines that start with `def` and the helper's own internal
  delegation (`form4_within_retention` calls `form4_retention_cutoff`
  in its body — counted once via dedicated awk rule that skips the
  helper definition file when computing the parity for
  insider_transactions.py). Concretely: count only call-sites inside
  the four target SQL blocks (block-scope aware) rather than blanket
  file count.
- SELECT-from-filing-events blocks: an `awk` pass that flags entry on
  `FROM filing_events` and looks for `JOIN insider_filings` within the
  next 5 lines; one block = one increment.

Required parity per file (exact equality, not "at least"):

- `app/services/insider_transactions.py`: SELECT blocks == 4;
  `form4_retention_cutoff(` call-sites == 4 (one per SQL block).
- `app/services/manifest_parsers/insider_345.py`: SELECT blocks == 0;
  `form4_within_retention(` call-sites == 1 inside `_parse_form4`.
- `app/services/sec_insider_dataset_ingest.py`: SELECT blocks == 0;
  `form4_retention_cutoff(` call-sites == 1 at archive level. Each
  per-row loop reuses the archive-anchored `retention_cutoff` date
  via a `filed_at.date() < retention_cutoff` predicate gated on the
  form-code prefix `4`; the lint guard pins TWO such predicate lines
  (one per loop) by counting the literal substring.

**B. Synthetic meta-test** — `tests/test_form4_retention_lint_guard.py`
covers every failure mode Codex 1b/1c/1d flagged:

- Copy source file → remove ONE helper call-site → assert guard exits
  non-zero (parity broken: SELECT count exceeds helper count).
- Copy source file → inject a NEW `SELECT ... FROM filing_events JOIN
  insider_filings ...` block with no helper call → assert guard exits
  non-zero (same parity failure direction).
- Copy source file → unchanged → assert guard exits zero.
- Fixture file with ONLY a `def form4_retention_cutoff(` line plus a
  docstring referencing the symbol → assert guard counts 0 call-sites
  (proves `def` exclusion + docstring-prose exclusion work).

The guard runs in the pre-push hook + CI alongside
`check_instruments_inserts.sh` (PR1 precedent).

## 7. Tests

`tests/test_insider_transactions_retention_cap.py` (new file, mirrors
PR3 `tests/test_filing_events_retention_cap.py` shape):

1. **Helper boundary** — `form4_retention_cutoff` calendar math + Feb 29 fallback.
2. **Boundary inclusion** — `form4_within_retention` true at exactly the cutoff date.
3. **Legacy universe path skips pre-3y** — `ingest_insider_transactions`.
4. **Legacy backfill outer aggregate skips pre-3y** — `ingest_insider_transactions_backfill`.
5. **Legacy backfill INNER SELECT skips pre-3y** (gap fix, Codex MED #2 coverage) — outer picks instrument with mixed inventory, inner only fetches inside-cap filings.
6. **`ingest_insider_transactions_for_instrument` inner SELECT skips pre-3y** (gap fix).
7. **Manifest-worker tombstones pre-3y row without fetching** — pre-fetch gate.
8. **Manifest-worker accepts row exactly at cutoff** — inclusive boundary.
9. **Manifest-worker `filed_at=None` → `tombstoned` status** (defensive, Codex MED #3; matches existing `missing instrument_id` pattern in `_parse_form4`).
10. **Bulk dataset path skips pre-3y Form 4** (Codex BLOCKING #1) — transactions loop + holdings loop both.
11. **Bulk dataset path retains pre-3y Form 5** (Codex MED #1) — Form 5 unbounded until PR10.
12. **Bulk dataset path retains pre-3y Form 3** — Form 3 unbounded until PR10.
13. **`refresh_insiders_current` steady-state** — pre-existing observations remain aggregated alongside post-cap rows.
14. **Future-dated filing accepted at every chokepoint** (Codex LOW #1) — universe SQL, backfill SQL, manifest worker, bulk dataset.

Existing `TestForm4DateFloor` in `tests/test_insider_transactions_ingest.py:1015`
updates to the new constant + helper.

## 8. PR shape

Single PR. Commits:

1. `feat(#1233): tighten Form 4 ingest cap 5y → 3y at every chokepoint`
2. `feat(#1233): bulk dataset Form-4-only retention filter`
3. `test(#1233): Form 4 retention cap coverage`
4. `chore(#1233): lint guard for Form 4 retention predicate`
5. `docs(#1233): remove spec contradiction on synthetic-anchor clause`

Branch: `feature/1233-pr4-form4-3y-cap`.

## 9. Acceptance

1. `INSIDER_FORM4_RETENTION_YEARS = 3` is the single source of truth.
2. Four legacy SQL chokepoints honor the cap via Python-computed cutoff.
3. Manifest-worker `_parse_form4` gates pre-3y rows with `tombstoned`
   before fetch, and `filed_at=None` rows with `tombstoned` (matches
   existing missing-metadata pattern).
4. Bulk dataset ingester gates pre-3y **Form 4 only**; Form 3 + Form 5
   continue to ingest unbounded (PR10 scope).
5. Spec §4.3 amended to resolve synthetic-anchor contradiction.
6. `refresh_insiders_current` behavior unchanged.
7. Lint guard catches missing predicate at every chokepoint file.
8. Pre-push hook + CI gates: ruff / pyright / pytest all green.
9. Codex 1a + 1b + 1c + 1d already passed on this plan. Codex 2 pre-push next.
10. PR description records: every chokepoint patched, every test exercising it, the spec amendment, the pre-wipe semantics statement, the rewash-via-full-wash note.

## 10. Risks + mitigations

- **Spec edit lands in a code PR** — minor process flex. The amendment
  is a sentence-level fix to resolve internal contradiction, not a
  policy change. Codex 1a explicitly flagged the contradiction;
  bundling the fix is the cleanest resolution.
- **Bulk dataset Form 5 retention** — explicitly preserved until PR10.
  Adds modest dev-DB storage for Form 5 (small volumes; PR10 will
  collapse to latest-per-pair).
- **Pre-fetch tombstones cannot be revived by parser-version rewash** —
  documented. Operator widens cap via `sec_rebuild` source reset.
- **`refresh_insiders_current` test for steady-state only** — post-wipe
  behavior tested implicitly via existing
  `TestRefreshInsidersCurrentRoundTrip` at
  `tests/test_ownership_observations.py:200`.

## 11. Codex review gate

- Codex 1a + 1b + 1c + 1d completed; this is the plan baseline going
  into implementation.
- Codex 2 pre-push on the diff after self-review + local gates green.
