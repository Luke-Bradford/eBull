# Chunk E filings backfill + F extension (#268) — design v5

**Goal:** drive every tradable SEC-covered instrument to a terminal `filings_status` (`analysable`, `fpi`, `structurally_young`, or `insufficient` when exhausted) by paging SEC `submissions.json` history + verifying 8-K completeness inside the 365-day window, with bounded retries only on recoverable failures. Extend `weekly_coverage_audit` to orchestrate backfill after its classifier pass.

**Scope:** Chunk E (backfill service) + Chunk F full (scheduler extension) + a minimal Chunk D extension (`audit_all_instruments` / `audit_instrument` must preserve backfill-owned `structurally_young` on demote paths — used by other call sites such as Chunk G universe-sync hooks; scheduler itself no longer runs a post-backfill audit, see v3 C1 below).

**Revision history:**
- v1 — initial.
- v2 — Codex-driven (8 issues): retryable-audit wipe (C1), 8-K gap gated (C2), early break leaves 8-K pages unfetched (H1), clean-exhaust misclassified as HTTP_ERROR (H2), zero-filings → YOUNG (H3), FPI not terminal (H4), implicit-tx durability (M1), `files[]` fixture shape (M2).
- v5 — Codex-driven (2 issues on v4):
  - (v4-H1) `status=None` retryable preservation + attempts cap could freeze an aged-out `structurally_young` row forever. Scenario: row was correctly young last month; earliest filing is now >= 18mo (aged out); 3 backfill runs hit HTTP/PARSE errors before step 5; each preserves young via `status=None` and increments attempts; cap gate then skips all future runs; Chunk D demote guard also blocks audit correction. Violates master-plan line 184. **Fix:** exempt rows where `filings_status = 'structurally_young'` from the attempts cap. Backoff (7 days) still bounds blast radius. When a young row eventually completes backfill cleanly, step 5 classifier picks `EXHAUSTED` (aged out) and writes `insufficient` — demoting correctly.
  - (v4-L1) Pseudocode used `relativedelta(months=18)` but python-dateutil is not a project dep. `timedelta(days=548)` is documented elsewhere as calendar-drift unsafe (`coverage_audit.py` line 22). **Fix:** do the 18-month boundary check in SQL via `CURRENT_DATE - INTERVAL '18 months'`. Replace `_earliest_sec_filing_date` + Python comparison with a single-query `_is_structurally_young(conn, instrument_id) -> bool` — step 3 has already upserted all fetched filings to `filing_events`, so DB state at step 5 is the authoritative union.
- v4 — Codex-driven (3 issues on v3):
  - (v3-H1) Metadata-first skip `if eight_k_window_covered and entry_filing_to < window_cutoff: continue` could skip pages that still carry in-window 10-K (3y window) or 10-Q (18mo window) data when `bar_met=False`. 8-K window (365d) is shorter than both base-form windows. **Fix:** remove the `continue` clause entirely. The loop terminator `if bar_met and eight_k_window_covered: break` already covers the only valid skip condition; additional per-entry skips are incorrect.
  - (v3-H2) Retryable outcomes (`HTTP_ERROR` / `PARSE_ERROR`) wrote `filings_status='insufficient'`, demoting a correctly-classified `structurally_young` row on transient failure. After 3 attempts the row was frozen insufficient. **Fix:** retryable outcomes pass `status=None` to `_finalise`; the UPDATE omits `filings_status` and preserves current value. `EXHAUSTED` continues to write `insufficient` (an issuer whose earliest filing is now >= 18mo ago has aged out of `structurally_young` and deserves the demote).
  - (v3-L1) 18-month boundary used `>=` in pseudocode but prose said "newer than 18 months". Exact-boundary issuers stayed young one day extra. **Fix:** use strict `>` in `_earliest_sec_filing_date` comparison.
- v3 — Codex-driven (3 issues on v2):
  - (v2-C1) Retryable 8-K failure could still publish `analysable`. Page-0 upserts met the 10-K/10-Q bar, in-loop `audit_instrument` probe *wrote* `analysable`, then 8-K continuation errored. `filing_events` for 10-K/10-Q stayed durable, so scheduler's post-audit re-wrote `analysable` anyway — publishing a row whose 8-K window was never verified. **Fix:** (a) replace in-loop probe with a pure read-only classifier `_probe_status` that does NOT UPDATE; (b) backfill writes coverage exactly once at step 5 (terminal write); (c) scheduler drops the post-backfill `audit_all_instruments` call — pre-audit + per-instrument terminal writes are sufficient and correct.
  - (v2-H1) Pagination stop condition was prose-only; pseudocode still fetched every page before checking `page_oldest_date`. Old-page 404 could then burn retry budget spuriously. **Fix:** consult `files[]` metadata (`filingTo`) BEFORE calling `fetch_submissions_page`; skip older pages once the 8-K window is covered AND base-form bar is met.
  - (v2-M1) Step 4's DB-side 8-K SELECT reopens an implicit tx, so the next `with conn.transaction():` becomes a savepoint, not a durable top-level commit. **Fix:** call `conn.commit()` after every read that isn't immediately followed by a mutating `with conn.transaction():`; commit is a no-op on an idle connection, so it's safe to invoke defensively. Document the invariant explicitly.

---

## Problem

After Chunks D + F-minimal, `weekly_coverage_audit` classifies `coverage.filings_status` from DB state only, leaving `insufficient` rows stuck. Master plan lines 171-184 specify the backfill pass; the per-chunk spec must resolve eight concrete correctness hazards that were latent in v1 (Codex review v2).

## Solution

### Chunk D extension — audit must preserve backfill-owned `structurally_young`

Problem: `audit_all_instruments` and `audit_instrument` classify into `{analysable, insufficient, fpi, no_primary_sec_cik}` only. Master plan line 165 + Chunk D docstring state that `structurally_young` is owned by backfill. But master plan line 191 also calls for `audit_all_instruments` to "re-run to settle" after backfill — which, as implemented today, would overwrite every `structurally_young` back to `insufficient`.

Fix (small patch to `app/services/coverage_audit.py`): change the demote direction only. Promotion to `analysable`/`fpi` always wins; a classifier output of `insufficient` is NOT written over an existing `structurally_young` row.

Implementation:

```python
# In audit_all_instruments bulk UPDATE:
UPDATE coverage c
SET filings_status = v.status,
    filings_audit_at = NOW()
FROM unnest(%s::bigint[], %s::text[]) AS v(instrument_id, status)
WHERE c.instrument_id = v.instrument_id
  AND NOT (
      c.filings_status = 'structurally_young'
      AND v.status = 'insufficient'
  )
```

`audit_instrument`: same guard on its single-row UPDATE. `filings_audit_at` is still bumped on the preserved row via a second UPDATE (see below) so downstream consumers can trust the audit timestamp.

```python
# Split into two UPDATEs so preserved young rows still get audit_at bump:
# 1) status-changing UPDATE (guarded as above)
# 2) audit_at-only UPDATE for rows we saw but left status alone:
UPDATE coverage c
SET filings_audit_at = NOW()
FROM unnest(%s::bigint[]) AS v(instrument_id)
WHERE c.instrument_id = v.instrument_id
```

This is a narrowly scoped change: tests 4, 5, 7 in `tests/test_coverage_audit.py` need extension for the preservation case. Ship it in the same PR as Chunk E — it is the prerequisite invariant that makes Chunk F's post-backfill re-audit safe.

### New service — `app/services/filings_backfill.py`

```python
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from enum import StrEnum
from typing import Any

import psycopg

from app.providers.implementations.sec_edgar import SecFilingsProvider


class BackfillOutcome(StrEnum):
    """Terminal classification for one backfill pass.

    Values are persisted into ``coverage.filings_backfill_reason``.

    Terminal success (attempts → 0):

    - ``COMPLETE_OK`` — post-backfill audit returns ``analysable``.
    - ``COMPLETE_FPI`` — post-backfill audit returns ``fpi``. FPI is
      a terminal coverage classification; no further backfill needed.

    Terminal insufficiency (attempts unchanged — no benefit from
    retrying):

    - ``STILL_INSUFFICIENT_EXHAUSTED`` — all SEC pages consumed
      cleanly, 8-K 365-day window fully verified, earliest known
      filing is >= 18 months ago (old-enough issuer) OR no filings
      found at all (can't prove youth). Still below the 10-K/10-Q
      bar. Further retries will not change this until new filings
      arrive; weekly audit naturally re-eligibilises the row.
    - ``STILL_INSUFFICIENT_STRUCTURALLY_YOUNG`` — all SEC pages
      consumed cleanly, earliest known SEC filing (DB ∪ fetched)
      is newer than 18 months before today, and at least one
      filing exists. Issuer has not existed long enough to meet
      the bar. Writes ``filings_status='structurally_young'``.

    Retryable insufficiency (attempts + 1):

    - ``STILL_INSUFFICIENT_HTTP_ERROR`` — network error / 5xx /
      timeout / 404 on `fetch_submissions` / 404 on secondary page
      that primary claimed exists.
    - ``STILL_INSUFFICIENT_PARSE_ERROR`` — `json.JSONDecodeError`
      / `TypeError` / `KeyError` while parsing a page.

    Gated (no fetch performed):

    - ``SKIPPED_ATTEMPTS_CAP`` — attempts >= 3 AND last_reason ∈
      retryable-insufficiency set.
    - ``SKIPPED_BACKOFF_WINDOW`` — last_at within past 7 days.
    """

    COMPLETE_OK = "COMPLETE_OK"
    COMPLETE_FPI = "COMPLETE_FPI"
    STILL_INSUFFICIENT_EXHAUSTED = "STILL_INSUFFICIENT_EXHAUSTED"
    STILL_INSUFFICIENT_STRUCTURALLY_YOUNG = "STILL_INSUFFICIENT_STRUCTURALLY_YOUNG"
    STILL_INSUFFICIENT_HTTP_ERROR = "STILL_INSUFFICIENT_HTTP_ERROR"
    STILL_INSUFFICIENT_PARSE_ERROR = "STILL_INSUFFICIENT_PARSE_ERROR"
    SKIPPED_ATTEMPTS_CAP = "SKIPPED_ATTEMPTS_CAP"
    SKIPPED_BACKOFF_WINDOW = "SKIPPED_BACKOFF_WINDOW"


_RETRYABLE_REASONS: frozenset[str] = frozenset({
    BackfillOutcome.STILL_INSUFFICIENT_HTTP_ERROR.value,
    BackfillOutcome.STILL_INSUFFICIENT_PARSE_ERROR.value,
})


@dataclass(frozen=True)
class BackfillResult:
    instrument_id: int
    outcome: BackfillOutcome
    pages_fetched: int
    filings_upserted: int
    eight_k_gap_filled: int
    final_status: str  # filings_status after the backfill's own writes


ATTEMPTS_CAP: int = 3
BACKOFF_DAYS: int = 7
STRUCTURAL_YOUNG_MONTHS: int = 18
EIGHT_K_WINDOW_DAYS: int = 365


def backfill_filings(
    conn: psycopg.Connection[Any],
    provider: SecFilingsProvider,
    cik: str,
    instrument_id: int,
) -> BackfillResult:
    ...
```

### Flow

#### Durability invariant (v3 M1)

psycopg3 opens an implicit transaction on the first `execute` against an idle connection. Any `with conn.transaction():` that follows becomes a savepoint, not a durable top-level commit. To keep per-page upserts durable against later errors, this module's rule is:

**Before every `with conn.transaction():` mutation block, call `conn.commit()`.** `commit()` is a no-op on an idle connection and cheap on a read-only implicit tx. This guarantees the subsequent `with` block is top-level and commits durably on exit.

Applied at: step 1 end, step 3 per-page, step 4 pre-SELECT and pre-mutation, step 5 pre-write.

#### 1. Gating check

```python
row = conn.execute(
    "SELECT filings_backfill_attempts, filings_backfill_last_at, "
    "filings_backfill_reason, filings_status "
    "FROM coverage WHERE instrument_id = %s",
    (instrument_id,),
).fetchone()
conn.commit()  # close the implicit read tx (v3 M1 invariant).
```

Gating outcomes:
- `last_at IS NOT NULL AND last_at > NOW() - INTERVAL '7 days'` → return `SKIPPED_BACKOFF_WINDOW`. No coverage write.
- `attempts >= 3 AND last_reason ∈ _RETRYABLE_REASONS AND filings_status != 'structurally_young'` → return `SKIPPED_ATTEMPTS_CAP`. No coverage write.
- Otherwise proceed.

The attempts cap bites only when the last terminal reason was HTTP/PARSE AND the current status is not `structurally_young` (v5 H1 — young rows must stay eligible indefinitely so an aged-out issuer can be demoted once backfill lands cleanly). EXHAUSTED rows with last-HTTP/PARSE still get capped (no harm in waiting for manual intervention); they are distinguished from young in that audit's demote guard does NOT protect insufficient rows, so a future fix arriving in DB via daily index ingest is caught by ordinary audit. The gating read loads `filings_status` alongside attempts/last_at/reason for this check.

#### 2. Fetch `submissions.json`

```python
try:
    submissions = provider.fetch_submissions(cik)
except httpx.HTTPError:
    return _finalise(conn, instrument_id, BackfillOutcome.STILL_INSUFFICIENT_HTTP_ERROR, ...)
except (json.JSONDecodeError, TypeError, KeyError):
    return _finalise(conn, instrument_id, BackfillOutcome.STILL_INSUFFICIENT_PARSE_ERROR, ...)

if submissions is None:
    # 404 — CIK was valid in external_identifiers but SEC has no
    # submissions for it. Classify as HTTP_ERROR (recoverable: CIK
    # correction via daily refresh may fix it) rather than
    # EXHAUSTED (which would imply "we looked and there genuinely
    # is nothing", untrue here).
    return _finalise(conn, instrument_id, BackfillOutcome.STILL_INSUFFICIENT_HTTP_ERROR, ...)
```

#### 3. Iterate pages recent-first

```python
bar_met: bool = False
eight_k_window_covered: bool = False
seen_filings: list[FilingSearchResult] = []
window_cutoff: date = date.today() - timedelta(days=EIGHT_K_WINDOW_DAYS)

# -- Phase A: process the inline `recent` block first.
try:
    recent_block = submissions['filings']['recent']
    recent_results = _normalise_submissions_block(recent_block, cik_padded)
except (KeyError, TypeError, ValueError):
    return _finalise(conn, instrument_id, BackfillOutcome.STILL_INSUFFICIENT_PARSE_ERROR, ...)

conn.commit()  # M1 invariant before mutation block.
with conn.transaction():
    for r in recent_results:
        _upsert_filing(conn, instrument_id, 'sec', r)
seen_filings.extend(recent_results)
pages_fetched += 1
filings_upserted += len(recent_results)

bar_met = _probe_status(conn, instrument_id) in ('analysable', 'fpi')
if recent_results:
    oldest_recent = min(r.filed_at.date() for r in recent_results)
    if oldest_recent <= window_cutoff:
        eight_k_window_covered = True

# -- Phase B: iterate `files[]` metadata. Skip entries by metadata
# BEFORE fetching, so an old-page 404/5xx cannot turn into a
# spurious retry-budget burn once the 8-K window is already
# covered (v3 H1).
files_meta = submissions.get('filings', {}).get('files') or []
try:
    entries = sorted(
        files_meta,
        key=lambda e: date.fromisoformat(e['filingTo']),
        reverse=True,
    )
except (KeyError, TypeError, ValueError):
    # Missing/malformed filingTo on at least one entry — fallback
    # to reversed original order (SEC documents files[] as
    # oldest→newest, so reversed == newest first).
    entries = list(reversed(files_meta))

for entry in entries:
    if bar_met and eight_k_window_covered:
        break  # nothing further to fetch (v4: this is the ONLY
               # valid skip condition; per-entry skip based on
               # filingTo alone is incorrect — 8-K window is 365d,
               # 10-K window is 3y, 10-Q window is 18mo, so an
               # entry older than 365d may still carry in-window
               # base-form filings when bar_met is False).

    try:
        page_raw = provider.fetch_submissions_page(entry['name'])
    except httpx.HTTPError:
        return _finalise(conn, instrument_id, BackfillOutcome.STILL_INSUFFICIENT_HTTP_ERROR, ...)
    if page_raw is None:
        # 404 on a page the primary response claimed exists — data
        # integrity; classify retryable.
        return _finalise(conn, instrument_id, BackfillOutcome.STILL_INSUFFICIENT_HTTP_ERROR, ...)

    try:
        page_results = _normalise_submissions_block(page_raw, cik_padded)
    except (KeyError, TypeError, ValueError):
        return _finalise(conn, instrument_id, BackfillOutcome.STILL_INSUFFICIENT_PARSE_ERROR, ...)

    conn.commit()  # M1 invariant.
    with conn.transaction():
        for r in page_results:
            _upsert_filing(conn, instrument_id, 'sec', r)
    seen_filings.extend(page_results)
    pages_fetched += 1
    filings_upserted += len(page_results)

    if not bar_met:
        bar_met = _probe_status(conn, instrument_id) in ('analysable', 'fpi')

    if page_results:
        page_oldest = min(r.filed_at.date() for r in page_results)
        if page_oldest <= window_cutoff:
            eight_k_window_covered = True
```

`_probe_status(conn, instrument_id) -> str` (v3 C1) is a new read-only helper in `app/services/coverage_audit.py`: runs the same aggregate + classifier logic as `audit_instrument` but does NOT UPDATE coverage. Used wherever backfill needs to know the current classifier output without publishing it.

```python
def _probe_status(conn: psycopg.Connection[Any], instrument_id: int) -> str:
    """Read-only classifier probe. Identical query + _classify call
    as audit_instrument, but never writes coverage. Chunk E uses
    this inside the pagination loop so a later retryable error
    cannot leave a premature 'analysable' in coverage (v3 C1).
    """
    # Body: the SELECT + _classify portion of audit_instrument,
    # minus the UPDATE.
```

Also: after `_probe_status`'s SELECT, `conn.commit()` is called internally before return (same M1 invariant).

#### 4. 8-K gap check (always runs — v2 C2)

Step 3's pagination loop already fetches pages until `eight_k_window_covered` is True. If it terminates with `eight_k_window_covered=False` (e.g., issuer has < 1 year of history total, or `files[]` is empty and `recent` didn't span the window), there are no more pages to fetch and the 365-day 8-K window is simply as complete as SEC's own record allows. Step 4 then reconciles fetched-in-window 8-Ks against DB:

```python
conn.commit()  # M1 invariant before the read below.
db_rows = conn.execute(
    """
    SELECT provider_filing_id
    FROM filing_events
    WHERE instrument_id = %s
      AND provider = 'sec'
      AND filing_type = '8-K'
      AND filing_date >= %s
    """,
    (instrument_id, window_cutoff),
).fetchall()
conn.commit()  # M1 invariant — SELECT leaves an implicit tx open.
db_eight_ks = {r[0] for r in db_rows}

fetched_eight_ks = {
    r.provider_filing_id
    for r in seen_filings
    if r.filing_type == '8-K' and r.filed_at.date() >= window_cutoff
}

# Diff is normally empty — step 3 upserts cover everything we
# fetched. Non-empty only when ON CONFLICT upsert was silently
# blocked (e.g., row deleted mid-flight by another writer).
# Defensive re-fetch via get_filing():
for missing_accession in fetched_eight_ks - db_eight_ks:
    try:
        event = provider.get_filing(missing_accession)
    except FilingNotFound:
        continue  # SEC deleted between pages; skip.
    except httpx.HTTPError:
        return _finalise(conn, instrument_id, BackfillOutcome.STILL_INSUFFICIENT_HTTP_ERROR, ...)

    conn.commit()  # M1 invariant.
    with conn.transaction():
        _upsert_filing_event(conn, instrument_id, 'sec', event)
    eight_k_gap_filled += 1
```

(`_upsert_filing_event` is a thin `_upsert_filing` variant that takes a `FilingEvent` rather than `FilingSearchResult`; `get_filing` returns the former. Implementing it = 10 lines, same ON CONFLICT target. Ship in same PR.)

#### 5. Terminal classification + single coverage write (v3 C1)

After step 4, DB reflects everything SEC knows about this CIK within the 365-day 8-K window + whatever base-form pages we paged to meet the bar. Probe once more (read-only), then write coverage exactly once via `_finalise`:

```python
final_status = _probe_status(conn, instrument_id)  # read-only

if final_status == 'analysable':
    outcome = BackfillOutcome.COMPLETE_OK
    status_to_write = 'analysable'
elif final_status == 'fpi':
    outcome = BackfillOutcome.COMPLETE_FPI
    status_to_write = 'fpi'
elif final_status == 'insufficient':
    # v5-L1: 18-month boundary computed in SQL for calendar-correct
    # arithmetic (matches the existing coverage_audit.py pattern;
    # no python-dateutil dep). Step 3's upserts have already
    # committed every fetched filing to filing_events, so the DB
    # MIN(filing_date) is the authoritative union of DB + seen.
    # Strict '>' — "newer than 18 months" per spec prose; exact-
    # boundary issuer is no longer young.
    if _is_structurally_young(conn, instrument_id):
        outcome = BackfillOutcome.STILL_INSUFFICIENT_STRUCTURALLY_YOUNG
        status_to_write = 'structurally_young'  # backfill-owned write
    else:
        outcome = BackfillOutcome.STILL_INSUFFICIENT_EXHAUSTED
        status_to_write = 'insufficient'
elif final_status == 'no_primary_sec_cik':
    # Not reachable — eligibility filter excluded this.
    raise RuntimeError(
        f"backfill_filings: unexpected no_primary_sec_cik for "
        f"instrument_id={instrument_id}; eligibility filter bug?"
    )
else:
    raise RuntimeError(f"unknown classifier status: {final_status!r}")

return _finalise(
    conn, instrument_id,
    outcome=outcome,
    status=status_to_write,
    pages_fetched=pages_fetched,
    filings_upserted=filings_upserted,
    eight_k_gap_filled=eight_k_gap_filled,
)
```

`_finalise` is the single coverage-write path (shared by gating paths + all terminal paths). Error paths (HTTP / PARSE) pass `status=None` so the current `filings_status` is preserved (v4 H2 — never demote `structurally_young` on transient failure):

```python
def _finalise(
    conn: psycopg.Connection[Any],
    instrument_id: int,
    *,
    outcome: BackfillOutcome,
    status: str | None,  # None = preserve current filings_status
    pages_fetched: int = 0,
    filings_upserted: int = 0,
    eight_k_gap_filled: int = 0,
) -> BackfillResult:
    """Single coverage-write sink. Computes attempts delta from
    outcome, writes status (when non-None) + attempts + last_at +
    reason in one UPDATE, commits explicitly.

    attempts delta by outcome:
    - COMPLETE_OK / COMPLETE_FPI       -> set 0
    - HTTP_ERROR / PARSE_ERROR         -> += 1
    - EXHAUSTED / STRUCTURALLY_YOUNG   -> unchanged
    - SKIPPED_*                        -> no write at all

    status write by outcome (v4 H2):
    - COMPLETE_OK                      -> 'analysable'
    - COMPLETE_FPI                     -> 'fpi'
    - STRUCTURALLY_YOUNG               -> 'structurally_young'
    - EXHAUSTED                        -> 'insufficient'
    - HTTP_ERROR / PARSE_ERROR         -> None (preserve current;
                                           must not demote a
                                           correctly-classified
                                           structurally_young row
                                           on transient failure)
    - SKIPPED_*                        -> no write at all
    """
    if outcome in (BackfillOutcome.SKIPPED_ATTEMPTS_CAP,
                   BackfillOutcome.SKIPPED_BACKOFF_WINDOW):
        # Gating path — no mutation.
        return BackfillResult(
            instrument_id=instrument_id,
            outcome=outcome,
            pages_fetched=0,
            filings_upserted=0,
            eight_k_gap_filled=0,
            final_status='',
        )

    delta_sql = {
        BackfillOutcome.COMPLETE_OK: "0",
        BackfillOutcome.COMPLETE_FPI: "0",
        BackfillOutcome.STILL_INSUFFICIENT_HTTP_ERROR: "filings_backfill_attempts + 1",
        BackfillOutcome.STILL_INSUFFICIENT_PARSE_ERROR: "filings_backfill_attempts + 1",
        BackfillOutcome.STILL_INSUFFICIENT_EXHAUSTED: "filings_backfill_attempts",
        BackfillOutcome.STILL_INSUFFICIENT_STRUCTURALLY_YOUNG: "filings_backfill_attempts",
    }[outcome]

    conn.commit()  # M1 invariant.
    if status is not None:
        sql = f"""
            UPDATE coverage
            SET filings_status            = %s,
                filings_backfill_attempts = {delta_sql},
                filings_backfill_last_at  = NOW(),
                filings_backfill_reason   = %s,
                filings_audit_at          = NOW()
            WHERE instrument_id = %s
        """
        conn.execute(sql, (status, outcome.value, instrument_id))
    else:
        # Preserve current filings_status (v4 H2).
        sql = f"""
            UPDATE coverage
            SET filings_backfill_attempts = {delta_sql},
                filings_backfill_last_at  = NOW(),
                filings_backfill_reason   = %s
            WHERE instrument_id = %s
        """
        conn.execute(sql, (outcome.value, instrument_id))
    conn.commit()  # K.2/K.3 durability pattern.

    # final_status for the caller: if we wrote status, that's it;
    # else re-read current value for logging.
    if status is not None:
        final = status
    else:
        row = conn.execute(
            "SELECT filings_status FROM coverage WHERE instrument_id = %s",
            (instrument_id,),
        ).fetchone()
        final = str(row[0]) if row is not None else ''
        conn.commit()  # M1.

    return BackfillResult(
        instrument_id=instrument_id,
        outcome=outcome,
        pages_fetched=pages_fetched,
        filings_upserted=filings_upserted,
        eight_k_gap_filled=eight_k_gap_filled,
        final_status=final,
    )
```

Every `_finalise` call site for retryable error outcomes passes `status=None`. Example: `return _finalise(conn, instrument_id, outcome=BackfillOutcome.STILL_INSUFFICIENT_HTTP_ERROR, status=None, ...)`.

`_is_structurally_young(conn, instrument_id)` (v5 L1) is a pure DB-side predicate — step 3's upserts have committed every fetched filing, so `filing_events` is the authoritative union:

```python
def _is_structurally_young(conn: psycopg.Connection[Any], instrument_id: int) -> bool:
    """True iff the instrument's earliest SEC filing is strictly
    newer than today - 18 months (calendar-correct via SQL INTERVAL).
    False when no filings exist at all (can't prove youth — classify
    as EXHAUSTED, not YOUNG; closes v2-H3).
    """
    row = conn.execute(
        """
        SELECT MIN(filing_date) > (CURRENT_DATE - INTERVAL '18 months')
        FROM filing_events
        WHERE instrument_id = %s AND provider = 'sec'
        """,
        (instrument_id,),
    ).fetchone()
    conn.commit()  # M1 invariant.
    return bool(row[0]) if row is not None and row[0] is not None else False
```

### Provider additions — `app/providers/implementations/sec_edgar.py`

1. **Extract `_normalise_submissions_block`** from the existing `_normalise_filings`:

```python
def _normalise_submissions_block(
    block: dict[str, object],
    cik_padded: str,
    start_date: date | None = None,
    end_date: date | None = None,
    filing_types: list[str] | None = None,
) -> list[FilingSearchResult]:
    """Pure normalisation of one submissions page. Both the
    inline ``filings.recent`` dict and any ``files[]`` secondary
    page JSON carry the same parallel-array shape —
    ``{accessionNumber, filingDate, form, primaryDocument, reportDate}``
    — so Chunk E's pagination loop can call this per page.
    """
    # body lifted unchanged from the existing "recent" block path
    # in _normalise_filings.
```

2. **Delegate `_normalise_filings` to it**:

```python
def _normalise_filings(raw, cik_padded, start_date, end_date, filing_types):
    recent = raw.get("filings", {}).get("recent") if isinstance(raw.get("filings"), dict) else None
    if not isinstance(recent, dict):
        return []
    return _normalise_submissions_block(recent, cik_padded, start_date, end_date, filing_types)
```

3. **Add `fetch_submissions_page`** (public):

```python
def fetch_submissions_page(self, name: str) -> dict[str, object] | None:
    """Fetch a secondary submissions page named in
    ``filings.files[].name`` (e.g. ``CIK0000320193-submissions-001.json``).

    Returns the parsed JSON dict or ``None`` on 404. Uses the same
    rate-limited HTTP client as ``fetch_submissions`` so the 10
    req/s SEC cap is respected across the combined call pattern.
    """
    path = f"/submissions/{name}"
    resp = self._http.get(path)
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    raw = resp.json()
    _persist_raw(f"sec_submissions_page_{name}", raw)
    return raw  # type: ignore[return-value]
```

### Scheduler extension — `app/workers/scheduler.py::weekly_coverage_audit`

Replace current audit-only body (lines 2084-2124) with:

```python
def weekly_coverage_audit() -> None:
    """Classify every tradable SEC-covered instrument via the bulk
    audit, then drive any non-terminal one toward terminal state
    via ``backfill_filings``. ``backfill_filings`` writes the
    terminal ``filings_status`` for each instrument it touches,
    so no post-audit re-sweep is needed (v3 C1: a post-audit
    could publish ``analysable`` for instruments whose 8-K window
    was not verified by backfill, because audit's bar is
    10-K/10-Q only).

    Eligibility for backfill: filings_status IN
    ('insufficient', 'unknown', 'structurally_young').
    Including ``structurally_young`` lets aging young issuers
    re-promote to ``analysable`` once they have filed past the
    18-month bar (master plan line 184). ``fpi`` and
    ``no_primary_sec_cik`` are terminal — not eligible.
    """
    with _tracked_job(JOB_WEEKLY_COVERAGE_AUDIT) as tracker:
        from app.services.coverage_audit import audit_all_instruments
        from app.services.filings_backfill import BackfillOutcome, backfill_filings

        with psycopg.connect(settings.database_url) as conn:
            pre_audit = audit_all_instruments(conn)

            eligible = conn.execute(
                """
                SELECT c.instrument_id, ei.identifier_value AS cik
                FROM coverage c
                JOIN external_identifiers ei
                    ON ei.instrument_id = c.instrument_id
                   AND ei.provider = 'sec'
                   AND ei.identifier_type = 'cik'
                   AND ei.is_primary = TRUE
                WHERE c.filings_status IN ('insufficient', 'unknown', 'structurally_young')
                """
            ).fetchall()
            conn.commit()  # M1 invariant after read.

            outcomes: dict[BackfillOutcome, int] = {o: 0 for o in BackfillOutcome}
            with SecFilingsProvider(user_agent=settings.sec_user_agent) as provider:
                for row in eligible:
                    iid, cik = int(row[0]), str(row[1])
                    try:
                        result = backfill_filings(conn, provider, cik, iid)
                    except Exception:
                        # K.1 review round 1: ``except psycopg.Error``
                        # too narrow; use bare ``Exception`` for
                        # per-instrument isolation across siblings.
                        logger.exception(
                            "weekly_coverage_audit: backfill raised for instrument_id=%d",
                            iid,
                        )
                        continue
                    outcomes[result.outcome] += 1

        tracker.row_count = pre_audit.total_updated + sum(
            outcomes[o] for o in (
                BackfillOutcome.COMPLETE_OK,
                BackfillOutcome.COMPLETE_FPI,
                BackfillOutcome.STILL_INSUFFICIENT_STRUCTURALLY_YOUNG,
                BackfillOutcome.STILL_INSUFFICIENT_EXHAUSTED,
                BackfillOutcome.STILL_INSUFFICIENT_HTTP_ERROR,
                BackfillOutcome.STILL_INSUFFICIENT_PARSE_ERROR,
            )
        )
        logger.info(
            "weekly_coverage_audit complete: "
            "pre_analysable=%d eligible=%d "
            "complete_ok=%d complete_fpi=%d structurally_young=%d "
            "exhausted=%d http_err=%d parse_err=%d "
            "skipped_cap=%d skipped_backoff=%d null_anomalies=%d",
            pre_audit.analysable,
            len(eligible),
            outcomes[BackfillOutcome.COMPLETE_OK],
            outcomes[BackfillOutcome.COMPLETE_FPI],
            outcomes[BackfillOutcome.STILL_INSUFFICIENT_STRUCTURALLY_YOUNG],
            outcomes[BackfillOutcome.STILL_INSUFFICIENT_EXHAUSTED],
            outcomes[BackfillOutcome.STILL_INSUFFICIENT_HTTP_ERROR],
            outcomes[BackfillOutcome.STILL_INSUFFICIENT_PARSE_ERROR],
            outcomes[BackfillOutcome.SKIPPED_ATTEMPTS_CAP],
            outcomes[BackfillOutcome.SKIPPED_BACKOFF_WINDOW],
            pre_audit.null_anomalies,
        )
```

All summary values read inside the `with _tracked_job(...)` block (F review gotcha).

Why no `post_audit`:

- Each `backfill_filings(...)` call writes the terminal `filings_status` for the instrument it touched (COMPLETE_OK → analysable, COMPLETE_FPI → fpi, STRUCTURALLY_YOUNG → structurally_young, EXHAUSTED/HTTP_ERROR/PARSE_ERROR → leaves `insufficient`).
- Instruments that backfill DIDN'T touch (terminal pre-audit classification: `analysable`, `fpi`, `no_primary_sec_cik`) are unchanged by the batch — `filing_events` inserts are per-instrument, no cross-instrument effects.
- Instruments where backfill RAISED before `_finalise` retain their pre-audit classification (per-instrument isolation). No inconsistency.
- Skipping the post-audit is precisely what closes v3 C1 (retryable-error leaks cannot promote to analysable via an audit that only checks 10-K/10-Q counts).

### Migration-less change

No new SQL. All required columns exist in migration 036.

## Fixtures — `tests/fixtures/sec/`

SEC wire shape for reference:

```json
{
  "cik": "0000320193",
  "filings": {
    "recent": { "accessionNumber": [...], "filingDate": [...], "form": [...], ... },
    "files": [
      { "name": "CIK0000320193-submissions-001.json", "filingCount": 1000, "filingFrom": "2018-01-01", "filingTo": "2020-12-31" },
      { "name": "CIK0000320193-submissions-002.json", "filingCount": 1000, "filingFrom": "2015-01-01", "filingTo": "2017-12-31" }
    ]
  }
}
```

Individual accessions live inside each secondary page's own `{accessionNumber, filingDate, form, ...}` arrays. `files[]` itself is only page metadata.

Fixture set:

- `submissions_MATURE.json` — `recent` has 2 × 10-K + 4 × 10-Q + 6 × 8-K within 365d; `files: []`. Drives `COMPLETE_OK` with `pages_fetched=1`, `eight_k_gap_filled=0`.
- `submissions_PAGED.json` — `recent` has 1 × 10-K + 2 × 10-Q, `files[]` names one older page. `submissions_PAGED-page-001.json` adds the missing 1 × 10-K + 2 × 10-Q within the 3-year / 18-month windows. Combined meets the bar. Drives `COMPLETE_OK` with `pages_fetched=2`.
- `submissions_YOUNG.json` — earliest filing 6 months ago, 1 × 10-K + 1 × 10-Q, `files: []`. Drives `STRUCTURALLY_YOUNG`.
- `submissions_EXHAUSTED.json` — earliest filing 5 years ago, all filings predate the 18-month window such that the 10-Q count in window is < 4. `files: []` (or all-historical pages, all fetched). Drives `STILL_INSUFFICIENT_EXHAUSTED`.
- `submissions_FPI.json` — 3 × 20-F + 12 × 6-K spanning 3 years, zero 10-K/10-Q/10-K/A/10-Q/A. Audit classifies `fpi`. Drives `COMPLETE_FPI`.
- `submissions_8K_GAP.json` + `submissions_8K_GAP-page-001.json` — `recent` meets base-form bar with `filingTo` on a date such that `filingFrom` is within the 365-day window (bar_met immediately, but `eight_k_window_covered=False`). Secondary page completes the 365-day 8-K coverage with 3 additional 8-Ks within window. Drives the pagination continuation path (bar met early, continues paging for 8-K window).
- `submissions_404.json` — not used directly; test stubs `fetch_submissions` to return `None`.
- `submissions_BAD_JSON` — test stubs `fetch_submissions` to raise `json.JSONDecodeError`.
- `submissions_HTTP_ERROR` — test stubs to raise `httpx.HTTPError`.

## Tests

**Unit — `tests/test_filings_backfill.py`:**

1. Gating: `attempts=3, last_reason=HTTP_ERROR` → `SKIPPED_ATTEMPTS_CAP`; no provider call, no coverage mutation.
2. Gating: `last_at=NOW() - 3 days` → `SKIPPED_BACKOFF_WINDOW`; no provider call, no coverage mutation.
3. Gating: `attempts=5, last_reason=STILL_INSUFFICIENT_EXHAUSTED` → proceeds (cap bites retryable-only).
4. Gating: `attempts=3, last_reason=STILL_INSUFFICIENT_STRUCTURALLY_YOUNG` → proceeds.
4b. Gating (v5 H1 regression): `filings_status='structurally_young', attempts=3, last_reason=STILL_INSUFFICIENT_HTTP_ERROR, last_at=NOW()-8d` → proceeds. Cap exemption for young rows lets an aged-out young issuer demote to EXHAUSTED on next clean run. Without this test, the v4-H1 freeze regression could reappear silently.
5. `COMPLETE_OK` on page 0: `MATURE` fixture, audit returns `analysable`, `attempts → 0`, `reason=COMPLETE_OK`, `last_at` set.
6. `COMPLETE_OK` after pagination: `PAGED` fixtures, `pages_fetched=2`, bar_met on page 1 AND 8-K window covered on page 0 (all 8-Ks in recent).
7. `COMPLETE_FPI`: `FPI` fixture, `final_status='fpi'`, `attempts → 0`, `reason=COMPLETE_FPI`.
8. `STRUCTURALLY_YOUNG`: `YOUNG` fixture, `filings_status='structurally_young'` written by backfill, `attempts` unchanged.
9. `EXHAUSTED` old issuer: `EXHAUSTED` fixture, `filings_status='insufficient'` (not written over), `attempts` unchanged, `reason=STILL_INSUFFICIENT_EXHAUSTED`.
10. `EXHAUSTED` zero filings (H3): stub `fetch_submissions` to return a valid but empty response, `seen_filings` empty, outcome = `STILL_INSUFFICIENT_EXHAUSTED` (NOT `STRUCTURALLY_YOUNG`).
11. `HTTP_ERROR` from `fetch_submissions`: raises `httpx.HTTPError`, `attempts + 1`, `reason=STILL_INSUFFICIENT_HTTP_ERROR`.
12. `HTTP_ERROR` from a secondary page fetch mid-pagination: partial page-0 upserts already durable (commits hold), `pages_fetched` counts the failed page only if upserts happened (0 here).
13. `HTTP_ERROR` on `fetch_submissions` returning `None` (404): `attempts + 1`, `reason=STILL_INSUFFICIENT_HTTP_ERROR` (not EXHAUSTED).
14. `PARSE_ERROR` from `json.JSONDecodeError`: `attempts + 1`, `reason=STILL_INSUFFICIENT_PARSE_ERROR`.
15. `PARSE_ERROR` from `KeyError` on missing `filings.recent`: same.
16. 8-K pagination-continuation path: `8K_GAP` fixtures — bar met on page 0, `eight_k_window_covered=False` on page 0, step 4 continues into page 1 and upserts the missing 8-Ks. `eight_k_gap_filled` reflects the upsert count; outcome = `COMPLETE_OK`.
17. 8-K HTTP error during continuation: partial 8-K inserts durable, outcome = `STILL_INSUFFICIENT_HTTP_ERROR`.
18. `_normalise_submissions_block` on `recent` dict: identical output to pre-refactor `_normalise_filings` (regression).
19. `_normalise_submissions_block` on a `files[]` page dict: same fields, same output shape.
20. Chunk D audit extension: `audit_all_instruments` on a `structurally_young` row whose classifier returns `insufficient` leaves `filings_status='structurally_young'` (preserves backfill ownership). `filings_audit_at` bumped.
21. Chunk D audit extension: `structurally_young` row whose classifier now returns `analysable` is promoted.
22. Chunk D audit extension: `audit_instrument` single-row path obeys the same demote-guard.

**Integration — `tests/integration/test_filings_backfill_real_db.py` (uses `ebull_test` DB per memory #test_db_isolation):**

23. End-to-end: seed empty `filing_events`, stub provider with `PAGED` fixtures, call `backfill_filings` → DB rows present, coverage columns updated, no other instruments' coverage touched.
24. Idempotency: back-to-back calls within backoff window → second is `SKIPPED_BACKOFF_WINDOW`, no duplicate upserts.
25. Per-page durability: seed fixture, inject `psycopg.Error` after page 1 upsert (via monkey-patched `_upsert_filing`), assert page-0 rows persist in DB after the raise.
26. Chunk D audit preservation: seed `structurally_young` row, run `audit_all_instruments` with classifier aggregates that would return `insufficient` → DB row still `structurally_young`.

**Integration — `tests/integration/test_weekly_coverage_audit.py` (extend existing F-minimal tests):**

27. Scheduler full path: 4 instruments {`analysable` pre-seeded, `insufficient` that backfill promotes to `analysable`, `insufficient` that backfill errors on, `structurally_young` that ages into `analysable`}. Assert: eligible set includes the latter three; per-instrument error doesn't abort the batch; final log line reflects outcome counts.
28. `fpi` path: `insufficient` instrument whose SEC history is 20-F heavy → backfill upserts 20-Fs → audit_instrument returns `fpi` → outcome `COMPLETE_FPI` → post-audit `fpi` preserved.
29. Per-instrument isolation: one instrument raises `RuntimeError` mid-`backfill_filings` (stubbed) — other instruments in the batch still processed.
30. `null_anomalies` counter exposed in final log line.

## Risks

- **Pagination exhaustion on a serial filer**: a CIK with 10+ secondary pages could trigger many HTTP calls. Mitigation: rate limiter enforces 10 rps; weekly cadence + retry backoff bound blast radius. If a pathological issuer consistently exhausts, the EXHAUSTED outcome + 7-day backoff prevents tight-loop re-attempts.
- **`files[]` ordering field absent on some responses**: fallback to reversed (SEC documents files[] as chronological oldest→newest, so `reversed()` ≡ newest-first). Guard with `except (KeyError, ValueError)`.
- **`eight_k_window_covered` false-negative on single-page responses**: if `recent` has zero filings older than 365d (because `filingCount` is small), `page_oldest_date > today - 365d` and the flag stays False. If `files: []` is also empty, the pagination iterator terminates with `eight_k_window_covered=False` but nothing more to fetch — safe: step 4 fetches nothing new, diff is empty, no false miss. Test 16 covers this via the `MATURE` fixture where `files: []`.
- **Race: post-audit reclassifies mid-backfill**: if an external writer modifies `filing_events` between step 3 and step 5, the final audit could re-read a mutated state. Acceptable — only the weekly job writes coverage, and `filing_events` writes are append-only.
- **Explicit `conn.commit()` incompatible with outer explicit tx**: scheduler today opens `psycopg.connect(settings.database_url)` and does not wrap. Documented in docstring. K.2/K.3 pattern.

## Codex checkpoints

Per CLAUDE.md:

1. ✅ Pre-spec review v1 — 8 issues → rolled into v2.
2. ✅ Pre-spec review v2 — 3 issues → rolled into v3.
3. ✅ Pre-spec review v3 — 3 issues → rolled into v4.
4. ✅ Pre-spec review v4 — 2 issues → rolled into v5.
5. Pre-spec review v5 — run before user approval.
6. Pre-push review — after implementation, before first `git push`.
7. Rebuttal-only merge — if warranted.

## Shipping order

1. **Chunk D audit extension** — demote-guard on `audit_all_instruments` + `audit_instrument`. Unit tests 20-22. Ship first because Chunk E depends on it.
2. **Provider refactor** — `_normalise_submissions_block` extraction + `fetch_submissions_page` addition. Unit tests 18-19.
3. **Gating + page iteration core** — `BackfillOutcome` + `BackfillResult` + flow steps 1-3. Unit tests 1-12.
4. **8-K gap check + pagination continuation** — step 4. Unit tests 16-17.
5. **Outcome classifier + coverage write** — step 5. Unit tests 13-15 (EXHAUSTED branches), polish 5-9.
6. **Integration tests** — 23-26.
7. **Scheduler extension** — replaces F-minimal body. Integration tests 27-30.
8. **Codex pre-push review** + PR.
