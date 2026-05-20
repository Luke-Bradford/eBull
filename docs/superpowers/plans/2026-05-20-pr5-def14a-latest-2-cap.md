# PR5 — DEF 14A latest-2-proxies-per-filer ingest cap (#1233 retention rubric)

> Created: **2026-05-20**. Revised after Codex 1a (10 findings) + Codex 1b
> (5 residual issues) + Codex 1c (3 residual issues) + Codex 1d (2 residual
> issues) + Codex 1e (1 residual issue — NULL cik mishandling).
> Spec: `docs/superpowers/specs/2026-05-19-data-retention-rubric.md` §4.7 + §7.
> PR4 (Form 4 3y cap) merged as #1240 (commit `97448d0`).
> PR3 (filing_events 10y cap) merged as #1239 (commit `85c9de0`).
> PR2 (XBRL 20y + whitelist) merged as #1237 (commit `ee95ca2`).
> PR5 prior partial: NUMERIC overflow fix folded into #1236 (`b7c566d`).
> Umbrella: **#1233**.

## 1. Scope

Apply the canonical DEF 14A depth cap — **latest 2 PRIMARY `DEF 14A` accessions
per issuer CIK** — at every writer chokepoint. Supplemental form variants
(`DEFA14A`, `DEFR14A`, `DEFM14A`) are uncapped: they're rare amendments/
supplements/merger proxies that don't drive bandwidth pressure, and capping
the primary while letting supplements through matches spec §4.7's
"current + one prior for change tracking" intent (which was talking about
annual primary proxies).

**Ingest-side only** — no `DELETE FROM def14a_*` on pre-cap rows; existing
rows survive until the operator-driven pre-wipe (spec §6.3).

Three chokepoint families:

- **Legacy `filing_events` → `def14a_beneficial_holdings` path**
  (`discover_pending_def14a` selectors driving `ingest_def14a` /
  `bootstrap_def14a`). Both per-instrument and universe-wide branches.
- **Manifest-worker `_parse_def14a` path** — pre-fetch gate that tombstones
  cap-bound rows before the SEC HTTP call.
- **Rewash rescue path** in `_apply_def14a` — the fallback that writes typed
  rows when none existed previously (Codex 1a finding #7). The happy-path
  (re-parse of accessions WITH existing typed rows) stays uncapped because
  it operates on already-existing-rows under spec §6.3.

Out of scope:

- Row deletion of any DEF 14A table — none.
- PRE 14A policy — already tombstoned independently (manifest parser
  line 173).
- 13F-HR 8-quarter cap — PR6.

## 2. Cap shape — rank-based on `DEF 14A` primary form only

PR2 (XBRL 20y), PR3 (filing_events 10y), PR4 (Form 4 3y) are all **time-window**
caps. PR5 is a **count-per-filer** cap: rank `filing_type = 'DEF 14A'`
accessions per issuer CIK, keep top `DEF14A_LATEST_PER_FILER_CAP = 2`.

`DEFA14A` (definitive additional materials), `DEFR14A` (revised definitive
proxy), `DEFM14A` (merger proxy) bypass the cap entirely — they are
supplemental / event-driven and a same-cycle DEFA14A shouldn't evict the
prior-year DEF 14A from the cap window (Codex 1a finding #6).

### 2.1 Ranking predicate

```sql
ROW_NUMBER() OVER (
    PARTITION BY <issuer_cik>, filing_type
    ORDER BY filing_date DESC, provider_filing_id DESC
) AS rank_within_form
```

Then the cap filter is:

```sql
WHERE filing_type <> 'DEF 14A'  -- supplemental forms pass unconditionally
   OR rank_within_form <= %(cap)s  -- primary DEF 14A capped
```

`provider_filing_id DESC` tie-break is deterministic (SEC accessions are
strictly increasing lexicographically within a filer-year). Matches the
legacy `discover_pending_def14a` `ORDER BY filing_date DESC, filing_event_id
DESC` secondary ordering — we use `provider_filing_id` (not
`filing_event_id`) because share-class siblings each produce their own
`filing_event_id` for the same accession; the rank must be invariant of
sibling fan-out.

### 2.2 Issuer-CIK resolution — prefer profile-bearing siblings

`instrument_sec_profile` may be missing for some siblings of a multi-listed
issuer (per #1102 share-class CIK semantics, only one sibling carries the
CIK row; the bulk-ingest fan-out is the PR-B follow-up still pending).

DISTINCT-ON-the-lowest-instrument_id (the legacy de-dupe order) would
arbitrarily keep a profile-less sibling, mis-CIK the accession, and the
`cik IS NULL` carve-out (§2.4) would route it as uncapped. Codex 1a
finding #2.

Fix: order DISTINCT ON to prefer profile-bearing siblings:

```sql
SELECT DISTINCT ON (fe.provider_filing_id)
    fe.provider_filing_id, fe.instrument_id, fe.filing_date,
    fe.filing_type, fe.primary_document_url, fe.filing_event_id, isp.cik
FROM filing_events fe
LEFT JOIN instrument_sec_profile isp ON isp.instrument_id = fe.instrument_id
WHERE fe.provider = 'sec'
  AND fe.filing_type = ANY(%(forms)s)
ORDER BY fe.provider_filing_id, (isp.cik IS NULL), fe.instrument_id
```

`(isp.cik IS NULL)` evaluates to FALSE (0) before TRUE (1) in ASC ordering,
so non-NULL CIK rows are preferred. If NO sibling has a profile, all rows
sort equally and the deterministic `fe.instrument_id` tie-break still holds
— `cik` is NULL on the kept row → carve-out at §2.4 lets the accession
pass.

### 2.3 Missing-URL rows are part of the rank set

Spec §4.7 says "latest 2 proxies per filer (current + one prior for change
tracking)". If the latest 2 primary DEF 14A accessions for a filer haven't
got URLs yet, the cap is satisfied — we wait for URLs to arrive on the
next sync. We do NOT promote rank-3 to fillable. Codex 1a finding #3.

The inner CTE therefore ranks **all** primary DEF 14A accessions (URL or
not). The `primary_document_url IS NOT NULL` filter moves to the OUTER
query (after the rank computation):

```sql
WITH per_accession AS ( ... -- ranked across ALL DEF 14A rows -- )
SELECT ...
FROM per_accession
WHERE primary_document_url IS NOT NULL   -- only fetch URL-bearing
  AND (filing_type <> 'DEF 14A' OR rank_within_form <= %(cap)s)
  AND log.accession_number IS NULL
```

### 2.4 NULL-CIK carve-out

`LEFT JOIN instrument_sec_profile`. Where `cik IS NULL` on the kept row, the
PARTITION BY cik groups them all together (one giant null partition). To
avoid that mis-rank, treat NULL-CIK rows as "pass" via the outer filter:

```sql
WHERE filing_type <> 'DEF 14A'
   OR cik IS NULL                  -- CIK-MISSING: pass; downstream tombstone fast
   OR rank_within_form <= %(cap)s
```

CIK-MISSING accessions still reach the parser, where the existing
`_resolve_issuer_cik → 'CIK-MISSING'` sentinel path tombstones them. The
cap doesn't drop a class of accessions that historically tombstoned via
the sentinel.

### 2.5 Ingest-log filter ordering

The rank MUST be computed across ALL accessions for the CIK (including
already-attempted ones in `def14a_ingest_log`). Otherwise once 2 are in
the log, the 3rd appears as rank-1 in the un-attempted subset and gets
fetched. The `log.accession_number IS NULL` exclusion lives in the
**outer** query, AFTER the rank has already been computed against the
full corpus.

### 2.6 Per-instrument branch — pre-scoped to target CIK

`discover_pending_def14a(instrument_id=X)` is used by ad-hoc re-ingest.

Codex 1a finding #1: the naïve "universe CTE + outer
`r.instrument_id=%(iid)s`" is BROKEN because the DISTINCT ON step kept
one arbitrary sibling per accession, so the outer filter on the calling
instrument matches zero or partial rows.

Codex 1b finding #2: scanning + ranking the entire DEF 14A universe for
every per-instrument call is wasteful. Pre-scope to target CIK in
Python (one round-trip), then rank only that CIK's accessions.

Resulting flow:

1. Python resolves target CIK via the sibling-aware lookup at
   §2.6.0. This catches the case where the calling instrument has no
   `instrument_sec_profile` row but a share-class sibling does — the
   cap MUST apply because the cap is per-CIK, not per-instrument
   (Codex 1c finding #1).
2. **CIK-known** (either direct or sibling-derived): rank only that
   CIK's DEF 14A accessions across siblings. Outer JOIN returns rows
   bound to the calling instrument.
3. **CIK-truly-missing** (no profile anywhere in the share-class
   fan-out): legacy un-capped path. Returns the calling instrument's
   own DEF 14A accessions; the parser-side cap-helper bypasses
   CIK-missing accessions (§3) so we keep legacy tombstone behaviour.

### 2.6.0 Sibling-aware target CIK resolution

```python
def _resolve_target_cik_for_discovery(
    conn: psycopg.Connection[tuple], *, instrument_id: int
) -> str | None:
    """Resolve the target CIK for per-instrument discovery.

    Two-step lookup so the cap applies even when the calling
    instrument has no instrument_sec_profile row but a share-class
    sibling does (per #1102 PR-B fan-out is still pending — only one
    sibling per accession typically carries the profile).

    Returns None only when NO sibling sharing any DEF 14A accession
    with the calling instrument has a profile (genuine CIK-MISSING).
    """
    with conn.cursor() as cur:
        # Step 1: direct profile lookup. Codex 1e — guard against a
        # profile row with NULL cik (legal schema state for some
        # rescue rows); without `cik IS NOT NULL` the `str(row[0])`
        # would return the literal "None" and skip the sibling
        # fallback, leaking the cap.
        cur.execute(
            """
            SELECT cik
            FROM instrument_sec_profile
            WHERE instrument_id = %s AND cik IS NOT NULL
            LIMIT 1
            """,
            (instrument_id,),
        )
        row = cur.fetchone()
        if row is not None:
            return str(row[0])

        # Step 2: sibling-via-filing_events fallback. `ORDER BY
        # fe_sib.instrument_id LIMIT 1` is the deterministic
        # tie-break (mirrors the DISTINCT ON ordering used by the
        # universe-wide discovery CTE at §2.6.2). Codex 1d finding #2.
        cur.execute(
            """
            SELECT isp.cik
            FROM filing_events fe_self
            JOIN filing_events fe_sib
                ON fe_sib.provider_filing_id = fe_self.provider_filing_id
               AND fe_sib.provider = 'sec'
               AND fe_sib.instrument_id <> fe_self.instrument_id
            JOIN instrument_sec_profile isp ON isp.instrument_id = fe_sib.instrument_id
            WHERE fe_self.instrument_id = %s
              AND fe_self.provider = 'sec'
              AND fe_self.filing_type = ANY(%s)
              AND isp.cik IS NOT NULL
            ORDER BY fe_sib.instrument_id
            LIMIT 1
            """,
            (instrument_id, list(_DEF14A_FORM_TYPES)),
        )
        row = cur.fetchone()
        return str(row[0]) if row is not None else None
```

The helper `def14a_within_cap` uses the SAME two-step resolution so
discovery and per-row gate agree on the target CIK.

```sql
-- per-instrument branch, CIK-known
WITH per_accession AS (
    SELECT DISTINCT ON (fe.provider_filing_id)
        fe.provider_filing_id, fe.filing_date, fe.filing_type,
        fe.primary_document_url, fe.filing_event_id
    FROM filing_events fe
    JOIN instrument_sec_profile isp ON isp.instrument_id = fe.instrument_id
    WHERE fe.provider = 'sec'
      AND fe.filing_type = ANY(%(forms)s)
      AND isp.cik = %(target_cik)s
    ORDER BY fe.provider_filing_id, fe.instrument_id
),
ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY filing_type
            ORDER BY filing_date DESC, provider_filing_id DESC
        ) AS rank_within_form
    FROM per_accession
)
SELECT fe.provider_filing_id, fe.instrument_id, fe.filing_date,
       fe.primary_document_url
FROM ranked r
JOIN filing_events fe
    ON fe.provider_filing_id = r.provider_filing_id
   AND fe.provider = 'sec'
   AND fe.primary_document_url IS NOT NULL
LEFT JOIN def14a_ingest_log log
    ON log.accession_number = r.provider_filing_id
WHERE log.accession_number IS NULL
  AND (r.filing_type <> 'DEF 14A' OR r.rank_within_form <= %(cap)s)
  AND fe.instrument_id = %(iid)s
ORDER BY fe.filing_date DESC, fe.filing_event_id DESC
LIMIT %(limit)s
```

Pre-scoping by `isp.cik = %(target_cik)s` in the inner CTE bounds the
rank universe to the filer's history. Single PARTITION on filing_type
(CIK is constant within the CTE).

### 2.6.1 Calling instrument missing accession's filing_events row

A sibling fan-out gap can mean the calling instrument doesn't have a
filing_events row for an accession (the ingest only landed for the
profile-bearing sibling, per #1102 PR-B pending). Pre-cap behaviour:
the per-instrument selector returned zero rows for that accession. The
cap doesn't change that — `fe.instrument_id = %(iid)s` in the outer
JOIN still filters to the calling instrument's view. Acceptable: the
per-instrument selector's contract is "accessions visible to this
instrument", not "accessions belonging to the issuer".

**Test scope clarification** (Codex 1c finding #2): the §6.2 "both
siblings see the same 2 accessions" claim holds only when both
siblings have filing_events rows for the accession (i.e. fan-out
complete). When fan-out is incomplete, sibling-without-rows sees zero
— matching legacy per-instrument behaviour. The cap doesn't promote
sibling-B to see an accession it never had a filing_events row for;
that would change the legacy contract. Test §6.2 must seed both
sibling rows to assert the "same 2 accessions" invariant, and a
separate test asserts the asymmetric-fan-out legacy preservation.

### 2.6.2 Universe-wide branch

```sql
-- universe-wide branch
WITH per_accession AS (
    SELECT DISTINCT ON (fe.provider_filing_id)
        fe.provider_filing_id, fe.instrument_id, fe.filing_date,
        fe.filing_type, fe.primary_document_url, fe.filing_event_id,
        isp.cik
    FROM filing_events fe
    LEFT JOIN instrument_sec_profile isp ON isp.instrument_id = fe.instrument_id
    WHERE fe.provider = 'sec'
      AND fe.filing_type = ANY(%(forms)s)
    ORDER BY fe.provider_filing_id, (isp.cik IS NULL), fe.instrument_id
),
ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY cik, filing_type
            ORDER BY filing_date DESC, provider_filing_id DESC
        ) AS rank_within_form
    FROM per_accession
)
SELECT r.provider_filing_id, r.instrument_id, r.filing_date,
       r.primary_document_url
FROM ranked r
LEFT JOIN def14a_ingest_log log
    ON log.accession_number = r.provider_filing_id
WHERE log.accession_number IS NULL
  AND r.primary_document_url IS NOT NULL
  AND (r.filing_type <> 'DEF 14A'
       OR r.cik IS NULL
       OR r.rank_within_form <= %(cap)s)
ORDER BY r.filing_date DESC, r.filing_event_id DESC
LIMIT %(limit)s
```

**Universe-wide returns directly from `ranked` — no outer JOIN back to
`filing_events`.** Codex 1b finding #1: an outer JOIN without
`instrument_id` scoping would fan one accession to N sibling rows,
duplicating accessions and burning the LIMIT slot. The `per_accession`
CTE already de-duped to one representative row per accession (the
profile-bearing sibling, lowest iid tie-break) — use it directly.

### 2.6.3 Conflicting profile-bearing CIKs (Codex 1b finding #3)

If two profile-bearing siblings for the same accession have different
`isp.cik` values, the DISTINCT ON deterministically keeps the
lowest-iid sibling. The cap is then computed against THAT sibling's
CIK. This is consistent (helper and discovery use the same
DISTINCT ON ordering, so both pick the same kept row), but it is a
data-integrity violation per #1102 "CIK = entity".

PR5 does NOT add conflict detection in the SQL. The CIK column on
`external_identifiers` already enforces partial-unique constraints
(sql/143). A conflict at the `instrument_sec_profile` level (the table
PR5 reads from) would already be a regression of #1102 invariants and
out of scope here. The plan records this trade-off so a future audit
ticket can revisit.

Optional: add a low-cost sanity log in the helper when it detects
`COUNT(DISTINCT cik) > 1` for the queried accession (single
GROUP BY query, no per-row cost on the cap path). Defer to a follow-up
if Codex 1c flags it again — keeps PR5 scope tight.

## 3. Constants + helper

`app/services/def14a_ingest.py`:

```python
DEF14A_LATEST_PER_FILER_CAP: int = 2
DEF14A_PRIMARY_FORM_TYPE: str = "DEF 14A"


def def14a_within_cap(
    conn: psycopg.Connection[Any],
    *,
    accession_number: str,
    instrument_id: int,
) -> bool:
    """True iff `accession_number` is allowed to ingest under the
    latest-2-primary-proxies-per-filer cap (#1233 §4.7).

    Returns True for:
    - non-DEF-14A primary form (DEFA14A, DEFR14A, DEFM14A) — supplemental.
    - DEF 14A primary AND within rank %(cap)s for its issuer CIK.
    - DEF 14A primary AND issuer CIK is missing (CIK-MISSING
      tombstone path handled downstream).

    Returns False for:
    - DEF 14A primary AND rank > cap for its issuer CIK.
    - Accession not present in `filing_events` (out-of-corpus —
      safe default; manifest dispatched something we cannot rank).
      Codex 1a finding #8.

    SQL mirrors the discovery CTE so a single rank source-of-truth
    is exercised at both discovery + per-row gate.
    """
```

### 3.1 Out-of-corpus default

The helper returns **False (refuse)** when the accession is absent from
`filing_events` (Codex 1a finding #8). The manifest worker is itself a
chokepoint — a "pass on unknown" default would let an unranked accession
through. Manifest rows whose `filing_events` source row is missing are
data-integrity anomalies that should tombstone, not fetch.

## 4. Chokepoint coverage

### 4.1 Legacy discovery path

`app/services/def14a_ingest.py::discover_pending_def14a`. Both branches
get the CTE described in §2.6.

| Branch | Current state | Fix |
| --- | --- | --- |
| `:202-223` per-instrument | No cap; targeted `fe.instrument_id=%(iid)s` | Replace with §2.6 per-instrument CTE shape |
| `:224-249` universe-wide | `DISTINCT ON (provider_filing_id)` only | Replace with §2.6 universe shape (no `fe.instrument_id` outer filter) |

Helper-bound: both branches pass `DEF14A_LATEST_PER_FILER_CAP` as
`%(cap)s` param + `_DEF14A_FORM_TYPES` as `%(forms)s`. No magic literal in
the SQL.

### 4.2 Manifest-worker parser path

`app/services/manifest_parsers/def14a.py::_parse_def14a`. Pre-fetch gate.

**Explicit gate order** (Codex 1a finding #4 + #5):

1. Missing URL → tombstone (line 154 — unchanged).
2. PRE 14A → tombstone (line 173 — unchanged).
3. Missing `instrument_id` → tombstone (line 183 — unchanged).
4. **NEW: cap check via `def14a_within_cap`** (inserted at line ~193,
   immediately before `fetch_document_text` at line 196).

Cap check requires non-NULL `instrument_id` so it MUST follow the
missing-iid guard. The PRE 14A check at line 173 fires before missing-iid
so PRE 14A with no iid still tombstones as PRE policy — matching legacy
behaviour. Form-type isn't passed to the helper because the helper reads
`filing_type` from `filing_events` itself (single source of truth).

```python
if not def14a_within_cap(
    conn,
    accession_number=accession,
    instrument_id=instrument_id,
):
    logger.debug(
        "def14a manifest parser: accession=%s exceeds latest-2-primary cap; tombstoning",
        accession,
    )
    return ParseOutcome(
        status="tombstoned",
        parser_version=_PARSER_VERSION_DEF14A,
        error="latest-N primary cap",
    )
```

No `def14a_ingest_log` row is written — capped rows are not "attempted"
in the ingest-log sense; they're refused upstream. Manifest row
transitions to `success` with the tombstoned outcome (mirrors PR4's
manifest-worker tombstone semantics).

Rewash via parser-version bump on a pre-fetch tombstone is NOT possible
because no `filing_raw_documents` row exists; operator revival path is
`POST /jobs/sec_rebuild/run` source reset (mirrors PR4 §6 trade-off).

### 4.3 Rewash path — rescue is capped, happy-path is not

`app/services/rewash_filings.py::_apply_def14a`. Codex 1a finding #7
caught the leak: the rescue fallback (lines 462-475) writes NEW typed
rows for accessions that previously had only an ingest_log entry. That
IS an ingest-side write and the cap MUST apply.

The happy-path (line 449-460 — typed rows already exist) is NOT capped:
re-parsing existing populated rows is the spec §6.3 "existing rows
untouched" carve-out (we're updating fields, not creating new typed-row
entries for new accessions).

Implementation:

```python
def _apply_def14a(conn, raw_doc):
    # ... existing typed-row lookup at lines 449-460 ...
    had_existing_rows = row is not None

    if row is None:
        # ... existing fallback to ingest_log + filing_events ...
        # NEW: cap check on the rescue path before writing typed rows
        if row is not None:
            iid_for_cap = int(row[1])  # instrument_id from fallback row
            if not def14a_within_cap(
                conn,
                accession_number=raw_doc.accession_number,
                instrument_id=iid_for_cap,
            ):
                logger.debug(
                    "def14a rewash: accession=%s rescue path blocked by latest-N cap",
                    raw_doc.accession_number,
                )
                return False
    if row is None:
        return False
    # ... rest of function unchanged ...
```

`return False` matches the existing "rewash isn't a first-time ingester"
contract (line 428-430). Operator-visible: rescue cohort that's out-of-
cap stays on the old parser_version. That's the correct behaviour —
they're scheduled for the pre-wipe + clean re-run, not the rescue sweep.

## 5. Lint guard

`scripts/check_def14a_cap.sh`. Mirrors `scripts/check_form4_retention.sh`
shape from PR4. Block-parsing via `awk` (BSD vs GNU `grep -P` portability).

Four invariants — all enforce **block-level**, not file-level, parity.

**A. Repo-wide DEF14A discovery block parity** (Codex 1a finding #9 +
Codex 1b finding #4 + Codex 1c finding #3 + Codex 1d finding #1):

   **Signal detection** — a SQL block is a DEF14A chokepoint if EITHER:

   1. The block body contains a DEF14A form-type literal:
      `'DEF 14A'` / `'DEFA14A'` / `'DEFM14A'` / `'DEFR14A'`.
   2. The block body contains `filing_type = ANY(%(forms)s)`
      (parameterised form) AND the surrounding ±20 lines of Python
      source reference `_DEF14A_FORM_TYPES` (the parameter binding).

   Both shapes appear in the codebase — the discovery CTEs use the
   parameterised `ANY(%(forms)s)` with `_DEF14A_FORM_TYPES` passed as
   the `forms` param. The lint catches both.

   The SAME block MUST contain ONE of three block-level compliance
   markers:

   1. SQL predicate `rank_within_form <= %(cap)s` (the inline rank CTE
      pattern used by `discover_pending_def14a`).
   2. A `def14a_within_cap(` Python call within ±5 lines of the SQL
      block boundary (the per-row Python-gate pattern, e.g. for paths
      that fetch a candidate then check the cap before writing).
   3. Single-line SQL comment `-- DEF14A-CAP-EXEMPT: <reason>` inside
      the block (explicit operator override; must include a reason
      string).

   Block extraction: awk parses Python triple-quoted SQL literals
   (`"""..."""` and `'''...'''`). For each block whose body contains a
   DEF14A signal, the awk pass records the start + end line numbers.
   Compliance check then scans (start-5 .. end+5) lines for any of
   the three markers above. Files where any chokepoint block has zero
   compliance markers fail.

   File-level presence is NOT enough: a file with one compliant block
   plus one uncapped block must fail. The awk pass tracks per-block
   booleans, not per-file aggregates.

**B. `app/services/def14a_ingest.py` per-block parity** (subset of A,
   exercised independently for fast regression detection):

   Every DEF14A-signal SQL block in `discover_pending_def14a` MUST
   contain ONE of the three compliance markers from invariant A.
   This explicitly mirrors A's "compliance marker count == chokepoint
   count" rule, so the per-instrument CIK-truly-missing legacy
   branch (which has no rank predicate by design — the cap relies on
   the parser-side helper to gate CIK-missing accessions) carries an
   inline `-- DEF14A-CAP-EXEMPT: CIK-MISSING legacy uncapped` SQL
   comment. Without that marker, the block fails parity.

**C. `app/services/manifest_parsers/def14a.py` pre-fetch placement**
   (Codex 1a finding #10):

   In `_parse_def14a` function body: the line number of the first
   `def14a_within_cap(` call MUST be > the line number of the
   missing-`instrument_id` check (so iid is non-None) AND < the line
   number of the first `provider.fetch_document_text(` call. Awk
   tracks line numbers; either ordering inversion fails.

**D. `app/services/rewash_filings.py::_apply_def14a` rescue-branch
   placement** (Codex 1b finding #5):

   Extract `_apply_def14a` function body. Locate the rescue-fallback
   block: the second `LEFT JOIN def14a_ingest_log` query that runs
   when `had_existing_rows` is false (between the `if row is None:`
   re-query and the parse step). The `def14a_within_cap(` call MUST
   appear AFTER `had_existing_rows = row is not None` AND AFTER the
   rescue-fallback SELECT that re-binds `row`, AND BEFORE the
   `parse_beneficial_ownership_table(` call.

   The lint asserts:
   1. Exactly one `def14a_within_cap(` call in `_apply_def14a`.
   2. Its line > the rescue-fallback SELECT's `SELECT log.issuer_cik`
      anchor line.
   3. Its line < `parse_beneficial_ownership_table(` line.

   Placement-aware: a misplaced cap call on the happy-path branch
   (before `had_existing_rows`) OR after the parse fails.

Wired into `.githooks/pre-push`.

## 6. Tests

New file `tests/test_def14a_latest_n_cap.py`. Mirrors PR4's
`test_insider_transactions_retention_cap.py` shape.

### 6.1 Helper unit tests (`def14a_within_cap`)

- 5 `DEF 14A` accessions for one CIK, ranked by `filing_date DESC`.
  Top-2 pass, bottom-3 fail.
- 5 `DEFA14A` for one CIK: all 5 pass (supplemental form uncapped).
- Mixed: 3 DEF 14A + 2 DEFA14A in same year. Top-2 DEF 14A pass; bottom
  DEF 14A fails; both DEFA14As pass.
- Tie-break: same `filing_date`, different `provider_filing_id` → higher
  `provider_filing_id` ranks higher.
- CIK-missing (no `instrument_sec_profile` row): helper returns `True`
  regardless of rank or form.
- Share-class siblings sharing CIK: 5 DEF 14A × 2 siblings = 5 distinct
  accessions ranked (not 10). Top-2 pass for both siblings; same set.
- Profile-only-on-one-sibling: rank source is the profile-bearing
  sibling; calling instrument may not have profile but its sibling does
  → cap applies. Verifies §2.2 fix.
- **NULL-cik direct profile + sibling fallback** (Codex 1e regression
  test): seed calling instrument with `instrument_sec_profile.cik IS
  NULL` and a sibling with non-NULL CIK → §2.6.0 step 1 skips the NULL
  row (due to `WHERE cik IS NOT NULL`), step 2 finds the sibling's
  CIK, cap applies.
- Accession not in `filing_events`: helper returns `False` (out-of-corpus
  refusal — §3.1).

### 6.2 Discovery query (`discover_pending_def14a`)

- Universe-wide: seed 5 DEF 14A across 2 CIKs, universe call → returns 4
  (2 per CIK). Bottom 6 not returned.
- Universe-wide with DEFA14A mixed: 3 DEF 14A + 2 DEFA14A → all 5
  returned (DEF 14A cap = 2; DEFA14A unconditional).
- Per-instrument: same 2-per-CIK set returned, scoped to the calling
  instrument's filing_events row (Codex finding #1 regression test).
- Per-instrument with sibling fan-out: sibling A and sibling B both
  call → both see the same 2 accessions, each bound to its own
  filing_events row.
- Existing `def14a_ingest_log` row excludes accession even if within
  rank — but the rank is computed across ALL accessions including
  logged ones. Verify: log the top 2 of 5, query → returns 0 (not the
  3rd) (Codex finding §2.5 regression test).
- Missing-URL accession at rank 1/2: latest-2-with-URLs NOT promoted;
  query returns 0 for that filer (Codex finding #3 regression test).
- CIK-missing instrument: all its primary DEF 14A accessions returned
  (uncapped path).
- DISTINCT-ON-prefers-profile-bearing-sibling: seed 1 accession with 2
  filing_events rows (one profile-bearing, one not) → rank computed
  using the profile sibling's CIK (Codex finding #2 regression test).

### 6.3 Parser pre-fetch gate (`_parse_def14a`)

- Cap-bound DEF 14A accession (rank 3+ for its CIK): parser returns
  tombstoned, NO HTTP call, NO `filing_raw_documents` write, NO
  `def14a_ingest_log` row.
- Within-cap DEF 14A (rank 1 or 2): parser proceeds (existing happy-path
  test should still pass).
- CIK-missing DEF 14A: parser bypasses cap (proceeds to existing
  CIK-MISSING tombstone path).
- DEFA14A at any rank: cap bypassed (supplemental form passes
  unconditionally).
- Out-of-corpus accession (no filing_events row): parser tombstones
  (helper returns False).
- Cap check fires AFTER PRE 14A + missing-iid checks (Codex finding #4 +
  #5 regression test).

### 6.4 Rewash path (`_apply_def14a`)

- Happy-path: accession with existing typed rows → cap NOT consulted
  (rewash proceeds even for rank-3+).
- Rescue path on within-cap accession: rewash writes typed rows.
- Rescue path on out-of-cap accession: rewash returns False, no typed
  rows written (Codex finding #7 regression test).

### 6.5 Lint guard meta-tests

`tests/test_check_def14a_cap_lint.py` — verify:

- Adding a new `filing_type = ANY(...)` block in a fresh file without
  `def14a_within_cap(` or `%(cap)s` predicate → guard fails.
- Removing the `def14a_within_cap(` call from `_parse_def14a` → guard
  fails.
- Moving the `def14a_within_cap(` call to AFTER `fetch_document_text(`
  in `_parse_def14a` → guard fails (placement check).
- Removing the cap call from `_apply_def14a` rescue branch → guard fails.

## 7. Spec amendment

Update spec §4.7:

- Reword "**Ingest depth cap**: **latest 2 proxies per filer** (current
  + one prior for change tracking) at the parser. Older filings not
  fetched." to clarify: **latest 2 PRIMARY `DEF 14A` accessions** per
  filer; `DEFA14A` / `DEFR14A` / `DEFM14A` uncapped.
- Note Codex 1a finding #6 rationale: supplemental forms shouldn't evict
  prior-year primary; uncapping them is the cleaner shape.

Update spec §7:

- PR5 status: "**PR5 — SHIPPED.** DEF 14A latest-2-primary-proxies cap
  at discovery + parser + rewash-rescue chokepoints. NUMERIC overflow
  #1228 already folded (#1236)."

Update spec §12 handover: next session picks up PR6 (13F-HR 8-quarter
cap, already cohort-bounded by #1010).

## 8. Test plan

- [ ] `uv run ruff check .` clean
- [ ] `uv run ruff format --check .` clean
- [ ] `uv run pyright` clean
- [ ] `bash scripts/check_def14a_cap.sh` clean
- [ ] `bash scripts/check_form4_retention.sh` clean (PR4 invariant survives)
- [ ] `bash scripts/check_instruments_inserts.sh` clean (PR1 invariant)
- [ ] `uv run pytest tests/test_def14a_latest_n_cap.py` — all green
- [ ] `uv run pytest tests/test_check_def14a_cap_lint.py` — all green
- [ ] `uv run pytest tests/test_def14a_ingest.py` — existing tests still green
- [ ] `uv run pytest tests/test_manifest_parser_def14a.py` — existing tests
  still green

## 9. Trade-offs

- **Latest-2-primary is operator-visible**: pre-rank-2 DEF 14A accessions
  stop being ingested. Operator widens via `DEF14A_LATEST_PER_FILER_CAP`
  if archaeology becomes a need.
- **Supplemental forms uncapped**: a filer with 10 DEFA14As in one year
  still ingests all 10. Acceptable because (a) DEFA14As are rare,
  (b) capping them risks evicting a prior-year primary from the window,
  (c) DEFA14As typically supplement the current cycle's primary —
  losing them would damage current-state holders rollups.
- **Out-of-corpus refusal**: manifest rows without `filing_events` source
  tombstone (safe-default). Operator path to revive: rebuild
  filing_events for the accession via `POST /jobs/sec_rebuild/run` source
  reset, then re-enqueue the manifest row.
- **Manifest tombstones not revivable via parser-version rewash**:
  pre-fetch tombstones write no `filing_raw_documents` row (mirrors PR4).
- **Cumulative state post-wipe**: pre-rank-2 primaries' top-5-holders
  pies reset; the next clean ingest builds going forward from the
  latest-2 only. Spec §6.3 — operator-accepted.

## 10. Scope NOT in this PR

- Existing pre-cap rows are untouched (#1233 §6.3 — operator-driven
  pre-wipe is the single purge event).
- PRE 14A policy change — already tombstoned (manifest parser line 173).
- 13F-HR 8-quarter cap — PR6.
- `ownership_*_current` size audit — PR12.

## 11. Codex review gate

- **Codex 1a — DONE** (10 findings).
- **Codex 1b — DONE** (5 residual issues).
- **Codex 1c — DONE** (3 residual issues).
- **Codex 1d — DONE** (2 residual issues).
- **Codex 1e — DONE** (1 residual issue).
- **Codex 1f — DONE** (3 residual issues; this revision).
- **Codex 2** pre-push on branch diff (mandatory per #1208 cadence).
- Standard review-bot + CI cadence thereafter.
