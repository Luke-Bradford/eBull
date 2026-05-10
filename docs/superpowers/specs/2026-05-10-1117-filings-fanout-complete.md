# #1117 PR-B fan-out — complete end-to-end design (v2 post-Codex)

Date: 2026-05-10
Status: spec drafted v2, addressing Codex round-1 findings.

## Codex round-1 deltas applied (4 BLOCKING + 3 MEDIUM + 1 NIT)

1. **BLOCKING — eight_k_filings/insider_filings child FKs.** PK relaxation
   on those tables would cascade through eight_k_items / eight_k_exhibits
   / insider_filers / insider_transaction_footnotes / insider_transactions
   FK rewrites and force per-sibling duplication of entity-level
   document detail. **Pivoting to Design B**: keep those PKs at accession
   alone (entity-level), make `filing_events` the per-sibling bridge,
   route `WHERE f.instrument_id = X` reads through filing_events.
2. **BLOCKING — ON CONFLICT sweep.** Under Design B, eight_k_events.py
   ON CONFLICTs at :428 + :516, insider_transactions.py at :942 + :1310,
   insider_form3_ingest.py at :122 + :470 stay accession-keyed
   unchanged. No sweep needed for those tables.
3. **BLOCKING — def14a holdings UNIQUE shape.** Actual schema is
   `(accession_number, holder_name)`, not the
   `(instrument_id, accession, holder_name)` v1 spec assumed. Change
   the UNIQUE shape in sql/144 + ON CONFLICT in def14a_ingest.py:315
   so per-sibling fan-out creates distinct rows instead of
   instrument_id flap.
4. **BLOCKING — read-site audit.** Under Design B the JOINs Codex
   flagged on insider_filings/eight_k_filings (no FK fan-out) collapse
   1:1 again. New audit task: enumerate every `WHERE f.instrument_id`
   on insider_filings + eight_k_filings and route through filing_events
   bridge. Listed below.
5. **MEDIUM — DISTINCT ON LIMIT semantics.** Use inner CTE for accession
   dedup, outer ORDER BY filing_date DESC + LIMIT for priority.
6. **MEDIUM — manifest seeder.** Inner DISTINCT ON accession only;
   pick canonical sibling explicitly (lowest instrument_id) for
   instrument_id/CIK columns.
7. **MEDIUM — migration shape check.** Validate constraint shape via
   pg_constraint.contype + conkey columns, not name alone.
8. **NIT — sec_identity helper.** Normalise CIK input (strip + 10-pad);
   document ordering as deterministic-only, not semantic-primary.

## Context

#1102 PR-A (sql/143, merged 2026-05-10) made `external_identifiers`
allow N `(provider='sec', identifier_type='cik', identifier_value=X)`
rows pointing at distinct `instrument_id`s. Share-class siblings
(GOOG/GOOGL, BRK.A/BRK.B) co-bind a CIK without flap.

#1117 is the follow-up: every code path that resolves CIK → instrument
must fan out to ALL siblings. The bulk-ingester multimap shape change
(`dict[str, list[X]]`) is in this PR's first commit — covers
`sec_companyfacts_ingest` (financial_facts_raw), `sec_insider_dataset_ingest`
(ownership_insiders_observations), `sec_submissions_ingest`
(instrument_sec_profile + filing_events). The remaining work is
schema relaxation for filing_events + def14a_beneficial_holdings,
read-site routing through filing_events as bridge, candidate-selector
DISTINCT ON dedup, per-filing-parser fan-out across siblings.

## Synthesised design principle (data-engineer + sec-edgar skills)

- **CIK identifies the issuer (entity).** Entity-level data —
  document body, items, exhibits, manifest tombstones, ingest logs,
  raw payloads — keyed at accession alone. One row per filing
  regardless of how many share classes the issuer has.
- **CUSIP identifies the security (per-share-class).** Per-instrument
  data — observations, beneficial holdings, fund positions,
  fundamentals — fans out per sibling.
- **filing_events is the per-instrument bridge.** "This filing
  applies to this instrument." Belongs at per-(accession, instrument).
  Reads filter `WHERE fe.instrument_id = X`; entity-level tables JOIN
  through it for sibling visibility.
- **eight_k_filings + insider_filings stay entity-level.** PK on
  accession; instrument_id column is "discovery sibling" denormalisation,
  informational only — reads use filing_events bridge instead.

## Schema migration — `sql/144_filings_fanout_per_instrument.sql`

```sql
-- 1. filing_events: relax UNIQUE to (provider, provider_filing_id, instrument_id).
--    Bridge table for per-sibling routing.
DO $$
DECLARE
    has_correct_shape BOOLEAN;
BEGIN
    SELECT EXISTS(
        SELECT 1 FROM pg_constraint c
        WHERE c.conname = 'uq_filing_events_provider_unique'
          AND c.contype = 'u'
          AND c.conrelid = 'filing_events'::regclass
          AND (
              SELECT array_agg(a.attname ORDER BY array_position(c.conkey, a.attnum))
              FROM pg_attribute a
              WHERE a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey)
          ) = ARRAY['provider', 'provider_filing_id', 'instrument_id']::name[]
    ) INTO has_correct_shape;

    IF NOT has_correct_shape THEN
        ALTER TABLE filing_events
            DROP CONSTRAINT IF EXISTS uq_filing_events_provider_unique;
        ALTER TABLE filing_events
            ADD CONSTRAINT uq_filing_events_provider_unique
                UNIQUE (provider, provider_filing_id, instrument_id);
    END IF;
END$$;

-- 2. def14a_beneficial_holdings: relax UNIQUE to per-(instrument, accession, holder).
--    Allows share-class siblings to each carry their own holdings rows.
DO $$
DECLARE
    has_correct_shape BOOLEAN;
BEGIN
    SELECT EXISTS(
        SELECT 1 FROM pg_indexes
        WHERE schemaname = current_schema()
          AND tablename = 'def14a_beneficial_holdings'
          AND indexname = 'uq_def14a_holdings_instrument_accession_holder'
    ) INTO has_correct_shape;

    IF NOT has_correct_shape THEN
        DROP INDEX IF EXISTS uq_def14a_holdings_accession_holder;
        CREATE UNIQUE INDEX uq_def14a_holdings_instrument_accession_holder
            ON def14a_beneficial_holdings (instrument_id, accession_number, holder_name);
    END IF;
END$$;
```

eight_k_filings + insider_filings stay PK=accession. Their child
tables (items, exhibits, filers, footnotes, transactions) keep their
accession-keyed FKs — entity-level invariants preserved.

## Code change 1 — filing_events ON CONFLICT (3 prod sites + 3 test sites)

`app/services/filings.py:449` (`_upsert_filing_event`),
`app/services/filings.py:499` (`_upsert_filing`),
`app/services/fundamentals.py:2255`:

Append `, instrument_id` to conflict target. DO UPDATE clauses
unchanged (don't touch instrument_id; hit means already correct).

```python
ON CONFLICT (provider, provider_filing_id, instrument_id) DO UPDATE SET
    filing_date          = EXCLUDED.filing_date,
    filing_type          = EXCLUDED.filing_type,
    source_url           = EXCLUDED.source_url,
    primary_document_url = EXCLUDED.primary_document_url
```

Test fixtures sweep: `tests/test_def14a_drill.py:425`,
`tests/test_rewash_filings.py:1514`, `tests/test_def14a_ingest.py:119`.
Same mechanical change.

## Code change 2 — def14a_beneficial_holdings ON CONFLICT

`app/services/def14a_ingest.py:315`:

Change conflict target from `(accession_number, holder_name)` to
`(instrument_id, accession_number, holder_name)`. Drop
`instrument_id = EXCLUDED.instrument_id` from DO UPDATE SET (now in
conflict target — hit means already correct).

```python
ON CONFLICT (instrument_id, accession_number, holder_name) DO UPDATE SET
    issuer_cik       = EXCLUDED.issuer_cik,
    holder_role      = EXCLUDED.holder_role,
    shares           = EXCLUDED.shares,
    percent_of_class = EXCLUDED.percent_of_class,
    as_of_date       = EXCLUDED.as_of_date,
    fetched_at       = NOW()
RETURNING (xmax = 0) AS inserted
```

## Code change 3 — siblings helper (new module)

`app/services/sec_identity.py`:

```python
"""Shared helpers for resolving SEC issuer CIKs to instrument siblings.

Per #1102, multiple instruments may share an SEC CIK (share-class
siblings GOOG/GOOGL, BRK.A/BRK.B). Per-filing parsers must fan out
their per-instrument writes across all siblings.
"""

from __future__ import annotations

from typing import Any

import psycopg


def siblings_for_issuer_cik(
    conn: psycopg.Connection[Any], cik: str
) -> list[int]:
    """Return all instrument_ids sharing this issuer CIK.

    Ordering is deterministic (instrument_id ASC), not semantic.
    Callers that need to fan out per-instrument writes should iterate
    the full list. Callers that must pick a single canonical sibling
    for entity-level row writes should choose by explicit policy
    (e.g. instruments.is_primary_listing) rather than relying on
    this ordering.

    The CIK is normalised (strip + zero-pad to 10 digits) before
    lookup; callers may pass either form.
    """
    cik_padded = str(cik).strip().zfill(10)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT instrument_id
            FROM external_identifiers
            WHERE provider = 'sec'
              AND identifier_type = 'cik'
              AND identifier_value = %s
            ORDER BY instrument_id
            """,
            (cik_padded,),
        )
        return [int(r[0]) for r in cur.fetchall()]
```

## Code change 4 — eight_k_filings reads route through filing_events

[app/services/eight_k_events.py:578](../../../app/services/eight_k_events.py#L578) — change `WHERE f.instrument_id = %s` to filing_events bridge:

```sql
SELECT
    f.accession_number, f.document_type, f.is_amendment,
    f.date_of_report, f.reporting_party,
    f.signature_name, f.signature_title, f.signature_date,
    f.primary_document_url
FROM eight_k_filings f
WHERE f.is_tombstone = FALSE
  AND EXISTS (
      SELECT 1 FROM filing_events fe
      WHERE fe.provider_filing_id = f.accession_number
        AND fe.provider = 'sec'
        AND fe.instrument_id = %s
        AND fe.filing_type IN ('8-K', '8-K/A')
  )
ORDER BY f.date_of_report DESC NULLS LAST, f.fetched_at DESC
LIMIT %s
```

[app/services/capabilities.py:175](../../../app/services/capabilities.py#L175) — same pattern:

```python
("eight_k", "sec_edgar"): (
    "SELECT EXISTS(SELECT 1 FROM filing_events fe "
    "WHERE fe.provider = 'sec' "
    "AND fe.filing_type IN ('8-K', '8-K/A') "
    "AND fe.instrument_id = %s)"
)
```

eight_k_filings.instrument_id column stays — repurposed as "discovery
sibling, informational only". Document the deprecation in a comment.

## Code change 5 — insider_filings reads route through filing_events

Per Codex audit, the affected sites with direct
`WHERE i.instrument_id = X` filter (NOT through observations) are:

- [app/api/instruments.py:2205, 2216, 2228](../../../app/api/instruments.py) — insider summary panels.
- [app/services/ownership_observations_sync.py:174, 232](../../../app/services/ownership_observations_sync.py) — backfill walk.
- [app/services/rewash_filings.py:299, 370](../../../app/services/rewash_filings.py) — rewash queries.
- [app/services/insider_form3_ingest.py:139, 768, 801](../../../app/services/insider_form3_ingest.py) — backfill.

Most of these JOIN `insider_filings` then filter via downstream
`WHERE instrument_id = X` on observations or transactions. Audit each
to verify the per-sibling filter happens at the per-instrument table,
NOT at insider_filings. Where it currently filters at insider_filings,
re-route to filing_events:

```sql
JOIN insider_filings f ON f.accession_number = obs.accession_number
WHERE EXISTS (
    SELECT 1 FROM filing_events fe
    WHERE fe.provider_filing_id = f.accession_number
      AND fe.provider = 'sec'
      AND fe.filing_type IN ('4', '4/A', '3', '3/A', '5', '5/A')
      AND fe.instrument_id = %s
)
```

Per-site audit table (filled in 2026-05-10):

| Site | Current filter | Action |
|---|---|---|
| `app/api/instruments.py:2206` | `WHERE h.instrument_id` on `insider_initial_holdings` | safe — per-instrument table |
| `app/api/instruments.py:2217` | `WHERE instrument_id` on `insider_filings` | **FIX** — bridge via filing_events |
| `app/api/instruments.py:2229` | `i.instrument_id` on `insider_filings` JOIN | **FIX** — bridge via filing_events |
| `app/services/ownership_drillthrough.py:292` | `WHERE t.instrument_id` on `insider_transactions` | safe — per-row table is observation-shaped |
| `app/services/ownership_drillthrough.py:304` | `WHERE instrument_id` on `insider_filings` | **FIX** — bridge via filing_events |
| `app/services/ownership_drillthrough.py:318` | `i.instrument_id` on `insider_filings` JOIN | **FIX** — bridge via filing_events |
| `app/services/ownership_drillthrough.py:353` | `WHERE h.instrument_id` on `insider_initial_holdings` | safe |
| `app/services/ownership_drillthrough.py:365` | `WHERE instrument_id` on `insider_filings` | **FIX** — bridge via filing_events |
| `app/services/ownership_drillthrough.py:379` | `i.instrument_id` on `insider_filings` JOIN | **FIX** — bridge via filing_events |
| `app/services/ownership_observations_sync.py:174, 232` | JOIN by accession; per-row processing | safe |
| `app/services/rewash_filings.py:299, 370` | JOIN/SELECT for rewash; observation-keyed | safe (per-row processing handles fan-out implicitly) |
| `app/services/insider_form3_ingest.py:139, 768, 801` | candidate selectors (covered by code change 8) + observation walk | candidate dedup; rewash queries safe |

Six insider/eight_k bad sites total (3 + 2 in instruments.py/eight_k_events.py + 6 in ownership_drillthrough). All get the EXISTS bridge predicate. Observation-keyed and per-row paths stay unchanged.

## Code change 6 — DEF 14A parser fan-out

[app/services/def14a_ingest.py](../../../app/services/def14a_ingest.py)
parses one DEF 14A at a time and writes
`def14a_beneficial_holdings` rows + `def14a_ingest_log`.

Post-fan-out:
- `def14a_ingest_log` stays PK=accession (entity-level tombstone — one
  parse attempt logged regardless of siblings).
- `def14a_beneficial_holdings` writes wrap in
  `for instrument_id in siblings_for_issuer_cik(conn, cik):` —
  each sibling gets its own rows under the new
  `(instrument_id, accession, holder_name)` UNIQUE.
- `record_def14a_observation()` + `refresh_def14a_current(instrument_id)`
  fan out across siblings.
- `record_esop_observation()` + `refresh_esop_current(instrument_id)`
  fan out across siblings (ESOP holdings live on def14a payloads).

## Code change 7 — Form 4/3 per-filing parser fan-out

[app/services/insider_transactions.py](../../../app/services/insider_transactions.py)
+ [app/services/insider_form3_ingest.py](../../../app/services/insider_form3_ingest.py).

`insider_filings` row stays PK=accession (entity-level — one filing
object). `insider_transactions` / `insider_initial_holdings` rows
stay PK=(accession, txn_row_num) / (accession, row_num) — entity-level
per-transaction details.

Per-instrument fan-out happens at the observation layer:
- `record_insider_observation()` + `refresh_insiders_current(instrument_id)`
  fan out across siblings.

The `insider_filings.instrument_id` column stays (informational —
"discovery sibling"). Same pattern as eight_k_filings.

## Code change 8 — candidate-selector DISTINCT ON dedup (inner CTE)

Six per-filing candidate-selector sites that LEFT JOIN entity-level
log/marker tables on accession alone — without dedup they batch the
same accession N times post-fan-out, wasting LIMIT budget:

| File:line | Pattern | Fix |
|---|---|---|
| `app/services/def14a_ingest.py:194` | LEFT JOIN def14a_ingest_log ON accession_number | Inner CTE dedup |
| `app/services/eight_k_events.py:717` | LEFT JOIN eight_k_filings ON accession_number IS NULL | Inner CTE dedup |
| `app/services/insider_transactions.py:1404, 1459, 1614, 1646` | LEFT JOIN insider_filings ON accession_number IS NULL | Inner CTE dedup |
| `app/services/insider_form3_ingest.py:520, 569` | Same Form 3 variant | Inner CTE dedup |

Pattern (preserves LIMIT-by-recency semantics):

```sql
WITH per_accession AS (
    SELECT DISTINCT ON (fe.provider_filing_id)
        fe.instrument_id, fe.provider_filing_id, fe.primary_document_url,
        fe.filing_date, fe.filing_event_id
    FROM filing_events fe
    LEFT JOIN <log_or_marker> ekf
        ON ekf.accession_number = fe.provider_filing_id
    WHERE fe.provider = 'sec'
      AND fe.filing_type IN (...)
      AND ekf.accession_number IS NULL
    ORDER BY fe.provider_filing_id, fe.instrument_id  -- inner: dedup
)
SELECT instrument_id, provider_filing_id, primary_document_url
FROM per_accession
ORDER BY filing_date DESC, filing_event_id DESC      -- outer: priority
LIMIT %s
```

Inner ORDER BY locks dedup choice (`fe.instrument_id ASC` —
deterministic, parser fans out anyway so the choice is academic).
Outer ORDER BY preserves "newest first" for LIMIT priority.

## Code change 9 — manifest seeder dedup (sec_first_install_drain)

[app/jobs/sec_first_install_drain.py:104-130](../../../app/jobs/sec_first_install_drain.py).

`sec_filing_manifest.accession_number` is PK. With per-instrument
filing_events, the seeder's straight `SELECT FROM filing_events`
emits one row per (accession, instrument_id) pair. The downstream
INSERT collapses on PK, last-row-wins.

Fix: dedup on accession in the SELECT, pick canonical sibling
explicitly (lowest instrument_id) for the row's instrument_id /
issuer_cik columns. Source must use `map_form_to_source(form)` —
manifest's source CHECK rejects `'sec_submissions'`:

```sql
INSERT INTO sec_filing_manifest (
    accession_number, cik, form, filed_at,
    instrument_id, source, ingest_status, ...
)
SELECT DISTINCT ON (fe.provider_filing_id)
    fe.provider_filing_id,
    -- Resolve issuer CIK from external_identifiers. Existing code
    -- pattern (preserve verbatim): ORDER BY is_primary DESC,
    -- external_identifier_id ASC — handles non-primary-only mappings
    -- AND multi-primary edge if a sibling has stale duplicates.
    (SELECT identifier_value
       FROM external_identifiers
      WHERE provider = 'sec' AND identifier_type = 'cik'
        AND instrument_id = fe.instrument_id
      ORDER BY is_primary DESC, external_identifier_id ASC
      LIMIT 1) AS cik,
    fe.filing_type,
    fe.filing_date,
    fe.instrument_id,                  -- discovery sibling (canonical)
    map_form_to_source(fe.filing_type) AS source,
    'pending',
    ...
FROM filing_events fe
WHERE fe.provider = 'sec'
  AND map_form_to_source(fe.filing_type) IS NOT NULL  -- skip unsupported forms
  AND NOT EXISTS (
      SELECT 1 FROM sec_filing_manifest m
      WHERE m.accession_number = fe.provider_filing_id
  )
ORDER BY fe.provider_filing_id, fe.instrument_id  -- canonical = lowest iid
ON CONFLICT (accession_number) DO NOTHING;
```

If `map_form_to_source` is a Python helper not a SQL function, the
seeder either (a) reads the rows in Python and partitions per-source
before INSERT or (b) embeds an inline `CASE WHEN fe.filing_type IN
('10-K', '10-K/A') THEN 'sec_10k' ...` map. (a) is the cleaner shape
since the existing form→source map already lives in
`app/services/sec_manifest.py`.

DISTINCT ON carve-out for targeted selectors: when the candidate
selector has `WHERE fe.instrument_id = %s`, only one sibling's rows
match the WHERE — no fan-out can multiply, dedup is unnecessary. The
inner-CTE pattern applies to **universe-wide** selectors only
(`WHERE fe.filing_type IN (...) AND ekf.accession_number IS NULL`
without instrument_id filter). Targeted backfill paths
(`fe.instrument_id = %s AND ekf.accession_number IS NULL`) skip
DISTINCT ON entirely.

`sec_filing_manifest.instrument_id` becomes "canonical sibling for
this filing"; the manifest worker / parsers fan out via
`siblings_for_issuer_cik(cik)` at parse time.

## Read-site audit (post-migration)

### filing_events (per-instrument post-migration)

All 28 read sites verified: filter by `WHERE fe.instrument_id = X`
in WHERE clause OR thread instrument_id through per-row processor.
Safe.

One genuine pre-existing edge: `app/services/rewash_filings.py:467,
777` — `JOIN filing_events ... LIMIT 1` without instrument filter
picks an arbitrary sibling. Pre-existing arbitrary-pick that surfaced
at #1102 PR-A. Add deterministic `ORDER BY fe.instrument_id ASC` so
LIMIT 1 is reproducible; document in commit message.

### eight_k_filings (entity-level, PK=accession unchanged)

- Read site `:578` — fixed in code change 4 (filing_events bridge).
- Read site `capabilities.py:175` — fixed in code change 4.
- Candidate selector `:717` — fixed in code change 8.

### insider_filings (entity-level, PK=accession unchanged)

- Per-site audit listed in code change 5.

### Other accession-keyed reads (cross-check)

- `def14a_ingest_log` PK=accession — unchanged shape, all reads safe.
- `n_port_ingest_log` PK=accession — unchanged.
- `sec_filing_manifest` PK=accession — fixed in code change 9.
- `filing_raw_documents` PK=(accession, kind) — unchanged.

## Test plan

### Existing fan-out tests (already in this PR's first commit)

- `tests/test_sec_companyfacts_ingest.py::test_share_class_siblings_both_receive_facts`
- `tests/test_sec_submissions_ingest.py::test_share_class_siblings_both_receive_filings_and_profile`
- `tests/test_sec_insider_dataset_ingest.py::test_share_class_siblings_both_receive_observations`

### New tests (this PR's second commit)

- `tests/test_def14a_ingest.py::test_share_class_siblings_both_receive_holdings_and_observations` — parse one DEF 14A; both siblings get def14a_beneficial_holdings rows + def14a_observations + esop_observations.
- `tests/test_eight_k_events.py::test_share_class_siblings_both_render_via_filing_events_bridge` — one 8-K parsed; eight_k_filings has one row (accession-keyed); both siblings see it via the read-side bridge query.
- `tests/test_insider_transactions.py::test_share_class_siblings_both_receive_observations_via_per_filing_path` — one Form 4 per-filing parse; both siblings get observations + refresh_current.
- `tests/test_filings.py::test_upsert_filing_per_instrument_idempotent` — call _upsert_filing twice with same (accession, instrument); one row. Twice with same accession + different instrument; two rows.
- `tests/test_sec_identity.py::test_siblings_for_issuer_cik_returns_all_siblings_deterministic` — helper unit test.
- Idempotency tests for each fan-out path (re-parse → no duplication).

### Migration test

- `tests/test_migration_144_filings_fanout.py` — apply migration cleanly + idempotently against post-#143 schema. Verify constraint shape via pg_constraint inspection.

## Verification matrix (DOD clauses 8-12)

Per CLAUDE.md ETL/parser/schema-migration clauses 8-12.

| Instrument | Class | Expected after sec_rebuild |
|---|---|---|
| AAPL | single | unchanged baseline (regression sentinel) |
| GOOG | share-class | filing_events ≥ N, def14a_beneficial_holdings ≥ J, ownership_insiders_observations ≥ L (per-instrument) |
| GOOGL | share-class | same row counts as GOOG |
| BRK.A | share-class | same |
| BRK.B | share-class | same |

SQL audit:

```sql
WITH siblings AS (
    SELECT instrument_id, symbol FROM instruments
    WHERE symbol IN ('AAPL', 'GOOG', 'GOOGL', 'BRK.A', 'BRK.B')
)
SELECT s.symbol,
       (SELECT COUNT(*) FROM filing_events                  WHERE instrument_id = s.instrument_id) AS filings,
       (SELECT COUNT(*) FROM def14a_beneficial_holdings     WHERE instrument_id = s.instrument_id) AS def14a,
       (SELECT COUNT(*) FROM ownership_insiders_observations WHERE instrument_id = s.instrument_id) AS obs
FROM siblings s ORDER BY s.symbol;
```

Both GOOG and GOOGL must return non-zero on every column.

Operator-visible verification (DOD clause 11):
- `/instruments/GOOG/ownership-rollup` AND `/instruments/GOOGL/ownership-rollup` both render with non-NULL totals + insider slice + def14a slice.
- `/instruments/GOOG/eight_k_filings` AND `/instruments/GOOGL/eight_k_filings` both return rows (via filing_events bridge).
- `/instruments/GOOG/insider_baseline` AND `/instruments/GOOGL/insider_baseline` both return rows.
- `/instruments/GOOG/def14a_holdings/drill` AND `/instruments/GOOGL/def14a_holdings/drill` both return rows.

Cross-source verify (DOD clause 9): GOOGL filing date or insider holder count against gurufocus / SEC EDGAR direct.

## Operator runbook (post-merge)

1. Apply migration sql/144.
2. Trigger rebuild:
   ```bash
   curl -X POST http://localhost:8000/jobs/sec_rebuild/run -d '{"source": "sec_submissions"}'
   curl -X POST http://localhost:8000/jobs/sec_rebuild/run -d '{"source": "sec_form4"}'
   curl -X POST http://localhost:8000/jobs/sec_rebuild/run -d '{"source": "sec_def14a"}'
   ```
3. Wait for manifest worker drain.
4. Run verification SQL above. Confirm GOOG + GOOGL parity.
5. Hit operator-visible endpoints listed above for both siblings.
6. Cross-source confirm one figure.

## Out of scope

- Per-sibling fan-out at eight_k_filings / insider_filings level — child
  tables (items, exhibits, footnotes, transactions) are entity-level
  by FK shape; per-sibling rows would force per-sibling duplication of
  document detail.
- `eight_k_filings.instrument_id` and `insider_filings.instrument_id`
  column drop — kept as informational "discovery sibling" denormalisation.
  Operator-visible behaviour reroutes via filing_events bridge.
- Universe expansion / share-class sibling discovery — already covered
  by #1102 PR-A.

## Codex spec review v2 prompt

Review correctness of v2 spec against round-1 findings:
(a) Design B PK preservation on eight_k_filings + insider_filings —
    are entity-level invariants intact?
(b) Read-site routing through filing_events — any sites missed where
    `WHERE f.instrument_id = X` on insider_filings/eight_k_filings
    won't be re-routed?
(c) def14a UNIQUE shape change idempotency — re-runnable index swap?
(d) Inner-CTE DISTINCT ON pattern — preserves LIMIT-by-recency?
(e) Manifest seeder canonical-sibling pick — leak any nondeterminism?
(f) Migration shape-check via pg_constraint.contype + conkey —
    handles partial-apply?
(g) sec_identity helper — naming, normalisation, ordering policy
    documentation?
Reply terse with severity-tagged findings (BLOCKING / MEDIUM / NIT).
