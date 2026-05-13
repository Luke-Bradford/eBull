# 10-K manifest adapter (final #873-series parser) — #1151

Date: 2026-05-13

## Goal

Register a `sec_10k` parser with the manifest worker so `10-K` / `10-K/A` rows in `sec_filing_manifest` drain through the single-writer manifest path. Final adapter in the #873-series rollout.

## Scope

In:

- `sql/148_instrument_business_summary_filed_at.sql` — adds nullable `filed_at TIMESTAMPTZ` + backfill from `filing_events.filing_date`.
- `app/services/business_summary.py::upsert_business_summary` — adds `filed_at` kwarg + conditional `ON CONFLICT` gated by `(filed_at, source_accession)` tuple (Option C); returns `Literal['inserted', 'updated', 'suppressed']`.
- Legacy `ingest_business_summaries` + `_find_prior_plain_10k` fallback threads `filed_at` through (DATE → TIMESTAMPTZ via `datetime.combine(d, time.min, tzinfo=UTC)`).
- `app/services/manifest_parsers/sec_10k.py` — new adapter, shape mirrors `def14a.py` (share-class fan-out) and `eight_k.py` (raw-payload contract).
- `register_all_parsers()` wires it.
- Tests + DB-level conditional-ON-CONFLICT regression test + share-class fanout test.

Out:

- Legacy `ingest_business_summaries` retirement.
- 10-Q manifest adapter (blocked on #414).
- N-PORT `period_of_report` overwrite bug.
- Sections-orphan cleanup.

## Why Option C

`iter_pending` orders `filed_at ASC` (oldest first). Unconditional `ON CONFLICT (instrument_id) DO UPDATE` would render OLDEST → NEWEST per-instrument during the drain — final state correct but operator briefly sees the 2018 10-K body before the 2024 update fires. The conditional makes stale-arrival a no-op so the operator always sees monotonically-newer or unchanged.

## Schema migration

`sql/148_instrument_business_summary_filed_at.sql`:

```sql
ALTER TABLE instrument_business_summary
    ADD COLUMN IF NOT EXISTS filed_at TIMESTAMPTZ;

-- Backfill from filing_events. Match by source_accession against the
-- canonical SEC accession; coerce DATE to TIMESTAMPTZ at midnight UTC.
UPDATE instrument_business_summary ibs
   SET filed_at = fe.filing_date::timestamptz
  FROM filing_events fe
 WHERE fe.provider = 'sec'
   AND fe.provider_filing_id = ibs.source_accession
   AND ibs.filed_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_instrument_business_summary_filed_at
    ON instrument_business_summary (filed_at);

COMMENT ON COLUMN instrument_business_summary.filed_at IS
    'TIMESTAMPTZ of the 10-K filing this row was extracted from. Gates '
    'the conditional ON CONFLICT in upsert_business_summary so a '
    'filed_at-ASC manifest drain does not render stale-then-fresh. '
    'NULL means the row pre-dates the column or originated as a '
    'service-level tombstone (record_parse_attempt).';
```

Nullable forever — tombstones from `record_parse_attempt` carry no source filing, and pre-#1151 rows whose `source_accession` does not match a `filing_events` row stay NULL.

## `upsert_business_summary` change

Signature change (legacy callers updated in lockstep):

```python
UpsertOutcome = Literal["inserted", "updated", "suppressed"]

def upsert_business_summary(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    body: str,
    source_accession: str,
    filed_at: datetime | None,
) -> UpsertOutcome:
```

SQL — gate on `(filed_at, source_accession)` tuple so two filings on the same calendar day still have a deterministic winner (SEC accessions are temporally ordered within an issuer):

```sql
INSERT INTO instrument_business_summary
    (instrument_id, body, source_accession, filed_at)
VALUES (%s, %s, %s, %s)
ON CONFLICT (instrument_id) DO UPDATE SET
    body                = EXCLUDED.body,
    source_accession    = EXCLUDED.source_accession,
    filed_at            = EXCLUDED.filed_at,
    fetched_at          = NOW(),
    last_parsed_at      = NOW(),
    attempt_count       = 0,
    last_failure_reason = NULL,
    next_retry_at       = NULL
WHERE
    instrument_business_summary.filed_at IS NULL
    OR (EXCLUDED.filed_at IS NOT NULL
        AND (EXCLUDED.filed_at, EXCLUDED.source_accession)
            >= (instrument_business_summary.filed_at, instrument_business_summary.source_accession))
RETURNING (xmax = 0) AS inserted
```

Return:

- `cur.fetchone() is None` → `'suppressed'` (UPDATE WHERE rejected — incumbent is newer or same-day-newer accession, or incoming filed_at is NULL against a dated incumbent).
- `row[0] is True` → `'inserted'`.
- `row[0] is False` → `'updated'`.

NULL handling:

- Incumbent NULL (legacy / pre-#1151 row) → any new write wins. The first dated write (manifest path) re-baselines the row.
- Incoming NULL against a dated incumbent → `'suppressed'`. No legitimate caller passes NULL after this PR — manifest adapter always has `row.filed_at`; legacy callers thread `filing_date`. A NULL-incoming write would be a bug; failing closed (suppression) preserves the dated incumbent rather than silently re-baselining.

## Legacy callers (`ingest_business_summaries`)

Both call sites already have `provider_filing_id` for the candidate row. Extend the candidate `SELECT` to include `fe.filing_date`. Convert in the Python loop:

```python
from datetime import UTC, datetime, time
filed_at_dt = datetime.combine(filing_date, time.min, tzinfo=UTC) if filing_date else None
```

Pass `filed_at=filed_at_dt` to `upsert_business_summary`. The 10-K/A fallback path uses `_find_prior_plain_10k`; extend its return tuple to include the fallback's `filing_date` and convert the same way.

Counters `inserted` vs `updated` in `IngestResult` map to the trinary as:

- `'inserted'` → `inserted += 1`
- `'updated'` → `updated += 1`
- `'suppressed'` → no-op (counter unchanged; logged at DEBUG). Legacy selector picks newest so this branch is essentially unreachable on the legacy path; treating it as a silent no-op avoids muddying `parse_misses` (which is reserved for fetch/parse failures, not "newer was already there").

## Adapter shape (`sec_10k.py`)

Mirrors `def14a.py` for share-class fan-out + `eight_k.py` for raw-payload contract.

### Steps

1. **Validate** — `row.primary_document_url`, `row.instrument_id`, `row.cik` non-null; missing → tombstone with descriptive error (no raw stored).
2. **Fetch** — `SecFilingsProvider.fetch_document_text(url)`. Exception → `_failed_outcome(error=..., raw_status=None)` (1h backoff). Empty/None → tombstoned (no raw).
3. **Store raw** — `store_raw(accession_number=accession, document_kind='primary_doc', payload=html, ...)` inside `with conn.transaction():`. Exception → `_failed_outcome`.
4. **Parse Item 1** — `extract_business_section(html)`. Exception → `_failed_outcome(raw_status='stored')`.
5. **Body extraction policy**:
   - **Body found AND `len(body) >= _MIN_BODY_LEN`** → use this body, accession=`row.accession_number`, filed_at=`row.filed_at`, html=`html` for sections.
   - **Body None / too short, `row.form == '10-K/A'`** → attempt fallback (see step 6).
   - **Body None / too short, plain `10-K`, no fallback** → tombstone (raw stored).
6. **10-K/A fallback** (mirrors legacy `business_summary.py:1657`–`1727`):
   - Look up prior plain 10-K via extended `_find_prior_plain_10k`: returns `(provider_filing_id, primary_document_url, filing_date)` or `None`.
   - `None` → tombstone (raw stored).
   - Else fetch fallback HTML.
     - Fallback fetch exception → `_failed_outcome(raw_status='stored')` (retry; original raw is already stored, no waste).
     - Fallback returns empty → tombstone (raw stored).
   - `store_raw(accession_number=fallback_acc, …)` inside `with conn.transaction():` so a partial write doesn't abort the worker's outer tx. Fallback `store_raw` exception → `_failed_outcome(raw_status='stored')` (original raw is already stored from step 3; do NOT downgrade `raw_status`).
   - `extract_business_section(fallback_html)` → parse exception → `_failed_outcome(raw_status='stored')`; None / too short → tombstone (both raws stored).
   - On success: use fallback body, accession=`fallback_acc`, filed_at=`datetime.combine(fallback_filing_date, time.min, tzinfo=UTC)`, html=`fallback_html` for sections.
7. **Extract sections once** — `sections = extract_business_sections(chosen_html)` BEFORE the fan-out loop, wrapped in `try/except Exception: log + sections = ()`. Sections are a best-effort enrichment; a section-parser bug must not escape after `store_raw` ran (would violate `raw_status='stored'` preservation per 8-K Codex round 2 BLOCKING). On `sections=()` the fan-out still writes the parent blob; the sections table simply gets no rows for this accession.
8. **Share-class fan-out — single batched savepoint** (mirrors `def14a._parse_def14a` lines 322–360). Wrap sibling resolution + entire parent/sections write batch in ONE `with conn.transaction():` so:
   - A mid-batch failure rolls back the whole sibling fan-out (no partial state across siblings).
   - Transient `OperationalError` anywhere → `_failed_outcome(raw_status='stored')`.
   - Deterministic Postgres error anywhere → tombstone with `format_upsert_error(exc)` (the savepoint rollback unwinds any earlier sibling writes from the same batch before tombstone state is emitted).
   - Sequence inside the savepoint:
     1. `siblings = _resolve_siblings(conn, instrument_id=row.instrument_id, issuer_cik=row.cik)`.
     2. For each `sibling_iid` in `siblings`:
        - `outcome = upsert_business_summary(conn, instrument_id=sibling_iid, body=…, source_accession=…, filed_at=…)`.
        - If `outcome in ('inserted', 'updated')` and `sections`: call `upsert_business_sections` inside a NESTED `with conn.transaction():` (savepoint-inside-savepoint). A sections exception is logged + swallowed, the nested savepoint rolls back so the outer batch's parent upsert survives, and the loop continues. Matches legacy "sections are best-effort enrichment; blob-only fallback is acceptable" (`business_summary.py:1774`). psycopg3 nests `conn.transaction()` cleanly as SAVEPOINT-within-SAVEPOINT.
     3. Tally per-sibling outcomes (counters in WorkerStats-equivalent local vars).
9. **Tombstone branches** — manifest path does NOT call `record_parse_attempt`. `record_parse_attempt` mutates `source_accession` on UPDATE and could corrupt an incumbent newer body's provenance. The manifest path returns `ParseOutcome(status='tombstoned')`; `transition_status` for tombstoned is terminal until a targeted rebuild re-promotes the row. `next_retry_at` is unused on tombstoned outcomes.
10. **Return** — `ParseOutcome(status='parsed', parser_version=_PARSER_VERSION_10K, raw_status='stored')` whether siblings inserted/updated or all returned `'suppressed'`. Suppression means a newer accession is already in the DB — the manifest's job ("drain this row") is satisfied either way.

### Parser version

Define a new module-level constant in `sec_10k.py`: `_PARSER_VERSION_10K = "10k-v1"`. Used in every `ParseOutcome.parser_version` return. `business_summary.py` doesn't expose a version constant; the manifest layer owns this version label, matching the convention in `def14a.py` (`_PARSER_VERSION_DEF14A`) and `insider_345.py` (`_PARSER_VERSION_FORM4`).

### `requires_raw_payload=True`

Register with the #938 invariant set. The adapter stores raw before the parse → parsed/suppressed outcomes carry `raw_status='stored'` by construction.

## Cross-cutting rule compliance

- **#1131 — transient discrimination.** Use `is_transient_upsert_error` + `format_upsert_error`.
- **#1126 — savepoint around store_raw + upsert.** Each in its own `with conn.transaction():`.
- **#1129 — parse-failure broad-except.** One `except Exception` block per parse + persistence boundary.
- **#1132 — psycopg3 transaction-vs-commit.** Worker owns the outer tx; we use savepoints only.
- **#1133 — test-name vs assertion.** Every `test_*tombstone*` asserts `ingest_status == 'tombstoned'`.
- **#1117 — share-class fanout per filing.** 10-K Item 1 is entity-level; per-instrument fan-out via `siblings_for_issuer_cik`.

## Tests (`tests/test_manifest_parser_sec_10k.py`)

Per the prevention log entry on "ON CONFLICT branch coverage" (line 582-583), at least one DB-level test exercises the conditional `WHERE` against the real schema.

1. `test_happy_path_parses_and_stores_raw` — fresh accession, fresh instrument, body extracted, sections written, parent inserted.
2. `test_happy_path_fans_out_to_share_class_siblings` — seed two siblings sharing CIK; one manifest row; both siblings receive body + sections; both rows point at the same accession.
3. `test_happy_path_10ka_with_item1_present` — 10-K/A whose body parses directly (no fallback).
4. `test_10ka_falls_back_to_prior_plain_10k` — 10-K/A Item 1 None; fallback returns hit; fallback fetched + raw stored under fallback_acc; parent + sections persisted under fallback_acc; fallback's filed_at threaded through.
5. `test_10ka_fallback_no_prior_filing_tombstones` — no prior plain 10-K; tombstone with raw stored.
6. `test_10ka_fallback_fetch_error_returns_failed_with_raw_stored` — fallback fetcher raises; outcome is `failed` not tombstone; original raw is in `filing_raw_documents`.
7. `test_10ka_fallback_empty_body_tombstones` — fallback HTML empty.
8. `test_10ka_fallback_parse_exception_returns_failed_with_raw_stored` — fallback `extract_business_section` raises.
9. `test_fetch_error_returns_failed_outcome` — original fetcher raises; no raw stored; `next_retry_at` ~1h.
10. `test_empty_body_tombstones_without_raw_stored` — original fetcher returns empty.
11. `test_body_too_short_tombstones_with_raw_stored` — body shorter than `_MIN_BODY_LEN`.
12. `test_filed_at_gate_suppresses_older_arrival` — seed `instrument_business_summary` with filed_at=2024 + accession=A1; manifest row filed_at=2020 + accession=A0; adapter returns parsed but body+sections unchanged.
13. `test_filed_at_gate_allows_newer_arrival` — seed filed_at=2020; manifest filed_at=2024; body+sections updated.
14. `test_same_day_accession_tiebreaker_picks_later_accession` — seed filed_at=2026-01-01 + accession=`0001-26-000001`; manifest filed_at=2026-01-01 + accession=`0001-26-000002` (later number) → wins. Reverse → suppressed.
15. `test_null_incumbent_filed_at_allows_write` — pre-seeded legacy row with filed_at=NULL; adapter writes.
16. `test_suppressed_parent_skips_sections_write` — seed sections under incumbent newer accession; adapter for older accession returns suppressed; sections untouched.
17. `test_transient_upsert_error_returns_failed_outcome` — patch `upsert_business_summary` to raise `SerializationFailure`.
18. `test_deterministic_upsert_error_tombstones` — patch to raise non-transient `psycopg.Error`.
19. `test_tombstone_path_does_not_mutate_existing_body_summary_row` — pre-seed an incumbent with real body; adapter for older accession parses miss + tombstones; incumbent body + source_accession unchanged.
20. `test_register_all_parsers_wires_sec_10k` — audit endpoint surfaces `has_registered_parser=True`.
21. `test_section_extraction_exception_does_not_break_parent_write` — patch `extract_business_sections` to raise; parent blob still written; `ParseOutcome.status='parsed'`, `raw_status='stored'`; sections table row count unchanged.
22. `test_partial_fanout_rollback_on_deterministic_error` — seed two siblings A + B sharing CIK; patch `upsert_business_summary` to succeed on A and raise `psycopg.errors.IntegrityError` on B; assert manifest tombstones, A's row reverts (savepoint rolls back), neither sibling has new blob.
23. `test_fallback_store_raw_failure_returns_failed_with_original_raw_stored` — 10-K/A fallback path; patch `store_raw` to raise on fallback accession only; original accession's raw row exists; outcome is `failed` with `raw_status='stored'`; no parent write.

## Smoke + verify (ETL clauses 8–12)

After merge:

- `POST /jobs/sec_rebuild/run` with `{"source": "sec_10k"}` on dev DB.
- Wait for manifest drain on the panel AAPL / GME / MSFT / JPM / HD.
- Verify `GET /instruments/{symbol}/research` (or the canonical 10-K Item 1 surface) renders fresh body.
- Cross-source: spot-check one issuer's Item 1 against the SEC EDGAR direct page.
- Record commit SHA + verification rows in PR body.

## Risks / open questions

1. **Sections rewrite on incumbent re-parse**: same-accession re-parse → `'updated'` → `upsert_business_sections` DELETE+INSERT for that accession only (savepoint-wrapped per #460 prevention).
2. **Legacy path producing `'suppressed'`**: dead branch on legacy (selector picks newest). No-op on counters; debug-log only.
3. **Backfill timing**: `instrument_business_summary` ≤4031 rows. Negligible.
4. **`record_parse_attempt` mutation hazard noted but unfixed**: the helper still UPDATEs `source_accession` on failed retries via the legacy path. Out of scope here; manifest path simply doesn't call it. Tech-debt eligible if it bites operator-visible figures.

## Codex checkpoints

- **Checkpoint 1**: addressed three BLOCKING, two HIGH, one MEDIUM via this revision.
- **Checkpoint 2** (before push): `codex.cmd exec review` on the branch diff.
