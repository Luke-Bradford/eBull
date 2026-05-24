# Share-class CIK uniqueness — implementation plan (#1102)

Date: 2026-05-10
Status: Spec drafted, Codex round 1+2 applied; ships as two PRs.

## Split

**PR1102a (this PR)** — schema migration + ON CONFLICT predicate sweep + filings.py upsert + tests/test_upsert_cik_mapping flips + settled-decisions. Stops the CIK flap. Bulk ingesters still write to one instrument per CIK (dict-collapse), but the binding is stable.

**PR1102b (follow-up)** — fan-out fix in `sec_companyfacts_ingest.py`, `sec_submissions_ingest.py`, `sec_insider_dataset_ingest.py` so share-class siblings BOTH receive bulk-ingest data. Full operator-visible win.

The split is deliberate: PR1102a is mechanical (predicate adds across ~25 sites). PR1102b changes per-callsite logic and adds tests proving fundamentals/insider/submissions land for both siblings.

## Problem

`external_identifiers` enforces a global UNIQUE on `(provider, identifier_type, identifier_value)`. When two share-class siblings (GOOG + GOOGL, BRK.A + BRK.B) legitimately share a CIK, `upsert_cik_mapping`'s ON CONFLICT clause rewrites the row's `instrument_id` to the last writer. `daily_cik_refresh` flaps the binding between siblings on every run, leaving one of them without a CIK and therefore without 10-K, fundamentals, or filings.

Research at #1094 comment (CRSP / Bloomberg / Yahoo / IEX / OpenFIGI / SEC EDGAR all encode CIK = entity, CUSIP = security; siblings co-bind the parent CIK).

## Decision (operator-locked 2026-05-10)

Option A. Allow N `(provider='sec', identifier_type='cik', identifier_value=X)` rows pointing at different `instrument_id`s. CIK becomes a many-to-one relationship from instruments to issuer.

## Schema migration — `sql/143_share_class_cik_uniqueness.sql`

```sql
-- Drop the global table-constraint UNIQUE on (provider, identifier_type, identifier_value).
ALTER TABLE external_identifiers
    DROP CONSTRAINT uq_external_identifiers_provider_value;

-- Replace with a partial UNIQUE INDEX that excludes (sec, cik). Every other
-- provider/type triple stays globally unique (CUSIP, symbol, etc.).
CREATE UNIQUE INDEX uq_external_identifiers_provider_value_non_cik
    ON external_identifiers (provider, identifier_type, identifier_value)
    WHERE NOT (provider = 'sec' AND identifier_type = 'cik');

-- For (sec, cik) rows, uniqueness is per-instrument so siblings co-bind.
-- Per-(provider, type, value, instrument_id) — one row per (CIK, instrument)
-- pair, but multiple instruments may share a CIK.
CREATE UNIQUE INDEX uq_external_identifiers_cik_per_instrument
    ON external_identifiers (provider, identifier_type, identifier_value, instrument_id)
    WHERE provider = 'sec' AND identifier_type = 'cik';
```

Rationale for partial-index split:
- Postgres CHECK / CONSTRAINT UNIQUE can't have a predicate. UNIQUE INDEX can.
- The two new indexes together preserve every old guarantee on non-CIK rows AND add the per-instrument shape we want for CIK rows.
- ON CONFLICT inference works against partial indexes when the INSERT row matches the predicate — Postgres docs confirm.

## Code change 1 — `app/services/filings.py::upsert_cik_mapping`

Codex spec round 1 BLOCKING: ON CONFLICT inference against a partial unique index requires the predicate be supplied, otherwise Postgres 17 reports "no unique or exclusion constraint matching the ON CONFLICT specification".

```python
INSERT INTO external_identifiers (
    instrument_id, provider, identifier_type, identifier_value,
    is_primary, last_verified_at
)
VALUES (
    %(instrument_id)s, 'sec', 'cik', %(cik)s,
    TRUE, NOW()
)
ON CONFLICT (provider, identifier_type, identifier_value, instrument_id)
    WHERE provider = 'sec' AND identifier_type = 'cik'
DO UPDATE SET
    is_primary       = TRUE,
    last_verified_at = NOW()
```

Notes:
- Drop `instrument_id = EXCLUDED.instrument_id` from the SET clause — the conflict target now includes `instrument_id` so a hit means it's already correct.
- Comment block updated to name both partial indexes.
- The is_primary demote UPDATE stays so a single instrument changing its CIK still demotes the prior row.

## Code change 2 — non-CIK ON CONFLICT call site

Codex spec round 1 BLOCKING: dropping the global constraint also breaks `app/services/sec_13f_securities_list.py:430`'s CUSIP upsert (`ON CONFLICT (provider, identifier_type, identifier_value) DO NOTHING`). With the global constraint gone the inference must match the new non-CIK partial index by supplying its predicate.

```python
INSERT INTO external_identifiers (
    instrument_id, provider, identifier_type, identifier_value, is_primary
) VALUES (%(iid)s, 'sec', 'cusip', %(cusip)s, FALSE)
ON CONFLICT (provider, identifier_type, identifier_value)
    WHERE NOT (provider = 'sec' AND identifier_type = 'cik')
DO NOTHING
RETURNING instrument_id
```

## Code change 3 — predicate sweep across ALL non-CIK ON CONFLICT sites (PR1102a)

Codex round 2 BLOCKING + MEDIUM: dropping the global constraint breaks every `ON CONFLICT (provider, identifier_type, identifier_value)` site that doesn't supply the new partial-index predicate. Audited via grep:

Production:

- `app/services/filings.py:379` (CIK upsert — already in code change 1)
- `app/services/sec_13f_securities_list.py:430` (CUSIP upsert — code change 2)
- `scripts/seed_holder_coverage.py:385` (CUSIP upsert — Codex round 2)

Test fixtures (~22 sites): `tests/test_insider_baseline_drill.py`, `tests/test_capabilities_resolver.py`, `tests/test_backfill_company_facts.py`, `tests/test_force_refresh_fundamentals.py`, `tests/test_filings_bulk_resolve.py`, `tests/test_def14a_drill.py`, `tests/test_ownership_drillthrough.py`, `tests/test_refresh_financial_facts_parallel.py`, `tests/test_filings_cancel_signal.py`, `tests/test_agent_cik_defense.py`, `tests/test_rewash_filings.py` (5 sites), `tests/test_blockholders_ingester.py`, `tests/test_n_port_ingest.py`, `tests/test_migration_066_purge_orphan_sec.py`, `tests/test_reconciliation.py`, `tests/test_institutional_holdings_ingester.py`.

Each gets the same mechanical change: append `WHERE NOT (provider = 'sec' AND identifier_type = 'cik')` to the `ON CONFLICT` clause. All these test fixtures insert non-CIK rows (CUSIP / symbol) so the predicate matches the new non-CIK partial index.

Empirically verified against Postgres 17 (T1/T2/T3/T4 in spec-review session): without the predicate, INSERT fails with `there is no unique or exclusion constraint matching the ON CONFLICT specification`.

## Code change 4 — fan-out CIK→instrument lookup in three bulk ingesters (PR1102b — DEFERRED)

Codex spec round 1 BLOCKING: three ingesters store `{cik: instrument_id}` as a dict, so a CIK that now legitimately binds to N siblings collapses to one. Each must change shape to a multimap and iterate the per-CIK list.

| File | Current return | New return |
|---|---|---|
| `app/services/sec_companyfacts_ingest.py:96` | `dict[str, int]` | `dict[str, list[int]]` |
| `app/services/sec_submissions_ingest.py:66` | `dict[str, tuple[int, str]]` | `dict[str, list[tuple[int, str]]]` |
| `app/services/sec_insider_dataset_ingest.py:65` | `dict[str, int]` | `dict[str, list[int]]` |

Per-callsite refactor: replace `instrument_id = cik_to_instrument.get(cik)` with `for instrument_id in cik_to_instrument.get(cik, []):` over the per-instrument write block. The persistence layer is per-instrument already — fan-out is a loop wrap, no new schema or write contract.

Without these: GOOG and GOOGL both have CIK rows but only one of them receives companyfacts / submissions / insider data on the next bulk ingest. The operator would correctly call this a half-shipped feature.

Performance: in the common case (no siblings) each list has one element; the loop iterates once and the cost matches today. Share-class CIKs (~10 instruments today) loop twice. SEC fetch deduplication is a follow-up if benchmarks demand it; nothing here makes the per-CIK fetch cost worse than today.

## Read-path audit

Five existing CIK-keyed read paths JOIN `external_identifiers`:

- `app/services/sec_submissions_files_walk.py:73` — JOIN-driven; per-row processing handles fan-out implicitly.
- `app/services/sec_companyfacts_ingest.py:104` — **collapses to dict[str,int]** → fixed in code change 3.
- `app/services/sec_insider_dataset_ingest.py:73` — **collapses to dict[str,int]** → fixed in code change 3.
- `app/services/sec_submissions_ingest.py:84` — **collapses to dict[str,tuple]** → fixed in code change 3.
- `app/services/capabilities.py:237` — JOIN-driven; per-row already.

Codex spec round 1 caught the three dict-collapse cases. The two JOIN-driven sites (sec_submissions_files_walk + capabilities) are confirmed safe.

## Test updates — `tests/test_upsert_cik_mapping.py`

| Existing test | Change |
|---|---|
| `test_first_insert_creates_primary_row` | unchanged |
| `test_idempotent_rerun_same_mapping` | unchanged |
| `test_cik_change_demotes_prior_primary` | unchanged (single-instrument path) |
| `test_cik_reassigned_to_different_instrument` | **flip behaviour**: after both calls, BOTH instruments hold the CIK as primary. Old assertion that instrument 1 has zero rows is now wrong — under #1102 the row stays. |
| `test_cik_reassigned_to_instrument_with_existing_different_cik` | **flip behaviour**: instrument 1 keeps its primary CIK 0000555555; instrument 2 holds both 0000555555 (primary) and 0000999999 (demoted) |

New tests:

- `test_share_class_siblings_co_bind_cik` — call upsert with GOOG + GOOGL on the same CIK; both rows are primary, neither flaps.
- `test_share_class_repeat_run_is_idempotent` — second call against same panel adds zero rows, both still primary, last_verified_at advanced.

## Verification matrix (dev DB, after migration runs)

Per CLAUDE.md definition-of-done clauses 8-12 (ETL/schema migration affecting identity resolution).

| Instrument | Class | Expected after `daily_cik_refresh` |
|---|---|---|
| AAPL | single | row primary on AAPL with CIK `0000320193`; control case ensures non-share-class regression |
| GOOG | share-class | row primary on GOOG with CIK `0001652044` |
| GOOGL | share-class | row primary on GOOGL with CIK `0001652044` |
| BRK.A | share-class | row primary on BRK.A with CIK `0001067983` |
| BRK.B | share-class | row primary on BRK.B with CIK `0001067983` |

Each row read via:
```sql
SELECT instrument_id, identifier_value, is_primary
  FROM external_identifiers
 WHERE provider='sec' AND identifier_type='cik' AND instrument_id IN (
   SELECT instrument_id FROM instruments WHERE symbol IN
     ('AAPL', 'GOOG', 'GOOGL', 'BRK.A', 'BRK.B')
 )
 ORDER BY instrument_id;
```

Operator-visible verification:
- `/instruments/GOOG/ownership-rollup` and `/instruments/GOOGL/ownership-rollup` both render with non-NULL totals.
- 10-K / fundamentals tab on both renders.

## Settled-decisions entry

Append to `docs/settled-decisions.md`:

```markdown
## CIK = entity; CUSIP = security (#1102, settled 2026-05-10)

Share-class siblings (GOOG/GOOGL, BRK.A/BRK.B) legitimately share a CIK
(SEC's per-issuer registration identifier) but have distinct CUSIPs (per-
security). Every reputable feed (CRSP, Bloomberg, Yahoo, IEX, OpenFIGI)
encodes this. eBull's `external_identifiers` table now allows N
`(sec, cik, value)` rows pointing at different `instrument_id`s, while
keeping uniqueness on every other (provider, type, value) triple.

`upsert_cik_mapping` claims the CIK independently per instrument. There
is no flap. Entity-level data (10-K text, business summary, facts) is
denormalised across siblings — acceptable for the small share-class
population today (~10 instruments). If that grows to 50+, file a
follow-up to introduce a proper `entities` layer (Option B from the
#1094 design discussion).

`canonical_instrument_id` (#819) is a different mechanism for `.RTH`
operational duplicates — same security, two ticker variants.
```

## Out of scope

- Option B entity layer (parked per design discussion).
- `.RTH` mechanism — covered by #819 separately.
- Universe expansion to seed share-class siblings — they already exist on the instruments table.
- SEC fetch deduplication for shared CIKs — file follow-up only if benchmarks show it.

## Codex spec review prompt

Review correctness of: (a) migration partial-index design + ON CONFLICT inference; (b) test behaviour flips; (c) is_primary demote interaction with the new conflict target. Reply terse with severity-tagged findings.
