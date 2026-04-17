# Filings status schema + audit v1 — design spec (#268 Chunk D)

**Parent plan:** `docs/superpowers/plans/2026-04-17-filings-cascade-master-plan.md` Chunk D
**Depends on:** #291 (execute_refresh writes filing_events — shipped), #292 (coverage row bootstrap — shipped)
**Precedes:** Chunks E (backfill), F (weekly job), G (universe hook), H (admin surface), J (scoring gate)
**Date:** 2026-04-17

---

## Goal

Add `coverage.filings_status` column + retry-tracking columns. Implement `coverage_audit.audit_all_instruments` + `audit_instrument` that classify every tradable instrument into one of four classifier outputs: `analysable`, `insufficient`, `fpi`, `no_primary_sec_cik`. Defer `structurally_young` assignment to Chunk E (backfill). `unknown` is a pre-audit placeholder written by Chunk G (universe-sync hook); the classifier itself never outputs it.

## Scope

**In scope**

- Migration `sql/036_coverage_filings_status.sql`:
  - `ADD COLUMN filings_status TEXT CHECK (filings_status IN ('analysable','insufficient','fpi','no_primary_sec_cik','structurally_young','unknown'))`.
  - `ADD COLUMN filings_audit_at TIMESTAMPTZ`.
  - `ADD COLUMN filings_backfill_attempts INTEGER NOT NULL DEFAULT 0`.
  - `ADD COLUMN filings_backfill_last_at TIMESTAMPTZ`.
  - `ADD COLUMN filings_backfill_reason TEXT`.
  - `CREATE INDEX idx_coverage_filings_status ON coverage(filings_status)`.
- New service `app/services/coverage_audit.py`:
  - `@dataclass AuditCounts` — per-CIK row for the aggregate.
  - `@dataclass AuditSummary` — run result: counts for the four classifier outputs `{analysable, insufficient, fpi, no_primary_sec_cik}` plus `total_updated` + `null_anomalies` (see below).
  - `audit_all_instruments(conn) -> AuditSummary` — full-universe scan.
  - `audit_instrument(conn, instrument_id) -> str` — single-instrument, returns the new status.
- Unit + real-DB integration tests.

**Out of scope (other chunks)**

- Backfill / `structurally_young` assignment — Chunk E.
- Weekly scheduler job — Chunk F.
- Universe-sync `unknown` marking — Chunk G.
- Admin UI surface — Chunk H.
- Scoring gate on `filings_status` — Chunk J.

---

## Schema

```sql
-- sql/036_coverage_filings_status.sql

ALTER TABLE coverage
    ADD COLUMN filings_status TEXT
    CHECK (filings_status IS NULL OR filings_status IN (
        'analysable',
        'insufficient',
        'fpi',
        'no_primary_sec_cik',
        'structurally_young',
        'unknown'
    ));

ALTER TABLE coverage ADD COLUMN filings_audit_at TIMESTAMPTZ;
ALTER TABLE coverage ADD COLUMN filings_backfill_attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE coverage ADD COLUMN filings_backfill_last_at TIMESTAMPTZ;
ALTER TABLE coverage ADD COLUMN filings_backfill_reason TEXT;

CREATE INDEX IF NOT EXISTS idx_coverage_filings_status
    ON coverage(filings_status);

-- Chunk D audit v1 does NOT set filings_status during the migration.
-- All existing rows keep filings_status = NULL until the weekly audit
-- (Chunk F) runs and classifies them. NULL semantics: "not yet audited"
-- (pre-first-audit only). After the first full audit, NULL filings_status
-- should never occur.
```

## Status value semantics

| Value | Meaning | Gate for downstream? |
|---|---|---|
| `analysable` | US domestic issuer; 10-K count ≥ 2 within 3y AND 10-Q count ≥ 4 within 18mo. | YES — only this passes |
| `insufficient` | Has primary SEC CIK (US or FPI); below the form thresholds. Probably needs backfill. | NO |
| `fpi` | Foreign Private Issuer: SEC CIK, zero 10-K/10-Q, ≥1 of {20-F, 40-F, 6-K}. | NO (v1; #279 handles UK-equivalent bar) |
| `no_primary_sec_cik` | No primary `sec`/`cik` row in `external_identifiers`. Non-US, crypto, ETFs, etc. | NO |
| `structurally_young` | Post-backfill status ONLY. IPO'd <18mo ago; cannot meet US-bar by calendar. NOT assigned by Chunk D audit. | NO |
| `unknown` | Not yet audited OR universe-sync-just-added. | NO |
| `NULL` | Pre-first-audit invariant. Post-first-audit is an error state. | NO |

## `audit_all_instruments` logic

```python
def audit_all_instruments(conn: psycopg.Connection[tuple]) -> AuditSummary:
    """Recompute filings_status for every tradable instrument.

    Does not touch filings_backfill_* columns (those are Chunk E's
    responsibility). Does not assign `structurally_young` (Chunk E
    owns that based on SEC earliest-filing date).

    Returns per-status counts for logging.
    """
    # 1. Single GROUP BY aggregate: counts per (instrument_id, form_type)
    #    WITHIN the recency windows (3y for 10-K, 18mo for 10-Q).
    #    Filter on fe.provider='sec' AND ei.provider='sec' AND
    #    ei.identifier_type='cik' AND ei.is_primary=TRUE.
    # 2. Cohort query: every tradable instrument + whether it has a
    #    primary SEC CIK.
    # 3. Classify in Python per the table above.
    # 4. Bulk UPDATE coverage via unnest(bigint[], text[]); set filings_audit_at=NOW().
    # 5. Return AuditSummary.
```

### SQL (aggregate query)

Recency windows are applied in SQL via `COUNT(*) FILTER (WHERE ...)` so the Python classifier receives exact per-window counts per instrument. Amendments are NOT counted toward history-depth bars — they prove the issuer re-stated a prior filing, not that they have more distinct reporting periods. Amendments DO still trigger event-driven refresh (Chunk I) and populate `filing_events`, they just don't satisfy the analysability bar.

```sql
SELECT
    fe.instrument_id,
    COUNT(*) FILTER (
        WHERE fe.filing_type = '10-K'
          AND fe.filing_date >= (CURRENT_DATE - INTERVAL '3 years')
    ) AS ten_k_in_3y,
    COUNT(*) FILTER (
        WHERE fe.filing_type = '10-Q'
          AND fe.filing_date >= (CURRENT_DATE - INTERVAL '18 months')
    ) AS ten_q_in_18m,
    COUNT(*) FILTER (
        WHERE fe.filing_type IN ('10-K','10-K/A','10-Q','10-Q/A')
    ) AS us_base_or_amend_total,
    COUNT(*) FILTER (
        WHERE fe.filing_type IN ('20-F','20-F/A','40-F','40-F/A','6-K','6-K/A')
    ) AS fpi_total
FROM filing_events fe
JOIN external_identifiers ei
    ON ei.instrument_id = fe.instrument_id
    AND ei.provider = 'sec'
    AND ei.identifier_type = 'cik'
    AND ei.is_primary = TRUE
WHERE fe.provider = 'sec'
GROUP BY fe.instrument_id;
```

**Key points:**
- Windows computed in SQL via `FILTER`, not Python. Avoids `timedelta(days=548)`-style drift at month boundaries. `INTERVAL '18 months'` is calendar-correct.
- `CURRENT_DATE` used once; all FILTERs reference the same anchor (Postgres evaluates `CURRENT_DATE` per-row but it's stable within a transaction).
- Base forms (`10-K`, `10-Q`) count toward the history bar. Amendments (`/A`) do NOT — they re-state the same period. `us_base_or_amend_total` is used for FPI detection (presence of ANY US-form-family filing rules out FPI), not for the bar.
- `fpi_total` aggregates 20-F/40-F/6-K families; used only if `us_base_or_amend_total = 0`.
- Zero-row instruments (no SEC filings yet) are omitted from this query's output — the cohort query joins them in separately so they classify as `insufficient` (if SEC CIK) or `no_primary_sec_cik`.

### Cohort SQL

```sql
SELECT
    i.instrument_id,
    CASE WHEN EXISTS (
        SELECT 1 FROM external_identifiers ei
        WHERE ei.instrument_id = i.instrument_id
          AND ei.provider = 'sec'
          AND ei.identifier_type = 'cik'
          AND ei.is_primary = TRUE
    ) THEN TRUE ELSE FALSE END AS has_sec_cik
FROM instruments i
WHERE i.is_tradable = TRUE
ORDER BY i.instrument_id;
```

### Classification

```python
@dataclass(frozen=True)
class _AggRow:
    instrument_id: int
    ten_k_in_3y: int            # base 10-K only, within 3 years
    ten_q_in_18m: int           # base 10-Q only, within 18 months
    us_base_or_amend_total: int # used for FPI detection (non-zero → not FPI)
    fpi_total: int              # 20-F/40-F/6-K family (base + amendments)


def _classify(agg: _AggRow | None, has_sec_cik: bool) -> str:
    """Pure function — windows/counts already computed in SQL.

    `agg` is None for cohort instruments with zero SEC filings in the
    filing_events table. `has_sec_cik` comes from the cohort query.
    """
    if not has_sec_cik:
        return 'no_primary_sec_cik'

    if agg is None:
        # SEC CIK present but no filings yet — likely pre-backfill.
        return 'insufficient'

    # FPI check FIRST: has SEC CIK, zero US base-or-amend filings,
    # at least one 20-F/40-F/6-K family filing.
    if agg.us_base_or_amend_total == 0 and agg.fpi_total > 0:
        return 'fpi'

    # US history bar: base forms only. Amendments do NOT count toward
    # distinct-period depth — a 10-K/A re-states the same annual
    # period, it does not prove an additional year of history.
    if agg.ten_k_in_3y >= 2 and agg.ten_q_in_18m >= 4:
        return 'analysable'

    return 'insufficient'
```

**Why amendments don't count toward the bar:** `10-K/A` / `10-Q/A` are restatements of already-filed periods. Two `10-K`s = two years of annual history. One `10-K` plus one `10-K/A` = still one year, restated. The analysability bar measures history depth, not filing volume. Amendments still populate `filing_events`, still trigger event-driven thesis refresh (Chunk I), and still register in the audit aggregate — they just don't satisfy `ten_k_in_3y >= 2`.

**Windowing is exact via SQL `FILTER`:** no `timedelta(days=N)` drift. `INTERVAL '18 months'` is calendar-correct across month-length variance.

### Bulk UPDATE

Use `UPDATE ... FROM unnest(%s::bigint[], %s::text[])` — native psycopg3 array adaptation, no psycopg2-era helpers. `psycopg.extras` does not exist in psycopg3; `execute_values` is psycopg2-only.

```sql
UPDATE coverage c
SET filings_status = v.status,
    filings_audit_at = NOW()
FROM unnest(%s::bigint[], %s::text[]) AS v(instrument_id, status)
WHERE c.instrument_id = v.instrument_id;
```

Python call:

```python
if not classifications:  # empty-cohort guard
    return AuditSummary(...)  # nothing to update

instrument_ids = [row[0] for row in classifications]
statuses = [row[1] for row in classifications]
conn.execute(sql, (instrument_ids, statuses))
```

psycopg3 adapts `list[int]` → `bigint[]` and `list[str]` → `text[]` automatically. One roundtrip. Empty-cohort case must short-circuit before the execute — `unnest('{}'::bigint[], '{}'::text[])` is legal but an early return is clearer.

### AuditSummary

```python
@dataclass(frozen=True)
class AuditSummary:
    analysable: int
    insufficient: int
    fpi: int
    no_primary_sec_cik: int
    total_updated: int
    null_anomalies: int  # Count from the following post-update query:
                         #   SELECT COUNT(*) FROM instruments i
                         #   LEFT JOIN coverage c USING (instrument_id)
                         #   WHERE i.is_tradable = TRUE
                         #     AND (c.instrument_id IS NULL
                         #          OR c.filings_status IS NULL);
                         # Captures both: tradable instruments with no
                         # coverage row at all (Chunk B regression)
                         # AND coverage rows whose filings_status
                         # remained NULL (bulk UPDATE missed them,
                         # e.g. classification path had a gap). Either
                         # is a data-integrity bug; logged at WARNING.
```

`unknown` is NOT a classifier output. The classifier in `_classify` returns one of `{analysable, insufficient, fpi, no_primary_sec_cik}` only. `unknown` is a pre-audit placeholder value for rows created by Chunk G (universe-sync hook marks new instruments `'unknown'` so the weekly audit picks them up). Once `audit_all_instruments` has processed a row, its `filings_status` is always one of the four classifier outputs — never `'unknown'` or `NULL`. `null_anomalies > 0` indicates either a tradable instrument without a `coverage` row (Chunk B regression) OR a coverage row whose `filings_status` remained NULL after the bulk UPDATE (classifier gap). Either case is logged at WARNING.

## Edge cases

| Condition | Behaviour |
|---|---|
| Tradable instrument with no `coverage` row | Cannot happen post-#292. Audit ignores; `total_updated` reflects only matched rows. Logs a warning if the cohort size > update count. |
| Instrument with `coverage` row but no `filing_events` | Classified per cohort rule: has_sec_cik → `insufficient`; no CIK → `no_primary_sec_cik`. |
| `filing_events` rows with `filing_type = 'unknown'` (legacy from migration 004) | Not in the aggregate's form-type filter; silently ignored. |
| Zero-filing instrument with SEC CIK | `insufficient` (might be a truly-young company — Chunk E upgrades to `structurally_young` if SEC confirms no historical filings exist either). |
| Running audit twice in quick succession | Idempotent; second run is a no-op at the aggregate level, `filings_audit_at` refreshes to NOW(). |
| Transaction scope | Entire audit body wrapped in `with conn.transaction():` so partial failures roll back. |

## Non-scope / explicit deferrals

- **`structurally_young`**: audit never assigns this. Chunk E's backfill service sets it when SEC's own historical submissions.json confirms the issuer has < N total filings ever.
- **8-K gap detection**: Chunk E's job. DB-internal count is insufficient; requires comparing DB rows vs SEC's 12-month accession list.
- **Re-classifying instruments to a higher tier on new filings**: audit is a full recompute each run; always reflects current DB state.
- **Companies House analysability bar**: #279.

## Testing

### Unit tests (`tests/test_coverage_audit.py` — mock-based)

- Classification rules (pure-function `_classify`):
  - No SEC CIK → `no_primary_sec_cik`.
  - Has SEC CIK + 2 × 10-K (recent) + 4 × 10-Q (recent) → `analysable`.
  - Has SEC CIK + 1 × 10-K + 4 × 10-Q → `insufficient`.
  - Has SEC CIK + 2 × 10-K + 3 × 10-Q → `insufficient`.
  - Has SEC CIK + 0 × 10-K + 0 × 10-Q + 1 × 20-F → `fpi`.
  - Has SEC CIK + 0 × 10-K + 0 × 10-Q + 1 × 6-K/A → `fpi`.
  - Has SEC CIK + 1 × 10-K + 1 × 20-F → `insufficient` (mixed; not FPI).
  - **Amendments do NOT count toward base-form thresholds.** `1 × 10-K + 1 × 10-K/A + 4 × 10-Q` → `insufficient` (amendment restates the same year; does not add distinct annual history). `1 × 10-Q + 3 × 10-Q/A + 2 × 10-K` → `insufficient` (one actual 10-Q period, not four).

### Integration tests (`tests/test_coverage_audit_integration.py` — real `ebull_test` DB)

- Full `audit_all_instruments` against seeded cohort with mixed instruments → verifies counts + per-instrument status + `filings_audit_at` advances.
- Idempotent: second run returns same counts, no row status changes.
- Single-instrument `audit_instrument`: returns the classified status + updates the row atomically.

### Pre-push gates

- `uv run ruff check .` + `ruff format --check`
- `uv run pyright`
- `uv run pytest`

## Expected impact

| Metric | Before | After |
|---|---|---|
| `coverage.filings_status` column exists | No | Yes |
| Every tradable instrument has an explicit filings_status (after first audit) | No | Yes (except for pre-first-audit NULLs) |
| Ability to write "only run X on analysable" queries | No | Yes |
| 8-K gap detection | No | No (Chunk E) |
| Historical backfill of missing filings | No | No (Chunk E) |

## Settled decisions preserved

- **Filing lookup rule**: SEC uses CIK, not symbol. Audit queries use `identifier_type = 'cik'`.
- **External identifiers `is_primary = TRUE`** enforced at every query — no `is_primary = FALSE` rows counted.
- **Fundamentals coverage semantics** unchanged — this adds a parallel filings gate.

## What this spec makes explicit

- `structurally_young` is a post-backfill-only status; Chunk D never assigns it.
- `filings_status = NULL` is pre-first-audit only; post-audit NULL is an error.
- 10-Q recency window is 18 months, 10-K is 3 years; limitation on windowed aggregate documented.
- FPI detection requires zero base forms + at least one FPI form (base OR amendment).
- `fe.provider = 'sec'` + `ei.is_primary = TRUE` mandatory in every audit query.
- Single bulk UPDATE via `unnest(bigint[], text[])` — psycopg3-native, no N+1.
- Transaction atomicity: full audit wraps one `with conn.transaction():`.
