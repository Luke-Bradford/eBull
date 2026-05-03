# Ownership Tier 0 + CIK history schema — design (#789 + #794-schema)

**Revision history:**

- v1 — initial spec.
- v2 — Codex spec review (2026-05-03) found 6 issues. Fixes:
  - DEF 14A has no `filer_cik`, so the priority dedup must enrich
    DEF 14A holders with a name-resolved CIK first (via
    `def14a_drift._resolve_holder_match`, lifted to a shared module).
  - Use `snapshot_read(conn)` around the whole handler — pooled
    connections already opened an implicit READ COMMITTED tx, so a
    later `conn.transaction()` becomes a SAVEPOINT and the isolation
    change never takes effect.
  - Coverage banner cannot use `sum(slices)/outstanding` (= float
    concentration) as a completeness signal — retail-heavy names
    would be permanently red. Banner is now driven by **universe
    coverage** (`known_filers / estimated_universe` per category)
    with explicit NULL-tolerant fallback for the v1 unknown-universe
    case. Concentration shown separately as an info chip.
  - History tables get the missing temporal invariants: `CHECK
    (effective_to IS NULL OR effective_to > effective_from)`, partial
    UNIQUE on `(instrument_id) WHERE effective_to IS NULL`, and a GIST
    `EXCLUDE` against overlapping date ranges.
  - Residual math clamps to 0 with an `oversubscribed: bool` flag for
    operator-facing copy when stale mixed-date inputs would yield a
    negative residual.
  - Dedup tie-break sequence pinned: `priority_rank ASC, as_of_date
    DESC NULLS LAST, accession_number DESC, source_row_id DESC`.
  - #794 stays schema-only; the spec no longer claims this batch closes
    the BBBY case. Synthetic symbol-history backfill from
    `former_names` (which is **name** history, not symbol history) is
    dropped — only the current symbol is seeded with `effective_from
    = first_seen_at`. The BBBY case closes in Batch 7 alongside the
    actual symbol-change ingester.

## Goal

Make the per-instrument ownership card production-trustworthy: unify the
denominator on `shares_outstanding` (XBRL DEI), dedup holders across
13F / 13D/G / DEF 14A / Form 4 / Form 3 by CIK with priority `Form 4 >
13D/G > DEF 14A > 13F`, render an explicit "Public / unattributed"
residual wedge, surface coverage with an honest banner that distinguishes
*float concentration* from *universe coverage*, repair the
`insider_initial_holdings.value_owned` schema drift, and lay the
CIK-history schema so the BBBY-style ticker-rename case can be wired up
in Batch 7.

Two ship-blockers from the codex audit (2026-05-03) close in this PR:

1. **Wrong denominator** — frontend uses `shares_outstanding +
   treasury_shares`; backend contract uses `shares_outstanding`. Treasury
   wedges every other category down by the treasury fraction.
2. **No cross-channel dedup** — Cohen on GME is summed across
   `insider_transactions` (Form 4) + `blockholder_filings` (13D/A) for
   ~75M shares; reality is ~38M.

Plus the pre-existing `insider_initial_holdings.value_owned` column
drift (migration 093 `CREATE TABLE IF NOT EXISTS` no-op'd onto an older
table shape) which silently breaks the Form 3 baseline reader on every
instrument.

## Non-goals

- Coverage **expansion** (top 150 13F seed list, Form 3 backfill,
  Soros/Geode disambig, backfill-on-activation). Ship in Batch 2
  (#791 + #790).
- Provenance footer, methodology disclosure, accession links, freshness
  chips on the new endpoint. Ship in Batch 3 (#792).
- First-run ingest progress, ingest-health page. Ship in Batch 4
  (#793 + #797 B4).
- N-PORT mutual fund stream, FINRA short interest. Batches 5 + 6.
- **Closing the BBBY case end-to-end**. The schema lands here so Batch 7
  has somewhere to write to, but the BBBY ownership card is *not*
  production-trustworthy at the end of this batch — it stays unchanged.
  Batch 7 ships the symbol-change ingester + the
  `HistoricalSymbolCallout` UI + reorg test fixtures.
- Adjacent bugs (13F dup-key race, sync_runs FK race, rogue created_at
  poller, test-seed cleanup, stray auth FATAL). Batch 8.

## Settled-decision impact

Touched:

- **Identifier strategy: filing lookup rule** — already settled that SEC
  uses CIK, not symbol. This batch installs the CIK-history schema so
  Batch 7 can enforce the rule on the ingest side. The reader path is
  unchanged this batch (every filings table already keys on
  `instrument_id`); the helper `historical_ciks_for(instrument_id)`
  exists as a stub with one row per current instrument.
- **Provider design rule** — the new `ownership_rollup` service lives
  at `app/services/`, not in any provider module.
- **Auditability** — every dedup decision keeps the losing source's
  accession in `dropped_sources` so the Batch 3 provenance footer can
  surface it without a second query.

No settled decisions changed.

## Prevention-log entries that apply

| Entry | How this batch respects it |
|---|---|
| `CREATE TABLE IF NOT EXISTS does not add columns to pre-existing tables` | Migration 101 uses `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`. Schema-drift smoke gate (B5 of #797) lands in this PR to prevent recurrence. |
| `JOIN fan-out inflates aggregate totals` | Dedup builds the canonical-holder set in a CTE before SUM. |
| `Bucket-arithmetic double-counting` | Dedup happens by `filer_cik` once, then slice rollups use the survivor set only. |
| `When a migration adds any table with a FK relationship, update _PLANNER_TABLES` | Migrations 102 + 103 add new FK tables; `_PLANNER_TABLES` updated. |
| `Frontend async render-surface isolation` | New `useAsync` usage on the rollup endpoint mirrors the existing pattern. |
| `New TEXT columns in migrations need CHECK constraints or Literal types` | Both `source_event` columns CHECK-constrained. |
| `Multi-query read handlers must use a single snapshot` | Endpoint uses `snapshot_read(conn)` around the whole handler — including symbol resolution. v1 spec error caught by Codex. |
| `Don't add scheduling or job execution to the API process` | No scheduler / executor changes. |
| `Internal exception text leaked into HTTP response bodies` | New endpoint returns sanitised detail. |

## Migrations

### `sql/101_insider_initial_holdings_value_owned.sql`

```sql
ALTER TABLE insider_initial_holdings
    ADD COLUMN IF NOT EXISTS value_owned NUMERIC(18, 6);

COMMENT ON COLUMN insider_initial_holdings.value_owned IS
    'Form 3 valueOwnedFollowingTransaction alternative to shares. SEC '
    'allows EITHER shares OR value (fractional-undivided-interest '
    'securities use the value branch). Recovery for migration 093 '
    'CREATE TABLE IF NOT EXISTS no-op on pre-existing schema.';
```

### `sql/102_instrument_cik_history.sql`

```sql
CREATE TABLE IF NOT EXISTS instrument_cik_history (
    instrument_id   BIGINT NOT NULL REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    cik             TEXT NOT NULL,
    effective_from  DATE NOT NULL,
    effective_to    DATE,
    source_event    TEXT NOT NULL CHECK (source_event IN
        ('imported', 'rebrand', 'reorg', 'merger', 'spinoff', 'manual')),
    PRIMARY KEY (instrument_id, cik, effective_from),
    CONSTRAINT instrument_cik_history_dates_ordered
        CHECK (effective_to IS NULL OR effective_to > effective_from)
);

-- One "current" CIK per instrument. Manual reorg insert that forgets
-- to close out the prior current row blows up loud at the DB.
CREATE UNIQUE INDEX IF NOT EXISTS uq_instrument_cik_history_current
    ON instrument_cik_history (instrument_id)
    WHERE effective_to IS NULL;

-- Non-overlapping ranges per instrument. GIST EXCLUDE with a daterange
-- so two historical CIK chains for one instrument cannot overlap in
-- time. Half-open daterange [from, to) where to=NULL becomes 'infinity'.
CREATE EXTENSION IF NOT EXISTS btree_gist;
ALTER TABLE instrument_cik_history
    ADD CONSTRAINT instrument_cik_history_no_overlap
    EXCLUDE USING GIST (
        instrument_id WITH =,
        daterange(effective_from, effective_to, '[)') WITH &&
    );

CREATE INDEX IF NOT EXISTS idx_instrument_cik_history_cik
    ON instrument_cik_history (cik);

COMMENT ON TABLE instrument_cik_history IS
    'CIK chain per instrument with effective-date ranges. Reader path '
    'on the ownership card uses this to resolve filings under a '
    'historical CIK back to the current instrument_id (#794). '
    'effective_to NULL = current. EXCLUDE constraint forbids overlapping '
    'ranges; UNIQUE INDEX forbids two "current" rows per instrument.';
```

### `sql/103_instrument_symbol_history.sql`

```sql
CREATE TABLE IF NOT EXISTS instrument_symbol_history (
    instrument_id   BIGINT NOT NULL REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    symbol          TEXT NOT NULL,
    effective_from  DATE NOT NULL,
    effective_to    DATE,
    source_event    TEXT NOT NULL CHECK (source_event IN
        ('imported', 'rebrand', 'delisting', 'relisting', 'manual')),
    PRIMARY KEY (instrument_id, symbol, effective_from),
    CONSTRAINT instrument_symbol_history_dates_ordered
        CHECK (effective_to IS NULL OR effective_to > effective_from)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_instrument_symbol_history_current
    ON instrument_symbol_history (instrument_id)
    WHERE effective_to IS NULL;

ALTER TABLE instrument_symbol_history
    ADD CONSTRAINT instrument_symbol_history_no_overlap
    EXCLUDE USING GIST (
        instrument_id WITH =,
        daterange(effective_from, effective_to, '[)') WITH &&
    );

CREATE INDEX IF NOT EXISTS idx_instrument_symbol_history_symbol
    ON instrument_symbol_history (symbol);

COMMENT ON TABLE instrument_symbol_history IS
    'Symbol history per instrument with effective-date ranges. '
    'Symbol-clash guard: every row scoped to one instrument_id by PK. '
    'Synthetic backfill from former_names is NOT performed — '
    'former_names is name history, not symbol history. The actual '
    'symbol-change ingest lands in Batch 7.';
```

### Backfill semantics — `scripts/backfill_instrument_history.py`

Idempotent. For each row in `instruments` joined to
`instrument_sec_profile`:

- Insert one `instrument_cik_history` row:
  `(instrument_id, sec_profile.cik, effective_from = COALESCE(first_seen_at::date, CURRENT_DATE), effective_to = NULL, source_event = 'imported')`
  with `ON CONFLICT DO NOTHING`.
- Insert one `instrument_symbol_history` row for the **current symbol
  only**:
  `(instrument_id, symbol, effective_from = COALESCE(first_seen_at::date, CURRENT_DATE), effective_to = NULL, source_event = 'imported')`
  with `ON CONFLICT DO NOTHING`.

No synthesis from `former_names`. Re-running the script after Batch 7
ingests real symbol-change events is safe — the conflict clauses
no-op on existing rows.

## API: `/api/instruments/{symbol}/ownership-rollup`

### Coverage model — two distinct metrics

This is the v2 spec's biggest change after Codex review. Two metrics
ship in the payload, one of them drives the banner.

**Float concentration** (`pct_outstanding_known`):
- Definition: `sum(slice.total_shares) / shares_outstanding`.
- Tells the operator: "X% of the float is held by filers we have on
  record." For retail-heavy names this can legitimately be 30%
  (because retail genuinely owns 70%) and that does NOT mean ingest
  is incomplete.
- Always computable. Surfaces as an info chip ("Known filers hold
  X% of float"). Does NOT drive the banner.

**Universe coverage** (`known_filers / estimated_universe`):
- Per-category:
  - **institutions:** known = `count(distinct filer_cik)` for the
    latest 13F period; estimated = the per-instrument 13F filer
    count from EDGAR full-text search (NULL until that ingest
    pipeline lands — see #790).
  - **blockholders:** known = `count(distinct filer_cik)` for active
    13D/G chains; estimated = NULL (no canonical estimate; SEC
    doesn't publish total ≥5% holder counts per instrument).
  - **insiders:** known = `count(distinct filer_cik)` for Form 4 +
    Form 3 baseline; estimated = NULL until #790 ships the
    `instrument_named_insiders` count (DEF 14A roster size).
  - **etfs:** known = `count(distinct filer_cik)` for ETF 13F
    filers; estimated = NULL.
- Tells the operator: "We have visibility on Y of Z filers SEC says
  exist for this instrument." This is the actual ingest-completeness
  signal.

**Banner state machine** (driven by universe coverage):

Per-category state (fold over the four tracked categories — insiders,
blockholders, institutions, etfs):

**Single canonical enum:** `no_data | red | unknown_universe | amber | green`.
Used identically as the per-category state, the banner state, and the
response payload value.

| Per-category state | Condition |
|---|---|
| `no_data` | `shares_outstanding IS NULL` (overrides every per-category state) |
| `unknown_universe` | `estimated_universe IS NULL` |
| `red` | `known_filers / estimated_universe < 0.50` |
| `amber` | `0.50 ≤ known_filers / estimated_universe < 0.80` |
| `green` | `known_filers / estimated_universe ≥ 0.80` |

**Banner state is the worst-of across categories**, with this strict
priority (worst → best): `no_data > red > unknown_universe > amber > green`.
The reason `unknown_universe` ranks worse than `amber`: a category with
an estimate and 65% coverage is *known* to be partial; a category
without an estimate is *unknown*, so the operator gets less
reassurance. Promoting `unknown_universe` to merely "amber-ish" would
let one well-seeded category mask blind spots in others — Codex flagged
this on the v2 review.

| Banner state | Render |
|---|---|
| `no_data` | Red banner. "Cannot compute ownership — XBRL shares outstanding not on file. Trigger fundamentals sync." |
| `red` | Red banner. "Coverage incomplete in `{worst_category}` — do not use for investment decisions. `{known}` of `{estimated}` known filers in `{worst_category}`; `{unknown_count}` categories without an estimate." |
| `unknown_universe` | Yellow banner. "Coverage estimate not available for `{unknown_categories_list}`. Known filings represent `{pct}`% of float. Treat as best-effort until coverage expansion lands (#790)." |
| `amber` | Amber banner. "Limited coverage in `{worst_category}` — verify against SEC EDGAR for major positions." |
| `green` | Green badge. "Coverage ≥ 80% universe coverage across all four categories." |

In v1 every instrument lands in the `unknown_universe` state because no
estimates are seeded. That's the honest answer and the banner says so
explicitly. As #790 lands per-category estimates one at a time, the
banner stays in `unknown_universe` until **every** category has an
estimate; then it can finally settle into `green` / `amber` / `red`.

### Response shape

```jsonc
{
  "symbol": "GME",
  "instrument_id": 730001,
  "shares_outstanding": 448375157,
  "shares_outstanding_as_of": "2026-03-18",
  "shares_outstanding_source": {
    "accession_number": "0001326380-25-...",
    "concept": "EntityCommonStockSharesOutstanding",
    "form_type": "10-Q"
  },
  "treasury_shares": 0,
  "treasury_as_of": null,
  "slices": [
    {
      "category": "insiders",
      "label": "Insiders",
      "total_shares": 38353123,
      "pct_outstanding": 0.0856,
      "filer_count": 5,
      "dominant_source": "form4",
      "holders": [
        {
          "filer_cik": "0001767470",
          "filer_name": "Cohen Ryan",
          "shares": 36847842,
          "pct_outstanding": 0.0822,
          "winning_source": "form4",
          "winning_accession": "0001767470-25-000003",
          "as_of_date": "2025-08-12",
          "dropped_sources": [
            {"source": "13d", "accession": "0001767470-25-000001", "shares": 36847842}
          ]
        }
      ]
    },
    {"category": "blockholders", "...": "..."},
    {"category": "institutions", "...": "..."},
    {"category": "etfs", "...": "..."},
    {"category": "def14a_unmatched", "...": "..."}
  ],
  "residual": {
    "shares": 408456901,
    "pct_outstanding": 0.9112,
    "label": "Public / unattributed",
    "tooltip": "Shares outstanding minus all known regulated filings and treasury. Includes retail, undeclared institutional, and any filer outside our coverage cohort.",
    "oversubscribed": false
  },
  "concentration": {
    "pct_outstanding_known": 0.0856,
    "info_chip": "Known filers hold 8.56% of float."
  },
  "coverage": {
    "state": "unknown_universe",
    "categories": {
      "insiders":     {"known_filers": 5, "estimated_universe": null, "pct_universe": null},
      "blockholders": {"known_filers": 1, "estimated_universe": null, "pct_universe": null},
      "institutions": {"known_filers": 7, "estimated_universe": null, "pct_universe": null},
      "etfs":         {"known_filers": 4, "estimated_universe": null, "pct_universe": null}
    }
  },
  "banner": {
    "state": "unknown_universe",
    "variant": "warning",
    "headline": "Coverage estimate not available",
    "body": "Known filings represent 8.56% of float. Universe estimate per category not yet seeded (#790). Treat as best-effort."
  },
  "computed_at": "2026-05-03T14:21:09Z"
}
```

### Dedup priority + DEF 14A enrichment

**Sources:** `form4`, `form3`, `13d`, `13g`, `def14a`, `13f`.
**Priority rank** (lower = wins):

| Source | Rank | as_of column |
|---|---|---|
| `form4` | 1 | `txn_date` |
| `form3` | 2 | `as_of_date` |
| `13d` | 3 | `filed_at` |
| `13g` | 3 | `filed_at` |
| `def14a` | 4 | `as_of_date` |
| `13f` | 5 | `period_of_report` |

**Tie-break:** `priority_rank ASC, as_of_date DESC NULLS LAST,
accession_number DESC, source_row_id DESC`.

**DEF 14A enrichment** — DEF 14A holders carry `holder_name` only, no
`filer_cik`. Before they enter dedup the rollup service calls
`resolve_holder_to_filer_cik(conn, instrument_id, holder_name)` —
extracted from `def14a_drift._resolve_holder_match` into a shared
module `app/services/holder_name_resolver.py` so both consumers
share one source of truth. The resolver returns `(matched_filer_cik,
matched_via_source)`:

- If the resolver returns a CIK, the DEF 14A row enters dedup with
  `filer_cik = matched_cik`. Form 4 will win the priority race
  (rank 1 < rank 4) so the DEF 14A row will lose, but its accession
  ships in `dropped_sources` for provenance.
- If the resolver returns no CIK, the DEF 14A row goes to the
  `def14a_unmatched` slice keyed on `holder_name` (no CIK is available
  to dedup against). These are mostly named officers who appear in
  the proxy but never filed a Form 4 — rare but real.

The resolver is the same code path the existing DEF 14A drift detector
uses, so the consistency story is "if drift detection sees a match,
ownership rollup also sees it; if drift detection treats it as a
coverage gap, ownership rollup sends it to the unmatched slice."

**SQL implementation** for the `form4 ∪ form3 ∪ 13d ∪ 13g ∪ 13f` union:

**Identity key** (used for both dedup and Form 3 suppression):

```
identity := CASE
    WHEN filer_cik IS NOT NULL THEN 'CIK:' || filer_cik
    ELSE 'NAME:' || LOWER(TRIM(filer_name))
END
```

This convention follows the existing repo pattern (`def14a_drift._normalise_name`,
`get_instrument_blockholders` reporter-identity fallback). Codex v2
review caught the prior version where Form 3 suppression collapsed every
NULL-CIK Form 4 row into one bucket and over-suppressed unrelated
NULL-CIK Form 3 filers — the identity key fixes that by name-falling
back per row.

**Canonical-holder union SQL** (5 sources; DEF 14A union'd in Python):

```sql
WITH canonical_holders AS (
    -- Form 4 latest cumulative per filer
    SELECT 'form4'::text AS source, 1 AS priority_rank,
           filer_cik, filer_name,
           NULL::text AS filer_type,
           post_transaction_shares AS shares,
           txn_date AS as_of_date,
           accession_number,
           id AS source_row_id
    FROM (
        SELECT DISTINCT ON (
            CASE WHEN filer_cik IS NOT NULL
                 THEN 'CIK:' || filer_cik
                 ELSE 'NAME:' || LOWER(TRIM(filer_name)) END
        )
            filer_cik, filer_name, post_transaction_shares,
            txn_date, accession_number, id
        FROM insider_transactions
        WHERE instrument_id = %(iid)s
          AND post_transaction_shares IS NOT NULL
          AND is_derivative = FALSE
        ORDER BY
            CASE WHEN filer_cik IS NOT NULL
                 THEN 'CIK:' || filer_cik
                 ELSE 'NAME:' || LOWER(TRIM(filer_name)) END,
            txn_date DESC NULLS LAST, id DESC
    ) AS form4_latest

    UNION ALL

    -- Form 3 baseline only for filers with NO Form 4 row sharing the
    -- same identity. Identity uses CIK when present, else
    -- normalised name — so two distinct NULL-CIK officers don't
    -- silently suppress each other.
    SELECT 'form3'::text AS source, 2 AS priority_rank,
           iih.filer_cik, iih.filer_name,
           NULL::text AS filer_type,
           iih.shares, iih.as_of_date,
           iih.accession_number, iih.id AS source_row_id
    FROM insider_initial_holdings iih
    WHERE iih.instrument_id = %(iid)s
      AND iih.shares IS NOT NULL
      AND iih.is_derivative = FALSE
      AND NOT EXISTS (
          SELECT 1 FROM insider_transactions it
          WHERE it.instrument_id = iih.instrument_id
            AND it.post_transaction_shares IS NOT NULL
            AND it.is_derivative = FALSE
            AND (
                (it.filer_cik IS NOT NULL AND iih.filer_cik IS NOT NULL
                 AND it.filer_cik = iih.filer_cik)
                OR
                (it.filer_cik IS NULL AND iih.filer_cik IS NULL
                 AND LOWER(TRIM(it.filer_name)) = LOWER(TRIM(iih.filer_name)))
            )
      )

    UNION ALL

    -- 13D/G per-block (joint-filer collapse already at reader layer)
    SELECT
        CASE WHEN bf.submission_type LIKE 'SCHEDULE 13D%' THEN '13d'
             ELSE '13g' END AS source,
        3 AS priority_rank,
        COALESCE(bf.reporter_cik, f.cik) AS filer_cik,
        COALESCE(bf.reporter_name, f.name) AS filer_name,
        NULL::text AS filer_type,
        block.aggregate_amount_owned AS shares,
        block.filed_at::date AS as_of_date,
        block.accession_number,
        block.filing_id AS source_row_id
    FROM (
        -- Per-accession-block max-aggregate, mirrors the existing
        -- /blockholders rollup (joint reporters collapsed to MAX).
        SELECT DISTINCT ON (accession_number)
               filing_id, accession_number, submission_type,
               aggregate_amount_owned, filed_at, filer_id
        FROM blockholder_filings
        WHERE instrument_id = %(iid)s
          AND aggregate_amount_owned IS NOT NULL
        ORDER BY accession_number, aggregate_amount_owned DESC NULLS LAST
    ) AS block
    JOIN blockholder_filings bf ON bf.filing_id = block.filing_id
    JOIN blockholder_filers f ON f.filer_id = block.filer_id

    UNION ALL

    -- 13F latest period only, equity-only. ``filer_type`` is carried
    -- through here because the slice bucketer (institutions vs ETFs)
    -- depends on it post-dedup. Codex v2 caught that v1's UNION
    -- dropped the column.
    SELECT '13f'::text AS source, 5 AS priority_rank,
           f.cik AS filer_cik, f.name AS filer_name,
           COALESCE(f.filer_type, 'OTHER') AS filer_type,
           h.shares, h.period_of_report AS as_of_date,
           h.accession_number, h.holding_id AS source_row_id
    FROM institutional_holdings h
    JOIN institutional_filers f USING (filer_id)
    WHERE h.instrument_id = %(iid)s
      AND h.is_put_call IS NULL
      AND h.period_of_report = (
          SELECT MAX(period_of_report) FROM institutional_holdings
          WHERE instrument_id = %(iid)s
      )

    -- DEF 14A union'd in Python after the holder-name resolver runs
    -- (no filer_cik in def14a_beneficial_holdings — the resolver
    -- enriches holder_name → filer_cik before dedup). DEF 14A rows
    -- carry filer_type=NULL.
)
SELECT DISTINCT ON (
    CASE WHEN filer_cik IS NOT NULL
         THEN 'CIK:' || filer_cik
         ELSE 'NAME:' || LOWER(TRIM(filer_name)) END
)
    *
FROM canonical_holders
ORDER BY
    CASE WHEN filer_cik IS NOT NULL
         THEN 'CIK:' || filer_cik
         ELSE 'NAME:' || LOWER(TRIM(filer_name)) END,
    priority_rank ASC,
    as_of_date DESC NULLS LAST,
    accession_number DESC,
    source_row_id DESC
```

After dedup `filer_type` survives only on rows whose winning source is
`13f` (the only source where `filer_type` is meaningful). The bucketer
uses it directly:

```python
def _bucket_into_slices(survivors: list[CanonicalHolder]) -> list[OwnershipSlice]:
    insiders, blocks, instits, etfs, def14a_unmatched = [], [], [], [], []
    for h in survivors:
        if h.source in ("form4", "form3"):
            insiders.append(h)
        elif h.source in ("13d", "13g"):
            blocks.append(h)
        elif h.source == "def14a":
            # def14a winners are rare (priority 4 — only fires when no
            # higher source has the holder); they go into insiders if
            # the resolver matched a Form 4 filer, else def14a_unmatched.
            insiders.append(h)
        elif h.source == "13f":
            if h.filer_type == "ETF":
                etfs.append(h)
            else:
                instits.append(h)
        else:
            raise ValueError(f"unknown source {h.source}")
    return [...]
```

The `'__NAME__:' || filer_name` fallback handles legacy NULL-CIK rows
the same way `def14a_drift._resolve_holder_match` does — keeps them
addressable rather than collapsed into one bucket.

DEF 14A rows get unioned in Python after the SQL pass:

```python
def _enrich_and_union_def14a(
    conn: psycopg.Connection[Any],
    instrument_id: int,
    sql_survivors: list[CanonicalHolder],
) -> list[CanonicalHolder]:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            "SELECT * FROM def14a_beneficial_holdings WHERE instrument_id = %s",
            (instrument_id,),
        )
        def14a_rows = cur.fetchall()
    enriched = []
    unmatched = []
    for row in def14a_rows:
        cik = resolve_holder_to_filer_cik(conn, instrument_id, row["holder_name"])
        candidate = CanonicalHolder(
            source="def14a", priority_rank=4,
            filer_cik=cik, filer_name=row["holder_name"],
            shares=row["shares"], as_of_date=row["as_of_date"],
            accession_number=row["accession_number"],
            source_row_id=row["holding_id"],
        )
        (enriched if cik else unmatched).append(candidate)
    # Re-run dedup with DEF 14A inserted; record which ones lost.
    return _dedup(sql_survivors + enriched), unmatched
```

### Slice categorisation

| Bucket | Survivors | Notes |
|---|---|---|
| `insiders` | Form 4 + Form 3 winners | |
| `blockholders` | 13D/G winners | One block per accession (joint reporters collapse pre-dedup) |
| `institutions` | 13F winners with `filer_type IN ('INV','INS','BD','OTHER')` | Equity-only |
| `etfs` | 13F winners with `filer_type = 'ETF'` | |
| `def14a_unmatched` | DEF 14A rows the resolver couldn't match | Rare; often named officers without Form 4 |

Treasury is its own additive top wedge sourced from the
`pickLatestBalance` mirror server-side.

### Residual math + oversubscription guard

```python
def _compute_residual(outstanding, slices, treasury):
    sum_known = sum(s.total_shares for s in slices)
    raw = outstanding - sum_known - (treasury or 0)
    return ResidualBlock(
        shares=max(raw, 0),
        pct_outstanding=max(raw, 0) / outstanding,
        oversubscribed=raw < 0,
        label="Public / unattributed",
        tooltip="...",
    )
```

Frontend renders the wedge with its actual size; when
`oversubscribed=true` a red info bar shows above the chart: "Category
totals exceed shares outstanding by X. Likely cause: stale 13F
quarter combined with fresh Form 4 / 13D. Awaiting next 13F cycle."

### Reader implementation

`app/services/ownership_rollup.py`:

```python
def get_ownership_rollup(
    conn: psycopg.Connection[Any], instrument_id: int
) -> OwnershipRollup:
    # Caller (the FastAPI handler) must already be inside snapshot_read;
    # this service does not open its own transaction.
    outstanding = _read_shares_outstanding(conn, instrument_id)
    treasury = _read_treasury(conn, instrument_id)
    if outstanding is None:
        return OwnershipRollup.no_data(instrument_id)
    holders_no_def14a = _collect_canonical_holders_sql(conn, instrument_id)
    holders_with_def14a, unmatched_def14a = _enrich_and_union_def14a(
        conn, instrument_id, holders_no_def14a
    )
    survivors = _dedup_by_priority(holders_with_def14a)
    slices = _bucket_into_slices(survivors)
    if unmatched_def14a:
        slices.append(_build_def14a_unmatched_slice(unmatched_def14a, outstanding))
    residual = _compute_residual(outstanding, slices, treasury)
    coverage = _compute_coverage(slices, conn, instrument_id)
    banner = _compute_banner_state(outstanding, coverage)
    return OwnershipRollup(...)
```

### Endpoint

```python
@router.get(
    "/{symbol}/ownership-rollup",
    response_model=OwnershipRollupResponse,
)
def get_instrument_ownership_rollup(
    symbol: str,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> OwnershipRollupResponse:
    """Cross-channel deduped ownership rollup. Tier 0 of #788."""
    with snapshot_read(conn):
        instrument_id = _resolve_instrument_id(conn, symbol)
        rollup = ownership_rollup.get_ownership_rollup(conn, instrument_id)
    return _to_response_model(rollup)
```

Empty / pre-ingest state: `slices=[]`, `coverage.state='unknown_universe'`
(or `'no_data'` if `shares_outstanding IS NULL`), residual = outstanding.
200 OK with the warning banner. 404 reserved for unknown symbol. The
endpoint never returns 503 — `no_data` is a banner state, not an error.

## Frontend

### `frontend/src/api/ownership.ts`

New module — single `fetchOwnershipRollup(symbol)` function returning
the typed payload. Replaces three of the existing five fetches in
`OwnershipPanel.tsx`:

- `fetchInstitutionalHoldings` — replaced
- `fetchBlockholders` — replaced
- `fetchInsiderTransactions` + `fetchInsiderBaseline` — replaced

`fetchInstrumentFinancials("balance")` stays for non-ownership
balance-sheet rows on the L1 page; the rollup endpoint sources
`shares_outstanding` and `treasury_shares` server-side.

L2 page (`OwnershipPage.tsx`) keeps its existing fetches because it
needs the per-filer drilldown shape that the rollup endpoint
deliberately doesn't return at full granularity (top-50 only). Batch 3
expands the rollup payload with a `per_filer_detail_url` link instead
of inlining hundreds of filers.

### `OwnershipPanel.tsx`

- Drop `shares_outstanding + treasury_shares` math at L505. Denominator
  is `data.outstanding` from the rollup response.
- Drop the multi-fetch `useAsync` wiring; replace with one
  `fetchOwnershipRollup` call.
- Render the banner driven by `rollup.banner.state`:
  - `no_data` → red banner with sync-trigger CTA copy
  - `unknown_universe` → yellow banner with the explicit "Estimate
    not available" copy
  - `red` / `amber` / `green` → matching variant
- Render the residual wedge with the new label "Public / unattributed".
- Treasury renders as a category wedge on top, additive — same as today
  but with the corrected denominator.
- Concentration info chip below banner: "Known filers hold X% of float."
- Oversubscription warning bar above the chart when
  `rollup.residual.oversubscribed=true`.

### `OwnershipPage.tsx` (L2)

- Drop `shares_outstanding + treasury_shares` math at L331. Denominator
  is `outstanding` only.
- L1 wedge label fix is **out of scope** for this batch (Batch 2 #791).

### `ownershipRings.ts`

- Drop treasury-in-denominator math at the call sites.
- Update comment header to document the corrected denominator semantics.
- Cross-category oversubscription guard stays.

## Tests

### Backend

`tests/test_ownership_rollup.py` (new):

- `test_dedup_form4_beats_13d_for_same_cik` — Cohen on GME shape.
- `test_joint_filer_13d_collapses_to_one_block` — verify rollup respects
  the existing reader's per-accession MAX collapse.
- `test_13f_loses_to_13g_on_same_cik` — 13G 5%, 13F same filer 4.9%.
- `test_def14a_resolver_matches_form4_filer` — DEF 14A holder name
  resolves to a Form 4 filer_cik via the shared resolver. Form 4 wins
  priority; DEF 14A row's accession lands in `dropped_sources`.
- `test_def14a_unmatched_when_resolver_fails` — proxy-only holder with
  no Form 4 / Form 3, expect `def14a_unmatched` slice membership.
- `test_dedup_tie_break_uses_accession_then_row_id` — two rows same
  source / same as_of_date, deterministic ordering.
- `test_residual_label_and_value` — fixture with 30% known + 10%
  treasury, residual=60% with `oversubscribed=false`.
- `test_residual_oversubscribed_flag` — fixture where holders sum to
  110% of outstanding, residual=0 with `oversubscribed=true`.
- `test_coverage_state_no_data` — instrument with NULL outstanding.
- `test_coverage_state_unknown_universe_default` — no estimates seeded.
- `test_coverage_state_red_when_universe_below_50` — fixture with
  estimate=100, known=30 → red banner.
- `test_coverage_state_amber_when_50_to_80` — fixture with 65%
  universe coverage in worst category.
- `test_coverage_state_green_when_all_above_80` — fixture with every
  category-with-estimate ≥ 80%.
- `test_concentration_info_chip_always_present` — chip text reflects
  `pct_outstanding_known` regardless of banner state.
- `test_treasury_excluded_from_concentration_numerator` — fixture
  with 10% treasury, 50% known, concentration = 50% (not 60%).
- `test_snapshot_isolation_holds_under_concurrent_write` — start two
  threads, one writes a new Form 4 mid-rollup, expect rollup to see
  the pre-write snapshot.
- `test_holder_name_resolver_extraction_parity` — assert the lifted
  `holder_name_resolver.resolve_holder_to_filer_cik` returns the same
  result as the original `def14a_drift._resolve_holder_match` for a
  matrix of input names.

### Smoke / migration

`tests/smoke/test_app_boots.py` — extend with
`test_insider_initial_holdings_value_owned_column_exists` against the
live DB.

`tests/smoke/test_schema_drift.py` (new — pulls B5 of #797 forward):

For every migration in `sql/`, parse the `CREATE TABLE` blocks and
assert every declared column exists in `information_schema.columns`.
Fails fast on missing columns. Tolerates extra columns (added by later
ALTER TABLE migrations).

`tests/test_instrument_history.py` (new):

- `test_cik_history_overlap_rejected` — insert overlapping ranges,
  expect `IntegrityError` from EXCLUDE constraint.
- `test_cik_history_two_current_rejected` — two rows with `effective_to
  IS NULL` for the same instrument, expect `IntegrityError`.
- `test_cik_history_inverted_range_rejected` — `effective_to <
  effective_from`, expect `IntegrityError`.
- `test_symbol_history_same_symbol_two_instruments_allowed` — symbol
  reused across instruments at different times, expect both inserts
  succeed (PK is per-instrument).
- `test_backfill_idempotent` — run the backfill script twice, assert
  no row count change second time.
- `test_historical_ciks_for_returns_imported_row` — after backfill, the
  helper returns the seeded CIK.

### Frontend

`frontend/src/components/instrument/OwnershipPanel.test.tsx` (extend):

- Snapshot fixture with GME-shaped rollup → single Cohen insider row,
  no double-count, residual = "Public / unattributed",
  unknown_universe banner.
- Treasury-bearing fixture (AAPL synthetic): treasury wedge added,
  denominator unchanged, residual still > 0.
- Banner state machine: 5 fixtures — `no_data`, `unknown_universe`,
  `red`, `amber`, `green` — each asserts the right banner variant +
  copy.
- Oversubscription warning bar fixture: `residual.oversubscribed=true`
  asserts the warning copy renders above the chart.

`frontend/src/api/ownership.test.ts` (new): contract test against a
captured live response fixture from the dev DB.

## File touchpoints

- `sql/101_insider_initial_holdings_value_owned.sql` (new)
- `sql/102_instrument_cik_history.sql` (new — incl. btree_gist + EXCLUDE)
- `sql/103_instrument_symbol_history.sql` (new — incl. EXCLUDE)
- `app/services/ownership_rollup.py` (new)
- `app/services/holder_name_resolver.py` (new — lifted from
  `def14a_drift._normalise_name` + `_resolve_holder_match`)
- `app/services/def14a_drift.py` (refactor — import from
  `holder_name_resolver` instead of inline private helpers)
- `app/services/instrument_history.py` (new — `historical_ciks_for(iid)`)
- `app/api/instruments.py` (new endpoint, `snapshot_read` wrap)
- `scripts/backfill_instrument_history.py` (new, idempotent, current
  symbol + current CIK only)
- `tests/test_ownership_rollup.py` (new)
- `tests/test_instrument_history.py` (new)
- `tests/test_holder_name_resolver.py` (new — extraction parity)
- `tests/smoke/test_app_boots.py` (extend)
- `tests/smoke/test_schema_drift.py` (new — B5 of #797 pulled forward)
- `tests/fixtures/ebull_test_db.py` (`_PLANNER_TABLES` extended)
- `frontend/src/api/ownership.ts` (new)
- `frontend/src/api/ownership.test.ts` (new)
- `frontend/src/components/instrument/OwnershipPanel.tsx` (rewrite
  body, banner, residual relabel, oversubscription warning)
- `frontend/src/components/instrument/OwnershipPanel.test.tsx` (extend)
- `frontend/src/components/instrument/ownershipRings.ts` (drop
  treasury-in-denominator math)
- `frontend/src/pages/OwnershipPage.tsx` (denominator fix only)

## Open questions

1. ~~`estimated_universe` for institutions~~ — resolved: NULL in v1,
   filled in by #790 alongside the per-instrument 13F filer-count
   ingest. Banner reflects the unknown-universe state honestly.
2. **`shares_outstanding` source** — reuse `instrument_share_count_latest`
   view. Confirmed.
3. **Endpoint version** — single-version v1.

## Codex spec review

v2 ready for re-review:

```
codex.cmd exec resume --last "Re-review the spec at docs/superpowers/specs/2026-05-03-ownership-tier0-and-cik-history-design.md after the v2 revision. Confirm fixes for the 6 prior findings and call out any new gaps. Reply terse."
```
