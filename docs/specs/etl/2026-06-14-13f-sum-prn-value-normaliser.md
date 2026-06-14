# 13F: SUM multi-row positions + PRN filter + VALUE cutover, across all four ingest paths

Issues: #1567 (BUG — sub-manager multi-row undercount), #1566 (tech-debt — PRN + VALUE missing on legacy/rewash). One PR.

## Problem

A 13F-HR legitimately splits ONE `(cusip, putCall)` position across multiple
`<infoTable>` rows by `otherManager` / investment discretion. EdgarTools
`parse_infotable` returns every row (empirically verified: Vanguard Group
Q4-2025 accession `0000102909-26-000031` carries **7** AAPL rows, CUSIP
037833100, summing **1,426,283,914** shares; the single SOLE row is
1,279,051,701).

**All four** ingest paths collapse these keep-one instead of summing:

| Path | Locus | Defects |
|------|-------|---------|
| legacy first-ingest / quarterly sweep | `institutional_holdings.py::_ingest_single_accession` | keep-first (`setdefault`, #889) + DB `ON CONFLICT DO NOTHING` + **no PRN filter + no VALUE cutover** |
| manifest worker | `manifest_parsers/sec_13f_hr.py::_parse_13f_hr` | keep-first + DO NOTHING (has PRN + VALUE) |
| CUSIP rewash | `rewash_filings.py::_apply_13f_infotable` | keep-first (#954) + **no PRN filter + no VALUE cutover** |
| **bulk DERA dataset** | `sec_13f_dataset_ingest.py::_INSERT_FROM_STG_SQL` | **`DISTINCT ON (...) ORDER BY ctid DESC` = keep-last** (comment line 352: "last-write wins"); has PRN + VALUE |

Codex ckpt-1 correction: the bulk path is keep-last, NOT summing. On dev it is
the dominant source — `ownership_institutions_observations` holds 6.25M live 13f
rows, ~99% written by 4 quarterly bulk-archive runs (2025Q2–2026Q1); the
per-filing manifest has drained almost nothing (3.7k). So the operator-visible
undercount on dev flows through the bulk path, and a per-filing-only fix would
not move the rollup. Hence all four paths are in scope.

Impact: the SOLE row (1,279,051,701) is recorded; the other 6 rows
(147,232,213 shares, **10.3%**) are dropped on AAPL's largest holder. Voting
authority is also wrong — keep-one records one row's label; the correct
aggregate compares the SUMMED voting sub-amounts (Vanguard: summed
sole=93,437 / shared=93,504,609 / none=1,334,321,750 → dominant **NONE**, not
the kept SOLE row's label).

## Fix

### A. Shared pure helper for the three per-filing (`ThirteenFHolding`) paths

New `app/services/thirteen_f_normalise.py`:

```
VALUE_DOLLARS_CUTOVER = date(2023, 1, 3)        # single source of truth

def normalise_13f_holdings(
    holdings: list[ThirteenFHolding], *, filed_at: datetime | None
) -> list[ThirteenFHolding]:
    """Drop PRN + bad-quantity, scale pre-cutover VALUE, SUM-aggregate.

    1. Drop rows where shares_or_principal_type.strip().upper() != 'SH'
       (bond principal). (parse_infotable already defaults blank/unknown
       Type -> 'SH', so blanks never reach here as PRN.)
    2. Drop SH rows with shares_or_principal is None or <= 0 (malformed;
       mirrors bulk #1433 guard). parse_infotable already drops both-zero.
    3. If filed_at and filed_at.date() < VALUE_DOLLARS_CUTOVER: value_usd x1000.
    4. Aggregate by (cusip, exposure) where exposure = put_call or 'EQUITY':
         SUM shares_or_principal, value_usd, voting_sole/shared/none.
         Keep cusip/name/title/put_call from first row in group.
         investment_discretion -> None if the group mixes labels, else kept.
    Returns one holding per (cusip, exposure), first-seen order.
    """
```

Pure / DB-free → exhaustively table-testable. Lives in the **service** layer
(settled decision: providers stay thin; `ThirteenFHolding` docstring already
says "service layer applies any conversion"). Imports `ThirteenFHolding` from
the provider.

**Aggregation key** is `(cusip, exposure)` — for 13F a position is one CUSIP;
all 7 Vanguard rows share CUSIP 037833100. The DB unique key and observation
identity are `(…, instrument_id, exposure)`; a cross-CUSIP→same-instrument
collapse inside one accession (two share classes mapped to one eBull instrument)
is vanishingly rare for 13F and remains governed by the DB key. The bulk path
groups by `instrument_id` (post-resolution) so it handles that edge as a bonus;
the per-filing helper logs a WARN if it detects two CUSIPs in one accession
resolving to the same instrument (diagnostic, no behaviour change).

**Voting**: SUM the three sub-amounts, then derive ONCE via the existing
`dominant_voting_authority` (SOLE wins ties, then SHARED). No new tie rule in
tests.

### B. Per-filing call sites

Each: parse → `normalise_13f_holdings(...)` → resolve CUSIP → write. The
per-path keep-first `setdefault` machinery is deleted (the helper guarantees one
row per `(cusip, exposure)`, so the #954 "two layers diverge" risk disappears —
both the typed table and observations now receive the identical summed row).

- **`institutional_holdings.py`**: drop setdefault loop; add normaliser; gains
  PRN + VALUE. `skipped_non_sh` counted from the pre-normalise input for the
  ingest-log line.
- **`_upsert_holding`**: `ON CONFLICT DO NOTHING` → `ON CONFLICT
  (accession_number, instrument_id, (COALESCE(is_put_call,'EQUITY'))) DO UPDATE
  SET shares/market_value_usd/voting_authority/filed_at/period_of_report =
  EXCLUDED.*`. Fixes Codex #1: re-ingest of an accession that already holds
  keep-first rows now corrects the typed table (and rowcount counts the update,
  so ingest-log `inserted` is honest). Idempotent — EXCLUDED is the summed value,
  identical each run (not additive). Migration 090 index is non-partial, so the
  explicit expression conflict target is valid.
- **`sec_13f_hr.py`**: replace inline PRN + VALUE + setdefault with the
  normaliser (PRN/VALUE behaviour-preserving, SUM behaviour-fixing). Manifest is
  the path `sec_rebuild` drives, so it MUST be corrective — `_upsert_holding`
  DO UPDATE covers the typed table.
- **`rewash_filings.py`**: drop setdefault; add normaliser; gains PRN + VALUE.
  Keeps DELETE-then-INSERT (it already clears typed + observation rows by
  accession). **All-PRN edge (Codex #6)**: if `parse_infotable` returns rows but
  normalisation empties them (entirely PRN/bad-quantity), do NOT raise
  `RewashParseError("parser regression")` — treat as a legitimate all-PRN
  filing: DELETE prior typed + obs rows for the accession, log success, bump
  parser_version. Distinguish from genuine empty-parse.

### C. Bulk path SUM (`sec_13f_dataset_ingest.py`)

Replace keep-last with a true aggregate; stage the raw voting sub-amounts so the
derived label is consistent with the helper.

- `_stg_13f`: drop `voting_authority TEXT`; add `voting_sole NUMERIC(24,4)`,
  `voting_shared NUMERIC(24,4)`, `voting_none NUMERIC(24,4)`.
- `_map_voting_authority` → `_read_voting_components(row) -> (sole, shared,
  none)`; `_build_copy_row` + `_STG_COPY_COLUMNS` + the INFOTABLE loop stage the
  three amounts.
- `VALUE_DOLLARS_CUTOVER`: import from the new module (delete the local
  duplicate — SSOT). PRN filter and `shares <= 0` guard unchanged.
- `_INSERT_FROM_STG_SQL`: `DISTINCT ON … ctid DESC` → `GROUP BY instrument_id,
  filer_cik, ownership_nature, source, source_document_id, period_end,
  exposure_kind`, with `SUM(shares)`, `SUM(market_value_usd)`, `SUM` of the three
  voting components fed through a CASE that mirrors `dominant_voting_authority`
  (cross-referenced in a comment + pinned by a parity test). Constant-per-group
  columns (filer_name, filer_type, source_url, filed_at, source_accession,
  ingest_run_id, period_start) via `max()`. `ON CONFLICT … DO UPDATE` unchanged.

## Tests

- `tests/test_thirteen_f_normalise.py` (pure, not db): PRN drop, blank-type kept,
  bad-quantity drop, VALUE boundary (pre/post/on cutover, filed_at None), SUM
  aggregate, voting-sum→dominant, PUT/CALL/EQUITY kept as separate exposures,
  empty list, single-row passthrough, discretion-mix→None.
- **Golden fixture**: trimmed real Vanguard infotable (7 AAPL rows + 1 PRN row +
  1 synthetic pre-cutover row) → AAPL EQUITY sums to 1,426,283,914, PRN dropped,
  voting=NONE. Cross-source: SEC EDGAR direct (the accession).
- **Voting parity** (pure): feed the 7-row voting fixture through the Python
  helper AND the bulk CASE (extracted as a tiny pure SQL-mirror fn or asserted
  via a db test on `_stg_13f`) → identical `(shares, value, voting)`.
- **Superset guard** (`tests/test_13f_normalise_applied_everywhere.py`): assert
  each of the three per-filing modules calls `normalise_13f_holdings`
  (AST/grep). Pins #1566's "every path applies the same normalisation"
  (prevention-log L1190).
- **Bulk db test** (`-m db`): COPY a multi-row + PRN + pre-cutover staging set,
  run the drain, assert one summed observation row.

## Backfill (post-merge, operator)

13F retention = 8 quarters; pre-2023 VALUE rows mostly out-of-cap, so #1566's
VALUE blast radius ≈ in-cap PRN rows. #1567's SUM affects every multi-sub-manager
filer in-cap (Vanguard, BlackRock, State Street, …).

1. Restart jobs proc onto new main.
2. **Bulk** is the dev source of truth → re-run the in-cap quarterly bulk
   archives through the corrected drain (the operator-visible fix). Then
   `POST /jobs/sec_rebuild/run {"source":"sec_13f_hr"}` (or filer-scoped for the
   verify pass) for the per-filing path.
3. Wait for drain; verify `/instruments/{AAPL,GME,MSFT,JPM,HD}/ownership-rollup`.
4. Cross-source: Vanguard AAPL EQUITY ≈ 1.426B vs SEC EDGAR direct.

No bulk-vs-per-filing overwrite hazard once both sum: observation identity is
per-accession `(instrument, filer_cik, period_end, source_document_id,
exposure_kind)`; whichever path writes an accession last writes the same summed
figure.

## Prevention

Extract to `docs/review-prevention-log.md` + the data-engineer skill: "a
multi-row source position (13F sub-manager splits, multi-lot anything) must be
SUM-aggregated at EVERY ingest locus — Python per-row paths AND the bulk
COPY/SQL drain — and the aggregation key + voting/secondary-field derivation must
be identical across paths or the layers silently disagree." (Generalises L1190.)
