# #1218 — XBRL parser out-of-window `period_end` guard

> Created: 2026-05-19. Branch `fix/1218-xbrl-parser-period-end`. Phase A.1 of
> `docs/superpowers/plans/2026-05-19-post-1208-cleardown.md` — must land
> pre-bootstrap so every retry of the companyfacts stage stops re-emitting
> junk `period_end` values.

## 1. Context

Issue #1208 Phase 3 (PR #1215, migration `sql/156`) partitioned `financial_facts_raw`
by `period_end` quarter and added a `DEFAULT` partition to absorb out-of-window
dates rather than mix them with valid historical data. Phase 4 (PR #1216,
`app/services/postgres_health.py::DEFAULT_PARTITION_WARN_ROWS = 5000`) added an
operator alarm on default-partition row count.

Pre-Phase-3 sample had `period_end = '6016-06-30'` and other pre-1900 / >2100
values that could only have come from a parser bug (XBRL `end` field
misread, year-digit overflow, or an issuer-emitted malformed context). The
retention sweep (also in #1208 P3) evicted those rows.

## 2. Spike — post-sweep state on dev DB

Dump (2026-05-19, post-PR-#1216):

```text
SELECT COUNT(*) FROM financial_facts_raw_default;
 count
-------
    42

SELECT
    COUNT(*) FILTER (WHERE period_end <  '1900-01-01')   AS pre_1900,
    COUNT(*) FILTER (WHERE period_end >= '2100-01-01')   AS post_2099,
    COUNT(*) FILTER (WHERE period_end >= '1900-01-01'
                       AND period_end <  '2100-01-01')   AS in_window
  FROM financial_facts_raw_default;
 pre_1900 | post_2099 | in_window
----------+-----------+-----------
        0 |         0 |        42

SELECT MIN(period_end), MAX(period_end) FROM financial_facts_raw;
 min        | max
------------+------------
 1995-12-31 | 2041-12-31
```

The 42 remaining default-partition rows are legitimate XBRL forward-projected
dates from concepts that the SEC taxonomy explicitly schedules into the
future:

- `OperatingLossCarryforwards` — per-year NOL expiration schedule (10-K
  / 20-F tax disclosure).
- `FiniteLivedIntangibleAssetsAmortizationExpenseAfterYearFive` —
  ASC 350 5-year amortization waterfall.
- `PrincipalAmountOutstandingOnLoansSecuritized` — securitization
  amortization schedule.
- `IncomeTaxExpenseBenefit` with `period_start=2033-01-01,
  period_end=2033-03-31` — forecast period context.

Every one of these has `period_end ∈ [2031, 2041]` — past the quarterly
partition ceiling at `financial_facts_raw_2030q4` but well inside the
`[1900, 2099]` parser-sanity window. **These are not parser bugs and must
not be evicted.**

## 3. Decisions

### 3.1 Parser-side validation window: `[1900-01-01, 2100-01-01)`

Reject any XBRL fact failing ANY of:

- `period_end < 1900-01-01` OR `period_end >= 2100-01-01`.
- `period_start` is not None AND (`period_start < 1900-01-01` OR
  `period_start >= 2100-01-01`).
- `period_start` is not None AND `period_start > period_end` (negative
  duration — the same parser-bug class typically also flips start /
  end on overflow).

`period_start` may legitimately be None — every balance-sheet
point-in-time concept (e.g. `Assets`, `CommonStockSharesOutstanding`)
emits `end` only. NULL `period_start` preserves the fact as long as
`period_end` is in-window.

- Inclusive lower bound `1900-01-01` — pre-1900 dates have no SEC
  filer use-case (EDGAR began 1993; XBRL became mandatory 2009).
- Exclusive upper bound `2100-01-01` — wide enough for every legitimate
  forward-projected schedule item observed (~75-year cushion). Tight
  enough to catch the year-6016 / digit-overflow bug class.
- Lands in `_extract_facts_from_section` at
  `app/providers/implementations/sec_fundamentals.py:315` — **the
  single chokepoint** for every XBRL ingest path:
  - per-CIK companyfacts (`extract_facts` /
    `extract_facts_and_catalog`).
  - bulk archive (`ingest_companyfacts_archive` →
    `extract_facts_from_companyfacts_payload`).
  - companyconcept primitive (`extract_concept_facts`).

  Validating here covers all three with one diff.

### 3.2 On-reject behaviour: skip + WARN log with full provenance

Mirror the existing same-block discipline (`Skipping XBRL entry for %s:
bad date format` at `sec_fundamentals.py:359`). Bump to `WARN` (was
`DEBUG`) because the year-6016 bug class is *silent* under DEBUG —
operator only sees the symptom when the alarm fires hours later.

The validator returns the *reason* (which check tripped), so the log
identifies which field caused the rejection — operator can grep
distinct reasons to triage:

```python
_REJ_END_OUT_OF_WINDOW   = "period_end_out_of_window"
_REJ_START_OUT_OF_WINDOW = "period_start_out_of_window"
_REJ_START_AFTER_END     = "period_start_after_period_end"

def _classify_period_rejection(
    period_start: date | None, period_end: date
) -> str | None:
    if not (_PERIOD_MIN <= period_end < _PERIOD_MAX):
        return _REJ_END_OUT_OF_WINDOW
    if period_start is not None:
        if not (_PERIOD_MIN <= period_start < _PERIOD_MAX):
            return _REJ_START_OUT_OF_WINDOW
        if period_start > period_end:
            return _REJ_START_AFTER_END
    return None
```

**Per-call WARN dedup on `(accession, reason)`.** A single malformed
filing can fan out across many `concept × unit_key` rows. Without
dedup, that prints hundreds of identical WARN lines per ingest tick
and drowns the signal. Mitigation lives at the log site (NOT in the
pure `_classify_period_rejection` predicate):

```python
def _extract_facts_from_section(...):
    ...
    warned_rejections: set[tuple[str, str]] = set()    # (accn, reason)
    for tag_name, fact_data in section.items():
        ...
        reason = _classify_period_rejection(period_start, period_end)
        if reason is not None:
            key = (accn, reason)
            if key not in warned_rejections:
                warned_rejections.add(key)
                logger.warning(...)
            continue
        facts.append(...)
```

The set is per-section call, so a separate companyfacts payload starts
fresh — operator still sees one WARN per accession-reason on every
ingest cycle if the bug is persistent.

Log line shape (one consistent template; reason names the failing
field; both dates included regardless of which one tripped):

```python
logger.warning(
    "XBRL parser: rejecting fact for %s/%s (taxonomy=%s, accn=%s, "
    "form=%s, filed=%s): %s — period_start=%s, period_end=%s; "
    "window [1900-01-01, 2100-01-01)",
    tag_name,
    unit_key,
    taxonomy,
    accn,
    form,
    filed_str,
    reason,             # one of the _REJ_* constants above
    start_str or "<null>",
    end_str,
)
```

Why all five provenance fields plus reason: the operator should be
able to grep one log line and immediately know (a) which CIK /
accession is bleeding, (b) which field caused it, (c) which taxonomy
section it came from, (d) whether it's a single accession or a
parser-wide regression.

### 3.3 Cleanup script — `scripts/cleanup_1218_out_of_window_facts.py`

One-shot. Not a migration (migrations are schema; data cleanup is
operator-tooling). Defensive: deletes only rows the parser guard
would now reject. **The cleanup predicate MUST mirror the parser
guard 1-to-1** — adding the negative-duration case so cleanup catches
the same shape as the parser:

```sql
period_end   <  DATE '1900-01-01'
OR period_end   >= DATE '2100-01-01'
OR period_start <  DATE '1900-01-01'
OR period_start >= DATE '2100-01-01'
OR (period_start IS NOT NULL AND period_start > period_end)
```

On dev right now this is 0 rows. Operator runs once after deploy:

```bash
uv run python scripts/cleanup_1218_out_of_window_facts.py --apply
```

Default mode prints the count + sample without deleting; `--apply`
performs the delete in a single transaction with a `RAISE NOTICE`
report. Idempotent re-run.

### 3.4 Out of scope (filed as follow-ups, not in this PR)

The 42 legitimate forward-projected rows still live on the DEFAULT
partition and contribute to the 5000-row alarm. Two follow-up options
are filed as a single tech-debt issue:

- **Option A:** extend quarterly partitions through 2050 in a future
  migration (`financial_facts_raw_2031q1` … `2050q4`, 80 leaves).
- **Option B:** redefine `DEFAULT_PARTITION_WARN_ROWS` to exclude
  legitimately-routed forward-projected rows (e.g. count only rows
  with `period_end < '1900-01-01' OR period_end >= '2100-01-01'`).

Follow-up ticket TBD — opened post-merge. Out of scope here because
neither option blocks the parser fix from landing.

## 4. Implementation

### 4.1 Files

| File | Change | LOC |
| --- | --- | --- |
| `app/providers/implementations/sec_fundamentals.py` | Add `_PERIOD_END_MIN` / `_PERIOD_END_MAX` constants; add window check inside `_extract_facts_from_section` after `date.fromisoformat`; bump log to `warning` + add provenance. | ~25 |
| `tests/test_sec_fundamentals_period_end_guard.py` | NEW. Regression test: feed extractor a synthetic section with `period_end='6016-06-30'` + a valid sibling; assert (a) bad row dropped, (b) good row kept, (c) WARN log emitted with accession. Plus boundary tests: `1899-12-31` (reject), `1900-01-01` (keep), `2099-12-31` (keep), `2100-01-01` (reject). Plus `period_start='1850-06-30', period_end='2024-06-30'` (reject — bad start). | ~120 |
| `scripts/cleanup_1218_out_of_window_facts.py` | NEW. One-shot evict + dry-run default. | ~80 |
| `.claude/skills/data-sources/sec-edgar.md` | Add §7.16 "XBRL `period_end` outside parser window" gotcha entry. | ~20 |
| `docs/review-prevention-log.md` | Add entry: "Silent parser bug fills DEFAULT partition / out-of-window range — validate at parser, fail-closed-with-WARN, single chokepoint." | ~15 |

Total ~260 LOC.

### 4.2 Test cases (regression test detail)

```python
def test_extractor_rejects_out_of_window_period_end(caplog):
    section = {
        "OperatingLossCarryforwards": {
            "units": {
                "USD": [
                    {
                        "end": "6016-06-30",                # bad
                        "val": "1000",
                        "accn": "0001234567-25-000001",
                        "form": "10-K",
                        "filed": "2025-01-15",
                    },
                    {
                        "end": "2024-12-31",                # good
                        "val": "2000",
                        "accn": "0001234567-25-000001",
                        "form": "10-K",
                        "filed": "2025-01-15",
                    },
                ]
            }
        }
    }
    with caplog.at_level(logging.WARNING):
        facts = _extract_facts_from_section(section, taxonomy="us-gaap")
    assert len(facts) == 1
    assert facts[0].period_end == date(2024, 12, 31)
    assert any("6016-06-30" in r.message for r in caplog.records)
    assert any("0001234567-25-000001" in r.message for r in caplog.records)
    assert any("period_end_out_of_window" in r.message for r in caplog.records)


@pytest.mark.parametrize(
    "end_str,kept",
    [
        ("1899-12-31", False),
        ("1900-01-01", True),
        ("2099-12-31", True),
        ("2100-01-01", False),
    ],
)
def test_period_end_boundaries(end_str, kept):
    ...


@pytest.mark.parametrize(
    "start_str,kept,reason",
    [
        ("1899-12-31", False, "period_start_out_of_window"),
        ("1900-01-01", True,  None),
        ("2099-12-31", True,  None),
        ("2100-01-01", False, "period_start_out_of_window"),
        (None,         True,  None),    # balance-sheet concept; start absent
    ],
)
def test_period_start_boundaries(start_str, kept, reason):
    # period_end = '2024-12-31' in every case so end-side is fine
    ...


def test_extractor_rejects_period_start_after_period_end(caplog):
    # start = 2025-06-30, end = 2024-12-31 — bug class where parser flips
    # start/end on a fp-context misread
    ...
    assert any("period_start_after_period_end" in r.message for r in caplog.records)


def test_extractor_rejects_far_future_year_overflow():
    # period_end='9999-12-31' edge — still rejected by < 2100 bound
    ...


def test_extractor_keeps_balance_sheet_with_null_start():
    # period_start absent (None), period_end valid — must be kept
    ...


def test_extractor_dei_taxonomy_path_validates_the_same():
    # Routing through dei vs us-gaap must not bypass the window
    # (same _extract_facts_from_section call). One bad + one good dei
    # fact; assert only good survives.
    ...
```

### 4.3 Cleanup script shape

```python
"""scripts/cleanup_1218_out_of_window_facts.py — one-shot evict of
out-of-window financial_facts_raw rows.

Default: dry-run (print count + sample, no delete). With ``--apply``,
delete in a single transaction. Idempotent; rerunnable.

Window: [1900-01-01, 2100-01-01). See spec
``docs/superpowers/specs/2026-05-19-1218-parser-period-end.md`` §3.1.
"""

import argparse, os, sys
import psycopg

WINDOW_PREDICATE = (
    "period_end   <  DATE '1900-01-01' "
    "OR period_end   >= DATE '2100-01-01' "
    "OR period_start <  DATE '1900-01-01' "
    "OR period_start >= DATE '2100-01-01' "
    "OR (period_start IS NOT NULL AND period_start > period_end)"
)

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    url = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/ebull")
    with psycopg.connect(url) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM financial_facts_raw WHERE {WINDOW_PREDICATE}")
        n = cur.fetchone()[0]
        if n == 0:
            print("0 out-of-window rows; nothing to do.")
            return 0
        cur.execute(
            f"SELECT period_end, accession_number, concept FROM financial_facts_raw "
            f"WHERE {WINDOW_PREDICATE} ORDER BY period_end LIMIT 20"
        )
        print(f"{n} rows out-of-window. Sample:")
        for row in cur.fetchall():
            print(" ", row)
        if not args.apply:
            print("Dry-run. Re-run with --apply to delete.")
            return 0
        cur.execute(f"DELETE FROM financial_facts_raw WHERE {WINDOW_PREDICATE}")
        conn.commit()
        print(f"Deleted {n} rows.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
```

## 5. Acceptance — maps to issue #1218

1. ✅ **Parser path identified** — single chokepoint
   `_extract_facts_from_section` at
   `app/providers/implementations/sec_fundamentals.py:315` covers
   per-CIK + bulk + companyconcept paths.
2. ✅ **Validation added** — `[1900-01-01, 2100-01-01)` window check
   after `date.fromisoformat` parse; WARN log with full provenance;
   row dropped before append to `facts` list.
3. ✅ **Cleanup script** — `scripts/cleanup_1218_out_of_window_facts.py`
   dry-run-default, `--apply` to commit. 0 rows on dev today (already
   swept by #1208 P3); defensive against future bleed pre-deploy.
4. ✅ **Regression test** — boundary parametrisation +
   accession-in-log assertion + bad-`period_start` case.
5. ✅ **Skill update** — `sec-edgar.md` §7.16 new gotcha entry.

## 6. Settled-decisions check

- **Provider strategy** (`docs/settled-decisions.md` §"Provider design
  rule"): providers are thin adapters; validation belongs in the
  extractor (provider boundary), not in a service-layer wrapper.
  Honoured — validation lands in `_extract_facts_from_section`, no
  service-layer surface added.
- **"Free regulated-source-only" (#532)**: SEC XBRL is the canonical
  US source; no third-party dependency added. Honoured.
- **Auditability**: log line includes accession + form + filed_date so
  operator can trace a rejection back to the exact filing. Honoured.

## 7. Prevention-log check

- `docs/review-prevention-log.md` "Missing data on hard-rule path
  silently passes" — adjacent. Here the symmetric case is "malformed
  data on best-effort path silently passes through to DEFAULT
  partition." The fix is symmetric: validate at the parser, fail-closed
  with a WARN-level signal instead of letting the bad row reach the
  table. **New entry added in §4.1 above** so this bug class is
  surfaced for future ingest work (silent-junk-fills-default-partition).

## 8. Smoke / verification plan (CLAUDE.md ETL clauses 8-12)

1. **Smoke against AAPL / MSFT / GME / JPM / HD** — re-run companyfacts
   ingest for each via the existing path (`POST /jobs/sec_rebuild/run`
   with `{"source":"sec_companyfacts_bulk"}` after deploy), confirm
   no WARN logs about out-of-window periods (these issuers are
   well-formed; sanity check that valid rows still write).
2. **Cross-source check** — synthetic test fixture above is the
   cross-source verification: SEC EDGAR Form data XML technical specs
   pin `xbrli:period` to `xs:date` which has no year bound in spec,
   but EDGAR submission rejection at filer time prevents most
   out-of-window contexts; ours is a defence-in-depth check.
3. **Backfill** — N/A. Cleanup script runs on demand; today's count
   is 0.
4. **Operator-visible figure** —
   `GET /system/postgres-health.financial_facts_raw_default_rows`
   still reads 42 post-deploy (legitimate forward-projected rows
   unchanged). Operator should NOT expect the count to drop; the win
   is "future ingests stop adding parser-junk to it."
5. **PR description** records the smoke panel + dry-run output of
   the cleanup script + commit SHA.

## 9. Risks / non-goals

- **Risk: false-positive rejection of an unusually-distant legitimate
  schedule item.** Mitigation: `2100-01-01` ceiling gives ~75 years
  beyond the most-distant value observed on dev (`2041-12-31`).
  If a future filer emits `2100-01-01+`, the WARN log surfaces it
  with full provenance and we tune the bound in a one-line follow-up.
- **Non-goal: extend quarterly partitions to 2050.** That's a separate
  tech-debt issue. This PR fixes the parser bleed; partition coverage
  of legitimate forward-projected data is orthogonal.
- **Non-goal: change `DEFAULT_PARTITION_WARN_ROWS`.** Same follow-up
  ticket as above.

## 10. Definition of done

- [ ] Validation lands; 4 boundary tests + 1 provenance-log test pass.
- [ ] Cleanup script lints + dry-runs cleanly on dev.
- [ ] `sec-edgar.md` gotcha appended.
- [ ] Prevention-log entry appended.
- [ ] PR body: `Closes #1218` on its own line.
- [ ] Codex 1a + 1b on this spec — findings addressed.
- [ ] Codex 2 on the diff before push.
- [ ] Bot review APPROVE on the most recent commit + CI green.
