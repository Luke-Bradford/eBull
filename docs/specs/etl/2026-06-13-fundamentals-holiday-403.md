# SEC daily-index 403 on federal holidays — fundamentals_sync wedge fix

Issue: #1612. Area: filings ETL / fundamentals provider.

## Problem

`fundamentals_sync` phase 1 fails every run since ≥2026-06-11. The
30-day master-index lookback (`LOOKBACK_DAYS=30`) in `plan_refresh`
requests `master.20260525.idx` (Memorial Day). SEC's Archives host
returns **403, not 404**, for a daily-index file that does not exist.
`SecFilingsProvider.fetch_master_index` tolerates 403 only for
weekends and the not-yet-published current day; a **past weekday
federal holiday** falls through to `resp.raise_for_status()` and the
exception propagates up through `plan_refresh` → `daily_financial_facts`
→ phase 1. The per-day watermark is only written on success, so the
holiday date is re-requested every run and wedges the job indefinitely.
Upcoming holidays (Juneteenth, Independence Day, Labor Day, …) each
re-wedge it.

## Empirical grounding (dev, app User-Agent `eBull luke.bradford@…`)

| date | classification | HTTP |
|---|---|---|
| 2026-05-25 Memorial (Mon) | fed holiday | 403 |
| 2025-06-19 Juneteenth (Thu) | fed holiday | 403 |
| 2025-10-13 Columbus (Mon) | fed-closed, NYSE-**open** | 403 |
| 2025-11-11 Veterans (Tue) | fed-closed, NYSE-**open** | 403 |
| 2026-05-26 (Tue) / 2025-10-14 (Tue) | business day | 200 |
| 2026-05-23 (Sat) | weekend | 403 |

Conclusion: SEC EDGAR daily-index publishing follows the **US federal**
calendar (Columbus + Veterans included) — *not* NYSE. So the correct
gate is `pandas.tseries.holiday.USFederalHolidayCalendar`, which already
emits the observed-day shifts (e.g. 2026-07-03 for Independence Day).

## Fix

Two SEC daily-index 403 sites share the identical bug + the identical
docstring punt ("US federal holidays … not enumerated"). Fix both with
one shared helper (Codex ckpt-1 finding 2):

1. `SecFilingsProvider.fetch_master_index` (`sec_edgar.py`) — wedges the
   fundamentals 30-day watermark (the reported failure).
2. `read_daily_index` (`sec_daily_index.py`) — `sec_daily_index_reconcile`
   reads *yesterday* only; it doesn't wedge a watermark but false-fails
   (verdict=attention) the day after every federal holiday. Same root
   cause, same fix.

In each `status_code == 403` branch: after the existing weekend check,
add a **federal-holiday** check. If `target_date` is a US federal
holiday → log + `return None` (resp. empty iterator), identical to the
weekend branch. `fetch_master_index`'s planner loop already `continue`s
on `None`, so the holiday day is skipped, no watermark is written for it
(correct — no file exists), and the rest of the window proceeds.

Anything that is a 403 on a **past business weekday** still raises (resp.
the iterator path raises) — that remains a genuine SEC block (UA /
rate-limit / WAF) and must surface.

### Shared helper

New module `app/providers/implementations/sec_calendar.py`:
`is_us_federal_holiday(d: date) -> bool`, imported by both providers
(single source of truth — no duplicated calendar).

- Backed by `USFederalHolidayCalendar().holidays(start, end)` over the
  calendar year of `d`, cached per-year in a module-level dict (the
  30-day planner loop calls it up to 30×/run; the holiday set is tiny
  and immutable per year).
- Returns True iff `d` is in that year's observed federal-holiday set.
  `USFederalHolidayCalendar.holidays()` already returns **observed**
  dates (a Saturday holiday → prior Friday, Sunday → following Monday),
  so a weekday observed date is matched directly and a Sat/Sun nominal
  holiday is already handled by the weekend branch upstream.

### Dependency

`pandas` is already installed (transitively, `Required-by: edgartools`,
a declared core dep). This fix imports `pandas.tseries.holiday`
directly, so pandas becomes an **explicit** dependency. Add it via
`uv add pandas` so `pyproject.toml` **and** `uv.lock` stay consistent
(Codex ckpt-1 finding 1 — a pyproject-only edit would leave the lock
inconsistent for `--frozen`/sync checks; pandas is already resolved in
the lock so the version does not change). Tradeoff vs a hand-rolled
federal-holiday calendar: pandas' calendar is battle-tested and computes
observed-day shifts correctly; a hand-rolled version is ~40 LOC of
fiddly fixed-vs-floating + weekend-observed date math (bug surface) for
zero benefit when the library is already present.

### Codex ckpt-1 finding 3 — REBUTTED

`-m "not db"` is correct: `tests/conftest.py:319
pytest_collection_modifyitems` applies the `db` marker **dynamically**
at collection (`item.add_marker("db")`); it is not a statically-declared
pyproject marker. CLAUDE.md documents `pytest -m "not db"` as the
fast-tier command. Codex inspected only the static pyproject markers.

## Tests (pure, no DB — `tests/test_sec_edgar*.py` or sibling)

A `respx`/transport-mocked or monkeypatched `_http_tickers.get` returning
403 for:

1. Memorial Day 2026-05-25 (Mon) → `fetch_master_index` returns `None`.
2. Juneteenth 2025-06-19 → `None`.
3. Columbus Day 2025-10-13 → `None` (NYSE-open, proves federal-not-NYSE).
4. Veterans Day 2025-11-11 → `None`.
5. Observed Independence Day 2026-07-03 (Fri, July 4 is Sat) → `None`.
6. **Past business weekday** (e.g. 2026-05-26 Tue) 403 → **raises**
   `HTTPStatusError` (the block case must still surface — guards against
   over-tolerating).
7. Weekend 403 (existing behaviour) → `None` (regression guard).
8. `_is_us_federal_holiday` unit table: the 11 federal holidays for
   2025 + 2026 incl observed shifts → True; adjacent business days → False.

## Out of scope

- Other SEC fetch paths that `raise_for_status()` on 403 (submissions,
  companyfacts) — those are CIK-keyed, not date-keyed, and do not wedge
  on a calendar date. No change.
- The `sec_daily_index.py` sibling has the same docstring punt
  (line ~171). Folding it in is a follow-up if its 403s also wedge a
  watermark; out of scope here (different job, no observed failure).

## Definition of done

- `uv run pytest -m "not db"` green incl new cases.
- Re-run `POST /jobs/fundamentals_sync/run` on dev → phase 1 SUCCESS
  (no 403 raise), watermarks advance across the holiday. Record the run
  outcome + commit SHA in the PR.
- ruff + pyright clean.
