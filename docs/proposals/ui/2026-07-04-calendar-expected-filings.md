# Calendar — surface expected filings (#1907)

## Problem
`/calendar` shows market-status pills + ex-dividends + a footnote saying filing
dates aren't ingested. That footnote is **stale**: the `expected_filings` poller
(#1788/#677) populates 10-Q/10-K due-windows per held/watched instrument, and the
calendar doesn't read the table.

## Full-population verification (dev DB, 2026-07-04)
`expected_filings` has 3 unfulfilled upcoming rows, all for held positions:
BBBY 10-Q (2026-07-30 → 08-24), IEP 10-Q (07-30 → 08-24), GME 10-Q
(08-31 → 09-25). The poller scopes to relevant instruments, so 3 rows is honest,
not thin. Ex-dividends for the 6 held names (WDC/BBBY/IEP/GME/QQQ/VOO): **0
upcoming** — verified by replicating the endpoint query. Genuinely empty, NOT a
filter bug (QQQ/VOO have 0 `dividend_events` rows at all — an ETF-dividend ingest
gap, out of scope; filed separately).

## Source rule
`expected_window_start/end` is the poller's heuristic polling window
(`app/jobs/expected_filings_poller.py` `_WINDOW_OFFSETS` / `next_form_and_window`):
predicted next period-end + offsets anchored on SEC statutory filing deadlines
(Form 10-Q ~40-45d after quarter-end; 10-K ~60-90d after FY-end), padded for
fiscal drift. It is NOT a complete legal due-date (no NT/late-filing extensions),
so the UI labels it "expected" as a date *range* ("expected 30 Jul – 24 Aug"),
never a "deadline" or a fake exact date. `expected_filing_type` is the literal
form ("10-Q"/"10-K").

## Scope parity fix (Codex ckpt-1)
`_scope_instruments` portfolio/all legs currently `JOIN positions` (ALL rows),
but a closed position persists in `positions` with `current_units = 0` (dev:
WDC). So the calendar leaks closed holdings into market-status + ex-dividends
today, and would leak stale expected_filings too. The poller itself scopes to
`positions WHERE current_units > 0` (`expected_filings_poller.py:148`). Tighten
the portfolio + all legs to `current_units > 0` for parity — a pre-existing
correctness fix that drops closed names from every calendar event type.

## Scope (this PR)
Items 1–3 of #1907, in the existing layout idiom:
1. **Surface expected filings.** New `expected_filings` list on
   `GET /calendar/events`, scope-filtered, `fulfilled_at IS NULL AND
   expected_window_end >= today`, ordered by `expected_window_start`. Mirrors the
   ex-dividends pattern (unbounded upcoming, independent of the `days` horizon —
   the horizon governs only the market-status grid; corporate events look
   further out, e.g. GME is ~8 weeks away).
2. **Ex-dividends.** No code change — verified correct above. The footnote is
   updated to reflect that filings now ARE shown.
3. **Earnings.** Forward *earnings* dates remain unmodelled — expected filing
   due-windows are NOT earnings dates (earnings often land earlier via a release
   / 8-K). No new paid provider (settled #609 posture). The footnote keeps an
   explicit earnings caveat.

Item 4 (week-grid re-layout) defers to the visual-system ticket #1908 — this PR
adds an "Expected filings" `Section` alongside the existing sections rather than
re-architecting the page.

### Backend — `app/api/calendar.py`
- New model `UpcomingExpectedFiling { symbol, instrument_id, filing_type,
  window_start, window_end }`.
- Add `expected_filings: list[UpcomingExpectedFiling]` to `CalendarEvents`.
- One query gated on the scope `instrument_ids` (same list already resolved for
  ex-dividends), guarded by the `if instrument_ids:` block.
- Update the module docstring (lines 16-17) — filing due-windows are now
  surfaced; only forward *earnings* remain unmodelled.

### Frontend
- `types.ts`: `UpcomingExpectedFiling` + field on `CalendarEvents`.
- `CalendarPage.tsx`: an "Expected filings" `Section` (empty-state honest),
  rows `SYMBOL … 10-Q · expected 30 Jul – 24 Aug`. Footnote narrowed to earnings
  only. Dark-mode + tabular-nums per conventions.

## Tests
- **db-tier gate test** (`ebull_test_conn`) pinning the exact SQL gate: seed one
  open-position instrument with a valid upcoming row + a fulfilled row + a
  past-window row, a closed-position (`current_units=0`) instrument with a valid
  row, and an out-of-scope instrument with a valid row; call `calendar_events`
  and assert ONLY the open-position valid row surfaces. Covers fulfilled_at,
  window_end < today, closed-position, and out-of-scope exclusion in one test.
- CalendarPage: extend the existing test's mock with `expected_filings` and
  assert the section renders a row.

## Out of scope
Week-grid re-layout (#1908). ETF/dividend ingest coverage (separate ticket).
Forward earnings source.
