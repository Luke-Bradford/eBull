# Tax & CGT read endpoints (#1905, PR1 of 2)

Build priority #8. The UK tax engine (`app/services/tax_ledger.py`) is fully built —
`ingest_tax_events` → `tax_lots`, `run_disposal_matching` → `disposal_matches` +
`s104_pool`, HMRC same-day / bed-&-breakfast / s104 matching, era-dependent CGT
scenario rates (owned by `_cgt_rates_for_disposal`; do NOT restate them here),
£3,000 annual exempt (`ANNUAL_EXEMPT`) — but has **zero API and zero UI**. This PR ships
the read endpoints; the page (#1905 PR2) consumes them.

## Scope (PR1 — backend only)

Four read-only endpoints under a new `/tax` router, operator-auth (mirror `app/api/budget.py`).
All wrap existing / thin-new service functions. No writes, no commit boundary.

- `GET /tax/summary?tax_year=YYYY/YY` — wraps `tax_year_summary`. The exempt maths lives in
  the **service** (source-rule: no tax treatment in the handler): `TaxYearSummary` gains
  `annual_exempt_gbp` (= `ANNUAL_EXEMPT`) + `exempt_remaining_gbp`
  (= `max(0, annual_exempt - max(net_gain, 0))`), computed in `tax_year_summary`.
- `GET /tax/disposals?tax_year=YYYY/YY` — new `disposals_for_tax_year(conn, ty)`:
  every `disposal_matches` row for the year INNER-joined to `instruments.symbol`,
  ordered `disposal_uk_date ASC, match_id ASC`. Keeps full engine provenance
  (`disposal_tax_lot_id`, nullable `acquisition_tax_lot_id`, `matched_at`).
- `GET /tax/pools` — new `s104_pool_rows(conn)`: every `s104_pool` row INNER-joined to
  `instruments.symbol`, ordered `pool_cost_gbp DESC`.
- `GET /tax/tax-years` — `{current, available[]}` for the FE selector.
  `available_tax_years(conn)` = `DISTINCT tax_year` across `tax_lots ∪ disposal_matches`,
  sorted DESC, with the current year always included. Empty tables ⇒ `[current]`.

`tax_year` query param: default = current UK tax year. Validation is **two-stage**:
`^\d{4}/\d{2}$` shape (422 on malformed) **and** semantic — the two-digit suffix must equal
`(start_year + 1) % 100` (rejects impossible years like `2026/99`), start_year in
`[MIN_SUPPORTED, current+1]` where `MIN_SUPPORTED` is derived from the engine's CGT rate
table (`_CGT_RATE_PERIODS[0]` = 2024/25). A year the engine can't price is a clean 422, not a
later `_cgt_rates_for_disposal` 500. `valid_tax_year(ty) -> bool` lives in the service; the
handler raises 422.
New public helper `current_tax_year()` wraps `_compute_tax_year(_to_uk_date(_utcnow()))`.

Each handler wraps its (multi-query) read in `snapshot_read(conn)` for a consistent snapshot.

## New service surface (`tax_ledger.py`)

- `current_tax_year() -> str`
- `valid_tax_year(tax_year: str) -> bool`
- `available_tax_years(conn) -> list[str]`
- `TaxYearSummary` gains `annual_exempt_gbp`, `exempt_remaining_gbp`.
- `@dataclass(frozen) TaxDisposalRow` (match_id, instrument_id, symbol, matching_rule,
  matched_units, acquisition_cost_gbp, disposal_proceeds_gbp, gain_or_loss_gbp,
  disposal_uk_date, tax_year, disposal_tax_lot_id, acquisition_tax_lot_id, matched_at)
  + `disposals_for_tax_year(conn, tax_year) -> list[...]`
- `@dataclass(frozen) S104PoolRow` (instrument_id, symbol, pool_units, pool_cost_gbp,
  pool_avg_cost_gbp, updated_at) + `s104_pool_rows(conn) -> list[...]`

## Source rule

CGT figures come **only** from the settled engine — no re-derivation. Same-day / B&B / s104
priority, 18/24 % rates, £3,000 exempt, YYYY/YY tax-year convention are all owned by
`tax_ledger.py` (per `docs/tax-engine.md`). Endpoints are a passive read of engine output.

## Data reality in dev (verification note)

Dev DB currently has 1 fill / 1 order and **empty** `tax_lots`/`disposal_matches`/`s104_pool`:
`daily_tax_reconciliation` can't ingest because there is no `fx_rates` USD→GBP row for the
fill date (FX unreachable in-loop). So dev verification confirms **well-formed empty/zero
payloads** (correct behaviour for a demo account with no completed disposals): `/tax/summary`
returns an all-zero `TaxYearSummary` for `2026/27` (crash-safe via COALESCE), `/tax/disposals`
and `/tax/pools` return `[]`. Non-zero figure verification is deferred to when a disposal +
FX rate exist (tracked in the PR + on #1905).

## Out of scope (PR2)

Tax page (year selector, exempt gauge, disposal table, s104 table), CSV export.
