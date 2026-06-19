# Sector peer-grouping — consume #1634 GICS sector as the filter/peer dimension (#1675)

## Problem
`instruments.sector` is an opaque 1–9 code with no GICS/SPDR meaning. SPY/XLE/XLF/XLK
(ETFs, no SIC) and JPM all sit in code `4`; AAPL in `3`. Every surface that groups or
filters on `i.sector` therefore produces garbage peer sets. #1634 shipped the real
SIC→GICS-sector→SPDR crosswalk (`resolve_sector_spdr`) but display-only. This switches
the **filter / peer-grouping dimension** to that crosswalk.

## Source rule
Sector classification is fixed by the #1634 crosswalk
(`docs/specs/metrics/2026-06-18-sic-gics-spdr-crosswalk.md`): SEC 4-digit SIC
(`instrument_sec_profile.sic`) → one of 11 GICS sectors → its State-Street sector SPDR.
`resolve_sector_spdr` is the single source of truth; this ticket adds a SQL projection of
the SAME `_CROSSWALK` (generated, not hand-duplicated) so backend filtering matches the
Python resolver byte-for-byte. No new classification logic, no new source.

## Full-population verification (dev, 12,547 instruments)
- `instrument_sec_profile.sic` is `text`, all values pure 4-digit numeric (incl. leading
  zeros e.g. `0100`→100, matches Python `int('0100')`). SQL `btrim(sic)` + `~ '^[0-9]+$'`
  guard + `::int` is a faithful mirror of `int(str(sic).strip())`.
- Crosswalk resolves 5,250 / 12,547 (41.8%); the rest are ETFs / foreign non-filers with
  no SIC → `None` (no peer set), by #1634 design. GICS dist sane: XLV 995, XLF 899,
  XLI 846, XLK 758, XLY 536, XLB 359, XLRE 242, XLP 167, XLC 161, XLE 159, XLU 128.
- Opaque-code garbage confirmed: SPY/XLE/XLF/XLK→opaque `4` (sic None→`None`), JPM opaque
  `4` sic 6021→**XLF**, AAPL opaque `3` sic 3571→**XLK**, XOM opaque `1` sic 2911→**XLE**.

## Premise falsified: NO stored/indexed column needed
At 12.5k instruments a `LEFT JOIN instrument_sec_profile` + a CASE-range resolve is a
sub-ms seq scan. Live SQL resolve keeps #1634's deliberate persist-nothing posture (no
migration, no backfill, no drift-on-SIC-change, no re-sync). A materialized column would
buy nothing and reintroduce drift.

## Two-path resolve (Codex ckpt-1 #2/#3)
- **Display** (`gics_sector` on every list/ranking row): the items query ALWAYS
  `LEFT JOIN instrument_sec_profile p` and selects `p.sic`; the Python row-builder calls
  `resolve_sector_spdr(row["sic"])` (same pattern as the detail endpoint, instruments.py
  ~3391). Pure-Python per row — no SQL CASE involved. Unconditional join (PK join, trivial).
- **Filter** (`?sector_spdr=XLK`): SQL-side resolution required for correct pagination, so
  the WHERE predicate uses the generated CASE `<case> = %(sector_spdr)s` over `p.sic`.
  Both display and filter read the SAME `_CROSSWALK`.

## SQL guard ≠ Python int() leniency — documented bounded divergence (Codex ckpt-1 #5)
Python `int(str(sic).strip())` also accepts `+0100` / `1_000`; SQL `btrim(sic) ~ '^[0-9]+$'`
rejects them. This divergence is unreachable: SEC SIC is canonically a bare 4-digit code
(EDGAR `assigned-sic`), never signed/separated — dev pop is 100% pure 4-digit. `^[0-9]+$`
is the correct SIC domain; the parity test asserts SQL≡Python over digit-string inputs
(where they agree exactly) plus null/blank/non-numeric (both → no match).

## API contract decision
**Add a new `sector_spdr` query param** (value = SPDR symbol, e.g. `XLK`) to `/instruments`
and the rankings endpoint (`/rankings`, served by `app/api/scores.py`). Leave the legacy
opaque `sector` param accepted (non-breaking) but mark it deprecated in the docstring; the
FE stops using it. Rationale: opaque `sector` has zero operator value; a clean explicit
dimension beats overloading the old param. The opaque `sector` field stays in responses
(SummaryStrip falls back to it when GICS is null).

## Changes

### Backend
1. `app/services/sector_classification.py` — `sector_spdr_case_sql(sic_col: str) -> str`
   generating a SQL `CASE` from `_CROSSWALK` in first-match order, safe text→int cast.
   Inputs are module constants (range ints + SPDR keys validated ∈ `SPDR_SECTORS`); the
   returned fragment embeds no user input. Pure helper, table-tested against
   `resolve_sector_spdr` over the full 0–9999 SIC space.
2. `app/api/instruments.py` `list_instruments` — add `sector_spdr` param; when set, join
   `instrument_sec_profile p` + WHERE `<case(p.sic)> = %(sector_spdr)s` on BOTH count and
   items (conditional join, mirroring the existing dividend/coverage pattern). Surface
   `gics_sector` + `sector_spdr` on `InstrumentListItem` for display (cheap — already
   joined when filtering; resolve in Python from the row's sic otherwise).
3. `app/api/scores.py` `list_rankings` — same `sector_spdr` param + conditional join +
   predicate. Add `gics_sector` to the ranking item so the table column shows the GICS
   label, not the opaque code.

### Frontend
4. `src/api/types.ts` — `RankingItem` + instrument list item gain `gics_sector`
   (+`sector_spdr` on list item), nullable to mirror backend.
5. `src/api/rankings.ts` / `src/api/instruments.ts` — query types gain `sector_spdr`;
   fetchers set the `sector_spdr` param.
6. shared FE const `SECTOR_SPDRS` (SPDR symbol → GICS label) mirroring `SPDR_SECTORS`,
   for the fixed 11-option dropdowns.
7. `src/components/rankings/RankingsFilters.tsx` + `src/pages/InstrumentsPage.tsx` — Sector
   dropdown becomes a fixed 11-option GICS list (label=GICS name, value=SPDR symbol) bound
   to the `sector_spdr` filter; drop the data-derived opaque-code dropdown.
8. `src/components/rankings/RankingsTable.tsx` — sector cell renders `gics_sector ?? "—"`.
9. `src/components/instrument/RightRail.tsx` — `PeerSnapshot` takes `sectorSpdr` (filter)
   + `sectorLabel` (GICS, for the title); fetches `/scores?sector_spdr=`.
10. `src/pages/InstrumentPage.tsx` — pass `identity.sector_spdr` + `identity.gics_sector`
    to RightRail.

## Tests
- Pure-logic: `sector_spdr_case_sql` SQL CASE evaluated (via the resolver's expected
  output) matches `resolve_sector_spdr` across all SIC 0–9999 — the generated SQL and the
  Python resolver cannot drift. (Eval the CASE semantics in Python by reusing `_CROSSWALK`,
  OR a DB-backed test inserting fixture sics and asserting the filter returns the right set.)
- FE: RightRail passes `sector_spdr`; dropdowns offer 11 fixed options.

## DoD / dev-verify
- Filter `/instruments?sector_spdr=XLK` and `/scores?sector_spdr=XLF` return only
  XLK/XLF-resolved instruments (spot-check JPM∈XLF, AAPL∈XLK, SPY∉any).
- RightRail on AAPL shows "Peer snapshot · Information Technology" with real XLK peers.
- No backend metric/job change → no jobs-proc restart, no backfill, no sec_rebuild.
