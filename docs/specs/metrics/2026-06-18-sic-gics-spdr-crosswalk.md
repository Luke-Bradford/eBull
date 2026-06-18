# #1634 — curated SIC → GICS-sector → SPDR crosswalk (substrate + display)

Status: spec. Follows #591 (cut sector-relative views: `instruments.sector` is an
opaque 1–9 code). Operator scope (2026-06-18): **substrate + display**, pure
read-path. Sector-relative beta in `risk_metrics` and real-sector peer-grouping
are filed follow-ups (the former touches the shipped versioned evidence layer +
needs a backfill).

## Source rule

- **`instruments.sector` (1–9) is unusable** — verified: SPY/XLE/XLF/XLK/JPM all
  `4`, AAPL `3`, MSFT `8`; 3293 of 12546 tradable are NULL; `industry` is 100%
  NULL. Not a GICS/SPDR crosswalk.
- **The real classification is SEC SIC** on `instrument_sec_profile.sic` (+
  `sic_description`). SIC is the SEC's own industry taxonomy assigned to every
  operating filer (5249/12546 tradable have one; 389 distinct codes). SIC's
  4-digit division/major-group structure is the documented, public mapping basis
  (SEC SIC code list). ETFs/SPDRs and non-filers have **no SIC** (SPY/XLK/XLF
  return NULL) — they are the sector *targets*, mapped by known ticker.
- **The 11 sector SPDRs are GICS-based**, already ingested by #591 PR-A
  (`BENCHMARK_SYMBOLS`): XLB Materials, XLC Communication Services, XLE Energy,
  XLF Financials, XLI Industrials, XLK Information Technology, XLP Consumer
  Staples, XLRE Real Estate, XLU Utilities, XLV Health Care, XLY Consumer
  Discretionary.
- **Honest limitation (caveat, not fabrication):** GICS is proprietary
  (MSCI/S&P) and **not 1:1 with SIC** — the crosswalk is a documented best-effort
  SIC→GICS-sector approximation, labelled as such, never presented as
  authoritative GICS. Marginal groups (misc-electrical 369x, wholesale 50–51xx)
  are assigned to their plurality GICS sector. A SIC with no confident mapping,
  or an instrument with no SIC, resolves to **None** (no sector view — honest
  gap, mirrors the #591 degrade), never a guessed sector.

## Approach — deterministic SIC-range crosswalk (curated, fail-closed)

`app/services/sector_classification.py`: an **ordered** list of SIC integer
ranges → `(gics_sector, spdr_symbol)`, most-specific override ranges first, then
major-group ranges. `resolve_sector_spdr(sic: str | int | None) -> SectorClassification
| None` parses the 4-digit SIC, walks the ranges, returns the first match (or
None). Pure, no DB. The SPDR symbols are referenced from a single constant set
(no magic strings; cross-checked against `BENCHMARK_SYMBOLS`).

### Codex ckpt-1 resolutions (population-backed, 2026-06-18)

The first cut's major-group ranges swallowed SICs that belong to other GICS
sectors. Refined against the full 389-SIC dev population — every flagged code now
verified (re-scan: 0 unmapped, 26/26 flagged-code checks pass). The authoritative
crosswalk is `_CROSSWALK` in `app/services/sector_classification.py`; the table
below is the human summary. Carve-outs added:

- **36xx** base → **XLI** (electrical equipment: transformers/motors/lighting/
  misc are Industrials, not IT); carve 363x/365x → XLY (appliances / consumer
  audio-video), 366x–367x → XLK (comms equipment, semis, components).
- **38xx** base → **XLK** (electronic/measuring instruments); carve 3812 → XLI
  (defense electronics), 3826 → XLV (life-sciences tools), 3840–3851 → XLV
  (medical devices), 3873 → XLY (watches).
- **49xx**: carve 4950–4959 → XLI (sanitary/refuse/hazardous-waste = Industrials).
- **63xx**: narrowed the health carve-out to **6324 only** (managed care); 6321
  accident/health insurance stays XLF.
- **67xx**: carve 6792 → XLE (oil royalty) and 6795 → XLB (mineral royalty); 6770
  blank-check/SPAC stays XLF.
- **50–51xx wholesale**: carve product-specific distributors present in the
  population — 5010–5019 → XLY (motor vehicle), 5045/5065 → XLK (computer/
  electronic), 5047/5122 → XLV (medical/drug), 5160–5169 → XLB (chemicals),
  5171–5172 → XLE (petroleum); durable base → XLI, nondurable base → XLP.
- **Ordering**: 5400–5499 (grocery) is in the carve-out block ahead of the
  5200–5999 retail major group (resolver is first-match; an order test pins it).
- **LEFT JOIN**: the summary read LEFT-JOINs `instrument_sec_profile` so a no-SIC
  instrument yields `gics_sector=None, sector_spdr=None`, never a dropped row.
- **PR artifact**: the full `sic → spdr → count → description` table for all 389
  codes is recorded as the verification record (not "operator eyeball" alone).

Crosswalk summary (major group → SPDR; *override ranges resolve first*):

**The code `_CROSSWALK` in `app/services/sector_classification.py` is
authoritative — this doc is not a second copy of it** (avoids spec-vs-code
drift; review WARNING). The carve-out summary in "Codex ckpt-1 resolutions"
above describes the GICS-driven overrides; the major-group bases are: agriculture
0xx→XLP, mining/metals/chemicals/paper→XLB, oil&gas/coal/petroleum/pipelines→XLE,
construction/machinery/**electrical-equipment 36xx**/transport-equip/fabricated-
metal/transport-services→XLI, **electronic & measuring instruments 38xx→XLK**
(medical devices carved to XLV), food/tobacco→XLP, textiles/apparel/furniture/
leather/retail/hotels/leisure/education→XLY, publishing/telecom/broadcast/motion-
pictures→XLC, utilities 49xx→XLU, banks/brokers/**insurance 63xx→XLF** (managed-
care 6324 carved to XLV), real-estate 65xx + REIT 6798→XLRE, health-services
80xx→XLV. Every range and its order is in `_CROSSWALK`; carve-outs precede their
major group (first match wins), pinned by `tests/test_sector_classification.py`.

**Full-population verification (mandatory):** a one-shot scan resolves every one
of the 389 distinct SICs in the dev DB and prints `sic → spdr` for operator
eyeball; the PR records that 100% of SIC-bearing tradable instruments resolve to
exactly one SPDR (or an explicit, justified None), and spot-checks the panel
(AAPL 3571→XLK, MSFT 7372→XLK, JPM 6021→XLF, a REIT 6798→XLRE, pharma 2834→XLV,
an oil name 1311→XLE).

## Exposure (display consumer — avoids dead code)

`app/api/instruments.py` summary endpoint (`InstrumentIdentity` / the summary
read): add `gics_sector: str | None` and `sector_spdr: str | None`, resolved
on-read by joining `instrument_sec_profile.sic` through
`resolve_sector_spdr`. Pure read-path — no migration, no stored column, no
backfill. `instruments.sector` (the 1–9 code) is left untouched (peer-grouping
still uses it; real-sector peer-grouping is a filed follow-up).

Frontend: render the real `gics_sector` (+ a small "sector: XLK" affordance) on
the instrument page where the opaque code is shown today
(`summary.identity.sector` → show `gics_sector` when present, fall back to the
code). `types.ts` mirrors the two new fields.

## Tests

`tests/test_sector_classification.py` (pure): panel mappings; each of the
override carve-outs (pharma/computers/semis/autos/REIT/software/advertising/
health-plan/bio-research); None for no-SIC and for an out-of-range/blank SIC;
every SPDR returned is in the SPDR constant set; a representative SIC from each
major group resolves to the spec's sector.

## Dev-verify

Run the full-population scan (389 SICs) on dev; hit
`/instruments/{AAPL,JPM,O,XOM,PFE}/summary` and confirm `gics_sector` +
`sector_spdr` render (XLK/XLF/XLRE/XLE/XLV). Record in the PR.

## Out of scope (filed follow-ups)

- Sector-relative beta/excess in `instrument_risk_metrics` (2nd benchmark →
  columns + metric_version + backfill).
- Real-sector peer-grouping (rankings / RightRail / instruments filter currently
  group on the 1–9 code).
- A `sector_spdr` from a true GICS feed (would need a licensed vendor — out of
  the eToro+SEC posture, same class as #1635's dividend-source decision).
