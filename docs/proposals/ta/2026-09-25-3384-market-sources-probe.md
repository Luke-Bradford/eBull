# #3384 slice 2b — admissibility probe: Cboe, FRED/ALFRED, CFTC COT, Wikipedia

Follows slice 1 (`2026-09-25-3384-hunt-admissibility-census.md`) and slice 2a
(`2026-09-25-3384-sec-sources-probe.md`). None of these sources is held, except Cboe VIX 2021+ (#2574). Nothing
is ingested and no outcome return is computed. All four are market-level except Wikipedia. The issue's size ×
survival strata apply only to Wikipedia's identity route, and only the **survival** half is measured here.

**Reproduce:** `PROBE_UA="<Name> <email>" PYTHONPATH=. uv run python -m scripts.probe_3384_market_sources --out
<path.json> --cache <empty dir>`. Committed output: `2026-09-25-3384-market-sources-probe.json`
(`probe_version` v2, `git_dirty: false`). Every figure below is read from it, except the quoted source rules, which
are cited to their documents.
⚠ Reproduction is of the **method**, not the figures. Every source here is mutable and only the Cboe CSV hashes and
the COT zips are retained; FRED, Wikidata and pageview responses, and per-request timestamps, are not. A re-run
measures that day's sources.

**v2 (after Codex ckpt-1, 52 findings):** ALFRED now reads only a 404 as "no vintage" (any other status raises) and
records every probed date; revision counts exclude missing values and bind the cut on both sides; Wikidata excludes
deprecated statements and counts statements distinctly. The re-run moved the Wikidata and revision-denominator
figures below; no other figure changed.

## Method, and what it can and cannot show
- **History:** each source's served file or endpoint, first and last row, rows per year, weekday mix and day-gap
  histogram (`cadence`). For daily series, every gap over 5 days is listed with its dates.
- **Back-calculation (Cboe):** each index's *First Value Date* and *Launch Date*, frozen from Cboe's methodology
  documents in `CBOE_LAUNCH` with their URLs. Those documents state that values before a launch date *"are
  calculated using a theoretical approach involving back-testing historical data in accordance with the
  methodology in place on the launch date"*. Rows before the first day of the launch month (or year, where only
  a year is printed) are counted as back-calculated, so the count is a **lower bound**: rows inside the launch
  month or year are not classified. SKEW and VVIX have no such table; the date of the white paper that introduced
  each is used as a **proxy** for its launch, not as a launch date.
- **Publication clock:** HTTP `Last-Modified`, which dates the served resource, not any row in it. It is a release
  clock only where one file holds one period (Cboe put/call daily JSON), and even there only if the header is not
  back-dated. Cboe index CSVs are rewritten in place. CFTC headers were not measured.
- **FRED/ALFRED** is measured by **vintage**, not by clock. The first date ALFRED serves a vintage is found by
  bisection over `alfredgraph.csv?vintage_date=` (404 before a series' first vintage). Bisection **assumes** the
  served dates form one run after the first; an earlier island would be missed. Every probed date and status is in
  the JSON (`alfred_probes`), with the first vintage's own observation span. Revisions compare observations up to
  2014-12-31 between the 2015-06-01 vintage and today: dates numeric in both, and how many differ. Two endpoints
  cannot show revisions that reversed in between, or any made before 2015-06-01. A difference is a changed number,
  whatever its cause (revision, rebasing, redefinition).
- **Identity (Wikipedia):** non-deprecated Wikidata `P414` (stock exchange) statements with a `P249` (ticker)
  qualifier, for NYSE and Nasdaq only, with their `P580`/`P582` start/end qualifiers. They are matched to Intrader
  symbols by survival stratum, with separators unified. This is a **symbol** match: flags are OR-ed across every
  statement carrying the symbol, so an end date and an article can come from different companies. It is not an
  identity rate.

## Cboe volatility indices
Source rule: the methodology documents in `CBOE_LAUNCH`. The VIX document: *"In 1993, Cboe … introduced the
Cboe Volatility Index"* on S&P 100 (OEX) options; the S&P 500-based index served today launched on September
22, 2003, with a first value date of January 1990. VIX is disseminated every 15 seconds in regular hours.

| index | first row | launch (document) | rows | rows before launch (lower bound) | rows launch-month → 2008 |
| --- | --- | --- | ---: | ---: | ---: |
| VIX | 1990-01-02 | 2003-09-22 | 9,280 | 3,443 | 1,345 |
| SKEW | 1990-01-02 | proxy: white paper dated January 2011 | 9,234 | 5,284 | 0 |
| VVIX | 2006-03-06 | proxy: white paper © 2012 | 5,112 | 1,409 | 0 |
| VIX1Y | 2007-01-03 | 2018 | 4,958 | 2,763 | 0 |
| VIX6M | 2008-01-02 | November 2013 | 4,713 | 1,470 | 0 |
| VIX3M | 2009-09-18 | October 2013 | 4,281 | 1,015 | 0 |
| VIX9D | 2011-01-04 | October 2013 | 3,955 | 689 | 0 |

- **In discovery (1990–2008), only VIX has published values**: 1,345 rows from September 2003 through 2008 (the
  launch month, before the 22nd, is included and unclassified). Its 1990 → August 2003 rows are back-calculated,
  and every other index's discovery rows are too. VIX3M and VIX9D have no discovery rows at all, so a VIX/VIX3M
  slope does not exist before 2009-09-18, and its first published value is in October 2013.
- **Back-calculated values:** per the methodology, each is computed from that day's option quotes by the
  launch-date formula. Its inputs were not audited here. Even with clean inputs, it carries hindsight in the
  **formula** (chosen later), and nobody could observe or trade it at the time.
- **VVIX:** the served history starts 2006-03-06, while the white paper's chart starts in June 2006. Not explained.
- **SKEW:** Cboe's consultation results (C2025071701, 2025-07-17) say *"modifications to the SKEW Index methodology
  are appropriate"*, effective date to be announced. Whether that has taken effect by now, and whether history
  would be restated, was not checked. `sha256` per CSV is in the JSON; a later re-run can show that a file changed,
  not whether history was restated or rows were only appended.
- Gaps over 5 days (JSON `long_gaps`): VIX and SKEW 2001-09-10 → 2001-09-17; SKEW 2000-09-19 → 2000-10-02; VVIX seven
  gaps of 6–21 days, all in March–August 2006.
- **Clock:** each CSV's `Last-Modified` falls after its last row (VIX: row 2026-09-24, modified 2026-09-25 00:31
  UTC). One observation per file, of the current process only.
- **Served vs stored:** our VIX copy (1,469 rows, 2021-01-04 → 2026-09-22) matches the served CSV on every
  overlapping date's OHLC at 4 decimals (`ohlc_differs` 0).

## Cboe put/call ratios
- **Archive CSVs:** `totalpc`, `equitypc`, `indexpc`, `etppc` 3,253 rows each, 2006-11-01 → 2019-10-04. `vixpc` 3,427
  rows, 2006-02-24 → 2019-10-04. No gap over 5 days. All five carry `Last-Modified` 2020-10-30 (one observation).
- **Daily JSON:** trade dates 2019-10-01…04 return 403 and 2019-10-07 (the next weekday) returns 200, as does every
  later sampled date. So the served JSON starts where the archive ends, across a weekend. 403 means "not served",
  which does not show the endpoint never held those dates. Only dates and ratio **names** were compared: no value,
  volume, coverage or definition is checked across the handoff, and one date a year after 2019 cannot show
  continuous coverage. Ratio names per file: 13 in 2019–2024 samples, 16 in 2025, 23 in 2026.
- **Definition breaks**, quoted from the archive preambles, which differ by file. All four of total/equity/index/ETP:
  *"Historical index volume from November 1 2006 through May 31 2012 is based on cleared volume as reported by the
  OCC. The volume data reflected after May 31 2012 … is based upon preliminary reported volume"*. Equity and index
  add *"As of June 11 2012 [Equity|Index] Volume includes only the volume of [equity|cash-settled index] option
  products and excludes that of exchange-traded products"*; ETP *"As of June 11 2012 the volume of exchange-traded
  products is represented as a unique product category"*. `vixpc` states neither date, only that its volume *"may
  be based upon preliminary reported volume"*.
- **Clock:** every sampled JSON for trade dates 2019-10-07 → 2024-06-03 carries `Last-Modified` 2024-08-12 or
  2024-08-13, consistent with a bulk re-post. So none of these samples dates a first release. Of the two later
  samples, 2025-06-02 was modified 01:37 UTC the next day (the same evening in New York), and 2026-06-01 was
  modified 15 days later. Whether a later sample was a revision or a re-post cannot be told from the header, and
  when the re-post boundary falls between 2024-06 and 2025-06 is not measured.

## FRED / ALFRED
Keyless: `fredgraph.csv` (current vintage) and `alfredgraph.csv?vintage_date=` both work without an API key.
The vintage-date list endpoint needs one (`api.stlouisfed.org` answers *"Variable api_key is not set"*), hence the
bisection.

| series | current from | first ALFRED vintage | that vintage's span | changed / numeric in both, ≤ 2014-12-31 |
| --- | --- | --- | --- | --- |
| BAA10Y (Moody's Baa − 10y) | 1986-01-02 | 2014-01-27 | 1986-01-02 → 2014-01-24 | 0 / 7,254 |
| AAA10Y | 1983-01-03 | 2014-01-27 | 1983-01-03 → 2014-01-24 | 0 / 8,001 |
| BAMLH0A0HYM2 (ICE BofA HY OAS) | **2023-09-26** | 2023-09-26 | one observation | — |
| BAMLC0A0CM (ICE BofA IG OAS) | **2023-09-26** | 2023-09-26 | one observation | — |
| T10Y2Y | 1976-06-01 | 2014-01-27 | 1976-06-01 → 2014-01-24 | 0 / 9,642 |
| T10Y3M | 1982-01-04 | 2014-01-27 | 1982-01-04 → 2014-01-24 | 0 / 8,251 |
| DGS10 | 1962-01-02 | 2005-06-28 | 1962-02-01 → 2005-06-24 | 1 / 13,234 |
| DTB3 | 1954-01-04 | 2005-06-28 | 1954-01-04 → 2005-06-24 | 34 / 15,240 |
| NFCI | 1971-01-08 | 2011-05-25 | 1973-01-05 → 2011-05-20 | 2,183 / 2,191 |
| STLFSI4 | 1993-12-31 | **2022-11-10** | 1993-12-31 → 2022-11-04 | not measured |
| ICSA | 1967-01-07 | 2009-05-28 | 1967-01-07 → 2009-05-23 | 172 / 2,504 |
| UNRATE | 1948-01-01 | 1960-03-15 | 1948-01-01 → 1960-02-01 | 4 / 804 |
| PAYEMS | 1939-01-01 | 1955-05-06 | 1951-01-01 → 1955-04-01 | 722 / 912 |
| INDPRO | 1919-01-01 | ≤ 1940-01-01 (the probe floor) | 1919-01-01 → 1939-11-01 | 1,152 / 1,152 |
| CPIAUCSL | 1947-01-01 | 1972-07-21 | 1970-12-01 → 1972-06-01 | 48 / 816 |

- **ICE BofA credit spreads:** today's FRED series starts 2023-09-26, and so does ALFRED's first vintage, which holds
  one observation. Of the series probed, only Moody's (BAA10Y 1986+, AAA10Y 1983+) has pre-2023 credit-spread history.
- Moody's spreads and the Treasury curve: 0 differences between the 2015-06-01 vintage and today, except DGS10 (1)
  and DTB3 (34). That bounds only those two endpoints. Before their first ALFRED vintage (2014 / 2005) there is no
  as-of record, so reading them as observed rests on that endpoint comparison, not on a vintage.
- **NFCI:** 2,183 of the 2,191 comparable pre-2015 values differ between the two vintages, and today's series has 104
  pre-2015 dates the 2015 vintage lacks. Its first vintage is 2011-05-25 and starts in 1973, two years after today's
  series. **STLFSI4:** first served vintage 2022-11-10, already spanning 1993-12-31 → 2022-11-04; its revisions were
  not measured. Neither has a served vintage inside discovery.
- Macro series differ between vintages (PAYEMS 722 / 912, INDPRO all 1,152); the cause is not measured. UNRATE,
  PAYEMS, INDPRO and CPIAUCSL have a served vintage before 1990, but the first one can be short: CPIAUCSL's
  1972-07-21 vintage holds only 1970-12 → 1972-06. ICSA's first served vintage is 2009-05-28, after discovery.
- These are first **served** vintages. Whether ALFRED's archive is complete for a series, or a predecessor series
  holds earlier vintages, was not checked. Release times within a day are not measured.

## CFTC Commitments of Traders
Source rules:
- *About the COT Reports:* *"Beginning as of June 30, 1962, COT data were published each month"*, *"switching to
  mid-month and month-end in 1990, to every two weeks in 1992, and to weekly in 2000"*. Publication moved *"to the
  sixth business day after the 'as of' date in 1990 and then to the third business day after the 'as of' date in
  1992"*. The original monthly reports were *"published on the 11th or 12th calendar day of the following month"*.
- *Historical Compressed:* *"For dates before September 30, 1992, only mid-month and month-end data is
  available"*. For TFF, complete files run *"from September 2009 forward"*.
- *Release Schedule:* released *"at 3:30 p.m. Eastern time"*, usually Friday, with data from the previous
  Tuesday. *"Federal holidays may delay release by one or two days."* That is today's page; when this schedule took
  effect was not established.
- *Suspensions:* the COT report was suspended during funding lapses and caught up afterwards. See the CFTC press
  releases 6745-13 (2013), 7864-19 (2018–19: last report 2018-12-21, scheduled to resume 2019-02-01, then
  two reports a week until current), 9138-25 and 9147-25 (2025), and 8662-23 ("Postponement of Commitments of
  Traders Report", 2023). This list is from a search of cftc.gov and is not exhaustive. The *COT Historical Special
  Announcements* page is the index to check.

Measured (legacy futures-only, 42 files; TFF futures-only, 18 files; 338 zip links on the page; 0 rows skipped as
short or blank-dated):

| era (legacy, all markets) | report dates | Tuesday | gap histogram (days: count) |
| --- | ---: | ---: | --- |
| 1986–1989 | 96 | 14 | 13–18 only (twice a month) |
| 1990–1991 | 48 | 6 | 13–18 only |
| 1992–1999 | 397 | 380 | 7: 374; 14–18: 17; 3/4/6/8: 5 |
| 2000–2026 | 1,396 | 1,374 | 7: 1,359; 6/8: 32; 3/4/11: 4 |

- **The file's cadence is not the publication cadence.** 1986–89 carries 24 dates a year (mid-month and
  month-end), while publication was monthly, on the 11th/12th of the next month. 1993–1999 carries 52–53 weekly dates
  a year, while publication was every two weeks until 2000. So an as-of date is not an availability date, and when
  the extra rows were first published is not stated.
- **2000 onward is not an availability date either:** the stated rule releases Tuesday's data on Friday. The 22
  non-Tuesday dates are listed in the JSON (`non_tuesday_dates`): 17 Mondays, 2 Wednesdays and 3 Fridays
  (2001-12-21, 2001-12-28, 2003-02-14). Whether holidays explain them was not tested.
- The files carry no release date. Release dates for suspension catch-ups would have to come from the press
  releases above; other historical release records were not investigated.
- **E-mini S&P 500 (`13874A`):** legacy from 1997-09-16 (1,510 dates, including gaps of 25 days after 1997-12-19 and 21
  days after 1998-09-15, not investigated). TFF from 2006-06-13 (1,059 dates). TFF rows before September 2009 predate
  the page's "complete files" start. When they were published is not stated. Per-file date counts are in the JSON:
  `fut_fin_txt_2010` alone holds 24 dates (July–December 2010), and the TFF union also includes the 2006–2016
  composite file. Overlaps between files were unioned, not compared.
- Identity: the CFTC contract market code; market-level, no security linkage needed.

## Wikipedia pageviews
- **REST pageviews:** Apple_Inc., General_Electric and Sears each return 4,104 days from **2015-07-01** (asked from
  2010-01-01) through 2026-09-24, with no missing date in the span. The run was on 2026-09-26, so the latest day was
  two days old at run time. That is one observation; it does not measure first-publication latency or later
  revision. Fixed titles were requested, so renames and redirects were not tested, and view values were not
  inspected. Older pagecount dumps were not probed.
- **Wikidata ticker map (NYSE + Nasdaq), non-deprecated statements:**

| exchange | ticker statements | items | with start time | with end time | items with an enwiki article |
| --- | ---: | ---: | ---: | ---: | ---: |
| NYSE | 2,012 | 1,971 | 722 | 179 | 1,848 |
| Nasdaq | 1,979 | 1,935 | 770 | 179 | 1,649 |

- **Symbol match onto admitted Intrader symbols:** alive 2,294 of 4,630 appear, 2,073 with an enwiki article.
  Terminating: **1,064 of 12,636**, 195 on an end-dated statement, 975 with an article. No symbol falls in both
  strata. Other exchanges (NYSE American, Arca, OTC) were not queried. Size strata were not measured.

## Inputs this gives #3385 / the Cboe, FRED/COT and Wikipedia ingests
Each is a finding for the spec that uses it to act on; none fixes a feature construction here.
1. **Published volatility inputs in discovery: VIX only, from 2003-09-22.** VIX before then, SKEW, VVIX and the
   term indices in discovery are back-calculated; VIX3M/VIX9D start in 2009/2011. A trial using them declares one
   of: back-calculated values admitted with that caveat (formula chosen later, inputs unaudited), or post-launch data
   only. A VIX3M term in discovery has no values at all; in 2009–2013 it has only back-calculated ones.
2. **Put/call** starts 2006-11-01 (total/equity/index/ETP) or 2006-02-24 (VIX). The first four carry the 2012 breaks
   quoted above; no value continuity across the 2019 handoff was checked; and none of the sampled JSON dates a first
   release before 2025. A trial using it declares how it treats the breaks and the handoff.
3. **Credit spreads:** of the series probed, Moody's BAA10Y/AAA10Y are the only pre-2023 option; ICE BofA OAS starts
   2023-09-26. NFCI and STLFSI4 have no served vintage in discovery, and NFCI differs across vintages almost
   everywhere, so neither has an as-of value there.
4. **COT availability is not the as-of date in any era.** Before 2000 the file cadence exceeds the publication
   cadence; from 2000 the stated rule lags Tuesday data to Friday, with holiday and suspension exceptions. An ingest
   needs an availability rule taken from CFTC's schedules and documented exceptions, with its effective dates
   established; this probe does not supply one.
5. **Wikipedia pageviews are served from 2015-07-01 only, inside validation.** Current Wikidata ticker statements
   cover 1,064 / 12,636 terminating symbols, 195 end-dated. Historical sitelinks and other mapping routes were not
   examined, so whether a dated article map can be recovered is open. The terminating stratum is the one
   survivorship depends on.
