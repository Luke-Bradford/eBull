# #3384 slice 2b — admissibility probe: Cboe, FRED/ALFRED, CFTC COT, Wikipedia

Follows slice 1 (`2026-09-25-3384-hunt-admissibility-census.md`) and slice 2a
(`2026-09-25-3384-sec-sources-probe.md`). None of these sources is held, except Cboe VIX 2021+ (#2574). Nothing
is ingested and no outcome return is computed. All four are market-level except Wikipedia, so the size ×
survival strata of the issue apply only to Wikipedia's identity route.

**Reproduce:** `PROBE_UA="<Name> <email>" PYTHONPATH=. uv run python -m scripts.probe_3384_market_sources --out
<path.json> --cache <dir>`. Committed output: `2026-09-25-3384-market-sources-probe.json` (`git_dirty: false`).
Every figure below is read from it, except the quoted source rules, which are cited to their documents.

## Method, and what it can and cannot show
- **History:** each source's served file or endpoint, first and last row, rows per year, weekday mix and day-gap
  histogram (`cadence`).
- **Back-calculation (Cboe):** each index's *First Value Date* and *Launch Date*, frozen from Cboe's methodology
  documents in `CBOE_LAUNCH` with their URLs. Those documents state that values before a launch date *"are
  calculated using a theoretical approach involving back-testing historical data in accordance with the
  methodology in place on the launch date"*. Rows before the first day of the launch month (or year, where only
  a year is printed) are counted as back-calculated. Rows inside that month or year are not classified.
- **Publication clock:** HTTP `Last-Modified` where a per-period file exists (Cboe put/call JSON). Cboe index
  CSVs and CFTC yearly files are rewritten in place, so their header dates only the latest row. FRED/ALFRED is
  measured by **vintage**, not by clock: the first date ALFRED serves a vintage (bisection over
  `alfredgraph.csv?vintage_date=`, which returns 404 before a series' first vintage), and how many observations
  up to 2014-12-31 changed between the 2015-06-01 vintage and today. One old vintage cannot show revisions made
  before it, so a 0 bounds only the 2015→2026 interval.
- **Identity (Wikipedia):** Wikidata `P414` (stock exchange) statements with a `P249` (ticker) qualifier, for
  NYSE and Nasdaq only, with their `P580`/`P582` start/end qualifiers. They are matched to Intrader symbols by
  survival stratum, with separators unified. This is a **symbol** match: reuse is not resolved.

## Cboe volatility indices
Source rule: the methodology documents in `CBOE_LAUNCH`. The VIX document: *"In 1993, Cboe … introduced the
Cboe Volatility Index"* on S&P 100 (OEX) options; the S&P 500-based index served today launched on September
22, 2003, with a first value date of January 1990. VIX is disseminated every 15 seconds in regular hours.

| index | first row | launch (document) | rows | rows before launch |
| --- | --- | --- | ---: | ---: |
| VIX | 1990-01-02 | 2003-09-22 | 9,280 | 3,443 |
| SKEW | 1990-01-02 | 2011 (white paper dated January 2011) | 9,234 | 5,284 |
| VVIX | 2006-03-06 | 2012 (white paper © 2012) | 5,112 | 1,409 |
| VIX1Y | 2007-01-03 | 2018 | 4,958 | 2,763 |
| VIX6M | 2008-01-02 | November 2013 | 4,713 | 1,470 |
| VIX3M | 2009-09-18 | October 2013 | 4,281 | 1,015 |
| VIX9D | 2011-01-04 | October 2013 | 3,955 | 689 |

- **Every row inside discovery (1990–2008) is back-calculated.** VIX3M and VIX9D have no rows in discovery at
  all. So the programme's "VIX/VIX3M slope" regime variable does not exist before 2009-09-18, and was not
  published before October 2013.
- A back-calculated value uses only that day's option prices, so it is not a data leak. What it carries is
  hindsight in the **formula**: the formula was chosen later, and nobody could observe or trade on the value then.
- **SKEW is under methodology review.** Cboe's consultation results (C2025071701, 2025-07-17) say *"modifications
  to the SKEW Index methodology are appropriate"*, effective date to be announced. Whether history will be
  restated is not stated. `sha256` per CSV is in the JSON, so a later re-run shows whether served history changed.
- Gaps over 5 days: VIX and SKEW at 2001-09-10 (the 9/11 closure), SKEW 13 days from 2000-09-19, and VVIX's
  sparse early rows (seven gaps of 6–21 days, all in 2006).
- **Clock:** each CSV's `Last-Modified` falls on the evening (US Eastern) of its last row: VIX, row 2026-09-24,
  modified 2026-09-25 00:31 UTC. That is one observation per file, of the current process only.
- **Served vs stored:** our VIX copy (1,469 rows, 2021-01-04 → 2026-09-22) matches the served CSV on every
  overlapping date's OHLC (`ohlc_differs` 0).

## Cboe put/call ratios
- **Archive CSVs** (`totalpc`, `equitypc`, `indexpc`, `etppc`): 3,253 rows, 2006-11-01 → 2019-10-04, no gap over
  5 days. `vixpc` starts 2006-02-24. All five were last modified 2020-10-30; they are frozen.
- **Daily JSON** continues the archive: trade dates 2019-10-01…04 return 403, and 2019-10-07 (the next weekday)
  returns 200. The handoff is a weekend, with no gap. Ratios per file: 13 in 2019–2024 samples, 16 in 2025, 23 in
  2026. The set grows.
- **Definition breaks**, quoted from the archive preambles: *"Historical index volume from November 1 2006
  through May 31 2012 is based on cleared volume as reported by the OCC. The volume data reflected after May 31
  2012 … is based upon preliminary reported volume"*. *"As of June 11 2012 Equity Volume includes only the volume
  of equity option products and excludes that of exchange-traded products"*; index volume likewise.
- **Clock:** every sampled JSON for trade dates 2019-10-07 → 2024-06-03 carries `Last-Modified` 2024-08-12 or
  2024-08-13, which looks like a bulk re-post. So **no release date is recovered before August 2024**. Of the two
  later samples, 2025-06-02 was modified 01:37 UTC the next day (the same evening in New York), and 2026-06-01
  was modified 15 days later. The header cannot tell whether that was a revision or a re-post.

## FRED / ALFRED
Keyless: `fredgraph.csv` (current vintage) and `alfredgraph.csv?vintage_date=` both work without an API key.
The vintage-date list endpoint needs one (`api.stlouisfed.org` answers *"Variable api_key is not set"*), hence the
bisection.

| series | observations from | first ALFRED vintage | changed / common obs ≤ 2014-12-31 (2015-06-01 vintage vs today) |
| --- | --- | --- | --- |
| BAA10Y (Moody's Baa − 10y) | 1986-01-02 | 2014-01-27 | 0 / 7,565 |
| AAA10Y | 1983-01-03 | 2014-01-27 | 0 / 8,348 |
| BAMLH0A0HYM2 (ICE BofA HY OAS) | **2023-09-26** | 2023-09-26 | — |
| BAMLC0A0CM (ICE BofA IG OAS) | **2023-09-26** | 2023-09-26 | — |
| T10Y2Y | 1976-06-01 | 2014-01-27 | 0 / 10,067 |
| T10Y3M | 1982-01-04 | 2014-01-27 | 0 / 8,608 |
| DGS10 | 1962-01-02 | 2005-06-28 | 1 / 13,827 |
| DTB3 | 1954-01-04 | 2005-06-28 | 34 / 15,913 |
| NFCI | 1971-01-08 | 2011-05-25 | 2,183 / 2,191 |
| STLFSI4 | 1993-12-31 | **2022-11-10** | — |
| ICSA | 1967-01-07 | 2009-05-28 | 172 / 2,504 |
| UNRATE | 1948-01-01 | 1960-03-15 | 4 / 804 |
| PAYEMS | 1939-01-01 | 1955-05-06 | 722 / 912 |
| INDPRO | 1919-01-01 | ≤ 1940-01-01 (the probe floor) | 1,152 / 1,152 |
| CPIAUCSL | 1947-01-01 | 1972-07-21 | 48 / 816 |

- **The ICE BofA credit spreads are served from 2023-09-26 only,** in both FRED and ALFRED. The only credit
  spread with pre-2023 history here is Moody's (BAA10Y 1986+, AAA10Y 1983+).
- Moody's spreads and the Treasury curve were not revised in 2015→2026 (0 changes, except DGS10 1 and DTB3 34).
  Before their first vintage (2014 / 2005) they have no as-of record, so reading them as observed rests on that
  zero-revision evidence, not on a vintage.
- **The financial-conditions indices are rewritten wholesale.** NFCI changed 2,183 of 2,191 pre-2015
  observations, and has no vintage before 2011-05-25. STLFSI4 has no vintage before 2022-11-10, so its whole
  1993–2022 history comes from a single later vintage. Neither has an as-of value anywhere in discovery.
- Macro releases are revised (PAYEMS 722 / 912, INDPRO all). UNRATE, PAYEMS, INDPRO and CPIAUCSL have vintages
  from before 1990. ICSA's start on 2009-05-28, after discovery.
- Release times within a day are not measured.

## CFTC Commitments of Traders
Source rules:
- *About the COT Reports:* *"Beginning as of June 30, 1962, COT data were published each month"*, *"switching to
  mid-month and month-end in 1990, to every two weeks in 1992, and to weekly in 2000"*. Publication moved *"to the
  sixth business day after the 'as of' date in 1990 and then to the third business day after the 'as of' date in
  1992"*. The original monthly reports were *"published on the 11th or 12th calendar day of the following month"*.
- *Historical Compressed:* *"For dates before September 30, 1992, only mid-month and month-end data is
  available"*. For TFF, complete files run *"from September 2009 forward"*.
- *Release Schedule:* released *"at 3:30 p.m. Eastern time"*, usually Friday, with data from the previous
  Tuesday. *"Federal holidays may delay release by one or two days."*
- *Suspensions:* the COT report was suspended during funding lapses and caught up afterwards. See the CFTC press
  releases 6745-13 (2013), 7864-19 (2018–19: last report 2018-12-21, scheduled to resume 2019-02-01, then
  two reports a week until current), 9138-25 and 9147-25 (2025), and 8662-23 ("Postponement of Commitments of Traders Report", 2023). This
  list is from a search of cftc.gov and is not exhaustive. The *COT Historical Special Announcements* page is
  the index to check.

Measured (legacy futures-only, 42 files; TFF futures-only, 18 files; 338 zip links on the page):

| era (legacy, all markets) | report dates | Tuesday | gap histogram (days: count) |
| --- | ---: | ---: | --- |
| 1986–1989 | 96 | 14 | 13–18 only (twice a month) |
| 1990–1991 | 48 | 6 | 13–18 only |
| 1992–1999 | 397 | 380 | 7: 374; 14–18: 17; 3/4/6/8: 5 |
| 2000–2026 | 1,396 | 1,374 | 7: 1,359; 6/8: 32; 3/4/11: 4 |

- **The file's cadence is not the publication cadence.** 1986–89 carries 24 dates a year (mid-month and
  month-end). Publication then was monthly, on the 11th/12th of the next month. 1993–1999 carries 52–53 weekly dates
  a year, while publication was every two weeks until 2000. So an as-of date is not an availability date before
  2000. When the extra rows were first published is not stated.
- 2000 onward: Tuesday as-of, with Monday/Wednesday shifts around holidays, consistent with the stated rule. The
  files carry no release date, so the suspension catch-ups can be dated only from the press releases.
- **E-mini S&P 500 (`13874A`):** legacy from 1997-09-16 (1,510 dates, including gaps of 14, 21 and 25 days, which
  were not investigated). TFF from 2006-06-13 (1,059 dates). TFF rows before September 2009 predate the page's
  "complete files" start. When they were published is not stated.
- Identity: the CFTC contract market code; market-level, no security linkage needed.

## Wikipedia pageviews
- **REST pageviews:** Apple_Inc., General_Electric and Sears each return 4,104 days from **2015-07-01** (asked
  from 2010-01-01) through 2026-09-24, with no missing day. The run was on 2026-09-25, so the latest day is at
  most one day old (one observation). Older pagecount dumps were not probed.
- **Wikidata ticker map (NYSE + Nasdaq):**

| exchange | ticker statements | items | with start time | with end time | items with an enwiki article |
| --- | ---: | ---: | ---: | ---: | ---: |
| NYSE | 2,064 | 2,010 | 742 | 204 | 1,887 |
| Nasdaq | 2,033 | 1,971 | 789 | 199 | 1,681 |

- **Match onto admitted Intrader symbols:** alive 2,295 of 4,630 symbols appear, 2,074 with an enwiki article.
  Terminating: **1,101 of 12,636**, of which 218 are on an end-dated statement and 1,012 have an article. Other
  exchanges (NYSE American, Arca, OTC) were not queried, and a symbol match is not identity.

## Inputs this gives #3385 / the Cboe, FRED/COT and Wikipedia ingests
1. **Regime conditioning has no published volatility input in discovery.** VIX before 2003-09 and SKEW before
   2011 are back-calculated, and VIX3M/VIX9D start in 2009/2011. #3385 must freeze one of two constructions.
   Either admit back-calculated values with that caveat declared (no per-value leak, formula chosen later), or
   confine volatility conditioning to post-launch data. The slope variable forces the second for any VIX3M term.
2. **Put/call starts 2006-11-01** and has two definition breaks in 2012 (cleared → preliminary volume after
   2012-05-31; ETPs split out on 2012-06-11). There is no release clock before August 2024. A consistent series
   needs one definition across the breaks, or a regime split at them.
3. **Credit spreads: Moody's BAA10Y/AAA10Y are the only pre-2023 option probed.** ICE BofA OAS starts
   2023-09-26. NFCI/STLFSI4 are revised wholesale and have no vintage in discovery, so they are inadmissible there
   as-of.
4. **COT availability is not the as-of date before 2000.** Publication was monthly (to 1990), then twice monthly,
   then every two weeks (1992–1999), each lagged. Since 2000 the rule is Friday 3:30 p.m. ET for Tuesday data,
   except for dated suspensions. The availability rule must be frozen by era.
5. **Wikipedia is validation-late and forward only (2015-07+).** No dated article map exists in Wikidata:
   terminating symbols match 1,101 / 12,636, and 218 carry an end date. That is the stratum survivorship depends
   on, so an ingest needs its own dated map.
