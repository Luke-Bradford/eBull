# #3384 slice 2a — admissibility probe: SEC fails-to-deliver and SEC MIDAS

Follows slice 1 (`2026-09-25-3384-hunt-admissibility-census.md`). Neither source is held. Nothing is ingested
and no outcome return is computed. Slice 2b (Cboe, FRED/ALFRED, COT, Wikipedia) comes separately.

**Reproduce:** `SEC_UA="<Name> <email>" PYTHONPATH=. uv run python -m scripts.probe_3384_sec_sources --out
<path.json> --cache <dir>`. Committed output: `2026-09-25-3384-sec-sources-probe.json`. Every figure below is
read from it. The file lists are the sources' own download pages as of the run (`generated_at`).

## Method, and what it can and cannot show
- **Inventory:** every `.zip` linked from the source's page, parsed to its period.
- **Publication clock:** HTTP `Last-Modified` minus period end, per file (UTC date). It is read as an upper
  bound on the first-release lag, **assuming the server does not backdate**. That assumption is not
  verifiable here. A `Last-Modified` before the period ends (including the epoch) is recorded as no clock.
  A date shared by ≥ 5 files is reported as a shared-date group and kept out of the quantiles; the header
  cannot say whether a group is a bulk re-post or a batch release. The lag is to the **file's** period end.
  An observation dated earlier in the period waits longer.
- **Identity:** FTD, one file per year; MIDAS, four years. In each file, the earliest date (FTD) or the first
  row's date in the first CSV member (MIDAS). The source's tickers on that date are matched to Intrader
  series whose stored bounds contain the date, with separators unified. This is a **symbol** match against
  today's admitted universe and stored symbols: reuse and renames are not resolved.
  `ambiguous_matched_symbols` (a symbol on two spanning series) is 0 in every sample.

## SEC fails-to-deliver
Source rule (the SEC page, quoted as read on the probe date; the page text is not stored in the JSON): the files give *"the date, CUSIP numbers, ticker
symbols, issuer name, price, and total number of fails-to-deliver (i.e., the balance level outstanding)"* from
NSCC's CNS system. **"Data prior to September 16, 2008 include only securities with a balance of total
fails-to-deliver of at least 10,000 shares"**. From then on, every security with a balance is included. *"The
first half of a given month is available at the end of the month. The second half … at about the 15th of the
next month."*

- **Files:** 434 (all HEAD 200; archives other than the 21 samples were not opened). Quarterly FOIA files
  2004Q1 → 2009Q2, then half-monthly (`a` = 1st–15th, `b` = 16th–month end) from 2009-07. Two periods (201910a,
  202308b) are published only under a `_0`-suffixed name. 0 unrecognised zip links. The page's release
  schedule speaks of half-months; nothing states when the quarterly FOIA files were first released.
- **Clock:** 278 files share Last-Modified 2020-12-19 (periods 2004-03 → 2020-11). 18 carry no clock (epoch;
  periods 2008-09 → 2018-04). **So this method recovers no release date for any period before 2020-12.** The
  other 138 files (periods from 2020-12): lag p50 15 days, p90 18; the six largest are 18, 21, 29, 37, 67 and
  987 days (`lags_outside_groups`). The centre matches the page's stated schedule.
- **Identity:** each row carries a CUSIP and a ticker. Tickers on the sampled dates:

| sample | tickers | matched to a spanning Intrader series | admitted series with a bar | … of which in FTD |
| --- | ---: | ---: | ---: | ---: |
| 2004q2 (2004-04-01) | 2,826 | 596 | 3,324 | 446 |
| 2008q2 (2008-04-01) | 3,313 | 1,113 | 4,405 | 868 |
| 200907a (2009-07-01) | 6,774 | 3,061 | 4,606 | 2,379 |
| 201306a (2013-06-03) | 5,603 | 3,632 | 5,885 | 2,726 |
| 201806a (2018-06-01) | 4,953 | 3,529 | 6,986 | 2,514 |
| 202206a (2022-06-01) | 5,976 | 4,477 | 8,301 | 2,741 |
| 202406a (2024-06-03) | 5,331 | 3,917 | 5,086 | 1,673 |

(all 21 years in the JSON). The page says the data "include fails-to-deliver in equity securities"; what the
unmatched tickers are is not measured here. The last column is **presence in the fails file**, not identity
success: a security with no fails balance that day is absent even when its identity is fine. After
2008-09-16, absence for a covered security on a complete file means no CNS balance. Before then it means
below 10,000 shares or none. Whole-file gaps and identity failure are separate cases.

## SEC MIDAS — metrics by individual security and exchange
- **Files:** 58 quarterly zips, 2012Q1 → 2026Q2 (2016Q1 is `…_q1-v2.zip`). The four sampled files are each a
  zip of a zip of monthly CSVs (76–187 MB); the rest were not opened.
  Columns: `Date, Security, Ticker, Exchange, McapRank, TurnRank, VolatilityRank, PriceRank, Cancels, Trades,
  LitTrades, OddLots, Hidden, TradesForHidden, OrderVol('000), TradeVol('000), LitVol('000), OddLotVol('000),
  HiddenVol('000), TradeVolForHidden('000)` in the sampled files. **No CUSIP or CIK column**; `Security` was
  not examined. The 2016Q2 sample writes `Date` as a float (`20160601.0`).
- **Clock:** 30 files share 2020-12-19 (periods to 2020-09), and 5 carry no clock. The 23 files from 2020-Q4
  onward: lag p50 32 days, p90 198. The 2025 quarters' Last-Modified dates are 163–289 days after quarter end.
  If those are release dates, **timing is not a constant**. If they are revisions, first release is earlier
  but the served content is revised. Either way, one fixed lag cannot be read off this.
- **Identity:** tickers matched to a spanning Intrader series: 3,887 / 4,949 (2012-04-02), 5,016 / 5,282
  (2016-06-01), 5,357 / 5,741 (2020-04-01), 6,644 / 7,340 (2024-04-01). Admitted series with a bar that appear
  in MIDAS: 3,091 / 5,464, 3,846 / 7,137, 3,686 / 6,578, 3,511 / 5,291. Here the last figure is
  closer to an identity rate: MIDAS lists traded securities, not only those with an event.
- `McapRank` is, by its name, a market-cap rank (its definition is not read here). If so, it is the only size
  axis in any source probed so far, and it exists only from 2012.

## Inputs this gives #3385 / the FTD and MIDAS ingests
1. **Before 2020-12, FTD availability is not recovered by this method.** The page's half-month schedule is
   the only stated rule, and it does not cover the quarterly FOIA era. Lags since 2020-12 centre on it (p50 15),
   with a tail to 67 days, plus one 987-day `_0` re-post. The availability lag is a construction choice to
   freeze. A measured tail bounds only what was observed, not unobserved delays, and revisions after first
   release are not dated at all.
2. **FTD changes its censoring on 2008-09-16** (≥ 10,000-share balances only before). A consistent
   variable needs the same threshold applied after, too. FTD starts in 2004, so there is none for 1990–2003.
   Settlement dates after 2008-09-16 inside discovery are only 3½ months, and their files arrive after
   2008.
3. **FTD has a CUSIP; we hold no dated CUSIP↔series map** (slice 1, Finding 4). Admitted-with-bar series
   present in the sampled fails file: 12–20% (2004–2008 samples) and 32–54% (2009–2024), by symbol match.
   That mixes "had no fails" with "did not match", so the identity rate itself is unmeasured until a CUSIP
   route exists.
4. **MIDAS stays validation-only** (2012+), as the programme doc already says. That is a scope limit, not an
   admissibility pass: release timing before 2020-Q4 is not recovered, later Last-Modified dates spread
   over hundreds of days, and identity is ticker-only.
