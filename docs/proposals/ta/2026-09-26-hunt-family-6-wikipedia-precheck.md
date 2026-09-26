# Hunt family 6 (MIDAS) and the Wikipedia source: pre-check — no trial registered

Refs #2437 (queue rules 2026-09-26: power first), #3384 (admissibility probe slices 2a and 2b), #3387 (hunt 1
bars). Programme doc: `2026-09-25-pattern-hunt-programme.md`, family 6 and the data table's Wikipedia row. Previous
pre-checks: hunt 1 (`2026-09-26-3387-hunt-1-precheck.md`), hunt 2 (`2026-09-26-hunt-2-families-4-5-precheck.md`),
family 8 (`2026-09-26-hunt-family-8-precheck.md`).
Security: none (a research document; no code, broker, auth or order path).

**Status:** v2, after Codex ckpt-1 (71 findings on v1). Applied: the IR threshold is a requirement, not a
ceiling, so the closure is a budget decision rather than an impossibility; rule 1's tension for validation-only
sources is stated, not assumed away; claims the cited abstracts do not make are withdrawn; the h = 20 hurdle is
corrected; reopen events carry the admissibility conditions and "a reopen permits a spec"; a forward route is
named as possible, not as running. No trial registered, no research return read, no ingest started. Inputs: the
#3384 probe outputs, hunt 1's bars, the #2902 power statement, and published abstracts. Nothing here enters M.

## Result
**A budget decision, as in the earlier pre-checks: register no trial on either source now.** Both start inside
the validation window (MIDAS 2012, pageviews 2015-07), so neither has a discovery period. A trial would be one
construction declared from the literature, with one validation look. Under the normal approximation, 50% power
on the t > 3 gate alone needs a stress-net active IR of at least about **0.97** (MIDAS) or **1.23** (Wikipedia),
before any of the deductions below. For comparison, not as a bound: value, measured on its own window ending
2013-06, has a **gross** IR of 0.87–0.94 (`2026-09-26-2902-power-first.md`). Neither source has a retrieved
magnitude or tracking error that would put a candidate near those thresholds. Neither source's admissibility
(MIDAS release clock, Wikipedia identity) is settled either.

⚠ Programme rule 1 admits to validation only "candidates frozen in discovery", while the data table lists both
sources as validation-only. That tension is the programme's to resolve before any trial on them. This note funds
none, so it does not have to resolve it.

## The IR requirement (both sources)
Power at bar t is Φ(IR × √years − t), reaching 0.5 at IR = t / √years (#2902, a known-variance normal
approximation; finite-sample t power is lower). The validation window ends 2021-06-28, the day before
`HOLDOUT_BOUNDARY` (programme rule 1). The holdout is a corroboration, not more validation years.

| source | first data | calendar span to 2021-06-28 | IR for 50% power at t > 3 |
| --- | --- | ---: | ---: |
| SEC MIDAS | 2012Q1 (first file, #3384 2a) | ≤ 9.5 y | ≥ 0.97 |
| Wikipedia pageviews | 2015-07-01 (#3384 2b) | ≤ 6.0 y | ≥ 1.23 |

The spans are upper bounds. Usable years shrink with the publication lag a point-in-time signal must add, the
63-session purge, formation history, identity and coverage gaps, and missing observations, and each of those
raises the IR needed. The IR is the arm's stress-net active return (edge − Bar B, as in the family-8 note) over
its long-run tracking error to the matched control. This is the t > 3 gate only: BY-FDR, DSR and the other cells
can bind harder, so 50% here is not 50% promotion.

## Family 6 — MIDAS footprints (odd-lot share, hidden share, cancel ratio)
What the probe established (#3384 slice 2a, "SEC MIDAS"), from four sampled files of 58:
- **Clock.** 30 files carry the same Last-Modified date, 2020-12-19 (periods to 2020-09), and 5 carry no clock,
  so release timing before 2020-Q4 is not recovered. The 23 files from 2020-Q4 onward show lag p50 32 days,
  p90 198. The probe says a fixed lag cannot be read off this, and whether late dates are releases or revisions is
  unknown. A freely chosen lag would therefore not make the served content point-in-time.
- **Identity.** In the sampled files the linkage is ticker-only (no CUSIP or CIK column; `Security` was not
  examined). Admitted series with a bar that appear: 3,091 / 5,464, 3,846 / 7,137, 3,686 / 6,578 and
  3,511 / 5,291 (57%, 54%, 56%, 66%). These are symbol matches, not verified identity, and MIDAS's inclusion rule
  was not read.
- **Columns.** Unsigned counts and volumes (`OddLots`, `Hidden`, `Cancels`, `Trades`, …) per security, exchange
  and day. Files are quarterly.

What the published evidence retrieved here says: Roseman, Van Ness & Van Ness (2018), *QREF* 69, 125–133
(abstract, via RePEc) report *"a positive relation between odd-lot order imbalance and returns"*. The abstract
states no horizon and does not say whether the relation is predictive or contemporaneous. An order imbalance is
signed, and the sampled MIDAS files carry no sign, so that signal is not directly buildable. Unsigned ratios,
changes and interactions are not excluded; none has a magnitude retrieved here. The same abstract records odd-lot
trades starting to report to the consolidated tape on 2013-12-09, a market-structure change inside the window.
Whether it breaks these MIDAS fields was not measured.

**Reopens when** all three hold:
1. A release clock with vintage evidence fixes point-in-time availability.
2. A published estimate for a long-only construction on MIDAS fields puts the active edge strictly above the
   larger of hunt 1's Bar A and Bar B under harness v1's mapping, after the availability lag. The estimate must
   come from outside the validation window or be selection-free on it, and must state its control, cost cell,
   entry lag, `lag + h − 1 ≤ 63` and g.
3. A tracking error for that construction gives power ≥ 0.5 over the usable years.

A reopen permits a spec; promotion still needs every programme gate (rule 3). Forward recording of MIDAS is
possible, but each quarter arrives 32–198+ days late, so it would be a stale quarterly feature. It is not proposed.

## The Wikipedia source (programme data table; no family of its own)
What the probe established (#3384 slice 2b, "Wikipedia pageviews"):
- The REST pageviews product serves data from **2015-07-01**, shown for three fixed titles with no gaps. Older
  pagecount dumps, renames/redirects, first-publication latency and revisions were not probed.
- **Identity.** Current Wikidata ticker statements (NYSE + Nasdaq only) match 2,294 of 4,630 alive admitted
  symbols (2,073 with an English article) and 1,064 of 12,636 terminating ones (975 with an article; 195 on an
  end-dated statement). A ticker statement is not a dated security-to-article map, and a survivorship-free arm
  needs one for the whole eligible universe, terminating names included.

Published evidence retrieved (no magnitude comparable to the bars):
- Behrendt, Peter & Zimmermann, *"An encyclopedia for stock markets? Wikipedia searches and stock returns"*,
  *International Review of Financial Analysis* (search-result abstract; the article page returned 403): daily
  searches and returns for 447 stocks, 2008–2017, by transfer entropy. That is an information-flow measure, not a
  ranking rule. Its pre-2015 data implies a source older than the REST product, and that source is unexamined here.
- Pyun (2025), SSRN 5172055, read only through a secondary summary: NYSE/NASDAQ 2016–2023; a high-minus-low
  attention alpha significant at 5–10%; *"weekly rebalancing is crucial — monthly rebalancing reduces
  effectiveness"*. A long-short alpha at that significance says nothing about a long-only arm against its control
  at t > 3.

If weekly rebalancing meant a full weekly round trip (h ≈ 5), a book entirely in one band ≥ $5 faces Bar B of
44–57%/yr. At h = 20 the band is 11.1–14.2%/yr, and at h = 21 it is 10.5–13.5%/yr, which is the monthly horizon
where the one available summary says the effect weakens. The summary gives no magnitude at either horizon.

**Reopens when** all three hold:
1. A dated security-to-article map covers the eligible universe, terminating names included, with renames and
   redirects resolved and availability and revision rules set.
2. An estimate from data not selected on the validation window, for a long-only construction under harness v1's
   mapping, puts the edge strictly above the larger of Bar A and Bar B.
3. Power is ≥ 0.5 over the usable years (IR ≥ 1.23 at the full 6.0).

A reopen permits a spec. The series is also served daily now, so it **could** be recorded forward like #3381. That
would be a separate recorder ticket, and a forward evaluation would face family 7's arithmetic: at 12 months the
same approximation needs IR ≈ 3.

## What this leaves
- Every family on held or ingestible data now has its published-estimate route screened: hunt 1 (families 1–3),
  hunt 2 (4–5), family 8 and this note (6, plus Wikipedia). None registers a trial. That is not exhaustion: family
  8's other architectures and features, and every stated reopen event, remain open.
- Family 7 (eToro crowd extremes) waits on ≥ 12 months of #3381 recording.
- Rule 5's terminal verdict is not reached here. Whether the open reopen events need a deadline, so that the
  programme does not become the indefinite search rule 5 forbids, is a programme decision, and this note does not
  make it.
