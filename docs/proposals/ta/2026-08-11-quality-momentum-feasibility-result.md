# Quality-plus-momentum point-in-time feasibility result

Status: **deferred / do not backtest from the current corpus**. This is the
source-only result for #2537 and candidate C-3. No return, rank, threshold,
weight, factor sort, or portfolio outcome was read to reach it.

Reproduce the full-population census with:

```bash
PYTHONPATH=. uv run python scripts/verify_2537_c3_feasibility.py
```

## What has evidence behind it

C-3 was not invented from this repo's returns. The published prior is real:

- Kenneth French's momentum construction ranks on prior 2–12 month return and
  forms value-weighted portfolios with NYSE breakpoints.
- Kenneth French's operating-profitability portfolios use revenues less cost
  of goods sold, interest expense and SG&A, divided by book equity, with the
  previous fiscal year's accounting data available by the June formation date.
- Novy-Marx's simpler gross-profitability measure is gross profit divided by
  assets. AQR's broader quality formulation combines profitability, growth,
  safety and payout, but needs substantially more point-in-time data.

Primary references:

- [Kenneth French momentum factor details](https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/Data_Library/det_mom_factor_daily.html)
- [Kenneth French operating-profitability portfolio details](https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/data_library/det_port_form_op.html)
- [Novy-Marx, The Other Side of Value](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=1598056)
- [AQR, Quality Minus Junk](https://www.aqr.com/Insights/Research/Working-Paper/Quality-Minus-Junk)

Those publications support a factor hypothesis. They do not establish that
eBull's current data can reproduce it without leakage, or that loading a known
risk premium would generate broker-executable alpha after costs.

## Measured source state

The 2026-08-11 development corpus contains 7,709 non-comparator price series.
Only 5,269 (68.3%) map to an instrument; 2,440 are unresolved, no archive row
carries a CIK, and only two carry a dated delisting. The free archive is a
Yahoo-derived snapshot keyed by live symbols. The repository's previously
measured acceptance test found no series served as a distinct delisted history.
That shape is fatal to a cross-sectional sort: a missing failed or acquired name
changes the rank and therefore changes which surviving name is selected.

`instrument_universe_membership` correctly refuses to invent history. Its
12,695 rows all begin on 2026-08-10; 12,687 are `imported`, whose true start is
explicitly unknown. It can support future prospective membership, not a
2020–2026 backtest.

The operational SEC table is also deliberately not a research warehouse. For
all 4,183 instruments with a 10-K-family filing it holds a median and maximum of
three distinct annual accessions, exactly the retention cap. The available SEC
bulk Company Facts archive can reconstruct deeper as-filed facts for issuers it
can identify, and should remain the free source if a future research corpus is
built. It cannot restore absent delisted price histories or historical tradable
membership.

## Optimistic accounting-input upper bound

For each 30 June decision date, the census counts an active archive series as
quality-ready when any 10-K/10-K-A filed by then and no more than 548 days old
contains assets plus either gross profit or revenue and cost of revenue for the
same period. This is deliberately generous: it does not yet require common
stock, valid positive values, consistent accession, liquidity, sector,
volatility, costs, or portfolio eligibility. A production gate can only reduce
these counts.

| decision date | active archive | identity mapped | optimistic quality-ready | share of archive | share of mapped |
| --- | ---: | ---: | ---: | ---: | ---: |
| 2020-06-30 | 4,000 | 3,547 | 0 | 0.0% | 0.0% |
| 2021-06-30 | 4,623 | 4,036 | 0 | 0.0% | 0.0% |
| 2022-06-30 | 5,055 | 4,367 | 1 | 0.0% | 0.0% |
| 2023-06-30 | 5,243 | 4,504 | 14 | 0.3% | 0.3% |
| 2024-06-30 | 5,515 | 4,684 | 1,575 | 28.6% | 33.6% |
| 2025-06-30 | 6,065 | 4,999 | 2,025 | 33.4% | 40.5% |
| 2026-06-30 | 6,865 | 5,066 | 2,147 | 31.3% | 42.4% |

The recent rows are not a usable recent validation window. They still select
from mapped/current survivors, omit historical membership, and cover less than
half even of the mapped population. Zero-filling missing accounting values or
ranking only complete cases would make missingness an undeclared alpha input.

## Decision and bounded ways to reopen it

Do not implement C-3, expose it as a strategy, or spend a trial on the current
corpus. Do not substitute today's eToro universe, today's instrument mapping,
or the existing contaminated S-2 result. Candidate C-3 is deferred and the
first bounded candidate budget ends with zero promotable alpha candidates.

It may reopen only after one of these independently auditable source contracts
exists:

1. a licensed point-in-time US common-equity price and membership corpus that
   includes dead names, corporate actions and stable security identity, joined
   to as-filed SEC facts; or
2. enough prospectively recorded membership, facts and total-return history to
   power a newly preregistered trial without historical backfill.

The first is not available from the free sources already exhaustively tested in
this repository; the second takes years rather than enabling paper allocation
tomorrow. Recording prospectively remains useful, but elapsed time must never
be treated as automatic promotion.

This disposition is a successful safety result: the published factor family
remains plausible, while this application is prevented from presenting a
survivor-biased simulation as evidence that it can manage money.
