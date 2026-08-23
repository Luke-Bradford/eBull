# R6 factor-construction validation declaration (#2912)

Status: **FROZEN BEFORE RESULTS**. This declaration is a software/data
validation protocol, not a strategy arm and not evidence that momentum is
investable. No correlation, regression coefficient, or constructed factor
return was read before this file was frozen and hashed.

## Question and construction inventory

Does the sign, timing and ordering produced by eBull's existing S-2
cross-sectional momentum construction agree sensibly with independently
published U.S. equity momentum factors?

The in-repository inventory is deliberately narrow:

- `app/services/strategies/s2_cross_sectional_momentum.py` is the only current
  strategy-grade construction with a published factor equivalent. It uses the
  published prior-(2–12)-month formation window, but intentionally differs
  from French MOM in breakpoints, size buckets and weighting.
- `app/services/scoring.py::_value_score` is a thesis-price-upside application
  heuristic, not a book-to-market factor. `_quality_score` is a coverage-aware
  application score, not a profitability-factor portfolio. Regressing either
  cross-sectional score against a monthly factor-return series would be a
  category error, so neither is relabelled as a factor here.
- No current production construction implements HML, RMW, CMA or AQR VAL.
  Future work that introduces one must add a published-equivalent validation
  under this harness before using its output as strategy evidence.

## Frozen reference series

1. Kenneth French Data Library monthly U.S. Momentum Factor (`Mom`), delivered
   as `F-F_Momentum_Factor_CSV.zip`; published percent values normalize to
   decimal returns. The missing sentinels `-99.99` and `-999` are rejected.
2. AQR *Value and Momentum Everywhere: Factors, Monthly*, worksheet `VME
   Factors`, column `MOMLS_VME_US90` (U.S. equity stock-selection momentum).
   Values are already decimal excess returns.

French five-factor data, all columns in the AQR factor workbook, and the frozen
FRED series are ingested for provenance and later regression context, but they
are not alternative outcomes for this one construction test.

## Frozen eBull diagnostic factor

- Universe: the complete `survivorship_free` selection from
  `app/services/universe_selection.py`, pinned to vendor
  `icyDenev/Intrader` and capture date 2024-09-27. Unlinked terminating names
  remain included under their negative in-pass series key; unlinked names
  classified alive remain excluded and counted exactly as the existing rule
  requires.
- Window: all complete calendar months that overlap the eBull construction and
  each reference, ending no later than 2024-08-31. September 2024 is excluded
  because the pinned corpus ends before that month completes. No lower date is
  selected after inspecting results; the 273-bar warm-up and source overlap
  determine it mechanically.
- Inputs: fail-closed research-corpus quarantine coverage at the current rule
  version. S-2's score is exactly `close[t-21] / close[t-252] - 1`, after its
  literal 273-bar eligibility rule and `$1` decision-close floor. `close` is
  the split-adjusted construction input, as S-2 declares.
- Calendar: the first weekday on the admitted panel's union calendar whose
  month differs from the prior weekday, using S-2's existing
  `rebalance_dates` rule.
- Sort: S-2's deterministic score-descending/key-ascending ordering. The top
  and bottom `floor(N/10)` eligible names form equal-weight diagnostic legs.
  A panel below ten names is rejected.
- Return: each member's total return from its decision-date `adj_close` to its
  `adj_close` on the next panel rebalance date. Both endpoint close bars must be
  return-usable and both adjusted closes finite and positive. Missing next
  endpoints are rejected, never filled with zero. The eBull diagnostic month
  is the month containing the entry rebalance. The diagnostic factor is
  arithmetic-mean top-leg return minus arithmetic-mean bottom-leg return.
- This long-short spread exists only to expose the ranking's sign and timing.
  It does not change S-2 into a short strategy, does not charge costs, and must
  not be quoted as an arm return.

## Frozen statistics and failure rules

For each reference separately, intersect by calendar month and report:

- overlap start/end and month count;
- Pearson correlation;
- OLS `ebull = alpha + beta * reference`, including alpha and beta;
- contemporaneous, reference-lag-one and reference-lead-one correlations;
- all population and endpoint rejection counts.

The construction passes a reference only if all of the following hold:

1. at least 24 overlapping months;
2. contemporaneous Pearson correlation is at least `+0.20`;
3. OLS beta is strictly positive; and
4. the absolute contemporaneous correlation is not lower than both absolute
   one-month displacement correlations.

The overall construction passes only if both French and AQR comparisons pass.
A correlation below `+0.20`, a non-positive beta, or a stronger displaced
relationship is a construction/parser/alignment bug to investigate and fix;
it is not a market finding. The `+0.20` floor is intentionally modest because
eBull is equal-weight/top-decile/all-admitted while French is value-weighted
with NYSE breakpoints and size buckets, and AQR is rank-weighted.

As a parser/alignment control, French MOM and AQR U.S. equity MOM must have at
least 24 overlaps, positive beta and correlation at least `+0.50`. Unit tests
must mutation-prove sign inversion and one-month displacement failures on
synthetic fixtures.

## Reporting boundary

The result report must print the declaration's full SHA-256, source-response
SHA-256 values, parser versions, normalized units, exact command, window,
population census, rejected counts, and every frozen statistic. It must also
state that this ticket validates construction identity only: no haircut,
cost-adjusted return, buy-and-hold comparison or arm verdict is created here.
Those belong only to a later preregistered arm.
