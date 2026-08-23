# R6 factor-validation correction 1: exchange test issues (#2912)

Status: **FROZEN BEFORE THE CORRECTED RESULT**. This correction declaration
records a data-identity defect discovered by the first run of the already
frozen factor-validation declaration. No corrected factor return,
correlation, or regression coefficient was read before this file was frozen
and hashed.

## Why the first run is invalid

The first run produced a French correlation of `+0.026861` and an AQR
correlation of `+0.023260`, so the frozen rule correctly classified the run as
a code/data bug rather than a market finding. Its constructed-factor
distribution contained one `-25.113837` month (2019-03), while the next most
negative month was `-0.436`.

Tracing the full March 2019 member ledger identified unlinked series 30460,
vendor symbol `ZBZZT`, in the bottom leg. Its stored close moved from `14` to
`199999.99`, creating a `+14284.713571` member return. This is not a company:
Nasdaq's 2016 Equity Trader Alert 2016-229 explicitly identifies `ZBZZT` as a
Tick Size Pilot test symbol, and Nasdaq's Symbol Directory defines `Test
Issue=Y` as a test security. The corpus admission code ignored that identity
field and therefore treated synthetic production-feed traffic as an equity.

The initial run is retained in the final report but discarded as validation
evidence. The correction is an instrument-identity repair, not an outlier
filter: no return threshold, winsorisation, clipping, or post-result symbol
choice is permitted.

## Frozen correction

For every universe, exclude exchange test issues before alive/terminated
classification. The exclusion set is the union of symbols carrying
`Test Issue=Y` in Nasdaq's two official Symbol Directory files captured on
2026-08-23. The raw-response identities are:

- `nasdaqlisted.txt`: SHA-256
  `7c1842e79962337d64c4a4d863a9ffa514e04378a2321aa8b527ec8cb115055c`
  (347,540 bytes);
- `otherlisted.txt`: SHA-256
  `6313546301188942efc4c666126be6a12b2d2543dab6a2cfe679559852457e91`
  (536,571 bytes).

The frozen normalized set is:

`ATEST`, `ATEST.A`, `ATEST.B`, `ATEST.C`, `CBO`, `CBX`, `CTEST`,
`CTEST.E`, `CTEST.G`, `CTEST.L`, `CTEST.O`, `CTEST.S`, `CTEST.V`, `IGZ`,
`MTEST`, `NTEST`, `NTEST.A`, `NTEST.B`, `NTEST.C`, `ZAZZT`, `ZBZX`,
`ZBZZT`, `ZCZZT`, `ZEXIT`, `ZIEXT`, `ZJZZT`, `ZTEST`, `ZVV`, `ZVZZT`,
`ZWZZT`, `ZXIET`, `ZXYZ.A`, `ZXZZT`.

The pinned Intrader corpus contains ten of those symbols: `ATEST`, `CBX`,
`CTEST`, `MTEST`, `NTEST`, `ZAZZT`, `ZBZZT`, `ZCZZT`, `ZJZZT`, and `ZTEST`.
All ten are excluded and counted in a closed
`universe_exchange_test_issues_excluded` census stratum. The universe census
must still reconcile exactly to the vendor total. The universe-selection rule
version must change, invalidating prior strategy identities under the old
population rule.

Unit tests must prove that a listed test issue is excluded whether it is alive
or terminated, an ordinary symbol remains admitted, and the new census term
is mandatory for persistence reconciliation.

## Corrected rerun boundary

After implementing and testing only the correction above, rerun the original
declaration unchanged. All original windows, construction rules, reference
series, thresholds, and reporting requirements remain frozen. The final
report must state both declaration hashes, retain the invalid first-run
numbers, report the corrected test-issue exclusion count, and make the verdict
from the corrected run only.
