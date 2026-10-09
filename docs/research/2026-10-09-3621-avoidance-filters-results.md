# #3621 avoidance filters, v1: results

Trial `3621-avoidance-filters-v1` (register r28, 62 searches, non-claiming).

- Run `2590008207b64e83baf019ad24ee93c6`, 2026-10-09, from `origin/main` at `1312fb52`.
- Hold-out access 742 (`evaluate`), with diagnostic reads 743 and 744.
- Ledger: `docs/research/3621-ledger.jsonl`. Verdicts and every ΔG: `docs/research/3621-avoidance-filters-v1-verdicts.json`.
- The full report (`report_sha256` `b26d5bc9…`) and the names file (`9f2b1ec1…`) stay under `var/research/3621/`.

Spec: `docs/research/2026-10-08-3621-avoidance-filters.md`. Every verdict is retrospective and holds only under the spread-only cost model.

## Gates

- **Reproduction.** Slice 1's flags gave premise 1's per-formation counts exactly on all 80 stage-A formations.
- **MAX fidelity: PASS.** Measured against JKP's `rmax1_21d` on 80 holding months, 2014-10..2021-05, with no undersized months.
  - Best-case arm: correlation 0.985, beta 0.971.
  - Worst-case arm: correlation 0.983, beta 0.968.
  - Lead and lag correlations are 0.08 or less.
- No pair was REFUSED. Every pair had exclusions in stage B, so no verdict carries the `no stage-B exclusions` label.

## Verdicts

ΔG is G(U_F) − G(U), in percentage points a year, for the worst-case arm at base cost. The JSON file has both arms and both costs.

| set | micro | small | large | mega | top 1,000 | rest |
|---|---|---|---|---|---|---|
| MAX | NOT (−4.58) | NOT (−0.15) | ELIGIBLE (+0.28) | ELIGIBLE (+0.06) | ELIGIBLE (+0.21) | NOT (−3.43) |
| sub-$5 | NOT (−21.61) | ELIGIBLE (+0.04) | ELIGIBLE (+0.03) | NOT (−0.11) | NOT (−0.02) | NOT (−13.22) |
| young | NOT (−7.01) | ELIGIBLE (+2.49) | ELIGIBLE (+0.98) | ELIGIBLE (+0.20) | ELIGIBLE (+0.69) | NOT (−3.75) |
| sub-$5 + young | NOT (−18.57) | ELIGIBLE (+2.21) | ELIGIBLE (+1.00) | ELIGIBLE (+0.09) | ELIGIBLE (+0.66) | NOT (−10.67) |
| all three | NOT (−18.16) | ELIGIBLE (+1.95) | ELIGIBLE (+1.20) | ELIGIBLE (+0.13) | ELIGIBLE (+0.79) | NOT (−10.62) |

## The micro and rest verdicts measure a data defect, not the filters (#3730)

The panel's holding returns include name-months above +300%. The largest, EXXI in 2017-01, is +24,900%. These are consistent with unadjusted reverse splits and old and new equity joined across a bankruptcy.

- Of the 257 such name-months, micro holds 252 and rest holds 256. The filters flag 249 of micro's 252.
- Small holds 4, large 0, mega 1 and top 1,000 1.
- The unfiltered equal-weight micro book shows G = 24.2%/yr over the whole path, and +110% in 2023 with a −9% maximum drawdown.

Removing the flagged names removes those false gains, so ΔG in micro, rest and the pooled population is dominated by the defect. Those verdicts are recorded as computed, under the frozen rule. They are not evidence that the filters hurt. A NOT ELIGIBLE verdict only forbids citing this study; it does not repeal a default (spec §"Question").

Small, large, mega and top 1,000 together hold at most 6 of the 257 name-months (mega and top 1,000 overlap), so their verdicts are far less exposed. They are still on the same panel and get re-checked when #3730 is fixed. A re-measurement on corrected data is a new trial id.

## What a later long-book spec may cite

ELIGIBLE pairs, under the spec's §"Inheritance contract", with #3730's status stated:
- **small:** sub-$5, young, sub-$5 + young, all three;
- **large:** all five sets;
- **mega:** MAX, young, sub-$5 + young, all three;
- **top 1,000:** MAX, young, sub-$5 + young, all three.

The `market-segments.md` defaults are unchanged: exclude sub-$5 names, young names and high-MAX names from longs.
