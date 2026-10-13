# #3621 avoidance filters, v2: results

Trial `3621-avoidance-filters-v2` (register r29, 86 searches, non-claiming). This is v1's five sets re-run on the
Amendment-3 panel (#3730: holding returns clipped to JKP's return cutoffs), plus the short-interest filter (SI) and
"all four".

- Run `1c5de22cccda4937b55eb7e16e505b7f`, 2026-10-09, from `origin/main` at `25577cff`; hold-out access 746.
- Ledger: `docs/research/3621-ledger.jsonl`. Verdicts, every ΔG, condition 5, the SI increment and the SI
  diagnostics' summary: `docs/research/3621-avoidance-filters-v2-verdicts.json`.
- The full report (`report_sha256` `68fff573…`) and the names file (`3b63458e…`) stay under `var/research/3621/`.

Spec: the addendum `docs/research/2026-10-09-3621-slice5-short-interest.md` over the base spec
`docs/research/2026-10-08-3621-avoidance-filters.md`. Every verdict is retrospective and holds only under the
spread-only cost model. v1's sets are retrospective twice over: their v1 outcomes were read before this declaration.

## Gates

- **Stage B rebuilt and identical.** Built inside the run (manifest `83422f50…`), binding step 2's SUB. All 673,374
  rows equal v1's stage B outside `prices.holding`, every v1 input byte-equal, and the only added input is the JKP
  return cutoffs.
- **Reproductions.** Premise 1's counts matched on all 80 stage-A formations. Premise 2's counts table matched cell
  for cell on all 38 covered formations, and the per-name file's sha256 equals the premise run's (`c005ca8a…`).
- **MAX fidelity: PASS.** Best-case arm: correlation 0.984, beta 0.964. Worst-case arm: correlation 0.981, beta 0.960.
  Lead and lag correlations are 0.09 or less.
- **Cross-source check passed before the declaration** (#3741): AAPL against Nasdaq, GME against
  shortinteresthistory.com, both equal to the stored FINRA payload.
- **Condition 5:** no SI-set population fell below the 90% coverage floor at any covered formation.
- No pair was REFUSED.

## Verdicts

37 of the 42 pairs are ELIGIBLE and 5 are NOT ELIGIBLE. To reproduce the count:

```bash
python3 -c "import json; v=[l for l in json.load(open('docs/research/3621-avoidance-filters-v2-verdicts.json'))['verdict_lines'] if ': ' in l and not l.startswith('MAX')]; print(len(v), sum(': ELIGIBLE' in l for l in v), sum('NOT ELIGIBLE' in l for l in v))"
```

ΔG is G(U_F) − G(U), in percentage points a year, for the worst-case arm at base cost over the whole path. The JSON
file has both arms, both costs and stage B.

| set | micro | small | large | mega | top 1,000 | rest |
|---|---|---|---|---|---|---|
| MAX | NOT (+1.80) | NOT (+0.18) | ELIGIBLE (+0.27) | ELIGIBLE (+0.06) | ELIGIBLE (+0.21) | NOT (+1.21) |
| sub-$5 | ELIGIBLE (+2.23) | ELIGIBLE (+0.24) | ELIGIBLE (+0.03) | NOT (−0.11) | NOT (−0.02) | ELIGIBLE (+1.97) |
| young | ELIGIBLE (+1.87) | ELIGIBLE (+2.43) | ELIGIBLE (+0.97) | ELIGIBLE (+0.20) | ELIGIBLE (+0.68) | ELIGIBLE (+2.25) |
| sub-$5 + young | ELIGIBLE (+5.56) | ELIGIBLE (+2.37) | ELIGIBLE (+0.99) | ELIGIBLE (+0.09) | ELIGIBLE (+0.66) | ELIGIBLE (+4.65) |
| all three | ELIGIBLE (+6.07) | ELIGIBLE (+2.29) | ELIGIBLE (+1.19) | ELIGIBLE (+0.12) | ELIGIBLE (+0.78) | ELIGIBLE (+4.83) |
| SI | ELIGIBLE (+0.62) | ELIGIBLE (+0.56) | ELIGIBLE (+0.59) | ELIGIBLE (+0.12) | ELIGIBLE (+0.40) | ELIGIBLE (+0.47) |
| all four | ELIGIBLE (+6.43) | ELIGIBLE (+2.76) | ELIGIBLE (+1.50) | ELIGIBLE (+0.22) | ELIGIBLE (+0.98) | ELIGIBLE (+5.28) |

- **MAX fails condition 1 on cost only** in micro, small and rest: the best-case arm at the 2x stress cost is negative
  (−0.38, −0.22 and −0.34; small's worst case at stress too, −0.13). At base cost MAX helps there.
- **sub-$5 fails in mega and top 1,000** on every cell, as in v1: there are almost no sub-$5 names to remove.

## Against v1: the clip moved micro and rest

Ten cells changed, all in micro and rest. sub-$5, young, sub-$5 + young and all three went from NOT ELIGIBLE (ΔG
−3.75 to −21.61) to ELIGIBLE (+1.87 to +6.07). MAX stayed NOT ELIGIBLE, but its ΔG turned positive (−4.58 → +1.80 in
micro) and its stage-B failure went away. Small, large, mega and top 1,000 kept every v1 verdict; their whole-path
worst-case base-cost ΔG moved by at most 0.34 pp (all three in small), and any cell by at most 0.58 pp (sub-$5 in small, stage B). This is what #3730 predicted: v1's micro and rest verdicts measured the unclipped holding-return
defect, not the filters.

## SI

- SI flags only from formation 2021-06 (38 formations), so its whole-path ΔG is 39/119 of a 2021-06..2024-08 effect;
  stage B alone runs +0.37 (mega) to +1.88 (micro) pp a year. The run checked that SI excludes nothing earlier.
- **Increment over all three** (G(all four) − G(all three), worst case, base cost): +0.10 (mega) to +0.48 (small)
  over the whole path, +0.29 to +1.46 in stage B. A diagnostic, with no verdict: the addendum does not test SI's
  increment.
- q ran 0.0795 to 0.1075. Under the unreported-is-zero rule 38 flags move over the 38 formations. The revision check
  and the identity audit (163 series, 150 among accepted matches) equal the premise run's.
- Known limits stand (addendum §"Known limits"): 38 formations in one regime, an unverified identity mapping, a
  retrospectively retrieved archive assumed first-published, and full-period calibrations.

## What a later long-book spec may cite

ELIGIBLE pairs, under the base spec's §"Inheritance contract", which lists both versions' inventories and verdicts:

- **micro, rest:** sub-$5, young, sub-$5 + young, all three, SI, all four;
- **small:** the same six;
- **large:** all seven;
- **mega, top 1,000:** MAX, young, sub-$5 + young, all three, SI, all four.

The `market-segments.md` defaults (exclude sub-$5, young and high-MAX names from longs) are unchanged. A NOT ELIGIBLE
verdict forbids citing this study for that pair; it does not repeal a default.
