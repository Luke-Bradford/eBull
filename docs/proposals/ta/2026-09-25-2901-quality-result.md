# #2901 quality arm — result

Verdict: **`GATE_FAIL`**. The identity gate did not pass, so no arm outcome was computed past the gate or published. Capital: **£0**, and no
paper ticket is opened.

- Declaration: `2026-09-25-2901-quality-declaration.md`, sha256
  `45f51798955be874d8a2ff163527eb5bbff6e1383062332889d94d3c87d5d852` at merged commit `8e8116a1` (#3400).
- The #2599 row is `declaration_id 12`, digest `9235ab28186b4a33b59f4580df1af89a2c371ed51724dac5e925e612c4582cc4`.
- The run went from 2026-09-25 20:11:41Z to 20:15:04Z, from a clean linked worktree at `8e8116a1`, with the
  declaration's command verbatim. Exit status 0, empty stderr, nothing sealed.
- Holdout access `642` (`r6-quality-gpa@r6-2901-quality-v1`, 20:11:42Z) was authorised against declaration 12.
- Output: `quality_2901/run-2026-09-25-8e8116a1.json`, sha256
  `2c0db72e87b0da86df6e0aafcb69d8a4dc8796912c8c50f2211810f8b7129211`. Its identity block matches every pin in the
  declaration, and adds `evidence_sha256` `95da8e1f3a69dbce4cbf8e95b9b2bfc57db2fdd77966ed5fbe8f8c807f69de29`.

## The gate readout (everything the verdict table allows to be published)
Gross (h = 0) monthly spread r(A) − r(D₀) against global-q GP/A `ret_vw` rank 10 − rank 1, over the 134 months
2013-07 … 2024-08, exact calendar alignment. The pass rule is #2908's frozen rule: correlation ≥ +0.20, OLS beta > 0,
and |corr| ≥ max(|lag|, |lead|), under every policy.

| policy | months | correlation | beta > 0 | lag corr | lead corr | passed |
| --- | ---: | ---: | --- | ---: | ---: | --- |
| zero_recovery | 134 | +0.198664 | yes | −0.032289 | +0.037848 | no |
| classified_worst | 134 | +0.195926 | yes | −0.036227 | +0.030359 | no |
| classified_best | 134 | +0.192826 | yes | −0.038722 | +0.019886 | no |

Composed state: F under all three policies. No policy refused. Every policy fails on the correlation bar alone: beta
is positive and the contemporaneous correlation dominates both displacements.

## What this means, and what it does not
- **The identity is unvalidated.** The declaration spec says so plainly: a failed gate "can be a defect, or it can be
  the deliberate differences (EW vs VW, universe, breakpoints, termination). It is not a market finding." Nothing
  about the quality premium follows from it, in either direction.
- **Why the verdict stands.** A correction needs an **independently evidenced** implementation or data defect: a
  failing fixture, a mismatched input or a mis-keyed month. None is evidenced. Every pin verified, pre-gate parity
  passed (a mismatch would have been `REFUSED_PRE_GATE`), and no stage raised.
  - If a later defect review produces such evidence, the spec's one gate-stage correction remains available through
    a hashed correction declaration frozen before the re-run. The evidence must stand on its own, not on this
    correlation's distance from the bar.
  - Lowering the inherited 0.20 is not a correction. The spec forbids it: "never a change of construction, threshold
    or policy made to improve a result".
  - Until then, `GATE_FAIL` is the verdict.
- **Not published:** either leg's mean, the beta's magnitude, the alpha, any cumulative figure, and every arm, control
  and comparator cell. They stay unread.

## Trial accounting carried forward
- The gate exposed A through A − D₀, so both of #2901's rows now have exposure (declaration spec, "Trial rows"):
  **2 observed rows**, A−C and A−C′. Its 6 reserved correction slots are unused and are **not** inherited.
- The next declaration in the programme (#2902) therefore counts at least **9 (#2908) + 2 (#2901) = 11** historical
  rows. Any other exposure recorded before it freezes is added under the same rule.

## Capital and programme consequence
- R6 stays at **£0**. The standing sleeve verdict (`2026-08-24-r6-sleeve-verdict.md`) is unchanged.
- #2901 closes on this result. Do not re-run it under a new threshold or construction. A future quality arm would be a
  new hypothesis with its own declaration, charged in the register.
- The programme continues with #2902 under the same harness. Its own gate is its own check; this result neither
  validates nor impeaches the shared simulator.
