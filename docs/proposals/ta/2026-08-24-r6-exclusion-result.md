# R6 #2908 exclusion-screen result

Scientific verdict: **FAIL — EXCLUSION DID NOT BEAT IDENTICAL EQUAL-WEIGHTING**

Programme-bar result: **PASS_ROBUST versus literal buy-and-hold**, under the frozen minimal pass rule. This is
reported, not promoted: the incremental exclusion claim failed its own plain-market control.

- Original declaration SHA-256: `91ec11351d8851e4b3b89ba51f965b649608916346f2e10d9a7cdede9fd2c62f`
  at commit `222d84386ba00753ec49a7974d694403cc58624e`
- Halt-bound correction SHA-256: `cd694a39f392cf438e4331a29b9fe8613048127ee37770295c00650758f376fa`
- ISO-date correction SHA-256: `becf2537852a85becfc0f444dccc56e9b50a16ebf48a339619bc8187b4e5a858`
- Cover-fallback correction SHA-256: `f6bbac8492967c881473b30cef3d475252976d9bcea9a75eb6936a5f6c34b425`
- Canonical complete result SHA-256: `1ed032ca1243301764eb6e9fd9abef562e884be61c476ff50ec377babe2e1426`
- Window: formation at `2022-06-30 16:00`; fills `2022-07-01`; final close `2024-09-27`
- Return/cost basis: USD split-and-dividend-adjusted wealth, long/x1, `0.725%` half-spread on every traded dollar,
  zero commission and structural-zero carry

Both failed attempts emitted zero stdout bytes. Correction 1 specified the already-declared best/zero bound for a
holding halted across a valuation session; correction 2 changed only ISO result encoding. No return cell was seen
before either correction was frozen.

## Factor identity gate

The gate passed on 27 months (`2022-07..2024-09`): correlation `+0.291888`, beta `+0.846831`, alpha
`+0.075376/month`, lag `+0.197732`, lead `+0.088728`; no invalid price row was skipped. Factor-only result SHA-256:
`bf64591739f0fd0dcfdb480b8120e51f68b8659f184337285ff4b0336f1e89cd`.

The large intercept and short window make this identity evidence modest, not an independent investability claim.
It nevertheless clears the frozen sign/timing gate and is not wrong-signed or displaced.

Pre-push review's resolver fix changed no population record or rank. Factor and outcome JSON reproduced exactly
after deleting only the manifest-identity field; the canonical result above binds corrected manifest
`0b25af8c37b12437963f4681df7513397b83cd0ba7e37b15e1165b52f90d17d2`.

## Full result

Total return over the pinned 27-month window:

| Portfolio | Names 2022 / 2023 / 2024 | Best net | Worst net | Worst vs annual 1/N | Worst 15% | Worst 58% |
|---|---:|---:|---:|---:|---:|---:|
| Literal 2022 buy-and-hold | 5,102 / — / — | +27.2910% | +0.6038% | — | — | — |
| Annual full 1/N | 5,102 / 4,993 / 4,771 | +53.5777% | +16.8024% | baseline | — | — |
| Dilution exclusion (primary) | 4,582 / 4,399 / 4,034 | +53.0641% | +16.4349% | **−0.3675 pp** | +13.8793% | +6.5533% |
| Filing-risk exclusion | 4,619 / 4,268 / 4,102 | +39.6028% | +12.6666% | **−4.1358 pp** | +10.6938% | +5.0387% |
| Union | 4,186 / 3,847 / 3,603 | +43.2194% | +15.8080% | **−0.9944 pp** | +13.3434% | +6.2784% |

The haircut columns are adjusted absolute net return and all exceed net buy-and-hold, including at 58%. The
primary therefore satisfies the programme's declared positive-absolute and buy-and-hold bar. But annual full 1/N
uses identical dates, source, execution, costs and termination rule, and beats every exclusion in both the best and
worst cases. The measured gain is annual refresh/rebalancing, not dilution avoidance. Shipping the screen would
discard return while adding a rule.

Worst-case dilution turnover was `3.289239x` initial capital; charged spread cost was `2.384698%` of initial
capital (£1,192.35 on £50,000). Its 58%-haircut return is +6.5533%, versus +0.6038% for buy-and-hold, but it still
lags the simpler annual full population. Best-case dilution also lags annual 1/N by 0.5136 pp.

## Overlap and termination sensitivity

Dilution/red-flag Jaccard overlap rose from `9.50%` to `15.10%` to `20.38%`; the two screens are related but not
duplicates. Quality overlap remains unavailable until ordered ticket #2901, so independence from quality is not
established.

Termination uncertainty is decisive: buy-and-hold ranges from +0.60% to +27.29%, and annual 1/N from +16.80% to
+53.58%. The runner counts every censored holding and worst-case zero recovery governs capital. No narrower year,
survivor-only subset, alternative breakpoint or cheaper cost case will be opened.

Conclusion: record the declared buy-and-hold pass, reject the exclusion hypothesis against its identical 1/N
control, and stop. Do not tune #2908.
