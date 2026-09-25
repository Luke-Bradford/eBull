# #2901 quality arm — frozen declaration

Status: **FROZEN BEFORE ANY RETURN, FACTOR SPREAD OR GATE STATISTIC OF THIS ARM WAS COMPUTED.**

This declaration pins every input and implementation of the one run (PR C). The rules it applies are in two
specs, pinned by hash below and not restated: the construction spec (`2026-09-24-2901-quality-arm.md`) and the
declaration spec (`2026-09-25-2901-quality-declaration-spec.md`). A conflict between this document and those specs
is a defect in this document. Refs #2901, #2908, #3362, #2829, #2599.

## What has been read before this freeze
Nothing outcome-dependent. Every item below reads construction facts, dates, or data from before the window:
- the pre-look census of the artefact (`census-2026-09-25-2fb142d4.json`, no returns; posted on #2901);
- `scripts/measure_2901_offcalendar.py`, a date-only audit of the mirror (output below);
- `scripts/measure_2901_power.py`, which reads global-q months 1967-07 … 2013-06 only (output below);
- `scripts/run_2901_quality_trial.py --census-only`, which builds the books from the artefact and runs the
  census-time assertions without reading a price (output below).

The runner has never run past `--census-only` on real data. `strategy_holdout_accesses` has no row for
`r6-quality-gpa`. **#2901 has 0 observed trial rows at this freeze.**

## Frozen identity
| pin | value |
| --- | --- |
| construction spec `2026-09-24-2901-quality-arm.md` | `bd06fee9abbc971f3f5a4b71738387e5b00b9f15905c0d87935a20624d1540f4` |
| declaration spec `2026-09-25-2901-quality-declaration-spec.md` (merged `c870f265`) | `7f15089c304abb4277a3e302b17f19cff5ac7504e961fd8b46a7b2a1f949c012` |
| artefact manifest `quality_2901/2026-09-25-2fb142d4/manifest.json` | `db8ff7fbd09e3a10ebc8ec10fe4dc2d842d13ed5fcd233d7aa9808e8e0b25c74` |
| pre-look census `census-2026-09-25-2fb142d4.json` | `cf16f6d581e1baf396b0025706c0c3d5db8eb0f7818792dfbef501568b9d6719` |
| Intrader mirror commit (clean; equals the artefact's `input_sha256.mirror_commit`) | `3dbfda5ca1ccecda443fd8979671fcfe47bc2a5c` |
| global-q `prof_monthly_2025.zip` | `18dfced977d9412e2e6ec0582646f0c61b596db3a4ac88dbcdd9dae53fef963c` |
| `scripts/run_2901_quality_trial.py` | `0fef419b489421ce6f2efa5e74f41de39d8d6b9e3339ab7feac630e250e2d4f4` |
| `app/services/r6_monthly_trial.py` | `8eadb9a1f12c73ac34ba5f0f627d4db481c13218a94113df311f80340c181e55` |
| `app/services/r6_exclusion_trial.py` (= #3362's harness sha) | `bc6d960d233309cdefd1621bc01d4483037e581cc83edc2e7d88db35fa07f2f2` |
| `termination_identity(PROGRAMME_POLICIES, None)`, canonical JSON | `86cf27532419cf995bfdce708f2b5bea448d55cebb4d6e1d4f17ddc5157e2569` |
| power statement stdout | `3c269cabed6a042265a40331a593419be52cf2450ad5e6b869755c8adbcf8040` |
| `scripts/measure_2901_offcalendar.py` | `f8ea1324a4eedb4c9f22b9afc75bdd381376f56cd3100667327e90b8ea10e513` |
| `scripts/measure_2901_power.py` | `f6c4b5a69898a51d3d6875dad1f3815deaea2f2bb284a2e408784eb09f05112d` |
| `--census-only` stdout | `7c17f3064e7efb4a227831a28645cf65a80b58c1640c9b1af4ed28692c4f04f9` |

The artefact and the zip live under `~/Library/Application Support/eBull/research/quality_2901/`. The mirror is the
clone at `~/Dev/eBull/var/research_corpus/mirrors/icyDenev_Intrader`. The canonical JSON is `sort_keys` with
`(",", ":")` separators (`run_2901_quality_trial.canonical_sha256`). Every value above was recomputed at
`18f58cdd` on 2026-09-25, and the runner re-verifies each one before it writes its holdout row.

### Off-calendar audit (verbatim)
`PYTHONPATH=. uv run python -m scripts.measure_2901_offcalendar --root <mirror>`
```
{
  "script_sha256": "f8ea1324a4eedb4c9f22b9afc75bdd381376f56cd3100667327e90b8ea10e513",
  "mirror_commit": "3dbfda5ca1ccecda443fd8979671fcfe47bc2a5c",
  "files": 22879,
  "bars": 50134060,
  "off_calendar": {
    "closure": 15880
  },
  "files_with_off_calendar": 2446,
  "files_starting_off_calendar": 1,
  "files_ending_off_calendar": 0,
  "off_calendar_in_window": 8,
  "window": [
    "2013-06-01",
    "2024-09-27"
  ]
}
```
This matches the declaration spec's measured values (no weekend bars, 8 closure-dated bars in the window, no file
ending on one).

### Power statement (verbatim; no verdict is predicted from it)
`PYTHONPATH=. uv run python -m scripts.measure_2901_power --zip <prof_monthly_2025.zip> --sha256 18dfced977d9412e2e6ec0582646f0c61b596db3a4ac88dbcdd9dae53fef963c`
```
months (1967, 7)..(2013, 6) n=552  tracking_error_annual=0.0798
measured proxy TE=0.0798 t>3.0: annual excess for 50% power 0.0717, 80% power 0.0918
measured proxy TE=0.0798 t>1.96: annual excess for 50% power 0.0468, 80% power 0.0669
sensitivity 6% TE=0.0600 t>3.0: annual excess for 50% power 0.0539, 80% power 0.0690
sensitivity 6% TE=0.0600 t>1.96: annual excess for 50% power 0.0352, 80% power 0.0503
sensitivity 10% TE=0.1000 t>3.0: annual excess for 50% power 0.0898, 80% power 0.1150
sensitivity 10% TE=0.1000 t>1.96: annual excess for 50% power 0.0587, 80% power 0.0838
```
Its limits are the declaration spec's ("Power"): IID, value-weight decile 10 against the decile average, the 2025
release, and the HAC t at 3.0 only.

### Census-time assertions (construction facts, not outcomes)
`PYTHONPATH=. uv run python -m scripts.run_2901_quality_trial --census-only --input <artefact> --input-sha256 db8ff7fb…5c74`
exits 0: every schedule assertion, the C′ schema pin and the D₀ formula check pass. Per formation:

| D | X(D) | E(D) = C(D) | A(D) | D₀(D) | C′(D) | decile-0 boundary ties |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 2013-06-28 | 2013-07-01 | 1,283 | 128 | 129 | 2,429 | 0 |
| 2014-06-30 | 2014-07-01 | 1,388 | 138 | 139 | 2,705 | 0 |
| 2015-06-30 | 2015-07-01 | 1,407 | 140 | 141 | 2,784 | 0 |
| 2016-06-30 | 2016-07-01 | 1,379 | 137 | 138 | 2,687 | 0 |
| 2017-06-30 | 2017-07-03 | 1,368 | 136 | 137 | 2,702 | 0 |
| 2018-06-29 | 2018-07-02 | 1,400 | 140 | 140 | 2,776 | 0 |
| 2019-06-28 | 2019-07-01 | 1,613 | 161 | 162 | 2,783 | 0 |
| 2020-06-30 | 2020-07-01 | 1,621 | 162 | 163 | 2,793 | 0 |
| 2021-06-30 | 2021-07-01 | 1,595 | 159 | 160 | 3,125 | 0 |
| 2022-06-30 | 2022-07-01 | 1,806 | 180 | 181 | 3,323 | 0 |
| 2023-06-30 | 2023-07-03 | 1,790 | 179 | 179 | 3,228 | 0 |
| 2024-06-28 | 2024-07-01 | 1,708 | 170 | 171 | 3,053 | 0 |

## Trial-row table H
The rule is the declaration spec's "Corrections and trial accounting". H = every exposed row of every register entry
in the r6/selection programme that exists at this freeze.

**Which entries exist.** Three places were checked on 2026-09-25:
- `strategy_preregistration_declarations` holds 7 rows (ids 5–11). None is r6 or selection-programme.
- `TRIAL_REGISTER` (r10) holds none either.
- `strategy_holdout_accesses` has r6 rows only for #2908: accesses 640 (factor gate) and 641 (outcome), both
  `r6-dilution-exclusion@r6-2908-exclusion-v1`. `strategy_holdout_access_refusals` and `strategy_results_store`
  have no r6 rows.

#3360, #3361 and #3362 produced censuses and fixtures only; #3362 excluded "any strategy look" from its scope. #2834
ARM B is a separate research seat and not in this programme. So H is #2908's rows alone. Reproduce the ledger half
with:
```
PYTHONPATH=. uv run python -c "
import psycopg; from app.config import settings
with psycopg.connect(settings.database_url) as c:
    print(c.execute(\"select strategy_id, strategy_version, prereg_purpose from strategy_preregistration_declarations order by declaration_id\").fetchall())
    print(c.execute(\"select access_id, strategy_id, accessed_by from strategy_holdout_accesses where strategy_id like 'r6%%' order by 1\").fetchall())"
```

**#2908's configurations.** A configuration is identified by the frozen documents and code that change a computed
cell. #2908's documents are its preregistration, corrections 1–4 and its result. They give three configurations:
- **K0**, the original declaration (`91ec1135…`, harness `aaa64784…`).
- **K1**, correction 1, the halt bound (harness `bfba9460…`). It changed outcome cells. Correction 2 (ISO date
  encoding) changed no computed cell, so it is also K1.
- **K2**, correction 3, the cover resolver (manifest `0b25af8c…`). The spec counts it as a new configuration.
  Correction 4 (holdout audit logging) changed no computed cell, so it is also K2.

| row | configuration | arm | exposure (evidence) |
| --- | --- | --- | --- |
| 1 | K0 | dilution exclusion | the factor identity gate's statistics were opened in K0 (correction 1: "the already opened factor-only PASS"). The gate is Nsi P10−P1, the dilution arm's excluded set, so it exposes that arm — as #2901's own gate exposes A |
| 2 | K1 | dilution exclusion | outcome `95ff5c23…` (all cells) |
| 3 | K1 | filing-risk exclusion | outcome `95ff5c23…` |
| 4 | K1 | union | outcome `95ff5c23…` |
| 5 | K2 | dilution exclusion | reproduction `1ed032ca…`, audited `99a6fcca…` (access 641); factor `cf05211d…` (access 640) |
| 6 | K2 | filing-risk exclusion | as row 5 |
| 7 | K2 | union | as row 5 |

**K0's failed outcome run is exempt, under the spec's condition.** Correction 1 records what it produced:
- zero stdout bytes and no return cell;
- a missing-bar failure: existing holding YTEN had no bar on 2024-07-01, and its series resumed on 2024-09-03.

Those are the exit status, the failure category and the traceback's content. They are bar dates and a membership fact,
which is the same class of evidence as the construction's executability reads and this programme's date-only
off-calendar audit. None is a price, a return or a flag derived from one. So K0's filing-risk and union arms have no
exposure. K0's dilution row stands because of the factor gate, not because of this run.

The literal buy-and-hold and the annual 1/N were emitted as comparators, not as arms. The spec's #2908 rows are
"each arm it actually emitted", so they are not rows.

**|H| = 7.**

## The frozen family
M = |H| + 2 + 6 = 7 + 2 + 6 = **15**:
- 7 historical rows (above);
- 2 observed rows at #2901's first run: A−C (headline) and A−C′ (diagnostic);
- 6 reserved slots, two for each of the 3 permitted corrections, counted whether used or not.

The runner takes `--history-rows 7` and computes M with `family_size`. It also computes the Bonferroni bar with
`bonferroni_t(M)`. Reproduce the bars with:
`PYTHONPATH=. uv run python -c "from app.services.r6_monthly_trial import family_size as f, bonferroni_t as b, cohort_t_bar as q; print(f(7), b(f(7)), q(10))"`.
At M = 15 the Bonferroni term is below 3, so the condition's binding half is t > 3.

The claim is FWER over this frozen family at α = 0.05 and nothing wider. A later declaration (#2902–#2904) inherits
#2901's **observed** rows only, not its reserved slots.

## The #2829 register row and the #2599 declaration
- **Trial register (r11).** `r6-2901-quality-gpa-2026-09-25`, with `searches = 2` and `EXACT`, carries
  `declared_for = ("r6-quality-gpa", "r6-2901-quality-v1")`. That is the identity the runner writes its holdout row
  under (`run_2901_quality_trial.STRATEGY_ID` / `STRATEGY_VERSION`).
- **#2908 is charged in the same bump.** It ran after `TRIAL_REGISTER_CUTOFF` without charging the register. Its
  entry is `r6-2908-exclusion-arms-2026-08-24` (searches = 3, one per variant). The register counts variants
  selected between; H counts (configuration, arm) rows under this programme's rule. The two numbers answer different
  questions and are not reconciled.
- **Preregistration row.** `scripts/freeze_2901_quality_declaration.py` freezes it:
  - purpose `capital_candidate`, universe basis `survivorship_free`, carry and FX unmodelled both `false`, so the
    expected structural refusals are empty;
  - policy `structural-refusal-policy-2026-08-21-v5-cagr-saturation`;
  - forward-shadow floor of 11 decision dates and 574 weeks, derived in the script;
  - dry-run digest `9235ab28186b4a33b59f4580df1af89a2c371ed51724dac5e925e612c4582cc4`.

  It is run once, after this merges and before the run, and #2599's gate inside `record_holdout_access` then checks
  the run against it. Two things this row does **not** do:
  - `capital_candidate` does not make QUALITY PIT-admissible. That is a separate gate, and the paper ticket carries
    it (below).
  - It does not authorise capital; the verdict table decides that.

## Declared residuals and deviations
Repeated verbatim from the construction spec:

- R1 linkage keeps about half of the in-range series, tilts liquid, and liquidity windows can span a succession. R2
  complete case and alias scope: issuers without a standard component tag are out; aliases differ in scope from each
  other and from Compustat. R3 SIC is current. R4 companyfacts is today's extraction (registry `system_versions`
  fail); integrity exclusion is snapshot-wide. R5 termination class is mostly `unknown` before 2019. R6 Intrader
  symbols, bar bounds, collisions and the 2013 start rest on capture metadata. R7 the X(D) row (its close and the
  vendor's retrospective adjustment). R8 components come from different accessions and restatement bases. R9
  plain/class grammar plus FPI form is not CRSP share code 10/11.
- D1 equal weight (HXZ: value weight), declared in PR B. D2 breakpoints over E(D) (HXZ: NYSE). D3 parent
  stockholders' equity for the sign screen (HXZ: book equity). D4 20-F/40-F as the share-code proxy. D5 formations
  start 2013, not 2011. D6 one security per issuer (the liquidity winner). D7 the annual-duration window, literal
  transition handling and the revenue/COGS sign screens. D8 size proxied by dollar volume and assets.
- **PIT registry**: `R6RankingIdentity.QUALITY` is not made admissible; the declaration lists every failing cell of
  `COMPANYFACTS_PIT` and `RESEARCH_PRICES` as a stated residual, as #2908 did for its families.

The failing cells are from `app/services/research_point_in_time.PROBE_MATRIX` at `18f58cdd`:

| family | failing condition | probes |
| --- | --- | --- |
| `companyfacts_pit` | `system_versions` | C2 |
| `companyfacts_pit` | `historical_population` | P2, P5 (#3361 linkage: typed abstentions; identity correctness unmeasured) |
| `research_prices` | `system_versions` | P1 |
| `research_prices` | `historical_population` | P2, P5 |
| `research_prices` | `causal_transform` | P3, P4 |

`IDENTITY_FAMILIES[QUALITY]` also names three more families: `fundamental_facts` (system_versions F1/F2,
historical_population P2/P5), `derived_fundamentals` (system_versions D1, historical_population P2/P5,
causal_transform D1) and `historical_population` (H1/H2/H3). They are listed for completeness. The artefact's
fundamentals come from the #3360 file store, which is `companyfacts_pit`, not from those database families.

Repeated verbatim from the declaration spec:
- **R10**: gaps at month-end marks are held at the last close. Stale marks relocate losses in time and can bias
  volatility, autocorrelation and covariance, and not necessarily equally across portfolios. The stale-mark census
  reports their exposure and durations.
- **R11**: recoveries sit in cash until the next X(D), identically for every portfolio.
- **R12**: `SPY` is charged the programme h.
- **R13**: the Bonferroni deflation is conservative under correlated trials, and the family is only as complete as
  the trial-row table.
- **R14**: minimum notional and broker reachability are unobservable historically, and are enforced only live.
- **R15**: C′'s screens bind only where they resolved (the missingness confound above).
- **R16**: the gap/terminated status is ex-post, taken from the stored series bounds. Under `zero_recovery` it moves
  only the month in which a loss is recorded; under the classified policies it also moves recovery values.
- **R17**: recoveries are credited at recognition, not at a dated payment.
- **R18**: executability reads the X(D) row itself, including its close, so no real-time-executable inference is
  drawn.
- **R19**: "worst" and "robust" are across the three programme policies only.
- **R20**: the reference file has no documented missing-value contract; in-domain missing codes are undetectable.

This declaration adds:
- **R21**: H is reconstructed from #2908's documents and the ledger, not from a register #2908 wrote at the time;
  #2908 had no #2599 declaration. The reconstruction is only as complete as those records.

## The run (PR C)
After the freeze, one command, from a linked worktree:
```
PYTHONPATH=. uv run python -m scripts.run_2901_quality_trial --acknowledge-open-preregistered-outcomes \
  --input "$HOME/Library/Application Support/eBull/research/quality_2901/2026-09-25-2fb142d4" \
  --input-sha256 db8ff7fbd09e3a10ebc8ec10fe4dc2d842d13ed5fcd233d7aa9808e8e0b25c74 \
  --price-mirror "$HOME/Dev/eBull/var/research_corpus/mirrors/icyDenev_Intrader" \
  --price-mirror-commit 3dbfda5ca1ccecda443fd8979671fcfe47bc2a5c \
  --global-q "$HOME/Library/Application Support/eBull/research/quality_2901/prof_monthly_2025.zip" \
  --global-q-sha256 18dfced977d9412e2e6ec0582646f0c61b596db3a4ac88dbcdd9dae53fef963c \
  --runner-sha256 0fef419b489421ce6f2efa5e74f41de39d8d6b9e3339ab7feac630e250e2d4f4 \
  --monthly-trial-sha256 8eadb9a1f12c73ac34ba5f0f627d4db481c13218a94113df311f80340c181e55 \
  --exclusion-trial-sha256 bc6d960d233309cdefd1621bc01d4483037e581cc83edc2e7d88db35fa07f2f2 \
  --termination-identity-sha256 86cf27532419cf995bfdce708f2b5bea448d55cebb4d6e1d4f17ddc5157e2569 \
  --power-output-sha256 3c269cabed6a042265a40331a593419be52cf2450ad5e6b869755c8adbcf8040 \
  --history-rows 7 \
  --sealed-dir "$HOME/Library/Application Support/eBull/research/quality_2901/sealed"
```
Everything the run emits is published: the gate readout, and after a pass every cell, readout and verdict the
declaration spec names. The same holds when the result destroys the arm. A refusal or raise publishes only what the
verdict table allows. Any correction follows the spec's rule: a hashed correction declaration frozen before the
re-run, at most one per stage, and a new `strategy_version` with its own register entry, because a declaration row
is immutable.
