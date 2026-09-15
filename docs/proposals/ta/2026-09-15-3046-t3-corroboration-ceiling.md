# #3046 residual 3 — the ceiling on T3's turnover corroboration, measured and published

Status: proposal (unshipped). Scope: residual 3 of #3046, the last open residual.

Revision 2. Revision 1 was reviewed by Codex at checkpoint 1, which returned 39
findings; the ones that changed the design are marked **⚠ ckpt-1** below. Two of
them inverted a conclusion, so revision 1's cause ranking is **withdrawn**, not
edited.

## What residual 3 asked

> `price_daily.volume` is NULL for **every bar of 3,010 of 12,284 instruments**
> (partial for 6,419, complete for 2,855). T3's turnover corroboration is
> unreachable for those, which is why both examples above land on
> `unclassifiable`. Worth stating as a known ceiling on T3 wherever it is cited;
> not necessarily fixable here.

The deliverable is a **statement of the ceiling wherever T3 is cited**. That makes
it a judgement artefact: the CAUSE and the NUMBER both have to be right, because
both become the next reader's premise.

## Source rule

The governing rule is the code's own, not a regulator's:
`price_quarantine._corroboration` (`app/services/price_quarantine.py:420-444`)
returns `unclassifiable` **iff** `_usable_volume` or `_usable_close` is `None` on
either endpoint, where `_usable_volume` rejects `None` **and any value `<= 0`**
(`:302-306`). Every arm below is defined against that predicate rather than against
`volume IS NULL`, because the two are not the same test.

Two further rules bind the measurement and were read before it, not after:

- **A provisional transition is `unclassifiable` WITHOUT READING VOLUME AT ALL**
  (`:486-492`): *"T3 reads volume, and a provisional bar's volume is a part-session
  count. DEFER the verdict."* A deferred verdict is not blindness, and mixing the
  two inflates the ceiling. ⚠ **ckpt-1 finding 11.**
- **The class the verdict was computed under is stored**, not derived live —
  `price_quarantine_coverage.asset_class`, *"as seen at evaluation time; NULL is a
  real state"* (`sql/247_price_quarantine.sql:45`). ⚠ **ckpt-1 finding 20.**
- **`_int_or_none` maps a source `0` to `None`** (*"returning None for zero or
  missing"*, `app/providers/implementations/etoro.py:861-869`). So a stored NULL
  cannot by itself distinguish "the source omitted volume" from "the source said
  zero", and no storage-side query can establish a source claim.
  ⚠ **ckpt-1 finding 22.** Measured consequence: `price_daily` holds **0** rows
  with `volume = 0` and **0** with `volume < 0`, so the stored column is
  NULL-or-positive by construction and `_usable_volume`'s `<= 0` clause never
  fires on it today.

## Scope of every figure below

`price_daily` only — the eToro execution corpus, 7,002,478 bars / 12,284
instruments. The same rule set also evaluates `research_price_daily` (75,972,665
bars) into `research_transition_quarantine`, whose volume coverage is a different
population and is NOT measured here. ⚠ **ckpt-1 finding 39.** The corpus holds
exactly one rule-set version, asked of the corpus rather than imported from the
module under measurement (prevention log, "a verifier pinned to its own code hash").

The denominator throughout is the **evaluated T3 trigger population** —
`corroboration <> 'not_applicable'`, the same predicate
`price_quarantine_store.py:381,402` uses. That is not "every large move": T1 and T2
suppress T3 evaluation entirely (`price_quarantine.py:485` — `and not rules`), so
transitions already explained by an unusable endpoint or a series hole never enter
it. ⚠ **ckpt-1 finding 17.**

## Full-population verification

### Arm 1 — the number is 25.8%, and 0% of it is deferral

| corroboration | rows | provisional |
| --- | ---: | ---: |
| `unclassifiable` | 359 | 0 |
| `spike` (admitted back) | 72 | 0 |
| `collapse` | 40 | 0 |
| `flat` | 13 | 0 |
| **trigger population** | **484** | **0** |

Reachable = **125 / 484 = 25.8%**. Three docstrings and one applied migration carry
a hand-written `~30%`.

⚠ The provisional column is **0 today and the arm still ships**, for the reason
this ticket recorded on residual 5: a clause justified by a count has to be
un-justified the moment the count moves, and the deferral path is a RULE, not an
observation. ⚠ **Reachable is not the same as admitted**: only the 72 `spike` rows
are admitted back; `collapse` and `flat` are classified AND still quarantined. So
"25.8% reachable" and "14.9% admitted" are different statements and the script
prints both. ⚠ **ckpt-1 finding 19.**

### Arm 2 — asset class explains very little, and "equity-only" is incomplete rather than false

Joined on `price_quarantine_coverage.asset_class` (the evaluated class). It agrees
with the live `instruments.exchange -> exchanges.asset_class` join on **12,284 of
12,284** rows today; the script reports that reconciliation rather than assuming it.

| evaluated class | trigger pop | reachable |
| --- | ---: | ---: |
| `us_equity` | 441 | 102 |
| `eu_equity` | 31 | 19 |
| `uk_equity` | 8 | 3 |
| `asia_equity` | 2 | 0 |
| `mena_equity` | 1 | 1 |
| `fx` | 1 | 0 |

**`us_equity` carries 441 of the 484 triggers.** The claim in the rule module —
*"(volume is equity-only, S3)"* — is defensible as a **tendency** and misleading as
**the** explanation, and revision 1's flat "FALSE" was itself an overstatement.
⚠ **ckpt-1 finding 5.** Measured per-instrument all-null rates: **90.8%** for the
structurally volume-free classes (crypto/fx/index/commodity, 396 of 436
instruments) against **22.1%** for equities (2,614 of 11,848). The tendency is real
and strong. What it cannot do is carry the ceiling: 2,614 of the 3,010 all-null
instruments are equities, and the trigger population is 91% `us_equity`.

### Arm 3 — ⚠⚠ revision 1's ranking was WRONG, in its UNIT

Revision 1 ranked the causes by counting **instruments** (68 never-volume vs 56
partial) and then allocated **335 transitions** across them. Those 68 instruments
could have carried anywhere from 68 to 279 of those transitions.
⚠ **ckpt-1 finding 1.** Counted per TRANSITION, the ranking inverts:

| instrument volume coverage | trigger pop | reachable | blind |
| --- | ---: | ---: | ---: |
| `partial` | 383 | 102 | **281** |
| `never_any_volume` | 78 | 0 | 78 |
| `complete` | 23 | 23 | 0 |

**The dominant blindness is on instruments that DO carry volume elsewhere.** The
never-volume population residual 3 was filed about contributes 78 of 359.

### Arm 4 — the era is real, small, and not the driver either

Classified per instrument against **its own** first volume-bearing bar, using
**both** endpoint dates (`prior_date` and `price_date`), not a single global date —
a transition can straddle the onset. ⚠ **ckpt-1 findings 8 and 16.**

| position relative to that instrument's volume onset | trigger pop | reachable |
| --- | ---: | ---: |
| both endpoints after onset | 388 | 125 |
| instrument never has volume | 78 | 0 |
| both endpoints before onset | 18 | 0 |
| straddles onset | 0 | 0 |

**263 of the 359 blind transitions sit entirely AFTER their own instrument's volume
onset.** Neither the era nor the never-volume set explains them.

There *is* a source-side onset cluster: the distribution of each instrument's first
volume-bearing bar has **2023-05-22 as a single dominant mode at 3,085
instruments**, against 540 for the runner-up (2024-11-21) once the censoring filter
is applied. ⚠ That is 3,085 of **9,274** volume-bearing instruments — a dominant
mode, **not "the bulk"**, which is what revision 1 called it. ⚠ The runner-up figure
differs from the one an unfiltered query returns (653): the script's number is the
censored-onset one, and the script is the authority for every figure in this file. ⚠ **ckpt-1 finding 15.** ⚠ `min(price_date) FILTER (WHERE volume IS NOT NULL)`
is censored by the instrument's first STORED bar, so the script reports onset only
where an earlier volume-free stored bar exists. ⚠ **ckpt-1 finding 14.**

### Arm 5 — what the blindness actually is: volume-free RUNS

Endpoint state of the 359 `unclassifiable` rows, `NULL` and `<= 0` distinguished:

| both endpoints NULL | exactly one NULL | neither NULL | any `<= 0` |
| ---: | ---: | ---: | ---: |
| 337 | 22 | 0 | 0 |

⚠ `neither NULL = 0` is **near-tautological** — `unclassifiable` is *defined* as a
missing usable volume on some endpoint — and is reported as a self-check on the
reproduction, not as evidence. ⚠ **ckpt-1 finding 12.** The informative cell is the
**337 / 22 split**: absence is overwhelmingly on BOTH sides, i.e. a run, not a
one-bar hole. Confirmed directly: of the 263 post-onset blind transitions, **219
sit inside a ±14-day window that is entirely volume-free**, and 44 in a partially
covered one.

### Arm 6 — ⚠⚠ the punchy claim was tested and DEFLATED

The volume-free runs concentrate on distressed names — the top carriers are
`FXLV`, `NKLAQ`, `ZPTA`, `QTTOY`, `XELA`, `IDEXQ`, `FRCB`, `GOEVQ`. The tempting
headline is *"corroboration is missing precisely where level breaks are generated"*.
Tested on the full population, **it does not hold as stated**, and the top-8 list
that suggested it is a sample of eight:

| observed cohort | instruments | trigger pop | blind | blind % |
| --- | ---: | ---: | ---: | ---: |
| symbol ends `Q` | 39 | 164 | 124 | 75.6 |
| other | 175 | 320 | 235 | 73.4 |

| series state | trigger pop | blind | blind % |
| --- | ---: | ---: | ---: |
| ENDED (last bar > 5d before frontier) | 317 | 247 | 77.9 |
| LIVE | 167 | 112 | 67.1 |

**Blindness is near-uniform at 67-78% across the two cohorts in THIS arm.** What
IS concentrated is the **trigger** side: `Q`-suffix names are **112 of 12,284**
instruments (0.9% of the corpus) but carry **164 of 484** triggers (33.9%). So
distress predicts *generating a level break*, not *being unadjudicable*.

⚠⚠ **DO NOT GENERALISE THAT BAND — revision 2 of this spec did, and arms 2 and 4
contradict it.** ⚠ **ckpt-2 P2.** Across asset class the spread is wide
(`us_equity` 339/441 = **76.9%** blind against `eu_equity` 12/31 = **38.7%**), and
the `never_any_volume` and `both endpoints before onset` strata are **100%** blind.
Nor is the exclusion claim safe: dropping the never-volume stratum alone moves
reachability from 125/484 (**25.8%**) to 125/406 (**30.8%**). The correct statement
is cohort-scoped — "no NAMEABLE DISTRESS cohort explains it", which is what arm 6
tests — not "uniform everywhere".

⚠ The `Q` suffix is a symbol-text cohort, and a classifier over source text is a
data-treatment decision. The structured sources (`sec_form25_register`,
`sec_form25_common_equity_delistings`) exist and the series-state arm above is the
structured one; the suffix cohort is reported as an **observed cohort only** and is
never used as a rule — the same treatment `verify_3046_weekend_bar_census.py` gives
`.24-7`.

## The honest statement of the ceiling

1. **25.8% of evaluated T3 triggers can be corroborated at all** (125/484), of
   which 14.9% (72/484) are actually admitted back.
2. **The blindness is broad in the sense that matters, and NOT uniform.** The
   distress cohorts arm 6 tests do not explain it (67-78%, a narrow band). But asset
   class and onset era both spread widely (38.7% to 100% blind), and excluding the
   never-volume stratum moves reachability 25.8% -> 30.8%. So: no nameable distress
   cohort carries the ceiling; several structural strata do move it.
3. **Its dominant carrier is volume-free RUNS inside instruments that do report
   volume elsewhere** (281 of 359 blind rows; 219 of the post-onset ones sit in a
   fully volume-free ±14-day window), not the never-volume instruments residual 3
   was filed about (78) and not the pre-onset era (18).
4. **Asset class is a strong per-instrument tendency (90.8% vs 22.1% all-null) that
   nevertheless explains almost none of the ceiling**, because 91% of triggers are
   `us_equity`.

## Why `price_quarantine.py` and `sql/247` are NOT edited — two mechanisms

Five sites carry the stale `~30%` / `equity-only` claim. **Two are immutable, for
two different and independently sufficient reasons:**

| site | correctable? | mechanism |
| --- | --- | --- |
| `app/services/price_quarantine.py:30` | **no** | code-hash identity |
| `app/services/price_quarantine.py:430-431` | **no** | code-hash identity |
| `sql/247_price_quarantine.sql:102-103` | **no** | migration content checksum |
| `app/services/price_quarantine_store.py:81` | yes | — |
| `app/services/price_quarantine_store.py:396` | yes | — |
| `app/api/price_quarantine.py:8-11` | yes | — |
| `docs/proposals/ta/strategy-catalogue-and-backtest-validity.md:375` | yes | — |

- **Code-hash identity.** `RULE_SET_VERSION = RULE_SET_ID + sha256(the module's own
  source)` (`price_quarantine.py:52-59`), and the module is in
  `strategy_registry.INPUT_RULE_SETS` (verified at run time:
  `price_quarantine = price-quarantine-v1+49ff29fea766`). Any byte change —
  docstring included — rotates strategy identity and invalidates 12,284 coverage
  rows, 4,340 transition rows, and on the research side 30,572 coverage rows over
  75,972,665 bars. This ticket's own prevention-log entry already names the trade.
- **Migration content checksum.** `app/db/migrations.py:188-199` raises
  `RuntimeError("Migration content drift")` **at boot** when an applied file's
  sha256 moves. Editing a comment in `sql/247` therefore breaks the app until the
  ledger row is reset. Found while enumerating the sites; it is a second,
  independent immutability and is worth recording because the first one does not
  imply it.

⚠ **ckpt-1 findings 29 and 33 — the deferral is a COST argument, not an
impossibility argument, and revision 1 wrote it as the latter.** The prose *can* be
corrected, by an intentional rotation plus recomputation. It is not being corrected
**now** because the cost is a full re-wash of both corpora and a strategy-identity
rotation, bought for a sentence. So the fix is a **durable supersession record**,
not silence: the corrected statement lands in the API docstring and the census
docstring, each naming the superseded line explicitly, and #3046 records that the
in-module correction rides the next intentional rotation of
`price_quarantine.py` — whenever a real rule change next pays for one.

## What ships

1. **`scripts/verify_3046_t3_corroboration_ceiling.py`** — read-only, one
   `REPEATABLE READ READ ONLY` transaction, six arms, **every figure computed at
   run time**, dated output, and explicit refusals (empty corpus, zero triggers,
   more than one rule-set version, asset-class join disagreement). The live eToro
   probe is a SEPARATE opt-in arm (`--probe`) because an HTTP observation cannot sit
   inside the DB snapshot. ⚠ **ckpt-1 findings 34, 37, 38.**
   ⚠⚠ **ckpt-2 P1 — the probe resolves the broker key with
   `master_key.ensure_broker_key_loaded`, NEVER `master_key.bootstrap`.** `bootstrap`
   runs `_revoke_stale_ciphertext` unconditionally, whose class 2 is *"derived_key is
   None ... every operator-owned active row is unrecoverable"* — so revision 2's
   probe would have SOFT-REVOKED every broker credential on a keyless host, and then
   printed "SKIPPED". A read-only diagnostic with a hidden write is the worst shape
   available, and nothing in the output would have shown it.
2. **`app/api/price_quarantine.py`** module docstring — the corrected statement,
   naming what it supersedes. This is the endpoint that publishes the census, so it
   is the right home for the explanation.
3. **`app/services/price_quarantine_store.py`** — the `~30%` at `:81` and the
   `~70%` at `:396` replaced by the qualitative shape plus the reproducing command.
   No hand-written statistic.
4. **`docs/proposals/ta/strategy-catalogue-and-backtest-validity.md` §2.3** — the
   "Volume is equity-only" section corrected to the measured tendency-vs-explanation
   split.
5. **`docs/review-prevention-log.md`** — the lesson: *a published-bias figure that
   lives inside a hashed or checksummed artefact cannot be corrected at the price of
   a sentence, so it must be computed by its PUBLISHER from the start.*

## What does NOT ship, and why

- **No change to T3, its threshold or its bands.** Corroboration is an ADMIT-BACK
  signal and never the gate (`price_quarantine.py:430-433`), so low reachability
  makes T3's containment bias larger than advertised; it does not make T3 wrong.
  ⚠ This is deliberately NOT a claim that T3 is correct — `_corroboration`'s
  share-count and split-invariance semantics rest on a single AAPL observation and
  have never had a full-population check. ⚠ **ckpt-1 findings 27, 28.** That is a
  real gap and it is named on #3046 rather than silently absorbed here.
- **No volume backfill.** The source-side onset is an observation, not a documented
  provider contract (⚠ **ckpt-1 finding 26**), so the script records the bounded
  observation — instrument, retrieval time, raw payload state by year — and claims
  nothing universal. What it does establish is that a `force_backfill` re-fetch is
  not the fix: today's 1,000-bar AAPL response carries **0 explicit zeros and 0
  unparseable values**, so all 96 of its 2022 absences are source nulls, not
  artefacts of `_int_or_none`'s zero-to-None mapping. ⚠ **ckpt-2 P2** — revision 2
  read the probe through `get_daily_candles`, i.e. through the very transformation
  the arm exists to rule out, and could not have told those two apart.
- **No new census column.** The census already stores the `t3_corroboration`
  buckets the reachability figure derives from (`price_quarantine_store.py:96`)
  and the API already exposes them (`app/api/price_quarantine.py:58`). What was
  missing was a correct sentence and a reproducible derivation.
  ⚠ **ckpt-1 finding 32 stands and is not closed here**: a Python docstring is not
  an operator-visible publication, and no frontend consumer of the census was
  found. Recorded on #3046 as a named gap, not fixed under a residual about prose.

## Security

No security surface — read-path data quality. The optional `--probe` arm makes one
informational market-data `GET` on the endpoint `daily_candle_refresh` already
calls every day. No broker mutation, no order path, no credential written.
