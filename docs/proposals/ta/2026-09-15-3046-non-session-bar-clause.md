# #3046 build item 2 — the weekend-bar direction, narrowed to the venue we can source

Status: proposal, revised after Codex checkpoint 1 (46 findings). Refs #3046, #3031, #2261, #2312.
Predecessors: the contract (`2026-09-15-3046-transition-verdict-contract.md`), build item 1
(`2026-09-15-3046-day-change-window-verdict.md`, merged `3fe3ce8b`), residual 2's census
(`5be4294d`, `scripts/verify_3046_weekend_bar_census.py`).

## 1. The premise this replaces

Residual 2 shipped **eight constraints rather than a choice** and named the open question as
*"the weekend-bar fix direction"*. Three directions were killed, two of them the author's own.

Before speccing a fourth, the population was re-measured on the full corpus (7,002,478 bars,
12,284 instruments, one `REPEATABLE READ READ ONLY` transaction). **Three of the residual's
framing assumptions do not survive that measurement.**

### 1a. "Weekend" is not the defect class — it selects 6.5% of the shape it is aimed at

The residual's implied defect is a bar carrying no session: flat `o=h=l=c` repeating the prior
close. On the classes `params_for` declares 5-day:

| where | all bars | flat **and** `close = prior close` | instruments |
| --- | ---: | ---: | ---: |
| weekday | 6,698,646 | **73,400** | 2,399 |
| weekend | 10,756 | **5,133** | 1,945 |

93.5% of that shape is on weekdays, so a weekend-scoped rule leaves it.

⚠ The weekday half must **not** be swept in: an illiquid name can genuinely print
`o=h=l=c=prior close` on a real session. The shape is suspicious, not impossible.

**The axis is therefore the DATE, not the shape: was this date a session for this venue?**
That is what constraint 5 already said — *"prefer the published calendar where one exists"* —
read as a statement about the axis rather than an implementation detail.

⚠ **Corrected after ckpt-1 #1.** An earlier draft argued *"if the venue held no session, no trade
can have occurred"*. That is too strong: FINRA's trade-reporting FAQ (102.3-102.4) provides
explicitly for equity trades executed on weekends and holidays, so exchange closure does not
prove fabrication. The claim this spec makes is only the one it can source: **the date is not a
session of the venue the instrument is listed on**, therefore a daily bar attributed to that
session is not the quantity it claims to be. Provenance is left unestablished.

### 1b. The larger and more urgent instance is a published US market holiday, not a weekend

Applying `market_calendar.us_market_specials` (already in the tree) to `asset_class='us_equity'`:

| date | reason | bars | neighbouring sessions | median positive volume | neighbours' median |
| --- | --- | ---: | ---: | ---: | ---: |
| 2026-04-03 | Good Friday | **534** | 6,272 / 6,279 / 6,267 | **771** | ~507k-590k |
| 2026-06-19 | Juneteenth | 524 | ~6,270 | — | — |
| 2025-11-27 | Thanksgiving | 491 | ~6,000 | — | — |

**6,770** `us_equity` bars sit on a NYSE-closed date (weekend or full closure) across the corpus,
1,257 instruments. ⚠ **That total includes 289 bars on 2021-12-31, which is a defect in our own
calendar and not a closure — see §2a. Corrected: 6,481 bars / 1,070 instruments** (ckpt-1 #40).

Every one of the 534 Good Friday instruments **also** has its 2026-04-02 bar, so these are extra
dates, not shifted ones. `AAPL` 2026-04-03 is `o 255.40 h 255.43 l 255.13 c 255.40`, volume
**127,920**, against 25,252,844 the prior session.

⚠ **Provenance is NOT claimed** (ckpt-1 #2). An earlier draft called these "off-exchange prints".
What is measured is: the date is a published US market closure; turnover is ~0.15% of the
neighbouring sessions' median; and the open is pinned to the prior close. No execution source or
trade-condition evidence was consulted and none is cited.

This cohort is **current** (2026) and on liquid names, where the 10,756 weekend bars are
overwhelmingly 2020-2023 and on the tail.

### 1c. Two sub-populations inside "weekend bars" are not weekend bars at all

A 2×2×flat partition on three structural signals separates the 10,756 **completely** — the eight
rows below sum to 10,756 (ckpt-1 #38 corrected an earlier table that summed to 10,205):

| has following Monday | `close = prior close` | flat OHLC | bars | instruments | bars/inst | positive volume |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| yes | yes | yes | 3,591 | 431 | 8.3 | 0 |
| no | no | no | **2,428** | 113 | 21.5 | 1 |
| no | no | yes | 1,654 | 1,648 | 1.0 | 0 |
| no | yes | yes | 1,542 | 1,533 | 1.0 | 0 |
| yes | no | no | 990 | 171 | 5.8 | **100** |
| yes | no | yes | 491 | 392 | 1.3 | 0 |
| no | yes | no | 56 | 34 | 1.6 | 0 |
| yes | yes | no | 4 | 4 | 1.0 | 1 |

Predicate definitions (ckpt-1 #39): `close = prior close` compares `NUMERIC` values exactly, with
`prior close` the `lag(close)` over `(PARTITION BY instrument_id ORDER BY price_date)` taken
**before** the weekend filter, so the prior bar is the true predecessor rather than the previous
weekend bar; a first observation has `prior close IS NULL` and reads `no`. `flat` is
`open=high AND high=low AND low=close` (a NULL in any of the four reads `no`). "has following
Monday" is an existence test for `price_date + (8 - isodow)`, which is `false` for a bar at the
end of stored history — the row-3/4 cohort, at 1.0 bars per instrument, is largely that.

Row 2 is a distinct defect: **real data on the wrong date**. ASX (`exchange='31'`) by year:

| year | Mon | Tue | Fri | Sat | total |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 2021 | **0** | 314 | 340 | **321** | 1,629 |
| 2022 | 1,599 | 2,875 | 3,076 | 1,389 | 14,943 |
| 2023 | 5,479 | 6,497 | 5,960 | 790 | 32,423 |
| 2024 | 10,770 | 11,382 | 8,963 | **0** | 52,740 |
| 2026 | 13,130 | 14,378 | 13,984 | **0** | 70,273 |

`CTD.ASX` March 2022 reads Tue, Wed, Thu, Fri, **Sat**, Tue, Wed, Thu, Fri, **Sat** — five bars a
week, Monday always absent. In 2021 the shift is total (zero Monday bars); from 2024 it is gone.

⚠ **This is reported, not acted on.** ckpt-1 (#20, #21, #22, #23) is right that a missing Monday
plus non-flat OHLC does not *prove* a +1 shift — suspensions, Monday holidays and sparse ingestion
produce the same signal, `sat = 0` from 2024 does not rule out a weekday-to-weekday shift, and the
argument was carried by one instrument plus aggregates. **Nothing in this build depends on the
shift claim**: ASX is `asia_equity`, and §3 narrows the clause to `us_equity`, so the cohort is
never reached. It is written up on the issue as a separate, unproven-but-evidenced corpus defect.

### 1d. One hypothesis raised and killed

Gulf venues historically traded Sunday-Thursday, so `mena_equity` Sunday bars could be genuine
sessions. Not in this corpus: Dubai Financial Market's 239 weekend bars are dated 2023-12-17 and
2024-05-05 → 2024-07-07, and DFM carries ordinary Friday bars throughout 2024-2026. DFM moved to
a Monday-Friday week effective 2022-01-03 (DFM press release, 2021-12-08).

⚠ But ckpt-1 #6 is right that this does **not** settle `mena_equity`: the Saudi Exchange trades
**Sunday-Thursday** today, and `calendar_days_per_bar` says a class is five-day without saying
**which** five days. That is one of the reasons §3 does not ship a generic weekend rule.

## 2. Source rule

| decision | governing rule | where |
| --- | --- | --- |
| which dates are US market sessions | **NYSE published holidays + early closings** (nyse.com/markets/hours-calendars); the Saturday-New-Year exception is NYSE Rule 7.2's | `app/services/market_calendar.py` |
| whether an instrument treats weekends as sessions | **no published formulation covers our whole universe**; fixed BY CONSTRUCTION and frozen | `price_window_verdict.WEEKEND_SESSION_RATIO = 1/7` (`3fe3ce8b`) |
| minimum history the habit needs | **the bound the constant was measured under** | see §2b |

⚠ Nasdaq, NYSE American, CBOE and OTC Markets observe the same **full-closure** days as NYSE.
ckpt-1 #4 is right that this spec cannot source per-venue equivalence for every year, so the
verifier prints the per-exchange bar counts on each closure date and the claim is falsifiable
from the output rather than asserted. Half days are deliberately **not** read: a 13:00 ET close
is a real session.

⚠ ckpt-1 #10: an NYSE **extraordinary** closure not transcribed in `_EXTRAORDINARY_CLOSURE_NAMES`
reads `open`, so clause 5 does not fire on it. That is the fail-safe direction (a real defect is
missed, a real session is never suppressed) and is the module's own documented behaviour.

⚠ ckpt-1 #26 — **`price_date` must be a New York civil date for `us_market_status` to apply.**
The provider gives `fromDate` truncated to 10 characters (`etoro._normalise_candle`), and the
timestamp's own semantics are not documented to us. The corpus is the evidence that the mapping
is correct **for US equities**: Dec 24 2021 (the observed Christmas closure) is absent, every
Saturday/Sunday is near-empty, and 2021-12-31 carries 289 bars against 287-291 on its neighbours.
For ASX the same evidence says the mapping is **wrong** (§1c) — which is a second reason this
clause is scoped to `us_equity` and not applied on the provider's word.

### 2a. A defect in that calendar, found by applying it, fixed here

`market_calendar` calls **2021-12-31 a full closure**. It is not. `nearest_workday` shifts a
Saturday New Year's Day back onto the preceding Friday; NYSE does not. NYSE's published rule is
that when a holiday falls on a Saturday the market closes the preceding Friday **except** for
New Year's Day, where the market stays open — the Exchange does not add a closure to the prior
accounting year. The published NYSE calendar carries no holiday for Saturday January 1 2028 and
none for Saturday January 1 2022.

Corpus corroboration (not the governing rule — ckpt-1 #9): `us_equity` bar counts run 287, 287,
290, 290, [Dec 24 absent], 289, 289, 289, 289, **289 on Dec 31**, then 291 on Jan 3 2022.

Two consequences beyond this clause, both wrong today: `latest_completed_us_session` skips that
date, and the intraday chart shades it closed.

⚠ **An existing test asserts the bug** (ckpt-1 #45): `tests/test_market_calendar.py:103`
`test_observed_new_year_attributed_by_observed_date` requires `date(2027,12,31)` in
`full_closures` — the same case, one occurrence later. Its *subject* is the year-attribution
straddle, not the Saturday exception; it is replaced by a test of the exception itself, plus
tests that Sunday observation (→ Monday) and the Christmas / Independence Day Saturday rules are
**unchanged**.

⚠ **The rotation this causes was measured, not waved at** (ckpt-1 #33).
`strategy_mt1_books.BOOK_RULE_VERSION` embeds `market_calendar.RULE_SET_VERSION`, and
`strategy_mt1_identity` embeds that in turn. Stored cost today: `strategy_mt1_trial_results` and
`strategy_mt1_trial_result_cells` hold **0 rows**, and **0 of the 7**
`strategy_preregistration_declarations` reference `nyse-market-calendar`. So the rotation
invalidates nothing, and it is the correct outcome regardless — a corrected calendar genuinely
produces different books.

⚠ Reported, not fixed (out of scope): `strategy_decision_context` calls `us_market_status` for VIX
eligibility but omits the calendar version from `CONTEXT_VERSION`, so a stored context can change
without its provenance moving. Pre-existing; noted on the PR.

### 2b. The habit floor is the bound its own constant was measured under

`WEEKEND_SESSION_RATIO`'s justification is an empty band measured *"on 12,157 instruments with
**>= 20 bars** in their lookback"* (`price_window_verdict.py:129-131`). **Production applies no
such floor**, and ckpt-1 #15 is right that this is unsafe. Measured on `us_equity`:

| symbol | weekend ratio | bars in lookback |
| --- | ---: | ---: |
| PSTX.CVR, FUSN.CVR, TECX.CVR, PTRCY | 1.0000 | **1** |
| GRCL.CVR | 1.0000 | **2** |
| SpaceX.IPO | 0.6667 | **3** |
| RSX | 0.1667 | **6** |
| AAPL.24-7 … TSLA.24-7 (8 names) | 0.2256-0.2829 | 99-195 |

**7 of the 15 habit-positive `us_equity` instruments qualify on fewer than 20 bars**, six of them
on a ratio of 1.0 from one or two observations. The genuine cohort sits at the 2/7 signature.

So `WEEKEND_HABIT_MIN_BARS = 20` is added — **not an invented constant**: it is the population
bound the frozen ratio was measured under, applied where the ratio is used. Below the floor the
habit is **unknown**, which §3 treats as "do not fire", never as "weekends are closed".

⚠ This changes behaviour shipped in `3fe3ce8b`: those 7 instruments flip habit true → false,
which changes `rule_w2`'s weekend deduction for them. The A/B (§6) reports that delta separately
from clause 5's.

## 3. What is proposed

**Clause 5 of `price_window_verdict` — `non_session_bar`.** A windowed read is `quarantined` when
**either endpoint bar sits on a date that is not a session for that instrument's venue**, where
"not a session" is established only where a published calendar exists:

```
fires  iff  asset_class == 'us_equity'
       and  inputs is not None                      # habit evidence was loaded
       and  NOT trades_weekends                     # with the >= 20-bar floor applied
       and  us_market_status(endpoint_date) == 'closed'
```

Everything else does **not fire**. Concretely (ckpt-1 #13, #31): an unknown/absent `asset_class`,
an instrument with no `WindowInputs` row, and an instrument below the habit floor all leave
clause 5 silent rather than quarantining on missing evidence.

⚠ **No generic weekend rule.** An earlier draft added *"otherwise, Sat/Sun is a non-session iff
`params_for(asset_class).calendar_days_per_bar != 1`"*. Withdrawn, for three reasons ckpt-1
supplied: `calendar_days_per_bar` is W2's nominal-span parameter and is not calendar authority,
so re-parameterising gap tolerance would silently redefine sessions (#12); a five-day declaration
does not say **which** five days, and the Saudi Exchange's Sunday-Thursday week is a live
counterexample inside `mena_equity` (#6); and `params_for(None)` defaults to five-day, which
would have turned "we have no exchange row" into "the venue was definitely shut" (#13).

⚠ **Endpoint-only, and the reason is corrected.** The earlier draft justified this by saying a
spanned non-session bar is `rule_w2`'s operand. **That is wrong** (ckpt-1 #27, with an executed
counterexample: 2026-01-08 → 01-14 returns `quarantined` on two endpoints and `ok` once weekend
bars are added, because extra bars *raise* W2's tolerance). The honest reason is narrower:
clause 5 is endpoint-only because its consumer computes an **endpoint-to-endpoint** quantity. An
interior phantom bar perturbs averages, volatility and every rolling statistic, and nothing here
catches that — named as a limit in §5, not covered by a claim.

⚠ **Containment, never repair.** The window is refused; the phantom endpoint is not silently
re-pointed at the prior real session. `price_window_verdict.py:26-29`.

## 4. How it satisfies the eight constraints

| # | constraint | how |
| --- | --- | --- |
| 1 | may not dodge strategy identity | §4a |
| 2 | derived, not in-place | nothing is written; `price_daily` untouched |
| 3 | volume may not be the gate | volume is read by **nothing** in the clause; it appears in §1b only as evidence for the reader |
| 4 | not keyed on a symbol suffix; must not delete `.24-7` | the habit is a measured ratio over the instrument's own history; the suffix is never read. §6 acceptance 2 proves the cohort survives |
| 5 | prefer the published calendar | the clause fires **only** where one exists, and §2a fixes it |
| 6 | state the treatment | refuse the window. Nothing masked, nothing removed, no warm-up window shortened |
| 7 | already-stored derived columns | **unreached, and stated as a limit.** `market_data._compute_and_store_features` evaluated their windows at write time; a read-time clause cannot reach them |
| 8 | versioning / coverage / staleness | inherits the module's, with its limits named in §4b |

### 4a. Why this is not in `price_quarantine`, and why that is not the bypass residual 2 killed

Residual 2's killed direction 3 put a verdict outside `price_quarantine` **specifically so
`INPUT_RULE_SETS` would not rotate**. That is a bypass and stays killed.

This clause is the pattern build item 1 shipped instead: **it stores nothing**.
`price_window_verdict.py:31-35` sets the contract — *"the first caller that STORES an assessment,
or that sits on a strategy path, owes this module an `INPUT_RULE_SETS` entry"*. The only caller
is an API display path. The distinction from the killed direction is the **storing**, not the
module name.

The alternative's cost is stated rather than waved at: `price_quarantine.RULE_SET_VERSION` hashes
its own source (`price_quarantine.py:50-58`) and is an `INPUT_RULE_SETS` member
(`strategy_registry.py:141`), so an edit rotates every strategy's declared inputs and marks all
7M stored bar verdicts stale.

⚠ ckpt-1 #34 — the module's own header says *"THIS MODULE DEFINES NO RULE"*, and clause 5 is a
rule. The header is amended in this PR to say what is now true: the module defines no
**bar-quarantine** rule and mirrors none, and it owns one composition rule — the session gate —
which moves into `price_quarantine` the moment a caller stores it or a strategy reads it.

### 4b. The limits of the inherited staleness contract (ckpt-1 #32)

The coverage interval and `QUARANTINE_RULE_SET_VERSION` certify **what the bar verdicts were
computed from**. They certify nothing about the calendar's validity for a given year, the
freshness of `exchanges.asset_class`, or the habit's lookback. Stated in the module docstring so
a later consumer does not read the existing `coverage_*` reasons as covering clause 5.

⚠ ckpt-1 #18 — the habit ends at the instrument's **latest** stored bar, not at the assessed
endpoint, so an old window is classified with later evidence. Today's only consumer assesses the
latest window, where the two coincide; recorded as a limit for any consumer that back-tests.

⚠ ckpt-1 #37 — `load_day_changes` loads prices and window inputs in separate statements, so a
concurrent ingest can pair endpoints with later habit evidence. Pre-existing, unchanged here,
and stated.

## 5. What is explicitly NOT built

- **Non-US venues entirely.** The ~6,987 weekend bars outside `us_equity` are uncovered. Reason:
  no published calendar in the tree, and `calendar_days_per_bar` cannot stand in for one (§3).
- **Non-US weekday holidays** (ckpt-1 #29) — the same limit, on a different day of the week.
- **The series date shift** (§1c) — reported on the issue, unproven, and out of this clause's
  reach by construction.
- **The weekday flat-carry population** (73,400 bars) — suspicious, not impossible (§1a).
- **Spanned non-session bars** and every interior-dependent quantity (§3).
- **Exceptional one-off weekend sessions** (ckpt-1 #7, e.g. NSE's 2024-03-02 special session).
  None exists in `us_equity`; a US-scoped clause cannot meet one.
- **`fx`.** `params_for` declares it seven-day, so it is outside every rule here.

## 6. Verification

`scripts/verify_3046_non_session_clause.py`, read-only, one `REPEATABLE READ READ ONLY`
transaction, every figure computed at run time (never hardcoded).

Declared acceptance conditions, all on the **full** population:

1. **The census reproduces §1** — the complete 8-row cohort table summing to 10,756, the
   weekday-vs-weekend flat-carry split, and the per-exchange bar counts on each NYSE closure date
   against their neighbouring sessions (which is what makes the cross-venue equivalence claim of
   §2 falsifiable from the output).
2. **The `.24-7` cohort is not suppressed** — zero of the 8 habit-positive 24/7 names, and zero
   of the 18 `.24-7` symbols, is flagged by clause 5. ⚠ The check is over the **symbol cohort**,
   not the volume-positive subset, because a volume-conditioned check cannot establish the
   broader claim (ckpt-1 #41).
3. **2021-12-31 is a session** — both arms printed: before the §2a fix its 289 bars are flagged,
   after it they are not.
4. **The habit floor's delta is isolated** — the 7 sub-floor instruments are listed with their
   before/after habit, and the change this causes to build item 1's `rule_w2` suppressions is
   reported **separately** from clause 5's own delta.
5. **A/B on `load_day_changes`** over all 12,284 instruments: the suppressed sets with and
   without clause 5, derived independently of the counter that reports them (build item 1's
   lesson: a check comparing a counter to the list it was built from cannot fail). Reported as
   gained / lost / unchanged, with **zero lost** from build item 1's 215 required (ckpt-1 #44).
6. **A historical arm** (ckpt-1 #42) — the latest window alone exercises almost no holiday
   endpoints, so the A/B also runs every `(instrument, closure date)` pair in the corpus as a
   synthetic endpoint and reports how many clause 5 would refuse.
7. **Revert-probes, on reasons and not only verdicts** (ckpt-1 #43) — removing the habit
   carve-out must re-admit the 24/7 cohort; removing the calendar test must lose the holiday
   instruments; and each probe compares the clause-5 reason set, since another clause can mask a
   verdict change.

Deterministic tests (ckpt-1 #46): both endpoint positions, identical endpoints, a half-day (must
**not** fire), an extraordinary closure, `inputs=None`, missing/stale coverage, a deferred
verdict, habit exactly at 1/7, habit above the ratio but below the bar floor, `us_equity` vs
non-`us_equity`, and coexistence with each of clauses 1-4 (clause 5 must add its reason without
removing theirs — ckpt-1 #30).

Frontend (ckpt-1 #35): `frontend/src/lib/dayChangeVerdict.ts` drops unknown reasons silently, so
a clause-5-only suppression would render with no explanation. The reason is added there in the
same PR.

Dev-verify: `GET /instruments/<symbol>/summary` on a name whose latest bar is a US closure date,
plus the `AAPL` / `GME` / `MSFT` panel to confirm no regression.

## 7. Expected effect

Upper bound, measured with the clause's calendar test alone and no carve-outs: **6,481 bars /
1,070 instruments** sit on a NYSE-closed date in `us_equity` after the §2a fix. On the day-change
window specifically, **63 of 12,284** instruments have an endpoint on a non-session date and 47
have their latest stored bar on one, before the habit and floor carve-outs are applied. The final
figures are whatever the A/B prints; nothing here is asserted as the outcome.
