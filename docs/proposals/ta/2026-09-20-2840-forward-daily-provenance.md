# #2840 arm 2 step 1 item 1 — what a daily series composed from the stored 30m bars would be

Status: **measurement + findings. Nothing declared, nothing frozen, `TRIAL_REGISTER_VERSION`
unmoved at `r9`.** The panel declaration stays refused; this answers the item the ckpt-1
refusal put ahead of it, and it moves the blocker rather than removing it.

Reproduce: `PYTHONPATH=. uv run python -m scripts.census_2840_forward_daily_provenance
--as-of 2026-09-20T00:00:00+00:00` (read-only, one `REPEATABLE READ READ ONLY` transaction).
Every figure in §§1-5 is emitted by that script, including the exchange-33 catalogue counts
and the job-run window. Figures taken from a code probe rather than the DB are labelled.

⚠ **An earlier draft of this document was refused at Codex checkpoint 1 (53 findings).** The
corrections are not cosmetic — the headline inverted, the warm-up arithmetic was wrong by
two bars, the nominality boundary was unsatisfiable, and the composition rule admitted
off-grid sessions. What changed and why is recorded in §7 so the next reader does not
re-derive the discarded version.

## The question

The refusal put three claims in order and forbade blurring them: *immutable* is established;
*a complete nominal daily series* is not; *aggregable to daily* is plausible and unmeasured.
So: can a nominal daily OHLC series be composed from the stored 30m RTH bars, and what
`strategy_version` reads it?

## Answer

**The composition works. A series that is both nominal-by-arithmetic and available in time
does not yet exist, and is 99 bars short of the first evaluable verdict.**

Three separable results:

1. **Composition: solved, and it must refuse rather than patch.** Post-activation, all seven
   liquid members compose **29/29** sessions. The rule requires the exact expected slot set,
   not a bar count.
2. **Nominal-and-in-time: 55.2%, and 11 of the 13 failures are a collector outage.** Of 29
   post-activation sessions, **16** were composable *and* nominal-by-arithmetic *and* present
   before the scan deadline. Of the 13 that were not, **11** fall inside a window holding no
   `strategy_intraday_harvest` run row at all (2026-08-27 .. 2026-09-11) and **2 do not**
   (2026-08-13, 2026-08-26) — those two are a different defect, unusable while the collector
   was running. The intersection is emitted rather than asserted.
3. **Wait to a first signal: 99 usable bars, and that is the optimistic reading.** S-4
   needs **115** bars for one evaluable verdict (computed by probe, not typed); the best
   member holds **16** usable ones. The two month figures below are arithmetic over emitted
   counts, not script output: at 21 sessions/month and a 100% usable rate, 99 bars is ~4.7
   months; at the observed 55.2% it is **~8.5 months**.

And the identity half has a harder answer than expected — see §6.

## 1. The composition rule

`store_intraday_bars` raises on any bar at or behind the stored watermark
(`app/services/strategy_observation_storage.py:580`), so this writer never re-bases a stored
row. ⚠ That is an **application convention, not a schema guarantee**: `sql/276` carries no
UPDATE/DELETE prohibition and does permit cascading deletion, and it makes the stored value
the first observation *this writer accepted*, not the first that existed.

The rule: compose a session only when its bars occupy **exactly** the expected slot set —
13 slots on a full session, 7 on a 13:00 half day — and omit it otherwise.

⚠ A count check is not enough, and the earlier draft's was not. Codex reproduced `complete`
on a session with the last slot replaced by a duplicate of the first, and on a whole session
shifted an hour off the grid. Both compose a plausible wrong daily bar rather than no bar,
and the field a missing last slot moves is the **close** — the one input S-12's absolute gate
reads.

⚠ **This is a conservative admission rule, not a demonstrated necessity.** A 30m window in
which nothing traded can leave the daily OHLC fully determined by the other twelve, and the
census cannot tell that case from a lost bar. ⚠ **Nor is omission compelled.** `BarSeries`
allows calendar gaps and never interpolates (`app/services/indicator_series.py:100`), so a
gap is legal — but dropping a session in which trading *did* happen is not the same object as
a holiday: it shifts ATR's recursion (`indicator_series.py:467-470`, which is where
persistent poisoning actually lives — the earlier draft mis-cited `s4:331`, a finite
breakout window), the breakout lookback, and the holding clock by one position.

⚠ One interface detail, surfaced by the typechecker rather than by reasoning: the daily row
type a strategy reads is `OHLCVRow` with **`volume: int | None`**
(`app/services/technical_analysis.py:16-21`), while `strategy_intraday_bars` stores OHLCV as
`DOUBLE PRECISION` (`sql/277`, which records the choice: *"this table is not an order or
accounting ledger"*). A composed session volume is therefore a float sum that has to be
narrowed, and the rounding rule for that is undeclared. It does not touch the gate — S-4 and
S-12 read no volume — but a forward composer cannot leave it implicit.

Composability, per member, over its own observed span and then post-activation:

| member | span sessions | composable | post-activation sessions | composable | **usable** |
| --- | ---: | ---: | ---: | ---: | ---: |
| SPY | 50 | 50 | 29 | 29 | **16** |
| QQQ | 50 | 49 | 29 | 29 | **16** |
| IWM | 107 | 101 | 29 | 29 | **16** |
| AAPL | 50 | 50 | 29 | 29 | **16** |
| CENN | 115 | 39 | 29 | **3** | **2** |
| F | 52 | 51 | 29 | 29 | **16** |
| KO | 51 | 50 | 29 | 29 | **16** |
| JPM | 51 | 50 | 29 | 29 | **16** |

Panel: 440/526 composable over all spans (83.65%); **206/232 composable and 114/232 usable
post-activation (49.14%)**.

⚠ The span denominator **conditions on successful observation** — leading and trailing
collection silence sits outside it — so it measures internal coverage and is not a
prospective reliability figure for a panel that has not been collected yet. The
post-activation column is the only window in which live collection was *possible*, so it is
the only one whose usable rate says anything about the design.

⚠ **CENN's 3/29 is one instrument, not a stratum property.** Its liquidity is taken from its
declared purpose ("low-volume contrast"), not measured; excluding it produces the flattering
headline and establishes nothing about the low-price stratum a straddling draw would
populate. What it does show is that composability is not uniform across price levels, which
a draw rule has to price in.

## 2. Nominality and availability are two DIFFERENT deadlines

A US split takes effect at a session **open**, so a bar whose every constituent was captured
before the *next* session's open had no intervening open at which anything could re-base it.
That is the only nominality claim available without a provider experiment, and it is
arithmetic.

⚠⚠ **The premise that the provider re-bases intraday history at fetch is UNVERIFIED and is
not asserted.** `app/services/market_data.py:751` records back-adjustment for the **daily**
path; it says nothing about `get_intraday_candles`, about every interval, or about every
instrument variant. Three claims that must not share a sentence: *this writer never re-bases a
stored row* (established) · *the provider does not re-base intraday history* (untested) · *a
bar captured before the next open had no opportunity to be re-based* (arithmetic).

⚠ `captured_at` is `DEFAULT now()` — the writing transaction's start. It **bounds** the
capture instant; it does not record the request, the response, or the provider's generation
time.

⚠ An earlier draft proposed restricting the series to captures inside the bar's **own
session**. That is unsatisfiable: the last 30m bar completes *at* the close, so any capture
after it fails, and the collector deliberately runs to close+10 minutes. The next-open
boundary is the correct one and it is the boundary the census now uses.

⚠⚠ **An earlier draft claimed the nominality deadline and the timeliness deadline were the
same number. They are not, and Codex checkpoint 2 caught it.** `strategy_signal_scan` fires
**daily at 06:45 UTC** (read from `SCHEDULED_JOBS` at run time, not typed —
`app/workers/scheduler.py:2502`), which is *before* the 13:30-UTC next open in EDT. So a
catch-up landing at 10:00 UTC is nominal-by-arithmetic and already too late for the scan that
would have used it. The census reports both conjuncts and their intersection:

- `settled_before_next_open` — no session open intervened. A **nominality** claim.
- `available_before_next_scan` — the session existed before the first scan that would load
  it. An **availability** claim, and the stricter of the two here.

⚠ The availability deadline used is the scan's time on the first calendar day after the
session. The true one is **later**: the scan's own registry description records that it runs
*"one bar in ARREARS"*, writing the bar before the modal last bar, so a session is not a
decision date until a further session completes. The conservative deadline is deliberate and
the direction is stated — **the reported availability is a lower bound**.

Opportunities a split check would have to rule out (per stored bar, all spans):

| member | before next open | after 1 | after 2 | after ≥3 | share settled-by-arithmetic |
| --- | ---: | ---: | ---: | ---: | ---: |
| SPY / QQQ / AAPL | 246 | 27 | 26 | 350-351 | 0.379 |
| F / KO / JPM | 246 | 27 | 26 | 358-366 | 0.370-0.374 |
| IWM | 246 | 27 | 26 | 1078 | 0.179 |
| CENN | 158 | 9 | 16 | 1045 | 0.129 |

⚠ The `after_*` bars are **not established as re-based** — only as unruled-out. ⚠ And this
repo stores **no corporate-action table** (`rg 'split_ratio|corporate_action'` over `sql/` and
`app/services/` reaches only `alpaca_delayed_sip_probe.py`, a probe), so the ruling-out cannot
be done from the store at all. `price_daily_revision.cause` is not a substitute: `sql/387`
calls it *"A BRANCH, NOT AN ECONOMIC CAUSE … an UPPER BOUND on attribution"*, it is a
per-write branch joined to no `(bar_time, captured_at]` window, and an initial
already-adjusted insert produces no revision row at all. ⚠ Absence of local storage is not
absence of an observable source — whether one qualifies is unexamined.

⚠ **Nominality does not solve splits.** Even perfectly contemporaneous captures leave pre-
and post-split bars on different scales inside one window, so a forward reader needs a
split-boundary policy regardless.

## 3. Collector uptime explains 11 of the 13 failures — not all of them

Emitted by the census: an **11-session window with no `strategy_intraday_harvest` run row at
all, 2026-08-27 .. 2026-09-11**. Run rows since 2026-04-07: 1,027 `success`, 5,605 `skipped`
(outside the collection window), 3 `failure`.

⚠ **The intersection is emitted, not asserted** — an earlier draft claimed the shortfall and
the outage were the same sessions, and the arithmetic refused it: each liquid member has **13**
unusable post-activation sessions against **11** outage sessions, leaving **2 residual**
(2026-08-13, 2026-08-26) that were unusable *while the collector was running*. A residual is a
different defect from an outage and is not folded into one.

The hole was repaired by multi-session capture days — 2026-08-10 wrote 285-1,013 bars per
member, 2026-09-13 wrote 81-156 covering 2026-08-26..2026-09-11. ⚠ **A capture-day grouping
is not a cause**: several requests inside one day produce the same shape. The run-row gap is
the independent witness, and it is what licenses calling 09-13 a repair.

⚠⚠ **The capability that saved coverage is the one that costs settlement.** A prior session
recorded *"collection itself is healthy … not a defect; checked rather than assumed"* — true
of bar coverage per session, and blind to the outage, because the catch-up had already filled
it. Coverage and settlement are different measurements and only the second sees this.

⚠ **The 5m absence witness exonerates nothing.** Of 302 missing 30m slots across the panel
(the sum of §3d's per-member column, not a figure the script prints on one line), **0** have
a 5m bar in the same window. Both tiers are fetched by the same job, so a shared
outage leaves no discordant witness — which looks identical to a healthy collector. And
"missing in both" merges *observed and empty* with *never observed*: the 5m tier has its own
membership dates, 12-month retention and a far shorter per-request reach. The finding is
only that no resolution-specific loss is evidenced.

## 4. Three candidate daily series, and none may be mixed

`price_daily` disagrees with the composed series on the member instrument at a median 2.6-7.6
bps and a maximum of up to **5.79%** (F) / **4.92%** (AAPL), with the reference range reaching
outside the composed RTH range on 70-86% of pairs. Against the member's **exchange-33
variant** the worst disagreement collapses to **≤13.8 bps** and escapes fall to 8-49%.

| member | vs self (max close reldiff) | vs exchange-33 variant |
| --- | ---: | ---: |
| SPY | 0.002215 | **0.000495** |
| QQQ | 0.004521 | **0.000599** |
| AAPL | **0.049233** | **0.000871** |
| F | **0.057935** | **0.001382** |
| KO | 0.022153 | **0.000906** |
| JPM | 0.005683 | **0.001355** |
| IWM | 0.000109 | (no variant redirects here) |
| CENN | 0.000000 | (no variant redirects here) |

⚠ **What this does and does not support.** It supports: the composed series tracks the
exchange-33 variant far more closely than the member instrument, so these are not
interchangeable references. It does **not** support: that an escape identifies
extended-hours trading (a capture-vintage adjustment, a quote-convention difference,
rounding or bad data produce one too), or any statement about *where* a discrepancy arose.
The earlier draft called a specific AAPL discrepancy an after-hours move; that attribution is
**withdrawn** — no after-hours observation was taken.

⚠ `open(D) == close(D-1)` on calendar-adjacent sessions runs 38.8-59.5% on the six members
with a variant and 0.9%/14.4% on IWM/CENN. A real opening print *can* equal the previous
close, so this motivates a question and is not the witness the earlier draft claimed.

⇒ The operative consequence is narrow and firm: **a warm-up drawn from one series and a gate
read off another is the #2066 split-cliff shape in a new costume.** Only the composed series
can carry the arithmetic nominality argument at all, because the other two are back-adjusted
at fetch on the daily path.

### The exchange-33 variants, measured rather than assumed

595 instruments on `exchange = '33'`, all `.RTH`-suffixed, **540** carrying a
`canonical_instrument_id` to a base twin, 595 tradable. The redirect is the join used here —
never string surgery, because `docs/review-prevention-log.md` records that matching on
`symbol` alone returned 8 rows with a different underlying (`AMPn` Ameriprise vs `AMP` a
token on eToro's Digital Currency venue).

⚠ **What this does not establish**, each named because the earlier draft asserted it:

- That `.RTH` is the instrument S-12 would execute on. `strategy_core_eligibility` makes
  underlying/CFD eligibility **account-specific**; the demo sleeve's first order was on
  `SPY.RTH`, which is one account's routing, not a universal rule.
- That IWM/CENN have no variant. **No redirect** is what was measured; an unresolved mapping
  or the exchange filter explains absence as well as non-existence does.
- That `get_intraday_candles` serves exchange-33 instrument ids at all. Untested. One
  informational call would settle it.
- Anything venue-wide: six mapped members cannot carry a claim about 595 instruments.

## 5. Warm-up: 115 bars, measured

`WARMUP_BARS = 113` is S-4's first evaluable **index**, so 114 bars reach it — and the
evaluator additionally refuses the final bar, which has no successor to fill on. Probed on a
synthetic series (`first_evaluable_series_length`, computed at run time so it moves if either
rule does): **0 evaluable verdicts at 113 and 114 bars, 1 at 115, 2 at 116.** The earlier
draft subtracted 113 and understated every wait by two bars.

Against 16 usable bars on the best member, that is **99 short**. ⚠ And those 18 are bars
already collected **on the base instrument**; they are not transferable inventory for a
differently-declared forward panel, and this census measures no accrual rate for one.

## 6. Which `strategy_version` reads it — the constraint is manifest-wide

`StrategyIdentity.version` hashes `params`, `universe`, `cost_model_id`, `source_hash`, the
registry module and `INPUT_RULE_SETS` (`app/services/strategy_registry.py:312-328`), and its
docstring states that *"one identity spanning two universes is not one strategy"*.

⚠⚠ **And `Universe` is a `Literal` inside `indicator_series`** (`indicator_series.py:77`),
whose `RULE_SET_VERSION` is `"indicator-series-v1+" + sha256(module source)` and is hashed
into **every** identity via `INPUT_RULE_SETS["indicator_series"]`. That module's own comment
calls the consequence deliberate: *"a comment edit changes the string, which makes every
previously stored signal visibly stale rather than silently mixed."*

⇒ **Introducing a new universe for the composed series rotates every strategy version in the
manifest**, not just S-12's. That strands every pending declaration — including arm 2's
in-sample one, which is exactly what `r9` is being held for — and every stored signal's
version linkage.

⚠ The earlier draft concluded "a new module with its own strategy id". That does not follow
and is **withdrawn**: the registry already hashes `universe` into the version under one
strategy id, so a distinct version does not require a distinct module — and a new module
would not protect the old version anyway, because the `Universe` edit lands in
`indicator_series` either way.

**The real structural finding is that the universe label is doing double duty.** It names the
*selection* (survivor-only vs survivorship-free) and, via `AS_TRADED_UNIVERSES`, is also what
S-12 keys its *price basis* on. A composed forward series is survivor-only by selection and
nominal by basis — a combination the label cannot express. So the identity question is not
"which version" but **"where does price provenance live in the identity"**, and nothing in the
repo answers it today: `PreregDeclaration` carries no corpus-version field either.

## 7. What the refused draft got wrong (so it is not re-derived)

| claim | what replaced it |
| --- | --- |
| "Yes, a nominal daily series is composable at 97.6%" | composable ≠ nominal-and-in-time; the intersection is 58.6% post-activation |
| composition validated by bar COUNT | exact slot-set match; a duplicate slot and an off-grid session both passed the count |
| restrict to same-session captures | unsatisfiable — the last bar completes at the close; the boundary is the next open |
| `WARMUP_BARS` = bars needed | 115 measured; 113 is the first evaluable index |
| "the shortfall is not the collector" | a shared-outage witness cannot exonerate the collector; the run-row gap shows the opposite |
| AAPL 07-30 was an after-hours move | attribution withdrawn; no after-hours observation was taken |
| `.RTH` is the tradable instrument | eligibility is account-specific; not demonstrated |
| provider back-adjusts intraday | untested; the cited line is the daily path |
| forward instrument needs a new module | the `Universe` literal rotates the whole manifest either way |
| "every figure is the script's output" | the catalogue and job-run figures are now emitted; the rest are labelled |

And what Codex checkpoint 2 then found in the corrected census (5 findings, all fixed):

| claim | what replaced it |
| --- | --- |
| nominality and timeliness are one deadline | two deadlines; the scan fires at 06:45 UTC, the next open at 13:30 UTC |
| the shortfall IS the outage | 11 of 13; the 2-session residual is emitted separately |
| `--as-of` bounds the measurement | it now bounds every timestamped source; `price_daily` and `instruments` carry no as-of column and cannot be bounded, which the report states |
| exact slot-set match is enough | a full session PLUS an extra off-grid row still composed; `off_grid == 0` is now part of the rule |
| the gate is 100.0 | read from S-12's `PRICE_FLOOR`, so a band recalibration moves it |

A confirming ckpt-2 pass on the corrected census then found four more, all fixed and none
changing a reported figure: the universe was resolved by `status = 'active'` rather than at the
cutoff (a historical run would analyse V2 over a window in which only V1 existed); `price_daily`
had a lower date bound only; `harvest_run_gaps` derived its span from the rows whose absence it
measures, so an outage at either edge vanished; and a **non-finite** constituent price composed
a complete-looking bar — the invariant `docs/review-prevention-log.md` records for exactly this
reason (PostgreSQL orders `NaN` above every value, so `open > 0` and the OHLC-shape CHECK both
pass and the row is reachable).

## 8. The next step — and it is not the panel

1. **Decide where price provenance lives in the identity** (§6). This is upstream of every
   panel question and is the actual blocker. Nothing about it is operator-gated: it is a
   schema/identity shape decision.
2. **Test the provider's intraday adjustment behaviour** — the premise the whole nominality
   argument rests on, currently untested. One instrument across a known split settles it.
3. **Then collector uptime**, because a 55.2% usable rate makes the wait ~8.5 months rather
   than ~4.7, and the composition rule cannot fix it. ⚠ Two of the 13 failures were not the
   outage, so uptime alone does not close it either.
4. Only then the collection panel, the trial subset, the draw rule and the two-book
   mechanics — where the refusal left them.

⚠ Step 1 was NOT in the refused draft's plan, which went straight to declaring a collection
panel. That was the same error the refusal named: declaring membership for an experiment whose
instrument does not exist.

Refs #2840. Refs #2437. Refs #2477.
