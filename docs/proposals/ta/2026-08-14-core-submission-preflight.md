# The core submission preflight (#2603 item 3, execution half — step 3b-1)

⚠ **Step 3b is the writer. This is the half of it that REFUSES against the WORLD.**

Step 3a (`f953f8e7`) decides whether a *stored verdict* may become an order — supersession,
mandate revision, in-flight suppression, eligibility. It closes with an explicit warning
that its ten codes are **not** the complete submission refusal vocabulary:

> ⚠⚠ THE VOCABULARY BELOW IS NOT THE COMPLETE SUBMISSION REFUSAL VOCABULARY. The kill
> switch, `enable_auto_trading`, the execution block, market-session state, quote
> availability and staleness, account-risk availability, broker minimums, cost assessment
> and broker rejection are all real refusals of a core submission and NONE of them is here
> […] A reader who takes this module for the full set will conclude the core arm has no
> kill-switch check. It has none YET.

This step builds the **DB-and-clock** part of that list. It is the part that needs no
broker: every input is a table read or the wall clock, so it is deterministic, table-
testable, and cannot mutate broker state.

The **broker** part — account-risk availability, what-if cost assessment, broker minimum,
broker rejection — stays with step 3b-2, the module that holds a broker handle. Splitting
on "does this need a broker" is not cosmetic: it is the line between a refusal that can be
proved in a pure test and one that can only be observed against a live account.

Prior steps: item 1 `sql/336` (mandate) · item 2 `sql/346` (eligibility proof) · step 1
`8af38991` (`sql/348`, the intent record) · step 2 `7513d77a` (`sql/349`, the exclusive
arc) · entry condition `14d1bf8b` (`GET/PUT /strategies/core-mandate`) · step 3a
`f953f8e7` (the admission gate) · #2704 `4c76a002` (the sleeve observer).

## Premise falsification

Measured, not inherited.

| claim | verdict | evidence |
| --- | --- | --- |
| the core arm carries none of this vocabulary | **CONFIRMED** | `grep -rn "kill_switch\|enable_auto_trading\|execution_block\|market_session\|halted" app/services/strategy_core_*.py` returns **only** step 3a's docstring warning. No core module reads any of them. |
| the signal path's preflight can be reused wholesale | **FALSE** | `_load_intent` (`strategy_paper_executor.py:222-530`) is 35 checks keyed on `strategy_signals.signal_id`, and most are alpha machinery — forecast policy, calibration holdout, prospective assessment, ranking membership, expectancy evidence. #2603 scope item 5 is *"Explicitly NO alpha input: reads mandate + current positions only; never reads `strategy_signals`."* The **primitives** are reusable; the function is not. |
| an operator declaration exists for the staleness bounds | **FALSE** | `max_quote_age_seconds` / `max_halt_feed_age_seconds` live on `strategy_execution_policies`, which is **per-deployment** (`sql/287:16-20`, NOT NULL with no DEFAULT). The core arm has no deployment. `SELECT … FROM strategy_execution_policies` on dev returns **0 rows** — there is nothing to borrow even by analogy. |

⚠ And a borrowing trap step 3a already paid for once: `CORE_ELIGIBILITY_MAX_AGE` is the
*proof's* bound. Reusing it here because it is nearby and core-shaped would repeat exactly
the error that comment records.

## Source rule

Two thresholds are needed and neither has a published formulation. Per the repo rule
("Source-rule before design" — where a published formulation genuinely does not exist, say
so and fix the rule **by construction**, freezing the constants in a version hash), both
are **derived from the producer's own scheduled cadence** rather than chosen.

The producers are in the scheduler and their cadences are facts:

| bound | producer | cadence | citation |
| --- | --- | --- | --- |
| quote freshness | `quotes_refresh` | **hourly at :23** → period 3600 s | `app/workers/scheduler.py:878`, `Cadence.hourly(minute=23)` |
| halt-feed freshness | `strategy_halt_feed_refresh` | **every 5 minutes** → period 300 s | `app/workers/scheduler.py`, `Cadence.every_n_minutes(interval=5)` |

The construction, applied identically to both:

> A freshness bound **below one producer period** refuses a healthy state in the **tail of
> every cycle** — immediately before each refresh the newest possible row is one full period
> old, so such a bound produces a recurring false refusal by construction. A bound **at or
> above two periods** does not make a missed cycle undetectable, but it defers detection by
> at least a further full period, during which a submission is sized off a quote from a
> producer that has already stopped. So the usable interval is `[period, 2 × period)`.

⚠ **Where in that interval is a CHOICE, not a derivation, and v1 says so.** `3 × period / 2`
is the midpoint: it tolerates half a period of scheduler lateness (lane ticks, prerequisite
skips and `catch_up_on_boot` all make lateness real) while capping detection latency at
under two periods. A tighter bound trades false refusals for detection speed and a looser
one the reverse; nothing in the producer settles the trade-off, so it is frozen in the
policy version rather than presented as derived. **Only the interval is derived.**

⚠ **And the interval is derived from NOMINAL cadence only.** Dispatch latency, fetch
duration, retries and lane contention are not modelled, and the coupling test below proves
configuration agreement, not that either producer actually lands a row inside the bound.
What the bound catches is a producer that has *stopped*; it is not an SLO.

That yields `CORE_MAX_QUOTE_AGE_SECONDS = 5400` and `CORE_MAX_HALT_FEED_AGE_SECONDS = 450`,
frozen in `CORE_PREFLIGHT_POLICY_VERSION = "core-preflight-v1"`.

⚠ **The derivation is asserted, not narrated.** `tests/test_2603_core_preflight.py` imports
the scheduler's actual `ScheduledJob` rows, recomputes each period from the registered
`Cadence`, and asserts `period <= bound < 2 * period`. If someone re-cadences
`quotes_refresh` to every 5 minutes, the test fails — the constant cannot silently drift
from the producer it was derived from. This is the "couple the set to the producer, do not
re-list it by hand" pattern from the prevention log (entry on allow-list drift), applied to
a threshold rather than a name set.

### Why the halt-feed bound is checked AFTER the session

`strategy_halt_feed_refresh` carries `prerequisite=_strategy_halt_collection_due` and its
own description says it runs *"every five minutes from 09:00 ET through the regular/early
close plus 15 minutes"*. Outside that window the producer is **deliberately** not running,
so the feed is legitimately hours stale and a 450 s bound would refuse every time. Checking
`core_market_session_closed` first makes the halt-feed bound meaningful: it is only ever
evaluated in the regime where its producer is scheduled. **Ordering here is correctness,
not tidiness.**

## The vocabulary

A closed `Literal`, precedence = declaration order (step 3a's device, and load-bearing for
the same reason: several can be true at once and the *recorded* explanation must not move
with a refactor).

| # | code | source |
| --- | --- | --- |
| 1 | `core_runtime_config_corrupt` | `get_runtime_config` raises `RuntimeConfigCorrupt` |
| 2 | `core_auto_trading_disabled` | `runtime_config.enable_auto_trading` false |
| 3 | `core_kill_switch_active_or_missing` | `kill_switch.is_active`, **or no row** |
| 4 | `core_execution_block_active` | any `strategy_execution_blocks.active` |
| 5 | `core_instrument_missing` | no `instruments` row for the mandate's instrument |
| 6 | `core_instrument_not_tradable` | `instruments.is_tradable` false |
| 7 | `core_unsupported_market_session` | `exchanges.asset_class <> 'us_equity'` or NULL |
| 8 | `core_market_session_closed` | `us_market_status` + RTH window |
| 9 | `core_halt_feed_missing` | no `strategy_halt_feed_state` row |
| 10 | `core_halt_feed_stale` | age past `CORE_MAX_HALT_FEED_AGE_SECONDS` |
| 11 | `core_instrument_halted` | open `strategy_market_halts` row |
| 12 | `core_quote_missing` | no `quotes` row |
| 13 | `core_quote_price_invalid` | **either** side non-finite or ≤ 0 |
| 14 | `core_quote_crossed` | `bid > ask` |
| 15 | `core_quote_spread_flagged` | `quotes.spread_flag` |
| 16 | `core_quote_stale` | age past `CORE_MAX_QUOTE_AGE_SECONDS` |

⚠ **Feed health (9, 10) precedes `core_instrument_halted` (11), and the order is
deliberate.** Both refuse, so ordering changes only which code is *recorded* — but recording
"halted" while the feed that supports the claim is stale attributes the refusal to the
instrument when the actionable fault is the infrastructure. Establishing the feed is
trustworthy first means `core_instrument_halted` is only ever reported when the halt row is
worth believing.

Codes 2 and 3 are **separate refusals on purpose**, not one combined check.
`docs/settled-decisions.md` ("Execution guard semantics → Config controls"):
*"`enable_auto_trading` is not the same as `enable_live_trading`; both may be checked;
**neither replaces the kill switch**."* Collapsing them would report a disabled config flag
as a kill-switch trip, and vice versa.

Code 3 refuses on a **missing** row, matching `strategy_paper_executor.py:1077`
(`kill_row is None or bool(kill_row[0])`). An absent kill switch is not an inactive one.

### ⚠ The price side depends on the action, and the signal path cannot tell us

`_load_intent` validates `q.ask` only, because the signal path **only ever buys**
(`strategy_paper_executor.py:444-448`, and the what-if order is hardcoded
`transaction="buy"` at `:1145`). A core rebalance emits `buy_core` **or** `sell_core`
(`strategy_core_allocator.py:324`). Sizing a sell off the ask overstates proceeds.

So the preflight takes the action and validates **`ask` for `buy_core`, `bid` for
`sell_core`**, returning that side's price. Both columns are `NOT NULL` on `quotes`
(measured), so the check is finiteness and positivity, not presence.

### ⚠ `us_equity` is an ALLOW-list, and that is the fail-closed direction

`exchanges.asset_class` is a CHECK vocabulary of ten values (`sql/067:55` extended by
`sql/068:54`), measured on dev as `us_equity` 7,335 · `eu_equity` 2,807 · `uk_equity` 990 ·
`asia_equity` 895 · `crypto` 295 · `unknown` 195 · `fx` 64 · `mena_equity` 61 ·
`commodity` 59 · `index` 38.

`us_market_status` (`app/services/market_calendar.py:199`) is the **only** session calendar
this repo has. So the supportable set is `{us_equity}` and everything else refuses — which
means a value added to the vocabulary later (as `mena_equity` was, in `sql/068`) lands on
the **refuse** side with no code change. An exclusion list would have done the opposite and
silently admitted it.

⚠ This does **not** contradict `docs/settled-decisions.md:901`, which puts core allocation
out of scope for the US-only *capital universe* restriction: *"the core instrument is a
mandate/eligibility question, not a strategy-validation one. A non-US-listed core
instrument is permitted if its eligibility proof passes."* That decision governs what the
mandate may **declare**. This code governs what we can **session-check at submission
time**, which is a different question with a different answer: a non-US core instrument is
a legal mandate, and submitting against it refuses here until a calendar for its venue
exists. Recording the two as compatible rather than letting a later reader discover an
apparent conflict.

The join is `LEFT JOIN exchanges e ON e.exchange_id = i.exchange` — note the column is
`instruments.exchange`, not `exchange_id`. Dev has 0 instruments with no exchange row, but
the LEFT JOIN keeps `asset_class` nullable and `None <> 'us_equity'` refuses, so absence
fails closed without depending on that measurement holding.

## Codex checkpoint 1 — what changed

Every claim below was checked against the tree or the dev DB before it was accepted or
rejected. Two were real defects in the spec, two were answered by a producer that already
does the work, one is a settled decision, and one is a pre-existing defect wider than this
slice.

### ⚠⚠ ACCEPTED — the quote producer's scope cannot serve a fresh mandate, so `core_quote_missing` would be PERMANENT

`quotes_refresh`'s scope query (`app/workers/scheduler.py`, the `quotes_refresh` body) is:

```sql
WHERE p.instrument_id IS NOT NULL                                  -- currently held
   OR (i.is_tradable = TRUE AND c.coverage_tier IN (1, 2))         -- Tier 1/2
   OR (i.is_tradable = TRUE AND i.symbol = ANY(%(benchmarks)s))    -- BENCHMARK_SYMBOLS
```

A core instrument that is not yet held, not Tier 1/2, and not one of the 14
`BENCHMARK_SYMBOLS` is **never quoted**. And the first core buy is by definition not yet
held — so the arc cannot bootstrap. Measured on dev over six plausible core ETFs:

| symbol | tradable | tier | held | quoted | in producer scope? |
| --- | --- | --- | --- | --- | --- |
| `SPY` | ✓ | 3 | ✗ | ✓ | yes — benchmark |
| `QQQ` | ✓ | 1 | ✓ | ✓ | yes — T1 + held |
| `VOO` | ✓ | 1 | ✓ | ✓ | yes — T1 + held |
| `IVV` | ✓ | 3 | ✗ | ✗ | **NO** |
| `VTI` | ✓ | 3 | ✗ | ✗ | **NO** |
| `SPY.RTH` | ✓ | 3 | ✗ | ✗ | **NO** |

**Three of six.** A mandate naming `IVV` — entirely legal, and `IVV` is one of the three
largest S&P trackers in existence — yields a preflight that refuses `core_quote_missing`
for ever, and no amount of waiting fixes it. That is the permanent-refusal class, reached
through a producer's scope rather than through any predicate in this module.

**Fix, in this slice:** `quotes_refresh` gains a fourth scope arm for the enabled mandate's
core instrument. Shipping the check without the arm ships a control that cannot pass.
⚠ This makes the diff touch the scheduler, so **the jobs daemon must be restarted after
merge** — recorded here because a scope change that never runs is the same as no change.

### ⚠ ACCEPTED — a CROSSED quote (`bid > ask`) is admitted today

`compute_spread_pct` (`app/services/market_data.py`) is `(ask - bid) / mid * 100`, and
`spread_flag` is set by `spread_pct is not None and spread_pct > max_spread_pct`
(`market_data.py:1080`). On a crossed quote the spread is **negative**, so it cannot exceed
the threshold and `spread_flag` stays `FALSE`. Relying on `spread_flag` alone therefore
fails open on the one quote shape that most clearly means "do not trade on this".

**Fix:** `core_quote_crossed` refuses `bid > ask`, and **both** sides are validated for
finiteness and positivity, not merely the side being traded — corruption of the untraded
side is evidence the quote is incoherent, and sizing off the other half of an incoherent
quote is not safer for being arithmetically possible.

### REBUTTED by the producer — halt-feed source recency and quote rollback

Both were raised as gaps; both are already enforced on the **write** side, which is the
right place, and this module cites rather than re-checks them.

- **`fetched_at` vs `source_pub_at`.** `strategy_halts.py:142-151` raises `HaltFeedError`
  when `source_pub_at > fetched_at + 5min` (implausible future), when
  `source_pub_at < fetched_at - _MAX_SOURCE_LAG` (5 min, stale publication), and when
  `source_pub_at` regresses below the stored value. A stored row therefore *guarantees*
  `|source_pub_at − fetched_at| ≤ 5 min` and monotonic publication, so a `fetched_at`
  freshness bound transitively bounds source recency to `bound + 5 min`. Checking
  `fetched_at` is checking "we successfully validated and stored a feed this recently",
  which is the decision-bearing fact.
- **Quote rollback.** `_upsert_quote`'s `ON CONFLICT` carries
  `WHERE quotes.quoted_at IS NULL OR EXCLUDED.quoted_at >= quotes.quoted_at` — the write is
  monotonic in `quoted_at` by construction (#2271, guarding the REST/WebSocket race). An
  older observation cannot overwrite a newer one.

### REBUTTED as a settled decision — kill-switch absence and the singleton repair

`ensure_kill_switch_singleton` (`app/services/ops_monitor.py`) re-seeds a vanished row with
`is_active = FALSE`. That is deliberate, documented at length against #1232, writes a
`runtime_config_audit` row and logs a WARNING; between loss and boot repair,
`get_kill_switch_status` fail-closes to `is_active = True`. This module's rule — *absent
refuses* — matches `strategy_paper_executor.py:1077` exactly. Re-litigating the repair's
posture is not this ticket's.

### DEFERRED to its own ticket — halt symbols do not match dotted instrument symbols

`strategy_market_halts` is matched on `mh.symbol = upper(i.symbol)`. Nasdaq publishes plain
symbols, so an instrument like `SPY.RTH` or `AAPL.24-7` can never match its own halt.
Measured on dev: **913 tradable `us_equity` instruments carry a dotted symbol.** This is a
**fail-open** — a halted instrument reads as un-halted.

⚠ It is **pre-existing and wider than this slice**: `strategy_paper_executor.py:245-249`
uses the identical predicate, so the signal path has carried it since #2507. Filed
separately rather than folded in; a one-module refusal gate is not the place to change how
the whole application resolves halt identity.

## Transaction, lock and TOCTOU contract

Raised at checkpoint 1 and load-bearing enough to be a section rather than a caveat.

1. **One snapshot.** Every DB input except the runtime config is read by ONE statement, so
   the kill switch, execution block, instrument, exchange, halt state, halt feed and quote
   are observed from a single MVCC snapshot. Reading them separately would let a verdict be
   assembled from several different worlds — an incoherence with no symptom.
2. **The runtime config is read first and separately**, because `get_runtime_config` raises
   `RuntimeConfigCorrupt` and that is a refusal, not an exception to leak. Its own
   window is accepted and stated: it precedes the snapshot rather than sharing it.
3. **The submission lock must be held**, and this module ASSERTS it rather than documenting
   it — step 3a's device, for step 3a's reason. Without it, kill switch and execution block
   are read at a moment with no defined relationship to the submission, and a caller can log
   an admission and walk into the race the lock exists to prevent. Not holding the lock is a
   caller bug, so it raises `StrategyCorePreflightError` rather than returning a refusal.
4. **The returned price is a time-of-check value and the verdict has no shelf life.** Both
   are stated in the return type's docstring at that strength: an admission means "not
   refused at `now`, under this lock hold", never "safe to submit later". 3b-2 submits
   inside the same lock hold or re-runs this.

## What this step deliberately does NOT do

Named, because step 3a's own lesson is that an incomplete control which does not say so is
read as complete.

1. **No broker call.** Account-risk availability, what-if cost assessment, broker minimum
   and broker rejection are 3b-2's. `evaluate_core_rebalance`'s `broker_minimum` parameter
   stays `None` from this module — and `None` there already means *"the caller has no
   applicable minimum to supply"*, not *"the broker has none"* (allocator docstring).
2. **No write.** It returns a verdict; it records nothing. The intent row, the order and
   the audit trail are 3b-2's.
3. **No sizing.** It returns the side's price so 3b-2 can size against a quote it did not
   have to re-read, but the currency→units conversion, the cost re-solve
   (`strategy_core_allocator.py:334-339`) and the reserve interaction are 3b-2's.
4. **No scan-watermark check.** `strategy_scan_watermark` is keyed
   `(strategy_id, strategy_version)` — it is opportunity-scan machinery, and the core arm
   has no strategy. `scan_stale` is correctly absent, not overlooked.
5. **No caller.** Like every step of this arc before it, it is reachable only from tests
   until 3b-2 wires it. Stated in the module docstring in the same terms.

## Testing

Pure-logic where the decision is pure; DB-backed only for the SQL observation itself.

- one table test per refusal code, asserting the code AND that the codes ahead of it in
  precedence do not fire;
- precedence: an input that trips several at once reports the declared-first;
- the producer-coupling assertions above (bound vs registered cadence);
- side selection: same quote, `buy_core` vs `sell_core`, different price returned; a
  negative `bid` refuses a sell and admits a buy;
- `asset_class` allow-list: `us_equity` admits, each other vocabulary value refuses, NULL
  refuses;
- kill switch **absent** refuses (not merely inactive).

## Acceptance

Unchanged and explicitly operator-attended: #2603's acceptance is a demo session. This step
executes no trade, holds no broker handle, and writes no row.
