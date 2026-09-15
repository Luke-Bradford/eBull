# #3046 residual 4 — the raw-`price_daily` consumer-exposure table

Scope item 1 of #3046, verbatim: *"for each raw `price_daily` consumer, determine
whether its window can span a stored `price_transition_quarantine` transition, and
report the count of affected instrument/metric pairs. If zero, close this with that
measurement recorded."*

Read-only. Nothing here refuses, deletes, rescales or re-classifies a bar, and no
rule-set version moves. `price_quarantine` stays in `INPUT_RULE_SETS` (#3031).

## Source rule

Every clause is cited. Where a constant is needed the script **imports** it from the
module that owns it rather than restating it.

### The crossing predicate is already written, and it is not mine

⚠⚠ The first draft of this spec invented a predicate
(`prior_date >= start AND price_date <= end`) and argued for it from first
principles. **`app/services/price_quarantine.py::rule_w1` already defines it**, and
Codex checkpoint 1 caught the omission:

> *"``quarantined_transition_dates`` are the LATER bar of each quarantined
> transition, so a transition counts as inside the window when that date is in
> ``(window_start, window_end]`` — the transition INTO the first bar happened
> before the window opened and does not contaminate it."*

`rule_w1` is adopted verbatim: `window_start < price_date <= window_end`, on the
transition's **later** date only. The invented form is not merely redundant, it
disagrees — a window whose start falls strictly inside a hole is counted by
`rule_w1` and missed by the invented one. The two coincide exactly when
`window_start` is an actual stored bar date, which is why every window below is
expressed as **the first operand bar the consumer actually reads**, never as a raw
calendar cutoff.

### What makes a consumer exposed

* **The defect lives on a transition, not a bar** — `sql/247_price_quarantine.sql`
  decision 10: *"Bars either side of a level break are valid prices in their own
  unit regime — it is the ratio between them that is not a return. So the
  transition is quarantined and both bars are kept."* Exposure is therefore a
  property of the **operand pair a metric spans**, not of the number of rows a
  query returns.
* **A break is a segment boundary, not a bad bar** — `outcome_resolver.py`:
  *"A trade entered on or after the break is entirely within the new scale and is
  perfectly resolvable — masking the break bar would reject it."*
* **Deferred ≠ quarantined** — `sql/247`: *"A transition touching a provisional bar
  gets NO T3 verdict — it is deferred, not quarantined."* Only rows with
  `cardinality(rules) > 0` count. Zero-rule rows are counted and printed
  **separately**; they are not evidence of safety in either direction.
* **Sparse tables are fail-closed against coverage** — `sql/247`: absence of a row
  means *"evaluated and clean" OR "never evaluated"*. The script asserts coverage
  rather than assuming it (below).

## ⚠ The issue body's premise is half wrong, and the design call turns on it

#3046 states *"The strategy path reads through `app/services/price_masked_bars.py`
and therefore honours the verdicts."* `price_masked_bars._LOAD_SQL` LEFT JOINs
**`price_bar_quarantine`** and joins `price_quarantine_coverage`. It never reads
`price_transition_quarantine`. Corrected map, verified against the code:

| surface | table | who reads it |
| --- | --- | --- |
| bar verdicts B1–B4 | `price_bar_quarantine` | `price_masked_bars._LOAD_SQL` (field mask) |
| bar verdicts, research corpus | `research_bar_quarantine` | `research_price_structure_store` |
| level breaks (T3-minted) | `price_series_break` | `price_segments.load_unresolved_breaks`, `price_adjustments.load_breaks` |
| T1 / T2 / T3 transitions | `price_transition_quarantine` | `price_quarantine_store`'s census, and nothing else |

Three corrections Codex forced on the first draft of this table, all verified:

1. `sql/247`'s *"two verdicts per bar, not one"* is about `return_usable` vs
   `range_usable` **within** `price_bar_quarantine`. It is not a statement about
   the bar table vs the transition table, and citing it that way was wrong.
2. `outcome_resolver` does not load `price_series_break`; it **receives** a segment
   boundary (`segment_end_index`). `price_segments` and `price_adjustments` are the
   loaders, and they differ: `price_segments` takes **unresolved** breaks only
   (`resolved_by IS NULL`), `price_adjustments` takes resolved ones to apply
   factors. A raw reader gets neither.
3. Masking does **not** remove a bar. `price_masked_bars` preserves the date and
   nulls fields, so a masked T1 endpoint still occupies its slot.

Consequence for finding 3 and for scope item 2: **"T1 is covered transitively by
the bar mask" is FALSE in general.** It holds only for a metric whose operands are
adjacent bars. S-2 reads `t−252` and `t−21` (`s2_cross_sectional_momentum.py`), so a
masked T1 endpoint sitting between those two operands invalidates nothing the mask
can see. And *"route raw readers through the masked loader"* is not an available
answer for transitions at all — the loader does not carry them.

## Method

### 1. The inventory

One declared entry per `FROM price_daily` occurrence under `app/`, each classified:

| class | meaning | exposed? |
| --- | --- | --- |
| `PROSE` | the match is a comment or docstring | no |
| `METADATA` | `max(price_date)` / `count(*)` / `EXISTS` — no price arithmetic | no |
| `SINGLE_BAR` | one bar's price, used on its own | no |
| `COMPOSED` | two or more single-bar reads combined into one quantity | yes |
| `DERIVED_COLUMN` | a single-bar read of a column `market_data` computed over a window | inherits the producer's per-metric window |
| `WINDOWED` | the consumer does its own multi-bar arithmetic | yes |
| `PRODUCER` | the quarantine detector itself | excluded — it is the source |

`COMPOSED` and `DERIVED_COLUMN` are the two classes a row-count reading misses:

* `api/portfolio.py` reads `curr.close` and `prior.close` through two separate
  LATERAL single-bar subqueries and divides them. Two `SINGLE_BAR` occurrences,
  one exposed day-change.
* `scoring`, `entry_timing`, `thesis` and `thesis_break_scan` each read ONE
  `price_daily` row and take `return_6m` / `sma_200` / `rsi_14` / `volatility_30d`
  off it. The SQL is a single-bar read; the *value* is a trailing-window statistic
  that `market_data._compute_and_store_features` computed. Classifying by row
  count would report the largest consumer of all as unexposed.

### 2. The completeness guard — and what it cannot do

The script and `tests/test_3046_consumer_inventory.py` (pure-logic, no DB)
re-derive every `FROM price_daily` occurrence under `app/` with a case-insensitive
regex and fail if the **per-file occurrence count** disagrees with the inventory.
Line numbers are recorded for navigation and deliberately NOT asserted — they churn
on every unrelated edit.

⚠ Stated limits, because Codex is right that the obvious claim overreaches:

* The guard binds **occurrence counts, not classifications**. Swapping one reader
  for another inside a file passes.
* A new consumer of an **existing** helper (`risk_metrics._load_closes`, the
  price-history endpoint) adds no occurrence and is invisible to it.
* It does not cover `frontend/`. `ChartWorkspaceCanvas.tsx` computes SMA/EMA,
  normalised returns and regression lines from the raw candles
  `/instruments/{id}/price-history` serves, so that endpoint is inventoried as
  `WINDOWED` with the frontend named as the arithmetic site.

So the guard is a **drift detector on the file set**, not a proof of completeness.
It is worth having for that and is described as that.

Codex checkpoint 2 added one clause the guard also enforces: **every occurrence
classified as exposed must name a `WindowSpec`.** It found
`research_comparator_snapshot` and `fair_value_band._OWN_HISTORY_SQL` inventoried as
`WINDOWED` with no measurement attached — the occurrence-count guard passed and the
table silently omitted them. `unmeasured_exposed_occurrences()` now fails on that.

### 3. Windows — per metric, from the producing code

No window below is a round number chosen here. `market_data._compute_and_store_features`
loads two independent 400-row slices — `close IS NOT NULL` for returns/volatility,
all four OHLC non-null for the TA set — and **400 is a load cap, not a window**.

⚠⚠ **A window has TWO halves and they are not the same number**: the OPERAND span
(which bars the value is computed from) and the COMPUTABILITY floor (how many bars
the producer needs before it writes anything). Codex checkpoint 2 caught four places
where the first draft collapsed them, each a false positive. The clearest, with its
own reproduction: `ema_12` reads its whole supplied slice, so a **two-bar** series
produced a window and any transition inside it counted — while `compute_indicators`
returns NULL for that instrument. `WindowSpec` now carries `min_bars` separately and
`tests/test_3046_consumer_inventory.py` pins it.

| metric | operand span (first → last) | floor | source |
| --- | --- | --- | --- |
| `return_1w/1m/3m/6m/1y` | latest close at-or-before `last − N` days, within the 400-row slice → last | — | `_compute_rolling_returns`, `_RETURN_WINDOWS` (imported) |
| `volatility_30d` | 31st-most-recent close **or the oldest if shorter** → last | 6 prices | `prices[-31:]` is *up to* 31, with a ≥5 valid-return floor |
| `sma_20`, `bb_upper`, `bb_lower` | 20th-most-recent OHLCV bar → last | 20 | `sma(closes, 20)`, `bollinger(period=20)` |
| `sma_50` / `sma_200` | 50th / 200th → last | 50 / 200 | `sma(closes, n)` |
| `stoch_k` | **14th** → last | 16 | `%K` uses `k_period` bars; only `%D` spans all 16 |
| `stoch_d` | 16th → last | 16 | `k_period + d_period − 1` |
| `ema_12` / `ema_26` | whole loaded slice (≤400) → last | 12 / 26 | seed + recursion retains the entire supplied history |
| `macd_line/signal/histogram` | whole slice → last | 34 | `slow + signal − 1` |
| `rsi_14` / `atr_14` | whole slice → last | 15 | `period + 1` |
| `risk_metrics` `1y` / `3y` | **oldest bar at-or-after** `as_of − WINDOW_LOOKBACK_DAYS[key]` → as_of | 2 | `_slice_window` keeps `cutoff <= d <= as_of` |
| `risk_metrics` `full` | oldest valid bar → as_of | 2 | `WINDOW_LOOKBACK_DAYS[full] is None` |
| `risk_metrics` `trailing_*` | latest valid close at-or-before `as_of − N` → as_of | — | `trailing_return` |
| `day_change` | 2nd-most-recent **strictly positive** close → last | — | `load_day_changes` ranks `close > 0`, not `close IS NOT NULL` |
| `rolling_pnl_1d/1w/1m` | latest close at-or-before `last − N` days, **uncapped** | — | `_ROLLING_PERIODS` (imported) |
| `thesis_52w_range` | **oldest** close at-or-after the cutoff, uncapped | 1 | the aggregate keeps every close `>= cutoff` |

⚠ `day_change` and the portfolio rolling P&L were one row in the first draft and are
four now. They do not share a window: `load_day_changes` compares the two most recent
strictly-positive closes, while rolling P&L takes `close IS NOT NULL` at 1/7/30-day
calendar lookbacks with no 400-bar cap.

`as_of` for `risk_metrics` is the instrument's **latest valid close date** capped at
today (`compute_all_instrument_risk`: `as_of_date = _latest_valid_close_date(closes)`),
not today and not the corpus frontier. Valid = finite and `> 0`, which is a
different population from the producer's `close IS NOT NULL` — both are built.

⚠ **The TA set is not always stored.** The writer computes TA only when the newest
complete-OHLCV bar equals the newest close row; otherwise it stores NULL. The
script applies that gate, so a hypothetical crossing on an instrument whose TA is
NULL is not counted as an affected stored metric.

⚠ **Caller-parameterised windows are reported as an UPPER BOUND, labelled.**
`return_attribution`, `reporting`, `thesis_outcomes`, the portfolio equity curve and
the price-history endpoint take their span from the caller or the request. Their
span is not a property of the corpus, so whole-history exposure is printed for them
with `upper_bound = True`. It is not added to the headline pair count.

### 4. Benchmark propagation

`market_regime_provider.load` reads one benchmark series and hands the regime map to
every consumer; `risk_metrics` takes beta and excess-CAGR against SPY and a sector
SPDR. If a benchmark series is itself exposed, **every dependent pair is exposed
regardless of its own history**. Measured and reported as its own line, not folded
into the per-instrument counts.

The benchmark's exposure is tested over its **whole history**, which makes a clean
result a containment argument rather than a sample: every window any consumer takes
against a benchmark is a sub-interval of that history, so if the whole history holds
no transition, no slice of it can — at any dependent instrument's `as_of_date`. That
matters because `compute_instrument_risk` slices the benchmark at each *dependent*
instrument's date, not at the benchmark's own, so a per-window check would have been
answering the wrong question.

⚠ The benchmark-dependent column set is `beta*` and `excess_*` **minus
`excess_kurtosis`**, which the prefix test alone gets wrong: it is a distribution
moment of the instrument's own returns, where "excess" means "over the normal
distribution", not "over a benchmark". The first run printed it in the dependent list.

### 5. Strata

* **all** — every instrument with a bar.
* **at frontier** — last bar `>=` the corpus frontier (`max(price_date)`).
  ⚠ The first draft used `most_recent_trading_day(today)` on the grounds that its
  docstring calls it *"the single definition of 'a price series is current'"*, and
  the first run proved that wrong: it is a **fetch target** — the bar we should try
  to have — so before the day's candle refresh lands it sits one session ahead of
  every stored series, and the stratum came back `0/0` on every window. An empty
  stratum reads exactly like "nothing is exposed". The frontier is the freshest bar
  the corpus actually holds, which is the question the stratum asks. Both dates are
  printed in the header so the gap is visible.
* **ranked** — the population `app/api/scores.py` will actually surface: latest
  `scored_at` for the model version, `is_tradable`, and
  `coverage.filings_status = 'analysable'` (#268 chunk J, #1918). "Has a `scores`
  row" is not ranking membership and is not used.

### 6. Run identity and isolation

One `REPEATABLE READ READ ONLY` transaction for the whole run, so coverage, windows
and counts cannot straddle a quarantine refresh. The header prints the git SHA,
`price_quarantine.RULE_SET_VERSION`, the database, the corpus frontier and the
coverage reconciliation:

* every instrument in the measured population has a coverage row at the current
  version, and its `[first_bar, last_bar]` contains the measured window;
* every joined transition row carries the current `rule_set_version`.

A failure of either prints and exits non-zero rather than reporting a number.

⚠ Disclosed, not solved: matching coverage bounds do **not** prove the verdicts are
current with respect to **in-place** OHLC revisions, which can change a verdict
without changing a date or a row count. The measurement is as of the last
quarantine evaluation, and says so.

## Acceptance

The script prints **every** declared consumer — including the unexposed classes and
the zero counts, with denominators — and the pair count scope item 1 asks for. A
pair is keyed `(instrument_id, stored metric column)`, deduplicated, so scoring's
single-instrument and bulk paths reading the same column count once.

⚠ A risk *window* is not one metric. `compute_instrument_risk` writes 21 window-scoped
business columns per `window_key` (`_RISK_BUSINESS_COLS`, imported, minus the
trailing set, the counts/ids and the `*_status` enums, which describe the window
rather than carrying a price statistic). The report therefore prints **both** the
instrument/window count and the column-weighted pair count, so the weighting is
visible rather than baked into a single headline number.

Zero closes step 1. Non-zero moves the ticket to step 2 with the population named.

## What this measurement does NOT say

* It does not say any stored number is **wrong**. A quarantined transition is a
  ratio the rule set declines to call a return; a 200-bar level average and a
  return taken straight across a break are not damaged the same way, and nothing
  here ranks them.
* It does not adjudicate the transitions. Residual 1 established that 153 of 244 T2
  rows rest on a fabricated prior level and 28 of the 33 both-endpoints-observed
  rows are archive-unreachable. An exposed consumer may be exposed to an artefact.
* **It measures the window each consumer would evaluate today, off today's corpus.**
  It is not a claim about what any stored value was historically computed across —
  `sql/198_instrument_risk_metrics.sql` expressly forbids reconstructing a historical
  computation from current bars, and this measurement does not attempt it.
* Zero, if it came out zero, would mean *no exposure under the current population
  and the current dependency rules* — not historical safety, not an inability to
  cross in future, and nothing at all about bar-only B2/B3 defects.
