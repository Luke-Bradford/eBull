# Strategy fire rate — how often a strategy fires, read from the durable census

Issue: #2623 gap 2. Scope: this gap only. Gap 1 (holding-period statistics) is a
corpus change and stays out; gap 3 (catalog view) needs 1 and 2 to render.

Rung: behavioural change with data semantics — a new derived metric on an existing
read endpoint. No migration, no re-run, no trial-register interaction.

## 1. The premise the ticket states, falsified

> "Wire the scan census into it or derive fires/week from `strategy_signals` history
> at read time."

The second route is the cheap one and it is **wrong on the denominator**.
`strategy_signals` is a SPARSE table by design (`app/services/strategy_observation_storage.py`
module docstring): fired rows stay durable because outcomes and trade ownership refer
to `signal_id`, while routine negatives are retained for 90 days in monthly partitions
of `strategy_signal_observations` and represented durably **only** by
`strategy_signal_daily_counts`.

Full population — every `(strategy_id, strategy_version)` key in either table, entries
only, dev 2026-08-14. Not a sample:

| strategy | version | bar dates in `strategy_signals` | bar dates in census | fired (both) |
| --- | --- | --- | --- | --- |
| s1 | `…ee566d7b` | 1 | **5** | 1,722 |
| s1 | `…f07c9d72` | 1 | 1 | 1,740 |
| s2 | `…7fcb1fca` | **absent** | 1 | 0 |
| s2 | `…89fd873d` | **absent** | **5** | 0 |
| s3 | `…01d3736e` | 1 | **5** | 11 |
| s3 | `…89368716` | 1 | 1 | 13 |
| s4 | `…d32dc4e4` | 1 | **5** | 108 |
| s4 | `…ff76dcde` | 1 | **5** | 108 |
| s4 | `…dde63f07` | 1 | 1 | 186 |

```sql
WITH sig AS (SELECT strategy_id,strategy_version,count(DISTINCT signal_bar_date) days
             FROM strategy_signals WHERE signal_kind='entry' GROUP BY 1,2),
     cen AS (SELECT strategy_id,strategy_version,count(DISTINCT signal_bar_date) days
             FROM strategy_signal_daily_counts WHERE signal_kind='entry' GROUP BY 1,2)
SELECT * FROM cen FULL JOIN sig USING (strategy_id,strategy_version);
```

Two consequences, both operator-visible:

1. **The fired counts agree exactly on all nine keys; only the days disagree.** So
   `strategy_signals` is a correct numerator and an unusable denominator. Dividing by
   the days it can see overstates the rate **5×** on four of the nine keys.
2. **s2 is invisible.** It scanned and fired nothing, so it has no `strategy_signals`
   rows at all. Sourced from there it would be absent from the catalog; sourced from
   the census it reads an honest **0**. "Scanned, never fired" and "never scanned" are
   different states and only the census separates them.

`docs/review-prevention-log.md` § *"On a SPARSE table, the storage predicate is part of
the census definition — and it is the part nobody reads"*, firing on live data.

## 2. Source rule

Two of the three decisions below are fixed by a rule this repo already wrote down. The
third has no published formulation and is fixed by construction, which is stated rather
than left implicit.

### 2.1 The rate axis — settled, and it is not `×5`

`app/services/strategy_statistics.py:9` — *"⚠⚠ THE ANNUALISATION FACTOR IS MEASURED OFF
THE DATE AXIS, NEVER THE 252 CONVENTION … picking one is exactly the 'am I about to pick
a threshold, ratio or window' trigger in `.claude/CLAUDE.md`. No published rule fixes it
— 252 is a convention, not a source rule."* `periods_per_year` implements it as
`(len(dates) - 1) / (span_days / 365.25)` and **raises** on an axis of fewer than two
dates: *"a single date has no span, and inventing one would put a divide-by-zero behind
a plausible number."*

A first draft of this spec scaled by a frozen `TRADING_DAYS_PER_WEEK = 5`. That is the
252 convention in miniature and the rule above governs it directly. Replaced:

```
entries_per_calendar_week = fired_entry_signals / (span_days / 7)
    where span_days = last_scanned_bar - first_scanned_bar
```

measured off the scanned-bar axis itself, and **`null` when that axis cannot carry a
rate** — fewer than two distinct scanned bar dates, or a zero span. No minimum-span
constant is picked; the refusal is the existing rule's, applied at the week scale
instead of the year scale.

⚠ Consequence, measured: every current strategy version has exactly **one** scanned bar
date, so `entries_per_calendar_week` is `null` for all four today. That is the correct
output, not a gap — it is the same refusal `periods_per_year` already makes, and it
resolves itself as the scan accrues days. The prior versions with 5-date axes show the
metric works the moment a version outlives one scan.

⚠ The axis is **bar dates**, not scan runs. A catch-up run can write several bar dates
at once, so `scanned_days` counts market days evaluated, not job invocations. That is
the right axis for "how many signals per week of market", which is the question asked.

### 2.2 The propensity denominator — the census carries it, so use it

Counting fires per day answers "how much lands in my lap" and is not a rate: a day on
which one instrument was evaluated weighs the same as a day on which 5,000 were, so
universe growth moves the number with no change in strategy behaviour. The census stores
`row_count` for **all three verdicts**, so the dimensionless form is available directly:

```
fired_share_of_evaluable = fired / (fired + not_fired)
```

`not_evaluable` is excluded from the denominator. Those bars were not judged — the
closed reason vocabulary is `missing_volume`, `insufficient_warmup`, `no_fill_bar` and
five siblings (`sql/276:20`) — so counting them as opportunities the strategy declined
would suppress the share as though it had been offered a chance it never got.

Both quantities ship. They answer different halves of the operator's question and
neither substitutes for the other.

### 2.3 Entries only — structural, not a corpus fact

`signal_kind = 'entry'`. An entry is an opportunity to commit capital, which is what
"how often might they fire" asks; an exit is management of capital already committed and
its frequency is a property of the open book. This is a definitional split, not an
observation about the current four strategies.

### 2.4 Population and version scope

- **Population** — `strategy_signal_daily_counts` (`sql/276_strategy_observation_storage.sql:11`),
  the durable census. NOT `strategy_signals`.
- **Version scope** — the CURRENT `strategy_version` only, over all of its census
  history. A version is a rule set (#2670); pooling two averages two arithmetics.
- **Universe label** — `survivor_only`, from `strategy_signal_scan.SCAN_UNIVERSE`, per
  #2288's contract that a metric computed on a survivor-only universe is marked as such.
  The census has no `universe` column, but it does not need one here: a universe change
  is a new `strategy_version` by construction (`strategy_signal_scan.py` — *"The switch
  to a point-in-time population is a NEW `universe` value, hence a new
  `strategy_version`"*), and this query filters to one version, so every row it reads
  was written under the label it reports.

## 3. Null is not zero, and the API says which

| state | `scanned_days` | `fired_share_of_evaluable` | `entries_per_calendar_week` | `rate_unavailable_reason` |
| --- | --- | --- | --- | --- |
| never scanned (new version) | 0 | `null` | `null` | `never_scanned` |
| one scan day | ≥1 | value | `null` | `single_scan_day` |
| scanned, never fired | ≥2 | `0.0000` | `0.00` | `null` |
| normal | ≥2 | value | value | `null` |

`rate_unavailable_reason` exists because `null` otherwise means three different things
and gap 3 requires empty states to say *which*. ⚠ `sum(row_count) FILTER (WHERE verdict
= 'fired')` returns **NULL**, not `0`, when a version has no fired bucket at all — s2 is
exactly that today — so the numerator is coalesced. Getting that wrong reports a scanned
strategy as unmeasured.

## 4. What ships

`app/services/strategy_monitoring.py` gains `load_fire_rate(conn, *, versions)`,
matching `load_attribution`'s existing shape — one query, keyed
`(strategy_id, strategy_version)`. `StrategyOverview` gains a `fire_rate` view:

| field | meaning |
| --- | --- |
| `universe` | `survivor_only` (§2.4) |
| `scanned_days` | distinct entry bar dates in the census for this version |
| `fired_days` | of those, days with ≥1 fired entry |
| `fired_entry_signals` | total fired entry signals |
| `evaluable_entry_decisions` | `fired + not_fired` decisions |
| `not_evaluable_entry_decisions` | excluded from the denominator, reported so the exclusion is visible |
| `fired_share_of_evaluable` | §2.2, 4 dp, `null` when no evaluable decisions |
| `entries_per_calendar_week` | §2.1, 2 dp, `null` per the table above |
| `first_scanned_bar` / `last_scanned_bar` | the axis the rate is measured on |
| `rate_unavailable_reason` | `never_scanned` \| `single_scan_day` \| `null` |

Quantised with `Decimal.quantize` at the stated precision so the JSON is stable rather
than a repeating expansion.

## 5. Edge cases considered and rejected as unreachable

- **A completed scan that wrote zero rows** would be census-invisible (`row_count > 0`
  is a CHECK) and indistinguishable from never-scanned. Not reachable through a
  successful scan: `assert_census_complete` (`strategy_signal_scan.py:791`) compares the
  censused count against the eligible population it computed and **fails the job** on a
  short leg — scan spec §9, *"a mismatch is a failure, not a log line"*.
- **Future-dated census rows** would enter "all history" unbounded. Not reachable: the
  scan runs in arrears and writes the bar *before* the frontier
  (`strategy_signal_scan.py` module docstring §1).

## 6. Not in scope

- `strategy_opportunity_forecasts` stays at 0 rows. Its `expected_duration_hours` is a
  forecast field needing gap 1's duration work behind it; this metric is observed
  history, and writing observed history into a forecast table would mislabel it.
- `StrategyAttribution.signals_last_30_days` is left alone. It counts the same sparse
  table against a 30-calendar-day filter, so it carries the same denominator weakness in
  a milder form. Changing it moves an existing operator-visible figure and belongs in
  its own ticket rather than being smuggled into this one.

## 7. Verification

Pure-logic table tests over the derivation (the loader splits into a pure
`derive_fire_rate` and a thin query, so every row of §3's table is a unit case):
never-scanned, single-scan-day, scanned-never-fired (NULL numerator coalesced), zero
evaluable decisions, a multi-day axis, and `fired_days <= scanned_days`.

One DB test that the loader reads the census and not `strategy_signals`, on a fixture
where the two disagree on days — the 5-vs-1 shape measured in §1.

Dev: `GET /strategies/overview` on the live `:8000`. Expected from §1's measurements —
s1 `fired_share_of_evaluable = 0.5210` (1,740 / 3,340), s2 `0.0000` (0 / 3,273), s3
`0.0039` (13 / 3,330), s4 `0.0340` (186 / 5,468), and
`rate_unavailable_reason = single_scan_day` on all four.
