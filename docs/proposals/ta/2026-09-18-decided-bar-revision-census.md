# #2414 — measure how often a stored verdict's own bars were overwritten afterwards

Refs #2414. Refs #2394. Prior art in this ticket: `sql/386` (pass-scoped digests cannot
answer this), `sql/387` (`price_daily_revision` — per-entity identity), `sql/388`
(`price_daily_backdated_insert`).

## What this is

A read-only census script, `scripts/census_2414_decided_bar_revisions.py`. It discharges
the one clause #2414 marked **"Not yet measured"**:

> How often a `price_daily` bar is actually revised after first write, and by how much.
> […] That measurement should come before the fix is chosen — **a revision rate of zero
> would change the priority**, and a non-trivial one bounds how much of a stored track
> record is silently stale.

It is an instrument, not a fix. It writes nothing and changes no reader.

## Why it is needed now, and why it was not before

`strategy_signals` held **0 rows** when #2414 was filed (measured 2026-08-08, recorded on
the ticket). It now holds **59,069**, written 2026-08-09 → 2026-09-17 across 8 strategy ids
and 4,271 instruments. The defect stopped being academic and nothing measured it.

Both producers of the answer landed after the ticket was written and neither has a reader:
`price_daily_revision` (sql/387, first row 2026-09-16T20:14:39Z) and
`price_daily_backdated_insert` (sql/388, 0 rows).

## Source rule — the predicate is fixed by sql/387's header, not invented

`sql/387`'s header is the governing document for what may be claimed from
`price_daily_revision`, and it **kills the obvious join** —

```sql
strategy_signals s JOIN price_daily_revision r
  ON r.instrument_id = s.instrument_id
 AND s.signal_bar_date >= r.price_date
 AND s.created_at < r.revised_at
```

— as *"neither an upper nor a lower bound"*, listing five overcounts (segmented evaluation
restarts indicator state; `load_masked_bars` masks fields; a volume-only revision is
indistinguishable from a close revision; `signal_bar_date >= price_date` does not establish
the bar existed at scan time; not every strategy has unbounded memory) and three undercounts
(the regime is computed on the **benchmark**, so instrument equality excludes every
regime-gated name; cross-sectional ranking evaluates instruments together; `resolve_fills`
reads the **next** bar's open, which `signal_bar_date >= price_date` excludes by
construction).

So this census **does not report that join as a headline number**. It reports arms whose
claims survive that header, each labelled with what it does and does not establish.

| arm | predicate | the claim, stated exactly |
| --- | --- | --- |
| **A — decision bar** | `r.price_date = s.signal_bar_date` | *The bar this verdict is about no longer holds the values it was decided on.* No inference about indicator reach, so none of the five overcounts apply. |
| **B — fill bar** | `r.price_date = s.fill_bar_date`, `verdict='fired'` | *The bar whose OPEN priced this fill was overwritten.* Closes sql/387's third undercount, which arm A excludes by construction. |
| **C — prefix reach** | `r.price_date < s.signal_bar_date` | ⚠ **NEITHER BOUND.** Reported because for a Wilder-recursive strategy (ATR/ADX) the segment prefix *is* the read set, so omitting it understates; labelled with sql/387's list rather than quoted as a count of affected verdicts. |
| **D — backdated insert** | `price_daily_backdated_insert`, same three arms | A historical **insert** produces no `price_daily_revision` row (sql/387 names this as a known gap). 0 rows today; the arm exists so it is not silently absent. |

All four carry `revised_at > s.created_at`: a bar overwritten *before* the signal was stored
was read in its corrected form.

**Why a revision row means the value really moved.** `_upsert_candles`' `ON CONFLICT DO
UPDATE … WHERE price_daily.open IS DISTINCT FROM EXCLUDED.open OR … volume IS DISTINCT FROM
EXCLUDED.volume` (`app/services/market_data.py:1439-1443`) suppresses a no-op write, and
`_record_bar_revisions` appends one row per bar the UPDATE actually touched, **inside the
same transaction** (`:1490-1506`). So a row is evidence of a real OHLCV change, not of a
re-fetch. Arm A's claim rests on that and on nothing inferred.

⚠ `revised_bar_dates` **may repeat a date** (`:1499`), so one signal can match an arm through
several revision rows. Every arm counts **DISTINCT `signal_id`**, never pairs.

⚠ **Arms are disjoint per (signal, revision) pair, and NOT additive per signal.**
A is `= signal_bar_date`, C is `< signal_bar_date`, B is `= fill_bar_date` where
`strategy_signals_fill_after_signal` guarantees `fill_bar_date > signal_bar_date`. So no pair
is in two arms — but one signal may appear in several, and the script prints the union
separately rather than letting a reader sum the columns.

⚠⚠ **`revised_at > s.created_at` UNDERCOUNTS, and that is the unsafe direction for an
alarm.** `strategy_signals.created_at` is the ledger **write** time, not the bar **read**
time — sql/387 states this and that no wall-clock column on either table repairs it. The scan
loads and computes before opening its write transaction, so a revision committed inside that
load→write gap was *not* seen by the scan and this predicate still calls it "read in its
corrected form". The miss is bounded by one scan's load→write duration and cannot be
eliminated without a read-set record. Stated, not glossed: these arms are floors.

## Full-population verification — and the denominator that is easy to get wrong

⚠⚠ **The denominator is NOT 59,069.** `price_daily_revision` records nothing before its
first row. A signal written on 2026-08-14 whose bar was overwritten on 2026-08-20 leaves no
trace anywhere, because `price_daily` is upserted in place and has no audit column. So the
observable population is **signals created at or after `min(revised_at)`**, and any rate
quoted over the full ledger understates by the ratio of the two — roughly 300×, which is the
difference between "this never happens" and "this happens".

The script therefore prints **three** figures and never one:

1. `min(revised_at)` / `min(inserted_at)` — the telemetry floor, read at run time;
2. the **observable** denominator (signals with `created_at >= floor`) and the arm counts
   over it;
3. the **unobservable** remainder (signals created before the floor), stated as
   *unmeasurable*, not as zero.

This is the #3111 lesson applied before review asks: a census whose population is bounded by
when its instrument started recording is a **sample in time**, and saying so is the
measurement.

Every arm runs against the whole of both tables. No sampling, no `LIMIT`.

## ⚠⚠ The result, and it inverted this spec's own hypothesis

Run 2026-09-18 against the dev corpus. Every figure below is printed by the script; none
is hardcoded anywhere.

**Part 1 — 221 of 59,069 stored fill prices (0.374%), across 27 instruments, no longer
match the bar they were copied from.** And the magnitudes are not noise: **every single
disagreement is an exact corporate-action ratio.**

| ratio (current open / stored fill) | signals | instruments |
| ---: | ---: | ---: |
| 0.5000 | 57 | 4 |
| 10.0000 | 39 | 4 |
| 0.6667 | 36 | 2 |
| 5.0000 | 24 | 2 |
| 15.0000 | 21 | 3 |
| 50.0000 | 17 | 2 |
| 80.0000 | 8 | 1 |
| …9 more, all exact (9, 12, 20, 25, 30, 100, 125) | 19 | — |

APH: stored `169.990000`, current open `84.995000` — exactly 2:1. `0.6667` is 3:2.
The large integers are reverse splits (LGVN, CXAI, NRDY, SFWL, GOSS). ⚠ `price_adjustments`
holds **0 rows**, so no adjustment layer explains this: the read path is
`price_daily` → `load_masked_bars` (masks to `None`, rescales nothing) → `resolve_fills`,
and equality is the invariant.

So #2414 is neither academic nor marginal: **a fifth of a percent of the stored track
record is wrong by a median factor of 4×**, concentrated in mid-August scans, and the
ledger's unique key cannot express the correction.

**The hypothesis this spec opened with was wrong, and its own instrument says so.**
The draft called severity "bimodal — `incremental` harmless, `adjustment_heal`
dangerous". On the observable window the audit arms found **0 decision-bar hits, 0
prefix hits, and 4 fill-bar hits — all `incremental`**, while `adjustment_heal` hit
nothing at all (its two instruments' signals predate the telemetry floor).

**And all 4 of those `incremental` hits are OVERCOUNTS.** The reconciliation checks each
one's stored price against the current open: MET.US, QCOM, CHRW and OXY all still agree
exactly. The revisions moved some other OHLCV field — a same-day close/high/low/volume
settle — on a bar whose OPEN never changed. That is sql/387's field-blindness overcount,
measured at **4 of 4** rather than left as a caveat.

Two consequences for whoever picks up the supersession half:

1. **The obvious join would have reported zero.** `signal_bar_date >= price_date` excludes
   the fill bar by construction (`fill_bar_date > signal_bar_date` is a CHECK), and the
   fill bar is where every real hit lives. sql/387's header names this as its third
   undercount; this is that undercount observed.
2. **The fill bar is structurally the most exposed bar in the ledger.** The scan decides on
   bar `t` and prices the fill at `open(t+1)`, so every fresh verdict's stored price sits on
   the frontier-adjacent bar — precisely the bar the next day's `incremental` pass rewrites.
   That is a property of the design, not an accident of this window.

## Background figures on the audit relations (reproduced by the script, not hardcoded)

Stated here as the motivation; the script recomputes all of it, per the repo's ban on
hand-written derived statistics.

- `price_daily_revision`: 1,011 rows / 413 instruments. `incremental` 623 across **409**
  instruments but only **2 distinct `price_date`s** (frontier-adjacent, age p50 = 1 day).
  `adjustment_heal` 382 across **2** instruments — MPU (198 bars, 2025-11-19 → 2026-09-15)
  and NCT.US (184 bars, 2025-12-22 → 2026-09-16), i.e. **whole-history rewrites**, age p50
  140 days, max 301. `force_backfill` 6 single bars.
⚠ These figures are from one read of a 1.5-day telemetry window and are **not** a rate.
They bound what part 2 can currently see — 183 observable signals against 58,886
unmeasurable — which is exactly why part 1 exists and leads.

## Why this is not the supersession fix, and what it is for

#2414 offers two candidate shapes — a corpus version in the key, or an explicit
supersede-and-record path. **Neither is chosen here**, deliberately:

- `strategy_signals` is read at **19 sites across 12 modules** (`strategy_paper_executor`,
  `strategy_live_gate`, `strategy_paper_runtime`, `outcome_ledger`, `strategy_monitoring`,
  `api/strategies`, …). A `superseded_at` state that those readers ignore is *worse* than no
  state: a row marked stale would still feed paper execution and the live gate, while the
  column asserts it does not.
- The rate that decides whether that cost is proportionate is the number this script exists
  to produce, and it did not exist when the work started.

So the ordering is the ticket's own: measure, then choose. This lands the measurement.

⚠ **What the measurement now says about that choice**, stated for the next session rather
than acted on here: 221 rows is small enough to repair and large enough to matter, the
damage is concentrated in 27 instruments, and the corrections are exact known ratios — so
the supersession design has a much easier job than "detect arbitrary staleness". It needs to
express *"this verdict's stored fill price was superseded by a corporate action"*, which is
a narrower claim than the general read-set problem sql/387 and sql/386 both ran into. That
is a design input, not a design.

## Reuse

Shape, CLI and read-only discipline follow `scripts/census_3157_core_quote_age.py` and
`scripts/census_3104_exit_gap.py`. One `psycopg.connect(settings.database_url)`, one
connection for the whole run (⚠ #3157's review finding: the dev cluster sits at its usable
`max_connections` ceiling, so a diagnostic must not hold or churn slots).

⚠ The whole census runs in **one `REPEATABLE READ` transaction**. sql/387's own checkpoint-2
finding is that under READ COMMITTED two statements let a deepening commit between them, so a
script can certify a state that never existed; sql/387 fixed it by collapsing to a single
SELECT, which does not scale to a per-cause breakdown. One snapshot across several statements
gives the same guarantee without the contortion, and `price_daily_revision` /
`price_daily_backdated_insert` are append-only so there is no serialisation failure to retry.

## Tests

Pure-logic, no DB (repo default). The SQL is a module constant; the tests cover the arm
predicates' *shape* — that each arm carries `revised_at > created_at`, that arm A is date
equality and arm C is strict `<` (so A and C cannot double-count the same row), and that the
observable denominator is floor-gated. One table-test over a small fixture of
(signal, revision) pairs against a pure classifier extracted from the SQL, so the arm
boundaries are exercised without Postgres.

## ⚠ Codex checkpoint 1 did not run

`codex exec` returned `You've hit your usage limit … try again at Sep 19th, 2026 11:57 PM`.
The spec was instead self-reviewed against the checkpoint-1 question list, which found and
fixed four things, all recorded above rather than silently corrected: the `revised_at >
created_at` direction is an **undercount** (the first draft called it "conservative in the
safe direction", which is backwards for an alarm); arms are disjoint per pair but **not
additive per signal**; `incremental` **can** touch the decided set's newest edge, so the
first draft's "no stored verdict has decided on yet" was too strong; and arm A's exactness
depends on the `IS DISTINCT FROM` guard, which was asserted before it was read and is now
cited. This is stated so a later reader does not mistake an unreviewed spec for a reviewed
one — the diff is read-only (`scripts/` + `tests/` + `docs/`, nothing under `app/`), which is
why it was shipped rather than parked.

## Acceptance

`PYTHONPATH=. uv run python -m scripts.census_2414_decided_bar_revisions` prints the
telemetry floor, the observable and unobservable denominators, and the four arms with their
per-cause split — and the output is pasted onto #2414 as the answer to its "Not yet measured"
clause, with the supersession decision stated against it.
