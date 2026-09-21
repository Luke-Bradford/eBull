# #2840 §8 obligation (a) — the RECORDING round is REFUSED, and the scan-path half is closed

Status: **refused**, with the measurements that refuse it. Refs #2840, #2437.

`54236fcc` refused obligation (a) as a static hash and re-framed it as *"record the route
with the evidence"*. This document is that construction, built and then refused. It is
committed rather than deleted because the refusal rests on four measurements a later session
would otherwise re-derive, and because the last one **closes** a queue head that has been
carried as open for three sessions.

## 1. The construction

Stamp every row and census bucket the scan writes with the certification state of the
price-basis carrier the pass actually dispatched — `not_consumed` / `uncertified` /
`observed_unadjusted` / `reconstructed_unadjusted` / `mixed`, derived from the carrier's
`values` and not stored on it (so `test_2840_price_basis_carrier.py:328`'s pinned
`from_undeclared_source(s) == from_archive_basis(None, s)` equality is untouched). Nullable
column on `strategy_signals`, `strategy_signal_observations` and
`strategy_signal_daily_counts`, following `sql/382`'s `corpus_generation` idiom; an
accumulate-and-refuse homogeneity check across the instrument loop to make one stamp per
pass sound.

The motivating attack throughout: an editor changes `strategy_signal_scan.py:987` from
`from_undeclared_source(series=series)` to a certifying constructor, and S-12 begins gating
on nominal `price_daily` levels the provider back-adjusts.

## 2. Why it is refused — four legs, each one command

### 2.1 `strategy_signals` stores one side of the split, so a column there sees one state, always

`store_strategy_observations` splits the batch: fired rows go to `store_signals`, everything
else to the observation tier (`strategy_observation_storage.py:398`). Measured on the dev DB:

```sql
select verdict, count(*) from strategy_signals group by 1;
-- ('fired', 59230)   -- 59,230 of 59,230
```

An **uncertified** S-12 carrier refuses every bar, so it contributes **zero** rows here. The
named victim — `_ATTRIBUTION_SQL`'s `GROUP BY s.strategy_id, s.strategy_version`
(`strategy_monitoring.py:260`) — additionally filters `WHERE s.verdict = 'fired'`. There is
therefore no pre-swap population in that query to pool a post-swap one *with*. The column
cannot discriminate what cannot co-occur.

⚠ This is the handoff's own refutation 1, restated. It was read at the start of this round
and then a column was specced onto `strategy_signals` anyway — see the prevention entry.

### 2.2 The transition is ALREADY recorded, by `(verdict, reason_code)`

`_s12_signals` **discards the regime** — `regime: RegimeSeries,  # noqa: ARG001` with a
docstring saying so (`strategy_manifest.py:712`). S-12's five `StrategyInput`s are
`price_basis`, close, ATR, compression and prior-high (`s12_cheapest_band_price_gated_
breakout.py:415-419`); only the first carries `PRICE_BASIS_REFUSAL_REASON`, and the other
four carry `masked_reason`.

⇒ For S-12, a stored `not_evaluable` / `missing_market_context` row is **unambiguously** a
price-basis refusal. The census today is exactly one bucket:

```sql
select verdict, reason_code, count(*) from strategy_signal_daily_counts
 where strategy_id like 's12%' group by 1,2;
-- ('not_evaluable', 'missing_market_context', 1)
```

A certifying swap makes `fired` / `not_fired` buckets appear on a dated census row, beside
the refusal buckets that stop. That is the transition, durably recorded, at bar-date grain,
in a table that outlives the 90-day observation detail — **with no new column**.

⚠ The tripwire's own comment warns that *"the reason code alone cannot separate a
price-basis refusal from a regime one"*. True in general; false for S-12's stored rows,
because S-12 declares no regime input. The general warning was read as if it bound this
case.

### 2.3 The transition a column could NOT make visible is unreachable on the scan path

A certification state says what was asserted about the bars, not where they came from. Two
different certifying archives both stamp `observed_unadjusted`, so an archive-A → archive-B
swap is invisible to it. That is the case where pooling under one `strategy_version` is
genuinely possible — and it cannot arise here:

- the scan has no archive at all: `from_undeclared_source(series=series)` unconditionally at
  `strategy_signal_scan.py:987` and `:1030`;
- `backtest_run`, which does select an archive (`:3254`), writes **neither** ledger table.

⇒ On the scan path the only reachable transition is undeclared → certified, which §2.2 shows
is already recorded. A ledger column would cover the covered class and miss the class that
matters — `54236fcc`'s own lesson, repeated one construction later.

### 2.4 The edit is already caught before it can write anything

`tests/test_2840_carrier_source_selection.py:93::test_the_scan_path_certifies_no_bar` drives
the real `_scan_per_series` and asserts `{row.verdict for row in out} == {"not_evaluable"}`.
Its regime fixture populates **every** date deliberately, so the carrier is the only thing in
the fixture that can produce `missing_market_context`. Its docstring: *"Swapping
`from_undeclared_source` at the call site fails this."*

⚠ One site is the right number, and it is already dispositioned at `:46`: *"ONE SITE, NOT
TWO. `_stage_cross_sectional` is not reachable for S-12."* No second tripwire is owed.

## 3. The cost that would have been paid for it

`PRICE_BASIS_RULE_VERSION` is composed from **this module's own bytes**:

```python
PRICE_BASIS_RULE_VERSION: Final[str] = (
    f"{_RULE_SET_ID}+{hashlib.sha256(Path(__file__).read_bytes()).hexdigest()[:12]}+archives-{_archives_hash()}"
)
```
(`strategy_price_basis.py:138-140`), and S-12 hashes it into `S12_PARAMS["price_basis_rule"]`
(`s12_cheapest_band_price_gated_breakout.py:251`), which enters `strategy_version`.

⇒ Adding `certification_state()` — a pure function that changes no verdict — to that module
**rotates S-12's identity**, and `strategy_scan_watermark` is keyed on `(strategy_id,
strategy_version)`, so S-12 cold-starts and its stored evidence detaches. The same
consequence `b9b2a1e5` recorded for S-12 and the 09-21 census recorded for S-4/S-8/S-11,
this time bought with a zero-behaviour edit. Extracted to the prevention log.

## 4. What the measurements DO settle, and what stays open

Measured on the dev DB, 2026-09-21, and reusable by whoever takes the residual:

| table | rows | distinct `(strategy_id, strategy_version)` | size | retention |
| --- | --- | --- | --- | --- |
| `strategy_signals` | 59,230 (all `fired`) | 28 | 34 MB | durable |
| `strategy_signal_observations` | 922,821 | 39 | 3 partitions | **90-day** |
| `strategy_signal_daily_counts` | 737 | 39 | 296 kB | durable |
| `strategy_scan_watermark` | 39 | 39 | 32 kB | durable, overwritten every pass |

- The "high-volume tables" objection (Codex finding 29 on the deleted draft) is true of one
  table, `strategy_signal_observations`. It is not what refuses this; §2 is.
- The watermark arm was never viable as a *recording* surface: `advance_watermark` upserts in
  place (`strategy_signal_scan.py:394-402`) and `sql/382` documents that overwrite as
  deliberate, so it can carry a current state and never a history.
- **Obligation (a)'s SCAN-PATH half is closed** — by §2.2's existing census encoding plus
  §2.4's existing tripwire, not by anything shipped here. Remove it from the queue.
- **What stays open is the BACKTEST path**, and it is the same wall: `backtest_run` selects
  an archive and writes neither ledger table, so no ledger column can reach it. The live
  residual on #2840 is **per-series bar provenance**, which is the other replacement
  `54236fcc` named.

## 5. Checkpoint 1

32 findings, run against this construction before any code was written. Decisive:
finding 1 (`_ATTRIBUTION_SQL` is fired-only → §2.1), finding 3 (S-12 discards the regime →
§2.2), finding 5 (certification state is not a source → §2.3), finding 4 (an existing
behavioural test already detects the edit → §2.4), finding 14 (the whole-file hash rotates
S-12 → §3), finding 12 (the spec named `_stage_per_series`; the function is `_scan_per_series`).

The remaining findings attack the construction's mechanics — `mixed` conflating four carrier
shapes, `states == set()` on a legitimately empty pass, `PriceBasisSeries.segment()` building
carriers the outer inventory never sees, the required keyword breaking
`scripts/verify_2240_outcome_ledger.py:183` with a `TypeError` rather than a NULL, and a
validated `CHECK` needing a scan rather than being catalog-only. All are real and none
needed answering once §2 refused the construction itself.

## 6. Prevention

Two entries in `docs/review-prevention-log.md`:

1. A whole-file hash makes every edit to that module an identity rotation.
2. Before adding a column to distinguish two populations, check both populations can have
   rows in that table — and that a refutation already on the ticket has been applied as a
   constraint, not merely read as context.
