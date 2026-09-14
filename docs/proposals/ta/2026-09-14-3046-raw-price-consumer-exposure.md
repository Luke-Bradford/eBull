# #3046 step 1 — the break-minting census, and why the consumer-exposure table is not this PR

Status: spec, **v3 (narrowed)**. Refs #3046, #2293, #2261, #2282, #3031.

## How this got here

#3046 step 1 asks for a per-consumer exposure table: *"for each raw `price_daily`
consumer, determine whether its window can span a stored `price_transition_quarantine`
transition."* Two Codex checkpoint-1 passes (41 findings, then 60) went at that design.

- **v1** split exposure by rule class — "only T3 changes the unit regime". Falsified in one
  line of source; see §1. That is this session's finding.
- **v2** rebuilt it with the window set by arithmetic class instead. The second pass
  established that a table meeting its own standard needs each consumer's *eligibility
  predicate* reproduced — the writer's 400-close load bound, risk metrics' finite-positive
  filter and future-bar ceiling, beta's date-intersection, the frontend's own SMA/EMA over
  raw candles — across ~30 sites including `frontend/`, and that even then it yields
  **potential** exposure, because `sql/198_instrument_risk_metrics.sql` records that a
  stored metric is not reconstructable from current bars.

So v3 ships the half that is sound and decisive and re-scopes the rest on the issue. The
residual is written up there with the requirement list, not left to be re-derived.

## 1. The finding — rule identity does not certify scale-safety

`app/services/price_quarantine.py:486`:

```python
if magnitude >= params.magnitude_threshold and not rules:
```

**T3 is evaluated only when neither T1 nor T2 has already fired.** The rationale for the T2
half is written directly above it and is sound on its own terms: *"a `price_series_break`
minted from a gap would strand history behind a break that never happened."*

Nothing addresses the other direction. A genuine scale change landing across a hole, or
beside a return-unusable close, is recorded as `{T2}` or `{T1}`, mints **no**
`price_series_break`, and is therefore invisible to `app/services/price_segments.py` —
which is the model `outcome_resolver` and every strategy consumer segment on. The strategy
path is the capital path, so this is not a research-only concern.

⚠ This is a suppression census, **not** a claim that the suppressed transitions are missed
splits. Whether a large move across a three-month hole is a scale change or a real move is
exactly the question the code declines to answer, and this measurement does not answer it
either. What it establishes is the **size of the population the segment model cannot see**,
which is the input the step-2 contract decision was missing.

⚠ Fixing the suppression is out of scope and would be the wrong move to make casually:
`price_quarantine` sits in `INPUT_RULE_SETS` (#3031), so editing the rules rotates strategy
identity **and** empties the 76M-bar backtest substrate, and only the first is visible.

## 2. What the script measures

`scripts/verify_3046_break_minting_census.py`, read-only, writes nothing.

**Pinned identities, printed before any count** — `price_quarantine.RULE_SET_VERSION`, the
distinct `rule_set_version` values actually present in each table, `current_date`, the
corpus max `price_date`, and the coverage completeness check (instruments with a coverage
row at the current version, and any whose `last_bar` trails their newest bar). A census
over stale verdicts is a different measurement and must not be reported as this one.

**Threshold operand.** Per instrument, from `price_quarantine_coverage.asset_class` — *"as
seen at evaluation time; NULL is a real state"* (`sql/247_price_quarantine.sql:45`). That
is the value that produced the stored verdicts, so it is the one that reproduces them;
today's `exchanges.asset_class` would be a different question. Thresholds are **imported**
from `price_quarantine._CLASS_PARAMS`, never copied, so a recalibration cannot leave this
script asserting a stale constant.

**Magnitude.** Symmetric `max(r, 1/r)` with an inclusive `>=`, mirroring the classifier.
Rows with a NULL `observed_ratio` are unmeasurable and are reported on their own line, not
folded into either side.

**Buckets**, each reported as `(rows, distinct instruments)`:

| bucket | meaning |
| --- | --- |
| `t3_minted` | `T3 ∈ rules` — the arm that must reconcile to `price_series_break` |
| `t1_suppressed` / `t2_suppressed` / `t1_t2_suppressed` | magnitude ≥ T, no T3, because T1 and/or T2 fired first |
| `admitted_or_deferred` | `rules = '{}'` at magnitude ≥ T — T3 triggered and `_corroboration` admitted it back, or a provisional bar deferred it. Split by `corroboration` and `provisional`; these are **not** quarantined |
| `below_threshold` | everything else |

**Reconciliation arm — the one that earns its keep.** `t3_minted` must equal the
`price_series_break` row count at the same version. If it does not, the census is not
measuring what it claims and the run says so rather than printing a suppression figure
nobody can trust. `price_series_break` is reported as unresolved / resolved separately
because `price_segments.load_unresolved_breaks` filters `resolved_by IS NULL`, and a
resolved row survives a verdict refresh (`price_quarantine_store.py:142` deletes only the
unresolved ones).

**Recency.** Each suppressed bucket is also reported for the trailing 400 days, because a
transition older than every live window bounds nothing that is being acted on today.

## 3. What the output may and may not be used to claim

- ✅ "N transitions clear their own class threshold and mint no break; the break table
  holds M." Both computed in the same run.
- ✅ "The segment model cannot see that population by construction."
- ❌ "N scale changes are missing." The suppression is a declined question, not a
  false negative — §1.
- ❌ Anything about B2/B3 range defects, which set `range_usable = false` with no
  transition row of their own (`sql/247_price_quarantine.sql:14-16`) and corrupt ATR, the
  stochastic and 52-week extrema. Outside this measurement; silence here is not safety.
- ❌ Anything about which *consumers* currently hold a value computed across one. That is
  the re-scoped residual.

## 4. What Codex checkpoint 2 changed in the implementation

Three P2s on the diff, all real, all fixed:

- **The reconciliation arm count-matched and passed by luck.** `price_quarantine_store.py:142`
  deletes only `resolved_by IS NULL` breaks on refresh, so a resolved row survives while
  its T3 verdict is rewritten — the counts then differ on valid data. Now a `FULL OUTER
  JOIN` on `(instrument_id, break_date)`, the writer's own key, reporting matched /
  T3-without-break / break-without-T3 separately. Only the middle cell fails the run.
- **The coverage check read `last_bar` alone**, which `refresh_market_data(force_backfill=True)`
  walks straight past: it adds OLDER bars without moving the newest date, the stale tables
  still reconcile, and the census exits 0 having never evaluated the backfilled history.
  Now also `first_bar` and `bars_evaluated`.
- **`read_only` is not isolation.** `price_quarantine_refresh` runs on a 24 h cadence and
  rewrites all three tables; under the default READ COMMITTED the identities, the break
  count and the transition rows could each come from a different committed state. Now
  `REPEATABLE READ`, which §2 specified and the first implementation did not do.

## 5. Acceptance

- Runs read-only against the dev corpus, writes nothing, completes.
- Prints pinned identities before any count; refuses to report if coverage is not at one
  version across the population.
- The `t3_minted` arm reconciles to `price_series_break`, and a mismatch is printed as a
  failure rather than a footnote.
- Thresholds are imported, and the per-class table is printed with the counts it produced.
- No derived statistic is hardcoded in this document, the script's docstring, or its output
  strings.
