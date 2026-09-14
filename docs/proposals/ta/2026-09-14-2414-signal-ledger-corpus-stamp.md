# #2414 — recording which corpus produced a stored signal

Status: spec, unshipped. Issue: #2414. Table: `sql/255_strategy_signals.sql`.
Writer: `app/services/signal_ledger.py`. Caller: `app/services/strategy_signal_scan.py`.
Storage path: `app/services/strategy_observation_storage.py`.

⚠ **This spec replaces a first draft that proposed one migration and a bar-prefix hash.
Codex checkpoint 1 returned 36 findings against it and three of them are fatal to that
shape. They are verified below against the code, not taken on report.** The conclusion is
that #2414 is not a one-migration ticket, and that shipping only the key change would
produce a column nothing can use.

## The defect, restated

`signal_ledger.store_signals` has no `ON CONFLICT`, deliberately — a re-run must not
overwrite a recorded decision. `LedgerRow` records the producer's identity
(`strategy_version`, which since #2333 and #3034 hashes the strategy's module, params,
universe, cost model and input rule sets) but records **nothing about the corpus it read**.
So when a `price_daily` bar is revised after a signal on it was written:

- the stored verdict is wrong against the corpus it claims to describe;
- the corrected row collides on `strategy_signals_unique` and raises;
- nothing in the key moved, so the two versions of the truth are indistinguishable.

## What is already settled, and stays settled

The embargo shape is dead. This ticket named its own discriminator — *"if `31_365` +
`over_365` are consistently zero, the embargo shape is sufficient"* — and every revision
the `ef93efcc` histogram has recorded sits in `31_365`, at a **275-day** max age. The
mechanism makes it deterministic rather than statistical: `price_daily` is retroactively
split-adjusted, and `_candles_fetch_count` falls back to `lookback_days` (1000 bars) for
any instrument stale by more than three days.

⚠ Two corrections to how that has been stated on the issue, both flagged by checkpoint 1
and both fair:

- The split-adjustment finding was measured on **NVDA/TSLA/GOOGL** — three instruments.
  That is enough to establish that retroactive adjustment *happens* (an existence claim),
  and **not** enough to characterise its corpus-wide rate. No decision here rests on the
  rate.
- The rewrite depth is bounded by `lookback_days` and by provider history, not by "all
  history held" — `app/services/market_data.py:929` says in terms that rows outside the
  fetched window keep the old scale. The relevant point survives: 1000 bars is far past
  any correction buffer, and a bar can be revised at an age no embargo would cover.

## ⛔ Three findings that kill the bar-prefix design

### 1. The cross-sectional path does not hand the writer the series the decision saw

`_PendingMember.series` is documented as *"A TRIMMED SLICE — the window bars plus the ONE
bar after them"* (`strategy_signal_scan.py:588-600`), and `_stage_cross_sectional` passes
exactly that to the writer: `resolve_fills(signals, series=member.series, …)`
(`:1067`). So for every cross-sectional strategy a prefix hash over the series
`resolve_fills` holds covers **the scan window**, not the lookback — and its value would
change with the window size, which is a scan parameter and not a corpus property.

Passing the untrimmed series instead is not available: the same docstring records why the
slice exists — *"holding the whole series for 6,547 members instead would be the
full-corpus materialisation the spec calls unsafe"*.

### 2. The instrument's own bars are not the decision's whole input set

`_scan_per_series` evaluates through `segmented_signals`, which takes a **regime series**
derived from a benchmark instrument and a set of **unresolved series breaks**
(`strategy_signal_scan.py:910-917`); `segmented_evaluation` then evaluates each *segment*
with fresh state. Cross-sectional strategies additionally depend on the **peer panel** —
other instruments' scores, membership, and the union calendar.

Consequences, both directions:

- A stamp over one instrument's bars is **incomplete**: resolving a series break, or a
  revision to the benchmark used for the regime, changes a verdict while leaving the stamp
  identical. The collision returns, at a lower rate.
- It is simultaneously **too broad**: segmentation means history before a break is
  excluded from the decision, so revising it would rotate a stamp for a decision that
  never read it — producing a duplicate row for an unchanged verdict.

### 3. The uniqueness key is not what blocks the correction

Even with the key widened, a corrected row for a past bar cannot be stored:

- `strategy_observation_storage.py:331-360` takes a per-identity advisory lock, reads
  `strategy_scan_watermark.frontier_date`, and rejects every row whose `signal_bar_date`
  is **before** that frontier as stale.
- `_FIND_CROSS_TABLE_SIGNAL_CONFLICT` (`:187`) keys its incoming batch on
  `(strategy_id, strategy_version, instrument_id, signal_bar_date, signal_kind)` — the old
  five columns, no stamp — so a corrected `fired` row and the stored routine observation
  for the same bar still read as a cross-table conflict.
- `strategy_signal_daily_counts` is written in the same transaction on its own unchanged
  key, so a row that passes the new ledger constraint can still abort the batch.

**So the ledger key is necessary and nowhere near sufficient.** Shipping it alone is the
declared-but-unwired shape the prevention log warns about: a column whose stated purpose
is to permit a correction the rest of the pipeline still refuses.

## Recommended shape — a scan GENERATION in the key, not a bar digest

One recommendation, not a menu. The bar-digest family fails on findings 1 and 2 because it
tries to describe *what* was read, and the inputs to a decision are spread across four
producers with different shapes and lifetimes. The property actually needed is weaker and
uniform: **two decisions for the same bar must be distinguishable when they were computed
against different corpus state.**

`corpus_generation` — a 12-hex stamp recorded **once per scan pass**, from state the scan
already has, and carried onto every row that pass writes.

⚠ The construction itself is **not fixed here** and is the next session's checkpoint-1
subject. What is fixed is the contract it must meet, in this order:

1. changes whenever any corpus input the pass read could have changed;
2. O(1) per pass, never per signal;
3. immutable once a row carries it;
4. hashes its own rule version, so a change to the construction rotates the stamp — the
   gap checkpoint 1 found in the first draft, where a `..._RULE_VERSION` constant existed
   but appeared in neither the payload nor the key.

Candidate components, each already available to the scan and each needing its own
justification when the construction is written: `price_quarantine.RULE_SET_VERSION`, the
pass's frontier date, and an observed corpus-revision state (`daily_candle_refresh`
persists `bars_revised` and `bars_revised_age_days` into `job_runs.progress_json` — whether
that is a usable watermark or merely a diagnostic has not been established).

Why this and not the digest:

| | bar prefix | scan generation |
| --- | --- | --- |
| cross-sectional trimmed slice (finding 1) | **breaks** | unaffected — not derived from the series |
| regime / series breaks / peer panel (finding 2) | not covered | covered, because it identifies *when*, not *what* |
| cost | O(history) per signal, per strategy | O(1) per scan pass |
| stable once written | yes | yes |
| re-scan of UNCHANGED corpus | same stamp, no new row | **new stamp, duplicate row** ⚠ |

The one weakness is the last row, and it is answerable rather than fatal: the writer gains
an **identical-replay rule** — when a candidate row's non-key content is byte-identical to
a stored row that differs only in `corpus_generation`, skip it rather than insert. That is
a rule the ledger needs anyway (checkpoint-1 finding 18: today a mixed batch of corrected
and unchanged rows aborts on the first unchanged duplicate), and it converts "duplicate on
every re-scan" into "a new row exactly when something changed", which is the ticket's
actual requirement.

⚠ This is a **hybrid of the issue's two candidates**, and worth naming as such rather than
claiming candidate 1 won: the key material is candidate 1's shape, the identical-replay
rule and the newest-generation selection below are candidate 2's readability. Checkpoint 1
was right that candidate 2 was dismissed too fast — nothing establishes it as structurally
impossible, only as insufficient on its own.

## The sites that must change together

Shipping fewer than these is shipping nothing usable.

1. **`sql/NNN`** — `strategy_signals.corpus_generation`, and the uniqueness key.
2. **`signal_ledger.LedgerRow`** — the field, its validation mirror, and the
   identical-replay rule in `store_signals`.
3. **`strategy_signal_scan`** — compute the generation once per pass, thread it through
   both the per-series and the cross-sectional staging paths.
4. **`strategy_observation_storage`** — the watermark staleness guard must admit a
   re-decided historical bar carrying a new generation, and
   `_FIND_CROSS_TABLE_SIGNAL_CONFLICT` must key on the generation too.
5. **`strategy_signal_daily_counts`** — same key change, or an explicit statement of why
   a census row is generation-independent.
6. **A selection rule for every reader** — `strategy_monitoring`, the fired-signals
   endpoint, `outcome_ledger`, paper opportunity. "The decision for bar *t*" must become
   "the newest generation for bar *t*", or every reader double-counts after the first
   correction. ⚠ `strategy_outcomes` keys on `signal_id`, so a second stamped parent
   silently produces a second outcome; `outcome_ledger.py:205` selects every matching
   parent and validates no corpus compatibility.
7. **Retraction** — a revision that makes a previously-fired signal *not* fire has no
   replacement row to carry a new generation. The old row stays and still reads as
   actionable. Needs an explicit encoding; a new stamp cannot express an absence.

## Schema, when it is built

- `corpus_generation TEXT` — **nullable, no default**, `CHECK (corpus_generation IS NULL
  OR corpus_generation ~ '^[0-9a-f]{12}$')`.
- Recreate `strategy_signals_unique` as `UNIQUE NULLS NOT DISTINCT (strategy_id,
  strategy_version, instrument_id, signal_bar_date, signal_kind, corpus_generation)`.
  PostgreSQL 15+; this cluster is **17.9** (`show server_version`, checked).
- ⚠ Drop the **constraint**, which owns its index, and recreate the named constraint —
  leaving any five-column unique index behind defeats the change. Check the live schema
  first: `sql/279_strategy_signal_fired_only_indexes.sql` already removed three of
  sql/255's secondary indexes and installed a fired-only one, so the migrated shape is not
  what sql/255 reads.

### Why NULL and not a sentinel, and what `NULLS NOT DISTINCT` does and does not do

The 58,711 existing rows cannot be stamped: which corpus state produced them is
unrecoverable, exactly as pre-`ef93efcc` revision ages are.

- A sentinel string would have to be admitted by the CHECK, so the CHECK could no longer
  say "a generation is twelve hex characters", and the `LedgerRow` mirror would have to
  diverge from it. The value of mirroring a constraint is that the two agree exactly.
- NULL is the honest encoding of "unknown".
- Default `UNIQUE` treats NULLs as **distinct**, which would silently remove uniqueness
  protection from every legacy row. `NULLS NOT DISTINCT` keeps them in one group.
- ⚠ It does **not** make NULL a wildcard: a legacy NULL row and a stamped row sharing the
  old five columns coexist, by design. That mixed case needs its own test.
- ⚠ `LedgerRow` having no default does not by itself prohibit an explicit `None` — the
  validator must reject it, and SQL still permits a NULL from a direct writer. Say so
  rather than implying the type system closes it.

## If a content digest is ever revisited, these must be fixed first

Recorded so the next reader does not re-derive them:

- **`Decimal.normalize()` is context-dependent and lossy.** Reproduced at the default
  `prec = 28`: `Decimal('1.0000000000000000000000000000001')` and `…002` are distinct
  values that both render `1`. `price_daily` is `numeric(18,6)` / `volume numeric(20,4)`,
  so no stored value can reach that precision today — but `getcontext()` is process-global
  and mutable, so the canonical form must be context-free: build it from `as_tuple()`,
  strip trailing zero digits explicitly, normalise `-0`, and refuse non-finite values.
- **Append stability is not unconditional.** A signal on the last bar stores
  `no_fill_bar`; when the fill bar arrives, the bar range the decision covers genuinely
  grows. That transition must be excluded from any "stable under append" claim.
- **The masked series is not append-stable at all.** Quarantine `rule_b4` reads the *next*
  close, so appending bar `t+2` can re-mask bar `t+1` and rotate an older digest with no
  raw bar revised.
- **48 bits is a choice, not a given.** It matches `strategy_version`'s width, which is a
  legibility argument, not a collision bound. A digest scheme needs its collision exposure
  stated within an otherwise-equal key.

## Evidence, measured on dev 2026-09-14

- `strategy_signals`: **58,711** rows, all `verdict = 'fired'`, across **24** distinct
  `(strategy_id, strategy_version)` pairs.
- ⚠ **Rows belonging to a currently-declared strategy: 3,402** —
  `s4-volatility-compression-breakout` (3,246) and `s8-range-mean-reversion` (156), both
  `retired_reason is None` in `STRATEGY_MANIFEST`. The previous session's *"all 58,711
  belong to the dead eight retired by #2845"* is **false at strategy grain**; s4 and s8
  survived the cut as #2840's substrate.
- **Rows on a current SCAN identity: 0.** Every stored `strategy_version` is stale against
  `app.api.strategies._current_scan_versions()`. That — not the retirement claim — is what
  makes the legacy NULL group inert.
- Cluster is PostgreSQL **17.9**.

⚠ These are dev-cluster figures. They bound what a migration must survive **here**; they
are not a claim about a future population, and none of the design above rests on
"there is nothing to collide with today".

## What this spec does NOT do

It does not implement. The framing changed under checkpoint 1, and building against a
rewritten premise is the error #2182 and #3031 both record. The next session builds the
seven sites above, with its own checkpoint 1 on the generation construction.
