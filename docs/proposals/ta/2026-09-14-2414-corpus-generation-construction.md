# #2414 — the `corpus_generation` construction

Status: shipped (this PR). Issue: #2414 — **stays open** on the half named in §5.
Parent: `docs/proposals/ta/2026-09-14-2414-signal-ledger-corpus-stamp.md` (merged `115c0eea`).
Origin: `docs/proposals/ta/2026-08-08-strategy-signal-scan.md` §12.
Module: `app/services/corpus_generation.py`. Table: `sql/382_…`.
Reproduce every figure: `PYTHONPATH=. uv run python -m scripts.verify_2414_corpus_generation`.

The parent spec fixed the generation's *contract* and left its construction and the
retraction encoding to this session's checkpoint 1. Two checkpoint-1 passes ran (44
findings, then 37) and between them they re-scoped the ticket. This document records the
construction that shipped, and both things the passes corrected.

---

## 1. Where #2414 comes from, and what it is NOT

The scan spec's §12 is the ticket's origin, verbatim:

> ⚠ **A corrected historical bar cannot be reflected in an already-written signal.**
> `LedgerRow` records `input_rule_set_versions` … but **no corpus version**, and the writer
> has no `ON CONFLICT`. … It needs its own ticket, and the shape of the fix (**a corpus
> version in the key, or an explicit supersede-and-record path**) is a decision about the
> ledger, not about the scan.

Both candidate shapes need a **computable corpus identity**, and neither can be built
without one. This PR builds that identity and stores it as a **non-key, write-once**
column, so it prejudges neither shape. §5 is what stays open.

### ⛔ Two corrections, both from checkpoint 1, both worth keeping

**1a. §11 does not settle §12, and an earlier draft of this document read it as if it did.**
`strategy_signal_scan` quotes §11 twice — *"It does not backfill. Signals are a function of
what was known on the day."* That governs the **cold start**: deriving a decision for a bar
that was never decided, which `write_window_indices` bounds to one bar. It is a different
rule from §12's, which is about a bar that *was* decided against data since corrected.
Conflating them would have closed #2414 on a misreading, and the draft that did so was
caught only because checkpoint 1 was pointed at the framing rather than at the diff.

**1b. The FK argument runs the other way.** A first draft proposed one current row per key
plus a revision archive, arguing from `strategy_signals`' five foreign-key dependents
(`strategy_decision_contexts`, `strategy_entry_preflights`, `strategy_funding_decisions`,
`strategy_opportunity_forecasts`, `strategy_outcomes`, all on `signal_id`). Checkpoint 1:

> With separate rows, each child's `signal_id` identifies its original generation through
> its parent. Updating that parent silently reinterprets the child against different
> evidence.

`strategy_forecast_outcome_resolution` and `strategy_monitoring` both read the parent's
`fill_bar_date` / `fill_price`. Making those mutable rewrites the inputs of forecasts
already issued and trades already executed. The five FKs are an argument **for**
immutability, which is also the parent spec's contract 3.

---

## 2. What shipped

`corpus_generation` — a 16-hex stamp computed **once per scan pass**, from the corpus that
pass read, written onto every row the pass writes, and never updated.

It answers one question:

> Were these two stored decisions computed against the same corpus state?

- **Not key material.** `strategy_signals_unique` is unchanged at five columns;
  `store_signals` still has no `ON CONFLICT` and still raises.
- **Write-once on every ledger row.** The one exception is
  `strategy_scan_watermark.corpus_generation`, which is overwritten each pass — a ledger row
  records the corpus that produced *that decision*, the watermark records which corpus an
  identity last got to. The second is what makes "has the corpus moved under this strategy
  since it last scanned?" answerable without re-deciding anything.
- **Nullable, no default.** Pre-#2414 rows carry NULL. Because the column is neither key
  material nor ever updated, a NULL needs no conflict semantics: a mixed-version rollout
  where an older writer inserts an unstamped row degrades to "provenance unrecorded".
  ⚠ A NULL is **unknown**, not a value — an audit grouping NULLs, or comparing them with
  `IS NOT DISTINCT FROM`, would assert common provenance for exactly the rows whose
  provenance is unrecoverable.

### Where it is set, and why not on `LedgerRow`

The digest is only complete after the scan's instrument loop, and `resolve_fills` runs
*inside* it. So the stamp is a **keyword-only argument with no default** on the writers
(`store_signals`, `store_strategy_observations`, `advance_watermark`) — the `universe`
pattern from #2288, for the same reason: *"a field with a default is a field a writer can
forget."* This also sidesteps the parent's fatal finding 1 outright: nothing about the stamp
is derived from the series `resolve_fills` holds, so the cross-sectional trimmed slice
cannot affect it.

---

## 3. The construction

### 3.1 Placement — at the loader, over the bars the pass read

`run_signal_scan` already streams every eligible instrument's full masked series through
`load_masked_bars` in one loop (`strategy_signal_scan.py:797-810`), shared by every strategy
in the pass. The digest accumulates there: no extra I/O, and it covers the corpus the
decisions read. An instrument skipped as `moved_mid_scan` (`len(series) < 2`) contributes
nothing — it fed no decision.

That placement answers the parent's two fatal findings:

- **Finding 1** (the cross-sectional trimmed slice) is a property of what the *writer*
  holds. The digest is not taken at the writer.
- **Finding 2** (regime, breaks, peer panel) — the peer panel derives from the same bars and
  is covered by construction; the other two are folded in explicitly.

### 3.2 The payload

| component | value | why |
| --- | --- | --- |
| `rule_version` | `CORPUS_GENERATION_RULE_VERSION` | parent contract 4 — a change to the construction must rotate the stamp |
| `frontier_date` | the pass's frontier | the corpus boundary |
| `quarantine_rule_set_version` | the loader's own constant | the mask is an input to every bar |
| `spans` | every **loadable** instrument's `(instrument_id, last_bar, bars)` | ⚠ all loadable, not just eligible. ⚠ `InstrumentBarSpan` carries `last_bar` and `bars` only — there is no `first_bar`, and an earlier draft of this table claimed one |
| `panel_calendars` | `panel_dates_by_plan` — the union calendars this pass published, per strategy | ⚠⚠ **Checkpoint 2, P2.** A span summary is not the calendar: a stale instrument whose *interior* dates move keeps its `last_bar` and `bars`, and its bars are never folded in because it is not eligible — so the pass produced **one identical stamp for two corpora that rebalanced S-2 on different dates** (reproduced: 2026-02-02 vs 2026-02-03). Both components ship: spans is the cheap detector for a name appearing/vanishing/growing, the calendar closes the interior case |
| `breaks` | `unresolved_breaks`, instrument-sorted | `segmented_signals` takes it; resolving a break changes a verdict with no bar changing |
| `regime` | `MarketRegimeProvider.classification_items()` — the benchmark map's own `(date, label)` pairs | ⚠ **membership, not just values**: `for_dates` reads a date *absent* from the map as `not_evaluable` and a date *present* with a `None` value as warm-up, and those produce different reason codes |
| `bars` | the streaming per-bar digest of §3.3 | the corpus itself |

Serialisation is fixed rather than delegated to `json.dumps`: each component is a
`blake2b(digest_size=8)` over UTF-8 bytes with `\x1f` between fields and `\x1e` between
records, and the outer digest is over `f"{key}={hexdigest}"` lines sorted by key. Sorting an
object's keys canonicalises nothing about opaque inner digests. ⚠ The two opaque version
strings are the only fields that could carry a delimiter, so the builder **refuses** one
containing `\x1f`, `\x1e` or `=` — delimiter framing is otherwise ambiguous.

⚠ `add_series` must be called in **strictly ascending** instrument order, and that is
enforced rather than documented: the digest streams (buffering 3.4M bars to sort them would
be the full-corpus materialisation the scan spec calls unsafe), so caller order *is* the
digest.

### 3.3 The per-bar encoding, and why `str(Decimal)`

Each field is `str(value)`, or `~` for `None`. The parent spec requires a context-free
canonical form built from `as_tuple()`; this needs a strictly weaker property, and naming it
is the point:

> The encoding must be **injective** — two different stored values must never produce one
> string. It need not be **canonical**: one value producing two strings is a *spurious*
> rotation, which costs an audit annotation and nothing else, because the stamp authorises
> no replacement.

⚠ `str(Decimal)` is context-dependent **in general**, and checkpoint 1 is right to say so —
reproduced: `str(Decimal("1e30"))` is `1E+30` at `capitals=1` and `1e+30` at `capitals=0`.
The claim is bounded by the **column type**, not asserted generally. Exponent form needs a
non-negative exponent; psycopg's `NumericLoader` is `Decimal(data.decode())` over Postgres'
own text output, so a stored `numeric(18,6)` / `numeric(20,4)` always carries its column
scale and the exponent is always negative. `tests/test_corpus_generation.py` pins that
bound against a **mutated context** and both columns' extremes — asserting plain form at the
default settings would prove nothing about a process that changed them.

⚠ **Non-finite values reach the encoder and are encoded, not refused.** An earlier draft
claimed `load_masked_bars` excludes them; it does not. That loader compares **`open` only**
(`price_masked_bars.py:219`) — `high`, `low`, `close` and `volume` pass through on the
quarantine flags alone, so a PostgreSQL `numeric` `NaN` in any of those four arrives intact.
A `NaN` in `open` is the one field that does not reach the encoder, and for a
context-dependent reason rather than a structural one: `Decimal("NaN") > 0` raises
`InvalidOperation` under the default traps, and with that trap disabled it masks to `None`
instead.

### 3.4 Collision exposure

64 bits at **both levels**: each component digest is truncated before the outer digest, so
an inner collision hides that component's difference permanently rather than being
re-randomised. Both carry the same ~2³² birthday bound. The exposure is acceptable only
because the stamp is not key material — a collision degrades an audit annotation and cannot
admit a wrong row. The parent's 12-hex width is inherited from `strategy_version`, a value an
operator reads and types; a generation is only ever compared, so width costs nothing.

### 3.5 Four things the stamp does not claim

1. **Not a snapshot.** `run_signal_scan` requires `autocommit=True`, so spans, breaks, the
   benchmark and each instrument's bars are separate reads; a concurrent
   `daily_candle_refresh` can produce a combination that never existed together. The digest
   records what the pass **read**, mixture included.
2. **Not a publication unit.** One pass commits one transaction per strategy, so strategy A
   can commit a generation while B fails.
3. **Not scope.** Two strategies in one pass may hold different watermarks and write
   different bars. That is scope, already recorded by `strategy_version` and
   `signal_bar_date`.
4. **Equality is the strong direction; inequality is not.** Equal stamps ⇒ one corpus.
   Different stamps ⇏ different corpora — the digest covers a superset of any one decision's
   inputs, so an unrelated instrument's revision rotates the pass stamp for every row in it.

---

## 4. Cost — full population, and the sample was wrong by 4.6×

⚠ A 400-instrument sample said 2.9s at full scale for the parent spec's `as_tuple()`
encoder. The full population says **10.30s**. The sample was not wrong about scaling — it
benchmarked a cheaper encoding than the one it was reporting on, which is the more useful way
for a sample to be wrong, and it is what `.claude/CLAUDE.md`'s full-population rule exists
for. So the A/B ships as `scripts/verify_2414_corpus_generation.py` rather than as a table in
prose; all three encoders hash **one** loaded corpus in one process, so the comparison is
between encodings and not between runs.

Measured 2026-09-14 — frontier 2026-09-11, 5,804 eligible instruments, **3,364,930 bars**:

| per-bar encoder | seconds | vs shipped |
| --- | ---: | ---: |
| `as_tuple()` canonical form (parent spec) | 10.30 | 4.6× |
| `repr(row)` | 2.91 | 1.3× |
| **`str()` per field (shipped)** | **2.26** | — |

The production builder — all components, not just the bars — takes **2.33s**, against the
newest `scanned` pass in `job_runs` at **86s** (2026-09-13, 208,722 rows): **2.7%**.

⚠ The denominator is named deliberately. An `up_to_date` pass (3s, 2026-09-14) returns
before the instrument loop and computes no generation, so dividing by it would report an
overhead the pass never pays. ⚠ It is also not the same as "an up-to-date pass writes
nothing": `_publish_decision_calendars` publishes before that early return. Decision
calendars are not ledger rows and carry no generation — named in §5 rather than papered over.

---

## 5. What stays open on #2414

1. **The supersession half — §12's actual ask.** Two checkpoint-1 passes established it
   reaches **eight** tables (the parent named seven; `strategy_signal_observations` has the
   same five-column primary key and 813,194 of the 871,905 stored decisions live there),
   the five FK dependents of `signal_id`, a **retraction** case with no encoding (a revision
   that makes a fired signal *not* fire leaves the old row reading as actionable), and census
   delta semantics on `strategy_signal_daily_counts`. It also needs a rule this ticket does
   not have: **which revisions make a stored verdict wrong.** A retroactive split adjustment
   does not — the strategy saw the unadjusted prices, and so did a live trader — whereas a
   corrected bad print does. §12 does not distinguish them and neither does the parent spec.
2. **Detecting that a *stored* row's corpus has moved.** The watermark comparison answers it
   per identity, going forward. Answering it for a row written in 2026-01 would need that
   pass's eligible set, which is not stored.
3. **Decision calendars carry no provenance** (§4).
4. **The scan's write path is still forward-only**, and this PR does not change it. A
   historical re-decision is reachable only through a deliberate watermark rewind, which is
   not a defined operation.
