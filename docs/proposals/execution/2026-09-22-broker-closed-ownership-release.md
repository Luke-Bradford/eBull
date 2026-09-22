# Releasing ownership of a broker-closed position — second design, REFUSED

Status: verdict · 2026-09-22 · **REFUSED at Codex checkpoint 1, two rounds. Nothing ships.**
See "Verdict" at the end; the design below is the round-2 text, kept so the next attempt
starts from it rather than from round 1 · Refs #2965, #3284, #2437. Supersedes nothing: the 09-21
design (`2026-09-21-externally-closed-ownership-release.md`) stays REFUSED. This one uses a
different key and a different site, and discharges that doc's findings one by one.

## Why now

#3284 made a broker-side SL/TP mandatory on every engine position. A stop or target firing
is now an expected event, not an exotic one. Today it wedges the engine:

1. `manage_owned_position` finds the owned id absent from `get_portfolio()`
   (`strategy_position_manager.py:1264-1274`), sets the trade `reconcile_required`, returns
   `owned_position_missing`, and leaves ownership `active`. Every later cycle repeats this.
2. `resolve_engine_capital_usage` iterates active core ownership and raises
   `engine_capital_ownership_unwitnessed` for an id absent from the snapshot
   (`strategy_engine_capital.py:412-418`). Fail-closed, and permanent: nothing releases.

So the 09-21 doc's Ground 1 ("the class is covered, loudly") holds, and it is exactly the
problem — the cover is a wedge that needs a human, on an event the operator mandated.

## Key: a WHOLE close, witnessed by the broker's own denominator

Round 1 (key = snapshot absence + any close row) was REFUSED at checkpoint 1: an earlier
partial-close row plus a transient omission satisfies it with live exposure remaining. The
key is now a unit balance against the broker's OWN whole-position figure. Release only when
ALL hold, inside the manager's existing `position is None` branch:

1. **Absent** from the branch's fresh `broker.get_portfolio()`.
2. **Archived**: a `broker_positions_closed` row for this id with
   `closed_detected_at >= own.claimed_at` (written by `sync_portfolio` on disappearance,
   `portfolio_sync.py:247-268`). Its `initial_units` is the broker's whole-position size,
   last seen while live — NOT the history-derived open row Ground 2 rejected. Its
   `instrument_id` must equal the trade's and `is_buy` must be true.
3. **Balanced**: Σ `trade_events.units` over close rows for this id with
   `own.claimed_at <= executed_at <= now()` is `>= initial_units`. A partial close cannot
   pass (the remainder is missing from the sum); a delayed final slice cannot pass; a
   dropped same-ms collision (`conflict_anomaly`, never stored) cannot pass. Closes before
   the claim (another incarnation) and future-dated rows are excluded, not clamped.
4. **Priced**: every such close row has non-NULL `realized_pnl_usd` (else
   `_load_realised_delta` would refuse the release anyway; do not create that state).
5. **Quiet**: no `strategy_position_operations` row for this ownership in
   `intent_persisted | submitting | submitted | reconcile_required` (`_resume_operation`
   sees only the first three; an ambiguous close stays #2979's, not ours), and the trade's
   entry order has `strategy_order_reconciliation_state.state = 'resolved'` (no delayed
   executions pending).

Any condition failing → existing behaviour (`reconcile_required`, `owned_position_missing`),
re-checked next cycle; `_load_owned` admits `reconcile_required` trades.

⚠ Residual, stated not solved: if eToro ever closes a partially-closed position by closing
the old id whole and re-opening the remainder under a NEW id, condition 3 passes for the old
id and the remainder is unowned. Nothing documents that behaviour; the engine performs no
partial close (the core sell leg is unbuilt) and SL/TP closes are whole. Watch item for the
first observed partial close, alongside #2965's partial-fill half.

## Site and locks

The `position is None` branch of `manage_owned_position` already holds
`_paper_allocator_lock` + `_position_lock(broker_position_id)` (advisory 1-2 of the module
order in `strategy_order_reconciliation.py:8-17`) on an idle connection, after `_resume_operation` returned `None` (condition 5 covers what it
does not). Row order inside one transaction, as SEPARATE statements so the count takes a
fresh READ COMMITTED snapshot after the trade lock is granted: ownership UPDATE (6) →
`SELECT … FROM strategy_trades … FOR UPDATE` (8) → `SELECT count(*)` of active ownership →
trade UPDATE. Matches the declared order. ⚠ `_finish_close` counts before locking the trade;
both writers hold `_paper_allocator_lock`, which serialises them, so the mixed-writer race
Codex raised cannot interleave on the manager path.

## Writes

- ownership: `status='released'`, `release_reason='broker_closed_externally'`,
  `released_at = max(executed_at)` over the balancing slices — the final slice's time, since a
  delayed final slice fails condition 3 — not `now()`, because `strategy_wealth.py:49-50,79` marks NAV by `released_at` date.
- trade: `closed` iff no active ownership remains after the lock; otherwise status untouched
  (never written `open`).
- result: `PositionManagerResult(..., "applied", "broker_closed_externally")`.

## The 09-21 findings, discharged

| finding | here |
| --- | --- |
| close row ≠ whole close | unit balance vs broker `initial_units` (cond. 3) |
| history-derived open is not a denominator | denominator is the live snapshot's `initial_units` |
| id reuse / pre-claim closes | archive and slices bounded below by `claimed_at` |
| `resolved` not guaranteed | entry reconciliation must already be `resolved` (cond. 5) |
| "else `open`" erases sibling state | trade status written only when remaining = 0 |
| concurrent sibling releases | trade row `FOR UPDATE` after own ownership update; READ COMMITTED count after the lock sees the other's committed release, so the last one writes `closed` |
| lock order | advisory 1-2 held, rows 6 → 8 |
| in-flight / ambiguous ops | cond. 5 refuses any open or `reconcile_required` operation |
| claim resurrects released trade | pre-existing, unchanged; out of scope |
| `released_at=now()` misstates NAV | final balancing slice's time; future rows excluded |
| repair vs guard population | no guard added |
| empty-portfolio guard aborts ingestion | known limit: if the account's LAST position closes, `sync_portfolio:831` refuses and no close row lands, so this stays `reconcile_required`. Dev holds 8 positions. |

## Consumers after release

`resolve_engine_capital_usage` no longer iterates the id — this is the whole effect: the
wedge clears. `_load_realised_delta` (`strategy_engine_capital.py:112-160`) already sums the
close rows for active AND released ownership, so realised P&L does not change at release; its
`released AND close_count=0` refusal cannot fire under condition 3, and condition 4 keeps its
NULL-P&L refusal from being newly created. An empty broker response cannot release: condition
3 needs the full unit balance, not any close row.

## Tests

One DB test module for the new SQL: whole close → released at final slice time, trade
closed; partial-close slice only → unchanged; pre-claim close only → unchanged; future-dated
slice → unchanged; NULL P&L slice → unchanged; no archive row → unchanged; archive for another
instrument → unchanged; open/`reconcile_required` operation → unchanged; entry not `resolved`
→ unchanged; present in portfolio → untouched; two-ownership trade → first release leaves
status, second writes `closed`.

## Verdict — refused twice; the MODEL is the open question, not the key

**Round 1** (absence + any close row): an earlier partial-close row plus a transient
omission satisfies it with live exposure remaining.

**Round 2** (the unit balance above): the denominator is not proven. `_upsert_broker_positions`
overwrites `initial_units` on every sync (`portfolio_sync.py:177-179`), so nothing shows it is
still the ORIGINAL size after a partial close — if eToro reports the post-close size, the
round-1 counterexample returns. The archive (`broker_positions_closed`) is itself an absence
inference, may hold several rows per id, and never exists for a position opened and closed
between syncs. Further round-2 findings, each binding any next attempt: numeric validity
(positive/finite denominator and slices, nonfinite P&L); slices filtered by id+time only,
unique on `(position_id, executed_at)` not on execution; pre-claim executions excluded
permanently wedge; `_resume_operation`'s filled-close path calls `_finish_close` with no
balance check (pre-existing bypass); `_finish_close` and abandoned-close recovery still write
sibling trades `open`; an edit interrupted before an external close becomes
`reconcile_required` and condition 5 then blocks forever.

Two falsified keys on one model (`.claude/CLAUDE.md` step 3c: question the model). Both keys
derive "whole close" from quantities whose post-partial-close semantics are **unobserved**:
dev has never held a partially closed position (all 8 live rows `initial_units = units`,
2026-09-22). The broker may state the fact directly — `BrokerClosedTrade` carries
`investment` / `initial_investment` per slice (`app/providers/broker.py:615-643`), and the
snapshot carries `initialUnits` — but which of those survive a partial close unchanged is an
observation, not a derivation.

**Wake (producible on demand):** one attended demo partial close via `UnitsToDeduct` on a
non-engine position, reading (a) the snapshot's `units` / `initialUnits` before and after,
and (b) the history rows' `units` / `investment` / `initial_investment` after the remainder is
also closed. If `initialUnits` is immutable, round 2's key stands on its denominator and the
remaining findings are implementation work; if not, the history slice fields are the next
candidate.
