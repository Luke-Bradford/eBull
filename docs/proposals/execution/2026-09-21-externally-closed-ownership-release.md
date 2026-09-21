# Releasing ownership of a broker-closed position — REFUSED

Status: verdict · 2026-09-21 · Refs #2602, #2965, #2844, #2437

Specced and killed at Codex checkpoint 1 on two independent grounds, both confirmed at
source. **Nothing ships.** The premise was wrong and the release key was unsound.

## What was proposed

One predicate used twice — *an ownership row with `status='active'` for which a
`trade_events` row exists with `event_kind='close'` and that `position_id`*. **Repair**:
release it inside `sync_portfolio`'s transaction after `record_trade_events`, terminalising
the trade by `_finish_close`'s rule. **Guard**: mirror the existing
`released AND close_count=0` refusal in `_load_realised_delta` with `active AND
close_count>0`.

## Ground 1 — the class is already covered

The spec claimed a double-count: a closed-but-still-`active` position counted both as live
exposure (`core_active_recorded_committed`) and as realised P&L. That is wrong about the
number that governs exposure.

`resolve_engine_capital_usage` — the function that actually resolves exposure against the
broker — already refuses this exact state:

```python
# app/services/strategy_engine_capital.py:414-418
row = positions.get(position_id)
if row is None:
    raise EngineCapitalObservationError(
        f"active core position {position_id} is absent from broker snapshot",
        "engine_capital_ownership_unwitnessed",
    )
```

`core_committed` is then built from the broker snapshot's own `row.amount`, not from
`core_active_recorded_committed`. So for the core arm an externally-closed owned position
raises loudly and fail-closed at the point exposure is computed. There is no silent
double-count to fix there, and the proposed guard would have been a second gate on a
covered class.

⚠ A residual remains and is **not** what was specced: stale reservations for the **alpha**
arm and pool configuration, where `alpha_committed` is charged for any non-terminal trade.
Any future work starts from that narrower statement, and from `resolve_engine_capital_usage`
rather than `load_engine_capital_authority`.

## Ground 2 — a close event does not mean the position closed

`events_from_history`'s own docstring (`app/services/trade_events.py:191-197`):

> *"Transform history rows into events: **one close per slice** plus one synthesized open
> per position"*

The schema agrees: `uq_trade_events_close` is unique on `(position_id, executed_at)` where
`event_kind='close'` (`sql/194:60-61`), so many close rows per position are the designed
shape, not an anomaly. A **partial** close therefore satisfies the proposed predicate
exactly.

Releasing there is **fail-open**, which is strictly worse than the state it set out to fix:
the position keeps real broker exposure while the strategy loses management
(`strategy_position_manager.py:303`), the close path, position limits, unrealised-P&L
reporting — and the manual-EXIT protection at `order_client.py:238`, which is the guard
that currently stops an operator EXIT touching a strategy lot.

Declaring partial closes "not in scope", as the spec did, does not remove them from the
predicate.

⛔ **And the obvious repair does not work.** Summing close units against the open row is
insufficient: history-derived opens contain only the fetched slices and first observation
wins (`trade_events.py:191-215`, `:326` `ON CONFLICT … DO NOTHING`), so the open row is not
a reliable denominator. Close-unit sums in excess merely log. There is no sound
close-completeness test available from this data alone.

## Further findings recorded, not fixed

Each was raised at checkpoint 1 and none is load-bearing now that the design is refused;
they bound any future attempt.

- **`resolved` is not guaranteed.** Neither release nor `_finish_close` sets entry
  reconciliation to `resolved`, so repair could produce `closed` + `pending` — still refused
  for core.
- **`_finish_close`'s "else `open`" erases sibling lifecycle state** (`closing`,
  `reconcile_required`) when one of several positions is released. Reusing that rule
  verbatim inherits the bug.
- **Concurrent sibling releases** can each see the other's uncommitted row as active and
  both write `open`, leaving a trade `open` with zero ownership.
- **Lock ordering is unspecified**, and the module's declared order
  (`strategy_order_reconciliation.py:8-17`) wants ownership locks in ascending position id
  before the trade. A per-position repair loop can invert against `_finish_close`.
  The existing lock helpers cannot simply be called inside `sync_portfolio` — they commit
  and require an idle connection, which would break that transaction.
- **Released ownership makes in-flight operations unreachable** — `_load_owned` fails
  before `_resume_operation`, so a persisted/submitting close can hang while its exit order
  is still live.
- **`claim_exact_position` and `_claim_entry_execution` can resurrect** a released row's
  trade without reactivating ownership.
- **`released_at=now()` after delayed history** misstates historical NAV, which marks from
  `claimed_at` (`strategy_wealth.py:48`).
- **Repair and guard populations differ** — the repair was written over all active
  ownership, while `_load_realised_delta` covers only eligible trades since the pool epoch.
- **Empty-portfolio guards run before event ingestion** (`portfolio_sync.py:614`), so a
  full liquidation — the case most needing repair — can abort the transaction first.

## Measured on dev, read-only, 2026-09-21

`strategy_position_ownership`: 1 row, `active`, trade `open`, 0 close events.
`released` rows: 0. `broker_positions_closed`: 0 rows. `trade_events` closes: 3, none for
an owned position. So nothing was going to fire today either way; #2844 clause 3's 09-26
countdown was never at risk from this. n=1.

## Process lesson — the same defect twice in one session

Both refusals today came from reading the function a ticket cites instead of the function
that **consumes** its output.

- #2965 (`b10ebeeb`): read `load_engine_capital_authority:278-280`, which the ticket
  quotes, and not `:354` twelve lines below — where the opposing raise sits.
- #2602 (this doc): read `load_engine_capital_authority` in full, and not
  `resolve_engine_capital_usage`, which is where exposure is actually resolved and where the
  covering guard already lives.

The second happened **after** the first was written up as a process lesson and merged. A
lesson recorded is not a lesson applied.

**Rule: before claiming a defect in a computed value, follow that value to its consumer and
read the consumer.** The producing function tells you what is stored; only the consumer
tells you whether anything is wrong with it. Concretely, grep the returned dataclass /
field name for its readers before writing the word "silently".
