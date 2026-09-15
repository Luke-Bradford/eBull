# #3068 — the reconciliation comparand marks its two sides at different instants

Status: PROPOSAL. Unshipped. Written 2026-09-15; revision 2 after Codex checkpoint 1,
which killed the first recommendation and surfaced the field that makes a better one
possible. Evidence table also on #3068.

## Problem

`account_reconciliation_check` (#2844 clause 3) decides a day by comparing

- **official** — `broker_account_equity_snapshots.official_direct_long_market_value`
  + `available_cash`, where the market value is `Σ(amount + unrealizedPnL.pnL)` over
  `isBuy` positions, parsed from eToro's `/api/v1/trading/info/demo/pnl` when
  `daily_portfolio_sync` runs — **23:55 UTC**; against
- **local** — `portfolio_eod_snapshots.total_value`, computed at 22:30 UTC as
  `Σ (amount ± units × (close − open_rate))` + cash, converted to the display currency,
  where `close` is `price_daily`'s **regular-session close** (20:00 UTC while the US is on
  EDT). ⚠ That is the real formula (`portfolio_eod.py:214-219`); it collapses to
  `units × close` only for an unleveraged long, which is what the whole current book is.

The tolerance is `RECONCILIATION_RULE_VERSION = "f0-reconcile-v1"`:
`MARK_ROUNDING_PER_UNIT` (one cent) per unit held, plus one cent of cash. Its own
docstring calls it *"the tightest bound defensible without measurement"*. It models
**rounding of one mark against the same mark**, so the rule carries an unstated premise:
*the broker's mark equals our session close*.

That premise is false for any instrument eToro quotes after 20:00 UTC, and the tolerance
has no term for the difference. The day's verdict is therefore a function of a quantity
the rule does not model. ⚠ It is not that every evening move reds a day — a small or
offsetting move still passes. It is that nothing bounds the one that does not.

## Measured

2026-09-14, the first session of the #2844 countdown: `diverged`,
`difference = -204.65` against `tolerance = 31.56`, **no incomplete reasons** — a hard red,
not a refusal. Attributed against eToro's own `get_intraday_candles` (`FifteenMinutes`)
around the snapshot instant:

| instrument | units | our close | broker mark @ ~23:55 UTC | Δ USD |
| --- | --- | --- | --- | --- |
| GME (1699) | 1500 | 21.63 | 21.50 (23:30 bar close) | −195.00 |
| QQQ (3006) | 17.252438 | 710.02 | 709.46 (23:45 bar close) | −9.66 |
| VOO (4238) | 16.929336 | 699.23 | no bar after 20:00 UTC | 0 |
| IEP (1571) | 315.948108 | 7.03 | no bar after 20:00 UTC | 0 |
| NXH (1181) | 1305.057096 | 3.60 | no bar after 20:00 UTC | 0 |
| | | | **total** | **−204.66** |

Observed −204.65, attributed −204.66: agreement **to within one cent**, and the residual is
expected — a 15-minute bar's close is not the tick at `observed_at`. ⚠ Nor does "no bar
after 20:00 UTC" prove those three marks were unchanged; it is consistent with it and is
not a measurement of the broker's mark. The two evening-quoted names carry the whole
signal either way.

Scale: `Σ |units| × 0.01 = 31.55`, so the tolerance is 31.56 including the cash cent. At
1500 GME units a **three-cent** evening move ($45) exceeds it; two cents ($30) does not.

⚠ **This is one day of one five-holding book.** It establishes the mechanism and the
arithmetic; it does not establish a rate across stored days, instruments, leverage,
currencies or corporate actions. No full-population claim is made here and none is needed
— the defect is in the rule's premise, not in a frequency.

Both stored `reconciled` days (08-24, 08-25) are already outside
`MAX_EVIDENCE_AGE_DAYS = 12`, so the streak is **void today**. A
`RECONCILIATION_RULE_VERSION` bump costs zero now and costs a restarted countdown later.

## Source rule

No regulator specifies how a firm reconciles its own book against its broker's, and the
search is not re-run here: it was already done and recorded at `RECONCILIATION_RULE_VERSION`
(an earlier draft's SEC Reg NMS Rule 612 citation was withdrawn as off-point — Rule 612
governs the increments on which NMS stocks may be QUOTED, not how far two valuations of one
holding may differ, and it does not reach CFDs).

⚠ That exemption covers the **tolerance magnitude and the cadence** only. It does not
license inventing field semantics: the operand definitions are governed by eToro's own
published contracts, which must be cited per field. The ones this proposal relies on, from
the committed `tests/fixtures/etoro/openapi_v1.375.0.json`:

- `ClientPortfolio.positions[]` is `TradingRealAdminApi_Position` in **both**
  `PortfolioResponse` and `PortfolioResponseWithPnl` — the same item schema on `/portfolio`
  and on `/pnl`.
- `TradingRealAdminApi_Position.units` — *"Number of units in the position"*.
- `.amount` — *"USD amount allocated to the position"*; `.openRate` — *"Entry price of the
  position in the instrument's currency"*; `.isBuy` — *"true for long (buy) positions"*;
  `.isPartiallyAltered` — *"whether this position was partially closed"*;
  `.initialUnits` — *"does not change"* (so `units` is the current quantity).
- `.unrealizedPnL` — *"only present in PnL endpoint"*, which is what makes the two
  responses differ at all.

Settled decisions consulted: 2026-09-13 "#2844 reconciliation countdown counts DAYS OF
EVIDENCE" (a `RECONCILIATION_RULE_VERSION` bump resets the countdown to zero — anticipated
there, and this proposal takes it) and 2026-08-22 "the allocation boundary is the ONLY
safety net". Neither is reversed.

## ⚠⚠ The field that changes the design — `units` is on the `/pnl` payload

`_parse_account_risk_snapshot` reads `amount`, `unrealizedPnL.pnL`, `instrumentId`,
`positionId`, `isBuy` and `isPartiallyAltered` from each position row and **does not read
`units`**, which the same row publishes. Revision 1 of this document asserted units were
absent from the payload and built a whole candidate around working around that. They are
not absent; they were unread. Checkpoint 1 caught it against the portal schema.

This matters because it makes a **per-position implied mark** computable from the official
payload alone: `mark_official(p) = (amount + pnL) / units`, all three terms from one `/pnl`
row. That removes the mark-instant mismatch **by construction** — the official mark and
the quantity it is applied to come from the same observation — without needing an
independent price at an arbitrary instant.

## ⚠ What the control can actually test — read before choosing

The countdown spec records it in its own limitations: *"`_read_positions` reads **current**
`broker_positions`, so a snapshot for session D is D's closes applied to the book at run
time."* So the local side is not an independent book; it is the broker's own position set
(from `/portfolio`), valued with our marks and our FX.

The comparison therefore tests our parse, our persistence, our valuation arithmetic, our FX
and our cash mapping — across two endpoints — and, nominally, our marks. Only the last is
broken by this defect, and it is the one the control cannot perform anyway: it cannot tell
a wrong mark from an evening move.

⚠ Two claims from revision 1 are withdrawn here, both falsified at checkpoint 1:

- *"a failed `sync_portfolio` leaves a stale book beside a fresh `/pnl` snapshot, and the
  control catches it."* It does not: in `daily_portfolio_sync` the snapshot is recorded
  **after** `sync_portfolio` returns, so an exception there prevents the official write
  too. Equality after a successful sync also demonstrates post-repair consistency, not
  that the run before it was correct.
- *"mark correctness is #3046's control."* Overstated. `portfolio_eod` reads raw
  `price_daily` with no quarantine gate, and quarantine's own admission test is
  *not-known-bad* (`app/services/price_quarantine.py:27`). Giving up the mark check here
  gives it up, full stop — see Accepted losses.

## Candidates

**A — mark the local side at the broker's instant from our own feed** (intraday bars at
`observed_at`). Keeps a mark check. Adds an external fetch inside the reconciliation path,
a bar-alignment rule, and a NEW unmeasured tolerance term (bar close vs tick). ⚠ And
`observed_at` is stamped after the HTTP call completes — it is when we received the
response, not when eToro struck the mark, so "the broker's instant" is itself approximate.

**B — capture the official snapshot just after the regular close**, so the existing premise
becomes true. Smallest diff. But "just after the close" does not guarantee the closing
mark (auction dissemination, bid/ask basis, DST, early closes, feed lag), it holds only for
a US-venue book, and it re-breaks silently with no reason emitted the day a non-US holding
appears. It couples a correctness control to a scheduler constant.

**C — reconcile structurally, mark-free** (position set + units + cash), value comparison
demoted to a recorded figure. Rejected at checkpoint 1 and correctly: as drafted it read
BOTH units operands from `/portfolio`-sourced storage, which compares a value to itself; it
gives up more than marks (a wrong `amount`, direction or currency mapping passes); and the
ledger writer requires a finite `difference` **and** `tolerance` for any decided row
(`account_reconciliation_ledger.py:359`, plus SQL constraints), so a mark-free verdict has
no legal value to store.

**D — value the local book at the official payload's own per-position mark.** ⭐

## Recommendation — D

For each direct long position `p` present in the official payload, with
`mark_official(p) = (amount_official(p) + pnL(p)) / units_official(p)`:

```
local_at_official_marks = Σ_p [ amount_local(p) + units_local(p) × (mark_official(p) − open_rate_local(p)) ]
                          + cash_local        (converted to the account currency)
difference              = official_comparand − local_at_official_marks
```

where `amount_local`, `units_local` and `open_rate_local` come from the stored
`broker_positions` rows written from `/portfolio`, and every official term comes from
`/pnl`.

Why this is the right construction:

- **The mark-instant mismatch cannot occur.** The mark and the quantity it prices are two
  fields of one observation. There is no second instant to align.
- **It stays a VALUE comparison**, so the ledger's finite-`difference`/finite-`tolerance`
  contract, the stored diagnostic figure, and the existing `/strategies` panel labels all
  keep working. C could not say that.
- **The units check is genuinely two-endpoint.** `units_local` is `/portfolio`-sourced and
  `units_official` is `/pnl`-sourced; they are separate requests whose agreement is a real
  assertion. ⚠ They are the same *schema*, so this tests our fetch/parse/persist of two
  responses, not two independent upstream computations. Claim it that narrowly.
- **A structural error shows at its own value.** A missing local position contributes its
  whole market value to `difference`; stale units contribute `Δunits × mark`. Both are
  exactly what the clause wants to catch.
- **The residual tolerance is honestly rounding again** — the division in
  `mark_official`, and FX. Which is what `f0-reconcile-v1` was already written for.

## Design if D is taken

**Parser.** `_parse_account_risk_snapshot` reads `units` per position into
`BrokerDirectPositionInvestment`. Refuse a non-positive or non-finite `units` on a row
carrying a non-zero market value — a zero divisor must not silently become an implied
mark. ⚠ `units` is a JSON number: define the decimal normalisation explicitly
(`Decimal(str(...))`, as every other money field here already does) and the precision the
storage column holds. Do not assert exactness that JSON decoding does not provide.

**Storage.** `broker_account_position_marks`, one row per direct position per snapshot day,
FK to `broker_account_equity_snapshots (environment, snapshot_date)`, keyed
`(environment, snapshot_date, position_id)`, carrying `instrument_id`, `is_buy`, `units`,
`amount`, `unrealized_pnl`, `market_value`, `is_partially_altered`. Written in the same
transaction as the parent, and — because the parent is an `ON CONFLICT DO UPDATE` that
only accepts a NEWER observation for TODAY — the child set must be **replaced wholesale
exactly when the parent accepts the write, and left untouched when it does not**. The
parent's `RETURNING` already reports which happened.

**Verdict inputs.** Position-set symmetric difference recorded with SIDES (missing-local
and extra-local separately, never a count), per-position units deltas, the cash delta, and
the value `difference` above. Existing FX and completeness refusals are unchanged.

**Legacy.** Every stored snapshot predating this has no child rows. That is **missing
evidence → `refused`**, with its own reason, never an empty official book (which would
otherwise read as "the broker holds nothing" and reconcile against an empty local side).

**Version.** `RECONCILIATION_RULE_VERSION` → `f0-reconcile-v2`; resets the countdown by
construction, and both current greens are already void.

**Still unsolved, and D does not claim otherwise.** The official set is captured at 23:55
UTC and the local EOD book at 22:30 UTC, and the countdown spec documents a 0-3 day lag on
the local stamp — so this is not reliably an 85-minute window. Any open, close, partial
close, deposit, withdrawal, fee or dividend in between is a real structural difference on a
healthy pipeline, and it freezes a red. D narrows the exposure from *every mark on every
evening-quoted name* to *account activity between two captures*, which on a static demo
book is currently zero and on a trading engine will not be. Closing it needs the two
captures paired in one observation with a pairing identity, which is its own slice.

## Accepted losses, stated

- A green no longer says anything about our `price_daily` marks, and nothing else currently
  does either (see the withdrawn claim above). This is a real reduction in coverage, taken
  deliberately: the mark check as built cannot distinguish a bad mark from an evening move,
  so what is being given up is the appearance of a control, not a control. If mark
  agreement is wanted it needs its own same-instant comparison, filed separately.
- Aggregate offsetting errors: D reduces this materially (per-position operands) but the
  stored verdict is still one scalar. The per-position deltas are recorded for audit.

## Tests

- Parser→storage→verdict, with `units` varied INDEPENDENTLY between the `/portfolio` and
  `/pnl` fixtures. A test that fabricates both from one constant cannot see the defect this
  whole document is about.
- Same-id corruption: right `position_id`, wrong `instrument_id` / wrong `is_buy`.
- The regression this exists for: an evening mark move of arbitrary size does NOT move the
  verdict, asserted against the measured 2026-09-14 numbers.
- Zero / missing / non-finite `units` on a non-zero market value → refusal, not a divide.
- Legacy snapshot with no child rows → `refused` with its own reason, not `reconciled`.
- Child-set replacement: parent write rejected (older observation) leaves children intact;
  parent accepted replaces them wholesale; a newer snapshot that drops a position removes
  its child row.
- The freeze trigger still refuses to move a decided row.

## Checkpoint-1 findings deferred, not answered

Recorded so the implementing session knows they were seen and are open: precedence between
"incomplete" and "proven mismatch" when both hold; the population contract for unmapped
instruments / zero-unit rows / duplicates / shorts / mirrors; `credit`-vs-documented-
available-cash and the pending-order refusal; per-currency cash scope; `is_partially_altered`
semantics for current-vs-initial units; read consistency when the EOD parent and child rows
are read in separate queries while a replacement runs; and mixed-version deployment/API-label
behaviour across the version bump.
