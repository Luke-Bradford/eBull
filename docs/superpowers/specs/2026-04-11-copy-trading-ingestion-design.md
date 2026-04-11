# Copy trading (mirrors) ingestion

**Issue**: #183
**Related**: Track 2 — social discovery as a research signal (GitHub issue to be opened after spec merge)
**Date**: 2026-04-11
**Status**: Draft for review

## Problem

The current eToro broker provider reads `clientPortfolio.positions` and
discards `clientPortfolio.mirrors`. On the demo account this hides two
copy portfolios worth roughly £29.3k — every AUM-denominated rule in the
execution guard is therefore computed against an incomplete denominator,
and the dashboard is silent about a five-figure slice of the operator's
capital.

The portfolio payload we already fetch on every sync contains the full
state of those copy portfolios: mirror-level metadata (initial
investment, available cash, realized P/L, stop loss, copy start date,
the trader's CID and username) plus nested per-position rows with
instrument, entry price, units, open timestamp, and stop/target levels.
On one mirror alone the demo account carries 198 nested positions.
First-class ingestion is a matter of stopping the discard, not adding
new API calls.

This ingestion has two further consequences we want to design for from
day one:

1. The execution guard must stay blind to mirrors for decision purposes
   (we cannot close or resize them, so they must not constrain eBull's
   own trades) but must see them for AUM sizing (the denominator in
   every position and sector % rule).
2. A separate workstream ("Track 2") will use eToro's `/user-info/people/*`
   endpoints to research traders we don't yet copy, snapshot their live
   portfolios over time, and derive accumulation signals for the ranking
   engine. Track 2 is a new issue, not part of this PR, but the data
   model laid down here should be directly extendable without migration
   churn when Track 2 lands.

## Scope

**In scope (this spec / PR):**

- New tables `copy_traders`, `copy_mirrors`, `copy_mirror_positions`
- Broker provider parses `clientPortfolio.mirrors[]` into typed
  dataclasses alongside the existing positions
- Portfolio sync upserts the three tables from the same `/portfolio`
  call it already makes
- AUM computation (execution guard) adds mirror equity to the total_aum
  denominator
- Dashboard read endpoint + frontend surface for copy traders
- Execution guard decision logic is unchanged — mirrors live in separate
  tables and never appear in sector-exposure or position-size queries

**Out of scope (deferred to Track 2):**

- `/user-info/people/*` endpoint integration (profile, gain series,
  trade info, live portfolio of arbitrary users, people/search)
- Trader discovery and watchlist
- Historical trader performance tracking (monthly/daily gain series)
- Current profile snapshot columns/tables (risk score, copiers count,
  popular-investor flag, weeks since registration, etc.) — Track 2
  decides at the time whether these belong on `copy_traders` or on a
  separate `trader_profile_snapshots` table
- Signal derivation (accumulation / divestment by a trusted cohort)
- Graceful mirror-closure semantics — v1 treats disappearing mirrors
  (partial *or* full) as API failures and raises; an operator
  deletes `copy_mirrors` rows by hand to intentionally un-copy
- Currency-aware MTM using a *current* FX rate — v1 uses the
  entry-time `open_conversion_rate` stored on each nested position;
  see §3.2 for the approximation
- Live verification of the short-position formula against a
  short-containing mirror (the demo account has none as of spec date)

## 1. Data model

Three new tables. `positions`, `cash_ledger`, and the existing
`positions.source` column are untouched.

### 1.1 `copy_traders`

```sql
CREATE TABLE copy_traders (
    parent_cid      BIGINT PRIMARY KEY,
    parent_username TEXT   NOT NULL,

    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX copy_traders_username_idx ON copy_traders (parent_username);
```

Semantics: one row per eToro trader identity, keyed by `parentCID`.
Track 1 populates every column.

Track 2 deliberately has no column footprint on this table yet. The
earlier draft reserved nullable columns (`risk_score`, `gain_1y_pct`,
etc.) to "avoid a migration later", but Track 2 already needs its own
tables for historical gain series, daily-gain series, and cohort
signals — one more column-adding migration alongside those tables is
not the pain point and YAGNI wins. Track 2 will decide at the time
whether the *current* profile snapshot belongs on `copy_traders` or in
a separate `trader_profile_snapshots` table (leaning towards the
latter: the gain and risk numbers are already time-series data by
nature).

Upsert (Track 1): `ON CONFLICT (parent_cid) DO UPDATE SET
parent_username = EXCLUDED.parent_username, updated_at = NOW()`. We
honour the latest username eToro returns.

### 1.2 `copy_mirrors`

```sql
CREATE TABLE copy_mirrors (
    mirror_id  BIGINT PRIMARY KEY,
    parent_cid BIGINT NOT NULL REFERENCES copy_traders(parent_cid),

    initial_investment          NUMERIC(20, 4) NOT NULL,
    deposit_summary             NUMERIC(20, 4) NOT NULL,
    withdrawal_summary          NUMERIC(20, 4) NOT NULL,
    available_amount            NUMERIC(20, 4) NOT NULL,
    closed_positions_net_profit NUMERIC(20, 4) NOT NULL,
    stop_loss_percentage        NUMERIC(10, 4),
    stop_loss_amount            NUMERIC(20, 4),
    mirror_status_id            INTEGER,
    mirror_calculation_type     INTEGER,
    pending_for_closure         BOOLEAN NOT NULL DEFAULT FALSE,
    started_copy_date           TIMESTAMPTZ NOT NULL,

    raw_payload JSONB NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX copy_mirrors_parent_cid_idx ON copy_mirrors (parent_cid);
```

Semantics: one row per mirror (one per currently-copied trader).
`parent_cid` is a FK to `copy_traders` so the trader identity is a
stable spine regardless of whether the copy is active, paused, or
restarted with a new `mirrorID`.

**Why `deposit_summary` / `withdrawal_summary` are first-class
columns and not buried in JSONB.** Funded capital for a mirror is
`initial_investment + deposit_summary − withdrawal_summary`, not
`initial_investment`. The demo account already shows this — mirror
`15714660` has `initialInvestment=17280` and `depositSummary=2251`.
Auditing "how much capital did we commit to this trader" without
these columns would give the wrong answer by $2,251 on day one. They
also make it trivial to reconcile our AUM identity (see §3).

`raw_payload` is the full mirror object from the broker as JSONB,
kept for auditability and schema evolution — if a future field
becomes interesting (e.g. `mirrorStatusID` sub-codes we haven't
categorised), we can backfill it from `raw_payload` without
re-fetching history. It is **not** a stable substrate for Track 2
profile data: the mirror payload does not contain `riskScore`,
`gain`, `copiers`, or `popularInvestor` — those come from
`/api/v1/user-info/people/{username}/tradeinfo` and require their
own fetch path.

We deliberately do **not** store a derived `mirror_equity` column.
Snapshotted derived values on a live account are a trap — equity
depends on current quotes for the nested positions, and storing a stale
denominator leads to stale rule evaluation. Equity is computed on read
(see §3).

Upsert: `ON CONFLICT (mirror_id) DO UPDATE SET ... , updated_at = NOW()`.
All mirror-level columns are refreshed from the latest payload.

### 1.3 `copy_mirror_positions`

```sql
CREATE TABLE copy_mirror_positions (
    mirror_id   BIGINT NOT NULL REFERENCES copy_mirrors(mirror_id) ON DELETE CASCADE,
    position_id BIGINT NOT NULL,
    PRIMARY KEY (mirror_id, position_id),

    parent_position_id BIGINT NOT NULL,
    instrument_id      BIGINT NOT NULL,

    is_buy                    BOOLEAN        NOT NULL,
    units                     NUMERIC(20, 8) NOT NULL,
    amount                    NUMERIC(20, 4) NOT NULL,
    initial_amount_in_dollars NUMERIC(20, 4) NOT NULL,
    open_rate                 NUMERIC(20, 6) NOT NULL,
    open_conversion_rate      NUMERIC(20, 10) NOT NULL,
    open_date_time            TIMESTAMPTZ    NOT NULL,
    take_profit_rate          NUMERIC(20, 6),
    stop_loss_rate            NUMERIC(20, 6),
    total_fees                NUMERIC(20, 4) NOT NULL DEFAULT 0,
    leverage                  INTEGER        NOT NULL DEFAULT 1,

    raw_payload JSONB NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX copy_mirror_positions_instrument_id_idx ON copy_mirror_positions (instrument_id);
```

Semantics: one row per nested position currently held inside a mirror.

**`instrument_id BIGINT`.** Matches [sql/001_init.sql:2](sql/001_init.sql#L2)
and every other FK-bearing table in the repo — the original draft's
`INTEGER` was a convention break.

**`open_conversion_rate NOT NULL`.** Non-negotiable. On the demo
account, 74 of 198 positions on mirror `15712187` are non-USD: GBP
(`~1.158`), JPY (`~0.01331`), ILS (`~0.103`), EUR (`~1.16`). Storing
only `open_rate` and computing `units * open_rate` gives
$313,171 for that mirror — a cross-currency sum of nonsense. The
identity `SUM(units * open_rate * open_conversion_rate) ≈ SUM(amount)`
has been verified empirically on both mirrors in
`data/raw/etoro_broker/etoro_portfolio_20260411T053000Z.json` and
differs by $0.01 (rounding). Without this column, the AUM query in
§3 is wildly wrong for every non-USD position.

**Composite primary key `(mirror_id, position_id)`, not
`position_id` alone.** The eToro API reference does not document
position-ID uniqueness across mirrors, and the code must not assume
it. A composite key is cheap (one extra `int8` per row, and the
`(mirror_id, ...)` prefix is already the natural access pattern for
§2.3's eviction query) and eliminates an invariant we cannot prove
without a broker source that does not exist. It also obviates a
separate `copy_mirror_positions_mirror_id_idx` — the PK covers it.

**No foreign key on `instrument_id`.** Copy traders trade a wider
universe than our synced `instruments` table (copy portfolios commonly
contain commodities, FX, crypto, and non-US equities we have not
onboarded). Enforcing the FK would force us to either reject these
rows or pre-sync the whole eToro universe. We accept unknown instrument
IDs as opaque identifiers and LEFT OUTER JOIN against `instruments` in
reads.

**`raw_payload JSONB NOT NULL`.** Every nested position keeps its own
raw payload, not just the mirror row. Codex flagged the original
design (raw on the mirror only) as brittle: the mirror row's
`raw_payload` gets overwritten on every sync, so historical per-row
audits would need to fan out through a time-travel query just to
reconstruct "what did that position look like an hour ago". Keeping
the raw per-row costs one JSONB column but makes every nested field
we haven't promoted to a typed column (e.g. `totalExternalFees`,
`unitsBaseValueDollars`, `pnlVersion`) debuggable from the DB alone.

**`ON DELETE CASCADE` from `copy_mirrors`.** Track 1 never deletes
`copy_mirrors` rows on sync (see §2.3), so this is defence-in-depth —
if a future PR introduces mirror deletion we do not want orphaned
nested position rows.

## 2. Sync flow changes

### 2.1 Broker provider interface (`app/providers/broker.py`)

Two new frozen dataclasses and one additive field on
`BrokerPortfolio`:

```python
@dataclass(frozen=True)
class BrokerMirrorPosition:
    position_id: int
    parent_position_id: int
    instrument_id: int
    is_buy: bool
    units: Decimal
    amount: Decimal                      # pre-converted USD cost basis
    initial_amount_in_dollars: Decimal
    open_rate: Decimal                   # entry price in the instrument's native currency
    open_conversion_rate: Decimal        # FX snapshot at open (native -> USD)
    open_date_time: datetime
    take_profit_rate: Decimal | None
    stop_loss_rate: Decimal | None
    total_fees: Decimal
    leverage: int
    raw_payload: dict[str, Any]


@dataclass(frozen=True)
class BrokerMirror:
    mirror_id: int
    parent_cid: int
    parent_username: str
    initial_investment: Decimal
    deposit_summary: Decimal
    withdrawal_summary: Decimal
    available_amount: Decimal
    closed_positions_net_profit: Decimal
    stop_loss_percentage: Decimal | None
    stop_loss_amount: Decimal | None
    mirror_status_id: int | None
    mirror_calculation_type: int | None
    pending_for_closure: bool
    started_copy_date: datetime
    positions: Sequence[BrokerMirrorPosition]
    raw_payload: dict[str, Any]


@dataclass(frozen=True)
class BrokerPortfolio:
    positions: Sequence[BrokerPosition]
    available_cash: Decimal
    raw_payload: dict[str, Any]
    mirrors: Sequence[BrokerMirror] = ()   # NEW — default keeps existing callers working
```

**Why `mirrors` has a default and not a required position.** There
are two existing `BrokerPortfolio(...)` call sites:
[etoro_broker.py:456](app/providers/implementations/etoro_broker.py#L456)
and the test helper at
[tests/test_portfolio_sync.py:56](tests/test_portfolio_sync.py#L56).
The earlier draft called the addition "non-breaking" — it isn't
unless the field has a default, because `@dataclass(frozen=True)`
produces a positional/keyword constructor and a new *required* field
breaks every existing call. Defaulting to an empty tuple preserves
both call sites unchanged at the type level, and the etoro_broker
parse pass below populates the real value. Tests that want to
exercise mirrors pass them explicitly.

The alternative — an additional method `get_mirrors()` on the
interface — was rejected because mirrors arrive in the same payload
as positions and splitting them would double the HTTP call count per
sync.

### 2.2 `etoro_broker.get_portfolio` (`app/providers/implementations/etoro_broker.py`)

The existing `portfolio = raw.get("clientPortfolio") or {}` block is
extended with a second parse pass over `portfolio.get("mirrors") or []`.

Parsing follows the existing malformed-row handling pattern at the
positions loop (line 427-428):

```python
for m in raw_mirrors:
    if not isinstance(m, dict):
        continue
    try:
        mirrors.append(_parse_mirror(m))
    except (KeyError, ValueError, TypeError) as exc:
        logger.warning("Skipping malformed mirror object: %s", exc)
        continue
```

`_parse_mirror` is a new pure normaliser function alongside
`_normalise_open_order_response` (line 519) — no I/O, no DB access, no
dependence on instance state. It validates required fields
(`mirrorID`, `parentCID`, `parentUsername`, `initialInvestment`,
`availableAmount`, `closedPositionsNetProfit`, `startedCopyDate`),
parses optional fields with defaults, and recursively normalises the
nested `positions[]` into `BrokerMirrorPosition` instances via a second
normaliser `_parse_mirror_position`.

Malformed nested positions inside a mirror are skipped, not failed
at parse time — the mirror as a whole is still returned. The
parse-success ratio is re-checked by the sync layer before eviction
runs (§2.3.3), and a catastrophic drop raises there. A mirror with
a genuinely-empty payload (`raw_positions == []`) is ingested with
`positions=[]` — that is a valid state (the mirror holds only
`available_amount` cash for the moment) and the §3.2 formula
handles it correctly as `mirror_equity = available_amount`.

### 2.3 `portfolio_sync` (`app/services/portfolio_sync.py`)

A new top-level function `_sync_mirrors(conn, mirrors, now)` is called
from `sync_portfolio` after the existing position and cash
reconciliation. It runs inside the same transaction.

```python
def _sync_mirrors(
    conn: psycopg.Connection[Any],
    mirrors: Sequence[BrokerMirror],
    now: datetime,
) -> tuple[int, int]:
    """Returns (mirrors_upserted, mirror_positions_upserted)."""
```

#### 2.3.1 Single-writer invariant

`daily_portfolio_sync` holds a session-scoped Postgres advisory lock
via `JobLock` ([app/jobs/locks.py:60](app/jobs/locks.py#L60)) for the
full duration of the run, and APScheduler's `max_instances=1`
default ([app/jobs/runtime.py:213](app/jobs/runtime.py#L213))
enforces one concurrent instance per job on top of that. These two
layers — process-local and cross-process — mean two instances of
`sync_portfolio` (and therefore `_sync_mirrors`) **cannot** run
concurrently against the same database.

`_sync_mirrors` relies on this invariant. It does *not* take its own
advisory lock, because stacking locks inside an already-serialised
job adds reasoning overhead with no correctness benefit. If a future
PR ever peels `_sync_mirrors` off onto a non-portfolio-sync code
path (e.g. an ad-hoc reconciliation endpoint that is not serialised
by `JobLock`), the author must add an advisory lock at that call
site — this spec documents the assumption so that author knows what
they're removing.

#### 2.3.2 Per-mirror sync

Per mirror:

1. **Upsert `copy_traders`** by `parent_cid`.
2. **Upsert `copy_mirrors`** by `mirror_id`. All mirror-level columns
   including `raw_payload` are refreshed.
3. **Replace nested positions for this mirror.** Each sync is an
   authoritative snapshot. Inside the transaction, for each mirror:

   a. Upsert every position in the payload by `(mirror_id,
      position_id)`.
   b. Evict positions that have closed since the last sync:
      `DELETE FROM copy_mirror_positions WHERE mirror_id = %s AND
      position_id <> ALL(%s::bigint[])`, passing the new IDs as a
      single array parameter (avoids SQL-injecting a variadic
      `NOT IN (...)` list and sidesteps the empty-list parser
      error).

      Postgres evaluates `position_id <> ALL('{}')` as `TRUE` for
      every row, so an empty array correctly deletes everything for
      that mirror. We exploit this rather than guarding against it —
      no special-case branch.

   Implemented as upsert + evict rather than delete-all + insert-all
   so that row locks are held for the shortest possible time and a
   crashed sync mid-replace can never briefly zero the table.

#### 2.3.3 Parser-failure safeguard

The malformed-row skip pattern from §2.2 (log + continue) is unsafe
when composed with the eviction step. If eToro returns 198 positions
and a schema change makes the parser reject half of them, the sync
would upsert the 99 good ones and evict the other 99 — silently
destroying local rows for the affected positions.

Guard: before `_sync_mirrors` commits the evict step for a given
mirror, compare `len(parsed_positions)` against `len(raw_positions)`.
If fewer than half the raw positions parsed successfully, and at
least one raw position existed, raise `RuntimeError` with the mirror
ID and the parse-success ratio. The whole transaction is rolled
back. The operator sees a failed `daily_portfolio_sync` in
`job_runs`, investigates the upstream schema change, and fixes the
parser before the next fire. Better to page than to delete.

Rejected alternatives:

- "Just skip eviction when any position failed." Leaks stale rows
  forever and the failure mode is invisible.
- "Require 100% parse success." Too brittle — a single new-enum
  value on one of 198 positions would block every mirror every sync.
- "Fail only if zero positions parsed." The 99-out-of-198 case is
  exactly the scenario we want to catch.

The 50% threshold is arbitrary-but-defensible for v1. A future PR
can tighten or make it configurable.

#### 2.3.4 Empty-mirrors and partial-disappearance guards

**Empty-mirrors guard.** Mirroring the existing positions guard at
[portfolio_sync.py:245](app/services/portfolio_sync.py#L245): if
`portfolio.mirrors` is empty but the local `copy_mirrors` table has
rows, `_sync_mirrors` raises a `RuntimeError`. Upstream "looks
broken" — do not silently zero local state. Operator-driven
un-copying is a manual DB operation in v1.

**Partial-disappearance guard.** Codex flagged that guarding only
the fully-empty case leaves a hole: if one of two mirrors disappears
from a non-empty payload, the local `copy_mirrors` row for the
missing mirror remains forever and keeps inflating AUM.

Track 1 response: after processing every mirror in the payload, if
any local `copy_mirrors.mirror_id` is absent from
`{m.mirror_id for m in portfolio.mirrors}`, raise `RuntimeError`
naming the missing mirror_id(s). Same rationale as the empty-mirrors
guard and as the existing positions "legitimate liquidation is
per-position, not whole-portfolio" invariant — a mirror disappearing
without the operator touching the DB is either a broker API
regression or a genuine un-copy, and neither should silently change
AUM math.

Operator un-copy workflow (manual, v1): `DELETE FROM copy_mirrors
WHERE mirror_id = ?;` before the next sync fires. Graceful closure
semantics — `closed_at` column, "keep history but mark as closed",
historical AUM reporting — are deferred to Track 2.

Result type extension:

```python
@dataclass
class PortfolioSyncResult:
    positions_updated: int
    positions_opened_externally: int
    positions_closed_externally: int
    cash_delta: Decimal
    broker_cash: Decimal
    local_cash: Decimal
    mirrors_upserted: int          # NEW
    mirror_positions_upserted: int # NEW
```

`sync_portfolio` calls `_sync_mirrors(conn, portfolio.mirrors, now)`
after cash reconciliation and before returning. Caller still owns the
commit.

## 3. AUM computation

The execution guard at
[execution_guard.py:249-287](app/services/execution_guard.py#L249-L287)
currently computes:

```text
total_positions = SUM( MTM over positions table, sector-grouped )
cash            = SUM(cash_ledger.amount)
total_aum       = total_positions + cash
```

We extend this with a third term, `mirror_equity`, derived from the
new tables.

### 3.1 The identity the formula must preserve

The earlier draft's formula had

```text
mirror_equity_bad = available + closed_pnl + SUM(sign * units * open_rate)
```

which is wrong on two counts that were verified against the real
payload at
`data/raw/etoro_broker/etoro_portfolio_20260411T053000Z.json`:

1. **`closed_pnl` is double-counted.** eToro already reconciles the
   closed P/L into `available + SUM(position.amount)`. Empirically,
   on both demo mirrors:

   ```text
   mirror 15712187 (thomaspj):
     available + SUM(amount)                       = 2800.33 + 17089.33 = 19889.66
     initial + deposit − withdrawal + closed_pnl   = 20000 + 0 − 0 + (−110.34) = 19889.66

   mirror 15714660 (triangulacapital):
     available + SUM(amount)                       = 1724.11 + 17666.76 = 19390.87
     initial + deposit − withdrawal + closed_pnl   = 17280 + 2251 − 0 + (−140.13) = 19390.87
   ```

   Adding `closed_pnl` explicitly on top of the cost-basis sum
   over-counts it by `2 * closed_pnl`. Silent $110 drift on a single
   mirror on day one.

2. **`units * open_rate` is cross-currency nonsense for non-USD
   instruments.** On mirror `15712187`, 74 of 198 positions are in
   GBP / JPY / ILS / EUR. Empirically:

   ```text
   SUM(units * open_rate)                        = 313,171.88  # nonsense
   SUM(units * open_rate * open_conversion_rate) =  17,089.32  # USD
   SUM(amount)                                   =  17,089.33  # USD (rounding)
   ```

   Without `open_conversion_rate`, AUM is inflated by $296k on a
   single mirror.

### 3.2 The Track 1 formula

```text
mirror_equity = SUM over copy_mirrors (
    m.available_amount
  + SUM over copy_mirror_positions in this mirror (
        cmp.amount
      + sign(cmp) * cmp.units * (COALESCE(q.last, cmp.open_rate) − cmp.open_rate)
                  * cmp.open_conversion_rate
    )
)
```

where `sign(cmp) = +1 if cmp.is_buy else -1`.

Decomposed by term:

- **`m.available_amount`** — uninvested USD cash held inside the
  mirror sub-account. Directly reported by the payload.

- **`cmp.amount`** — per-position cost basis in USD. When no
  `quotes.last` exists, the MTM-delta term below evaluates to zero
  and this is the only contribution — we fall back to cost basis,
  matching the conservatism the guard already applies to eBull-owned
  positions at [execution_guard.py:255](app/services/execution_guard.py#L255).
  When summed across the mirror, `available + SUM(amount)` is
  exactly the identity in §3.1 (initial + net-funded + closed P/L),
  so `closed_positions_net_profit` is covered by this term and **is
  not re-added**.

- **`sign * units * (q.last − open_rate) * open_conversion_rate`**
  — the MTM *delta* since entry. Zero when no quote is available
  (fallback `q.last := open_rate`). When a quote exists, the delta
  is converted to USD using the entry-time conversion rate. This is
  an approximation (FX may have drifted since entry) but is
  documented and testable; a proper current-FX MTM requires a
  currency-aware `quotes` table, which is Track 2 scope. Sign
  handles longs (positive delta → equity up) and shorts (positive
  delta → equity down) correctly.

### 3.3 Short-position handling

Codex correctly observed that `-1 * units * price` (the draft's
formula) is short *notional*, not short *equity*. The revised formula
above does not compute notional at all — it computes the **delta from
cost basis**, which is the right accounting quantity for a CFD or
cash short:

```text
long  with delta +X → equity ↑ by X
short with delta +X → equity ↓ by X   (handled by sign = -1)
```

And for leverage > 1, the cost-basis term (`cmp.amount`) is already
what the trader committed (not the notional exposure) — exactly the
quantity that should contribute to the AUM denominator.

The demo payload contains only `isBuy = true`, `leverage = 1`
positions, so the short and leverage paths are not exercised by the
day-one fixtures. Track 2 adds a verification step against a real
short-containing mirror once one is observed on the demo account.

### 3.4 SQL sketch

Implemented as a single CTE alongside the guard's existing
portfolio-read block:

```sql
WITH mirror_equity AS (
    SELECT COALESCE(SUM(
        m.available_amount + COALESCE(p.mv, 0)
    ), 0) AS total
    FROM copy_mirrors m
    LEFT JOIN LATERAL (
        SELECT SUM(
              cmp.amount
            + (CASE WHEN cmp.is_buy THEN 1 ELSE -1 END)
              * cmp.units
              * (COALESCE(q.last, cmp.open_rate) - cmp.open_rate)
              * cmp.open_conversion_rate
        ) AS mv
        FROM copy_mirror_positions cmp
        LEFT JOIN LATERAL (
            SELECT last
            FROM quotes
            WHERE instrument_id = cmp.instrument_id
            ORDER BY quoted_at DESC
            LIMIT 1
        ) q ON TRUE
        WHERE cmp.mirror_id = m.mirror_id
    ) p ON TRUE
)
SELECT total FROM mirror_equity;
```

**Critical: this query feeds the denominator only.** It does not
contribute to `sector_values` or any per-sector aggregation. Mirrors
can never push us past a sector concentration limit — they only make
the per-rule percentage denominator larger (more permissive), which is
the correct behaviour because the operator has already committed this
capital to the mirrors and cannot unwind it through the execution
guard.

## 4. Execution guard isolation — what does NOT change

The execution guard's rule queries (sector exposure, per-position %,
initial-position %) read `FROM positions` only. Mirrors live in
separate tables. No existing query is edited to filter out mirrors
because there is nothing to filter out.

This is the specific property that Option C (three-table split)
secures: a PR that touches copy-trading ingestion does not need to
audit every query in `app/services/execution_guard.py` to add a
`WHERE source != 'copy_trading'` clause. The type system does the work
for us.

## 5. Short positions

See §3.3 — short handling is now folded into the §3.2 formula via
the `sign * (quote − open_rate)` delta term, which is the correct
accounting quantity for a CFD/cash short. The demo-account raw dump
contains only `isBuy = true, leverage = 1` rows, so the short path
is covered by unit-test fixtures rather than live data. Track 2
adds a live-mirror verification step once a short-containing mirror
is observed on the demo account.

## 6. Where AUM is computed — all three call sites

Codex correctly flagged that §3's query change does not, on its own,
fix AUM everywhere the dashboard and review paths read it. There are
**three** AUM call sites in the current codebase, and each needs an
explicit update in this PR:

### 6.1 `app/services/execution_guard.py` (lines 245-289)

Primary site — the execution guard denominator. Loads positions and
cash, computes `total_aum`, then applies position-% and sector-% rules
as ratios. Update: add the `mirror_equity` CTE from §3.4 and sum it
into `total_aum`. Sector and per-position aggregates are NOT touched,
which is what §4 is about.

### 6.2 `app/api/portfolio.py` (`get_portfolio`, lines 111-175)

`GET /api/portfolio` is the public read endpoint backing the
dashboard top-line summary. It runs its own positions+cash queries
and computes `total_aum = total_market + (cash_balance or 0.0)` at
line 166. This is a separate code path — the query change in §3
does not touch it.

Update: after computing `total_market` and `cash_balance`, run the
mirror-equity query from §3.4 and add the result to `total_aum`.
The `PortfolioResponse` dataclass (lines 64-67) adds a new optional
field `mirror_equity: float | None = None` so the frontend can
display the breakdown (AUM = positions + cash + mirrors) rather
than just a lump total. A null value means no mirrors are held.

### 6.3 `app/services/portfolio.py` (`run_portfolio_review`, line 752-753)

`run_portfolio_review` computes AUM in Python from its own
`_load_positions` / `_load_cash` helpers:

```python
total_market_value = sum(p.market_value for p in positions.values())
total_aum = total_market_value + (cash if cash_known else 0.0)
```

then passes `total_aum` through to `_evaluate_add`, `_evaluate_buy`,
and `_sector_pct` for recommendation gating. This is the periodic
BUY/ADD/HOLD/EXIT pipeline, and it must see the same AUM the
execution guard sees — otherwise recommendations could be made
against a denominator that the guard will then reject against a
different denominator.

Update: add a new `_load_mirror_equity(conn) -> float` helper that
runs the §3.4 query, and sum its result into `total_aum` at line
753. Plumb `mirror_equity` through `PortfolioReviewResult` the same
way `total_aum` is already plumbed, so the snapshot row in
`portfolio_reviews` captures it (for audit).

### 6.4 New REST endpoint

`GET /api/portfolio/copy-trading` — list of copy traders with
mirror-level aggregates and a per-mirror summary of nested positions.
Joins `copy_traders` × `copy_mirrors` × `copy_mirror_positions` and
runs a per-mirror equity calculation for display. Detailed field
list deferred to the implementation plan.

### 6.5 Frontend

Frontend changes are in scope for the PR: a copy-trading section on
the portfolio dashboard that consumes `/api/portfolio/copy-trading`,
and a top-line AUM breakdown showing positions + cash + mirror
equity. Component structure deferred to the implementation plan —
the spec fixes the data contract, the plan fixes the components.

## 7. Migration (022)

Single migration, one transaction, following the
[sql/021_positions_source.sql](sql/021_positions_source.sql) format:

```sql
-- Migration 022: copy trading ingestion
BEGIN;

CREATE TABLE copy_traders (...);          -- §1.1
CREATE TABLE copy_mirrors (...);          -- §1.2
CREATE TABLE copy_mirror_positions (...); -- §1.3, composite PK (mirror_id, position_id)

CREATE INDEX copy_traders_username_idx               ON copy_traders (parent_username);
CREATE INDEX copy_mirrors_parent_cid_idx             ON copy_mirrors (parent_cid);
CREATE INDEX copy_mirror_positions_instrument_id_idx ON copy_mirror_positions (instrument_id);

COMMIT;
```

Note: there is no standalone `copy_mirror_positions_mirror_id_idx`
because the composite primary key `(mirror_id, position_id)` already
covers `WHERE mirror_id = ?` queries as a leftmost-prefix scan.

No backfill is required — the tables start empty and fill up on the
next portfolio sync. The existing `positions.source` CHECK constraint
remains `('ebull', 'broker_sync')` — we do NOT add a third value,
because mirrors never become `positions` rows.

## 8. Testing strategy

**Unit tests (pure, no DB):**

- `_parse_mirror` / `_parse_mirror_position` against fixtures derived
  from the real `data/raw/etoro_broker/etoro_portfolio_*.json` payload
  (trimmed to 2 mirrors × 3 nested positions each for readability, at
  least one non-USD position to exercise `openConversionRate`)
- Malformed mirror → skipped with warning, other mirrors still parsed
- Malformed nested position → skipped, sibling positions still parsed
- Missing optional fields (stop loss, take profit) → `None` on the
  dataclass
- Missing `openConversionRate` → defaults to `Decimal("1")` so legacy
  USD-only fixtures keep working

**Service-layer tests (real test DB, per
`feedback_test_db_isolation` rule — `ebull_test`, never
`settings.database_url`):**

- First `sync_portfolio` call with 2 mirrors × 3 positions → rows in
  `copy_traders`, `copy_mirrors`, `copy_mirror_positions`
- Second `sync_portfolio` with one nested position removed → that row
  is DELETEd, siblings untouched
- Re-running the same payload is idempotent (row counts unchanged,
  `updated_at` refreshed)
- Mirror-level metadata changed on second sync → `copy_mirrors` row
  updated, trader row untouched apart from `updated_at`
- Parent username changed on second sync → `copy_traders.parent_username`
  updated
- **Empty `mirrors` array with local mirrors present** → `RuntimeError`
  raised, caller can roll back
- **Partial mirror disappearance** (two local mirrors, payload has
  one) → `RuntimeError` raised naming the missing mirror_id, caller
  can roll back
- **Parser-failure safeguard** → payload with 10 raw positions, 8 of
  which are malformed such that the parser skips them → `_sync_mirrors`
  raises before eviction runs; existing rows survive the rollback

**AUM identity tests (the core correctness test for §3):**

Two small fixtures inserted directly into `copy_mirrors` and
`copy_mirror_positions`:

- **No-quote cost-basis fallback.** One mirror with
  `available_amount = 2800.33`, two positions with
  `amount = 50.00` and `amount = 17039.33`, no quotes → the §3.4
  query returns `2800.33 + 50.00 + 17039.33 = 19889.66`. This is
  the empirically-reconciled identity on mirror 15712187.
- **MTM delta with FX.** One mirror, one long position with
  `open_rate = 1207.4994`, `units = 6.28927`,
  `open_conversion_rate = 0.01331`, `amount = 101.08`, and a quote
  `quotes.last = 1400.0` → delta = `1 * 6.28927 * (1400.0 -
  1207.4994) * 0.01331 ≈ 16.12`, so mirror equity ≈
  `available + 101.08 + 16.12`. Asserts FX is applied to the delta.
- **Short delta.** Same fixture with `is_buy = false` and
  `quotes.last = 1000.0` (below entry) → delta = `-1 * 6.28927 *
  (1000.0 - 1207.4994) * 0.01331 ≈ +17.37`, equity goes **up**
  because a short is profitable when the price falls.

**Guard AUM integration test:**

- Existing guard test fixture + one mirror containing a single
  USD position with `amount = 1000`, `open_rate = 10`, no quote →
  `total_aum` increases by exactly `available_amount + 1000`
- Same fixture + a quote higher than `open_rate` → AUM delta grows
  by the MTM delta
- Sector exposure check on an instrument that is held in a mirror but
  not in `positions` → sector exposure numerator is 0 (mirror is
  ignored for concentration), AUM denominator is still increased
  (rule is more permissive, not less)

**Three-call-site AUM consistency test:**

A single DB fixture is observed through all three AUM paths in one
test to prove they agree:

1. Direct `execution_guard` call via `run_execution_guard`
2. `GET /api/portfolio` via `TestClient`
3. `run_portfolio_review`

All three must report the same `total_aum`. This is the regression
test for §6 — if a future PR updates one AUM path but not the
others, this test fails.

**Smoke gate:** `tests/smoke/test_app_boots.py` remains green (the
FastAPI lifespan touches nothing new; migration 022 runs during
dev-DB bootstrap).

## 9. Track 2 preview (not implemented in this PR)

A new GitHub issue will be opened after this spec merges, covering
the `user-info/people/*` surface we now know exists:

- `GET /api/v1/user-info/people/search` — discovery with filters for
  `popularInvestor`, `period`, `gainMax`,
  `maxDailyRiskScoreMin/Max`, `maxMonthlyRiskScoreMin/Max`,
  `weeksSinceRegistrationMin`, `countryId`, `instrumentId`,
  `instrumentPctMin/Max`
- `GET /api/v1/user-info/people/{username}/portfolio/live` — live
  portfolio of any trader by username, including positions +
  socialTrades
- `GET /api/v1/user-info/people/{username}/tradeinfo` — per-period
  stats: `gain`, `dailyGain`, `riskScore`, `copiers`, `copiersGain`
- `GET /api/v1/user-info/people/{username}/gain` — monthly + yearly
  historical gain series
- `GET /api/v1/user-info/people/{username}/daily-gain` — daily gain
  series over a date range
- `GET /api/v1/user-info/people` — batch profile lookup by usernames
  or CID list

Track 2's job is to turn that surface into:

1. A periodic discovery sweep that populates `copy_traders` rows for
   traders we are **not** currently copying (today they are only
   created when we see a mirror for them).
2. Historical performance snapshots we can join against `copy_traders`
   for confidence scoring.
3. Signal derivation — if a cohort of high-quality traders is
   accumulating instrument X over a window, tilt the ranking engine in
   favour of X.
4. Verification of the short-position AUM math on live mirrors.
5. Graceful mirror-closure semantics (`closed_at` column, disappear
   vs delete vs preserve-history policy).

Track 2 will introduce its own schema migration(s) for trader
history, daily-gain series, cohort signals, and whatever
discovery-side tables it needs. The Track 1 schema
(`copy_traders`, `copy_mirrors`, `copy_mirror_positions`) needs no
*structural* changes to support Track 2 — existing rows are not
rewritten — but it may grow columns or sibling tables. That's fine.
Migrations are cheap; designing forward by guessing columns Track 2
might need is not.

## 10. Open questions

None at spec-revision time. The main decisions — three-table split,
full granular position capture, `positions`/guard untouched, empty
mirrors → raise, partial disappearance → raise, cost-basis AUM with
optional MTM delta, FX via `openConversionRate` — have all been made
above. If a second round of spec review surfaces new questions they
will be appended here before the writing-plans handoff.

## Appendix A. Revision log

This spec went through a pre-implementation Codex review (see
`.claude/codex-spec-review.log` in the branch history). Findings and
resolutions:

- **A. AUM formula double-counts `closed_pnl`.** §3 rewritten around
  the `available + SUM(amount) + SUM(MTM delta)` identity, which is
  exactly equal to the reconciled funded-capital quantity.
- **B. `units * open_rate` is cross-currency nonsense.** New NOT NULL
  `open_conversion_rate` column (§1.3), used in every AUM term (§3.4).
- **C. Missing `deposit_summary` / `withdrawal_summary`.** Added as
  first-class NOT NULL columns on `copy_mirrors` (§1.2).
- **D. `instrument_id INTEGER` breaks repo convention.** Changed to
  `BIGINT` (§1.3), matching `sql/001_init.sql`.
- **E. `BrokerPortfolio.mirrors` claimed non-breaking but has no
  default.** Field defaults to `()` (§2.1); both existing call sites
  unchanged.
- **F. `position_id` uniqueness across mirrors unproven.** Primary key
  is now `(mirror_id, position_id)` composite (§1.3).
- **G. `raw_payload` only on mirror row is brittle.** Added
  `raw_payload JSONB NOT NULL` on `copy_mirror_positions` too (§1.3).
- **H. Concurrent sync races not addressed.** Single-writer invariant
  documented against `JobLock` + APScheduler (§2.3.1); no new lock
  added.
- **I. Parser-failure + eviction = silent data loss.** 50%-of-raw
  parse-success threshold guard added (§2.3.3).
- **J. Only `mirrors=[]` guarded; partial disappearance ignored.**
  Partial-disappearance guard added (§2.3.4).
- **K. Short handling as `-1 * units * price` wrong for CFDs.** §3.2
  uses `sign * delta * ocr`, correct for longs, shorts, and
  leverage-1; §3.3 explains.
- **L. "via the query change in §3" false for dashboard & review.**
  §6 split into three explicit AUM call sites with per-site updates.
- **M. `copy_traders` Track 2 column stubs are YAGNI overreach.**
  Stubs removed (§1.1); Track 2 will migrate its own columns/tables.
- **N. `raw_payload` can't backfill Track 2 profile data.** Claim
  removed (§1.2); `user-info/people/*` is now called out as the
  source.
