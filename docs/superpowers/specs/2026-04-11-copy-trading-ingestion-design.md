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
- Populating the Track-2-only columns on `copy_traders`
  (`risk_score`, `gain_1y_pct`, `copiers_count`, `is_popular_investor`,
  `weeks_since_registration`, `last_profile_refresh_at`)
- Trader discovery and watchlist
- Historical trader performance tracking
- Signal derivation (accumulation / divestment by a trusted cohort)
- Reconciliation on mirror disappearance (graceful closure semantics —
  v1 treats an empty `mirrors[]` with local rows as an API failure and
  raises, same as the existing positions guard)
- Short positions on mirrors beyond the "negative MV" math sketched in
  §5 — to be verified against live data in Track 2

## 1. Data model

Three new tables. `positions`, `cash_ledger`, and the existing
`positions.source` column are untouched.

### 1.1 `copy_traders`

```sql
CREATE TABLE copy_traders (
    parent_cid BIGINT PRIMARY KEY,
    parent_username TEXT NOT NULL,

    -- Track 2 columns. Nullable, not populated by the Track 1 sync path.
    -- A separate upsert path (added in the Track 2 PR) will write these
    -- from /api/v1/user-info/people/{username}/tradeinfo.
    risk_score INTEGER,
    gain_1y_pct NUMERIC(10, 4),
    copiers_count INTEGER,
    is_popular_investor BOOLEAN,
    weeks_since_registration INTEGER,
    last_profile_refresh_at TIMESTAMPTZ,

    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX copy_traders_username_idx ON copy_traders (parent_username);
```

Semantics: one row per eToro trader identity, keyed by `parentCID`.
Track 1 populates only `parent_cid`, `parent_username`, `first_seen_at`,
`updated_at`. The Track 2 profile columns are set aside now so the
Track 2 PR can populate them without a schema migration.

Upsert (Track 1): `ON CONFLICT (parent_cid) DO UPDATE SET
parent_username = EXCLUDED.parent_username, updated_at = NOW()`. We
honour the latest username eToro returns; Track 2's columns are NOT
touched by this upsert path.

### 1.2 `copy_mirrors`

```sql
CREATE TABLE copy_mirrors (
    mirror_id BIGINT PRIMARY KEY,
    parent_cid BIGINT NOT NULL REFERENCES copy_traders(parent_cid),

    initial_investment          NUMERIC(20, 4) NOT NULL,
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

`raw_payload` is the full mirror object from the broker as JSONB, for
auditability and so Track 2 can backfill Track-2-only columns without
re-fetching.

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
    position_id BIGINT PRIMARY KEY,
    mirror_id   BIGINT NOT NULL REFERENCES copy_mirrors(mirror_id) ON DELETE CASCADE,

    parent_position_id BIGINT NOT NULL,
    instrument_id      INTEGER NOT NULL,

    is_buy                    BOOLEAN NOT NULL,
    units                     NUMERIC(20, 8) NOT NULL,
    amount                    NUMERIC(20, 4) NOT NULL,
    initial_amount_in_dollars NUMERIC(20, 4) NOT NULL,
    open_rate                 NUMERIC(20, 6) NOT NULL,
    open_date_time            TIMESTAMPTZ NOT NULL,
    take_profit_rate          NUMERIC(20, 6),
    stop_loss_rate            NUMERIC(20, 6),
    total_fees                NUMERIC(20, 4) NOT NULL DEFAULT 0,
    leverage                  INTEGER NOT NULL DEFAULT 1,

    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX copy_mirror_positions_mirror_id_idx      ON copy_mirror_positions (mirror_id);
CREATE INDEX copy_mirror_positions_instrument_id_idx  ON copy_mirror_positions (instrument_id);
```

Semantics: one row per nested position currently held inside a mirror.

**No foreign key on `instrument_id`.** Copy traders trade a wider
universe than our synced `instruments` table (copy portfolios commonly
contain commodities, FX, crypto, and non-US equities we have not
onboarded). Enforcing the FK would force us to either reject these rows
or pre-sync the whole eToro universe. We accept unknown instrument IDs
as opaque identifiers and LEFT OUTER JOIN against `instruments` in
reads.

**`ON DELETE CASCADE` from `copy_mirrors`.** Track 1 never deletes
`copy_mirrors` rows on sync (see §2.3), so this is defence-in-depth —
if a future PR introduces mirror deletion we do not want orphaned
nested position rows.

## 2. Sync flow changes

### 2.1 Broker provider interface (`app/providers/broker.py`)

Two new frozen dataclasses and one non-breaking extension to
`BrokerPortfolio`:

```python
@dataclass(frozen=True)
class BrokerMirrorPosition:
    position_id: int
    parent_position_id: int
    instrument_id: int
    is_buy: bool
    units: Decimal
    amount: Decimal
    initial_amount_in_dollars: Decimal
    open_rate: Decimal
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
    mirrors: Sequence[BrokerMirror]          # NEW
    raw_payload: dict[str, Any]
```

Adding a field to the existing `BrokerPortfolio` is non-breaking:
`positions` and `available_cash` continue to work, callers that do not
care about mirrors simply ignore the new attribute. The alternative —
an additional method `get_mirrors()` on the interface — was rejected
because mirrors arrive in the same payload as positions and splitting
them would double the HTTP call count per sync.

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

Malformed nested positions inside a mirror are skipped, not failed —
the mirror as a whole is still ingested. A mirror with zero valid
nested positions is ingested with `positions=[]` (still useful for AUM
via `available_amount + closed_positions_net_profit`).

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

Per mirror:

1. **Upsert `copy_traders`** by `parent_cid`. Track 2 columns are not
   touched.
2. **Upsert `copy_mirrors`** by `mirror_id`. All mirror-level columns
   including `raw_payload` are refreshed.
3. **Replace nested positions for this mirror.** The sync is the sole
   writer of `copy_mirror_positions`, so each sync is an authoritative
   snapshot. Inside the transaction we:

   a. Upsert every position in the payload by `position_id` primary
      key.
   b. Evict positions that have closed since the last sync. When the
      new payload has ≥1 position, this is `DELETE FROM
      copy_mirror_positions WHERE mirror_id = %s AND position_id <>
      ALL(%s::bigint[])` passing the new IDs as a single array
      parameter (avoids SQL-injecting a variadic `NOT IN (...)` list
      and sidesteps the empty-list parser error). When the new
      payload has 0 positions (rare but valid — an empty mirror), the
      same query with an empty array deletes everything for that
      mirror, which is exactly what we want.

   This is a per-mirror full-replace, but implemented as upsert + evict
   rather than delete-all + insert-all so that row locks are held for
   the shortest possible time and an empty payload for a mirror does
   not briefly zero the table.

**Empty-mirrors guard.** Mirroring the existing positions guard at
[portfolio_sync.py:245](app/services/portfolio_sync.py#L245): if
`portfolio.mirrors` is empty but the local `copy_mirrors` table has
rows, `_sync_mirrors` raises a `RuntimeError`. This preserves the
"upstream looks broken — do not silently zero local state" invariant.
Operator-driven un-copying is a manual operation in v1 (delete the row
from `copy_mirrors`); graceful closure semantics are deferred to
Track 2.

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

We extend this with a third term:

```text
mirror_equity = SUM over copy_mirrors (
    available_amount
  + closed_positions_net_profit
  + SUM over copy_mirror_positions in this mirror (
        sign * units * COALESCE(latest_quote, open_rate)
    )
)
total_aum = total_positions + cash + mirror_equity
```

where `sign = +1 if is_buy else -1`. This is the standard accounting
identity for a broker sub-account: uninvested cash + live MV of open
positions + realized P/L from closures.

The MTM-via-quote pattern matches the existing guard query at
[execution_guard.py:255](app/services/execution_guard.py#L255): latest
`quotes.last` for the instrument, falling back to the position's
entry price when no quote is available (same conservatism we already
apply to our own positions).

The query is implemented as a single CTE / lateral join added to the
guard's existing portfolio-read block. Sketch:

```sql
WITH mirror_equity AS (
    SELECT COALESCE(SUM(
        m.available_amount + m.closed_positions_net_profit + COALESCE(p.mv, 0)
    ), 0) AS total
    FROM copy_mirrors m
    LEFT JOIN LATERAL (
        SELECT SUM(
            CASE WHEN cmp.is_buy THEN 1 ELSE -1 END
              * cmp.units
              * COALESCE(q.last, cmp.open_rate)
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

The demo-account raw dump
(`data/raw/etoro_broker/etoro_portfolio_20260411T053000Z.json`) contains
only `isBuy = true` nested positions. The AUM query in §3 treats a
short (`is_buy = false`) as `-1 * units * price`, which is the standard
short valuation. We stand by this math but have not yet tested it
against a live short in a mirror. Track 2 includes a verification step
against a short-containing mirror once one is observed.

## 6. Dashboard surface

New REST endpoint, matching the shape of existing portfolio endpoints
in `app/api/portfolio.py`:

```http
GET /api/portfolio/copy-trading
```

Response: list of copy traders with mirror-level aggregates and a
summary of nested positions. Detailed field list deferred to the
implementation plan.

Existing `GET /api/portfolio` is updated so that the AUM total shown
in the top-line summary includes `mirror_equity` (via the query
change in §3).

Frontend changes are out of scope for this spec's text but in scope
for the PR. The implementation plan will sketch the component
structure.

## 7. Migration (022)

Single migration, one transaction:

```sql
-- Migration 022: copy trading ingestion
BEGIN;

CREATE TABLE copy_traders (...);
CREATE TABLE copy_mirrors (...);
CREATE TABLE copy_mirror_positions (...);

CREATE INDEX copy_traders_username_idx            ON copy_traders (parent_username);
CREATE INDEX copy_mirrors_parent_cid_idx          ON copy_mirrors (parent_cid);
CREATE INDEX copy_mirror_positions_mirror_id_idx      ON copy_mirror_positions (mirror_id);
CREATE INDEX copy_mirror_positions_instrument_id_idx  ON copy_mirror_positions (instrument_id);

COMMIT;
```

No backfill is required — the tables start empty and fill up on the
next portfolio sync. The existing `positions.source` CHECK constraint
remains `('ebull', 'broker_sync')` — we do NOT add a third value,
because mirrors never become `positions` rows.

## 8. Testing strategy

**Unit tests (pure, no DB):**

- `_parse_mirror` / `_parse_mirror_position` against fixtures derived
  from the real `data/raw/etoro_broker/etoro_portfolio_*.json` payload
  (trimmed to 2 mirrors × 3 nested positions each for readability)
- Malformed mirror → skipped with warning, other mirrors still parsed
- Malformed nested position → skipped, sibling positions still parsed
- Missing optional fields (stop loss, take profit) → `None` on the
  dataclass

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
- Empty `mirrors` array with local mirrors present → `RuntimeError`
  raised, transaction can be rolled back by caller

**Guard AUM test:**

- Existing guard test fixture + one mirror containing a single
  nested position with known entry rate, no quote → AUM includes
  `available_amount + closed_positions_net_profit + units * open_rate`
- Same fixture + a quote in `quotes` → AUM uses `quotes.last` not
  `open_rate`
- Sector exposure check on an instrument that is held in a mirror but
  not in `positions` → sector exposure is 0 (mirror is ignored for
  concentration), AUM denominator is still increased (so the rule is
  more permissive, not less)

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

None of Track 2 requires a migration of the tables defined in this
spec. The nullable Track 2 columns on `copy_traders` are a placeholder
precisely so that no existing row in the table has to be rewritten
when the discovery path lands.

## 10. Open questions

None at spec-draft time. The main decisions — three-table split,
full granular position capture, `positions`/guard untouched, empty
mirrors → raise, shorts as `-1 * units * price` — have all been made
above. If spec review surfaces new questions they will be appended
here before the writing-plans handoff.
