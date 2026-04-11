# Copy trading (mirrors) ingestion

**Issue**: #183
**Related**:

- Track 1.5 — REST endpoint + frontend panel (Appendix B)
- Track 2 — social discovery as a research signal (new ticket,
  opened when this spec merges)

**Date**: 2026-04-11
**Status**: Draft for review (round 2)

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
  call it already makes, with soft-close semantics for partially
  disappearing mirrors (§2.3.4)
- AUM correction at all three call sites:
  `execution_guard` (§6.1), `api/portfolio.get_portfolio` (§6.2),
  `services/portfolio.run_portfolio_review` (§6.3). Each site adds
  `mirror_equity` to `total_aum`; each site gets a regression test in §8
- Execution guard decision logic is unchanged — mirrors live in separate
  tables and never appear in sector-exposure or position-size queries

**Out of scope (deferred):**

- New dashboard read endpoint `GET /api/portfolio/copy-trading` and
  the frontend copy-trading panel. These move to a **Track 1.5**
  follow-up PR (see Appendix B for ticket outline). Rationale: the
  AUM correction is the load-bearing change; shipping it alone gets
  the numbers right in the guard, the dashboard top-line, and the
  recommender without also having to ship a new REST surface, a new
  React component, and the UX copy for un-copying. `PortfolioResponse`
  does grow a `mirror_equity: float = 0.0` field in this PR so the
  existing dashboard top-line can show the breakdown (see §6.4 for
  why `0.0` over `None`).

Deferred to **Track 2** (new ticket opened when this spec merges):

- `/user-info/people/*` endpoint integration (profile, gain series,
  trade info, live portfolio of arbitrary users, people/search)
- Trader discovery and watchlist
- Historical trader performance tracking (monthly/daily gain series)
- Current profile snapshot columns/tables (risk score, copiers count,
  popular-investor flag, weeks since registration, etc.) — Track 2
  decides at the time whether these belong on `copy_traders` or on a
  separate `trader_profile_snapshots` table
- Signal derivation (accumulation / divestment by a trusted cohort)
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

    active      BOOLEAN       NOT NULL DEFAULT TRUE,
    closed_at   TIMESTAMPTZ   NULL,

    raw_payload JSONB NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX copy_mirrors_parent_cid_idx ON copy_mirrors (parent_cid);
CREATE INDEX copy_mirrors_active_idx     ON copy_mirrors (active) WHERE active;
```

Semantics: one row per mirror (one per copy session with a trader).
`parent_cid` is a FK to `copy_traders` so the trader identity is a
stable spine regardless of whether the copy is active, paused, or
restarted with a new `mirrorID`.

**`active` / `closed_at` soft-close columns.** Mirrors are
externally-driven state — the operator un-copies through the eToro
UI, not through eBull — and the only signal eBull gets is "row
disappears from the next `/portfolio` payload." Deleting the local
row on disappearance loses history; raising on disappearance turns
every normal un-copy into a failed sync and a manual `DELETE FROM`.
Soft-close splits the difference: a mirror that disappears from the
payload is marked `active=false, closed_at=<injected sync
timestamp>`, nested positions are retained on the closed row for
audit, AUM queries filter on `active=true`, and re-copying the same
`mirror_id` (rare but possible if eToro recycles IDs) flips `active`
back to true in the upsert path. The partial
`copy_mirrors_active_idx` is populated only by the small set of
live rows, so the AUM denominator filter stays cheap as closed
mirrors accumulate over time. (§2.3.4 specifies the SQL binding for
the injected timestamp; this prose intentionally avoids writing
`NOW()` so a reader does not mis-implement it as DB wall clock.)

**`active` is synthetic, not sourced from the payload.** The
mirror JSON does not contain an `active` field — it's an
eBull-local column derived from "is this mirror_id present in the
latest sync." The upsert sets `active=TRUE, closed_at=NULL` on
every row in the payload; §2.3.4 sets `active=FALSE, closed_at=<injected
sync timestamp>` on local rows absent from the payload (see §2.3.4 for
the `%(now)s` binding — this prose deliberately mirrors the soft-close
paragraph above).

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

Upsert: `ON CONFLICT (mirror_id) DO UPDATE SET ..., active = TRUE,
closed_at = NULL, updated_at = NOW()`. All payload-sourced columns
are refreshed from the latest payload; `active`/`closed_at` are
reset to the "live" state because presence in the payload means
the mirror is live.

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
`copy_mirrors` rows on sync (disappeared mirrors are soft-closed
via `active` / `closed_at`, not DELETEd — see §2.3.4), so this is
defence-in-depth: if a future PR ever introduces hard mirror
deletion (e.g. an operator "purge closed mirrors older than N
years" job), we do not want orphaned nested position rows.

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
    open_rate: Decimal                   # entry price, native ccy
    open_conversion_rate: Decimal        # FX at open (native -> USD)
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
    mirrors: Sequence[BrokerMirror] = ()   # NEW — default preserves callers
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

#### 2.2.1 `PortfolioParseError` exception type

A new exception is declared in
`app/providers/implementations/etoro_broker.py`:

```python
class PortfolioParseError(Exception):
    """Raised when a mirrors[] row cannot be parsed safely.

    Directly subclasses Exception (NOT ValueError / TypeError /
    KeyError) so the outer parse loop can distinguish it from
    incidental exceptions and re-raise. Never swallowed by any
    `except (KeyError, ValueError, TypeError)` block.
    """
```

**Hierarchy rationale (Codex v3 finding U).** `PortfolioParseError`
must be a direct subclass of `Exception`. If it subclassed
`ValueError` (which `_parse_mirror_position` naturally raises for
numeric conversion errors) the outer loop's `except (KeyError,
ValueError, TypeError)` clause would silently swallow it, defeating
§2.3.3's strict-raise. The outer loop catches `PortfolioParseError`
*first* and re-raises, then falls through to the incidental
`(KeyError, ValueError, TypeError)` catch for the narrow "payload
shape is unrecognisable" case below.

Module path used throughout the spec and tests:
`app.providers.implementations.etoro_broker.PortfolioParseError`.

#### 2.2.2 Strict-raise parse contract

Codex v3 (finding V) flagged that a log-and-skip on top-level mirror
rows interacts badly with the §2.3.4 soft-close: a known mirror whose
top-level fields parse-fail would be log-skipped, then interpreted as
"disappeared from the payload", then silently soft-closed and dropped
from AUM. That is the exact data-loss failure mode §2.3.3 already
rejected for nested positions.

**v1 rule — strict raise on any row that carries a recognisable
`mirrorID`.** The top-level parse loop is:

```python
for m in raw_mirrors:
    if not isinstance(m, dict) or "mirrorID" not in m:
        # Unrecognisable shape with no usable identifier. This
        # cannot collide with a known local mirror row, so skip
        # safely. In practice this branch should never fire in
        # production — if it does, the payload schema has broken.
        logger.warning(
            "Skipping unrecognisable mirrors[] element: %r", m
        )
        continue

    try:
        mirrors.append(_parse_mirror(m))
    except PortfolioParseError:
        # Nested-position failure (raised from _parse_mirror with
        # mirror_id + position index context), or top-level
        # known-mirror failure that _parse_mirror already wrapped.
        # Re-raise unchanged — §2.3.3 requires full sync rollback,
        # and §2.3.4 soft-close must not silently fire on a
        # mirrorID that we still see in the payload.
        raise
    except (
        KeyError,
        ValueError,
        TypeError,
        decimal.DecimalException,
    ) as exc:
        # Fallback wrap for exceptions that leaked past
        # _parse_mirror's own try/except. `decimal.DecimalException`
        # is the parent of `decimal.InvalidOperation`, which
        # `Decimal(str(value))` raises on non-numeric input — it is
        # NOT a ValueError, so it must be caught explicitly or the
        # outer loop would miss it. Attribution falls to the
        # top-level mirror (no position index) since this branch
        # only fires if the wrapping inside _parse_mirror missed it.
        raise PortfolioParseError(
            f"Failed to parse mirror {m.get('mirrorID')!r}: {exc}"
        ) from exc
```

**Nested-position parse — wrap at call site.** `_parse_mirror`
iterates over `raw_positions` with its own inner try/except:

```python
def _parse_mirror(m: dict[str, Any]) -> BrokerMirror:
    # ... required top-level field extraction with Decimal(str(...))
    # ... top-level numeric/string conversions may raise
    raw_positions = m.get("positions") or []
    parsed_positions: list[BrokerMirrorPosition] = []
    for idx, pos in enumerate(raw_positions):
        try:
            parsed_positions.append(_parse_mirror_position(pos))
        except (
            KeyError,
            ValueError,
            TypeError,
            decimal.DecimalException,
        ) as exc:
            raise PortfolioParseError(
                f"Mirror {m.get('mirrorID')!r} "
                f"position[{idx}]: {exc}"
            ) from exc
    return BrokerMirror(...)
```

This guarantees that *every* nested-position parse failure carries
`mirror_id + position index` in the error message, and that the
`PortfolioParseError` is raised *from within* `_parse_mirror` so
the outer loop's `except PortfolioParseError: raise` catches and
re-raises it without the attribution degrading to a top-level
message. §2.3.3 uses this as the reason the sync transaction rolls
back before eviction or soft-close touches the DB.

**Decimal conversion is the common hazard.** Every payload numeric
field goes through `Decimal(str(value))`. A non-numeric value (e.g.
`units: "bogus"`) raises `decimal.InvalidOperation`, a subclass of
`decimal.DecimalException`, **not** a `ValueError`. Both the inner
`_parse_mirror` wrap and the outer top-level wrap list
`decimal.DecimalException` explicitly. Test `8.1` asserts the
hierarchy: `isinstance(decimal.InvalidOperation(),
decimal.DecimalException) is True` and no path silently loses a
malformed decimal.

**Why no log-and-skip path survives for rows with a `mirrorID`.**
The only safe skip is a row the parser cannot match back to a local
`copy_mirrors` row — i.e. a row with no `mirrorID` to compare
against. Any row that *could* match a known local row but fails to
parse must raise, otherwise the combination of "silent skip" +
"soft-close absent mirrors" = silent data loss. The `mirrorID
not in m` branch above is the only surviving skip path.

Rationale:

- One bad nested position out of 198 is indistinguishable from a
  parser that has drifted against a payload schema change. Silently
  skipping means "delete the valid local row on next eviction and
  pretend nothing happened"; raising means "page the operator, fix
  the parser, re-run the sync."
- One bad top-level field on a known `mirrorID` is exactly the same
  failure mode — it looks like a disappearance to §2.3.4, and
  §2.3.4 would soft-close the row. Same correctness outcome, same
  response: raise and stop.

`_parse_mirror` / `_parse_mirror_position` are pure normaliser
functions alongside `_normalise_open_order_response` (line 519) — no
I/O, no DB access, no dependence on instance state. They validate
required fields (`mirrorID`, `parentCID`, `parentUsername`,
`initialInvestment`, `availableAmount`, `closedPositionsNetProfit`,
`startedCopyDate`, and `openConversionRate` on every nested
position), parse optional fields with defaults for genuinely
optional payload fields only (stop loss, take profit), and never
default a value that would silently corrupt downstream arithmetic.

`openConversionRate` is a required field on `_parse_mirror_position`
and has **no production default**. A mirror position whose payload
omits it raises at parse time and triggers the strict-raise path
above. Test helpers in `tests/test_portfolio_sync.py` retain the
convenience of a `Decimal("1")` default for USD-only fixtures; this
is scoped to the helper, not the parser.

A mirror with a genuinely-empty payload (`raw_positions == []`) is
ingested with `positions=[]` — that is a valid state (the mirror
holds only `available_amount` cash) and the §3.2 formula handles it
correctly as `mirror_equity = available_amount`.

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

`_sync_mirrors` relies on the same serialisation guarantee as every
other portfolio-sync writer in the codebase. The guarantee is
provided by `JobRuntime`, not by `daily_portfolio_sync` itself:

1. **`JobRuntime._wrap_invoker`** takes a session-scoped Postgres
   advisory lock via `JobLock`
   ([app/jobs/locks.py:60](app/jobs/locks.py#L60)) before dispatching
   the wrapped job function, releases it after return. This wraps
   every scheduled fire.
2. **`JobRuntime._run_manual`** takes the same lock around manual
   triggers (operator-invoked runs, catch-up runs).
3. **APScheduler `max_instances=1`** default
   ([app/jobs/runtime.py:213](app/jobs/runtime.py#L213)) enforces
   one concurrent instance per job at the scheduler layer on top of
   the DB lock.

Practical consequence: any code path that reaches `sync_portfolio`
**via `JobRuntime`** is serialised at both the process (APScheduler)
and cross-process (Postgres advisory lock) layers, so two instances
of `_sync_mirrors` cannot run concurrently. This is the only app
code path that invokes `sync_portfolio` today
([app/workers/scheduler.py:868](app/workers/scheduler.py#L868)).

**What the invariant does NOT cover.** A direct call to
`sync_portfolio(conn, portfolio)` outside of `JobRuntime` — an
ad-hoc REPL session, a future unlocked REST endpoint, a new CLI
command — bypasses both layers. Tests already do this
([tests/test_portfolio_sync.py](tests/test_portfolio_sync.py))
but run serially against an isolated test DB, so they cannot race.
Any *future* production caller that peels `sync_portfolio` off
`JobRuntime` must add its own advisory lock at the call site. This
spec documents the assumption so that author knows what they're
removing.

`_sync_mirrors` itself does not take an additional advisory lock;
stacking locks inside an already-serialised job adds reasoning
overhead with no correctness benefit.

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

The failure mode the guard protects against: eToro returns 198
nested positions, a payload schema change makes the parser reject
one or more of them, the sync upserts the parsed subset and evicts
everything else — silently destroying local rows for the rejected
positions. Codex v2 correctly flagged that a ratio-based guard
(50%, 80%, …) is indefensible here: it trips pathologically at
small N (a single mirror with one position: N=1, one failure is
100% failure *and* 0% failure depending on how you count), and it
is too lax at large N (on a 198-position mirror, an 80% threshold
still allows ~40 rows to be silently deleted).

**v1 rule — strict raise, zero budget.** If `_parse_mirror_position`
raises on *any* nested position inside a mirror, the mirror parse
raises `PortfolioParseError` with the mirror_id, position index, and
cause. `_sync_mirrors` catches nothing and lets the exception
propagate up through `sync_portfolio` to the caller. The caller
(scheduler job, test) rolls back the transaction. Nothing is
upserted, nothing is evicted, local state is preserved exactly as
it was before the sync started.

The operator sees a failed `daily_portfolio_sync` in `job_runs`
with the exception message, investigates the upstream schema
change, extends the parser, and re-runs the sync manually. Next
scheduled fire picks up the fix.

Rejected alternatives:

- **"Log and skip failed rows, track separately, delete only known
  IDs."** Plausible but adds three moving parts (a parse-failure
  log, a two-set eviction array, an "already-evicted-but-stale"
  reconciliation step) to save the operator a manual `gh workflow
  run`. Not worth it for v1.
- **"Allow an absolute failure budget (e.g. ≤2)."** Still bad at
  small N and still silently evicts the budgeted-off rows. A
  budget is "how many rows am I allowed to silently delete", which
  is the wrong shape of question.
- **"50% ratio threshold."** Codex v2 correctly rejected this —
  indefensible at any N, pathological at small N.

The tradeoff is accepted: a single bad row blocks the entire sync.
The fix path (extend the parser) is always cheap, always local, and
always reversible. The alternative (silent data loss) is not.

#### 2.3.4 Disappearance handling — soft-close

The failure mode: the operator un-copies a trader through the eToro
UI. On the very next `/portfolio` fetch, the mirror is gone from
`clientPortfolio.mirrors[]`. eBull has to decide what to do about
the local `copy_mirrors` row.

Codex v2 rejected the earlier "raise on any disappearance" design
because it turns every normal un-copy into a failed sync and a
manual `DELETE FROM copy_mirrors WHERE mirror_id = ?;` step.
Un-copying is a normal operator workflow, not an emergency, and
eBull should handle it without pages.

**v1 rule — partial disappearance soft-closes, total disappearance
raises.**

1. **After upserting every mirror in the payload**, compute
   `disappeared_ids = active_local_mirror_ids − payload_mirror_ids`.
2. **Total disappearance (operator intent unclear).** If
   `payload_mirror_ids` is empty AND
   `active_local_mirror_ids` is non-empty, raise `RuntimeError`.
   This is the same invariant as the positions guard at
   [portfolio_sync.py:245](app/services/portfolio_sync.py#L245):
   "all records gone at once" is indistinguishable from an API
   regression, so we stop and page. Operator investigates, then
   either manually closes the mirrors or waits for the API to
   recover.
3. **Partial disappearance (operator un-copy).** If
   `payload_mirror_ids` is non-empty AND `disappeared_ids` is
   non-empty, run a soft-close using the `now` parameter threaded
   into `_sync_mirrors` (same value used by the rest of the sync
   transaction for `updated_at` timestamps — tests can freeze it):

   ```sql
   UPDATE copy_mirrors
      SET active = FALSE,
          closed_at = %(now)s,
          updated_at = %(now)s
    WHERE mirror_id = ANY(%(disappeared_ids)s::bigint[])
      AND active = TRUE;
   ```

   Parameters are bound with `psycopg.sql.SQL` + a dict payload —
   never interpolated. Using the injected `now` (not DB `NOW()`)
   is what makes the disappearance tests below deterministic
   against a frozen timestamp.

   Nested `copy_mirror_positions` rows for the closed mirror are
   **retained** — they are historical fact, and the `active` filter
   in the §3.4 AUM query already excludes them from forward-looking
   calculations. A follow-up eviction step is explicitly not run.

   A single-line `INFO` log per closed mirror
   (`"mirror %s (%s) disappeared from payload — marked closed"`)
   gives the operator audit trail without paging.

4. **Re-copy.** If the operator later re-copies the same trader and
   eToro issues a new `mirror_id`, the new mirror hits the insert
   branch of the upsert, and the old closed mirror row remains for
   history. If eToro reuses the `mirror_id` (never observed in the
   wild, but possible), the upsert's `ON CONFLICT` clause resets
   `active = TRUE, closed_at = NULL` from §1.2, and the mirror is
   live again.

**Why total disappearance still raises.** The decision is
asymmetric by design:

- Partial disappearance: 1 of 2 mirrors gone, 1 still live. We
  have strong evidence the rest of the payload is healthy (the
  other mirror upserted normally, positions and cash reconciled
  normally). The disappeared one is almost certainly an un-copy.
- Total disappearance: 2 of 2 mirrors gone, zero live. We have
  no evidence the payload is healthy for *mirrors* specifically
  — maybe eToro stopped returning the `mirrors[]` field entirely,
  maybe the JSON schema changed, maybe the account really was
  bulk-un-copied. Raising routes this decision to a human.

This matches the existing positions guard, which also distinguishes
"individual position closed" (fine, normal) from "entire positions
array empty" (raise, unsafe).

**Operator runbook for total disappearance.** If the raise turns
out to be a genuine "un-copied everything" — not an API bug —
manually soft-close every row:

```sql
UPDATE copy_mirrors
   SET active = FALSE, closed_at = NOW(), updated_at = NOW()
 WHERE active = TRUE;
```

Then re-run the sync. One-time cost for what should be a very rare
event.

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
    mirrors_closed: int            # NEW — soft-closed on this sync
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
     available + SUM(amount)   = 2800.33 + 17089.33        = 19889.66
     init + dep − wd + closed  = 20000 + 0 − 0 + (−110.34) = 19889.66

   mirror 15714660 (triangulacapital):
     available + SUM(amount)   = 1724.11 + 17666.76        = 19390.87
     init + dep − wd + closed  = 17280 + 2251 − 0 + (−140.13) = 19390.87
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
    WHERE m.active
)
SELECT total FROM mirror_equity;
```

The `WHERE m.active` filter is what makes the soft-close design
from §2.3.4 work end-to-end: closed mirrors and their nested
positions stay in the DB for audit and history, but they contribute
zero to the AUM denominator the guard, the dashboard, and the
recommender compute against. The partial index
`copy_mirrors_active_idx (...) WHERE active` from §1.2 keeps this
filter scanning only live rows as closed mirrors accumulate.

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

### 6.0 Shared helper — `_load_mirror_equity(conn)`

All three call sites run the exact same §3.4 mirror-equity SQL, so
the query is defined once as a module-level helper and imported
by each call site. Codex v4 (finding AA) flagged that the earlier
revision implicitly put this helper in `app/services/portfolio.py`
(where `_load_cash` / `_load_positions` already live) but then
called it like a shared helper from two other call sites, without
naming the module. That ambiguity is closed now.

**Location:**
[app/services/portfolio.py](app/services/portfolio.py), as a
module-level private function alongside the existing `_load_cash`
([portfolio.py:114](app/services/portfolio.py#L114)) and
`_load_positions` ([portfolio.py:129](app/services/portfolio.py#L129))
helpers. No new file. The existing read-helper pattern in that
module is the natural home, and there is no circular import risk
(neither `execution_guard` nor `api/portfolio.py` is currently
imported by `services/portfolio.py`).

**Signature:**

```python
def _load_mirror_equity(conn: psycopg.Connection[Any]) -> float:
    """Return the summed mirror_equity across all active mirrors.

    Runs the §3.4 mirror-equity SQL under the existing connection
    and returns a float. The value is `0.0` when `copy_mirrors` is
    empty or every row is `active = FALSE` — §3.4's
    `COALESCE(SUM(...), 0)` turns an empty result set into `0.0`,
    never `NULL`, which is why this function's return type is
    `float` and not `float | None` (see §6.4 for the contract
    rationale).

    The value is usually non-negative but is NOT mathematically
    floored at zero: if a mirror's MTM delta on a leveraged
    position exceeds `available + SUM(amount)`, the per-mirror
    term can go negative, and the aggregate can too. Callers treat
    it as an additive AUM contribution and sum it directly into
    `total_aum`; they do not assume it is non-negative.
    """
```

**Importers:** `app/services/execution_guard.py` (§6.1),
`app/api/portfolio.py` (§6.2), and the rest of
`app/services/portfolio.py` itself (§6.3, `run_portfolio_review`).
All three call it the same way:
`mirror_equity = _load_mirror_equity(conn)` inside the existing
connection scope, and add the float to their respective
`total_aum` running totals. No call site re-implements the SQL.

The §8.4 identity tests, the §8.5 guard integration test, and the
§8.6 three-call-site consistency test all exercise this exact
helper — §8.4's "empty `copy_mirrors` → `0.0`" regression test is
written as `_load_mirror_equity(conn)`, not as an inline query.

### 6.1 `app/services/execution_guard.py` — `_load_sector_exposure`

Exact site: the private helper
[`_load_sector_exposure`](app/services/execution_guard.py#L235)
(lines 229-289), which currently computes `total_aum =
total_positions + cash` at
[execution_guard.py:286](app/services/execution_guard.py#L286).
This function is called from `evaluate_recommendation`
([execution_guard.py:593](app/services/execution_guard.py#L593))
and returns the `total_aum` consumed by the concentration rule.
The public `evaluate_recommendation` entry point is **not**
modified — the change is surgical, inside the helper.

Update: `_load_sector_exposure` imports `_load_mirror_equity`
from §6.0 and sums its return value into `total_aum` **after**
the existing `total_positions + cash` line. Sector numerator
(`sector_values[sector]`) is NOT touched, which is what §4 is
about — mirrors inflate the denominator only. `GuardResult` is
not modified: AUM remains a local variable inside
`evaluate_recommendation`, as it is today. §8.5 tests this
function directly.

### 6.2 `app/api/portfolio.py` (`get_portfolio`, lines 111-175)

`GET /api/portfolio` is the public read endpoint backing the
dashboard top-line summary. It runs its own positions+cash queries
and computes `total_aum = total_market + (cash_balance or 0.0)` at
line 166. This is a separate code path — the query change in §3
does not touch it.

Update: after computing `total_market` and `cash_balance`, import
and call `_load_mirror_equity` from §6.0 and add the result to
`total_aum`. The `PortfolioResponse` pydantic `BaseModel`
([api/portfolio.py:63-67](app/api/portfolio.py#L63-L67)) grows one
new required-with-default field `mirror_equity: float = 0.0` so the
frontend can display the breakdown (AUM = positions + cash +
mirrors) rather than just a lump total. The field is always a
number: `0.0` when no mirrors are held or all mirrors are
`active = FALSE`, matching the §3.4 query's `COALESCE(SUM(...), 0)`
default. See §6.4 for why `0.0` beats `None` here.

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

Update: add `mirror_equity = _load_mirror_equity(conn)` at line
752 (the helper is now a sibling in the same module, per §6.0)
and sum it into `total_aum` at line 753.

**No audit persistence.** The earlier revision of this spec claimed
`run_portfolio_review` would capture `mirror_equity` into a
`portfolio_reviews` table for audit. Codex v2 correctly pointed out
that no such table and no such snapshot write path exist in the
repo — `run_portfolio_review` computes AUM, uses it, logs it,
writes `recommendations` rows, and never persists the AUM figure
itself. The earlier claim invented audit persistence that does not
exist. Correction: this PR adds nothing to
`run_portfolio_review` beyond the `_load_mirror_equity` helper and
its contribution to `total_aum`. If snapshot persistence is wanted
later, it lives on a separate ticket and a separate migration.

### 6.4 REST endpoint — deferred to Track 1.5

`GET /api/portfolio/copy-trading` (listing copy traders with
per-mirror aggregates and nested-position summaries) is deferred to
a follow-up PR. The existing `PortfolioResponse` pydantic
`BaseModel` at
[api/portfolio.py:63-67](app/api/portfolio.py#L63-L67) grows one
new field in this PR:

```python
class PortfolioResponse(BaseModel):
    positions: list[PositionItem]
    position_count: int
    total_aum: float
    cash_balance: float | None
    mirror_equity: float = 0.0  # NEW — 0.0 when no active mirrors
```

**Why `float = 0.0` not `float | None = None`** (Codex v3
finding W). Two call-site contracts were ambiguous:

- §3.4's AUM query is wrapped in `COALESCE(SUM(...), 0)`, so it
  always returns a number — `0` when the table is empty or every
  mirror is `active = FALSE`.
- `cash_balance` is `float | None` because "the cash_ledger is
  empty" is a genuinely unknown state that the dashboard should
  render as "—" rather than "£0.00". That reasoning does **not**
  apply to `mirror_equity`: if no mirrors exist, mirror equity is
  a *computed* zero, not an unknown. The dashboard should render
  "£0.00" confidently.

Aligning `mirror_equity` with its query means the frontend never
has to branch on `null` and can always do
`total_aum = positions + cash + mirror_equity` unconditionally.
`cash_balance` keeps the `| None` because its underlying domain is
genuinely "known unknown vs known zero"; `mirror_equity` does not.

That is the minimum change needed for the dashboard top-line to
display the AUM breakdown without a new endpoint. Anything
dedicated to copy-trader browsing lives in Track 1.5.

### 6.5 Frontend copy-trading panel — deferred to Track 1.5

No frontend changes in this PR beyond (possibly) reading the new
`mirror_equity` field from `PortfolioResponse` if the top-line
summary already has the hooks to render a "positions + cash +
mirrors = AUM" breakdown. The full copy-trading panel — trader
list, per-mirror cards, nested-position drill-down, un-copy UX —
lives in Track 1.5 (see Appendix B).

Rationale for the split: the load-bearing change in this PR is
"AUM is correct everywhere it is computed." That change requires
the schema, the sync, the parser, the three AUM integrations, and
every supporting test. Bundling the REST endpoint + React panel +
UX copy on top would roughly double the diff surface for the same
correctness outcome. Shipping AUM correctness first and the UI
second is both smaller and safer.

## 7. Migration (022)

Single migration, one transaction, following the
[sql/021_positions_source.sql](sql/021_positions_source.sql) format:

```sql
-- Migration 022: copy trading ingestion
BEGIN;

CREATE TABLE copy_traders (...);          -- §1.1
CREATE TABLE copy_mirrors (...);          -- §1.2, with active/closed_at
CREATE TABLE copy_mirror_positions (...); -- §1.3, composite PK (mirror_id, position_id)

CREATE INDEX copy_traders_username_idx
    ON copy_traders (parent_username);
CREATE INDEX copy_mirrors_parent_cid_idx
    ON copy_mirrors (parent_cid);
CREATE INDEX copy_mirrors_active_idx
    ON copy_mirrors (active) WHERE active;
CREATE INDEX copy_mirror_positions_instrument_id_idx
    ON copy_mirror_positions (instrument_id);

COMMIT;
```

Two index notes:

- There is no standalone `copy_mirror_positions_mirror_id_idx`
  because the composite primary key `(mirror_id, position_id)`
  already covers `WHERE mirror_id = ?` queries as a
  leftmost-prefix scan.
- `copy_mirrors_active_idx` is a partial index — it indexes only
  rows where `active` is true. This keeps the AUM query fast
  (`WHERE m.active`) even as closed mirrors accumulate across
  years of un-copy history, without the dead-weight of indexing
  "closed" as a separate value.

No backfill is required — the tables start empty and fill up on the
next portfolio sync. The existing `positions.source` CHECK constraint
remains `('ebull', 'broker_sync')` — we do NOT add a third value,
because mirrors never become `positions` rows.

## 8. Testing strategy

### 8.0 Shared fixtures (named)

Codex v3 (finding Y) flagged that several test scenarios share the
same underlying data shape and should be backed by named fixtures,
not narrative prose, so test authors cannot silently drift. The
fixtures below live in a **new file**
`tests/fixtures/copy_mirrors.py` (not `tests/conftest.py`, which
already carries cross-cutting DB fixtures — bloating it further
would bury copy-trading state in unrelated tests). They are
imported by every test that needs them via
`from tests.fixtures.copy_mirrors import two_mirror_payload` etc.

- **`two_mirror_payload`** — `BrokerPortfolio` with 2 mirrors × 3
  nested positions each, derived from the real
  `data/raw/etoro_broker/etoro_portfolio_*.json` payload (trimmed
  for readability, at least one non-USD position to exercise
  `openConversionRate`). This is the canonical "healthy multi-mirror
  sync" fixture. Every positive-path service-layer test starts
  here; disappearance tests start here and then remove a mirror.
- **`two_mirror_seed_rows`** — the same two mirrors pre-inserted
  into `copy_mirrors` (active = TRUE) and `copy_mirror_positions`,
  ready for "seed the DB, call sync with a *different* payload"
  tests. Used by disappearance, re-copy, and parser-abort tests.
- **`mirror_aum_fixture`** — the load-bearing DB fixture for
  §8.4 AUM identity, §8.5 guard integration, and §8.6 per-call-
  site delta tests. Codex v5 (finding AH) required it to carry
  enough state that all three call sites actually reach their
  AUM blocks. Concretely, it seeds:

  1. **Two mirrors in `copy_mirrors`** — one `active = TRUE`,
     one `active = FALSE`, both with concrete
     `available_amount`, `initial_investment`, `deposit_summary`,
     `withdrawal_summary`, `closed_positions_net_profit`,
     `started_copy_date` values (numbers small enough to
     hand-compute the expected `total_aum`).
  2. **Matching `copy_mirror_positions` rows** for each mirror —
     at least one long position per mirror, with concrete
     `units`, `open_rate`, `open_conversion_rate`, `amount`,
     `is_buy = TRUE`, and distinct `instrument_id` values so the
     §3.4 LATERAL join has a non-empty result for the active
     mirror and an equal contribution (which the `WHERE m.active`
     filter then zeroes out) for the closed one.
  3. **Matching `quotes` rows** for every mirror position's
     instrument — so the §3.4 query's `COALESCE(q.last,
     cmp.open_rate)` fallback is exercised for at least one
     position (quote present) and at least one position is
     tested with no quote (quote absent → falls back to
     cost basis).
  4. **An `instruments` row for at least one of the mirror
     instrument IDs** — §8.5's `_load_sector_exposure` call
     needs a matching instrument in `instruments` to resolve a
     sector. The fixture picks one mirror-held instrument as
     the "guard test instrument" and sets its sector to an
     explicit value so the sector-numerator assertion in §8.5
     is deterministic.
  5. **A single `latest_scores` row** for any instrument (does
     NOT need to be one of the mirror-held IDs) keyed on
     `model_version = "v1-balanced"` — this is the §8.6 Test 2
     precondition that prevents `run_portfolio_review` from
     early-returning at
     [portfolio.py:733](app/services/portfolio.py#L733) before
     it reaches the AUM block. Without this row the review
     path never exercises `_load_mirror_equity`, so the test
     is silently a no-op.
  6. **Empty `positions` and `cash_ledger`** — the fixture
     intentionally carries no eBull-owned positions or cash so
     the expected `total_aum` in the delta tests is exactly
     `_load_mirror_equity(conn)` plus `(0 + 0)`. §8.6's
     "baseline" repeat then flips the active mirror's
     `active = FALSE` and asserts `total_aum` returns to 0.
     (A separate dedicated test for the full
     `positions + cash + mirror_equity` combination lives in
     §8.4 identity tests, which use this same fixture and add
     one position + one cash row via `UPDATE`/`INSERT` setup
     helpers.)

  Has deterministic numbers so the expected `total_aum` is
  computable by hand from the fixture's declared values.
- **`no_quote_mirror_fixture`** — the empirically-reconciled mirror
  15712187 shape (`available = 2800.33`, positions
  `amount = 50.00` and `amount = 17039.33`) with no matching quotes
  rows. Used by the `available + SUM(amount)` identity test.
- **`mtm_delta_mirror_fixture`** — one long position with
  `open_rate = 1207.4994`, `units = 6.28927`,
  `open_conversion_rate = 0.01331`, `amount = 101.08`, plus a
  matching `quotes.last = 1400.0` row. Used by the MTM-delta +
  FX test (and its short-side variant, which flips `is_buy`
  and swaps the quote).

**Frozen `now` value — ownership locked to the fixture file.**
`tests/fixtures/copy_mirrors.py` **owns** the canonical module
constant:

```python
_NOW: datetime = datetime(2026, 4, 10, 5, 30, tzinfo=UTC)
```

The value is identical to the constant currently declared at
[tests/test_portfolio_sync.py:20](tests/test_portfolio_sync.py#L20),
so behaviour is preserved. As part of this PR,
`tests/test_portfolio_sync.py` is edited to remove its local
declaration and import the constant from the fixture module
instead:

```python
from tests.fixtures.copy_mirrors import _NOW
```

Codex v5 (finding AI) correctly observed that fixtures importing
from a test module is backwards coupling — tests depend on
fixtures, not the other way around. Owning `_NOW` in the fixture
module fixes the coupling direction in a single file rename +
import swap, with zero behavioural change.

`_NOW` is the `now` parameter §2.3.4 binds as `%(now)s` in the
soft-close SQL; §8.3's disappearance tests assert the exact
value round-trips through the SQL as the stored `closed_at`.

### 8.1 Parser unit tests (pure, no DB)

- `_parse_mirror` / `_parse_mirror_position` against
  `two_mirror_payload`: verify the `BrokerMirror` structure,
  required field presence, and at least one non-USD FX rate round-
  trip through `_parse_mirror_position`.
- **Unrecognisable top-level mirror element (no `mirrorID`)** —
  element is not a dict, or is a dict with no `mirrorID` key →
  logged warning and skipped, other mirrors still parsed. This is
  the only surviving log-and-skip path per §2.2.2.
- **Known-mirror top-level parse failure** — element has
  `mirrorID` present but a required field (`parentCID`,
  `parentUsername`, `initialInvestment`, `availableAmount`,
  `closedPositionsNetProfit`, `startedCopyDate`) missing or
  malformed → raises `PortfolioParseError` wrapping the underlying
  exception, message names the mirror_id. No partial result.
- **Malformed nested position** (missing required field, non-numeric
  `units`, missing `openConversionRate`) → `_parse_mirror` raises
  `PortfolioParseError`. Tests assert the raised exception's
  `str(exc)` contains both the mirror_id **and** the position index
  (`"position[2]"` shape) — the inner-loop wrap in §2.2.2
  guarantees this context survives the outer loop's re-raise. This
  is the strict inner-loop behaviour from §2.2.2 and the parser-
  failure safeguard from §2.3.3.
- **Non-numeric `units` hits `decimal.InvalidOperation`, not
  `ValueError`** (Codex v4 finding AB). `units: "bogus"` →
  `Decimal(str("bogus"))` raises `decimal.InvalidOperation`.
  `_parse_mirror`'s inner catch list in §2.2.2 names
  `decimal.DecimalException`, so this is caught and re-raised as
  `PortfolioParseError` with full position-index context, not
  leaked to the outer top-level wrap. Test asserts exactly this
  path: the raised type is `PortfolioParseError`, the `__cause__`
  is a `decimal.InvalidOperation`, and the message contains the
  position index.
- **`PortfolioParseError` hierarchy test.** Assertions:
  `issubclass(PortfolioParseError, Exception) is True` AND
  `issubclass(PortfolioParseError, (ValueError, TypeError,
  KeyError, decimal.DecimalException)) is False`. This protects
  against a future refactor that accidentally subclasses
  `ValueError` (or any of the other catch-list exceptions) and
  defeats the outer-loop re-raise. (Codex v3 finding U, extended
  by Codex v4 finding AB.)
- Missing optional fields (stop loss, take profit) → `None` on
  the `BrokerMirrorPosition` dataclass.
- **`openConversionRate` is required in production.** A unit test
  asserts that `_parse_mirror_position` raises when
  `openConversionRate` is absent. The `Decimal("1")` fallback
  lives only on the `_mk_position` test helper in
  `tests/test_portfolio_sync.py`, and a separate unit test asserts
  that the helper's default is scoped to USD-only fixtures. No
  production code path silently defaults this field.

### 8.2 Service-layer tests (real test DB)

Real test DB per `feedback_test_db_isolation` rule — `ebull_test`,
never `settings.database_url`.

- First `sync_portfolio` call with `two_mirror_payload` → rows in
  `copy_traders`, `copy_mirrors` (both `active = TRUE`),
  `copy_mirror_positions`.
- Second `sync_portfolio` with one nested position removed → that
  row is DELETEd, siblings untouched, `copy_mirrors.active`
  unchanged.
- Re-running the same payload is idempotent (row counts unchanged,
  `updated_at` refreshed, `active` still `TRUE`).
- Mirror-level metadata changed on second sync → `copy_mirrors`
  row updated, trader row untouched apart from `updated_at`.
- Parent username changed on second sync →
  `copy_traders.parent_username` updated.

### 8.3 Disappearance handling tests (§2.3.4)

Every test below starts from `two_mirror_seed_rows` and calls
`sync_portfolio` with a modified payload.

- **Empty `mirrors[]` with active local mirrors present** →
  `RuntimeError` raised, transaction rolls back, both seed rows
  remain `active = TRUE`. Matches the positions guard's "total
  disappearance is unsafe" invariant.
- **Partial mirror disappearance: soft-close.** Payload contains
  only 1 of the 2 seed mirrors. Assert: the matching mirror stays
  `active = TRUE, closed_at IS NULL`; the missing mirror flips to
  `active = FALSE, closed_at = frozen_now` (exact timestamp
  match against the injected `now` parameter); nested
  `copy_mirror_positions` rows for the closed mirror **remain**;
  the result's `mirrors_closed = 1`.
- **Re-copy (same `mirror_id` reuse).** Pre-set one seed mirror to
  `active = FALSE, closed_at = <past>`, then call sync with
  `two_mirror_payload`. Assert: the row is back to
  `active = TRUE, closed_at = NULL`; `updated_at` advances; nested
  positions are upserted correctly.
- **Parser-failure abort before eviction.** Call sync with a
  payload where one nested position in one mirror is malformed.
  Assert: `PortfolioParseError` propagates, the transaction rolls
  back, all seed rows survive, no partial upsert or eviction
  occurred, `copy_mirrors.active` on both seed mirrors is
  unchanged.
- **Known-mirror top-level parse failure also aborts.** Call sync
  with a payload where one mirror has `mirrorID` present but
  `availableAmount` missing. Assert: `PortfolioParseError`
  propagates (wrapped from a `KeyError`), both seed rows survive
  unchanged — this is the regression test for the Codex v3
  finding V parse-and-soft-close hole.

### 8.4 AUM identity tests (§3 correctness core)

- **No-quote cost-basis fallback.** Using `no_quote_mirror_fixture`
  → the §3.4 query returns `2800.33 + 50.00 + 17039.33 =
  19889.66`. The empirically-reconciled identity on mirror
  15712187.
- **MTM delta with FX.** Using `mtm_delta_mirror_fixture` → delta
  = `1 * 6.28927 * (1400.0 - 1207.4994) * 0.01331 ≈ 16.12`, so
  mirror equity ≈ `available + 101.08 + 16.12`. Asserts FX is
  applied to the delta.
- **Short delta.** Same fixture with `is_buy = false` and
  `quotes.last = 1000.0` (below entry) → delta = `-1 * 6.28927 *
  (1000.0 - 1207.4994) * 0.01331 ≈ +17.37`, equity goes **up**
  because a short is profitable when the price falls.
- **Closed mirror excluded from AUM.** Using `mirror_aum_fixture`
  → §3.4 query returns only the active mirror's equity; closed
  mirror contributes zero. Regression test for the
  `WHERE m.active` filter.
- **Empty `copy_mirrors` table → `mirror_equity = 0.0` (not null).**
  Call `_load_mirror_equity(conn)` against an empty schema and
  assert the returned value is the float `0.0`, not `None`. This
  is the regression test for the §6.4 contract change (Codex v3
  finding W).

### 8.5 Guard AUM integration test

Tests the guard-side integration surgically at the private helper
level, not via `evaluate_recommendation` end-to-end. Codex v5
finding AG correctly observed that:

- The existing guard entry point is `evaluate_recommendation`
  (not `run_execution_guard`), and
- `GuardResult` has no `total_aum` field (AUM is a local variable
  inside `evaluate_recommendation`, consumed by the concentration
  rule and not exposed on the return value).

So the guard integration test targets
`_load_sector_exposure(conn, instrument_id)`
([execution_guard.py:235](app/services/execution_guard.py#L235)) —
the private helper that returns `total_aum` and is the exact
function §6.1 modifies to add `_load_mirror_equity(conn)` to the
existing `total_positions + cash` sum.

Fixture: `mirror_aum_fixture` (§8.0) seeded into `ebull_test`,
plus an `instruments` row for the instrument passed to
`_load_sector_exposure` (any instrument_id present in the mirror
positions will do). No `evaluate_recommendation`,
`trade_recommendations`, `kill_switch`, or `runtime_config` setup
needed — `_load_sector_exposure` does not touch those tables.

Scenarios:

- **Empty baseline (no mirrors at all).** `_load_sector_exposure`
  returns `total_aum == positions_mv + cash`, the pre-PR contract.
- **Active mirror adds to denominator.** Seed
  `mirror_aum_fixture`'s active mirror (with positions + available
  cash + matching quotes). `_load_sector_exposure` now returns
  `total_aum == positions_mv + cash + _load_mirror_equity(conn)`
  where `_load_mirror_equity(conn)` is computed once from the
  same connection as the expected additive contribution.
- **Closed mirror contributes nothing.** Flip `mirror_aum_fixture`'s
  active mirror to `active = FALSE` via an `UPDATE` at test
  setup. `_load_sector_exposure` returns the baseline again —
  soft-closed mirrors do not inflate the denominator.
- **Sector numerator unchanged.** With an active mirror holding an
  instrument NOT in `positions`, `_load_sector_exposure`'s
  `current_sector_pct` numerator is unaffected. The mirror only
  expands the denominator; the rule stays more permissive, not
  less. This is the regression test for §4 "execution guard
  isolation".

### 8.6 AUM delta tests per call site

Codex v5 finding AG/AH correctly observed that the earlier
"three-call-site consistency" framing asserted an equality
surface (`same mirror_equity component`) that does not exist on
the `GuardResult` return value, and that `run_portfolio_review`
early-returns before the AUM block when there are no ranked
candidates. The rewrite below tests each path independently,
using `_load_mirror_equity(conn)` computed against the same DB
state as the **expected additive contribution**, and asserts each
path's `total_aum` absorbs exactly that value vs. a baseline with
mirrors soft-closed. Three separate tests, not one mega-test.
The common thread is that the additive delta is read from
`_load_mirror_equity(conn)` — if a future PR breaks any one path,
its test alone fails.

Shared bootstrap (all three tests): `ebull_test`,
`mirror_aum_fixture` seeded, plus the per-path extras below.
`expected_mirror_contribution = _load_mirror_equity(conn)` is
computed once in each test's setup, before the call site fires.

**Test 1 — API path.** `GET /api/portfolio` via `TestClient` with
`app.dependency_overrides[get_conn]` pointing at the `ebull_test`
connection. Assertions:

- `response.json()["mirror_equity"] == expected_mirror_contribution`
- `response.json()["total_aum"] == (positions_market_value + cash + expected_mirror_contribution)`

Baseline repeat with the mirror soft-closed (UPDATE `active =
FALSE`) → `mirror_equity == 0.0` and `total_aum` returns to
`positions + cash`.

**Test 2 — `run_portfolio_review` path.** Calls
`run_portfolio_review(conn)` with the `mirror_aum_fixture`
**plus at least one `latest_scores` row** (any instrument,
matching `model_version`) so the early-return at
[portfolio.py:733](app/services/portfolio.py#L733) is not hit
and the AUM block actually runs. `latest_scores` setup is part
of `mirror_aum_fixture` (see §8.0). Assertion:

- `result.total_aum == (positions_market_value + (cash or 0.0) + expected_mirror_contribution)`

Baseline repeat with the mirror soft-closed → `result.total_aum`
returns to `positions + cash`.

**Test 3 — guard path.** Directly calls
`_load_sector_exposure(conn, instrument_id)` (same as §8.5) and
asserts its returned `total_aum` carries the `expected_mirror_
contribution`. This is the same function and assertion surface
as §8.5, repeated here under the "delta per call site" framing
for symmetry — the test is cheap and it keeps the §8.6 block
self-contained. If §8.5 is implemented, this sub-test is one
line (`assert sector_exposure_total_aum == positions + cash +
expected_mirror_contribution`).

**What this test explicitly does NOT assert:**

- Cross-path equality of a `mirror_equity` *field*. Only
  `PortfolioResponse` exposes a `mirror_equity` field (see §6.2);
  `PortfolioReviewResult` and `GuardResult` do not. §6 does not
  add them because the review/guard paths only care about the
  additive sum, not the component. Asserting a field that
  doesn't exist would be theatre.
- Persisted AUM snapshots. Per §6.3, no AUM snapshot is written.

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

(The earlier draft had a "graceful mirror-closure semantics
(`closed_at` column, disappear vs delete vs preserve-history
policy)" bullet here. That work moved into Track 1 in round 2 —
§1.2 `active`/`closed_at` columns and §2.3.4 soft-close semantics
are now shipped in this PR. Removed in round 3.)

Track 2 will introduce its own schema migration(s) for trader
history, daily-gain series, cohort signals, and whatever
discovery-side tables it needs. The Track 1 schema
(`copy_traders`, `copy_mirrors`, `copy_mirror_positions`) needs no
*structural* changes to support Track 2 — existing rows are not
rewritten — but it may grow columns or sibling tables. That's fine.
Migrations are cheap; designing forward by guessing columns Track 2
might need is not.

## 10. Open questions

None at v5 spec-revision time. The load-bearing decisions are all
locked:

- Three-table split (`copy_traders`, `copy_mirrors`,
  `copy_mirror_positions`), composite PK on the positions table.
- Full granular position capture with `open_conversion_rate` as a
  required NOT NULL column.
- `positions` and the execution guard's rule queries are untouched
  — mirrors never appear in sector or per-position aggregations.
- Empty `mirrors[]` with active local rows → raise.
- Partial disappearance → soft-close via `active` / `closed_at`
  columns; nested positions retained.
- Any parse failure on a row carrying a usable `mirrorID` (top-
  level or nested) → strict raise `PortfolioParseError`, full sync
  rollback, operator investigates. Only rows with no `mirrorID` at
  all are log-and-skipped.
- `PortfolioParseError` declared in
  `app.providers.implementations.etoro_broker` as a direct
  `Exception` subclass (not `ValueError` / `TypeError` / `KeyError`
  / `decimal.DecimalException`). Inner `_parse_mirror` wrap and
  outer top-level wrap both include `decimal.DecimalException` in
  their catch list.
- `openConversionRate` required in production; default only in
  test helpers.
- `mirror_equity: float = 0.0` (not `float | None`) on
  `PortfolioResponse`, aligned with §3.4's `COALESCE(SUM, 0)`.
- `_sync_mirrors` soft-close SQL binds the injected `now`
  parameter (not DB `NOW()`) so frozen-time tests are
  deterministic.
- `_load_mirror_equity(conn)` is a module-level helper in
  `app/services/portfolio.py` alongside `_load_cash` /
  `_load_positions`, imported by `execution_guard`, `api/portfolio`,
  and `run_portfolio_review` (§6.0).
- The execution_guard change is surgical: `_load_sector_exposure`
  at [execution_guard.py:235](app/services/execution_guard.py#L235)
  adds the mirror-equity contribution to its local `total_aum`
  return value. `evaluate_recommendation` and `GuardResult` are
  not touched; AUM remains a local variable inside the guard,
  consumed by the concentration rule. (§6.1, locked round 5.)
- Shared copy-trading test fixtures live in a new file
  `tests/fixtures/copy_mirrors.py` (not `conftest.py`). The
  fixture file **owns** the canonical `_NOW = datetime(2026,
  4, 10, 5, 30, UTC)` constant; `tests/test_portfolio_sync.py`
  is edited in this PR to import `_NOW` from the fixture
  module instead of declaring it locally. (§8.0, locked round 5.)
- `mirror_aum_fixture` seeds two mirrors (one active, one
  closed) with positions/quotes, **plus** one `instruments` row
  for the guard sector-exposure assertion, **plus** one
  `latest_scores` row so `run_portfolio_review` does not early-
  return before reaching the AUM block. (§8.0, locked round 5.)
- AUM correction at all three call sites tested via
  `_load_mirror_equity(conn)` as the expected additive
  contribution — no cross-path `mirror_equity` field equality
  assertion, because only `PortfolioResponse` exposes that
  field; `PortfolioReviewResult` and `GuardResult` do not.
  (§8.6, locked round 5.)
- AUM correction at all three call sites
  (execution_guard / api/portfolio / run_portfolio_review), no
  persisted AUM snapshot.
- REST endpoint and frontend panel deferred to Track 1.5 (see
  Appendix B).
- Track 2 (`/user-info/people/*` surface, trader discovery,
  historical gain series, cohort signals) is a separate ticket
  opened when this spec merges.

If a sixth round of review surfaces new questions they will be
appended here before the writing-plans handoff.

## Appendix A. Revision log

### Round 1 — Codex review of the first draft

See `.claude/codex-spec-review.log` in the branch history. Findings
and resolutions:

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
  documented against `JobLock` + APScheduler (§2.3.1).
- **I. Parser-failure + eviction = silent data loss.** Parser-failure
  guard added. Later tightened to strict-raise in round 2.
- **J. Only `mirrors=[]` guarded; partial disappearance ignored.**
  Partial-disappearance guard added. Later changed from "raise" to
  "soft-close" in round 2.
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

### Round 2 — Codex review of the round-1 revision

See `.claude/codex-spec-review-v2.log` in the branch history.
Findings and resolutions:

- **O. §2.3.1 wording overclaims where the lock lives.** Codex
  observed that `daily_portfolio_sync()` itself does not hold
  `JobLock`; `JobRuntime._wrap_invoker` / `_run_manual` do. Fix:
  §2.3.1 now states explicitly that the serialisation guarantee
  lives on the runtime wrapper, not the job function, and that
  direct callers outside `JobRuntime` bypass it.
- **P. §2.3.3 50% parser-failure threshold is indefensible.** Codex
  observed that a ratio-based guard is pathological at small N
  (1-of-1 fails at 100%, 1-of-2 passes at 50%) and too lax at
  large N (~40 silently-deleted rows is still bad). Fix: §2.3.3
  tightened to strict-raise on any nested-position parse failure,
  zero budget, full sync rollback.
- **Q. §2.3.4 partial-disappearance raise is operationally harsh.**
  Codex observed that a genuine un-copy via the eToro UI would
  page the operator, roll back the sync, and keep the stale local
  mirror inflating AUM until someone manually `DELETE`d the row.
  Fix: §1.2 gained `active` / `closed_at` columns; §2.3.4 now
  soft-closes partially-disappeared mirrors (keep the row, flip
  `active=false`, retain nested positions for audit); §3.4's
  `WHERE m.active` filter excludes closed mirrors from the AUM
  denominator. Total disappearance still raises, matching the
  positions guard invariant.
- **R. §6.3 invented a `portfolio_reviews` audit table.** Codex
  grepped the repo and found no such table and no snapshot write
  path. Fix: §6.3 now states that `run_portfolio_review` adds
  nothing beyond `_load_mirror_equity`, with a note naming the
  earlier claim as incorrect. No AUM snapshot persistence in this
  PR.
- **S. §8 `openConversionRate` production default of `Decimal("1")`
  is unsafe.** Codex observed that silently defaulting FX to 1 in
  production would mix native-currency notionals into USD AUM on
  any payload with the field missing. Fix: §2.2 and §8 now state
  that `openConversionRate` is required in production parsing and
  that the `Decimal("1")` fallback is scoped to the
  `tests/test_portfolio_sync.py` fixture helper only, with a unit
  test asserting the production raise path.
- **T. §6 frontend scope was muddy.** The earlier "frontend in scope
  for PR but detailed fields deferred" position meant the spec did
  not lock the data contract. Fix: §6.4 / §6.5 explicitly defer
  the new REST endpoint and the copy-trading panel to a Track 1.5
  follow-up PR (Appendix B). This PR ships only the AUM correction
  and a single additive `mirror_equity` field on the existing
  `PortfolioResponse`. Round 3 later tightened the field type from
  `float | None = None` to `float = 0.0` — see finding W.

### Round 3 — Codex review of the round-2 revision

See `.claude/codex-spec-review-v3.log` in the branch history.
Findings and resolutions:

- **U. `PortfolioParseError` hierarchy unspecified.** Codex v3
  observed that if the new exception subclasses `ValueError` /
  `TypeError` / `KeyError`, the existing outer-loop
  `except (KeyError, ValueError, TypeError)` silently swallows
  nested-position failures, defeating §2.3.3's strict-raise. Fix:
  §2.2.1 declares `PortfolioParseError(Exception)` as a direct
  `Exception` subclass in
  `app.providers.implementations.etoro_broker`, and §8.1 adds a
  unit test that asserts the hierarchy to prevent future drift.
- **V. §2.2 log-and-skip of known mirrors is dangerous under
  soft-close.** Codex v3 observed that a top-level parse failure
  on a row with a valid `mirrorID` would be log-skipped by the
  earlier asymmetric parser contract, then interpreted by §2.3.4
  as a partial disappearance, then silently soft-closed — dropping
  a still-live mirror from AUM for exactly the same failure mode
  §2.3.3 already strict-raises on for nested positions. Fix:
  §2.2.2 rewrites the parse contract so the only surviving
  log-and-skip path is a row with **no `mirrorID` at all**; any
  row with `mirrorID` present and a required top-level field
  missing or malformed now raises `PortfolioParseError`, which the
  outer loop catches first and re-raises. §8.3 adds a "known-mirror
  top-level parse failure also aborts" regression test.
- **W. `mirror_equity` contract ambiguous (`None` vs `0.0`).**
  Codex v3 observed that §3.4's query returns `0` for no active
  mirrors (wrapped in `COALESCE(SUM(...), 0)`) while §6.4 declared
  the API field as `float | None = None` — an unnecessary
  difference the frontend would have to branch on. Fix: §6.4
  changes the field to `mirror_equity: float = 0.0`. `cash_balance`
  keeps its `float | None` because "empty cash_ledger" is a
  genuinely unknown domain state, unlike "no mirrors held" which
  is a computed zero. §8.4 adds a regression test for the empty-
  table case.
- **W-bis. `PortfolioResponse` mis-described as `@dataclass`.**
  While verifying W against the real code, discovered that §6.4
  called `PortfolioResponse` a `@dataclass` when
  [api/portfolio.py:63](app/api/portfolio.py#L63) declares it as
  `class PortfolioResponse(BaseModel)` — a pydantic `BaseModel`.
  Fix: §6.2 and §6.4 now correctly name the type.
- **X. §2.3.4 SQL uses DB `NOW()` instead of the injected `now`.**
  Codex v3 observed that the round-2 SQL sketch called
  `NOW()` directly, which means tests that freeze time
  (as the soft-close / re-copy tests need to in order to assert
  exact `closed_at` / `updated_at` values) would drift from the
  DB wall clock. Fix: §2.3.4 now binds `%(now)s` from the `now`
  parameter `_sync_mirrors` already accepts in its signature, and
  §8.0 fixture contract standardises the frozen timestamp
  `datetime(2026, 4, 11, 5, 30, tzinfo=timezone.utc)` so every
  deterministic test asserts against the same value. The manual
  operator runbook on total-disappearance keeps `NOW()` — it is
  typed by a human interactively, no injected `now` exists.
- **Y. §8 test fixtures described narratively, not named.**
  Codex v3 observed that several tests reference the same
  underlying shape ("2 mirrors × 3 nested positions", "active +
  closed mirror AUM") without a named fixture, which invites
  implementation drift if the test author writes the shape from
  scratch in each test. Fix: §8.0 names five fixtures —
  `two_mirror_payload`, `two_mirror_seed_rows`,
  `mirror_aum_fixture`, `no_quote_mirror_fixture`,
  `mtm_delta_mirror_fixture` — with explicit shapes and the
  frozen `now` contract, and every §8.1-8.4 test reads from them.
- **Z. §9 Track 2 closure bullet stale.** Codex v3 observed that
  the "graceful mirror-closure semantics (`closed_at` column,
  disappear vs delete vs preserve-history policy)" bullet still
  lived in the Track 2 preview even though round 2 moved the
  work into Track 1 (§1.2 `active`/`closed_at` columns + §2.3.4
  soft-close). Fix: §9 now explicitly records that the work moved
  out of Track 2 in round 2 rather than silently deleting the
  bullet, so future readers understand the migration.

### Round 4 — Codex review of the round-3 revision

See `.claude/codex-spec-review-v4.log` in the branch history.
Findings and resolutions:

- **AA. `_load_mirror_equity(conn)` module location unspecified.**
  Codex v4 observed that §6.3 implicitly placed the helper in
  `app/services/portfolio.py` but §8.4 and §6.1 / §6.2 called it
  like a shared helper without naming the module, leaving
  writing-plans to guess whether to duplicate SQL or cross-import.
  Fix: new §6.0 subsection declares the helper at module level in
  `app/services/portfolio.py` alongside `_load_cash` /
  `_load_positions`, pinning signature and importers. All three
  call sites now explicitly import from §6.0.
- **AB. `decimal.DecimalException` not in §2.2.2 catch list.**
  Codex v4 observed that `Decimal(str("bogus"))` raises
  `decimal.InvalidOperation` — a subclass of
  `decimal.DecimalException`, **not** `ValueError`. The round-3
  outer catch `except (KeyError, ValueError, TypeError)` would
  miss it, leaking a bare `DecimalException` past the outer loop
  and defeating the strict-raise guarantee. Fix: §2.2.2 now adds
  `decimal.DecimalException` to both the outer top-level wrap
  catch and the inner `_parse_mirror` per-position wrap. §8.1
  adds the "non-numeric units" regression test asserting that
  `__cause__` is a `decimal.InvalidOperation` and the message
  contains the position index. §8.1 also extends the hierarchy
  test to assert `PortfolioParseError` is NOT a
  `DecimalException` subclass.
- **AC. §8.4 narrative "existing guard fixture" and three-call-
  site fixture.** Codex v4 observed that §8.4's guard integration
  test and three-call-site consistency test still described the
  DB state narratively ("existing guard test fixture + one
  mirror", "a single DB fixture") rather than naming one of the
  §8.0 fixtures. Fix: the guard test and the consistency test
  both now explicitly use `mirror_aum_fixture` (whose §8.0 entry
  was extended to cover all three usages). §8.4's closed-mirror
  test, §8.5 guard integration, and §8.6 three-call-site test
  all name this fixture by name.
- **AD. §8.0 fixture path was "or".** Codex v4 observed that the
  path was hedged as `tests/conftest.py` OR
  `tests/fixtures/copy_mirrors.py`. Fix: §8.0 now pins the path
  to `tests/fixtures/copy_mirrors.py` (new file, not bloating
  `conftest.py`) and standardises the import pattern.
- **AE. §8.0 frozen `now` drifted from existing `_NOW`.** Codex
  v4 observed that the spec declared
  `datetime(2026, 4, 11, 5, 30, UTC)` while
  [tests/test_portfolio_sync.py:20](tests/test_portfolio_sync.py#L20)
  already uses `datetime(2026, 4, 10, 5, 30, UTC)`. Fix: §8.0
  now reuses the existing `_NOW` value so tests that sit
  alongside each other share one canonical timestamp.
- **AF. §1.2 prose still said `closed_at=NOW()`.** Codex v4
  flagged that leaving the prose form of "NOW()" in the §1.2
  narrative could lead a reader (or a future `writing-plans`
  invocation) to mis-implement the soft-close SQL as DB wall
  clock instead of the injected `now`. Fix: §1.2 now says
  "marked `active=false, closed_at=<injected sync timestamp>`"
  with an explicit forward pointer to §2.3.4.

### Round 5 — Codex review of the round-4 revision

See `.claude/codex-spec-review-v5.log` in the branch history.
Findings and resolutions:

- **AG. §8.6 named a non-existent guard entry point and
  asserted a non-existent return-value field.** Codex v5
  observed that the round-4 §8.6 called
  `run_execution_guard`, which does not exist — the guard's
  public entry point is
  [`evaluate_recommendation`](app/services/execution_guard.py#L538)
  and its return type `GuardResult` has no `total_aum` or
  `mirror_equity` field. `total_aum` is a local variable
  inside `evaluate_recommendation`, computed by the private
  helper `_load_sector_exposure`
  ([execution_guard.py:235](app/services/execution_guard.py#L235))
  and consumed by the concentration rule; it is not
  exposed on any return value. The round-4 "three-call-
  site consistency" framing therefore asserted equality
  across a surface that does not exist. Fix: §6.1 is
  rewritten to name `_load_sector_exposure` as the exact
  private function being modified; §8.5 tests that function
  directly; §8.6 is rewritten as three per-call-site delta
  tests (API / review / guard), each computing
  `_load_mirror_equity(conn)` once as the expected additive
  contribution and asserting the path's `total_aum`
  absorbs that value. No cross-path `mirror_equity` field
  equality is asserted — only `PortfolioResponse` exposes
  a `mirror_equity` field, so only the API test asserts
  it. §8.6's explicit "what this test does NOT assert"
  block locks this scope in writing.
- **AH. §8.5/§8.6 fixture shape missing state for the
  review and guard paths.** Codex v5 observed that
  `run_portfolio_review` returns early at
  [portfolio.py:733](app/services/portfolio.py#L733) when
  there are no ranked candidates and no open positions,
  before the AUM block ever runs; and that `mirror_aum_
  fixture` as defined in round 4 did not seed an
  `instruments` row for the guard path's sector lookup,
  nor a `latest_scores` row to bypass the review early-
  return. This meant §8.6 Test 2 (review path) would
  silently be a no-op. Fix: `mirror_aum_fixture`'s §8.0
  entry now explicitly enumerates six components: two
  mirrors, their positions, matching quotes, an
  `instruments` row for the guard path, a `latest_scores`
  row for the review path, and empty
  `positions`/`cash_ledger` so the expected `total_aum`
  collapses to `_load_mirror_equity(conn)` plus `(0 + 0)`
  in the delta tests.
- **AI. §8.0 `_NOW` ownership still hedged as "or".** Codex
  v5 observed that round 4's fix pinned the fixture path
  but left `_NOW` ownership open with "re-exports `_NOW`
  (or imports from the existing test module)". Fix: §8.0
  now locks `tests/fixtures/copy_mirrors.py` as the
  canonical owner of `_NOW = datetime(2026, 4, 10, 5, 30,
  tzinfo=UTC)`. `tests/test_portfolio_sync.py` is edited
  in this PR to import `_NOW` from the fixture module
  instead of declaring it locally. The value is
  bit-identical, so behaviour is preserved; the coupling
  direction (tests depend on fixtures, not the reverse)
  is now correct. §10 reflects this as a locked decision.

## Appendix B. Track 1.5 — REST endpoint and frontend panel

**Scope:** a new ticket that ships the copy-trading browsing UX on
top of the tables and the AUM correction delivered by this PR.

**Minimum viable slice:**

- `GET /api/portfolio/copy-trading` endpoint returning a list of
  copy traders, each with:
  - `parent_cid`, `parent_username`
  - mirror-level aggregates: `active`, `initial_investment`,
    `deposit_summary`, `withdrawal_summary`, `available_amount`,
    `closed_positions_net_profit`, `mirror_equity` (computed
    from §3.4)
  - nested-position summary: count, top N by cost basis,
    per-instrument breakdown
- React dashboard panel consuming the endpoint, with:
  - Top-line summary: AUM = positions + cash + mirror_equity
  - Per-trader cards with mirror-level numbers
  - Drill-down showing nested positions, cost basis, entry FX,
    MTM delta
  - Closed mirrors visible in a separate "history" section
    (filter on `active = FALSE`)
- Data-contract tests aligned with
  `.claude/skills/frontend/api-shape-and-types.md`

**Explicitly NOT in Track 1.5:**

- An "un-copy from eBull" action. v1 un-copies happen on the eToro
  UI; eBull reflects them on the next sync. A UI button that calls
  the eToro API to un-copy is out of scope until Track 2's
  authenticated-user surface lands.
- Historical gain/loss charts for mirrors. Those depend on
  snapshot persistence which v1 does not do.
- Any change to the tables defined in this PR. Track 1.5 is a pure
  read surface on top of an already-correct data model.
