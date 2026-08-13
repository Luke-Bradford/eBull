# Copy Trading Ingestion — Track 1a Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ingest eToro copy-trading mirrors (`clientPortfolio.mirrors[]`) into three new DB tables so downstream AUM correction (Track 1b, #187), browsing (Track 1.5, #188), and discovery (Track 2, #189) can consume them.

**Architecture:** New sibling schema (`copy_traders`, `copy_mirrors`, `copy_mirror_positions`) populated from the same `/portfolio` call the broker already makes. Existing `positions` / `cash_ledger` are untouched. A strict-raise parser with nested error attribution feeds a new `_sync_mirrors` helper that runs inside the existing `sync_portfolio` transaction, with upsert + nested-eviction + soft-close semantics for operator un-copies.

**Tech Stack:** Python 3.12, psycopg3 (named `%(name)s` params, `dict_row`), Postgres 15+ (partial indexes, JSONB, `ANY(%s::bigint[])`), pytest (real `ebull_test` DB for service-layer tests).

**Spec reference:** `docs/superpowers/specs/2026-04-11-copy-trading-ingestion-design.md` §1 (schema), §2 (sync flow + parser), §8.0 (fixtures), §8.1–§8.3 (tests), §10 (locked decisions). Track 1a implements §1, §2, §8.0–§8.3 only; §3.4 query helper and §6.x call-site wiring are **Track 1b** (#187) and are out of scope for this plan.

---

## Scope of this plan

**In scope (Track 1a):**

- Migration 022 with three new tables + indices (spec §1, §7)
- New `BrokerMirrorPosition` / `BrokerMirror` dataclasses + additive `mirrors: Sequence[BrokerMirror] = ()` field on `BrokerPortfolio` (spec §2.1)
- `PortfolioParseError` exception class and strict-raise parser in `etoro_broker.py` (spec §2.2)
- New `_sync_mirrors(conn, mirrors, now)` helper inside the existing `sync_portfolio` transaction, with upsert → evict → soft-close → total-disappearance guard (spec §2.3)
- `PortfolioSyncResult` extended with `mirrors_upserted`, `mirrors_closed`, `mirror_positions_upserted`
- `tests/fixtures/copy_mirrors.py` **new file** owning `_NOW`, `_GUARD_INSTRUMENT_ID = 990001`, `_GUARD_INSTRUMENT_SECTOR = 'technology'`, plus named fixture builders (spec §8.0)
- `tests/test_portfolio_sync.py` edited to import `_NOW` from the fixture module (removes a test→test import inversion)
- §8.1 parser unit tests, §8.2 service-layer upsert/eviction tests, §8.3 disappearance + parser-abort tests

**Explicitly out of scope (Track 1b / 1.5 / 2):**

- `_load_mirror_equity(conn)` helper and its three call sites (guard, REST, review) → Track 1b #187
- `PortfolioResponse.mirror_equity` field → Track 1b #187
- `§3.4` SQL query inside any consumer → Track 1b #187
- §8.4 AUM identity tests, §8.5 guard integration tests, §8.6 three-call-site delta tests → Track 1b #187
- `mirror_aum_fixture`, `no_quote_mirror_fixture`, `mtm_delta_mirror_fixture` builders → Track 1b #187 (the constants `_NOW` / `_GUARD_INSTRUMENT_ID` / `_GUARD_INSTRUMENT_SECTOR` ship now; the fixtures that use them ship in Track 1b)
- REST endpoint `GET /api/portfolio/copy-trading` and the frontend panel → Track 1.5 #188
- `/user-info/people/*` discovery + history → Track 2 #189

---

## Settled-decisions check

Working order step 2/3 (CLAUDE.md): read `docs/settled-decisions.md` and `docs/review-prevention-log.md` before coding. Relevant entries:

- **Prevention log "test must use `ebull_test`"** → every new service-layer test below uses the `_test_database_url()` pattern already established in `tests/test_operator_setup_race.py:77`. The plan does **not** introduce a shortcut against `settings.database_url`.
- **Prevention log "tests must never wipe dev DB"** → all DB state for §8.2/§8.3 is seeded + torn down inside a fixture-scoped transaction that is rolled back, or by DELETEing only rows the test inserted. No TRUNCATE against a shared table without `_assert_test_db`.
- **Prevention log "smoke gate must catch lifespan swallowed failures"** → migration 022 runs at dev-DB bootstrap. Task 1 verifies `tests/smoke/test_app_boots.py` stays green. No lifespan change is introduced.
- **Settled decision "single source of truth for constants"** → `_NOW`, `_GUARD_INSTRUMENT_ID`, `_GUARD_INSTRUMENT_SECTOR` are declared exactly once (in the new fixture file) and imported everywhere. Task 3 removes the `tests/test_portfolio_sync.py:20` duplicate.
- **Settled decision "params, not interpolation"** → every SQL binding in the sync helper uses `%(name)s` named placeholders; the only place this plan writes a string-formatted SQL is the `sql/022_*.sql` migration file itself (pure DDL, no user input).

If implementation pressure during any task suggests deviating from the above, stop and surface it — do not paper over it.

---

## File structure

**Created:**

- `sql/022_copy_trading_tables.sql` — single-transaction DDL for three tables and four indices (spec §1, §7)
- `tests/fixtures/__init__.py` — marker so `tests/fixtures` is a package (empty file)
- `tests/fixtures/copy_mirrors.py` — canonical `_NOW`, guard-instrument constants, and named fixture builders (spec §8.0)
- `tests/test_copy_mirrors_parser.py` — §8.1 pure parser unit tests (no DB, no I/O)
- `tests/test_portfolio_sync_mirrors.py` — §8.2 + §8.3 service-layer tests against `ebull_test`

**Modified:**

- `app/providers/broker.py` — two new frozen dataclasses + additive `mirrors` field on `BrokerPortfolio`
- `app/providers/implementations/etoro_broker.py` — new `PortfolioParseError`, new `_parse_mirror`, `_parse_mirror_position`, extended `get_portfolio`
- `app/services/portfolio_sync.py` — new `_sync_mirrors` helper, extended `PortfolioSyncResult`, call wired into `sync_portfolio`
- `app/workers/scheduler.py` — `daily_portfolio_sync` log line picks up the new `mirrors_*` counters
- `tests/test_portfolio_sync.py` — remove local `_NOW`, import from `tests.fixtures.copy_mirrors`

All other files are untouched.

---

## Pre-push gate (run before every task's final push)

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest
```

All four must pass. This is the CLAUDE.md non-negotiable. Per-task commits may skip `pyright` / full `pytest` for iteration speed, but the final commit at the end of each task must pass all four and the smoke gate (`tests/smoke/test_app_boots.py`).

---

## Task 1: Schema migration 022

**Files:**

- Create: `sql/022_copy_trading_tables.sql`
- Test: `tests/smoke/test_app_boots.py` (existing — verifies dev DB bootstrap still boots after the migration applies)

- [ ] **Step 1: Write the migration file**

```sql
-- Migration 022: copy trading ingestion (Track 1a)
--
-- Adds three sibling tables so the eToro /portfolio payload's
-- clientPortfolio.mirrors[] data can be ingested first-class:
--
--   copy_traders          — one row per eToro trader identity
--   copy_mirrors          — one row per copy relationship (mirror_id)
--   copy_mirror_positions — one row per nested position inside a mirror
--
-- Existing tables (positions, cash_ledger, positions.source) are
-- untouched. The execution guard's rule queries continue to read
-- FROM positions only; mirrors inflate AUM via a separate query in
-- Track 1b (#187) — this migration is the schema prerequisite.
--
-- Soft-close semantics: copy_mirrors.active / closed_at columns let
-- a mirror that disappears from the payload be marked closed rather
-- than deleted. Nested positions are retained on soft-closed mirrors
-- for audit. See spec §1 and §2.3.4.
--
-- Issue: #183

BEGIN;

CREATE TABLE copy_traders (
    parent_cid      BIGINT PRIMARY KEY,
    parent_username TEXT   NOT NULL,

    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX copy_traders_username_idx ON copy_traders (parent_username);

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

    active      BOOLEAN     NOT NULL DEFAULT TRUE,
    closed_at   TIMESTAMPTZ NULL,

    raw_payload JSONB NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX copy_mirrors_parent_cid_idx ON copy_mirrors (parent_cid);
CREATE INDEX copy_mirrors_active_idx     ON copy_mirrors (active) WHERE active;

CREATE TABLE copy_mirror_positions (
    mirror_id   BIGINT NOT NULL REFERENCES copy_mirrors(mirror_id) ON DELETE CASCADE,
    position_id BIGINT NOT NULL,
    PRIMARY KEY (mirror_id, position_id),

    parent_position_id BIGINT NOT NULL,
    instrument_id      BIGINT NOT NULL,

    is_buy                    BOOLEAN         NOT NULL,
    units                     NUMERIC(20, 8)  NOT NULL,
    amount                    NUMERIC(20, 4)  NOT NULL,
    initial_amount_in_dollars NUMERIC(20, 4)  NOT NULL,
    open_rate                 NUMERIC(20, 6)  NOT NULL,
    open_conversion_rate      NUMERIC(20, 10) NOT NULL,
    open_date_time            TIMESTAMPTZ     NOT NULL,
    take_profit_rate          NUMERIC(20, 6),
    stop_loss_rate            NUMERIC(20, 6),
    total_fees                NUMERIC(20, 4)  NOT NULL DEFAULT 0,
    leverage                  INTEGER         NOT NULL DEFAULT 1,

    raw_payload JSONB NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX copy_mirror_positions_instrument_id_idx
    ON copy_mirror_positions (instrument_id);

COMMIT;
```

- [ ] **Step 2: Verify migration applies cleanly to `ebull_test`**

Run: `uv run python -c "import psycopg; from tests.test_operator_setup_race import _test_database_url, _apply_migrations_to_test_db; _apply_migrations_to_test_db(); conn = psycopg.connect(_test_database_url()); cur = conn.cursor(); cur.execute(\"SELECT COUNT(*) FROM copy_traders\"); print(cur.fetchone()); conn.close()"`

Expected: prints `(0,)` — table exists and is empty.

Alternative (if the one-liner is awkward on bash/win): drop `ebull_test`, let `_test_db_available()` recreate it on next pytest run, and confirm the run logs show migration 022 applied.

- [ ] **Step 3: Verify smoke gate stays green against dev DB**

Run: `uv run pytest tests/smoke/test_app_boots.py -v`

Expected: PASS. The lifespan auto-applies pending migrations at boot, so `ebull` also gets 022.

- [ ] **Step 4: Commit**

```bash
git add sql/022_copy_trading_tables.sql
git commit -m "feat(#183): migration 022 — copy trading ingestion tables

Adds copy_traders, copy_mirrors, copy_mirror_positions with the
indices from spec §1. Soft-close via copy_mirrors.active/closed_at.
Partial index on copy_mirrors(active) WHERE active keeps the AUM
denominator scan cheap as closed mirrors accumulate.

No data backfill — tables fill on the next portfolio sync."
```

---

## Task 2: Fixtures package scaffold + canonical constants

**Files:**

- Create: `tests/fixtures/__init__.py`
- Create: `tests/fixtures/copy_mirrors.py`

- [ ] **Step 1: Create the package marker**

```python
# tests/fixtures/__init__.py
```

(Empty file — just makes `tests.fixtures` importable.)

- [ ] **Step 2: Create the copy-mirrors fixture module with constants only**

```python
# tests/fixtures/copy_mirrors.py
"""Shared test fixtures for copy-trading ingestion (spec §8.0).

This module owns the canonical `_NOW` constant used by every test
that exercises the mirror sync soft-close path. The value is
pinned to a frozen UTC timestamp so that `_sync_mirrors`'s
`UPDATE ... closed_at = %(now)s` clause produces a deterministic
stored value and tests can assert the exact round-trip.

It also owns `_GUARD_INSTRUMENT_ID` and `_GUARD_INSTRUMENT_SECTOR`
— the deterministic instrument-row identifiers used by the
guard-path fixtures delivered in Track 1b (#187). They are
declared here in Track 1a so all callers import them from one
place once Track 1b lands.

Track 1a ships the constants and the parser/sync fixture
builders (`two_mirror_payload`, `parse_failure_payload`,
`two_mirror_seed_rows`). Track 1b adds `mirror_aum_fixture`,
`no_quote_mirror_fixture`, `mtm_delta_mirror_fixture` on top.
"""

from __future__ import annotations

from datetime import UTC, datetime

# Frozen "now" for every sync-side test. Matches the value
# tests/test_portfolio_sync.py used locally before this refactor
# (bit-identical — no behaviour change).
_NOW: datetime = datetime(2026, 4, 10, 5, 30, tzinfo=UTC)

# Guard test instrument — chosen well above any seed data in
# sql/001_init.sql so it cannot collide with real instruments.
# Track 1b's guard-integration test fixtures reuse it.
_GUARD_INSTRUMENT_ID: int = 990001
_GUARD_INSTRUMENT_SECTOR: str = "technology"
```

- [ ] **Step 3: Verify imports resolve**

Run: `uv run python -c "from tests.fixtures.copy_mirrors import _NOW, _GUARD_INSTRUMENT_ID, _GUARD_INSTRUMENT_SECTOR; print(_NOW, _GUARD_INSTRUMENT_ID, _GUARD_INSTRUMENT_SECTOR)"`

Expected: `2026-04-10 05:30:00+00:00 990001 technology`

- [ ] **Step 4: Run ruff + pyright on the new file**

Run: `uv run ruff check tests/fixtures/ && uv run ruff format --check tests/fixtures/ && uv run pyright tests/fixtures/`

Expected: all three pass.

- [ ] **Step 5: Commit**

```bash
git add tests/fixtures/__init__.py tests/fixtures/copy_mirrors.py
git commit -m "test(#183): new tests/fixtures/copy_mirrors module

Owns the canonical _NOW timestamp and the _GUARD_INSTRUMENT_ID
constants shared across Track 1a parser/sync tests and Track 1b
AUM tests. Empty of fixture builders yet — those land in later
tasks."
```

---

## Task 3: Migrate `tests/test_portfolio_sync.py` to import `_NOW` from the fixture module

**Files:**

- Modify: `tests/test_portfolio_sync.py:20` — delete local declaration, add import

- [ ] **Step 1: Verify current test passes before refactor**

Run: `uv run pytest tests/test_portfolio_sync.py -v`

Expected: PASS (baseline).

- [ ] **Step 2: Replace the local constant with an import**

Current at `tests/test_portfolio_sync.py:13-20`:

```python
from app.providers.broker import BrokerPortfolio, BrokerPosition
from app.services.portfolio_sync import (
    PortfolioSyncResult,
    _aggregate_by_instrument,
    sync_portfolio,
)

_NOW = datetime(2026, 4, 10, 5, 30, tzinfo=UTC)
```

Replace with:

```python
from app.providers.broker import BrokerPortfolio, BrokerPosition
from app.services.portfolio_sync import (
    PortfolioSyncResult,
    _aggregate_by_instrument,
    sync_portfolio,
)
from tests.fixtures.copy_mirrors import _NOW
```

(The `from datetime import UTC, datetime` import at the top of the file stays — it may still be referenced by other code in the file.)

- [ ] **Step 3: Verify nothing else in the file re-declares `_NOW`**

Run: `uv run python -c "import tests.test_portfolio_sync as m; print(m._NOW)"`

Expected: `2026-04-10 05:30:00+00:00`

- [ ] **Step 4: Re-run the test module to verify behaviour unchanged**

Run: `uv run pytest tests/test_portfolio_sync.py -v`

Expected: PASS, same test count, no new warnings.

- [ ] **Step 5: Commit**

```bash
git add tests/test_portfolio_sync.py
git commit -m "test(#183): import _NOW from tests/fixtures/copy_mirrors

Removes the test→test import inversion where a fixture value
lived in a test module. Value is bit-identical to the previous
local declaration. Addresses Codex v5 finding AI."
```

---

## Task 4: Broker interface — `BrokerMirrorPosition`, `BrokerMirror`, `mirrors` field

**Files:**

- Modify: `app/providers/broker.py` — add two frozen dataclasses, add `mirrors` field
- Test: `tests/test_broker_provider.py` (existing — add dataclass round-trip test)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_broker_provider.py`:

```python
from datetime import UTC, datetime
from decimal import Decimal

from app.providers.broker import (
    BrokerMirror,
    BrokerMirrorPosition,
    BrokerPortfolio,
    BrokerPosition,
)


def test_broker_mirror_position_round_trip() -> None:
    pos = BrokerMirrorPosition(
        position_id=1001,
        parent_position_id=5001,
        instrument_id=42,
        is_buy=True,
        units=Decimal("6.28927"),
        amount=Decimal("101.08"),
        initial_amount_in_dollars=Decimal("101.08"),
        open_rate=Decimal("1207.4994"),
        open_conversion_rate=Decimal("0.01331"),
        open_date_time=datetime(2026, 4, 10, 0, 0, tzinfo=UTC),
        take_profit_rate=None,
        stop_loss_rate=None,
        total_fees=Decimal("0"),
        leverage=1,
        raw_payload={"positionID": 1001},
    )
    assert pos.units == Decimal("6.28927")
    assert pos.open_conversion_rate == Decimal("0.01331")
    assert pos.is_buy is True
    assert pos.raw_payload["positionID"] == 1001


def test_broker_mirror_round_trip() -> None:
    mirror = BrokerMirror(
        mirror_id=15712187,
        parent_cid=111,
        parent_username="thomaspj",
        initial_investment=Decimal("20000"),
        deposit_summary=Decimal("0"),
        withdrawal_summary=Decimal("0"),
        available_amount=Decimal("2800.33"),
        closed_positions_net_profit=Decimal("-110.34"),
        stop_loss_percentage=None,
        stop_loss_amount=None,
        mirror_status_id=None,
        mirror_calculation_type=None,
        pending_for_closure=False,
        started_copy_date=datetime(2025, 1, 1, tzinfo=UTC),
        positions=(),
        raw_payload={"mirrorID": 15712187},
    )
    assert mirror.mirror_id == 15712187
    assert mirror.parent_username == "thomaspj"
    assert mirror.positions == ()


def test_broker_portfolio_mirrors_defaults_to_empty_tuple() -> None:
    """Existing callers must still be able to construct BrokerPortfolio
    without supplying mirrors (spec §2.1 non-breaking addition)."""
    portfolio = BrokerPortfolio(
        positions=(),
        available_cash=Decimal("0"),
        raw_payload={},
    )
    assert portfolio.mirrors == ()
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/test_broker_provider.py::test_broker_mirror_position_round_trip -v`

Expected: FAIL with `ImportError: cannot import name 'BrokerMirror' from 'app.providers.broker'`.

- [ ] **Step 3: Add the dataclasses and the field**

Edit `app/providers/broker.py`. Add imports:

```python
from datetime import datetime
```

(append after existing `from decimal import Decimal` at line 17).

Insert two new dataclasses between `BrokerPosition` (line 35-43) and `BrokerPortfolio` (line 46-52):

```python
@dataclass(frozen=True)
class BrokerMirrorPosition:
    """A single nested position inside a copy-trader mirror.

    `amount` is the pre-converted USD cost basis reported by eToro.
    `open_rate` is the entry price in the instrument's native
    currency; `open_conversion_rate` is the native→USD FX rate at
    open. Both are required — see spec §1.3 "openConversionRate NOT
    NULL" for the AUM correctness reason.
    """

    position_id: int
    parent_position_id: int
    instrument_id: int
    is_buy: bool
    units: Decimal
    amount: Decimal
    initial_amount_in_dollars: Decimal
    open_rate: Decimal
    open_conversion_rate: Decimal
    open_date_time: datetime
    take_profit_rate: Decimal | None
    stop_loss_rate: Decimal | None
    total_fees: Decimal
    leverage: int
    raw_payload: dict[str, Any]


@dataclass(frozen=True)
class BrokerMirror:
    """A single copy-trading mirror (one per copy session with a trader)."""

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
```

Modify `BrokerPortfolio` (currently line 46-52) to add the `mirrors` field with a default:

```python
@dataclass(frozen=True)
class BrokerPortfolio:
    """Snapshot of the broker account: positions + available cash + mirrors."""

    positions: Sequence[BrokerPosition]
    available_cash: Decimal
    raw_payload: dict[str, Any]
    mirrors: Sequence[BrokerMirror] = ()
```

The default preserves both existing constructor call sites (`etoro_broker.py:456`, `tests/test_portfolio_sync.py:56`) unchanged at the type level — see spec §2.1 rationale.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_broker_provider.py -v`

Expected: all three new tests PASS plus existing tests still PASS.

- [ ] **Step 5: Lint + typecheck**

Run: `uv run ruff check app/providers/broker.py tests/test_broker_provider.py && uv run pyright app/providers/broker.py`

Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add app/providers/broker.py tests/test_broker_provider.py
git commit -m "feat(#183): add BrokerMirror/BrokerMirrorPosition dataclasses

New frozen dataclasses for copy-trading mirror payload parsing,
plus additive 'mirrors: Sequence[BrokerMirror] = ()' field on
BrokerPortfolio. Default preserves the two existing constructor
call sites (etoro_broker.get_portfolio, tests/test_portfolio_sync)
unchanged — see spec §2.1."
```

---

## Task 5: `PortfolioParseError` exception class + hierarchy test

**Files:**

- Modify: `app/providers/implementations/etoro_broker.py` — add new exception class near the top of the module (below existing imports, above the class definition)
- Create: `tests/test_copy_mirrors_parser.py` — new test module for §8.1 parser tests

- [ ] **Step 1: Write the failing hierarchy test**

```python
# tests/test_copy_mirrors_parser.py
"""§8.1 parser unit tests for copy-trading mirror ingestion.

Pure unit tests — no DB, no I/O, no broker HTTP. Exercises
_parse_mirror / _parse_mirror_position and the outer top-level
loop in etoro_broker.get_portfolio's mirrors[] branch.
"""

from __future__ import annotations

import decimal

import pytest

from app.providers.implementations.etoro_broker import PortfolioParseError


def test_portfolio_parse_error_is_direct_exception_subclass() -> None:
    """Spec §2.2.1: PortfolioParseError MUST subclass Exception directly.

    If it subclassed ValueError / TypeError / KeyError /
    decimal.DecimalException, the outer parse loop's
    `except (KeyError, ValueError, TypeError, decimal.DecimalException)`
    block would silently swallow it, defeating the §2.3.3 strict-raise
    and enabling the §2.3.4 soft-close hole Codex v3 finding V flagged.
    """
    assert issubclass(PortfolioParseError, Exception) is True
    assert issubclass(PortfolioParseError, ValueError) is False
    assert issubclass(PortfolioParseError, TypeError) is False
    assert issubclass(PortfolioParseError, KeyError) is False
    assert issubclass(PortfolioParseError, decimal.DecimalException) is False


def test_portfolio_parse_error_is_raisable_with_cause() -> None:
    inner = ValueError("boom")
    with pytest.raises(PortfolioParseError) as excinfo:
        raise PortfolioParseError("wrap") from inner
    assert excinfo.value.__cause__ is inner
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/test_copy_mirrors_parser.py -v`

Expected: FAIL with `ImportError: cannot import name 'PortfolioParseError' from 'app.providers.implementations.etoro_broker'`.

- [ ] **Step 3: Add the exception class**

Edit `app/providers/implementations/etoro_broker.py`. Find the existing module docstring / logger setup / class definition, and add immediately above the `class EtoroBrokerProvider` line:

```python
class PortfolioParseError(Exception):
    """Raised when a mirrors[] row cannot be parsed safely.

    Directly subclasses Exception (NOT ValueError / TypeError /
    KeyError / decimal.DecimalException) so the outer parse loop can
    distinguish it from incidental exceptions and re-raise. Never
    swallowed by any `except (KeyError, ValueError, TypeError,
    decimal.DecimalException)` block.

    See spec §2.2.1 for the hierarchy rationale and §2.3.3 for the
    strict-raise sync contract that depends on it.
    """
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `uv run pytest tests/test_copy_mirrors_parser.py -v`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add app/providers/implementations/etoro_broker.py tests/test_copy_mirrors_parser.py
git commit -m "feat(#183): add PortfolioParseError exception class

Direct Exception subclass (not ValueError/TypeError/KeyError/
DecimalException) so the outer parse loop's catch-list cannot
swallow it. See spec §2.2.1 and Codex v3 finding U."
```

---

## Task 6: `_parse_mirror_position` — happy path + required-field failures

**Files:**

- Modify: `app/providers/implementations/etoro_broker.py` — add `_parse_mirror_position` pure helper below `_normalise_order_info_response` (the existing normaliser pattern)
- Modify: `tests/test_copy_mirrors_parser.py` — add §8.1 tests for the position parser

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_copy_mirrors_parser.py`:

```python
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from app.providers.broker import BrokerMirrorPosition
from app.providers.implementations.etoro_broker import _parse_mirror_position


def _make_position_payload(**overrides: Any) -> dict[str, Any]:
    """Return a valid mirror-position payload; override any field."""
    base: dict[str, Any] = {
        "positionID": 1001,
        "parentPositionID": 5001,
        "instrumentID": 42,
        "isBuy": True,
        "units": "6.28927",
        "amount": "101.08",
        "initialAmountInDollars": "101.08",
        "openRate": "1207.4994",
        "openConversionRate": "0.01331",
        "openDateTime": "2026-04-10T00:00:00Z",
        "takeProfitRate": None,
        "stopLossRate": None,
        "totalFees": "0",
        "leverage": 1,
    }
    base.update(overrides)
    return base


def test_parse_mirror_position_happy_path_non_usd() -> None:
    payload = _make_position_payload()
    pos = _parse_mirror_position(payload)
    assert isinstance(pos, BrokerMirrorPosition)
    assert pos.position_id == 1001
    assert pos.instrument_id == 42
    assert pos.is_buy is True
    assert pos.units == Decimal("6.28927")
    assert pos.open_rate == Decimal("1207.4994")
    assert pos.open_conversion_rate == Decimal("0.01331")  # FX round-trip
    assert pos.open_date_time == datetime(2026, 4, 10, 0, 0, tzinfo=UTC)
    assert pos.take_profit_rate is None
    assert pos.stop_loss_rate is None
    assert pos.total_fees == Decimal("0")
    assert pos.leverage == 1
    assert pos.raw_payload is payload  # stored as-is


def test_parse_mirror_position_missing_open_conversion_rate_raises() -> None:
    """Spec §2.2.2: openConversionRate is a required field in prod
    — no silent default. A mirror-position without it raises."""
    payload = _make_position_payload()
    del payload["openConversionRate"]
    with pytest.raises(KeyError):
        _parse_mirror_position(payload)


def test_parse_mirror_position_non_numeric_units_raises_decimal_exc() -> None:
    """Spec §2.2.2 + §8.1: Decimal(str('bogus')) raises
    decimal.InvalidOperation, a subclass of DecimalException —
    NOT a ValueError. This test pins the exception type so the
    caller's `except DecimalException` clause catches correctly."""
    payload = _make_position_payload(units="bogus")
    with pytest.raises(decimal.DecimalException):
        _parse_mirror_position(payload)


def test_parse_mirror_position_optional_fields_none() -> None:
    payload = _make_position_payload(takeProfitRate=None, stopLossRate=None)
    pos = _parse_mirror_position(payload)
    assert pos.take_profit_rate is None
    assert pos.stop_loss_rate is None


def test_parse_mirror_position_optional_fields_present() -> None:
    payload = _make_position_payload(takeProfitRate="1500.0", stopLossRate="1000.0")
    pos = _parse_mirror_position(payload)
    assert pos.take_profit_rate == Decimal("1500.0")
    assert pos.stop_loss_rate == Decimal("1000.0")
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_copy_mirrors_parser.py -v`

Expected: FAIL — `_parse_mirror_position` is not yet importable.

- [ ] **Step 3: Implement `_parse_mirror_position`**

Edit `app/providers/implementations/etoro_broker.py`. Add a `decimal` import at the top if not already present:

```python
import decimal
from datetime import datetime
from decimal import Decimal
```

Add the helper below `_normalise_order_info_response` (near the existing normalisers at line 549+):

```python
def _parse_mirror_position(payload: dict[str, Any]) -> BrokerMirrorPosition:
    """Parse a nested copy-mirror position payload into a typed dataclass.

    Pure normaliser — no I/O, no instance state. Required fields
    raise KeyError on absence; numeric fields go through
    Decimal(str(value)) and raise decimal.InvalidOperation
    (a subclass of decimal.DecimalException) on non-numeric input.
    The caller (_parse_mirror) wraps both exception types in a
    PortfolioParseError with position-index attribution.

    openConversionRate is required — see spec §2.2.2 and the
    74/198 non-USD positions on demo mirror 15712187 that would
    otherwise be AUM-nonsense.
    """

    def _opt_decimal(key: str) -> Decimal | None:
        value = payload.get(key)
        if value is None:
            return None
        return Decimal(str(value))

    return BrokerMirrorPosition(
        position_id=int(payload["positionID"]),
        parent_position_id=int(payload["parentPositionID"]),
        instrument_id=int(payload["instrumentID"]),
        is_buy=bool(payload["isBuy"]),
        units=Decimal(str(payload["units"])),
        amount=Decimal(str(payload["amount"])),
        initial_amount_in_dollars=Decimal(str(payload["initialAmountInDollars"])),
        open_rate=Decimal(str(payload["openRate"])),
        open_conversion_rate=Decimal(str(payload["openConversionRate"])),
        open_date_time=_parse_iso_datetime(payload["openDateTime"]),
        take_profit_rate=_opt_decimal("takeProfitRate"),
        stop_loss_rate=_opt_decimal("stopLossRate"),
        total_fees=Decimal(str(payload.get("totalFees", "0"))),
        leverage=int(payload.get("leverage", 1)),
        raw_payload=payload,
    )


def _parse_iso_datetime(value: str) -> datetime:
    """Parse an ISO-8601 datetime string from an eToro payload.

    eToro returns `2026-04-10T00:00:00Z`; Python's fromisoformat
    below 3.11 rejects the trailing `Z`, so we normalise to `+00:00`.
    """
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value)
```

You will also need to import `BrokerMirrorPosition` at the top of the file:

```python
from app.providers.broker import (
    BrokerMirror,
    BrokerMirrorPosition,
    BrokerOrderResult,
    BrokerPortfolio,
    BrokerPosition,
)
```

(Only add the names that aren't already imported.)

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_copy_mirrors_parser.py -v`

Expected: all six tests PASS.

- [ ] **Step 5: Lint + typecheck**

Run: `uv run ruff check app/providers/implementations/etoro_broker.py tests/test_copy_mirrors_parser.py && uv run pyright app/providers/implementations/etoro_broker.py`

Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add app/providers/implementations/etoro_broker.py tests/test_copy_mirrors_parser.py
git commit -m "feat(#183): add _parse_mirror_position normaliser

Pure helper parsing nested copy-mirror position payloads into
BrokerMirrorPosition dataclasses. Required fields raise on
absence; numeric fields raise decimal.InvalidOperation on
non-numeric input. openConversionRate is required in prod — see
spec §2.2.2. Exception wrapping into PortfolioParseError with
position-index attribution lives in the next task."
```

---

## Task 7: `_parse_mirror` — nested wrap with position-index attribution

**Files:**

- Modify: `app/providers/implementations/etoro_broker.py` — add `_parse_mirror` below `_parse_mirror_position`
- Modify: `tests/test_copy_mirrors_parser.py` — add tests for happy path + nested failure wrap

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_copy_mirrors_parser.py`:

```python
from app.providers.broker import BrokerMirror
from app.providers.implementations.etoro_broker import _parse_mirror


def _make_mirror_payload(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "mirrorID": 15712187,
        "parentCID": 111,
        "parentUsername": "thomaspj",
        "initialInvestment": "20000",
        "depositSummary": "0",
        "withdrawalSummary": "0",
        "availableAmount": "2800.33",
        "closedPositionsNetProfit": "-110.34",
        "stopLossPercentage": None,
        "stopLossAmount": None,
        "mirrorStatusID": None,
        "mirrorCalculationType": None,
        "pendingForClosure": False,
        "startedCopyDate": "2025-01-01T00:00:00Z",
        "positions": [_make_position_payload(positionID=1001)],
    }
    base.update(overrides)
    return base


def test_parse_mirror_happy_path() -> None:
    payload = _make_mirror_payload()
    mirror = _parse_mirror(payload)
    assert isinstance(mirror, BrokerMirror)
    assert mirror.mirror_id == 15712187
    assert mirror.parent_cid == 111
    assert mirror.parent_username == "thomaspj"
    assert mirror.available_amount == Decimal("2800.33")
    assert mirror.closed_positions_net_profit == Decimal("-110.34")
    assert len(mirror.positions) == 1
    assert mirror.positions[0].position_id == 1001
    assert mirror.started_copy_date == datetime(2025, 1, 1, tzinfo=UTC)
    assert mirror.raw_payload is payload


def test_parse_mirror_empty_positions_is_valid() -> None:
    """A mirror with positions == [] is a valid state (holds only cash).

    §2.2.2: raw_positions == [] yields positions=(), which the §3.2
    AUM formula in Track 1b handles as mirror_equity = available_amount.
    Nothing raises.
    """
    payload = _make_mirror_payload(positions=[])
    mirror = _parse_mirror(payload)
    assert mirror.positions == ()


def test_parse_mirror_nested_failure_wraps_with_index() -> None:
    """Spec §2.2.2: inner loop catches (KeyError, ValueError, TypeError,
    DecimalException) and re-raises as PortfolioParseError with both
    the mirror_id AND the position index in the message."""
    bad_pos = _make_position_payload(positionID=9999, units="bogus")
    payload = _make_mirror_payload(
        positions=[
            _make_position_payload(positionID=1001),
            _make_position_payload(positionID=1002),
            bad_pos,  # idx 2 — this is the failing one
        ]
    )
    with pytest.raises(PortfolioParseError) as excinfo:
        _parse_mirror(payload)
    msg = str(excinfo.value)
    assert "15712187" in msg
    assert "position[2]" in msg
    assert isinstance(excinfo.value.__cause__, decimal.InvalidOperation)


def test_parse_mirror_nested_key_error_wraps() -> None:
    """Missing openConversionRate in a nested position raises
    KeyError from _parse_mirror_position, which _parse_mirror's
    inner wrap catches and re-raises as PortfolioParseError."""
    bad_pos = _make_position_payload(positionID=9999)
    del bad_pos["openConversionRate"]
    payload = _make_mirror_payload(positions=[bad_pos])
    with pytest.raises(PortfolioParseError) as excinfo:
        _parse_mirror(payload)
    assert "15712187" in str(excinfo.value)
    assert "position[0]" in str(excinfo.value)
    assert isinstance(excinfo.value.__cause__, KeyError)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_copy_mirrors_parser.py -v`

Expected: FAIL — `_parse_mirror` not yet importable.

- [ ] **Step 3: Implement `_parse_mirror`**

Append to `app/providers/implementations/etoro_broker.py` (below `_parse_iso_datetime`):

```python
def _parse_mirror(payload: dict[str, Any]) -> BrokerMirror:
    """Parse a top-level copy-trading mirror payload.

    Nested positions are iterated under an inner try/except that
    wraps (KeyError, ValueError, TypeError, decimal.DecimalException)
    in PortfolioParseError with mirror_id + position index
    attribution. See spec §2.2.2 for why the inner wrap is mandatory
    — without it, a single malformed nested position degrades to a
    top-level error message that cannot tell the operator *which*
    row failed.

    Top-level numeric/string extraction may also raise
    (KeyError / ValueError / TypeError / DecimalException); those
    propagate up to the outer get_portfolio loop where §2.2.2's
    fallback wrap catches and re-raises as PortfolioParseError
    keyed on the mirror_id alone.
    """
    raw_positions = payload.get("positions") or []
    parsed_positions: list[BrokerMirrorPosition] = []
    for idx, pos in enumerate(raw_positions):
        try:
            parsed_positions.append(_parse_mirror_position(pos))
        except (KeyError, ValueError, TypeError, decimal.DecimalException) as exc:
            raise PortfolioParseError(
                f"Mirror {payload.get('mirrorID')!r} position[{idx}]: {exc}"
            ) from exc

    def _opt_decimal(key: str) -> Decimal | None:
        value = payload.get(key)
        if value is None:
            return None
        return Decimal(str(value))

    def _opt_int(key: str) -> int | None:
        value = payload.get(key)
        if value is None:
            return None
        return int(value)

    return BrokerMirror(
        mirror_id=int(payload["mirrorID"]),
        parent_cid=int(payload["parentCID"]),
        parent_username=str(payload["parentUsername"]),
        initial_investment=Decimal(str(payload["initialInvestment"])),
        deposit_summary=Decimal(str(payload.get("depositSummary", "0"))),
        withdrawal_summary=Decimal(str(payload.get("withdrawalSummary", "0"))),
        available_amount=Decimal(str(payload["availableAmount"])),
        closed_positions_net_profit=Decimal(
            str(payload["closedPositionsNetProfit"])
        ),
        stop_loss_percentage=_opt_decimal("stopLossPercentage"),
        stop_loss_amount=_opt_decimal("stopLossAmount"),
        mirror_status_id=_opt_int("mirrorStatusID"),
        mirror_calculation_type=_opt_int("mirrorCalculationType"),
        pending_for_closure=bool(payload.get("pendingForClosure", False)),
        started_copy_date=_parse_iso_datetime(payload["startedCopyDate"]),
        positions=tuple(parsed_positions),
        raw_payload=payload,
    )
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_copy_mirrors_parser.py -v`

Expected: all tests PASS (including the earlier position-level tests).

- [ ] **Step 5: Lint + typecheck**

Run: `uv run ruff check app/providers/implementations/etoro_broker.py tests/test_copy_mirrors_parser.py && uv run pyright app/providers/implementations/etoro_broker.py`

Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add app/providers/implementations/etoro_broker.py tests/test_copy_mirrors_parser.py
git commit -m "feat(#183): add _parse_mirror with nested-wrap attribution

Inner loop catches (KeyError, ValueError, TypeError,
DecimalException) at per-nested-position granularity and
re-raises as PortfolioParseError with mirror_id + position[idx]
context. Empty positions[] is a valid state (mirror holds only
cash). See spec §2.2.2."
```

---

## Task 8: Wire mirror parsing into `get_portfolio`

**Files:**

- Modify: `app/providers/implementations/etoro_broker.py` — extend `get_portfolio` (line 403-460) to parse `mirrors[]` after the existing positions loop
- Modify: `tests/test_copy_mirrors_parser.py` — add top-level loop tests (unrecognisable skip, known-mirror raise, fallback wrap)

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_copy_mirrors_parser.py`:

```python
import logging

from app.providers.implementations.etoro_broker import (
    _parse_mirrors_payload,
)


def test_parse_mirrors_payload_happy_path_two_mirrors() -> None:
    raw = [_make_mirror_payload(mirrorID=1), _make_mirror_payload(mirrorID=2)]
    result = _parse_mirrors_payload(raw)
    assert len(result) == 2
    assert result[0].mirror_id == 1
    assert result[1].mirror_id == 2


def test_parse_mirrors_payload_skips_unrecognisable_no_mirror_id(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Spec §2.2.2: the ONLY surviving log-and-skip path is a row
    with no usable mirrorID — it cannot collide with any known
    local row, so it is safe to skip."""
    raw = [
        {"not a mirror": True},  # no mirrorID → safe skip
        "not even a dict",  # not a dict → safe skip
        _make_mirror_payload(mirrorID=42),  # valid → parsed
    ]
    with caplog.at_level(logging.WARNING):
        result = _parse_mirrors_payload(raw)
    assert len(result) == 1
    assert result[0].mirror_id == 42
    assert any("unrecognisable" in rec.message.lower() for rec in caplog.records)


def test_parse_mirrors_payload_known_mirror_top_level_failure_raises() -> None:
    """Spec §2.2.2: a row with a recognisable mirrorID but a
    missing/malformed required top-level field raises
    PortfolioParseError — NOT log-and-skip. Otherwise the sync
    would then interpret this as a disappearance and soft-close
    the local row (Codex v3 finding V parse-and-soft-close hole)."""
    bad = _make_mirror_payload(mirrorID=15712187)
    del bad["availableAmount"]
    raw = [bad, _make_mirror_payload(mirrorID=42)]
    with pytest.raises(PortfolioParseError) as excinfo:
        _parse_mirrors_payload(raw)
    assert "15712187" in str(excinfo.value)
    # The underlying cause is a KeyError on the missing key.
    assert isinstance(excinfo.value.__cause__, KeyError)


def test_parse_mirrors_payload_known_mirror_decimal_failure_raises() -> None:
    """Non-numeric top-level availableAmount raises
    decimal.InvalidOperation, which the outer fallback catch wraps
    as PortfolioParseError with mirror_id attribution."""
    bad = _make_mirror_payload(mirrorID=15712187, availableAmount="bogus")
    with pytest.raises(PortfolioParseError) as excinfo:
        _parse_mirrors_payload([bad])
    assert "15712187" in str(excinfo.value)
    assert isinstance(excinfo.value.__cause__, decimal.InvalidOperation)


def test_parse_mirrors_payload_nested_failure_propagates_unchanged() -> None:
    """Spec §2.2.2: the outer loop's `except PortfolioParseError: raise`
    preserves the inner-loop's position[idx] attribution."""
    bad_pos = _make_position_payload(positionID=9999, units="bogus")
    bad_mirror = _make_mirror_payload(
        mirrorID=15712187, positions=[bad_pos]
    )
    with pytest.raises(PortfolioParseError) as excinfo:
        _parse_mirrors_payload([bad_mirror])
    assert "15712187" in str(excinfo.value)
    assert "position[0]" in str(excinfo.value)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_copy_mirrors_parser.py -v`

Expected: FAIL — `_parse_mirrors_payload` not yet importable.

- [ ] **Step 3: Add `_parse_mirrors_payload` helper and wire it into `get_portfolio`**

Append to `app/providers/implementations/etoro_broker.py` (below `_parse_mirror`):

```python
def _parse_mirrors_payload(
    raw_mirrors: Sequence[Any],
) -> list[BrokerMirror]:
    """Parse clientPortfolio.mirrors[] into a list of BrokerMirror.

    Implements the outer top-level loop from spec §2.2.2:

    1. Rows that are not dicts, or dicts with no `mirrorID` key, are
       logged and skipped (the ONLY surviving log-and-skip path —
       they cannot collide with any known local row, so silent skip
       is safe).
    2. Rows with a recognisable `mirrorID` are parsed via
       `_parse_mirror`. Any failure raises PortfolioParseError —
       log-and-skip on a known mirror_id would look like a
       disappearance to §2.3.4's soft-close and silently destroy
       the local row.
    3. PortfolioParseError raised by the nested-position wrap inside
       `_parse_mirror` is re-raised unchanged so the caller sees the
       `position[idx]` attribution.
    4. Any other exception escaping `_parse_mirror` (KeyError,
       ValueError, TypeError, decimal.DecimalException) is
       fallback-wrapped in PortfolioParseError with mirror_id-only
       attribution.
    """
    mirrors: list[BrokerMirror] = []
    for m in raw_mirrors:
        if not isinstance(m, dict) or "mirrorID" not in m:
            logger.warning(
                "Skipping unrecognisable mirrors[] element: %r", m
            )
            continue

        try:
            mirrors.append(_parse_mirror(m))
        except PortfolioParseError:
            raise
        except (KeyError, ValueError, TypeError, decimal.DecimalException) as exc:
            raise PortfolioParseError(
                f"Failed to parse mirror {m.get('mirrorID')!r}: {exc}"
            ) from exc
    return mirrors
```

Then edit `get_portfolio` at `app/providers/implementations/etoro_broker.py:422`. Find the `portfolio = raw.get("clientPortfolio") or {}` line and, after `raw_positions = portfolio.get("positions") or []` (line 423), add a sibling line:

```python
raw_mirrors: list[Any] = portfolio.get("mirrors") or []
```

And at the `return BrokerPortfolio(...)` at line 456, pass the parsed mirrors:

```python
return BrokerPortfolio(
    positions=positions,
    available_cash=Decimal(str(credit)) if credit is not None else Decimal("0"),
    raw_payload=raw,
    mirrors=tuple(_parse_mirrors_payload(raw_mirrors)),
)
```

- [ ] **Step 4: Run the parser tests to verify they pass**

Run: `uv run pytest tests/test_copy_mirrors_parser.py -v`

Expected: all tests PASS.

- [ ] **Step 5: Run the full broker provider test suite to verify nothing regressed**

Run: `uv run pytest tests/test_broker_provider.py tests/test_copy_mirrors_parser.py -v`

Expected: everything PASS.

- [ ] **Step 6: Lint + typecheck**

Run: `uv run ruff check app/providers/implementations/etoro_broker.py tests/test_copy_mirrors_parser.py && uv run pyright app/providers/implementations/etoro_broker.py`

Expected: clean.

- [ ] **Step 7: Commit**

```bash
git add app/providers/implementations/etoro_broker.py tests/test_copy_mirrors_parser.py
git commit -m "feat(#183): parse clientPortfolio.mirrors[] in get_portfolio

Top-level _parse_mirrors_payload loop catches PortfolioParseError
and re-raises unchanged to preserve nested position[idx]
attribution; fallback wraps (KeyError, ValueError, TypeError,
DecimalException) on known mirrorIDs; only unrecognisable rows
(no mirrorID) are log-and-skipped. Closes the Codex v3 finding V
parse-and-soft-close hole — no known-mirror failure is silently
skipped."
```

---

## Task 9: Fixture data builders — `two_mirror_payload`, `parse_failure_payload`, `two_mirror_seed_rows`

**Files:**

- Modify: `tests/fixtures/copy_mirrors.py` — add the three named fixture builders

These are the service-layer test precursors for §8.2 and §8.3. `mirror_aum_fixture`, `no_quote_mirror_fixture`, `mtm_delta_mirror_fixture` are NOT built here — they ship in Track 1b (#187).

- [ ] **Step 1: Add the fixture builders**

Append to `tests/fixtures/copy_mirrors.py`:

```python
from collections.abc import Sequence
from decimal import Decimal
from typing import Any

import psycopg
import psycopg.rows

from app.providers.broker import (
    BrokerMirror,
    BrokerMirrorPosition,
    BrokerPortfolio,
    BrokerPosition,
)


def _make_mirror_position(
    position_id: int,
    instrument_id: int = 42,
    units: Decimal = Decimal("6.28927"),
    open_rate: Decimal = Decimal("1207.4994"),
    open_conversion_rate: Decimal = Decimal("0.01331"),
    amount: Decimal = Decimal("101.08"),
    is_buy: bool = True,
) -> BrokerMirrorPosition:
    return BrokerMirrorPosition(
        position_id=position_id,
        parent_position_id=position_id + 4000,
        instrument_id=instrument_id,
        is_buy=is_buy,
        units=units,
        amount=amount,
        initial_amount_in_dollars=amount,
        open_rate=open_rate,
        open_conversion_rate=open_conversion_rate,
        open_date_time=_NOW,
        take_profit_rate=None,
        stop_loss_rate=None,
        total_fees=Decimal("0"),
        leverage=1,
        raw_payload={
            "positionID": position_id,
            "instrumentID": instrument_id,
        },
    )


def _make_mirror(
    mirror_id: int,
    parent_cid: int,
    parent_username: str,
    positions: Sequence[BrokerMirrorPosition],
    available_amount: Decimal = Decimal("2800.33"),
    initial_investment: Decimal = Decimal("20000"),
    deposit_summary: Decimal = Decimal("0"),
    withdrawal_summary: Decimal = Decimal("0"),
    closed_positions_net_profit: Decimal = Decimal("-110.34"),
) -> BrokerMirror:
    return BrokerMirror(
        mirror_id=mirror_id,
        parent_cid=parent_cid,
        parent_username=parent_username,
        initial_investment=initial_investment,
        deposit_summary=deposit_summary,
        withdrawal_summary=withdrawal_summary,
        available_amount=available_amount,
        closed_positions_net_profit=closed_positions_net_profit,
        stop_loss_percentage=None,
        stop_loss_amount=None,
        mirror_status_id=None,
        mirror_calculation_type=None,
        pending_for_closure=False,
        started_copy_date=_NOW,
        positions=tuple(positions),
        raw_payload={"mirrorID": mirror_id, "parentCID": parent_cid},
    )


def two_mirror_payload() -> BrokerPortfolio:
    """Canonical 2 mirrors × 3 positions each BrokerPortfolio fixture.

    Derived from the real etoro_portfolio_20260411T053000Z.json
    payload — trimmed for test readability, includes at least one
    non-USD position (GBP conversion rate 1.158) so the
    openConversionRate round-trip is exercised in every test that
    uses this fixture.
    """
    mirror_a = _make_mirror(
        mirror_id=15712187,
        parent_cid=111,
        parent_username="thomaspj",
        available_amount=Decimal("2800.33"),
        initial_investment=Decimal("20000"),
        deposit_summary=Decimal("0"),
        withdrawal_summary=Decimal("0"),
        closed_positions_net_profit=Decimal("-110.34"),
        positions=[
            _make_mirror_position(
                position_id=1001,
                instrument_id=42,
                units=Decimal("6.28927"),
                open_rate=Decimal("1207.4994"),
                open_conversion_rate=Decimal("0.01331"),  # JPY
                amount=Decimal("101.08"),
            ),
            _make_mirror_position(
                position_id=1002,
                instrument_id=43,
                units=Decimal("2.0"),
                open_rate=Decimal("150.00"),
                open_conversion_rate=Decimal("1.158"),  # GBP
                amount=Decimal("347.40"),
            ),
            _make_mirror_position(
                position_id=1003,
                instrument_id=44,
                units=Decimal("10.0"),
                open_rate=Decimal("100.00"),
                open_conversion_rate=Decimal("1.0"),  # USD
                amount=Decimal("1000.00"),
            ),
        ],
    )
    mirror_b = _make_mirror(
        mirror_id=15714660,
        parent_cid=222,
        parent_username="triangulacapital",
        available_amount=Decimal("1724.11"),
        initial_investment=Decimal("17280"),
        deposit_summary=Decimal("2251"),
        withdrawal_summary=Decimal("0"),
        closed_positions_net_profit=Decimal("-140.13"),
        positions=[
            _make_mirror_position(
                position_id=2001,
                instrument_id=52,
                units=Decimal("1.0"),
                open_rate=Decimal("500.00"),
                open_conversion_rate=Decimal("1.0"),
                amount=Decimal("500.00"),
            ),
            _make_mirror_position(
                position_id=2002,
                instrument_id=53,
                units=Decimal("3.0"),
                open_rate=Decimal("200.00"),
                open_conversion_rate=Decimal("1.0"),
                amount=Decimal("600.00"),
            ),
            _make_mirror_position(
                position_id=2003,
                instrument_id=54,
                units=Decimal("5.0"),
                open_rate=Decimal("80.00"),
                open_conversion_rate=Decimal("1.0"),
                amount=Decimal("400.00"),
            ),
        ],
    )
    return BrokerPortfolio(
        positions=(),
        available_cash=Decimal("0"),
        raw_payload={},
        mirrors=(mirror_a, mirror_b),
    )


def parse_failure_payload() -> list[dict[str, Any]]:
    """Raw `clientPortfolio.mirrors[]` list with one malformed
    nested position. Used by §8.3 to prove the sync aborts before
    eviction / soft-close when the parser raises.

    Returns a raw list (not BrokerPortfolio) because the test
    exercises the parse step itself — `_parse_mirrors_payload`
    must raise on this input.
    """
    return [
        {
            "mirrorID": 15712187,
            "parentCID": 111,
            "parentUsername": "thomaspj",
            "initialInvestment": "20000",
            "depositSummary": "0",
            "withdrawalSummary": "0",
            "availableAmount": "2800.33",
            "closedPositionsNetProfit": "-110.34",
            "stopLossPercentage": None,
            "stopLossAmount": None,
            "mirrorStatusID": None,
            "mirrorCalculationType": None,
            "pendingForClosure": False,
            "startedCopyDate": "2025-01-01T00:00:00Z",
            "positions": [
                {
                    "positionID": 1001,
                    "parentPositionID": 5001,
                    "instrumentID": 42,
                    "isBuy": True,
                    "units": "bogus",  # <-- non-numeric → DecimalException
                    "amount": "101.08",
                    "initialAmountInDollars": "101.08",
                    "openRate": "1207.4994",
                    "openConversionRate": "0.01331",
                    "openDateTime": "2026-04-10T00:00:00Z",
                    "takeProfitRate": None,
                    "stopLossRate": None,
                    "totalFees": "0",
                    "leverage": 1,
                },
            ],
        }
    ]


def two_mirror_seed_rows(conn: psycopg.Connection[Any]) -> None:
    """INSERT the two_mirror_payload mirrors directly into
    copy_traders / copy_mirrors / copy_mirror_positions so
    disappearance and re-copy tests can seed the DB before
    calling sync_portfolio with a *different* payload.

    Caller is responsible for commit/rollback. Safe to run only
    against ebull_test — callers must enforce this themselves
    before calling (see _assert_test_db in test modules).
    """
    payload = two_mirror_payload()
    with conn.cursor() as cur:
        for mirror in payload.mirrors:
            cur.execute(
                """
                INSERT INTO copy_traders (parent_cid, parent_username,
                                          first_seen_at, updated_at)
                VALUES (%(cid)s, %(username)s, %(now)s, %(now)s)
                ON CONFLICT (parent_cid) DO NOTHING
                """,
                {
                    "cid": mirror.parent_cid,
                    "username": mirror.parent_username,
                    "now": _NOW,
                },
            )
            cur.execute(
                """
                INSERT INTO copy_mirrors (
                    mirror_id, parent_cid, initial_investment,
                    deposit_summary, withdrawal_summary,
                    available_amount, closed_positions_net_profit,
                    stop_loss_percentage, stop_loss_amount,
                    mirror_status_id, mirror_calculation_type,
                    pending_for_closure, started_copy_date,
                    active, closed_at, raw_payload, updated_at
                ) VALUES (
                    %(mirror_id)s, %(parent_cid)s, %(initial_investment)s,
                    %(deposit_summary)s, %(withdrawal_summary)s,
                    %(available_amount)s, %(closed_positions_net_profit)s,
                    NULL, NULL, NULL, NULL, FALSE, %(started_copy_date)s,
                    TRUE, NULL, %(raw_payload)s::jsonb, %(now)s
                )
                """,
                {
                    "mirror_id": mirror.mirror_id,
                    "parent_cid": mirror.parent_cid,
                    "initial_investment": mirror.initial_investment,
                    "deposit_summary": mirror.deposit_summary,
                    "withdrawal_summary": mirror.withdrawal_summary,
                    "available_amount": mirror.available_amount,
                    "closed_positions_net_profit": mirror.closed_positions_net_profit,
                    "started_copy_date": mirror.started_copy_date,
                    "raw_payload": psycopg.types.json.Jsonb(mirror.raw_payload),
                    "now": _NOW,
                },
            )
            for pos in mirror.positions:
                cur.execute(
                    """
                    INSERT INTO copy_mirror_positions (
                        mirror_id, position_id, parent_position_id,
                        instrument_id, is_buy, units, amount,
                        initial_amount_in_dollars, open_rate,
                        open_conversion_rate, open_date_time,
                        take_profit_rate, stop_loss_rate,
                        total_fees, leverage, raw_payload, updated_at
                    ) VALUES (
                        %(mirror_id)s, %(position_id)s, %(parent_position_id)s,
                        %(instrument_id)s, %(is_buy)s, %(units)s, %(amount)s,
                        %(initial_amount)s, %(open_rate)s,
                        %(open_conversion_rate)s, %(open_date_time)s,
                        %(take_profit_rate)s, %(stop_loss_rate)s,
                        %(total_fees)s, %(leverage)s, %(raw_payload)s::jsonb,
                        %(now)s
                    )
                    """,
                    {
                        "mirror_id": mirror.mirror_id,
                        "position_id": pos.position_id,
                        "parent_position_id": pos.parent_position_id,
                        "instrument_id": pos.instrument_id,
                        "is_buy": pos.is_buy,
                        "units": pos.units,
                        "amount": pos.amount,
                        "initial_amount": pos.initial_amount_in_dollars,
                        "open_rate": pos.open_rate,
                        "open_conversion_rate": pos.open_conversion_rate,
                        "open_date_time": pos.open_date_time,
                        "take_profit_rate": pos.take_profit_rate,
                        "stop_loss_rate": pos.stop_loss_rate,
                        "total_fees": pos.total_fees,
                        "leverage": pos.leverage,
                        "raw_payload": psycopg.types.json.Jsonb(pos.raw_payload),
                        "now": _NOW,
                    },
                )
```

At the top of the file, update the imports block to add `psycopg`:

```python
from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import psycopg
import psycopg.types.json

from app.providers.broker import (
    BrokerMirror,
    BrokerMirrorPosition,
    BrokerPortfolio,
)
```

(The `BrokerPosition` import is not needed yet — Track 1b tests that use mirror_aum_fixture will add it. `psycopg.rows` is not imported here: `two_mirror_seed_rows` uses a plain `conn.cursor()` with the default tuple row factory, so the submodule reference is unnecessary — adding it would be an unused import ruff failure.)

- [ ] **Step 2: Verify imports and basic shapes**

Run: `uv run python -c "from tests.fixtures.copy_mirrors import two_mirror_payload, parse_failure_payload; p = two_mirror_payload(); print(len(p.mirrors), len(p.mirrors[0].positions)); print(len(parse_failure_payload()))"`

Expected: `2 3` and `1`.

- [ ] **Step 3: Lint + typecheck**

Run: `uv run ruff check tests/fixtures/copy_mirrors.py && uv run ruff format --check tests/fixtures/copy_mirrors.py && uv run pyright tests/fixtures/copy_mirrors.py`

Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add tests/fixtures/copy_mirrors.py
git commit -m "test(#183): add two_mirror_payload and seed helper fixtures

Three named builders per spec §8.0: two_mirror_payload (2×3
canonical BrokerPortfolio with a non-USD position),
parse_failure_payload (raw dict with non-numeric units for the
parser-abort test), two_mirror_seed_rows (direct INSERT into
copy_* tables for disappearance/re-copy test setup).
mirror_aum_fixture etc. land in Track 1b."
```

---

## Task 10: Extend `PortfolioSyncResult` with mirror counters

**Files:**

- Modify: `app/services/portfolio_sync.py` — add three new fields to the dataclass
- Modify: `tests/test_portfolio_sync.py` — add a construction test for the new fields

- [ ] **Step 1: Write the failing test**

Append to `tests/test_portfolio_sync.py`:

```python
def test_portfolio_sync_result_has_mirror_counters() -> None:
    """Spec §2.3 result extension — mirrors_upserted, mirrors_closed,
    mirror_positions_upserted are part of the return contract."""
    result = PortfolioSyncResult(
        positions_updated=0,
        positions_opened_externally=0,
        positions_closed_externally=0,
        cash_delta=Decimal("0"),
        broker_cash=Decimal("0"),
        local_cash=Decimal("0"),
        mirrors_upserted=2,
        mirrors_closed=1,
        mirror_positions_upserted=6,
    )
    assert result.mirrors_upserted == 2
    assert result.mirrors_closed == 1
    assert result.mirror_positions_upserted == 6
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/test_portfolio_sync.py::test_portfolio_sync_result_has_mirror_counters -v`

Expected: FAIL with `TypeError: PortfolioSyncResult.__init__() got an unexpected keyword argument 'mirrors_upserted'`.

- [ ] **Step 3: Extend the dataclass**

Edit `app/services/portfolio_sync.py:43-52`. Replace:

```python
@dataclass
class PortfolioSyncResult:
    """Summary of a portfolio sync run."""

    positions_updated: int
    positions_opened_externally: int
    positions_closed_externally: int
    cash_delta: Decimal
    broker_cash: Decimal
    local_cash: Decimal
```

With:

```python
@dataclass
class PortfolioSyncResult:
    """Summary of a portfolio sync run."""

    positions_updated: int
    positions_opened_externally: int
    positions_closed_externally: int
    cash_delta: Decimal
    broker_cash: Decimal
    local_cash: Decimal
    mirrors_upserted: int = 0
    mirrors_closed: int = 0
    mirror_positions_upserted: int = 0
```

Defaults of `0` preserve every existing test that constructs `PortfolioSyncResult` without the new fields — the existing assertions in `tests/test_portfolio_sync.py` do not need updating.

- [ ] **Step 4: Run the full test file**

Run: `uv run pytest tests/test_portfolio_sync.py -v`

Expected: the new test PASSES, and every existing test still passes (no regression from the default-value additions).

- [ ] **Step 5: Commit**

```bash
git add app/services/portfolio_sync.py tests/test_portfolio_sync.py
git commit -m "feat(#183): extend PortfolioSyncResult with mirror counters

New fields mirrors_upserted, mirrors_closed,
mirror_positions_upserted default to 0 so existing call sites
compile unchanged. Populated by _sync_mirrors in the next task."
```

---

## Task 11: `_sync_mirrors` — upsert path (copy_traders + copy_mirrors + copy_mirror_positions)

**Files:**

- Modify: `app/services/portfolio_sync.py` — add `_sync_mirrors(conn, mirrors, now)` helper with the upsert path only (eviction, soft-close, and total-disappearance guard come in Tasks 12–14)
- Create: `tests/test_portfolio_sync_mirrors.py` — new service-layer test module using `ebull_test`

- [ ] **Step 1: Create the service-layer test module skeleton**

The test module needs a cleanup fixture and a helper to copy the `_assert_test_db` guard pattern from `tests/test_operator_setup_race.py`.

```python
# tests/test_portfolio_sync_mirrors.py
"""§8.2 + §8.3 service-layer tests for copy-trading mirror sync.

All tests run against the dedicated ebull_test database (never
settings.database_url) — the same isolation pattern as
tests/test_operator_setup_race.py.
"""

from __future__ import annotations

from collections.abc import Iterator
from decimal import Decimal
from typing import Any

import psycopg
import psycopg.rows
import pytest

from app.providers.broker import BrokerPortfolio
from app.providers.implementations.etoro_broker import (
    PortfolioParseError,
    _parse_mirrors_payload,
)
from app.services.portfolio_sync import sync_portfolio
from tests.fixtures.copy_mirrors import (
    _NOW,
    parse_failure_payload,
    two_mirror_payload,
    two_mirror_seed_rows,
)
from tests.test_operator_setup_race import (
    _assert_test_db,
    _test_database_url,
    _test_db_available,
)

pytestmark = pytest.mark.skipif(
    not _test_db_available(),
    reason="ebull_test DB unavailable — skipping real-DB mirror sync test",
)


@pytest.fixture
def conn() -> Iterator[psycopg.Connection[Any]]:
    """Yield a fresh connection to ebull_test with copy_* tables
    truncated at the start of each test. Rollback on failure."""
    with psycopg.connect(_test_database_url()) as c:
        _assert_test_db(c)
        with c.cursor() as cur:
            cur.execute(
                "TRUNCATE copy_mirror_positions, copy_mirrors, copy_traders "
                "RESTART IDENTITY CASCADE"
            )
        c.commit()
        yield c
        c.rollback()


def _count(conn: psycopg.Connection[Any], table: str) -> int:
    with conn.cursor(row_factory=psycopg.rows.tuple_row) as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table}")  # table is hard-coded
        row = cur.fetchone()
        return int(row[0]) if row else 0


def _empty_local_portfolio(payload: BrokerPortfolio) -> BrokerPortfolio:
    """Wrap a two_mirror_payload in a BrokerPortfolio with no
    broker-side positions or cash — so the positions/cash sync
    branches are no-ops and we test only the mirror branch."""
    return payload


def test_sync_mirrors_fresh_insert(conn: psycopg.Connection[Any]) -> None:
    """Spec §8.2: first sync inserts copy_traders + copy_mirrors +
    copy_mirror_positions rows with active=TRUE."""
    payload = two_mirror_payload()
    result = sync_portfolio(conn, payload, now=_NOW)
    conn.commit()

    assert _count(conn, "copy_traders") == 2
    assert _count(conn, "copy_mirrors") == 2
    assert _count(conn, "copy_mirror_positions") == 6
    assert result.mirrors_upserted == 2
    assert result.mirror_positions_upserted == 6
    assert result.mirrors_closed == 0

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            "SELECT active, closed_at FROM copy_mirrors ORDER BY mirror_id"
        )
        rows = cur.fetchall()
    for row in rows:
        assert row["active"] is True
        assert row["closed_at"] is None


def test_sync_mirrors_idempotent_resync(conn: psycopg.Connection[Any]) -> None:
    """Spec §8.2: re-running the same payload is idempotent —
    row counts unchanged, active still TRUE, updated_at refreshed."""
    payload = two_mirror_payload()
    sync_portfolio(conn, payload, now=_NOW)
    conn.commit()
    sync_portfolio(conn, payload, now=_NOW)
    conn.commit()

    assert _count(conn, "copy_traders") == 2
    assert _count(conn, "copy_mirrors") == 2
    assert _count(conn, "copy_mirror_positions") == 6
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_portfolio_sync_mirrors.py -v`

Expected: FAIL — either the tables don't receive mirror rows (because `_sync_mirrors` isn't wired yet) or the fixture setup trips.

- [ ] **Step 3: Add the `_sync_mirrors` helper and wire it into `sync_portfolio`**

Edit `app/services/portfolio_sync.py`. Update imports at the top (line 35) to include the new dataclasses:

```python
from app.providers.broker import (
    BrokerMirror,
    BrokerPortfolio,
    BrokerPosition,
)
```

Add the helper below `_aggregate_by_instrument` (around line 110), before `sync_portfolio`:

```python
def _sync_mirrors(
    conn: psycopg.Connection[Any],
    mirrors: Sequence[BrokerMirror],
    now: datetime,
) -> tuple[int, int, int]:
    """Upsert copy_traders/copy_mirrors/copy_mirror_positions from a
    freshly-parsed mirror payload. Returns
    ``(mirrors_upserted, mirror_positions_upserted, mirrors_closed)``.

    Must be called inside the caller's transaction — this function
    never commits. Caller owns rollback on any raise.

    Disappearance handling (total → raise, partial → soft-close)
    lives in the caller or in a follow-up step; this function only
    handles the rows that are present in the payload.

    Single-writer serialisation is guaranteed by JobRuntime's
    APScheduler+JobLock stack (spec §2.3.1); _sync_mirrors does not
    take its own advisory lock.
    """
    mirrors_upserted = 0
    mirror_positions_upserted = 0

    for mirror in mirrors:
        # 1. Upsert the trader row (parent_cid is the identity
        #    spine).
        conn.execute(
            """
            INSERT INTO copy_traders (
                parent_cid, parent_username, first_seen_at, updated_at
            ) VALUES (
                %(cid)s, %(username)s, %(now)s, %(now)s
            )
            ON CONFLICT (parent_cid) DO UPDATE SET
                parent_username = EXCLUDED.parent_username,
                updated_at = EXCLUDED.updated_at
            """,
            {
                "cid": mirror.parent_cid,
                "username": mirror.parent_username,
                "now": now,
            },
        )

        # 2. Upsert the mirror row. active=TRUE, closed_at=NULL on
        #    every row the payload contains — re-copy of a
        #    previously-closed mirror_id flips those back to live.
        conn.execute(
            """
            INSERT INTO copy_mirrors (
                mirror_id, parent_cid, initial_investment,
                deposit_summary, withdrawal_summary,
                available_amount, closed_positions_net_profit,
                stop_loss_percentage, stop_loss_amount,
                mirror_status_id, mirror_calculation_type,
                pending_for_closure, started_copy_date,
                active, closed_at, raw_payload, updated_at
            ) VALUES (
                %(mirror_id)s, %(parent_cid)s, %(initial_investment)s,
                %(deposit_summary)s, %(withdrawal_summary)s,
                %(available_amount)s, %(closed_positions_net_profit)s,
                %(stop_loss_percentage)s, %(stop_loss_amount)s,
                %(mirror_status_id)s, %(mirror_calculation_type)s,
                %(pending_for_closure)s, %(started_copy_date)s,
                TRUE, NULL, %(raw_payload)s, %(now)s
            )
            ON CONFLICT (mirror_id) DO UPDATE SET
                parent_cid                  = EXCLUDED.parent_cid,
                initial_investment          = EXCLUDED.initial_investment,
                deposit_summary             = EXCLUDED.deposit_summary,
                withdrawal_summary          = EXCLUDED.withdrawal_summary,
                available_amount            = EXCLUDED.available_amount,
                closed_positions_net_profit = EXCLUDED.closed_positions_net_profit,
                stop_loss_percentage        = EXCLUDED.stop_loss_percentage,
                stop_loss_amount            = EXCLUDED.stop_loss_amount,
                mirror_status_id            = EXCLUDED.mirror_status_id,
                mirror_calculation_type     = EXCLUDED.mirror_calculation_type,
                pending_for_closure         = EXCLUDED.pending_for_closure,
                started_copy_date           = EXCLUDED.started_copy_date,
                active                      = TRUE,
                closed_at                   = NULL,
                raw_payload                 = EXCLUDED.raw_payload,
                updated_at                  = EXCLUDED.updated_at
            """,
            {
                "mirror_id": mirror.mirror_id,
                "parent_cid": mirror.parent_cid,
                "initial_investment": mirror.initial_investment,
                "deposit_summary": mirror.deposit_summary,
                "withdrawal_summary": mirror.withdrawal_summary,
                "available_amount": mirror.available_amount,
                "closed_positions_net_profit": mirror.closed_positions_net_profit,
                "stop_loss_percentage": mirror.stop_loss_percentage,
                "stop_loss_amount": mirror.stop_loss_amount,
                "mirror_status_id": mirror.mirror_status_id,
                "mirror_calculation_type": mirror.mirror_calculation_type,
                "pending_for_closure": mirror.pending_for_closure,
                "started_copy_date": mirror.started_copy_date,
                "raw_payload": psycopg.types.json.Jsonb(mirror.raw_payload),
                "now": now,
            },
        )
        mirrors_upserted += 1

        # 3. Upsert every nested position in the payload. Eviction
        #    of disappeared positions is a separate statement (see
        #    Task 12).
        for pos in mirror.positions:
            conn.execute(
                """
                INSERT INTO copy_mirror_positions (
                    mirror_id, position_id, parent_position_id,
                    instrument_id, is_buy, units, amount,
                    initial_amount_in_dollars, open_rate,
                    open_conversion_rate, open_date_time,
                    take_profit_rate, stop_loss_rate,
                    total_fees, leverage, raw_payload, updated_at
                ) VALUES (
                    %(mirror_id)s, %(position_id)s, %(parent_position_id)s,
                    %(instrument_id)s, %(is_buy)s, %(units)s, %(amount)s,
                    %(initial_amount)s, %(open_rate)s,
                    %(open_conversion_rate)s, %(open_date_time)s,
                    %(take_profit_rate)s, %(stop_loss_rate)s,
                    %(total_fees)s, %(leverage)s, %(raw_payload)s,
                    %(now)s
                )
                ON CONFLICT (mirror_id, position_id) DO UPDATE SET
                    parent_position_id        = EXCLUDED.parent_position_id,
                    instrument_id             = EXCLUDED.instrument_id,
                    is_buy                    = EXCLUDED.is_buy,
                    units                     = EXCLUDED.units,
                    amount                    = EXCLUDED.amount,
                    initial_amount_in_dollars = EXCLUDED.initial_amount_in_dollars,
                    open_rate                 = EXCLUDED.open_rate,
                    open_conversion_rate      = EXCLUDED.open_conversion_rate,
                    open_date_time            = EXCLUDED.open_date_time,
                    take_profit_rate          = EXCLUDED.take_profit_rate,
                    stop_loss_rate            = EXCLUDED.stop_loss_rate,
                    total_fees                = EXCLUDED.total_fees,
                    leverage                  = EXCLUDED.leverage,
                    raw_payload               = EXCLUDED.raw_payload,
                    updated_at                = EXCLUDED.updated_at
                """,
                {
                    "mirror_id": mirror.mirror_id,
                    "position_id": pos.position_id,
                    "parent_position_id": pos.parent_position_id,
                    "instrument_id": pos.instrument_id,
                    "is_buy": pos.is_buy,
                    "units": pos.units,
                    "amount": pos.amount,
                    "initial_amount": pos.initial_amount_in_dollars,
                    "open_rate": pos.open_rate,
                    "open_conversion_rate": pos.open_conversion_rate,
                    "open_date_time": pos.open_date_time,
                    "take_profit_rate": pos.take_profit_rate,
                    "stop_loss_rate": pos.stop_loss_rate,
                    "total_fees": pos.total_fees,
                    "leverage": pos.leverage,
                    "raw_payload": psycopg.types.json.Jsonb(pos.raw_payload),
                    "now": now,
                },
            )
            mirror_positions_upserted += 1

    mirrors_closed = 0  # populated by Task 12 soft-close step
    return mirrors_upserted, mirror_positions_upserted, mirrors_closed
```

Add the `psycopg.types.json` import near the top:

```python
import psycopg
import psycopg.rows
import psycopg.types.json
```

Now wire `_sync_mirrors` into `sync_portfolio`. At the end of `sync_portfolio` (just before the `return PortfolioSyncResult(...)` at line 297), add:

```python
    # 4. Reconcile copy-trading mirrors (spec §2.3).
    mirrors_upserted, mirror_positions_upserted, mirrors_closed = _sync_mirrors(
        conn, portfolio.mirrors, now
    )
```

And update the return statement to pass the new fields:

```python
    return PortfolioSyncResult(
        positions_updated=updated,
        positions_opened_externally=opened_externally,
        positions_closed_externally=closed_externally,
        cash_delta=cash_delta,
        broker_cash=broker_cash,
        local_cash=local_cash,
        mirrors_upserted=mirrors_upserted,
        mirrors_closed=mirrors_closed,
        mirror_positions_upserted=mirror_positions_upserted,
    )
```

- [ ] **Step 4: Run the tests**

Run: `uv run pytest tests/test_portfolio_sync_mirrors.py::test_sync_mirrors_fresh_insert tests/test_portfolio_sync_mirrors.py::test_sync_mirrors_idempotent_resync -v`

Expected: both tests PASS.

- [ ] **Step 5: Re-run the full test_portfolio_sync.py to catch regressions**

Run: `uv run pytest tests/test_portfolio_sync.py tests/test_portfolio_sync_mirrors.py -v`

Expected: all PASS. The mock-based tests in `test_portfolio_sync.py` may now need an update if their mock cursor chokes on the new `conn.execute("INSERT INTO copy_traders ...")` — if so, extend `_mock_conn` to match "INSERT INTO copy_traders" / "INSERT INTO copy_mirrors" / "INSERT INTO copy_mirror_positions" as no-op writes.

- [ ] **Step 6: Lint + typecheck**

Run: `uv run ruff check app/services/portfolio_sync.py tests/test_portfolio_sync_mirrors.py && uv run pyright app/services/portfolio_sync.py`

Expected: clean.

- [ ] **Step 7: Commit**

```bash
git add app/services/portfolio_sync.py tests/test_portfolio_sync_mirrors.py
git commit -m "feat(#183): _sync_mirrors upsert path

Upserts copy_traders/copy_mirrors/copy_mirror_positions from the
BrokerPortfolio.mirrors field inside the existing sync_portfolio
transaction. Re-copy of a previously-closed mirror_id resets
active=TRUE, closed_at=NULL via the ON CONFLICT clause. Nested
position eviction and soft-close land in the next tasks."
```

---

## Task 12: Nested position eviction

**Files:**

- Modify: `app/services/portfolio_sync.py` — extend `_sync_mirrors` to DELETE disappeared nested positions per mirror
- Modify: `tests/test_portfolio_sync_mirrors.py` — add eviction test

- [ ] **Step 1: Write the failing test**

Append to `tests/test_portfolio_sync_mirrors.py`:

```python
import dataclasses


def test_sync_mirrors_evicts_closed_nested_positions(
    conn: psycopg.Connection[Any],
) -> None:
    """Spec §2.3.2: a nested position removed from the payload is
    DELETEd from copy_mirror_positions. Sibling positions in the
    same mirror and positions in other mirrors are untouched.
    copy_mirrors.active stays TRUE."""
    payload = two_mirror_payload()
    sync_portfolio(conn, payload, now=_NOW)
    conn.commit()
    assert _count(conn, "copy_mirror_positions") == 6

    # Remove one nested position from the first mirror and re-sync.
    trimmed_positions = payload.mirrors[0].positions[1:]  # drop pos 1001
    trimmed_mirror = dataclasses.replace(
        payload.mirrors[0], positions=trimmed_positions
    )
    trimmed_payload = dataclasses.replace(
        payload,
        mirrors=(trimmed_mirror, payload.mirrors[1]),
    )
    sync_portfolio(conn, trimmed_payload, now=_NOW)
    conn.commit()

    # The removed row is gone, siblings remain.
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT position_id FROM copy_mirror_positions
            WHERE mirror_id = %s ORDER BY position_id
            """,
            (payload.mirrors[0].mirror_id,),
        )
        remaining = [r["position_id"] for r in cur.fetchall()]
    assert remaining == [1002, 1003]

    # The other mirror is untouched.
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            "SELECT COUNT(*) AS n FROM copy_mirror_positions WHERE mirror_id = %s",
            (payload.mirrors[1].mirror_id,),
        )
        row = cur.fetchone()
        assert row is not None
        assert row["n"] == 3

    # The mirror row itself is still active.
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            "SELECT active FROM copy_mirrors WHERE mirror_id = %s",
            (payload.mirrors[0].mirror_id,),
        )
        row = cur.fetchone()
        assert row is not None
        assert row["active"] is True


def test_sync_mirrors_evicts_all_positions_when_mirror_empties(
    conn: psycopg.Connection[Any],
) -> None:
    """Spec §2.3.2: an empty positions[] evicts every nested row for
    that mirror (exploits Postgres `position_id <> ALL('{}')` === TRUE
    semantics)."""
    payload = two_mirror_payload()
    sync_portfolio(conn, payload, now=_NOW)
    conn.commit()

    empty_mirror = dataclasses.replace(payload.mirrors[0], positions=())
    emptied_payload = dataclasses.replace(
        payload,
        mirrors=(empty_mirror, payload.mirrors[1]),
    )
    sync_portfolio(conn, emptied_payload, now=_NOW)
    conn.commit()

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            "SELECT COUNT(*) AS n FROM copy_mirror_positions WHERE mirror_id = %s",
            (payload.mirrors[0].mirror_id,),
        )
        row = cur.fetchone()
        assert row is not None
        assert row["n"] == 0
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_portfolio_sync_mirrors.py::test_sync_mirrors_evicts_closed_nested_positions tests/test_portfolio_sync_mirrors.py::test_sync_mirrors_evicts_all_positions_when_mirror_empties -v`

Expected: FAIL — the removed position is still in the DB because `_sync_mirrors` has no eviction step yet.

- [ ] **Step 3: Add the eviction step**

In `app/services/portfolio_sync.py`, inside `_sync_mirrors`, **before** the inner `for pos in mirror.positions:` upsert loop, add the eviction step. The ordering is: (1) upsert copy_traders, (2) upsert copy_mirrors, (3) evict disappeared positions for this mirror, (4) upsert remaining positions.

Update the per-mirror section of `_sync_mirrors`:

```python
        # 3a. Evict nested positions that have closed since the last
        #     sync. Passing the new IDs as a single array parameter
        #     sidesteps the empty-list SQL parser error and exploits
        #     Postgres's `position_id <> ALL('{}')` === TRUE semantics
        #     to correctly delete every existing row when the payload
        #     has zero positions for this mirror.
        current_position_ids = [int(p.position_id) for p in mirror.positions]
        conn.execute(
            """
            DELETE FROM copy_mirror_positions
            WHERE mirror_id = %(mirror_id)s
              AND position_id <> ALL(%(position_ids)s::bigint[])
            """,
            {
                "mirror_id": mirror.mirror_id,
                "position_ids": current_position_ids,
            },
        )

        # 3b. Upsert every position in the payload.
        for pos in mirror.positions:
            # ... existing INSERT...ON CONFLICT ... (unchanged)
```

(Keep the existing inner `for pos in mirror.positions:` loop exactly as it is — only the DELETE statement is new, inserted immediately before the loop.)

- [ ] **Step 4: Run the tests**

Run: `uv run pytest tests/test_portfolio_sync_mirrors.py -v`

Expected: all tests PASS, including the new eviction tests and the earlier upsert/idempotency tests.

- [ ] **Step 5: Commit**

```bash
git add app/services/portfolio_sync.py tests/test_portfolio_sync_mirrors.py
git commit -m "feat(#183): evict disappeared nested mirror positions

DELETE FROM copy_mirror_positions WHERE mirror_id = ? AND
position_id <> ALL(?::bigint[]) runs once per mirror before the
per-position upsert loop. Empty array correctly deletes every
row for that mirror via Postgres's `<> ALL('{}')` === TRUE
semantics — no special-case branch."
```

---

## Task 13: Partial-disappearance soft-close + re-copy

**Files:**

- Modify: `app/services/portfolio_sync.py` — extend `_sync_mirrors` with the soft-close step after the per-mirror upsert loop
- Modify: `tests/test_portfolio_sync_mirrors.py` — add partial-disappearance and re-copy tests

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_portfolio_sync_mirrors.py`:

```python
def test_sync_mirrors_partial_disappearance_soft_closes(
    conn: psycopg.Connection[Any],
) -> None:
    """Spec §2.3.4: a mirror that disappears from the payload
    (while other mirrors are still present) is soft-closed —
    active=FALSE, closed_at=%(now)s. Nested positions are RETAINED
    for audit."""
    two_mirror_seed_rows(conn)
    conn.commit()
    assert _count(conn, "copy_mirrors") == 2

    # Sync a payload that only contains the second mirror.
    full_payload = two_mirror_payload()
    partial_payload = dataclasses.replace(
        full_payload,
        mirrors=(full_payload.mirrors[1],),  # drop mirror A
    )
    result = sync_portfolio(conn, partial_payload, now=_NOW)
    conn.commit()

    # Mirror A: soft-closed, nested positions retained.
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT active, closed_at FROM copy_mirrors
            WHERE mirror_id = %s
            """,
            (full_payload.mirrors[0].mirror_id,),
        )
        row = cur.fetchone()
    assert row is not None
    assert row["active"] is False
    assert row["closed_at"] == _NOW

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            "SELECT COUNT(*) AS n FROM copy_mirror_positions WHERE mirror_id = %s",
            (full_payload.mirrors[0].mirror_id,),
        )
        row = cur.fetchone()
    assert row is not None
    assert row["n"] == 3  # retained for audit

    # Mirror B: still active.
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            "SELECT active FROM copy_mirrors WHERE mirror_id = %s",
            (full_payload.mirrors[1].mirror_id,),
        )
        row = cur.fetchone()
    assert row is not None
    assert row["active"] is True

    assert result.mirrors_closed == 1


def test_sync_mirrors_recopy_resurrects_closed_mirror(
    conn: psycopg.Connection[Any],
) -> None:
    """Spec §2.3.4 / §1.2: if eToro reuses a previously-seen
    mirror_id, the ON CONFLICT DO UPDATE clause resets
    active=TRUE, closed_at=NULL so the mirror is live again."""
    two_mirror_seed_rows(conn)
    # Pre-close mirror A so it starts the test soft-closed.
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE copy_mirrors
               SET active = FALSE,
                   closed_at = %(closed)s
             WHERE mirror_id = %(mid)s
            """,
            {
                "closed": _NOW,
                "mid": two_mirror_payload().mirrors[0].mirror_id,
            },
        )
    conn.commit()

    # Sync the full payload — mirror A re-appears.
    sync_portfolio(conn, two_mirror_payload(), now=_NOW)
    conn.commit()

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            "SELECT active, closed_at FROM copy_mirrors WHERE mirror_id = %s",
            (two_mirror_payload().mirrors[0].mirror_id,),
        )
        row = cur.fetchone()
    assert row is not None
    assert row["active"] is True
    assert row["closed_at"] is None
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_portfolio_sync_mirrors.py::test_sync_mirrors_partial_disappearance_soft_closes tests/test_portfolio_sync_mirrors.py::test_sync_mirrors_recopy_resurrects_closed_mirror -v`

Expected: FAIL — mirror A stays active because the soft-close step doesn't exist yet. The re-copy test may pass (the ON CONFLICT clause from Task 11 already resets active/closed_at), but keep it as regression coverage.

- [ ] **Step 3: Add the soft-close step to `_sync_mirrors`**

Edit `_sync_mirrors` in `app/services/portfolio_sync.py`. After the per-mirror upsert loop and before the `return` statement, add:

```python
    # 4. Disappearance handling (§2.3.4).
    #
    # Total disappearance (payload empty AND active local rows
    # exist) is handled in step 5 below — it raises to force
    # operator investigation. Here we soft-close mirrors that have
    # disappeared from a NON-EMPTY payload.
    payload_mirror_ids = [int(m.mirror_id) for m in mirrors]

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            "SELECT mirror_id FROM copy_mirrors WHERE active = TRUE"
        )
        active_local_ids = {int(r["mirror_id"]) for r in cur.fetchall()}

    disappeared_ids = sorted(active_local_ids - set(payload_mirror_ids))

    if not payload_mirror_ids and active_local_ids:
        # Total disappearance — raise for operator investigation.
        # See step 5 (Task 14) for the exact error contract.
        raise RuntimeError(
            "Broker returned empty mirrors[] but "
            f"{len(active_local_ids)} active local mirror(s) exist — "
            "refusing to soft-close en masse. Likely upstream API "
            "regression; investigate before manual cleanup."
        )

    if disappeared_ids:
        conn.execute(
            """
            UPDATE copy_mirrors
               SET active = FALSE,
                   closed_at = %(now)s,
                   updated_at = %(now)s
             WHERE mirror_id = ANY(%(disappeared_ids)s::bigint[])
               AND active = TRUE
            """,
            {
                "now": now,
                "disappeared_ids": disappeared_ids,
            },
        )
        for mirror_id in disappeared_ids:
            logger.info(
                "mirror %d disappeared from payload — marked closed",
                mirror_id,
            )
        mirrors_closed = len(disappeared_ids)
```

Update the `return` at the bottom of `_sync_mirrors` — `mirrors_closed` is now populated above, so the earlier `mirrors_closed = 0` default-line becomes an initial assignment:

```python
    mirrors_closed = 0
    # ... existing upsert loop ...
    # ... soft-close step above may set mirrors_closed = len(disappeared_ids) ...
    return mirrors_upserted, mirror_positions_upserted, mirrors_closed
```

Make sure `mirrors_closed = 0` is declared *before* the for-mirror upsert loop, not after it, so the soft-close step can reassign it.

- [ ] **Step 4: Run the tests**

Run: `uv run pytest tests/test_portfolio_sync_mirrors.py -v`

Expected: all tests PASS including the new partial-disappearance and re-copy tests and the earlier upsert/eviction tests.

- [ ] **Step 5: Commit**

```bash
git add app/services/portfolio_sync.py tests/test_portfolio_sync_mirrors.py
git commit -m "feat(#183): soft-close disappeared mirrors

Partial disappearance (payload non-empty AND disappeared_ids
non-empty) flips active=FALSE, closed_at=%(now)s on the
affected rows. Nested copy_mirror_positions are retained for
audit — the §3.4 AUM query's WHERE m.active filter excludes
closed mirrors from forward-looking calculations without
deleting their history. Re-copy of a closed mirror_id is
already covered by Task 11's ON CONFLICT clause."
```

---

## Task 14: Total-disappearance raise + parser-failure rollback (pre-eviction)

**Files:**

- Modify: `app/services/portfolio_sync.py` — no code changes (the raise from Task 13 already covers step 5; Task 14 is test-only, asserting the invariant holds and the parser-failure path rolls back cleanly)
- Modify: `tests/test_portfolio_sync_mirrors.py` — add total-disappearance and parser-failure tests

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_portfolio_sync_mirrors.py`:

```python
def test_sync_mirrors_total_disappearance_raises(
    conn: psycopg.Connection[Any],
) -> None:
    """Spec §2.3.4 asymmetry: if the payload mirrors[] is empty but
    local active mirrors exist, raise RuntimeError. Rows survive
    unchanged after the rollback."""
    two_mirror_seed_rows(conn)
    conn.commit()

    empty_payload = dataclasses.replace(two_mirror_payload(), mirrors=())

    with pytest.raises(RuntimeError, match="empty mirrors"):
        sync_portfolio(conn, empty_payload, now=_NOW)
    conn.rollback()

    # Both rows survive as active=TRUE.
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute("SELECT active FROM copy_mirrors ORDER BY mirror_id")
        rows = cur.fetchall()
    assert len(rows) == 2
    assert all(r["active"] is True for r in rows)


def test_sync_mirrors_parser_failure_aborts_before_eviction(
    conn: psycopg.Connection[Any],
) -> None:
    """Spec §2.3.3: if _parse_mirrors_payload raises
    PortfolioParseError, the sync transaction is rolled back before
    any upsert or eviction touches the DB. Seed rows survive
    unchanged — this is the regression test for the Codex v3
    finding V parse-and-soft-close hole."""
    two_mirror_seed_rows(conn)
    conn.commit()
    baseline_positions = _count(conn, "copy_mirror_positions")
    assert baseline_positions == 6

    raw_failure = parse_failure_payload()
    with pytest.raises(PortfolioParseError):
        # The failure fires inside the parser — callers of
        # sync_portfolio parse first, then call sync. In production
        # this is get_portfolio → sync_portfolio; in tests we
        # exercise the same ordering explicitly.
        _ = _parse_mirrors_payload(raw_failure)

    # sync_portfolio is never called — rows untouched.
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute("SELECT active FROM copy_mirrors ORDER BY mirror_id")
        rows = cur.fetchall()
    assert len(rows) == 2
    assert all(r["active"] is True for r in rows)
    assert _count(conn, "copy_mirror_positions") == baseline_positions


def test_sync_mirrors_known_mirror_top_level_parse_failure_aborts(
    conn: psycopg.Connection[Any],
) -> None:
    """Spec §2.2.2 / §2.3.3: a known mirrorID with a missing
    required top-level field raises PortfolioParseError, NOT
    log-and-skip. The outer _parse_mirrors_payload wraps the
    underlying KeyError. Without this, the sync would interpret
    the known mirror as disappeared and soft-close it — the hole
    Codex v3 finding V identified."""
    two_mirror_seed_rows(conn)
    conn.commit()

    bad_raw = parse_failure_payload()
    # Break the top-level field (not the nested one) this time.
    bad_raw[0]["positions"][0]["units"] = "1.0"  # fix the nested row
    del bad_raw[0]["availableAmount"]  # break the top-level row

    with pytest.raises(PortfolioParseError) as excinfo:
        _parse_mirrors_payload(bad_raw)
    assert "15712187" in str(excinfo.value)
    assert isinstance(excinfo.value.__cause__, KeyError)

    # Seed rows are untouched — sync_portfolio never reached.
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute("SELECT active FROM copy_mirrors ORDER BY mirror_id")
        rows = cur.fetchall()
    assert len(rows) == 2
    assert all(r["active"] is True for r in rows)
```

- [ ] **Step 2: Run the tests**

Run: `uv run pytest tests/test_portfolio_sync_mirrors.py -v`

Expected: all tests PASS — the `RuntimeError` raise from Task 13 and the parser raises from Tasks 7–8 together cover every assertion. No code changes should be needed.

If `test_sync_mirrors_total_disappearance_raises` fails because the soft-close branch in `_sync_mirrors` was accidentally ordered after the empty-check, fix the ordering so the `if not payload_mirror_ids and active_local_ids:` branch runs before the `if disappeared_ids:` branch.

- [ ] **Step 3: Run the full pre-push gate**

Run:

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest
```

Expected: all four PASS. This is the full CLAUDE.md gate.

- [ ] **Step 4: Commit**

```bash
git add tests/test_portfolio_sync_mirrors.py
git commit -m "test(#183): §8.3 disappearance and parse-abort regressions

Three regression tests:
- total disappearance raises RuntimeError and leaves rows intact
- malformed nested position raises PortfolioParseError; seed
  rows untouched (pre-eviction)
- malformed known-mirror top-level field raises
  PortfolioParseError; seed rows untouched (closes the
  Codex v3 finding V parse-and-soft-close hole)"
```

---

## Task 15: Scheduler log extension

**Files:**

- Modify: `app/workers/scheduler.py:874-883` — extend the existing `logger.info(...)` line in `daily_portfolio_sync` to include the new mirror counters

- [ ] **Step 1: Locate the existing log line**

Current state at `app/workers/scheduler.py:874-883`:

```python
        logger.info(
            "Portfolio sync complete: updated=%d opened_ext=%d closed_ext=%d "
            "broker_cash=%.2f local_cash=%.2f delta=%.2f",
            result.positions_updated,
            result.positions_opened_externally,
            result.positions_closed_externally,
            result.broker_cash,
            result.local_cash,
            result.cash_delta,
        )
```

- [ ] **Step 2: Extend the format string and argument list**

Replace with:

```python
        logger.info(
            "Portfolio sync complete: updated=%d opened_ext=%d closed_ext=%d "
            "mirrors_up=%d mirrors_closed=%d mirror_positions_up=%d "
            "broker_cash=%.2f local_cash=%.2f delta=%.2f",
            result.positions_updated,
            result.positions_opened_externally,
            result.positions_closed_externally,
            result.mirrors_upserted,
            result.mirrors_closed,
            result.mirror_positions_upserted,
            result.broker_cash,
            result.local_cash,
            result.cash_delta,
        )
```

No test change is needed — this is an observability-only string and the existing scheduler tests don't assert on its format. The change is covered by the fact that `PortfolioSyncResult` already carries the new fields (defaulted to 0 before Task 11's wire-up populates them).

- [ ] **Step 3: Run the scheduler tests to confirm nothing regressed**

Run: `uv run pytest tests/test_workers_scheduler.py -v 2>&1 | head -50`

(If `tests/test_workers_scheduler.py` doesn't exist — in which case `Glob` the repo for any scheduler test — skip to step 4.)

Expected: all scheduler tests still PASS (or SKIP on systems without a full scheduler test harness).

- [ ] **Step 4: Full pre-push gate**

Run:

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest
```

Expected: all four PASS.

- [ ] **Step 5: Commit**

```bash
git add app/workers/scheduler.py
git commit -m "feat(#183): log mirror counters in daily portfolio sync

Adds mirrors_up/mirrors_closed/mirror_positions_up to the
existing Portfolio sync complete INFO line so operators see
mirror activity in the job_runs audit trail without needing
to query the DB."
```

---

## Self-review (run after finishing all 15 tasks)

Before opening the PR, walk through this checklist inline — do not delegate to a subagent.

**1. Spec coverage.** For each spec section Track 1a owns, point to the task that implements it:

- §1.1 `copy_traders` DDL → Task 1
- §1.2 `copy_mirrors` DDL + active/closed_at + partial index → Task 1
- §1.3 `copy_mirror_positions` DDL + composite PK + ON DELETE CASCADE → Task 1
- §2.1 `BrokerMirrorPosition` / `BrokerMirror` / `BrokerPortfolio.mirrors` field → Task 4
- §2.2.1 `PortfolioParseError` hierarchy → Task 5
- §2.2.2 `_parse_mirror_position` / `_parse_mirror` / strict-raise outer loop → Tasks 6, 7, 8
- §2.3.1 single-writer invariant — documented in `_sync_mirrors` docstring → Task 11
- §2.3.2 per-mirror upsert + nested eviction → Tasks 11, 12
- §2.3.3 parser-failure safeguard (strict-raise, no partial write) → Tasks 8, 13, 14
- §2.3.4 soft-close + total-disappearance raise → Tasks 13, 14
- §2.3 `PortfolioSyncResult` extension → Task 10
- §7 migration 022 → Task 1
- §8.0 fixture file + `_NOW` ownership migration → Tasks 2, 3, 9
- §8.1 parser tests → Tasks 5, 6, 7, 8
- §8.2 service-layer upsert/idempotency/eviction tests → Tasks 11, 12
- §8.3 disappearance + parse-abort tests → Tasks 13, 14
- §10 locked decisions — nothing in this plan deviates

Explicitly NOT covered (Track 1b, 1.5, 2 territory):
- `_load_mirror_equity` helper + 3 call sites → Track 1b #187
- `PortfolioResponse.mirror_equity` → Track 1b #187
- §8.4, §8.5, §8.6 tests → Track 1b #187
- REST endpoint + frontend panel → Track 1.5 #188
- `/user-info/people/*` discovery → Track 2 #189

**2. Placeholder scan.** Grep the plan for `TODO`, `TBD`, `implement later`, `fill in`, `similar to Task`. None should exist. Fix any that do.

**3. Type consistency.** The identifiers this plan names must match across tasks:

- `BrokerMirror`, `BrokerMirrorPosition`, `BrokerPortfolio.mirrors` (declared Task 4, consumed Tasks 6–14)
- `PortfolioParseError` (declared Task 5, consumed Tasks 6–8, 14)
- `_parse_mirror_position` (Task 6), `_parse_mirror` (Task 7), `_parse_mirrors_payload` (Task 8)
- `_sync_mirrors(conn, mirrors, now) -> tuple[int, int, int]` (Task 11; extended Tasks 12, 13; order is `(mirrors_upserted, mirror_positions_upserted, mirrors_closed)`)
- `PortfolioSyncResult.mirrors_upserted` / `mirrors_closed` / `mirror_positions_upserted` (Task 10)
- `_NOW`, `_GUARD_INSTRUMENT_ID`, `_GUARD_INSTRUMENT_SECTOR` in `tests.fixtures.copy_mirrors` (Task 2)
- `two_mirror_payload()`, `parse_failure_payload()`, `two_mirror_seed_rows(conn)` (Task 9)

If any identifier drifts between tasks during implementation, stop and reconcile. Do not push a commit with mismatched names.

**4. Pre-push gate.** Before any push, run all four commands from CLAUDE.md. Then `tests/smoke/test_app_boots.py` must stay green end-to-end.

---

## PR description checklist (for the final commit before push)

When opening the PR (after Task 15 is merged into the branch):

- [ ] Title: `feat(#183): ingest eToro copy-trading mirrors (Track 1a)`
- [ ] Body mentions that this is Track 1a; Track 1b (#187), Track 1.5 (#188), Track 2 (#189) are dependent follow-ups
- [ ] Body summarises the schema (3 tables), the parser contract (strict-raise, `PortfolioParseError`), and the sync contract (upsert / nested-evict / soft-close / total-disappearance raise)
- [ ] Body names the security model: pure read-from-broker / write-to-local-DB, no new network call, no new order-placing path, no new un-copy UX, no new cross-user data flow
- [ ] Body flags every deviation from the spec (ideally none — if there is one, explain why)
- [ ] Body links `docs/superpowers/specs/2026-04-11-copy-trading-ingestion-design.md`
