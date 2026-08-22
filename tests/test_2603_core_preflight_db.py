"""#2603 item 3 step 3b-1 — the two SQL mechanisms, against a real database.

The precedence order is pure and lives in ``test_2603_core_preflight``.  What needs
a database is exactly two things, and neither is provable any other way:

1. **The observation returns ONE row even when the instrument does not exist.**
   The query is anchored on the parameter rather than on ``instruments``
   specifically so an emergency stop stays readable in the branch where the
   instrument is missing.  Anchor it the obvious way instead and this test is the
   only thing that notices.
2. **The ``quotes_refresh`` scope arms reach the mandate's core instrument (arm 4)
   and the candidates being MEASURED for one (arm 5, #2833).** Without arm 4
   ``core_quote_missing`` is PERMANENT for any mandate naming an instrument that is
   not held, not Tier 1/2 and not a benchmark — measured on dev, that is `IVV`,
   `VTI` and `SPY.RTH` among the obvious core candidates. Without arm 5 the
   sleeve's spread bar cannot be measured before the mandate that would make it
   measurable, which is a cycle rather than a wait.

⚠ In its own ``_db`` module deliberately: the string ``ebull_test_conn`` in a test
source db-marks the WHOLE module at collection.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import psycopg
import pytest

from app.services.strategy_core_eligibility import (
    CORE_ELIGIBILITY_MAX_AGE,
    CORE_ELIGIBILITY_PASS_VERDICT,
    CORE_ELIGIBILITY_POLICY_VERSION,
)
from app.services.strategy_core_preflight import (
    _PREFLIGHT_SQL,
    StrategyCorePreflightError,
    preflight_core_submission,
)
from app.workers.scheduler import BENCHMARK_SYMBOLS, QUOTES_REFRESH_SCOPE_SQL

# ⚠ Distinct from every other core-arc DB module's ids, and checked rather than
# assumed: `test_2603_core_trade_arc_db.py` already uses 920605/920606 and
# `test_2603_core_{eligibility,rebalance_intent}_db.py` use 920604.  Worker DBs are
# private and `_reset_planner_tables` wipes between tests, so a shared id is not a
# live race — but the seeding helper is `ON CONFLICT DO UPDATE`, so a reused id
# would silently absorb a cleanup leak instead of failing on it.
#
# ⚠ MISSING_INSTRUMENT_ID must be one that NOTHING in the suite ever inserts: the
# tests that use it assert absence, so an id another module seeds would turn a real
# regression into a confusing failure in this file.
INSTRUMENT_ID = 920_611
MISSING_INSTRUMENT_ID = 920_612


def _seed_instrument(conn: psycopg.Connection[Any], *, tradable: bool = True) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable, exchange)
        VALUES (%s, 'CORETEST', 'Core Test ETF', %s, '5')
        ON CONFLICT (instrument_id) DO UPDATE SET is_tradable = EXCLUDED.is_tradable
        """,
        (INSTRUMENT_ID, tradable),
    )


# --------------------------------------------------------------------------
# 1. The observation is anchored on the parameter, not on `instruments`
# --------------------------------------------------------------------------


def test_the_observation_returns_one_row_even_for_an_instrument_that_does_not_exist(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """⚠ The whole reason the FROM clause is a one-row anchor.

    With ``instruments`` as the anchor a missing instrument returns NO row, and the
    kill switch and execution block become unreadable in precisely the branch where
    an emergency stop still has to be reported.  The decision function would then
    have to re-read them in a SECOND statement, from a different snapshot.
    """
    row = ebull_test_conn.execute(_PREFLIGHT_SQL, {"core_instrument_id": MISSING_INSTRUMENT_ID}).fetchone()
    assert row is not None
    assert row[0] is False, "instrument_present must report absence, not vanish"
    ebull_test_conn.rollback()


def test_the_observation_reads_the_kill_switch_alongside_a_missing_instrument(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """An active kill switch must be visible even with no instrument row.

    This is the fail-open this anchor prevents: a caller that learned only
    "instrument missing" would fix the mandate and retry straight into an active
    emergency stop.
    """
    ebull_test_conn.execute(
        "INSERT INTO kill_switch (id, is_active, reason) VALUES (true, true, 'test') "
        "ON CONFLICT (id) DO UPDATE SET is_active = true"
    )
    row = ebull_test_conn.execute(_PREFLIGHT_SQL, {"core_instrument_id": MISSING_INSTRUMENT_ID}).fetchone()
    assert row is not None
    assert row[0] is False  # instrument_present
    assert row[4] is True  # kill_switch_active — still readable
    ebull_test_conn.rollback()


def test_the_observation_binds_the_quote_and_exchange_to_the_instrument(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """The join columns are easy to get wrong and silent when wrong.

    ``exchanges`` is keyed by a TEXT id that ``instruments`` stores in a column
    called ``exchange``, not ``exchange_id``.  A wrong join name would not error —
    it would leave ``asset_class`` NULL for every instrument, which refuses
    everything for a reason no one could diagnose from the code.
    """
    _seed_instrument(ebull_test_conn)
    ebull_test_conn.execute(
        "INSERT INTO exchanges (exchange_id, asset_class) VALUES ('5', 'us_equity') "
        "ON CONFLICT (exchange_id) DO UPDATE SET asset_class = 'us_equity'"
    )
    ebull_test_conn.execute(
        """
        INSERT INTO quotes (instrument_id, quoted_at, bid, ask, spread_flag)
        VALUES (%s, now(), 100.00, 100.02, false)
        ON CONFLICT (instrument_id) DO UPDATE SET quoted_at = EXCLUDED.quoted_at
        """,
        (INSTRUMENT_ID,),
    )
    row = ebull_test_conn.execute(_PREFLIGHT_SQL, {"core_instrument_id": INSTRUMENT_ID}).fetchone()
    assert row is not None
    assert row[0] is True  # instrument_present
    assert row[3] == "us_equity"  # asset_class, via i.exchange -> e.exchange_id
    assert row[8] is not None  # quoted_at
    ebull_test_conn.rollback()


def test_core_preflight_matches_a_plain_halt_to_a_session_variant(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """Nasdaq's CORETEST halt applies to eToro's CORETEST.24-7 row."""
    _seed_instrument(ebull_test_conn)
    ebull_test_conn.execute(
        "UPDATE instruments SET symbol='CORETEST.24-7' WHERE instrument_id=%s",
        (INSTRUMENT_ID,),
    )
    ebull_test_conn.execute(
        """
        INSERT INTO strategy_market_halts (
            source, symbol, halt_at, market, reason_code, resumed_at, observed_at
        ) VALUES ('nasdaq_trader_rss', 'CORETEST', now(), 'NASDAQ', 'T1', NULL, now())
        """
    )

    row = ebull_test_conn.execute(_PREFLIGHT_SQL, {"core_instrument_id": INSTRUMENT_ID}).fetchone()

    assert row is not None
    assert row[1] == "CORETEST.24-7"
    assert row[6] is True
    ebull_test_conn.rollback()


# --------------------------------------------------------------------------
# The lock contract
# --------------------------------------------------------------------------


def test_the_preflight_refuses_to_run_without_the_submission_lock(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """Not holding the lock is a caller BUG, so it raises rather than refusing.

    A refusal is something a caller logs and carries on from — straight into the
    race the lock exists to prevent, with the kill switch read at a moment that has
    no defined relationship to the submission.
    """
    with pytest.raises(StrategyCorePreflightError, match="core_submission_lock"):
        preflight_core_submission(
            ebull_test_conn,
            core_instrument_id=INSTRUMENT_ID,
            action="buy_core",
            now=datetime.now(UTC),
        )
    ebull_test_conn.rollback()


# --------------------------------------------------------------------------
# 2. The quotes_refresh scope arms — the permanent-refusal fixes
# --------------------------------------------------------------------------

#: ⚠ The SQL the JOB executes, imported rather than copied.  Until #2833 this was a
#: hand-copy of arm 4 guarded by a substring assertion; extracting
#: ``QUOTES_REFRESH_SCOPE_SQL`` to module scope retired both, so these tests can no
#: longer pass against SQL that runs nowhere.
_SCOPE_PARAMS = {
    "benchmarks": sorted(BENCHMARK_SYMBOLS),
    "pass_verdict": CORE_ELIGIBILITY_PASS_VERDICT,
}

_MANDATE_INSERT = """
INSERT INTO strategy_core_mandate_events (
    revision, enabled, base_currency, core_instrument_id, core_target_pct,
    liquidity_reserve_pct, rebalance_band_pct, min_rebalance_amount,
    policy_version, mode, changed_by, reason
) VALUES (
    %(revision)s, %(enabled)s, 'USD', %(instrument_id)s, 60, 5, 5, 100,
    'core-mandate-v2', 'paper', 'test', 'scope test'
)
"""


#: A passing proof needs an operator and both live credential rows (FKs).  The
#: ciphertext is a placeholder: nothing here decrypts, and the arm reads only
#: ``instrument_id``/``environment``/``observed_at``/``verdict``.
_PROOF_INSERT = """
INSERT INTO strategy_core_eligibility_proofs (
    instrument_id, operator_id, provider, environment,
    api_key_credential_id, user_key_credential_id, observed_at,
    verdict, reason_code, requested_currency, response_currency,
    settlement_type, direction, leverage_values, qualifying_arm_count,
    allow_open_position, response_digest, policy_version, recorded_by
) VALUES (
    %(instrument_id)s, %(operator_id)s, 'etoro', %(environment)s,
    %(api_key_credential_id)s, %(user_key_credential_id)s, %(observed_at)s,
    %(verdict)s, %(reason_code)s, 'USD', 'usd',
    %(settlement_type)s, %(direction)s, %(leverage_values)s, %(qualifying_arm_count)s,
    %(allow_open_position)s, %(digest)s, %(policy_version)s, 'test'
)
"""


def _seed_account(conn: psycopg.Connection[Any]) -> tuple[UUID, UUID, UUID]:
    operator_id = uuid4()
    conn.execute(
        "INSERT INTO operators (operator_id, username, password_hash) VALUES (%s,%s,'x')",
        (operator_id, f"op_{operator_id.hex[:8]}"),
    )
    ids: list[UUID] = []
    for label in ("api_key", "user_key"):
        row = conn.execute(
            """
            INSERT INTO broker_credentials
                (operator_id, provider, label, environment, ciphertext, last_four, key_version)
            VALUES (%s,'etoro',%s,'demo','\\x00'::bytea,'0000',1)
            RETURNING id
            """,
            (operator_id, label),
        ).fetchone()
        assert row is not None
        ids.append(row[0])
    return operator_id, ids[0], ids[1]


def _record_proof(
    conn: psycopg.Connection[Any],
    account: tuple[UUID, UUID, UUID],
    *,
    verdict: str,
    observed_at: datetime,
    environment: str = "demo",
) -> None:
    operator_id, api_key_id, user_key_id = account
    passing = verdict == CORE_ELIGIBILITY_PASS_VERDICT
    conn.execute(
        _PROOF_INSERT,
        {
            "instrument_id": INSTRUMENT_ID,
            "operator_id": operator_id,
            "environment": environment,
            "api_key_credential_id": api_key_id,
            "user_key_credential_id": user_key_id,
            "observed_at": observed_at,
            "verdict": verdict,
            "reason_code": None if passing else "no_underlying_arm",
            "settlement_type": "real" if passing else None,
            "direction": "long" if passing else None,
            "leverage_values": [1] if passing else None,
            "qualifying_arm_count": 1 if passing else 0,
            "allow_open_position": True if passing else None,
            "digest": "a" * 64,
            "policy_version": CORE_ELIGIBILITY_POLICY_VERSION,
        },
    )


def _in_scope(conn: psycopg.Connection[Any]) -> bool:
    rows = conn.execute(QUOTES_REFRESH_SCOPE_SQL, _SCOPE_PARAMS).fetchall()
    return INSTRUMENT_ID in {int(r[0]) for r in rows}


def test_an_enabled_mandate_puts_its_core_instrument_in_the_quote_scope(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """The bootstrap case: the FIRST core buy is by definition not yet held.

    Without this arm the instrument is quoted only if it happens to be Tier 1/2 or
    a benchmark, so a mandate naming `IVV` refuses ``core_quote_missing`` for ever
    and no amount of waiting fixes it.
    """
    _seed_instrument(ebull_test_conn)
    assert _in_scope(ebull_test_conn) is False, "no mandate — must not be in scope"

    ebull_test_conn.execute(_MANDATE_INSERT, {"revision": 1, "enabled": True, "instrument_id": INSTRUMENT_ID})
    assert _in_scope(ebull_test_conn) is True
    ebull_test_conn.rollback()


def test_a_disabled_LATEST_revision_drops_the_instrument_from_the_quote_scope(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """⚠ THE mandate is the latest revision, not the latest ENABLED one.

    ``load_core_mandate`` is ``ORDER BY revision DESC LIMIT 1`` with no ``WHERE``.
    A scope arm written as ``WHERE enabled ORDER BY revision DESC`` would keep
    quoting the instrument of a superseded enabled revision after the operator
    disabled the mandate — harmless in itself, but a second definition of "the
    mandate", which is how the two silently diverge.
    """
    _seed_instrument(ebull_test_conn)
    ebull_test_conn.execute(_MANDATE_INSERT, {"revision": 1, "enabled": True, "instrument_id": INSTRUMENT_ID})
    assert _in_scope(ebull_test_conn) is True

    ebull_test_conn.execute(_MANDATE_INSERT, {"revision": 2, "enabled": False, "instrument_id": INSTRUMENT_ID})
    assert _in_scope(ebull_test_conn) is False
    ebull_test_conn.rollback()


def test_a_non_tradable_core_instrument_stays_out_of_the_quote_scope(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """Consistent with the other three arms, all of which require ``is_tradable``."""
    _seed_instrument(ebull_test_conn, tradable=False)
    ebull_test_conn.execute(_MANDATE_INSERT, {"revision": 1, "enabled": True, "instrument_id": INSTRUMENT_ID})
    assert _in_scope(ebull_test_conn) is False
    ebull_test_conn.rollback()


# --------------------------------------------------------------------------
# 3. Arm 5 — the CANDIDATE arm (#2833)
# --------------------------------------------------------------------------


def test_a_passing_eligibility_proof_puts_a_candidate_in_the_quote_scope(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """Arm 4 quotes the instrument a mandate NAMES; nothing quoted the one being MEASURED.

    The sleeve's pass bar is a spread percentile over ~5 trading days of stored
    quotes, and only an enabled mandate put an instrument in scope — so the
    measurement that decides the mandate needed quotes that only began accruing
    once the mandate was enabled.  Measured on dev 2026-08-22, all three proved
    candidates (3417/3434/3075) had zero ``quotes`` rows.
    """
    _seed_instrument(ebull_test_conn)
    account = _seed_account(ebull_test_conn)
    assert _in_scope(ebull_test_conn) is False, "unproved — must not be in scope"

    _record_proof(ebull_test_conn, account, verdict="underlying", observed_at=datetime.now(UTC))
    assert _in_scope(ebull_test_conn) is True
    ebull_test_conn.rollback()


def test_a_failing_proof_does_not_put_a_candidate_in_the_quote_scope(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """The arm admits on the VERDICT, not on having been asked.

    Every US-domiciled ETF measured for #2834 (MTUM, SPMO, QUAL, SPHQ, IUSV, IWD,
    AVUV) returned ``no_underlying_arm`` — CFD-only, so the sleeve can never hold
    one.  Quoting them would spend the fetch budget on instruments no reader of
    ``quotes`` can act on.
    """
    _seed_instrument(ebull_test_conn)
    account = _seed_account(ebull_test_conn)
    _record_proof(ebull_test_conn, account, verdict="not_underlying", observed_at=datetime.now(UTC))
    assert _in_scope(ebull_test_conn) is False
    ebull_test_conn.rollback()


def test_a_later_failing_proof_removes_a_candidate_from_the_quote_scope(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """⚠ Membership is the LATEST proof per environment, not "ever passed".

    Written as ``EXISTS (… verdict = 'underlying')`` over all rows, an instrument
    that passed once would be quoted for ever after the broker stopped offering the
    underlying — the append-only table makes a stale belief permanent unless the
    reader takes the newest row.  Re-proving is how a candidate leaves scope.
    """
    _seed_instrument(ebull_test_conn)
    account = _seed_account(ebull_test_conn)
    now = datetime.now(UTC)
    _record_proof(ebull_test_conn, account, verdict="underlying", observed_at=now - timedelta(days=2))
    assert _in_scope(ebull_test_conn) is True

    _record_proof(ebull_test_conn, account, verdict="not_underlying", observed_at=now)
    assert _in_scope(ebull_test_conn) is False
    ebull_test_conn.rollback()


def test_two_proofs_at_the_SAME_instant_resolve_to_the_later_row(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """⚠ ``observed_at`` DEFAULTs to ``now()`` — the TRANSACTION timestamp, not the clock.

    Two proofs written in one transaction therefore TIE, and ``ORDER BY observed_at
    DESC`` alone picks either one.  ``load_latest_core_eligibility_proof`` breaks the
    tie on ``core_eligibility_proof_id DESC``; the arm must break it the same way or
    "the latest proof" means two different rows depending on which module asks.
    """
    _seed_instrument(ebull_test_conn)
    account = _seed_account(ebull_test_conn)
    tied = datetime.now(UTC)
    _record_proof(ebull_test_conn, account, verdict="underlying", observed_at=tied)
    _record_proof(ebull_test_conn, account, verdict="not_underlying", observed_at=tied)
    assert _in_scope(ebull_test_conn) is False, "the LATER row is the failing one"
    ebull_test_conn.rollback()


def test_a_stale_passing_proof_still_holds_a_candidate_in_the_quote_scope(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """⚠ Deliberately NOT gated on ``CORE_ELIGIBILITY_MAX_AGE`` (24h).

    That ceiling governs CONFIGURING a mandate — an act that commits capital and
    must rest on a fresh observation.  Applying it here would re-create the cycle
    the arm exists to break: a spread measurement spans five trading days, so a
    24h membership window means the candidate drops out on day two and the bar can
    never be measured.
    """
    _seed_instrument(ebull_test_conn)
    account = _seed_account(ebull_test_conn)
    stale = datetime.now(UTC) - (CORE_ELIGIBILITY_MAX_AGE * 10)
    _record_proof(ebull_test_conn, account, verdict="underlying", observed_at=stale)
    assert _in_scope(ebull_test_conn) is True
    ebull_test_conn.rollback()


def test_a_non_tradable_candidate_stays_out_of_the_quote_scope(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """Consistent with all four other arms."""
    _seed_instrument(ebull_test_conn, tradable=False)
    account = _seed_account(ebull_test_conn)
    _record_proof(ebull_test_conn, account, verdict="underlying", observed_at=datetime.now(UTC))
    assert _in_scope(ebull_test_conn) is False
    ebull_test_conn.rollback()
