"""Statutory matching against the test database (#3616 slice 3d-2; spec tests 14-19, sql/489).

Auto-marked ``db`` (pulls ``ebull_test_conn``, which wipes between tests). Generations are seeded directly into the
sql/488 tables on autocommit connections, as the evidence run publishes them; every run is ``run_tax_matching`` on its
own autocommit connection, as the job calls it.
"""

from __future__ import annotations

import hashlib
import threading
from collections.abc import Iterator
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import psycopg
import pytest
from psycopg.types.json import Jsonb

from app.services import tax_matching
from app.services.tax_matching import MatchingRunReport, effective_tax_matching, run_tax_matching
from tests.fixtures.ebull_test_db import test_database_url

ACCOUNT = 4242
D = Decimal

#: (subject, outcome, instrument_id)
Subject = tuple[str, str, int | None]
#: (lot_key, subject, instrument_id, direction, uk_date, units, amount_gbp or None for pending FX)
Lot = tuple[str, str, int, str, date, str, str | None]


def _connect() -> psycopg.Connection[tuple]:
    return psycopg.connect(test_database_url(), autocommit=True)


def _rows(sql: str, params: tuple[Any, ...] = ()) -> list[tuple]:
    with _connect() as conn:
        return conn.execute(sql, params).fetchall()  # type: ignore[arg-type]


def _run() -> MatchingRunReport:
    with _connect() as conn:
        return run_tax_matching(conn)


def _effective() -> tax_matching.EffectiveMatching | None:
    with _connect() as conn:
        return effective_tax_matching(conn)


def _tax_year(day: date) -> str:
    start = day.year if (day.month, day.day) >= (4, 6) else day.year - 1
    return f"{start}/{(start + 1) % 100:02d}"


def _instrument(iid: int, symbol: str, canonical: int | None = None) -> None:
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, country, is_tradable,
                                     canonical_instrument_id)
            VALUES (%s, %s, 'tax 3616 co', '4', 'USD', 'US', TRUE, %s)
            """,
            (iid, symbol, canonical),
        )


def _scope() -> None:
    with _connect() as conn:
        conn.execute(
            "INSERT INTO trade_ledger_scope (singleton, environment, account_id, established_at) "
            "VALUES (TRUE, 'demo', %s, now())",
            (ACCOUNT,),
        )


def _insert_subject(conn: psycopg.Connection[tuple], run_id: int, s: Subject) -> None:
    subject, outcome, instrument_id = s
    conn.execute(
        """
        INSERT INTO tax_evidence_subjects
            (run_id, subject, order_id, etoro_instrument_id, instrument_id, product, outcome, reasons,
             trade_event_ids, held_positions, execution_evidence, total_fees_unobserved_position_ids)
        VALUES (%s, %s, %s, 1, %s, 'real', %s, %s, '{}', '{}'::jsonb, %s, '{}')
        """,
        (
            run_id,
            subject,
            int(subject.split(":")[1]),
            instrument_id,
            outcome,
            ["price_unestablished"] if outcome == "unresolved" else [],
            "lookup" if outcome == "admitted" else None,
        ),
    )


def _insert_lot(conn: psycopg.Connection[tuple], run_id: int, lot: Lot) -> None:
    key, subject, instrument_id, direction, day, units, gbp = lot
    conn.execute(
        """
        INSERT INTO tax_evidence_lots
            (run_id, lot_key, subject, order_id, instrument_id, etoro_instrument_id, direction, position_ids,
             trade_event_ids, executed_at, uk_date, tax_year, units, usd_amount, consideration_status,
             execution_evidence, fx_rate_date, fx_status, fx_gbp_per_usd, fx_policy_id, amount_gbp, fingerprint)
        VALUES (%s, %s, %s, %s, %s, 1, %s, '{1}', '{1}', %s, %s, %s, %s, 1, 'broker_stated_unreconciled',
                'lookup', %s, %s, %s, 'tax_fx_v1', %s, %s)
        """,
        (
            run_id,
            key,
            subject,
            int(subject.split(":")[1]),
            instrument_id,
            direction,
            datetime(day.year, day.month, day.day, 12, tzinfo=UTC),
            day,
            _tax_year(day),
            units,
            day,
            "pending" if gbp is None else "published",
            None if gbp is None else 1,
            gbp,
            hashlib.sha256(f"{key}|{units}|{gbp}".encode()).hexdigest(),
        ),
    )


def _generation(
    subjects: list[Subject],
    lots: list[Lot],
    *,
    policy: str = "broker_evidence_v2",
    account: int = ACCOUNT,
    fetch_started: datetime | None = None,
) -> int:
    """A published generation, as sql/488 holds one."""
    with _connect() as conn, conn.transaction():
        fetch_id = None
        if fetch_started is not None:
            fetch = conn.execute(
                """
                INSERT INTO trade_history_fetches
                    (environment, account_id, portfolio_fetched_at, portfolio_real_positions, started_at, finished_at,
                     outcome, row_count)
                VALUES ('demo', %s, %s, 0, %s, %s, 'ok', 0) RETURNING fetch_id
                """,
                (account, fetch_started, fetch_started, fetch_started),
            ).fetchone()
            assert fetch is not None
            fetch_id = fetch[0]
        row = conn.execute(
            "INSERT INTO tax_evidence_runs (environment, account_id, started_at, state, evidence_policy_id) "
            "VALUES ('demo', %s, now(), 'in_progress', %s) RETURNING run_id",
            (account, policy),
        ).fetchone()
        assert row is not None
        run_id = int(row[0])
        for s in subjects:
            _insert_subject(conn, run_id, s)
        for lot in lots:
            _insert_lot(conn, run_id, lot)
        conn.execute(
            "UPDATE tax_evidence_runs SET state = 'published', finished_at = now(), counts = %s, "
            "history_fetch_ok_id = %s WHERE run_id = %s",
            (Jsonb({}), fetch_id, run_id),
        )
    return run_id


ADMITTED: list[Subject] = [("order:1", "admitted", 1)]
LOTS: list[Lot] = [
    ("L1", "order:1", 1, "acquisition", date(2026, 1, 5), "10", "100"),
    ("L2", "order:1", 1, "disposal", date(2026, 3, 1), "4", "60"),
]


@pytest.fixture
def spy(ebull_test_conn: psycopg.Connection[Any]) -> Iterator[None]:
    """The ledger scope and one instrument, on the wiped test database."""
    _scope()
    _instrument(1, "SPY")
    yield


def _children(run_id: int) -> int:
    return sum(
        _rows(f"SELECT count(*) FROM {table} WHERE matching_run_id = %s", (run_id,))[0][0]  # noqa: S608 - fixed names
        for table in (
            "tax_matching_instruments",
            "tax_matching_lot_classes",
            "tax_matching_securities",
            "tax_matching_dependencies",
            "tax_statutory_disposals",
            "tax_identifications",
        )
    )


# --- 14. one run per key ----------------------------------------------------------------------------------------------


def test_one_published_run_per_key(spy: None) -> None:
    generation = _generation(ADMITTED, LOTS)
    first = _run()
    assert (first.outcome, first.evidence_run_id) == ("published", generation)
    assert first.counts == {
        "lots": 2,
        "securities_computed": 1,
        "securities_withheld": 0,
        "statutory_disposals": 1,
        "identifications": 1,
        "dependencies": 0,
    }
    assert _rows(
        "SELECT uk_date, tax_year, proceeds_gbp, cost_gbp, gain_gbp, status FROM tax_statutory_disposals "
        "WHERE matching_run_id = %s",
        (first.matching_run_id,),
    ) == [(date(2026, 3, 1), "2025/26", D("60"), D("40"), D("20"), "computed")]
    assert _rows(
        "SELECT status, pool_units, pool_cost_gbp FROM tax_matching_securities WHERE matching_run_id = %s",
        (first.matching_run_id,),
    ) == [("computed", D("6"), D("60"))]

    second = _run()
    assert (second.outcome, second.matching_run_id) == ("already_published", first.matching_run_id)

    # An instruments change in the security's component is a new digest, so a new run.
    _instrument(2, "SPY.RTH", 1)
    third = _run()
    assert third.outcome == "published" and third.matching_run_id != first.matching_run_id
    assert _rows(
        "SELECT instrument_ids FROM tax_matching_securities WHERE matching_run_id = %s", (third.matching_run_id,)
    ) == [([1, 2],)]


def test_another_evidence_policy_is_refused_and_stays_refused(spy: None) -> None:
    _generation(ADMITTED, LOTS, policy="broker_evidence_v1")
    refused = _run()
    assert (refused.outcome, refused.reason) == ("refused", "evidence_policy_unsupported")
    assert _children(refused.matching_run_id or 0) == 0
    again = _run()
    assert (again.outcome, again.matching_run_id, again.reason) == (
        "refused",
        refused.matching_run_id,
        "evidence_policy_unsupported",
    )
    assert _rows("SELECT state, reason FROM tax_matching_runs") == [("refused", "evidence_policy_unsupported")]


def test_no_generation_writes_nothing(spy: None) -> None:
    assert _run().outcome == "no_generation"
    assert _rows("SELECT count(*) FROM tax_matching_runs") == [(0,)]


# --- 15. the seal -----------------------------------------------------------------------------------------------------

_DOC = '{"version":"matching_inputs_v1"}'


def _building(conn: psycopg.Connection[tuple], evidence_run_id: int, document: str = _DOC) -> int:
    row = conn.execute(
        """
        INSERT INTO tax_matching_runs (evidence_run_id, matching_policy_id, class_policy_id, inputs_digest,
                                       inputs_document, state, computed_at, withheld_all)
        VALUES (%s, 'share_identification_v1', 'consideration_classes_v1', %s, %s, 'building', now(), FALSE)
        RETURNING matching_run_id
        """,
        (evidence_run_id, hashlib.sha256(document.encode()).hexdigest(), document),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _child(conn: psycopg.Connection[tuple], run_id: int) -> None:
    conn.execute(
        "INSERT INTO tax_matching_instruments (matching_run_id, instrument_id, symbol, component_id) "
        "VALUES (%s, 1, 'SPY', 1)",
        (run_id,),
    )


def test_the_seal(spy: None) -> None:
    generation = _generation(ADMITTED, LOTS)
    with _connect() as conn:
        with pytest.raises(psycopg.errors.RaiseException, match="inserted building"), conn.transaction():
            conn.execute(
                "INSERT INTO tax_matching_runs (evidence_run_id, matching_policy_id, class_policy_id, inputs_digest, "
                "inputs_document, state, computed_at, withheld_all, counts) "
                "VALUES (%s, 'p', 'c', %s, %s, 'published', now(), FALSE, '{}')",
                (generation, hashlib.sha256(_DOC.encode()).hexdigest(), _DOC),
            )

        with conn.transaction():
            run_id = _building(conn, generation)
            _child(conn, run_id)
            conn.execute(
                "UPDATE tax_matching_runs SET state = 'published', counts = '{}' WHERE matching_run_id = %s", (run_id,)
            )

        refusals = [
            (
                "UPDATE tax_matching_runs SET state = 'refused', reason = 'x' WHERE matching_run_id = %s",
                "cannot change",
            ),
            ("UPDATE tax_matching_runs SET counts = '{\"a\": 1}' WHERE matching_run_id = %s", "cannot change"),
            ("DELETE FROM tax_matching_runs WHERE matching_run_id = %s", "cannot be deleted"),
            (
                "INSERT INTO tax_matching_instruments (matching_run_id, instrument_id, symbol, component_id) "
                "VALUES (%s, 2, 'X', 2)",
                "takes no rows",
            ),
            ("UPDATE tax_matching_instruments SET symbol = 'Y' WHERE matching_run_id = %s", "never updated"),
            ("DELETE FROM tax_matching_instruments WHERE matching_run_id = %s", "never updated"),
        ]
        for sql, message in refusals:
            with pytest.raises(psycopg.errors.RaiseException, match=message), conn.transaction():
                conn.execute(sql, (run_id,))  # type: ignore[arg-type]

        # A building run with a child cannot be refused, nor sealed with another column changed.
        with pytest.raises(psycopg.errors.RaiseException, match="cannot be refused"), conn.transaction():
            other = _building(conn, generation, '{"version":"other"}')
            _child(conn, other)
            conn.execute(
                "UPDATE tax_matching_runs SET state = 'refused', reason = 'x', counts = '{}' "
                "WHERE matching_run_id = %s",
                (other,),
            )
        with pytest.raises(psycopg.errors.RaiseException, match="changes only"), conn.transaction():
            other = _building(conn, generation, '{"version":"other"}')
            conn.execute(
                "UPDATE tax_matching_runs SET state = 'published', counts = '{}', "
                "computed_at = now() - interval '1 day' WHERE matching_run_id = %s",
                (other,),
            )
        # Nor with its identity moved (an identity column accepts DEFAULT on UPDATE).
        with pytest.raises(psycopg.errors.RaiseException, match="changes only"), conn.transaction():
            other = _building(conn, generation, '{"version":"other"}')
            conn.execute(
                "UPDATE tax_matching_runs SET state = 'published', counts = '{}', matching_run_id = DEFAULT "
                "WHERE matching_run_id = %s",
                (other,),
            )
        # A commit leaving a run building is refused (the deferred trigger fires at COMMIT).
        with pytest.raises(psycopg.errors.RaiseException, match="still building at commit"), conn.transaction():
            _building(conn, generation, '{"version":"other"}')

    assert _rows("SELECT matching_run_id, state FROM tax_matching_runs") == [(run_id, "published")]


def test_an_exception_after_child_inserts_leaves_nothing(spy: None, monkeypatch: pytest.MonkeyPatch) -> None:
    _generation(ADMITTED, LOTS)

    def broken_seal(*_args: object) -> None:
        raise RuntimeError("seal failed")

    monkeypatch.setattr(tax_matching, "_seal", broken_seal)
    with pytest.raises(RuntimeError, match="seal failed"):
        _run()
    assert _rows("SELECT count(*) FROM tax_matching_runs") == [(0,)]
    assert _rows("SELECT count(*) FROM tax_matching_securities") == [(0,)]
    # The lock was released: a later run proceeds.
    monkeypatch.undo()
    assert _run().outcome == "published"


# --- 16. a change to a published generation ---------------------------------------------------------------------------


@pytest.mark.parametrize("late", ["lot", "subject"])
def test_a_late_insert_into_a_published_generation_makes_a_new_run(spy: None, late: str) -> None:
    generation = _generation(ADMITTED, LOTS)
    first = _run()
    with _connect() as conn:
        if late == "lot":
            _insert_lot(conn, generation, ("L3", "order:1", 1, "acquisition", date(2026, 3, 10), "2", "30"))
        else:
            _insert_subject(conn, generation, ("order:2", "unresolved", 1))
    assert _effective() is None  # the old run is no longer selected
    ((document, digest),) = _rows(
        "SELECT inputs_document, inputs_digest FROM tax_matching_runs WHERE matching_run_id = %s",
        (first.matching_run_id,),
    )
    assert hashlib.sha256(document.encode()).hexdigest() == digest
    second = _run()
    assert second.outcome == "published" and second.matching_run_id != first.matching_run_id
    assert _effective() == tax_matching.EffectiveMatching(second.matching_run_id or 0, generation)
    expected = {"lot": ("computed", [("bed_and_breakfast",), ("s104_pool",)]), "subject": ("withheld", [])}[late]
    assert _rows(
        "SELECT status FROM tax_matching_securities WHERE matching_run_id = %s", (second.matching_run_id,)
    ) == [(expected[0],)]
    assert (
        _rows("SELECT rule FROM tax_identifications WHERE matching_run_id = %s ORDER BY seq", (second.matching_run_id,))
        == expected[1]
    )


# --- 17. withholding --------------------------------------------------------------------------------------------------


def test_an_unresolved_subject_withholds_its_security_and_names_itself(spy: None) -> None:
    _generation([*ADMITTED, ("order:2", "unresolved", 1)], LOTS)
    run = _run()
    assert _rows(
        "SELECT status, pool_units FROM tax_matching_securities WHERE matching_run_id = %s", (run.matching_run_id,)
    ) == [("withheld", None)]
    assert _rows(
        "SELECT seq, tax_security_id, reason, subject, instrument_id FROM tax_matching_dependencies "
        "WHERE matching_run_id = %s",
        (run.matching_run_id,),
    ) == [(1, 1, "unresolved_subject", "order:2", 1)]
    assert _rows("SELECT count(*) FROM tax_statutory_disposals") == [(0,)]


def test_an_unplaceable_subject_withholds_everything_with_no_security_row(spy: None) -> None:
    _generation([("order:2", "unresolved", None)], [])
    run = _run()
    assert _rows("SELECT withheld_all FROM tax_matching_runs") == [(True,)]
    assert _rows("SELECT count(*) FROM tax_matching_securities") == [(0,)]
    assert _rows(
        "SELECT tax_security_id, reason, subject FROM tax_matching_dependencies WHERE matching_run_id = %s",
        (run.matching_run_id,),
    ) == [(None, "unplaceable", "order:2")]


def test_a_security_withheld_or_gone_in_the_next_generation(spy: None) -> None:
    _instrument(3, "QQQ")
    q = [
        ("Q1", "order:3", 3, "acquisition", date(2026, 1, 6), "5", "50"),
        ("Q2", "order:3", 3, "disposal", date(2026, 3, 2), "5", "70"),
    ]
    _generation([*ADMITTED, ("order:3", "admitted", 3)], [*LOTS, *q])
    n = _run()
    # N+1: security 1 gains an unresolved subject; security 3's lots leave the generation.
    _generation([*ADMITTED, ("order:2", "unresolved", 1)], LOTS)
    n1 = _run()
    assert _rows(
        "SELECT tax_security_id, status FROM tax_matching_securities WHERE matching_run_id = %s ORDER BY 1",
        (n.matching_run_id,),
    ) == [(1, "computed"), (3, "computed")]
    assert _rows(
        "SELECT tax_security_id, status, pool_units FROM tax_matching_securities WHERE matching_run_id = %s",
        (n1.matching_run_id,),
    ) == [(1, "withheld", None)]
    assert _rows("SELECT count(*) FROM tax_statutory_disposals WHERE matching_run_id = %s", (n1.matching_run_id,)) == [
        (0,)
    ]
    assert _rows("SELECT count(*) FROM tax_statutory_disposals WHERE matching_run_id = %s", (n.matching_run_id,)) == [
        (2,)
    ]


# --- 18. concurrency --------------------------------------------------------------------------------------------------


def test_two_callers_on_one_generation_make_one_run(spy: None) -> None:
    _generation(ADMITTED, LOTS)
    outcomes: list[str] = []
    barrier = threading.Barrier(2)

    def call() -> None:
        barrier.wait()
        outcomes.append(_run().outcome)

    threads = [threading.Thread(target=call) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sorted(outcomes) == ["already_published", "published"]
    assert _rows("SELECT count(*) FROM tax_matching_runs") == [(1,)]


def test_a_run_matches_the_generation_it_resolved(spy: None, monkeypatch: pytest.MonkeyPatch) -> None:
    first = _generation(ADMITTED, LOTS)
    compute = tax_matching.compute_matching
    published: list[int] = []

    def publish_meanwhile(inputs: tax_matching.MatchingInputs) -> tax_matching.MatchingResult:
        published.append(_generation(ADMITTED, LOTS[:1]))
        return compute(inputs)

    monkeypatch.setattr(tax_matching, "compute_matching", publish_meanwhile)
    run = _run()
    assert (run.outcome, run.evidence_run_id) == ("published", first)
    monkeypatch.undo()
    later = _run()
    assert (later.outcome, later.evidence_run_id) == ("published", published[0])


# --- 19. selection ----------------------------------------------------------------------------------------------------


def test_selection_is_exact_with_no_fallback(spy: None) -> None:
    first = _generation(ADMITTED, LOTS)
    run = _run()
    assert _effective() == tax_matching.EffectiveMatching(run.matching_run_id or 0, first)
    # A published generation of another scope is never selected.
    _generation(ADMITTED, LOTS[:1], account=777)
    assert _effective() == tax_matching.EffectiveMatching(run.matching_run_id or 0, first)
    # The effective generation without a run: unavailable, though an older published run exists.
    _generation(ADMITTED, LOTS[:1])
    assert _effective() is None
    # A refused run is unavailable too.
    _generation(ADMITTED, LOTS, policy="broker_evidence_v1")
    assert _run().outcome == "refused"
    assert _effective() is None


def test_the_window_reads_the_generations_ok_fetch(spy: None) -> None:
    lots: list[Lot] = [
        ("L1", "order:1", 1, "acquisition", date(2026, 9, 1), "10", "100"),
        ("L2", "order:1", 1, "disposal", date(2026, 9, 23), "4", "60"),
    ]
    _generation(ADMITTED, lots, fetch_started=datetime(2026, 10, 23, 22, 59, 59, tzinfo=UTC))
    opened = _run()
    _generation(ADMITTED, lots, fetch_started=datetime(2026, 10, 23, 23, tzinfo=UTC))
    elapsed = _run()
    window = "SELECT 'window_open' = ANY(reasons) FROM tax_statutory_disposals WHERE matching_run_id = %s"
    assert _rows(window, (opened.matching_run_id,)) == [(True,)]
    assert _rows(window, (elapsed.matching_run_id,)) == [(False,)]
    assert _rows("SELECT history_fetch_started_at FROM tax_matching_runs ORDER BY 1") == [
        (datetime(2026, 10, 23, 22, 59, 59, tzinfo=UTC),),
        (datetime(2026, 10, 23, 23, tzinfo=UTC),),
    ]
