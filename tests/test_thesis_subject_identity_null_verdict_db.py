"""#2647 — the NULL subject-identity triple must PLAN, not just typecheck.

Both statements that write ``theses.subject_identity_ok`` re-derive the sibling
columns in SQL from the same named parameter::

    %(ok)s, CASE WHEN %(ok)s IS NULL THEN NULL ELSE ... END

psycopg3 dedups that into one ``$n``. When the value is ``None`` it is sent
untyped (OID 0) and its only type-determining context is a ``NullTest``, which
constrains nothing — so Postgres cannot plan the statement and raises
``AmbiguousParameter``. The failure is confined to the ``None`` binding: for
``True``/``False`` psycopg infers OID 16 from the Python type and every shape
plans, which is how a fail-closed branch shipped behind a green suite.

⚠⚠ WHY THIS FILE IS DB-TIER AND ITS SIBLING IS NOT.
``tests/test_thesis_subject_identity_quarantine.py`` already asserts the boot
probe's ``ok=None`` reset — and passes, because it drives a ``_FakeConn`` that
records parameters and never sends the statement anywhere. A mocked cursor
cannot observe a plan-time type-inference failure; the invariant is a property
of the PLAN, not of the Python. That file stays pure by design, so the guard
that needs a real backend lives here instead.

Two statements, one defect class, so one file: ``_insert_thesis_atomic``
(``app/services/thesis.py``) and ``ensure_subject_identity_verdicts``
(``app/services/thesis_subject_identity.py``). Each is exercised on BOTH sides —
the NULL path that raised and the set path that always worked — because a
"fix" that writes NULL unconditionally would satisfy the NULL half alone.
"""

from __future__ import annotations

from typing import Any

import psycopg
import pytest

from app.services.thesis import _insert_thesis_atomic
from app.services.thesis_subject_identity import RULE_SET_VERSION, ensure_subject_identity_verdicts

pytestmark = pytest.mark.db

_WRITER: dict[str, object] = {
    "thesis_type": "compounder",
    "confidence_score": 0.75,
    "stance": "buy",
    "buy_zone_low": 150.0,
    "buy_zone_high": 170.0,
    "base_value": 200.0,
    "bull_value": 250.0,
    "bear_value": 120.0,
    "break_conditions": ["Revenue growth falls below 10% for two consecutive quarters"],
    # Names OTEX explicitly so the checkable-subject case verdicts TRUE and the
    # set-triple assertion is not accidentally satisfied by a FALSE verdict.
    "memo_markdown": "## OTEX\n\nOpen Text renewals are sticky.",
}


@pytest.fixture
def conn(ebull_test_conn: psycopg.Connection[Any]) -> psycopg.Connection[Any]:
    return ebull_test_conn


def _seed_instrument(
    conn: psycopg.Connection[Any],
    instrument_id: int,
    symbol: str,
    company_name: str,
) -> int:
    """⚠ ``symbol`` and ``company_name`` are both NOT NULL, so an unverdictable
    instrument is one carrying EMPTY STRINGS, not NULLs — which is exactly what
    ``subject_is_checkable`` strips-and-tests for."""
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable)"
        " VALUES (%s, %s, %s, TRUE)",
        (instrument_id, symbol, company_name),
    )
    conn.commit()
    return instrument_id


def _read_triple(conn: psycopg.Connection[Any], thesis_id: int) -> tuple[Any, Any, Any]:
    row = conn.execute(
        "SELECT subject_identity_ok, subject_identity_rule_version, subject_identity_checked_at"
        " FROM theses WHERE thesis_id = %s",
        (thesis_id,),
    ).fetchone()
    assert row is not None
    return (row[0], row[1], row[2])


class TestInsertThesisAtomic:
    def test_unverdictable_subject_stores_the_whole_triple_null(
        self, conn: psycopg.Connection[Any]
    ) -> None:
        """The defect: this path ABORTED the insert rather than degrading to it.

        ``_insert_thesis_atomic``'s docstring makes the NULL triple the
        deliberate representation of "no usable subject, so nobody decided" —
        the state consumers fail closed on. Untyped, writing it raised
        ``AmbiguousParameter`` and no thesis row was written at all, which is a
        strictly worse outcome than a row every consumer refuses.
        """
        iid = _seed_instrument(conn, 9201, "TNV", "Thesis Null Verdict Test Co")

        with conn.transaction():
            thesis_id, _version = _insert_thesis_atomic(
                conn,
                iid,
                _WRITER,
                None,
                model="qwen3:14b",
                provider="openai_compatible",
                subject=None,  # nothing to check against -> verdict NULL
            )

        assert _read_triple(conn, thesis_id) == (None, None, None)

    def test_empty_subject_dict_is_also_unverdictable(
        self, conn: psycopg.Connection[Any]
    ) -> None:
        """``_build_context`` yields ``{}`` when the instruments row is missing,
        and that is the production shape of this path — not the ``None`` above.
        Scored as False it would read as "checked and failed"; it must be NULL.
        """
        iid = _seed_instrument(conn, 9202, "TNW", "Thesis Null Verdict Test Co Two")

        with conn.transaction():
            thesis_id, _version = _insert_thesis_atomic(
                conn, iid, _WRITER, None, model="m", provider="p", subject={}
            )

        assert _read_triple(conn, thesis_id) == (None, None, None)

    def test_checkable_subject_stores_the_whole_triple_set(
        self, conn: psycopg.Connection[Any]
    ) -> None:
        """The other side of sql/332's all-or-nothing CHECK. Without this a fix
        that binds NULL unconditionally would pass the two tests above."""
        iid = _seed_instrument(conn, 9203, "OTEX", "Open Text Corp")

        with conn.transaction():
            thesis_id, _version = _insert_thesis_atomic(
                conn,
                iid,
                _WRITER,
                None,
                model="m",
                provider="p",
                subject={"symbol": "OTEX", "company_name": "Open Text Corp"},
            )

        ok, version, checked_at = _read_triple(conn, thesis_id)
        assert ok is True
        assert version == RULE_SET_VERSION
        assert checked_at is not None


class TestEnsureSubjectIdentityVerdictsAgainstPostgres:
    """⚠ This runs at LIFESPAN. Untyped, the reset direction raised at
    application start, not in a background job."""

    def test_a_row_that_became_unverdictable_is_reset_to_null(
        self, conn: psycopg.Connection[Any]
    ) -> None:
        """Real-backend twin of the ``_FakeConn`` test of the same name in
        ``tests/test_thesis_subject_identity_quarantine.py``. That one asserts
        the probe DECIDES ``ok=None``; this one asserts the decision can be
        WRITTEN.
        """
        iid = _seed_instrument(conn, 9204, "", "")  # unverdictable: both blank

        with conn.transaction():
            thesis_id, _version = _insert_thesis_atomic(
                conn,
                iid,
                _WRITER,
                None,
                model="m",
                provider="p",
                # Stamp a verdict the current rule can no longer reproduce, so
                # the probe has something to reset rather than a no-op skip.
                subject={"symbol": "OTEX", "company_name": "Open Text Corp"},
            )
        assert _read_triple(conn, thesis_id)[0] is True

        # ⚠ The probe selects on ``rule_version IS DISTINCT FROM`` the current
        # one, so a row stamped with the CURRENT version is never re-read. Age
        # the stamp to reach the reset — which is also how it happens for real:
        # the rule changes, and only then is the corpus re-verdicted.
        with conn.transaction():
            conn.execute(
                "UPDATE theses SET subject_identity_rule_version = %s WHERE thesis_id = %s",
                ("thesis-subject-identity-v1+000000000000", thesis_id),
            )

        with conn.transaction():
            written = ensure_subject_identity_verdicts(conn)

        assert written == 1
        assert _read_triple(conn, thesis_id) == (None, None, None)

    def test_a_verdictable_row_keeps_a_set_triple(self, conn: psycopg.Connection[Any]) -> None:
        """The probe's write path on a non-NULL verdict — the shape that always
        planned, kept here so a regression is attributed to the NULL binding
        rather than to the probe as a whole."""
        iid = _seed_instrument(conn, 9205, "OTEX", "Open Text Corp")

        with conn.transaction():
            thesis_id, _version = _insert_thesis_atomic(
                conn, iid, _WRITER, None, model="m", provider="p", subject=None
            )
        assert _read_triple(conn, thesis_id) == (None, None, None)

        with conn.transaction():
            written = ensure_subject_identity_verdicts(conn)

        assert written == 1
        ok, version, checked_at = _read_triple(conn, thesis_id)
        assert ok is True
        assert version == RULE_SET_VERSION
        assert checked_at is not None
