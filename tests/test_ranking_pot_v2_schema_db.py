"""#3592 slice 3a — ``sql/463`` against real Postgres (spec ``2026-10-03-3592-ranking-pot-v2.md`` §4, §8; Appendix A
R2-46–52).

- Declaration terms: ``ranking-pot-v2`` carries ``execution = "none"`` and ``book_count = k_controls + 3``; any other id
  may carry ``"none"`` and, with a ``book_count``, exactly ``k_controls + 2``.
- The completion fence counts the declaration's own books (exactly 0..count − 1, well-formed, flat); a declaration
  without ``book_count`` keeps v1's K + 2.
- ``execution = "none"``: ``→ executing`` and every executed-book relation refuse it; the relation inventory is pinned.
- v2's rebalance refusals are in the attempt table's vocabulary.

v1's own freeze, activation, look and wind-down paths stay covered by ``test_ranking_pot_*_db.py``; the v1 cases here
pin the fence's legacy branch directly.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import date
from typing import Any, LiteralString, cast, get_args

import psycopg
import pytest
from psycopg.types.json import Jsonb

from app.services import ranking_pot_v2 as v2
from app.services import result_ledger
from app.services.ai_trial_pack import canonical_sha256
from app.services.ranking_pot_freeze import prereg_declaration
from app.services.result_ledger import freeze_preregistration
from tests.test_ranking_pot_rebalance_db import _decided, _insert
from tests.test_ranking_pot_schema_db import _move

Conn = psycopg.Connection[Any]

V2 = "ranking-pot-v2"
V1 = "ranking-pot-v1"
K = 2
V2_TERMS: dict[str, Any] = {"k_controls": K, "book_count": K + 3, "execution": "none"}

#: Every relation that holds executed-book state (sql/449, sql/450, sql/453) plus sql/458's entry tickets.
EXECUTED_BOOK_RELATIONS = frozenset(
    {
        "ranking_pot_activations",
        "ranking_pot_exec_rebalances",
        "ranking_pot_exec_lifecycles",
        "ranking_pot_exec_decisions",
        "ranking_pot_exec_exit_stamps",
        "ranking_pot_exec_submissions",
        "ranking_pot_exec_level_observations",
        "strategy_entry_tickets",
    }
)


class _RegisterWithV2:
    """The trial register as it will be once the freeze slice adds v2's entry; the schema does not depend on it."""

    def __init__(self, real: Any) -> None:
        self._real = real

    def trial_for_declaration(self, strategy_id: str, strategy_version: str) -> object | None:
        return object() if strategy_id == V2 else self._real.trial_for_declaration(strategy_id, strategy_version)


@pytest.fixture(autouse=True)
def _v2_registered(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(result_ledger, "TRIAL_REGISTER", _RegisterWithV2(result_ledger.TRIAL_REGISTER))


def _declaration_sql(conn: Conn, strategy_id: str, terms: dict[str, Any]) -> int:
    """The #2599 row, the document and the genesis event, in the caller's transaction."""
    row = conn.execute("SELECT coalesce(max(family_seq), 0) + 1 FROM ranking_pot_declarations").fetchone()
    assert row is not None
    seq = int(row[0])
    doc = {"strategy_id": strategy_id, "strategy_version": "v1", "family": "ranking-pot", "family_seq": seq}
    doc["terms"] = terms
    sha = canonical_sha256(doc)
    prereg = replace(prereg_declaration(doc_sha256=sha, declared_by="t"), strategy_id=strategy_id)
    decl = freeze_preregistration(cast(psycopg.Connection[tuple], conn), prereg)
    conn.execute(
        "INSERT INTO ranking_pot_declarations "
        "(declaration_id, strategy_id, strategy_version, family, family_seq, doc_path, doc, doc_sha256) "
        "VALUES (%s, %s, 'v1', 'ranking-pot', %s, 'p', %s, %s)",
        (decl, strategy_id, seq, Jsonb(doc), sha),
    )
    conn.execute(
        "INSERT INTO ranking_pot_state_events (declaration_id, from_state, to_state, reason, actor) "
        "VALUES (%s, NULL, 'shadow_only', 'freeze', 'supervisor')",
        (decl,),
    )
    return decl


def _declare(conn: Conn, strategy_id: str = V2, terms: dict[str, Any] | None = None) -> int:
    decl = _declaration_sql(conn, strategy_id, V2_TERMS if terms is None else terms)
    conn.commit()
    return decl


def _refused(conn: Conn, sql: LiteralString, params: tuple[Any, ...], match: str) -> None:
    with pytest.raises(psycopg.errors.RaiseException, match=match):
        conn.execute(sql, params)
    conn.rollback()


# ---------------------------------------------------------------------------
# 1. Declaration terms
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    ("strategy_id", "terms", "match"),
    [
        (V2, {"k_controls": K, "book_count": K + 3}, "must carry terms.execution"),
        (V2, {"k_controls": K, "book_count": K + 3, "execution": "full"}, "is the only value"),
        (V2, {"k_controls": K, "book_count": K + 3, "execution": None}, "is the only value"),
        (V2, {"k_controls": K, "execution": "none"}, "is not k_controls"),
        (V2, {"k_controls": K, "book_count": K + 2, "execution": "none"}, "is not k_controls"),
        (V2, {"k_controls": K, "book_count": str(K + 3), "execution": "none"}, "unsigned integers"),
        (V2, {"k_controls": K, "book_count": float(K + 3), "execution": "none"}, "unsigned integers"),
        (V2, {"book_count": K + 3, "execution": "none"}, "unsigned integers"),
        (V2, cast(dict[str, Any], "none"), "must be a JSON object"),
        (V1, {"k_controls": K, "book_count": K + 3}, r"is not k_controls 2 \+ 2"),
        (V1, {"k_controls": K, "execution": "full"}, "is the only value"),
    ],
)
def test_declaration_terms_are_validated(
    ebull_test_conn: Conn, strategy_id: str, terms: dict[str, Any], match: str
) -> None:
    conn = ebull_test_conn
    with pytest.raises(psycopg.errors.RaiseException, match=match):
        _declaration_sql(conn, strategy_id, terms)
    conn.rollback()


def test_well_formed_declarations_are_accepted(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    _declare(conn, V1, {"k_controls": K})  # legacy: neither term
    _declare(conn, V2)
    row = conn.execute("SELECT count(*) FROM ranking_pot_declarations").fetchone()
    assert row is not None and row[0] == 2


# ---------------------------------------------------------------------------
# 2. The completion fence
# ---------------------------------------------------------------------------
def _checkpoint(conn: Conn, decl: int, book: int, state: dict[str, Any], session: date = date(2026, 10, 2)) -> None:
    conn.execute(
        "INSERT INTO ranking_pot_book_checkpoints (declaration_id, book, last_session, state, instrument_ids) "
        "VALUES (%s, %s, %s, %s, '{}') "
        "ON CONFLICT (declaration_id, book) DO UPDATE SET last_session = EXCLUDED.last_session, state = EXCLUDED.state",
        (decl, book, session, Jsonb(state)),
    )
    conn.commit()


FLAT_BOOK: dict[str, Any] = {"positions": [], "pending": []}


def _winding_down_with_a_decision(conn: Conn, strategy_id: str, terms: dict[str, Any]) -> int:
    decl = _declare(conn, strategy_id, terms)
    _insert(conn, decl, **_decided())
    _move(conn, decl, "shadow_only", "winding_down", "operator", "operator")
    return decl


def _complete(conn: Conn, decl: int, match: str) -> None:
    with pytest.raises(psycopg.errors.RaiseException, match=match):
        _move(conn, decl, "winding_down", "completed", "engine")


def test_the_v2_fence_counts_k_plus_3_well_formed_flat_books(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    decl = _winding_down_with_a_decision(conn, V2, V2_TERMS)
    for book in range(K + 2):  # v1's layout: no reference book
        _checkpoint(conn, decl, book, FLAT_BOOK)
    _complete(conn, decl, r"books_incomplete: ranking pot \d+ lacks its 5 book checkpoints \(books 0..4\)")

    _checkpoint(conn, decl, K + 2, {"positions": {}, "pending": []})
    _complete(conn, decl, "books_malformed")
    _checkpoint(conn, decl, K + 2, {"pending": []}, date(2026, 10, 5))  # absent, not an empty array
    _complete(conn, decl, "books_malformed")
    _checkpoint(conn, decl, K + 2, {"positions": [1], "pending": []}, date(2026, 10, 6))
    _complete(conn, decl, "books_not_flat")
    _checkpoint(conn, decl, K + 2, FLAT_BOOK, date(2026, 10, 7))
    _complete(conn, decl, "wind_down_not_stepped")  # every book check passed

    _checkpoint(conn, decl, K + 3, FLAT_BOOK)  # one book too many
    _complete(conn, decl, "books_incomplete")


def test_a_declaration_without_book_count_keeps_k_plus_2(ebull_test_conn: Conn) -> None:
    """v1's legacy branch: K + 2 books (shadow, K controls, variant), exactly."""
    conn = ebull_test_conn
    decl = _winding_down_with_a_decision(conn, V1, {"k_controls": K})
    for book in range(K + 1):
        _checkpoint(conn, decl, book, FLAT_BOOK)
    _complete(conn, decl, "lacks its 4 book checkpoints")
    _checkpoint(conn, decl, K + 1, FLAT_BOOK)
    _complete(conn, decl, "wind_down_not_stepped")
    _checkpoint(conn, decl, K + 2, FLAT_BOOK)
    _complete(conn, decl, "books_incomplete")


def test_completion_without_a_decision_has_no_fence(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    decl = _declare(conn)
    _move(conn, decl, "shadow_only", "winding_down", "operator", "operator")
    _move(conn, decl, "winding_down", "completed", "engine")


# ---------------------------------------------------------------------------
# 3–4. `execution = "none"`
# ---------------------------------------------------------------------------
def test_v2_cannot_enter_executing(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    decl = _declare(conn)
    _refused(
        conn,
        "INSERT INTO ranking_pot_state_events (declaration_id, from_state, to_state, reason, actor) "
        "VALUES (%s, 'shadow_only', 'executing', 't', 'supervisor')",
        (decl,),
        "execution_none",
    )


def test_every_executed_book_relation_carries_the_guard(ebull_test_conn: Conn) -> None:
    """The inventory, pinned both ways: every guarded relation is listed, and every ranking-pot executed-book
    relation in the schema is guarded (a new ``ranking_pot_exec_*`` table without the trigger fails here)."""
    conn = ebull_test_conn
    rows = conn.execute(
        "SELECT c.relname FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid "
        "WHERE NOT t.tgisinternal AND t.tgfoid = 'ranking_pot_executed_book_guard'::regproc"
    ).fetchall()
    guarded = {r[0] for r in rows}
    assert guarded == EXECUTED_BOOK_RELATIONS
    rows = conn.execute(
        "SELECT tablename FROM pg_tables WHERE schemaname = current_schema() "
        "AND (tablename LIKE 'ranking\\_pot\\_exec\\_%%' OR tablename = 'ranking_pot_activations')"
    ).fetchall()
    assert {r[0] for r in rows} <= guarded
    conn.rollback()


def test_executed_book_rows_are_refused_under_v2(ebull_test_conn: Conn) -> None:
    """The relations keyed directly on the declaration. The others (decisions, submissions, level observations)
    resolve it through a parent row these refusals make unwritable; their guard is pinned by the inventory test.
    BEFORE triggers run ahead of the foreign keys, so the placeholder parent ids never reach them."""
    conn = ebull_test_conn
    decl = _declare(conn)
    _insert(conn, decl, **_decided())
    row = conn.execute("SELECT max(attempt_id) FROM ranking_pot_rebalance_attempts").fetchone()
    assert row is not None
    attempt = int(row[0])
    conn.commit()
    cases: list[tuple[LiteralString, tuple[Any, ...]]] = [
        ("INSERT INTO ranking_pot_activations (declaration_id, pot_capital) VALUES (%s, 1000)", (decl,)),
        (
            "INSERT INTO ranking_pot_exec_rebalances "
            "(attempt_id, declaration_id, state, entries_allowed, v1_active, held, closed_lifecycle_ids) "
            "VALUES (%s, %s, 'executing', FALSE, FALSE, '[]', '{}')",
            (attempt, decl),
        ),
        (
            "INSERT INTO ranking_pot_exec_lifecycles "
            "(attempt_id, declaration_id, instrument_id, slot, signal_id, ticket, ticket_sha256) "
            "VALUES (%s, %s, 1, 1, 1, '{}', %s)",
            (attempt, decl, "a" * 64),
        ),
        (
            "INSERT INTO ranking_pot_exec_exit_stamps (lifecycle_id, declaration_id) VALUES (1, %s)",
            (decl,),
        ),
        (
            "INSERT INTO strategy_entry_tickets (order_id, strategy_trade_id, rationale_class, rule_id, "
            "evidence_kind, evidence_id, why_now, exit_rule, expected_cost_usd, cost_basis) "
            "VALUES (1, 1, 'rebalance', 'r', 'ranking_pot_declaration', %s, 'w', 'e', 0, 'c')",
            (decl,),
        ),
    ]
    for sql, params in cases:
        _refused(conn, sql, params, "execution_none")


def test_a_parent_the_guard_cannot_see_is_refused(ebull_test_conn: Conn) -> None:
    """Codex ckpt-2: a declaration and its activation written by ONE data-modifying CTE — a STABLE lookup reads the
    statement's snapshot, which cannot see the sibling insert; the VOLATILE one refuses it. A parent the guard cannot
    resolve at all is refused (``execution_unresolved``) rather than read as "not declared none", ahead of the
    foreign key."""
    conn = ebull_test_conn
    terms = V2_TERMS
    doc = {"strategy_id": V2, "strategy_version": "v1", "family": "ranking-pot", "family_seq": 1, "terms": terms}
    sha = canonical_sha256(doc)
    prereg = replace(prereg_declaration(doc_sha256=sha, declared_by="t"), strategy_id=V2)
    decl = freeze_preregistration(cast(psycopg.Connection[tuple], conn), prereg)
    with pytest.raises(psycopg.errors.RaiseException, match="execution_none"):
        conn.execute(
            "WITH d AS (INSERT INTO ranking_pot_declarations "
            "(declaration_id, strategy_id, strategy_version, family, family_seq, doc_path, doc, doc_sha256) "
            "VALUES (%s, %s, 'v1', 'ranking-pot', 1, 'p', %s, %s) RETURNING declaration_id) "
            "INSERT INTO ranking_pot_activations (declaration_id, pot_capital) SELECT declaration_id, 1000 FROM d",
            (decl, V2, Jsonb(doc), sha),
        )
    conn.rollback()
    _refused(
        conn,
        "INSERT INTO ranking_pot_exec_decisions (attempt_id, instrument_id, action, reason) "
        "VALUES (-1, 1, 'hold', 'r')",
        (),
        "execution_unresolved",
    )


def test_a_v1_activation_row_is_still_written(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    decl = _declare(conn, V1, {"k_controls": K})
    conn.execute("INSERT INTO ranking_pot_activations (declaration_id, pot_capital) VALUES (%s, 1000)", (decl,))
    conn.commit()
    _move(conn, decl, "shadow_only", "executing", "supervisor")


# ---------------------------------------------------------------------------
# 5. v2's rebalance refusals
# ---------------------------------------------------------------------------
def test_v2_refusal_codes_are_in_the_attempt_vocabulary(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    decl = _declare(conn)
    codes = get_args(v2.DtcRefusal) + get_args(v2.InsiderRefusal)
    assert len(codes) == 4
    for code in codes:
        _insert(conn, decl, refusal=code)
    row = conn.execute(
        "SELECT array_agg(refusal ORDER BY attempt_id) FROM ranking_pot_rebalance_attempts WHERE declaration_id = %s",
        (decl,),
    ).fetchone()
    assert row is not None and tuple(row[0]) == codes
