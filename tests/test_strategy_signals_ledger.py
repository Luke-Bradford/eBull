"""Phase 3b — the ledger's constraints, exercised against a real database.

⚠ ONE integration test, per the repo's test-tiering rule: the genuinely-new
SQL mechanism is this table's constraint set, and a mocked cursor asserts the
parameters while passing against a constraint that would reject them.
"""

from __future__ import annotations

import psycopg
import pytest
from psycopg.types.json import Jsonb

_BASE = {
    "strategy_id": "S-TEST",
    "strategy_version": "strategy-registry-v1+abc123",
    "signal_bar_date": "2024-01-02",
    "signal_kind": "entry",
    "verdict": "not_fired",
    "universe": "survivor_only",
    # #2333 (sql/257). NOT NULL with no default, so it is stated here rather
    # than defaulted — an insert omitting it must fail, which is its own case
    # in the rejection matrix below.
    "input_rule_set_versions": Jsonb({"indicator_series": "indicator-series-v1+abc123"}),
}


#: ⚠ A FIXED statement, not an f-string built from the override keys. psycopg
#: types `query` as `LiteralString` precisely to stop dynamic SQL, and the
#: pre-push hook caught the f-string version — correctly. A fixed column list
#: is also more honest about what is under test.
_INSERT = """
    INSERT INTO strategy_signals (
        strategy_id, strategy_version, instrument_id, signal_bar_date,
        signal_kind, verdict, not_evaluable_reason, fill_bar_date,
        fill_price, universe, input_rule_set_versions
    ) VALUES (
        %(strategy_id)s, %(strategy_version)s, %(instrument_id)s, %(signal_bar_date)s,
        %(signal_kind)s, %(verdict)s, %(not_evaluable_reason)s, %(fill_bar_date)s,
        %(fill_price)s, %(universe)s, %(input_rule_set_versions)s
    )
"""

#: ⚠ The same statement MINUS `input_rule_set_versions`, for the one case that
#: cannot be expressed as an override: a writer that never mentions the column.
#: NOT NULL with no default is what makes that fail, and a default would make
#: it pass silently with an invented indicator version (#2288's argument for
#: `universe`, inherited).
_INSERT_WITHOUT_RULE_SETS = """
    INSERT INTO strategy_signals (
        strategy_id, strategy_version, instrument_id, signal_bar_date,
        signal_kind, verdict, universe
    ) VALUES (
        %(strategy_id)s, %(strategy_version)s, %(instrument_id)s, %(signal_bar_date)s,
        %(signal_kind)s, %(verdict)s, %(universe)s
    )
"""


def _insert(conn: psycopg.Connection[tuple], instrument_id: int, **overrides: object) -> None:
    row: dict[str, object] = {
        **_BASE,
        "instrument_id": instrument_id,
        "not_evaluable_reason": None,
        "fill_bar_date": None,
        "fill_price": None,
    }
    row.update(overrides)
    conn.execute(_INSERT, row)


@pytest.fixture
def instrument_id(ebull_test_conn: psycopg.Connection[tuple]) -> int:
    # ⚠ `instruments.instrument_id` is eToro's identifier, assigned upstream —
    # NOT a serial. An insert that omits it fails NOT NULL, which is how this
    # fixture failed first time round.
    row = ebull_test_conn.execute("SELECT instrument_id FROM instruments LIMIT 1").fetchone()
    if row is not None:
        return int(row[0])
    ebull_test_conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) "
        "VALUES (999001, 'TESTX', 'Test Instrument', true)"
    )
    return 999001


@pytest.mark.parametrize(
    ("label", "overrides"),
    [
        # ⚠ The backstop. NOT the mechanism — a writer can record
        # signal_bar_date = t-1, fill on t, and pass every constraint here.
        # Same-bar fills are made impossible by the registry API carrying no
        # fill field at all (phase 3a).
        ("same-bar fill", {"verdict": "fired", "fill_bar_date": "2024-01-02", "fill_price": 10}),
        ("fill before signal", {"verdict": "fired", "fill_bar_date": "2024-01-01", "fill_price": 10}),
        ("fired with no fill", {"verdict": "fired"}),
        ("not_evaluable with no reason", {"verdict": "not_evaluable"}),
        (
            "reason on a fired row",
            {
                "verdict": "fired",
                "fill_bar_date": "2024-01-03",
                "fill_price": 10,
                "not_evaluable_reason": "series_break",
            },
        ),
        # Free text is what criterion 9 cannot count.
        ("free-text reason", {"verdict": "not_evaluable", "not_evaluable_reason": "because reasons"}),
        ("unknown verdict", {"verdict": "maybe"}),
        ("unknown universe", {"universe": "everything"}),
        ("unknown signal_kind", {"signal_kind": "hedge"}),
        # #2333 / sql/257. Every one of these is a state NOT NULL alone admits:
        # the column is PRESENT and records nothing usable.
        ("empty rule-set object", {"input_rule_set_versions": Jsonb({})}),
        ("blank rule-set version", {"input_rule_set_versions": Jsonb({"indicator_series": ""})}),
        ("whitespace rule-set version", {"input_rule_set_versions": Jsonb({"indicator_series": "   "})}),
        ("non-string rule-set version", {"input_rule_set_versions": Jsonb({"indicator_series": 5})}),
        ("null rule-set version", {"input_rule_set_versions": Jsonb({"indicator_series": None})}),
        ("rule sets as an array", {"input_rule_set_versions": Jsonb(["indicator_series"])}),
        ("rule sets as a scalar", {"input_rule_set_versions": Jsonb("indicator-series-v1+abc123")}),
        ("rule sets explicitly null", {"input_rule_set_versions": None}),
    ],
)
def test_ledger_rejects(
    ebull_test_conn: psycopg.Connection[tuple], instrument_id: int, label: str, overrides: dict
) -> None:
    with pytest.raises(psycopg.errors.Error), ebull_test_conn.transaction():
        _insert(ebull_test_conn, instrument_id, **overrides)


def test_a_writer_that_omits_the_rule_sets_is_refused(
    ebull_test_conn: psycopg.Connection[tuple], instrument_id: int
) -> None:
    """#2333. The column carries no default ON PURPOSE, so the failure mode is
    a loud NOT NULL rather than a row claiming an indicator rule set nobody
    stated. Not expressible as an override — it needs a statement that never
    names the column."""
    with pytest.raises(psycopg.errors.NotNullViolation), ebull_test_conn.transaction():
        ebull_test_conn.execute(
            _INSERT_WITHOUT_RULE_SETS,
            {
                "strategy_id": _BASE["strategy_id"],
                "strategy_version": _BASE["strategy_version"],
                "instrument_id": instrument_id,
                "signal_bar_date": _BASE["signal_bar_date"],
                "signal_kind": _BASE["signal_kind"],
                "verdict": _BASE["verdict"],
                "universe": _BASE["universe"],
            },
        )


def test_ledger_accepts_the_valid_shapes(ebull_test_conn: psycopg.Connection[tuple], instrument_id: int) -> None:
    with ebull_test_conn.transaction():
        _insert(ebull_test_conn, instrument_id)
        _insert(
            ebull_test_conn,
            instrument_id,
            signal_bar_date="2024-02-05",
            verdict="fired",
            fill_bar_date="2024-02-06",
            fill_price=101.25,
        )
        _insert(
            ebull_test_conn,
            instrument_id,
            signal_kind="exit",
            verdict="not_evaluable",
            not_evaluable_reason="no_fill_bar",
        )


def test_same_bar_and_kind_collides_but_a_new_version_does_not(
    ebull_test_conn: psycopg.Connection[tuple], instrument_id: int
) -> None:
    """The uniqueness key: a changed strategy must NOT overwrite an old signal,
    which is the whole reason strategy_version is IN the key."""
    with ebull_test_conn.transaction():
        _insert(ebull_test_conn, instrument_id)

    with pytest.raises(psycopg.errors.UniqueViolation), ebull_test_conn.transaction():
        _insert(ebull_test_conn, instrument_id)

    # Same bar, same instrument, DIFFERENT version — a distinct decision.
    with ebull_test_conn.transaction():
        _insert(ebull_test_conn, instrument_id, strategy_version="strategy-registry-v1+def456")

    # ...and the same bar as an EXIT is also distinct (parent §3.5 covers
    # "entries and exits alike").
    with ebull_test_conn.transaction():
        _insert(ebull_test_conn, instrument_id, signal_kind="exit")
