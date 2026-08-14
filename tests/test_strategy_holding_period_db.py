"""#2623 gap 1 — what ``sql/347`` did to the schema, and the percentile parity.

⚠ THE MIGRATION'S EFFECTS AND THE CROSS-ENGINE CHECK, NOT THE PYTHON RULE. The
derivation is covered by pure tests in ``test_strategy_holding_period``; these
assert what a Python test structurally cannot see — that the view EXPOSES the
three columns, that each CHECK refuses what it was written to refuse, and that
numpy's linear percentile and Postgres' ``percentile_cont`` agree.

⚠ In its own ``_db`` module deliberately, following ``test_2363_carry_fx_split_db``:
``ebull_test_conn`` in a test source db-marks the WHOLE module at collection, so
mixing these with pure tests would drag the fast tier onto Postgres.
"""

from typing import Any, LiteralString

import numpy as np
import psycopg
import psycopg.sql
import pytest


@pytest.mark.parametrize("column", ["median_hold_days", "hold_days_p25", "hold_days_p75"])
def test_the_columns_are_nullable_with_no_default(ebull_test_conn: psycopg.Connection[Any], column: str) -> None:
    """Nullable and undefaulted is what makes the migration rolling-safe.

    ``strategy_backtest_run`` is a live job, so between the migration applying
    and the daemon picking up new code an old writer INSERTs without these
    columns. That is legal against a nullable column — and a DEFAULT would be
    actively wrong here, because any value it invented would be a holding period
    nobody measured.
    """
    row = ebull_test_conn.execute(
        """
        SELECT is_nullable, column_default
        FROM information_schema.columns
        WHERE table_name = 'strategy_results_store' AND column_name = %(column)s
        """,
        {"column": column},
    ).fetchone()
    assert row is not None, f"strategy_results_store.{column} does not exist — sql/347 did not apply"
    is_nullable, column_default = row
    assert is_nullable == "YES"
    assert column_default is None


@pytest.mark.parametrize("column", ["median_hold_days", "hold_days_p25", "hold_days_p75"])
def test_the_in_sample_view_exposes_the_new_columns(ebull_test_conn: psycopg.Connection[Any], column: str) -> None:
    """``strategy_results`` is a VIEW and ``SELECT *`` is expanded at creation.

    A column added to the store is invisible through the view until the view is
    recreated, so a reader going through the view would see a column that is not
    there. The catalog (#2623 gap 3) reads this surface.
    """
    row = ebull_test_conn.execute(
        """
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'strategy_results' AND column_name = %(column)s
        """,
        {"column": column},
    ).fetchone()
    assert row is not None, f"the view does not expose {column} — sql/347 did not recreate it"


def test_the_view_still_carries_its_cascaded_check_option(ebull_test_conn: psycopg.Connection[Any]) -> None:
    """``CREATE OR REPLACE VIEW`` DROPS the check option, so it must be restored.

    Losing it is silent: the view keeps working and simply stops refusing writes
    that fall outside its own predicate.
    """
    row = ebull_test_conn.execute(
        "SELECT check_option FROM information_schema.views WHERE table_name = 'strategy_results'"
    ).fetchone()
    assert row is not None
    assert row[0] == "CASCADED"


class TestTheChecks:
    """Each CHECK refuses what it was written to refuse.

    ⚠ Asserted one per constraint rather than once in aggregate: a single
    "bad rows are rejected" test passes when only one of three constraints
    exists, which is the state a partially-applied migration leaves behind.
    """

    @staticmethod
    def _update(conn: psycopg.Connection[Any], assignments: LiteralString) -> None:
        result_id = conn.execute("SELECT min(result_id) FROM strategy_results_store").fetchone()
        if result_id is None or result_id[0] is None:
            pytest.skip("no stored result to probe the constraints against")
        # ⚠ `assignments` is typed `LiteralString`, so only a source literal can
        # reach it — no runtime-built fragment can. The row id stays a bound
        # parameter.
        conn.execute(
            psycopg.sql.SQL("UPDATE strategy_results_store SET {} WHERE result_id = %(id)s").format(
                psycopg.sql.SQL(assignments)
            ),
            {"id": result_id[0]},
        )

    def test_a_partial_triple_is_refused(self, ebull_test_conn: psycopg.Connection[Any]) -> None:
        # A half-run derivation would otherwise render as one plausible number.
        with pytest.raises(psycopg.errors.CheckViolation):
            self._update(ebull_test_conn, "median_hold_days=1, hold_days_p25=NULL, hold_days_p75=2")

    def test_an_unordered_triple_is_refused(self, ebull_test_conn: psycopg.Connection[Any]) -> None:
        with pytest.raises(psycopg.errors.CheckViolation):
            self._update(ebull_test_conn, "median_hold_days=1, hold_days_p25=5, hold_days_p75=2")

    def test_a_negative_hold_is_refused(self, ebull_test_conn: psycopg.Connection[Any]) -> None:
        with pytest.raises(psycopg.errors.CheckViolation):
            self._update(ebull_test_conn, "median_hold_days=-1, hold_days_p25=-2, hold_days_p75=0")

    def test_a_v2_row_with_realised_trades_must_carry_the_triple(
        self, ebull_test_conn: psycopg.Connection[Any]
    ) -> None:
        """The provenance rule the ``METRIC_SET_ID`` bump exists to make enforceable.

        Without it a null on a future row is indistinguishable from a legitimate
        legacy null, so a writer defect would be permanently invisible.
        """
        with pytest.raises(psycopg.errors.CheckViolation):
            self._update(ebull_test_conn, "metric_set_id='criterion7-v2'")

    def test_a_legacy_v1_row_may_keep_its_nulls(self, ebull_test_conn: psycopg.Connection[Any]) -> None:
        # The 324 stored rows are all `criterion7-v1` and cannot be backfilled
        # without a register-charging re-run, so this direction MUST stay legal.
        self._update(ebull_test_conn, "metric_set_id='criterion7-v1'")

    def test_a_zero_day_hold_is_accepted(self, ebull_test_conn: psycopg.Connection[Any]) -> None:
        # Every trade closing same-day is a real measurement, not an absence.
        self._update(ebull_test_conn, "median_hold_days=0, hold_days_p25=0, hold_days_p75=0")


@pytest.mark.parametrize(
    "holds",
    [
        pytest.param([1, 2, 3, 4, 5], id="odd"),
        pytest.param([1, 2, 3, 4], id="even-interpolates"),
        pytest.param([5, 5, 5, 5], id="duplicates"),
        pytest.param([0, 0, 1], id="zero-holds"),
        pytest.param([7], id="single"),
        pytest.param([2, 9, 9, 1, 40, 3, 3], id="skewed"),
    ],
)
def test_numpy_linear_matches_postgres_percentile_cont(
    ebull_test_conn: psycopg.Connection[Any], holds: list[int]
) -> None:
    """§3.2's whole reason for naming the method.

    The live path reports the same quantity in the same unit via
    ``percentile_cont`` (``strategy_monitoring``'s ``median_days_to_outcome``).
    If the two engines disagreed, two adjacent figures on one catalog row would
    differ on identical data — so the equality is pinned rather than assumed.
    """
    row = ebull_test_conn.execute(
        """
        SELECT percentile_cont(0.25) WITHIN GROUP (ORDER BY d),
               percentile_cont(0.5)  WITHIN GROUP (ORDER BY d),
               percentile_cont(0.75) WITHIN GROUP (ORDER BY d)
        FROM unnest(%(holds)s::numeric[]) AS t(d)
        """,
        {"holds": holds},
    ).fetchone()
    assert row is not None
    expected = np.percentile(np.asarray(holds, dtype=np.float64), [25, 50, 75], method="linear")
    assert [float(value) for value in row] == pytest.approx(list(expected))
