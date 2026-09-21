"""Research-corpus schema invariants (#2282 stage 2a, sql/249).

These are SQL-level invariants, not policy, so they get ONE DB-backed test for
the genuinely-new mechanism rather than a file per code path (per the repo's
lean-tests rule). What they pin down is the pair of decisions that a later
change is most likely to "simplify" back out:

1. **The series is keyed on (vendor, vendor_symbol), and `instrument_id` is
   nullable.** An unresolved series is a MEASUREMENT — it is a company that was
   listed and is not on eToro's book, i.e. the direct measure of eToro-listing
   bias (#2289 §4.0). Making `instrument_id` NOT NULL would look like tidying
   and would delete the evidence the census exists to produce.

2. **The census columns are denormalised and therefore drift.** They live on
   the series row so the census is an aggregate over ~7,693 rows instead of a
   scan of ~25.8M bars. `research_series_census_drift` is how that trade is
   kept honest, so it has to actually catch an unmaintained write.
"""

from __future__ import annotations

import psycopg
import pytest

_VENDOR = "paperswithbacktest/Stocks-Daily-Price"


def _new_series(
    conn: psycopg.Connection[tuple],
    symbol: str,
    *,
    instrument_id: int | None = None,
    resolution_method: str | None = None,
) -> int:
    row = conn.execute(
        """
        INSERT INTO research_price_series
            (vendor, vendor_symbol, upstream_source, licence,
             adjustment_basis, instrument_id, resolution_method)
        VALUES (%s, %s, 'yahoo_derivative', 'other/unspecified',
                'unknown', %s, %s)
        RETURNING series_id
        """,
        (_VENDOR, symbol, instrument_id, resolution_method),
    ).fetchone()
    assert row is not None
    conn.commit()
    return row[0]


@pytest.mark.db
class TestSeriesIdentity:
    def test_series_resolves_to_no_instrument(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """An unresolved series is legal and is reported, not dropped.

        This is the whole reason the corpus is not keyed on instrument_id.
        """
        _new_series(ebull_test_conn, "UMPQ")

        row = ebull_test_conn.execute(
            "SELECT asset_class, series, resolved_series FROM research_corpus_census"
        ).fetchone()
        assert row == ("(unresolved)", 1, 0)

    def test_resolution_without_method_is_rejected(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        seeded_instrument_id: int,
    ) -> None:
        """A join with no recorded evidence is unauditable.

        Same failure class the CUSIP/CIK identity work already guards against:
        an inferred mapping that cannot be re-checked later.
        """
        with pytest.raises(psycopg.errors.CheckViolation):
            _new_series(ebull_test_conn, "AAPL", instrument_id=seeded_instrument_id)

    def test_one_vendor_cannot_map_two_symbols_to_one_instrument(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        seeded_instrument_id: int,
    ) -> None:
        """The ticker-reuse guard, at the schema level.

        Two symbols from ONE vendor resolving to one instrument means the
        resolver has welded a later occupant of a ticker onto the company that
        vacated it — the `SI` / Silvergate case, where a series beginning
        2025-07-31 attaches to a company that failed in 2023.
        """
        _new_series(
            ebull_test_conn,
            "SI",
            instrument_id=seeded_instrument_id,
            resolution_method="symbol_exact",
        )
        with pytest.raises(psycopg.errors.UniqueViolation):
            _new_series(
                ebull_test_conn,
                "SI-DELISTED",
                instrument_id=seeded_instrument_id,
                resolution_method="symbol_exact",
            )

    def test_delisting_date_requires_its_source(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """A delisting date whose provenance is unknown cannot be trusted to
        truncate a series — a Form 25 carries three distinct dates and only the
        suspension date is the last tradable day (sec-edgar.md §2.6 trap 5)."""
        series_id = _new_series(ebull_test_conn, "FRCB")
        with pytest.raises(psycopg.errors.CheckViolation):
            ebull_test_conn.execute(
                "UPDATE research_price_series SET delisting_date = %s WHERE series_id = %s",
                ("2023-05-01", series_id),
            )

    def test_upstream_source_vocabulary_is_closed(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """Two vendors that both resolve to Yahoo are ONE observation.

        An open text column lets a Yahoo derivative be recorded as something
        independent, which makes a circular cross-source check look like
        corroboration — the #2284 finding in one column.
        """
        with pytest.raises(psycopg.errors.CheckViolation):
            ebull_test_conn.execute(
                """
                INSERT INTO research_price_series
                    (vendor, vendor_symbol, upstream_source, licence,
                     adjustment_basis)
                VALUES (%s, 'X', 'definitely_independent', 'mit', 'unknown')
                """,
                (_VENDOR,),
            )


@pytest.mark.db
class TestCensusDrift:
    def test_drift_view_catches_unmaintained_census_columns(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """Bars written without maintaining the denormalised census must show.

        If this ever returns empty for an unmaintained write, the census is
        silently wrong and every survivorship answer read off it is too.
        """
        series_id = _new_series(ebull_test_conn, "GME")
        ebull_test_conn.execute(
            "INSERT INTO research_price_daily (series_id, bar_date, close) "
            "VALUES (%s, '2023-01-03', 10.5), (%s, '2023-02-28', 9.25)",
            (series_id, series_id),
        )
        ebull_test_conn.commit()

        row = ebull_test_conn.execute(
            "SELECT stored_bar_count, actual_bar_count, actual_first_bar, "
            "       actual_last_bar FROM research_series_census_drift"
        ).fetchone()
        assert row is not None
        stored_count, actual_count, first_bar, last_bar = row
        assert stored_count is None
        assert actual_count == 2
        assert str(first_bar) == "2023-01-03"
        assert str(last_bar) == "2023-02-28"

        ebull_test_conn.execute(
            "UPDATE research_price_series SET first_bar = %s, last_bar = %s, bar_count = 2 WHERE series_id = %s",
            ("2023-01-03", "2023-02-28", series_id),
        )
        ebull_test_conn.commit()

        remaining = ebull_test_conn.execute("SELECT count(*) FROM research_series_census_drift").fetchone()
        assert remaining == (0,)

    def test_zero_bar_count_is_unrepresentable(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """ "No bars" must have exactly ONE spelling.

        Regression for a real gap: the drift view used to compare
        ``COALESCE(bar_count, 0)``, so a series storing ``bar_count = 0`` with
        no bar rows reconciled cleanly — while ``series_without_bars`` (keyed on
        ``IS NULL``) also missed it. The row was absent from BOTH audits at
        once, which is the worst place for a row to be. The constraint now makes
        the second spelling impossible rather than teaching each view about it.
        """
        series_id = _new_series(ebull_test_conn, "ZERO")
        with pytest.raises(psycopg.errors.CheckViolation):
            ebull_test_conn.execute(
                "UPDATE research_price_series "
                "SET first_bar = '2023-01-03', last_bar = '2023-01-03', bar_count = 0 "
                "WHERE series_id = %s",
                (series_id,),
            )

    def test_census_columns_are_all_or_nothing(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """A half-written census is worse than none — it reads as authoritative."""
        series_id = _new_series(ebull_test_conn, "PARTIAL")
        with pytest.raises(psycopg.errors.CheckViolation):
            ebull_test_conn.execute(
                "UPDATE research_price_series SET first_bar = '2023-01-03' WHERE series_id = %s",
                (series_id,),
            )

    def test_bars_ordered_constraint(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        series_id = _new_series(ebull_test_conn, "MSFT")
        with pytest.raises(psycopg.errors.CheckViolation):
            ebull_test_conn.execute(
                "UPDATE research_price_series "
                "SET first_bar = '2023-06-01', last_bar = '2020-01-01' "
                "WHERE series_id = %s",
                (series_id,),
            )


@pytest.mark.db
class TestCorporateActionStamps:
    """sql/405 — the two states a split scale must be able to tell apart.

    ONE test per genuinely-new mechanism: the positivity CHECK (because the
    derivation divides by these) and the closed marker vocabulary (because the
    marker is what makes NULL mean 'unknown' rather than 'no event').
    """

    def test_a_non_positive_split_factor_is_rejected(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """0 is a division by zero and a negative flips every prior price.

        The parser already reads both as absent, so this CHECK cannot fire from
        today's loader. It is the guard for the NEXT writer — the one that
        bulk-inserts from somewhere else and never reads this file.
        """
        series_id = _new_series(ebull_test_conn, "STAMPCHK")
        for bad in ("0", "-2"):
            with pytest.raises(psycopg.errors.CheckViolation):
                ebull_test_conn.execute(
                    "INSERT INTO research_price_daily (series_id, bar_date, close, split_factor)"
                    " VALUES (%s, '2023-01-05', 1.25, %s)",
                    (series_id, bad),
                )
            ebull_test_conn.rollback()

    def test_an_absent_split_factor_is_legal_and_is_not_a_unit_factor(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        """NULL has to be storable: the Parquet archive ships no stamp column.

        And it must not be readable as 1 — which is exactly what
        ``COALESCE(split_factor, 1)`` does, so the series-level marker is the
        only thing that separates them. The assertion is on the marker's
        DEFAULT, because a series nobody has re-loaded is 'absent' and saying
        so is the migration's safe direction.
        """
        series_id = _new_series(ebull_test_conn, "STAMPNULL")
        ebull_test_conn.execute(
            "INSERT INTO research_price_daily (series_id, bar_date, close) VALUES (%s, '2023-01-05', 1.25)",
            (series_id,),
        )
        ebull_test_conn.commit()
        row = ebull_test_conn.execute(
            """
            SELECT d.split_factor, d.dividend, s.corporate_action_stamps
            FROM research_price_daily d
            JOIN research_price_series s ON s.series_id = d.series_id
            WHERE d.series_id = %s
            """,
            (series_id,),
        ).fetchone()
        assert row == (None, None, "absent")

    def test_the_marker_vocabulary_is_closed(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """Two states, spelled one way each.

        A third spelling ('yes', 'partial', 'unknown') is how a reader ends up
        with a marker it does not recognise and defaults to trusting.
        """
        series_id = _new_series(ebull_test_conn, "STAMPVOCAB")
        with pytest.raises(psycopg.errors.CheckViolation):
            ebull_test_conn.execute(
                "UPDATE research_price_series SET corporate_action_stamps = 'partial' WHERE series_id = %s",
                (series_id,),
            )
        ebull_test_conn.rollback()
        ebull_test_conn.execute(
            "UPDATE research_price_series SET corporate_action_stamps = 'vendor_supplied' WHERE series_id = %s",
            (series_id,),
        )
        ebull_test_conn.commit()

    @pytest.mark.parametrize("bad", ["NaN", "Infinity"])
    def test_a_non_finite_split_factor_is_rejected(self, ebull_test_conn: psycopg.Connection[tuple], bad: str) -> None:
        """`> 0` alone admits both, because Postgres orders NaN above everything.

        Measured on this cluster: ``select 'NaN'::numeric > 0`` returns **true**,
        and so does ``'Infinity'::numeric > 0``. A bare positivity CHECK would
        let either through and a cumulative split scale would then carry it into
        every corrected price as NaN. `-Infinity` needs no clause — `> 0` has it.
        """
        series_id = _new_series(ebull_test_conn, f"STAMP{bad[:3].upper()}")
        with pytest.raises(psycopg.errors.CheckViolation):
            ebull_test_conn.execute(
                "INSERT INTO research_price_daily (series_id, bar_date, close, split_factor)"
                " VALUES (%s, '2023-01-05', 1.25, %s::numeric)",
                (series_id, bad),
            )
        ebull_test_conn.rollback()
