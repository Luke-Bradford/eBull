"""#3227 item 2 — the Table I line-grain discriminators carried on ``:NDH:`` rows.

Pure-logic only. The parsing decisions and the four-way column-list agreement are the
genuinely new mechanisms; the write itself is exercised by the dev-verify backfill and by
the existing DERA ingest integration tests.
"""

from __future__ import annotations

import re

import pytest

from app.services.sec_insider_dataset_ingest import (
    _CREATE_STG_SQL,
    _INSERT_FROM_STG_SQL,
    _STG_COPY_COLUMNS,
    _parse_direct_indirect,
    _parse_text,
)

_NEW_COLUMNS = ("security_title", "direct_indirect", "nature_of_ownership")


class TestParseText:
    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("Common Stock", "Common Stock"),
            ("  Class A Common Stock  ", "Class A Common Stock"),
            ("", None),
            ("   ", None),
            (None, None),
            # Case is NOT normalised: SECURITY_TITLE carries 9,422 distinct values across
            # the cached corpus including `Common Stock`, `COMMON STOCK` and `Common stock`,
            # and folding them here would invent a canonicalisation the source does not make.
            ("COMMON STOCK", "COMMON STOCK"),
        ],
    )
    def test_strips_and_nulls_empty(self, raw: str | None, expected: str | None) -> None:
        assert _parse_text(raw) == expected


class TestParseDirectIndirect:
    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("D", "D"),
            ("I", "I"),
            ("  D  ", "D"),
            ("", None),
            (None, None),
            # Anything outside the SEC's two-value enum becomes NULL rather than reaching
            # the sql/400 CHECK. This is the load-bearing case: `COPY ... ON_ERROR ignore`
            # protects the copy into staging but NOT the `INSERT ... SELECT` that drains it,
            # so an unexpected value would abort the whole archive's drain, not skip a row.
            ("X", None),
            ("d", None),
            ("i", None),
            ("DI", None),
            ("Direct", None),
        ],
    )
    def test_sanitises_to_the_sec_enum_or_null(self, raw: str | None, expected: str | None) -> None:
        assert _parse_direct_indirect(raw) == expected

    def test_lowercase_is_rejected_not_upcased(self) -> None:
        """``d`` is not silently promoted to ``D``.

        The XML sanitiser this mirrors (``insider_transactions.py:1241``) is also exact-match,
        and the two paths write the same column. Upcasing here would make the DERA path accept
        a form the XML path rejects, so the same source value would store differently
        depending on which writer reached it first.
        """
        assert _parse_direct_indirect("d") is None


class TestStagingColumnAgreement:
    """The new columns must appear in all four places or the COPY silently misaligns.

    ``copy.write_row`` is POSITIONAL against ``_STG_COPY_COLUMNS``, and the staging DDL and
    the drain's INSERT/SELECT lists are three further copies of the same ordering. A column
    added to one and missed in another shifts every later value by one position — which
    would write ``shares`` into ``ingest_run_id`` rather than raising anything obvious.
    """

    def test_copy_columns_carry_the_three_discriminators(self) -> None:
        for column in _NEW_COLUMNS:
            assert column in _STG_COPY_COLUMNS

    def test_staging_ddl_declares_every_copy_column(self) -> None:
        for column in _STG_COPY_COLUMNS:
            assert re.search(rf"^\s+{column}\s", _CREATE_STG_SQL, re.MULTILINE), column

    def test_drain_insert_and_select_both_carry_them(self) -> None:
        insert_list = _INSERT_FROM_STG_SQL.split("SELECT DISTINCT ON", 1)[0]
        select_list = _INSERT_FROM_STG_SQL.split(")\n", 1)[1].split("FROM _stg_insider", 1)[0]
        for column in _NEW_COLUMNS:
            assert column in insert_list, f"{column} missing from the INSERT column list"
            assert column in select_list, f"{column} missing from the SELECT list"

    def test_conflict_update_refreshes_them(self) -> None:
        do_update = _INSERT_FROM_STG_SQL.split("DO UPDATE SET", 1)[1]
        for column in _NEW_COLUMNS:
            assert f"{column} = EXCLUDED.{column}" in do_update, column


class TestDistinctOnKeyIsFrozen:
    """The new columns are PAYLOAD. Adding one to the DISTINCT ON key or its ORDER BY would
    change which staging row wins, and therefore which line reaches ``_current`` — the exact
    behaviour #3227 is trying to stop guessing at.
    """

    def _distinct_on_key(self) -> str:
        return _INSERT_FROM_STG_SQL.split("SELECT DISTINCT ON (", 1)[1].split(")\n", 1)[0]

    def test_new_columns_are_not_in_the_distinct_on_key(self) -> None:
        key = self._distinct_on_key()
        for column in _NEW_COLUMNS:
            assert column not in key, f"{column} must not join the DISTINCT ON key (item 3's job)"

    def test_new_columns_are_not_in_the_order_by(self) -> None:
        order_by = _INSERT_FROM_STG_SQL.split("ORDER BY", 1)[1].split("ON CONFLICT", 1)[0]
        for column in _NEW_COLUMNS:
            assert column not in order_by, f"{column} must not affect tie-breaking"

    def test_conflict_target_is_unchanged(self) -> None:
        conflict = _INSERT_FROM_STG_SQL.split("ON CONFLICT (", 1)[1].split(")", 1)[0]
        assert "source_document_id" in conflict
        for column in _NEW_COLUMNS:
            assert column not in conflict
