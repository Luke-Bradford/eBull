"""Pure-logic table test for the symbol-or-id path-param branch (#2184).

`/instrument/1001/fundamentals` 404'd because every drill endpoint
resolved `WHERE UPPER(symbol) = %(s)s`. `instrument_ref_query` is the one
place that decision now lives; these tests pin the branch and the bound
params without touching Postgres.
"""

from __future__ import annotations

import sys

import pytest

from app.api.instruments import (
    _RESOLVE_BY_ID_SQL,
    _RESOLVE_BY_SYMBOL_SQL,
    instrument_ref_query,
)


@pytest.mark.parametrize(
    ("ref", "expected_sql", "expected_params"),
    [
        # --- id branch: entirely ASCII digits ---
        ("1001", _RESOLVE_BY_ID_SQL, {"iid": 1001}),
        ("1", _RESOLVE_BY_ID_SQL, {"iid": 1}),
        # Leading zeros are still an id; int() normalises them.
        ("0001001", _RESOLVE_BY_ID_SQL, {"iid": 1001}),
        # Whitespace is stripped before the branch is chosen.
        ("  1001  ", _RESOLVE_BY_ID_SQL, {"iid": 1001}),
        # 19 digits = BIGINT's own width, so the bound never rejects a
        # value that could be a real id. This one is past BIGINT's RANGE
        # and still takes the id branch — Postgres compares it as numeric
        # and returns no rows, i.e. a clean 404 (verified against the dev
        # DB rather than assumed).
        ("9" * 19, _RESOLVE_BY_ID_SQL, {"iid": int("9" * 19)}),
        # --- symbol branch: anything else ---
        # 20+ digits cannot be an instrument_id, so it falls here and
        # misses. Load-bearing: `int()` raises ValueError past
        # `sys.get_int_max_str_digits()` (4300), which an unbounded
        # numeric branch would turn into a 500 instead of a 404.
        ("9" * 20, _RESOLVE_BY_SYMBOL_SQL, {"s": "9" * 20}),
        ("1" * 5000, _RESOLVE_BY_SYMBOL_SQL, {"s": "1" * 5000}),
        ("AAPL", _RESOLVE_BY_SYMBOL_SQL, {"s": "AAPL"}),
        # Case is normalised for the UPPER(symbol) comparison.
        ("aapl", _RESOLVE_BY_SYMBOL_SQL, {"s": "AAPL"}),
        ("  aapl  ", _RESOLVE_BY_SYMBOL_SQL, {"s": "AAPL"}),
        # Numeric-LEADING tickers are symbols, not ids — every one in the
        # universe carries a non-digit exchange suffix (0 purely-numeric
        # symbols across the full instruments table, checked 2026-07-31).
        ("1810.HK", _RESOLVE_BY_SYMBOL_SQL, {"s": "1810.HK"}),
        ("2501.T", _RESOLVE_BY_SYMBOL_SQL, {"s": "2501.T"}),
        ("3DA.ASX", _RESOLVE_BY_SYMBOL_SQL, {"s": "3DA.ASX"}),
        ("BRK.B", _RESOLVE_BY_SYMBOL_SQL, {"s": "BRK.B"}),
        # `str.isdigit()` is True for these but `int()` either raises
        # ('²') or silently accepts a non-ASCII id ('١٠٠١'). The ASCII
        # regex keeps both on the symbol branch, where they harmlessly
        # miss.
        ("²", _RESOLVE_BY_SYMBOL_SQL, {"s": "²"}),
        ("١٠٠١", _RESOLVE_BY_SYMBOL_SQL, {"s": "١٠٠١"}),
        # Mixed digits + letters stay on the symbol branch.
        ("1001A", _RESOLVE_BY_SYMBOL_SQL, {"s": "1001A"}),
        ("-1001", _RESOLVE_BY_SYMBOL_SQL, {"s": "-1001"}),
    ],
)
def test_instrument_ref_query_branch(ref: str, expected_sql: str, expected_params: dict[str, object]) -> None:
    sql, params = instrument_ref_query(ref)
    assert sql == expected_sql
    assert params == expected_params


def test_symbol_branch_keeps_the_deterministic_tie_break() -> None:
    """`symbol` is not UNIQUE across exchanges (migration 043) — the
    collision winner must stay `is_primary_listing DESC, instrument_id
    ASC`, exactly as the 27 unconverted call sites still spell it."""
    sql, _ = instrument_ref_query("AAPL")
    assert "ORDER BY is_primary_listing DESC, instrument_id ASC" in sql
    assert "LIMIT 1" in sql


def test_a_pathological_digit_string_never_reaches_int() -> None:
    """Regression: `int()` raises `ValueError` past
    `sys.get_int_max_str_digits()`, so an unbounded numeric branch turned
    `/instruments/<5000 digits>/financials` into a 500. The 19-digit
    bound must keep that input on the symbol branch — no raise."""
    assert int("1" * 4000)  # inside the limit
    with pytest.raises(ValueError):
        int("1" * (sys.get_int_max_str_digits() + 1))

    sql, params = instrument_ref_query("1" * (sys.get_int_max_str_digits() + 1))
    assert sql == _RESOLVE_BY_SYMBOL_SQL
    assert "iid" not in params


def test_id_branch_does_not_filter_on_symbol() -> None:
    """An id lookup is unique by primary key — no UPPER(symbol) predicate,
    and no ORDER BY needed to make it deterministic."""
    sql, _ = instrument_ref_query("1001")
    assert "UPPER(symbol)" not in sql
    assert "instrument_id = %(iid)s" in sql
