"""Pure-logic table test for the symbol-or-id path-param resolution (#2184).

`/instrument/1001/fundamentals` 404'd because every drill endpoint
resolved `WHERE UPPER(symbol) = %(s)s`. `instrument_ref_queries` is the
one place that decision now lives; these tests pin the attempt ORDER and
the bound params without touching Postgres.

The order is the safety property: symbol always first, id only as a
fallback, so an unrelated `instrument_id` can never shadow a real ticker.
See the function's own docstring for why that is not hypothetical.

The end-to-end "by id AND by symbol both 200" acceptance test named in
spec §1.1 lives in `tests/test_api_instrument_financials.py` — it drives
the real handler through `resolve_instrument_ref`, which this file does
not exercise.
"""

from __future__ import annotations

import sys

import pytest

from app.api.instruments import (
    _RESOLVE_BY_ID_SQL,
    _RESOLVE_BY_SYMBOL_SQL,
    instrument_ref_queries,
)


def _by_symbol(s: str) -> tuple[str, dict[str, object]]:
    return (_RESOLVE_BY_SYMBOL_SQL, {"s": s})


def _by_id(iid: int) -> tuple[str, dict[str, object]]:
    return (_RESOLVE_BY_ID_SQL, {"iid": iid})


@pytest.mark.parametrize(
    ("ref", "expected"),
    [
        # --- numeric refs: symbol attempt FIRST, id attempt second ---
        ("1001", [_by_symbol("1001"), _by_id(1001)]),
        ("1", [_by_symbol("1"), _by_id(1)]),
        # Leading zeros: the symbol attempt keeps them verbatim (a ticker
        # "0001001" would be a literal match), the id attempt normalises
        # them via int().
        ("0001001", [_by_symbol("0001001"), _by_id(1001)]),
        # Whitespace is stripped before either attempt is built.
        ("  1001  ", [_by_symbol("1001"), _by_id(1001)]),
        # 19 digits = BIGINT's own width, so the bound never rejects a
        # value that could be a real id. This one is past BIGINT's RANGE
        # and still gets the id attempt — Postgres compares it as numeric
        # and returns no rows, i.e. a clean 404 (verified against the dev
        # DB rather than assumed).
        ("9" * 19, [_by_symbol("9" * 19), _by_id(int("9" * 19))]),
        # --- non-numeric refs: symbol attempt ONLY ---
        # 20+ digits cannot be an instrument_id, so no id attempt is made
        # and the symbol attempt misses. Load-bearing: `int()` raises
        # ValueError past `sys.get_int_max_str_digits()` (4300), which an
        # unbounded numeric branch would turn into a 500 instead of a 404.
        ("9" * 20, [_by_symbol("9" * 20)]),
        ("1" * 5000, [_by_symbol("1" * 5000)]),
        ("AAPL", [_by_symbol("AAPL")]),
        # Case is normalised for the UPPER(symbol) comparison.
        ("aapl", [_by_symbol("AAPL")]),
        ("  aapl  ", [_by_symbol("AAPL")]),
        # Numeric-LEADING tickers are symbols, not ids — every one in the
        # universe carries a non-digit exchange suffix (0 purely-numeric
        # symbols across the full instruments table, checked 2026-07-31).
        ("1810.HK", [_by_symbol("1810.HK")]),
        ("2501.T", [_by_symbol("2501.T")]),
        ("3DA.ASX", [_by_symbol("3DA.ASX")]),
        ("BRK.B", [_by_symbol("BRK.B")]),
        # `str.isdigit()` is True for these but `int()` either raises
        # ('²') or silently accepts a non-ASCII id ('١٠٠١'). The ASCII
        # regex means no id attempt is built; the symbol attempt
        # harmlessly misses.
        ("²", [_by_symbol("²")]),
        ("١٠٠١", [_by_symbol("١٠٠١")]),
        # Mixed digits + letters get the symbol attempt only.
        ("1001A", [_by_symbol("1001A")]),
        ("-1001", [_by_symbol("-1001")]),
    ],
)
def test_instrument_ref_queries(ref: str, expected: list[tuple[str, dict[str, object]]]) -> None:
    assert instrument_ref_queries(ref) == expected


def test_symbol_is_always_the_first_attempt() -> None:
    """The ordering is the safety property, so pin it explicitly rather
    than leaving it implied by the table above.

    An id-first branch would let `instrument_id = 1810` shadow a ticker
    `1810` and serve a DIFFERENT issuer's financials under it. 0 of 12,691
    instruments have a purely-numeric symbol today, but nothing enforces
    that, so the order — not the population — is what makes this safe."""
    for ref in ("1001", "1", "9" * 19, "AAPL", "1810.HK"):
        first_sql, _ = instrument_ref_queries(ref)[0]
        assert first_sql == _RESOLVE_BY_SYMBOL_SQL, ref


def test_symbol_attempt_keeps_the_deterministic_tie_break() -> None:
    """`symbol` is not UNIQUE across exchanges (migration 043) — the
    collision winner must stay `is_primary_listing DESC, instrument_id
    ASC`, exactly as the 27 unconverted call sites still spell it."""
    sql, _ = instrument_ref_queries("AAPL")[0]
    assert "ORDER BY is_primary_listing DESC, instrument_id ASC" in sql
    assert "LIMIT 1" in sql


def test_a_pathological_digit_string_never_reaches_int() -> None:
    """Regression: `int()` raises `ValueError` past
    `sys.get_int_max_str_digits()`, so an unbounded numeric branch turned
    `/instruments/<5000 digits>/financials` into a 500. The 19-digit
    bound must mean no id attempt is built — no raise."""
    assert int("1" * 4000)  # inside the limit
    with pytest.raises(ValueError):
        int("1" * (sys.get_int_max_str_digits() + 1))

    attempts = instrument_ref_queries("1" * (sys.get_int_max_str_digits() + 1))
    assert len(attempts) == 1
    assert attempts[0][0] == _RESOLVE_BY_SYMBOL_SQL


def test_id_attempt_does_not_filter_on_symbol() -> None:
    """An id lookup is unique by primary key — no UPPER(symbol) predicate,
    and no ORDER BY needed to make it deterministic."""
    sql, params = instrument_ref_queries("1001")[1]
    assert "UPPER(symbol)" not in sql
    assert "instrument_id = %(iid)s" in sql
    assert params == {"iid": 1001}
