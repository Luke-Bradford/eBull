"""Pure-logic tests for the force-refresh helpers (#677 Part A).

No DB — these pin the symbol-normalization + instrument-dedup contracts
that shape the ``POST /admin/fundamentals/refresh`` request handling.
The DB-backed resolver (``resolve_symbols``) is covered separately in
``tests/test_force_refresh_fundamentals.py``.
"""

from __future__ import annotations

from app.api.fundamentals_admin import _distinct_symbols
from app.services.fundamentals.force_refresh import ResolvedSymbol, dedupe_resolved


class TestDistinctSymbols:
    def test_uppercases_strips_and_dedups_preserving_order(self) -> None:
        assert _distinct_symbols([" aapl ", "MSFT", "AApl", "msft", "gme"]) == ["AAPL", "MSFT", "GME"]

    def test_drops_empty_and_whitespace_only(self) -> None:
        assert _distinct_symbols(["", "   ", "IEP"]) == ["IEP"]

    def test_empty_input(self) -> None:
        assert _distinct_symbols([]) == []


def _rs(symbol: str, instrument_id: int, cik: str = "0000000001") -> ResolvedSymbol:
    return ResolvedSymbol(symbol=symbol, instrument_id=instrument_id, cik=cik)


class TestDedupeResolved:
    def test_collapses_duplicate_instrument_ids_first_wins(self) -> None:
        rows = [_rs("IEP", 1), _rs("IEP", 1), _rs("MPLX", 2)]
        out = dedupe_resolved(rows)
        assert [(r.symbol, r.instrument_id) for r in out] == [("IEP", 1), ("MPLX", 2)]

    def test_two_symbols_same_instrument_keep_first(self) -> None:
        # Distinct tickers can map to the same instrument_id; we re-fetch
        # the CIK once, keeping the first occurrence.
        rows = [_rs("GOOG", 9), _rs("GOOGL", 9)]
        out = dedupe_resolved(rows)
        assert [r.symbol for r in out] == ["GOOG"]

    def test_no_duplicates_passthrough(self) -> None:
        rows = [_rs("A", 1), _rs("B", 2), _rs("C", 3)]
        assert dedupe_resolved(rows) == rows

    def test_empty(self) -> None:
        assert dedupe_resolved([]) == []
