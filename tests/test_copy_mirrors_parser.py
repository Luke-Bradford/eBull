# tests/test_copy_mirrors_parser.py
"""§8.1 parser unit tests for copy-trading mirror ingestion.

Pure unit tests — no DB, no I/O, no broker HTTP. Exercises
_parse_mirror / _parse_mirror_position and the outer top-level
loop in etoro_broker.get_portfolio's mirrors[] branch.
"""

from __future__ import annotations

import decimal

import pytest

from app.providers.implementations.etoro_broker import PortfolioParseError


def test_portfolio_parse_error_is_direct_exception_subclass() -> None:
    """Spec §2.2.1: PortfolioParseError MUST subclass Exception directly.

    If it subclassed ValueError / TypeError / KeyError /
    decimal.DecimalException, the outer parse loop's
    `except (KeyError, ValueError, TypeError, decimal.DecimalException)`
    block would silently swallow it, defeating the §2.3.3 strict-raise
    and enabling the §2.3.4 soft-close hole Codex v3 finding V flagged.
    """
    assert issubclass(PortfolioParseError, Exception) is True
    assert issubclass(PortfolioParseError, ValueError) is False
    assert issubclass(PortfolioParseError, TypeError) is False
    assert issubclass(PortfolioParseError, KeyError) is False
    assert issubclass(PortfolioParseError, decimal.DecimalException) is False


def test_portfolio_parse_error_is_raisable_with_cause() -> None:
    inner = ValueError("boom")
    with pytest.raises(PortfolioParseError) as excinfo:
        raise PortfolioParseError("wrap") from inner
    assert excinfo.value.__cause__ is inner
