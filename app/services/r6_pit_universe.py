"""Frozen, outcome-blind listed-common-equity classifier for R6.

SEC ``Security12bTitle`` is the point-in-time class identity available in the
retained filing.  The source does not expose CRSP share codes, so this module
fixes the documented text proxy used by both the #2908 population and its
reference-factor diagnostic.  Exclusions are evaluated before inclusions so a
unit or warrant mentioning an underlying common share cannot enter.
"""

from __future__ import annotations

from typing import Final

SUPPORTED_EXCHANGES: Final = frozenset(
    {
        "CHX",
        "CboeBZX",
        "MIAX",
        "NASDAQ",
        "NYSE",
        "NYSEAMER",
        "NYSEArca",
    }
)

_EXCLUDED_TITLE_TERMS: Final = (
    "beneficial interest",
    "bond",
    "debenture",
    "depositary",
    "depository",
    "exchange-traded fund",
    "exchange traded fund",
    "fund share",
    "note",
    "preferred",
    "right",
    "trust share",
    "unit",
    "warrant",
)
_COMMON_EQUITY_TERMS: Final = ("common stock", "common share", "ordinary share")


def common_equity_reason(*, security_title: str, exchange: str) -> str:
    """Return the first frozen inclusion/exclusion reason for one SEC class."""
    if exchange not in SUPPORTED_EXCHANGES:
        return "unsupported_exchange"
    title = " ".join(security_title.casefold().split())
    if not title or title in {"n/a", "none"}:
        return "missing_security_title"
    for term in _EXCLUDED_TITLE_TERMS:
        if term in title:
            return f"excluded_title:{term}"
    if any(term in title for term in _COMMON_EQUITY_TERMS):
        return "included_common_equity"
    return "unrecognised_security_title"


def is_common_equity(*, security_title: str, exchange: str) -> bool:
    return common_equity_reason(security_title=security_title, exchange=exchange) == "included_common_equity"


__all__ = ["SUPPORTED_EXCHANGES", "common_equity_reason", "is_common_equity"]
