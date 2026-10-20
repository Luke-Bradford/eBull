"""CUSIP check digit: the "modulus 10 double-add-double" rule.

Source rule: CUSIP Global Services (ANSI X9.6), inherited unchanged by CINS for its letter-leading prefixes. Character
values ``0-9`` → 0-9, ``A-Z`` → 10-35, ``*`` → 36, ``@`` → 37, ``#`` → 38; every second value (positions 2, 4, 6, 8)
doubled; the digits of each result summed; the check digit is ``(10 - total % 10) % 10``. OpenFIGI applies the same
rule (#2304). Moved here from ``scripts/audit_cusip_check_digit.py`` for #2327, whose bridge gate R1 is app code.
"""

from __future__ import annotations

from typing import Final

_CHAR_VALUES: Final[dict[str, int]] = {
    **{c: i for i, c in enumerate("0123456789")},
    **{c: 10 + i for i, c in enumerate("ABCDEFGHIJKLMNOPQRSTUVWXYZ")},
    "*": 36,
    "@": 37,
    "#": 38,
}


def cusip_check_digit(first_eight: str) -> int:
    """Return the check digit the mod-10 rule requires for ``first_eight``.

    Raises ``KeyError`` on a character outside the CUSIP alphabet — callers
    that accept arbitrary stored text should use :func:`is_valid_cusip`.
    """
    total = 0
    for position, char in enumerate(first_eight, start=1):
        value = _CHAR_VALUES[char]
        if position % 2 == 0:
            value *= 2
        total += value // 10 + value % 10
    return (10 - total % 10) % 10


def is_valid_cusip(cusip: str) -> bool:
    """True when ``cusip`` is 9 characters in the CUSIP alphabet whose final
    character is the digit the mod-10 rule requires."""
    if len(cusip) != 9 or any(char not in _CHAR_VALUES for char in cusip):
        return False
    return cusip[8].isdigit() and cusip_check_digit(cusip[:8]) == int(cusip[8])
