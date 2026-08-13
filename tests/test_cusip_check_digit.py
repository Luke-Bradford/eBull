"""#2304 — pin the CUSIP mod-10 check digit that gates the tombstone reset.

``scripts/audit_cusip_check_digit.py`` decides WHICH ``openfigi_unknown``
rows an operator is offered for reset, so its check-digit rule is a
data-treatment decision, not a script detail. It had no test.

The rule is not ours: CUSIP Issuer Number check digit, "modulus 10
double-add-double" (ANSI X9.6 / CUSIP Global Services user manual), which
CINS inherits unchanged for its letter-leading prefixes. It is also the
rule OpenFIGI itself applies — a failing value comes back as a per-item
``{"error": "Invalid idValue format."}``, which is the whole subject of
#2304 (probed live 2026-08-06, ``.claude/skills/data-sources/openfigi.md``
§4.3).

⚠ Every expectation below is hand-transcribed, never computed from the
module under test — a reference that imports the thing it validates
passes no matter what that thing does. Worked example for ``037833100``
(AAPL), doubling at even positions and summing the digits of each result:
``0, 3→6, 7, 8→16→7, 3, 3→6, 1, 0`` sums to 30, so the check digit is
``(10 - 30 % 10) % 10 = 0``, matching the stored 9th character.
"""

from __future__ import annotations

import pytest

from scripts.audit_cusip_check_digit import cusip_check_digit, is_valid_cusip


class TestCusipCheckDigit:
    @pytest.mark.parametrize(
        ("cusip", "expected_check_digit"),
        [
            # The clause-8 smoke panel, check digits transcribed by hand.
            ("037833100", 0),  # AAPL
            ("36467W109", 9),  # GME
            ("437076102", 2),  # HD
            ("46625H100", 0),  # JPM
            ("594918104", 4),  # MSFT
            # A CINS. Letter-leading and foreign, and still valid — the
            # discriminant is the check digit, NOT the character class.
            ("G0692U109", 9),
        ],
    )
    def test_known_valid_identifiers(self, cusip: str, expected_check_digit: int) -> None:
        assert cusip_check_digit(cusip[:8]) == expected_check_digit
        assert is_valid_cusip(cusip) is True

    def test_unassigned_but_well_formed_identifier_is_valid(self) -> None:
        """``000000000`` satisfies the check digit and OpenFIGI answers it
        with a ``warning`` (no identifier found), not an ``error``.

        Pins the boundary #2304 exists to hold: "the source has no mapping"
        and "the source rejected the input" are different facts, and this
        value is the first, not the second.
        """
        assert is_valid_cusip("000000000") is True

    @pytest.mark.parametrize(
        ("cusip", "reason"),
        [
            # Passes every local shape test — 9 characters, all in the
            # CUSIP alphabet — and still fails, because the check digit
            # over 'ZZZZZZZZ' is 0 and the 9th character is 'Z'. This is
            # the value OpenFIGI rejects outright.
            ("ZZZZZZZZZ", "shape-valid, check digit wrong"),
            ("037833101", "AAPL with the check digit off by one"),
            ("88160R10", "eight characters, truncated"),
            ("037833abc", "lowercase is outside the CUSIP alphabet"),
            ("", "empty"),
            ("03783310X", "check position is not a digit"),
        ],
    )
    def test_rejected_identifiers(self, cusip: str, reason: str) -> None:
        assert is_valid_cusip(cusip) is False, reason
