"""#3622 slice 1 — IBKR borrow file parser: framing, quirks, and refusal of a truncated or malformed file."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from app.services.ibkr_borrow_archive import BorrowFileError, parse_borrow_file

_HEAD = "#BOF|2026.10.04|08:36:44\n#SYM|CUR|NAME|CON|ISIN|REBATERATE|FEERATE|AVAILABLE|FIGI|\n"
_GME = "GME|USD|GAMESTOP CORP-CLASS A|36285627|XXXXXXXW1099|3.6123|0.2677|>10000000|BBG000BB5BF6|\n"
_NA = "ZZZ|USD|SOME NAME|42|US0000000000|NA|NA|2000||\n"


def test_parses_rates_caps_and_na_and_stamps_as_of_in_new_york() -> None:
    parsed = parse_borrow_file((_HEAD + _GME + _NA + "#EOF|2\n").encode())
    assert parsed.provider_as_of == datetime(2026, 10, 4, 12, 36, 44, tzinfo=UTC)
    gme, zzz = parsed.rates
    assert (gme.conid, gme.fee_rate_pct, gme.rebate_rate_pct) == (36285627, Decimal("0.2677"), Decimal("3.6123"))
    assert (gme.available_shares, gme.available_capped) == (10_000_000, True)
    assert (zzz.fee_rate_pct, zzz.rebate_rate_pct, zzz.figi) == (None, None, None)
    assert (zzz.available_shares, zzz.available_capped) == (2000, False)


@pytest.mark.parametrize(
    ("text", "match"),
    [
        (_HEAD + _GME, "#EOF"),
        (_HEAD + _GME + "#EOF|2\n", "declares 2 rows"),
        ("#BOF|2026-10-04|08:36:44\n" + _HEAD.split("\n", 1)[1] + _GME + "#EOF|1\n", "#BOF"),
        (_HEAD.replace("FIGI|", "FIGI|X|") + _GME + "#EOF|1\n", "header"),
        (_HEAD + _GME.replace("|BBG000BB5BF6|", "|BBG000BB5BF6") + "#EOF|1\n", "pipe-terminated"),
        (_HEAD + _GME + _GME + "#EOF|2\n", "duplicate CON"),
        (_HEAD + _GME.replace("0.2677", "n/a") + "#EOF|1\n", "FEERATE"),
        (_HEAD + _GME.replace(">10000000", "lots") + "#EOF|1\n", "AVAILABLE"),
        (_HEAD + "#EOF|0\n", "no rows"),
    ],
)
def test_refuses_a_malformed_or_truncated_file(text: str, match: str) -> None:
    with pytest.raises(BorrowFileError, match=match):
        parse_borrow_file(text.encode())
