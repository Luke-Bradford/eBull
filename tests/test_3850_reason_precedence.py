"""#3850: the acceptance script's independently derived final reason follows the spec's precedence order.

Spec "Construction" item 7: dual_class, ads_ratio, fpi_adr, no_pit_row, then PIT's own reason. sql/505 encodes the
same order; ``scripts/measure_3850_acceptance.py`` section 1 checks the view against this function row by row.
"""

from __future__ import annotations

from typing import Any

from scripts.measure_3850_acceptance import expected_reason


def _row(**flags: Any) -> dict[str, Any]:
    base = {
        "is_dual_class": False,
        "has_ratio": False,
        "is_fpi_adr": False,
        "has_pit": True,
        "pit_reason": None,
    }
    return base | flags


def test_precedence_order() -> None:
    every = {"is_dual_class": True, "has_ratio": True, "is_fpi_adr": True, "has_pit": False}
    assert expected_reason(_row(**every)) == "dual_class"
    assert expected_reason(_row(**(every | {"is_dual_class": False}))) == "ads_ratio"
    assert expected_reason(_row(is_fpi_adr=True, has_pit=False)) == "fpi_adr"
    assert expected_reason(_row(has_pit=False, pit_reason="no_left_anchor")) == "no_pit_row"
    assert expected_reason(_row(pit_reason="basis_stale")) == "basis_stale"
    assert expected_reason(_row()) is None
