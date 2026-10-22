"""Shared 13F test helpers."""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal
from typing import Any

from app.providers.implementations.sec_13f import ThirteenFInfotableControls


def with_controls(parse: Callable[[str], Any]) -> Callable[[str], tuple[Any, ThirteenFInfotableControls]]:
    """Adapt a ``parse_infotable`` stub to ``parse_infotable_with_controls``, which the per-filing writers call
    (#3227 (IM)). The controls are empty: these stubs exercise holdings, not the capture."""
    return lambda xml: (parse(xml), ThirteenFInfotableControls(line_count=0, value_total=Decimal(0)))
