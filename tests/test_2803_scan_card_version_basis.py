"""The scan card reads the SCAN identity basis, not the result basis (#2803).

Pure (no Postgres). The defect was a params binding, not logic: three queries over
live-scan relations were handed ``_current_versions()``, which is built on
``BACKTEST_UNIVERSE``. ``universe`` is part of ``StrategyIdentity``, so those
predicates matched nothing and every strategy reported ``scan.status = "rotated"``
with zero counts immediately after a scan that wrote 81,485 rows.

Measured on dev at the fix: ``_SCAN_SQL`` returned 0 rows on the result basis and
10 on the scan basis.
"""

from __future__ import annotations

import inspect
import re

from app.api.strategies import (
    _EXCLUSIONS_SQL,
    _PRIOR_VERSION_SCANS_SQL,
    _SCAN_SQL,
    _current_scan_versions,
    _current_versions,
    get_strategy_overview,
)
from app.services.backtest_run import BACKTEST_UNIVERSE
from app.services.cost_model import COST_MODEL_ID
from app.services.strategy_manifest import STRATEGY_MANIFEST
from app.services.strategy_signal_scan import SCAN_UNIVERSE

#: Every relation the live scan writes. A query naming one of these MUST be
#: filtered on the scan basis.
_SCAN_RELATIONS = ("strategy_scan_watermark", "strategy_signal_daily_counts")


def test_the_scan_basis_is_the_universe_the_scanner_actually_stamps() -> None:
    expected = {
        strategy_id: entry.identity(universe=SCAN_UNIVERSE, cost_model_id=COST_MODEL_ID).version
        for strategy_id, entry in STRATEGY_MANIFEST.items()
    }
    assert _current_scan_versions() == expected


def test_the_two_bases_are_disjoint_so_one_cannot_stand_in_for_the_other() -> None:
    """The whole defect in one assertion.

    If these sets ever overlapped, binding the wrong one would degrade to a
    partial miss. They do not overlap, so the wrong one matches NOTHING — which
    is why the bug presented as a permanently empty card rather than a flaky one.
    """
    assert SCAN_UNIVERSE != BACKTEST_UNIVERSE
    assert not set(_current_versions().values()) & set(_current_scan_versions().values())


def test_every_scan_relation_query_is_executed_on_the_scan_basis() -> None:
    """Guard the BINDING, not just the helper.

    ``_current_scan_versions`` existing changes nothing if the overview keeps
    passing ``params``; that pairing is the thing that regressed, and no type or
    comment catches it because both dicts have the same shape and key name.
    """
    source = inspect.getsource(get_strategy_overview)
    scan_sql = {
        "_SCAN_SQL": _SCAN_SQL,
        "_EXCLUSIONS_SQL": _EXCLUSIONS_SQL,
        "_PRIOR_VERSION_SCANS_SQL": _PRIOR_VERSION_SCANS_SQL,
    }
    for name, sql in scan_sql.items():
        assert any(relation in sql for relation in _SCAN_RELATIONS), (
            f"{name} no longer reads a scan relation — retarget this guard rather than deleting it"
        )
        executed = re.findall(rf"cur\.execute\(\s*{name}\s*,\s*(\w+)\s*\)", source)
        assert executed, f"{name} is not executed by get_strategy_overview"
        for params_name in executed:
            assert "scan" in params_name, (
                f"{name} reads {_SCAN_RELATIONS} but is executed with {params_name!r}; "
                "live-scan relations are keyed by the scan identity basis (#2803)"
            )
