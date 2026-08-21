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

import ast
import inspect
import re
import textwrap

from app.api.strategies import (
    _EXCLUSIONS_SQL,
    _PRIOR_VERSION_SCANS_SQL,
    _SCAN_SQL,
    _current_scan_versions,
    _current_versions,
    get_strategy_overview,
)
from app.services import strategy_monitoring
from app.services.backtest_run import BACKTEST_UNIVERSE
from app.services.cost_model import COST_MODEL_ID
from app.services.strategy_manifest import STRATEGY_MANIFEST
from app.services.strategy_signal_scan import SCAN_UNIVERSE

#: Every relation the live scan writes. A query naming one of these MUST be
#: filtered on the scan basis.
#:
#: ⚠ ``strategy_signals`` joined the list with #2806, which found the #2803
#: defect intact on the loaders in ``strategy_monitoring``.
_SCAN_RELATIONS = ("strategy_scan_watermark", "strategy_signal_daily_counts", "strategy_signals")

#: Relations a query actually READS, as opposed to ones its comments mention.
_READ_RELATION = re.compile(r"(?:FROM|JOIN)\s+(\w+)", re.IGNORECASE)


def _names_derived_from(source: str, origin: str) -> frozenset[str]:
    """Local names in ``source`` whose value FLOWS FROM ``origin()``.

    Provenance, not naming — PR #2808 review, raised on two rounds. The earlier
    form of the guards below asserted ``"scan" in argument``, which reads the
    variable's NAME: a misleadingly named result-basis value would pass, and a
    correctly derived one called something else would fail. Both errors point the
    wrong way, because what regressed in #2803/#2806 is where the value CAME
    FROM, and the two bases are structurally identical otherwise (same dict
    shape, same ``versions`` key, same ``str`` versions).

    Forward taint over assignments: a name is derived when its right-hand side
    mentions ``origin`` or an already-derived name. That reaches the real chain
    in ``get_strategy_overview`` — ``scan_versions`` -> ``scan_version_values``
    -> ``scan_params`` / ``scan_pnl_versions`` / ``scan_key`` — without naming
    any of them here.
    """
    tree = ast.parse(textwrap.dedent(source))
    assignments = [
        (
            [target.id for target in node.targets if isinstance(target, ast.Name)],
            {ref.id for ref in ast.walk(node.value) if isinstance(ref, ast.Name)},
        )
        for node in ast.walk(tree)
        if isinstance(node, ast.Assign)
    ]
    derived = {origin}
    while True:
        grown = {name for targets, refs in assignments if refs & derived for name in targets} - derived
        if not grown:
            return frozenset(derived - {origin})
        derived |= grown


def _the_two_bases_as_named_in(source: str) -> tuple[frozenset[str], frozenset[str]]:
    """``(scan-derived, result-derived)`` names in ``source``, both floored non-empty.

    ⚠ Deliberately NOT asserting the two sets are globally disjoint, though they
    are today. Taint flows forward, so any value that legitimately consumes both
    bases — a card field assembled from a result row and a scan count — would be
    in both and trip a confusing failure far from the binding it is about. That
    is the cry-wolf shape this file has already been narrowed twice to avoid.
    The mixing check belongs per-name, at the three sites that bind a basis.
    """
    scan_derived = _names_derived_from(source, "_current_scan_versions")
    result_derived = _names_derived_from(source, "_current_versions")
    assert scan_derived, "nothing is derived from _current_scan_versions — retarget this guard"
    assert result_derived, "nothing is derived from _current_versions — retarget this guard"
    return scan_derived, result_derived


def _loaders_over_a_scan_relation() -> dict[str, str]:
    """``strategy_monitoring`` loader name -> the scan relation its SQL filters.

    DERIVED, never listed. A fourth loader added over a scan relation has to be
    bound correctly or this guard fails on it without anyone remembering to
    extend a literal — which is exactly how #2806 survived #2803.

    ⚠ Two narrowings, both from the PR #2808 review, and both guard against a
    FALSE positive rather than a miss — a guard that fires on a loader which
    legitimately reads the result basis would be read as noise and then muted:

    1. The relation must appear after ``FROM``/``JOIN`` and match a table name
       EXACTLY, so neither a comment mentioning a scan relation nor a longer
       table name containing one can pull a loader in.
    2. The SQL must bind ``%(versions)s`` at all. A query with no version
       predicate has no basis to get wrong.
    """
    found: dict[str, str] = {}
    for name in dir(strategy_monitoring):
        loader = getattr(strategy_monitoring, name)
        if not name.startswith("load_") or not inspect.isfunction(loader):
            continue
        for const in re.findall(r"\b(_\w+_SQL)\b", inspect.getsource(loader)):
            sql = getattr(strategy_monitoring, const, "")
            if "%(versions)s" not in sql:
                continue
            read = set(_READ_RELATION.findall(sql))
            relation = next((r for r in _SCAN_RELATIONS if r in read), None)
            if relation is not None:
                found[name] = relation
    return found


def test_a_result_basis_loader_is_not_swept_into_the_scan_guard() -> None:
    """`load_control_state` reads the RESULT basis and must stay out (PR #2808).

    Asserted rather than reasoned about, because the cost of being wrong is
    asymmetric: a false guard failure on a correct loader teaches the next reader
    that this file cries wolf, which is how the #2803 guard's real coverage gap
    would have been dismissed too.
    """
    assert "load_control_state" not in _loaders_over_a_scan_relation()
    assert "strategy_results_store" in strategy_monitoring._CONTROL_SQL, (
        "load_control_state no longer reads the result store — recheck which basis it belongs on"
    )


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
    scan_derived, result_derived = _the_two_bases_as_named_in(source)
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
            assert params_name in scan_derived and params_name not in result_derived, (
                f"{name} reads {_SCAN_RELATIONS} but is executed with {params_name!r}, which is not "
                "derived from _current_scan_versions(); live-scan relations are keyed by the scan "
                "identity basis (#2803)"
            )


def test_every_loader_over_a_scan_relation_is_called_on_the_scan_basis() -> None:
    """The #2806 half: a loader binds its versions through a KEYWORD ARGUMENT.

    The guard above only sees ``cur.execute(_X_SQL, params)`` pairs inside this
    file, so it could not see `load_fire_rate` / `load_attribution` /
    `load_owned_pnl` — which live in another module and take ``versions=`` — and
    all three were still called with the result basis after #2803 shipped. On dev
    that made every strategy report ``share_unavailable_reason: never_scanned``
    hours after a scan wrote 216 census rows for all ten of them.
    """
    source = inspect.getsource(get_strategy_overview)
    scan_derived, result_derived = _the_two_bases_as_named_in(source)
    loaders = _loaders_over_a_scan_relation()
    # ⚠ A FLOOR, not a list — new loaders are still picked up automatically, but
    # a known one dropping out has to be loud. Found by revert-probe: renaming the
    # census table made `load_fire_rate` vanish from the derived set and every
    # assertion below still passed, because the survivors satisfied them. Silent
    # coverage loss is how the #2803 guard came to protect nothing here.
    assert {"load_attribution", "load_fire_rate", "load_owned_pnl"} <= set(loaders), (
        f"a known scan-basis loader dropped out of the derived set ({sorted(loaders)}) — "
        "retarget this guard rather than letting it pass on the remainder"
    )
    for name, relation in sorted(loaders.items()):
        called_with = re.findall(rf"\b{name}\(\s*conn\s*,\s*versions=(\w+)", source)
        assert called_with, f"{name} filters {relation} but get_strategy_overview does not call it"
        for argument in called_with:
            assert argument in scan_derived and argument not in result_derived, (
                f"{name} filters {relation} but is called with {argument!r}, which is not derived "
                "from _current_scan_versions(); live-scan relations are keyed by the scan identity "
                "basis (#2806)"
            )


def test_the_scan_basis_loaders_are_read_back_at_the_scan_key() -> None:
    """Rebinding the query is only half the fix, and the halves fail identically.

    Both a result-basis filter and a result-basis dict lookup produce an empty
    card, so fixing one and not the other looks exactly like fixing neither.
    """
    source = inspect.getsource(get_strategy_overview)
    scan_derived, result_derived = _the_two_bases_as_named_in(source)
    for name in sorted(_loaders_over_a_scan_relation()):
        assigned = re.findall(rf"(\w+)\s*=\s*{name}\(", source)
        assert assigned, f"{name} result is not bound to a name in get_strategy_overview"
        for holder in assigned:
            lookups = re.findall(rf"\b{holder}\.get\(\s*(\w+)", source)
            assert lookups, f"{holder} is never read back"
            for lookup_key in lookups:
                assert lookup_key in scan_derived and lookup_key not in result_derived, (
                    f"{holder} holds rows keyed by the scan basis but is read at {lookup_key!r}, "
                    "which is not derived from _current_scan_versions() (#2806)"
                )
