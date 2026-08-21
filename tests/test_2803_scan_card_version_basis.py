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
from typing import get_type_hints

from app.api import strategies as strategies_api
from app.api.strategies import (
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


def _scan_basis_sql_in_the_api_module() -> dict[str, str]:
    """Module-level ``_*_SQL`` in ``app.api.strategies`` -> the scan relation it reads.

    DERIVED over the whole module, never listed — the same narrowings as
    ``_loaders_over_a_scan_relation``: the relation must follow ``FROM``/``JOIN``
    and match exactly, and the SQL must bind ``%(versions)s`` at all.

    ⚠ This replaced a literal list of three constants read out of
    ``get_strategy_overview`` alone, which is how #2814 survived #2806 by one
    function. ``strategy_signals`` was already in ``_SCAN_RELATIONS`` and
    ``_FIRED_SIGNALS_SQL`` already read it on the result basis; the guard simply
    was not looking at ``get_fired_signals``. #2806's own prevention entry says
    "derive the guarded set from the code under guard, never from a literal
    list" — it derived the ``strategy_monitoring`` half and left this half
    enumerated.
    """
    found: dict[str, str] = {}
    for name in dir(strategies_api):
        if not name.endswith("_SQL"):
            continue
        sql = getattr(strategies_api, name)
        if not isinstance(sql, str) or "%(versions)s" not in sql:
            continue
        read = set(_READ_RELATION.findall(sql))
        relation = next((r for r in _SCAN_RELATIONS if r in read), None)
        if relation is not None:
            found[name] = relation
    return found


def _executors_of(constant: str) -> dict[str, list[str]]:
    """Function name -> the params names it passes to ``cur.execute(constant, …)``.

    Searched across every function in the module, not one named endpoint: the
    executor of a scan-relation query is exactly what #2814 got wrong.
    """
    executed: dict[str, list[str]] = {}
    for name in dir(strategies_api):
        function = getattr(strategies_api, name)
        if not inspect.isfunction(function) or function.__module__ != strategies_api.__name__:
            continue
        arguments = re.findall(rf"cur\.execute\(\s*{constant}\s*,\s*(\w+)\s*\)", inspect.getsource(function))
        if arguments:
            executed[name] = arguments
    return executed


def test_a_result_basis_query_is_not_swept_into_the_api_scan_guard() -> None:
    """The result-store queries must stay OUT, or the guard cries wolf (#2808).

    Same asymmetry as ``test_a_result_basis_loader_is_not_swept_into_the_scan_guard``:
    a guard that fires on a correctly-bound result-basis query gets muted, and a
    muted guard is how #2803 came back twice.
    """
    guarded = _scan_basis_sql_in_the_api_module()
    for name in ("_RESULTS_SQL", "_RESULT_COUNTS_SQL", "_PRIOR_VERSION_RESULTS_SQL"):
        assert name not in guarded, f"{name} reads the result store and must stay on _current_versions()"
        assert "%(versions)s" in getattr(strategies_api, name), (
            f"{name} no longer binds versions — recheck which basis it belongs on"
        )


def test_every_scan_relation_query_is_executed_on_the_scan_basis() -> None:
    """Guard the BINDING, not just the helper.

    ``_current_scan_versions`` existing changes nothing if a caller keeps passing
    the result-basis params; that pairing is the thing that regressed three
    times, and no type or comment catches it because both dicts have the same
    shape and the same ``versions`` key.

    The basis sets are computed per EXECUTING FUNCTION rather than once over
    ``get_strategy_overview``: an endpoint that binds only the scan basis (as
    ``get_fired_signals`` does after #2814) mentions ``_current_versions``
    nowhere, so a whole-module or single-function floor on both names would
    either fail spuriously or send the reader to the wrong function.
    """
    guarded = _scan_basis_sql_in_the_api_module()
    # ⚠ A FLOOR, not a list — the same revert-probe lesson as the loader guard
    # below. New constants are still picked up automatically; a known one
    # dropping out (a renamed table, a dropped version predicate) has to be loud,
    # because the surviving subjects would satisfy every assertion beneath it.
    assert {"_SCAN_SQL", "_EXCLUSIONS_SQL", "_PRIOR_VERSION_SCANS_SQL", "_FIRED_SIGNALS_SQL"} <= set(guarded), (
        f"a known scan-basis query dropped out of the derived set ({sorted(guarded)}) — "
        "retarget this guard rather than letting it pass on the remainder"
    )
    for name, relation in sorted(guarded.items()):
        executed = _executors_of(name)
        assert executed, f"{name} filters {relation} but no function in app.api.strategies executes it"
        for function_name, arguments in sorted(executed.items()):
            source = inspect.getsource(getattr(strategies_api, function_name))
            scan_derived = _names_derived_from(source, "_current_scan_versions")
            result_derived = _names_derived_from(source, "_current_versions")
            for params_name in arguments:
                assert params_name in scan_derived and params_name not in result_derived, (
                    f"{function_name} executes {name}, which reads {relation}, with {params_name!r} — "
                    "not derived from _current_scan_versions(); live-scan relations are keyed by the "
                    "scan identity basis (#2803/#2806/#2814)"
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


def _assert_read_through_a_version_collapsing_pool(
    source: str,
    holder: str,
    scan_derived: frozenset[str],
    result_derived: frozenset[str],
) -> None:
    """The second safe read shape: the version dimension is ERASED, not chosen (#2807).

    ``load_owned_pnl``'s rows are no longer read one version at a time. A paper
    deployment stays at the version it was deployed on while the live scan
    rotates, so the card pools every version the strategy holds positions under —
    there is then no version key to get wrong, which is a stronger property than
    picking the right one.

    That is only true while the pooling function really collapses the version, so
    this asserts its RETURN TYPE rather than trusting its name: a bare ``str`` key
    is a ``strategy_id``, and a tuple key would put the #2806 defect straight back
    with a guard that no longer looks for it.
    """
    pooled = re.findall(rf"(\w+)\s*=\s*(\w+)\(\s*{holder}\s*\)", source)
    assert pooled, (
        f"{holder} is never read back — neither at a scan key nor through a pooling call that "
        "takes it whole (#2806/#2807)"
    )
    for pooled_name, pooler_name in pooled:
        pooler = getattr(strategy_monitoring, pooler_name, None)
        assert pooler is not None, f"{pooler_name} is not a strategy_monitoring function"
        returns = get_type_hints(pooler)["return"]
        assert returns == dict[str, strategy_monitoring.StrategyPnl], (
            f"{pooler_name} returns {returns!r}, so it does not collapse the version dimension — "
            "a pooled read is only basis-safe while the key is a bare strategy_id (#2807)"
        )
        lookups = re.findall(rf"\b{pooled_name}\.get\(\s*(\w+)", source)
        assert lookups, f"{pooled_name} is never read back"
        for lookup_key in lookups:
            assert lookup_key not in scan_derived and lookup_key not in result_derived, (
                f"{pooled_name} holds one row per strategy but is read at {lookup_key!r}, which "
                "carries a version basis — the pooled dict has no version key (#2807)"
            )


def test_the_scan_basis_loaders_are_read_back_at_the_scan_key() -> None:
    """Rebinding the query is only half the fix, and the halves fail identically.

    Both a result-basis filter and a result-basis dict lookup produce an empty
    card, so fixing one and not the other looks exactly like fixing neither.

    Two read shapes are safe, and a loader must use one of them: per-version at a
    scan-derived key, or pooled through a call that erases the version entirely
    (#2807). Neither branch is an exemption — the pooled one carries its own
    assertions in the helper above.
    """
    source = inspect.getsource(get_strategy_overview)
    scan_derived, result_derived = _the_two_bases_as_named_in(source)
    for name in sorted(_loaders_over_a_scan_relation()):
        assigned = re.findall(rf"(\w+)\s*=\s*{name}\(", source)
        assert assigned, f"{name} result is not bound to a name in get_strategy_overview"
        for holder in assigned:
            lookups = re.findall(rf"\b{holder}\.get\(\s*(\w+)", source)
            if not lookups:
                _assert_read_through_a_version_collapsing_pool(source, holder, scan_derived, result_derived)
                continue
            for lookup_key in lookups:
                assert lookup_key in scan_derived and lookup_key not in result_derived, (
                    f"{holder} holds rows keyed by the scan basis but is read at {lookup_key!r}, "
                    "which is not derived from _current_scan_versions() (#2806)"
                )
