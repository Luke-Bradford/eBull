"""Drift and floor-safety tests for the eToro quota lane map (#2946 items 1 + 3).

Pure logic — no DB, no network, no broker call.  The census these tests guard is
in ``docs/proposals/execution/2026-09-13-etoro-quota-lane-map.md``.

⚠ What these tests are NOT, stated up front because the spec's first draft
implied more than they deliver:

* ``test_configured_floor_is_at_least_the_conservative_minimum`` is a NECESSARY
  LOCAL CHECK, not a safety proof.  It cannot see how many provider instances or
  processes exist on one user key (each gets its own clock — two instances pass
  this test while issuing roughly twice the rate), nor retries, nor the four
  unthrottled bypasses, nor a ``RateGate`` overriding the interval entirely.
* The AST tests COUNT.  An equal-size substitution, or adding and removing an
  endpoint in one commit, passes silently, and a count can never detect a WRONG
  lane assignment.  They are a re-read trigger, not a proof.
* Nothing here can detect that the portal CHANGED.  Only a re-fetch can.
"""

from __future__ import annotations

import ast
import importlib
import pathlib

import pytest

from app.providers.implementations.etoro_quota_lanes import (
    ACCEPTED_FLOOR_EXCEPTIONS,
    CALL_SITES,
    EXPRESSION_COUNTS,
    FLOOR_CONSTANT_NAMES,
    KNOWN_PATH_DRIFT,
    KNOWN_UNTHROTTLED,
    LANES,
    RAW_HTTPX_EXPRESSION_COUNTS,
    min_interval_for_stamps,
)

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]

# An accepted floor exception must carry a REASON, not a label. The threshold is
# a floor on prose length: long enough that "deferred" or "known issue" fails,
# short enough that a genuine one-sentence justification passes.
_MIN_EXCEPTION_REASON_CHARS = 80


# ---------------------------------------------------------------------------
# Table invariants
# ---------------------------------------------------------------------------


def test_lane_table_is_internally_consistent() -> None:
    for key, lane in LANES.items():
        assert lane.key == key, f"{key}: lane.key disagrees with its dict key"
        assert lane.documented_per_minute > 0, f"{key}: non-positive limit"
        assert lane.window_s > 0, f"{key}: non-positive window"
        assert lane.witnesses >= 1, f"{key}: a lane with no witness is not a census entry"
        assert lane.source_url.startswith("https://api-portal.etoro.com/"), f"{key}: source is not the live portal"
        # ⚠ Sustained CADENCE, deliberately not the stamp-safe interval — see the
        # property's docstring and `test_min_interval_for_stamps_is_safe_by_the_rolling
        # _window_rule` for the one-request difference between them.
        assert lane.min_interval_s == pytest.approx(lane.window_s / lane.documented_per_minute)


def test_every_call_site_names_a_known_lane() -> None:
    for site in CALL_SITES:
        assert site.lane in LANES, f"{site.module}::{site.method} -> unknown lane {site.lane!r}"
        assert site.general_tier_per_minute > 0
        assert site.demo_path.startswith("/api/"), f"{site.method}: path is not an API path"


def test_call_site_paths_are_unique_within_a_lane_and_method() -> None:
    """Two methods may share a path (both core and strategy submit orders).

    The same (method, path) pair appearing twice would be a copy-paste slip.
    """
    seen = [(s.module, s.method, s.demo_path) for s in CALL_SITES]
    assert len(seen) == len(set(seen)), "duplicate (module, method, path) entry in CALL_SITES"


def test_conservative_reading_is_never_looser_than_either_source() -> None:
    """The policy is 'take the lower of the two readings' — assert it holds."""
    for site in CALL_SITES:
        lane = LANES[site.lane]
        assert site.conservative_per_minute <= lane.documented_per_minute
        assert site.conservative_per_minute <= site.general_tier_per_minute


def test_membership_ambiguity_is_recorded_only_where_the_readings_disagree() -> None:
    """A flag on a site whose two readings agree would be noise, not a finding."""
    for site in CALL_SITES:
        if site.membership_ambiguous:
            assert site.general_tier_per_minute != LANES[site.lane].documented_per_minute, (
                f"{site.method}: flagged ambiguous but both readings agree"
            )


def test_min_interval_for_stamps_is_safe_by_the_rolling_window_rule() -> None:
    """The derivation behind every floor requirement, and the off-by-one it replaced.

    A rolling quota counts STAMPS: a caller spaced at ``i`` fires at 0, i, 2i, ... so a
    window holds ``floor(window / i) + 1`` of them, one MORE than sustained throughput.
    ``window / budget`` -- what ``conservative_min_interval_s`` returned before #2946
    step 3 -- therefore places ``budget + 1``, and the floor guard below admitted it.

    ⚠ The guarantee asserted here is the ``<=``, not an exact count. Binary rounding can
    make the returned interval land one request BELOW the target (it does at lane B:
    ``60 / 18`` rounds up, so the result places 18 and not 19). Never above.
    """

    def placed(interval_s: float) -> int:
        return int(60.0 // interval_s) + 1

    for key, lane in LANES.items():
        budget = lane.documented_per_minute
        safe = min_interval_for_stamps(lane.window_s, budget)
        assert placed(safe) <= budget, f"{key}: {safe}s places {placed(safe)} against {budget}/min"

        # The revert probe, in-file: the arithmetic this replaced is over budget by
        # exactly one at every lane, which is why it could not stay.
        naive = lane.window_s / budget
        assert placed(naive) == budget + 1, f"{key}: window/budget is expected to be over by one"

    with pytest.raises(ValueError):
        min_interval_for_stamps(60, 1)


# ---------------------------------------------------------------------------
# The floor invariant — the one test here that guards live behaviour
# ---------------------------------------------------------------------------


def test_configured_floor_is_at_least_the_conservative_minimum() -> None:
    """Each client's real floor must be >= the strictest lane it serves.

    Floors are read from the PROVIDER MODULES by name, not copied into the lane
    table, so this compares live configuration against the documented limit
    rather than the table against itself.  Lowering ``_ETORO_READ_INTERVAL_S``
    to 0.5s to "use the market-data headroom" fails here, because the broker
    read client also serves lanes D and E at 1.0s.
    """
    for (module, client_attr), sites in _sites_by_client().items():
        mod_name, const_name = FLOOR_CONSTANT_NAMES[(module, client_attr)]
        floor = getattr(importlib.import_module(mod_name), const_name)

        for site in sites:
            if (site.module, site.method) in ACCEPTED_FLOOR_EXCEPTIONS:
                continue
            required = site.conservative_min_interval_s
            assert floor >= required, (
                f"{module}::{site.method} rides {client_attr} at a {floor}s floor, but lane "
                f"{site.lane} allows {site.conservative_per_minute}/min = {required}s under the "
                f"conservative reading. Either raise {const_name} or record an explicit entry in "
                f"ACCEPTED_FLOOR_EXCEPTIONS with a reason."
            )


def test_accepted_floor_exceptions_are_real_exceptions_with_a_reason() -> None:
    """An exception that no longer violates the floor is stale — delete it.

    This keeps the exception list from becoming a place where entries accumulate
    after the underlying problem was fixed.
    """
    by_method = {(s.module, s.method): s for s in CALL_SITES}
    for key, reason in ACCEPTED_FLOOR_EXCEPTIONS.items():
        assert key in by_method, f"{key}: exception names a call site that does not exist"
        assert len(reason) > _MIN_EXCEPTION_REASON_CHARS, f"{key}: an exception needs a stated reason, not a label"

        site = by_method[key]
        mod_name, const_name = FLOOR_CONSTANT_NAMES[(site.module, site.client_attr)]
        floor = getattr(importlib.import_module(mod_name), const_name)
        assert floor < site.conservative_min_interval_s, (
            f"{key}: floor {floor}s now satisfies the {site.conservative_min_interval_s}s "
            f"conservative minimum — remove this exception."
        )


# ---------------------------------------------------------------------------
# Drift traps
# ---------------------------------------------------------------------------


def test_throttled_call_expression_counts_match_the_lane_map() -> None:
    """A new eToro endpoint changes these counts until its lane is recorded."""
    for (module, client_attr), expected in EXPRESSION_COUNTS.items():
        found = _count_self_attr_calls(module, client_attr)
        assert found == expected, (
            f"{module}: found {found} `self.{client_attr}.*()` call expressions, lane map records "
            f"{expected}. An endpoint was added, removed or renamed — assign it a quota lane in "
            f"etoro_quota_lanes.CALL_SITES and update EXPRESSION_COUNTS, then re-read the census "
            f"doc to confirm the lane is still right."
        )


def test_raw_httpx_call_counts_match_the_known_bypass_list() -> None:
    """A fifth unthrottled eToro call must not appear silently.

    Counts REQUEST expressions, not ``httpx.Client`` constructions — a third
    request issued from an existing client would otherwise pass unnoticed.

    Since #2946 step 3 item 3 every one of them must also go through
    ``issue_raw_request``, which is what makes a raw site VISIBLE (it records before
    the caller's early return and on a raised request). A direct verb call here is
    therefore a regression on two counts, so it is asserted separately.
    """
    for module, expected in RAW_HTTPX_EXPRESSION_COUNTS.items():
        direct, via_helper = _count_httpx_request_calls(module)
        assert direct == 0, (
            f"{module}: found {direct} raw httpx verb call(s) not routed through "
            f"etoro_request_log.issue_raw_request. A direct call is UNCOUNTED — it records "
            f"nothing on a non-200 early return and nothing on a transport failure."
        )
        assert via_helper == expected, (
            f"{module}: found {via_helper} raw eToro request expressions, lane map records "
            f"{expected}. Every raw call bypasses ResilientClient entirely — record it in "
            f"etoro_quota_lanes.KNOWN_UNTHROTTLED with its quota lane."
        )

    recorded = sum(RAW_HTTPX_EXPRESSION_COUNTS.values())
    assert len(KNOWN_UNTHROTTLED) == recorded, "KNOWN_UNTHROTTLED and the AST counts disagree"


def test_known_unthrottled_calls_name_known_lanes() -> None:
    for call in KNOWN_UNTHROTTLED:
        assert call.lane in LANES, f"{call.module}:{call.line_hint} -> unknown lane {call.lane!r}"
        assert call.note, "an unthrottled bypass without a note is an unexplained hole"


def test_real_env_path_drift_is_recorded_as_unknown_not_silently_fixed() -> None:
    """The portal contradicts itself on the v1 real shape, so no shape can be
    derived from documentation. These entries must stay until one informational
    call on real credentials settles them (real-env enablement, #2843/#2844).
    """
    methods = {s.method for s in CALL_SITES}
    for drift in KNOWN_PATH_DRIFT:
        assert drift.method in methods, f"{drift.method}: drift entry names no known call site"
        assert drift.documented_real_path != drift.we_build_for_real


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _sites_by_client() -> dict[tuple[str, str], list]:
    grouped: dict[tuple[str, str], list] = {}
    for site in CALL_SITES:
        grouped.setdefault((site.module, site.client_attr), []).append(site)
    assert set(grouped) == set(FLOOR_CONSTANT_NAMES), "CALL_SITES and FLOOR_CONSTANT_NAMES disagree on clients"
    return grouped


def _parse(module: str) -> ast.Module:
    return ast.parse((_REPO_ROOT / module).read_text())


def _count_self_attr_calls(module: str, client_attr: str) -> int:
    """Count ``self.<client_attr>.<verb>(...)`` call expressions."""
    total = 0
    for node in ast.walk(_parse(module)):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        receiver = node.func.value
        if (
            isinstance(receiver, ast.Attribute)
            and isinstance(receiver.value, ast.Name)
            and receiver.value.id == "self"
            and receiver.attr == client_attr
        ):
            total += 1
    return total


_HTTPX_VERBS = {"get", "post", "put", "patch", "delete", "request", "send", "stream"}


def _count_httpx_request_calls(module: str) -> tuple[int, int]:
    """Count request expressions on locals bound from an ``httpx.Client``.

    Returns ``(direct, via_helper)``.

    Deliberately narrow: it resolves the names bound by ``with httpx.Client(...)
    as <name>`` and ``<name> = httpx.Client(...)`` in the module, then counts
    verb calls on those names.

    ⚠ It used to count only ``direct`` and its docstring warned that "a helper that
    hides the client behind another layer would evade it". #2946 step 3 item 3 built
    exactly that helper, on purpose: ``etoro_request_log.issue_raw_request`` is the one
    place a raw site is guaranteed to account for its request both before the caller's
    early return and on a raised request. So the scanner now recognises
    ``issue_raw_request(<client>, ...)`` as a request expression on that client, and
    the caller asserts ``direct == 0`` -- a raw verb call reappearing in one of these
    modules is a REGRESSION, not merely an uncounted endpoint.
    """
    tree = _parse(module)
    client_names: set[str] = set()

    def _is_httpx_client(node: ast.expr) -> bool:
        return (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in {"Client", "AsyncClient"}
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "httpx"
        )

    for node in ast.walk(tree):
        if isinstance(node, ast.With | ast.AsyncWith):
            for item in node.items:
                if _is_httpx_client(item.context_expr) and isinstance(item.optional_vars, ast.Name):
                    client_names.add(item.optional_vars.id)
        elif isinstance(node, ast.Assign) and _is_httpx_client(node.value):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    client_names.add(target.id)

    assert client_names, f"{module}: no httpx client binding found — the scanner's assumption broke"

    direct = 0
    via_helper = 0
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if (
            isinstance(node.func, ast.Attribute)
            and node.func.attr in _HTTPX_VERBS
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id in client_names
        ):
            direct += 1
        elif (
            isinstance(node.func, ast.Name)
            and node.func.id == "issue_raw_request"
            and node.args
            and isinstance(node.args[0], ast.Name)
            and node.args[0].id in client_names
        ):
            via_helper += 1
    return direct, via_helper
