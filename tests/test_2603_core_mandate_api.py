"""The core/cash mandate endpoints (#2603 item 3's entry condition).

Pure-tier by design. ``configure_core_mandate``'s own refusals — the eligibility
gate, the material-change rule, the percentage invariants — are already exercised
against a real Postgres in ``tests/test_2603_core_mandate_db.py`` and
``tests/test_2603_core_eligibility_db.py``. Re-asserting them through HTTP would
buy nothing and cost a database.

What is genuinely NEW here is the wiring, and each of these three is a thing the
service tests cannot see:

* the route table (a static path shadowed by a path parameter),
* the response mapping (including a derived field with no stored column),
* which auth dependency the mutating route carries.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from fastapi.routing import APIRoute

from app.api.strategies import CoreMandateUpdateRequest, _core_mandate_response, router
from app.services.broker_credentials import (
    CredentialValidationError,
    normalise_environment,
    normalise_provider,
)
from app.services.strategy_core_mandate import CoreMandate

CORE_MANDATE_PATH = "/strategies/core-mandate"


def _routes() -> list[APIRoute]:
    return [route for route in router.routes if isinstance(route, APIRoute)]


def _mandate(**overrides: object) -> CoreMandate:
    values: dict[str, object] = {
        "event_id": 7,
        "revision": 3,
        "enabled": True,
        "base_currency": "USD",
        "core_instrument_id": 42,
        "core_target_pct": Decimal("60"),
        "liquidity_reserve_pct": Decimal("20"),
        "rebalance_band_pct": Decimal("5"),
        "min_rebalance_amount": Decimal("25"),
        "policy_version": "core-mandate-v1",
    }
    values.update(overrides)
    return CoreMandate(**values)  # type: ignore[arg-type]


class TestTheRouteTable:
    def test_the_static_path_is_declared_before_every_strategy_id_route(self) -> None:
        """⚠⚠ FastAPI matches in DECLARATION ORDER, so a `/{strategy_id}` route
        declared first would swallow `/strategies/core-mandate` with
        `strategy_id="core-mandate"`. The failure surfaces as a 404 or a
        "strategy not found", which reads as a missing record rather than a
        routing bug — so nobody looks at the route table.

        Asserted as an ORDER, not as "the route exists": the route existing is
        what a reader checks and is exactly what stays true when this breaks."""
        paths = [route.path for route in _routes()]
        assert CORE_MANDATE_PATH in paths
        first_param_route = next(
            (index for index, path in enumerate(paths) if "{strategy_id}" in path),
            len(paths),
        )
        # ⚠ THE ANTI-VACUITY ASSERT, and it is the load-bearing one. With no
        # `{strategy_id}` route on the router at all, `first_param_route` falls
        # back to `len(paths)` and the comparison below holds for ANY placement
        # — the test would go permanently green while guarding nothing. Pin that
        # the hazard still exists before asserting it is avoided.
        assert first_param_route < len(paths), "no {strategy_id} route — this test can no longer fail"
        assert paths.index(CORE_MANDATE_PATH) < first_param_route

    def test_both_verbs_are_registered_on_the_one_path(self) -> None:
        """A read that 405s is a config surface an operator cannot inspect
        before writing to it."""
        methods = {method for route in _routes() if route.path == CORE_MANDATE_PATH for method in route.methods}
        assert {"GET", "PUT"} <= methods


class TestTheAuthDependency:
    def test_the_mutating_route_requires_a_session_not_a_service_token(self) -> None:
        """⚠ The router's default is `require_session_or_service_token`. A mandate
        revision is an operator authorisation: it is stored with a named
        `changed_by`, and `configure_core_mandate` needs a real `operator_id` to
        select the right per-account eligibility proof. A service token has
        neither, so the PUT overrides the router default.

        Asserted on the dependency NAMES rather than on behaviour, because the
        defect this guards is someone deleting the override — which changes no
        test that exercises the happy path as an operator."""
        put = next(route for route in _routes() if route.path == CORE_MANDATE_PATH and "PUT" in route.methods)
        names = {dependency.call.__name__ for dependency in put.dependant.dependencies if dependency.call is not None}
        assert "require_session" in names


class TestTheAccountSelector:
    def test_an_unsupported_provider_is_rejected_before_any_transaction_opens(self) -> None:
        """⚠⚠ Codex checkpoint 2 (PR for #2603). `normalise_provider` raises
        `CredentialValidationError` on an unrecognised value, which the endpoint
        did not catch — so a typo'd account name returned a **500** for ordinary
        malformed input.

        Two things are pinned, not one. That it raises at all is the shape the
        endpoint must handle; that it raises from `normalise_*` rather than from
        `configure_core_mandate` is why it maps to 400 and not to the 409 the
        mandate refusals use. A 409 would tell the operator "the mandate was
        refused" for what is a spelling mistake in the account selector."""
        for bad in ("not-a-broker", ""):
            with pytest.raises(CredentialValidationError):
                normalise_provider(bad)

    def test_case_and_whitespace_are_tolerated_rather_than_refused(self) -> None:
        """⚠ Written after asserting the opposite and being wrong.
        `normalise_provider` is `raw.strip().lower()` before the membership test,
        so `"ETORO "` is ACCEPTED. That matters to the endpoint: it is why the
        400 above is narrow — reserved for a value that is genuinely not a
        provider — rather than a trap an operator hits by pasting a name with a
        trailing space."""
        assert normalise_provider("ETORO ") == "etoro"
        assert normalise_environment(" Demo") == "demo"

    def test_an_unsupported_environment_is_rejected_the_same_way(self) -> None:
        with pytest.raises(CredentialValidationError):
            normalise_environment("production")

    def test_the_defaults_normalise_rather_than_raise(self) -> None:
        """The request model's defaults must be values the normalisers accept,
        or every request omitting them 400s — a default that cannot survive its
        own validator is worse than a required field."""
        defaults = CoreMandateUpdateRequest.model_fields
        assert normalise_provider(str(defaults["provider"].default)) == "etoro"
        assert normalise_environment(str(defaults["environment"].default)) == "demo"


class TestTheResponseMapping:
    def test_an_unconfigured_mandate_is_a_200_not_an_absence(self) -> None:
        """⚠ "Never configured" is where every install starts, and it is where
        this tree stood until the endpoint existed. It must be distinguishable
        from a broken lookup, which a 404 would conflate it with."""
        response = _core_mandate_response(None)
        assert response.configured is False
        assert response.revision is None
        assert response.core_target_pct is None

    def test_every_stored_field_reaches_the_response(self) -> None:
        """⚠ Listed explicitly rather than derived from the dataclass — deriving
        them would pass for however many fields happen to exist, which is the
        bug (a field added to `CoreMandate` and not to the response) rather than
        the check."""
        response = _core_mandate_response(_mandate())
        assert (
            response.configured,
            response.event_id,
            response.revision,
            response.enabled,
            response.base_currency,
            response.core_instrument_id,
            response.core_target_pct,
            response.liquidity_reserve_pct,
            response.rebalance_band_pct,
            response.min_rebalance_amount,
            response.policy_version,
        ) == (True, 7, 3, True, "USD", 42, Decimal("60"), Decimal("20"), Decimal("5"), Decimal("25"), "core-mandate-v1")

    def test_cash_target_is_the_services_complement_and_not_recomputed(self) -> None:
        """⚠⚠ Cash has no stored column — it is `PERCENT_BASIS - core_target_pct`,
        owned by `CoreMandate.cash_target_pct`. The response must READ that
        property rather than subtract for itself, or the API acquires a second
        definition of the same quantity against a basis nobody re-checks."""
        mandate = _mandate(core_target_pct=Decimal("60"))
        assert _core_mandate_response(mandate).cash_target_pct == mandate.cash_target_pct

    def test_a_disabled_mandate_may_still_name_an_instrument(self) -> None:
        """The `strategy_core_mandate_enabled_has_instrument` invariant is
        one-directional, so the response must not treat a named instrument on a
        disabled mandate as a contradiction to be normalised away."""
        response = _core_mandate_response(_mandate(enabled=False, core_instrument_id=42))
        assert (response.enabled, response.core_instrument_id) == (False, 42)
