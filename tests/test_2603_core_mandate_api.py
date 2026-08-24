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

import inspect
from contextlib import nullcontext
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException
from fastapi.params import Depends as DependsParam
from fastapi.routing import APIRoute

from app.api.strategies import (
    CoreMandateUpdateRequest,
    _core_mandate_response,
    read_core_sleeve,
    rebalance_core_sleeve,
    router,
    update_core_mandate,
)
from app.services.broker_credentials import (
    CredentialValidationError,
    LoadedCredential,
    normalise_environment,
    normalise_provider,
)
from app.services.strategy_core_executor import CoreExecutionResult, CoreResumeAuthority
from app.services.strategy_core_mandate import CoreMandate
from app.services.strategy_core_selection import CoreCandidateCoverage, CoreSelection


def _capital_authority(
    *,
    active_position_ids: tuple[int, ...] = (),
    alpha_committed: Decimal = Decimal("0"),
) -> SimpleNamespace:
    return SimpleNamespace(
        enabled=True,
        capital_limit=Decimal("1000"),
        capital_mode="fixed",
        realised_delta=Decimal("0"),
        alpha_committed=alpha_committed,
        core_pending_committed=Decimal("0"),
        core_active_recorded_committed=Decimal("0"),
        core_active_position_ids=active_position_ids,
    )


CORE_MANDATE_PATH = "/strategies/core-mandate"
CORE_SLEEVE_PATH = "/strategies/core-sleeve"
CORE_REBALANCE_PATH = "/strategies/core-sleeve/rebalance"


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
        "policy_version": "core-mandate-v2",
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

    def test_the_operator_sleeve_view_is_registered_before_strategy_id_routes(self) -> None:
        paths = [route.path for route in _routes()]
        first_param_route = next(index for index, path in enumerate(paths) if "{strategy_id}" in path)
        assert paths.index(CORE_SLEEVE_PATH) < first_param_route
        assert paths.index(CORE_REBALANCE_PATH) < first_param_route

    def test_the_attended_rebalance_is_post_only(self) -> None:
        methods = {method for route in _routes() if route.path == CORE_REBALANCE_PATH for method in route.methods}
        assert methods == {"POST"}


def test_collecting_state_reports_cash_and_server_derived_coverage(monkeypatch: pytest.MonkeyPatch) -> None:
    selection = CoreSelection(
        state="evidence_collecting",
        selected_instrument_id=None,
        selected_symbol=None,
        evidence_ref=None,
        required_trading_days=5,
        observed_trading_days=1,
        max_cost_bps=60,
        candidates=(
            CoreCandidateCoverage(3417, "SPY.RTH", 1, None, None),
            CoreCandidateCoverage(3434, "CSPX.L", 1, None, None),
            CoreCandidateCoverage(3075, "IUSA.L", 1, None, None),
        ),
        missing_candidate_ids=(),
        configuration_error=None,
    )
    monkeypatch.setattr("app.api.strategies.load_core_selection", lambda _conn: selection)
    monkeypatch.setattr("app.api.strategies.load_core_mandate", lambda _conn: None)
    monkeypatch.setattr("app.api.strategies.load_core_resume_authority", lambda _conn: None)
    monkeypatch.setattr("app.api.strategies.load_engine_capital_authority", lambda _conn: None)
    response = read_core_sleeve(cast(Any, MagicMock()))
    assert response.state == "evidence_collecting"
    assert response.selected_instrument_id is None
    assert response.observed_trading_days == 1
    assert response.earliest_possible_verdict_at == datetime(2026, 9, 2, tzinfo=UTC)
    assert response.can_configure is False
    assert response.can_enable_pool is False
    assert response.can_rebalance is False
    assert response.can_resume is False
    assert response.execution_action == "blocked"
    assert [blocker.code for blocker in response.blockers] == [
        "core_paper_pool_unconfigured",
        "core_evidence_collecting",
        "core_mandate_unconfigured",
    ]


def test_rebalance_passes_the_exact_loaded_credential_ids_to_the_executor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from uuid import UUID

    api = LoadedCredential(UUID("ba39f751-d4bd-4553-ab25-d9acbb73fbe8"), "api-secret")
    user = LoadedCredential(UUID("f7306e0b-9494-415e-85fd-97874510cc83"), "user-secret")
    monkeypatch.setattr("app.api.strategies.ensure_broker_key_loaded", lambda _conn: True)
    monkeypatch.setattr("app.api.strategies.load_core_resume_authority", lambda _conn: None)
    monkeypatch.setattr(
        "app.api.strategies.load_credential_with_id_for_provider_use",
        MagicMock(side_effect=[api, user]),
    )
    execute = MagicMock(return_value=CoreExecutionResult("held", "within_band", 11, None, None, Decimal("0")))
    monkeypatch.setattr("app.api.strategies.execute_core_rebalance", execute)

    class BrokerContext:
        def __enter__(self) -> object:
            return object()

        def __exit__(self, *_args: object) -> None:
            return None

    monkeypatch.setattr("app.api.strategies.EtoroBrokerProvider", lambda **_kwargs: BrokerContext())
    conn = MagicMock()
    conn.transaction.return_value = nullcontext()
    request = MagicMock()
    request.app.state.audit_pool = None
    session = MagicMock(operator_id=UUID("73d8ad78-3062-4ef5-8f0a-7428865e23d7"), username="operator")

    response = rebalance_core_sleeve(request=request, session=session, conn=cast(Any, conn))

    assert response.state == "held"
    assert response.submission_policy_version == "core-submission-v1"
    assert response.preflight_policy_version == "core-preflight-v2"
    assert response.broker_preflight_policy_version == "core-broker-preflight-v2"
    assert execute.call_args.kwargs["api_key_credential_id"] == api.id
    assert execute.call_args.kwargs["user_key_credential_id"] == user.id


def test_operator_view_labels_an_unresolved_order_as_resume_not_rebalance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from uuid import UUID

    selection = CoreSelection(
        state="ready",
        selected_instrument_id=3417,
        selected_symbol="SPY.RTH",
        evidence_ref="#2833 verdict",
        required_trading_days=5,
        observed_trading_days=5,
        max_cost_bps=60,
        candidates=(),
        missing_candidate_ids=(),
        configuration_error=None,
    )
    authority = CoreResumeAuthority(
        intent_id=11,
        trade_id=21,
        order_id=31,
        instrument_id=3417,
        amount=Decimal("49.9"),
        request_id=UUID("bd779053-d550-4bb4-9f8d-f3b2fa5633ac"),
        broker_order_ref=None,
        eligibility_proof_id=7,
        operator_id=UUID("73d8ad78-3062-4ef5-8f0a-7428865e23d7"),
        api_key_credential_id=UUID("ba39f751-d4bd-4553-ab25-d9acbb73fbe8"),
        user_key_credential_id=UUID("f7306e0b-9494-415e-85fd-97874510cc83"),
    )
    monkeypatch.setattr("app.api.strategies.load_core_selection", lambda _conn: selection)
    monkeypatch.setattr("app.api.strategies.load_core_mandate", lambda _conn: _mandate(core_instrument_id=3417))
    monkeypatch.setattr("app.api.strategies.load_core_resume_authority", lambda _conn: authority)
    monkeypatch.setattr("app.api.strategies.load_engine_capital_authority", lambda _conn: _capital_authority())
    monkeypatch.setattr("app.api.strategies.settings.etoro_env", "demo")

    response = read_core_sleeve(cast(Any, MagicMock()))

    assert response.can_rebalance is False
    assert response.can_enable_pool is True
    assert response.can_resume is True
    assert response.execution_action == "resume"
    assert response.pending_order_id == 31
    assert [blocker.code for blocker in response.blockers] == ["core_order_unresolved"]


@pytest.mark.parametrize(
    ("capital_authority", "expected_blocker"),
    [
        (_capital_authority(active_position_ids=(99,)), "core_live_snapshot_required"),
        (_capital_authority(alpha_committed=Decimal("1000")), "core_sandbox_exceeded"),
    ],
)
def test_operator_view_does_not_advertise_unavailable_core_headroom(
    monkeypatch: pytest.MonkeyPatch,
    capital_authority: SimpleNamespace,
    expected_blocker: str,
) -> None:
    selection = CoreSelection(
        state="ready",
        selected_instrument_id=3417,
        selected_symbol="SPY.RTH",
        evidence_ref="#2833 verdict",
        required_trading_days=5,
        observed_trading_days=5,
        max_cost_bps=60,
        candidates=(),
        missing_candidate_ids=(),
        configuration_error=None,
    )
    monkeypatch.setattr("app.api.strategies.load_core_selection", lambda _conn: selection)
    monkeypatch.setattr("app.api.strategies.load_core_mandate", lambda _conn: _mandate(core_instrument_id=3417))
    monkeypatch.setattr("app.api.strategies.load_core_resume_authority", lambda _conn: None)
    monkeypatch.setattr(
        "app.api.strategies.load_engine_capital_authority",
        lambda _conn: capital_authority,
    )
    monkeypatch.setattr("app.api.strategies.settings.etoro_env", "demo")

    response = read_core_sleeve(cast(Any, MagicMock()))

    assert response.can_enable_pool is True
    assert response.can_rebalance is False
    assert response.execution_action == "blocked"
    assert [blocker.code for blocker in response.blockers] == [expected_blocker]


def test_operator_view_refuses_a_mandate_for_the_previous_reviewed_selection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    selection = CoreSelection(
        state="ready",
        selected_instrument_id=3434,
        selected_symbol="CSPX.L",
        evidence_ref="#2833 revised verdict",
        required_trading_days=5,
        observed_trading_days=5,
        max_cost_bps=60,
        candidates=(),
        missing_candidate_ids=(),
        configuration_error=None,
    )
    monkeypatch.setattr("app.api.strategies.load_core_selection", lambda _conn: selection)
    monkeypatch.setattr("app.api.strategies.load_core_mandate", lambda _conn: _mandate(core_instrument_id=3417))
    monkeypatch.setattr("app.api.strategies.load_core_resume_authority", lambda _conn: None)
    monkeypatch.setattr("app.api.strategies.load_engine_capital_authority", lambda _conn: _capital_authority())
    monkeypatch.setattr("app.api.strategies.settings.etoro_env", "demo")

    response = read_core_sleeve(cast(Any, MagicMock()))

    assert response.can_enable_pool is False
    assert response.can_rebalance is False
    assert response.execution_action == "blocked"
    assert [blocker.code for blocker in response.blockers] == ["core_mandate_selection_mismatch"]


def test_operator_view_refuses_a_superseded_mandate_policy(monkeypatch: pytest.MonkeyPatch) -> None:
    selection = CoreSelection(
        state="ready",
        selected_instrument_id=3417,
        selected_symbol="SPY.RTH",
        evidence_ref="#2833 verdict",
        required_trading_days=5,
        observed_trading_days=5,
        max_cost_bps=60,
        candidates=(),
        missing_candidate_ids=(),
        configuration_error=None,
    )
    monkeypatch.setattr("app.api.strategies.load_core_selection", lambda _conn: selection)
    monkeypatch.setattr(
        "app.api.strategies.load_core_mandate",
        lambda _conn: _mandate(core_instrument_id=3417, policy_version="core-mandate-v1"),
    )
    monkeypatch.setattr("app.api.strategies.load_core_resume_authority", lambda _conn: None)
    monkeypatch.setattr("app.api.strategies.load_engine_capital_authority", lambda _conn: _capital_authority())
    monkeypatch.setattr("app.api.strategies.settings.etoro_env", "demo")

    response = read_core_sleeve(cast(Any, MagicMock()))

    assert response.can_enable_pool is False
    assert response.can_rebalance is False
    assert [blocker.code for blocker in response.blockers] == ["core_mandate_policy_unsupported"]


def test_resume_refuses_credentials_from_a_different_account(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from uuid import UUID

    authority = CoreResumeAuthority(
        intent_id=11,
        trade_id=21,
        order_id=31,
        instrument_id=3417,
        amount=Decimal("49.9"),
        request_id=UUID("bd779053-d550-4bb4-9f8d-f3b2fa5633ac"),
        broker_order_ref=None,
        eligibility_proof_id=7,
        operator_id=UUID("73d8ad78-3062-4ef5-8f0a-7428865e23d7"),
        api_key_credential_id=UUID("ba39f751-d4bd-4553-ab25-d9acbb73fbe8"),
        user_key_credential_id=UUID("f7306e0b-9494-415e-85fd-97874510cc83"),
    )
    replacement = LoadedCredential(UUID("c05aa4fd-984b-47d0-a823-3728d95041fb"), "replacement")
    monkeypatch.setattr("app.api.strategies.ensure_broker_key_loaded", lambda _conn: True)
    monkeypatch.setattr("app.api.strategies.load_core_resume_authority", lambda _conn: authority)
    monkeypatch.setattr(
        "app.api.strategies.load_credential_with_id_for_provider_use",
        MagicMock(return_value=replacement),
    )
    broker_factory = MagicMock()
    monkeypatch.setattr("app.api.strategies.EtoroBrokerProvider", broker_factory)
    conn = MagicMock()
    conn.transaction.return_value = nullcontext()
    request = MagicMock()
    request.app.state.audit_pool = None
    session = MagicMock(operator_id=UUID("73d8ad78-3062-4ef5-8f0a-7428865e23d7"), username="operator")

    with pytest.raises(HTTPException) as raised:
        rebalance_core_sleeve(request=request, session=session, conn=cast(Any, conn))

    assert raised.value.status_code == 409
    assert "different account" in str(raised.value.detail)
    broker_factory.assert_not_called()


class TestTheAuthDependency:
    def test_the_mutating_route_requires_a_session_not_a_service_token(self) -> None:
        """⚠ The router's default is `require_session_or_service_token`. A mandate
        revision is an operator authorisation: it is stored with a named
        `changed_by`, and `configure_core_mandate` needs a real `operator_id` to
        select the right per-account eligibility proof. A service token has
        neither, so the PUT overrides the router default.

        Asserted on the dependency NAMES rather than on behaviour, because the
        defect this guards is someone deleting the override — which changes no
        test that exercises the happy path as an operator.

        ⚠ READ OFF THE ENDPOINT'S OWN SIGNATURE, not off `route.dependant`
        (review NITPICK, PR #2702). `Depends.dependency` is FastAPI's public
        surface; the resolved dependant tree is an internal it is free to
        reshape. It is also the more precise question: the dependant tree
        contains BOTH `require_session_or_service_token` (inherited from the
        router) and `require_session`, because both run — measured, not assumed.
        The security claim rests on the STRICTER one being present, since it
        401s a caller with no session whatever the router-level one allowed.

        ⚠ On the failure direction: if FastAPI ever composed these differently,
        `require_session` would go MISSING from the set and this test would
        fail. That is the safe direction — a false fail, which gets triaged, not
        a false pass, which does not. The anti-vacuity assert below makes it so
        explicitly rather than by luck."""
        signature = inspect.signature(update_core_mandate)
        declared = {
            parameter.default.dependency.__name__
            for parameter in signature.parameters.values()
            if isinstance(parameter.default, DependsParam) and parameter.default.dependency is not None
        }
        assert declared, "no Depends markers on the endpoint — this test can no longer fail"
        assert "require_session" in declared

    def test_the_rebalance_route_requires_a_named_session(self) -> None:
        signature = inspect.signature(rebalance_core_sleeve)
        declared = {
            parameter.default.dependency.__name__
            for parameter in signature.parameters.values()
            if isinstance(parameter.default, DependsParam) and parameter.default.dependency is not None
        }
        assert declared
        assert "require_session" in declared


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
        ) == (True, 7, 3, True, "USD", 42, Decimal("60"), Decimal("20"), Decimal("5"), Decimal("25"), "core-mandate-v2")

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
