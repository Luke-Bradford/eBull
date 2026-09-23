"""#2942 attended recommendation window-B release — the pure halves.

The EXIT witness, the shared-evaluator hardening (#2961's evaluator, tightened in
place), the payload-reference scan, the digest and the wait. The DB-backed act is in
``tests/test_2942_recommendation_window_b_release_db.py``.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from app.services.order_client import unexpected_exception_park_payload
from app.services.recommendation_window_b_release import (
    RECOMMENDATION_WINDOW_B_RULE_VERSION,
    evaluate_recommendation_exit_witness,
    payload_digest,
    payload_reference_refusal,
)
from app.services.strategy_core_window_b_release import (
    WINDOW_B_RELEASE_RULE_VERSION,
    WINDOW_B_VISIBILITY_WAIT,
    evaluate_window_b_witness,
    window_b_wait_remaining,
)

IID = 4242
LOT = 3308442058
_NB = datetime(2026, 9, 23, 12, 0, tzinfo=UTC)
_OBS = _NB + timedelta(seconds=1)


def _lot(**overrides: Any) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "positionID": LOT,
        "instrumentID": IID,
        "isBuy": True,
        "mirrorID": 0,
        "units": 10.5,
        "openDateTime": "2026-09-01T10:00:00Z",
    }
    entry.update(overrides)
    return {key: value for key, value in entry.items() if value is not _DROP}


_DROP = object()


def _pnl(positions: list[Any], orders_for_open: list[Any] | None = None, orders: list[Any] | None = None) -> Any:
    return {"clientPortfolio": {"positions": positions, "ordersForOpen": orders_for_open or [], "orders": orders or []}}


def _exit(raw: Any, *, units: str = "10.5", observed_at: Any = _OBS) -> tuple[str, ...]:
    return evaluate_recommendation_exit_witness(
        raw,
        instrument_id=IID,
        position_id=LOT,
        recorded_units=Decimal(units),
        observed_at=observed_at,
        not_before=_NB,
    ).refusals


class TestExitWitness:
    def test_the_lot_present_and_unchanged_passes(self) -> None:
        assert _exit(_pnl([_lot(), _lot(positionID=77, instrumentID=9)])) == ()

    def test_the_lot_absent_refuses_because_the_close_may_have_landed(self) -> None:
        assert _exit(_pnl([_lot(positionID=77)])) == ("recommendation_release_exit_lot_gone",)

    def test_units_differing_at_the_eighth_decimal_refuse(self) -> None:
        raw = _pnl([_lot(units="10.50000001")])
        assert _exit(raw, units="10.5") == ("recommendation_release_exit_lot_changed",)

    def test_trailing_zeros_past_eight_places_still_compare_exactly(self) -> None:
        assert _exit(_pnl([_lot(units="1.000000000")]), units="1.00000000") == ()

    @pytest.mark.parametrize("units", [True, "NaN", "Infinity", 0, -1, "0.000000001", None, [1], _DROP], ids=repr)
    def test_invalid_units_are_malformed(self, units: Any) -> None:
        assert _exit(_pnl([_lot(units=units)])) == ("window_b_witness_malformed",)

    @pytest.mark.parametrize("is_buy", [_DROP, None, "true", 1, False], ids=repr)
    def test_is_buy_must_be_exactly_true(self, is_buy: Any) -> None:
        assert _exit(_pnl([_lot(isBuy=is_buy)])) == ("recommendation_release_exit_lot_identity_changed",)

    def test_the_lot_as_a_mirror_refuses(self) -> None:
        assert _exit(_pnl([_lot(mirrorID=5)])) == ("recommendation_release_exit_lot_identity_changed",)

    def test_a_duplicate_position_id_is_malformed(self) -> None:
        assert _exit(_pnl([_lot(), _lot()])) == ("window_b_witness_malformed",)

    def test_the_lot_on_another_instrument_is_malformed(self) -> None:
        assert _exit(_pnl([_lot(instrumentID=9)])) == ("window_b_witness_malformed",)

    def test_a_pending_order_on_the_instrument_refuses(self) -> None:
        raw = _pnl([_lot()], orders=[{"instrumentID": IID}])
        assert _exit(raw) == ("window_b_witness_pending_order",)
        raw = _pnl([_lot()], orders_for_open=[{"instrumentID": IID, "mirrorID": 0}])
        assert _exit(raw) == ("window_b_witness_pending_order",)

    def test_a_malformed_entry_anywhere_in_positions_refuses(self) -> None:
        assert "window_b_witness_malformed" in _exit(_pnl([_lot(), "x"]))
        assert "window_b_witness_malformed" in _exit(_pnl([_lot(), {"instrumentID": 9}]))

    def test_a_missing_array_is_incomplete(self) -> None:
        assert _exit({"clientPortfolio": {"positions": [_lot()], "orders": []}}) == ("window_b_witness_incomplete",)

    def test_a_read_before_the_deadline_refuses(self) -> None:
        assert "window_b_witness_before_deadline" in _exit(_pnl([_lot()]), observed_at=_NB - timedelta(seconds=1))

    @pytest.mark.parametrize("observed_at", [datetime(2026, 9, 23, 13, 0), "2026-09-23T13:00:00Z"], ids=repr)
    def test_a_naive_or_non_datetime_observation_refuses(self, observed_at: Any) -> None:
        assert "window_b_witness_observed_at_invalid" in _exit(_pnl([_lot()]), observed_at=observed_at)


class TestSharedEvaluatorHardening:
    """Each change is strictly tighter, which is why core bumped to v2."""

    def _open(self, positions: list[Any], observed_at: Any = _OBS) -> tuple[str, ...]:
        return evaluate_window_b_witness(
            _pnl(positions),
            instrument_id=IID,
            authority_created_at=datetime(2026, 9, 20, tzinfo=UTC),
            observed_at=observed_at,
            not_before=_NB,
        ).refusals

    def test_a_clean_prior_position_passes(self) -> None:
        assert self._open([_lot()]) == ()

    @pytest.mark.parametrize(
        "overrides",
        [
            {"mirrorID": -1},
            {"positionID": 0},
            {"positionID": -5},
            {"instrumentId": IID + 1},  # conflicting instrument aliases
            {"positionId": LOT + 1},  # conflicting position aliases
            {"mirrorId": 3},  # conflicting mirror aliases
            {"positionID": True},
            {"positionID": 1.0},
            {"mirrorID": None},
            {"positionId": None},  # a present null alongside a valid alternate alias
            {"instrumentId": None},
        ],
        ids=repr,
    )
    def test_malformed_ids_refuse(self, overrides: dict[str, Any]) -> None:
        assert self._open([_lot(**overrides)]) == ("window_b_witness_malformed",)

    @pytest.mark.parametrize("observed_at", [datetime(2026, 9, 23, 13, 0), None], ids=repr)
    def test_a_naive_or_non_datetime_observation_refuses(self, observed_at: Any) -> None:
        assert "window_b_witness_observed_at_invalid" in self._open([_lot()], observed_at=observed_at)

    def test_the_recommendation_version_is_derived_from_core(self) -> None:
        assert WINDOW_B_RELEASE_RULE_VERSION == "core-window-b-v2"
        assert RECOMMENDATION_WINDOW_B_RULE_VERSION == "recommendation-window-b-v1+core-window-b-v2"


class TestPayloadReferenceScan:
    @pytest.mark.parametrize(
        "payload",
        [
            {"orderID": 1},
            {"positionId": 2},
            {"order_id": 3},
            {"detail": {"nested": [{"OrderId": 1.5}]}},
            [{"x": "the positionID was 9"}],
            {"body": json.dumps({"orderForOpen": {"orderID": 4}})},
            {"body": json.dumps(json.dumps({"orderId": 5}))},  # doubly serialised
            {"body": '{"a": 1, "orderID": 6, "orderID": null}'},  # duplicate key in a serialised body
            {"text": "\\u006frderID=7"},  # escaped key inside prose
            {"repr": {"repr_truncated": True}},
        ],
        ids=repr,
    )
    def test_a_reference_in_the_payload_refuses(self, payload: Any) -> None:
        assert payload_reference_refusal(payload, "timeout") == "recommendation_release_payload_has_ref"

    def test_a_reference_only_in_the_park_message_refuses(self) -> None:
        assert payload_reference_refusal({"status": 504}, "504 body: {'orderID': 1}") is not None

    def test_a_clean_payload_and_ordinary_exception_text_pass(self) -> None:
        payload = unexpected_exception_park_payload(ConnectionError("Connection reset by peer"))
        assert payload_reference_refusal(payload, "Connection reset by peer") is None
        assert payload_reference_refusal({"status_code": 502, "body": "Bad Gateway"}, "HTTP 502") is None

    def test_the_digest_covers_the_payload_and_the_message(self) -> None:
        base = payload_digest({"a": 1}, "m")
        assert base == payload_digest({"a": 1}, "m")
        assert base != payload_digest({"a": 2}, "m")
        assert base != payload_digest({"a": 1}, "n")


class TestUnexpectedExceptionPark:
    def test_str_repr_and_raw_payload_are_kept(self) -> None:
        exc = ValueError("boom")
        exc.raw_payload = {"x": 1}  # type: ignore[attr-defined]
        payload = unexpected_exception_park_payload(exc)
        assert payload == {
            "exception": "ValueError",
            "repr": "ValueError('boom')",
            "str": "boom",
            "raw_payload": {"x": 1},
        }

    def test_a_repr_over_64_kib_is_replaced_by_the_refused_marker(self) -> None:
        payload = unexpected_exception_park_payload(ValueError("x" * (64 * 1024)))
        assert payload["repr"] == {"repr_truncated": True}
        assert payload_reference_refusal(payload, "x") == "recommendation_release_payload_has_ref"


class TestWaitFromParkObservation:
    @pytest.mark.parametrize("parked_offset", [timedelta(hours=-5), timedelta(minutes=5)], ids=["long_past", "future"])
    def test_the_monotonic_leg_always_waits_t_after_the_observation(self, parked_offset: timedelta) -> None:
        now = datetime(2026, 9, 23, 12, 0, tzinfo=UTC)
        remaining = window_b_wait_remaining(
            dead_monotonic=100.0, now_monotonic=100.0, entered_at=now + parked_offset, now_wall=now
        )
        assert remaining >= WINDOW_B_VISIBILITY_WAIT.total_seconds()


class TestCli:
    """The script's three verdicts and exit codes (spec, "The surface is a script")."""

    @pytest.mark.parametrize(
        ("passed", "slug", "code", "verdict"),
        [
            (True, "recommendation_released_attended", 0, "RELEASED"),
            (True, "recommendation_release_already_released", 0, "RELEASED"),
            (False, "window_b_sender_alive", 1, "REFUSED window_b_sender_alive"),
            (False, "recommendation_release_outcome_indeterminate", 2, "INDETERMINATE"),
        ],
    )
    def test_verdicts_and_exit_codes(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
        passed: bool,
        slug: str,
        code: int,
        verdict: str,
    ) -> None:
        from unittest.mock import MagicMock

        from app.services.strategy_core_window_b_release import WindowBReleaseResult
        from scripts import release_recommendation_window_b as cli

        conn = MagicMock()
        monkeypatch.setattr(cli.psycopg, "connect", lambda *_a, **_k: conn)
        monkeypatch.setattr(
            cli,
            "release_recommendation_window_b",
            lambda *_a, **_k: WindowBReleaseResult(7, passed, slug, None, {"order_id": 7}),
        )
        args = ["--order-id", "7", "--operator-id", "op", "--payload-sha256", "a" * 64, "--attestation", "why"]
        assert cli.main(args) == code
        assert json.loads(capsys.readouterr().out)["verdict"] == verdict
        conn.close.assert_called_once()

    def test_a_release_without_the_digest_is_a_usage_error(self) -> None:
        from scripts import release_recommendation_window_b as cli

        with pytest.raises(SystemExit):
            cli.main(["--order-id", "7", "--operator-id", "op", "--attestation", "why"])
