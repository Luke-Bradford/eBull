"""Pure contract checks for #2834 ARM A's tilt-ETF cost-bar readout.

Two jobs, and the second is the one that pays: assert ARM A's own rule, AND
assert that extracting the rule out of ``verify_2833_core_selection.py`` did not
move #2833's sealed behaviour two days before its verdict opens.
``tests/test_2833_core_selection_verdict.py`` is deliberately NOT edited for the
same reason -- it exercises the old import surface unchanged.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from scripts._core_selection_rule import (
    Eligibility,
    Observation,
    evaluate,
    load_declaration,
    verdict_mode_of,
)
from scripts.verify_2833_core_selection import DECLARATION_PATH as CORE_DECLARATION_PATH
from scripts.verify_2833_core_selection import DECLARATION_SHA256 as CORE_DECLARATION_SHA256
from scripts.verify_2834_arm_a_selection import DECLARATION_PATH, DECLARATION_SHA256

ARM_A_IDS = (14465, 15445, 15446)
_SYMBOLS = {14465: "R1VL.L", 15445: "IUMO.L", 15446: "IUQA.L"}
_AFTER_WINDOW = datetime(2026, 9, 1, tzinfo=UTC)


def _declaration() -> dict[str, object]:
    return dict(load_declaration(DECLARATION_PATH, DECLARATION_SHA256))


def _eligibility(instrument_id: int, *, proved: bool = True) -> Eligibility:
    return Eligibility(
        proof_id=instrument_id,
        instrument_id=instrument_id,
        observed_at=datetime(2026, 8, 24, tzinfo=UTC),
        verdict="underlying" if proved else "not_underlying",
        settlement_type="real" if proved else "cfd",
        direction="long",
        leverage_values=(1,) if proved else (2, 5),
        allow_open_position=proved,
        response_digest=f"digest-{instrument_id}",
    )


def _population(
    spreads: dict[int, Decimal],
    *,
    days: int = 5,
    conversion: dict[int, Decimal | None] | None = None,
) -> list[Observation]:
    rows: list[Observation] = []
    start = datetime(2026, 8, 25, 10, tzinfo=UTC)
    for day in range(days):
        for instrument_id, symbol in _SYMBOLS.items():
            for hour in range(3):
                rate = Decimal(1) if conversion is None else conversion.get(instrument_id, Decimal(1))
                rows.append(
                    Observation(
                        instrument_id=instrument_id,
                        symbol=symbol,
                        sample_bucket=start + timedelta(days=day, hours=hour),
                        status="observed",
                        spread_bps=spreads[instrument_id],
                        conversion_rate=rate,
                    )
                )
    return rows


def _evaluate(
    spreads: dict[int, Decimal],
    *,
    declaration: dict[str, object] | None = None,
    now: datetime = _AFTER_WINDOW,
    days: int = 5,
    conversion: dict[int, Decimal | None] | None = None,
    unproved: tuple[int, ...] = (),
) -> dict[str, object]:
    return dict(
        evaluate(
            _population(spreads, days=days, conversion=conversion),
            {i: _eligibility(i, proved=i not in unproved) for i in ARM_A_IDS},
            declaration or _declaration(),
            now=now,
            declaration_sha256=DECLARATION_SHA256,
        )
    )


def test_frozen_declaration_digest_is_intact() -> None:
    declaration = _declaration()
    assert declaration["schema_version"] == "core-selection-2834-arm-a-v1"
    assert declaration["issue"] == 2834
    assert declaration["pass_bar_bps"] == "50"
    assert declaration["verdict_mode"] == "per_candidate"
    assert declaration["evidence_not_before"] == "2026-08-25T00:00:00Z"
    assert sorted(declaration["candidate_ids"]) == sorted(ARM_A_IDS)  # type: ignore[arg-type]
    # The boundary is INHERITED from #2833, not minted here.
    assert (
        declaration["evidence_not_before"]
        == load_declaration(CORE_DECLARATION_PATH, CORE_DECLARATION_SHA256)["evidence_not_before"]
    )
    # Nothing here claims to supersede #2833's declaration.
    assert "supersedes_declaration_sha256" not in declaration


def test_all_three_under_the_bar_pass_and_no_winner_is_named() -> None:
    result = _evaluate({14465: Decimal("10"), 15445: Decimal("20"), 15446: Decimal("30")})
    assert result["outcome"] == "pass"
    assert result["verdict_mode"] == "per_candidate"
    assert result["passing_instrument_ids"] == sorted(ARM_A_IDS)
    assert "selected_instrument_id" not in result
    assert "selected_symbol" not in result


def test_one_candidate_over_the_bar_is_partial_and_labels_the_declared_bar() -> None:
    result = _evaluate({14465: Decimal("10"), 15445: Decimal("20"), 15446: Decimal("51")})
    assert result["outcome"] == "partial"
    assert result["passing_instrument_ids"] == [14465, 15445]
    failing = next(c for c in result["candidates"] if c["instrument_id"] == 15446)  # type: ignore[attr-defined]
    assert "cost_above_50_bps" in failing["refusals"]
    assert "cost_above_60_bps" not in failing["refusals"]


def test_every_candidate_over_the_bar_fails_with_an_empty_passing_set() -> None:
    result = _evaluate({i: Decimal("80") for i in ARM_A_IDS})
    assert result["outcome"] == "fail"
    assert result["passing_instrument_ids"] == []


def test_the_bar_is_exclusive_so_exactly_fifty_passes_and_one_bps_over_does_not() -> None:
    assert _evaluate({i: Decimal("50") for i in ARM_A_IDS})["outcome"] == "pass"
    assert _evaluate({i: Decimal("51") for i in ARM_A_IDS})["outcome"] == "fail"


def test_a_non_unit_or_missing_conversion_rate_refuses_a_cheap_candidate() -> None:
    result = _evaluate(
        {i: Decimal("1") for i in ARM_A_IDS},
        conversion={14465: Decimal("0.0136315"), 15445: None},
    )
    assert result["outcome"] == "partial"
    assert result["passing_instrument_ids"] == [15446]
    by_id = {c["instrument_id"]: c for c in result["candidates"]}  # type: ignore[attr-defined]
    # ⚠ NULL means the provider OMITTED the rate; sql/366 is explicit that it
    # does NOT mean USD, so it must refuse exactly as a GBX rate does.
    assert "fx_unmodelled" in by_id[14465]["refusals"]
    assert "fx_unmodelled" in by_id[15445]["refusals"]
    assert by_id[15446]["verdict"] == "PASS"


def test_an_unproved_product_cannot_pass_on_a_tight_spread() -> None:
    result = _evaluate({i: Decimal("1") for i in ARM_A_IDS}, unproved=(15446,))
    assert result["outcome"] == "partial"
    assert result["passing_instrument_ids"] == [14465, 15445]


def test_no_candidate_metric_is_revealed_before_five_common_dates() -> None:
    result = _evaluate({i: Decimal("1") for i in ARM_A_IDS}, days=4)
    assert result["outcome"] == "evidence_collecting"
    assert result["common_dates_observed"] == 4
    assert "candidates" not in result
    assert "passing_instrument_ids" not in result


def test_the_fifth_date_stays_sealed_until_the_following_utc_midnight() -> None:
    sealed = _evaluate({i: Decimal("1") for i in ARM_A_IDS}, now=datetime(2026, 8, 29, 23, 59, tzinfo=UTC))
    assert sealed["outcome"] == "evidence_collecting"
    assert sealed["verdict_opens_at"] == datetime(2026, 8, 30, tzinfo=UTC)
    assert "candidates" not in sealed
    opened = _evaluate({i: Decimal("1") for i in ARM_A_IDS}, now=datetime(2026, 8, 30, tzinfo=UTC))
    assert opened["outcome"] == "pass"


def test_a_sixth_common_date_does_not_displace_the_first_five() -> None:
    result = _evaluate({i: Decimal("1") for i in ARM_A_IDS}, days=6)
    assert result["window_dates"] == [
        "2026-08-25",
        "2026-08-26",
        "2026-08-27",
        "2026-08-28",
        "2026-08-29",
    ]


def test_rows_before_the_declared_boundary_are_excluded() -> None:
    rows = _population({i: Decimal("1") for i in ARM_A_IDS}, days=5)
    early = [
        Observation(
            instrument_id=i,
            symbol=_SYMBOLS[i],
            sample_bucket=datetime(2026, 8, 24, 10, tzinfo=UTC),
            status="observed",
            spread_bps=Decimal("1"),
            conversion_rate=Decimal(1),
        )
        for i in ARM_A_IDS
    ]
    result = dict(
        evaluate(
            early + rows,
            {i: _eligibility(i) for i in ARM_A_IDS},
            _declaration(),
            now=_AFTER_WINDOW,
            declaration_sha256=DECLARATION_SHA256,
        )
    )
    assert result["window_dates"][0] == "2026-08-25"


def test_both_declarations_keep_their_own_identity_in_one_process() -> None:
    """The extraction must not leak either arm's identity into the other."""
    core = load_declaration(CORE_DECLARATION_PATH, CORE_DECLARATION_SHA256)
    arm_a = load_declaration(DECLARATION_PATH, DECLARATION_SHA256)
    assert verdict_mode_of(core) == "select_one"
    assert verdict_mode_of(arm_a) == "per_candidate"
    assert core["schema_version"] != arm_a["schema_version"]
    assert core["pass_bar_bps"] == "60"
    assert arm_a["pass_bar_bps"] == "50"

    core_ids = tuple(int(v) for v in core["candidate_ids"])
    core_symbols = {3417: "SPY.RTH", 3434: "CSPX.L", 3075: "IUSA.L"}
    core_rows = [
        Observation(
            instrument_id=i,
            symbol=core_symbols[i],
            sample_bucket=datetime(2026, 8, 25, 10, tzinfo=UTC) + timedelta(days=d, hours=h),
            status="observed",
            spread_bps=Decimal("4") if i == 3417 else Decimal("2"),
            conversion_rate=Decimal(1),
        )
        for d in range(5)
        for i in core_ids
        for h in range(3)
    ]
    core_result = dict(
        evaluate(
            core_rows,
            {i: _eligibility(i) for i in core_ids},
            core,
            now=_AFTER_WINDOW,
            declaration_sha256=CORE_DECLARATION_SHA256,
        )
    )
    arm_a_result = _evaluate({i: Decimal("2") for i in ARM_A_IDS})

    assert core_result["declaration_sha256"] == CORE_DECLARATION_SHA256
    assert arm_a_result["declaration_sha256"] == DECLARATION_SHA256
    # #2833's payload shape is unchanged: a winner, and NO verdict_mode key.
    assert core_result["selected_instrument_id"] in core_ids
    assert "verdict_mode" not in core_result
    assert "passing_instrument_ids" not in core_result


def test_select_one_still_reports_cash_when_nothing_passes() -> None:
    core = load_declaration(CORE_DECLARATION_PATH, CORE_DECLARATION_SHA256)
    core_ids = tuple(int(v) for v in core["candidate_ids"])
    rows = [
        Observation(
            instrument_id=i,
            symbol=str(i),
            sample_bucket=datetime(2026, 8, 25, 10, tzinfo=UTC) + timedelta(days=d, hours=h),
            status="observed",
            spread_bps=Decimal("900"),
            conversion_rate=Decimal(1),
        )
        for d in range(5)
        for i in core_ids
        for h in range(3)
    ]
    result = dict(
        evaluate(
            rows,
            {i: _eligibility(i) for i in core_ids},
            core,
            now=_AFTER_WINDOW,
            declaration_sha256=CORE_DECLARATION_SHA256,
        )
    )
    assert result["outcome"] == "cash"
    assert result["selected_instrument_id"] is None
    failing = result["candidates"][0]  # type: ignore[index]
    assert "cost_above_60_bps" in failing["refusals"]


def test_verdict_mode_defaults_only_by_schema_identity() -> None:
    assert verdict_mode_of({"schema_version": "core-selection-2833-v1"}) == "select_one"
    with pytest.raises(RuntimeError, match="must state verdict_mode"):
        verdict_mode_of({"schema_version": "something-else-v1"})
    with pytest.raises(RuntimeError, match="unrecognised verdict_mode"):
        verdict_mode_of({"schema_version": "x", "verdict_mode": "select_two"})


def test_a_declaration_stating_a_rule_the_code_does_not_implement_is_refused(tmp_path) -> None:
    import json

    payload = _declaration()
    payload["fx_rule"] = "ignore the conversion rate"
    path = tmp_path / "tampered.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    import hashlib

    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    with pytest.raises(RuntimeError, match="does not implement"):
        load_declaration(path, digest)


def test_an_empty_candidate_set_is_refused_rather_than_passing_vacuously(tmp_path) -> None:
    import hashlib
    import json

    payload = _declaration()
    payload["candidate_ids"] = []
    path = tmp_path / "empty.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    with pytest.raises(RuntimeError, match="no candidate_ids"):
        load_declaration(path, digest)
