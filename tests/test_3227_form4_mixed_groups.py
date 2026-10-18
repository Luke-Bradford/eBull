"""#3227 — a Form 4's holding lines fold into the same filer's transaction observation.

Spec: docs/specs/ownership/2026-10-18-3227-form4-mixed-groups.md (Acceptance 1). Pure tests of
``form4_fold_shares``, the one helper both Form 4 observation writers call for a mixed key."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal

from app.services.insider_transactions import (
    FORM4_FOLD_NOT_MIXED,
    FORM4_VEHICLE_SUMS,
    form4_fold_shares,
    form4_holding_observations,
    form4_observation_shares,
)

_CIK = "0001"


@dataclass(frozen=True)
class _T:
    txn_row_num: int
    shares: Decimal | None
    acquired_disposed_code: str | None
    post_transaction_shares: Decimal | None
    direct_indirect: str | None
    nature_of_ownership: str | None
    security_title: str | None = "Common Stock"
    txn_date: date | None = date(2026, 6, 1)
    txn_date_invalid: bool = False
    is_derivative: bool = False


@dataclass(frozen=True)
class _H:
    shares: Decimal | None
    direct_indirect: str | None
    nature_of_ownership: str | None = None
    security_title: str | None = "Common Stock"
    is_derivative: bool = False


def _txns(*specs: tuple[str | None, int | None, int | None, str | None, str | None]) -> list[_T]:
    """(A/D code, shares, post, D/I, nature) per line, ``txn_row_num`` in order."""
    return [
        _T(
            txn_row_num=i,
            acquired_disposed_code=ad,
            shares=Decimal(sh) if sh is not None else None,
            post_transaction_shares=Decimal(post) if post is not None else None,
            direct_indirect=di,
            nature_of_ownership=nat,
        )
        for i, (ad, sh, post, di, nat) in enumerate(specs)
    ]


def _h(
    shares: int | None,
    di: str | None = "D",
    nature: str | None = None,
    *,
    security_title: str = "Common Stock",
    is_derivative: bool = False,
) -> _H:
    return _H(
        shares=Decimal(shares) if shares is not None else None,
        direct_indirect=di,
        nature_of_ownership=nature,
        security_title=security_title,
        is_derivative=is_derivative,
    )


def _fold(
    txns: list[_T],
    holdings: list[_H],
    di: str,
    *,
    document_type: str = "4",
    owners: tuple[str, ...] = (_CIK,),
    history: Sequence[tuple[str | None, str | None, Decimal]] | None = (),
) -> tuple[str, Decimal | None]:
    return form4_fold_shares(
        txns,
        holdings,
        di,
        document_type=document_type,
        filer_cik=_CIK,
        owner_ciks=owners,
        earlier_balances=lambda _title: history,
    )


# ---------------------------------------------------------------- folds (stored lines, dev DB)

# APO 0001858681-24-000036 (Kleinman): HCM Series C transacts; nine other vehicles are holdings.
_APO_TXNS = _txns(
    ("D", 10_664, 4_857_074, "D", None),
    ("D", 134_006, 4_723_068, "D", None),
    ("D", 26_365, 4_696_703, "D", None),
    ("D", 3_574, 4_693_129, "D", None),
    ("D", 736, 4_692_393, "D", None),
    ("D", 2_629, 140_358, "I", "HCM APO Series LLC, Series C"),
    ("D", 23_226, 117_132, "I", "HCM APO Series LLC, Series C"),
    ("D", 5_480, 111_652, "I", "HCM APO Series LLC, Series C"),
    ("D", 900, 110_752, "I", "HCM APO Series LLC, Series C"),
)
_APO_HOLDINGS = [
    _h(324_435, "I", "HCM APO Series LLC, Series A"),
    _h(27_408, "I", "HCM APO Series LLC, Series B"),
    _h(1_806_086, "I", "KRT Delaware LLC"),
    _h(9_391, "I", "KRT Investments LLC"),
    _h(4_584, "I", "KRT Investments VII LLC"),
    _h(440_364, "I", "KRT Investments IX LLC"),
    _h(702_496, "I", "The Kleinman Children's Trust"),
    _h(77_335, "I", "The Kleinman Descendant's GST-Exempt Trust"),
    _h(28_934, "I", "Heathcote Capital Partners LP"),
]

# SLSN 0000933430-25-000004 (Whitmore): two transacting vehicles, a Grace holding, and a D holding
# with no D transaction (a #3882 holding-only group).
_SLSN_TXNS = _txns(
    ("A", 2_500_000, 26_184_270, "I", "By Strandler, LLC"),
    ("A", 4_189_000, 30_373_270, "I", "By Strandler, LLC"),
    ("D", 2_500_000, 22_858_879, "I", "By Whitmore Holdings, LLC"),
    ("D", 4_189_000, 18_669_879, "I", "By Whitmore Holdings, LLC"),
)
_SLSN_HOLDINGS = [_h(108_141, "D"), _h(675_515, "I", "By Grace Investments, LP")]

# IP 0000051434-26-000058: a joint-brokerage series from zero, and a director-award holding (F5).
_IP_TXNS = _txns(("A", 12_875, 12_875, "D", None), ("A", 13_217, 26_092, "D", None))
_IP_HOLDINGS = [_h(55_634, "D")]


def test_apo_folds_its_nine_holding_vehicles() -> None:
    assert _fold(_APO_TXNS, _APO_HOLDINGS, "I") == (FORM4_VEHICLE_SUMS, Decimal(3_531_785))
    assert _fold(_APO_TXNS, _APO_HOLDINGS, "D")[0] == FORM4_FOLD_NOT_MIXED


def test_slsn_folds_grace_and_matches_the_13da() -> None:
    verdict, value = _fold(_SLSN_TXNS, _SLSN_HOLDINGS, "I")
    assert (verdict, value) == (FORM4_VEHICLE_SUMS, Decimal(49_718_664))
    assert value is not None and value + 108_141 == 49_826_805  # 13D/A 0000933430-25-000005


def test_ip_folds_the_director_award_into_direct() -> None:
    assert _fold(_IP_TXNS, _IP_HOLDINGS, "D") == (FORM4_VEHICLE_SUMS, Decimal(81_726))


def test_a_holding_only_group_beside_a_mixed_key_is_left_to_its_own_writer() -> None:
    assert _fold(_SLSN_TXNS, _SLSN_HOLDINGS, "D")[0] == FORM4_FOLD_NOT_MIXED
    sig = (datetime.now(UTC) - timedelta(days=30)).date()
    groups = form4_holding_observations(
        _SLSN_HOLDINGS,
        _SLSN_TXNS,
        document_type="4",
        owner_ciks=[_CIK],
        signature_date=sig,
        period_of_report=sig,
        manifest_filed_at=datetime.combine(sig, time(21), tzinfo=UTC),
    )
    assert [(g.direct_indirect, g.verdict, g.shares) for g in groups] == [("D", "one_line", Decimal(108_141))]


def test_a_same_nature_holding_beside_its_transacting_form_folds() -> None:
    # LGCY 0001493152-25-007246: several "See Footnote" lines, one transacting.
    txns = _txns(("A", 100, 100, "I", "See Footnote"), ("A", 50, 150, "I", "See Footnote"))
    assert _fold(txns, [_h(40, "I", "See Footnote")], "I") == (FORM4_VEHICLE_SUMS, Decimal(190))


def test_a_zero_opening_series_needs_no_manifest_time() -> None:
    txns = _txns(("A", 100, 100, "D", None))
    assert _fold(txns, [_h(7)], "D", history=None) == (FORM4_VEHICLE_SUMS, Decimal(107))


def test_zero_terms_are_ignored_by_the_equality_guards() -> None:
    zero_balance = _txns(("D", 100, 0, "D", None))
    assert _fold(zero_balance, [_h(0)], "D") == (FORM4_VEHICLE_SUMS, Decimal(0))
    assert _fold(_txns(("A", 10, 10, "D", None)), [_h(0), _h(0)], "D") == (FORM4_VEHICLE_SUMS, Decimal(10))


def test_5c_is_key_specific() -> None:
    txns = _txns(("A", 10, 20, "D", None), ("A", 10, 40, "I", "By Trust"))
    holdings = [_h(7, "D"), _h(9, "I", "By LLC")]
    history = [("I", "By Other", Decimal(30))]
    assert _fold(txns, holdings, "D", history=history) == (FORM4_VEHICLE_SUMS, Decimal(27))
    assert _fold(txns, holdings, "I", history=history) == ("5c running_total_across_filings", None)


def test_a_4a_repeating_the_transaction_with_one_corrected_holding_folds_what_it_carries() -> None:
    txns = _txns(("A", 100, 100, "D", None))
    assert _fold(txns, [_h(40), _h(60)], "D", document_type="4") == (FORM4_VEHICLE_SUMS, Decimal(200))
    assert _fold(txns, [_h(50)], "D", document_type="4/A") == (FORM4_VEHICLE_SUMS, Decimal(150))


# ---------------------------------------------------------------- refusals


def test_5b_a_transaction_opening_at_a_holding_refuses() -> None:
    assert _fold(_txns(("A", 50, 150, "D", None)), [_h(100)], "D") == ("5b running_total", None)


def test_5b_prime_a_holding_equal_to_a_transaction_balance_refuses() -> None:
    assert _fold(_txns(("A", 10, 150, "D", None)), [_h(150)], "D", history=[]) == (
        "5b holding_equals_transaction_balance",
        None,
    )


def test_3_two_classes_refuse() -> None:
    # HEI: Class A Common Stock beside Common Stock.
    txns = _txns(("A", 10, 10, "D", None))
    assert _fold(txns, [_h(5, security_title="Class A Common Stock")], "D") == ("3 multi_title", None)


def test_4_a_joint_filing_refuses() -> None:
    txns = _txns(("A", 10, 10, "D", None))
    assert _fold(txns, [_h(5)], "D", owners=(_CIK, "0002")) == ("4 joint_or_owner_mismatch", None)


def test_6_equal_holdings_refuse() -> None:
    # MXL: equal pairs of holding lines.
    txns = _txns(("A", 10, 10, "I", "By Trust"))
    assert _fold(txns, [_h(300, "I", "By A"), _h(300, "I", "By B")], "I") == ("6 equal_finals", None)


def test_2_a_transaction_with_no_di_and_a_value_only_holding_refuse() -> None:
    assert _fold(_txns(("A", 10, 10, None, None)), [_h(5)], "D") == ("2 incomplete", None)
    assert _fold(_txns(("A", 10, 10, "D", None)), [_h(None)], "D") == ("2 incomplete", None)
    assert _fold(_txns(("A", 10, 10, "I", "By Trust")), [_h(5, "I", "  ")], "I") == ("2 incomplete", None)


def test_5c_refuses_a_d_series() -> None:
    txns = _txns(("A", 10, 20, "D", None))
    assert _fold(txns, [_h(7)], "D", history=[("I", "By Trust", Decimal(10))]) == (
        "5c running_total_across_filings",
        None,
    )
    assert _fold(txns, [_h(7)], "D", history=None) == ("5c running_total_across_filings", None)


def test_guards_cross_d_and_i() -> None:
    # An I holding equal to a D transaction balance refuses the D key (5b').
    d_txns = _txns(("A", 10, 20, "D", None))
    assert _fold(d_txns, [_h(7, "D"), _h(20, "I", "By LLC")], "D")[0] == "5b holding_equals_transaction_balance"
    # A D holding equal to an I holding refuses both keys (6).
    txns = _txns(("A", 20, 20, "D", None), ("A", 30, 30, "I", "By Trust"))
    holdings = [_h(5, "D"), _h(5, "I", "By LLC")]
    assert _fold(txns, holdings, "D") == ("6 equal_finals", None)
    assert _fold(txns, holdings, "I") == ("6 equal_finals", None)


def test_a_refused_fold_keeps_todays_vehicle_sum_terms() -> None:
    # The fold refuses at 5b' (the holding restates Trust A); the vehicle sum, which never sees
    # holdings, still sums — the writer keeps it.
    txns = _txns(("A", 10, 10, "I", "By Trust A"), ("A", 20, 20, "I", "By Trust B"))
    assert _fold(txns, [_h(10, "I", "By LLC")], "I")[0] == "5b holding_equals_transaction_balance"
    assert form4_observation_shares(
        txns, document_type="4", filer_cik=_CIK, owner_ciks=[_CIK], earlier_balances=lambda _t: []
    ) == (FORM4_VEHICLE_SUMS, Decimal(30))


# ---------------------------------------------------------------- scope


def test_no_holding_line_is_not_mixed_and_form5_is_out_of_scope() -> None:
    txns = _txns(("A", 10, 10, "D", None), ("A", 5, 5, "I", "By Trust"))
    assert _fold(txns, [], "D")[0] == FORM4_FOLD_NOT_MIXED
    assert _fold(txns, [], "I")[0] == FORM4_FOLD_NOT_MIXED
    assert _fold(txns, [_h(7)], "D", document_type="5") == ("out_of_scope", None)
    assert (
        form4_fold_shares(
            txns, [_h(7)], "D", document_type="4", filer_cik=None, owner_ciks=[], earlier_balances=lambda _t: []
        )[0]
        == "out_of_scope"
    )


def test_derivative_lines_are_ignored() -> None:
    txns = _txns(("A", 10, 10, "D", None))
    assert _fold(txns, [_h(7, is_derivative=True)], "D")[0] == FORM4_FOLD_NOT_MIXED
    assert _fold(txns, [_h(7), _h(10, is_derivative=True)], "D") == (FORM4_VEHICLE_SUMS, Decimal(17))
