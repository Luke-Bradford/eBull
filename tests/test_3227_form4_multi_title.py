"""#3227 — a Form 4 unit whose security titles differ but name one class is read as one title.

Spec: docs/specs/ownership/2026-10-18-3227-form4-multi-title.md. Pure tests of
``form4_title_class`` / ``form4_title_match`` and of the three Form 4 helpers that call it."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal

import pytest

from app.services.insider_transactions import (
    FORM4_TITLE_CLASS_VERSION,
    FORM4_VEHICLE_SUMS,
    Form4TitleMatch,
    form4_fold_shares,
    form4_holding_observations,
    form4_observation_shares,
    form4_title_class,
    form4_title_class_fingerprint,
    form4_title_match,
)
from app.services.ownership_rollup import _summable_scope

_CIK = "0001"


def test_rule_is_frozen() -> None:
    # The dropped words and both functions' source: a change re-runs the A/B and re-reads its title sets.
    assert form4_title_class_fingerprint() == FORM4_TITLE_CLASS_VERSION


# Title sets the A/B (scripts.ab_3227_form4_multi_title) unifies, as stored.
_ONE_CLASS = [
    ["Common Stock", "common Stock", "Common  Stock", "COMMON", "Common Shares", "Common Stock."],
    ["Common Stock", "Common Stock, $.10 par value", 'Common Stock, par value $0.0001 per share ("Common Stock")'],
    ["Common Stock, no par value", "Common Stock, without par value", "$2.50 par value Common Stock"],
    ["Common Stock", "Common Stock-401(k) Plan", "Common Stock 401k", "Common Stock (401-K Plan)", "Common - 401(k)"],
    ["Common Stock", "Common Stock (401)k", "Common Stock(401(k))", "Common Stock 403(b)"],
    ["Common Stock", "Restricted Stock", "Restricted Shares of Common Stock", "Common Stock Unvested"],
    ["Common Stock (Book Entry)", "Common Stock (Held in Street Name)", "Common Stock (DRP)"],
    ["Common Stock (Direct)", "Common Stock (Indirect)", "Common Stock - Held by Spouse", "Common Stock (IRA)"],
    ["Mid Penn Bancorp, Inc. Common Restricted Stock", "Mid Penn Bancorp, Inc. Common Stock"],
    ["Common Class A", "Common Class A - Retirement Plan"],
    ["Class A Common Stock", "Class A Common", "Class A common stock, par value $0.01"],
    ["Ordinary Shares", "Ordinary Shares - ESOP", "Ordinary shares, no par value"],
    ["Common Stock with Voting Rights", "Common Stock (Voting Rights)"],
    [
        "Common Stock",
        "Common Stock in Profit Sharing Plan",
        'Common Stock, par value $0.01 per share (the "Common Stock")',
    ],
]

# Different classes, or a word the rule does not explain (which may name one). The constructed
# pairs are Codex checkpoint 1's (rounds 1 and 2).
_REFUSE = [
    ["Class A Common Stock", "Class B Common Stock"],
    ["Class A Common Stock", "Common Stock"],
    ["Common Class A", "Class A Common Stock"],
    ["Bio-Rad A Common Stock", "Bio-Rad B Common Stock"],
    ["Common Stock (A)", "Common Stock (B)"],
    ["Common Stock (Cl. A)", "Common Stock (Cl. B)"],
    ["Common Stock Class A 1", "Common Stock Class A 2"],
    ["Common Stock Class A.1", "Common Stock Class A.2"],
    ["Class A-1-1 Common Stock", "Class A-1-2 Common Stock"],
    ["Class B-1 Common Stock", "Class B1 Common Stock"],
    ["Class B Common", "Class B-1 Common"],
    ["Common Stock II", "Common Stock III"],
    ["Common Stock (Α)", "Common Stock (Β)"],
    ["Common Stock Class A/B", "Common Stock Class A"],
    ["Common Stock Class A and Class B", "Common Stock Class A"],
    [
        "Class A Common Stock convertible into Class B Common Stock",
        "Class B Common Stock convertible into Class A Common Stock",
    ],
    ["Common Stock Series 1, $0.01 par value", "Common Stock Series 2, $0.01 par value"],
    ["Common Stock Class A 1, no par value", "Common Stock Class A 2, no par value"],
    ["6% Series A Preferred Stock, $25 par value", "8% Series A Preferred Stock, $25 par value"],
    ["Common Stock Series 2024", "Common Stock Series 2025"],
    ["2024 Annual Restricted Stock Units", "2025 Annual Restricted Stock Units"],
    ["Common Stock Series 401", "Common Stock Series 403"],
    ["Common Stock (Voting Rights)", "Common Stock (No Voting Rights)"],
    ["Common Stock with Voting Rights", "Common Stock without Voting Rights"],
    ["Voting Shares", "Restricted Voting Shares"],
    ["Ordinary Shares", "Employee Ordinary Shares"],
    ["Common Stock", "Common Trust Shares"],
    ["Common Stock with Dividend", "Common Stock without Dividend"],
    ["Common Stock", "Employee Stock Purchase Plan"],
    ["Prfd Stock", "Common Stock"],
    ["Common Stock", "Series A Preferred Stock"],
    ["Common shares - BBDC3", "Preference shares - BBDC4"],
    ["Ordinary Shares", "Ordinary Shares (represented by American depositary shares)"],
    ["Ordinary Shares", "Common Stock"],
    ["Common Stock (Voting)", "Common Stock - Non-Voting"],
    ["Common Stock", "Restricted Stock Units"],
    ["Common Stock", "Common Stock (SUA)"],
    ["Common Stock", "Common Stock/ Serp"],
    ["Common Stock", "Estimated Shares held in 401(k)"],
    ["common shares", "common shares (opening balance)"],
    ["Class A Common Stock", "Class A Common Stock-Dir. Def. Stock Comp Plan-1"],
    [
        "Orrstown Financial Services, Inc., Common, Rest. Stock. Unt.",
        "Orrstown Financial Services, Inc., Common, Restricted Stock",
    ],
    ["Common Stock", "Commom Stock"],
    ["Common Stock", "K Common Stock"],
    ["Common Stock", "Common Stock - Family Trust"],
    ["Common Stock", "Common Stock, $.10 par value (2019 SIP)"],
    ["Common Stock", "401(k) Plan"],
    # Codex checkpoint 1, round 3.
    ["Non-Voting Non-Participating Preferred Stock", "Non-Voting Participating Preferred Stock"],
    ["6% Preferred Stock", "6.6% Preferred Stock"],
    [
        "Class A Common Stock convertible into Class B Common Stock",
        "Class A Common Stock convertible into Class B Common Stock and Class A Common Stock",
    ],
    ["Common Stock, $0.01 par value 6% dividend", "Common Stock, $0.01 par value 8% dividend"],
    ["Common Stock with Profit Sharing Rights", "Common Stock with Rights"],
    ["Common Stock Class 401k", "Common Stock Class 403b"],
    ["Common Stock Series A-1", "Common Stock Series A/1"],
    ["Common Stock with 1/2 Voting Rights", "Common Stock with 1.2 Voting Rights"],
    ["Common Stock Class A–1", "Common Stock Class A/1"],
    ["Common Stock (A\u0301)", "Common Stock (A)"],
    ["Common Stock", None],
    ["Common Stock", ""],
]


@pytest.mark.parametrize("titles", _ONE_CLASS)
def test_one_class(titles: list[str]) -> None:
    match = form4_title_match(titles)
    assert match is not None
    assert all(match(t) for t in titles)
    assert not match(None)


@pytest.mark.parametrize("titles", _REFUSE)
def test_several_classes_refuse(titles: list[str | None]) -> None:
    assert form4_title_match(titles) is None


def test_title_class_words() -> None:
    assert form4_title_class("Common Stock, $.10 par value (SIP)") == ("common",)
    assert form4_title_class("Restricted Stock") == ("common",)
    assert form4_title_class("Common Class A - Retirement Plan") == ("common", "class:a")
    assert form4_title_class("Class C Capital Stock") == ("class:c", "capital")
    assert form4_title_class("Restricted Voting Shares") == ("restricted", "voting", "common")
    assert form4_title_class("UMH Properties Inc") is None
    assert form4_title_class("401(k) Plan") is None
    assert form4_title_class(None) is None


def test_one_title_is_exact_equality_whatever_its_class() -> None:
    # The rule before #3227 multi-title, unchanged: a single title passes, even one with no class.
    match = form4_title_match(["Series B Preferred Stock", "Series B Preferred Stock"])
    assert match is not None
    assert match("Series B Preferred Stock")
    assert not match("Series B Preferred Stock ")
    assert not match(None)


def test_several_titles_of_one_class_match_by_class() -> None:
    match = form4_title_match(["Common Stock", "Common Stock 401k"])
    assert match is not None
    assert match("Common Stock, par value $0.01 per share")
    assert not match("Class A Common Stock")
    assert not match("Common Stock Units")


# ---------------------------------------------------------------- the helpers


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
    holding_form_type: str | None = None


def _history(
    seen: list[Form4TitleMatch], rows: Sequence[tuple[str | None, str | None, Decimal]] = ()
) -> Callable[[Form4TitleMatch], Sequence[tuple[str | None, str | None, Decimal]]]:
    def earlier(match: Form4TitleMatch) -> Sequence[tuple[str | None, str | None, Decimal]]:
        seen.append(match)
        return rows

    return earlier


def test_vehicle_sum_across_one_class_titles() -> None:
    # Two trusts, one titled with its par value; before this change "3 multi_title".
    lines = [
        _T(0, Decimal(100), "A", Decimal(1100), "I", "By Trust A", "Common Stock"),
        _T(1, Decimal(50), "A", Decimal(550), "I", "By Trust B", "Common Stock, par value $0.01"),
    ]
    seen: list[Form4TitleMatch] = []
    verdict, total = form4_observation_shares(
        lines,
        document_type="4",
        filer_cik=_CIK,
        owner_ciks=[_CIK],
        earlier_balances=_history(seen),
    )
    assert (verdict, total) == (FORM4_VEHICLE_SUMS, Decimal(1650))
    # 5c reads earlier balances of the class, not of one spelling.
    assert seen and seen[0]("COMMON STOCK") and not seen[0]("Class A Common Stock")


def test_vehicle_sum_refuses_two_classes() -> None:
    lines = [
        _T(0, Decimal(100), "A", Decimal(1100), "I", "By Trust A", "Class A Common Stock"),
        _T(1, Decimal(50), "A", Decimal(550), "I", "By Trust B", "Class B Common Stock"),
    ]
    verdict, total = form4_observation_shares(
        lines, document_type="4", filer_cik=_CIK, owner_ciks=[_CIK], earlier_balances=lambda _m: []
    )
    assert (verdict, total) == ("3 multi_title", None)


def test_fold_reads_a_401k_holding_beside_a_transaction() -> None:
    # 0001504649-26-000009 as stored: a D sale of 6,204 to an LLC, the LLC and spouse holdings,
    # and a D "Common Stock 401(k)" holding.
    txns = [_T(0, Decimal(6204), "D", Decimal(85532), "D", None)]
    held = [
        _H(Decimal(6204), "I", "By LLC"),
        _H(Decimal(168), "I", "Spouse"),
        _H(Decimal("24669.6562"), "D", None, "Common Stock 401(k)"),
    ]
    verdict, value = form4_fold_shares(
        txns, held, "D", document_type="4", filer_cik=_CIK, owner_ciks=[_CIK], earlier_balances=lambda _m: []
    )
    assert (verdict, value) == (FORM4_VEHICLE_SUMS, Decimal("110201.6562"))


def test_fold_refuses_a_unit_holding_beside_common() -> None:
    txns = [_T(0, Decimal(10), "A", Decimal(110), "D", None)]
    held = [_H(Decimal(40), "D", None, "Restricted Stock Units")]
    verdict, value = form4_fold_shares(
        txns, held, "D", document_type="4", filer_cik=_CIK, owner_ciks=[_CIK], earlier_balances=lambda _m: []
    )
    assert (verdict, value) == ("3 multi_title", None)


_SIG = (datetime.now(UTC) - timedelta(days=30)).date()
_FILED = datetime.combine(_SIG, time(21, 30), tzinfo=UTC)


def _groups(holdings: list[_H], transactions: Sequence[_T] = ()) -> dict[str, tuple[str, Decimal | None]]:
    groups = form4_holding_observations(
        holdings,
        list(transactions),
        document_type="4",
        owner_ciks=[_CIK],
        signature_date=_SIG,
        period_of_report=_SIG - timedelta(days=4),
        manifest_filed_at=_FILED,
        not_subject_to_section_16=None,
    )
    return {g.direct_indirect: (g.verdict, g.shares) for g in groups}


def test_holding_group_sums_one_class_titles() -> None:
    held = [_H(Decimal(100), "D"), _H(Decimal(50), "D", None, "Common Stock 401(k)")]
    assert _groups(held) == {"D": ("summed", Decimal(150))}


def test_holding_group_refuses_two_classes() -> None:
    held = [_H(Decimal(100), "D", None, "Class A Common Stock"), _H(Decimal(50), "D", None, "Class B Common Stock")]
    assert _groups(held) == {"D": ("refused:multi_title", None)}


def test_holding_group_compares_amounts_across_the_class() -> None:
    # The I line restates the D 401(k) amount under another spelling: equal amounts of one class refuse.
    held = [
        _H(Decimal(100), "D"),
        _H(Decimal(50), "D", None, "Common Stock 401(k)"),
        _H(Decimal(50), "I", "By Plan", "Common Stock (Plan)"),
    ]
    assert _groups(held)["D"] == ("refused:equal_amounts", None)


def test_single_title_holding_group_ignores_other_spellings() -> None:
    # A one-title group keeps exact matching: another spelling's equal amount does not refuse it.
    held = [_H(Decimal(100), "D"), _H(Decimal(60), "D"), _H(Decimal(60), "I", "By Plan", "Common Stock (Plan)")]
    assert _groups(held)["D"] == ("summed", Decimal(160))


def test_fold_refuses_two_classes() -> None:
    txns = [_T(0, Decimal(10), "A", Decimal(110), "D", None, "Class A Common Stock")]
    held = [_H(Decimal(40), "D", None, "Class B Common Stock")]
    verdict, value = form4_fold_shares(
        txns, held, "D", document_type="4", filer_cik=_CIK, owner_ciks=[_CIK], earlier_balances=lambda _m: []
    )
    assert (verdict, value) == ("3 multi_title", None)


def test_holding_group_refuses_a_transaction_balance_of_the_class() -> None:
    # The I holding group is holding-only; its 401(k) line equals the D transaction's balance, which
    # is written in another spelling of the class.
    held = [_H(Decimal(900), "I", "By Plan", "Common Stock 401(k)"), _H(Decimal(30), "I", "By Trust")]
    txns = [_T(0, Decimal(100), "D", Decimal(900), "D", None, "Common Stock, par value $0.01")]
    assert _groups(held, txns)["I"] == ("refused:equal_to_transaction_balance", None)


def test_rollup_scope_takes_each_writers_title_test() -> None:
    one_class = ["Common Stock", "Restricted Stock"]
    assert _summable_scope(True, one_class, form3=False, holdings=False)
    # Form 3 writers keep exact titles.
    assert not _summable_scope(True, one_class, form3=True, holdings=False)
    assert _summable_scope(True, ["Common Stock"], form3=True, holdings=True)
    # A holding group's titles are stripped, as form4_holding_observations strips them.
    assert _summable_scope(
        True, ["Limited Partnership Units", " Limited Partnership Units "], form3=False, holdings=True
    )
    assert not _summable_scope(
        True, ["Limited Partnership Units", " Limited Partnership Units "], form3=False, holdings=False
    )
    assert not _summable_scope(False, ["Common Stock"], form3=False, holdings=False)
    assert not _summable_scope(True, None, form3=None, holdings=False)
