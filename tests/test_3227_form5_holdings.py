"""#3227 — a Form 5's Table I holding lines reach the insider observation.

Spec: docs/specs/ownership/2026-10-18-3227-form5-holdings.md. Pure tests of the Form 5 parser change
and of ``form4_holding_observations``'s Form 5 date, retention and gates."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

from app.services.insider_transactions import (
    SECTION_16_ENACTED,
    form4_holding_observations,
    form5_holding_as_of,
    form5_retention_cutoff,
    parse_form_4_xml,
    parse_form_5_xml,
)

# Relative to today, so the Form 5 retention window (18 months back) never ages the fixtures out.
_FILED = datetime.combine((datetime.now(UTC) - timedelta(days=40)).date(), time(21, 30), tzinfo=UTC)
_FYE = _FILED.date() - timedelta(days=44)
_SIG = _FILED.date() - timedelta(days=1)
_OWNER = ["0000000001"]


@dataclass(frozen=True)
class _H:
    shares: Decimal | None
    direct_indirect: str | None = "D"
    security_title: str | None = "Common Stock"
    is_derivative: bool = False
    holding_form_type: str | None = None


@dataclass(frozen=True)
class _T:
    post_transaction_shares: Decimal | None
    direct_indirect: str | None = "D"
    security_title: str | None = "Common Stock"
    is_derivative: bool = False


def _run(
    holdings: list[_H],
    transactions: list[_T] | None = None,
    *,
    document_type: str = "5",
    owners: list[str] = _OWNER,
    period: date | None = _FYE,
    filed_at: datetime | None = _FILED,
    exit_box: bool | None = None,
) -> dict[str, tuple[str, Decimal | None, date | None]]:
    groups = form4_holding_observations(
        holdings,
        transactions or [],
        document_type=document_type,
        owner_ciks=owners,
        signature_date=_SIG,
        period_of_report=period,
        manifest_filed_at=filed_at,
        not_subject_to_section_16=exit_box,
    )
    return {g.direct_indirect: (g.verdict, g.shares, g.period_end) for g in groups}


def _d(v: str) -> Decimal:
    return Decimal(v)


# ---------------------------------------------------------------- parser


def _xml(body: str, *, document_type: str = "5") -> str:
    return f"""<?xml version="1.0"?>
<ownershipDocument>
  <documentType>{document_type}</documentType>
  <periodOfReport>2025-12-31</periodOfReport>
  <issuer><issuerCik>0000933430</issuerCik><issuerName>X</issuerName></issuer>
  <reportingOwner>
    <reportingOwnerId><rptOwnerCik>0001998412</rptOwnerCik><rptOwnerName>Filer</rptOwnerName></reportingOwnerId>
    <reportingOwnerRelationship><isDirector>1</isDirector></reportingOwnerRelationship>
  </reportingOwner>
  {body}
  <ownerSignature><signatureName>F</signatureName><signatureDate>2026-02-10</signatureDate></ownerSignature>
</ownershipDocument>
"""


def _holding(shares: str, di: str, *, form_type: str | None = None) -> str:
    coding = (
        f"<transactionCoding><transactionFormType>{form_type}</transactionFormType></transactionCoding>"
        if form_type
        else ""
    )
    return f"""
    <nonDerivativeHolding>
      <securityTitle><value>Common Stock</value></securityTitle>{coding}
      <postTransactionAmounts>
        <sharesOwnedFollowingTransaction><value>{shares}</value></sharesOwnedFollowingTransaction>
      </postTransactionAmounts>
      <ownershipNature><directOrIndirectOwnership><value>{di}</value></directOrIndirectOwnership></ownershipNature>
    </nonDerivativeHolding>"""


_D_HOLDING = """
  <derivativeTable>
    <derivativeHolding>
      <securityTitle><value>Stock Option</value></securityTitle>
      <conversionOrExercisePrice><value>10</value></conversionOrExercisePrice>
      <underlyingSecurity>
        <underlyingSecurityTitle><value>Common Stock</value></underlyingSecurityTitle>
        <underlyingSecurityShares><value>500</value></underlyingSecurityShares>
      </underlyingSecurity>
      <postTransactionAmounts>
        <sharesOwnedFollowingTransaction><value>500</value></sharesOwnedFollowingTransaction>
      </postTransactionAmounts>
      <ownershipNature><directOrIndirectOwnership><value>D</value></directOrIndirectOwnership></ownershipNature>
    </derivativeHolding>
  </derivativeTable>
"""


def test_the_form5_parser_returns_holdings_and_their_form_type() -> None:
    # iid 1048597's shape: a D holding and an I holding, no transaction.
    table = (
        f"<nonDerivativeTable>{_holding('4000', 'D')}{_holding('2070.436', 'I', form_type='3')}</nonDerivativeTable>"
    )
    parsed = parse_form_5_xml(_xml(table))
    assert parsed is not None
    assert parsed.transactions == ()
    assert parsed.period_of_report == date(2025, 12, 31)
    assert [(h.row_num, h.direct_indirect, h.shares, h.holding_form_type) for h in parsed.holdings] == [
        (0, "D", _d("4000"), None),
        (1, "I", _d("2070.436"), "3"),
    ]


def test_a_derivative_holdings_only_form5_is_a_filing() -> None:
    parsed = parse_form_5_xml(_xml(_D_HOLDING))
    assert parsed is not None
    assert [h.is_derivative for h in parsed.holdings] == [True]


def test_a_5a_holdings_only_parses() -> None:
    parsed = parse_form_5_xml(
        _xml(f"<nonDerivativeTable>{_holding('1', 'D')}</nonDerivativeTable>", document_type="5/A")
    )
    assert parsed is not None and parsed.document_type == "5/A"


def test_a_form4_holding_has_no_form_type() -> None:
    parsed = parse_form_4_xml(_xml(f"<nonDerivativeTable>{_holding('1', 'D')}</nonDerivativeTable>", document_type="4"))
    assert parsed is not None and parsed.holdings[0].holding_form_type is None


# ---------------------------------------------------------------- date


def test_a_group_is_dated_by_the_period_of_report_not_the_signature() -> None:
    assert _run([_H(_d("4000")), _H(_d("2070.436"), "I")]) == {
        "D": ("one_line", _d("4000"), _FYE),
        "I": ("one_line", _d("2070.436"), _FYE),
    }


def test_form5_holding_as_of_branches() -> None:
    filed = date(2026, 2, 14)
    assert form5_holding_as_of(date(2025, 12, 31), filed) == date(2025, 12, 31)
    assert form5_holding_as_of(filed, filed) == filed
    assert form5_holding_as_of(filed + timedelta(days=1), filed) is None
    assert form5_holding_as_of(None, filed) is None
    assert form5_holding_as_of(SECTION_16_ENACTED - timedelta(days=1), filed) is None
    assert form5_holding_as_of(SECTION_16_ENACTED, filed) == SECTION_16_ENACTED


def test_a_period_after_the_filed_date_or_missing_writes_nothing() -> None:
    assert _run([_H(_d("5"))], period=_FILED.date() + timedelta(days=1))["D"] == ("one_line", None, None)
    assert _run([_H(_d("5"))], period=None)["D"] == ("one_line", None, None)


def test_a_period_before_the_section_16_floor_writes_nothing() -> None:
    assert _run([_H(_d("5"))], period=date(1900, 12, 31))["D"] == ("one_line", None, None)


def test_a_missing_manifest_filed_at_writes_nothing() -> None:
    assert _run([_H(_d("5"))], filed_at=None) == {}


# ---------------------------------------------------------------- retention


def test_the_retention_cutoff_is_inclusive_on_the_utc_date() -> None:
    cutoff = form5_retention_cutoff()
    on = datetime.combine(cutoff, time(12), tzinfo=UTC)
    assert _run([_H(_d("5"))], filed_at=on, period=cutoff)["D"][1] == _d("5")
    assert _run([_H(_d("5"))], filed_at=on - timedelta(days=1), period=cutoff - timedelta(days=1)) == {}
    # 22:00 in New York on the day before the cutoff is 02:00 or 03:00 UTC on the cutoff: retained.
    ny = datetime.combine(cutoff - timedelta(days=1), time(22), tzinfo=ZoneInfo("America/New_York"))
    assert ny.astimezone(UTC).date() == cutoff
    assert _run([_H(_d("5"))], filed_at=ny, period=cutoff - timedelta(days=30))["D"][1] == _d("5")
    # 20:00 in New York on the day before is still the day before in UTC: refused.
    early = datetime.combine(cutoff - timedelta(days=1), time(18), tzinfo=ZoneInfo("America/New_York"))
    assert early.astimezone(UTC).date() == cutoff - timedelta(days=1)
    assert _run([_H(_d("5"))], filed_at=early, period=cutoff - timedelta(days=30)) == {}


def test_form5_retention_is_not_form4s() -> None:
    # Two years back is inside Form 4's 3-year window and outside Form 5's 18 months.
    old = _FILED - timedelta(days=730)
    assert _run([_H(_d("5"))], document_type="5", filed_at=old, period=old.date()) == {}
    assert _run([_H(_d("5"))], document_type="4", filed_at=old, period=old.date())["D"][1] == _d("5")


# ---------------------------------------------------------------- gates


def test_a_5a_writes_nothing() -> None:
    assert _run([_H(_d("5")), _H(_d("7"), "I")], document_type="5/A") == {
        "D": ("refused:amendment", None, _FYE),
        "I": ("refused:amendment", None, _FYE),
    }


def test_an_exit_box_filing_writes_nothing() -> None:
    assert _run([_H(_d("5"))], exit_box=True) == {"D": ("refused:exit_box", None, _FYE)}
    assert _run([_H(_d("5"))], exit_box=False)["D"][0] == "one_line"


def test_a_delinquent_form3_line_refuses_its_group_only() -> None:
    out = _run([_H(_d("5")), _H(_d("6"), holding_form_type="3"), _H(_d("7"), "I")])
    assert out == {
        "D": ("refused:delinquent_form3", None, _FYE),
        "I": ("one_line", _d("7"), _FYE),
    }
    assert _run([_H(_d("5"), holding_form_type="5")])["D"][0] == "one_line"


# ---------------------------------------------------------------- verdicts


def test_several_lines_of_one_title_sum() -> None:
    assert _run([_H(_d("10"), "I"), _H(_d("20"), "I")])["I"] == ("summed", _d("30"), _FYE)


def test_a_refused_multi_line_group_writes_nothing() -> None:
    assert _run([_H(_d("10"), "I"), _H(_d("10"), "I")])["I"] == ("refused:equal_amounts", None, _FYE)
    assert _run([_H(_d("10")), _H(_d("20"), security_title="Preferred Stock")])["D"][:2] == (
        "refused:multi_title",
        None,
    )


def test_a_mixed_group_is_left_to_the_transaction_rule() -> None:
    assert _run([_H(_d("5")), _H(_d("7"), "I")], [_T(_d("900"))]) == {"I": ("one_line", _d("7"), _FYE)}


# ---------------------------------------------------------------- Form 4 unchanged


def test_form4_ignores_the_form5_gates() -> None:
    form4 = _run([_H(_d("5"), holding_form_type="3")], document_type="4", exit_box=True)
    # Form 4 dates a holding by the signature date (``form4_holding_as_of``).
    assert form4 == {"D": ("one_line", _d("5"), _SIG)}
