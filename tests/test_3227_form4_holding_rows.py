"""#3227 — a Form 4's Table I holding lines reach the insider observation.

Spec: docs/specs/ownership/2026-10-18-3227-form4-holding-rows.md. Pure tests of the parser change
and of ``form4_holding_observations``, the one helper both Form 4 observation writers call."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal

from app.services.insider_transactions import (
    form4_holding_as_of,
    form4_holding_observations,
    parse_form_4_xml,
)

# Relative to today, so the Form 4 retention window (3 years back) never ages the fixtures out.
_SIG = (datetime.now(UTC) - timedelta(days=30)).date()
_POR = _SIG - timedelta(days=4)
_FILED = datetime.combine(_SIG, time(21, 30), tzinfo=UTC)
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
    document_type: str = "4",
    owners: list[str] = _OWNER,
    signature: date | None = _SIG,
    period: date | None = _POR,
    filed_at: datetime | None = _FILED,
) -> dict[str, tuple[str, Decimal | None, date | None]]:
    groups = form4_holding_observations(
        holdings,
        transactions or [],
        document_type=document_type,
        owner_ciks=owners,
        signature_date=signature,
        period_of_report=period,
        manifest_filed_at=filed_at,
        not_subject_to_section_16=None,
    )
    return {g.direct_indirect: (g.verdict, g.shares, g.period_end) for g in groups}


def _d(v: str) -> Decimal:
    return Decimal(v)


# ---------------------------------------------------------------- parser


def _xml(body: str, *, document_type: str = "4") -> str:
    return f"""<?xml version="1.0"?>
<ownershipDocument>
  <documentType>{document_type}</documentType>
  <periodOfReport>2025-05-29</periodOfReport>
  <issuer><issuerCik>0000933430</issuerCik><issuerName>X</issuerName></issuer>
  <reportingOwner>
    <reportingOwnerId><rptOwnerCik>0000000001</rptOwnerCik><rptOwnerName>Whitmore</rptOwnerName></reportingOwnerId>
    <reportingOwnerRelationship><isDirector>1</isDirector></reportingOwnerRelationship>
  </reportingOwner>
  {body}
  <ownerSignature><signatureName>W</signatureName><signatureDate>2025-06-02</signatureDate></ownerSignature>
</ownershipDocument>
"""


_ND_HOLDING = """
  <nonDerivativeTable>
    <nonDerivativeHolding>
      <securityTitle><value>Common Stock</value></securityTitle>
      <postTransactionAmounts>
        <sharesOwnedFollowingTransaction><value>108141</value></sharesOwnedFollowingTransaction>
      </postTransactionAmounts>
      <ownershipNature><directOrIndirectOwnership><value>D</value></directOrIndirectOwnership></ownershipNature>
    </nonDerivativeHolding>
  </nonDerivativeTable>
"""

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


def test_a_holdings_only_form4_is_a_filing_with_its_holdings() -> None:
    parsed = parse_form_4_xml(_xml(_ND_HOLDING + _D_HOLDING))
    assert parsed is not None
    assert parsed.transactions == ()
    assert [(h.row_num, h.is_derivative, h.shares, h.filer_cik) for h in parsed.holdings] == [
        (0, False, _d("108141"), "0000000001"),
        (1, True, _d("500"), "0000000001"),
    ]
    assert parsed.signature_date == date(2025, 6, 2)


def test_a_derivative_holdings_only_form4_is_a_filing() -> None:
    parsed = parse_form_4_xml(_xml(_D_HOLDING))
    assert parsed is not None
    assert len(parsed.holdings) == 1 and parsed.holdings[0].is_derivative


def test_a_4a_holdings_only_parses() -> None:
    parsed = parse_form_4_xml(_xml(_ND_HOLDING, document_type="4/A"))
    assert parsed is not None and parsed.document_type == "4/A"


# ---------------------------------------------------------------- values


def test_one_line_d_writes_its_shares() -> None:
    # SLSN 0000933430-25-000004: the D line is a holding of 108,141.
    assert _run([_H(_d("108141"))]) == {"D": ("one_line", _d("108141"), _SIG)}


def test_several_lines_of_one_title_sum() -> None:
    # BWFG 0001505732-26-000116: four restricted grants beside the main block.
    lines = [_H(_d(v)) for v in ("400", "534", "1200", "1455", "37330")]
    assert _run(lines)["D"][:2] == ("summed", _d("40919"))


def test_multi_title_refuses_and_writes_nothing_and_the_guard_is_per_title() -> None:
    # UMH 0000752642-26-000124: D is common + "Preferred D" (refused, never the last line, 500);
    # I is three common lines, one equal to the preferred 500, which does not refuse the sum.
    out = _run(
        [
            _H(_d("37674.78")),
            _H(_d("500"), security_title="Preferred D"),
            _H(_d("500"), direct_indirect="I"),
            _H(_d("763.72"), direct_indirect="I"),
            _H(_d("648.14"), direct_indirect="I"),
        ]
    )
    assert out["D"] == ("refused:multi_title", None, _SIG)
    assert out["I"][:2] == ("summed", _d("1911.86"))


def test_equal_holding_amounts_across_d_and_i_refuse() -> None:
    out = _run([_H(_d("100")), _H(_d("40")), _H(_d("100"), direct_indirect="I")])
    assert out["D"][:2] == ("refused:equal_amounts", None)
    assert out["I"][:2] == ("one_line", _d("100"))  # one line has nothing to sum


def test_a_holding_equal_to_a_transaction_balance_refuses() -> None:
    # The I transaction keeps I out of scope (mixed); its balance still guards the D sum.
    out = _run([_H(_d("250")), _H(_d("90"))], [_T(_d("250"), direct_indirect="I")])
    assert out == {"D": ("refused:equal_to_transaction_balance", None, _SIG)}


def test_a_line_without_a_share_count_refuses() -> None:
    assert _run([_H(_d("10")), _H(None)])["D"][:2] == ("refused:no_share_count_line", None)
    assert _run([_H(None)])["D"][:2] == ("refused:no_share_count_line", None)


def test_a_joint_filing_refuses_a_sum_but_writes_one_line() -> None:
    owners = ["0000000001", "0000000002"]
    assert _run([_H(_d("10")), _H(_d("20"))], owners=owners)["D"][:2] == ("refused:joint_filing", None)
    assert _run([_H(_d("10"))], owners=owners)["D"][:2] == ("one_line", _d("10"))


# ---------------------------------------------------------------- scope


def test_a_mixed_group_is_left_to_the_transaction_rule() -> None:
    assert _run([_H(_d("5")), _H(_d("7"), direct_indirect="I")], [_T(_d("100"))]) == {"I": ("one_line", _d("7"), _SIG)}


def test_a_transaction_with_no_di_covers_d() -> None:
    assert _run([_H(_d("5"))], [_T(_d("100"), direct_indirect=None)]) == {}


def test_derivative_lines_are_ignored() -> None:
    out = _run([_H(_d("5"), is_derivative=True)], [_T(_d("100"), is_derivative=True)])
    assert out == {}
    assert _run([_H(_d("5"))], [_T(_d("100"), is_derivative=True)])["D"][:2] == ("one_line", _d("5"))


def test_a_holding_line_with_no_di_joins_no_group() -> None:
    assert _run([_H(_d("5"), direct_indirect=None)]) == {}


def test_a_4a_is_in_scope_and_form3_is_not() -> None:
    # Form 5 is in scope since #3227's Form 5 slice (tests/test_3227_form5_holdings.py).
    assert _run([_H(_d("5"))], document_type="4/A")["D"][:2] == ("one_line", _d("5"))
    assert _run([_H(_d("5"))], document_type="3") == {}


def test_retention_and_a_missing_manifest_filed_at_write_nothing() -> None:
    assert _run([_H(_d("5"))], filed_at=None) == {}
    old = datetime(2015, 6, 2, tzinfo=UTC)
    assert _run([_H(_d("5"))], filed_at=old, signature=date(2015, 6, 2), period=date(2015, 6, 1)) == {}


# ---------------------------------------------------------------- period_end


def test_period_end_branches() -> None:
    filed = date(2026, 6, 2)
    assert form4_holding_as_of(date(2026, 6, 1), date(2026, 5, 29), filed) == date(2026, 6, 1)
    assert form4_holding_as_of(date(2026, 6, 2), date(2026, 6, 2), filed) == date(2026, 6, 2)
    # A future signature falls back to the period of report.
    assert form4_holding_as_of(date(2026, 7, 1), date(2026, 5, 29), filed) == date(2026, 5, 29)
    # A signature before the period of report falls back too.
    assert form4_holding_as_of(date(2026, 5, 1), date(2026, 5, 29), filed) == date(2026, 5, 29)
    assert form4_holding_as_of(None, date(2026, 5, 29), filed) == date(2026, 5, 29)
    # A future period of report, or none, gives no date.
    assert form4_holding_as_of(date(2026, 6, 1), date(2026, 6, 3), filed) is None
    assert form4_holding_as_of(date(2026, 6, 1), None, filed) is None


def test_the_filed_date_is_the_utc_date() -> None:
    # 23:30 in New York is the next day in UTC: a signature dated that next day is in bounds.
    nxt = _SIG + timedelta(days=1)
    late = datetime.combine(nxt, time(3, 30), tzinfo=UTC)
    out = _run([_H(_d("5"))], filed_at=late, signature=nxt)
    assert out["D"] == ("one_line", _d("5"), nxt)


def test_a_group_without_a_date_writes_nothing() -> None:
    assert _run([_H(_d("5"))], period=_SIG + timedelta(days=1)) == {"D": ("one_line", None, None)}
