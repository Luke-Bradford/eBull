"""Unit tests for the SEC 13F-HR XML parser (#925 — EdgarTools wrapper).

Two flavours of test:

  * **Hand-crafted XML fixtures** exercise wrapper-level behaviour we
    layer on top of EdgarTools — CIK extraction, signature-date
    timezone coercion, ``putCall`` normalisation, ``Type`` code
    passthrough (``SH`` / ``PRN``), empty-CUSIP drop, and the
    :func:`dominant_voting_authority` helper.
  * **Golden-file replay** at the end of the file reads a real
    Berkshire Hathaway 13F-HR (2024Q3, accession
    ``0000950123-24-011775``) and asserts the parser surfaces the
    correct header + holdings totals against an independently
    verifiable cross-source figure (gurufocus / SEC EDGAR direct).
    This is the regression lock that catches EdgarTools library
    drift on a tight pin bump.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from app.providers.implementations.sec_13f import (
    ThirteenFHolding,
    dominant_voting_authority,
    parse_infotable,
    parse_primary_doc,
)

# ---------------------------------------------------------------------------
# Fixture builders
# ---------------------------------------------------------------------------


_PRIMARY_DOC_NS = "http://www.sec.gov/edgar/thirteenffiler"
_INFOTABLE_NS = "http://www.sec.gov/edgar/document/thirteenf/informationtable"


def _primary_doc(
    *,
    cik: str = "0001067983",
    name: str = "BERKSHIRE HATHAWAY INC",
    period: str = "09-30-2024",
    signature: str | None = "11-14-2024",
    table_total: str | None = "266380000000",
) -> str:
    """Hand-crafted primary_doc.xml mirroring SEC's namespace + shape.

    Real SEC primary_doc.xml carries:

      * ``periodOfReport`` inside ``filerInfo`` (mandatory).
      * ``reportCalendarOrQuarter`` inside ``coverPage`` (also present
        for legacy reasons; same date).
      * a ``filingManager > address`` block (mandatory for EdgarTools).
      * a ``signatureBlock`` (mandatory for EdgarTools, even when
        ``signatureDate`` itself is missing).

    The builder pins all three so EdgarTools' parser does not abort
    early on otherwise-defensible fixtures.
    """
    sig_block = f"<signatureDate>{signature}</signatureDate>" if signature else ""
    summary = (
        f"<summaryPage><tableValueTotal>{table_total}</tableValueTotal></summaryPage>"
        if table_total is not None
        else ""
    )
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<edgarSubmission xmlns="{_PRIMARY_DOC_NS}">
  <headerData>
    <filerInfo>
      <filer>
        <credentials>
          <cik>{cik}</cik>
        </credentials>
      </filer>
      <periodOfReport>{period}</periodOfReport>
    </filerInfo>
  </headerData>
  <formData>
    <coverPage>
      <reportCalendarOrQuarter>{period}</reportCalendarOrQuarter>
      <filingManager>
        <name>{name}</name>
        <address>
          <street1>3555 Farnam Street</street1>
          <city>Omaha</city>
          <stateOrCountry>NE</stateOrCountry>
          <zipCode>68131</zipCode>
        </address>
      </filingManager>
    </coverPage>
    {summary}
    <signatureBlock>
      <name>SIGNER</name>
      {sig_block}
    </signatureBlock>
  </formData>
</edgarSubmission>
"""


def _infotable_row(
    *,
    cusip: str,
    name: str = "APPLE INC",
    title: str = "COM",
    value: str = "69900000",
    shares: str = "300000000",
    shares_type: str = "SH",
    put_call: str | None = None,
    discretion: str = "SOLE",
    sole: str = "300000000",
    shared: str = "0",
    none: str = "0",
) -> str:
    put_call_xml = f"<putCall>{put_call}</putCall>" if put_call is not None else ""
    return f"""<infoTable>
  <nameOfIssuer>{name}</nameOfIssuer>
  <titleOfClass>{title}</titleOfClass>
  <cusip>{cusip}</cusip>
  <value>{value}</value>
  <shrsOrPrnAmt>
    <sshPrnamt>{shares}</sshPrnamt>
    <sshPrnamtType>{shares_type}</sshPrnamtType>
  </shrsOrPrnAmt>
  {put_call_xml}
  <investmentDiscretion>{discretion}</investmentDiscretion>
  <votingAuthority>
    <Sole>{sole}</Sole>
    <Shared>{shared}</Shared>
    <None>{none}</None>
  </votingAuthority>
</infoTable>"""


def _infotable(rows: list[str]) -> str:
    body = "\n  ".join(rows)
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<informationTable xmlns="{_INFOTABLE_NS}">
  {body}
</informationTable>
"""


# ---------------------------------------------------------------------------
# parse_primary_doc — wrapper-level behaviour
# ---------------------------------------------------------------------------


class TestParsePrimaryDoc:
    def test_round_trip_typical_filing(self) -> None:
        info = parse_primary_doc(_primary_doc())
        assert info.cik == "0001067983"
        assert info.name == "BERKSHIRE HATHAWAY INC"
        assert info.period_of_report == date(2024, 9, 30)
        assert info.filed_at == datetime(2024, 11, 14, tzinfo=UTC)
        # filed_at must be tz-aware so it lands in TIMESTAMPTZ
        # without psycopg falling back to the server's local zone.
        assert info.filed_at is not None and info.filed_at.tzinfo is UTC
        assert info.table_value_total_usd == Decimal("266380000000")

    def test_missing_signature_date_returns_none_filed_at(self) -> None:
        info = parse_primary_doc(_primary_doc(signature=None))
        assert info.filed_at is None
        # Other fields still populate.
        assert info.cik == "0001067983"
        assert info.period_of_report == date(2024, 9, 30)

    def test_missing_table_value_total_is_optional(self) -> None:
        info = parse_primary_doc(_primary_doc(table_total=None))
        assert info.table_value_total_usd is None

    def test_zero_total_value_collapses_to_none(self) -> None:
        """EdgarTools defaults a missing summaryPage total to 0; our
        wrapper collapses that back to NULL because zero is not a
        meaningful filer-reported total."""
        info = parse_primary_doc(_primary_doc(table_total="0"))
        assert info.table_value_total_usd is None

    def test_zero_pads_short_cik(self) -> None:
        """SEC CIKs are 10-digit; some filings carry the unpadded form."""
        info = parse_primary_doc(_primary_doc(cik="1067983"))
        assert info.cik == "0001067983"

    def test_strips_non_numeric_cik_chars(self) -> None:
        """Defensive: a future filer / agent might prefix CIK with 'CIK'."""
        info = parse_primary_doc(_primary_doc(cik="CIK0001067983"))
        assert info.cik == "0001067983"

    def test_missing_cik_raises_value_error(self) -> None:
        broken = _primary_doc().replace("<cik>0001067983</cik>", "")
        with pytest.raises(ValueError, match="missing a <cik>"):
            parse_primary_doc(broken)

    def test_missing_filing_manager_name_raises_value_error(self) -> None:
        broken = _primary_doc().replace("<name>BERKSHIRE HATHAWAY INC</name>", "<name></name>")
        with pytest.raises(ValueError, match="filingManager"):
            parse_primary_doc(broken)


# ---------------------------------------------------------------------------
# parse_infotable — wrapper-level behaviour
# ---------------------------------------------------------------------------


class TestParseInfotable:
    def test_single_holding_round_trip(self) -> None:
        xml = _infotable([_infotable_row(cusip="037833100")])
        holdings = parse_infotable(xml)
        assert len(holdings) == 1
        h = holdings[0]
        assert h.cusip == "037833100"
        assert h.name_of_issuer == "APPLE INC"
        assert h.title_of_class == "COM"
        assert h.value_usd == Decimal("69900000")
        assert h.shares_or_principal == Decimal("300000000")
        assert h.shares_or_principal_type == "SH"
        assert h.put_call is None
        assert h.investment_discretion == "SOLE"
        assert h.voting_sole == Decimal("300000000")
        assert h.voting_shared == Decimal(0)
        assert h.voting_none == Decimal(0)

    def test_multiple_holdings(self) -> None:
        xml = _infotable(
            [
                _infotable_row(cusip="037833100", name="APPLE INC"),
                _infotable_row(cusip="594918104", name="MICROSOFT CORP", value="42000000"),
                _infotable_row(cusip="023135106", name="AMAZON COM INC", value="36000000"),
            ]
        )
        holdings = parse_infotable(xml)
        assert len(holdings) == 3
        cusips = [h.cusip for h in holdings]
        assert cusips == ["037833100", "594918104", "023135106"]

    def test_put_call_normalises_to_uppercase(self) -> None:
        xml = _infotable(
            [
                _infotable_row(cusip="037833100", put_call="Put"),
                _infotable_row(cusip="037833101", put_call="CALL"),
                _infotable_row(cusip="037833102", put_call="put"),
            ]
        )
        holdings = parse_infotable(xml)
        assert holdings[0].put_call == "PUT"
        assert holdings[1].put_call == "CALL"
        assert holdings[2].put_call == "PUT"

    def test_unknown_put_call_falls_back_to_none(self) -> None:
        """A future schema field smuggled into putCall must not corrupt
        the constrained Literal — fall back to None and warn."""
        xml = _infotable([_infotable_row(cusip="037833100", put_call="Straddle")])
        holdings = parse_infotable(xml)
        assert holdings[0].put_call is None

    def test_voting_authority_breakdown_preserved(self) -> None:
        xml = _infotable(
            [
                _infotable_row(
                    cusip="037833100",
                    sole="200000000",
                    shared="50000000",
                    none="50000000",
                )
            ]
        )
        h = parse_infotable(xml)[0]
        assert h.voting_sole == Decimal("200000000")
        assert h.voting_shared == Decimal("50000000")
        assert h.voting_none == Decimal("50000000")

    def test_principal_amount_holding(self) -> None:
        """Bond holdings report PRN, not SH, in sshPrnamtType. The
        wrapper preserves the SEC two-letter code even though
        EdgarTools relabels it to ``Principal``."""
        xml = _infotable([_infotable_row(cusip="912828YT0", shares="100000", shares_type="PRN")])
        h = parse_infotable(xml)[0]
        assert h.shares_or_principal == Decimal("100000")
        assert h.shares_or_principal_type == "PRN"

    def test_empty_cusip_row_is_dropped(self) -> None:
        """An infotable row with an empty CUSIP cannot resolve to an
        instrument downstream. Drop it so the unresolvable holding
        does not silently appear with a blank identifier."""
        xml = _infotable(
            [
                _infotable_row(cusip="037833100"),
                _infotable_row(cusip=""),
                _infotable_row(cusip="594918104"),
            ]
        )
        holdings = parse_infotable(xml)
        cusips = [h.cusip for h in holdings]
        assert cusips == ["037833100", "594918104"]

    def test_both_zero_value_and_shares_row_is_dropped(self) -> None:
        """EdgarTools' XML parser falls back to ``0`` when ``<value>``
        or ``<sshPrnamt>`` is missing rather than raising. A genuine
        13F-HR row always carries at least one positive numeric
        column — a both-zero row is malformed input that the bespoke
        parser this wrapper replaces would have dropped via
        ``_decimal_or_none`` returning ``None``. Codex pre-push
        finding."""
        xml = _infotable(
            [
                _infotable_row(cusip="037833100"),
                _infotable_row(cusip="594918104", value="0", shares="0", sole="0"),
                _infotable_row(cusip="023135106"),
            ]
        )
        holdings = parse_infotable(xml)
        cusips = [h.cusip for h in holdings]
        assert cusips == ["037833100", "023135106"]

    def test_empty_infotable_returns_empty_list(self) -> None:
        xml = _infotable([])
        assert parse_infotable(xml) == []


# ---------------------------------------------------------------------------
# dominant_voting_authority — pure logic, lib-independent
# ---------------------------------------------------------------------------


class TestDominantVotingAuthority:
    def _holding(self, *, sole: int, shared: int, none: int) -> ThirteenFHolding:
        return ThirteenFHolding(
            cusip="037833100",
            name_of_issuer="APPLE INC",
            title_of_class="COM",
            value_usd=Decimal("69900000"),
            shares_or_principal=Decimal("300000000"),
            shares_or_principal_type="SH",
            put_call=None,
            investment_discretion="SOLE",
            voting_sole=Decimal(sole),
            voting_shared=Decimal(shared),
            voting_none=Decimal(none),
        )

    def test_sole_dominant(self) -> None:
        h = self._holding(sole=300, shared=10, none=0)
        assert dominant_voting_authority(h) == "SOLE"

    def test_shared_dominant(self) -> None:
        h = self._holding(sole=10, shared=300, none=20)
        assert dominant_voting_authority(h) == "SHARED"

    def test_none_dominant(self) -> None:
        h = self._holding(sole=0, shared=0, none=500)
        assert dominant_voting_authority(h) == "NONE"

    def test_all_zero_returns_none_label(self) -> None:
        """All-zero is rare but legal in the SEC schema (filing
        manager has no voting authority over the position). Maps to
        NULL on insert via the canonical column."""
        h = self._holding(sole=0, shared=0, none=0)
        assert dominant_voting_authority(h) is None

    def test_tie_prefers_sole(self) -> None:
        """When sole == shared, sole wins. Documented preference for
        ownership-card reporting consistency."""
        h = self._holding(sole=100, shared=100, none=0)
        assert dominant_voting_authority(h) == "SOLE"


# ---------------------------------------------------------------------------
# Golden-file replay — Berkshire Hathaway 13F-HR 2024Q3
# ---------------------------------------------------------------------------


_FIXTURE_DIR = Path(__file__).parent / "fixtures" / "sec" / "13f"


class TestBerkshireGoldenFile:
    """Lock the EdgarTools-driven parser against a real 13F-HR.

    Source: SEC EDGAR accession ``0000950123-24-011775`` (filed
    2024-11-14, period 2024-09-30). Cross-source-verified via SEC
    EDGAR direct: 121 holdings, total table value $266,378,900,503.
    """

    def test_primary_doc_round_trip(self) -> None:
        xml = (_FIXTURE_DIR / "berkshire_2024q3_primary_doc.xml").read_text()
        info = parse_primary_doc(xml)
        assert info.cik == "0001067983"
        assert info.name == "Berkshire Hathaway Inc"
        assert info.period_of_report == date(2024, 9, 30)
        assert info.filed_at == datetime(2024, 11, 14, tzinfo=UTC)
        assert info.table_value_total_usd == Decimal("266378900503")

    def test_infotable_holdings_count_and_total(self) -> None:
        xml = (_FIXTURE_DIR / "berkshire_2024q3_infotable.xml").read_text()
        holdings = parse_infotable(xml)
        # SEC summaryPage tableEntryTotal = 121.
        assert len(holdings) == 121
        # Sum of value column = SEC summaryPage tableValueTotal.
        # Lib-drift regression: any future EdgarTools release that
        # changes value-column semantics or skips a row trips this.
        total = sum((h.value_usd for h in holdings), start=Decimal(0))
        assert total == Decimal("266378900503")

    def test_infotable_first_row_shape(self) -> None:
        """Lock first-row contents (alphabetically-first ALLY FINL
        INC) against EdgarTools' output. If a future version of the
        library renames a column or shifts semantics, this trips."""
        xml = (_FIXTURE_DIR / "berkshire_2024q3_infotable.xml").read_text()
        h = parse_infotable(xml)[0]
        assert h.cusip == "02005N100"
        assert h.name_of_issuer == "ALLY FINL INC"
        assert h.title_of_class == "COM"
        assert h.value_usd == Decimal("452693233")
        assert h.shares_or_principal == Decimal("12719675")
        assert h.shares_or_principal_type == "SH"
        assert h.put_call is None
        assert h.investment_discretion == "DFND"
        assert h.voting_sole == Decimal("12719675")
        assert h.voting_shared == Decimal(0)
        assert h.voting_none == Decimal(0)

    def test_infotable_does_not_call_network(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """``parse_infotable`` must not perform any HTTP at *call*
        time — EdgarTools' bundled ticker-mapping parquet plus the
        input XML are sufficient.

        Caveat (Codex pre-push finding): the offline guarantee here
        covers the parse call itself, not module import. Importing
        ``edgar`` (or any of its submodules) initialises the package
        cache, which mkdirs ``~/.edgar/_tcache``. The wrapper at
        :mod:`app.providers.implementations.sec_13f` defers that
        import until the first parse call to keep module import
        side-effect-free, but the cache directory is still created
        once per process. Tests that exercise the parse path on a
        sandboxed home should pre-set ``EDGAR_LOCAL_DATA_DIR`` (or
        ``HOME``) to a writable temp dir.

        We force-fail every outbound HTTP path EdgarTools could
        plausibly use; if the library introduces a fetch in a future
        release, this trips before the rest of the suite green-lights
        the bump.
        """
        import urllib.request

        import httpx

        def _block(*_args: object, **_kwargs: object) -> object:
            raise RuntimeError("13F parse attempted a network call — offline guarantee broken")

        monkeypatch.setattr(httpx.Client, "request", _block, raising=True)
        monkeypatch.setattr(httpx.AsyncClient, "request", _block, raising=True)
        monkeypatch.setattr(urllib.request, "urlopen", _block, raising=True)

        xml = (_FIXTURE_DIR / "berkshire_2024q3_infotable.xml").read_text()
        holdings = parse_infotable(xml)
        assert len(holdings) == 121
