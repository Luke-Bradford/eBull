"""#2282 stage 2c — Form 25 / 25-NSE parsing and provision classification.

Pure tests. Every case below is a trap from ``sec-edgar.md`` §2.6 that was
measured on the 2023 cohort, and each one silently corrupts a delisting
register in a different direction:

* counting index ROWS gives 2,437 where there are 1,282 filings;
* treating a CIK as delisted because it appears in a Form 25 marks Berkshire
  Hathaway delisted in January 2023, on the strength of two bond filings;
* treating a missing ``<ruleProvision>`` as unparseable drops all 128
  issuer-filed Form 25s, every one of which IS a delisting;
* counting ``(a)(1)`` and ``(a)(2)`` as delistings inflates 2023 by 34%.

The XML below is the real ``primary_doc.xml`` of 1Life Healthcare's
2023-02-22 25-NSE (accession 0001354457-23-000108, One Medical / Amazon),
trimmed to the elements the parser reads.
"""

from __future__ import annotations

from datetime import date

import pytest

from app.services.sec_form25_register import (
    Form25IndexRow,
    classify_provision,
    classify_security,
    clean_trading_symbol,
    normalise_provision,
    parse_index_line,
    parse_submission,
    parse_suspension_date,
)

_ONEMEDICAL_25NSE = """<?xml version="1.0"?>
<notificationOfRemoval>
    <schemaVersion>X0203</schemaVersion>
    <exchange>
        <cik>0001354457</cik>
        <entityName>Nasdaq Stock Market LLC</entityName>
    </exchange>
    <issuer>
        <cik>0001404123</cik>
        <entityName>1Life Healthcare Inc</entityName>
        <fileNumber>001-39203</fileNumber>
    </issuer>
    <descriptionClassSecurity>Common Stock</descriptionClassSecurity>
    <ruleProvision>17 CFR 240.12d2-2(a)(3)</ruleProvision>
    <signatureData>
        <signatureName>Tara Petta</signatureName>
        <signatureTitle>Director</signatureTitle>
        <signatureDate>2023-02-22</signatureDate>
    </signatureData>
</notificationOfRemoval>
"""

_INDEX_ROW = Form25IndexRow(
    form="25-NSE",
    company="1Life Healthcare Inc",
    cik="1404123",
    filed_date=date(2023, 2, 22),
    path="edgar/data/1404123/0001354457-23-000108.txt",
)


class TestIndexParsing:
    def test_parses_a_form25_row(self) -> None:
        line = (
            "25-NSE           1Life Healthcare Inc"
            "                                          "
            "1404123     2023-02-22  edgar/data/1404123/0001354457-23-000108.txt"
        )
        row = parse_index_line(line)
        assert row is not None
        assert row.form == "25-NSE"
        assert row.cik == "1404123"
        assert row.filed_date == date(2023, 2, 22)
        assert row.accession_number == "0001354457-23-000108"

    def test_regulation_a_forms_are_not_form_25(self) -> None:
        """``253G2`` starts with '25' and is a Reg-A offering circular.

        Prefix-matching instead of exact-matching inflates 2023 QTR1 from 622
        rows to 807 — and every one of the extras is an offering, i.e. the
        opposite of a delisting.
        """
        line = (
            "253G2            Some Issuer Inc"
            "                                               "
            "1234567     2023-03-01  edgar/data/1234567/0001234567-23-000001.txt"
        )
        assert parse_index_line(line) is None

    def test_accession_dedups_the_dual_cik_indexing(self) -> None:
        """EDGAR indexes a 25-NSE under BOTH the exchange CIK and the issuer CIK.

        The two rows carry different ``cik`` values and the SAME accession, so
        the accession is the only safe de-duplication key. 2023 is 2,437 rows /
        1,282 filings; de-duplicating on ``(cik, form)`` keeps both.
        """
        exchange_row = parse_index_line(
            "25-NSE           Nasdaq Stock Market LLC"
            "                                       "
            "1354457     2023-02-22  edgar/data/1354457/0001354457-23-000108.txt"
        )
        issuer_row = parse_index_line(
            "25-NSE           1Life Healthcare Inc"
            "                                          "
            "1404123     2023-02-22  edgar/data/1404123/0001354457-23-000108.txt"
        )
        assert exchange_row is not None and issuer_row is not None
        assert exchange_row.cik != issuer_row.cik
        assert exchange_row.accession_number == issuer_row.accession_number


class TestProvision:
    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("17 CFR 240.12d2-2(a)(3)", "(a)(3)"),
            ("17 CFR 240.12d2-2(b)", "(b)"),
            ("240.12d2-2 (a)(1)", "(a)(1)"),
            (None, None),
            ("", None),
        ],
    )
    def test_normalise(self, raw: str | None, expected: str | None) -> None:
        assert normalise_provision(raw) == expected

    @pytest.mark.parametrize(
        ("provision", "expected"),
        [
            # Delisting-meaning: (a)(3) merger/reorg, (a)(4) rights
            # extinguished, (b) exchange-initiated non-compliance.
            ("(a)(3)", "equity_delisting"),
            ("(a)(4)", "equity_delisting"),
            ("(b)", "equity_delisting"),
            # Debt lifecycle — 440 of 2023's 1,282 filings (34.3%). A register
            # that counts these is wrong by half.
            ("(a)(1)", "debt_lifecycle"),
            ("(a)(2)", "debt_lifecycle"),
        ],
    )
    def test_exchange_filed(self, provision: str, expected: str) -> None:
        assert classify_provision("25-NSE", provision) == expected

    def test_issuer_filed_form_25_has_no_provision_and_is_a_delisting(self) -> None:
        """Paragraph (c), voluntary withdrawal — 128 filings in 2023.

        This is the case that punishes branching on the provision alone. An
        issuer-filed Form 25 is a different document shape with no
        ``<ruleProvision>`` element, so "no provision ⇒ unparseable" silently
        discards 128 real delistings.
        """
        assert classify_provision("25", None) == "equity_delisting"
        assert classify_provision("25/A", None) == "equity_delisting"

    def test_missing_provision_on_an_exchange_filing_is_not_assumed(self) -> None:
        """An exchange filing SHOULD carry a provision. Absent one, say unknown.

        Inheriting the (c) treatment here would quietly promote a parse failure
        into a delisting.
        """
        assert classify_provision("25-NSE", None) == "unknown"


class TestSecurityClass:
    """The SECOND axis. Provision classifies the EVENT, not what came off the tape.

    Measured on 2023: provision-filtering alone gives 842 "delisting-meaning"
    filings, of which 155 are warrants, 128 issuer-filed (security unstated),
    111 ETFs/funds, 62 units, 56 preferred, 10 NOTES and 3 rights. Only **317**
    are common equity. Collapsing the two axes is why the #2282 handoff's "578
    filings / 443 issuers" could not be reproduced — its provision counts are
    exactly right, but a cohort of common-equity delistings needs the security
    too.
    """

    @pytest.mark.parametrize(
        ("description", "expected"),
        [
            ("Common Stock", "common_equity"),
            ("Class A Common Stock", "common_equity"),
            ("Ordinary Shares", "common_equity"),
            ("Warrants to purchase Common Stock", "warrant"),
            # "Unit" is typically one share PLUS one warrant, so it must be
            # tested before common — the description contains both words.
            ("Units, each consisting of one share of Class A common stock", "unit"),
            ("6.75% Series A Preferred Stock", "preferred"),
            ("Depositary Shares", "preferred"),
            # A delisting PROVISION on a DEBT security: 15 of 2023's.
            ("0.625% Senior Notes due 2023", "debt"),
            ("Rights", "right"),
            # Issuer-filed Form 25 carries no description at all.
            (None, "unknown"),
            ("", "unknown"),
        ],
    )
    def test_classify(self, description: str | None, expected: str) -> None:
        assert classify_security(description) == expected

    @pytest.mark.parametrize(
        "description",
        [
            "ConvexityShares 1x SPIKES Futures ETF",
            "iShares MSCI Russia ETF",
            "WisdomTree Trust",
        ],
    )
    def test_fund_brand_names_ending_in_shares_are_not_common_equity(self, description: str) -> None:
        """Word boundaries on BOTH sides of ``shares``.

        ``shares?\\b`` without a leading ``\\b`` matches the embedded "Shares"
        in a fund BRAND — ConvexityShares, iShares — so the fund is classified
        common_equity before the ``fund`` rule ever runs. That put SPKY
        (ConvexityShares 1x SPIKES Futures ETF) into the first draft of the
        committed cohort: an ETF inside the ex-ETF cohort, which defeats the
        filter entirely. Found by Codex on the 2c branch.
        """
        assert classify_security(description) == "fund"

    @pytest.mark.parametrize("description", ["Common Shares", "Ordinary Shares", "Class B Non-Voting Shares"])
    def test_genuine_share_descriptions_still_classify(self, description: str) -> None:
        """The boundary fix must not cost the real ones.

        Foreign private issuers and Canadian filers write "Common Shares" or
        "Ordinary Shares" where a US filer writes "Common Stock"; 12 cohort
        members use that wording.
        """
        assert classify_security(description) == "common_equity"

    def test_a_delisting_provision_on_a_note_is_not_common_equity(self) -> None:
        """The two axes are orthogonal and both are needed.

        A filing can carry (a)(3) — a real delisting event — while the security
        struck from the tape is a bond. Filtering on provision alone puts it in
        a common-equity cohort.
        """
        assert classify_provision("25-NSE", "(a)(3)") == "equity_delisting"
        assert classify_security("0.625% Senior Notes due 2023") == "debt"


class TestSubmissionParsing:
    def test_parses_the_real_25nse(self) -> None:
        filing = parse_submission(_INDEX_ROW, _ONEMEDICAL_25NSE)
        assert filing.accession_number == "0001354457-23-000108"
        assert filing.issuer_cik == "0001404123"
        assert filing.issuer_name == "1Life Healthcare Inc"
        assert filing.exchange_cik == "0001354457"
        assert filing.description_class_security == "Common Stock"
        assert filing.rule_provision == "(a)(3)"
        assert filing.provision_class == "equity_delisting"
        assert filing.signature_date == date(2023, 2, 22)

    def test_issuer_cik_is_not_the_indexed_cik(self) -> None:
        """The index row's CIK may be the EXCHANGE. Read the issuer from the XML.

        A register that trusted ``form.idx``'s CIK would attribute half its
        delistings to Nasdaq and NYSE.
        """
        exchange_indexed = Form25IndexRow(
            form="25-NSE",
            company="Nasdaq Stock Market LLC",
            cik="1354457",
            filed_date=date(2023, 2, 22),
            path="edgar/data/1354457/0001354457-23-000108.txt",
        )
        filing = parse_submission(exchange_indexed, _ONEMEDICAL_25NSE)
        assert filing.issuer_cik == "0001404123"

    def test_security_description_survives(self) -> None:
        """Trap 2 in one field: a Form 25 is per-SECURITY.

        Berkshire's two 2023 filings both name notes. Without this field a
        consumer cannot tell a common-stock delisting from a bond maturing.
        """
        bond = _ONEMEDICAL_25NSE.replace(
            "<descriptionClassSecurity>Common Stock</descriptionClassSecurity>",
            "<descriptionClassSecurity>0.625% Senior Notes due 2023</descriptionClassSecurity>",
        ).replace("(a)(3)", "(a)(2)")
        filing = parse_submission(_INDEX_ROW, bond)
        assert filing.description_class_security == "0.625% Senior Notes due 2023"
        assert filing.provision_class == "debt_lifecycle"

    def test_issuer_filed_form_25_takes_its_issuer_from_the_index_row(self) -> None:
        """Paragraph (c) filings have no ``<issuer>`` block — the FILER is the issuer.

        Reading only the XML leaves all 128 of 2023's issuer-filed Form 25s
        with a NULL issuer CIK: an entire delisting category that joins to
        nothing. The substitution is safe here and would be WRONG for a 25-NSE,
        where the index CIK is as often the exchange as the issuer.
        """
        issuer_filed = Form25IndexRow(
            form="25",
            company="AUTOSCOPE TECHNOLOGIES CORP",
            cik="943034",
            filed_date=date(2023, 1, 6),
            path="edgar/data/943034/0000897101-23-000004.txt",
        )
        filing = parse_submission(issuer_filed, "<html>not a notificationOfRemoval</html>")
        assert filing.issuer_cik == "0000943034"
        assert filing.issuer_name == "AUTOSCOPE TECHNOLOGIES CORP"
        assert filing.provision_class == "equity_delisting"
        # ...but the SECURITY is unverifiable, so it is not cohort-eligible.
        assert filing.security_class == "unknown"

    def test_exchange_filing_without_an_issuer_block_stays_null(self) -> None:
        """The index CIK of a 25-NSE may be the exchange. Never substitute it.

        Doing so would attribute delistings to Nasdaq and NYSE.
        """
        filing = parse_submission(_INDEX_ROW, "<html>unparseable</html>")
        assert filing.issuer_cik is None

    def test_short_padded_cik_is_normalised(self) -> None:
        """SEC's own XML does not reliably zero-pad the CIK.

        Medtronic's issuer CIK in accession 0000876661-23-000234 is
        ``000064670`` — nine digits, where every CIK column in this schema is
        ten. Stored verbatim it would be a second spelling of CIK 64670 that
        joins to nothing. Caught by the migration's CHECK on a live harvest,
        which is the argument for having the CHECK.
        """
        short = _ONEMEDICAL_25NSE.replace("<cik>0001404123</cik>", "<cik>000064670</cik>")
        assert parse_submission(_INDEX_ROW, short).issuer_cik == "0000064670"

    def test_no_suspension_date_when_the_filing_does_not_state_one(self) -> None:
        """Most exchange filings attach a stub EX-99 and state no date.

        NULL must stay NULL. ``research_price_series.delisting_date`` is
        CHECK-paired to its source precisely so a missing suspension date is
        never back-filled from ``filed_date`` — they are different events and
        substituting one mistruncates the series.
        """
        assert parse_submission(_INDEX_ROW, _ONEMEDICAL_25NSE).suspension_date is None


class TestTradingSymbol:
    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            # ⚠ The footnote marker is INSIDE the tag, put there by the filer.
            # Verified against the live document: Invacare's 2023-02-03 8-K
            # (0000742112-23-000014) tags
            # <ix:nonNumeric name="dei:TradingSymbol">IVC*</ix:nonNumeric>.
            # NantHealth does the same with NHIQ*. Both delisted in 2023, so
            # the asterisk points at a cover-page footnote about the
            # suspension. Storing IVC* would fail every downstream join AND
            # make the vendor acceptance test score a miss against a vendor
            # that served the name correctly.
            ("IVC*", "IVC"),
            ("NHIQ*", "NHIQ"),
            ("  aapl ", "AAPL"),
            # Class suffixes are part of the ticker and must survive.
            ("BRK.B", "BRK.B"),
            ("BRK-A", "BRK-A"),
            ("GOOGL", "GOOGL"),
            (None, None),
            ("*", None),
        ],
    )
    def test_clean(self, raw: str | None, expected: str | None) -> None:
        assert clean_trading_symbol(raw) == expected


class TestSuspensionDate:
    def test_reads_the_berkshire_sentence(self) -> None:
        """§2.6 trap 5 — three dates in one filing, and only one is the last
        tradable day."""
        text = (
            "the Exchange stated its intention to remove the security at the "
            "opening of business on January 30, 2023; the security was "
            "redeemed or paid at maturity on January 17, 2023; this security "
            "was suspended from trading on January 17, 2023."
        )
        assert parse_suspension_date(text) == date(2023, 1, 17)

    def test_removal_effective_date_is_not_mistaken_for_suspension(self) -> None:
        """Removal-effective is a LATER, different event. Taking it would leave
        a series running past its last tradable day."""
        text = "intention to remove the security at the opening of business on January 30, 2023."
        assert parse_suspension_date(text) is None

    def test_no_match_returns_none(self) -> None:
        assert parse_suspension_date("Onem-form25") is None
