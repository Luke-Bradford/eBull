"""Unit tests for ``app.services.dividend_calendar`` (#434).

Parser-level tests only — the ingester's DB path is covered in
``test_dividend_calendar_ingest.py`` once the ingester lands.

Fixtures are stripped-down excerpts of real SEC 8-K Item 8.01
announcement language observed in Dividend Aristocrats filings
(KO, PG, JNJ, MMM). Keeping them as inline strings (rather than
on-disk HTML fixtures) means the tests exercise the parser against
the exact shapes we expect to see in production without dragging
vendor HTML into the repo.
"""

from __future__ import annotations

from datetime import date

import pytest

from app.services.dividend_calendar import (
    DividendAnnouncement,
    parse_dividend_announcement,
)


class TestParseDividendAnnouncement:
    """Golden-path + boundary tests for the 8-K 8.01 regex parser."""

    def test_full_calendar_with_all_four_dates_and_amount(self) -> None:
        """Canonical Coca-Cola style: every date + amount labelled."""
        text = (
            "On February 15, 2024, the Board of Directors of The "
            "Coca-Cola Company declared a regular quarterly cash "
            "dividend of $0.485 per share, payable on April 1, 2024, "
            "to shareholders of record as of March 15, 2024. The "
            "ex-dividend date is March 14, 2024."
        )
        result = parse_dividend_announcement(text)
        assert result == DividendAnnouncement(
            declaration_date=date(2024, 2, 15),
            ex_date=date(2024, 3, 14),
            record_date=date(2024, 3, 15),
            pay_date=date(2024, 4, 1),
            dps_declared="0.485",
        )

    def test_dollar_amount_with_four_decimal_places(self) -> None:
        """Some issuers quote fractional cents (e.g. MMM specialties)."""
        text = (
            "The Board declared a quarterly dividend of $1.5100 per "
            "share, payable June 12, 2024 to shareholders of record "
            "at the close of business on May 24, 2024."
        )
        result = parse_dividend_announcement(text)
        assert result is not None
        assert result.dps_declared == "1.5100"
        assert result.pay_date == date(2024, 6, 12)
        assert result.record_date == date(2024, 5, 24)

    def test_record_date_without_separate_ex_date(self) -> None:
        """Many 8.01s skip the ex-date line (investors derive it from
        record date − 1 business day). Parser must NOT invent one."""
        text = (
            "Procter & Gamble's Board declared a quarterly cash "
            "dividend of $1.0065 per share, payable on May 15, 2024, "
            "to shareholders of record on April 19, 2024."
        )
        result = parse_dividend_announcement(text)
        assert result is not None
        assert result.ex_date is None
        assert result.record_date == date(2024, 4, 19)
        assert result.pay_date == date(2024, 5, 15)
        assert result.dps_declared == "1.0065"

    def test_numeric_date_format(self) -> None:
        """Some smaller filers write dates as MM/DD/YYYY."""
        text = (
            "The Company declared a cash dividend of $0.22 per share "
            "payable on 06/14/2024 to stockholders of record at the "
            "close of business on 05/17/2024. The ex-dividend date "
            "will be 05/16/2024."
        )
        result = parse_dividend_announcement(text)
        assert result is not None
        assert result.ex_date == date(2024, 5, 16)
        assert result.record_date == date(2024, 5, 17)
        assert result.pay_date == date(2024, 6, 14)
        assert result.dps_declared == "0.22"

    def test_special_dividend_recognised(self) -> None:
        """Special / one-time dividends use the same date + amount
        shape; parser must not bail just because "special" appears."""
        text = (
            "On November 1, 2023, the Board declared a special cash "
            "dividend of $10.00 per share, payable December 15, 2023 "
            "to shareholders of record on December 1, 2023."
        )
        result = parse_dividend_announcement(text)
        assert result is not None
        assert result.dps_declared == "10.00"
        assert result.pay_date == date(2023, 12, 15)
        assert result.record_date == date(2023, 12, 1)

    def test_non_dividend_8k_returns_none(self) -> None:
        """Item 8.01 is "Other Events" and covers non-dividend news
        (share buyback updates, litigation, etc.). Parser must return
        None when no dividend language appears — a row with nothing
        but NULLs would clutter the table."""
        text = (
            "On March 1, 2024, the Company announced that it has "
            "entered into a material definitive agreement with XYZ "
            "Corporation regarding a joint venture in the European "
            "market. Additional details are included as Exhibit 99.1."
        )
        assert parse_dividend_announcement(text) is None

    def test_buyback_announcement_returns_none(self) -> None:
        """Buyback authorisations mention "per share" and dollar
        amounts — must not be mistaken for a dividend."""
        text = (
            "The Board authorised the repurchase of up to $5.0 "
            "billion of the Company's common stock. The repurchase "
            "program has no fixed expiration date."
        )
        assert parse_dividend_announcement(text) is None

    def test_amount_only_no_dates(self) -> None:
        """Partial parse is allowed — if the amount is present but no
        date survives the regex, return a record with amount only."""
        text = (
            "The Board declared a quarterly cash dividend of $0.25 "
            "per share. Details regarding the record date and "
            "payment date will be disclosed in a subsequent filing."
        )
        result = parse_dividend_announcement(text)
        assert result is not None
        assert result.dps_declared == "0.25"
        assert result.ex_date is None
        assert result.record_date is None
        assert result.pay_date is None

    def test_html_tags_stripped_before_matching(self) -> None:
        """Primary documents are HTML. Parser must tolerate tags
        embedded within its target phrases (<b>, <i>, <span>, etc.)
        by stripping tags before matching."""
        text = (
            "<p>On <b>February 15, 2024</b>, the Board declared a "
            "regular quarterly cash dividend of "
            "<span>$0.485 per share</span>, payable "
            "<i>April 1, 2024</i>, to shareholders of record as of "
            "March 15, 2024.</p>"
        )
        result = parse_dividend_announcement(text)
        assert result is not None
        assert result.dps_declared == "0.485"
        assert result.pay_date == date(2024, 4, 1)
        assert result.record_date == date(2024, 3, 15)

    def test_whitespace_and_nbsp_between_label_and_date(self) -> None:
        """EDGAR HTML heavily uses &nbsp; and multi-space indentation.
        Parser must treat nbsp + multi-space as a single boundary."""
        text = (
            "The Board declared a quarterly cash dividend of "
            "$0.485 per share, payable on  May   10,"
            "   2024, to shareholders of record on "
            "April 19, 2024."
        )
        result = parse_dividend_announcement(text)
        assert result is not None
        assert result.pay_date == date(2024, 5, 10)
        assert result.record_date == date(2024, 4, 19)
        assert result.dps_declared == "0.485"

    def test_empty_string_returns_none(self) -> None:
        """Degenerate input (empty primary document) must not crash."""
        assert parse_dividend_announcement("") is None

    @pytest.mark.parametrize(
        "text",
        [
            "dividend of $0.50/share",  # Slash-notation — not yet supported, accepted limitation.
            "dividend of £0.85 per share",  # Foreign currency — bail, matches USD-only
        ],
    )
    def test_known_limitations_do_not_crash(self, text: str) -> None:
        """Documenting known parser blind spots as non-crashing. They
        return None (not a row with amount=None) — deliberate so
        operators aren't misled into thinking parsing succeeded."""
        # Current regex is USD-only and requires "$N.NN per share" with
        # either hyphen or space. Both inputs fail gracefully rather
        # than matching half the announcement.
        assert parse_dividend_announcement(text) is None
