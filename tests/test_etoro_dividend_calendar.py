from datetime import date

from app.services.etoro_dividend_calendar import CalendarRow, page_is_complete, parse_calendar


def _row(ticker: str, ex: str, cells: str) -> str:
    # The shape served on 2026-10-16 (``etoro_dividend_calendar_captures``), trimmed of logo and watchlist markup.
    return f"""
    <tr class="actual initial" data-report-date="2026-10-29">
        <td data-company-name="X"><a class="wp-block-etoro-ed-company" href="/markets/{ticker.lower()}">
            <div class="wp-block-etoro-ed-company__name">
                <span class="wp-block-etoro-ed-company__ticker">{ticker}</span>
                <span class="wp-block-etoro-ed-company__full-name">X</span>
            </div></a></td>
        <td>Technology</td>
        <td class="wp-block-etoro-ed-date-field" data-ex-dividend-date="{ex}">{cells}
        <td class="wp-block-etoro-ed-add-to-watchlist-container"><a href="/watchlists">+</a></td>
    </tr>"""


def test_parses_ticker_dates_and_the_periodic_amount() -> None:
    html = _row("MU", "2026-10-14", '2026-10-14</td><td class="d">2026-10-29</td><td>0.6</td><td>0.15</td>')
    page = parse_calendar(html)
    assert page.unparsed == 0
    assert page.rows == (CalendarRow("MU", date(2026, 10, 14), date(2026, 10, 29), 0.15),)


def test_upper_cases_the_ticker_to_match_instrument_symbols() -> None:
    html = _row("stx.us", "2026-10-14", "2026-10-14</td><td>2026-10-29</td><td>2.96</td><td>0.74</td>")
    assert parse_calendar(html).rows[0].ticker == "STX.US"


def test_counts_rather_than_guesses_a_malformed_row() -> None:
    bad_amount = _row("A", "2026-10-14", "2026-10-14</td><td>2026-10-29</td><td>1.0</td><td>n/a</td>")
    date_disagrees = _row("B", "2026-10-14", "2026-10-15</td><td>2026-10-29</td><td>1.0</td><td>0.25</td>")
    missing_cell = _row("C", "2026-10-14", "2026-10-14</td><td>2026-10-29</td><td>0.25</td>")
    good = _row("D", "2026-10-14", "2026-10-14</td><td>2026-10-29</td><td>1.0</td><td>0.25</td>")
    page = parse_calendar(bad_amount + date_disagrees + missing_cell + good)
    assert page.unparsed == 3
    assert [r.ticker for r in page.rows] == ["D"]


def test_a_day_is_covered_only_when_every_marker_parsed() -> None:
    good = parse_calendar(_row("D", "2026-10-14", "2026-10-14</td><td>2026-10-29</td><td>1.0</td><td>0.25</td>"))
    bad = parse_calendar(_row("A", "2026-10-14", "2026-10-14</td><td>2026-10-29</td><td>1.0</td><td>n/a</td>"))
    assert page_is_complete(good, 1)
    assert not page_is_complete(good, 2)  # a row the parser did not see at all (drift)
    assert not page_is_complete(bad, 1)
    assert not page_is_complete(parse_calendar(""), 0)  # the empty page stored before its job fails
