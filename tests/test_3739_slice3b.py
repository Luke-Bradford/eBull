"""#3739 slice 3b: the §3.3 deterministic checker (``scripts/build_3739_slice3b.py``)."""

from __future__ import annotations

import csv
import gzip
import hashlib
from datetime import date
from fractions import Fraction
from pathlib import Path

import pytest

from scripts import build_3739_slice3b as s3b

ACCESSION = "0001045810-24-000144"
ACCEPTANCE = "20240607161934"
COVER = "Common Stock, $0.001 par value per share NVDA The Nasdaq Global Select Market"


def split(**overrides: str) -> dict[str, str]:
    values = {
        "cik": "0001045810",
        "class_title": "Common Stock, $0.001 par value per share",
        "class_symbol": "NVDA",
        "effective_date": "2024-06-10",
        "ratio": "10",
        "record_date": "",
        "payable_date": "",
        "effective_basis": "stated",
        "status": "confirmed",
    }
    return values | overrides


# --------------------------------------------------------------------------- stated values


@pytest.mark.parametrize(
    ("text", "ratio"),
    [
        ("announced a ten-for-one forward stock split", Fraction(10)),
        ("a 1-for-10 reverse stock split", Fraction(1, 10)),
        ("a three (3)-for-one (1) split", Fraction(3)),
        ("a three-for-two stock split", Fraction(3, 2)),
        ("one-for-one hundred", Fraction(1, 100)),
        ("a 1 – for – 20 reverse split", Fraction(1, 20)),  # en dashes, spaced
        ("a 1,000-for-1 split", Fraction(1000)),
        ("a split ratio of 10:1", Fraction(10)),
        ("twenty-five-for-one", Fraction(25)),
    ],
)
def test_the_ratio_grammar_reads_new_shares_for_old(text: str, ratio: Fraction) -> None:
    assert ratio in {value for value, _, _ in s3b.ratio_spans(text)}


@pytest.mark.parametrize(
    ("text", "ratio"),
    [
        ("a 5% stock dividend", Fraction(21, 20)),
        ("a five percent stock dividend", Fraction(21, 20)),
        ("a 100 per cent stock dividend", Fraction(2)),
        ("three additional shares for each share held", Fraction(4)),
        ("one additional share of common stock for each one share outstanding", Fraction(2)),
        ("0.5 additional shares per share", Fraction(3, 2)),
    ],
)
def test_amendment_1_reads_percentage_and_additional_share_ratios(text: str, ratio: Fraction) -> None:
    assert ratio in {value for value, _, _ in s3b.ratio_spans(text)}


def test_a_percentage_is_a_ratio_only_beside_split_words() -> None:
    assert s3b.stated_ratios("declared a 5% stock dividend payable December 14") == {Fraction(21, 20)}
    assert s3b.stated_ratios("issued 4.5% notes due 2030") == set()
    assert s3b.stated_ratios("holders approved the plan by 95%") == set()
    # A percentage beside split words that sizes something else is no ratio.
    assert s3b.stated_ratios("Following the 1-for-10 reverse stock split, shares decreased by 90%") == {Fraction(1, 10)}
    assert s3b.stated_ratios("The stock split occurred; ownership remained 5%.") == set()


def test_words_and_a_disagreeing_bracketed_numeral_state_no_ratio() -> None:
    assert s3b.ratio_spans("a three (4)-for-one split") == []


def test_text_with_no_ratio_states_none() -> None:
    assert s3b.ratio_spans("shares for issuance under the plan") == []


def test_a_ratio_is_stated_only_beside_split_words() -> None:
    assert s3b.stated_ratios("a ten-for-one forward stock split") == {Fraction(10)}
    assert s3b.stated_ratios("approved by a margin of 4:1 at the annual meeting") == set()
    assert s3b.stated_ratios("holders receive one for one in the merger") == set()
    assert s3b.stated_ratios("a 1-for-20 reverse split") == {Fraction(1, 20)}


def test_a_clock_time_is_not_a_colon_ratio() -> None:
    assert s3b.ratio_spans("effective at 4:01 p.m. Eastern Time") == []
    assert s3b.ratio_spans("effective at 10:30 am") == []
    assert s3b.ratio_spans("the split is effective at 16:01 Eastern") == []
    assert s3b.ratio_spans("the split is effective at 00:01 ET") == []


def test_a_cash_distribution_or_business_combination_is_not_split_terms() -> None:
    assert s3b.stated_ratios("in the business combination, holders receive one for one") == set()
    assert s3b.stated_ratios("a cash distribution approved 4:1 by holders") == set()
    assert s3b.stated_ratios("a combination of the outstanding shares on a 1-for-10 basis") == {Fraction(1, 10)}
    assert s3b.stated_ratios("a 5% stock dividend, or 21-for-20") == {Fraction(21, 20)}


@pytest.mark.parametrize(
    "text",
    ["June 10, 2024", "June 10 2024", "Jun. 10, 2024", "10th June 2024", "10 of June, 2024", "2024-06-10", "6/10/2024"],
)
def test_stated_dates_read_the_common_forms(text: str) -> None:
    assert s3b.stated_dates(f"effective on {text}.") == {date(2024, 6, 10)}


def test_an_impossible_date_is_not_stated() -> None:
    assert s3b.stated_dates("February 30, 2024") == set()


def test_a_symbol_is_a_token_not_a_substring() -> None:
    assert s3b.names_symbol("under the symbol NVDA.", "NVDA")
    assert not s3b.names_symbol("under the symbol NVDA.", "NV")
    assert not s3b.names_symbol("anything", "")


# --------------------------------------------------------------------------- document text


def test_document_text_strips_tags_entities_and_hidden_blocks() -> None:
    raw = (
        b"<html><style>p{}</style><!-- a ten-for-one comment --><p>a&#160;ten-for-<b>one</b></p>"
        b"<script>x</script><td>June</td><td>10, 2024</td></html>"
    )
    text = s3b.document_text(raw, "a.htm")
    assert text == "a ten-for- one June 10, 2024"
    assert "comment" not in text and "p{}" not in text


def test_a_quote_matches_across_a_tag_boundary_with_whitespace_removed() -> None:
    text = s3b.document_text(b"<p>ten-for-<b>one</b> split</p>", "a.htm")
    assert s3b.compact("ten-for-one split") in s3b.compact(text)


def test_a_plain_text_document_keeps_angle_brackets() -> None:
    assert s3b.document_text(b"ratio <1 per share", "a.txt") == "ratio <1 per share"


def test_a_cp1252_document_decodes() -> None:
    assert s3b.document_text("Company’s".encode("cp1252"), "a.txt") == "Company’s"


# --------------------------------------------------------------------------- per-type field requirements


SPLIT_QUOTES = [
    "announced a ten-for-one forward stock split",
    "Trading is expected to commence on a split-adjusted basis at market open on June 10, 2024.",
    COVER,
]
NVDA_COVER = ("Common Stock, $0.001 par value per share", "NVDA")
NO_DATE_ROLE = (
    "no quote states effective_date {} as the adjusted-basis session, nor record_date and payable_date in their roles"
)
NO_CLASS = ["neither an action quote nor a single-class cover names the class title or symbol"]


def test_a_split_stating_ratio_date_and_class_passes() -> None:
    assert s3b.field_failures("split", split(), SPLIT_QUOTES, [NVDA_COVER]) == []


def test_an_action_quote_names_the_class_by_title_or_symbol() -> None:
    by_symbol = ["NVDA holders receive nine additional shares in a ten-for-one split", SPLIT_QUOTES[1]]
    by_title = ["a ten-for-one split of the Common Stock, $0.001 par value per share", SPLIT_QUOTES[1]]
    assert s3b.field_failures("split", split(), by_symbol) == []
    assert s3b.field_failures("split", split(), by_title) == []


def test_a_cover_row_alone_identifies_the_class_only_on_a_single_class_cover() -> None:
    # A multi-class cover yields no single class, so its quoted 12(b) row does not say which class split.
    assert s3b.field_failures("split", split(), SPLIT_QUOTES) == NO_CLASS
    assert s3b.field_failures("split", split(), SPLIT_QUOTES, [("Class B Common Stock", "XYZ")]) == NO_CLASS


def test_a_split_whose_ratio_is_not_quoted_fails() -> None:
    failures = s3b.field_failures("split", split(ratio="2"), SPLIT_QUOTES, [NVDA_COVER])
    assert failures == ["no quote states the ratio 2"]


def test_the_effective_date_must_be_quoted_in_its_role() -> None:
    # June 7 is quoted, but as the amendment's date, not the adjusted-basis session.
    quotes = [*SPLIT_QUOTES, "The Amendment became effective at 4:01 p.m. Eastern Time on June 7, 2024."]
    failures = s3b.field_failures("split", split(effective_date="2024-06-07"), quotes, [NVDA_COVER])
    assert failures == [NO_DATE_ROLE.format("2024-06-07")]
    bare = ["a ten-for-one split", "June 10, 2024", COVER]
    assert s3b.field_failures("split", split(), bare, [NVDA_COVER]) == [NO_DATE_ROLE.format("2024-06-10")]


def test_a_large_distribution_goes_ex_the_session_after_payable() -> None:
    # FINRA 11140(b)(2): payable Friday 2024-06-07, so ex Monday 2024-06-10.
    quotes = ["a ten-for-one split", "to holders of record on June 6, 2024.", "The shares are payable on June 7, 2024."]
    values = split(record_date="2024-06-06", payable_date="2024-06-07", effective_basis="fallback_b2")
    assert s3b.field_failures("split", values, quotes, [NVDA_COVER]) == []
    wrong = s3b.field_failures("split", values | {"effective_date": "2024-06-06"}, quotes, [NVDA_COVER])
    assert wrong == ["effective_date 2024-06-06 is not 2024-06-10, the FINRA 11140 date from record and payable dates"]


def test_record_and_payable_dates_must_be_quoted_in_their_roles() -> None:
    swapped = [
        "a ten-for-one split",
        "to holders of record on June 6, 2024.",
        "The shares are payable on June 7, 2024.",
    ]
    values = split(record_date="2024-06-07", payable_date="2024-06-06")
    assert s3b.field_failures("split", values, swapped, [NVDA_COVER]) == [NO_DATE_ROLE.format("2024-06-10")]


def test_the_session_after_payable_skips_an_nyse_holiday() -> None:
    assert s3b.next_session(date(2024, 7, 3)) == date(2024, 7, 5)  # Independence Day


def test_a_small_distribution_goes_ex_by_the_b1_text_in_force_on_the_record_date() -> None:
    # FINRA 11140(b)(1), a distribution < 25%: from 2024-05-28 the record date itself.
    quotes = ["a 5% stock dividend", "record on June 3, 2024, payable on June 14, 2024"]
    values = split(
        ratio="21/20",
        effective_date="2024-06-03",
        record_date="2024-06-03",
        payable_date="2024-06-14",
        effective_basis="fallback_b1",
    )
    assert s3b.field_failures("split", values, quotes, [NVDA_COVER]) == []
    # Before 2024-05-28 (Regulatory Notice 17-19): the NYSE business day before the record date. Record Thursday
    # 2022-12-01, so ex Wednesday 2022-11-30.
    old = ["a 5% stock dividend", "record on December 1, 2022, payable on December 14, 2022"]
    before = values | {"record_date": "2022-12-01", "payable_date": "2022-12-14", "effective_date": "2022-11-30"}
    assert s3b.field_failures("split", before, old, [NVDA_COVER]) == []
    on_record = s3b.field_failures("split", before | {"effective_date": "2022-12-01"}, old, [NVDA_COVER])
    assert on_record == [
        "effective_date 2022-12-01 is not 2022-11-30, the FINRA 11140 date from record and payable dates"
    ]


def test_the_b1_business_day_before_the_record_date_skips_a_holiday_and_a_weekend() -> None:
    # Record Monday 2022-01-03: the session before is Friday 2021-12-31 (New Year's Day 2022 fell on a Saturday and
    # NYSE does not observe it on the Friday). Record Tuesday 2022-01-18: Monday was Martin Luther King Jr. Day.
    assert s3b.fallback_date(Fraction(21, 20), date(2022, 1, 3), date(2022, 1, 14)) == (
        date(2021, 12, 31),
        "fallback_b1",
    )
    assert s3b.fallback_date(Fraction(21, 20), date(2022, 1, 18), date(2022, 1, 28))[0] == date(2022, 1, 14)
    assert s3b.fallback_date(Fraction(5, 4), date(2022, 1, 18), date(2022, 1, 28)) == (
        date(2022, 1, 31),
        "fallback_b2",
    )


def test_the_effective_basis_must_say_how_the_quotes_set_the_date() -> None:
    assert s3b.field_failures("split", split(effective_basis="fallback_b1"), SPLIT_QUOTES, [NVDA_COVER]) == [
        "effective_basis 'fallback_b1' is not 'stated', how the quotes set effective_date"
    ]
    assert s3b.field_failures("split", split(effective_basis=""), SPLIT_QUOTES, [NVDA_COVER]) == [
        "effective_basis '' is not one of ['stated', 'fallback_b2', 'fallback_b1']"
    ]


def test_a_reverse_split_has_no_record_date_fallback() -> None:
    quotes = ["a one-for-ten reverse split", "record on June 6, 2024, payable on June 7, 2024"]
    values = split(ratio="1/10", record_date="2024-06-06", payable_date="2024-06-07", effective_basis="fallback_b2")
    assert s3b.field_failures("split", values, quotes, [NVDA_COVER]) == [
        "no quote states effective_date 2024-06-10 as the adjusted-basis session; a reverse split has no fallback"
    ]


def test_a_date_beyond_the_role_reach_is_not_in_that_role() -> None:
    far = "split-adjusted basis" + " filler" * 10 + " June 10, 2024"
    assert not s3b.dated_near(far, date(2024, 6, 10), s3b.EFFECTIVE_WORDS)
    assert s3b.dated_near(
        "June 10, 2024 is the first day on a post-split basis", date(2024, 6, 10), s3b.EFFECTIVE_WORDS
    )


def ix(context: str, name: str, text: str) -> str:
    return f'<ix:nonNumeric name="dei:{name}" contextRef="{context}" format="x">{text}</ix:nonNumeric>'


def test_single_class_reads_the_cover_12b_contexts() -> None:
    one = (
        ix("c1", "Security12bTitle", "Common Stock, $0.001 par<br/> value per share")
        + ix("c1", "TradingSymbol", "NVDA")
    ).encode()
    assert s3b.single_class(one) == NVDA_COVER
    two = one + (ix("c2", "Security12bTitle", "4.5% Notes due 2030") + ix("c2", "TradingSymbol", "NVDA30")).encode()
    assert len(s3b.cover_classes(two)) == 2
    assert s3b.single_class(two) is None
    assert s3b.single_class(b"<p>no inline XBRL</p>") is None


def test_an_empty_class_title_or_symbol_matches_no_cover() -> None:
    quotes = SPLIT_QUOTES[:2]
    assert s3b.field_failures("split", split(class_title="", class_symbol=""), quotes, [("", "")]) == NO_CLASS


def test_only_a_served_body_is_mirrored() -> None:
    header_url = f"https://www.sec.gov/Archives/edgar/data/1/x/{ACCESSION}-index-headers.html"
    assert s3b.served_body(header_url, f"<ACCEPTANCE-DATETIME>{ACCEPTANCE}".encode())
    assert not s3b.served_body(header_url, b"<html>Request Rate Threshold Exceeded</html>")
    assert s3b.served_body("https://www.sec.gov/a/doc.htm", DOCUMENT)
    assert not s3b.served_body(
        "https://www.sec.gov/a/doc.htm", b"Your Request Originates from an Undeclared Automated Tool"
    )
    assert not s3b.served_body("https://www.sec.gov/a/doc.htm", b"  ")


def test_a_cancelled_row_states_no_fields() -> None:
    assert s3b.field_failures("split", split(status="cancelled"), ["the split was withdrawn"]) == []


def test_a_symbol_change_needs_both_symbols_and_the_date() -> None:
    values = {"status": "confirmed", "old_symbol": "SQ", "new_symbol": "XYZ", "effective_date": "2025-01-21"}
    quotes = ["will change its ticker symbol from SQ to XYZ effective January 21, 2025"]
    assert s3b.field_failures("symbol_change", values, quotes) == []
    assert s3b.field_failures("symbol_change", values | {"new_symbol": "BLOCK"}, quotes) == [
        "no quote names new_symbol 'BLOCK'"
    ]


def test_a_first_trade_needs_the_date_and_first_session_words() -> None:
    values = {"status": "confirmed", "first_session": "2024-03-21"}
    assert s3b.field_failures("first_trade", values, ["shares began trading on March 21, 2024"]) == []
    assert s3b.field_failures("first_trade", values, ["the offering closed on March 21, 2024"]) == [
        "no quote states first_session 2024-03-21 beside the words of its role"
    ]


@pytest.mark.parametrize(
    ("basis", "quote"),
    [
        ("last_trading_day", "The last day of trading will be May 1, 2025."),
        ("suspended", "trading will be suspended before the open on May 2, 2025"),
        ("merger_closing", "On May 1, 2025, the Company completed the merger."),
    ],
)
def test_a_termination_needs_the_date_and_its_basis_words(basis: str, quote: str) -> None:
    values = {"status": "confirmed", "last_session": "2025-05-01", "last_session_basis": basis}
    if basis == "suspended":
        values["last_session"] = "2025-05-02"  # the stated date; §3.1's previous session is the build's
    assert s3b.field_failures("termination_end", values, [quote]) == []


def test_a_termination_without_its_basis_words_fails_and_not_stated_needs_none() -> None:
    values = {"status": "confirmed", "last_session": "2025-05-01", "last_session_basis": "merger_closing"}
    assert s3b.field_failures("termination_end", values, ["Trading ends on May 1, 2025."]) == [
        "no quote states last_session 2025-05-01 beside the words of its role"
    ]
    not_stated = {"status": "confirmed", "last_session": "not_stated", "last_session_basis": ""}
    assert s3b.field_failures("termination_end", not_stated, ["anything"]) == []


# --------------------------------------------------------------------------- rows and the mirror


def write_records(path: Path, rows: list[dict[str, str]]) -> Path:
    columns = sorted({k for row in rows for k in row})
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, columns)
        writer.writeheader()
        writer.writerows(rows)
    return path


def evidence(n: int, quote: str, **overrides: str) -> dict[str, str]:
    item = {"accession": ACCESSION, "document": "nvda-20240607.htm", "quote": quote, "acceptance": ACCEPTANCE}
    return {f"evidence_{n}_{k}": v for k, v in (item | overrides).items()}


def test_read_rows_flags_every_malformed_item_and_never_skips(tmp_path: Path) -> None:
    rows = [
        split() | evidence(1, "q", cik="0000000001"),
        split(counterparty_cik="0000000001") | evidence(1, "q", cik="0000000001"),
        split() | evidence(1, "q", acceptance="2024-06-07", document="../x"),
        split() | evidence(1, "x" * 301),
        split(status="maybe"),
    ]
    read = s3b.read_rows(write_records(tmp_path / "r.csv", rows))
    assert [r.problems for r in read] == [
        ("evidence_1: CIK 0000000001 is neither the row's nor its counterparty's",),
        (),
        ("evidence_1: malformed accession or document name", "evidence_1: acceptance is not 14 digits"),
        ("evidence_1: quote is empty or longer than 300 characters",),
        ("no evidence item", "status 'maybe' is not one of ['cancelled', 'confirmed', 'contested']"),
    ]
    assert read[0].evidence[0].document_url == (
        "https://www.sec.gov/Archives/edgar/data/1/000104581024000144/nvda-20240607.htm"
    )


def mirror_filing(mirror: Path, document: bytes, acceptance: str = ACCEPTANCE) -> None:
    folder = mirror / ACCESSION
    folder.mkdir(parents=True)
    (folder / "nvda-20240607.htm.gz").write_bytes(gzip.compress(document, mtime=0))
    header = f"<pre><ACCEPTANCE-DATETIME>{acceptance}\n</pre>".encode()
    (folder / f"{ACCESSION}-index-headers.html.gz").write_bytes(gzip.compress(header, mtime=0))


DOCUMENT = (
    b"<p>announced a ten-for-one forward stock split</p><p>Trading is expected to commence on a split-adjusted basis"
    b" at market open on June 10, 2024.</p>"
    b'<td><ix:nonNumeric name="dei:Security12bTitle" contextRef="c1">Common Stock, $0.001 par value per share'
    b'</ix:nonNumeric></td><td><ix:nonNumeric name="dei:TradingSymbol" contextRef="c1">NVDA</ix:nonNumeric></td>'
    b"<td>The Nasdaq Global Select Market</td>"
)


def split_row(tmp_path: Path) -> s3b.Row:
    values = split() | evidence(1, SPLIT_QUOTES[0]) | evidence(2, SPLIT_QUOTES[1]) | evidence(3, COVER)
    return s3b.read_rows(write_records(tmp_path / "r.csv", [values]))[0]


def test_check_row_passes_a_mirrored_filing_and_records_its_digest(tmp_path: Path) -> None:
    mirror_filing(tmp_path / "m", DOCUMENT)
    verdict = s3b.check_row("split", split_row(tmp_path), tmp_path / "m")
    assert verdict.passed, verdict.failures
    assert {e["sha256"] for e in verdict.evidence} == {hashlib.sha256(DOCUMENT).hexdigest()}
    assert verdict.evidence[0]["single_class"] == list(NVDA_COVER)


def test_a_counterparty_cover_does_not_identify_the_class(tmp_path: Path) -> None:
    # The same single-class cover, served under the counterparty's CIK: it cannot say which of the row's classes split.
    items = [evidence(n, q, cik="0000000001") for n, q in enumerate((*SPLIT_QUOTES[:2], COVER), 1)]
    values = split(counterparty_cik="0000000001") | items[0] | items[1] | items[2]
    row = s3b.read_rows(write_records(tmp_path / "r.csv", [values]))[0]
    mirror_filing(tmp_path / "m", DOCUMENT)
    verdict = s3b.check_row("split", row, tmp_path / "m")
    assert verdict.failures == NO_CLASS
    assert {e["single_class"] for e in verdict.evidence} == {None}


def test_a_misquoted_item_lends_no_single_class(tmp_path: Path) -> None:
    values = split() | evidence(1, SPLIT_QUOTES[0]) | evidence(2, SPLIT_QUOTES[1]) | evidence(3, "not in the document")
    row = s3b.read_rows(write_records(tmp_path / "r.csv", [values]))[0]
    mirror_filing(tmp_path / "m", DOCUMENT)
    verdict = s3b.check_row("split", row, tmp_path / "m")
    assert verdict.evidence[2]["single_class"] is None
    assert "evidence_3: quote does not occur in nvda-20240607.htm" in verdict.failures


def chain(*links: tuple[str, str], key: str = "2024-06-10") -> list[s3b.Row]:
    return [
        s3b.Row(line, split(effective_date=key, version=version, supersedes=supersedes), ())
        for line, (version, supersedes) in enumerate(links, 2)
    ]


def test_every_record_type_the_checker_reads_has_a_key() -> None:
    assert s3b.RECORD_TYPES == ("split", "symbol_change", "first_trade", "termination_end")
    assert set(s3b.RECORD_TYPES) == set(s3b.RECORD_KEYS)


def test_one_supersession_chain_per_key_passes() -> None:
    rows = chain(("1", ""), ("2", "1"), ("3", "2")) + chain(("1", ""), key="2024-06-11")
    assert s3b.version_failures("split", rows) == {}


@pytest.mark.parametrize(
    ("links", "problem"),
    [
        ((("1", ""), ("1", "")), "version 1 is repeated"),  # a second action at the key
        ((("1", ""), ("2", "1"), ("3", "1")), "version 1 is superseded 2 times"),  # a branch
        ((("1", ""), ("2", "")), "version 2 supersedes None: only version 1 supersedes nothing"),
        ((("1", "1"),), "version 1 supersedes 1: only version 1 supersedes nothing"),
        ((("1", ""), ("3", "2")), "version 3 supersedes 2, not an earlier version of the key"),
        ((("1", ""), ("x", "1")), "line 3: version 'x' / supersedes '1' is malformed"),
    ],
)
def test_a_broken_chain_fails_every_row_of_its_key(links: tuple[tuple[str, str], ...], problem: str) -> None:
    rows = chain(*links)
    failures = s3b.version_failures("split", rows)
    assert set(failures) == {r.line for r in rows}
    assert all(problem in f[0] for f in failures.values()), failures


def test_check_row_fails_a_misquote_a_wrong_acceptance_and_an_unserved_document(tmp_path: Path) -> None:
    mirror_filing(tmp_path / "m", DOCUMENT.replace(b"ten-for-one", b"two-for-one"), acceptance="20240607161935")
    verdict = s3b.check_row("split", split_row(tmp_path), tmp_path / "m")
    assert not verdict.passed
    assert "evidence_1: quote does not occur in nvda-20240607.htm" in verdict.failures
    assert "evidence_1: acceptance 20240607161934 != index headers 20240607161935" in verdict.failures
    unserved = s3b.check_row("split", split_row(tmp_path), tmp_path / "empty")
    assert unserved.failures[:3] == [f"evidence_{n}: not served by EDGAR" for n in (1, 2, 3)]
