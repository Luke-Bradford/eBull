from __future__ import annotations

import csv
import gzip
import io
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any

from scripts.census_2900_sec_cover_identity import (
    FORMATION_CLOSES,
    Submission,
    _cache_path,
    _price_symbol,
    load_submissions,
    parse_cover_contexts,
    price_sessions_by_symbol,
    resolve_cover_submissions,
    select_formation_submissions,
)


def _write_fsds(path: Path) -> None:
    fields = ["adsh", "cik", "form", "accepted", "period", "instance"]
    rows = [
        ["0000000001-22-000001", "1", "10-K", "2022-06-30 15:59:59.0", "20211231", "a.xml"],
        ["0000000001-22-000002", "1", "10-K/A", "2022-06-30 16:00:01.0", "20211231", "b.xml"],
        ["0000000001-23-000003", "1", "10-K/A", "2023-06-01 12:00:00.0", "20201231", "old.xml"],
        ["0000000002-22-000001", "2", "8-K", "2022-01-01 12:00:00.0", "20211231", "c.xml"],
    ]
    text = io.StringIO()
    writer = csv.writer(text, delimiter="\t", lineterminator="\n")
    writer.writerow(fields)
    writer.writerows(rows)
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("sub.txt", text.getvalue())


def test_selection_uses_latest_annual_public_by_formation_close(tmp_path: Path) -> None:
    _write_fsds(tmp_path / "2022q2.zip")

    submissions, _ = load_submissions(tmp_path)
    selected = select_formation_submissions(submissions)

    assert set(submissions) == {
        "0000000001-22-000001",
        "0000000001-22-000002",
        "0000000001-23-000003",
    }
    assert [row.accession for row in selected[FORMATION_CLOSES[0]]] == ["0000000001-22-000001"]
    assert [row.accession for row in selected[FORMATION_CLOSES[1]]] == ["0000000001-22-000002"]


def test_cover_context_requires_all_facts_in_same_context(tmp_path: Path) -> None:
    payload = b"""<?xml version="1.0"?>
    <xbrl xmlns="http://www.xbrl.org/2003/instance"
          xmlns:dei="http://xbrl.sec.gov/dei/2024"
          xmlns:xbrldi="http://xbrl.org/2006/xbrldi"
          xmlns:ex="http://example.test">
      <context id="security-a">
        <entity><identifier scheme="http://www.sec.gov/CIK">1</identifier>
          <segment>
            <xbrldi:explicitMember dimension="dei:StatementClassOfStockAxis">
              ex:ClassA
            </xbrldi:explicitMember>
          </segment>
        </entity>
        <period><instant>2021-12-31</instant></period>
      </context>
      <context id="incomplete"><entity><identifier scheme="http://www.sec.gov/CIK">1</identifier></entity>
        <period><instant>2021-12-31</instant></period></context>
      <dei:DocumentPeriodEndDate contextRef="incomplete">2021-12-31</dei:DocumentPeriodEndDate>
      <dei:Security12bTitle contextRef="security-a">Class A Common Stock</dei:Security12bTitle>
      <dei:TradingSymbol contextRef="security-a">TEST.A</dei:TradingSymbol>
      <dei:SecurityExchangeName contextRef="security-a">NYSE</dei:SecurityExchangeName>
      <dei:TradingSymbol contextRef="incomplete">WRONG</dei:TradingSymbol>
    </xbrl>"""
    path = tmp_path / "instance.xml.gz"
    with gzip.open(path, "wb") as handle:
        handle.write(payload)

    contexts = parse_cover_contexts(path)

    assert contexts == [
        {
            "context_ref": "security-a",
            "dimensions": {"dei:StatementClassOfStockAxis": "ex:ClassA"},
            "period": {"instant": "2021-12-31"},
            "facts": {
                "DocumentPeriodEndDate": ["2021-12-31"],
                "Security12bTitle": ["Class A Common Stock"],
                "SecurityExchangeName": ["NYSE"],
                "TradingSymbol": ["TEST.A"],
            },
        }
    ]


def test_price_symbol_only_normalizes_class_punctuation() -> None:
    assert _price_symbol("brk.b") == "BRK_B"
    assert _price_symbol("BF-B") == "BF_B"
    assert _price_symbol("ABCDQ") == "ABCDQ"


def test_price_session_census_reads_dates_not_values(tmp_path: Path) -> None:
    (tmp_path / "TEST_A.csv").write_text(
        "2022-06-29,SECRET\n2022-06-30,NOT_A_NUMBER\n2023-06-30,ALSO_SECRET\n",
        encoding="utf-8",
    )

    assert price_sessions_by_symbol(
        tmp_path,
        frozenset({FORMATION_CLOSES[0].date(), FORMATION_CLOSES[1].date()}),
    ) == {"TEST_A": frozenset({FORMATION_CLOSES[0].date(), FORMATION_CLOSES[1].date()})}


def test_cover_selection_falls_back_when_latest_amendment_omits_cover(tmp_path: Path) -> None:
    original = Submission(
        accession="0000000001-22-000001",
        cik="0000000001",
        form="10-K",
        accepted=datetime(2022, 2, 28, 16),
        period="20211231",
        instance="original.xml",
    )
    amendment = Submission(
        accession="0000000001-22-000002",
        cik="0000000001",
        form="10-K/A",
        accepted=datetime(2022, 3, 30, 16),
        period="20211231",
        instance="amendment.xml",
    )
    original_payload = b"""<xbrl xmlns="http://www.xbrl.org/2003/instance"
      xmlns:dei="http://xbrl.sec.gov/dei/2022">
      <context id="c"><entity><identifier scheme="sec">1</identifier></entity>
      <period><instant>2021-12-31</instant></period></context>
      <dei:DocumentPeriodEndDate contextRef="c">2021-12-31</dei:DocumentPeriodEndDate>
      <dei:Security12bTitle contextRef="c">Common Stock</dei:Security12bTitle>
      <dei:TradingSymbol contextRef="c">TEST</dei:TradingSymbol>
      <dei:SecurityExchangeName contextRef="c">NYSE</dei:SecurityExchangeName>
    </xbrl>"""
    amendment_payload = b"""<xbrl xmlns="http://www.xbrl.org/2003/instance"
      xmlns:dei="http://xbrl.sec.gov/dei/2022">
      <context id="c"><entity><identifier scheme="sec">1</identifier></entity>
      <period><instant>2021-12-31</instant></period></context>
      <dei:DocumentPeriodEndDate contextRef="c">2021-12-31</dei:DocumentPeriodEndDate>
    </xbrl>"""
    for submission, payload in ((original, original_payload), (amendment, amendment_payload)):
        path = _cache_path(tmp_path, submission)
        path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(path, "wb") as handle:
            handle.write(payload)

    resolved, downloads = resolve_cover_submissions(
        {original.accession: original, amendment.accession: amendment},
        {FORMATION_CLOSES[0]: (amendment,)},
        tmp_path,
    )

    assert downloads == {}
    assert resolved[FORMATION_CLOSES[0]][original.cik] == original


def test_cover_selection_does_not_fallback_after_unparseable_latest(tmp_path: Path) -> None:
    original = Submission(
        accession="0000000001-22-000001",
        cik="0000000001",
        form="10-K",
        accepted=datetime(2022, 2, 28, 16),
        period="20211231",
        instance="original.xml",
    )
    amendment = Submission(
        accession="0000000001-22-000002",
        cik="0000000001",
        form="10-K/A",
        accepted=datetime(2022, 3, 30, 16),
        period="20211231",
        instance="amendment.xml",
    )
    for submission in (original, amendment):
        path = _cache_path(tmp_path, submission)
        path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(path, "wb") as handle:
            handle.write(b"not xml")
    parsed: dict[str, list[dict[str, Any]] | None] = {
        amendment.accession: None,
        original.accession: [
            {
                "facts": {
                    "Security12bTitle": ["Common Stock"],
                    "TradingSymbol": ["TEST"],
                    "SecurityExchangeName": ["NYSE"],
                }
            }
        ],
    }

    resolved, _ = resolve_cover_submissions(
        {original.accession: original, amendment.accession: amendment},
        {FORMATION_CLOSES[0]: (amendment,)},
        tmp_path,
        parsed,
    )

    assert original.cik not in resolved[FORMATION_CLOSES[0]]


def test_cover_selection_falls_back_after_conflicting_latest_context(tmp_path: Path) -> None:
    original = Submission(
        accession="0000000001-22-000001",
        cik="0000000001",
        form="10-K",
        accepted=datetime(2022, 2, 28, 16),
        period="20211231",
        instance="original.xml",
    )
    amendment = Submission(
        accession="0000000001-22-000002",
        cik="0000000001",
        form="10-K/A",
        accepted=datetime(2022, 3, 30, 16),
        period="20211231",
        instance="amendment.xml",
    )
    parsed: dict[str, list[dict[str, Any]] | None] = {
        amendment.accession: [
            {
                "facts": {
                    "Security12bTitle": ["Common Stock"],
                    "TradingSymbol": ["A", "B"],
                    "SecurityExchangeName": ["NYSE"],
                }
            }
        ],
        original.accession: [
            {
                "facts": {
                    "Security12bTitle": ["Common Stock"],
                    "TradingSymbol": ["TEST"],
                    "SecurityExchangeName": ["NYSE"],
                }
            }
        ],
    }

    resolved, _ = resolve_cover_submissions(
        {original.accession: original, amendment.accession: amendment},
        {FORMATION_CLOSES[0]: (amendment,)},
        tmp_path,
        parsed,
    )

    assert resolved[FORMATION_CLOSES[0]][original.cik] == original
