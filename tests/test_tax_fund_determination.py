"""Pure tests for #3616 slice 3d-3c-2's determination format, rules and outcome order (spec tests 1-5).

Spec: docs/specs/metrics/2026-10-14-3616-slice3d3c2-fund-determination.md. The committed SPY determination is read
from app/services/tax_fund_determinations/; every other case uses a small synthetic determination built here, whose
documents are short HTML bodies hashed at build time. No test touches the network or the DB.
"""

from __future__ import annotations

import copy
import hashlib
import json
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from app.services.tax_fund_determination import (
    GOVERNING_FORMS,
    POLICY_ASSUMPTIONS,
    QUESTIONS,
    Decision,
    Fetch,
    FundObservation,
    RunState,
    authority_refusal,
    authority_text,
    content_sha256,
    decide,
    document_text,
    newest_fetch,
    offshore_fund,
    parse_determination,
    reporting_status,
    submissions_coverage,
    unreviewed_accessions,
    validate,
)

DETERMINATIONS = Path(__file__).parents[1] / "app" / "services" / "tax_fund_determinations"
SPY_FILE = DETERMINATIONS / "0000884394_v1.json"
CIK = "0000884394"
ACC = "0001193125-26-022316"
URL = "https://www.sec.gov/Archives/edgar/data/884394/000119312526022316/d77353d485bpos.htm"
BODY = b"<html><p>The Trust is a unit <b>inv</b>estment trust.</p><p>Units are redeemed at NAV.</p></html>"
AUTH_URL = "https://www.legislation.gov.uk/ukpga/2010/8/section/355"
AUTH_RAW = f"URL: {AUTH_URL}\nArrangements are an offshore fund if they are a mutual fund.\n".encode()
CUTOFF = datetime(2026, 10, 14, 22, 0, tzinfo=UTC)
FETCHED = datetime(2026, 10, 14, 20, 0, tzinfo=UTC)
ANSWERS = {
    "arrangement": "single_arrangement_single_class",
    "s355_limb": "b",
    "s356_condition_a": "met",
    "s356_condition_b": "met",
    "s356_condition_c": "met",
    "s357_condition_d": "not_met",
    "other_exclusions": "no_applicable_exclusion",
    "transparent_fund": "unresolved",
    "reg29_sufficient_information": "unresolved",
    "reg29_nonreporting_holdings": "unresolved",
}
DOC_QUOTE = {"source_id": "prospectus", "locator": "ORGANIZATION", "text": "The Trust is a unit investment trust."}
AUTH_QUOTE = {"source_id": "tiopa_s355", "locator": "s355(1)", "text": "an offshore fund if they are a mutual fund"}


def _finding(question: str, answer: str, assumptions: list[str] | None = None) -> dict[str, Any]:
    unresolved = answer == "unresolved"
    return {
        "question": question,
        "answer": answer,
        "basis": "unresolved" if unresolved else ("assumed" if assumptions else "established"),
        "assumptions": assumptions or [],
        "authorities": ["tiopa_s355"],
        "quotes": [] if unresolved else [dict(DOC_QUOTE), dict(AUTH_QUOTE)],
        "contrary": [],
        "reasoning": "Read from the prospectus.",
        "would_resolve": "The governing Indenture." if unresolved else None,
    }


def make_det(**answers: str) -> dict[str, Any]:
    """A valid synthetic determination: one 485BPOS document, one authority, every finding established unless
    unresolved; ``answers`` overrides a question's answer."""
    assumption_ids = ["A1_trustee_cmc", *POLICY_ASSUMPTIONS]
    return {
        "determination_id": f"fund_determination_v1:{CIK}:1",
        "policy_id": "fund_determination_v1",
        "cik": CIK,
        "version": 1,
        "binding": {"cik": CIK, "lei": "549300NZAMSJ8FXPQQ63", "isin": "US78462F1030", "mic": "ARCX", "ticker": "SPY"},
        "valid_from": "2026-01-26",
        "valid_from_document": "prospectus",
        "reviewed_at": "2026-10-14",
        "review_by": "2027-10-14",
        "monitor_from": "2026-01-26",
        "refresh_forms": sorted(GOVERNING_FORMS),
        "reviewed_filings": [{"accession": ACC, "form": "485BPOS", "filed": "2026-01-26", "documents": ["prospectus"]}],
        "documents": [
            {
                "doc_id": "prospectus",
                "url": URL,
                "accession": ACC,
                "form": "485BPOS",
                "exhibit": None,
                "filed": "2026-01-26",
                "role": "Prospectus",
                "content_sha256": content_sha256(URL, BODY),
            }
        ],
        "authorities": [
            {
                "authority_id": "tiopa_s355",
                "url": AUTH_URL,
                "version": "as in force 2026-10-14",
                "content_sha256": hashlib.sha256(AUTH_RAW).hexdigest(),
            }
        ],
        "assumptions": [
            {"assumption_id": a, "statement": "Stated.", "evidence": [dict(DOC_QUOTE)], "residual": "Residual."}
            for a in assumption_ids
        ],
        "findings": [_finding(q, answers.get(q, a)) for q, a in ANSWERS.items()],
    }


def subs_body(rows: list[tuple[str, str, str]], *, cik: str = CIK, files: Any = None) -> bytes:
    """A synthetic SEC submissions body; ``rows`` are (form, filingDate, accessionNumber)."""
    recent = {
        "form": [r[0] for r in rows],
        "filingDate": [r[1] for r in rows],
        "accessionNumber": [r[2] for r in rows],
    }
    return json.dumps({"cik": cik, "filings": {"recent": recent, "files": [] if files is None else files}}).encode()


ANCHOR = ("485BPOS", "2026-01-26", ACC)
NEW_497 = ("497", "2026-03-02", "0001193125-26-077777")


def subs(
    rows: list[tuple[str, str, str]] | None = None, *, at: datetime = FETCHED, fetch_id: int = 50, **kw: Any
) -> Fetch:
    return Fetch(fetch_id, at, subs_body([ANCHOR] if rows is None else rows, **kw))


SPY_OBS = FundObservation(1, "candidate", CIK, "549300NZAMSJ8FXPQQ63", "US78462F1030", "ARCX", "SPY")


def run_decide(
    det: dict[str, Any] | None = None,
    *,
    obs: FundObservation | None = SPY_OBS,
    fetches: list[Fetch] | None = None,
    submissions: Fetch | None | str = "default",
    unreviewed: tuple[str, ...] = (),
    cutoff: datetime = CUTOFF,
) -> Decision:
    det = make_det() if det is None else det
    return decide(
        obs,
        {det["cik"]: det},
        {URL: [Fetch(10, FETCHED, BODY)] if fetches is None else fetches},
        {"tiopa_s355": authority_text(AUTH_RAW)},
        subs() if submissions == "default" else submissions,  # type: ignore[arg-type]
        unreviewed,
        cutoff,
    )


# --- Test 1: format ----------------------------------------------------------------------------------------------


def test_committed_spy_determination_validates() -> None:
    det, errs = parse_determination(SPY_FILE.read_bytes())
    assert errs == []
    assert det is not None and det["determination_id"] == "fund_determination_v1:0000884394:1"


def test_synthetic_determination_validates() -> None:
    assert validate(make_det()) == []


def _set(path: str, value: Any) -> Callable[[dict[str, Any]], None]:
    def mutate(det: dict[str, Any]) -> None:
        *head, last = path.split(".")
        node: Any = det
        for p in head:
            node = node[int(p)] if p.isdigit() else node[p]
        if value is _DELETE:
            del node[last]
        else:
            node[int(last) if last.isdigit() else last] = value

    return mutate


_DELETE = object()
_BASE_URL = "https://www.sec.gov/Archives/edgar/data/884394/000119312526022316/"


def _dup_document(det: dict[str, Any]) -> None:
    det["documents"].append(copy.deepcopy(det["documents"][0]))


def _second_manifest_entry(det: dict[str, Any]) -> None:
    det["reviewed_filings"].append({"accession": "0001193125-26-099999", "form": "485BPOS", "filed": "2026-01-26",
                                    "documents": ["prospectus"]})  # fmt: skip


def _drop_question(det: dict[str, Any]) -> None:
    det["findings"].pop()


def _assumed_without(det: dict[str, Any]) -> None:
    det["findings"][0]["basis"] = "assumed"


def _established_with(det: dict[str, Any]) -> None:
    det["findings"][0]["assumptions"] = ["A1_trustee_cmc"]


FORMAT_CASES: dict[str, Callable[[dict[str, Any]], None]] = {
    "null lei": _set("binding.lei", None),
    "null documents": _set("documents", None),
    "string version": _set("version", "1"),
    "boolean version": _set("version", True),
    "version 2": _set("version", 2),
    "missing top key": _set("monitor_from", _DELETE),
    "extra top key": _set("extra", 1),
    "extra binding key": _set("binding.extra", "x"),
    "missing document key": _set("documents.0.role", _DELETE),
    "extra finding key": _set("findings.0.extra", 1),
    "id disagrees with cik": _set("determination_id", "fund_determination_v1:0000000001:1"),
    "other policy": _set("policy_id", "fund_determination_v2"),
    "malformed doc id": _set("documents.0.doc_id", "Prospectus"),
    "duplicate document id": _dup_document,
    "malformed assumption id": _set("assumptions.0.assumption_id", "B1_x"),
    "invalid date": _set("reviewed_at", "2026-02-30"),
    "anchor not 485BPOS date": _set("valid_from", "2026-01-25"),
    "review_by not one year": _set("review_by", "2027-10-13"),
    "document after reviewed_at": _set("documents.0.filed", "2026-10-15"),
    "refresh_forms missing a form": _set("refresh_forms", sorted(GOVERNING_FORMS - {"497"})),
    "refresh_forms holding a number": _set("refresh_forms", [*sorted(GOVERNING_FORMS), 7]),
    "url other cik": _set("documents.0.url", "https://www.sec.gov/Archives/edgar/data/1/000119312526022316/a.htm"),
    "url other accession": _set(
        "documents.0.url", "https://www.sec.gov/Archives/edgar/data/884394/000000000000000000/a.htm"
    ),
    "url other host": _set("documents.0.url", URL.replace("www.sec.gov", "sec.gov")),
    "url query": _set("documents.0.url", URL + "?x=1"),
    "url fragment": _set("documents.0.url", URL + "#p"),
    "url parameters": _set("documents.0.url", _BASE_URL + "a.htm;x"),
    "url empty segment": _set("documents.0.url", _BASE_URL),
    "url nested segment": _set("documents.0.url", _BASE_URL + "a/b.htm"),
    "url dot segment": _set("documents.0.url", _BASE_URL + ".."),
    "url percent-encoding": _set("documents.0.url", _BASE_URL + "a%20b.htm"),
    "url space": _set("documents.0.url", _BASE_URL + "a b.htm"),
    "url tab": _set("documents.0.url", _BASE_URL + "a\tb.htm"),
    "url newline": _set("documents.0.url", _BASE_URL + "a\nb.htm"),
    "url DEL": _set("documents.0.url", _BASE_URL + "a\x7fb.htm"),
    "url U+0080": _set("documents.0.url", _BASE_URL + "a\x80b.htm"),
    "url U+0085": _set("documents.0.url", _BASE_URL + "a\x85b.htm"),
    "url U+00A0": _set("documents.0.url", _BASE_URL + "a\xa0b.htm"),
    "url U+2003": _set("documents.0.url", _BASE_URL + "a b.htm"),
    "url not canonical": _set("documents.0.url", URL.replace("https://www.sec.gov", "HTTPS://www.sec.gov")),
    "document in no manifest entry": _set("reviewed_filings.0.documents", ["other"]),
    "document in two manifest entries": _second_manifest_entry,
    "manifest names another date": _set("reviewed_filings.0.filed", "2026-01-27"),
    "malformed hash": _set("documents.0.content_sha256", "ABC"),
    "pending hash": _set("authorities.0.content_sha256", "PENDING"),
    "missing policy assumption": _set("assumptions", []),
    "unknown question": _set("findings.0.question", "s999"),
    "unknown answer": _set("findings.0.answer", "maybe"),
    "missing question": _drop_question,
    "resolved holding-dependent finding": _set("findings.8.answer", "made_available"),
    "unresolved answer, other basis": _set("findings.7.basis", "established"),
    "assumed with no assumption": _assumed_without,
    "established with one": _established_with,
    "unresolved without would_resolve": _set("findings.7.would_resolve", None),
    "resolved answer without a quote": _set("findings.0.quotes", []),
    "blank reasoning": _set("findings.0.reasoning", "  "),
    "unknown assumption": _set("findings.0.assumptions", ["A99_unknown"]),
    "unknown authority": _set("findings.0.authorities", ["nope"]),
    "document named as authority": _set("findings.0.authorities", ["prospectus"]),
    "blank locator": _set("findings.0.quotes.0.locator", " "),
    "white-space quote": _set("findings.0.quotes.0.text", " \n "),
    "blank evidence": _set("assumptions.0.evidence.0.text", ""),
}

#: The error each case must raise (one of possibly several), so a refusal for another reason fails.
FORMAT_ERRORS: dict[str, str] = {
    "anchor not 485BPOS date": "valid_from_document must be the newest reviewed 485BPOS, and valid_from its date",
    "assumed with no assumption": "arrangement: assumed without an assumption",
    "blank evidence": "assumption A1_trustee_cmc: quote",
    "blank locator": "arrangement: quote",
    "blank reasoning": "arrangement: reasoning",
    "boolean version": "version (policy v1 supports version 1 only)",
    "document after reviewed_at": "reviewed filing 0001193125-26-022316: document prospectus",
    "document in no manifest entry": "reviewed filing 0001193125-26-022316: document other",
    "document in two manifest entries": "reviewed filing 0001193125-26-099999: document prospectus",
    "document named as authority": "arrangement: unknown authority",
    "duplicate document id": "document id prospectus",
    "established with one": "arrangement: assumptions on a basis other than assumed",
    "extra binding key": "binding: keys",
    "extra finding key": "finding: keys",
    "extra top key": "determination: keys",
    "id disagrees with cik": "determination_id",
    "invalid date": "dates",
    "malformed assumption id": "assumption B1_x",
    "malformed doc id": "other_exclusions: quote",
    "malformed hash": "document prospectus: hash",
    "manifest names another date": "reviewed filing 0001193125-26-022316: document prospectus",
    "missing document key": "document: keys",
    "missing policy assumption": "policy assumption A5_part3_not_applying missing",
    "missing question": "questions ['reg29_nonreporting_holdings']",
    "missing top key": "determination: keys",
    "null documents": "documents: not a list",
    "null lei": "binding",
    "other policy": "policy or cik",
    "pending hash": "authority tiopa_s355: hash",
    "refresh_forms holding a number": "refresh_forms must be strings including every governing form",
    "refresh_forms missing a form": "refresh_forms must be strings including every governing form",
    "resolved answer without a quote": "arrangement: no quote",
    "resolved holding-dependent finding": "reg29_sufficient_information: answer or basis",
    "review_by not one year": "review_by must be one year after reviewed_at",
    "string version": "version (policy v1 supports version 1 only)",
    "unknown answer": "arrangement: answer or basis",
    "unknown assumption": "arrangement: assumptions on a basis other than assumed",
    "unknown authority": "arrangement: unknown authority",
    "unknown question": "s999: answer or basis",
    "unresolved answer, other basis": "transparent_fund: unresolved answer needs unresolved basis",
    "unresolved without would_resolve": "transparent_fund: unresolved without would_resolve",
    "url DEL": "document prospectus: accession or url",
    "url U+0080": "document prospectus: accession or url",
    "url U+0085": "document prospectus: accession or url",
    "url U+00A0": "document prospectus: accession or url",
    "url U+2003": "document prospectus: accession or url",
    "url dot segment": "document prospectus: accession or url",
    "url empty segment": "document prospectus: accession or url",
    "url fragment": "document prospectus: accession or url",
    "url nested segment": "document prospectus: accession or url",
    "url newline": "document prospectus: accession or url",
    "url not canonical": "document prospectus: accession or url",
    "url other accession": "document prospectus: accession or url",
    "url other cik": "document prospectus: accession or url",
    "url other host": "document prospectus: accession or url",
    "url parameters": "document prospectus: accession or url",
    "url percent-encoding": "document prospectus: accession or url",
    "url query": "document prospectus: accession or url",
    "url space": "document prospectus: accession or url",
    "url tab": "document prospectus: accession or url",
    "version 2": "version (policy v1 supports version 1 only)",
    "white-space quote": "arrangement: quote",
}


@pytest.mark.parametrize("case", sorted(FORMAT_CASES))
def test_format_refusal(case: str) -> None:
    det = make_det()
    FORMAT_CASES[case](det)
    assert FORMAT_ERRORS[case] in validate(det)


def test_printable_ascii_bounds_accepted() -> None:
    det = make_det()
    det["documents"][0]["url"] = _BASE_URL + "a!~.htm"
    assert validate(det) == []


def test_duplicate_json_key_and_bad_utf8_refused_not_raised() -> None:
    raw = SPY_FILE.read_bytes()
    dup = raw.replace(b'"version": 1,', b'"version": 1, "version": 1,', 1)
    assert dup != raw
    assert parse_determination(dup)[0] is None
    assert parse_determination(b"\xff{}")[0] is None


def test_leap_day_review_by() -> None:
    det = make_det()
    det["reviewed_at"], det["review_by"] = "2028-02-29", "2029-02-28"
    assert "review_by must be one year after reviewed_at" not in validate(det)


def test_authority_refusal() -> None:
    a = make_det()["authorities"][0]
    assert authority_refusal(a, AUTH_RAW) is None
    assert authority_refusal(a, AUTH_RAW + b"x") == "hash"
    other = AUTH_RAW.replace(b"section/355", b"section/356")
    assert authority_refusal({**a, "content_sha256": hashlib.sha256(other).hexdigest()}, other) == "url line"


# --- Test 2: offshore_fund ---------------------------------------------------------------------------------------


def test_offshore_fund_spy() -> None:
    det, _ = parse_determination(SPY_FILE.read_bytes())
    assert det is not None
    r = offshore_fund(det)
    assert (r.answer, r.basis) == ("true", "assumed")
    assert r.assumptions == ("A1_trustee_cmc", "A2_no_uk_branch", "A3_indenture", "A4_no_other_exclusion")


def _with(det: dict[str, Any], question: str, answer: str, assumptions: list[str] | None = None) -> dict[str, Any]:
    det["findings"] = [_finding(q, answer, assumptions) if q == question else f for q, f in
                       ((f["question"], f) for f in det["findings"])]  # fmt: skip
    return det


def test_offshore_fund_cases() -> None:
    assert offshore_fund(make_det()) == offshore_fund(make_det())
    r = offshore_fund(make_det())
    assert (r.answer, r.basis, r.assumptions) == ("true", "established", ())
    r = offshore_fund(_with(make_det(), "s356_condition_b", "not_met", ["A1_trustee_cmc"]))
    assert (r.answer, r.basis, r.assumptions) == ("false", "assumed", ("A1_trustee_cmc",))
    both = _with(_with(make_det(), "s356_condition_b", "not_met", ["A1_trustee_cmc"]), "other_exclusions", "applies")
    r = offshore_fund(both)
    assert (r.answer, r.basis) == ("false", "established")
    r = offshore_fund(_with(make_det(), "s357_condition_d", "met"))
    assert (r.answer, r.basis) == ("unresolved", "unresolved")
    r = offshore_fund(_with(make_det(), "s356_condition_a", "unresolved"))
    assert (r.answer, r.basis) == ("unresolved", "unresolved")
    assert offshore_fund(make_det(transparent_fund="opaque")) == offshore_fund(make_det())


# --- Test 3: document_text and content_sha256 --------------------------------------------------------------------


def test_document_text() -> None:
    assert document_text(b"<p>inv<b>est</b>ment</p>") == "investment"
    assert document_text(b"<p>one</p><p>two</p>") == "one two"
    assert document_text(b"<p>&amp;amp;</p>") == "&amp;"
    assert document_text(b"<script>var x=1;</script><p>kept</p>") == "kept"
    assert document_text("<p>a  b</p>".encode()) == "a b"
    assert document_text(b"<PAGE>joined<PAGE>here") == "joinedhere"
    with pytest.raises(UnicodeDecodeError):
        document_text(b"\xff")


def test_xml_hash_binds_attributes() -> None:
    url = _BASE_URL + "primary_doc.xml"
    a = b'<edgarSubmission><reportEndingPeriod>2025-12-31</reportEndingPeriod><x fundsTickerSymbol="SPY"/>'
    assert content_sha256(url, a) != content_sha256(url, a.replace(b'"SPY"', b'"SPYX"'))
    assert content_sha256(url, a) != content_sha256(url, a.replace(b"2025-12-31", b"2025-12-30"))


# --- Test 4: the outcome table -----------------------------------------------------------------------------------


def test_outcome_rows_in_order() -> None:
    assert run_decide(obs=None).outcome == "fund_observation_unavailable"
    assert (
        run_decide(obs=FundObservation(1, "cik_unestablished", None, None, None, None, "IUSA")).outcome
        == "not_candidate"
    )
    other = FundObservation(1, "candidate", "0000000001", "549300NZAMSJ8FXPQQ63", "US78462F1030", "ARCX", "SPY")
    assert run_decide(obs=other).outcome == "no_determination"
    lower = FundObservation(1, "candidate", CIK, "549300NZAMSJ8FXPQQ63", "US78462F1030", "ARCX", "spy")
    assert run_decide(obs=lower).outcome == "determined"
    isin = FundObservation(1, "candidate", CIK, "549300NZAMSJ8FXPQQ63", "US78462F1022", "ARCX", "SPY")
    assert run_decide(obs=isin) == Decision("binding_mismatch", "isin")
    assert run_decide(fetches=[]).outcome == "documents_unavailable"
    assert run_decide(fetches=[Fetch(10, CUTOFF + timedelta(seconds=1), BODY)]).outcome == "documents_unavailable"
    assert run_decide(fetches=[Fetch(10, FETCHED, BODY + b"<p>changed</p>")]).outcome == "document_changed"
    d = run_decide()
    assert d.outcome == "determined" and d.document_fetch_ids == {"prospectus": 10} and d.submissions_fetch_id == 50
    assert d.offshore is not None and d.offshore.answer == "true"


def test_quote_unverified_contrary_and_authority() -> None:
    det = make_det()
    det["findings"][0]["contrary"] = [{"source_id": "prospectus", "locator": "x", "text": "not in the document"}]
    assert run_decide(det) == Decision("quote_unverified", "arrangement:prospectus", det["determination_id"])
    det = make_det()
    det["findings"][1]["quotes"] = [{"source_id": "tiopa_s355", "locator": "x", "text": "not in the snapshot"}]
    assert run_decide(det).outcome == "quote_unverified"


def test_document_selection_by_hash_then_recency() -> None:
    changed = BODY + b"<p>changed</p>"
    older_match = [Fetch(10, FETCHED - timedelta(days=1), BODY), Fetch(11, FETCHED, changed)]
    assert run_decide(fetches=older_match).document_fetch_ids == {"prospectus": 10}
    newer_match = [Fetch(10, FETCHED - timedelta(days=1), changed), Fetch(11, FETCHED, BODY)]
    assert run_decide(fetches=newer_match).document_fetch_ids == {"prospectus": 11}


def test_review_window() -> None:
    det = make_det()
    det["reviewed_at"], det["review_by"] = "2026-10-15", "2027-10-15"
    assert run_decide(det).outcome == "review_out_of_date"
    late = datetime(2027, 10, 15, 0, 0, 1, tzinfo=UTC)
    assert run_decide(cutoff=late, submissions=subs(at=late - timedelta(hours=1))).outcome == "review_out_of_date"


@pytest.mark.parametrize(
    ("submissions", "refusal"),
    [
        (None, "submissions missing"),
        (subs(at=CUTOFF - timedelta(days=31)), "submissions not current"),
        (subs(at=CUTOFF + timedelta(seconds=1)), "submissions dated after the cutoff"),
        (subs(cik="0000000001"), "submissions unsound"),
        (
            Fetch(
                50,
                FETCHED,
                b'{"cik": "0000884394", "filings": {"recent": {"form": "485BPOS", "filingDate": [], '
                b'"accessionNumber": []}, "files": []}}',
            ),
            "submissions unsound",
        ),  # fmt: skip
        (
            Fetch(
                50,
                FETCHED,
                json.dumps(
                    {
                        "cik": CIK,
                        "filings": {
                            "recent": {
                                "form": ["485BPOS", "497"],
                                "filingDate": ["2026-01-26"],
                                "accessionNumber": [ACC],
                            },
                            "files": [],
                        },
                    }
                ).encode(),
            ),
            "submissions unsound",
        ),  # fmt: skip
        (
            Fetch(
                50,
                FETCHED,
                json.dumps(
                    {
                        "cik": CIK,
                        "filings": {
                            "recent": {"form": ["485BPOS"], "filingDate": ["2026-01-26"], "accessionNumber": [ACC]}
                        },
                    }
                ).encode(),
            ),
            "submissions unsound",
        ),  # fmt: skip
        (subs(files=False), "submissions unsound"),
        (subs(files=[{"name": "CIK0000884394-submissions-001.json"}]), "submissions overflow files"),
        (subs([ANCHOR, ANCHOR]), "submissions unsound"),
        (subs([ANCHOR, ("497", "2026-10-15", "0001193125-26-088888")]), "submissions unsound"),
        (subs([("485BPOS", "2026-01-27", ACC)]), f"submissions disagree with reviewed filing {ACC}"),
        (subs([("497", "2026-01-26", ACC)]), f"submissions disagree with reviewed filing {ACC}"),
    ],
)
def test_coverage_unavailable(submissions: Fetch | None, refusal: str) -> None:
    d = run_decide(submissions=submissions)
    assert (d.outcome, d.refusal) == ("coverage_unavailable", refusal)


def test_coverage_currency_boundaries() -> None:
    det = make_det()
    assert submissions_coverage(det, subs(at=CUTOFF - timedelta(days=30)), CUTOFF) is None
    late = subs(at=CUTOFF - timedelta(days=30, seconds=1))
    assert submissions_coverage(det, late, CUTOFF) == "submissions not current"


def test_filing_after_fetch_date_but_on_cutoff_date() -> None:
    """Fetched 2026-10-13, a filing dated 2026-10-14, cutoff 2026-10-14: refused, and not recorded."""
    det = make_det()
    future = ("497", "2026-10-14", "0001193125-26-088888")
    fetch = subs([ANCHOR, future], at=datetime(2026, 10, 13, 20, tzinfo=UTC))
    assert submissions_coverage(det, fetch, CUTOFF) == "submissions unsound"
    assert unreviewed_accessions(det, fetch) == []
    assert run_decide(submissions=subs([ANCHOR])).outcome == "determined"


def test_newest_submissions_fetch_selection() -> None:
    sound = subs(at=CUTOFF - timedelta(hours=3), fetch_id=60)
    unsound = Fetch(61, CUTOFF - timedelta(hours=1), b"not json")
    assert newest_fetch([sound, unsound], CUTOFF) == unsound
    assert submissions_coverage(make_det(), newest_fetch([sound, unsound], CUTOFF), CUTOFF) == "submissions unsound"
    tie = [subs(fetch_id=70), Fetch(71, FETCHED, b"not json")]
    assert newest_fetch(tie, CUTOFF) == tie[1]
    assert newest_fetch([subs(at=CUTOFF + timedelta(seconds=1))], CUTOFF) is None


def test_unreviewed_accessions() -> None:
    det = make_det()
    n_port = ("NPORT-P", "2026-03-02", "0001193125-26-066666")
    n_30d = ("N-30D", "2026-03-03", "0001193125-26-055555")
    fetch = subs([ANCHOR, NEW_497, n_port, n_30d])
    assert [x.accession for x in unreviewed_accessions(det, fetch)] == [NEW_497[2]]
    det["refresh_forms"] = [*det["refresh_forms"], "N-30D"]
    assert [x.form for x in unreviewed_accessions(det, fetch)] == ["497", "N-30D"]
    before = ("497", "2026-01-25", "0001193125-26-044444")
    assert unreviewed_accessions(make_det(), subs([ANCHOR, before])) == []


def test_discovery_needs_less_than_coverage() -> None:
    """Overflow ``files`` and a 40-day-old fetch refuse coverage but still record the discovery."""
    det = make_det()
    overflow = subs([ANCHOR, NEW_497], files=[{"name": "x.json"}])
    old = subs([ANCHOR, NEW_497], at=CUTOFF - timedelta(days=40))
    for fetch in (overflow, old):
        assert submissions_coverage(det, fetch, CUTOFF) is not None
        assert [x.accession for x in unreviewed_accessions(det, fetch)] == [NEW_497[2]]
    assert unreviewed_accessions(det, subs([ANCHOR, NEW_497], cik="0000000001")) == []


def test_stale_latched_after_omission() -> None:
    """A recorded discovery keeps the determination stale when a later sound inventory omits it."""
    d = run_decide(submissions=subs([ANCHOR]), unreviewed=(NEW_497[2],))
    assert (d.outcome, d.refusal) == ("stale", NEW_497[2])
    assert run_decide(fetches=[], unreviewed=(NEW_497[2],)).outcome == "documents_unavailable"


# --- Test 5: reporting_status ------------------------------------------------------------------------------------

DISPOSAL = date(2026, 9, 23)
OK_RUN = RunState("succeeded", CUTOFF)
EVAL = CUTOFF + timedelta(hours=1)


def _spy() -> dict[str, Any]:
    det, _ = parse_determination(SPY_FILE.read_bytes())
    assert det is not None
    return det


def test_reporting_status_spy() -> None:
    det = _spy()
    r = reporting_status(OK_RUN, Decision("determined", determination_id=det["determination_id"]), det, "not_found",
                         DISPOSAL, EVAL)  # fmt: skip
    assert r.status == "non_reporting_assumed"
    assert r.assumptions == (
        "A1_trustee_cmc", "A2_no_uk_branch", "A3_indenture", "A4_no_other_exclusion", "A5_part3_not_applying",
        "A6_law_current", "A7_identity_continuity", "A8_inventory_complete", "A9_terms_continuity",
    )  # fmt: skip


def test_reporting_status_run_gates() -> None:
    det = make_det()
    determined = Decision("determined")
    assert reporting_status(None, None, None, None, DISPOSAL, EVAL).status == "no_run"
    assert (
        reporting_status(RunState("failed", None), determined, det, "not_found", DISPOSAL, EVAL).status == "run_failed"
    )
    assert reporting_status(OK_RUN, None, det, "not_found", DISPOSAL, EVAL).status == "not_observed"
    for outcome in ("fund_observation_unavailable", "not_candidate", "no_determination", "binding_mismatch"):
        r = reporting_status(OK_RUN, Decision(outcome), None, None, date(2000, 1, 1), EVAL)  # type: ignore[arg-type]
        assert r.status == outcome


def test_reporting_status_steps() -> None:
    det = make_det()
    determined = Decision("determined")
    assert reporting_status(OK_RUN, determined, det, "not_found", date(2026, 1, 25), EVAL).status == "outside_interval"
    assert reporting_status(OK_RUN, determined, det, "not_found", date(2026, 10, 15), EVAL).status == "outside_interval"
    assert reporting_status(OK_RUN, Decision("stale", "x"), det, "not_found", DISPOSAL, EVAL).status == "stale"
    neg = _with(make_det(), "s356_condition_b", "not_met", ["A1_trustee_cmc"])
    r = reporting_status(OK_RUN, determined, neg, "not_found", DISPOSAL, EVAL)
    assert (r.status, r.assumptions) == ("not_offshore_fund", ("A1_trustee_cmc", *POLICY_ASSUMPTIONS[1:]))
    unres = _with(make_det(), "s356_condition_a", "unresolved")
    r = reporting_status(OK_RUN, determined, unres, "not_found", DISPOSAL, EVAL)
    assert (r.status, r.assumptions) == ("offshore_fund_unresolved", POLICY_ASSUMPTIONS[1:])
    for search, reason in (
        ("listed", "list_match"),
        ("unresolved", "list_match"),
        ("unavailable", "search_unavailable"),
    ):
        r = reporting_status(OK_RUN, determined, det, search, DISPOSAL, EVAL)
        assert (r.status, r.reason) == ("reporting_status_unestablished", reason)
    r = reporting_status(OK_RUN, determined, det, "not_found", DISPOSAL, EVAL)
    assert (r.status, r.assumptions) == ("non_reporting_assumed", POLICY_ASSUMPTIONS)


def test_reporting_status_review_deadline_precedes() -> None:
    det = make_det()
    after = datetime(2027, 10, 15, 0, 0, 1, tzinfo=UTC)
    for observed in (Decision("determined"), Decision("stale", "x"), Decision("documents_unavailable", "prospectus")):
        assert reporting_status(OK_RUN, observed, det, "not_found", DISPOSAL, after).status == "review_out_of_date"
    on_deadline = datetime(2027, 10, 14, 23, 59, tzinfo=UTC)
    assert reporting_status(OK_RUN, Decision("determined"), det, "not_found", DISPOSAL, on_deadline).status == (
        "non_reporting_assumed"
    )


def test_questions_cover_offshore_inputs() -> None:
    assert set(QUESTIONS) == set(ANSWERS)


def test_undecodable_xml_with_the_pinned_hash_is_changed_not_raised() -> None:
    """An XML document is bound by its bytes, but its quotes need its text: undecodable bytes are not evidence."""
    det = make_det()
    xml_url = _BASE_URL + "primary_doc.xml"
    raw = b"<edgarSubmission>\xff</edgarSubmission>"
    det["documents"][0]["url"] = xml_url
    det["documents"][0]["content_sha256"] = hashlib.sha256(raw).hexdigest()
    d = decide(SPY_OBS, {CIK: det}, {xml_url: [Fetch(10, FETCHED, raw)]}, {}, subs(), (), CUTOFF)
    assert (d.outcome, d.refusal) == ("document_changed", "prospectus")


def test_deeply_nested_json_is_refused_not_raised() -> None:
    deep = b"[" * 100_000 + b"]" * 100_000
    assert parse_determination(deep)[0] is None
    assert submissions_coverage(make_det(), Fetch(1, FETCHED, deep), CUTOFF) == "submissions unsound"
    assert validate(json.loads(b"[" * 500 + b"]" * 500)) != []
