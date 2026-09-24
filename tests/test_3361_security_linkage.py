"""#3361 acceptance item 1: pure fixtures for the dated series ↔ CIK linkage bundle.

Each test writes tiny Form 3/4/5 quarter archives + a #3360-shaped submissions pin, runs
the real builder, loads the bundle through the policy-bound loader and reads it. Spec:
``docs/proposals/ta/2026-09-24-3361-security-linkage.md``.
"""

from __future__ import annotations

import hashlib
import json
import zipfile
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pytest

from app.services import security_linkage as sl
from app.services.r6_pit_bundle import R6PitBundleError
from app.services.security_linkage import Grammar, Reason, SecurityLinkageError, load_security_linkage
from scripts.build_3361_security_linkage import build, quarter_end

HEADER = ("ACCESSION_NUMBER", "FILING_DATE", "DOCUMENT_TYPE", "ISSUERCIK", "ISSUERNAME", "ISSUERTRADINGSYMBOL")
INTRADER = sl.IN_SCOPE_VENDOR
C1, C2, C3 = "0000000001", "0000000002", "0000000003"
LAST_QUARTER = (2021, 4)
Q2019 = "insider_2019q1.zip"  # ledger keys are quarter FILE names


def _accn(n: int) -> str:
    return f"0000000000-20-{n:06d}"


def _ny4pm(day: str) -> str:
    """A 16:00 New York acceptance on ``day`` (EST/EDT handled by picking the UTC hour)."""
    d = date.fromisoformat(day)
    hour = 20 if 3 <= d.month <= 10 else 21
    return f"{day}T{hour}:00:00.000Z"


@dataclass
class Corpus:
    """Rows per quarter, and the submissions index per issuer CIK."""

    rows: dict[str, list[str]] = field(default_factory=dict)
    filings: dict[str, list[tuple[str, str | None, str]]] = field(default_factory=dict)
    raw_members: dict[str, bytes] = field(default_factory=dict)  # quarter -> raw SUBMISSION.tsv
    n: int = 0

    def file(
        self,
        cik: str,
        symbol: str,
        acceptance: str | None,
        *,
        document_type: str = "4",
        index_form: str | None = None,
        raw_cik: str | None = None,
        accession: str | None = None,
        in_index: bool = True,
        filing_date: str = "01-JAN-2019",
    ) -> str:
        """One Form 4 row; its quarter is taken from ``acceptance`` (or 2019q1)."""
        self.n += 1
        accn = accession or _accn(self.n)
        day = date.fromisoformat(acceptance[:10]) if acceptance and acceptance[:4].isdigit() else date(2019, 1, 1)
        quarter = f"{day.year}q{(day.month - 1) // 3 + 1}"
        fields = (accn, filing_date, document_type, raw_cik if raw_cik is not None else str(int(cik)), "X", symbol)
        self.rows.setdefault(quarter, []).append("\t".join(fields))
        if in_index:
            self.filings.setdefault(cik, []).append((accn, acceptance, index_form or document_type))
        return accn


def _write_zip(path: Path, members: Mapping[str, bytes | str]) -> Path:
    with zipfile.ZipFile(path, "w") as archive:
        for name, body in members.items():
            archive.writestr(name, body)
    return path


def _quarter_names(last: tuple[int, int] = LAST_QUARTER) -> list[str]:
    names = []
    year, quarter = sl.FIRST_QUARTER
    while (year, quarter) <= last:
        names.append(f"{year}q{quarter}")
        year, quarter = (year + 1, 1) if quarter == 4 else (year, quarter + 1)
    return names


def _tsv(lines: list[str]) -> str:
    return "\n".join(["\t".join(HEADER), *lines]) + "\n"


def _submissions(corpus: Corpus) -> dict[str, Any]:
    members: dict[str, Any] = {}
    for cik, filings in corpus.filings.items():
        members[f"CIK{cik}.json"] = {
            "cik": cik,
            "filings": {
                "recent": {
                    "accessionNumber": [f[0] for f in filings],
                    "acceptanceDateTime": [f[1] for f in filings],
                    "form": [f[2] for f in filings],
                    "items": ["" for _ in filings],
                },
                "files": [],
            },
        }
    return members


def _series(series_id: int, symbol: str, first: str = "2008-01-01", last: str = "2021-12-31", vendor: str = INTRADER):
    return [series_id, vendor, symbol, first, last]


def _build(
    tmp_path: Path,
    corpus: Corpus,
    series: list[list[Any]],
    *,
    form25_rows: list[list[Any]] | None = None,
    form25_span: list[str] | None = None,
    form25: bool = True,
    name: str = "bundle",
    quarters: list[Path] | None = None,
    submissions: dict[str, Any] | None = None,
    crosscheck_series: list[list[Any]] | None = None,
    instrument_cik_history: list[list[Any]] | None = None,
) -> tuple[sl.SecurityLinkageBundle, dict[str, Any], dict[str, Any]]:
    src = tmp_path / f"src-{name}"
    (src / "pit" / "inputs").mkdir(parents=True)
    if quarters is None:
        quarters = []
        for q in _quarter_names():
            member = corpus.raw_members.get(q, _tsv(corpus.rows.get(q, [])))
            quarters.append(_write_zip(src / f"insider_{q}.zip", {"SUBMISSION.tsv": member}))
    subs_members = {k: json.dumps(v) for k, v in (submissions or _submissions(corpus)).items()}
    subs = _write_zip(src / "pit" / "inputs" / "submissions.zip", subs_members)
    pit_manifest = {
        "input_sha256": {"submissions": hashlib.sha256(subs.read_bytes()).hexdigest()},
        "supported_through": "2022-06-30",
    }
    (src / "pit" / "manifest.json").write_text(json.dumps(pit_manifest))
    rows = form25_rows or []
    db = {
        "series_inventory": series,
        "form25": {
            "rows": rows,
            "span": form25_span if form25_span else (["2013-01-01", "2021-12-31"] if rows else None),
        },
        "crosscheck_series": crosscheck_series or [],
        "instrument_cik_history": instrument_cik_history or [],
    }
    out = tmp_path / name
    manifest = build(
        quarters=quarters,
        pit_bundle=src / "pit",
        pit_manifest_sha256=hashlib.sha256((src / "pit" / "manifest.json").read_bytes()).hexdigest(),
        db=db,
        out=out,
        form25=form25,
    )
    ledger = json.loads((out / sl.LEDGER_FILENAME).read_bytes())
    return load_security_linkage(out, expected_manifest_sha256=_sha(out)), manifest, ledger


def _sha(root: Path) -> str:
    return hashlib.sha256((root / sl.MANIFEST_FILENAME).read_bytes()).hexdigest()


def _d(text: str) -> date:
    return date.fromisoformat(text)


# ------------------------------------------------------------------ linking


def test_single_cik_link_is_public_only_after_the_acceptance_ny_date_and_expires(tmp_path: Path) -> None:
    corpus = Corpus()
    corpus.file(C1, "ABC", _ny4pm("2019-03-01"))
    bundle, _, _ = _build(tmp_path, corpus, [_series(1, "ABC")])

    on_d = bundle.link_as_of(1, _d("2019-03-01"))
    assert (on_d.reason, on_d.detail, on_d.grammar) == (Reason.NO_RECENT_EVIDENCE, "never_seen", "plain")
    linked = bundle.link_as_of(1, _d("2019-03-02"))
    assert (linked.reason, linked.cik, linked.basis, linked.q_alias) == (Reason.LINKED, C1, "single_cik", False)
    assert [o.cik for o in linked.observations] == [C1]
    # +730: the last day inside the window; +731: aged out, but seen before -> no sub-reason.
    assert bundle.link_as_of(1, _d("2019-03-01") + timedelta(days=730)).reason is Reason.LINKED
    expired = bundle.link_as_of(1, _d("2019-03-01") + timedelta(days=731))
    assert (expired.reason, expired.detail, expired.observations) == (Reason.NO_RECENT_EVIDENCE, None, ())


def test_strict_succession_then_reversion_follows_the_prefix(tmp_path: Path) -> None:
    corpus = Corpus()
    corpus.file(C1, "ACET", _ny4pm("2015-01-05"))
    corpus.file(C2, "ACET", _ny4pm("2016-01-05"))
    corpus.file(C1, "ACET", _ny4pm("2017-01-05"))
    bundle, _, _ = _build(tmp_path, corpus, [_series(1, "ACET")])

    assert bundle.link_as_of(1, _d("2015-06-01")).cik == C1
    succession = bundle.link_as_of(1, _d("2016-01-06"))
    assert (succession.reason, succession.cik, succession.basis) == (Reason.LINKED, C2, "succession")
    # The 2015 C1 filing aged out (D - 730 = 2015-01-07): C2 then C1 again is a succession back.
    reverted = bundle.link_as_of(1, _d("2017-01-06"))
    assert (reverted.reason, reverted.cik, reverted.basis) == (Reason.LINKED, C1, "succession")


def test_equal_acceptance_across_ciks_is_a_conflict(tmp_path: Path) -> None:
    corpus = Corpus()
    corpus.file(C1, "AB", _ny4pm("2019-03-01"))
    corpus.file(C2, "AB", _ny4pm("2019-03-01"))
    bundle, _, _ = _build(tmp_path, corpus, [_series(1, "AB")])
    result = bundle.link_as_of(1, _d("2019-03-02"))
    assert (result.reason, result.cik, len(result.observations)) == (Reason.CONFLICTING_EVIDENCE, None, 2)


def test_interleaved_stray_is_a_conflict_that_lapses_as_its_evidence_ages_out(tmp_path: Path) -> None:
    corpus = Corpus()
    first, stray = _d("2015-01-05"), _d("2015-06-01")
    corpus.file(C1, "ACM", _ny4pm(first.isoformat()))
    corpus.file(C2, "ACM", _ny4pm(stray.isoformat()))
    corpus.file(C1, "ACM", _ny4pm("2016-06-01"))
    bundle, _, _ = _build(tmp_path, corpus, [_series(1, "ACM")])
    assert bundle.link_as_of(1, _d("2016-06-02")).reason is Reason.CONFLICTING_EVIDENCE
    assert bundle.link_as_of(1, first + timedelta(days=730)).reason is Reason.CONFLICTING_EVIDENCE
    # C1's first filing aged out: C2 then C1 is now a strict succession (a stated cost: the
    # stray predecessor persists while its evidence is under 730 days old) ...
    stale = bundle.link_as_of(1, first + timedelta(days=731))
    assert (stale.reason, stale.cik, stale.basis) == (Reason.LINKED, C1, "succession")
    # ... until the stray itself ages out.
    lapsed = bundle.link_as_of(1, stray + timedelta(days=731))
    assert (lapsed.reason, lapsed.cik, lapsed.basis) == (Reason.LINKED, C1, "single_cik")


def test_decide_orders_ciks_by_first_acceptance() -> None:
    def obs(cik: str, acceptance: str) -> dict[str, str]:
        return {"cik": cik, "acceptance": acceptance}

    t = ["2019-01-01T00:00:00.000Z", "2019-02-01T00:00:00.000Z", "2019-03-01T00:00:00.000Z"]
    assert sl.decide([obs(C2, t[2]), obs(C1, t[0]), obs(C1, t[1])]) == (Reason.LINKED, C2, "succession")
    assert sl.decide([obs(C1, t[0]), obs(C2, t[1]), obs(C3, t[2])]) == (Reason.LINKED, C3, "succession")
    assert sl.decide([obs(C1, t[0]), obs(C2, t[1]), obs(C1, t[2])])[0] is Reason.CONFLICTING_EVIDENCE


# ------------------------------------------------------------------ clock


def test_acceptance_later_than_filing_date_is_public_by_acceptance_in_new_york(tmp_path: Path) -> None:
    corpus = Corpus()
    # FILING_DATE says 2019-03-01; accepted 2019-03-05 22:00 New York (03:00Z on the 6th).
    corpus.file(C1, "LATE", "2019-03-06T03:00:00.000Z", filing_date="01-MAR-2019")
    bundle, _, _ = _build(tmp_path, corpus, [_series(1, "LATE")])
    assert bundle.link_as_of(1, _d("2019-03-05")).detail == "never_seen"
    assert bundle.link_as_of(1, _d("2019-03-06")).reason is Reason.LINKED


# ------------------------------------------------------------------ admission


def test_admission_outcomes_are_ledgered_with_locators_and_reconcile(tmp_path: Path) -> None:
    corpus = Corpus()
    good = corpus.file(C1, "ABC", _ny4pm("2019-03-01"))
    corpus.file(C1, "ABC", _ny4pm("2019-03-02"), in_index=False)  # absent from the issuer's index
    corpus.file(C1, "ABC", "2019-03-03T10:00:00")  # naive timestamp -> no valid acceptance
    corpus.file(C1, "ABC", _ny4pm("2019-03-04"), index_form="8-K")  # index form not 3/4/5
    corpus.file(C1, "ABC", _ny4pm("2019-03-05"), document_type="4X")
    corpus.file(C1, "N/A", _ny4pm("2019-03-06"))
    corpus.file(C1, "BF B", _ny4pm("2019-03-07"))
    corpus.file(C1, "A,B", _ny4pm("2019-03-08"))
    corpus.file(C1, "ABC", _ny4pm("2019-03-09"), raw_cik="0")
    corpus.file(C1, "ABC", _ny4pm("2019-03-10"), raw_cik=" 1")
    corpus.file(C1, "ABC", _ny4pm("2019-03-11"), accession="not-an-accession")
    # collapse: two identical rows; conflict: same accession, different symbol.
    dup = corpus.file(C1, "ABC", _ny4pm("2019-03-12"))
    corpus.rows["2019q1"].append(corpus.rows["2019q1"][-1])
    clash = corpus.file(C1, "ABC", _ny4pm("2019-03-13"))
    corpus.rows["2019q1"].append(corpus.rows["2019q1"][-1].replace("\tABC", "\tXYZ"))
    corpus.rows["2019q1"].append("")  # blank line
    corpus.rows["2019q1"].append("too\tfew")
    bundle, _, ledger = _build(tmp_path, corpus, [_series(1, "ABC")])

    assert ledger["rows_in"][Q2019] == 17
    assert ledger["row_outcomes"][Q2019] == {
        "accession_conflict": 2,
        "empty_symbol": 1,
        "malformed_cik": 2,
        "malformed_row": 3,
        "multi_symbol": 2,
        "no_acceptance": 3,
        "stored": 3,
        "unsupported_document_type": 1,
    }
    assert ledger["observation_outcomes"] == {"no_acceptance": 3, "stored": 2}
    assert sorted(raw for _, _, raw in ledger["locators"]["malformed_cik"]) == [" 1", "0"]
    assert [loc[0] for loc in ledger["locators"]["accession_conflict"]] == [Q2019, Q2019]
    assert sum(len(v) for v in ledger["locators"].values()) == 17 - 3  # every non-stored row

    stored = bundle.link_as_of(1, _d("2019-03-20")).observations
    assert [(o.accession, o.multiplicity) for o in stored] == [(good, 1), (dup, 2)]
    assert clash not in {o.accession for o in stored}


def test_issuer_integrity_failure_excludes_every_observation_of_the_issuer(tmp_path: Path) -> None:
    corpus = Corpus()
    corpus.file(C1, "ABC", _ny4pm("2019-03-01"))
    corpus.file(C2, "XYZ", _ny4pm("2019-03-01"), in_index=False)  # C2 has no submissions member at all
    subs = _submissions(corpus)
    subs[f"CIK{C1}.json"]["cik"] = C3  # cik mismatch -> #3360 rule-5 failure
    bundle, _, ledger = _build(tmp_path, corpus, [_series(1, "ABC"), _series(2, "XYZ")], submissions=subs)
    assert ledger["issuer_integrity_failures"] == {C1: "submissions_cik_mismatch", C2: "no_submissions_entry"}
    assert ledger["observation_outcomes"] == {"issuer_integrity_excluded": 2}
    assert bundle.link_as_of(1, _d("2019-03-02")).detail == "never_seen"


def test_placeholder_is_evidence_side_only_so_root_na_still_parses(tmp_path: Path) -> None:
    corpus = Corpus()
    corpus.file(C1, "NA", _ny4pm("2019-03-01"))
    bundle, _, ledger = _build(tmp_path, corpus, [_series(1, "NA")])
    assert ledger["row_outcomes"][Q2019] == {"empty_symbol": 1}
    result = bundle.link_as_of(1, _d("2019-03-02"))
    assert (result.reason, result.grammar, result.detail) == (Reason.NO_RECENT_EVIDENCE, "plain", "never_seen")


# ------------------------------------------------------------------ matching


def test_q_alias_only_for_a_four_character_root(tmp_path: Path) -> None:
    corpus = Corpus()
    corpus.file(C1, "BBBYQ", _ny4pm("2019-03-01"))
    corpus.file(C2, "ABCQ", _ny4pm("2019-03-01"))
    bundle, _, _ = _build(tmp_path, corpus, [_series(1, "BBBY"), _series(2, "ABC")])
    alias = bundle.link_as_of(1, _d("2019-03-02"))
    assert (alias.reason, alias.cik, alias.q_alias, alias.observations[0].q_alias) == (Reason.LINKED, C1, True, True)
    assert bundle.link_as_of(2, _d("2019-03-02")).detail == "never_seen"


def test_class_series_matches_every_separator_but_never_the_bare_forms(tmp_path: Path) -> None:
    corpus = Corpus()
    for i, symbol in enumerate(("BF.B", "BF-B", "BF/B", "BF_B")):
        corpus.file(C1, symbol, _ny4pm(f"2019-03-0{i + 1}"))
    corpus.file(C2, "BFB", _ny4pm("2019-03-05"))
    corpus.file(C3, "BF", _ny4pm("2019-03-05"))
    bundle, _, _ = _build(tmp_path, corpus, [_series(1, "BF_B"), _series(2, "BF")])
    result = bundle.link_as_of(1, _d("2019-03-10"))
    assert (result.reason, result.cik, result.grammar, len(result.observations)) == (Reason.LINKED, C1, "class:B", 4)
    plain = bundle.link_as_of(2, _d("2019-03-10"))
    assert (plain.cik, [o.cik for o in plain.observations]) == (C3, [C3])


# ------------------------------------------------------------------ grammar


@pytest.mark.parametrize(
    ("symbol", "grammar"),
    [
        ("ABC", Grammar("plain", "ABC")),
        ("BRK_B", Grammar("class", "BRK", share_class="B")),
        ("ABC_P", Grammar("non_common", "ABC", token="P")),
        ("ABC_P_A_CL", Grammar("non_common", "ABC", token="P")),
        ("ABC_WS", Grammar("non_common", "ABC", token="WS")),
        ("ABC_R_W", Grammar("non_common", "ABC", token="R")),
        ("ABC_CL", Grammar("non_common", "ABC", token="CL")),
        ("ABC_A_B", Grammar("unparsed", "ABC")),
        ("ABC_", Grammar("unparsed", "ABC")),
        ("ABC_1", Grammar("unparsed", "ABC")),
        ("ABCDEF", Grammar("unparsed")),
        ("abc", Grammar("unparsed")),
        ("AB.C", Grammar("unparsed")),
        ("ATEST", Grammar("test")),
        ("ZXYZ_A", Grammar("test")),
        ("CTEST_Q", Grammar("test")),
    ],
)
def test_vendor_grammar(symbol: str, grammar: Grammar) -> None:
    assert sl.parse_vendor_symbol(symbol) == grammar


def test_grammar_abstentions_carry_their_class(tmp_path: Path) -> None:
    series = [_series(1, "ABC_P_A_CL"), _series(2, "ABC_WS"), _series(3, "ABC_A_B"), _series(4, "ZVZZT")]
    bundle, _, _ = _build(tmp_path, Corpus(), series)
    d = _d("2019-03-02")
    assert bundle.link_as_of(1, d).label == "non_common_symbol_form:P"
    assert bundle.link_as_of(2, d).grammar == "non_common:WS"
    assert (bundle.link_as_of(3, d).reason, bundle.link_as_of(3, d).grammar) == (
        Reason.UNPARSED_SYMBOL_FORM,
        "unparsed",
    )
    assert bundle.link_as_of(4, d).reason is Reason.VENDOR_TEST_SYMBOL


def test_overlapping_match_sets_abstain_both_series_whole(tmp_path: Path) -> None:
    corpus = Corpus()
    corpus.file(C1, "ABCDQ", _ny4pm("2019-03-01"))
    bundle, manifest, _ = _build(tmp_path, corpus, [_series(1, "ABCD"), _series(2, "ABCDQ"), _series(3, "XYZ")])
    assert manifest["collisions"] == [1, 2]
    for series_id in (1, 2):
        assert bundle.link_as_of(series_id, _d("2019-03-02")).reason is Reason.VENDOR_SYMBOL_COLLISION
        # Applied at every D, including before any evidence exists.
        assert bundle.link_as_of(series_id, _d("2010-01-01")).reason is Reason.VENDOR_SYMBOL_COLLISION


# ------------------------------------------------------------------ Form 25


def test_form25_flags_only_before_d_inside_the_span_and_never_move_the_link(tmp_path: Path) -> None:
    corpus = Corpus()
    corpus.file(C1, "ABC", _ny4pm("2019-03-01"))
    rows = [
        ["0000000000-19-000001", "2019-05-01", C1, "ABC"],
        ["0000000000-19-000002", "2019-06-01", C1, "OTHER"],
        ["0000000000-19-000003", "2019-07-01", C1, None],
        ["0000000000-19-000004", "2019-07-01", C2, "ABC"],  # another CIK: never on this result
    ]
    bundle, _, _ = _build(
        tmp_path, corpus, [_series(1, "ABC")], form25_rows=rows, form25_span=["2013-01-02", "2019-12-31"]
    )
    assert bundle.link_as_of(1, _d("2019-05-01")).form25 == ()
    flagged = bundle.link_as_of(1, _d("2019-07-02"))
    assert [(f.filed_date, f.match) for f in flagged.form25] == [
        ("2019-05-01", "symbol_match"),
        ("2019-06-01", "symbol_other"),
        ("2019-07-01", "symbol_null"),
    ]
    assert (flagged.reason, flagged.cik, flagged.form25_unobserved) == (Reason.LINKED, C1, False)
    after_span = bundle.link_as_of(1, _d("2020-01-01"))
    assert (after_span.reason, after_span.cik, after_span.form25_unobserved, after_span.form25) == (
        Reason.LINKED,
        C1,
        True,
        (),
    )


def test_without_form25_mode_is_a_different_policy_and_never_observes(tmp_path: Path) -> None:
    corpus = Corpus()
    corpus.file(C1, "ABC", _ny4pm("2019-03-01"))
    rows = [["0000000000-19-000001", "2019-05-01", C1, "ABC"]]
    bundle, manifest, _ = _build(tmp_path, corpus, [_series(1, "ABC")], form25_rows=rows, form25=False)
    assert manifest["form25_mode"] is False and "form25" not in manifest["input_sha256"]
    assert manifest["policy"] != sl.policy_sha256(form25=True)
    result = bundle.link_as_of(1, _d("2019-07-02"))
    assert (result.cik, result.form25_unobserved, result.form25) == (C1, True, ())


def test_empty_register_is_unobserved_everywhere(tmp_path: Path) -> None:
    corpus = Corpus()
    corpus.file(C1, "ABC", _ny4pm("2019-03-01"))
    bundle, manifest, _ = _build(tmp_path, corpus, [_series(1, "ABC")])
    assert manifest["form25_span"] is None
    assert bundle.link_as_of(1, _d("2019-07-02")).form25_unobserved is True


# ------------------------------------------------------------------ bounds


def test_bounds_and_scope_precedence(tmp_path: Path) -> None:
    series = [
        _series(1, "ABC", first="2007-06-01", last="2021-06-30"),
        _series(2, "ABC", vendor="paperswithbacktest/Stocks-Daily-Price"),
        _series(3, "ZZZ", last="2021-12-31"),
    ]
    bundle, manifest, _ = _build(tmp_path, Corpus(), series)
    assert manifest["supported_through"] == "2021-12-31"  # min(quarter end, #3360, Intrader capture)
    assert bundle.link_as_of(99, _d("2019-01-01")).reason is Reason.SERIES_NOT_IN_BUNDLE
    assert bundle.link_as_of(2, _d("2019-01-01")).reason is Reason.VENDOR_OUT_OF_SCOPE
    assert bundle.link_as_of(1, _d("2022-01-01")).reason is Reason.AFTER_CAPTURE
    assert bundle.link_as_of(1, _d("2021-07-01")).reason is Reason.OUTSIDE_SERIES
    assert bundle.link_as_of(1, _d("2007-05-31")).reason is Reason.OUTSIDE_SERIES
    assert bundle.link_as_of(1, _d("2007-12-31")).reason is Reason.BEFORE_COVERAGE
    assert bundle.link_as_of(1, _d("2008-01-01")).reason is Reason.NO_RECENT_EVIDENCE


def test_quarter_end() -> None:
    assert [quarter_end(2019, q) for q in (1, 2, 3, 4)] == [
        date(2019, 3, 31),
        date(2019, 6, 30),
        date(2019, 9, 30),
        date(2019, 12, 31),
    ]


# ------------------------------------------------------------------ build failures


def _src_quarters(tmp_path: Path, override: dict[str, dict[str, bytes | str]] | None = None) -> list[Path]:
    src = tmp_path / "raw"
    src.mkdir(exist_ok=True)
    paths = []
    for q in _quarter_names():
        members = (override or {}).get(q, {"SUBMISSION.tsv": _tsv([])})
        paths.append(_write_zip(src / f"insider_{q}.zip", members))
    return paths


@pytest.mark.parametrize(
    ("override", "message"),
    [
        ({"2010q1": {"SUBMISSION.tsv": b"\xef\xbb\xbf" + _tsv([]).encode()}}, "byte-order mark"),
        ({"2010q1": {"SUBMISSION.tsv": _tsv([]).encode() + b"\xff\n"}}, "strict UTF-8"),
        ({"2010q1": {"SUBMISSION.tsv": "ACCESSION_NUMBER\tACCESSION_NUMBER\n"}}, "header"),
        ({"2010q1": {"SUBMISSION.tsv": "ACCESSION_NUMBER\tISSUERCIK\n"}}, "header"),
        ({"2010q1": {"SUBMISSION.tsv": _tsv([]), "x/SUBMISSION.tsv": _tsv([])}}, "exactly one"),
        ({"2010q1": {"OTHER.tsv": _tsv([])}}, "exactly one"),
    ],
)
def test_malformed_quarter_archive_fails_the_build(tmp_path: Path, override: dict[str, Any], message: str) -> None:
    with pytest.raises(RuntimeError, match=message):
        _build(tmp_path, Corpus(), [_series(1, "ABC")], quarters=_src_quarters(tmp_path, override))
    assert not (tmp_path / "bundle").exists()  # a crashed build is deleted, never resumed


def test_duplicate_zip_member_fails_the_build(tmp_path: Path) -> None:
    quarters = _src_quarters(tmp_path)
    with zipfile.ZipFile(quarters[3], "a") as archive, pytest.warns(UserWarning):
        archive.writestr("SUBMISSION.tsv", _tsv([]))
    with pytest.raises(RuntimeError, match="duplicate zip members"):
        _build(tmp_path, Corpus(), [_series(1, "ABC")], quarters=quarters)


def test_gap_and_duplicate_quarters_fail_the_build(tmp_path: Path) -> None:
    quarters = _src_quarters(tmp_path)
    with pytest.raises(RuntimeError, match="not contiguous"):
        _build(tmp_path, Corpus(), [_series(1, "ABC")], quarters=quarters[:5] + quarters[6:], name="gap")
    with pytest.raises(RuntimeError, match="appears twice"):
        _build(tmp_path, Corpus(), [_series(1, "ABC")], quarters=[*quarters, quarters[0]], name="dup")
    with pytest.raises(RuntimeError, match="not contiguous"):
        _build(tmp_path, Corpus(), [_series(1, "ABC")], quarters=quarters[1:], name="late")


@pytest.mark.parametrize(
    ("series", "message"),
    [
        ([_series(1, "ABC"), _series(1, "XYZ")], "series_id repeats"),
        ([_series(1, "ABC", first="2020-01-02", last="2020-01-01")], "reversed"),
        ([_series(0, "ABC")], "positive integer"),
        ([_series(1, " ")], "empty vendor symbol"),
        ([_series(1, "ABC", vendor="cboe")], "no icyDenev/Intrader series"),
    ],
)
def test_bad_inventory_fails_the_build(tmp_path: Path, series: list[list[Any]], message: str) -> None:
    with pytest.raises(RuntimeError, match=message):
        _build(tmp_path, Corpus(), series)


def test_bad_form25_row_fails_the_build(tmp_path: Path) -> None:
    rows = [["0000000000-19-000001", "2019-05-01", "12x", "ABC"]]
    with pytest.raises(RuntimeError, match="malformed issuer_cik"):
        _build(tmp_path, Corpus(), [_series(1, "ABC")], form25_rows=rows)


def test_pinned_3360_manifest_and_its_submissions_must_agree(tmp_path: Path) -> None:
    src = tmp_path / "pit"
    (src / "inputs").mkdir(parents=True)
    _write_zip(src / "inputs" / "submissions.zip", {})
    (src / "manifest.json").write_text(
        json.dumps({"input_sha256": {"submissions": "0" * 64}, "supported_through": "2022-06-30"})
    )
    db = {"series_inventory": [_series(1, "ABC")], "form25": {"rows": [], "span": None}}
    db |= {"crosscheck_series": [], "instrument_cik_history": []}
    kwargs: dict[str, Any] = {"quarters": _src_quarters(tmp_path), "pit_bundle": src, "db": db}
    with pytest.raises(RuntimeError, match="!= pinned"):
        build(pit_manifest_sha256="f" * 64, out=tmp_path / "a", **kwargs)
    with pytest.raises(RuntimeError, match="does not match its manifest"):
        build(
            pit_manifest_sha256=hashlib.sha256((src / "manifest.json").read_bytes()).hexdigest(),
            out=tmp_path / "b",
            **kwargs,
        )


def test_existing_bundle_directory_is_refused(tmp_path: Path) -> None:
    (tmp_path / "bundle").mkdir()
    with pytest.raises(FileExistsError):
        _build(tmp_path, Corpus(), [_series(1, "ABC")])


def test_rebuild_is_byte_deterministic(tmp_path: Path) -> None:
    corpus = Corpus()
    corpus.file(C1, "ABC", _ny4pm("2019-03-01"))
    corpus.file(C2, "ABC", _ny4pm("2019-03-05"))
    _, first, _ = _build(tmp_path, corpus, [_series(1, "ABC")], name="a")
    _, second, _ = _build(tmp_path, corpus, [_series(1, "ABC")], name="b")
    assert first == second
    for relative in (sl.MANIFEST_FILENAME, sl.LEDGER_FILENAME, "series/1.json"):
        assert (tmp_path / "a" / relative).read_bytes() == (tmp_path / "b" / relative).read_bytes()


# ------------------------------------------------------------------ loader refusals


@pytest.fixture
def built(tmp_path: Path) -> Path:
    corpus = Corpus()
    corpus.file(C1, "ABC", _ny4pm("2019-03-01"))
    _build(tmp_path, corpus, [_series(1, "ABC"), _series(2, "XYZ")])
    return tmp_path / "bundle"


def test_unpinned_manifest_digest_is_refused(built: Path) -> None:
    with pytest.raises(SecurityLinkageError, match="!= pinned"):
        load_security_linkage(built, expected_manifest_sha256="0" * 64)


def test_policy_mismatch_is_refused(built: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sl, "WINDOW_DAYS", 365)
    with pytest.raises(SecurityLinkageError, match="POLICY"):
        load_security_linkage(built, expected_manifest_sha256=_sha(built))


def test_corrupt_series_document_is_refused(built: Path) -> None:
    path = built / "series" / "1.json"
    path.chmod(0o644)
    path.write_text(path.read_text().replace('"ABC"', '"ABD"'))
    bundle = load_security_linkage(built, expected_manifest_sha256=_sha(built))
    with pytest.raises(SecurityLinkageError, match="digest moved"):
        bundle.link_as_of(1, _d("2019-03-02"))


def test_moved_ledger_is_refused(built: Path) -> None:
    path = built / sl.LEDGER_FILENAME
    path.chmod(0o644)
    path.write_text(path.read_text() + " ")
    with pytest.raises(SecurityLinkageError, match="ledger"):
        load_security_linkage(built, expected_manifest_sha256=_sha(built))


def test_symlinked_manifest_is_refused(built: Path, tmp_path: Path) -> None:
    moved = tmp_path / "elsewhere.json"
    (built / sl.MANIFEST_FILENAME).rename(moved)
    (built / sl.MANIFEST_FILENAME).symlink_to(moved)
    with pytest.raises(R6PitBundleError):
        load_security_linkage(built, expected_manifest_sha256=hashlib.sha256(moved.read_bytes()).hexdigest())


@pytest.mark.parametrize(
    "mutate",
    [
        lambda doc: doc["observations"].reverse(),
        lambda doc: doc["observations"][0].update(symbol="XYZ"),
        lambda doc: doc["observations"][0].update(multiplicity=0),
        lambda doc: doc["observations"][0].update(cik="1"),
        lambda doc: doc["grammar"].update(kind="class"),
        lambda doc: doc.update(collision="yes"),
        lambda doc: doc["form25"].append({"accession": "a", "filed_date": "2019-01-01", "issuer_cik": C3}),
    ],
)
def test_validate_series_refuses_malformed_documents(built: Path, mutate: Any) -> None:
    document = json.loads((built / "series" / "1.json").read_bytes())
    document["observations"].append(
        {**document["observations"][0], "accession": _accn(999), "acceptance": "2019-03-05T21:00:00.000Z"}
    )
    sl.validate_series(document, series_id=1)
    mutate(document)
    with pytest.raises(SecurityLinkageError):
        sl.validate_series(document, series_id=1)


def test_policy_files_cover_every_repo_module_the_builder_and_reader_import() -> None:
    # Drift guard (as #3360): logic moved into a new repo module must join POLICY_FILES.
    import ast

    root = Path(sl.__file__).resolve().parents[2]
    imported: set[str] = set()
    for relative in ("app/services/security_linkage.py", "scripts/build_3361_security_linkage.py"):
        for node in ast.walk(ast.parse((root / relative).read_text())):
            if isinstance(node, ast.ImportFrom) and node.module:
                modules = [node.module]
            elif isinstance(node, ast.Import):
                modules = [alias.name for alias in node.names]
            else:
                continue
            imported |= {m.replace(".", "/") + ".py" for m in modules if m.split(".")[0] in {"app", "scripts"}}
    # ``app.config`` / ``app.security.master_key`` are CLI-only (paths + DB URL), not logic.
    assert imported - {"app/config.py", "app/security/master_key.py"} <= set(sl.POLICY_FILES)


# ------------------------------------------------------------------ causal reference (item 2)


def _causal_corpus() -> tuple[Corpus, list[list[Any]], list[list[Any]]]:
    corpus = Corpus()
    corpus.file(C1, "ACET", _ny4pm("2015-01-05"))
    corpus.file(C2, "ACETQ", _ny4pm("2016-01-05"))
    corpus.file(C1, "ACET", _ny4pm("2016-01-05"))  # same acceptance as C2: a cross-CIK tie -> conflict
    corpus.file(C3, "ABC", _ny4pm("2019-03-01"))
    corpus.file(C3, "ABC", _ny4pm("2019-03-01"), in_index=False)
    series = [
        _series(1, "ACET"),
        _series(2, "ABC", first="2007-01-02", last="2020-12-31"),
        _series(3, "ABC_WS"),
        _series(4, "XYZ", vendor="cboe"),
        _series(5, "ABCD"),
        _series(6, "ABCDQ"),
    ]
    form25 = [["0000000000-19-000001", "2019-05-01", C3, "ABC"], ["0000000000-16-000001", "2016-02-01", C2, None]]
    return corpus, series, form25


def test_causal_reference_agrees_with_the_bundle(tmp_path: Path) -> None:
    from scripts.causal_3361_security_linkage import load_reference, verify

    corpus, series, form25 = _causal_corpus()
    bundle, manifest, _ = _build(tmp_path, corpus, series, form25_rows=form25)
    tally = verify(bundle, load_reference(tmp_path / "bundle", manifest))
    assert tally.mismatches == []
    assert tally.counts["compared"] > 50
    for reason in (
        "linked",
        "conflicting_evidence",
        "no_recent_evidence",
        "vendor_symbol_collision",
        "before_coverage",
    ):
        assert tally.counts[f"reason_{reason}"] > 0, reason


def test_causal_reference_catches_a_reader_that_leaks(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Negative control: a reader that looks one day ahead must be caught.
    from scripts.causal_3361_security_linkage import load_reference, verify

    corpus, series, form25 = _causal_corpus()
    bundle, manifest, _ = _build(tmp_path, corpus, series, form25_rows=form25)
    honest = bundle.link_as_of
    monkeypatch.setattr(bundle, "link_as_of", lambda sid, d: honest(sid, d + timedelta(days=1)))
    assert verify(bundle, load_reference(tmp_path / "bundle", manifest)).counts["mismatch"] > 0


# ------------------------------------------------------------------ cross-checks (item 3)


def test_etoro_crosscheck_classifies_by_history_containment(tmp_path: Path) -> None:
    from scripts.crosscheck_3361_security_linkage import check_etoro

    corpus = Corpus()
    for symbol, cik in (("AAA", C1), ("BBB", C2), ("CCC", C1), ("DDD", C1)):
        corpus.file(cik, symbol, _ny4pm("2021-03-01"))
    series = [_series(i, s) for i, s in enumerate(("AAA", "BBB", "CCC", "DDD", "EEE"), start=1)]
    crosscheck = [
        [1, 11, None, None],
        [2, 12, None, None],
        [3, 13, None, None],
        [4, 14, None, None],
        [5, 15, None, None],
    ]
    history = [
        [11, C1, "2020-01-01", None, "imported"],  # agree
        [12, C1, "2020-01-01", None, "imported"],  # disagree: the link says C2
        [13, C1, "2022-01-01", None, "imported"],  # starts after last_bar
        [14, "1", "2020-01-01", None, "imported"],  # malformed CIK
        [15, C1, "2020-01-01", None, "imported"],  # reader: never seen
    ]
    bundle, _, _ = _build(tmp_path, corpus, series, crosscheck_series=crosscheck, instrument_cik_history=history)
    inventory = json.loads((tmp_path / "bundle" / "inputs" / "series_inventory.json").read_bytes())
    report = check_etoro(bundle, inventory, crosscheck, history).to_json()
    assert (report["population"], report["agree"], report["disagree"]) == (5, 1, 1)
    assert report["disagreements"][0] | {} == {
        "series_id": 2,
        "at": "2021-12-31",
        "link": C2,
        "basis": "single_cik",
        "comparator": C1,
    }
    assert report["not_comparable_by_reason"] == {
        "comparator:malformed_history_row": 1,
        "comparator:no_history_row_contains_last_bar": 1,
        "reader:no_recent_evidence:never_seen": 1,
    }


def test_form25_crosscheck_uses_the_register_filed_on_the_delisting_date(tmp_path: Path) -> None:
    from scripts.crosscheck_3361_security_linkage import check_form25

    corpus = Corpus()
    corpus.file(C1, "AAA", _ny4pm("2019-03-01"))
    corpus.file(C2, "BBB", _ny4pm("2019-03-01"))
    series = [_series(1, "AAA"), _series(2, "BBB"), _series(3, "CCC_WS"), _series(4, "DDD", last="2019-02-01")]
    crosscheck = [[1, None, "(b)", "2019-06-03"], [2, None, "(a)(3)", "2019-06-03"], [3, None, "(b)", "2019-06-03"]]
    crosscheck.append([4, None, None, None])  # no provision: outside the population
    register = [
        ["0000000000-19-000001", "2019-06-03", C1, "AAA"],
        ["0000000000-19-000002", "2019-06-03", C1, "BBB"],
        ["0000000000-19-000003", "2019-06-04", C2, "BBB"],  # filed another day: not the comparator
    ]
    without, _, _ = _build(tmp_path, corpus, series, form25_rows=register, form25=False, crosscheck_series=crosscheck)
    inventory = json.loads((tmp_path / "bundle" / "inputs" / "series_inventory.json").read_bytes())
    report = check_form25(without, inventory, crosscheck, register).to_json()
    assert (report["population"], report["agree"], report["disagree"]) == (3, 1, 1)
    assert report["disagreements"][0]["series_id"] == 2
    assert report["not_comparable_by_reason"] == {"comparator:no_rule4_match_set": 1}


# ------------------------------------------------------------------ registry (item 5)


def test_registry_cell_citing_3361_stays_fail() -> None:
    from app.services.research_point_in_time import FIELD_REGISTRY, PROBE_MATRIX, RankingFamily

    cell = PROBE_MATRIX[RankingFamily.COMPANYFACTS_PIT]["historical_population"]
    assert cell.outcome == "fail" and cell.qualification is not None and "#3361" in cell.qualification
    assert FIELD_REGISTRY[RankingFamily.COMPANYFACTS_PIT].status == "refused"
