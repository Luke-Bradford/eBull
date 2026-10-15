"""``refresh_fund_determinations`` against the test database (#3616 slice 3d-3c-2; spec tests 4 and 6, the step's part).

Auto-marked ``db`` (pulls ``ebull_test_conn`` through the ``funds`` fixture, which wipes between tests). The generation
holding SPY and CSPX.L is ``tests.test_tax_fund_refresh_db``'s, and its fund observations are written by the real
``refresh_fund_evidence``; the determination is the small synthetic one of the pure tests, written to a temporary
directory with its authority snapshot. No test touches the network: SEC is an ``httpx.MockTransport`` and the clock
is fake. The schema's own refusals are ``tests/test_tax_fund_determination_db.py``'s.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest

from app.services import tax_fund_determination_refresh as tfdr
from app.services.tax_fund_determination import SUBMISSIONS_URL
from app.services.tax_fund_determination_refresh import (
    DeterminationReport,
    DeterminationRunFailed,
    refresh_fund_determinations,
)
from app.services.tax_fund_refresh import _store  # pyright: ignore[reportPrivateUsage]
from app.services.tax_matching import effective_tax_matching
from tests.test_tax_fund_determination import ANCHOR, AUTH_RAW, BODY, NEW_497, URL, make_det, subs_body
from tests.test_tax_fund_refresh_db import (  # noqa: F401 - `funds` is a fixture
    CSPX,
    SPY,
    FakeClock,
    T,
    Web,
    _connect,
    _rows,
    _run,
    funds,
)

CIK = "0000884394"
DID = f"fund_determination_v1:{CIK}:1"
SUBS_URL = SUBMISSIONS_URL.format(cik=CIK)
#: The determination step runs a minute after the fund step.
T1 = T + timedelta(minutes=1)

Responder = httpx.Response | Callable[[httpx.Request], httpx.Response]


@dataclass
class Sec:
    """SEC as the determination step reads it: the cited document and the filing list; each request is logged."""

    document: Responder = field(default_factory=lambda: httpx.Response(200, content=BODY))
    submissions: Responder = field(default_factory=lambda: httpx.Response(200, content=subs_body([ANCHOR])))
    log: list[str] = field(default_factory=list)

    def handle(self, request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        assert url in (URL, SUBS_URL), url
        assert request.headers["user-agent"]
        self.log.append("document" if url == URL else "submissions")
        r = self.document if url == URL else self.submissions
        return r(request) if callable(r) else httpx.Response(r.status_code, headers=r.headers, content=r.content)


def _write(directory: Path, det: dict[str, Any] | None = None, snapshot: bytes = AUTH_RAW) -> Path:
    (directory / "authorities").mkdir(parents=True, exist_ok=True)
    (directory / f"{CIK}_v1.json").write_bytes(json.dumps(make_det() if det is None else det).encode())
    (directory / "authorities" / "tiopa_s355.txt").write_bytes(snapshot)
    return directory


@pytest.fixture
def fund_at(funds: None) -> datetime:  # noqa: F811
    """The fund step's ``observed_at``: SPY a candidate, CSPX.L ``cik_unestablished``."""
    report = _run(Web())
    assert report.failures == () and report.observed_at == T
    assert sorted(o[:2] for o in report.observations) == [(SPY, "candidate"), (CSPX, "cik_unestablished")]
    return T


@pytest.fixture
def dets(tmp_path: Path) -> Path:
    return _write(tmp_path / "dets")


def _step(
    directory: Path,
    fund_observed_at: datetime | None,
    sec: Sec | None = None,
    at: datetime = T1,
    job_run_id: int | None = None,
) -> DeterminationReport:
    with _connect() as conn, httpx.Client(transport=httpx.MockTransport((sec or Sec()).handle)) as client:
        return refresh_fund_determinations(
            conn,
            fund_observed_at,
            job_run_id=job_run_id,
            client=client,
            clock=FakeClock(at).clock(),
            directory=directory,
        )


def _outcomes(run_id: int | None = None) -> dict[int, tuple[str, str | None]]:
    rows = _rows(
        "SELECT tax_security_id, outcome, refusal FROM tax_fund_determination_observations WHERE run_id = "
        "COALESCE(%s, (SELECT max(run_id) FROM tax_fund_determination_runs))",
        (run_id,),
    )
    return {sid: (outcome, refusal) for sid, outcome, refusal in rows}


def _fetch_count(source: str) -> int:
    return _rows("SELECT count(*) FROM tax_fund_fetches WHERE source = %s", (source,))[0][0]


def _run_row(run_id: int) -> tuple[Any, ...]:
    return _rows(
        "SELECT status, cutoff, failure, fund_observed_at, job_run_id FROM tax_fund_determination_runs "
        "WHERE run_id = %s",
        (run_id,),
    )[0]


# --- the run ---------------------------------------------------------------------------------------------------------


def test_spy_is_determined_with_one_document_row_per_document(fund_at: datetime, dets: Path) -> None:
    with _connect() as conn:
        before = effective_tax_matching(conn)
    sec = Sec()
    report = _step(dets, fund_at, sec)
    assert (report.loaded, dict(report.stored), report.failures) == (
        (DID,),
        {"sec_governing_document": 1, "sec_submissions": 1},
        (),
    )
    assert sec.log == ["document", "submissions"]
    assert report.run_id is not None
    assert _run_row(report.run_id) == ("succeeded", T1, None, fund_at, None)
    assert _outcomes() == {SPY: ("determined", None), CSPX: ("not_candidate", None)}
    (row,) = _rows(
        "SELECT o.offshore_fund, o.offshore_basis, o.submissions_fetch_id, f.request_key FROM "
        "tax_fund_determination_observations o JOIN tax_fund_fetches f ON f.fetch_id = o.submissions_fetch_id "
        "WHERE o.tax_security_id = %s",
        (SPY,),
    )
    assert (row[0], row[1], row[3]) == ("true", "established", SUBS_URL)
    assert _rows(
        "SELECT d.doc_id, f.request_key FROM tax_fund_determination_documents d "
        "JOIN tax_fund_fetches f USING (fetch_id)"
    ) == [("prospectus", URL)]
    assert _rows("SELECT count(*) FROM tax_fund_determination_authorities WHERE determination_id = %s", (DID,)) == [
        (1,)
    ]
    assert _rows("SELECT determination_id FROM tax_fund_determination_run_loads") == [(DID,)]
    with _connect() as conn:
        assert effective_tax_matching(conn) == before  # matching unchanged


def test_the_job_run_id_is_recorded(fund_at: datetime, dets: Path) -> None:
    with _connect() as conn:
        row = conn.execute(
            "INSERT INTO job_runs (job_name, started_at, status) VALUES ('daily_tax_reconciliation', %s, 'running') "
            "RETURNING run_id",
            (T,),
        ).fetchone()
    assert row is not None
    report = _step(dets, fund_at, job_run_id=row[0])
    assert report.run_id is not None and _run_row(report.run_id)[4] == row[0]


def test_a_stored_document_is_not_fetched_again_and_the_list_not_within_seven_days(
    fund_at: datetime, dets: Path
) -> None:
    _step(dets, fund_at)
    sec = Sec()
    second = _step(dets, fund_at, sec, at=T1 + timedelta(days=7))
    assert (sec.log, dict(second.stored)) == ([], {})
    assert _outcomes(second.run_id)[SPY] == ("determined", None)
    third = _step(dets, fund_at, sec, at=T1 + timedelta(days=7, seconds=1))
    assert (sec.log, dict(third.stored)) == (["submissions"], {"sec_submissions": 1})
    assert _fetch_count("sec_governing_document") == 1


def test_the_documents_and_list_are_stored_for_a_loaded_determination_with_no_candidate(
    funds: None,  # noqa: F811
    dets: Path,
) -> None:
    report = _step(dets, None)
    assert dict(report.stored) == {"sec_governing_document": 1, "sec_submissions": 1}
    assert _outcomes() == {
        SPY: ("fund_observation_unavailable", None),
        CSPX: ("fund_observation_unavailable", None),
    }


def test_no_determination_file_gives_no_determination(fund_at: datetime, tmp_path: Path) -> None:
    report = _step(tmp_path, fund_at)
    assert (report.loaded, dict(report.stored)) == ((), {})
    assert _outcomes() == {SPY: ("no_determination", None), CSPX: ("not_candidate", None)}


def test_a_determination_whose_file_is_gone_is_not_active(fund_at: datetime, dets: Path) -> None:
    _step(dets, fund_at)
    (dets / f"{CIK}_v1.json").unlink()
    report = _step(dets, fund_at)
    assert report.loaded == ()
    assert _outcomes(report.run_id)[SPY] == ("no_determination", None)


# --- failures --------------------------------------------------------------------------------------------------------


def _failed(directory: Path, fund_at: datetime, match: str) -> int:
    with pytest.raises(DeterminationRunFailed, match=match):
        _step(directory, fund_at)
    run_id = _rows("SELECT max(run_id) FROM tax_fund_determination_runs")[0][0]
    status, cutoff, failure, *_ = _run_row(run_id)
    assert (status, cutoff) == ("failed", None) and match in failure
    assert _outcomes(run_id) == {}
    return run_id


def test_a_changed_file_under_a_stored_id_fails_the_run_with_no_observation(fund_at: datetime, dets: Path) -> None:
    _step(dets, fund_at)
    det = make_det()
    det["findings"][0]["reasoning"] = "Read again from the prospectus."
    _write(dets, det)
    _failed(dets, fund_at, "stored with file hash")


def test_an_authority_snapshot_with_a_wrong_hash_fails_the_run(fund_at: datetime, dets: Path) -> None:
    _write(dets, snapshot=AUTH_RAW + b"amended\n")
    _failed(dets, fund_at, "authority tiopa_s355 snapshot refused: hash")


def test_a_malformed_file_fails_the_run(fund_at: datetime, dets: Path) -> None:
    det = make_det()
    det["version"] = 2
    _write(dets, det)
    _failed(dets, fund_at, "format errors")
    assert _rows("SELECT count(*) FROM tax_fund_determinations") == [(0,)]


@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(503),
        httpx.Response(206, content=BODY),
        httpx.Response(301, headers={"location": URL + "x"}),
        httpx.Response(200, content=b"\xff" + BODY),
        httpx.Response(200, content=b"x" * (tfdr.FETCH_LIMIT + 1)),
    ],
)
def test_a_failed_fetch_stores_nothing(fund_at: datetime, dets: Path, response: httpx.Response) -> None:
    report = _step(dets, fund_at, Sec(document=response))
    assert len(report.failures) == 1 and report.failures[0].startswith(f"sec_governing_document {URL}: ")
    assert _fetch_count("sec_governing_document") == 0
    assert report.run_id is not None and _run_row(report.run_id)[0] == "succeeded"
    assert _outcomes()[SPY] == ("documents_unavailable", "prospectus")


def test_a_failure_after_the_run_started_fails_it(
    fund_at: datetime, dets: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def boom(_conn: object) -> None:
        raise OSError("population unreadable")

    monkeypatch.setattr(tfdr, "_read_funds", boom)
    _failed(dets, fund_at, "OSError: population unreadable")


def test_a_concurrent_invocation_fails_at_once(fund_at: datetime, dets: Path) -> None:
    with _connect() as holder:
        holder.execute("SELECT pg_advisory_lock(%s)", (tfdr.FUND_DETERMINATION_LOCK_KEY,))
        report = _step(dets, fund_at)
    assert (report.run_id, report.failures) == (None, ("concurrent refresh",))
    assert _rows("SELECT count(*) FROM tax_fund_determination_runs") == [(0,)]


# --- outcomes --------------------------------------------------------------------------------------------------------


def _quote_moved() -> dict[str, Any]:
    det = make_det()
    det["findings"][0]["contrary"] = [{"source_id": "prospectus", "locator": "X", "text": "Units are never redeemed."}]
    return det


def _isin_differs() -> dict[str, Any]:
    det = make_det()
    det["binding"]["isin"] = "US78462F1048"
    return det


def _ticker_case() -> dict[str, Any]:
    det = make_det()
    det["binding"]["ticker"] = "spy"
    return det


_OUTCOMES: dict[str, tuple[dict[str, Any], tuple[str, str | None]]] = {
    "binding_mismatch": ({"det": _isin_differs()}, ("binding_mismatch", "isin")),
    "ticker_case": ({"det": _ticker_case()}, ("determined", None)),
    "document_changed": (
        {"sec": Sec(document=httpx.Response(200, content=BODY.replace(b"NAV", b"net asset value")))},
        ("document_changed", "prospectus"),
    ),
    "quote_unverified": ({"det": _quote_moved()}, ("quote_unverified", "arrangement:prospectus")),
    "review_out_of_date": ({"at": datetime(2027, 10, 15, 0, 1, tzinfo=UTC)}, ("review_out_of_date", None)),
    "coverage_unavailable": (
        {"sec": Sec(submissions=httpx.Response(200, content=subs_body([ANCHOR], files=[{"name": "x.json"}])))},
        ("coverage_unavailable", "submissions overflow files"),
    ),
    "coverage_other_cik": (
        {"sec": Sec(submissions=httpx.Response(200, content=subs_body([ANCHOR], cik="0000000001")))},
        ("coverage_unavailable", "submissions unsound"),
    ),
    "stale": (
        {"sec": Sec(submissions=httpx.Response(200, content=subs_body([ANCHOR, NEW_497])))},
        ("stale", NEW_497[2]),
    ),
}


@pytest.mark.parametrize("case", list(_OUTCOMES))
def test_each_outcome_is_recorded_with_its_refusal(fund_at: datetime, tmp_path: Path, case: str) -> None:
    given, expected = _OUTCOMES[case]
    directory = _write(tmp_path / "dets", given.get("det"))
    _step(directory, fund_at, given.get("sec"), at=given.get("at", T1))
    outcome, refusal = _outcomes()[SPY]
    if case == "review_out_of_date":
        assert outcome == "review_out_of_date" and refusal is not None and refusal.startswith("cutoff 2027-10-15")
    else:
        assert (outcome, refusal) == expected
    assert _outcomes()[CSPX] == ("not_candidate", None)


def test_a_failed_fund_step_gives_fund_observation_unavailable(fund_at: datetime, dets: Path) -> None:
    report = _step(dets, None)
    assert report.run_id is not None and _run_row(report.run_id)[3] is None
    assert _outcomes() == {
        SPY: ("fund_observation_unavailable", None),
        CSPX: ("fund_observation_unavailable", None),
    }


def test_a_changed_document_is_refetched_once_per_run_and_a_matching_one_restores_it(
    fund_at: datetime, dets: Path
) -> None:
    changed = Sec(document=httpx.Response(200, content=BODY + b"<p>Supplement.</p>"))
    _step(dets, fund_at, changed)
    _step(dets, fund_at, changed)
    assert changed.log == ["document", "submissions", "document"]
    assert _outcomes()[SPY] == ("document_changed", "prospectus")
    sec = Sec()
    report = _step(dets, fund_at, sec)
    assert sec.log == ["document"]
    assert _outcomes(report.run_id)[SPY] == ("determined", None)
    # The matching fetch is used although a mismatching one is older.
    assert _rows(
        "SELECT f.fetch_id = (SELECT max(fetch_id) FROM tax_fund_fetches WHERE source = 'sec_governing_document') "
        "FROM tax_fund_determination_documents d JOIN tax_fund_fetches f USING (fetch_id)"
    ) == [(True,)]


# --- discoveries latch across runs ----------------------------------------------------------------------------------


def test_a_discovery_while_documents_are_unavailable_latches_staleness(fund_at: datetime, dets: Path) -> None:
    first = _step(
        dets,
        fund_at,
        Sec(document=httpx.Response(503), submissions=httpx.Response(200, content=subs_body([ANCHOR, NEW_497]))),
    )
    assert first.discovered == 1
    assert _outcomes(first.run_id)[SPY] == ("documents_unavailable", "prospectus")
    second = _step(dets, fund_at, Sec(), at=T1 + timedelta(days=8))
    assert (second.discovered, dict(second.stored)) == (0, {"sec_governing_document": 1, "sec_submissions": 1})
    assert _outcomes(second.run_id)[SPY] == ("stale", NEW_497[2])


def test_a_discovery_survives_its_runs_failure(fund_at: datetime, dets: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(_conn: object) -> None:
        raise OSError("population unreadable")

    with monkeypatch.context() as m:
        m.setattr(tfdr, "_read_funds", boom)
        with pytest.raises(DeterminationRunFailed):
            _step(dets, fund_at, Sec(submissions=httpx.Response(200, content=subs_body([ANCHOR, NEW_497]))))
    assert _rows("SELECT accession, first_run_id IS NOT NULL FROM tax_fund_determination_unreviewed") == [
        (NEW_497[2], True)
    ]
    report = _step(dets, fund_at, Sec(), at=T1 + timedelta(days=8))
    assert _outcomes(report.run_id)[SPY] == ("stale", NEW_497[2])


def test_a_future_dated_filing_is_not_recorded_and_refuses_coverage(fund_at: datetime, dets: Path) -> None:
    future = ("497", T1.date().isoformat(), "0001193125-26-088888")
    # The list is fetched the day before the run, so the 497 dated on the cutoff's day is after its fetch date.
    with _connect() as conn:
        _store(conn, "sec_submissions", SUBS_URL, SUBS_URL, T1 - timedelta(days=1), subs_body([ANCHOR, future]))
    report = _step(dets, fund_at)
    assert report.discovered == 0
    assert _outcomes(report.run_id)[SPY] == ("coverage_unavailable", "submissions unsound")
    corrected = _step(dets, fund_at, Sec(), at=T1 + timedelta(days=8))
    assert _outcomes(corrected.run_id)[SPY] == ("determined", None)
