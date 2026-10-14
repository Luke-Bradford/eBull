"""Fund evidence storage against the test database (#3616 slice 3d-3c-1; sql/492, spec Design 3).

Auto-marked ``db`` (pulls ``ebull_test_conn``, which wipes between tests). Rows are inserted directly on autocommit
connections, as ``refresh_fund_evidence`` will write them; these are the schema's own refusals (spec tests 5 and 6,
"Refused"). The capture and observation behaviour is part B2's.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any, get_args

import psycopg
import pytest
from psycopg import sql
from psycopg.types.json import Jsonb

from app.services.tax_fund_evidence import FUND_IDENTITY_POLICY_ID, IdentityOutcome, figi_request_key, valid_isin
from tests.fixtures.ebull_test_db import test_database_url

T0 = datetime(2026, 10, 14, 15, 0, tzinfo=UTC)
ACCESSION = "0001410368-26-023904"
LEI = "549300NZAMSJ8FXPQQ63"
ISIN = "US78462F1030"
NAME_KEYS = ["SPDR1", "SPDRSP500"]
ASSET = "https://assets.publishing.service.gov.uk/media/abc/reporting-funds.ods"
LIST_BODY = b"PK\x03\x04" + b"\0" * 60
POLICY = FUND_IDENTITY_POLICY_ID


def _connect() -> psycopg.Connection[tuple]:
    return psycopg.connect(test_database_url(), autocommit=True)


def _one(query: str, params: tuple[Any, ...] = ()) -> Any:
    with _connect() as conn:
        row = conn.execute(query, params).fetchone()  # type: ignore[arg-type]
    assert row is not None
    return row[0]


def _content(
    *, url: str = ASSET, size: int = len(LIST_BODY), published: str = "2026-10-12T11:30:44+01:00", root: Any = None
) -> bytes:
    item = (
        root
        if root is not None
        else {
            "public_updated_at": published,
            "details": {"attachments": [{"url": url, "file_size": size}, {"url": "https://www.gov.uk/x.pdf"}]},
        }
    )
    return json.dumps(item).encode()


def _fetch(
    source: str,
    key: str,
    body: bytes,
    *,
    at: datetime = T0 - timedelta(minutes=5),
    parent: int | None = None,
    url: str | None = None,
) -> int:
    sha = hashlib.sha256(body).hexdigest()
    with _connect() as conn:
        conn.execute(
            "INSERT INTO tax_fund_documents (body_sha256, body) VALUES (%s, %s) ON CONFLICT DO NOTHING", (sha, body)
        )
        row = conn.execute(
            "INSERT INTO tax_fund_fetches (source, request_key, url, fetched_at, body_sha256, parent_fetch_id) "
            "VALUES (%s, %s, %s, %s, %s, %s) RETURNING fetch_id",
            (source, key, url or key, at, sha, parent),
        ).fetchone()
    assert row is not None
    return int(row[0])


def _version(list_fetch_id: int, *, rows: int | None = 125_022, refusal: str | None = None) -> None:
    with _connect() as conn:
        conn.execute(
            "INSERT INTO tax_fund_list_versions (list_fetch_id, policy_id, usable, data_rows, refusal, assessed_at) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (list_fetch_id, POLICY, refusal is None, rows, refusal, T0),
        )


def _example(method: str, key: str, cells: int = 9) -> dict[str, Any]:
    return {"method": method, "key": key, "cells": ["x"] * cells}


def _search(list_fetch_id: int, **over: Any) -> None:
    row: dict[str, Any] = {
        "policy_id": POLICY,
        "isin": ISIN,
        "name_keys": NAME_KEYS,
        "list_fetch_id": list_fetch_id,
        "searched_at": T0,
        "identifier_count": 0,
        "identifier_in_text_count": 0,
        "near_identifier_count": 0,
        "name_count": 0,
        "examples": [],
    } | over
    row["examples"] = Jsonb(row["examples"])
    _insert("tax_fund_list_searches", row)


def _insert(table: str, row: dict[str, Any]) -> None:
    query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
        sql.Identifier(table),
        sql.SQL(", ").join(sql.Identifier(c) for c in row),
        sql.SQL(", ").join(sql.Placeholder() for _ in row),
    )
    with _connect() as conn:
        conn.execute(query, tuple(row.values()))


@pytest.fixture
def spy(ebull_test_conn: psycopg.Connection[Any]) -> Iterator[dict[str, Any]]:
    """SPY's stored chain, one usable searched list version, and a sound `candidate`/`not_found` observation's
    columns (not inserted)."""
    run_id = _one(
        "INSERT INTO tax_evidence_runs (environment, account_id, started_at, state, evidence_policy_id) "
        "VALUES ('demo', 4242, now(), 'in_progress', 'tax_evidence_v3') RETURNING run_id"
    )
    content = _fetch("hmrc_content_item", "/government/publications/approved-offshore-reporting-funds", _content())
    listed = _fetch("hmrc_reporting_list", ASSET, LIST_BODY, parent=content)
    _version(listed)
    _search(listed)
    yield {
        "observed_at": T0,
        "policy_id": POLICY,
        "evidence_run_id": run_id,
        "tax_security_id": 3000,
        "member_ids": [3000],
        "symbol": "SPY",
        "outcome": "candidate",
        "cik": "0000884394",
        "newest_period": datetime(2025, 12, 31).date(),
        "amendment_accessions": [],
        "ncen_accession": ACCESSION,
        "ncen_report_date": datetime(2025, 12, 31).date(),
        "ncen_source_url": "https://www.sec.gov/Archives/edgar/data/884394/000141036826023904/primary_doc.xml",
        "ncen_fetch_id": _fetch("sec_ncen", ACCESSION, b"<edgarSubmission/>"),
        "lei": LEI,
        "record_fetch_id": _fetch("gleif_record", LEI, b'{"record": 1}'),
        "isins_fetch_id": _fetch("gleif_isins", LEI, b'{"isins": 1}'),
        "figi_fetch_id": _fetch("openfigi_mapping", figi_request_key([ISIN], "ARCX"), b"[]"),
        "listings": Jsonb([["ARCX", "SPY"]]),
        "mic": "ARCX",
        "isin_set": [ISIN],
        "isin": ISIN,
        "name_keys": NAME_KEYS,
        "search_outcome": "not_found",
        "content_fetch_id": content,
        "list_fetch_id": listed,
        "evaluated_at": T0 + timedelta(seconds=1),
        "coverage": Jsonb([{"list_fetch_id": listed, "state": "searched"}]),
    }


def _observe(row: dict[str, Any], **over: Any) -> None:
    _insert("tax_fund_observations", row | over)


def test_a_sound_candidate_and_a_cik_unestablished_observation_insert(spy: dict[str, Any]) -> None:
    _observe(spy)
    _observe(
        {k: spy[k] for k in ("observed_at", "policy_id", "evidence_run_id", "evaluated_at")},
        tax_security_id=3434,
        member_ids=[3434],
        symbol="CSPX.L",
        outcome="cik_unestablished",
    )
    assert _one("SELECT count(*) FROM tax_fund_observations") == 2


def test_the_outcome_check_is_the_identity_table(ebull_test_conn: psycopg.Connection[Any]) -> None:
    """The DB's outcome codes are exactly ``IdentityOutcome`` (one source of truth, checked)."""
    definition = _one(
        "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
        "WHERE conrelid = 'tax_fund_observations'::regclass AND conname = 'tax_fund_observations_outcome_check'"
    )
    assert sorted(re.findall(r"'([a-z_]+)'::text", definition)) == sorted(get_args(IdentityOutcome))


@pytest.mark.parametrize(
    "isin",
    [
        "US78462F1030",
        "US78462N1030",
        "US78462F1048",
        "IE00B5BMR087",
        "IE0031442068",
        "KYG4330C1097",
        "US78462F1031",
        "us78462F1030",
        "US78462F103",
    ],
)
def test_the_isin_check_agrees_with_the_reader(ebull_test_conn: psycopg.Connection[Any], isin: str) -> None:
    assert _one("SELECT tax_fund_valid_isin(%s)", (isin,)) is valid_isin(isin)


# --- test 5: fetches --------------------------------------------------------------------------------------------------


def test_a_second_ncen_fetch_of_an_accession_is_refused(spy: dict[str, Any]) -> None:
    with pytest.raises(psycopg.errors.UniqueViolation):
        _fetch("sec_ncen", ACCESSION, b"<edgarSubmission/> ")


def test_an_unchanged_mutable_refetch_adds_a_fetch_and_no_document(spy: dict[str, Any]) -> None:
    documents = _one("SELECT count(*) FROM tax_fund_documents")
    _fetch("gleif_record", LEI, b'{"record": 1}', at=T0 - timedelta(minutes=1))
    assert _one("SELECT count(*) FROM tax_fund_documents") == documents
    assert _one("SELECT count(*) FROM tax_fund_fetches WHERE source = 'gleif_record'") == 2


def test_a_list_needs_a_content_item_parent_with_its_attachment_url_and_size(spy: dict[str, Any]) -> None:
    with pytest.raises(psycopg.errors.RaiseException, match="not a content item"):
        _fetch("hmrc_reporting_list", ASSET, LIST_BODY, parent=spy["record_fetch_id"])
    content = _fetch("hmrc_content_item", "/government/publications/approved-offshore-reporting-funds", _content())
    with pytest.raises(psycopg.errors.RaiseException, match="attachment URL"):
        _fetch("hmrc_reporting_list", ASSET.replace("abc", "abd"), LIST_BODY, parent=content)
    with pytest.raises(psycopg.errors.RaiseException, match="attachment URL"):
        _fetch("hmrc_reporting_list", ASSET, LIST_BODY, parent=content, url=ASSET + "x")
    with pytest.raises(psycopg.errors.RaiseException, match="file_size"):
        _fetch("hmrc_reporting_list", ASSET, LIST_BODY + b"\0", parent=content)
    with pytest.raises(psycopg.errors.CheckViolation):
        _fetch("gleif_record", "X" + LEI, b"{}", parent=content)


def test_two_lists_with_one_parent_are_refused(spy: dict[str, Any]) -> None:
    with pytest.raises(psycopg.errors.UniqueViolation):
        _fetch("hmrc_reporting_list", ASSET, LIST_BODY, parent=spy["content_fetch_id"], at=T0)


@pytest.mark.parametrize(
    "body",
    [
        _content(url=ASSET.replace("assets.publishing.service.gov.uk", "assets.example.com")),
        _content(url=ASSET.replace("https://", "http://")),
        _content(url=ASSET.replace(".gov.uk/", ".gov.uk:443/")),
        _content(url=ASSET + "?v=2"),
        _content(size=0),
        _content(size=10**20),
        _content(root=[{"public_updated_at": "2026-10-12T11:30:44+01:00"}]),
        _content(published="2026-99-14T11:30:44+01:00"),
        _content(published="2026-10-12T11:30:44+00:60"),
        _content(published="2026-10-12T11:30:44+01:99"),
        _content(published="2026-10-12T11:30:44"),
        _content(published="2026-10-14T16:00:00+01:00"),  # after the fetch time
        b"not json",
    ],
)
def test_an_unsound_content_item_is_refused(ebull_test_conn: psycopg.Connection[Any], body: bytes) -> None:
    with pytest.raises(psycopg.errors.RaiseException, match="is not sound"):
        _fetch("hmrc_content_item", "/government/publications/approved-offshore-reporting-funds", body)


def test_two_ods_attachments_are_refused(ebull_test_conn: psycopg.Connection[Any]) -> None:
    item = {
        "public_updated_at": "2026-10-12T11:30:44Z",
        "details": {"attachments": [{"url": ASSET, "file_size": 64}, {"url": ASSET[:-4] + "2.ods", "file_size": 64}]},
    }
    with pytest.raises(psycopg.errors.RaiseException, match="is not sound"):
        _fetch(
            "hmrc_content_item", "/government/publications/approved-offshore-reporting-funds", json.dumps(item).encode()
        )


@pytest.mark.parametrize(
    "table",
    [
        "tax_fund_documents",
        "tax_fund_fetches",
        "tax_fund_list_versions",
        "tax_fund_list_searches",
        "tax_fund_observations",
    ],
)
def test_every_table_refuses_update_and_delete(spy: dict[str, Any], table: str) -> None:
    _observe(spy)
    with _connect() as conn:
        for statement in ("UPDATE {} SET policy_id = policy_id", "DELETE FROM {}"):
            if table in ("tax_fund_documents", "tax_fund_fetches") and "UPDATE" in statement:
                statement = "UPDATE {} SET body_sha256 = body_sha256"
            with pytest.raises(psycopg.errors.RaiseException, match="never updated or deleted"):
                conn.execute(sql.SQL(statement).format(sql.Identifier(table)))


# --- test 6: assessments and searches ---------------------------------------------------------------------------------


def _new_list() -> int:
    content = _fetch(
        "hmrc_content_item", "/government/publications/approved-offshore-reporting-funds", _content(), at=T0
    )
    return _fetch("hmrc_reporting_list", ASSET, LIST_BODY, parent=content, at=T0)


def test_an_assessment_is_of_a_list_and_a_usable_one_has_rows(spy: dict[str, Any]) -> None:
    with pytest.raises(psycopg.errors.RaiseException, match="not a reporting list"):
        _version(spy["record_fetch_id"])
    listed = _new_list()
    with pytest.raises(psycopg.errors.CheckViolation):
        _version(listed, rows=0)
    with pytest.raises(psycopg.errors.CheckViolation):
        _version(listed, rows=10, refusal="size floor")
    with pytest.raises(psycopg.errors.CheckViolation):
        _version(listed, rows=None, refusal=None)


@pytest.mark.parametrize("keys", [["SPDR1", "SPDR1"], ["", "SPDR1"], ["SPDR1", None], [], ["SPDRSP500", "SPDR1"]])
def test_a_name_key_set_is_canonical(spy: dict[str, Any], keys: list[str | None]) -> None:
    with pytest.raises(psycopg.errors.CheckViolation):
        _search(spy["list_fetch_id"], name_keys=keys)
    # The coverage trigger (BEFORE) or the CHECK refuses it, whichever runs first.
    with pytest.raises((psycopg.errors.CheckViolation, psycopg.errors.RaiseException)):
        _observe(spy, name_keys=keys)


def test_a_search_is_of_a_usable_version_with_bounded_counts_and_exact_examples(spy: dict[str, Any]) -> None:
    unusable = _new_list()
    _version(unusable, rows=None, refusal="size floor")
    with pytest.raises(psycopg.errors.RaiseException, match="not usable"):
        _search(unusable)
    listed = _new_list()
    _version(listed, rows=30)
    with pytest.raises(psycopg.errors.RaiseException, match="sum past"):
        _search(listed, name_count=20, near_identifier_count=11)
    with pytest.raises(psycopg.errors.RaiseException, match="nine cells"):
        _search(listed, name_count=1, examples=[_example("name", "SPDR1", cells=8)])
    with pytest.raises(psycopg.errors.RaiseException, match="nine cells"):
        _search(listed, name_count=1, examples=["SPDR1"])
    with pytest.raises(psycopg.errors.RaiseException, match="nine cells"):
        _search(listed, examples=[{}])
    with pytest.raises(psycopg.errors.RaiseException, match="least"):
        _search(listed, name_count=27, examples=[_example("name", "SPDR1")] * 24)
    with pytest.raises(psycopg.errors.RaiseException, match="least"):
        _search(listed, name_count=1, near_identifier_count=1, examples=[_example("name", "SPDR1")])
    with pytest.raises(psycopg.errors.RaiseException, match="key"):
        _search(listed, name_count=1, examples=[_example("name", "SPDRX")])
    with pytest.raises(psycopg.errors.RaiseException, match="key"):
        _search(listed, identifier_count=1, examples=[_example("identifier", "SPDR1")])
    with pytest.raises(psycopg.errors.CheckViolation):
        _search(listed, isin="US78462F1031")
    _search(
        listed,
        identifier_count=1,
        near_identifier_count=1,
        name_count=27,
        examples=[_example("identifier", ISIN), _example("near_identifier", "78462F103")]
        + [_example("name", "SPDRSP500")] * 25,
    )


# --- test 6: observations ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "column",
    [
        "cik",
        "ncen_accession",
        "ncen_report_date",
        "ncen_fetch_id",
        "lei",
        "record_fetch_id",
        "isins_fetch_id",
        "figi_fetch_id",
        "listings",
        "mic",
        "isin_set",
        "isin",
        "name_keys",
        "search_outcome",
        "coverage",
    ],
)
def test_a_candidate_missing_a_required_column_is_refused(spy: dict[str, Any], column: str) -> None:
    with pytest.raises((psycopg.errors.CheckViolation, psycopg.errors.RaiseException)):
        _observe(spy, **{column: None})


@pytest.mark.parametrize(
    "over",
    [{"search_outcome": "not_found"}, {"isin": ISIN}, {"content_fetch_id": "content"}, {"coverage": Jsonb([])}],
)
def test_a_non_candidate_has_no_search_columns(spy: dict[str, Any], over: dict[str, Any]) -> None:
    if over.get("content_fetch_id") == "content":
        over = {"content_fetch_id": spy["content_fetch_id"], "list_fetch_id": spy["list_fetch_id"]}
    with pytest.raises(psycopg.errors.CheckViolation):
        _observe(
            {k: spy[k] for k in ("observed_at", "policy_id", "evidence_run_id", "evaluated_at")},
            tax_security_id=3434,
            member_ids=[3434],
            outcome="cik_unestablished",
            **over,
        )


def test_the_bound_isin_is_in_the_set(spy: dict[str, Any]) -> None:
    with pytest.raises((psycopg.errors.CheckViolation, psycopg.errors.RaiseException)):
        _observe(spy, isin="IE00B5BMR087")


def test_fetch_ids_belong_to_the_observation(spy: dict[str, Any]) -> None:
    other = _fetch("gleif_record", "5493001KJTIIGC8Y1R12", b'{"other": 1}')
    with pytest.raises(psycopg.errors.RaiseException, match="another request key"):
        _observe(spy, record_fetch_id=other)
    with pytest.raises(psycopg.errors.RaiseException, match="is not a gleif_isins fetch"):
        _observe(spy, isins_fetch_id=spy["record_fetch_id"])
    with pytest.raises(psycopg.errors.RaiseException, match="other jobs"):
        _observe(spy, figi_fetch_id=_fetch("openfigi_mapping", figi_request_key([ISIN], "BATS"), b"[]"))
    with pytest.raises(psycopg.errors.RaiseException, match="other jobs"):
        _observe(spy, mic="BATS")
    with pytest.raises(psycopg.errors.RaiseException, match="after observed_at"):
        _observe(spy, record_fetch_id=_fetch("gleif_record", LEI, b'{"late": 1}', at=T0 + timedelta(seconds=1)))


def test_a_figi_request_key_is_compared_in_job_order(spy: dict[str, Any]) -> None:
    two = ["IE00B5BMR087", ISIN]
    reversed_jobs = _fetch("openfigi_mapping", figi_request_key(list(reversed(two)), "ARCX"), b"[1]")
    with pytest.raises(psycopg.errors.RaiseException, match="other jobs"):
        _observe(spy, isin_set=two, figi_fetch_id=reversed_jobs)
    in_order = _fetch("openfigi_mapping", figi_request_key(two, "ARCX"), b"[2]")
    _observe(spy, isin_set=two, figi_fetch_id=in_order)


def test_not_found_needs_a_usable_newest_list(spy: dict[str, Any]) -> None:
    with pytest.raises((psycopg.errors.CheckViolation, psycopg.errors.RaiseException)):
        _observe(spy, list_fetch_id=None)
    listed = _new_list()
    _version(listed, rows=None, refusal="size floor")
    content = _one("SELECT parent_fetch_id FROM tax_fund_fetches WHERE fetch_id = %s", (listed,))
    coverage = Jsonb(
        [{"list_fetch_id": spy["list_fetch_id"], "state": "searched"}, {"list_fetch_id": listed, "state": "unusable"}]
    )
    at = T0 + timedelta(seconds=1)
    with pytest.raises(psycopg.errors.RaiseException, match="usable list"):
        _observe(
            spy, observed_at=at, evaluated_at=at, content_fetch_id=content, list_fetch_id=listed, coverage=coverage
        )
    _observe(
        spy,
        observed_at=at,
        evaluated_at=at,
        content_fetch_id=content,
        list_fetch_id=listed,
        coverage=coverage,
        search_outcome="unavailable",
    )


def test_the_fetch_pair_is_the_newest_content_fetch_and_its_child(spy: dict[str, Any]) -> None:
    with pytest.raises(psycopg.errors.RaiseException, match="newest content fetch"):
        _observe(spy, content_fetch_id=None, list_fetch_id=None, search_outcome="unavailable")
    with pytest.raises(psycopg.errors.RaiseException, match="child"):
        _observe(spy, list_fetch_id=None, search_outcome="unavailable")


def test_coverage_is_the_stored_state(spy: dict[str, Any]) -> None:
    listed = spy["list_fetch_id"]
    for coverage in (
        [{"list_fetch_id": listed, "state": "searched"}, {"list_fetch_id": listed + 99, "state": "pending"}],
        [{"list_fetch_id": listed, "state": "usable_unsearched"}],
        [],
    ):
        with pytest.raises(psycopg.errors.RaiseException, match="not the stored state"):
            _observe(spy, coverage=Jsonb(coverage))
    # A pending later version, then its assessment without a search: the states an `unavailable` keeps.
    pending = _new_list()
    content = _one("SELECT parent_fetch_id FROM tax_fund_fetches WHERE fetch_id = %s", (pending,))
    at = T0 + timedelta(seconds=1)
    pair = {
        "observed_at": at,
        "evaluated_at": at,
        "content_fetch_id": content,
        "list_fetch_id": pending,
        "search_outcome": "unavailable",
    }
    _observe(
        spy,
        **pair,
        coverage=Jsonb(
            [{"list_fetch_id": listed, "state": "searched"}, {"list_fetch_id": pending, "state": "pending"}]
        ),
    )
    _version(pending)
    at = T0 + timedelta(seconds=2)
    pair |= {"observed_at": at, "evaluated_at": at}
    with pytest.raises(psycopg.errors.RaiseException, match="not the stored state"):
        _observe(
            spy,
            **pair,
            coverage=Jsonb(
                [{"list_fetch_id": listed, "state": "searched"}, {"list_fetch_id": pending, "state": "searched"}]
            ),
        )
    _observe(
        spy,
        **pair,
        coverage=Jsonb(
            [{"list_fetch_id": listed, "state": "searched"}, {"list_fetch_id": pending, "state": "usable_unsearched"}]
        ),
    )


def test_two_observations_of_one_security_at_one_time_are_refused(spy: dict[str, Any]) -> None:
    _observe(spy)
    with pytest.raises(psycopg.errors.UniqueViolation):
        _observe(spy, evaluated_at=T0 + timedelta(seconds=5))


def test_evaluation_is_not_before_the_cutoff(spy: dict[str, Any]) -> None:
    with pytest.raises(psycopg.errors.CheckViolation):
        _observe(spy, evaluated_at=T0 - timedelta(microseconds=1))
