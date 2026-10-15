"""Fund determination storage against the test database (#3616 slice 3d-3c-2; sql/493, spec Design 4).

Auto-marked ``db`` (pulls ``ebull_test_conn``, which wipes between tests). Rows are inserted directly, as
``refresh_fund_determinations`` will write them, with SPY's committed determination; these are the schema's own
refusals (spec test 6: insert-only, transition, deletion and sealing triggers, the determination and authority
CHECKs, discoveries, the fund-observation triggers and document rows). The step's behaviour is part B2's.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import psycopg
import pytest
from psycopg import sql
from psycopg.types.json import Jsonb

from app.services.tax_fund_determination import (
    ACCEPTED_IDENTITY_POLICY_ID,
    FUND_DETERMINATION_POLICY_ID,
    OUTCOMES,
    SUBMISSIONS_URL,
    UNDETERMINED_OUTCOMES,
)
from app.services.tax_fund_evidence import FUND_IDENTITY_POLICY_ID
from tests.fixtures.ebull_test_db import test_database_url

SPY_FILE = Path(__file__).resolve().parents[1] / "app/services/tax_fund_determinations/0000884394_v1.json"
SPY = json.loads(SPY_FILE.read_bytes())
CIK = SPY["cik"]
DID = SPY["determination_id"]
T0 = datetime(2026, 10, 15, 9, 0, tzinfo=UTC)
SECURITY = 3000
#: The columns of an observation whose fund step left nothing to read.
UNAVAILABLE = {
    "outcome": "fund_observation_unavailable",
    "fund_observation_id": None,
    "determination_id": None,
    "refusal": None,
    "submissions_fetch_id": None,
}


def _connect() -> psycopg.Connection[tuple]:
    return psycopg.connect(test_database_url(), autocommit=True)


def _one(query: str, params: tuple[Any, ...] = ()) -> Any:
    with _connect() as conn:
        row = conn.execute(query, params).fetchone()  # type: ignore[arg-type]
    assert row is not None
    return row[0]


def _insert(table: str, row: dict[str, Any], conn: psycopg.Connection[tuple] | None = None) -> Any:
    query = sql.SQL("INSERT INTO {} ({}) VALUES ({}) RETURNING *").format(
        sql.Identifier(table),
        sql.SQL(", ").join(sql.Identifier(c) for c in row),
        sql.SQL(", ").join(sql.Placeholder() for _ in row),
    )
    if conn is not None:
        out = conn.execute(query, tuple(row.values())).fetchone()
    else:
        with _connect() as own:
            out = own.execute(query, tuple(row.values())).fetchone()
    assert out is not None
    return out[0]


def _fetch(source: str, key: str, body: bytes, *, at: datetime = T0 - timedelta(minutes=5)) -> int:
    sha = hashlib.sha256(body).hexdigest()
    with _connect() as conn:
        conn.execute(
            "INSERT INTO tax_fund_documents (body_sha256, body) VALUES (%s, %s) ON CONFLICT DO NOTHING", (sha, body)
        )
    return int(
        _insert(
            "tax_fund_fetches",
            {"source": source, "request_key": key, "url": key, "fetched_at": at, "body_sha256": sha},
        )
    )


def _determination(**over: Any) -> None:
    row = {
        "determination_id": DID,
        "policy_id": FUND_DETERMINATION_POLICY_ID,
        "cik": CIK,
        "version": 1,
        "file_sha256": hashlib.sha256(SPY_FILE.read_bytes()).hexdigest(),
        "body": SPY,
        "loaded_at": T0,
    } | over
    row["body"] = Jsonb(row["body"])
    _insert("tax_fund_determinations", row)


def _run(*, fund_observed_at: datetime | None = T0) -> int:
    job = _insert("job_runs", {"job_name": "tax_broker_evidence_refresh", "started_at": T0, "status": "running"})
    return int(
        _insert(
            "tax_fund_determination_runs",
            {"job_run_id": job, "fund_observed_at": fund_observed_at, "started_at": T0, "status": "running"},
        )
    )


def _finish(run_id: int, status: str = "succeeded") -> None:
    with _connect() as conn:
        conn.execute(
            "UPDATE tax_fund_determination_runs SET status = %s, cutoff = %s, failure = %s WHERE run_id = %s",
            (
                status,
                T0 + timedelta(minutes=1) if status == "succeeded" else None,
                "a stored id has another hash" if status == "failed" else None,
                run_id,
            ),
        )


def _load(run_id: int) -> None:
    _insert("tax_fund_determination_run_loads", {"run_id": run_id, "determination_id": DID})


def _fund_observation(
    *, security: int = SECURITY, observed_at: datetime = T0, policy: str = FUND_IDENTITY_POLICY_ID
) -> int:
    # One evidence run may be in progress at a time; every fund observation here hangs off it.
    evidence_run = _one("SELECT min(run_id) FROM tax_evidence_runs") or _insert(
        "tax_evidence_runs",
        {
            "environment": "demo",
            "account_id": 4242,
            "started_at": T0,
            "state": "in_progress",
            "evidence_policy_id": "tax_evidence_v3",
        },
    )
    return int(
        _insert(
            "tax_fund_observations",
            {
                "observed_at": observed_at,
                "policy_id": policy,
                "evidence_run_id": evidence_run,
                "tax_security_id": security,
                "member_ids": [security],
                "outcome": "cik_unestablished",
                "evaluated_at": observed_at,
            },
        )
    )


def _submissions(*, cik: str = CIK, at: datetime = T0 - timedelta(minutes=5), source: str = "sec_submissions") -> int:
    return _fetch(source, SUBMISSIONS_URL.format(cik=cik), json.dumps({"cik": cik, "at": str(at)}).encode(), at=at)


def _document_fetches() -> dict[str, int]:
    return {d["doc_id"]: _fetch("sec_governing_document", d["url"], d["doc_id"].encode()) for d in SPY["documents"]}


@pytest.fixture
def spy(ebull_test_conn: psycopg.Connection[Any]) -> Iterator[dict[str, Any]]:
    """SPY's determination, a running run that loaded it, a fund observation at its `fund_observed_at`, a filing list
    and every document's fetch; and a sound `stale` observation's columns (not inserted)."""
    _determination()
    run_id = _run()
    _load(run_id)
    submissions = _submissions()
    yield {
        "run_id": run_id,
        "submissions": submissions,
        "documents": _document_fetches(),
        "row": {
            "run_id": run_id,
            "policy_id": FUND_DETERMINATION_POLICY_ID,
            "tax_security_id": SECURITY,
            "fund_observation_id": _fund_observation(),
            "determination_id": DID,
            "outcome": "stale",
            "refusal": "0001193125-26-100000",
            "submissions_fetch_id": submissions,
        },
    }


def _observe(row: dict[str, Any], conn: psycopg.Connection[tuple] | None = None, **over: Any) -> int:
    return int(_insert("tax_fund_determination_observations", row | over, conn))


def _determined(spy: dict[str, Any], *, skip: int = 0) -> int:
    """A `determined` observation and its document rows, in one transaction (all but `skip` of them)."""
    with psycopg.connect(test_database_url()) as conn:
        observation = _observe(
            spy["row"], conn, outcome="determined", refusal=None, offshore_fund="true", offshore_basis="assumed"
        )
        for doc_id, fetch_id in list(spy["documents"].items())[skip:]:
            _insert(
                "tax_fund_determination_documents",
                {"observation_id": observation, "doc_id": doc_id, "fetch_id": fetch_id},
                conn,
            )
    return observation


def _discover(spy: dict[str, Any], **over: Any) -> None:
    _insert(
        "tax_fund_determination_unreviewed",
        {
            "determination_id": DID,
            "accession": "0001193125-26-100000",
            "form": "497",
            "filed": T0.date(),
            "first_run_id": spy["run_id"],
            "submissions_fetch_id": spy["submissions"],
        }
        | over,
    )


# --- the DB agrees with the pure module -------------------------------------------------------------------------------


def _constraint(table: str, name: str) -> str:
    return _one(
        "SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid = %s::regclass AND conname = %s",
        (table, name),
    )


def test_the_codes_are_the_pure_modules(ebull_test_conn: psycopg.Connection[Any]) -> None:
    """One source of truth, checked: the outcome table, the undetermined four, the policy ids and the list URL."""
    outcomes = _constraint("tax_fund_determination_observations", "tax_fund_determination_observations_outcome_check")
    assert sorted(re.findall(r"'([a-z_]+)'::text", outcomes)) == sorted(OUTCOMES)
    undetermined = _one(
        "SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid = "
        "'tax_fund_determination_observations'::regclass "
        "AND pg_get_constraintdef(oid) LIKE '%%determination_id IS NULL%%'"
    )
    assert set(re.findall(r"'([a-z_]+)'::text", undetermined)) == UNDETERMINED_OUTCOMES
    policy = _constraint("tax_fund_determinations", "tax_fund_determinations_policy_id_check")
    assert re.findall(r"'([a-z_0-9]+)'::text", policy) == [FUND_DETERMINATION_POLICY_ID]
    assert ACCEPTED_IDENTITY_POLICY_ID == FUND_IDENTITY_POLICY_ID
    assert f"'{ACCEPTED_IDENTITY_POLICY_ID}'" in _one(
        "SELECT prosrc FROM pg_proc WHERE proname = 'tax_fund_determination_observations_check'"
    )
    assert _one("SELECT tax_fund_submissions_url(%s)", (CIK,)) == SUBMISSIONS_URL.format(cik=CIK)
    sources = _constraint("tax_fund_fetches", "tax_fund_fetches_source_check")
    assert {"sec_governing_document", "sec_submissions"} <= set(re.findall(r"'([a-z_]+)'::text", sources))


# --- determinations and authorities -----------------------------------------------------------------------------------


def test_spys_determination_loads(ebull_test_conn: psycopg.Connection[Any]) -> None:
    _determination()
    assert _one("SELECT body -> 'binding' ->> 'ticker' FROM tax_fund_determinations") == "SPY"


@pytest.mark.parametrize(
    "over",
    [
        {"determination_id": f"{FUND_DETERMINATION_POLICY_ID}:0000884395:1"},
        {"policy_id": "fund_determination_v2", "determination_id": f"fund_determination_v2:{CIK}:1"},
        {"version": 2, "determination_id": f"{FUND_DETERMINATION_POLICY_ID}:{CIK}:2", "body": SPY | {"version": 2}},
        {"cik": "884394", "determination_id": f"{FUND_DETERMINATION_POLICY_ID}:884394:1"},
        {"body": SPY | {"cik": "0000884395"}},
        {"body": SPY | {"determination_id": f"{FUND_DETERMINATION_POLICY_ID}:{CIK}:01"}},
        {"body": SPY | {"version": "1"}},
        {"body": SPY | {"version": True}},
        {"body": {k: v for k, v in SPY.items() if k != "policy_id"}},
        {"body": [SPY]},
        {"file_sha256": "A" * 64},
    ],
)
def test_a_determination_disagreeing_with_its_id_or_body_is_refused(
    ebull_test_conn: psycopg.Connection[Any], over: dict[str, Any]
) -> None:
    with pytest.raises(psycopg.errors.CheckViolation):
        _determination(**over)


def test_one_version_per_cik(ebull_test_conn: psycopg.Connection[Any]) -> None:
    _determination()
    with pytest.raises(psycopg.errors.UniqueViolation):
        _determination()


def test_an_authority_snapshot_is_its_hash(ebull_test_conn: psycopg.Connection[Any]) -> None:
    body = b"https://www.legislation.gov.uk/ukpga/2010/8/section/355/data.xht\n355 Offshore funds\n"
    with pytest.raises(psycopg.errors.CheckViolation):
        _insert("tax_fund_authority_snapshots", {"content_sha256": "0" * 64, "body": body})
    sha = _insert("tax_fund_authority_snapshots", {"content_sha256": hashlib.sha256(body).hexdigest(), "body": body})
    _determination()
    _insert(
        "tax_fund_determination_authorities",
        {"determination_id": DID, "authority_id": "tiopa_s355", "content_sha256": sha},
    )
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        _insert(
            "tax_fund_determination_authorities",
            {"determination_id": DID, "authority_id": "tiopa_s356", "content_sha256": "1" * 64},
        )


# --- runs -------------------------------------------------------------------------------------------------------------


def test_a_run_moves_once_from_running(ebull_test_conn: psycopg.Connection[Any]) -> None:
    with pytest.raises(psycopg.errors.RaiseException, match="starts running"):
        _insert("tax_fund_determination_runs", {"started_at": T0, "status": "failed", "failure": "x"})
    run_id = _run()
    with _connect() as conn:
        with pytest.raises(psycopg.errors.RaiseException, match="only to succeeded or failed"):
            conn.execute("UPDATE tax_fund_determination_runs SET status = 'running' WHERE run_id = %s", (run_id,))
        with pytest.raises(psycopg.errors.RaiseException, match="changes only"):
            conn.execute(
                "UPDATE tax_fund_determination_runs SET status = 'failed', failure = 'x', started_at = %s "
                "WHERE run_id = %s",
                (T0 - timedelta(seconds=1), run_id),
            )
        with pytest.raises(psycopg.errors.CheckViolation):
            conn.execute("UPDATE tax_fund_determination_runs SET status = 'succeeded' WHERE run_id = %s", (run_id,))
        with pytest.raises(psycopg.errors.CheckViolation):
            conn.execute("UPDATE tax_fund_determination_runs SET status = 'failed' WHERE run_id = %s", (run_id,))
        with pytest.raises(psycopg.errors.CheckViolation):
            conn.execute(
                "UPDATE tax_fund_determination_runs SET status = 'succeeded', cutoff = %s WHERE run_id = %s",
                (T0 - timedelta(seconds=1), run_id),
            )
    _finish(run_id)
    with _connect() as conn:
        with pytest.raises(psycopg.errors.RaiseException, match="is finished"):
            conn.execute(
                "UPDATE tax_fund_determination_runs SET status = 'failed', cutoff = NULL, failure = 'x' "
                "WHERE run_id = %s",
                (run_id,),
            )
        with pytest.raises(psycopg.errors.RaiseException, match="never deleted"):
            conn.execute("DELETE FROM tax_fund_determination_runs WHERE run_id = %s", (run_id,))
    failed = _run(fund_observed_at=None)
    _finish(failed, "failed")
    assert _one("SELECT array_agg(status ORDER BY run_id) FROM tax_fund_determination_runs") == ["succeeded", "failed"]


def test_a_finished_run_is_sealed(spy: dict[str, Any]) -> None:
    determined = _determined(spy)
    _finish(spy["run_id"])
    with pytest.raises(psycopg.errors.RaiseException, match="is not running"):
        _load(spy["run_id"])
    with pytest.raises(psycopg.errors.RaiseException, match="is not running"):
        _discover(spy)
    with pytest.raises(psycopg.errors.RaiseException, match="is not running"):
        _observe(spy["row"] | UNAVAILABLE, tax_security_id=SECURITY + 1)
    doc_id, fetch_id = next(iter(spy["documents"].items()))
    with pytest.raises(psycopg.errors.RaiseException, match="is not running"):
        _insert(
            "tax_fund_determination_documents", {"observation_id": determined, "doc_id": doc_id, "fetch_id": fetch_id}
        )


def test_a_load_names_a_stored_determination(ebull_test_conn: psycopg.Connection[Any]) -> None:
    run_id = _run()
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        _load(run_id)


# --- unreviewed accessions --------------------------------------------------------------------------------------------


def test_a_discovery_survives_its_runs_failure(spy: dict[str, Any]) -> None:
    _discover(spy)
    _finish(spy["run_id"], "failed")
    assert _one("SELECT count(*) FROM tax_fund_determination_unreviewed") == 1


def test_a_discovery_needs_a_run_that_loaded_the_determination(spy: dict[str, Any]) -> None:
    other = _run()
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        _discover(spy, first_run_id=other)


def test_a_discovery_is_dated_no_later_than_its_lists_utc_fetch_date(spy: dict[str, Any]) -> None:
    late = datetime(2026, 10, 15, 23, 30, tzinfo=UTC)  # 00:30 on the 16th in London: the UTC date governs
    listing = _submissions(at=late)
    with pytest.raises(psycopg.errors.RaiseException, match="dated after"):
        _discover(spy, submissions_fetch_id=listing, filed=late.date() + timedelta(days=1))
    _discover(spy, submissions_fetch_id=listing, filed=late.date())


@pytest.mark.parametrize("kind", ["other_cik", "other_source"])
def test_a_discovery_is_from_the_ciks_filing_list(spy: dict[str, Any], kind: str) -> None:
    listing = _submissions(cik="0000884395") if kind == "other_cik" else _submissions(source="sec_governing_document")
    with pytest.raises(psycopg.errors.RaiseException, match="not the filing list"):
        _discover(spy, submissions_fetch_id=listing)


def test_an_accession_is_recorded_once_per_determination(spy: dict[str, Any]) -> None:
    _discover(spy)
    with pytest.raises(psycopg.errors.UniqueViolation):
        _discover(spy, form="497K")


# --- observations -----------------------------------------------------------------------------------------------------


def test_each_outcome_records_its_columns(spy: dict[str, Any]) -> None:
    row = spy["row"]
    none = {"fund_observation_id": None, "determination_id": None, "refusal": None, "submissions_fetch_id": None}
    undetermined = none | {"fund_observation_id": "fund"}
    by_outcome = [
        UNAVAILABLE,
        undetermined | {"outcome": "not_candidate"},
        undetermined | {"outcome": "no_determination"},
        undetermined | {"outcome": "binding_mismatch", "refusal": "isin"},
        {"outcome": "documents_unavailable", "refusal": "prospectus_2026", "submissions_fetch_id": None},
        {"outcome": "document_changed", "refusal": "prospectus_2026", "submissions_fetch_id": None},
        {"outcome": "quote_unverified", "refusal": "s355_limb:tiopa_s355", "submissions_fetch_id": None},
        {"outcome": "review_out_of_date", "refusal": "cutoff 2027-10-15", "submissions_fetch_id": None},
        {"outcome": "coverage_unavailable", "refusal": "submissions missing", "submissions_fetch_id": None},
        {"outcome": "coverage_unavailable", "refusal": "submissions not current"},
        {"outcome": "stale"},
    ]
    for n, over in enumerate(by_outcome, start=1):
        if over.get("fund_observation_id", "fund") == "fund":
            over |= {"fund_observation_id": _fund_observation(security=SECURITY + n)}
        _observe(row, tax_security_id=SECURITY + n, **over)
    _determined(spy)
    assert _one("SELECT count(DISTINCT outcome) FROM tax_fund_determination_observations") == len(OUTCOMES)


@pytest.mark.parametrize(
    "over",
    [
        {"outcome": "fund_observation_unavailable"},  # a fund observation it says is unavailable
        {"outcome": "not_candidate", "refusal": None},  # carries a determination
        {"outcome": "binding_mismatch", "refusal": "isin"},  # carries a determination
        {
            "outcome": "documents_unavailable",
            "refusal": "prospectus_2026",
            "submissions_fetch_id": None,
            "determination_id": None,
        },  # fmt: skip
        {"refusal": None},
        {"outcome": "documents_unavailable", "refusal": "prospectus_2026"},  # a filing list before coverage
        {"submissions_fetch_id": None},  # stale with no filing list
        {"offshore_fund": "true", "offshore_basis": "assumed"},  # an offshore answer when not determined
        {"outcome": "determined", "refusal": None},  # determined with no offshore answer
        {"outcome": "determined", "refusal": None, "offshore_fund": "yes", "offshore_basis": "assumed"},
        {"refusal": ""},
    ],
)
def test_an_observation_with_the_wrong_columns_for_its_outcome_is_refused(
    spy: dict[str, Any], over: dict[str, Any]
) -> None:
    with pytest.raises(psycopg.errors.CheckViolation):
        _observe(spy["row"], **over)


def test_one_observation_per_security_per_run(spy: dict[str, Any]) -> None:
    _observe(spy["row"])
    with pytest.raises(psycopg.errors.UniqueViolation):
        _observe(spy["row"], refusal="0001193125-26-100001")


def test_the_fund_observation_is_the_securitys_at_the_runs_fund_observed_at(spy: dict[str, Any]) -> None:
    with pytest.raises(psycopg.errors.RaiseException, match="another security"):
        _observe(spy["row"], fund_observation_id=_fund_observation(security=SECURITY + 1))
    with pytest.raises(psycopg.errors.RaiseException, match="fund_observed_at"):
        _observe(spy["row"], fund_observation_id=_fund_observation(observed_at=T0 - timedelta(microseconds=1)))
    # Another identity policy's observation of the security at another run's fund_observed_at.
    later = T0 + timedelta(seconds=1)
    other_policy = _run(fund_observed_at=later)
    _load(other_policy)
    with pytest.raises(psycopg.errors.RaiseException, match="not fund_identity_v1"):
        _observe(
            spy["row"],
            run_id=other_policy,
            fund_observation_id=_fund_observation(observed_at=later, policy="fund_identity_v2"),
        )
    # A run whose fund step failed has no fund observation to name.
    failed_fund_step = _run(fund_observed_at=None)
    _load(failed_fund_step)
    with pytest.raises(psycopg.errors.RaiseException, match="fund_observed_at"):
        _observe(spy["row"], run_id=failed_fund_step)
    _observe(spy["row"] | UNAVAILABLE, run_id=failed_fund_step)


def test_the_determination_is_loaded_by_the_run_and_of_its_policy(spy: dict[str, Any]) -> None:
    unloaded = _run()
    with pytest.raises(psycopg.errors.RaiseException, match="did not load"):
        _observe(spy["row"], run_id=unloaded)
    with pytest.raises(psycopg.errors.RaiseException, match="not the determination's"):
        _observe(spy["row"], policy_id="fund_determination_v2")


@pytest.mark.parametrize("kind", ["other_cik", "other_source"])
def test_the_filing_list_is_the_ciks(spy: dict[str, Any], kind: str) -> None:
    listing = _submissions(cik="0000884395") if kind == "other_cik" else _submissions(source="sec_governing_document")
    with pytest.raises(psycopg.errors.RaiseException, match="not the filing list"):
        _observe(spy["row"], submissions_fetch_id=listing)


# --- document rows ----------------------------------------------------------------------------------------------------


def test_a_determined_observation_has_one_row_per_document(spy: dict[str, Any]) -> None:
    observation = _determined(spy)
    assert _one(
        "SELECT count(*) FROM tax_fund_determination_documents WHERE observation_id = %s", (observation,)
    ) == len(SPY["documents"])


def test_a_determined_observation_missing_a_document_row_is_refused_at_commit(spy: dict[str, Any]) -> None:
    with pytest.raises(psycopg.errors.RaiseException, match="a document row per document"):
        _determined(spy, skip=1)
    assert _one("SELECT count(*) FROM tax_fund_determination_observations") == 0


def test_a_document_row_names_a_document_of_the_determination_and_its_fetch(spy: dict[str, Any]) -> None:
    doc_id, fetch_id = next(iter(spy["documents"].items()))
    other_fetch_id = list(spy["documents"].values())[1]
    with psycopg.connect(test_database_url()) as conn:
        observation = _observe(
            spy["row"], conn, outcome="determined", refusal=None, offshore_fund="true", offshore_basis="assumed"
        )
        for over, match in (
            ({"doc_id": "annual_report_9999"}, "not a document of"),
            ({"fetch_id": other_fetch_id}, "not a governing-document fetch"),
            ({"fetch_id": spy["submissions"]}, "not a governing-document fetch"),
        ):
            with pytest.raises(psycopg.errors.RaiseException, match=match), conn.transaction():
                _insert(
                    "tax_fund_determination_documents",
                    {"observation_id": observation, "doc_id": doc_id, "fetch_id": fetch_id} | over,
                    conn,
                )
        conn.rollback()


def test_a_document_row_is_only_of_a_determined_observation(spy: dict[str, Any]) -> None:
    stale = _observe(spy["row"])
    doc_id, fetch_id = next(iter(spy["documents"].items()))
    with pytest.raises(psycopg.errors.RaiseException, match="not determined"):
        _insert("tax_fund_determination_documents", {"observation_id": stale, "doc_id": doc_id, "fetch_id": fetch_id})


# --- insert only ------------------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("table", "column"),
    [
        ("tax_fund_determinations", "loaded_at"),
        ("tax_fund_authority_snapshots", "body"),
        ("tax_fund_determination_authorities", "authority_id"),
        ("tax_fund_determination_run_loads", "determination_id"),
        ("tax_fund_determination_unreviewed", "form"),
        ("tax_fund_determination_observations", "refusal"),
        ("tax_fund_determination_documents", "fetch_id"),
    ],
)
def test_every_table_refuses_update_and_delete(spy: dict[str, Any], table: str, column: str) -> None:
    body = b"https://www.legislation.gov.uk/ukpga/2010/8/section/355/data.xht\n"
    sha = _insert("tax_fund_authority_snapshots", {"content_sha256": hashlib.sha256(body).hexdigest(), "body": body})
    _insert(
        "tax_fund_determination_authorities",
        {"determination_id": DID, "authority_id": "tiopa_s355", "content_sha256": sha},
    )
    _discover(spy)
    _determined(spy)
    with _connect() as conn:
        for statement in ("UPDATE {t} SET {c} = {c}", "DELETE FROM {t}"):
            with pytest.raises(psycopg.errors.RaiseException, match="never updated or deleted"):
                conn.execute(sql.SQL(statement).format(t=sql.Identifier(table), c=sql.Identifier(column)))
