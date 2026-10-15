"""Slice 3d-3c-3 part B against the test database: sql/494's contracts and the v2 run (spec Tests 9 and 10).

Spec: docs/specs/metrics/2026-10-15-3616-slice3d3c3-fund-disposals.md. Auto-marked ``db`` (pulls ``ebull_test_conn``
through the ``funds`` fixture, which wipes between tests). The determination is the synthetic one of
``tests.test_tax_fund_determination_refresh_db``, stored by the real ``refresh_fund_determinations`` step; SPY's lots
are evidence run 7's (spec fact 2). Matching's clock is fixed at ``AS_OF`` so ``review_by`` is reached only on purpose.

Schema tests insert rows by hand inside one transaction and roll it back: each refused insert runs in its own
savepoint. Shares under the v2 ids are ``tests.test_tax_matching_db``'s whole suite, whose figures are the v1 run's.
"""

from __future__ import annotations

import hashlib
import uuid
from collections.abc import Iterator
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg
import pytest
from psycopg import sql
from psycopg.types.json import Jsonb

from app.services import tax_matching
from app.services.tax_matching import effective_tax_matching, run_tax_matching
from tests.fixtures.ebull_test_db import test_database_url
from tests.test_tax_fund_determination import make_det
from tests.test_tax_fund_determination_refresh_db import (  # noqa: F401 - `fund_at`, `dets`, `funds` are fixtures
    DID,
    _step,
    _write,
    dets,
    fund_at,
    funds,
)
from tests.test_tax_fund_refresh_db import CSPX, SPY, _connect, _rows
from tests.test_tax_matching_db import ACCOUNT, _insert_subject

D = Decimal
AS_OF = datetime(2026, 10, 15, 6, 0, tzinfo=UTC)
FETCH = datetime(2026, 10, 15, 5, 4, 16, tzinfo=UTC)
MIGRATION = Path(__file__).resolve().parents[1] / "sql/494_tax_fund_disposals.sql"
SPREAD, FORMULA = "spread_inclusion_assumed", "disposal_formula_assumed"
HCU, NRS, F2, WO = (
    "history_coverage_unestablished",
    "non_reporting_status_assumed",
    "thirty_day_period_assumed",
    "window_open",
)

#: (lot_key, subject, direction, executed_at UTC, units, £ or None for pending FX): spec fact 2.
SPY_LOTS = [
    (
        "acq:demo:382257232",
        "order:382257232",
        "acquisition",
        "2026-09-18 15:36:45.083",
        "0.29615500",
        "168.43050669140000",
    ),
    (
        "dis:demo:3601264304",
        "order:382257232",
        "disposal",
        "2026-09-23 13:35:31.583",
        "0.29615500",
        "172.44628722262500",
    ),
    (
        "acq:demo:383458210",
        "order:383458210",
        "acquisition",
        "2026-09-23 14:02:07.123",
        "0.16227400",
        "94.27063701503500",
    ),
    (
        "acq:demo:383458257",
        "order:383458257",
        "acquisition",
        "2026-09-23 14:10:19.957",
        "0.12993500",
        "75.03203920117100",
    ),
    (
        "acq:demo:385158730",
        "order:385158730",
        "acquisition",
        "2026-09-30 15:37:50.750",
        "20.62672000",
        "11933.68460630607000",
    ),
    (
        "acq:demo:385501993",
        "order:385501993",
        "acquisition",
        "2026-10-01 15:37:26.687",
        "0.13442600",
        "77.30850742070600",
    ),
    (
        "acq:demo:387752115",
        "order:387752115",
        "acquisition",
        "2026-10-09 15:37:35.540",
        "0.61384700",
        "360.56710775200500",
    ),
    ("acq:demo:388523915", "order:388523915", "acquisition", "2026-10-14 15:38:12.083", "0.09032700", None),
]


def _tax_year(day: date) -> str:
    start = day.year if (day.month, day.day) >= (4, 6) else day.year - 1
    return f"{start}/{(start + 1) % 100:02d}"


def _spy_generation(
    lots: list[tuple[str, str, str, str, str, str | None]] = SPY_LOTS, policy: str = "broker_evidence_v3"
) -> int:
    """A published generation holding ``lots`` of SPY, with an ``ok`` history fetch started at ``FETCH``."""
    with _connect() as conn, conn.transaction():
        fetch = conn.execute(
            "INSERT INTO trade_history_fetches (environment, account_id, portfolio_fetched_at, "
            "portfolio_real_positions, started_at, finished_at, outcome, row_count) "
            "VALUES ('demo', %s, %s, 0, %s, %s, 'ok', 0) RETURNING fetch_id",
            (ACCOUNT, FETCH, FETCH, FETCH),
        ).fetchone()
        row = conn.execute(
            "INSERT INTO tax_evidence_runs (environment, account_id, started_at, state, evidence_policy_id) "
            "VALUES ('demo', %s, now(), 'in_progress', %s) RETURNING run_id",
            (ACCOUNT, policy),
        ).fetchone()
        assert fetch is not None and row is not None
        run_id = int(row[0])
        for subject in sorted({lot[1] for lot in lots}):
            _insert_subject(conn, run_id, (subject, "admitted", SPY))
        for key, subject, direction, at, units, gbp in lots:
            executed = datetime.fromisoformat(at).replace(tzinfo=UTC)
            conn.execute(
                """
                INSERT INTO tax_evidence_lots
                    (run_id, lot_key, subject, order_id, instrument_id, etoro_instrument_id, direction, position_ids,
                     trade_event_ids, executed_at, uk_date, tax_year, units, usd_amount, consideration_status,
                     execution_evidence, fx_rate_date, fx_status, fx_gbp_per_usd, fx_policy_id, amount_gbp, fingerprint)
                VALUES (%s, %s, %s, %s, %s, 1, %s, '{1}', '{1}', %s, %s, %s, %s, 1, 'broker_stated_unreconciled',
                        'lookup', %s, %s, %s, 'tax_fx_v1', %s, %s)
                """,
                (
                    run_id,
                    key,
                    subject,
                    int(subject.split(":")[1]),
                    SPY,
                    direction,
                    executed,
                    executed.date(),
                    _tax_year(executed.date()),
                    units,
                    executed.date(),
                    "pending" if gbp is None else "published",
                    None if gbp is None else 1,
                    gbp,
                    hashlib.sha256(f"{key}|{units}|{gbp}".encode()).hexdigest(),
                ),
            )
        conn.execute(
            "UPDATE tax_evidence_runs SET state = 'published', finished_at = now(), counts = %s, "
            "history_fetch_ok_id = %s WHERE run_id = %s",
            (Jsonb({}), fetch[0], run_id),
        )
    return run_id


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> Iterator[list[datetime]]:
    """Matching's ``as_of``; a test moves it by replacing the list's element."""
    now = [AS_OF]
    monkeypatch.setattr(tax_matching, "_clock", lambda conn: now[0])
    yield now


@pytest.fixture
def determined(fund_at: datetime, dets: Path, clock: list[datetime]) -> int:  # noqa: F811
    """A succeeded determination run in which SPY is ``determined`` (transparency unresolved) and CSPX.L
    ``not_candidate``; returns its id."""
    report = _step(dets, fund_at)
    assert report.run_id is not None
    return report.run_id


def _run() -> tax_matching.MatchingRunReport:
    with _connect() as conn:
        return run_tax_matching(conn)


def _effective() -> tax_matching.EffectiveMatching | None:
    with _connect() as conn:
        return effective_tax_matching(conn)


# --- 10. the run and selection ----------------------------------------------------------------------------------------


def test_spy_publishes_one_disposal_one_identification_and_seven_balances(determined: int) -> None:
    generation = _spy_generation()
    assert _effective() is None
    run = _run()
    assert (run.outcome, run.evidence_run_id) == ("published", generation)
    assert {k: run.counts[k] for k in ("fund_disposals", "fund_identifications", "fund_balances")} == {
        "fund_disposals": 1,
        "fund_identifications": 1,
        "fund_balances": 7,
    }
    assert _effective() == tax_matching.EffectiveMatching(run.matching_run_id or 0, generation)
    rid = (run.matching_run_id,)
    assert _rows(
        "SELECT matching_policy_id, character_policy_id FROM tax_matching_runs WHERE matching_run_id = %s", rid
    ) == [("identification_v2", "tax_character_v2")]
    ((character, detail, status, det, det_run, transparent, basis, assumptions),) = _rows(
        "SELECT character, detail, fund_status, fund_determination_id, fund_determination_run_id, transparent_fund, "
        "transparent_basis, fund_assumptions FROM tax_matching_characters WHERE matching_run_id = %s",
        rid,
    )
    assert (character, detail, status, det, det_run, transparent, basis) == (
        "non_reporting_fund",
        None,
        None,
        DID,
        determined,
        "unresolved",
        "unresolved",
    )
    assert assumptions == sorted(set(assumptions)) and "A5_part3_not_applying" in assumptions
    assert _rows(
        "SELECT status, holding_basis, pool_units, pool_cost_gbp, pool_missing_lot_keys, pool_reasons, "
        "window_open_since FROM tax_matching_securities WHERE matching_run_id = %s",
        rid,
    ) == [
        (
            "computed",
            "relevant_securities",
            D("21.75752900"),
            None,
            ["acq:demo:388523915"],
            sorted([HCU, NRS, SPREAD, F2, WO]),
            date(2026, 9, 23),
        )
    ]
    assert _rows(
        "SELECT lot_key, seq, uk_date, tax_year, units, proceeds_gbp, cost_gbp, computed_gain_gbp, basic_gain_gbp, "
        "offshore_income_gain_gbp, chargeable_gain_gbp, status, gain_character, missing_lot_keys, reasons "
        "FROM tax_fund_disposals WHERE matching_run_id = %s",
        rid,
    ) == [
        (
            "dis:demo:3601264304",
            1,
            date(2026, 9, 23),
            "2026/27",
            D("0.29615500"),
            D("172.44628722262500"),
            D("171.34185001689916"),
            D("1.10443720572584"),
            D("1.10443720572584"),
            None,
            None,
            "computed",
            "oig_or_capital_unresolved",
            [],
            sorted([FORMULA, HCU, NRS, SPREAD, F2, WO]),
        )
    ]
    assert _rows(
        "SELECT disposal_lot_key, seq, rule, acquisition_lot_key, units, cost_gbp FROM tax_fund_identifications "
        "WHERE matching_run_id = %s",
        rid,
    ) == [("dis:demo:3601264304", 1, "thirty_day", "acq:demo:385158730", D("0.29615500"), D("171.34185001689916"))]
    balances = _rows(
        "SELECT lot_key, units_left, cost_left_gbp, missing_lot_keys FROM tax_fund_acquisition_balances "
        "WHERE matching_run_id = %s ORDER BY lot_key",
        rid,
    )
    assert len(balances) == 7
    assert ("acq:demo:385158730", D("20.33056500"), D("11762.34275628917084"), []) in balances
    assert ("acq:demo:388523915", D("0.09032700"), None, ["acq:demo:388523915"]) in balances
    assert _rows("SELECT count(*) FROM tax_statutory_disposals WHERE matching_run_id = %s", rid) == [(0,)]
    assert _run().outcome == "already_published"


def test_unavailable_until_a_v2_run_publishes_and_a_refused_one_is_never_selected(determined: int) -> None:
    generation = _spy_generation()
    with _connect() as conn, conn.transaction():  # a v1 run over the same generation, as deployed before
        v1 = _building(conn, generation, "share_identification_v1", "tax_character_v1")
        conn.execute(
            "UPDATE tax_matching_runs SET state = 'published', counts = '{}' WHERE matching_run_id = %s", (v1,)
        )
    assert _effective() is None  # never the v1 run
    run = _run()
    assert run.outcome == "published" and _effective() == tax_matching.EffectiveMatching(
        run.matching_run_id or 0, generation
    )
    _spy_generation(policy="broker_evidence_v2")  # a newer generation the v2 run refuses
    refused = _run()
    assert (refused.outcome, refused.reason) == ("refused", "evidence_policy_unsupported")
    assert _effective() is None


def test_a_run_published_before_review_by_is_not_selected_after_it(determined: int, clock: list[datetime]) -> None:
    generation = _spy_generation()
    run = _run()
    assert _effective() == tax_matching.EffectiveMatching(run.matching_run_id or 0, generation)
    clock[0] = datetime(2027, 10, 15, tzinfo=UTC)  # make_det's review_by is 2027-10-14
    assert _effective() is None  # the selection recomputes the status with its own as_of
    later = _run()
    assert later.outcome == "published" and later.matching_run_id != run.matching_run_id
    assert _rows(
        "SELECT character, detail, fund_status FROM tax_matching_characters WHERE matching_run_id = %s",
        (later.matching_run_id,),
    ) == [("unestablished", "exchange_traded_fund", "review_out_of_date")]


# --- 9. the schema ----------------------------------------------------------------------------------------------------

V2 = ("identification_v2", "tax_character_v2")


def _building(conn: psycopg.Connection[Any], evidence_run_id: int, matching: str, character: str | None) -> int:
    document = f'{{"test":"{uuid.uuid4()}"}}'
    row = conn.execute(
        "INSERT INTO tax_matching_runs (evidence_run_id, matching_policy_id, class_policy_id, character_policy_id, "
        "as_of, inputs_digest, inputs_document, state, computed_at, withheld_all) "
        "VALUES (%s, %s, 'consideration_classes_v2', %s, %s, %s, %s, 'building', now(), FALSE) "
        "RETURNING matching_run_id",
        (
            evidence_run_id,
            matching,
            character,
            None if character is None else AS_OF,
            hashlib.sha256(document.encode()).hexdigest(),
            document,
        ),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _security(conn: psycopg.Connection[Any], run: int, sid: int, status: str, basis: str | None) -> None:
    pool = (None,) * 5 if status == "withheld" else (D("1"), D("1"), [], [], None)
    conn.execute(
        "INSERT INTO tax_matching_securities (matching_run_id, tax_security_id, instrument_ids, status, pool_units, "
        "pool_cost_gbp, pool_missing_lot_keys, pool_reasons, window_open_since, holding_basis) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (run, sid, [sid], status, *pool, basis),
    )


_FUND_COLUMNS = (
    "fund_status",
    "fund_status_reason",
    "fund_status_date",
    "fund_determination_id",
    "fund_determination_run_id",
    "transparent_fund",
    "transparent_basis",
    "fund_assumptions",
)


def _character(
    conn: psycopg.Connection[Any], run: int, sid: int, character: str, detail: str | None, **fund: Any
) -> None:
    conn.execute(
        "INSERT INTO tax_matching_characters (matching_run_id, tax_security_id, character, detail, "
        + ", ".join(_FUND_COLUMNS)
        + ") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (run, sid, character, detail, *(fund.get(c) for c in _FUND_COLUMNS)),
    )


def _nrf(det_run: int, transparent: tuple[str, str] = ("unresolved", "unresolved")) -> dict[str, Any]:
    """A ``non_reporting_fund`` row's fund columns."""
    return {
        "fund_determination_id": DID,
        "fund_determination_run_id": det_run,
        "transparent_fund": transparent[0],
        "transparent_basis": transparent[1],
        "fund_assumptions": ["A1_trustee_cmc", "A5_part3_not_applying"],
    }


def _disposal(conn: psycopg.Connection[Any], run: int, sid: int, key: str, **overrides: Any) -> None:
    """A computed £5 gain, unresolved, unless ``overrides`` say otherwise."""
    row: dict[str, Any] = {
        "seq": 1,
        "executed_at": datetime(2026, 9, 23, 12, tzinfo=UTC),
        "uk_date": date(2026, 9, 23),
        "tax_year": "2026/27",
        "units": D("1"),
        "proceeds_gbp": D("15"),
        "cost_gbp": D("10"),
        "computed_gain_gbp": D("5"),
        "basic_gain_gbp": D("5"),
        "offshore_income_gain_gbp": None,
        "chargeable_gain_gbp": None,
        "status": "computed",
        "gain_character": "oig_or_capital_unresolved",
        "missing_lot_keys": [],
        "reasons": [],
    } | overrides
    conn.execute(
        sql.SQL(
            "INSERT INTO tax_fund_disposals (matching_run_id, tax_security_id, lot_key, {}) VALUES (%s, %s, %s, {})"
        ).format(sql.SQL(", ").join(map(sql.Identifier, row)), sql.SQL(", ").join([sql.Placeholder()] * len(row))),
        (run, sid, key, *row.values()),
    )


def _identification(
    conn: psycopg.Connection[Any], run: int, sid: int, disposal: str, cost: Any, missing: list[str]
) -> None:
    conn.execute(
        "INSERT INTO tax_fund_identifications (matching_run_id, tax_security_id, disposal_lot_key, seq, rule, "
        "acquisition_lot_key, acquisition_executed_at, acquisition_uk_date, units, cost_gbp, missing_lot_keys, "
        "reasons) "
        "VALUES (%s, %s, %s, 1, 'lifo', 'acq:1', %s, %s, 1, %s, %s, '{}')",
        (run, sid, disposal, datetime(2026, 9, 18, 12, tzinfo=UTC), date(2026, 9, 18), cost, missing),
    )


def _balance(conn: psycopg.Connection[Any], run: int, sid: int, cost: Any = D("1"), key: str = "acq:2") -> None:
    conn.execute(
        "INSERT INTO tax_fund_acquisition_balances (matching_run_id, tax_security_id, lot_key, units_left, "
        "cost_left_gbp, missing_lot_keys, reasons) VALUES (%s, %s, %s, 1, %s, %s, '{}')",
        (run, sid, key, cost, [] if cost is not None else [key]),
    )


def _refused(conn: psycopg.Connection[Any], insert: Any, *args: Any, **kwargs: Any) -> None:
    with pytest.raises(psycopg.Error), conn.transaction():
        insert(conn, *args, **kwargs)


@pytest.fixture
def generation(determined: int) -> int:
    return _spy_generation()


def test_design_3_rows_their_sign_violations_and_the_column_contracts(determined: int, generation: int) -> None:
    with _connect() as conn, conn.transaction():
        run = _building(conn, generation, *V2)
        _security(conn, run, SPY, "computed", "relevant_securities")
        _character(conn, run, SPY, "non_reporting_fund", None, **_nrf(determined))
        loss = {"cost_gbp": D("20"), "computed_gain_gbp": D("-5"), "basic_gain_gbp": D("0")}
        accepted = {
            "loss": loss
            | {"gain_character": "capital_loss", "offshore_income_gain_gbp": D("0"), "chargeable_gain_gbp": D("-5")},
            "nil": {
                "cost_gbp": D("15"),
                "computed_gain_gbp": D("0"),
                "basic_gain_gbp": D("0"),
                "gain_character": "nil",
                "offshore_income_gain_gbp": D("0"),
                "chargeable_gain_gbp": D("0"),
            },
            "gain": {},
            "pending": {
                "cost_gbp": None,
                "computed_gain_gbp": None,
                "basic_gain_gbp": None,
                "gain_character": None,
                "status": "amount_missing",
                "missing_lot_keys": ["acq:1"],
            },
        }
        for seq, (key, overrides) in enumerate(accepted.items(), start=1):
            _disposal(conn, run, SPY, key, **(overrides | {"seq": seq}))
        refusals = [
            loss
            | {"gain_character": "capital_loss", "offshore_income_gain_gbp": D("0"), "chargeable_gain_gbp": D("0")},
            {"gain_character": "capital_loss", "offshore_income_gain_gbp": D("0"), "chargeable_gain_gbp": D("5")},
            {"gain_character": "nil", "offshore_income_gain_gbp": D("0"), "chargeable_gain_gbp": D("0")},
            {"offshore_income_gain_gbp": D("5")},  # unresolved with a split
            {"basic_gain_gbp": D("4")},
            {"computed_gain_gbp": D("6"), "basic_gain_gbp": D("6")},  # not proceeds - cost
            accepted["pending"] | {"chargeable_gain_gbp": D("0")},  # amount_missing with a split
            accepted["pending"] | {"missing_lot_keys": []},
            {
                "gain_character": "offshore_income_gain",
                "offshore_income_gain_gbp": D("5"),
                "chargeable_gain_gbp": D("0"),
            },
            {"tax_year": "2026-27"},
            {"units": D("0")},
        ]
        for n, overrides in enumerate(refusals, start=10):
            _refused(conn, _disposal, run, SPY, f"bad{n}", **(overrides | {"seq": n}))
        _identification(conn, run, SPY, "gain", D("10"), [])
        _refused(conn, _identification, run, SPY, "loss", D("-1"), [])
        _refused(conn, _identification, run, SPY, "nil", None, [])
        _refused(conn, _identification, run, SPY, "nil", None, ["acq:other"])
        _identification(conn, run, SPY, "pending", None, ["acq:1"])
        _balance(conn, run, SPY)
        _balance(conn, run, SPY, None, "acq:3")
        _refused(conn, _balance, run, SPY, D("-1"), "acq:4")
        raise psycopg.Rollback()


def test_an_opaque_funds_positive_gain_is_an_offshore_income_gain(
    fund_at: datetime,  # noqa: F811
    tmp_path: Path,
    clock: list[datetime],
) -> None:
    report = _step(_write(tmp_path / "opaque", make_det(transparent_fund="opaque")), fund_at)
    assert report.run_id is not None
    generation = _spy_generation()
    with _connect() as conn, conn.transaction():
        run = _building(conn, generation, *V2)
        _security(conn, run, SPY, "computed", "relevant_securities")
        # The copied transparency must be the determination's.
        _refused(conn, _character, run, SPY, "non_reporting_fund", None, **_nrf(report.run_id))
        _character(conn, run, SPY, "non_reporting_fund", None, **_nrf(report.run_id, ("opaque", "established")))
        _refused(conn, _disposal, run, SPY, "unresolved")
        oig = {
            "gain_character": "offshore_income_gain",
            "offshore_income_gain_gbp": D("5"),
            "chargeable_gain_gbp": D("0"),
        }
        _disposal(conn, run, SPY, "oig", **oig)
        raise psycopg.Rollback()


def test_a_v2_character_row_takes_design_5s_truth_table(determined: int, generation: int) -> None:
    with _connect() as conn, conn.transaction():
        run = _building(conn, generation, *V2)
        for sid in (SPY, CSPX, 7, 8, 9):
            _security(conn, run, sid, "withheld", None)
        etf = ("unestablished", "exchange_traded_fund")
        # A determination the run did not determine for this security; a transparency differing from it.
        _refused(conn, _character, run, CSPX, "non_reporting_fund", None, **_nrf(determined))
        _refused(conn, _character, run, SPY, "non_reporting_fund", None, **_nrf(determined, ("opaque", "established")))
        _refused(
            conn,
            _character,
            run,
            SPY,
            "non_reporting_fund",
            None,
            **(_nrf(determined) | {"fund_assumptions": ["b", "a"]}),
        )
        _refused(
            conn, _character, run, SPY, "non_reporting_fund", None, **(_nrf(determined) | {"fund_status": "stale"})
        )
        # The ETF line carries a failing status, only it, and nothing of a determination.
        _refused(conn, _character, run, CSPX, *etf)
        _refused(conn, _character, run, CSPX, *etf, fund_status="non_reporting_assumed")
        _refused(conn, _character, run, CSPX, *etf, fund_status="not_candidate", fund_determination_id=DID)
        _refused(conn, _character, run, CSPX, *etf, fund_status="not_candidate", transparent_fund="unresolved")
        _refused(conn, _character, run, CSPX, *etf, fund_status="outside_interval")
        _refused(conn, _character, run, CSPX, *etf, fund_status="stale", fund_status_date=date(2026, 1, 1))
        _refused(conn, _character, run, 7, "share", None, fund_status="stale")
        _refused(conn, _character, run, 8, "unestablished", "member_conflict", fund_status="stale")
        _character(conn, run, CSPX, *etf, fund_status="outside_interval", fund_status_date=date(2026, 1, 25))
        _character(conn, run, 7, "share", None)
        _character(conn, run, 8, "unestablished", "member_conflict")
        _character(conn, run, 9, *etf, fund_status="reporting_status_unestablished", fund_status_reason="list_match")
        _character(conn, run, SPY, "non_reporting_fund", None, **_nrf(determined))
        raise psycopg.Rollback()


def test_rows_are_routed_by_holding_basis(determined: int, generation: int) -> None:
    with _connect() as conn, conn.transaction():
        run = _building(conn, generation, *V2)
        _refused(conn, _security, run, 1, "computed", None)
        _refused(conn, _security, run, 1, "withheld", "s104_pool")
        _security(conn, run, SPY, "computed", "s104_pool")
        _security(conn, run, CSPX, "withheld", None)
        _security(conn, run, 7, "computed", "relevant_securities")
        for sid in (SPY, CSPX):
            _refused(conn, _disposal, run, sid, "d")
            _refused(conn, _balance, run, sid)
        _refused(
            conn,
            lambda c: c.execute(
                "INSERT INTO tax_statutory_disposals (matching_run_id, tax_security_id, uk_date, tax_year, lot_keys, "
                "units, proceeds_gbp, cost_gbp, gain_gbp, status, missing_lot_keys, reasons) "
                "VALUES (%s, 7, '2026-09-23', '2026/27', '{d}', 1, 2, 1, 1, 'computed', '{}', '{}')",
                (run,),
            ),
        )
        raise psycopg.Rollback()


@pytest.mark.parametrize(
    ("matching", "character"),
    [
        ("share_identification_v1", "tax_character_v1"),
        ("share_identification_v1", None),
        ("identification_v2", "tax_character_v1"),
        ("share_identification_v1", "tax_character_v2"),
    ],
)
def test_any_other_policy_pair_takes_legacy_rows_only(
    determined: int, generation: int, matching: str, character: str | None
) -> None:
    with _connect() as conn, conn.transaction():
        run = _building(conn, generation, matching, character)
        _refused(conn, _security, run, 1, "computed", "s104_pool")
        _security(conn, run, SPY, "computed", None)
        _security(conn, run, CSPX, "withheld", None)
        _refused(conn, _character, run, SPY, "non_reporting_fund", None, **_nrf(determined))
        _refused(conn, _character, run, CSPX, "unestablished", "exchange_traded_fund", fund_status="not_candidate")
        _character(conn, run, SPY, "share", None)
        _character(conn, run, CSPX, "unestablished", "exchange_traded_fund")
        _refused(conn, _disposal, run, SPY, "d")
        _refused(conn, _balance, run, SPY)
        raise psycopg.Rollback()


def test_a_sealed_run_takes_no_fund_row(determined: int, generation: int) -> None:
    with _connect() as conn, conn.transaction():
        run = _building(conn, generation, *V2)
        _security(conn, run, SPY, "computed", "relevant_securities")
        _character(conn, run, SPY, "non_reporting_fund", None, **_nrf(determined))
        conn.execute(
            "UPDATE tax_matching_runs SET state = 'published', counts = '{}' WHERE matching_run_id = %s", (run,)
        )
        _refused(conn, _balance, run, SPY)
        _refused(conn, _disposal, run, SPY, "d")
        raise psycopg.Rollback()


def test_the_migration_leaves_sealed_v1_rows_unchanged(determined: int, generation: int) -> None:
    """Sealed v1 rows (an ETF, a share with a disposal, a withheld security) survive re-applying sql/494: its
    constraints validate them, and they read back unchanged with every new column NULL."""
    with _connect() as conn, conn.transaction():
        run = _building(conn, generation, "share_identification_v1", "tax_character_v1")
        _security(conn, run, SPY, "withheld", None)
        _security(conn, run, 7, "computed", None)
        _security(conn, run, 8, "withheld", None)
        _character(conn, run, SPY, "unestablished", "exchange_traded_fund")
        _character(conn, run, 7, "share", None)
        _character(conn, run, 8, "unestablished", "sic")
        conn.execute(
            "INSERT INTO tax_statutory_disposals (matching_run_id, tax_security_id, uk_date, tax_year, lot_keys, "
            "units, proceeds_gbp, cost_gbp, gain_gbp, status, missing_lot_keys, reasons) "
            "VALUES (%s, 7, '2026-09-23', '2026/27', '{d}', 1, 2, 1, 1, 'computed', '{}', '{}')",
            (run,),
        )
        conn.execute(
            "UPDATE tax_matching_runs SET state = 'published', counts = '{}' WHERE matching_run_id = %s", (run,)
        )
    tables = ("tax_matching_securities", "tax_matching_characters", "tax_statutory_disposals")

    def snapshot() -> list[list[tuple]]:
        return [
            _rows(f"SELECT to_jsonb(t)::text FROM {table} t WHERE matching_run_id = %s ORDER BY 1", (run,))  # noqa: S608
            for table in tables
        ]

    before = snapshot()
    with psycopg.connect(test_database_url()) as conn, psycopg.ClientCursor(conn) as cur:  # as the runner applies it
        cur.execute(MIGRATION.read_text())  # type: ignore[call-overload]
    assert snapshot() == before
    assert _rows(
        "SELECT count(*) FROM tax_matching_characters WHERE matching_run_id = %s AND num_nonnulls("
        + ", ".join(_FUND_COLUMNS)
        + ") = 0",
        (run,),
    ) == [(3,)]
    assert _rows(
        "SELECT count(*) FROM tax_matching_securities WHERE matching_run_id = %s AND holding_basis IS NULL", (run,)
    ) == [(3,)]
