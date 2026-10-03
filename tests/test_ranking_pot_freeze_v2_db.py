"""#3592 slice 3c — the v2 freeze and the v1/v2 loaders against real Postgres (spec §8; Appendix A (59)).

- The freeze measures the strata, the DTC baseline and the history floor in its own transaction, writes v2's #2599 row,
  document and genesis event through v1's write path, and a second freeze is ``already_frozen``.
- Its refusals: no or stale S* (``dtc_unavailable``), a fresh S* with nothing usable (``dtc_baseline_empty``), no
  insider history (``history_floor_unavailable``).
- Isolation: v1's loader never returns v2's declaration and v2's never returns v1's; both share the family's sequence.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any

import psycopg
import pytest

from app.services import ranking_pot_policy, ranking_pot_v2_policy
from app.services import ranking_pot_v2 as v2
from app.services.ai_trial_pack import canonical_sha256
from app.services.ranking_pot_freeze import freeze_pot
from app.services.ranking_pot_freeze_v2 import freeze_pot_v2
from app.services.ranking_pot_rebalance import load_declaration as load_v1
from app.services.ranking_pot_v2_declaration import frozen_terms
from app.services.ranking_pot_v2_declaration import load_declaration as load_v2
from tests.test_ranking_pot_schema_db import PROVENANCE, _seed_scores
from tests.test_ranking_pot_v2_inputs_db import _dtc, _txn

Conn = psycopg.Connection[Any]

IDS = (2842, 2843, 2844)
ISSUER, FILER = "0000000789", "0006666001"
NOW = datetime.now(UTC)
FREEZE_MONTH = NOW.date().replace(day=1)
HISTORY_MONTHS = 30


@pytest.fixture(autouse=True)
def _build_complete(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ranking_pot_v2_policy, "BUILD_COMPLETE", True)
    monkeypatch.setattr(ranking_pot_policy, "BUILD_COMPLETE", True)


def _iso(ts: datetime) -> str:
    return ts.astimezone(UTC).replace(tzinfo=None).isoformat()


def _seed_dtc(conn: Conn, *, age_days: int = 10, adv: int | None = 50_000) -> date:
    settle = NOW.date() - timedelta(days=age_days)
    for iid in IDS:
        _dtc(conn, iid, settle.isoformat(), _iso(NOW - timedelta(hours=1)), "2.50", adv)
    conn.commit()
    return settle


def _seed_history(conn: Conn) -> None:
    """One open-market trade in each of the ``HISTORY_MONTHS`` complete months before the freeze month."""
    for k in range(1, HISTORY_MONTHS + 1):
        m = v2.months_back(FREEZE_MONTH, k)
        _txn(
            conn,
            iid=IDS[0],
            issuer=ISSUER,
            filer=FILER,
            txn=(m + timedelta(days=4)).isoformat(),
            filed=(m + timedelta(days=5)).isoformat(),
            code="S",
            ad="D",
        )
    conn.commit()


def _seed(conn: Conn) -> date:
    _seed_scores(conn, IDS)
    _seed_history(conn)
    return _seed_dtc(conn)


def _counts(conn: Conn) -> tuple[int, int, int]:
    row = conn.execute(
        """
        SELECT (SELECT count(*) FROM strategy_preregistration_declarations WHERE strategy_id = %(id)s),
               (SELECT count(*) FROM ranking_pot_declarations WHERE strategy_id = %(id)s),
               (SELECT count(*) FROM ranking_pot_state_events e JOIN ranking_pot_declarations d USING (declaration_id)
                 WHERE d.strategy_id = %(id)s)
        """,
        {"id": v2.STRATEGY_ID},
    ).fetchone()
    conn.commit()
    assert row is not None
    return (int(row[0]), int(row[1]), int(row[2]))


def test_dry_run_writes_nothing_and_apply_freezes_the_measured_terms(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    settle = _seed(conn)
    dry = freeze_pot_v2(conn, provenance=PROVENANCE, apply=False)
    assert (dry.refusals, dry.applied, dry.declaration_id) == ((), False, None)
    assert _counts(conn) == (0, 0, 0)

    applied = freeze_pot_v2(conn, provenance=PROVENANCE, apply=True, declared_by="supervisor")
    assert (applied.refusals, applied.applied) == ((), True)
    assert applied.doc_sha256 is not None and applied.doc is not None
    assert _counts(conn) == (1, 1, 1)

    decl = load_v2(conn)
    conn.commit()
    assert decl is not None and decl.state == "shadow_only" and decl.doc_sha256 == applied.doc_sha256
    assert canonical_sha256(decl.doc) == applied.doc_sha256
    terms = frozen_terms(decl)
    floor = v2.months_back(FREEZE_MONTH, HISTORY_MONTHS - 1)  # the run's first month is treated as partly ingested
    assert terms.strata == dict.fromkeys(IDS, 0)  # equal scores: every name at or below the first cut
    assert (terms.dtc_settlement_date, terms.dtc_baseline) == (settle, len(IDS))
    assert (terms.history_floor, str(terms.floor_threshold)) == (floor, "1/10")
    assert terms.floor_counts == {v2.months_back(FREEZE_MONTH, k): 1 for k in range(1, HISTORY_MONTHS)}
    assert decl.doc["terms"]["book_count"] == decl.doc["terms"]["k_controls"] + 3
    row = conn.execute(
        "SELECT p.contract_version, d.doc_path FROM strategy_preregistration_declarations p "
        "JOIN ranking_pot_declarations d USING (declaration_id) WHERE d.declaration_id = %s",
        (decl.declaration_id,),
    ).fetchone()
    conn.commit()
    assert row is not None and row[0] == f"ranking-pot-declaration-v1:{applied.doc_sha256}"
    assert str(row[1]).startswith("generated:ranking_pot_freeze_v2.build_declaration@")

    again = freeze_pot_v2(conn, provenance=PROVENANCE, apply=True, declared_by="supervisor")
    assert "already_frozen" in again.refusals and not again.applied
    assert again.existing_doc_sha256 == applied.doc_sha256
    assert _counts(conn) == (1, 1, 1)
    assert load_v1(conn) is None  # v1's loader never sees v2's declaration
    conn.commit()


def test_v1_and_v2_load_only_their_own_and_share_the_family_sequence(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    _seed(conn)
    v1_report = freeze_pot(conn, provenance=PROVENANCE, apply=True, declared_by="supervisor")
    assert (v1_report.refusals, v1_report.applied) == ((), True)
    assert load_v2(conn) is None  # v2's loader never sees v1's declaration
    conn.commit()
    v2_report = freeze_pot_v2(conn, provenance=PROVENANCE, apply=True, declared_by="supervisor")
    assert (v2_report.refusals, v2_report.applied) == ((), True)

    one, two = load_v1(conn), load_v2(conn)
    conn.commit()
    assert one is not None and two is not None
    assert (one.declaration_id, two.declaration_id) == (v1_report.declaration_id, v2_report.declaration_id)
    assert (one.doc["strategy_id"], one.doc["family_seq"]) == ("ranking-pot-v1", 1)
    assert (two.doc["strategy_id"], two.doc["family_seq"], two.doc["terms"]["per_look_alpha"]) == (
        "ranking-pot-v2",
        2,
        "1/160",
    )


def _refusals(conn: Conn) -> tuple[str, ...]:
    report = freeze_pot_v2(conn, provenance=PROVENANCE, apply=False)
    assert report.doc is None and report.declaration_id is None
    return report.refusals


def test_no_s_star_and_no_history_refuse(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    _seed_scores(conn, IDS)
    assert _refusals(conn) == ("dtc_unavailable", "history_floor_unavailable")


def test_a_stale_s_star_refuses_unavailable(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    _seed_scores(conn, IDS)
    _seed_history(conn)
    _seed_dtc(conn, age_days=v2.DTC_MAX_AGE_DAYS + 1)
    assert _refusals(conn) == ("dtc_unavailable",)


def test_a_fresh_s_star_with_nothing_usable_refuses_an_empty_baseline(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    _seed_scores(conn, IDS)
    _seed_history(conn)
    _seed_dtc(conn, adv=0)  # FINRA's N/A on every name
    assert _refusals(conn) == ("dtc_baseline_empty",)
