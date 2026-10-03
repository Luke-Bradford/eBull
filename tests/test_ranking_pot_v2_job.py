"""#3592 slice 3c-ii — ranking-pot-v2's rebalance job, the pure parts (spec §4).

- The copy of v1's orchestration keeps v1's window exactly; only the fire minute differs, pinned to the scheduler.
- v2's refusal vocabulary is exactly what sql/463 admits beyond v1's.
- The snapshot is v1's document under v2's hash and strategy id plus the ``v2`` block; the per-rebalance record.
- Isolation by import: neither job imports the other.
"""

from __future__ import annotations

import ast
import re
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import get_args

import pytest

from app.services import ranking_pot_job as v1_job
from app.services import ranking_pot_rebalance as rb
from app.services import ranking_pot_v2 as v2
from app.services import ranking_pot_v2_job as job
from app.services.ai_trial_pack import canonical_sha256
from app.services.ranking_pot_v2_policy import RANKING_POT_V2_POLICY_HASH
from app.workers import scheduler
from tests.test_ranking_pot_v2 import AS_OF, K2, S2, TARGET, _dtc, _history, _inputs, _read, _row

ROOT = Path(__file__).resolve().parents[1]
THU = date(2026, 10, 1)


def test_the_window_is_v1s() -> None:
    assert (job.WINDOW_OPENS, job.BARS_WAIT_UNTIL, job.WINDOW_CLOSES) == (
        v1_job.WINDOW_OPENS,
        v1_job.BARS_WAIT_UNTIL,
        v1_job.WINDOW_CLOSES,
    )
    start = datetime.combine(THU, time(0), UTC)
    for minutes in range(0, 8 * 24 * 60, 10):  # a week and a day, every 10 minutes
        at = start + timedelta(minutes=minutes)
        assert job.window_phase(at) == v1_job.window_phase(at), at
    with pytest.raises(ValueError, match="timezone-aware"):
        job.window_phase(datetime(2026, 10, 1, 23, 50))


def test_fire_minute_is_pinned_to_the_scheduler_on_v1s_lane() -> None:
    from app.jobs.sources import source_for

    assert scheduler.RANKING_POT_V2_FIRE_MINUTE == job.FIRE_MINUTE == 50
    assert job.FIRE_MINUTE not in (v1_job.FIRE_MINUTE, scheduler.RANKING_POT_STEP_FIRE_MINUTE)
    registered = next(j for j in scheduler.SCHEDULED_JOBS if j.name == scheduler.JOB_RANKING_POT_V2_REBALANCE)
    assert (registered.source, registered.cadence.kind, registered.cadence.minute) == ("db", "hourly", 50)
    assert source_for(scheduler.JOB_RANKING_POT_V2_REBALANCE) == source_for(scheduler.JOB_RANKING_POT_REBALANCE)
    fires = [datetime.combine(THU, time(23, 50), UTC) + timedelta(hours=h) for h in range(14)]
    assert [job.window_phase(f) for f in fires].count("final") == 3  # 09:50, 10:50, 11:50


def _literals(tp: object) -> set[str]:
    args = get_args(tp)
    return {a for a in args if isinstance(a, str)} | {s for a in args if not isinstance(a, str) for s in _literals(a)}


def test_the_refusals_are_v1s_plus_sql_463s() -> None:
    sql = (ROOT / "sql" / "463_ranking_pot_v2_shadow_only.sql").read_text(encoding="utf-8")
    check = sql[sql.index("ranking_pot_rebalance_attempts_refusal_check CHECK") :]
    admitted = set(re.findall(r"'([a-z_]+)'", check[: check.index(";")]))
    assert _literals(job.V2Refusal) == admitted - {"not_attempted"}  # a skip's reason, never a fire's refusal


def _imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    out: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            out.add(node.module.rsplit(".", 1)[-1])
            if node.module == "app.services":
                out.update(a.name for a in node.names)
    return out


def test_neither_job_imports_the_other() -> None:
    services = ROOT / "app" / "services"
    assert not {"ranking_pot_job", "ranking_pot_exec", "ranking_pot_step"} & _imports(
        services / "ranking_pot_v2_job.py"
    )
    assert not {m for m in _imports(services / "ranking_pot_job.py") if m.startswith("ranking_pot_v2")}


def test_snapshot_is_v1s_document_under_v2s_identity_plus_the_block() -> None:
    inputs = rb.SnapshotInputs(
        declaration_id=1,
        declaration_sha256="d" * 64,
        policy_hash="0" * 64,  # replaced: the snapshot carries v2's hash whatever v1's reader stamped
        as_of=AS_OF,
        last_session=date(2026, 10, 2),
        target_session=TARGET,
        scored_at=AS_OF,
        facts=(),
        scores={},
        nyse_caps=(),
        spy=rb.SpyInputs(None, None, None, None),
        theses={},
    )
    block = _inputs()
    doc = job.snapshot_of(inputs, block)
    assert (doc["strategy_id"], doc["policy_hash"]) == (v2.STRATEGY_ID, RANKING_POT_V2_POLICY_HASH)
    assert v2.v2_block_of(doc) == v2.decode_v2_block(v2.encode_v2_block(block))
    assert rb.decode_snapshot(doc).target_session == TARGET  # v1's decoder still reads v1's part
    v1_doc = rb.encode_snapshot(inputs)
    assert {k: v for k, v in doc.items() if k not in ("strategy_id", "policy_hash", "v2")} == {
        k: v for k, v in v1_doc.items() if k not in ("strategy_id", "policy_hash")
    }
    assert canonical_sha256(doc) != canonical_sha256(v1_doc)


def test_factor_detail_records_the_reads_and_rs_diagnostics() -> None:
    buy = _row(txn_date=date(2026, 5, 4))  # name 7, an opportunistic pair (2023-03 / 2024-04 / 2025-05)
    ins = _read([buy], _history({2023: [3], 2024: [4], 2025: [5]}))
    dtc = v2.dtc_read([_dtc(7, S2, "4", K2), _dtc(8, S2, "999.99", K2, adv=0)], s0_ids={7, 8, 9}, as_of=AS_OF)
    detail = job.factor_detail(ins, dtc, {"form4_manifest_unparsed": 2}, r_ids={7, 8})
    assert detail == {
        "s0_buyers": 1,
        "purchases_in_window": 1,
        "excluded_null_cik": 0,
        "unclassifiable_pairs": 0,
        "unclassifiable_names": 0,
        "indeterminate_pairs": 0,
        "indeterminate_names": 0,
        "dtc_missing": {"no_row": 1, "not_available": 1},
        "reader_counts": {"form4_manifest_unparsed": 2},
        "r_buyers": 1,
        "r_dtc_coverage": 1,
        "dtc_constant": True,
        "ins_constant": False,
    }
