"""#3592 slice 4c-ii — Appendix A (67): v2's K + 3-book step cost on a dev replay (spec §4 "Operational cost").

Acceptance (**by construction**, §4): on a replay of 20 consecutive sessions with populated books, the step's p95 per
session ≤ half the step job's cadence, and a 5-session catch-up completes within one cadence.

A measurement, not a regression test: it runs only with ``EBULL_MEASURE_3592=1`` (it reads the dev database and takes
minutes), and prints its figures for the PR. ``EBULL_MEASURE_3592=1 uv run pytest -m db -n 0 -s
tests/test_ranking_pot_v2_step_cost_db.py``.

What is real:
- **the declaration**: the v2 freeze's own dry run on dev (``freeze_pot_v2(apply=False)``, rolled back), written into
  the scratch database through the freeze's own write path, so K, N, S₀, the strata and the seeds are the real ones;
- **the §5 inputs** at each rebalance: dev's insider and FINRA reads (``read_v2_inputs``) at the target session;
- **the bars**: dev's masked bars through the step's own ``read_session_bars``, so the per-session read cost is in the
  figure;
- **the step**: ``run_step_job`` unchanged, every book, control order, checkpoint and step row written to Postgres.

What is synthetic (none of it scales with K): R_t = S₀'s names with a positive frozen score and F_t = R_t's names with a
bar on the last session (the snapshot's eligibility rules are not replayed), each name's ATR = 3% of its close, a flat
10 bp half spread, and a one-row characteristics table. Two rebalances fall in the window (the initial fill and the
next month's first session), as a live month does.
"""

from __future__ import annotations

import os
import time
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from datetime import time as dtime
from decimal import Decimal
from fractions import Fraction
from statistics import quantiles
from typing import Any

import psycopg
import pytest
from psycopg.types.json import Jsonb

from app.config import settings
from app.services import ranking_pot as pot
from app.services import ranking_pot_exposure as ex
from app.services import ranking_pot_freeze_v2 as fv2
from app.services import ranking_pot_rebalance as rb
from app.services import ranking_pot_sim as sim
from app.services import ranking_pot_v2 as v2
from app.services import ranking_pot_v2_inputs as rd
from app.services import ranking_pot_v2_step as st
from app.services.ai_trial_freeze import read_provenance
from app.services.ai_trial_pack import canonical_sha256
from app.services.ranking_pot_freeze import _write
from app.services.ranking_pot_v2_declaration import decode_frozen
from app.workers.scheduler import JOB_RANKING_POT_V2_STEP, SCHEDULED_JOBS
from tests.test_ranking_pot_rebalance_db import _decided, _insert

pytestmark = pytest.mark.skipif(os.environ.get("EBULL_MEASURE_3592") != "1", reason="dev measurement (#3592 A67)")

Conn = psycopg.Connection[Any]
#: The replay's first target session; 20 sessions from it cross into September (a second rebalance).
T0 = date(2026, 8, 19)
SESSIONS = 20
CATCH_UP = 5


def _sessions(first: date, count: int) -> list[date]:
    out = [first]
    while len(out) < count:
        out.append(sim.next_session(out[-1]))
    return out


def _prev_session(d: date) -> date:
    p = d - timedelta(days=1)
    while sim.next_session(p) != d:
        p -= timedelta(days=1)
    return p


def _closes(dev: Conn, ids: list[int], session: date) -> dict[int, Decimal]:
    rows = dev.execute(
        "SELECT instrument_id, close FROM price_daily WHERE price_date = %s AND instrument_id = ANY(%s) AND close > 0",
        (session, ids),
    ).fetchall()
    return {int(r[0]): Decimal(r[1]) for r in rows}


def _rebalance(dev: Conn, decl_doc: dict[str, Any], attempt_id: int, target: date) -> st.Rebalance:
    """A real-scale decided rebalance: S₀'s frozen scores, dev's §5 reads at ``target`` and its last-session closes."""
    s0 = tuple(int(i) for i in decl_doc["s0"]["instrument_ids"])
    frozen = decode_frozen(decl_doc["v2"], s0_ids=s0)
    last = _prev_session(target)
    as_of = datetime.combine(target, dtime(1), UTC)
    r = {iid: s for iid, s in frozen.scores.items() if s is not None and s > 0}
    closes = _closes(dev, sorted(r), last)
    f = frozenset(closes)
    universes = pot.Universes(
        breakpoint=Decimal(1),
        max_cut=Fraction(1, 10),
        r_ids=frozenset(r),
        f_ids=f,
        hold_failure={iid: "score_not_positive" for iid in s0 if iid not in r},
        entry_failure={iid: "quote_ineligible" for iid in r if iid not in f},
        own_score=r,
        max_return={},
        atr={iid: Fraction(closes[iid]) * Fraction(3, 100) for iid in f},
        max_population=0,
        nyse_cap_population=0,
    )
    inputs = rd.read_v2_inputs(dev, s0_ids=s0, target_session=target, as_of=as_of)
    dev.rollback()
    dtc = v2.dtc_read(inputs.dtc, s0_ids=s0, as_of=as_of)
    insider = v2.insider_read(
        inputs.purchases,
        inputs.history,
        s0_ids=s0,
        target_session=target,
        as_of=as_of,
        history_floor=frozen.history_floor,
    )
    return st.Rebalance(
        attempt_id=attempt_id,
        target_session=target,
        last_session=last,
        universes=universes,
        half_spread={iid: Decimal("0.001") for iid in s0},
        snapshot_close=closes,
        dtc=dtc.values,
        buyers=insider.buyers,
        dtc_read=dtc,
        insider=insider,
        purchases=inputs.purchases,
    )


def _cadence_seconds() -> int:
    [job] = [j for j in SCHEDULED_JOBS if j.name == JOB_RANKING_POT_V2_STEP]
    assert job.cadence.kind == "hourly", job.cadence  # the figure below is an hour
    return 3600


def test_v2_step_cost_on_a_dev_replay(ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch) -> None:
    conn = ebull_test_conn
    with psycopg.connect(settings.database_url) as dev:
        report = fv2.freeze_pot_v2(dev, provenance=read_provenance(fetch=False, spec_path=fv2.SPEC_PATH), apply=False)
        # Provenance refusals (a dirty or unmerged checkout) do not change the measured document.
        assert report.doc is not None and report.doc_sha256 is not None, report.refusals
        doc = report.doc
        decl = _write(
            conn,
            doc=doc,
            doc_sha256=report.doc_sha256,
            declared_by="measure-3592-a67",
            strategy_id=v2.STRATEGY_ID,
            builder=fv2.BUILDER,
        )
        sessions = _sessions(T0, SESSIONS)
        targets = [T0, next(s for s in sessions if s.month != T0.month)]
        for target in targets:
            snapshot = {
                "kind": rb.SNAPSHOT_KIND,
                "strategy_id": v2.STRATEGY_ID,
                "target_session": target.isoformat(),
                "last_session": _prev_session(target).isoformat(),
                "spy": {"bid": "499.9", "ask": "500.1"},
            }
            _insert(
                conn,
                decl,
                target_session=target,
                month=target.replace(day=1),
                **_decided(snapshot=Jsonb(snapshot), snapshot_sha256=canonical_sha256(snapshot)),
            )
        conn.commit()

        built: dict[int, st.Rebalance] = {}

        def of(cls: type[st.Rebalance], attempt_id: int, stored: dict[str, Any], **_kw: Any) -> st.Rebalance:
            if attempt_id not in built:
                built[attempt_id] = _rebalance(dev, doc, attempt_id, date.fromisoformat(stored["target_session"]))
            return built[attempt_id]

        monkeypatch.setattr(st.Rebalance, "of", classmethod(of))
        real_bars = st.read_session_bars

        def bars(_conn: Conn, ids: Any, session: date) -> st.SessionBars:
            out = real_bars(dev, ids, session)
            dev.rollback()
            return out

        monkeypatch.setattr(st, "read_session_bars", bars)
        cheap = ex.Characteristic("XLE", None, None, "too_few_pairs", None)
        monkeypatch.setattr(st, "table_for", lambda _reb, ids: dict.fromkeys(ids, cheap))

        timings: list[tuple[date, float, str]] = []
        real_step: Callable[..., Any] = st.step_next_session

        def timed(*args: Any, **kwargs: Any) -> Any:
            started = time.perf_counter()
            outcome = real_step(*args, **kwargs)
            if outcome is not None and outcome.stepped:
                timings.append((outcome.session, time.perf_counter() - started, outcome.note))
            return outcome

        monkeypatch.setattr(st, "step_next_session", timed)
        conn.autocommit = True
        last = sessions[-1]
        result = st.run_step_job(conn, now=lambda: datetime.combine(last + timedelta(days=1), dtime(9, 30), UTC))

    cadence = _cadence_seconds()
    seconds = [t for _, t, _ in timings]
    p95 = quantiles(seconds, n=20, method="inclusive")[-1]
    catch_up = max(sum(seconds[i : i + CATCH_UP]) for i in range(len(seconds) - CATCH_UP + 1))
    k = int(doc["terms"]["k_controls"])
    print(f"\n#3592 A67 — K={k}, books={k + 3}, |S0|={len(doc['s0']['instrument_ids'])}, sessions={len(timings)}")
    for session, t, note in timings:
        print(f"  {session}  {t:8.1f}s  {note[:100]}")
    print(f"  p95 {p95:.1f}s vs {cadence / 2:.0f}s (half the cadence)")
    print(f"  worst {CATCH_UP}-session catch-up {catch_up:.1f}s vs {cadence}s (one cadence)")
    print(f"  job note: {result.note[:300]}")
    assert len(timings) == SESSIONS, [s for s, _, _ in timings]
    assert p95 <= cadence / 2 and catch_up <= cadence, (p95, catch_up)
