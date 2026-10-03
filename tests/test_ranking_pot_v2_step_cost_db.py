"""#3592 slice 4c-ii — Appendix A (67): v1's and v2's step cost on a dev replay (spec §4 "Operational cost").

Acceptance (**by construction**, §4): on a replay of 20 consecutive sessions with populated books, v2's step p95 per
session ≤ half the step job's cadence, and a 5-session catch-up completes within one cadence. Both steps share one
lane, which serialises them (§4: "Lanes serialise jobs"), so the harness also replays v1 beside v2 and checks that the
two steps' work for any session, and a 5-session catch-up of both, fit within one cadence.

A measurement, not a regression test: it runs only with ``EBULL_MEASURE_3592=1`` (it reads the dev database and takes
~45 minutes), and prints its figures for the PR. ``EBULL_MEASURE_3592=1 uv run pytest -m db -n 0 -s
tests/test_ranking_pot_v2_step_cost_db.py``.

What is real:
- **the declarations**: each freeze's own dry run on dev (``freeze_pot`` / ``freeze_pot_v2``, ``apply=False``, rolled
  back), written into the scratch database through the freeze's own write path, so K, N, S₀, the strata and the seeds
  are the real ones (sql/445 lets the two coexist, as they will);
- **v2's §5 inputs** at each rebalance: dev's insider and FINRA reads (``read_v2_inputs``) at the target session;
- **the bars**: dev's masked bars through each step's own ``read_session_bars``, so the per-session read is in the
  figure;
- **the jobs**: ``run_step_job`` unchanged for both, every book, control order, checkpoint and step row written to
  Postgres. A session's time is its ``step_next_session`` plus the look pass after it; the job's prelude (state, first
  decision, the first look pass) is timed once and charged to every catch-up.

What is synthetic (none of it scales with K): R_t = S₀'s names with a positive score and F_t = R_t's names with a bar on
the last session (the snapshot's eligibility rules are not replayed), each name's ATR = 3% of its close, a flat 10 bp
half spread, and a one-row characteristics table. The decided snapshot's decode (``Rebalance.of``) is replaced by that
construction, so its once-per-rebalance cost is not in the figure. Two rebalances fall in the window (the initial fill
and the next month's first session), as a live month does.
"""

from __future__ import annotations

import os
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
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
from app.services import ranking_pot_freeze as fv1
from app.services import ranking_pot_freeze_v2 as fv2
from app.services import ranking_pot_rebalance as rb
from app.services import ranking_pot_sim as sim
from app.services import ranking_pot_step as st1
from app.services import ranking_pot_v2 as v2
from app.services import ranking_pot_v2_inputs as rd
from app.services import ranking_pot_v2_look as v2look
from app.services import ranking_pot_v2_step as st
from app.services.ai_trial_freeze import read_provenance
from app.services.ai_trial_pack import canonical_sha256
from app.services.market_calendar import us_market_status
from app.services.ranking_pot_v2_declaration import decode_frozen
from app.services.scoring import _DEFAULT_MODEL_VERSION
from app.workers.scheduler import JOB_RANKING_POT_STEP, JOB_RANKING_POT_V2_STEP, SCHEDULED_JOBS
from tests.test_ranking_pot_rebalance_db import _decided, _insert

pytestmark = pytest.mark.skipif(os.environ.get("EBULL_MEASURE_3592") != "1", reason="dev measurement (#3592 A67)")

Conn = psycopg.Connection[Any]
#: The replay's first target session; 20 sessions from it cross into September (a second rebalance).
T0 = date(2026, 8, 19)
SESSIONS = 20
CATCH_UP = 5
_CHEAP = ex.Characteristic("XLE", None, None, "too_few_pairs", None)


def _sessions(first: date, count: int) -> list[date]:
    out = [first]
    while len(out) < count:
        out.append(sim.next_session(out[-1]))
    return out


def _prev_session(d: date) -> date:
    """The NYSE session before ``d`` (``sim.next_session``'s calendar, run backwards)."""
    p = d - timedelta(days=1)
    while us_market_status(p) == "closed":
        p -= timedelta(days=1)
    return p


def _scores(dev: Conn, doc: Mapping[str, Any]) -> dict[int, Decimal | None]:
    rows = dev.execute(
        "SELECT instrument_id, total_score FROM scores WHERE model_version = %s AND scored_at = %s",
        (_DEFAULT_MODEL_VERSION, datetime.fromisoformat(doc["s0"]["scored_at"])),
    ).fetchall()
    dev.rollback()
    return {int(r[0]): r[1] for r in rows}


def _universes(dev: Conn, scores: Mapping[int, Decimal | None], last: date) -> tuple[pot.Universes, dict[int, Decimal]]:
    r = {iid: s for iid, s in scores.items() if s is not None and s > 0}
    rows = dev.execute(
        "SELECT instrument_id, close FROM price_daily WHERE price_date = %s AND instrument_id = ANY(%s) AND close > 0",
        (last, sorted(r)),
    ).fetchall()
    dev.rollback()
    closes = {int(row[0]): Decimal(row[1]) for row in rows}
    f = frozenset(closes)
    universes = pot.Universes(
        breakpoint=Decimal(1),
        max_cut=Fraction(1, 10),
        r_ids=frozenset(r),
        f_ids=f,
        hold_failure={iid: "score_not_positive" for iid in scores if iid not in r},
        entry_failure={iid: "quote_ineligible" for iid in r if iid not in f},
        own_score=r,
        max_return={},
        atr={iid: Fraction(closes[iid]) * Fraction(3, 100) for iid in f},
        max_population=0,
        nyse_cap_population=0,
    )
    return universes, closes


def _v1_rebalance(dev: Conn, scores: Mapping[int, Decimal | None], attempt_id: int, target: date) -> st1.Rebalance:
    last = _prev_session(target)
    universes, closes = _universes(dev, scores, last)
    return st1.Rebalance(
        attempt_id=attempt_id,
        target_session=target,
        last_session=last,
        universes=universes,
        run_scores=dict(scores),
        half_spread=dict.fromkeys(scores, Decimal("0.001")),
        snapshot_close=closes,
    )


def _v2_rebalance(
    dev: Conn, doc: Mapping[str, Any], scores: Mapping[int, Decimal | None], attempt_id: int, target: date
) -> st.Rebalance:
    """v2's real-scale rebalance: dev's §5 reads at ``target`` over the declaration's S₀ and history floor."""
    s0 = tuple(int(i) for i in doc["s0"]["instrument_ids"])
    floor = decode_frozen(doc["v2"], s0_ids=s0).history_floor
    last = _prev_session(target)
    as_of = datetime.combine(target, dtime(1), UTC)
    universes, closes = _universes(dev, scores, last)
    inputs = rd.read_v2_inputs(dev, s0_ids=s0, target_session=target, as_of=as_of)
    dev.rollback()
    dtc = v2.dtc_read(inputs.dtc, s0_ids=s0, as_of=as_of)
    insider = v2.insider_read(
        inputs.purchases, inputs.history, s0_ids=s0, target_session=target, as_of=as_of, history_floor=floor
    )
    return st.Rebalance(
        attempt_id=attempt_id,
        target_session=target,
        last_session=last,
        universes=universes,
        half_spread=dict.fromkeys(s0, Decimal("0.001")),
        snapshot_close=closes,
        dtc=dtc.values,
        buyers=insider.buyers,
        dtc_read=dtc,
        insider=insider,
        purchases=inputs.purchases,
    )


def _declare(
    conn: Conn, doc: dict[str, Any], *, strategy_id: str, builder: str, targets: list[date]
) -> dict[date, int]:
    """A dry run's document through the freeze's write path, and one decided attempt per target session."""
    decl = fv1._write(
        conn,
        doc=doc,
        doc_sha256=canonical_sha256(doc),
        declared_by="measure-3592-a67",
        strategy_id=strategy_id,
        builder=builder,
    )
    attempts: dict[date, int] = {}
    for target in targets:
        snapshot = {
            "kind": rb.SNAPSHOT_KIND,
            "strategy_id": strategy_id,
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
        row = conn.execute(
            "SELECT attempt_id FROM ranking_pot_rebalance_attempts WHERE declaration_id = %s AND target_session = %s",
            (decl, target),
        ).fetchone()
        assert row is not None
        attempts[target] = int(row[0])
    conn.commit()
    return attempts


@dataclass
class Clock:
    """Per stepped session: its step plus the look pass after it; the job's remainder is its prelude."""

    sessions: list[tuple[date, float]] = field(default_factory=list)
    pending: float = 0.0
    total: float = 0.0

    def wrap_step(self, real: Callable[..., Any]) -> Callable[..., Any]:
        def timed(*args: Any, **kwargs: Any) -> Any:
            started = time.perf_counter()
            outcome = real(*args, **kwargs)
            if outcome is not None and outcome.stepped:
                self.sessions.append((outcome.session, time.perf_counter() - started))
            return outcome

        return timed

    def wrap_looks(self, real: Callable[..., Any]) -> Callable[..., Any]:
        def timed(*args: Any, **kwargs: Any) -> Any:
            started = time.perf_counter()
            out = real(*args, **kwargs)
            spent = time.perf_counter() - started
            if self.sessions:  # the pass after a stepped session is that session's
                session, t = self.sessions[-1]
                self.sessions[-1] = (session, t + spent)
            return out

        return timed

    def run(self, job: Callable[[], Any]) -> Any:
        started = time.perf_counter()
        out = job()
        self.total = time.perf_counter() - started
        return out

    def seconds(self) -> list[float]:
        return [t for _, t in self.sessions]

    def prelude(self) -> float:
        return self.total - sum(self.seconds())


def _cadence_seconds() -> int:
    for name in (JOB_RANKING_POT_STEP, JOB_RANKING_POT_V2_STEP):
        [job] = [j for j in SCHEDULED_JOBS if j.name == name]
        assert job.cadence.kind == "hourly", job.cadence  # the figure below is an hour
    return 3600


def _p95(xs: list[float]) -> float:
    return quantiles(xs, n=20, method="inclusive")[-1]


def _worst_window(xs: list[float]) -> float:
    return max(sum(xs[i : i + CATCH_UP]) for i in range(len(xs) - CATCH_UP + 1))


def test_step_cost_on_a_dev_replay(ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch) -> None:
    conn = ebull_test_conn
    sessions = _sessions(T0, SESSIONS)
    targets = [T0, next(s for s in sessions if s.month != T0.month)]
    now = datetime.combine(sessions[-1] + timedelta(days=1), dtime(9, 30), UTC)
    v1_clock, v2_clock = Clock(), Clock()
    with psycopg.connect(settings.database_url) as dev:
        # Provenance refusals (a dirty or unmerged checkout) do not change the measured documents.
        r1 = fv1.freeze_pot(dev, provenance=read_provenance(fetch=False, spec_path=fv1.SPEC_PATH), apply=False)
        r2 = fv2.freeze_pot_v2(dev, provenance=read_provenance(fetch=False, spec_path=fv2.SPEC_PATH), apply=False)
        assert r1.doc is not None and r2.doc is not None, (r1.refusals, r2.refusals)
        # Each dry run took family_seq 1 on dev; frozen in queue order, v2 is the family's second member.
        doc1, doc2 = r1.doc, {**r2.doc, "family_seq": r1.doc["family_seq"] + 1}
        scores1, scores2 = _scores(dev, doc1), _scores(dev, doc2)
        v1_attempts = _declare(conn, doc1, strategy_id=rb.STRATEGY_ID, builder=fv1.BUILDER, targets=targets)
        _declare(conn, doc2, strategy_id=v2.STRATEGY_ID, builder=fv2.BUILDER, targets=targets)

        v1_built = {s: _v1_rebalance(dev, scores1, a, s) for s, a in v1_attempts.items()}
        monkeypatch.setattr(st1, "_decided_for", lambda _c, _d, session: v1_built.get(session))
        v2_built: dict[int, st.Rebalance] = {}

        def of(cls: type[st.Rebalance], attempt_id: int, stored: dict[str, Any], **_kw: Any) -> st.Rebalance:
            if attempt_id not in v2_built:
                target = date.fromisoformat(stored["target_session"])
                v2_built[attempt_id] = _v2_rebalance(dev, doc2, scores2, attempt_id, target)
            return v2_built[attempt_id]

        monkeypatch.setattr(st.Rebalance, "of", classmethod(of))
        for module in (st1, st):
            real_bars = module.read_session_bars

            def bars(_conn: Conn, ids: Any, session: date, _real: Any = real_bars) -> Any:
                out = _real(dev, ids, session)
                dev.rollback()
                return out

            monkeypatch.setattr(module, "read_session_bars", bars)
            monkeypatch.setattr(module, "table_for", lambda _reb, ids: dict.fromkeys(ids, _CHEAP))
        monkeypatch.setattr(st1, "step_next_session", v1_clock.wrap_step(st1.step_next_session))
        monkeypatch.setattr(st1, "_looks", v1_clock.wrap_looks(st1._looks))
        monkeypatch.setattr(st, "step_next_session", v2_clock.wrap_step(st.step_next_session))
        monkeypatch.setattr(v2look, "run_looks", v2_clock.wrap_looks(v2look.run_looks))

        conn.autocommit = True
        v1_result = v1_clock.run(lambda: st1.run_step_job(conn, now=lambda: now))
        v2_result = v2_clock.run(lambda: st.run_step_job(conn, now=lambda: now))

    cadence = _cadence_seconds()
    assert [s for s, _ in v1_clock.sessions] == [s for s, _ in v2_clock.sessions] == sessions, (
        v1_result.note[:500],
        v2_result.note[:500],
    )
    one, two = v1_clock.seconds(), v2_clock.seconds()
    both = [a + b for a, b in zip(one, two, strict=True)]
    prelude = v1_clock.prelude() + v2_clock.prelude()
    print(
        f"\n#3592 A67 — v1 K={doc1['terms']['k_controls']} books={doc1['terms']['k_controls'] + 2}, "
        f"v2 K={doc2['terms']['k_controls']} books={doc2['terms']['book_count']}, "
        f"|S0|={len(doc2['s0']['instrument_ids'])}"
    )
    print("  session       v1 (s)    v2 (s)   both (s)")
    for (session, a), b in zip(v1_clock.sessions, two, strict=True):
        print(f"  {session}  {a:8.1f}  {b:8.1f}  {a + b:8.1f}{'  rebalance' if session in targets else ''}")
    print(f"  prelude: v1 {v1_clock.prelude():.1f}s, v2 {v2_clock.prelude():.1f}s")
    print(f"  v2 p95 {_p95(two):.1f}s vs {cadence / 2:.0f}s (half the cadence)")
    v2_catch_up = _worst_window(two) + v2_clock.prelude()
    print(f"  v2 worst {CATCH_UP}-session catch-up {v2_catch_up:.1f}s vs {cadence}s (one cadence)")
    v1_catch_up = _worst_window(one) + v1_clock.prelude()
    print(f"  v1 p95 {_p95(one):.1f}s; worst {CATCH_UP}-session catch-up {v1_catch_up:.1f}s")
    print(f"  lane (v1 + v2): worst session {max(both):.1f}s, worst catch-up {_worst_window(both) + prelude:.1f}s")
    assert _p95(two) <= cadence / 2 and v2_catch_up <= cadence, (_p95(two), v2_catch_up)
    assert max(both) <= cadence and _worst_window(both) + prelude <= cadence, (max(both), _worst_window(both))
