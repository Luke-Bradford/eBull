"""#2842 slice 6c-i — the readout's SQL path: one REPEATABLE READ READ ONLY transaction, the step rows streamed
through a server-side cursor, the decided snapshots and SPY's masked bars. ``tests/test_ranking_pot_readout.py``
covers the arithmetic; bars are synthetic (``tests/test_ranking_pot_look_db._stepped_pot``)."""

from __future__ import annotations

from datetime import UTC, date, datetime
from types import SimpleNamespace
from typing import Any

import psycopg
import pytest

from app.services import ranking_pot_readout as ro
from app.services import ranking_pot_rebalance as rb
from app.services import ranking_pot_step as st
from tests.test_ranking_pot_look_db import _stepped_pot
from tests.test_ranking_pot_step import _universes
from tests.test_ranking_pot_step_db import _at

Conn = psycopg.Connection[Any]


def test_the_readout_reads_the_stored_rows_in_one_read_only_transaction(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    decl_id, t0, e12, _ = _stepped_pot(conn, monkeypatch)
    r = {2842: "0.9", 2843: "0.8", 2844: "0.7"}
    monkeypatch.setattr(
        rb,
        "decode_snapshot",
        lambda doc: SimpleNamespace(
            target_session=date.fromisoformat(doc["target_session"]),
            last_session=date.fromisoformat(doc["last_session"]),
            as_of=datetime(2026, 1, 1, tzinfo=UTC),
            theses={},
        ),
    )
    monkeypatch.setattr(rb, "universes_of", lambda _inputs: _universes(r, set(r)))
    assert st.run_step_job(conn, now=lambda: _at(t0)).stepped == 1
    assert st.run_step_job(conn, now=lambda: _at(e12)).stepped == 1
    decl = rb.load_declaration(conn)
    assert decl is not None and decl.declaration_id == decl_id

    out = ro.readout(conn, decl, e12)
    assert conn.info.transaction_status == psycopg.pq.TransactionStatus.IDLE
    assert (out["t0"], out["endpoint"], out["sessions"]) == (t0.isoformat(), e12.isoformat(), 2)
    assert out["lifecycles"]["count"] == 2  # N = 2 entered at T₀, both open at E
    per = out["turnover_occupancy"]["per_rebalance"]
    assert len(per) == 1 and per[0]["missing_donor_share"]["r_size"] == 3
    # The test database has no SPY bars, so the regime is unavailable rather than guessed.
    assert per[0]["regime"]["label"] == "unavailable" and set(out["regime_cohorts"]) == {"unavailable"}
    assert out["exits"]["flag"] is False and "policy_drift" in out
    with pytest.raises(ValueError, match="no stepped window"):
        ro.readout(conn, decl, date(2000, 1, 3))
    with conn.transaction(), pytest.raises(RuntimeError, match="own transaction"):
        ro.readout(conn, decl, e12)
