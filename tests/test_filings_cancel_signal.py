"""Tests for the bootstrap cancel-signal adoption inside ``refresh_filings``.

Issue #1064 PR3d follow-up. The bootstrap stage 14 invoker
``filings_history_seed`` walks the full CIK-mapped tradable cohort
through ``refresh_filings``; without a poll the operator's cancel
signal isn't observed until the per-instrument loop drains. Polling
between instruments cuts cancel-observation latency to ~7s.

Outside a bootstrap dispatch the contextvar is unset → the poll
short-circuits to False, so the daily research refresh + every other
non-bootstrap caller of ``refresh_filings`` is unaffected.

Lives in its own file (rather than alongside ``test_filings_bulk_resolve``)
so it can use the worker-template ``ebull_test_conn`` fixture and run
locally without depending on a static ``ebull_test`` DB existing.
"""

from __future__ import annotations

from datetime import date

import psycopg
import pytest

from app.providers.filings import FilingSearchResult, FilingsProvider
from app.services.filings import refresh_filings
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export


class _StubFilingsProvider(FilingsProvider):
    """Stub matching test_filings_bulk_resolve._StubFilingsProvider."""

    def __init__(self, results_by_cik: dict[str, list[FilingSearchResult]]) -> None:
        self._results = results_by_cik
        self.calls: list[tuple[str, str]] = []

    def list_filings_by_identifier(  # type: ignore[override]
        self,
        *,
        identifier_type: str,
        identifier_value: str,
        start_date: date | None = None,
        end_date: date | None = None,
        filing_types: list[str] | None = None,
    ) -> list[FilingSearchResult]:
        self.calls.append((identifier_type, identifier_value))
        return self._results.get(identifier_value, [])

    def get_filing(self, *args, **kwargs):  # type: ignore[override]
        raise NotImplementedError

    def build_cik_mapping(self):  # type: ignore[override]
        raise NotImplementedError


def _seed_instrument_with_cik(
    conn: psycopg.Connection,  # type: ignore[type-arg]
    instrument_id: int,
    symbol: str,
    cik: str,
) -> None:
    conn.execute(
        """
        INSERT INTO exchanges (exchange_id, description, country, asset_class)
        VALUES (%s, %s, 'US', 'us_equity')
        ON CONFLICT (exchange_id) DO NOTHING
        """,
        (f"test_cancel_{instrument_id}", f"Test exchange {instrument_id}"),
    )
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (instrument_id, symbol, f"Test {symbol}", f"test_cancel_{instrument_id}"),
    )
    conn.execute(
        """
        INSERT INTO external_identifiers
            (instrument_id, provider, identifier_type, identifier_value, is_primary)
        VALUES (%s, 'sec', 'cik', %s, TRUE)
        ON CONFLICT (provider, identifier_type, identifier_value, instrument_id)
            WHERE provider = 'sec' AND identifier_type = 'cik'
        DO NOTHING
        """,
        (instrument_id, cik),
    )


def test_refresh_filings_observes_bootstrap_cancel_signal(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When invoked under ``active_bootstrap_run``, ``refresh_filings``
    polls the cancel signal between instruments and raises
    ``BootstrapStageCancelled`` on observed cancel. The provider is
    never contacted on a cancel-on-iter-0 path.
    """
    from app.config import settings as app_settings
    from app.services.bootstrap_state import (
        BootstrapStageCancelled,
        StageSpec,
        cancel_run,
        start_run,
    )
    from app.services.processes.bootstrap_cancel_signal import (
        active_bootstrap_run,
    )
    from tests.fixtures.ebull_test_db import test_database_url

    monkeypatch.setattr(app_settings, "database_url", test_database_url())
    conn = ebull_test_conn
    conn.execute(
        """
        UPDATE bootstrap_state
           SET status='pending', last_run_id=NULL, last_completed_at=NULL
         WHERE id=1
        """
    )
    run_id = start_run(
        conn,
        operator_id=None,
        stage_specs=(
            StageSpec(
                stage_key="filings_history_seed",
                stage_order=1,
                lane="sec_rate",
                job_name="filings_history_seed",
            ),
        ),
    )
    cancel_run(conn, requested_by_operator_id=None)
    conn.commit()

    _seed_instrument_with_cik(conn, 1064201, "CANA", "0001064201")
    _seed_instrument_with_cik(conn, 1064202, "CANB", "0001064202")
    conn.commit()

    provider = _StubFilingsProvider({"0001064201": [], "0001064202": []})

    with active_bootstrap_run(run_id, "filings_history_seed"):
        with pytest.raises(BootstrapStageCancelled) as exc_info:
            refresh_filings(
                provider=provider,  # type: ignore[arg-type]
                provider_name="sec",
                identifier_type="cik",
                conn=conn,
                instrument_ids=["1064201", "1064202"],
            )

    assert "cancelled by operator" in str(exc_info.value)
    # #1114: stage_key on the exception is read from the contextvar,
    # not hardcoded inside refresh_filings.
    assert exc_info.value.stage_key == "filings_history_seed"
    # Cancel observed on iteration 0 — provider untouched.
    assert provider.calls == []


def test_refresh_filings_unaffected_outside_bootstrap_dispatch(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Without ``active_bootstrap_run`` the contextvar is unset and the
    helper short-circuits to False — the scheduled / manual-trigger
    callers of ``refresh_filings`` see no behaviour change. Provider is
    contacted; the call returns a normal summary.
    """
    conn = ebull_test_conn
    _seed_instrument_with_cik(conn, 1064210, "OUTA", "0001064210")
    conn.commit()

    provider = _StubFilingsProvider({"0001064210": []})

    summary = refresh_filings(
        provider=provider,  # type: ignore[arg-type]
        provider_name="sec",
        identifier_type="cik",
        conn=conn,
        instrument_ids=["1064210"],
    )

    # Provider WAS contacted; no exception.
    assert provider.calls == [("cik", "0001064210")]
    assert summary.instruments_attempted == 1
    assert summary.instruments_skipped == 0
