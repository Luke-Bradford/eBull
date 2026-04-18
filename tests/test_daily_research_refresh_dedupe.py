"""Tests for Chunk L — SEC filings fetch dedupe feature flag.

Flag default (False): daily_research_refresh runs SEC refresh_filings
as before. Flag=True: skips the SEC filings block entirely and logs
the reason. Companies House path unaffected either way.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from app.workers import scheduler


def _stub_conn_fetchall() -> list[tuple[str, str]]:
    """Two tradable rows so daily_research_refresh doesn't short-circuit."""
    return [("AAPL", "1"), ("MSFT", "2")]


def _stub_cik_rows() -> list[tuple[str, str]]:
    return [("AAPL", "0000320193"), ("MSFT", "0000789019")]


def _base_mocks(monkeypatch) -> tuple[MagicMock, MagicMock, MagicMock]:
    """Stub the global settings + _tracked_job + psycopg.connect chain.

    Returns the patched SEC filings provider, refresh_filings callable,
    and refresh_fundamentals callable so each test can assert on them.
    """
    tracker = MagicMock()
    tracker.row_count = None
    cm = MagicMock()
    cm.__enter__.return_value = tracker
    cm.__exit__.return_value = False
    monkeypatch.setattr(scheduler, "_tracked_job", MagicMock(return_value=cm))

    fake_conn = MagicMock()
    # Two execute() calls in daily_research_refresh return
    # (a) tradable rows (b) cik rows. Order matters.
    fake_conn.execute.return_value.fetchall.side_effect = [
        _stub_conn_fetchall(),
        _stub_cik_rows(),
    ]
    conn_cm = MagicMock()
    conn_cm.__enter__.return_value = fake_conn
    conn_cm.__exit__.return_value = False
    monkeypatch.setattr(
        scheduler.psycopg,
        "connect",
        MagicMock(return_value=conn_cm),
    )

    # Stub provider factories — they're context managers.
    sec_fund_cm = MagicMock()
    sec_fund_cm.__enter__.return_value = MagicMock()
    sec_fund_cm.__exit__.return_value = False
    monkeypatch.setattr(scheduler, "SecFundamentalsProvider", MagicMock(return_value=sec_fund_cm))

    sec_fil_cm = MagicMock()
    sec_fil_cm.__enter__.return_value = MagicMock()
    sec_fil_cm.__exit__.return_value = False
    sec_fil_cls = MagicMock(return_value=sec_fil_cm)
    monkeypatch.setattr(scheduler, "SecFilingsProvider", sec_fil_cls)

    # refresh_* helpers — canned summary mocks.
    refresh_fund_mock = MagicMock(return_value=MagicMock(symbols_attempted=2, snapshots_upserted=2, symbols_skipped=0))
    monkeypatch.setattr(scheduler, "refresh_fundamentals", refresh_fund_mock)
    refresh_filings_mock = MagicMock(
        return_value=MagicMock(instruments_attempted=2, filings_upserted=5, instruments_skipped=0)
    )
    monkeypatch.setattr(scheduler, "refresh_filings", refresh_filings_mock)

    return sec_fil_cls, refresh_filings_mock, refresh_fund_mock


def test_flag_false_calls_sec_refresh_filings(monkeypatch) -> None:
    """Default behaviour — flag off, SEC filings block runs."""
    stub_settings = MagicMock()
    stub_settings.database_url = "postgresql://test"
    stub_settings.sec_user_agent = "test"
    stub_settings.companies_house_api_key = None
    stub_settings.fmp_api_key = None
    stub_settings.enable_filings_fetch_dedupe = False
    monkeypatch.setattr(scheduler, "settings", stub_settings)

    sec_fil_cls, refresh_filings_mock, _ = _base_mocks(monkeypatch)

    scheduler.daily_research_refresh()

    # SEC filings provider constructed + refresh_filings called once.
    sec_fil_cls.assert_called_once()
    assert refresh_filings_mock.call_count == 1
    kwargs = refresh_filings_mock.call_args.kwargs
    assert kwargs["provider_name"] == "sec"


def test_flag_true_skips_sec_refresh_filings(monkeypatch) -> None:
    """Chunk L behaviour — flag on, SEC filings block skipped entirely."""
    stub_settings = MagicMock()
    stub_settings.database_url = "postgresql://test"
    stub_settings.sec_user_agent = "test"
    stub_settings.companies_house_api_key = None
    stub_settings.fmp_api_key = None
    stub_settings.enable_filings_fetch_dedupe = True
    monkeypatch.setattr(scheduler, "settings", stub_settings)

    sec_fil_cls, refresh_filings_mock, _ = _base_mocks(monkeypatch)

    scheduler.daily_research_refresh()

    # SEC filings provider NOT constructed — block skipped.
    sec_fil_cls.assert_not_called()
    # refresh_filings is only called for SEC path in the default tests
    # (CH skipped via no API key); with flag=True it must not fire.
    assert refresh_filings_mock.call_count == 0


def test_flag_true_leaves_ch_filings_path_available(monkeypatch) -> None:
    """Flag affects only the SEC block — Companies House path unaffected.
    (When COMPANIES_HOUSE_API_KEY is set, CH refresh_filings still fires.)
    """
    stub_settings = MagicMock()
    stub_settings.database_url = "postgresql://test"
    stub_settings.sec_user_agent = "test"
    stub_settings.companies_house_api_key = "ch-key"
    stub_settings.fmp_api_key = None
    stub_settings.enable_filings_fetch_dedupe = True
    monkeypatch.setattr(scheduler, "settings", stub_settings)

    sec_fil_cls, refresh_filings_mock, _ = _base_mocks(monkeypatch)

    # CH provider stub.
    ch_cm = MagicMock()
    ch_cm.__enter__.return_value = MagicMock()
    ch_cm.__exit__.return_value = False
    monkeypatch.setattr(scheduler, "CompaniesHouseFilingsProvider", MagicMock(return_value=ch_cm))

    scheduler.daily_research_refresh()

    # SEC not called; CH called once.
    sec_fil_cls.assert_not_called()
    ch_calls = [c for c in refresh_filings_mock.call_args_list if c.kwargs.get("provider_name") == "companies_house"]
    assert len(ch_calls) == 1
