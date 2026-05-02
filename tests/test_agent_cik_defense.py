"""Tests for #752 agent-CIK defense-in-depth.

Pins two contracts:
  1. ``fetch_filing_index`` legacy fallback REJECTS known-agent
     CIK prefixes — returns ``None`` with a clear warning rather than
     producing a guaranteed-404 round trip against SEC.
  2. ``audit_agent_cik_contamination`` reports every external_identifier
     row whose CIK matches the block-list, splitting on ``is_primary``.

The original ticket #752 hypothesised that ``external_identifiers`` was
contaminated. Live diagnostic on 2026-05-02 returned zero contaminated
rows — root cause was a stale long-running worker. Tests here cover the
defense-in-depth path so a future regression that leaks agent CIKs into
the table OR that drops issuer_cik from a fetch_filing_index call site
fails fast instead of silently 404'ing.
"""

from __future__ import annotations

import logging

import psycopg
import pytest

from app.providers.implementations.sec_edgar import (
    KNOWN_FILING_AGENT_CIKS,
    SecFilingsProvider,
)
from scripts.audit_agent_cik_contamination import find_contaminated


class _FakeResponse:
    def __init__(self, status_code: int, body: dict[str, object] | None = None) -> None:
        self.status_code = status_code
        self._body = body or {}

    def json(self) -> dict[str, object]:
        return self._body

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class _RecordingClient:
    """Captures URLs the legacy fallback would otherwise send to SEC."""

    def __init__(self, status: int = 404) -> None:
        self.urls: list[str] = []
        self.status = status

    def get(self, url: str) -> _FakeResponse:
        self.urls.append(url)
        return _FakeResponse(self.status)


@pytest.fixture
def provider_with_recording_clients() -> tuple[SecFilingsProvider, _RecordingClient]:
    provider = SecFilingsProvider(user_agent="test")
    recorder = _RecordingClient(status=404)
    provider._http_tickers = recorder  # type: ignore[assignment]
    return provider, recorder


class TestLegacyFallbackRejectsAgentCiks:
    def test_agent_prefix_returns_none_without_http_call(
        self,
        provider_with_recording_clients: tuple[SecFilingsProvider, _RecordingClient],
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        provider, recorder = provider_with_recording_clients
        # Accession with EdgarOnline (1213900) prefix — known agent.
        with caplog.at_level(logging.WARNING):
            result = provider.fetch_filing_index("0001213900-26-048837")

        assert result is None
        # Must NOT have hit SEC — the URL would be guaranteed 404
        # and burn rate-limit budget.
        assert recorder.urls == []
        # Operator-facing warning must surface so a stale-process
        # diagnosis is one log grep away.
        assert any("0001213900" in r.message for r in caplog.records)
        assert any("issuer_cik" in r.message for r in caplog.records)

    def test_globenewswire_prefix_also_rejected(
        self,
        provider_with_recording_clients: tuple[SecFilingsProvider, _RecordingClient],
    ) -> None:
        provider, recorder = provider_with_recording_clients
        # GlobeNewswire (1493152).
        result = provider.fetch_filing_index("0001493152-26-019605")
        assert result is None
        assert recorder.urls == []

    def test_non_agent_prefix_falls_through_to_legacy_fetch(
        self,
        provider_with_recording_clients: tuple[SecFilingsProvider, _RecordingClient],
    ) -> None:
        # AAPL self-files as 0000320193 — not an agent. Legacy
        # fallback proceeds (the URL might still 404 in the real
        # world but the block-list doesn't trip).
        provider, recorder = provider_with_recording_clients
        provider.fetch_filing_index("0000320193-24-000001")
        assert len(recorder.urls) == 1
        assert "320193" in recorder.urls[0]

    def test_explicit_issuer_cik_bypasses_block_list(
        self,
        provider_with_recording_clients: tuple[SecFilingsProvider, _RecordingClient],
    ) -> None:
        # When the caller passes issuer_cik explicitly, the block-list
        # branch is not reached — even if the accession's agent-prefix
        # would otherwise trigger it. This is the production happy path.
        provider, recorder = provider_with_recording_clients
        recorder.status = 404  # SEC returns 404 — caller handles None.
        result = provider.fetch_filing_index("0001493152-26-019605", issuer_cik="0002032379")
        assert result is None
        # URL was still attempted (under the issuer's CIK).
        assert len(recorder.urls) == 1
        assert "/2032379/" in recorder.urls[0]

    def test_known_agent_set_includes_documented_filers(self) -> None:
        # Pin the block-list contents so a future PR that adds /
        # removes an agent has to update the set explicitly.
        for cik in (
            "0001213900",  # EdgarOnline
            "0001493152",  # GlobeNewswire
            "0001193125",  # Donnelley R.R. & Sons
            "0001437749",  # Edgar Agents LLC
        ):
            assert cik in KNOWN_FILING_AGENT_CIKS


# ---------------------------------------------------------------------------
# Audit script: ``find_contaminated``
# ---------------------------------------------------------------------------


def _seed(
    conn: psycopg.Connection[tuple],
    instrument_id: int,
    symbol: str,
    cik: str,
    *,
    is_primary: bool,
) -> None:
    conn.execute(
        """
        INSERT INTO exchanges (exchange_id, description, country, asset_class)
        VALUES (%s, %s, 'US', 'us_equity')
        ON CONFLICT (exchange_id) DO NOTHING
        """,
        (f"acd_{instrument_id}", f"Test {instrument_id}"),
    )
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (instrument_id, symbol, f"Test {symbol}", f"acd_{instrument_id}"),
    )
    conn.execute(
        """
        INSERT INTO external_identifiers
            (instrument_id, provider, identifier_type, identifier_value, is_primary)
        VALUES (%s, 'sec', 'cik', %s, %s)
        ON CONFLICT (provider, identifier_type, identifier_value) DO NOTHING
        """,
        (instrument_id, cik, is_primary),
    )


def test_audit_finds_primary_agent_cik_contamination(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    # Contaminated: GlobeNewswire CIK as primary identifier on a
    # real-looking instrument. The whole point of the audit.
    _seed(ebull_test_conn, 991_001, "ACD_BAD", "0001493152", is_primary=True)
    # Healthy: real issuer CIK on a real instrument.
    _seed(ebull_test_conn, 991_002, "ACD_OK", "0000320193", is_primary=True)

    rows = find_contaminated(ebull_test_conn, include_secondary=False)
    contaminated_ids = {r[0] for r in rows}
    assert 991_001 in contaminated_ids
    assert 991_002 not in contaminated_ids


def test_audit_excludes_secondary_by_default(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    # Secondary (is_primary=FALSE) agent CIK doesn't drive routing —
    # excluded from default report so operators don't get false
    # alarms on benign historical CIK aliases.
    _seed(ebull_test_conn, 992_001, "ACD_SEC", "0001493152", is_primary=False)
    rows = find_contaminated(ebull_test_conn, include_secondary=False)
    assert 992_001 not in {r[0] for r in rows}

    rows_all = find_contaminated(ebull_test_conn, include_secondary=True)
    assert 992_001 in {r[0] for r in rows_all}
