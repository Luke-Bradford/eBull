"""Shared test fixtures for copy-trading ingestion (spec §8.0).

This module owns the canonical `_NOW` constant used by every test
that exercises the mirror sync soft-close path. The value is
pinned to a frozen UTC timestamp so that `_sync_mirrors`'s
`UPDATE ... closed_at = %(now)s` clause produces a deterministic
stored value and tests can assert the exact round-trip.

It also owns `_GUARD_INSTRUMENT_ID` and `_GUARD_INSTRUMENT_SECTOR`
— the deterministic instrument-row identifiers used by the
guard-path fixtures delivered in Track 1b (#187). They are
declared here in Track 1a so all callers import them from one
place once Track 1b lands.

Track 1a ships the constants and the parser/sync fixture
builders (`two_mirror_payload`, `parse_failure_payload`,
`two_mirror_seed_rows`). Track 1b adds `mirror_aum_fixture`,
`no_quote_mirror_fixture`, `mtm_delta_mirror_fixture` on top.
"""

from __future__ import annotations

from datetime import UTC, datetime

# Frozen "now" for every sync-side test. Matches the value
# tests/test_portfolio_sync.py used locally before this refactor
# (bit-identical — no behaviour change).
_NOW: datetime = datetime(2026, 4, 10, 5, 30, tzinfo=UTC)

# Guard test instrument — chosen well above any seed data in
# sql/001_init.sql so it cannot collide with real instruments.
# Track 1b's guard-integration test fixtures reuse it.
_GUARD_INSTRUMENT_ID: int = 990001
_GUARD_INSTRUMENT_SECTOR: str = "technology"
