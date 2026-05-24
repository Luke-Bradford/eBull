"""SEC entry-point ``is_tradable`` filter audit (#1233 §6.2).

Each of the four SEC ingest entry points modified by PR1 must skip
delisted instruments (``is_tradable=FALSE``) when building the
candidate set. Delisted instruments consume bootstrap budget for no
operator value.

Covered:

1. ``app/services/sec_submissions_ingest.py::_load_cik_to_instrument``
   — bulk archive walk; CIK → instrument_id map drives every
   ``ingest_submissions_archive`` write.
2. ``app/services/sec_submissions_files_walk.py::_list_cik_secondary_pages``
   — per-CIK secondary-pages walk; same shape.
3. ``app/services/finra_short_interest_ingest.py::build_preloaded_symbol_resolver``
   — normalised-symbol resolver; delisted symbols inflate the
   collision space and force the legitimate active row into the
   ambiguous bucket.
4. ``app/services/mf_directory.py::refresh_mf_directory`` —
   class_id seeding; delisted fund classes burn scheduler budget.
"""

from __future__ import annotations

import psycopg
import pytest

from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration


def _seed_pair(
    conn: psycopg.Connection[tuple],
    *,
    tradable_id: int,
    delisted_id: int,
    cik_tradable: str,
    cik_delisted: str,
    symbol_tradable: str,
    symbol_delisted: str,
) -> None:
    """Seed two instruments — one tradable, one delisted — both with
    SEC CIK external_identifiers. Every entry-point query should
    return the tradable row only."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable)
            VALUES (%s, %s, %s, TRUE), (%s, %s, %s, FALSE)
            ON CONFLICT (instrument_id) DO UPDATE SET
                is_tradable = EXCLUDED.is_tradable,
                symbol = EXCLUDED.symbol
            """,
            (
                tradable_id,
                symbol_tradable,
                f"Test {symbol_tradable}",
                delisted_id,
                symbol_delisted,
                f"Test {symbol_delisted}",
            ),
        )
        cur.execute(
            """
            INSERT INTO external_identifiers (instrument_id, provider, identifier_type, identifier_value, is_primary)
            VALUES (%s, 'sec', 'cik', %s, TRUE), (%s, 'sec', 'cik', %s, TRUE)
            ON CONFLICT DO NOTHING
            """,
            (tradable_id, cik_tradable, delisted_id, cik_delisted),
        )


class TestSubmissionsIngestCikMapFiltersDelisted:
    def test_load_cik_to_instrument_skips_delisted(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        from app.services.sec_submissions_ingest import _load_cik_to_instrument

        _seed_pair(
            ebull_test_conn,
            tradable_id=820001,
            delisted_id=820002,
            cik_tradable="0000820001",
            cik_delisted="0000820002",
            symbol_tradable="LIVE820",
            symbol_delisted="DEAD820",
        )
        ebull_test_conn.commit()

        mapping = _load_cik_to_instrument(ebull_test_conn)

        assert "0000820001" in mapping, "tradable CIK must be present"
        assert "0000820002" not in mapping, "delisted CIK must be filtered (#1233 §6.2)"


class TestSubmissionsFilesWalkFiltersDelisted:
    def test_load_cik_universe_skips_delisted(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        from app.services.sec_submissions_files_walk import _list_cik_secondary_pages

        _seed_pair(
            ebull_test_conn,
            tradable_id=830001,
            delisted_id=830002,
            cik_tradable="0000830001",
            cik_delisted="0000830002",
            symbol_tradable="LIVE830",
            symbol_delisted="DEAD830",
        )
        ebull_test_conn.commit()

        rows = _list_cik_secondary_pages(ebull_test_conn)

        # Tuple shape updated in #1233 Stream A PR-B: now 4-tuple
        # (instrument_id, cik, symbol, sidecar_pages) — added per-CIK
        # sidecar page list for S14 to consume without re-fetching primary.
        ciks = {cik for _, cik, _, _ in rows}
        assert "0000830001" in ciks
        assert "0000830002" not in ciks, "delisted CIK must be filtered (#1233 §6.2)"


class TestFinraResolverFiltersDelisted:
    def test_preloaded_symbol_resolver_skips_delisted(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        from app.services.finra_short_interest_ingest import build_preloaded_symbol_resolver

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable)
                VALUES (%s, %s, %s, TRUE), (%s, %s, %s, FALSE)
                ON CONFLICT (instrument_id) DO UPDATE SET
                    is_tradable = EXCLUDED.is_tradable,
                    symbol = EXCLUDED.symbol
                """,
                (
                    870001,
                    "LIVE870",
                    "Test Live 870",
                    870002,
                    "DEAD870",
                    "Test Dead 870",
                ),
            )
        ebull_test_conn.commit()

        resolver = build_preloaded_symbol_resolver(ebull_test_conn)

        assert resolver("LIVE870") == 870001
        assert resolver("DEAD870") is None, "delisted symbol must not resolve via FINRA SI resolver (#1233 §6.2)"

    def test_delisted_does_not_create_ambiguous_collision(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """A delisted symbol whose normalised key collides with a
        tradable instrument's normalised key must NOT push the
        tradable into the ambiguous bucket — because the delisted row
        is filtered before normalisation."""
        from app.services.finra_short_interest_ingest import build_preloaded_symbol_resolver, normalise_symbol

        # Both normalise to "ZBRK872" so they would collide if the
        # delisted row were retained.
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable)
                VALUES (%s, 'ZBRK872.A', 'Live class A', TRUE),
                       (%s, 'ZBRK872A',  'Delisted plain', FALSE)
                ON CONFLICT (instrument_id) DO UPDATE SET
                    is_tradable = EXCLUDED.is_tradable,
                    symbol = EXCLUDED.symbol
                """,
                (872001, 872002),
            )
        ebull_test_conn.commit()

        # Confirm the two normalise to the same key (so the test is
        # meaningful — if normalise_symbol changes, this guard wakes).
        assert normalise_symbol("ZBRK872.A") == normalise_symbol("ZBRK872A")

        resolver = build_preloaded_symbol_resolver(ebull_test_conn)

        # The tradable row resolves cleanly; the delisted one is
        # filtered out before it can collide.
        assert resolver("ZBRK872.A") == 872001


class TestMfDirectoryFiltersDelisted:
    def test_refresh_mf_directory_skips_delisted_fund_class(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """``refresh_mf_directory`` resolves each fund-class symbol →
        ``instrument_id`` via ``instruments`` and seeds an
        ``external_identifiers (provider='sec', identifier_type='class_id')``
        row only when the instrument is tradable. A delisted fund
        class with the same symbol must NOT receive a class_id
        external_identifier row.

        This exercises the production ``refresh_mf_directory`` body
        (Codex 1a #5 hardening — the prior SQL-only test pinned
        only the SELECT shape, not the writer's behaviour)."""
        import json

        from app.services import mf_directory as mf_directory_module

        # Seed two universe instruments sharing different symbols —
        # one tradable, one delisted.
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable)
                VALUES (%s, 'LIVEFUND880', 'Live fund', TRUE),
                       (%s, 'DEADFUND880', 'Delisted fund', FALSE)
                ON CONFLICT (instrument_id) DO UPDATE SET
                    is_tradable = EXCLUDED.is_tradable,
                    symbol = EXCLUDED.symbol
                """,
                (880001, 880002),
            )
        ebull_test_conn.commit()

        # Stub ``_fetch_directory`` so the test doesn't hit
        # data.sec.gov. Payload shape matches the real
        # ``company_tickers_mf.json`` (fields + data rows).
        payload = {
            "fields": ["cik", "seriesId", "classId", "symbol"],
            "data": [
                # Tradable row — should seed class_id.
                [1234567, "S000000001", "C000000001", "LIVEFUND880"],
                # Delisted row — must NOT seed class_id (#1233 §6.2).
                [1234568, "S000000002", "C000000002", "DEADFUND880"],
            ],
        }

        def fake_fetch(provider: object) -> dict[str, object]:
            return json.loads(json.dumps(payload))

        monkeypatch.setattr(mf_directory_module, "_fetch_directory", fake_fetch)

        # ``provider`` is unused (fake_fetch ignores it); pass a
        # sentinel so the function doesn't try to open the real one.
        class _NoOpProvider:
            def __enter__(self) -> _NoOpProvider:
                return self

            def __exit__(self, *args: object) -> None:
                pass

        result = mf_directory_module.refresh_mf_directory(
            ebull_test_conn,
            provider=_NoOpProvider(),  # type: ignore[arg-type]
        )

        # Both directory rows land in ``cik_refresh_mf_directory``
        # (that table is the SEC mirror, no is_tradable filter).
        assert result["directory_rows"] == 2

        # Only the tradable row seeds a class_id external_identifier.
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                SELECT instrument_id, identifier_value
                FROM external_identifiers
                WHERE provider = 'sec'
                  AND identifier_type = 'class_id'
                  AND identifier_value IN ('C000000001', 'C000000002')
                """
            )
            ext_rows = cur.fetchall()
        ebull_test_conn.commit()

        assert ext_rows == [(880001, "C000000001")], (
            f"Expected only the tradable instrument's class_id to seed; got {ext_rows!r}. "
            "#1233 §6.2 — delisted fund classes must not seed class_id."
        )
