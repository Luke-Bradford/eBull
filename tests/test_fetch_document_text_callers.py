"""Writer-discipline regression guard for ``fetch_document_text`` (#453).

The operator directive from #448 forbids disk-only persistence of
upstream body text: every caller of
``SecFilingsProvider.fetch_document_text`` MUST route the body
through a service-layer ingester that normalises every structured
field into SQL. This test pins the allowed-caller set so a future
commit that adds a new caller (e.g. to write to ``data/raw/*``
without a matching SQL normalisation path) fails loudly instead of
silently regressing the contract.

Pattern copied from ``tests/test_raw_persistence.py`` sentinel tests
— the prevention log entry "Empty-parametrize silent pass" applies:
we assert ``>= MIN`` expected hits so a mis-compiled pattern that
returns zero matches still fails the file rather than passing a
green no-op.
"""

from __future__ import annotations

import re
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]


# Every file in the repo that legitimately calls ``fetch_document_text``
# on an SEC provider. New entries require a matching service-layer
# ingester that normalises into SQL — see docs/review-prevention-log.md
# "Every structured field from an upstream document lands in SQL".
_ALLOWED_CALLER_FILES: frozenset[str] = frozenset(
    {
        # Production callers. Each one normalises its domain into SQL:
        #   business_summary    — 10-K Item 1 blob + sections (#428 / #449)
        #   dividend_calendar   — 8-K Item 8.01 (#434)
        #   insider_transactions — Form 4 XML (#429)
        #   eight_k_events      — 8-K full structure (#450)
        #   institutional_holdings — 13F-HR primary_doc + infotable XML (#730)
        #   insider_form3_ingest    — Form 3 initial-holdings XML (#768)
        #   blockholders        — Schedule 13D / 13G primary_doc XML (#766)
        #   def14a_ingest       — DEF 14A beneficial-ownership table HTML (#769)
        #   ncen_classifier     — N-CEN annual fund-census XML (#782)
        #   n_port_ingest       — NPORT-P / NPORT-P/A primary XML →
        #                         ``ownership_funds_observations`` via
        #                         ``record_fund_observation`` (#917)
        "app/services/business_summary.py",
        "app/services/dividend_calendar.py",
        "app/services/insider_transactions.py",
        "app/services/insider_form3_ingest.py",
        "app/services/eight_k_events.py",
        "app/services/institutional_holdings.py",
        "app/services/blockholders.py",
        "app/services/def14a_ingest.py",
        "app/services/ncen_classifier.py",
        "app/services/n_port_ingest.py",
        # Manifest-worker adapters (#1126 / #1128 / #1129 / #1130 /
        # #1133 / #1134 / #1151). Each one wraps a legacy service-layer
        # ingester whose SQL normalisation already lives on this allow-
        # list — the adapter is a thin per-accession driver, not a new
        # disk-only persistence path.
        "app/services/manifest_parsers/def14a.py",
        "app/services/manifest_parsers/eight_k.py",
        "app/services/manifest_parsers/insider_345.py",
        "app/services/manifest_parsers/sec_10k.py",
        "app/services/manifest_parsers/sec_13dg.py",
        "app/services/manifest_parsers/sec_13f_hr.py",
        "app/services/manifest_parsers/sec_n_port.py",
        # Bounded pipelined fetcher (#1045) — concurrent transport
        # wrapper used by ``ingest_business_summaries`` to prefetch
        # primary docs. Doesn't persist anything itself; the wrapped
        # caller (business_summary) owns the SQL normalisation.
        "app/services/sec_pipelined_fetcher.py",
        # Provider implementation owns the method itself.
        "app/providers/implementations/sec_edgar.py",
        # Bounded-concurrency wrapper (#726). Calls the method via a
        # narrow Protocol but does NOT persist anything itself —
        # callers (insider_transactions / dividend_calendar / etc.)
        # own the SQL normalisation as before. The helper is
        # transport-only.
        "app/providers/concurrent_fetch.py",
        # Tests that exercise the ingesters use stub _DocFetcher classes
        # that shadow the method name — these are test-only and don't
        # persist to disk.
        "tests/test_business_summary_ingest.py",
        "tests/test_dividend_calendar_ingest.py",
        "tests/test_insider_transactions_ingest.py",
        "tests/test_insider_form3_ingest.py",
        "tests/test_eight_k_events_ingest.py",
        "tests/test_concurrent_fetch.py",
        "tests/test_institutional_holdings_ingester.py",
        "tests/test_blockholders_ingester.py",
        "tests/test_def14a_ingest.py",
        "tests/test_ncen_classifier.py",
        "tests/test_n_port_ingest.py",
        # Manifest-worker adapter tests — exercise the adapters above
        # and naturally reference fetch_document_text via monkeypatch.
        "tests/test_manifest_parser_def14a.py",
        "tests/test_manifest_parser_eight_k.py",
        "tests/test_manifest_parser_insider_345.py",
        "tests/test_manifest_parser_sec_10k.py",
        "tests/test_manifest_parser_sec_13dg.py",
        "tests/test_manifest_parser_sec_13f_hr.py",
        "tests/test_manifest_parser_sec_n_port.py",
        "tests/test_sec_pipelined_fetcher.py",
        # This guard file itself references the method name in its
        # contract sentence.
        "tests/test_fetch_document_text_callers.py",
    }
)


def _all_caller_files() -> set[str]:
    """Scan app/ and tests/ for ``fetch_document_text`` occurrences,
    returning repo-relative forward-slash paths."""
    pattern = re.compile(r"\bfetch_document_text\b")
    hits: set[str] = set()
    for root_name in ("app", "tests"):
        root = _REPO_ROOT / root_name
        for path in root.rglob("*.py"):
            try:
                text = path.read_text(encoding="utf-8")
            except OSError:
                continue
            if pattern.search(text):
                rel = path.relative_to(_REPO_ROOT).as_posix()
                hits.add(rel)
    return hits


def test_fetch_document_text_caller_set_is_pinned() -> None:
    """Every file that references ``fetch_document_text`` is on the
    allow-list. A new caller fails this test — update the allow-list
    only alongside a documented SQL-normalisation path (see
    docs/review-prevention-log.md "Every structured field from an
    upstream document lands in SQL")."""
    actual = _all_caller_files()
    unexpected = actual - _ALLOWED_CALLER_FILES
    missing = _ALLOWED_CALLER_FILES - actual
    assert not unexpected, (
        "New caller(s) of fetch_document_text detected outside the "
        "allow-list. Each new caller must route the body through a "
        "service-layer ingester that normalises every structured "
        "field into SQL (#448 / #453). Offenders: "
        f"{sorted(unexpected)}"
    )
    assert not missing, (
        "Allow-listed caller(s) no longer reference fetch_document_text. "
        "Remove stale entries from _ALLOWED_CALLER_FILES so the guard "
        f"doesn't silently accept new callers. Missing: {sorted(missing)}"
    )


def test_caller_scan_finds_expected_minimum() -> None:
    """Defensive sentinel (#436 "Empty-parametrize silent pass"
    pattern): the grep-based walk must return ``>= 9`` hits. If the
    regex or the file walk breaks, the main test above would silently
    pass with an empty diff — this assertion makes the guard loud
    when the scan itself is broken.
    """
    hits = _all_caller_files()
    assert len(hits) >= 9, (
        f"Scanner returned only {len(hits)} files matching "
        f"fetch_document_text; expected >= 9. Scanner or repository "
        "layout changed — re-audit before relaxing this bound."
    )
