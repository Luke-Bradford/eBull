# FINRA RegSHO daily short volume (#916) — implementation plan

> Spec: `docs/superpowers/specs/2026-05-18-finra-regsho-daily.md` (CLEAN through Codex 1a r3).
> Spike: `docs/superpowers/spikes/2026-05-18-finra-regsho-daily-feasibility.md`.
> Parent plan: `docs/superpowers/plans/2026-05-17-us-etl-completion.md` §2 Phase 6 PR 12.
> Branch: `feature/916-finra-regsho-daily-short-volume` (already on).
> Architectural siblings (clone shape, refine substance):
> - G6/#915 bimonthly (`app/services/finra_short_interest_ingest.py`, `app/jobs/finra_short_interest_refresh.py`, `app/providers/implementations/finra_short_interest.py`) — same `finra` Lane, shared throttle, same synth-noop-manifest pattern.
> - G12 `sec_master_idx_quarterly_sweep` — preloaded resolver + per-target failure isolation.
> - G7 `sec_xbrl_facts` — synth no-op manifest parser shape.

## 1. Task DAG

```
T1  Migrations (sql/153 enum extension + sql/154 regsho table)
T2  DocumentKind Literal widening + ManifestSource Literal widening + _CADENCE entry
T3  Provider — app/providers/implementations/finra_regsho.py
T4  Service — app/services/finra_regsho_ingest.py
T5  ScheduledJob body — app/jobs/finra_regsho_daily_refresh.py
T6  scheduler.py wiring — JOB_FINRA_REGSHO_DAILY_REFRESH + ScheduledJob entry + body shim
T7  sources.py MANUAL_TRIGGER_JOB_SOURCES + runtime.py _INVOKERS
T8  Manifest parser — app/services/manifest_parsers/finra_regsho_daily.py + register
T9  Fixtures — CNMS_panel_20260515.txt (pristine) + FNRA_empty_20260515.txt + CNMS_row_defects_20260515.txt + CNMS_header_corrupt_20260515.txt + CNMS_footer_mismatch_20260515.txt + CNMS_body_date_mismatch_20260515.txt
T10 Tests (provider + service + refresh + wiring + manifest-parser + layer123 + universal-gate + fetch-doc-text-callers)
T11 Skill + matrix + memory updates
T12 Local gates (ruff check + ruff format check + pyright + pytest)
T13 Codex 2 pre-push
T14 PR body + push
T15 Bot review loop → APPROVE + CI green → merge
```

Dependencies (corrected from Codex 1b r1 MED — sibling JOB module does NOT import scheduler constants; the scheduler shim lazily imports the JOB module's `run_*` function):
- T1 (SQL migrations) is standalone — DB connection only. Pre-ingest dry-run runs `psql -f` inside the dev container to count partitions (expect 25). Does NOT depend on T2.
- T2 (Literal + cadence widening) blocks T3 + T4 + T5 + T8 + T10 (any module using the new source / document_kind strings).
- T3 blocks T5 + T10.
- T4 blocks T5 + T10.
- T5 (JOB module exposing `run_finra_regsho_daily_refresh`) blocks T6 (scheduler shim lazily imports from T5 at runtime; tests gate on the import path being valid).
- T6 (scheduler constants + ScheduledJob entry + body shim) blocks T7 (runtime `_INVOKERS` wraps the scheduler shim).
- T8 blocks T10 + T12 (manifest parser registry tests; module import path).
- T9 blocks T10.
- T10 blocks T12.
- T12 blocks T13.
- T13 blocks T14.

Practical ordering: T1 → T2 → T9 (fixtures, parallel) → T3 → T4 → T5 → T6 → T7 → T8 → T10 → T11 → T12 → T13 → T14 → T15.

## 2. T1 — Migrations

### 2.1 `sql/153_finra_regsho_daily_enum.sql`

Three CHECK constraints widened in lock-step (`filing_raw_documents.document_kind`, `sec_filing_manifest.source`, `data_freshness_index.source`). Each follows the DROP + ADD CHECK pattern from sql/151 (`finra_short_interest_csv` widening). Verbatim from spec §5.1.

### 2.2 `sql/154_finra_regsho_daily.sql`

Verbatim from spec §5.2. The DO loop generates **25 partitions** spanning 2024-Q1 → 2030-Q1 inclusive (loop bound `q_start < '2030-04-01'` includes 2030-Q1 as the last iteration).

**Pre-ingest dry-run** — same shape as #915 plan §2.2:

```bash
docker exec ebull-postgres bash -c "psql -U postgres -d ebull -f /tmp/154.sql && \
  psql -U postgres -d ebull -c \"SELECT count(*) FROM pg_inherits WHERE inhparent = 'finra_regsho_daily_observations'::regclass\""
```

Expected: `25`. If count diverges, fix the DO bound before committing.

### 2.3 `_PLANNER_TABLES` registration

Add `finra_regsho_daily_observations` to `tests/fixtures/ebull_test_db.py::_PLANNER_TABLES` so cross-test cleanup wipes dev rows. Mirror PR #1194 G8 + PR #1207 G6 bimonthly precedent.

## 3. T2 — Literal + cadence widening

### 3.1 `app/services/raw_filings.py`

```python
DocumentKind = Literal[
    "primary_doc",
    "infotable_13f",
    "primary_doc_13dg",
    "form4_xml",
    "form3_xml",
    "form5_xml",
    "def14a_body",
    "nport_xml",
    "finra_short_interest_csv",
    "finra_regsho_daily_txt",      # NEW
]
```

### 3.2 `app/services/sec_manifest.py`

```python
ManifestSource = Literal[
    "sec_form3", "sec_form4", "sec_form5",
    "sec_13d", "sec_13g",
    "sec_13f_hr",
    "sec_def14a",
    "sec_n_port", "sec_n_csr",
    "sec_10k", "sec_10q", "sec_8k",
    "sec_xbrl_facts",
    "finra_short_interest",
    "finra_regsho_daily",          # NEW
]
```

### 3.3 `app/services/data_freshness.py`

```python
_CADENCE: dict[ManifestSource, timedelta] = {
    ...
    "finra_short_interest": timedelta(days=20),
    "finra_regsho_daily": timedelta(days=2),   # NEW — daily publication
                                                # + 1 weekend + holiday slack
}
```

### 3.4 `app/services/capability_manifest_mapping.py`

The unmapped-manifest-source allow-list at `_UNMAPPED_MANIFEST_SOURCES` already has a `finra_short_interest` entry. Add the matching:

```python
"finra_regsho_daily": (
    "FINRA, not SEC — no capability tag yet. Add a `finra_regsho_daily` "
    "capability tag once the operator-visible memo overlay lands."
),
```

This prevents the all-sources-have-either-a-capability-or-an-allowlist-reason invariant test from failing on the new source.

## 4. T3 — Provider

### 4.1 File: `app/providers/implementations/finra_regsho.py`

```python
"""FINRA RegSHO Daily Short Volume CDN provider (#916).

Sibling to ``finra_short_interest`` — same host (cdn.finra.org), shares
the FINRA throttle clock + lock module-globals so bimonthly + daily
ingest never combine to exceed the 1 req/s polite floor.

Endpoint shape (empirically verified 2026-05-18 in spike §3):
  URL: https://cdn.finra.org/equity/regsho/daily/{PREFIX}shvol{YYYYMMDD}.txt
  Format: pipe-delimited TEXT, CRLF line terminators.
  Prefixes: CNMS, FNQC, FNRA, FNSQ, FNYX, FORF.
  Auth: anonymous CDN.
"""

from __future__ import annotations

from datetime import date
from typing import Final

import httpx

from app.providers.implementations.finra_short_interest import (
    _FINRA_MIN_INTERVAL_S,
    _FINRA_RATE_LIMIT_CLOCK,
    _FINRA_RATE_LIMIT_LOCK,
    FinraNotFound,
)
from app.providers.resilient_client import ResilientClient

PREFIXES: Final[tuple[str, ...]] = ("CNMS", "FNQC", "FNRA", "FNSQ", "FNYX", "FORF")


class FinraRegShoProvider:
    BASE_URL: Final[str] = "https://cdn.finra.org/equity/regsho/daily/"

    def __init__(self, http_client: ResilientClient | None = None) -> None:
        if http_client is None:
            inner = httpx.Client(
                timeout=httpx.Timeout(30.0),
                headers={
                    "User-Agent": "eBull/0.1 (luke.bradford@hotmail.co.uk)",
                    "Accept": "text/plain,*/*",
                },
            )
            http_client = ResilientClient(
                inner,
                min_request_interval_s=_FINRA_MIN_INTERVAL_S,
                shared_last_request=_FINRA_RATE_LIMIT_CLOCK,
                shared_throttle_lock=_FINRA_RATE_LIMIT_LOCK,
            )
        self._http = http_client

    def regsho_daily_url(self, trade_date: date, prefix: str) -> str:
        if prefix not in PREFIXES:
            raise ValueError(f"unknown FINRA RegSHO prefix: {prefix!r} (allowed: {PREFIXES})")
        return f"{self.BASE_URL}{prefix}shvol{trade_date.strftime('%Y%m%d')}.txt"

    def fetch_regsho_daily_file(self, trade_date: date, prefix: str) -> bytes:
        url = self.regsho_daily_url(trade_date, prefix)
        response = self._http.get(url)
        if response.status_code == 404:
            raise FinraNotFound(f"FINRA RegSHO daily file not found: {url}")
        response.raise_for_status()
        return response.content
```

### 4.2 Tests in `tests/test_finra_regsho_daily_provider.py`

| Test | Asserts |
|---|---|
| `test_regsho_daily_url_builder_iso_date` | `provider.regsho_daily_url(date(2026,5,15), "CNMS") == "https://cdn.finra.org/equity/regsho/daily/CNMSshvol20260515.txt"` |
| `test_regsho_daily_url_builder_all_prefixes` | URL built for every value in `PREFIXES` matches `{prefix}shvol{YYYYMMDD}.txt` |
| `test_regsho_daily_url_builder_unknown_prefix_raises` | `provider.regsho_daily_url(date(2026,5,15), "ZZZZ")` → `ValueError` |
| `test_404_raises_finra_not_found` | `MockTransport` 404 → `FinraNotFound` |
| `test_5xx_raises_http_status_error` | `MockTransport` 500 with attached `Request` → `httpx.HTTPStatusError` after retries; `max_retries=0` test wrapper |
| `test_rate_limit_clock_identity_with_bimonthly` | Two `FinraRegShoProvider` instances share `_FINRA_RATE_LIMIT_CLOCK`; also assert the clock list IS IDENTITY-SHARED with `FinraShortInterestProvider`'s — both reach the same module-global |
| `test_back_to_back_throttle_enforces_min_interval` | `MockTransport` 200 ×2 → measured gap ≥ `_FINRA_MIN_INTERVAL_S`; teardown resets clock to 0.0 |

## 5. T4 — Service

### 5.1 File: `app/services/finra_regsho_ingest.py`

Reuses `normalise_symbol` + `build_preloaded_symbol_resolver` + `HeaderCorruptionError` via import from `app.services.finra_short_interest_ingest`. No duplication.

Public surface (per spec §7.1 + §7.2):

```python
"""FINRA RegSHO daily short volume service (#916).

Parses pipe-delim daily files from the FINRA CDN, resolves symbol →
instrument_id via the preloaded resolver from the bimonthly module,
UPSERTs typed observations + synthetic FINRA manifest row.

Transaction contract (#915 Codex 1b r1 HIGH 2 lesson): the SERVICE
emits SQL only into the caller's open transaction. NEVER calls
``conn.commit()`` / ``conn.rollback()`` AND NEVER enters its own
``with conn.transaction():``. Caller MUST wrap the call site in
``with conn.transaction():``.

Raw-payload-before-parse contract (#1168) is JOB-enforced: caller MUST
run ``raw_filings.store_raw(...)`` + ``conn.commit()`` BEFORE calling
this function.

Manifest atomicity contract (spec §7.3): the manifest UPSERT runs
INSIDE the caller-owned txn, AFTER the observations writes. Atomic-
with-the-data — ``manifest.ingest_status='parsed'`` always implies
observations durable.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any
from uuid import UUID

import psycopg

from app.services.finra_short_interest_ingest import (
    HeaderCorruptionError,
    normalise_symbol,
)

logger = logging.getLogger(__name__)

PARSER_VERSION = "finra-regsho-daily-v1"

_EXPECTED_HEADER: tuple[str, ...] = (
    "Date",
    "Symbol",
    "ShortVolume",
    "ShortExemptVolume",
    "TotalVolume",
    "Market",
)


@dataclass(frozen=True)
class RegShoDailyIngestStats:
    trade_date: date
    prefix: str
    rows_parsed: int = 0
    rows_resolved: int = 0
    rows_upserted: int = 0
    skipped_no_instrument_match: int = 0
    skipped_ambiguous_symbol: int = 0
    skipped_invalid_row: int = 0
    failed: bool = False
    error_detail: str | None = None


def _opt_decimal(v: str | None) -> Decimal | None:
    if v is None or v == "":
        return None
    try:
        return Decimal(v)
    except (InvalidOperation, ValueError, TypeError):
        return None


def ingest_regsho_daily_file(
    conn: psycopg.Connection[Any],
    trade_date: date,
    prefix: str,
    raw_bytes: bytes,
    resolver: Callable[[str], int | None],
    ingest_run_id: UUID,
) -> RegShoDailyIngestStats:
    """Parse + UPSERT + manifest write. SQL-only — caller owns txn."""
```

Parse algorithm (per spec §7.2):

```python
text = raw_bytes.decode("utf-8")
# Split, strip trailing empty lines.
lines = [ln for ln in text.split("\n")]
lines = [ln.rstrip("\r") for ln in lines]
while lines and lines[-1] == "":
    lines.pop()
if not lines:
    raise HeaderCorruptionError(f"RegSHO daily file empty: trade_date={trade_date} prefix={prefix}")

# Header.
header_cols = tuple(lines[0].split("|"))
if header_cols != _EXPECTED_HEADER:
    raise HeaderCorruptionError(
        f"RegSHO header mismatch at trade_date={trade_date} prefix={prefix}: "
        f"expected {_EXPECTED_HEADER}, got {header_cols}"
    )

# Footer.
try:
    footer_int = int(lines[-1].strip())
except ValueError:
    raise HeaderCorruptionError(
        f"RegSHO footer missing/non-int at trade_date={trade_date} prefix={prefix}: "
        f"last line={lines[-1]!r}"
    ) from None

body = lines[1:-1]

expected_date_str = trade_date.strftime("%Y%m%d")
# NOTE — footer-row-count check happens AFTER the body loop per spec
# §7.2 step 7. The body-date check happens INSIDE the per-row loop
# per spec §7.2 step 6. This ordering ensures a fixture with a
# body-date defect surfaces THAT defect (not the footer count) so
# debug telemetry points at the right failure mode.
```

Per-row processing inside `with conn.cursor() as cur:` (and the resolver's ambiguous-key set captured BEFORE the loop):

```python
# Capture ambiguous-key set from the resolver attribute before the
# loop (mirrors #915 finra_short_interest_ingest.py:194). The closure
# returns None for both unknown AND ambiguous; this set lets the
# row loop disambiguate which counter to bump.
ambiguous_keys: frozenset[str] = getattr(resolver, "ambiguous_keys", frozenset())

for raw_line in body:
    rows_parsed += 1                # bump BEFORE validation — every body
                                    # line counts toward parsed/resolved
                                    # ratio used in the match-rate WARNING
                                    # (Codex 1b r2 MED).
    parts = raw_line.split("|")    # BARE split — no maxsplit.
    if len(parts) != 6:
        skipped_invalid_row += 1
        continue
    body_date, symbol, short_vol_raw, short_exempt_raw, total_vol_raw, market = parts

    # Spec §7.2 step 6 — body-Date column must match trade_date arg.
    # Mismatch is file-level (every row must have same date); raise.
    if body_date != expected_date_str:
        raise HeaderCorruptionError(
            f"RegSHO body-date mismatch at trade_date={trade_date} prefix={prefix}: "
            f"row date={body_date!r} != expected {expected_date_str!r}"
        )
    symbol = symbol.strip()
    if not symbol:
        skipped_invalid_row += 1
        continue
    short_vol = _opt_decimal(short_vol_raw)
    short_exempt = _opt_decimal(short_exempt_raw)
    total_vol = _opt_decimal(total_vol_raw)
    if short_vol is None or short_exempt is None or total_vol is None:
        skipped_invalid_row += 1
        continue
    market = market.strip()
    if not market:
        skipped_invalid_row += 1
        continue

    key = normalise_symbol(symbol)
    if key in ambiguous_keys:
        skipped_ambiguous_symbol += 1
        continue
    instrument_id = resolver(symbol)
    if instrument_id is None:
        skipped_no_instrument_match += 1
        continue
    rows_resolved += 1
    cur.execute("""<INSERT/ON CONFLICT into finra_regsho_daily_observations>""", {...})
    rows_upserted += 1

# After the loop — footer-row-count validation per spec §7.2 step 7.
# Compares the body line count we iterated to the footer integer.
# Mismatch = structural defect; raise inside the caller's txn so the
# whole file rolls back atomically.
if len(body) != footer_int:
    raise HeaderCorruptionError(
        f"RegSHO footer-count mismatch at trade_date={trade_date} prefix={prefix}: "
        f"parsed {len(body)} body rows, footer says {footer_int}"
    )
```

The observations UPSERT pinned shape:

```sql
INSERT INTO finra_regsho_daily_observations (
    instrument_id, trade_date, market, source_document_id,
    short_volume, short_exempt_volume, total_volume,
    source, source_url, filed_at, period_end,
    known_from, ingest_run_id
) VALUES (
    %(instrument_id)s, %(trade_date)s, %(market)s, %(source_document_id)s,
    %(short_volume)s, %(short_exempt_volume)s, %(total_volume)s,
    'finra_regsho', %(source_url)s, %(filed_at)s, %(period_end)s,
    NOW(), %(ingest_run_id)s
)
ON CONFLICT (instrument_id, trade_date, market, source_document_id) DO UPDATE SET
    short_volume = EXCLUDED.short_volume,
    short_exempt_volume = EXCLUDED.short_exempt_volume,
    total_volume = EXCLUDED.total_volume,
    source_url = EXCLUDED.source_url,
    filed_at = EXCLUDED.filed_at,
    period_end = EXCLUDED.period_end,
    known_from = NOW(),
    ingest_run_id = EXCLUDED.ingest_run_id;
```

Bind values: `source_document_id = f"{prefix}_{expected_date_str}"`, `source_url = f"{FinraRegShoProvider.BASE_URL}{prefix}shvol{expected_date_str}.txt"`, `filed_at = datetime.combine(trade_date, datetime.min.time(), tzinfo=UTC)`, `period_end = trade_date`.

After the for-loop:

- Manifest UPSERT (per spec §7.3) for `accession = f"FINRA_REGSHO_{prefix}_{expected_date_str}"`.
- `seed_freshness_for_manifest_row(conn, subject_type='finra_universe', subject_id='FINRA_REGSHO', source='finra_regsho_daily', cik='FINRA_REGSHO', instrument_id=None, accession_number=accession, filed_at=filed_at)`.

### 5.2 Tests in `tests/test_finra_regsho_daily_ingest.py`

Integration against `ebull_test_conn`:

| Test | Asserts |
|---|---|
| `test_happy_path_cnms_panel` | Load `CNMS_panel_20260515.txt` fixture; resolver pre-seeded with AAPL/GME/MSFT/JPM/HD instrument_ids; 5 rows upserted; manifest row `parsed`; freshness row seeded |
| `test_empty_file_fnra` | Load `FNRA_empty_20260515.txt` fixture; `rows_parsed=0, rows_upserted=0, failed=False`; manifest row `parsed`; freshness row seeded |
| `test_header_corruption_raises` | Defects fixture line 1 != header → `HeaderCorruptionError` |
| `test_footer_count_mismatch_raises` | Defects fixture body has 3 rows + footer says 5 → `HeaderCorruptionError` |
| `test_body_date_mismatch_raises` | Defects fixture body row 1's `Date` column != trade_date arg → `HeaderCorruptionError` |
| `test_truncated_row_skipped` | Defects fixture body row missing `Market` → `skipped_invalid_row` incremented; other rows unaffected |
| `test_malformed_decimal_skipped` | Defects fixture body row has `ShortVolume='abc'` → `skipped_invalid_row` |
| `test_blank_symbol_skipped` | Defects fixture body row has empty Symbol → `skipped_invalid_row` |
| `test_no_instrument_match_counter` | Defects fixture row uses symbol `ZZZZZ_UNKNOWN` (not in instruments) → `skipped_no_instrument_match` |
| `test_ambiguous_symbol_counter` | Seed two instruments whose symbols collapse to the same normalised key → `skipped_ambiguous_symbol` |
| `test_multi_prefix_coexistence` | Insert CNMS row + FNQC row for same instrument/trade_date — both rows present (PK includes `market` + `source_document_id`) |
| `test_revision_upsert` | Re-ingest same `(prefix, trade_date)` with mutated `short_volume` — row updates, no PK violation |
| `test_service_no_commit_invariant` | After `ingest_regsho_daily_file` returns, BEFORE caller commits, `SELECT FROM ...` from a SECOND connection sees zero rows (proves no implicit commit happened mid-service) |
| `test_normalise_symbol_imported` | Sanity-check `normalise_symbol` imported from bimonthly module works (`BRK.A` → `BRKA`) |

## 6. T5 — ScheduledJob body

### 6.1 File: `app/jobs/finra_regsho_daily_refresh.py`

Closely mirrors `app/jobs/finra_short_interest_refresh.py` (just merged with #1207). Differences from bimonthly:

- Cadence: trade-date weekday-only enumeration (`_trade_dates_to_fetch`), no EOM / mid-15th logic.
- Revision window: `(date, prefix)` pair set, last-2-dates × 6 prefixes.
- Manifest filter: parse accessions back via `_parse_accession` per spec §8.1.
- Iteration: `for (trade_date, prefix) in sorted(targets):` instead of just `for date in targets:`.

```python
"""FINRA RegSHO daily short volume refresh (#916).

ScheduledJob body — mirrors finra_short_interest_refresh shape:
preloaded resolver → candidate dates → manifest-parsed filter →
revision-window union → per-(date, prefix) fetch + parse + upsert.
"""

PREFIXES: Final[tuple[str, ...]] = ("CNMS", "FNQC", "FNRA", "FNSQ", "FNYX", "FORF")

_ACCESSION_PREFIX: Final[str] = "FINRA_REGSHO_"

# NOTE — Codex 1b r1 LOW: the job-name constant
# ``JOB_FINRA_REGSHO_DAILY_REFRESH`` lives ONLY in
# ``app/workers/scheduler.py`` (sibling #915 precedent). Do NOT
# duplicate it here — drift between the two would break the
# scheduler-wiring invariant test silently.


def _parse_accession(accession: str) -> tuple[date, str] | None:
    """Reverse 'FINRA_REGSHO_{PREFIX}_{YYYYMMDD}'."""
    if not accession.startswith(_ACCESSION_PREFIX):
        return None
    tail = accession[len(_ACCESSION_PREFIX):]
    if "_" not in tail:
        return None
    prefix_part, date_part = tail.rsplit("_", 1)
    if prefix_part not in PREFIXES:
        return None
    try:
        td = datetime.strptime(date_part, "%Y%m%d").date()
    except ValueError:
        return None
    return (td, prefix_part)


def _trade_dates_to_fetch(now: datetime, backfill_window_days: int = 30) -> list[date]:
    earliest = (now - timedelta(days=backfill_window_days)).date()
    today = now.date()
    out: list[date] = []
    d = earliest
    while d <= today:
        if d.weekday() < 5:  # 0-4 = Mon-Fri
            out.append(d)
        d += timedelta(days=1)
    return out


def _already_parsed_pairs(conn: psycopg.Connection[Any]) -> set[tuple[date, str]]:
    out: set[tuple[date, str]] = set()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT accession_number FROM sec_filing_manifest
            WHERE source = 'finra_regsho_daily' AND ingest_status = 'parsed'
            """
        )
        for (accession,) in cur.fetchall():
            parsed = _parse_accession(accession)
            if parsed is not None:
                out.add(parsed)
    return out


def _compute_targets(
    candidate_dates: list[date],
    already_parsed: set[tuple[date, str]],
) -> list[tuple[date, str]]:
    sorted_candidates = sorted(candidate_dates)
    revision_window = {
        (d, p) for d in sorted_candidates[-2:] for p in PREFIXES
    } if sorted_candidates else set()
    all_pairs = {(d, p) for d in candidate_dates for p in PREFIXES}
    return sorted(all_pairs - already_parsed | revision_window)


def run_finra_regsho_daily_refresh(
    conn: psycopg.Connection[Any],
    *,
    now: datetime | None = None,
    backfill_window_days: int = 30,
    provider: FinraRegShoProvider | None = None,
) -> RegShoDailyRefreshStats:
    """Per-fire orchestration — see module docstring."""
    # body shape verbatim from #915 — fetch / store_raw / commit /
    # with conn.transaction(): ingest_regsho_daily_file(...).
    # Phase-1 store_raw wrapped in try/except per spec §4 (Codex 1a r1 MED).
    #
    # AFTER per-file loop:
    #   total_parsed = sum(s.rows_parsed for s in stats_list)
    #   total_resolved = sum(s.rows_resolved for s in stats_list)
    #   if total_parsed > 0:
    #       match_rate = total_resolved / total_parsed
    #       if match_rate < 0.50:
    #           logger.warning(
    #               "finra_regsho_daily_refresh: match rate %.2f%% below 50%% "
    #               "(parsed=%d resolved=%d) — universe drift or FINRA "
    #               "column-shape regression suspected",
    #               100 * match_rate, total_parsed, total_resolved,
    #           )
    # Mirrors #915 spec §4 + Codex 1b r1 MED on this plan.
```

### 6.2 Tests in `tests/test_finra_regsho_daily_refresh.py`

| Test | Asserts |
|---|---|
| `test_trade_dates_to_fetch_weekday_filter` | `_trade_dates_to_fetch(date(2026,5,18), 7)` (Mon) returns 6 weekdays (Mon back to prev-Tue), skips Sat/Sun |
| `test_trade_dates_to_fetch_30_day_window` | 30-day window count: ~22 weekdays (depending on calendar) |
| `test_parse_accession_clean` | `_parse_accession("FINRA_REGSHO_CNMS_20260515") == (date(2026,5,15), "CNMS")` |
| `test_parse_accession_unknown_prefix_returns_none` | `_parse_accession("FINRA_REGSHO_XXXX_20260515") is None` |
| `test_parse_accession_malformed_date_returns_none` | `_parse_accession("FINRA_REGSHO_CNMS_NOT_A_DATE") is None` |
| `test_parse_accession_wrong_root_returns_none` | `_parse_accession("FINRA_SI_20260515") is None` |
| `test_already_parsed_pairs_skips_bimonthly` | Pre-seed manifest with `FINRA_SI_20260515` (bimonthly) + `FINRA_REGSHO_CNMS_20260515` (daily); only the latter appears in `_already_parsed_pairs` |
| `test_compute_targets_subtraction` | candidates=[d1, d2, d3], already=[(d1, CNMS)] → targets contains (d1, FNQC) but NOT (d1, CNMS) UNLESS d1 is in revision window |
| `test_compute_targets_revision_window_union` | last-2 dates × 6 prefixes always present in targets |
| `test_per_file_failure_isolated` | Provider injection: fetch CNMS succeeds, fetch FNQC raises HTTPError; assert CNMS row committed, FNQC counted as failed, FNSQ etc. continue |
| `test_partial_failure_raises_runtime_error` | failed_files > 0 → `RuntimeError` raised; successful rows still in DB |
| `test_404_skips_silently` | Provider raises `FinraNotFound` → file not appended to stats; no row written; no error |
| `test_raw_store_failure_does_not_poison_connection` | Stub `store_raw` to raise on first call; assert per-file failure counted, `conn.rollback()` called, next iteration's fetch succeeds |
| `test_match_rate_warning_logged_below_50pct` | Force resolver to return None for >50% of body rows; assert WARNING log line emitted with `match rate %.2f%% below 50%%` and exact parsed/resolved counts |
| `test_match_rate_no_warning_on_zero_parsed` | All files empty (FNRA-shape); `total_parsed=0` → no WARNING line emitted (division-by-zero guard) |

## 7. T6 — scheduler.py wiring

### 7.1 Constants

Add to `app/workers/scheduler.py` after `JOB_FINRA_SHORT_INTEREST_REFRESH`:

```python
JOB_FINRA_REGSHO_DAILY_REFRESH = "finra_regsho_daily_refresh"
```

### 7.2 ScheduledJob entry

Verbatim spec §8.2 — added to `SCHEDULED_JOBS` after the bimonthly entry.

### 7.3 Body shim

Verbatim shape per `app/workers/scheduler.py:4680` (sibling #915). `_tracked_job` is a **context manager**, NOT a decorator (Codex 1b r1 HIGH):

```python
def finra_regsho_daily_refresh() -> None:
    """``_INVOKERS['finra_regsho_daily_refresh']`` — G6/#916.

    Daily 23:00 UTC; opens its own DB connection. Per-file commit /
    rollback ownership lives inside ``run_finra_regsho_daily_refresh``.
    No operator params at v1 — extended-window backfill is a REPL
    runbook against ``run_finra_regsho_daily_refresh(conn,
    backfill_window_days=N)``.

    Failure surfacing: any per-file failure inside the job raises
    ``RuntimeError`` so ``_tracked_job`` records
    ``job_runs.status='failure'`` with the failed-file detail.
    Successful files still commit before the raise — partial work is
    durable.
    """
    from app.jobs.finra_regsho_daily_refresh import (
        run_finra_regsho_daily_refresh,
    )

    with _tracked_job(JOB_FINRA_REGSHO_DAILY_REFRESH) as tracker:
        with psycopg.connect(settings.database_url) as conn:
            stats = run_finra_regsho_daily_refresh(conn)
        tracker.row_count = stats.total_upserted
        logger.info(
            "finra_regsho_daily_refresh: files=%d total_upserted=%d failed=%d",
            len(stats.daily_files),
            stats.total_upserted,
            stats.failed_files,
        )
        # RuntimeError on partial failure lives inside
        # run_finra_regsho_daily_refresh so _tracked_job records
        # status='failure'; this block just surfaces operator-visible
        # row_count + summary log.
```

## 8. T7 — sources.py + runtime.py

`app/jobs/sources.py` — add to `MANUAL_TRIGGER_JOB_SOURCES`:

```python
"finra_regsho_daily_refresh": "finra",
```

`app/jobs/runtime.py` — add to `_INVOKERS` block:

```python
_INVOKERS[_scheduler.JOB_FINRA_REGSHO_DAILY_REFRESH] = _adapt_zero_arg(
    _scheduler.finra_regsho_daily_refresh
)
```

The existing `finra` Lane docstring at `app/jobs/sources.py:130` reads "v1 single job (`finra_short_interest_refresh`, G6/#915); FINRA RegSHO daily (#916) adds a second job in the same lane." — that comment becomes load-bearing, update to "v1 jobs: `finra_short_interest_refresh` (G6/#915, bimonthly) + `finra_regsho_daily_refresh` (G6/#916, daily)."

## 9. T8 — Manifest parser

### 9.1 File: `app/services/manifest_parsers/finra_regsho_daily.py`

Verbatim per spec §9 (synth no-op, same shape as G6 bimonthly):

Verbatim shape per `app/services/manifest_parsers/finra_short_interest.py` (sibling #915). `register_parser` takes `requires_raw_payload=False` kw-only (Codex 1b r1 HIGH — `parser_version=` is NOT a parameter; the version lives inside the `ParseOutcome`):

```python
"""finra_regsho_daily manifest-worker parser — synth no-op (G6/#916).

[See spec §9 for full docstring.]
"""

from __future__ import annotations

import logging
from typing import Any

import psycopg

logger = logging.getLogger(__name__)

PARSER_VERSION = "finra-regsho-daily-v1"


def _parse_finra_regsho_daily(
    conn: psycopg.Connection[Any],  # noqa: ARG001 — synth no-op uses no DB
    row: Any,  # ManifestRow — forward-ref to avoid circular import
) -> Any:  # ParseOutcome — forward-ref
    from app.jobs.sec_manifest_worker import ParseOutcome

    logger.debug(
        "finra_regsho_daily manifest parser: synth no-op for accession=%s "
        "(RegSHO daily lands via finra_regsho_daily_refresh ScheduledJob; "
        "no per-filing payload work)",
        row.accession_number,
    )
    return ParseOutcome(status="parsed", parser_version=PARSER_VERSION)


def register() -> None:
    from app.jobs.sec_manifest_worker import register_parser

    register_parser(
        "finra_regsho_daily",
        _parse_finra_regsho_daily,
        requires_raw_payload=False,
    )
```

### 9.2 Registration

`app/services/manifest_parsers/__init__.py::register_all_parsers`:

```python
from app.services.manifest_parsers import finra_regsho_daily as _finra_regsho_daily
# ...
_finra_regsho_daily.register()  # synth no-op (G6/#916 — ScheduledJob owns writes)
```

### 9.3 Tests in `tests/test_finra_regsho_daily_manifest_parser.py`

| Test | Asserts |
|---|---|
| `test_parser_synth_noop_outcome` | Direct call returns `ParseOutcome(status='parsed', parser_version='finra-regsho-daily-v1')` |
| `test_parser_no_network_no_db_write` | Mock `fetch_document_text` / `store_raw` / `conn.execute` / `conn.cursor` / `conn.transaction` — none called |
| `test_parser_registered_after_register_all` | After `clear_registered_parsers()` + `register_all_parsers()`, the `finra_regsho_daily` source has a registered parser |

## 10. T9 — Fixtures

Per Codex 1b r1 MED — one fixture cannot deterministically drive BOTH a fatal raise AND row-level skip-counter assertions. Split into single-purpose fixtures (each isolates one failure mode):

| File | Purpose | Header | Footer | Body |
|---|---|---|---|---|
| `tests/fixtures/finra/regsho/CNMS_panel_20260515.txt` | Pristine happy path | valid | `5` | 5 valid panel rows (AAPL/GME/HD/JPM/MSFT). Already committed by spike. |
| `tests/fixtures/finra/regsho/FNRA_empty_20260515.txt` | Empty-prefix shape (FNRA legitimate-empty path) | valid | `0` | (none). Already committed by spike. |
| `tests/fixtures/finra/regsho/CNMS_row_defects_20260515.txt` | Row-level skip counters | valid | `5` | 5 rows: 1 happy + 1 truncated + 1 malformed-decimal + 1 blank-Symbol + 1 unknown-symbol. Footer matches body count so the file is structurally valid; service iterates all rows and bumps the appropriate per-row counter (`skipped_invalid_row` ×3 + `skipped_no_instrument_match` ×1) plus 1 happy upsert. |
| `tests/fixtures/finra/regsho/CNMS_header_corrupt_20260515.txt` | Header-corruption fatal | INVALID (column re-ordered) | `0` | (none). Service raises `HeaderCorruptionError` before reaching body iteration. |
| `tests/fixtures/finra/regsho/CNMS_footer_mismatch_20260515.txt` | Footer-mismatch fatal | valid | `5` | 2 valid rows (count mismatch). Service raises `HeaderCorruptionError` AFTER body iteration. |
| `tests/fixtures/finra/regsho/CNMS_body_date_mismatch_20260515.txt` | Body-date fatal | valid | `1` | 1 row with `Date=20260516` (caller passes `trade_date=2026-05-15`). Service raises `HeaderCorruptionError` mid-body. |

The ambiguous-symbol counter test does NOT need a defects fixture — it pre-seeds two instruments in the test DB whose symbols collapse to the same normalised key, then ingests the pristine `CNMS_panel_20260515.txt` with the resolver's `ambiguous_keys` set containing that key (the test's resolver is built from the test DB so the ambiguity propagates naturally).

## 11. T10 — Tests catalogue

| File | Purpose |
|---|---|
| `tests/test_finra_regsho_daily_provider.py` | §4.2 above |
| `tests/test_finra_regsho_daily_ingest.py` | §5.2 above |
| `tests/test_finra_regsho_daily_refresh.py` | §6.2 above |
| `tests/test_finra_regsho_daily_scheduler_wiring.py` | `JOB_FINRA_REGSHO_DAILY_REFRESH` constant; ScheduledJob shape (`source='finra'`, cadence `Cadence.daily(hour=23, minute=0)`, prereq=`_bootstrap_complete`, `catch_up_on_boot=False`); `_INVOKERS` identity; `source_for("finra_regsho_daily_refresh") == "finra"`; `MANUAL_TRIGGER_JOB_SOURCES` entry. Plus a direct **shim-invocation smoke** — monkeypatch `run_finra_regsho_daily_refresh` + `psycopg.connect` and call `_scheduler.finra_regsho_daily_refresh()`; assert `tracker.row_count` was set to the stub's `total_upserted` (catches the `_tracked_job` decorator-vs-context-manager misuse — Codex 1b r1 LOW) |
| `tests/test_finra_regsho_daily_manifest_parser.py` | §9.3 above |
| `tests/test_layer_123_wiring.py` (extend) | Layer-4 row for `finra_regsho_daily_refresh` |
| `tests/test_universal_gate_carve_out.py` (extend) | Positive assertion `finra_regsho_daily_refresh` NOT in exempt allow-list (prereq=`_bootstrap_complete`) |
| `tests/test_fetch_document_text_callers.py` (extend) | Allow-list extended for new provider + service + parser modules (per #453 contract) — modules touch raw bytes via `httpx`, not `fetch_document_text`, so should NOT appear in the caller list; the registry-invariant test asserts the new modules are NOT in the caller dict |
| `tests/test_capability_manifest_mapping.py` (touch-up) | Ensure `finra_regsho_daily` in `_UNMAPPED_MANIFEST_SOURCES` allow-list; invariant test passes |
| `tests/test_data_freshness_cadence.py` (extend) | Cadence for `finra_regsho_daily` is 2 days |

## 12. T11 — Skill + matrix + memory

### 12.1 `.claude/skills/data-sources/finra.md`

- §1 table: flip RegSHO Daily row to `✅ WIRED 2026-05-18 (#916)`.
- §2.5 (NEW): `Decimal volumes + comma-joined Market on CNMS aggregate` — document the spike findings.
- §2.6 (NEW): `Footer-row-count validation` — every file ends with single-int row count; mismatch is structural defect.
- §3 (rate-limit posture): confirm shared throttle clock + lock with bimonthly, IDENTITY-shared module-globals.
- §4: rename to "Architecture in eBull — Option A" and add daily ScheduledJob alongside the bimonthly one.
- §6.4 (NEW): `RegSHO daily partition extension` runbook — operator must extend partition window before 2030-Q2.
- §8 (forward references): drop the `#916 adds RegSHO daily ingest sibling` row (now wired).

### 12.2 `.claude/skills/data-engineer/etl-endpoint-coverage.md`

- §2 `finra_short_interest` row: matrix entry expanded — bimonthly + daily both wired. Or split into two rows (bimonthly + RegSHO daily) — bimonthly is `finra_short_interest`, daily is `finra_regsho_daily`. Two separate freshness slots; two rows is more accurate.
- §7 G6 row: note both sub-rows wired; bimonthly cadence 20d, daily cadence 2d.

### 12.3 Memory

- Create new memory `project_916_finra_regsho_daily.md` capturing:
  - PR #NNN merged YYYY-MM-DD.
  - Architecture: sibling provider module importing throttle globals; service reuses `normalise_symbol` + `build_preloaded_symbol_resolver` from bimonthly.
  - Decimal-volume + comma-joined Market on CNMS findings.
  - Body-Date validation invariant (file-level fatal on row date != caller trade_date).
  - Operator runbook for extended-window backfill via REPL.
  - Cross-links to [[915-finra-bimonthly-short-interest]], [[us-source-coverage]], [[psycopg3-savepoint-commit]].
- Update `[[us-source-coverage]]` to flip G6 daily slot to wired.

## 13. T12 — Local gates

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest tests/test_finra_regsho_daily_provider.py \
              tests/test_finra_regsho_daily_ingest.py \
              tests/test_finra_regsho_daily_refresh.py \
              tests/test_finra_regsho_daily_scheduler_wiring.py \
              tests/test_finra_regsho_daily_manifest_parser.py \
              tests/test_universal_gate_carve_out.py \
              tests/test_layer_123_wiring.py
uv run pytest    # full suite
```

All four (ruff / format / pyright / pytest) must pass before push.

## 14. T13 — Codex 2 pre-push

```bash
codex exec review --output-last-message /tmp/codex2.txt \
  "Review the diff on this branch for #916 FINRA RegSHO daily short volume ingest..."
```

Mandatory per CLAUDE.md before push; fix everything Codex flags as real.

## 15. T14 — PR + push

PR body shape (mirror #1207, refined):

```
## What
[1-line summary]

## Why
[1-line motivation]

## Scope
- sql/153 enum extension (3 CHECK constraints widened in lock-step).
- sql/154 finra_regsho_daily_observations partitioned table (25 quarterly partitions 2024-Q1 → 2030-Q1).
- DocumentKind + ManifestSource Literal widening + _CADENCE entry + capability allow-list entry.
- New FinraRegShoProvider (sibling module sharing FINRA throttle globals).
- New ingest_regsho_daily_file service (SQL-only, caller-owned txn — psycopg3 savepoint-vs-commit invariant).
- New finra_regsho_daily_refresh ScheduledJob (daily 23:00 UTC, lane=finra, prereq=_bootstrap_complete).
- Synth no-op manifest parser (G7/G6 precedent).
- Fixtures (panel + empty FNRA + defects).
- Full test coverage (provider/service/refresh/wiring/manifest-parser/layer123/universal-gate/callers/cadence).
- Skill + matrix + memory updates.

## ETL DoD #8-#12 evidence
[Table per spec §11]

## Out of scope
[Per spec §12]

Closes #916
```

## 16. T15 — Bot review loop

Standard post-push cycle per `feedback_post_push_cycle.md`. Resolve every comment FIXED/DEFERRED/REBUTTED. Codex 3 (rebuttal-only round) if needed.

## 17. Risks + mitigations

| Risk | Mitigation |
|---|---|
| Throttle-state sharing test brittle to import order | Test asserts `_FINRA_RATE_LIMIT_CLOCK` from both modules IS-identity equal via `is` operator — survives import order |
| Body-Date mismatch on every fixture if `trade_date` arg wrong | Test fixture `CNMS_panel_20260515.txt` has body Date `20260515`; tests pass `trade_date=date(2026,5,15)`; mismatch fixture is its own file |
| Multi-prefix coexistence query confusion | PK includes `market` + `source_document_id` — distinct facts; spec §5.3 documents the operator filter `WHERE source_document_id LIKE 'CNMS_%'` for aggregate-only queries |
| Bootstrap-incomplete dev DB | Job has `prerequisite=_bootstrap_complete`; universal gate blocks; same as #915 — operator runs from admin UI Retry-failed; not a PR-blocking concern |
