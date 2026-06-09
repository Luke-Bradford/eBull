"""CUSIP → instrument_id resolver via fuzzy issuer-name match (#781).

Walks ``unresolved_13f_cusips`` (populated by the 13F-HR ingester
on each unresolved CUSIP) and tries to promote rows into
``external_identifiers`` by fuzzy-matching the filer-supplied
``name_of_issuer`` against ``instruments.company_name``. Successful
matches are removed from the unresolved table; rejections are
tombstoned with ``resolution_status='unresolvable'`` so the next
run skips them.

This is the practical alternative to parsing the SEC's quarterly
Official List of Section 13(f) Securities (PDF-only, no
machine-readable feed). Every CUSIP that appears in any 13F-HR
filing is by definition a 13F-eligible security; we already fetch
the holdings during the #730 ingest so the unresolved-CUSIP set
is populated naturally.

Match strategy (deliberately conservative — false positives in
``external_identifiers`` corrupt every downstream join):

  1. **Normalise** both sides:
     - Strip common corporate-form suffixes (``"INC"``, ``"CORP"``,
       ``"CO"``, ``"LTD"``, ``"LLC"``, ``"PLC"``, ``"NV"``, ``"AG"``,
       ``"SA"``, etc.) — proxies and 13F filers vary on inclusion.
     - Strip share-class suffixes (``"CL A"``, ``"CL B"``,
       ``"CLASS A"``, ``"COM"``, ``"COMMON"``, etc.) — the CUSIP
       already encodes the share class via the last digit, so the
       textual suffix is redundant noise.
     - Drop punctuation; collapse whitespace; uppercase.
  2. **Score** via a Jaro-Winkler-style similarity on the
     normalised forms. Stdlib ``difflib.SequenceMatcher.ratio()``
     is the no-dep choice here — accuracy is good enough for the
     "Berkshire Hathaway" / "Berkshire Hathaway Inc" /
     "Berkshire Hathaway Inc. Class B" trio that 13F filings
     produce in practice.
  3. **Promote** when ``ratio >= MATCH_THRESHOLD``. Below the
     threshold, mark ``resolution_status='unresolvable'``.

Deliberately *no* dependency added — stdlib ``difflib`` is enough
for v1. If recall ever tightens, ``rapidfuzz`` would be the
incremental upgrade.

Idempotent: the resolver is safe to run repeatedly. Each
successful promotion deletes the source row, so re-runs only see
unresolved + tombstoned entries. Operator can clear tombstoned
rows to force a retry once instrument coverage improves (e.g.
after a new instrument is seeded).
"""

from __future__ import annotations

import logging
import re
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import date
from difflib import SequenceMatcher
from typing import TYPE_CHECKING, Any, Final, Literal, Protocol

import psycopg
import psycopg.rows

from app.services import rewash_filings

if TYPE_CHECKING:
    from app.services.openfigi_resolver import OpenFigiMapping


class OpenFigiResolverProtocol(Protocol):
    """Structural type for the OpenFIGI sweep collaborator.

    The concrete implementation lives in
    ``app/services/openfigi_resolver.py``; this Protocol decouples
    cusip_resolver from that module so:

      * tests can inject a fake resolver without spinning up httpx;
      * the import cycle (cusip_resolver ↔ openfigi_resolver) is
        avoided — only the sweep CALLER depends on the concrete
        class, and the sweep itself depends only on this Protocol.
    """

    def resolve_cusips(self, cusips: Iterable[str]) -> dict[str, OpenFigiMapping]: ...


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


# Similarity floor. Tuned empirically: 0.92 is conservative enough
# that "Apple Inc" doesn't match "Apple Hospitality REIT" but
# "Berkshire Hathaway" matches "BERKSHIRE HATHAWAY INC". Increasing
# to 0.95+ would push more CUSIPs into the unresolvable bucket;
# decreasing below 0.90 starts seeing common-prefix false positives.
MATCH_THRESHOLD: Final[float] = 0.92


# Corporate-form suffix patterns stripped during normalisation.
# Order matters: longer-prefix variants must precede shorter ones
# so the stripper doesn't bail out on a partial substring.
_CORPORATE_SUFFIXES: Final[tuple[str, ...]] = (
    "INCORPORATED",
    "CORPORATION",
    "COMPANY",
    "LIMITED",
    "INC.",
    "INC",
    "CORP.",
    "CORP",
    "CO.",
    "CO",
    "LTD.",
    "LTD",
    "LLC",
    "L.L.C.",
    "PLC",
    "P.L.C.",
    "NV",
    "N.V.",
    "AG",
    "A.G.",
    "SA",
    "S.A.",
    "GMBH",
    "AB",
    "OYJ",
    "ASA",
    "PTE",
    "BHD",
    "TRUST",
    "FUND",
    "HOLDINGS",
    "GROUP",
)

# Share-class / security-type suffix patterns. CUSIP already
# encodes the share class via its last digit, so the textual
# suffix is informational noise during name comparison.
_SHARE_CLASS_PATTERNS: Final[re.Pattern[str]] = re.compile(
    r"\b(?:"
    r"CLASS\s+[A-Z]"
    r"|CL\s+[A-Z]"
    r"|COM(?:MON)?\s+(?:STK|STOCK|SHARES)?"
    r"|PRF(?:D)?(?:\s+STK)?"
    r"|PFD"
    r"|ADR"
    r"|ADS"
    r"|REIT"
    r"|UNITS?"
    r"|WT"
    r"|WAR(?:RANTS?)?"
    r"|CAP(?:ITAL)?\s+STK"
    r"|RIGHTS"
    r")\b",
    re.IGNORECASE,
)

# Punctuation stripped from both sides during normalisation. Hyphens
# preserved (some legitimate company names — "PG&E", "BLOCK H&R" —
# rely on them); commas / apostrophes / parens dropped.
_PUNCTUATION_RE: Final[re.Pattern[str]] = re.compile(r"[.,'\"()\[\]/]")
_WHITESPACE_RE: Final[re.Pattern[str]] = re.compile(r"\s+")


# ---------------------------------------------------------------------------
# Public dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ResolveReport:
    """Per-run rollup. Drives the ops monitor's "13F coverage"
    indicator and the operator-facing CLI summary.

    Counter semantics:

      * ``promotions`` — new mappings inserted into
        ``external_identifiers``. The operator-facing chip moves
        on this counter, not on already-resolved or conflict
        outcomes.
      * ``already_resolved`` — CUSIPs that were already mapped to
        the same ``instrument_id`` (another path beat the resolver
        to it). Source backlog row deleted; no new
        ``external_identifiers`` row written.
      * ``tombstoned_unresolvable`` / ``ambiguous`` / ``conflict``
        — three distinct tombstone reasons (see
        :data:`unresolved_13f_cusips.resolution_status`).
    """

    candidates_seen: int
    promotions: int
    already_resolved: int
    tombstoned_unresolvable: int
    tombstoned_ambiguous: int
    tombstoned_conflict: int

    @property
    def tombstones(self) -> int:
        """Total tombstones across all reason buckets — back-compat
        accessor for tests / callers that don't care about the
        breakdown."""
        return self.tombstoned_unresolvable + self.tombstoned_ambiguous + self.tombstoned_conflict


@dataclass(frozen=True)
class _Match:
    """Internal — best-match result for one unresolved CUSIP."""

    instrument_id: int
    company_name: str
    score: float


# ---------------------------------------------------------------------------
# Bulk-path unresolved-CUSIP writer (#1233 PR-1a)
# ---------------------------------------------------------------------------


# Valid ``source`` values for the bulk-path writer. Mirrors the
# CHECK constraint on ``unresolved_13f_cusips.source`` added by
# sql/164. Keep in sync with PR-1b if new bulk sources are added.
BulkCusipSource = Literal["bulk_13f_dataset", "bulk_nport_dataset"]


def load_bulk_cusip_map(conn: psycopg.Connection[Any]) -> dict[str, int]:
    """Preload all CUSIP → instrument_id mappings (SEC + OpenFIGI) into a dict.

    Shared by the 13F and N-PORT bulk dataset ingesters (#1437 — was
    duplicated lock-step in both modules). INFOTABLE rows can number in
    the millions per archive; doing one indexed DB query per row is the
    dominant cost of the bulk ingest. Loading the entire map once at the
    top of ``ingest_*_dataset_archive`` collapses millions of round
    trips into one SELECT. CUSIP universe is bounded (~13k SEC Form 13F
    securities list rows + ~1500 universe instruments + OpenFIGI
    promotions), so the dict fits comfortably in memory.

    Provider widening (#1233 PR-1b): the WHERE filter reads
    ``provider IN ('sec', 'openfigi')`` so post-bulk-sweep OpenFIGI
    promotions become visible to the next bulk ingest pass without
    a schema-level UNION view. The SEC-curated mappings remain
    authoritative — ``ORDER BY is_primary DESC, external_identifier_id ASC``
    means a SEC ``is_primary=TRUE`` row wins over an OpenFIGI
    ``is_primary=FALSE`` row when both exist for the same CUSIP
    (the OpenFIGI sweep deliberately writes ``is_primary=FALSE``;
    see ``_promote_openfigi_mapping``). The CUSIP-uniqueness constraint
    (``uq_external_identifiers_provider_value`` on
    ``(provider, identifier_type, identifier_value)``) means at most
    one row per (provider, cusip), so the dedup happens at the row
    level — ``setdefault`` keeps the first-seen mapping per CUSIP
    after the ORDER BY.
    """
    out: dict[str, int] = {}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT identifier_value, instrument_id
            FROM external_identifiers
            WHERE provider IN ('sec', 'openfigi') AND identifier_type = 'cusip'
            ORDER BY is_primary DESC, external_identifier_id ASC
            """,
        )
        for row in cur.fetchall():
            cusip, instrument_id = row
            out.setdefault(str(cusip).strip().upper(), int(instrument_id))
    return out


def record_unresolved_cusip_from_bulk(
    conn: psycopg.Connection[Any],
    *,
    cusip: str,
    filer_cik: str,
    period_end: date,
    source: BulkCusipSource,
) -> None:
    """Bulk-path unresolved-CUSIP write. Idempotent.

    Distinct from the legacy :func:`_record_unresolved_cusip`
    (``app/services/institutional_holdings.py``) which requires
    ``name_of_issuer`` + ``accession_number`` — both available on
    the per-filing path. The bulk Form 13F / N-PORT datasets carry
    ``period_end`` + filer CIK but **not** issuer name (the dataset
    INFOTABLE row publishes CUSIP only; issuer name is filled later
    by the PR-1b OpenFIGI sweep which writes back the ``name`` field
    OpenFIGI returns).

    Idempotent on the partial UNIQUE INDEX
    ``unresolved_13f_cusips_bulk_idx`` (sql/164). A second call with
    the same ``(cusip, filer_cik, period_end, source)`` tuple is a
    no-op. A call with a different ``(filer_cik, period_end, source)``
    for the same CUSIP inserts a new row — the bulk path records
    *every* (cusip × filer × period) observation so the sweep can
    rewash the right accessions once the CUSIP resolves.

    The caller is responsible for committing the transaction.

    Note: this single-row writer is retained for back-compat (tests
    + ad-hoc callers). The bulk ingest paths (13F dataset, NPORT
    dataset) batch-flush via :func:`flush_unresolved_cusips_bulk`
    instead — #1233 PR for #1295 — which is ~200× faster on large
    unresolved sets by replacing per-row INSERT + SAVEPOINT with a
    single COPY + INSERT...SELECT...ON CONFLICT.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO unresolved_13f_cusips (
                cusip, name_of_issuer, last_accession_number,
                filer_cik, period_end, source
            )
            VALUES (
                %(cusip)s, NULL, NULL,
                %(filer_cik)s, %(period_end)s, %(source)s
            )
            ON CONFLICT (
                cusip,
                COALESCE(filer_cik, ''),
                COALESCE(period_end, '0001-01-01'::date),
                COALESCE(source, '')
            ) WHERE source IS NOT NULL DO NOTHING
            """,
            {
                "cusip": cusip.strip().upper(),
                "filer_cik": filer_cik.strip(),
                "period_end": period_end,
                "source": source,
            },
        )


# Internal — staging-table column list for the bulk flush. Order
# pinned because both ``cur.copy(...)`` writes and the
# ``INSERT...SELECT`` projection bind by position.
_BULK_STG_COLS: Final = ("cusip", "filer_cik", "period_end", "source")


def flush_unresolved_cusips_bulk(
    conn: psycopg.Connection[Any],
    buffer: Iterable[tuple[str, str, date]],
    *,
    source: BulkCusipSource,
) -> int:
    """Drain accumulated ``(cusip, filer_cik, period_end)`` triples
    into ``unresolved_13f_cusips`` in one COPY + INSERT...SELECT
    pass. Idempotent on the same partial UNIQUE INDEX as
    :func:`record_unresolved_cusip_from_bulk`.

    Returns the number of rows successfully written (post ON CONFLICT
    de-dup). A second flush of the same triples returns 0.

    Performance: pre-PR-1295 used per-row INSERT + SAVEPOINT (~1000
    rows/s ceiling on a 2M-row archive). The COPY + INSERT...SELECT
    shape mirrors PR-3 (#1283) and lifts the ceiling to ~30k-50k
    rows/s — empirically saves 15-30 min Phase C wall-clock on a
    full bootstrap with a large unresolved backlog. The flush is
    called ONCE per archive, after the main ``cur.copy()`` for the
    observations table has closed, so the cursor is free for
    sequential statements again. Single-pass iteration — buffer is
    streamed directly into COPY without materialising a normalised
    copy (memory parity with the caller's existing list).

    Safety: ``source`` is a ``Literal`` constrained by the CHECK
    constraint on ``unresolved_13f_cusips.source``. ``cusip`` is
    upper-cased + stripped here so caller-side preprocessing is
    optional. Malformed rows are filtered (empty cusip/filer or
    NULL period_end) so the helper degrades to a no-op rather than
    aborting on bad input.

    Transaction safety: the helper does NOT own a savepoint — if
    the INSERT (or any earlier statement) raises, the caller's
    open transaction enters ``InFailedSqlTransaction``. Callers
    that need flush-failure isolation MUST wrap the call in
    ``with conn.transaction():`` so a failure rolls back to a
    savepoint without poisoning the outer archive tx. Both bulk
    ingesters (13F + NPORT) do this in their ``_flush_unresolved_buffer``
    wrappers (#1295).
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TEMP TABLE IF NOT EXISTS _stg_unresolved_cusips_bulk (
                cusip       TEXT NOT NULL,
                filer_cik   TEXT,
                period_end  DATE,
                source      TEXT
            ) ON COMMIT DROP
            """
        )
        # If the staging table was created earlier in the same tx
        # (e.g. by a prior helper invocation in tests) clear it so
        # this flush sees only its own rows.
        cur.execute("TRUNCATE _stg_unresolved_cusips_bulk")

        copy_sql = (
            "COPY _stg_unresolved_cusips_bulk ("
            + ", ".join(_BULK_STG_COLS)
            + ") FROM STDIN WITH (FORMAT text, ON_ERROR ignore, LOG_VERBOSITY verbose)"
        )
        # Stream the caller's buffer directly into COPY. No
        # materialised second list — peak memory is one buffer
        # entry per row regardless of buffer length. Filter empty /
        # incomplete triples in the same pass.
        staged = 0
        with cur.copy(copy_sql) as copy:
            for cusip, filer_cik, period_end in buffer:
                if not cusip or not filer_cik or period_end is None:
                    continue
                copy.write_row(
                    (
                        cusip.strip().upper(),
                        filer_cik.strip(),
                        period_end,
                        source,
                    )
                )
                staged += 1
        if staged == 0:
            return 0

        # Drain staging into the target via INSERT...SELECT...ON
        # CONFLICT. The partial UNIQUE INDEX
        # ``unresolved_13f_cusips_bulk_idx`` enforces the dedup; the
        # explicit ``WHERE source IS NOT NULL`` disambiguates from
        # the legacy partial UNIQUE INDEX
        # ``unresolved_13f_cusips_legacy_idx`` on ``(cusip) WHERE
        # source IS NULL`` (sql/164).
        cur.execute(
            """
            INSERT INTO unresolved_13f_cusips (
                cusip, name_of_issuer, last_accession_number,
                filer_cik, period_end, source
            )
            SELECT
                cusip, NULL, NULL,
                filer_cik, period_end, source
            FROM _stg_unresolved_cusips_bulk
            ON CONFLICT (
                cusip,
                COALESCE(filer_cik, ''),
                COALESCE(period_end, '0001-01-01'::date),
                COALESCE(source, '')
            ) WHERE source IS NOT NULL DO NOTHING
            """
        )
        return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0


# Internal — staging-table column list for the resolved-marker delete.
# Order pinned because the ``cur.copy(...)`` write binds by position.
_RESOLVED_STG_COLS: Final = ("cusip", "filer_cik", "period_end")


def in_window_bulk_markers_exist(
    conn: psycopg.Connection[Any],
    source: BulkCusipSource,
    cutoff: date,
) -> bool:
    """Preflight gate for :func:`delete_resolved_bulk_markers` (#1399).

    True iff any bulk marker for ``source`` has ``period_end >= cutoff``.
    A resolved observation is always in-window (the ingest materialise
    gate rejects out-of-retention rows), so a marker the ingest could
    delete sits at ``period_end >= cutoff``. When none exist the
    ingester skips collection entirely — no set growth, no temp table,
    no DELETE. Post-PR1 (#1398 retention purge) the in-window marker set
    is small and shrinking, so most steady-state runs short-circuit
    here, keeping the inline delete from building an archive-scale temp
    that matches ~0 rows (Codex ckpt-1 efficiency gate).
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT EXISTS (SELECT 1 FROM unresolved_13f_cusips "
            "WHERE source = %(source)s AND period_end >= %(cutoff)s)",
            {"source": source, "cutoff": cutoff},
        )
        row = cur.fetchone()
    return bool(row and row[0])


def reconcile_survived_markers(
    markers: Iterable[tuple[str, str, date, int]],
    survived_obs_keys: set[tuple[int, str, date]],
) -> set[tuple[str, str, date]]:
    """Keep only markers whose observation survived the COPY into staging.

    The bulk ingesters collect ``(cusip, filer_cik, period_end,
    instrument_id)`` for every CUSIP that resolves during the archive
    walk — appended right after ``copy.write_row``, BEFORE the
    ``ON_ERROR ignore`` COPY confirms the row coerced into the staging
    table. A wire-skipped holding therefore never reaches staging, so
    deleting its marker would drop a hint for an observation that never
    materialised (#1399, Codex ckpt-2 HIGH).

    This filters the collected set against ``survived_obs_keys`` — the
    DISTINCT ``(instrument_id, filer_cik, period_end)`` obs grain read
    back from the per-archive staging table AFTER the drain. A marker is
    deletable iff a row for its obs grain actually landed. Returns the
    marker-grain ``(cusip, filer_cik, period_end)`` set to delete.
    """
    return {
        (cusip, filer_cik, period_end)
        for (cusip, filer_cik, period_end, instrument_id) in markers
        if (instrument_id, filer_cik, period_end) in survived_obs_keys
    }


def delete_resolved_bulk_markers(
    conn: psycopg.Connection[Any],
    buffer: Iterable[tuple[str, str, date]],
    *,
    source: BulkCusipSource,
) -> int:
    """Delete ``unresolved_13f_cusips`` bulk markers a later run resolved.

    When a bulk ingest materialises an observation for a ``(cusip,
    filer_cik, period_end)`` that an EARLIER run recorded as unresolved
    (the CUSIP was unmapped then, is mapped now), the marker row is
    redundant — the observation now exists. Drains the buffered
    now-resolved triples into a TEMP staging table and deletes the EXACT
    matching bulk rows in one ``DELETE ... USING`` pass.

    Precise grain (spec §2a): the match is the full bulk-marker key
    ``(source, cusip, filer_cik, period_end)`` with the same COALESCE
    sentinels as ``unresolved_13f_cusips_bulk_idx`` (sql/164) so the
    planner uses that index and no coarser observation row can
    false-positive a delete. ``source`` is always non-null for bulk
    markers (the index is partial ``WHERE source IS NOT NULL``), so the
    literal equality is exact — no COALESCE needed on it.

    Malformed triples (empty cusip/filer or NULL period) are filtered in
    the COPY pass, mirroring :func:`flush_unresolved_cusips_bulk`; both
    callers only buffer fully-populated triples so this is belt-and-
    braces.

    Transaction safety: like :func:`flush_unresolved_cusips_bulk`, this
    helper does NOT own a savepoint — a raise leaves the caller's tx in
    ``InFailedSqlTransaction``. Callers MUST wrap in
    ``with conn.transaction():`` for failure isolation. A delete failure
    is non-fatal: a redundant marker is harmless (the retention purge or
    a later run reclaims it) — markers are a hint, not source of truth.

    Returns the number of rows deleted.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TEMP TABLE IF NOT EXISTS _stg_resolved_markers (
                cusip       TEXT NOT NULL,
                filer_cik   TEXT,
                period_end  DATE
            ) ON COMMIT DROP
            """
        )
        # Clear any rows left by a prior invocation in the same tx
        # (tests, or a multi-archive caller reusing the connection).
        cur.execute("TRUNCATE _stg_resolved_markers")

        copy_sql = (
            "COPY _stg_resolved_markers ("
            + ", ".join(_RESOLVED_STG_COLS)
            + ") FROM STDIN WITH (FORMAT text, ON_ERROR ignore, LOG_VERBOSITY verbose)"
        )
        staged = 0
        with cur.copy(copy_sql) as copy:
            for cusip, filer_cik, period_end in buffer:
                if not cusip or not filer_cik or period_end is None:
                    continue
                copy.write_row((cusip.strip().upper(), filer_cik.strip(), period_end))
                staged += 1
        if staged == 0:
            return 0

        # ``source`` equality is exact (bulk markers are always
        # non-null source). COALESCE on filer_cik/period_end mirrors the
        # bulk partial UNIQUE INDEX expression so the planner can use it.
        cur.execute(
            """
            DELETE FROM unresolved_13f_cusips u
             USING _stg_resolved_markers s
             WHERE u.source = %(source)s
               AND u.cusip = s.cusip
               AND COALESCE(u.filer_cik, '') = COALESCE(s.filer_cik, '')
               AND COALESCE(u.period_end, '0001-01-01'::date)
                   = COALESCE(s.period_end, '0001-01-01'::date)
            """,
            {"source": source},
        )
        return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0


# ---------------------------------------------------------------------------
# Normalisation
# ---------------------------------------------------------------------------


def _normalise_name(raw: str) -> str:
    """Normalise an issuer / instrument company name for fuzzy
    comparison.

    Pipeline (order matters):
      1. Uppercase + strip.
      2. Drop bracketed / parenthesised qualifiers ("(NEW)").
      3. Strip share-class suffixes via :data:`_SHARE_CLASS_PATTERNS`.
      4. Drop punctuation per :data:`_PUNCTUATION_RE`.
      5. Strip every trailing corporate-form suffix token in turn
         (``"Vanguard Group Holdings Trust"`` -> ``"VANGUARD"``).
      6. Collapse whitespace.

    Returns the empty string when nothing is left after stripping —
    the resolver treats that as "unmatchable" rather than letting
    a degenerate result feed the similarity scorer.
    """
    if not raw:
        return ""

    # 1. Uppercase + trim.
    s = raw.upper().strip()

    # 2. Drop bracketed qualifiers (typical 13F / proxy footnotes:
    # ``"BERKSHIRE HATHAWAY INC (NEW)"``, ``"AAPL [HOLDINGS]"``).
    s = re.sub(r"[(\[][^)\]]*[)\]]", " ", s)

    # 3. Strip share-class indicators (CUSIPs already encode class).
    s = _SHARE_CLASS_PATTERNS.sub(" ", s)

    # 4. Drop punctuation.
    s = _PUNCTUATION_RE.sub(" ", s)

    # 5. Strip every trailing corporate-form suffix token. The
    #    while loop handles names like ``"Vanguard Group Holdings
    #    Trust"`` where the operator-facing label stacks several
    #    structural words; stripping just one would leave residual
    #    noise that hurts comparison. Bot review caught the prior
    #    "single trailing" comment.
    tokens = s.split()
    while tokens and tokens[-1] in _CORPORATE_SUFFIXES:
        tokens.pop()
    s = " ".join(tokens)

    # 6. Collapse whitespace.
    return _WHITESPACE_RE.sub(" ", s).strip()


def _similarity(a: str, b: str) -> float:
    """Return a 0..1 similarity ratio between two normalised names.

    Uses :func:`difflib.SequenceMatcher.ratio()` — a stdlib
    no-dep choice that's good enough for the bulk of issuer-name
    variation seen in 13F filings. ``rapidfuzz`` would be the
    incremental upgrade if recall tightens.
    """
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------


def _select_pending_unresolved(
    conn: psycopg.Connection[tuple],
    *,
    limit: int = 500,
) -> list[dict[str, Any]]:
    """Return up to ``limit`` unresolved CUSIPs that haven't been
    tombstoned, ordered by observation count DESC so the
    highest-leverage entries resolve first.

    Scoped to legacy rows (``source IS NULL``) so the post-sql/164
    bulk rows (which carry NULL ``name_of_issuer`` / NULL
    ``last_accession_number`` by design) are NOT picked up by the
    legacy fuzzy-name resolver — it would call ``_normalise_name(None)``
    and tombstone every bulk row as ``unresolvable``. PR-1b's
    OpenFIGI sweep owns the bulk partition independently.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT cusip, name_of_issuer, observation_count
            FROM unresolved_13f_cusips
            WHERE resolution_status IS NULL
              AND source IS NULL
            ORDER BY observation_count DESC, last_observed_at DESC
            LIMIT %(limit)s
            """,
            {"limit": limit},
        )
        return list(cur.fetchall())


def _select_instrument_candidates(
    conn: psycopg.Connection[tuple],
) -> list[tuple[int, str]]:
    """Return ``(instrument_id, normalised_company_name)`` pairs for
    every instrument with a non-empty company name. Normalisation
    happens in Python so the resolver's match logic is the single
    canonical source.

    Pulling the whole instruments universe per resolver pass is
    fine — a typical eBull deployment carries low thousands of
    instruments, well under the row threshold where SQL-side
    indexed search would matter.
    """
    pairs: list[tuple[int, str]] = []
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT instrument_id, company_name
            FROM instruments
            WHERE company_name IS NOT NULL
              AND company_name <> ''
            """
        )
        for row in cur.fetchall():
            normalised = _normalise_name(str(row[1]))
            if normalised:
                pairs.append((int(row[0]), normalised))
    return pairs


def _best_match(
    *,
    target: str,
    candidates: list[tuple[int, str]],
) -> tuple[_Match | None, bool]:
    """Return ``(best_match, is_ambiguous)``.

    ``best_match`` is the highest-similarity candidate or ``None``
    when no candidate meets the score floor. ``is_ambiguous`` is
    ``True`` when two or more distinct candidates tied at the top
    score — common collision: ``"ALPHABET INC CL A"`` and
    ``"ALPHABET INC CL C"`` both normalise to ``"ALPHABET"`` after
    share-class strip, so an unresolved CUSIP ``"ALPHABET INC CL C"``
    has two equally-good candidates and the resolver cannot pick one
    unambiguously. Codex pre-push review caught the prior code's
    arbitrary first-wins behaviour.

    Linear scan is fine at the candidate counts involved (low
    thousands × ~hundreds of unresolved CUSIPs per pass = sub-second
    on commodity hardware).
    """
    if not target or not candidates:
        return (None, False)

    top_score = 0.0
    top_matches: list[_Match] = []
    for instrument_id, candidate in candidates:
        score = _similarity(target, candidate)
        if score > top_score:
            top_score = score
            top_matches = [_Match(instrument_id=instrument_id, company_name=candidate, score=score)]
        elif score == top_score and score > 0.0:
            top_matches.append(_Match(instrument_id=instrument_id, company_name=candidate, score=score))

    if not top_matches:
        return (None, False)

    is_ambiguous = len({m.instrument_id for m in top_matches}) > 1
    # Return the first top-scoring match for diagnostic logging
    # purposes; the caller treats ``is_ambiguous=True`` as a
    # tombstone signal regardless of which match was picked.
    return (top_matches[0], is_ambiguous)


def _promote_to_external_identifier(
    conn: psycopg.Connection[tuple],
    *,
    cusip: str,
    instrument_id: int,
) -> str:
    """Try to promote a resolved CUSIP into ``external_identifiers``.

    Returns one of:

      * ``"inserted"`` — a new mapping was created; the source row
        is deleted from ``unresolved_13f_cusips``.
      * ``"already_resolved"`` — the CUSIP was already mapped to
        the SAME instrument_id (another path beat us to it). Source
        row deleted; not counted as a new promotion.
      * ``"conflict"`` — the CUSIP was already mapped to a
        DIFFERENT instrument_id. The existing mapping is preserved
        (we never silently overwrite), the resolver does NOT delete
        the source row, and the caller tombstones it with
        ``resolution_status='conflict'`` so an operator can audit.
        Codex pre-push review caught the prior code silently
        treating a conflicting pre-existing mapping as success.

    ``is_primary`` is FALSE on the new INSERT path because the
    curated mapping (when one exists) takes precedence; the
    resolver only adds entries that were missing entirely.
    """
    cusip_norm = cusip.strip().upper()

    # Two-step: probe first, write second. ON CONFLICT ... DO NOTHING
    # would silently swallow the conflict and we couldn't distinguish
    # "race-loss / already-mapped to same iid" from "wrong-mapping".
    # The probe + write pair is wrapped in the same transaction.
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT instrument_id FROM external_identifiers
            WHERE provider = 'sec'
              AND identifier_type = 'cusip'
              AND identifier_value = %s
            """,
            (cusip_norm,),
        )
        existing = cur.fetchone()

    if existing is not None:
        existing_iid = int(existing[0])
        if existing_iid == instrument_id:
            # Already mapped to the same instrument — source row is
            # safely redundant. Scope the DELETE to the legacy
            # partition (sql/164 #1233 PR-1a): a CUSIP may have
            # multiple bulk rows whose lifecycle is owned by the
            # PR-1b OpenFIGI sweep, not by this legacy resolver.
            conn.execute(
                "DELETE FROM unresolved_13f_cusips WHERE cusip = %s AND source IS NULL",
                (cusip_norm,),
            )
            return "already_resolved"
        # Conflicting pre-existing mapping. Keep the existing row;
        # leave the source row in place so the tombstone path can
        # mark it 'conflict' for operator audit.
        return "conflict"

    conn.execute(
        """
        INSERT INTO external_identifiers (
            instrument_id, provider, identifier_type, identifier_value, is_primary
        ) VALUES (%(iid)s, 'sec', 'cusip', %(cusip)s, FALSE)
        """,
        {"iid": instrument_id, "cusip": cusip_norm},
    )
    # Legacy partition only — see comment above.
    conn.execute(
        "DELETE FROM unresolved_13f_cusips WHERE cusip = %s AND source IS NULL",
        (cusip_norm,),
    )
    return "inserted"


_TombstoneStatus = str  # one of 'unresolvable' | 'ambiguous' | 'conflict'


def _tombstone(
    conn: psycopg.Connection[tuple],
    *,
    cusip: str,
    status: _TombstoneStatus,
) -> None:
    """Mark a CUSIP for skip on subsequent runs with a reason tag.

    Statuses (must match the schema CHECK on
    ``unresolved_13f_cusips.resolution_status``):

      * ``'unresolvable'`` — no candidate met the similarity floor.
      * ``'ambiguous'`` — multiple candidates tied at the top score
        (e.g. Alphabet Class A vs Class C share-class collisions).
        Operator disambiguates via the curated mapping seed.
      * ``'conflict'`` — ``external_identifiers`` already maps this
        CUSIP to a DIFFERENT instrument_id. Operator audits which
        side is correct before clearing.

    The row stays in the table for operator audit; clearing
    ``resolution_status`` forces a retry on the next run.

    Scoped to the legacy partition (``source IS NULL``) — the bulk
    rows (sql/164 #1233 PR-1a) have their own status lifecycle owned
    by PR-1b's OpenFIGI sweep. A legacy tombstone must NOT mutate
    bulk rows for the same CUSIP.
    """
    conn.execute(
        """
        UPDATE unresolved_13f_cusips
        SET resolution_status = %s,
            last_observed_at = NOW()
        WHERE cusip = %s
          AND source IS NULL
        """,
        (status, cusip.strip().upper()),
    )


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def resolve_unresolved_cusips(
    conn: psycopg.Connection[tuple],
    *,
    limit: int = 500,
    threshold: float = MATCH_THRESHOLD,
) -> ResolveReport:
    """Run one resolver pass.

    Reads up to ``limit`` pending CUSIPs from
    ``unresolved_13f_cusips``, fuzzy-matches each against the
    instruments universe, and promotes confident matches into
    ``external_identifiers``. Each match decision (promote /
    tombstone) commits in its own write — caller is responsible
    for committing the transaction at the end.

    ``threshold`` defaults to :data:`MATCH_THRESHOLD`; tests
    override it for deterministic fuzzy-edge cases.
    """
    pending = _select_pending_unresolved(conn, limit=limit)
    if not pending:
        return ResolveReport(
            candidates_seen=0,
            promotions=0,
            already_resolved=0,
            tombstoned_unresolvable=0,
            tombstoned_ambiguous=0,
            tombstoned_conflict=0,
        )

    candidates = _select_instrument_candidates(conn)
    if not candidates:
        # Empty instruments table — every CUSIP is unresolvable
        # by definition. Tombstone them all so the next run skips
        # them; clearing rows is the operator path to retry once
        # instruments are seeded.
        for row in pending:
            _tombstone(conn, cusip=str(row["cusip"]), status="unresolvable")
        return ResolveReport(
            candidates_seen=len(pending),
            promotions=0,
            already_resolved=0,
            tombstoned_unresolvable=len(pending),
            tombstoned_ambiguous=0,
            tombstoned_conflict=0,
        )

    promotions = 0
    already_resolved = 0
    unresolvable = 0
    ambiguous = 0
    conflict = 0

    for row in pending:
        cusip = str(row["cusip"])
        target = _normalise_name(str(row["name_of_issuer"]))
        if not target:
            # Issuer name normalised to empty (extreme edge — pure
            # punctuation / suffix-only string). Tombstone.
            _tombstone(conn, cusip=cusip, status="unresolvable")
            unresolvable += 1
            continue

        best, is_ambiguous = _best_match(target=target, candidates=candidates)
        if best is None or best.score < threshold:
            _tombstone(conn, cusip=cusip, status="unresolvable")
            unresolvable += 1
            continue

        if is_ambiguous:
            # Two or more distinct instruments tied at the top
            # score (typical share-class collision: Alphabet CL A
            # vs CL C). Refuse to pick arbitrarily — operator
            # disambiguates via the curated mapping seed.
            _tombstone(conn, cusip=cusip, status="ambiguous")
            ambiguous += 1
            logger.info(
                "13F CUSIP resolver: ambiguous %s (score=%.3f, %r); operator must disambiguate",
                cusip,
                best.score,
                target,
            )
            continue

        outcome = _promote_to_external_identifier(conn, cusip=cusip, instrument_id=best.instrument_id)
        if outcome == "inserted":
            promotions += 1
            logger.info(
                "13F CUSIP resolver: promoted %s -> instrument_id=%d (score=%.3f, %r ~ %r)",
                cusip,
                best.instrument_id,
                best.score,
                target,
                best.company_name,
            )
        elif outcome == "already_resolved":
            already_resolved += 1
        else:  # 'conflict'
            _tombstone(conn, cusip=cusip, status="conflict")
            conflict += 1
            logger.warning(
                "13F CUSIP resolver: conflict %s — existing mapping differs from match %d",
                cusip,
                best.instrument_id,
            )

    return ResolveReport(
        candidates_seen=len(pending),
        promotions=promotions,
        already_resolved=already_resolved,
        tombstoned_unresolvable=unresolvable,
        tombstoned_ambiguous=ambiguous,
        tombstoned_conflict=conflict,
    )


# ---------------------------------------------------------------------------
# extid sweep — recover unresolved CUSIPs that already have a curated mapping
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SweepReport:
    """Per-run rollup for :func:`sweep_resolvable_unresolved_cusips`.

    Counter semantics:

      * ``candidates_seen`` — rows whose CUSIP joined a row in
        ``external_identifiers`` (provider='sec', identifier_type='cusip')
        and were still pending (``resolution_status IS NULL``).
      * ``promoted`` — rows transitioned to
        ``resolution_status='resolved_via_extid'`` by this sweep.
      * ``rewashed`` — rewash of ``last_accession_number`` returned
        ``True`` (typed-table upsert ran or rescue-cohort log entry
        recorded).
      * ``rewash_deferred`` — rewash returned ``False`` (raw body
        absent, no existing typed row / ingest log row, or the
        any-CUSIP-still-unresolved partial path in
        ``_apply_13f_infotable`` deferred the replace). The extid
        promotion stays — a subsequent bulk ``run_rewash`` will pick
        the accession up once #740 closes the remaining CUSIP gap.
      * ``rewash_failed`` — rewash raised an exception (parser
        regression or DB error). The extid promotion stays; the
        accession is logged for operator audit.
    """

    candidates_seen: int
    promoted: int
    rewashed: int
    rewash_deferred: int
    rewash_failed: int


def _select_resolvable_via_extid(
    conn: psycopg.Connection[tuple],
    *,
    limit: int,
) -> list[dict[str, Any]]:
    """Return up to ``limit`` pending unresolved CUSIPs whose CUSIP
    already has a row in ``external_identifiers``. Ordered by
    observation_count DESC so the highest-leverage entries (Fortune-100
    names with hundreds of stranded observations) resolve first.

    Scoped to legacy rows (``source IS NULL``) — the extid sweep
    relies on ``last_accession_number`` to drive the per-filing
    rewash, and bulk rows (sql/164 #1233 PR-1a) leave that NULL
    by design. PR-1b's OpenFIGI sweep handles the bulk partition
    via its own re-ingest path (``rewash_bulk_source_filings``).
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT u.cusip,
                   u.last_accession_number,
                   ei.instrument_id
            FROM unresolved_13f_cusips u
            JOIN external_identifiers ei
              ON ei.identifier_value = u.cusip
             AND ei.provider = 'sec'
             AND ei.identifier_type = 'cusip'
            WHERE u.resolution_status IS NULL
              AND u.source IS NULL
            ORDER BY u.observation_count DESC, u.last_observed_at DESC
            LIMIT %(limit)s
            """,
            {"limit": limit},
        )
        return list(cur.fetchall())


def sweep_resolvable_unresolved_cusips(
    conn: psycopg.Connection[tuple],
    *,
    limit: int = 1000,
) -> SweepReport:
    """Sweep ``unresolved_13f_cusips`` for rows whose CUSIP already
    matches an ``external_identifiers`` mapping (#788 / #836).

    Recovers the race-loss path: 13F-HR holdings ingested *before*
    the CUSIP backfill landed end up tombstoned in
    ``unresolved_13f_cusips`` with no link to ``institutional_holdings``.
    Once the backfill (or a curated mapping) populates the
    corresponding ``external_identifiers`` row, those tombstones are
    immediately resolvable — but nothing re-triggers the typed-table
    upsert because the original ingest run is long done. This sweep
    closes the loop:

      1. Find every pending unresolved row whose CUSIP already exists
         in ``external_identifiers`` (provider='sec',
         identifier_type='cusip').
      2. Mark the row ``resolution_status='resolved_via_extid'`` so a
         second pass is a no-op.
      3. Trigger ``rewash_filings._rewash_13f_accession`` against the
         row's ``last_accession_number`` so the now-resolvable holding
         lands in ``institutional_holdings``.

    Idempotent: a second invocation finds zero pending rows because
    every match was tombstoned on the first pass. The caller is
    responsible for committing the outer transaction at the end —
    matches the contract of :func:`resolve_unresolved_cusips`.

    The mark and the rewash run inside per-row savepoints so a single
    bad accession (parser regression, raw body absent, etc.) doesn't
    abort the rest of the sweep. The mark always lands on the outer
    transaction; the rewash side-effect is rolled back to its
    savepoint on exception. The extid promotion is recorded
    independent of the rewash outcome — the mapping in
    ``external_identifiers`` is authoritative regardless of whether
    the typed-table upsert ran.

    ``limit`` caps the per-pass workload. Default 1000 is
    deliberately higher than the resolver's 500 because the sweep is
    cheap (no fuzzy scoring; one indexed JOIN) and the operator
    audit found ~119 stranded names today — even a single full pass
    drains the backlog. Subsequent runs only see new race-loss
    arrivals.
    """
    pending = _select_resolvable_via_extid(conn, limit=limit)
    if not pending:
        return SweepReport(
            candidates_seen=0,
            promoted=0,
            rewashed=0,
            rewash_deferred=0,
            rewash_failed=0,
        )

    promoted = 0
    rewashed = 0
    rewash_deferred = 0
    rewash_failed = 0

    for row in pending:
        cusip = str(row["cusip"]).strip().upper()
        accession = str(row["last_accession_number"])

        # Mark first inside its own savepoint so the extid promotion
        # lands even if the rewash blows up below. The conditional
        # ``resolution_status IS NULL`` guards against a concurrent
        # writer that already tombstoned the row — keeps the sweep
        # idempotent under concurrency.
        #
        # Codex pre-push review caught the prior version which
        # incremented ``promoted`` and triggered rewash regardless of
        # the rowcount: a concurrent sweep that already promoted the
        # same row would race-lose here, but the loser still claimed
        # work it didn't do AND re-ran the rewash. Gate on rowcount
        # so the loser cleanly skips.
        with conn.transaction():
            with conn.cursor() as cur:
                # Legacy partition only (sql/164 #1233 PR-1a) — the
                # ``last_accession_number`` rewash semantics don't
                # apply to bulk rows; PR-1b's OpenFIGI sweep owns
                # that lifecycle separately.
                cur.execute(
                    """
                    UPDATE unresolved_13f_cusips
                    SET resolution_status = 'resolved_via_extid',
                        last_observed_at = NOW()
                    WHERE cusip = %s
                      AND resolution_status IS NULL
                      AND source IS NULL
                    """,
                    (cusip,),
                )
                rowcount = cur.rowcount
        if rowcount == 0:
            # Concurrent winner already promoted (or operator manually
            # tombstoned between our SELECT and UPDATE). Skip both the
            # counter bump and the rewash trigger — repeating someone
            # else's work would distort the report and (more importantly)
            # let two sweeps clobber each other in
            # ``_apply_13f_infotable``.
            continue
        promoted += 1

        # Rewash in its own savepoint. A parser regression on one
        # accession must not poison the outer sweep; isolate via
        # nested transaction. Note: rewash_filings._rewash_13f_accession
        # may itself raise RewashParseError on parser regression —
        # we count those as failures and continue.
        try:
            with conn.transaction():
                applied = rewash_filings._rewash_13f_accession(
                    conn,
                    accession_number=accession,
                )
            if applied:
                rewashed += 1
                logger.info(
                    "cusip extid sweep: rewashed accession=%s for cusip=%s -> instrument_id=%s",
                    accession,
                    cusip,
                    row["instrument_id"],
                )
            else:
                rewash_deferred += 1
                logger.info(
                    "cusip extid sweep: rewash deferred accession=%s for cusip=%s "
                    "(raw body missing OR partial CUSIP gap)",
                    accession,
                    cusip,
                )
        except Exception:  # noqa: BLE001 — single-accession failure must not abort the sweep
            logger.exception(
                "cusip extid sweep: rewash failed accession=%s cusip=%s",
                accession,
                cusip,
            )
            rewash_failed += 1

    return SweepReport(
        candidates_seen=len(pending),
        promoted=promoted,
        rewashed=rewashed,
        rewash_deferred=rewash_deferred,
        rewash_failed=rewash_failed,
    )


# ---------------------------------------------------------------------------
# OpenFIGI sweep — bulk-source resolution path (#1233 PR-1b)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class OpenFigiSweepReport:
    """Per-run rollup for :func:`sweep_unresolved_cusips_via_openfigi`.

    Counter semantics:

      * ``candidates_seen`` — distinct bulk-source CUSIPs selected for
        OpenFIGI lookup this pass (post-deduplication, post-LIMIT).
      * ``resolved`` — entries OpenFIGI returned with a US-primary
        common-stock ticker.
      * ``promoted`` — resolutions that matched an existing
        ``instruments.symbol`` and wrote a new
        ``external_identifiers (provider='openfigi', identifier_type='cusip')``
        row. ON CONFLICT DO NOTHING means an already-existing row is
        counted as a non-promotion (audit trail says the mapping was
        already there — likely from a prior sweep).
      * ``no_instrument_match`` — OpenFIGI returned a US ticker but
        no row in ``instruments`` matches (case-insensitive). Common
        for newly-listed names not yet seeded in the universe — the
        row stays pending until either OpenFIGI returns a different
        ticker (unlikely) or the operator manually seeds.
      * ``unresolved_by_openfigi`` — OpenFIGI returned warning / error /
        no-US-row for these CUSIPs. Row stays pending.
      * ``api_errors`` — number of CUSIPs we failed to lookup due to
        transport / 429-saturation errors. Row stays pending.
    """

    candidates_seen: int
    resolved: int
    promoted: int
    no_instrument_match: int
    unresolved_by_openfigi: int
    api_errors: int


def _select_unresolved_bulk_cusips(
    conn: psycopg.Connection[tuple],
    *,
    limit: int,
) -> list[str]:
    """Return up to ``limit`` distinct bulk-source CUSIPs pending OpenFIGI
    resolution.

    Bulk rows are the post-PR-1a partition (``source IN
    ('bulk_13f_dataset', 'bulk_nport_dataset')``). The legacy partition
    (``source IS NULL``) is owned by ``sweep_resolvable_unresolved_cusips``
    above and the fuzzy resolver — this sweep ignores it.

    De-duplicated: a CUSIP may appear in N bulk rows (different
    filer × period) but only one OpenFIGI lookup is needed.

    Filters out CUSIPs that ALREADY have an ``external_identifiers``
    row (provider='sec' OR 'openfigi'). A CUSIP that was bulk-recorded
    pre-mapping but later mapped by some other path (e.g. fuzzy
    resolver running in parallel, or a prior OpenFIGI sweep) has no
    business burning rate-limit budget.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT u.cusip
              FROM unresolved_13f_cusips u
             WHERE u.resolution_status IS NULL
               AND u.source IS NOT NULL
               AND NOT EXISTS (
                   SELECT 1 FROM external_identifiers ei
                    WHERE ei.identifier_value = u.cusip
                      AND ei.identifier_type = 'cusip'
                      AND ei.provider IN ('sec', 'openfigi')
               )
             ORDER BY u.cusip
             LIMIT %(limit)s
            """,
            {"limit": limit},
        )
        return [str(row[0]).strip().upper() for row in cur.fetchall()]


def _find_instrument_by_ticker(
    conn: psycopg.Connection[tuple],
    *,
    ticker: str,
) -> int | None:
    """Resolve an OpenFIGI-returned ticker to an ``instruments.instrument_id``.

    Case-insensitive exact match against ``instruments.symbol``. Returns
    None when:
      * No row matches (newly-listed, not in universe).
      * Multiple rows match (ambiguous — surfaced by the caller as
        ``no_instrument_match`` rather than blindly picking).

    Scoped to ``is_tradable=TRUE`` so a deprecated symbol that survived
    in instruments doesn't ghost-match. Cross-source verification at
    the universe layer keeps the symbol mappings tight (#1060 etc.).
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT instrument_id
              FROM instruments
             WHERE is_tradable = TRUE
               AND UPPER(symbol) = UPPER(%(ticker)s)
             LIMIT 2
            """,
            {"ticker": ticker},
        )
        rows = cur.fetchall()
    if len(rows) != 1:
        return None
    return int(rows[0][0])


def _promote_openfigi_mapping(
    conn: psycopg.Connection[tuple],
    *,
    cusip: str,
    instrument_id: int,
) -> bool:
    """Insert the OpenFIGI mapping into ``external_identifiers``.

    Returns True iff a new row was inserted. ON CONFLICT DO NOTHING
    against the ``(provider, identifier_type, identifier_value)``
    UNIQUE — a re-run that sees a prior OpenFIGI row already in place
    returns False so the caller can record the difference between
    "promoted now" and "was already promoted".

    ``is_primary=FALSE`` because OpenFIGI is the FALLBACK provider —
    the SEC curated mapping (``provider='sec'``) is authoritative when
    both exist. The partial UNIQUE index
    ``uq_external_identifiers_primary`` would block a second
    ``is_primary=TRUE`` row for the same (instrument_id, provider,
    identifier_type) triple anyway, so primary-FALSE is the safe
    default.
    """
    with conn.cursor() as cur:
        # ON CONFLICT inference against the partial unique index
        # ``uq_external_identifiers_provider_value_non_cik`` (sql/143).
        # The partial index has a WHERE predicate
        # ``NOT (provider = 'sec' AND identifier_type = 'cik')``, so the
        # ON CONFLICT clause must include a matching WHERE predicate for
        # PG to infer the partial index — without it PG raises
        # ``there is no unique or exclusion constraint matching the
        # ON CONFLICT specification``.
        # The inserted row has ``provider='openfigi' AND
        # identifier_type='cusip'``, in the partial index's filtered set.
        cur.execute(
            """
            INSERT INTO external_identifiers (
                instrument_id, provider, identifier_type,
                identifier_value, is_primary
            ) VALUES (
                %(instrument_id)s, 'openfigi', 'cusip',
                %(cusip)s, FALSE
            )
            ON CONFLICT (provider, identifier_type, identifier_value)
                WHERE NOT (provider = 'sec' AND identifier_type = 'cik')
            DO NOTHING
            """,
            {"instrument_id": instrument_id, "cusip": cusip},
        )
        return cur.rowcount == 1


def _tombstone_bulk_rows_for_cusip(
    conn: psycopg.Connection[tuple],
    *,
    cusip: str,
    status: Literal["resolved_via_openfigi"],
) -> int:
    """Mark every bulk-source row for ``cusip`` with the given status.

    Bulk rows that share a CUSIP are independent observations
    (different filer × period). A successful OpenFIGI resolution
    means the CUSIP→instrument_id mapping is now in
    ``external_identifiers``; the next bulk ingest pass will load
    the mapping via ``_load_cusip_map`` and write into typed tables
    directly, so the unresolved rows have served their purpose.

    Scoped to ``source IS NOT NULL`` so the legacy partition is
    untouched. Returns rowcount.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE unresolved_13f_cusips
               SET resolution_status = %(status)s,
                   last_observed_at = NOW()
             WHERE cusip = %(cusip)s
               AND source IS NOT NULL
               AND resolution_status IS NULL
            """,
            {"cusip": cusip, "status": status},
        )
        return int(cur.rowcount)


def purge_unresolved_bulk_rows_outside_retention(
    conn: psycopg.Connection[tuple],
    *,
    source: BulkCusipSource,
    cutoff: date,
    limit: int = 100_000,
) -> int:
    """Delete up to ``limit`` bulk-partition rows for ``source`` whose
    ``period_end`` is older than ``cutoff`` (the per-source ingest
    retention floor). Returns the number of rows deleted (#1349 PR1).

    Such rows are markers for periods that **no pipeline will ever
    materialise** — the bulk ingest rejects ``period_end < cutoff`` at
    its retention gate (`sec_13f_dataset_ingest.py:621`; the N-PORT bulk
    ingest applies the same floor) — so the observation is permanently
    unrecoverable and the marker is pure dead weight. This period-based
    predicate is the ONLY provably-safe cleanup: it does not depend on
    the coarse ``(cusip, filer_cik, period_end, source)`` bulk-row grain,
    which cannot prove redundancy against the fine-grained
    ``ownership_institutions_observations`` /
    ``ownership_funds_observations`` tables (spec
    `docs/proposals/etl/1349-unresolved-13f-cusips-bloat.md` §2a).

    **Single bounded pass.** ``ctid``-capped at ``limit`` *physical rows*
    (not distinct CUSIPs — one high-fanout CUSIP must not blow the cap).
    Does **not** commit: the caller owns the transaction (matches
    :func:`sweep_unresolved_cusips_via_openfigi`). The caller loops +
    commits per pass to drain a large backlog without one giant txn.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM unresolved_13f_cusips
             WHERE ctid IN (
                 SELECT ctid
                   FROM unresolved_13f_cusips
                  WHERE source = %(source)s
                    AND period_end < %(cutoff)s
                  LIMIT %(limit)s
             )
            """,
            {"source": source, "cutoff": cutoff, "limit": limit},
        )
        return int(cur.rowcount)


def sweep_unresolved_cusips_via_openfigi(
    conn: psycopg.Connection[tuple],
    *,
    resolver: OpenFigiResolverProtocol,
    limit: int = 1000,
) -> OpenFigiSweepReport:
    """Resolve bulk-source unresolved CUSIPs via the OpenFIGI v3 API.

    Pipeline:

      1. Select up to ``limit`` distinct bulk-source CUSIPs whose
         ``resolution_status IS NULL`` and that do NOT already have
         an ``external_identifiers`` row (sec or openfigi).
      2. Batch-call ``resolver.resolve_cusips`` (the per-instance
         rate limiter handles OpenFIGI's tier budget).
      3. For each resolution, match the returned US-primary ticker
         against ``instruments.symbol`` (case-insensitive exact).
      4. On match: write ``external_identifiers (provider='openfigi',
         identifier_type='cusip', is_primary=FALSE)``, then tombstone
         every bulk row for that CUSIP with
         ``resolution_status='resolved_via_openfigi'``.
      5. On no-ticker / no-instrument-match: leave the row pending —
         next sweep retries (idempotent — the row stays in
         ``resolution_status IS NULL``).

    Idempotent. A second run on the same backlog re-issues the same
    OpenFIGI lookups for any rows still pending, which is wasteful
    but not incorrect; the ``LIMIT`` cap bounds the per-pass cost.

    The caller is responsible for the transaction boundary. The sweep
    issues per-row UPDATEs in the outer conn — no per-cusip savepoint
    is needed because a single failed match cannot poison the others
    (each row's promote + tombstone is its own SQL statement, and
    psycopg in non-autocommit mode auto-aborts on error which the
    caller's transaction wrapper handles).

    Surfacing ``OpenFigiTransportError`` / ``OpenFigiRateLimited``:
    if the resolver raises mid-batch, the partially-completed batch's
    successful resolutions ARE already committed-or-pending; the
    remaining CUSIPs in the failed batch + every subsequent batch
    increment ``api_errors``. The outer caller (S13 invoker) records
    ``coverage_floor_met=FALSE`` if coverage drops below 0.80, which
    correctly reflects the partial-outage state.
    """
    cusips = _select_unresolved_bulk_cusips(conn, limit=limit)
    if not cusips:
        return OpenFigiSweepReport(
            candidates_seen=0,
            resolved=0,
            promoted=0,
            no_instrument_match=0,
            unresolved_by_openfigi=0,
            api_errors=0,
        )

    resolved_count = 0
    promoted_count = 0
    no_instrument = 0
    api_errors = 0

    # Drive the OpenFIGI call inside a try/except so a transport-level
    # failure surfaces as an error count rather than aborting the
    # outer transaction. The caller's S13 invoker turns this into a
    # ``coverage_floor_met=FALSE`` outcome; no exception propagates.
    try:
        mappings = resolver.resolve_cusips(cusips)
    except Exception as exc:  # noqa: BLE001 — bound failure to error counter
        # Subclass of OpenFigiError OR an unexpected exception — both
        # collapse to api_errors. Logging captures the breakdown.
        logger.warning(
            "openfigi sweep: resolver raised %s; all %d CUSIPs marked api_error",
            type(exc).__name__,
            len(cusips),
        )
        mappings = {}
        api_errors = len(cusips)

    for cusip in cusips:
        mapping = mappings.get(cusip)
        if mapping is None:
            # api_errors already counted above (whole-batch failure)
            # OR resolver returned warning/error/no-US-row for this
            # CUSIP.
            if api_errors == 0:
                # Normal "no resolution" path — the resolver succeeded
                # but OpenFIGI doesn't know the CUSIP.
                pass
            continue
        resolved_count += 1
        instrument_id = _find_instrument_by_ticker(conn, ticker=mapping.ticker)
        if instrument_id is None:
            no_instrument += 1
            logger.info(
                "openfigi sweep: ticker %s for cusip %s has no unique instrument match",
                mapping.ticker,
                cusip,
            )
            continue
        inserted = _promote_openfigi_mapping(
            conn,
            cusip=cusip,
            instrument_id=instrument_id,
        )
        if inserted:
            promoted_count += 1
        # Tombstone all bulk rows for this CUSIP either way — even when
        # the external_identifiers row already existed (a prior sweep's
        # rowcount-0 insert), the bulk rows can be retired because the
        # mapping is now live.
        tombstoned = _tombstone_bulk_rows_for_cusip(
            conn,
            cusip=cusip,
            status="resolved_via_openfigi",
        )
        if inserted:
            logger.info(
                "openfigi sweep: promoted cusip=%s ticker=%s instrument_id=%d (%d bulk rows tombstoned)",
                cusip,
                mapping.ticker,
                instrument_id,
                tombstoned,
            )

    unresolved_by_openfigi = len(cusips) - resolved_count - api_errors if api_errors < len(cusips) else 0

    return OpenFigiSweepReport(
        candidates_seen=len(cusips),
        resolved=resolved_count,
        promoted=promoted_count,
        no_instrument_match=no_instrument,
        unresolved_by_openfigi=unresolved_by_openfigi,
        api_errors=api_errors,
    )


# ---------------------------------------------------------------------------
# Reader (exposed for ad-hoc admin queries)
# ---------------------------------------------------------------------------


def iter_pending_unresolved(
    conn: psycopg.Connection[tuple],
    *,
    limit: int = 100,
) -> Iterator[dict[str, Any]]:
    """Yield unresolved CUSIPs (resolution_status IS NULL) ordered
    by observation count DESC. Used by the operator CLI to inspect
    the resolver backlog before triggering a manual mapping
    upsert."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT cusip, name_of_issuer, observation_count,
                   last_accession_number, first_observed_at, last_observed_at
            FROM unresolved_13f_cusips
            WHERE resolution_status IS NULL
            ORDER BY observation_count DESC, last_observed_at DESC
            LIMIT %s
            """,
            (limit,),
        )
        for row in cur.fetchall():
            yield dict(row)
