"""SEC Official List of Section 13(f) Securities — CUSIP universe backfill (#914).

The Official List is the canonical free regulated source for
CUSIP↔issuer mapping for US-listed equities and ADRs. SEC publishes
it quarterly as a fixed-width TXT under
``https://www.sec.gov/files/investment/13flist{year}q{quarter}.txt``.

Why this matters: eBull's settled "free regulated-source-only"
posture (#532) means we cannot license CUSIPs from CGS. The eToro
universe carries ticker + exchange + name + CIK but never CUSIP.
Without CUSIP coverage on ``external_identifiers``, the 13F-HR
holdings ingester (#913 quarterly sweep) cannot resolve issuer
identity and drops every holding into ``unresolved_13f_cusips`` —
operator audit 2026-05-03 found 119 Fortune-100 names stranded
that way; post-#913 universe sweep that count exploded to ~377k.

This module walks the latest closed quarter's Official List, fuzzy-
matches each row's ``issuer_name`` against
``instruments.company_name`` (re-using the normaliser + similarity
threshold from :mod:`app.services.cusip_resolver`), and INSERTs
confident matches into ``external_identifiers`` (provider='sec',
identifier_type='cusip', is_primary=FALSE — the curated path takes
precedence when one exists). After the batch, calls
:func:`sweep_resolvable_unresolved_cusips` to promote any
previously-stranded 13F holdings the moment the new mapping arrives.

Non-goal: this is a forward backfill from SEC's authoritative list
to our instrument universe. The reverse path (filer-reported CUSIPs
in ``unresolved_13f_cusips`` → fuzzy match against instruments) is
already covered by ``cusip_resolver.resolve_unresolved_cusips``.
The two paths complement each other: this one is the
operator-priority bulk backfill; the resolver handles the residual
long tail.

Format of the SEC TXT (one fixed-width line per security; no
header preamble):

  cols 0..8   - 9-char CUSIP (digit-prefix US, alpha-prefix CINS)
  col  9      - ' ' or '*' (asterisk = added since previous list)
  cols 10..42 - issuer name (right-padded with spaces)
  cols 42..68 - security description (e.g. ``COM``, ``SHS``, ``CL A``)
  cols 68..78 - per-row status flags (``*A*`` added, ``*D*`` deleted)
  last column - status code letter (``E`` existing, ``N`` new, ``D`` deleted)

Numbers above are approximate — column widths drift slightly across
quarterly publications. The parser anchors on the leading 9-char
CUSIP and the trailing single-letter status code, splitting the
middle on 2+-space gaps to recover issuer name + description.
"""

from __future__ import annotations

import logging
import re
import urllib.request
from collections import defaultdict
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import date

import psycopg
import psycopg.rows

from app.config import settings
from app.services.cusip_resolver import (
    MATCH_THRESHOLD,
    SweepReport,
    _normalise_name,
    _similarity,
    sweep_resolvable_unresolved_cusips,
)
from app.services.sec_13f_filer_directory import _last_completed_quarter

logger = logging.getLogger(__name__)


_LIST_URL = "https://www.sec.gov/files/investment/13flist{year}q{quarter}.txt"

# CUSIP shape: 9 alphanumeric. SEC uses CUSIP for US issuers and
# CINS (CUSIP International Numbering System — same shape, alpha
# prefix instead of digit prefix) for foreign-domiciled securities.
# We accept both — both are valid identifiers stored in
# ``external_identifiers.identifier_value``.
_CUSIP_RE = re.compile(r"^[A-Z0-9]{9}$")


@dataclass(frozen=True)
class ThirteenFSecurity:
    """One row from the SEC Official List."""

    cusip: str
    issuer_name: str
    description: str
    is_added_since_last: bool
    status: str  # 'E' / 'N' / 'D'


@dataclass(frozen=True)
class CusipCoverageBackfillResult:
    """Per-run rollup."""

    list_rows: int
    instruments_seen: int
    inserted: int
    skipped_already_mapped: int
    tombstoned_unresolvable: int
    tombstoned_ambiguous: int
    tombstoned_conflict: int
    sweep: SweepReport


def fetch_13f_list_txt(year: int, quarter: int) -> str:
    """Fetch one quarterly Official List TXT. Raises on network or
    decode failure — caller decides whether to retry or surface."""
    url = _LIST_URL.format(year=year, quarter=quarter)
    req = urllib.request.Request(url, headers={"User-Agent": settings.sec_user_agent})
    with urllib.request.urlopen(req, timeout=120) as resp:  # noqa: S310 — fixed SEC URL
        # SEC ships the list as ASCII; latin-1 decode is the safe
        # fallback for the rare row with extended chars in issuer
        # name (e.g. accented letters in foreign filer names).
        return resp.read().decode("latin-1")


def parse_13f_list(payload: str) -> Iterator[ThirteenFSecurity]:
    """Yield one :class:`ThirteenFSecurity` per parseable row.

    Uses fixed-width column slicing (issuer cols 10:40, 30 wide;
    description cols 40:68, 28 wide) so an issuer name that fills
    its column doesn't bleed into the description on a 1-space gap.
    The trailing 12-char tail carries the per-row status flag
    (``*A*`` added / ``*D*`` deleted / blank unchanged) and an
    optional one-letter legacy status (``E`` / ``N`` / ``D``).

    ``*D*`` rows are returned with ``status='D'`` so the caller can
    decide whether to map them. The backfill skips ``D`` rows by
    default — a deleted-from-list CUSIP shouldn't anchor a new
    instrument mapping. Codex pre-push review #914.

    Rows that don't match the basic shape (CUSIP at col 0:9) are
    silently skipped — preamble / blank lines look the same.
    """
    for raw_line in payload.splitlines():
        line = raw_line.rstrip()
        if len(line) < 12:
            continue
        cusip_token = line[0:9].strip()
        if not _CUSIP_RE.match(cusip_token):
            continue
        added_flag = line[9:10] == "*"

        # Fixed-width column slicing for the well-defined fields.
        issuer_name = line[10:40].strip() if len(line) >= 40 else line[10:].strip()
        description_field = line[40:68].strip() if len(line) >= 41 else ""
        # Tail contains the per-row flag (``*A*`` / ``*D*``) plus
        # an optional one-letter legacy status code at end.
        tail = line[68:] if len(line) >= 68 else ""

        # Per-row flag — this is the authoritative add/delete signal
        # per SEC's published format. ``*D*`` means the security was
        # removed from the list this quarter; new mappings must NOT
        # anchor on it. Codex pre-push review #914.
        per_row_flag_match = re.search(r"\*([AD])\*", tail)
        per_row_flag = per_row_flag_match.group(1) if per_row_flag_match else ""

        # Optional legacy single-char status at tail end.
        legacy_status_match = re.search(r"([END])\s*$", tail)
        legacy_status = legacy_status_match.group(1) if legacy_status_match else ""

        # Compose the status: per-row flag wins (it's authoritative
        # for this quarter's diff); legacy single-char is the
        # fallback when no per-row flag is set.
        if per_row_flag == "D":
            status = "D"
        elif per_row_flag == "A":
            status = "N"
        elif legacy_status:
            status = legacy_status
        else:
            status = "E"

        if not issuer_name:
            continue

        yield ThirteenFSecurity(
            cusip=cusip_token,
            issuer_name=issuer_name,
            description=description_field,
            is_added_since_last=added_flag or per_row_flag == "A",
            status=status,
        )


def _select_unmapped_instruments(
    conn: psycopg.Connection[tuple],
) -> list[tuple[int, str]]:
    """Return ``(instrument_id, company_name)`` pairs for every
    tradable instrument that does NOT yet carry a SEC CUSIP entry
    in ``external_identifiers``."""
    pairs: list[tuple[int, str]] = []
    with conn.cursor(row_factory=psycopg.rows.tuple_row) as cur:
        cur.execute(
            """
            SELECT i.instrument_id, i.company_name
            FROM instruments i
            LEFT JOIN external_identifiers ei
              ON ei.instrument_id = i.instrument_id
             AND ei.provider = 'sec'
             AND ei.identifier_type = 'cusip'
            WHERE i.is_tradable = TRUE
              AND ei.instrument_id IS NULL
              AND i.company_name IS NOT NULL
              AND i.company_name <> ''
            ORDER BY i.instrument_id
            """,
        )
        for row in cur.fetchall():
            pairs.append((int(row[0]), str(row[1])))
    return pairs


def _bucket_by_first_token(
    securities: list[ThirteenFSecurity],
) -> dict[str, list[tuple[str, ThirteenFSecurity]]]:
    """Pre-normalize + bucket every Official-List entry by its
    normalised name's first token. The fuzzy scan per-instrument
    then only walks the matching bucket — cuts the comparison
    count by ~25x on a 12k×12k pairing.

    Returns ``{first_token -> [(normalised_full_name, security)]}``.
    Securities whose normalised name is empty (pure-suffix /
    pure-punctuation names — extreme edge) drop out.
    """
    out: dict[str, list[tuple[str, ThirteenFSecurity]]] = defaultdict(list)
    for sec in securities:
        normalised = _normalise_name(sec.issuer_name)
        if not normalised:
            continue
        first_token = normalised.split(" ", 1)[0]
        out[first_token].append((normalised, sec))
    return out


def _best_match(
    target: str,
    bucket: list[tuple[str, ThirteenFSecurity]],
    *,
    threshold: float,
) -> tuple[ThirteenFSecurity | None, bool]:
    """Return ``(best_security, is_ambiguous)``.

    Mirrors the resolver's same-named helper: we walk the bucket,
    pick the highest similarity, return a flag when two distinct
    CUSIPs tie at the top score (typical SPAC / share-class
    collision — operator must disambiguate via curated mapping).
    """
    if not target or not bucket:
        return (None, False)
    top_score = 0.0
    top_securities: list[ThirteenFSecurity] = []
    for normalised, sec in bucket:
        score = _similarity(target, normalised)
        if score < threshold:
            continue
        if score > top_score:
            top_score = score
            top_securities = [sec]
        elif score == top_score:
            top_securities.append(sec)
    if not top_securities:
        return (None, False)
    distinct_cusips = {s.cusip for s in top_securities}
    is_ambiguous = len(distinct_cusips) > 1
    return (top_securities[0], is_ambiguous)


def _insert_external_identifier(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    cusip: str,
) -> str:
    """Race-safe insert. Returns one of:

      * ``'inserted'`` — new mapping created.
      * ``'already_mapped'`` — instrument already has a SEC CUSIP
        (from any path: curated, prior backfill run, concurrent
        writer). Counted as a no-op.
      * ``'conflict'`` — the CUSIP is already mapped to a DIFFERENT
        instrument. Existing row preserved; caller tombstones the
        instrument as ``'conflict'`` so an operator can audit.

    Three Codex pre-push review #914 fixes applied:

      1. Pre-check ``(instrument_id, provider='sec',
         identifier_type='cusip')`` before INSERT — guards the
         stale-snapshot race where the unmapped-instruments SELECT
         ran 5 minutes ago and another writer mapped this
         instrument since.
      2. Per-row savepoint via ``conn.transaction()`` — a unique-
         violation on the INSERT rolls back this row only, not the
         entire backfill batch.
      3. ``ON CONFLICT (provider, identifier_type, identifier_value)
         DO NOTHING RETURNING xmax`` — distinguishes fresh INSERT
         from same-CUSIP-already-mapped without a second probe.
         Re-probe on no-row-returned tells us whether the conflict
         is same-instrument (already_mapped) or different
         (conflict).
    """
    cusip_norm = cusip.strip().upper()

    # 1. Stale-snapshot guard: instrument already mapped?
    with conn.cursor(row_factory=psycopg.rows.tuple_row) as cur:
        cur.execute(
            """
            SELECT 1 FROM external_identifiers
            WHERE instrument_id = %s
              AND provider = 'sec'
              AND identifier_type = 'cusip'
            LIMIT 1
            """,
            (instrument_id,),
        )
        if cur.fetchone() is not None:
            return "already_mapped"

    # 2. + 3. Per-row savepoint + ON CONFLICT RETURNING.
    with conn.transaction():
        with conn.cursor(row_factory=psycopg.rows.tuple_row) as cur:
            cur.execute(
                """
                INSERT INTO external_identifiers (
                    instrument_id, provider, identifier_type, identifier_value, is_primary
                ) VALUES (%(iid)s, 'sec', 'cusip', %(cusip)s, FALSE)
                ON CONFLICT (provider, identifier_type, identifier_value) DO NOTHING
                RETURNING instrument_id
                """,
                {"iid": instrument_id, "cusip": cusip_norm},
            )
            inserted_row = cur.fetchone()
            if inserted_row is not None:
                return "inserted"

            # Conflict — re-probe to classify same vs different.
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
    return "already_mapped" if existing is not None and int(existing[0]) == instrument_id else "conflict"


def backfill_cusip_coverage(
    conn: psycopg.Connection[tuple],
    *,
    year: int | None = None,
    quarter: int | None = None,
    fetch: Callable[[int, int], str] = fetch_13f_list_txt,
    today: date | None = None,
    threshold: float = MATCH_THRESHOLD,
) -> CusipCoverageBackfillResult:
    """Walk SEC's latest Official List + fuzzy-match every unmapped
    instrument's company_name against the list's issuer_names.
    INSERTs confident matches into ``external_identifiers``; calls
    :func:`sweep_resolvable_unresolved_cusips` post-batch to flush
    previously-stranded 13F holdings.

    Idempotent — already-mapped instruments are filtered in the
    SELECT; re-running on a populated install is cheap (one read,
    zero writes).
    """
    if year is None or quarter is None:
        today_d = today if today is not None else date.today()
        y, q = _last_completed_quarter(today_d)
        year = year if year is not None else y
        quarter = quarter if quarter is not None else q

    payload = fetch(year, quarter)
    raw_securities = list(parse_13f_list(payload))
    # Skip deleted-this-quarter rows so a new mapping doesn't anchor
    # on a CUSIP the SEC just removed from the 13(f)-eligible list.
    # Codex pre-push review #914.
    securities = [s for s in raw_securities if s.status != "D"]
    logger.info(
        "cusip_universe_backfill: %d rows (%d non-deleted) from %sQ%s 13F Official List",
        len(raw_securities),
        len(securities),
        year,
        quarter,
    )

    instruments = _select_unmapped_instruments(conn)
    logger.info("cusip_universe_backfill: %d unmapped instruments to evaluate", len(instruments))

    buckets = _bucket_by_first_token(securities)

    inserted = 0
    skipped_already_mapped = 0
    unresolvable = 0
    ambiguous = 0
    conflict = 0

    for iid, company_name in instruments:
        target = _normalise_name(company_name)
        if not target:
            unresolvable += 1
            continue
        first_token = target.split(" ", 1)[0]
        bucket = buckets.get(first_token, [])
        best, is_ambig = _best_match(target, bucket, threshold=threshold)
        if best is None:
            unresolvable += 1
            continue
        if is_ambig:
            ambiguous += 1
            logger.info(
                "cusip_universe_backfill: ambiguous match for instrument_id=%d %r — multiple CUSIPs at top score",
                iid,
                company_name,
            )
            continue
        outcome = _insert_external_identifier(conn, instrument_id=iid, cusip=best.cusip)
        if outcome == "inserted":
            inserted += 1
        elif outcome == "already_mapped":
            skipped_already_mapped += 1
        else:  # 'conflict'
            conflict += 1
            logger.warning(
                "cusip_universe_backfill: conflict cusip=%s already mapped to "
                "different instrument; instrument_id=%d kept unmapped",
                best.cusip,
                iid,
            )

    # Sweep pulls previously-stranded 13F holdings into
    # institutional_holdings now that the new mapping exists.
    # The underlying helper caps each pass at 1000 rows to keep
    # individual transactions bounded. Post-#913 the
    # unresolved_13f_cusips backlog can hit ~377k rows, so a single
    # 1000-row pass leaves ~99% stranded. Loop until either no
    # candidates remain or the sweep stops promoting (defensive
    # guard against an infinite loop in the unlikely case that
    # every pending row is permanently unresolvable through the
    # extid path). Codex pre-push review #914.
    sweep = SweepReport(candidates_seen=0, promoted=0, rewashed=0, rewash_deferred=0, rewash_failed=0)
    while True:
        pass_report = sweep_resolvable_unresolved_cusips(conn)
        sweep = SweepReport(
            candidates_seen=sweep.candidates_seen + pass_report.candidates_seen,
            promoted=sweep.promoted + pass_report.promoted,
            rewashed=sweep.rewashed + pass_report.rewashed,
            rewash_deferred=sweep.rewash_deferred + pass_report.rewash_deferred,
            rewash_failed=sweep.rewash_failed + pass_report.rewash_failed,
        )
        if pass_report.candidates_seen == 0 or pass_report.promoted == 0:
            break

    conn.commit()

    logger.info(
        "cusip_universe_backfill: inserted=%d already_mapped=%d unresolvable=%d ambiguous=%d conflict=%d "
        "sweep_promoted=%d sweep_rewashed=%d",
        inserted,
        skipped_already_mapped,
        unresolvable,
        ambiguous,
        conflict,
        sweep.promoted,
        sweep.rewashed,
    )

    return CusipCoverageBackfillResult(
        list_rows=len(securities),
        instruments_seen=len(instruments),
        inserted=inserted,
        skipped_already_mapped=skipped_already_mapped,
        tombstoned_unresolvable=unresolvable,
        tombstoned_ambiguous=ambiguous,
        tombstoned_conflict=conflict,
        sweep=sweep,
    )
