"""SEC Official List of Section 13(f) Securities — CUSIP universe backfill (#914).

The Official List is the canonical free regulated source for
CUSIP↔issuer mapping for US-listed equities and ADRs. SEC publishes
it quarterly as a fixed-width TXT under
``https://www.sec.gov/files/investment/13flist{year}q{quarter}-txt.txt``
(renamed from ``13flist{year}q{quarter}.txt`` at 2026q2 — #2118; the
fetcher tries the current name first and falls back to the legacy one
for older quarters).

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
import urllib.error
import urllib.request
from collections import defaultdict
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from datetime import date
from typing import Final

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


# SEC renamed the TXT at 2026q2 (#2118): the pre-rename name 404s for
# 2026q2+. Try the current name first, fall back to the legacy name so
# re-fetches of older quarters keep working. Same class as
# prevention-log #1769 — SEC renames break pinned constants.
_LIST_URL_PATTERNS = (
    "https://www.sec.gov/files/investment/13flist{year}q{quarter}-txt.txt",
    "https://www.sec.gov/files/investment/13flist{year}q{quarter}.txt",
)

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
    tombstoned_option_pseudo_cusip: int = 0
    """Pending ``unresolved_13f_cusips`` rows claimed as Official-List
    option classes this run (#2353). Defaulted so existing constructions
    in tests keep working; the production path always supplies it."""


def fetch_13f_list_txt(year: int, quarter: int) -> tuple[str, str]:
    """Fetch one quarterly Official List TXT. Returns ``(payload,
    source_url)`` — the URL that actually served the body, so the raw
    store's audit trail stays honest across the #2118 rename. Tries the
    current ``-txt.txt`` name first, falls back to the legacy name on
    404; raises on network or decode failure (and re-raises the LAST
    404 if every candidate misses) — caller decides whether to retry."""
    last_err: urllib.error.HTTPError | None = None
    for pattern in _LIST_URL_PATTERNS:
        url = pattern.format(year=year, quarter=quarter)
        req = urllib.request.Request(url, headers={"User-Agent": settings.sec_user_agent})
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:  # noqa: S310 — fixed SEC URL
                # SEC ships the list as ASCII; latin-1 decode is the safe
                # fallback for the rare row with extended chars in issuer
                # name (e.g. accented letters in foreign filer names).
                return resp.read().decode("latin-1"), url
        except urllib.error.HTTPError as err:
            if err.code != 404:
                raise
            last_err = err
    assert last_err is not None  # loop body always runs: patterns is non-empty
    raise last_err


def _store_raw_list(
    conn: psycopg.Connection[tuple],
    *,
    year: int,
    quarter: int,
    payload: str,
    source_url: str,
) -> None:
    """Persist the raw SEC TXT body to ``sec_reference_documents``
    BEFORE the parse step runs. Implements the eBull
    raw-payload-before-normalisation non-negotiable. Idempotent:
    re-fetching the same quarter overwrites the body and refreshes
    ``fetched_at``. Codex / Claude review BLOCKING for #914.
    ``source_url`` is the URL the fetch actually served (#2118 — the
    current-vs-legacy name differs by quarter)."""
    url = source_url
    conn.execute(
        """
        INSERT INTO sec_reference_documents (
            document_kind, period_year, period_quarter, payload, source_url
        ) VALUES ('13f_securities_list', %(year)s, %(quarter)s, %(payload)s, %(url)s)
        ON CONFLICT (document_kind, period_year, period_quarter) DO UPDATE SET
            payload = EXCLUDED.payload,
            source_url = EXCLUDED.source_url,
            fetched_at = NOW()
        """,
        {"year": year, "quarter": quarter, "payload": payload, "url": url},
    )


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


# Description tokens that mark equity/common-share rows on the SEC
# 13F Official List. Every issuer's COM line is paired with CALL +
# PUT (and sometimes WT/UNIT/RIGHTS) sharing the SAME issuer_name —
# without preference filtering, every matched issuer trips the
# ambiguity rejection (real-world bug surfaced 2026-05-08:
# backfill returned inserted=0 because every issuer had >1 same-
# score row across COM/CALL/PUT). Selecting the COM row over the
# option row is unambiguous: CALL/PUT have distinct CUSIPs from the
# underlying common but represent options on the same issuer; the
# 13F holdings ingester filters PUTCALL separately so picking the
# COM CUSIP is the correct identity for ownership joins.
# UNIT is intentionally OMITTED — SPAC unit CUSIPs (which often
# include warrants attached) are NOT the same security as the
# common stock and would map a stock instrument to the wrong CUSIP.
# Codex pre-push MEDIUM for #1054.
_COMMON_SHARE_DESC_TOKENS: frozenset[str] = frozenset(
    {
        "COM",
        "COMMON",
        "SHS",
        "SHARES",
        "CL",  # CL A / CL B common-share class designators
        "CLASS",
        "ORD",  # ordinary shares
        "ADS",  # American Depositary Shares
        "ADR",  # American Depositary Receipts
        "REIT",
    }
)
_OPTION_DESC_TOKENS: frozenset[str] = frozenset({"CALL", "PUT", "WTS", "WARRANT", "WT", "RIGHT", "RIGHTS"})

# The option CLASS descriptions SEC uses on the Official List (#2353).
# An EXACT match after whitespace collapse and ``*``-flag strip — NOT a
# token-containment test, and NOT ``_OPTION_DESC_TOKENS``. Both looser
# forms admit genuine securities, and each was measured on
# ``13flist2026q2-txt.txt`` rather than reasoned about:
#
#   * ``_OPTION_DESC_TOKENS`` also carries WTS / WARRANT / WT / RIGHT /
#     RIGHTS. A warrant or right is a real security with a real CUSIP —
#     0 of those 121 distinct CUSIPs fail the mod-10 check digit,
#     against 9,977 of 10,171 CALL/PUT ones.
#   * CONTAINING the token "CALL" catches 7 rows whose description is
#     compound: 4 BMO structured notes (``CALL NRGU 45``, ``CALL NRGD
#     45``, ``CALL BNKU 45``, ``CALL LKD 41``) and 3 covered-call ETFs
#     (``ETHE CO CALL ETF``, ``KWEB COVERD CALL``, ``YIEL S& CALL
#     ETF``). All 7 pass the check digit, and OpenFIGI answers
#     ``063679427`` (``CALL NRGU 45``) with a populated ``data`` array —
#     probed live 2026-08-08, which is how this was caught, since the
#     check digit hides it for every other row.
#
# Bare ``CALL`` / ``PUT`` is what SEC writes for an issuer's option
# class; see ``sql/274_unresolved_13f_option_pseudo_cusip.sql``.
_PUT_CALL_DESCRIPTIONS: frozenset[str] = frozenset({"CALL", "PUT"})

STATUS_OPTION_PSEUDO_CUSIP: Final[str] = "option_pseudo_cusip"
"""Terminal verdict for a CUSIP that is an Official-List option class.

Written by :func:`tombstone_option_pseudo_cusips`. See
``sql/274_unresolved_13f_option_pseudo_cusip.sql`` for the Form 13F
Special Instruction 10 rule that makes such a row a filer deviation
rather than an unmappable security.
"""


def _is_common_share(sec: ThirteenFSecurity) -> bool:
    desc = (sec.description or "").upper().strip()
    if not desc:
        return False
    tokens = set(desc.split())
    if tokens & _OPTION_DESC_TOKENS:
        return False
    return bool(tokens & _COMMON_SHARE_DESC_TOKENS)


def _is_option(sec: ThirteenFSecurity) -> bool:
    desc = (sec.description or "").upper().strip()
    tokens = set(desc.split())
    return bool(tokens & _OPTION_DESC_TOKENS)


def _is_put_call(sec: ThirteenFSecurity) -> bool:
    """True iff this Official-List row IS an issuer's option class.

    EXACT description match after upper-casing, stripping the ``*``
    added-since-last flag and collapsing whitespace. ⚠ Strictly narrower
    than :func:`_is_option`, and strictly narrower than "contains the
    word CALL" — see :data:`_PUT_CALL_DESCRIPTIONS` for the seven
    genuine securities the containment form swallows and the live
    OpenFIGI probe that exposed them. This is the only admissible
    discriminator for :func:`tombstone_option_pseudo_cusips`.
    """
    desc = " ".join((sec.description or "").upper().replace("*", " ").split())
    return desc in _PUT_CALL_DESCRIPTIONS


def tombstone_option_pseudo_cusips(
    conn: psycopg.Connection[tuple],
    securities: Iterable[ThirteenFSecurity],
) -> int:
    """Give every PENDING option-class CUSIP its own terminal verdict.

    A 13F Information Table row must carry the CUSIP of the security
    *underlying* an option, not the option's own identifier — Form 13F
    Special Instruction 10 puts Columns 1 through 5 (Column 3 is the
    CUSIP, per 11.b.iii) "in terms of the securities underlying the
    options, not the options themselves", with PUT/CALL designated in
    Column 5. So a stored CUSIP that matches an Official-List CALL or
    PUT row is a filer deviation, and it can never resolve: OpenFIGI
    rejects it on the check digit, :func:`backfill_cusip_coverage` maps
    only ``_is_common_share`` rows, and the legacy fuzzy-name resolver
    would match it to the UNDERLYING instrument and mint a wrong
    ``external_identifiers`` mapping.

    Applies to BOTH partitions of ``unresolved_13f_cusips`` — the bulk
    one (``source IS NOT NULL``, owned by the OpenFIGI sweep) and the
    legacy one (``source IS NULL``, owned by the fuzzy resolver). Both
    hold these rows and both mishandle them, differently.

    ⚠ Claims ONLY rows whose ``resolution_status IS NULL``. An existing
    verdict answers a different question and its provenance is not
    re-derivable, so it is never overwritten — including the OpenFIGI
    negatives (the #2304 reasoning). Rows already tombstoned
    ``openfigi_unknown`` are claimed on the pass AFTER the operator
    reset returns them to NULL; ``cusip_universe_backfill`` runs Sunday
    05:00 UTC and ``cusip_resolver_post_bulk_sweep`` Sunday 06:00, so
    the ordering that makes that work is the existing schedule's, not a
    new invariant.

    ``securities`` should be the UNFILTERED parse — including
    ``status='D'`` rows. A CALL class SEC removed from 13(f) eligibility
    is still a CALL class, and its identifier is still not what Column 3
    asks for; dropping ``D`` rows here would leave those stored CUSIPs
    pending forever.

    Returns the number of rows tombstoned. Caller owns the transaction.
    """
    cusips = sorted({s.cusip for s in securities if _is_put_call(s)})
    if not cusips:
        return 0
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE unresolved_13f_cusips
               SET resolution_status = %(status)s,
                   last_observed_at  = NOW()
             WHERE resolution_status IS NULL
               AND cusip = ANY(%(cusips)s)
            """,
            {"status": STATUS_OPTION_PSEUDO_CUSIP, "cusips": cusips},
        )
        # Plain rowcount: psycopg only reports -1 when no statement has run,
        # and an UPDATE has just run. Verified against psycopg 3.3.3 — a
        # zero-row UPDATE returns 0, not -1. The defensive
        # ``if rowcount and rowcount > 0`` form in cusip_resolver.py's
        # sibling sweeps is redundant here and implies a negative is
        # reachable, which it is not.
        return cur.rowcount


def _best_match(
    target: str,
    bucket: list[tuple[str, ThirteenFSecurity]],
    *,
    threshold: float,
) -> tuple[ThirteenFSecurity | None, bool]:
    """Return ``(best_security, is_ambiguous)``.

    Mirrors the resolver's same-named helper: we walk the bucket,
    pick the highest similarity, return a flag when two distinct
    CUSIPs tie at the top score.

    Tie-break for SEC 13F triplets (#1054): when the top-score set
    contains both common-share and option (CALL/PUT) rows under the
    same issuer name, prefer the common-share row. Ambiguity only
    fires when distinct ISSUERS tie at the top (true SPAC /
    share-class collision that needs operator disambiguation).
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
    # Prefer common-share rows over option rows when both share the
    # top score (the SEC list always emits COM + CALL + PUT triplets
    # for each underlying issuer).
    common = [s for s in top_securities if _is_common_share(s)]
    options = [s for s in top_securities if _is_option(s)]
    preferred: list[ThirteenFSecurity]
    if common and options and len(common) + len(options) == len(top_securities):
        # Every top row is either common or option; restrict to common.
        preferred = common
    elif common and len(common) < len(top_securities):
        # Mixed: prefer common, but keep ambiguity check honest by
        # only collapsing if every non-common is an option.
        non_common_non_option = [s for s in top_securities if not _is_common_share(s) and not _is_option(s)]
        preferred = common if not non_common_non_option else top_securities
    else:
        preferred = top_securities
    distinct_cusips = {s.cusip for s in preferred}
    is_ambiguous = len(distinct_cusips) > 1
    return (preferred[0], is_ambiguous)


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
         WHERE NOT (provider='sec' AND identifier_type='cik')
         DO NOTHING RETURNING instrument_id`` — distinguishes fresh
         INSERT from same-CUSIP-already-mapped without a second probe.
         Re-probe on no-row-returned tells us whether the conflict
         is same-instrument (already_mapped) or different
         (conflict). The partial-index predicate is required by
         Postgres ON CONFLICT inference post-#1102 (the global
         unique-on-value constraint was replaced with two partial
         indexes; CUSIP rows live under the non-CIK partial index).
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
                ON CONFLICT (provider, identifier_type, identifier_value)
                    WHERE NOT (provider = 'sec' AND identifier_type = 'cik')
                DO NOTHING
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
    fetch: Callable[[int, int], tuple[str, str]] = fetch_13f_list_txt,
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

    payload, source_url = fetch(year, quarter)
    # Persist the raw SEC body BEFORE parsing — eBull non-negotiable
    # (Claude review BLOCKING #914). Re-wash workflows can replay
    # against the stored body without re-fetching from SEC; the
    # operator gets a "what did SEC say last quarter" audit trail.
    _store_raw_list(conn, year=year, quarter=quarter, payload=payload, source_url=source_url)
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

    # #2353 — claim the option-class CUSIPs BEFORE the matcher runs, so
    # a pseudo-CUSIP is out of the pending set before anything can try
    # to resolve it. Fed the UNFILTERED parse deliberately (see the
    # helper's docstring): a delisted CALL class is still a CALL class.
    option_tombstoned = tombstone_option_pseudo_cusips(conn, raw_securities)
    if option_tombstoned:
        logger.info(
            "cusip_universe_backfill: tombstoned %d pending unresolved_13f_cusips rows as %s",
            option_tombstoned,
            STATUS_OPTION_PSEUDO_CUSIP,
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
        "option_pseudo_cusip=%d sweep_promoted=%d sweep_rewashed=%d",
        inserted,
        skipped_already_mapped,
        unresolvable,
        ambiguous,
        conflict,
        option_tombstoned,
        sweep.promoted,
        sweep.rewashed,
    )

    return CusipCoverageBackfillResult(
        # Raw count — matches the operator-readable phrase
        # "rows from the Official List" before any post-fetch
        # filtering. Codex / Claude review WARNING #914.
        list_rows=len(raw_securities),
        instruments_seen=len(instruments),
        inserted=inserted,
        skipped_already_mapped=skipped_already_mapped,
        tombstoned_unresolvable=unresolvable,
        tombstoned_ambiguous=ambiguous,
        tombstoned_conflict=conflict,
        sweep=sweep,
        tombstoned_option_pseudo_cusip=option_tombstoned,
    )
