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
from collections.abc import Iterator
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Any, Final

import psycopg
import psycopg.rows

from app.services import rewash_filings

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
    highest-leverage entries resolve first."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT cusip, name_of_issuer, observation_count
            FROM unresolved_13f_cusips
            WHERE resolution_status IS NULL
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
            # safely redundant.
            conn.execute(
                "DELETE FROM unresolved_13f_cusips WHERE cusip = %s",
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
    conn.execute(
        "DELETE FROM unresolved_13f_cusips WHERE cusip = %s",
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
    """
    conn.execute(
        """
        UPDATE unresolved_13f_cusips
        SET resolution_status = %s,
            last_observed_at = NOW()
        WHERE cusip = %s
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
    names with hundreds of stranded observations) resolve first."""
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
                cur.execute(
                    """
                    UPDATE unresolved_13f_cusips
                    SET resolution_status = 'resolved_via_extid',
                        last_observed_at = NOW()
                    WHERE cusip = %s
                      AND resolution_status IS NULL
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
