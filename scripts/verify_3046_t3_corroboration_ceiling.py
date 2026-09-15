"""#3046 residual 3 — the ceiling on T3's turnover corroboration, on the full corpus.

Spec: ``docs/proposals/ta/2026-09-15-3046-t3-corroboration-ceiling.md``.
Read-only. Writes nothing. One ``REPEATABLE READ READ ONLY`` transaction.

WHY THIS EXISTS. Three docstrings and one applied migration state that turnover
corroboration *"reaches only ~30% of the population (volume is equity-only, S3)"*.
Both halves are hand-written, and a hand-written derived statistic goes stale in the
place a reader trusts most. This script computes the figure and its drivers at run
time so the sentences beside it never have to carry a number.

⚠⚠ ``unclassifiable`` IS NOT THE SAME TEST AS ``volume IS NULL``, and every arm is
defined against the rule rather than against the column. ``_corroboration``
(``price_quarantine.py:420``) returns ``unclassifiable`` iff ``_usable_volume`` or
``_usable_close`` is None on EITHER endpoint, and ``_usable_volume`` rejects None
**and any value <= 0** (``:302``). Arm 5 reports the ``<= 0`` cell separately for
exactly this reason.

⚠⚠ A PROVISIONAL TRANSITION IS ``unclassifiable`` WITHOUT READING VOLUME AT ALL —
``price_quarantine.py:486-492`` DEFERS the verdict because a forming bar's volume is
a part-session count. A deferred verdict is not blindness. Arm 1 splits them even
though the deferred count is 0 today: the deferral is a RULE, and a clause justified
by a count has to be un-justified the moment the count moves.

⚠⚠ THE ASSET CLASS COMES FROM ``price_quarantine_coverage.asset_class`` — *"as seen
at evaluation time; NULL is a real state"* (``sql/247_price_quarantine.sql:45``) —
NOT from a live ``instruments -> exchanges`` join. The two agree on the whole corpus
today and arm 2 REPORTS that reconciliation rather than assuming it; current
instrument metadata is not evidence about a verdict computed earlier.

⚠⚠ THE DENOMINATOR IS THE EVALUATED T3 TRIGGER POPULATION, not "every large move".
T1 and T2 suppress T3 evaluation entirely (``price_quarantine.py:485`` — ``and not
rules``), so a transition already explained by an unusable endpoint or a series hole
never enters it. ``corroboration <> 'not_applicable'`` is the same predicate
``price_quarantine_store.py:381,402`` uses, and it INCLUDES the admitted-back
``spike`` rows, which carry no ``T3`` in ``rules``.

⚠ SCOPE IS ``price_daily`` ONLY — the eToro execution corpus. The same rule set also
evaluates ``research_price_daily`` into ``research_transition_quarantine``, whose
volume coverage is a different population and is not measured here.

⚠ NO STORED VERSION CONSTANT IS IMPORTED FOR FILTERING. The script asks the CORPUS
which rule-set version produced its rows and refuses when there is more than one
(prevention log, "a verifier pinned to its own code hash"). ``RULE_SET_VERSION`` is
printed for comparison only.

Usage::

    PYTHONPATH=. uv run python -m scripts.verify_3046_t3_corroboration_ceiling
    PYTHONPATH=. uv run python -m scripts.verify_3046_t3_corroboration_ceiling --probe
"""

from __future__ import annotations

import argparse
import subprocess
from datetime import UTC, datetime
from typing import Any, LiteralString

import psycopg

from app.config import settings
from app.services.price_quarantine import RULE_SET_VERSION

#: Bumped whenever an arm's stratum definition changes, so two runs are comparable
#: only when it matches. Not a rule constant — every rule called here lives in
#: ``price_quarantine`` and is read, never restated.
CENSUS_VERSION = "t3-ceiling-v1"

#: The window used to ask whether a blind transition sits in a volume-free RUN or an
#: isolated hole. Fixed BY CONSTRUCTION and frozen here rather than tuned: +/- 14
#: calendar days is two trading weeks either side, the smallest window that can
#: distinguish "this instrument stopped reporting volume for a while" from "these two
#: particular bars are missing it". No published formulation exists for this question;
#: the figure is descriptive and no decision is gated on it.
RUN_WINDOW_DAYS = 14

#: The only two ways ``git rev-parse`` can fail: the binary is missing (``OSError``)
#: or it exits non-zero. Bound to a name because ``ruff format`` strips the
#: parentheses from ``except (A, B):`` on this Python target, leaving a form the
#: review bot reads as Python 2 syntax.
_GIT_SHA_FAILURES = (OSError, subprocess.SubprocessError)

#: ``corroboration <> 'not_applicable'`` — the evaluated T3 TRIGGER population.
#: Verbatim in shape from ``price_quarantine_store.py:381,402`` so this census
#: partitions the same rows the published census counts.
_TRIGGER = "q.corroboration <> 'not_applicable'"

#: ⚠⚠ THE CAUSAL ARMS (3-6) ADD THIS; ARM 1 DELIBERATELY DOES NOT.
#: A provisional transition is stored ``unclassifiable`` WITHOUT the volume ever
#: being read (``price_quarantine.py:486-492`` defers the verdict) — so it can sit in
#: the blind set with positive volume on BOTH endpoints. Counting it as volume
#: blindness would attribute a deferral to a data gap, and no arm downstream could
#: tell. Arm 1 keeps them so the published reachability rate matches the census
#: endpoint's own buckets; every arm that asks WHY excludes them.
_ADJUDICATED = "NOT q.provisional"

#: Per-instrument volume coverage over the WHOLE stored history. Deliberately not
#: restricted to trigger dates: the question arm 3 asks is about the instrument, and
#: filtering to the rows that already selected for missing volume would answer itself.
_COVERAGE = """
    SELECT instrument_id,
           count(*)        AS bars,
           count(volume)   AS vols,
           min(price_date) AS first_bar,
           max(price_date) AS last_bar,
           min(price_date) FILTER (WHERE volume IS NOT NULL) AS onset
      FROM price_daily
     GROUP BY instrument_id
"""


def _git_sha() -> str:
    try:
        return subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True, check=True
        ).stdout.strip()
    except _GIT_SHA_FAILURES:
        return "unknown"


def _rows(
    conn: psycopg.Connection[Any], sql: LiteralString, params: dict[str, Any] | None = None
) -> list[tuple[Any, ...]]:
    """``LiteralString``, not ``str``, is the point — it is psycopg3's injection guard.

    Every query below is an f-string over module-level literals (``_TRIGGER``,
    ``_COVERAGE``) only; the rule-set version, the window and the probe arguments are
    all BOUND parameters. Widening this to ``str`` would let a future edit interpolate
    a value and typecheck clean.
    """
    return conn.execute(sql, params or {}).fetchall()


def _pct(num: int, den: int) -> str:
    return "n/a" if den == 0 else f"{100.0 * num / den:.1f}%"


def corpus_rule_set_version(conn: psycopg.Connection[Any]) -> str:
    """The version the STORED rows were produced by — asked, never imported.

    Refuses on more than one. A corpus mid-re-evaluation would have each arm
    comparing a mixture of two rule sets against itself, and every percentage below
    would be a blend of two populations with no way to see it in the output.
    """
    versions = _rows(
        conn, "SELECT rule_set_version, count(*) FROM price_transition_quarantine GROUP BY 1 ORDER BY 2 DESC"
    )
    if not versions:
        raise SystemExit("REFUSED: price_transition_quarantine is empty — nothing to census.")
    if len(versions) > 1:
        raise SystemExit(
            f"REFUSED: corpus holds {len(versions)} rule-set versions, cannot census a mixture: {versions}"
        )
    return str(versions[0][0])


def arm_reachability(conn: psycopg.Connection[Any], version: str) -> int:
    """Arm 1 — the headline number, with deferral split out and ADMITTED separated.

    ``reachable`` (anything but ``unclassifiable``) and ``admitted`` (``spike`` only)
    are different statements: ``collapse`` and ``flat`` are classified AND still
    quarantined. The stale docstrings quote a single "~30%" without saying which.
    """
    print("\n-- ARM 1: reachability of the evaluated T3 trigger population " + "-" * 36)
    rows = _rows(
        conn,
        f"""
        SELECT q.corroboration, count(*), count(*) FILTER (WHERE q.provisional)
          FROM price_transition_quarantine q
         WHERE {_TRIGGER} AND q.rule_set_version = %(v)s
         GROUP BY 1 ORDER BY 2 DESC
        """,
        {"v": version},
    )
    total = sum(int(r[1]) for r in rows)
    if total == 0:
        raise SystemExit("REFUSED: zero T3 triggers in the corpus — every percentage below would be n/a.")
    blind = sum(int(r[1]) for r in rows if r[0] == "unclassifiable")
    admitted = sum(int(r[1]) for r in rows if r[0] == "spike")
    deferred = sum(int(r[2]) for r in rows)
    print(f"  {'corroboration':<18}{'rows':>8}{'provisional':>14}")
    for corr, n, prov in rows:
        print(f"  {str(corr):<18}{int(n):>8}{int(prov):>14}")
    print(f"  {'TRIGGER POPULATION':<18}{total:>8}{deferred:>14}")
    print(f"\n  reachable (not unclassifiable)  {total - blind:>6} of {total}  = {_pct(total - blind, total)}")
    print(f"  admitted back (spike only)      {admitted:>6} of {total}  = {_pct(admitted, total)}")
    held = total - blind - admitted
    print(f"  classified AND still quarantined {held:>5} of {total}  = {_pct(held, total)}")
    if deferred == 0:
        print("\n  ⚠ 0 deferred (provisional) rows today. The arm still ships: the deferral at")
        print("    price_quarantine.py:486-492 is a RULE, and a zero count does not un-justify it.")
    return total


def arm_asset_class(conn: psycopg.Connection[Any], version: str) -> None:
    """Arm 2 — class AS EVALUATED, plus the reconciliation against the live join."""
    print("\n-- ARM 2: asset class, as evaluated " + "-" * 61)
    for ac, pop, reach in _rows(
        conn,
        f"""
        SELECT coalesce(c.asset_class, '(null)'), count(*),
               count(*) FILTER (WHERE q.corroboration <> 'unclassifiable')
          FROM price_transition_quarantine q
          JOIN price_quarantine_coverage c
            ON c.instrument_id = q.instrument_id AND c.rule_set_version = q.rule_set_version
         WHERE {_TRIGGER} AND q.rule_set_version = %(v)s
         GROUP BY 1 ORDER BY 2 DESC
        """,
        {"v": version},
    ):
        print(f"  {str(ac):<14}{int(pop):>8} triggers{int(reach):>8} reachable   {_pct(int(reach), int(pop)):>7}")

    drift = _rows(
        conn,
        """
        SELECT count(*),
               count(*) FILTER (WHERE coalesce(c.asset_class, '~') IS DISTINCT FROM coalesce(e.asset_class, '~'))
          FROM price_quarantine_coverage c
          JOIN instruments i ON i.instrument_id = c.instrument_id
          LEFT JOIN exchanges e ON e.exchange_id = i.exchange
         WHERE c.rule_set_version = %(v)s
        """,
        {"v": version},
    )[0]
    print(f"\n  evaluated-vs-live class join: {int(drift[1])} disagreements of {int(drift[0])} coverage rows")
    if int(drift[1]):
        print("  ⚠ NON-ZERO — the live join and the stored class describe different populations.")

    print("\n  per-INSTRUMENT all-null rate, structurally volume-free classes vs equities:")
    for grp, insts, allnull in _rows(
        conn,
        f"""
        WITH cov AS ({_COVERAGE})
        SELECT CASE WHEN c.asset_class IN ('crypto', 'fx', 'index', 'commodity')
                    THEN 'crypto/fx/index/commodity' ELSE 'equity classes' END,
               count(*), count(*) FILTER (WHERE cov.vols = 0)
          FROM price_quarantine_coverage c JOIN cov ON cov.instrument_id = c.instrument_id
         WHERE c.rule_set_version = %(v)s
         GROUP BY 1 ORDER BY 2
        """,
        {"v": version},
    ):
        print(f"    {str(grp):<28}{int(allnull):>6} of {int(insts):<6} = {_pct(int(allnull), int(insts))}")
    print("  ⚠ The tendency is real and strong. It is not the EXPLANATION — see the class")
    print("    table above, where the trigger population is overwhelmingly one equity class.")


def arm_instrument_coverage(conn: psycopg.Connection[Any], version: str) -> None:
    """Arm 3 — blindness per TRANSITION, not per instrument.

    ⚠ The unit matters and getting it wrong inverted this ticket's first ranking:
    counting INSTRUMENTS in each coverage category says nothing about how many
    TRANSITIONS they carry, and the two answers disagree.
    """
    print("\n-- ARM 3: blindness by the instrument's own volume coverage (per TRANSITION) " + "-" * 20)
    for cat, pop, reach, blind, insts in _rows(
        conn,
        f"""
        WITH cov AS ({_COVERAGE})
        SELECT CASE WHEN cov.vols = 0 THEN 'never_any_volume'
                    WHEN cov.vols = cov.bars THEN 'complete' ELSE 'partial' END,
               count(*),
               count(*) FILTER (WHERE q.corroboration <> 'unclassifiable'),
               count(*) FILTER (WHERE q.corroboration = 'unclassifiable'),
               count(DISTINCT q.instrument_id)
          FROM price_transition_quarantine q JOIN cov ON cov.instrument_id = q.instrument_id
         WHERE {_TRIGGER} AND {_ADJUDICATED} AND q.rule_set_version = %(v)s
         GROUP BY 1 ORDER BY 2 DESC
        """,
        {"v": version},
    ):
        print(
            f"  {str(cat):<18}{int(pop):>7} triggers{int(reach):>7} reachable{int(blind):>7} blind"
            f"   ({int(insts)} instruments)"
        )


def arm_onset(conn: psycopg.Connection[Any], version: str) -> None:
    """Arm 4 — position relative to THAT INSTRUMENT'S own volume onset.

    ⚠ Both endpoint dates, not one. A transition spans ``prior_date`` -> ``price_date``
    and can straddle the onset; classifying on the later date alone would file a
    straddling transition as fully covered.

    ⚠ ``onset`` is censored by the instrument's first STORED bar — if the series
    begins already carrying volume, ``min(price_date) FILTER (WHERE volume IS NOT
    NULL)`` is the start of history, not the start of reporting. The onset-cluster
    readout below therefore requires a strictly earlier volume-free stored bar.
    """
    print("\n-- ARM 4: position relative to the instrument's own volume onset " + "-" * 32)
    for era, pop, reach in _rows(
        conn,
        f"""
        WITH cov AS ({_COVERAGE})
        SELECT CASE WHEN cov.onset IS NULL          THEN 'instrument never has volume'
                    WHEN q.prior_date >= cov.onset  THEN 'both endpoints after onset'
                    WHEN q.price_date >= cov.onset  THEN 'straddles onset'
                    ELSE 'both endpoints before onset' END,
               count(*), count(*) FILTER (WHERE q.corroboration <> 'unclassifiable')
          FROM price_transition_quarantine q JOIN cov ON cov.instrument_id = q.instrument_id
         WHERE {_TRIGGER} AND {_ADJUDICATED} AND q.rule_set_version = %(v)s
         GROUP BY 1 ORDER BY 2 DESC
        """,
        {"v": version},
    ):
        print(f"  {str(era):<30}{int(pop):>7} triggers{int(reach):>7} reachable   {_pct(int(reach), int(pop)):>7}")

    print("\n  onset CLUSTER — each instrument's first volume-bearing bar, uncensored only")
    print("  (an earlier volume-free stored bar must exist, else the date is start-of-history):")
    clustered = _rows(
        conn,
        f"""
        WITH cov AS ({_COVERAGE})
        SELECT cov.onset, count(*)
          FROM cov WHERE cov.onset IS NOT NULL AND cov.onset > cov.first_bar
         GROUP BY 1 ORDER BY 2 DESC LIMIT 3
        """,
    )
    uncensored = _rows(
        conn,
        f"WITH cov AS ({_COVERAGE}) SELECT count(*) FILTER (WHERE onset IS NOT NULL AND onset > first_bar),"
        f" count(*) FILTER (WHERE onset IS NOT NULL) FROM cov",
    )[0]
    for d, n in clustered:
        share = _pct(int(n), int(uncensored[1]))
        print(f"    {d}  {int(n):>6} instruments   {share} of all volume-bearing instruments")
    print(f"  uncensored onsets: {int(uncensored[0])} of {int(uncensored[1])} volume-bearing instruments")
    print("  ⚠ A dominant MODE is not 'the bulk' — read the share column, not the rank.")


def arm_endpoints(conn: psycopg.Connection[Any], version: str) -> None:
    """Arm 5 — which endpoint lacks a USABLE volume, NULL and ``<= 0`` distinguished.

    ⚠ ``neither missing = 0`` is near-tautological: ``unclassifiable`` is DEFINED as a
    missing usable volume on some endpoint. It is printed as a self-check that the
    reproduction matches the rule, never as evidence for a cause. The informative
    cell is the both-vs-one split, which says whether absence is a RUN or a hole.
    """
    print("\n-- ARM 5: endpoint volume state of the blind transitions " + "-" * 40)
    row = _rows(
        conn,
        """
        WITH t AS (
          SELECT q.instrument_id, q.price_date, q.prior_date,
                 (SELECT p.volume FROM price_daily p
                   WHERE p.instrument_id = q.instrument_id AND p.price_date = q.price_date) AS v_cur,
                 (SELECT p.volume FROM price_daily p
                   WHERE p.instrument_id = q.instrument_id AND p.price_date = q.prior_date) AS v_prev
            FROM price_transition_quarantine q
           WHERE q.corroboration = 'unclassifiable' AND NOT q.provisional
             AND q.rule_set_version = %(v)s)
        SELECT count(*),
               count(*) FILTER (WHERE v_cur IS NULL AND v_prev IS NULL),
               count(*) FILTER (WHERE (v_cur IS NULL) <> (v_prev IS NULL)),
               count(*) FILTER (WHERE v_cur IS NOT NULL AND v_prev IS NOT NULL),
               count(*) FILTER (WHERE v_cur <= 0 OR v_prev <= 0)
          FROM t
        """,
        {"v": version},
    )[0]
    total, both, one, neither, nonpos = (int(x) for x in row)
    print(f"  blind transitions          {total:>7}")
    print(f"  both endpoints NULL        {both:>7}   {_pct(both, total)}")
    print(f"  exactly one endpoint NULL  {one:>7}   {_pct(one, total)}")
    print(f"  neither NULL               {neither:>7}   (self-check: non-zero only via the <= 0 branch)")
    print(f"  some endpoint <= 0         {nonpos:>7}")
    zeros = _rows(
        conn, "SELECT count(*) FILTER (WHERE volume = 0), count(*) FILTER (WHERE volume < 0) FROM price_daily"
    )[0]
    print(f"\n  stored volume: {int(zeros[0])} zero, {int(zeros[1])} negative")
    print("  ⚠ _int_or_none maps a SOURCE zero to NULL (etoro.py:861-869), so a stored NULL")
    print("    cannot distinguish 'the source omitted it' from 'the source said zero'. No")
    print("    storage-side query can establish a source claim — see --probe.")

    print(f"\n  is the absence a RUN? +/-{RUN_WINDOW_DAYS}d window around each POST-ONSET blind transition:")
    for shape, n in _rows(
        conn,
        f"""
        WITH cov AS ({_COVERAGE}),
        blind AS (
          SELECT q.instrument_id, q.price_date FROM price_transition_quarantine q
            JOIN cov ON cov.instrument_id = q.instrument_id
           WHERE q.corroboration = 'unclassifiable' AND NOT q.provisional
             AND q.rule_set_version = %(v)s
             AND cov.onset IS NOT NULL AND q.prior_date >= cov.onset),
        win AS (
          SELECT b.instrument_id, b.price_date,
                 (SELECT count(*) FROM price_daily p WHERE p.instrument_id = b.instrument_id
                    AND p.price_date BETWEEN b.price_date - %(w)s AND b.price_date + %(w)s) AS bars,
                 (SELECT count(p.volume) FROM price_daily p WHERE p.instrument_id = b.instrument_id
                    AND p.price_date BETWEEN b.price_date - %(w)s AND b.price_date + %(w)s) AS vols
            FROM blind b)
        SELECT CASE WHEN vols = 0 THEN 'window ENTIRELY volume-free'
                    WHEN vols < bars THEN 'window partially volume-free'
                    ELSE 'window fully covered (isolated hole)' END, count(*)
          FROM win GROUP BY 1 ORDER BY 2 DESC
        """,
        {"v": version, "w": RUN_WINDOW_DAYS},
    ):
        print(f"    {str(shape):<40}{int(n):>7}")


def arm_cohorts(conn: psycopg.Connection[Any], version: str) -> None:
    """Arm 6 — is the blindness CONCENTRATED on a nameable cohort? (Tested, not assumed.)

    ⚠ The top carriers of blind transitions are distressed names, which makes
    *"corroboration is missing precisely where level breaks are generated"* an
    attractive headline. It is a sample-of-eight observation and this arm is what
    tests it on the full population. Read the blind% column across cohorts before
    quoting any concentration claim.

    ⚠ The ``Q`` suffix is a SYMBOL-TEXT cohort — a classifier over source text, which
    is a data-treatment decision. The structured sources exist
    (``sec_form25_register``, ``sec_form25_common_equity_delistings``) and the series
    -state split below is the structured one; the suffix cohort is OBSERVED and is
    never used as a rule.
    """
    print("\n-- ARM 6: is blindness concentrated on a cohort? " + "-" * 48)
    frontier = _rows(conn, "SELECT max(price_date) FROM price_daily")[0][0]
    # Annotated because a tuple literal widens ``LiteralString`` to ``str``, which
    # would silently drop _rows' injection guard for these two members alone.
    cohorts: tuple[tuple[str, LiteralString], ...] = (
        (
            "series state (STRUCTURED: last stored bar vs frontier)",
            f"""
            WITH cov AS ({_COVERAGE})
            SELECT CASE WHEN cov.last_bar < %(f)s::date - 5 THEN 'series ENDED' ELSE 'series LIVE' END,
                   count(*), count(*) FILTER (WHERE q.corroboration = 'unclassifiable')
              FROM price_transition_quarantine q JOIN cov ON cov.instrument_id = q.instrument_id
             WHERE {_TRIGGER} AND {_ADJUDICATED} AND q.rule_set_version = %(v)s GROUP BY 1 ORDER BY 2 DESC
            """,
        ),
        (
            "symbol suffix (OBSERVED cohort, never used as a rule)",
            f"""
            SELECT CASE WHEN i.symbol ~ 'Q$' THEN 'symbol ends Q' ELSE 'other' END,
                   count(*), count(*) FILTER (WHERE q.corroboration = 'unclassifiable')
              FROM price_transition_quarantine q JOIN instruments i ON i.instrument_id = q.instrument_id
             WHERE {_TRIGGER} AND {_ADJUDICATED} AND q.rule_set_version = %(v)s GROUP BY 1 ORDER BY 2 DESC
            """,
        ),
    )
    for label, sql in cohorts:
        print(f"  {label}")
        for coh, pop, blind in _rows(conn, sql, {"v": version, "f": frontier}):
            print(f"    {str(coh):<26}{int(pop):>7} triggers{int(blind):>7} blind   {_pct(int(blind), int(pop)):>7}")

    base = _rows(
        conn,
        f"""
        SELECT (SELECT count(*) FROM instruments i
                 WHERE i.symbol ~ 'Q$'
                   AND EXISTS (SELECT 1 FROM price_daily p WHERE p.instrument_id = i.instrument_id)),
               (SELECT count(DISTINCT instrument_id) FROM price_daily),
               (SELECT count(*) FROM price_transition_quarantine q
                  JOIN instruments i ON i.instrument_id = q.instrument_id
                 WHERE {_TRIGGER} AND {_ADJUDICATED} AND q.rule_set_version = %(v)s AND i.symbol ~ 'Q$'),
               (SELECT count(*) FROM price_transition_quarantine q
                 WHERE {_TRIGGER} AND {_ADJUDICATED} AND q.rule_set_version = %(v)s)
        """,
        {"v": version},
    )[0]
    q_inst, all_inst, q_trig, all_trig = (int(x) for x in base)
    print(
        f"\n  Q-suffix share of CORPUS {_pct(q_inst, all_inst)} ({q_inst}/{all_inst})"
        f"  vs of TRIGGERS {_pct(q_trig, all_trig)} ({q_trig}/{all_trig})"
    )
    print("  ⚠ Read these two columns separately. Distress predicts GENERATING a level break;")
    print("    the blind% column above is what says whether it predicts being unadjudicable.")


def arm_probe(instrument_id: int, count: int) -> None:
    """Optional — one informational GET, to separate a SOURCE gap from an INGEST gap.

    ⚠ SEPARATE FROM THE DB SNAPSHOT ON PURPOSE, and on its OWN CONNECTION. An HTTP
    observation is taken at a different instant from the ``REPEATABLE READ``
    transaction around every other arm, so folding it in would print two inconsistent
    times under one heading — and the credential read commits, which a ``READ ONLY``
    transaction cannot do.

    ⚠⚠ IT READS THE **RAW** PAYLOAD, NOT ``get_daily_candles``. That method runs the
    response through ``_normalise_candle`` -> ``_int_or_none``, which maps a source
    ``0`` (and any unparseable value) to ``None`` — i.e. it applies the very ingest
    transformation this arm exists to distinguish from source-side absence. Reading
    the normalised bars would make a raw zero and a raw null print identically and
    the arm would answer its own question. Both views are reported from ONE response.

    ⚠⚠ THE KEY IS RESOLVED READ-ONLY. ``master_key.bootstrap`` runs
    ``_revoke_stale_ciphertext`` unconditionally, and its class 2 is *"derived_key is
    None ... every operator-owned active row is unrecoverable"* — so a diagnostic run
    on a host without the key would SOFT-REVOKE every broker credential on its way to
    printing "skipped". ``ensure_broker_key_loaded`` is the sanctioned read-only
    counterpart (*"runs no stale-revoke pass and no boot-state transition"*).

    ⚠ THIS IS A BOUNDED OBSERVATION, NOT A PROVIDER CONTRACT. It says what this
    instrument's response carried at this moment. No eToro documentation states a
    daily-volume cutover, and none is claimed here.

    Informational only: same URL family ``daily_candle_refresh`` calls every day, no
    broker mutation, no order path.
    """
    import uuid

    import httpx

    from app.providers.implementations.etoro import _normalise_candles
    from app.providers.implementations.etoro_request_log import issue_raw_request
    from app.security.master_key import ensure_broker_key_loaded
    from app.services.broker_credentials import load_credential_for_provider_use
    from app.services.operators import sole_operator_id

    print("\n-- PROBE: is the pre-onset absence SOURCE-side or INGEST-side? " + "-" * 34)
    creds = {}
    with psycopg.connect(settings.database_url) as conn:
        if not ensure_broker_key_loaded(conn):
            print("  SKIPPED: no broker-encryption key derivable on this host (nothing mutated).")
            return
        op = sole_operator_id(conn)
        for label in ("api_key", "user_key"):
            creds[label] = load_credential_for_provider_use(
                conn,
                operator_id=op,
                provider="etoro",
                label=label,
                environment=settings.etoro_env,
                caller="verify_3046_t3_corroboration_ceiling",
            )
            conn.commit()

    url = f"{settings.etoro_base_url}/api/v1/market-data/instruments/{instrument_id}/history/candles/asc/OneDay/{count}"
    headers = {"x-api-key": creds["api_key"], "x-user-key": creds["user_key"], "x-request-id": str(uuid.uuid4())}
    taken = datetime.now(UTC).isoformat(timespec="seconds")
    with httpx.Client(timeout=30.0) as client:
        resp = issue_raw_request(
            client, "GET", url, src="verify_3046_t3_ceiling", env=settings.etoro_env, headers=headers
        )
    if resp.status_code != 200:
        print(f"  HTTP {resp.status_code}: {resp.text[:300]}")
        return
    outer = resp.json().get("candles", [])
    raw = outer[0].get("candles", []) if outer else []
    if not raw:
        print(f"  instrument {instrument_id}: provider returned no bars.")
        return

    bars = _normalise_candles(resp.json())
    print(f"  instrument {instrument_id}, {len(raw)} raw bars, retrieved {taken}")
    print(f"  normalised to {len(bars)} bars, {bars[0].price_date} -> {bars[-1].price_date}")
    print("\n  RAW payload volume state by year — absent/null vs an explicit zero:")
    print(f"    {'year':<6}{'bars':>7}{'positive':>10}{'explicit 0':>12}{'null/absent':>13}{'other':>8}")
    by_year: dict[int, list[int]] = {}
    for b in raw:
        year = int(str(b.get("fromDate"))[:4])
        slot = by_year.setdefault(year, [0, 0, 0, 0, 0])
        slot[0] += 1
        v = b.get("volume")
        if v is None:
            slot[3] += 1
        elif isinstance(v, int | float) and v > 0:
            slot[1] += 1
        elif isinstance(v, int | float) and v == 0:
            slot[2] += 1
        else:
            slot[4] += 1
    for y in sorted(by_year):
        n, pos, zero, null, other = by_year[y]
        print(f"    {y:<6}{n:>7}{pos:>10}{zero:>12}{null:>13}{other:>8}")
    zeros = sum(v[2] for v in by_year.values())
    others = sum(v[4] for v in by_year.values())
    print(f"\n  explicit zeros in the raw payload: {zeros}   unparseable: {others}")
    if zeros == 0 and others == 0:
        print("  => every absence is a SOURCE null, not an ingest artefact of _int_or_none.")
    else:
        print("  ⚠ NON-ZERO — _int_or_none collapses these into the same stored NULL, so the")
        print("    source-vs-ingest question is NOT settled for this instrument.")
    last_null = max((b.price_date for b in bars if b.volume is None), default=None)
    first_vol = min((b.price_date for b in bars if b.volume is not None), default=None)
    print(f"  last bar WITHOUT volume {last_null}   first bar WITH volume {first_vol}")
    print("  ⚠ ONE instrument. A boundary here is this response's boundary, not a corpus rule —")
    print("    arm 4's onset cluster is the full-population statement.")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--probe", action="store_true", help="Also make ONE informational eToro daily-candle GET")
    parser.add_argument(
        "--probe-instrument", type=int, default=1001, help="Instrument for --probe (default 1001 = AAPL)"
    )
    parser.add_argument(
        "--probe-count", type=int, default=1000, help="Bars to request for --probe (eToro caps at 1000)"
    )
    args = parser.parse_args(argv)

    with psycopg.connect(settings.database_url) as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        frontier = _rows(conn, "SELECT max(price_date), count(*), count(DISTINCT instrument_id) FROM price_daily")[0]
        version = corpus_rule_set_version(conn)

        print("=" * 100)
        print("#3046 residual 3 — the ceiling on T3's turnover corroboration, full corpus")
        print("=" * 100)
        print(f"  git                    {_git_sha()}")
        print(f"  census version         {CENSUS_VERSION}")
        print(f"  run at                 {datetime.now(UTC).isoformat(timespec='seconds')}")
        print(f"  corpus RULE_SET        {version}   (asked of the corpus, not imported)")
        print(f"  module RULE_SET_VERSION {RULE_SET_VERSION}   (printed for comparison only)")
        if version != RULE_SET_VERSION:
            print("  ⚠ STORED ROWS PREDATE THE DEPLOYED MODULE — every figure below describes the")
            print("    version named on the line above it, not the code in this checkout.")
        print(
            f"  corpus                 {int(frontier[1])} bars / {int(frontier[2])} instruments, frontier {frontier[0]}"
        )
        print("  scope                  price_daily ONLY — research_price_daily is a different population")

        arm_reachability(conn, version)
        arm_asset_class(conn, version)
        arm_instrument_coverage(conn, version)
        arm_onset(conn, version)
        arm_endpoints(conn, version)
        arm_cohorts(conn, version)

    if args.probe:
        arm_probe(args.probe_instrument, args.probe_count)

    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
