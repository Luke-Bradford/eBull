"""Read-only evidence behind #3146 — which Table I line becomes the "current" balance.

#3146: ``refresh_insiders_current``'s ``DISTINCT ON`` ends its ``ORDER BY`` with
``source_document_id ASC`` (``app/services/ownership_observations.py``). When one filing
reports several Table I lines for the same ``(holder_identity_key, ownership_nature)`` on the
same date, every prior key ties, so the balance that becomes current is decided by string
order on a DERA surrogate key.

Spec: ``docs/proposals/ownership/2026-09-17-3146-insider-line-order.md``.

Three modes, one per claim the verdict makes. **Every figure is computed at run time** —
nothing is hand-written into prose, so a re-harvest cannot leave a number lying
(prevention-log: "never hardcode a derived statistic").

``--order-rule``
    Is ascending ``NONDERIV_TRANS_SK`` the XML document order? The DERA readme §5.3 calls the
    SK a *surrogate key* and defines no ordinal, so this cannot be assumed — it is measured
    against ``insider_transactions.txn_row_num`` on every accession both stores hold.
    Carries a **negative control**: the same comparison with one SK pair per accession
    transposed MUST report discordance, or the check is structurally unable to fail
    (prevention-log: "a check that passes may be structurally UNABLE to fail"). Exclusions are
    reported with counts, and concordance is reported both overall and restricted to the
    same-date groups that actually exercise the tie-break.

``--census``
    What the affected population is made of: the provenance split of tied groups, how many
    disagree on value (NULL-aware), how many the XML could resolve, and the date span of the
    cohort it could not.

``--ab``
    Full-population A/B of the projection's winner set, old rule vs new, **after** the #1805
    de-collision filter — a FULL OUTER comparison over
    ``(instrument_id, holder_identity_key, ownership_nature)`` keys so a key the de-collision
    DROPS is visible. The control arm is the STORED ``ownership_insiders_current``, not a
    re-derivation (``full-population-ab.md``: never simulate the control); the old rule is
    re-derived as well, purely to prove the harness reproduces what is stored. Includes the
    XML oracle, evaluated only on keys with no XML sibling — where the plain XML row already
    wins, agreement with it says nothing about SK ordering.

Usage (read-only; one REPEATABLE READ snapshot per run, verified not asserted):

    PYTHONPATH=. uv run python -m scripts.audit_3146_insider_line_order --order-rule
    PYTHONPATH=. uv run python -m scripts.audit_3146_insider_line_order --census
    PYTHONPATH=. uv run python -m scripts.audit_3146_insider_line_order --ab

Exits 1 on an empty census, on any discordance in the order rule, on a negative control that
fails to fail, on the harness disagreeing with stored ``_current``, and on any key DELETED by
the new ordering — a measurement that cannot fail is worth nothing, and a released ordering
that deletes an operator-visible holder is not shippable.
"""

from __future__ import annotations

import argparse
import sys
from typing import Any, LiteralString, NoReturn, cast

import psycopg
from psycopg import IsolationLevel

from app.config import settings
from app.services.ownership_observations import (
    _INSIDER_DUAL_PIPELINE_DECOLLISION,
    _INSIDER_WINNER_ORDER_TAIL,
)

# The ORDER BY tail as it stood BEFORE #3146, copied verbatim from the pre-change source.
# The A/B control must be the old rule itself, never the new code with a flag flipped.
_OLD_ORDER_TAIL: LiteralString = """
                        source_document_id ASC
"""

# Everything above the tail is common to both arms and to both refresh functions.
_WINNER_ORDER_HEAD: LiteralString = """
                        instrument_id,
                        holder_identity_key,
                        ownership_nature,
                        CASE source
                            WHEN 'form4'    THEN 1
                            WHEN 'form3'    THEN 2
                            WHEN '13d'      THEN 3
                            WHEN '13g'      THEN 3
                            WHEN 'def14a'   THEN 4
                            WHEN '13f'      THEN 5
                            WHEN 'nport'    THEN 6
                            WHEN 'ncsr'     THEN 6
                            WHEN 'xbrl_dei' THEN 7
                            WHEN '10k_note' THEN 8
                            WHEN 'finra_si' THEN 9
                            ELSE 10
                        END ASC,
                        period_end DESC,
                        filed_at DESC,
                        source ASC,
"""


def _materialise_winners(cur: psycopg.Cursor[Any], *, target: str, order_tail: str) -> None:
    """Build one arm's post-de-collision winner set for the WHOLE population.

    Mirrors ``refresh_insiders_current_batch``'s source query — same DISTINCT ON key, same
    ORDER BY head, and the de-collision filter applied to the ``winners`` set (never to raw
    observations, which would drop a different set — see the constant's own comment).

    ⚠ The two steps are split because the de-collision predicate is a correlated ``EXISTS``
    over ``winners``. In production ``winners`` is one instrument's handful of rows; at
    full-population scale the same CTE re-scans ~2M rows per probe and does not finish. A TEMP
    TABLE named ``winners`` with the EXISTS's own key indexed makes it index-driven while
    leaving ``_INSIDER_DUAL_PIPELINE_DECOLLISION`` textually verbatim — the filter must be the
    shipped one, not a re-spelling of it.
    """
    cur.execute(
        cast(
            LiteralString,
            f"""
        CREATE TEMP TABLE winners AS
        SELECT DISTINCT ON (instrument_id, holder_identity_key, ownership_nature)
            instrument_id, holder_cik, holder_name, holder_identity_key,
            ownership_nature, source, source_document_id, source_accession,
            filed_at, period_end, shares
        FROM ownership_insiders_observations
        WHERE known_to IS NULL
        ORDER BY {_WINNER_ORDER_HEAD} {order_tail}
    """,
        )
    )
    cur.execute("CREATE INDEX ON winners(instrument_id, holder_cik, source_accession)")
    cur.execute("ANALYZE winners")
    cur.execute(
        cast(
            LiteralString,
            f"""
        CREATE TEMP TABLE {target} AS
        SELECT w.* FROM winners w
        {_INSIDER_DUAL_PIPELINE_DECOLLISION}
    """,
        )
    )
    cur.execute(cast(LiteralString, f"CREATE INDEX ON {target}(instrument_id, holder_identity_key, ownership_nature)"))
    cur.execute(cast(LiteralString, f"ANALYZE {target}"))
    cur.execute("DROP TABLE winners")


def _fail(msg: str) -> NoReturn:
    print(f"FAIL: {msg}")
    sys.exit(1)


def _verify_snapshot(cur: psycopg.Cursor[Any], label: str, expect: Any = None) -> Any:
    """Prove the whole run sits in ONE snapshot rather than asserting it in a docstring.

    #3115's first verdict quoted figures from two different snapshots under a docstring
    promising one; the fix was to read ``pg_stat_activity`` instead of trusting the block.

    ⚠ ``clock_timestamp()``, not ``now()`` — inside a transaction ``now()`` IS the transaction
    start, so ``now() - xact_start`` is identically zero and an elapsed-time check written that
    way can never show anything. Returns ``xact_start`` so the caller can assert the closing
    read saw the SAME transaction, which is the part that actually carries the guarantee.
    """
    row = cur.execute(
        "SELECT xact_start, clock_timestamp() - xact_start FROM pg_stat_activity WHERE pid = pg_backend_pid()"
    ).fetchone()
    if row is None or row[0] is None:
        _fail(f"{label}: no open transaction — the snapshot was not held")
    print(f"[snapshot] {label}: held since {row[0]} (elapsed {row[1]})")
    if expect is not None and row[0] != expect:
        _fail(f"{label}: snapshot changed mid-run ({expect} → {row[0]}) — figures are not comparable")
    return row[0]


# ---------------------------------------------------------------------------
# --order-rule
# ---------------------------------------------------------------------------


def _build_order_rule_tables(cur: psycopg.Cursor[Any]) -> None:
    cur.execute("""
      CREATE TEMP TABLE xml_multi AS
      SELECT t.accession_number AS accn, t.instrument_id, t.txn_row_num AS rn,
             t.txn_date AS d, t.post_transaction_shares::numeric AS post
      FROM insider_transactions t
      WHERE NOT t.is_derivative AND t.post_transaction_shares IS NOT NULL
        AND NOT t.txn_date_invalid
        AND t.accession_number IN (
          SELECT accession_number FROM insider_transactions
          WHERE NOT is_derivative AND post_transaction_shares IS NOT NULL
            AND NOT txn_date_invalid
          GROUP BY 1 HAVING count(*) > 1)
    """)
    cur.execute("ANALYZE xml_multi")
    cur.execute("CREATE TEMP TABLE xa AS SELECT DISTINCT accn FROM xml_multi")
    cur.execute("CREATE INDEX ON xa(accn)")
    cur.execute("ANALYZE xa")
    # DERA side, restricted to accessions the XML side actually holds. The SK is read from
    # the doc id, which is the column the shipped ORDER BY reads — not from ``source_field``,
    # so the test exercises the same carrier the fix does.
    cur.execute("""
      CREATE TEMP TABLE dera_rows AS
      SELECT DISTINCT o.source_accession AS accn, o.instrument_id,
             split_part(o.source_document_id, ':NDT:', 2)::numeric AS sk,
             o.period_end AS d, o.shares::numeric AS post
      FROM ownership_insiders_observations o
      JOIN xa ON xa.accn = o.source_accession
      WHERE o.known_to IS NULL AND o.source_document_id ~ ':NDT:[0-9]+$'
    """)
    cur.execute("ANALYZE dera_rows")
    # 1:1 matchable rows only — a repeated (accession, date, balance) cannot be attributed to
    # one line on either side. Excluded rows are COUNTED below, not silently dropped.
    cur.execute("""
      CREATE TEMP TABLE d1 AS SELECT accn, instrument_id, sk, d, post FROM
        (SELECT *, count(*) OVER (PARTITION BY accn, d, post) c FROM dera_rows) z WHERE c = 1
    """)
    cur.execute("""
      CREATE TEMP TABLE x1 AS SELECT accn, instrument_id, rn, d, post FROM
        (SELECT *, count(*) OVER (PARTITION BY accn, d, post) c FROM xml_multi) z WHERE c = 1
    """)
    cur.execute("ANALYZE d1")
    cur.execute("ANALYZE x1")
    cur.execute("""
      CREATE TEMP TABLE m AS
      SELECT d1.accn, d1.sk, x1.rn, d1.d
      FROM d1 JOIN x1
        ON d1.accn = x1.accn AND d1.instrument_id = x1.instrument_id
       AND d1.d = x1.d AND d1.post = x1.post
    """)
    cur.execute("ANALYZE m")


_CONCORDANCE_SQL: LiteralString = """
      WITH r AS (
        SELECT accn,
               rank() OVER (PARTITION BY accn ORDER BY sk) AS rsk,
               rank() OVER (PARTITION BY accn ORDER BY rn) AS rrn,
               count(*) OVER (PARTITION BY accn) AS n
        FROM {src}
      )
      SELECT count(*) AS accessions,
             count(*) FILTER (WHERE ok) AS concordant,
             count(*) FILTER (WHERE NOT ok) AS discordant
      FROM (SELECT accn, bool_and(rsk = rrn) AS ok FROM r WHERE n > 1 GROUP BY accn) z
"""


def run_order_rule(cur: psycopg.Cursor[Any]) -> None:
    print("\n=== #3146 --order-rule: is NONDERIV_TRANS_SK order the XML document order? ===")
    print("Source: DERA Insider Transactions readme §5.3 defines NONDERIV_TRANS_SK as a")
    print("SURROGATE KEY and the table carries no ordinal, so this is measured, not assumed.")
    _build_order_rule_tables(cur)

    xml_rows, xml_accn = cur.execute("SELECT count(*), count(DISTINCT accn) FROM xml_multi").fetchone()  # type: ignore[misc]
    dera_rows, dera_accn = cur.execute("SELECT count(*), count(DISTINCT accn) FROM dera_rows").fetchone()  # type: ignore[misc]
    m_rows, m_accn = cur.execute("SELECT count(*), count(DISTINCT accn) FROM m").fetchone()  # type: ignore[misc]
    print(f"\nXML multi-line rows           {xml_rows:>9,}  accessions {xml_accn:>8,}")
    print(f"DERA :NDT: rows on those      {dera_rows:>9,}  accessions {dera_accn:>8,}")
    print(f"matched 1:1 (accn, date, bal) {m_rows:>9,}  accessions {m_accn:>8,}")
    print("\nExclusions, with reasons (rows dropped before matching):")
    d1_rows = cur.execute("SELECT count(*) FROM d1").fetchone()[0]  # type: ignore[index]
    x1_rows = cur.execute("SELECT count(*) FROM x1").fetchone()[0]  # type: ignore[index]
    d_excl = dera_rows - d1_rows
    x_excl = xml_rows - x1_rows
    unmatched = d1_rows - m_rows
    print(f"  repeated (accn, date, balance) on the DERA side {d_excl:>9,}")
    print(f"  repeated (accn, date, balance) on the XML side  {x_excl:>9,}")
    print(f"  no counterpart on the other side                {unmatched:>9,}")

    acc, conc, disc = cur.execute(cast(LiteralString, _CONCORDANCE_SQL.format(src="m"))).fetchone()  # type: ignore[misc]
    print(f"\nALL multi-line accessions:  tested {acc:,}  concordant {conc:,}  discordant {disc:,}")

    # Restricted to same-date groups — the population that actually reaches this tie-break,
    # since period_end DESC separates lines on different dates before the tail is consulted.
    cur.execute("""
      CREATE TEMP TABLE m_sameday AS
      SELECT * FROM m WHERE (accn, d) IN (SELECT accn, d FROM m GROUP BY 1, 2 HAVING count(*) > 1)
    """)
    cur.execute("ANALYZE m_sameday")
    sd_acc, sd_conc, sd_disc = cur.execute(
        cast(LiteralString, _CONCORDANCE_SQL.format(src="(SELECT accn || '|' || d AS accn, sk, rn FROM m_sameday) q"))
    ).fetchone()  # type: ignore[misc]
    print(f"SAME-DATE groups only:      tested {sd_acc:,}  concordant {sd_conc:,}  discordant {sd_disc:,}")

    # Negative control — transpose the two lowest SKs of each accession. A test that cannot
    # fail proves nothing, so this MUST report discordance.
    cur.execute("""
      CREATE TEMP TABLE m_swapped AS
      WITH ranked AS (
        SELECT accn, sk, rn, row_number() OVER (PARTITION BY accn ORDER BY sk) AS pos,
               count(*) OVER (PARTITION BY accn) AS n FROM m)
      SELECT accn, sk,
             CASE WHEN n > 1 AND pos = 1 THEN lead(rn) OVER (PARTITION BY accn ORDER BY pos)
                  WHEN n > 1 AND pos = 2 THEN lag(rn) OVER (PARTITION BY accn ORDER BY pos)
                  ELSE rn END AS rn
      FROM ranked
    """)
    cur.execute("ANALYZE m_swapped")
    nc_acc, nc_conc, nc_disc = cur.execute(cast(LiteralString, _CONCORDANCE_SQL.format(src="m_swapped"))).fetchone()  # type: ignore[misc]
    print(f"NEGATIVE CONTROL (swapped): tested {nc_acc:,}  concordant {nc_conc:,}  discordant {nc_disc:,}")

    span = cur.execute("SELECT min(d), max(d) FROM m").fetchone()
    print(f"\nperiod_end span of the tested cohort: {span[0]} … {span[1]}")  # type: ignore[index]
    print("⚠ This cohort is the accessions this deployment holds parsed XML for. It is NOT")
    print("  evidence about accessions with no XML, and not an SEC guarantee — re-run as the")
    print("  drift detector after any dataset release.")

    if acc == 0:
        _fail("--order-rule tested zero accessions")
    if disc != 0:
        _fail(f"--order-rule found {disc:,} discordant accessions — the SK is NOT document order")
    if nc_disc == 0:
        _fail("negative control reported ZERO discordance — the comparison cannot fail")


# ---------------------------------------------------------------------------
# --census
# ---------------------------------------------------------------------------

_CENSUS_TIED: LiteralString = """
      CREATE TEMP TABLE tied AS
      SELECT c.instrument_id, c.holder_identity_key, c.ownership_nature,
             c.source_document_id AS win_doc, c.shares AS win_shares,
             c.source_accession, c.period_end,
             o.source_document_id AS sib_doc, o.shares AS sib_shares
      FROM ownership_insiders_current c
      JOIN ownership_insiders_observations o
        ON o.instrument_id = c.instrument_id
       AND o.holder_identity_key = c.holder_identity_key
       AND o.ownership_nature = c.ownership_nature
       AND o.source = c.source
       AND o.period_end = c.period_end
       AND o.filed_at = c.filed_at
       AND o.known_to IS NULL
       AND o.source_document_id <> c.source_document_id
"""


def run_census(cur: psycopg.Cursor[Any]) -> None:
    print("\n=== #3146 --census: what the affected population is made of ===")
    cur.execute(_CENSUS_TIED)
    cur.execute("ANALYZE tied")
    total = cur.execute("SELECT count(*) FROM tied").fetchone()[0]  # type: ignore[index]
    if total == 0:
        _fail("--census found no tied observations at all")

    print("\nProvenance of the tie (winner vs its tied sibling):")
    cur.execute("""
      SELECT (win_doc ~ ':(NDT|NDH):') AS winner_dera, (sib_doc ~ ':(NDT|NDH):') AS sibling_dera,
             count(*) AS pairs,
             count(DISTINCT (instrument_id, holder_identity_key, ownership_nature)) AS keys
      FROM tied GROUP BY 1, 2 ORDER BY 3 DESC
    """)
    for winner_dera, sib_dera, pairs, keys in cur.fetchall():
        print(f"  winner_dera={winner_dera!s:<5} sibling_dera={sib_dera!s:<5} pairs {pairs:>8,}  keys {keys:>8,}")

    # NULL-aware value disagreement: count(DISTINCT shares) cannot see NULL-vs-value.
    cur.execute("""
      CREATE TEMP TABLE grp AS
      SELECT instrument_id, holder_identity_key, ownership_nature, source_accession, period_end,
             bool_or(win_shares IS DISTINCT FROM sib_shares) AS disagrees
      FROM tied WHERE win_doc ~ ':NDT:' GROUP BY 1, 2, 3, 4, 5
    """)
    cur.execute("ANALYZE grp")
    groups, dis_groups, dis_keys, dis_instr = cur.execute("""
      SELECT count(*), count(*) FILTER (WHERE disagrees),
             count(DISTINCT (instrument_id, holder_identity_key, ownership_nature)) FILTER (WHERE disagrees),
             count(DISTINCT instrument_id) FILTER (WHERE disagrees)
      FROM grp
    """).fetchone()  # type: ignore[misc]
    print(f"\nDERA-won groups                       {groups:>8,}")
    print(f"  … whose tied set disagrees on value {dis_groups:>8,}  (NULL-aware)")
    print(f"  distinct (instrument, holder, nature) keys {dis_keys:>8,}")
    print(f"  instruments                                {dis_instr:>8,}")

    print("\nHow far the XML document order could reach:")
    cur.execute("""
      SELECT count(*) FILTER (WHERE xml_line) AS resolvable,
             count(*) FILTER (WHERE NOT xml_line) AS unresolvable,
             count(DISTINCT source_accession) FILTER (WHERE NOT xml_line) AS accessions_without_xml,
             min(period_end) FILTER (WHERE NOT xml_line) AS min_pe,
             max(period_end) FILTER (WHERE NOT xml_line) AS max_pe
      FROM (SELECT g.*, EXISTS (SELECT 1 FROM insider_transactions t
                                 WHERE t.instrument_id = g.instrument_id
                                   AND t.accession_number = g.source_accession
                                   AND NOT t.is_derivative AND t.txn_date = g.period_end) AS xml_line
              FROM grp g WHERE g.disagrees) z
    """)
    res, unres, accn_no_xml, min_pe, max_pe = cur.fetchone()  # type: ignore[misc]
    pct = (res / (res + unres) * 100) if (res + unres) else 0.0
    print(f"  resolvable from held XML   {res:>8,}  ({pct:.1f}%)")
    print(f"  NOT resolvable             {unres:>8,}  over {accn_no_xml:,} accessions, period_end {min_pe} … {max_pe}")


# ---------------------------------------------------------------------------
# --ab
# ---------------------------------------------------------------------------


def run_ab(cur: psycopg.Cursor[Any]) -> None:
    print("\n=== #3146 --ab: full-population A/B of the projection's winner set ===")
    _materialise_winners(cur, target="w_old", order_tail=_OLD_ORDER_TAIL)
    print("  old arm materialised")
    _materialise_winners(cur, target="w_new", order_tail=_INSIDER_WINNER_ORDER_TAIL)
    print("  new arm materialised")
    n_old = cur.execute("SELECT count(*) FROM w_old").fetchone()[0]  # type: ignore[index]
    n_new = cur.execute("SELECT count(*) FROM w_new").fetchone()[0]  # type: ignore[index]
    print(f"\nwinner rows  old {n_old:,}   new {n_new:,}")
    if n_old == 0:
        _fail("--ab derived an empty old winner set")

    # Harness validation, SYMMETRIC. A one-way EXCEPT sees only rows the re-derivation adds and
    # would miss a stored row it drops, which is the direction that matters — a holder that
    # disappears from the operator's card. Reported both ways.
    stored_n = cur.execute("SELECT count(*) FROM ownership_insiders_current").fetchone()[0]  # type: ignore[index]
    only_derived, only_stored = cur.execute("""
      SELECT (SELECT count(*) FROM (
                SELECT instrument_id, holder_identity_key, ownership_nature, source_document_id, shares FROM w_old
                EXCEPT
                SELECT instrument_id, holder_identity_key, ownership_nature, source_document_id, shares
                  FROM ownership_insiders_current) a),
             (SELECT count(*) FROM (
                SELECT instrument_id, holder_identity_key, ownership_nature, source_document_id, shares
                  FROM ownership_insiders_current
                EXCEPT
                SELECT instrument_id, holder_identity_key, ownership_nature, source_document_id, shares FROM w_old) b)
    """).fetchone()  # type: ignore[misc]
    print(f"stored _current rows {stored_n:,}")
    print(f"  in the re-derived OLD arm but not stored: {only_derived:,}   (instruments never projected)")
    print(f"  stored but NOT in the re-derived OLD arm: {only_stored:,}   ← must be 0")

    # The release gate the operator actually cares about: does the new rule remove a holder
    # that is on the card TODAY? Measured against the STORED table, not against a re-derivation
    # of the old rule (full-population-ab.md: never simulate the control).
    stored_added, stored_deleted, stored_changed = cur.execute("""
      SELECT count(*) FILTER (WHERE c.instrument_id IS NULL),
             count(*) FILTER (WHERE n.instrument_id IS NULL),
             count(*) FILTER (WHERE c.instrument_id IS NOT NULL AND n.instrument_id IS NOT NULL
                              AND c.shares IS DISTINCT FROM n.shares)
      FROM ownership_insiders_current c FULL OUTER JOIN w_new n
        ON c.instrument_id = n.instrument_id
       AND c.holder_identity_key = n.holder_identity_key
       AND c.ownership_nature = n.ownership_nature
    """).fetchone()  # type: ignore[misc]
    print("\nNEW arm vs STORED _current (the operator-visible control):")
    print(f"  keys added     {stored_added:,}")
    print(f"  keys REMOVED   {stored_deleted:,}   ← release condition: must be 0")
    print(f"  value changed  {stored_changed:,}")

    print("\nFULL OUTER comparison over (instrument, holder, nature) keys:")
    cur.execute("""
      SELECT count(*) FILTER (WHERE o.instrument_id IS NULL) AS added,
             count(*) FILTER (WHERE n.instrument_id IS NULL) AS deleted,
             count(*) FILTER (WHERE o.instrument_id IS NOT NULL AND n.instrument_id IS NOT NULL
                              AND o.shares IS DISTINCT FROM n.shares) AS value_changed,
             count(*) FILTER (WHERE o.instrument_id IS NOT NULL AND n.instrument_id IS NOT NULL
                              AND o.source_document_id IS DISTINCT FROM n.source_document_id) AS doc_changed,
             count(DISTINCT coalesce(o.instrument_id, n.instrument_id))
               FILTER (WHERE o.shares IS DISTINCT FROM n.shares) AS instruments_touched
      FROM w_old o FULL OUTER JOIN w_new n
        ON o.instrument_id = n.instrument_id
       AND o.holder_identity_key = n.holder_identity_key
       AND o.ownership_nature = n.ownership_nature
    """)
    added, deleted, value_changed, doc_changed, instruments = cur.fetchone()  # type: ignore[misc]
    print(f"  added          {added:>8,}")
    print(f"  deleted        {deleted:>8,}   ← release condition: must be 0")
    print(f"  value changed  {value_changed:>8,}   over {instruments:,} instruments")
    print(f"  doc changed    {doc_changed:>8,}")

    print("\nDirection of the change (new vs old balance):")
    cur.execute("""
      SELECT count(*) FILTER (WHERE n.shares > o.shares) AS new_larger,
             count(*) FILTER (WHERE n.shares < o.shares) AS new_smaller,
             count(*) FILTER (WHERE n.shares IS NULL OR o.shares IS NULL) AS null_involved
      FROM w_old o JOIN w_new n
        ON o.instrument_id = n.instrument_id
       AND o.holder_identity_key = n.holder_identity_key
       AND o.ownership_nature = n.ownership_nature
      WHERE o.shares IS DISTINCT FROM n.shares
    """)
    larger, smaller, null_involved = cur.fetchone()  # type: ignore[misc]
    print(f"  new larger {larger:,}   new smaller {smaller:,}   NULL involved {null_involved:,}")

    print("\nCross-accession winners must NOT move (the change is within-filing only):")
    cross = cur.execute("""
      SELECT count(*) FROM w_old o JOIN w_new n
        ON o.instrument_id = n.instrument_id
       AND o.holder_identity_key = n.holder_identity_key
       AND o.ownership_nature = n.ownership_nature
      WHERE split_part(o.source_document_id, ':', 1) IS DISTINCT FROM split_part(n.source_document_id, ':', 1)
    """).fetchone()[0]  # type: ignore[index]
    print(f"  keys whose winning ACCESSION changed: {cross:,}   ← must be 0")

    print("\n:NDH: (Form 3 holdings) rows must NOT move — no ordinal was established for them:")
    ndh = cur.execute("""
      SELECT count(*) FROM w_old o JOIN w_new n
        ON o.instrument_id = n.instrument_id
       AND o.holder_identity_key = n.holder_identity_key
       AND o.ownership_nature = n.ownership_nature
      WHERE o.source_document_id ~ ':NDH:' AND o.source_document_id IS DISTINCT FROM n.source_document_id
    """).fetchone()[0]  # type: ignore[index]
    print(f"  :NDH: winners that moved: {ndh:,}   ← must be 0")

    _run_xml_oracle(cur)

    if deleted != 0:
        _fail(f"{deleted:,} keys DELETED by the new ordering — not shippable")
    if stored_deleted != 0:
        _fail(f"{stored_deleted:,} keys present in STORED _current are absent from the new arm")
    if only_stored != 0:
        _fail(f"{only_stored:,} stored rows the re-derived OLD arm cannot reproduce — harness is not the control")
    if cross != 0:
        _fail(f"{cross:,} keys changed winning accession — the change is not within-filing")
    if ndh != 0:
        _fail(f"{ndh:,} :NDH: winners moved — the tie-break leaked past :NDT:")


def _run_xml_oracle(cur: psycopg.Cursor[Any]) -> None:
    """Ground truth: on keys with NO XML sibling, does the new winner match the XML's last line?

    Restricted to DERA-won keys because where the plain XML observation already wins the key,
    agreement with the XML says nothing about the SK ordering (Codex ckpt-1 #21). The oracle
    reproduces the XML path's own filters — ``txn_date_invalid`` and NULL post-balances
    excluded, per ``insider_transactions.py``'s holdings reduction.

    ⚠ **Grouped the way the XML path groups**, per ``(filer_cik, direct_indirect)``, not one
    last line for the whole accession (Codex ckpt-2 P2). An accession can carry a Direct and an
    Indirect series at once; taking the accession-wide last line would score a DERA `direct`
    key that picked the final INDIRECT balance as "agreement", masking exactly the #2385/#2386
    conflation the proposal says it does not fix. The cohorts are therefore reported apart:

    * **single-series accessions** — one ``(filer_cik, direct_indirect)`` series, so the
      terminal balance is unambiguous. This is the real ground truth.
    * **multi-series accessions** — the DERA key's role-derived nature cannot be mapped to a
      D/I series at all, so the oracle can only ask whether the winner is SOME series'
      terminal line. Reported separately and never pooled into the headline.
    """
    print("\nXML oracle — DERA-won keys whose accession we ALSO hold parsed XML for:")
    cur.execute("""
      CREATE TEMP TABLE xml_series AS
      SELECT DISTINCT ON (instrument_id, accession_number, filer_cik, direct_indirect)
             instrument_id, accession_number, filer_cik, direct_indirect,
             post_transaction_shares::numeric AS terminal
        FROM insider_transactions
       WHERE NOT is_derivative AND NOT txn_date_invalid AND post_transaction_shares IS NOT NULL
       ORDER BY instrument_id, accession_number, filer_cik, direct_indirect,
                txn_date DESC, txn_row_num DESC
    """)
    cur.execute("CREATE INDEX ON xml_series(instrument_id, accession_number)")
    cur.execute("ANALYZE xml_series")
    cur.execute("""
      CREATE TEMP TABLE oracle AS
      SELECT n.instrument_id, n.shares AS new_shares, o.shares AS old_shares,
             s.n_series, s.terminals
        FROM w_new n
        JOIN w_old o
          ON o.instrument_id = n.instrument_id
         AND o.holder_identity_key = n.holder_identity_key
         AND o.ownership_nature = n.ownership_nature
        JOIN LATERAL (
              SELECT count(*) AS n_series, array_agg(x.terminal) AS terminals
                FROM xml_series x
               WHERE x.instrument_id = n.instrument_id
                 AND x.accession_number = split_part(n.source_document_id, ':', 1)
             ) s ON s.n_series > 0
       WHERE n.source_document_id ~ ':NDT:'
    """)
    cur.execute("ANALYZE oracle")
    cur.execute("""
      SELECT count(*) FILTER (WHERE n_series = 1) AS single,
             count(*) FILTER (WHERE n_series = 1 AND new_shares = terminals[1]) AS single_new,
             count(*) FILTER (WHERE n_series = 1 AND old_shares = terminals[1]) AS single_old,
             count(*) FILTER (WHERE n_series > 1) AS multi,
             count(*) FILTER (WHERE n_series > 1 AND new_shares = ANY(terminals)) AS multi_new,
             count(*) FILTER (WHERE n_series > 1 AND old_shares = ANY(terminals)) AS multi_old
      FROM oracle
    """)
    single, single_new, single_old, multi, multi_new, multi_old = cur.fetchone()  # type: ignore[misc]
    if single == 0 and multi == 0:
        print("  no testable keys — oracle inconclusive")
        return
    if single:
        print(f"  UNAMBIGUOUS cohort — accession carries one (filer, D/I) series: {single:,} keys")
        print(f"    NEW winner is that series' terminal line: {single_new:,} ({single_new / single * 100:.1f}%)")
        print(f"    OLD winner is that series' terminal line: {single_old:,} ({single_old / single * 100:.1f}%)")
    if multi:
        print(f"  CONFLATED cohort — accession carries >1 series, D/I unmappable (#2385): {multi:,} keys")
        print(f"    NEW winner is SOME series' terminal line: {multi_new:,} ({multi_new / multi * 100:.1f}%)")
        print(f"    OLD winner is SOME series' terminal line: {multi_old:,} ({multi_old / multi * 100:.1f}%)")
        print("    ⚠ this cohort sizes the conflation exposure; it is NOT pooled into the headline.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--order-rule", action="store_true", help="is the DERA SK the XML document order?")
    parser.add_argument("--census", action="store_true", help="what the affected population is made of")
    parser.add_argument("--ab", action="store_true", help="full-population A/B of the winner set")
    args = parser.parse_args()
    if not (args.order_rule or args.census or args.ab):
        parser.error("choose at least one of --order-rule / --census / --ab")

    conn = psycopg.connect(settings.database_url)
    conn.isolation_level = IsolationLevel.REPEATABLE_READ
    try:
        with conn.transaction(), conn.cursor() as cur:
            cur.execute("SET LOCAL statement_timeout = '30min'")
            cur.execute("SET LOCAL work_mem = '256MB'")  # two full-population DISTINCT ON sorts
            started = _verify_snapshot(cur, "start")
            if args.order_rule:
                run_order_rule(cur)
            if args.census:
                run_census(cur)
            if args.ab:
                run_ab(cur)
            _verify_snapshot(cur, "end", expect=started)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
