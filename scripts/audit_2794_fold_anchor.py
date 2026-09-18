"""Read-only census behind the #2794 verdict — what the control-group fold could key on
INSTEAD of an exact share value, and what the ticket's worked case actually is.

#2794 reports that the control-group fold buckets candidates by EXACT ``shares``, so a fold
is anchor-dependent: remove any member row and the survivors may no longer match, and a
block counted ONCE is counted N times. It proposes keying membership on the record-holder /
``natureOfOwnership`` chain (#2408) instead.

Three censuses, one per claim in the verdict. **Every figure the verdict quotes is computed
here at run time** — nothing is hand-written into prose, so a re-harvest cannot leave a
number lying (prevention-log: "never hardcode a derived statistic").

``--edges``
    Every #2408 naming edge on the full population, classified by whether the two endpoints
    carry the same share value. Answers: does a value-free membership signal exist, and does
    it reach the cases #2794 names?

``--joint``
    Joint-accession (Rule 16a-3(j) co-filed) insider clusters, split by whether their
    members report one value or several, and by the Rule 16a-1(a)(2) ownership-form shape.
    Answers: how much does the exact-value key refuse on clusters that already hold direct
    co-filing evidence?

``--balances``
    (instrument, holder, nature) keys whose ``ownership_insiders_current`` winner is decided
    by the projection's final lexical tie-break (``source_document_id ASC``,
    ``ownership_observations.py:341``) against another observation from the SAME filing at a
    DIFFERENT value. Answers: what is the ticket's worked case actually made of?

``--reach``
    Joins the two above. For every cluster the exact-value key REFUSES, does a usable naming
    edge exist BETWEEN ITS OWN MEMBERS? Answers the one question the previous round left
    open: the naming signal is real and large, but nothing established that it lands on the
    cases this ticket is about.

    ⚠ The ticket's recorded next step is phrased instrument-level ("intersect the 154
    unique-unequal-edge instruments with the 146 refused-cluster instruments"). That figure
    is computed here too, and it is an UPPER BOUND, not the answer: an instrument may carry
    its naming edge on one accession and its refused cluster on another, and the
    instrument-level join counts that as a hit. Same defect shape as the prevention-log
    entry "instrument-level set arithmetic answering an identity-level question"
    (``docs/review-prevention-log.md``, #2230 2026-08-20). The per-cluster figure is the one
    that decides.

``--pipeline``
    The same-Table-I-line test, and the one that re-homes the ticket. For every cluster the
    exact-value key refuses, are its two values TWO DISCLOSURES or ONE line set read twice?
    A joint Form 3/4 carries one Table I attributed to nobody, and our two ingest paths
    invent different attributions for it — the XML parser gives every line to ``filers[0]``,
    the DERA bulk path gives every line to every reporting owner. Answers whether the fold
    anchor is implicated at all. Carries a deterministic mis-paired-accession control so the
    lookup cannot pass by matching anything, and prices the blast radius of the fix the
    finding implies.

Usage (read-only, one REPEATABLE READ snapshot per mode):

    PYTHONPATH=. uv run python -m scripts.audit_2794_fold_anchor --edges --out /tmp/a2794.jsonl
    PYTHONPATH=. uv run python -m scripts.audit_2794_fold_anchor --summarise /tmp/a2794.jsonl
    PYTHONPATH=. uv run python -m scripts.audit_2794_fold_anchor --joint
    PYTHONPATH=. uv run python -m scripts.audit_2794_fold_anchor --balances
    PYTHONPATH=. uv run python -m scripts.audit_2794_fold_anchor --reach /tmp/a2794.jsonl
    PYTHONPATH=. uv run python -m scripts.audit_2794_fold_anchor --pipeline

Exits 1 on any per-instrument error, on an empty census, and on a summary whose input held
no instruments — a census that measured nothing must not look clean.

⚠ ``--balances`` exits **2** unconditionally. It is withdrawn pending a re-spec: its ORDER BY
is a hand copy of the pre-#3146 winner rule, so it measures something production no longer
does. See the ``BALANCES_SQL`` header. That is the same principle one step further on — a
census measuring the WRONG thing must not look clean either, and only an exit code says so to
a caller that is not a person.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from decimal import Decimal
from typing import Any, Final, LiteralString

import psycopg

from app.config import settings
from app.db.snapshot import snapshot_read
from app.services import ownership_rollup as orl
from app.services.insider_transactions import form4_retention_cutoff

# The rollup's own insider read, narrowed to the columns an edge needs. Both filters are the
# module's OWN constants rather than re-spelled copies: the #2788 Form 4 retention bound and
# the #788 dual-pipeline de-collision. Omitting either would build edges out of rows the
# operator-visible slice does not contain — and the de-collision omission is the one that
# reads as clean, because it only ever ADDS a duplicate identity (the Codex checkpoint-2
# finding recorded above ``_INSIDER_DUAL_PIPELINE_DECOLLISION_SQL``).
#
# ⚠ This is the CANDIDATE population, not the rendered one. The rollup applies cross-source
# dedup, institutional-family reconciliation and owner-once downstream of here, so a share
# total computed off this set is an upper bound on what any change could move — never a
# measurement of rendered double-counting.
_LIVE_INSIDER_ROWS = f"""
    SELECT oc.instrument_id, oc.holder_identity_key, oc.holder_cik, oc.holder_name,
           oc.ownership_nature, oc.source, oc.source_accession, oc.shares,
           (oc.source_document_id !~ ':(NDT|NDH):') AS table_i
      FROM ownership_insiders_current oc
     WHERE oc.source IN ('form3', 'form4')
       AND oc.shares > 0
       AND oc.source_accession IS NOT NULL
       AND btrim(oc.source_accession) <> ''
       AND NOT ({orl._INSIDER_DUAL_PIPELINE_DECOLLISION_SQL})
       AND NOT ({orl._INSIDER_BEYOND_RETENTION_SQL})
"""

HOLDERS_SQL = f"{_LIVE_INSIDER_ROWS} ORDER BY oc.instrument_id"

# One unit of account per (accession, identity) — a holder's several ``ownership_nature``
# rows are ONE cluster member, not several. Codex checkpoint 1 caught the first draft
# quoting a row-level cluster count beside identity-level shares, which does not reconcile.
#
# The cluster definition is ONE string shared by ``JOINT_SQL`` and ``REFUSED_CLUSTERS_SQL``
# rather than two copies of the same CTEs. A copy would let ``--reach``'s denominator drift
# from the census's ``unequal_refused_today`` silently, and a reach rate is only readable
# against the population the census reports.
_JOINT_CLUSTER_CTE = f"""
WITH live AS ({_LIVE_INSIDER_ROWS}),
per_ident AS (
  SELECT instrument_id, source_accession AS acc, holder_identity_key AS ident,
         max(shares) AS shares,
         bool_or(ownership_nature = 'direct') AS any_direct,
         bool_or(ownership_nature = 'direct' AND table_i) AS direct_table_i
    FROM live GROUP BY 1, 2, 3
),
cl AS (
  SELECT instrument_id, acc,
         count(*) AS idents,
         count(DISTINCT shares) AS distinct_values,
         count(*) FILTER (WHERE any_direct) AS n_direct,
         count(*) FILTER (WHERE direct_table_i) AS n_direct_table_i,
         max(shares) AS mx, sum(shares) AS sm
    FROM per_ident GROUP BY 1, 2 HAVING count(*) >= 2
)
"""

JOINT_SQL = f"""
{_JOINT_CLUSTER_CTE}
SELECT count(*) AS joint_accession_clusters,
       count(*) FILTER (WHERE distinct_values = 1) AS equal_value_folds_today,
       count(*) FILTER (WHERE distinct_values > 1) AS unequal_refused_today,
       count(DISTINCT instrument_id) FILTER (WHERE distinct_values > 1) AS instruments_refused,
       sum(sm - mx) FILTER (WHERE distinct_values > 1) AS refused_sum_minus_max,
       count(*) FILTER (WHERE distinct_values > 1 AND n_direct <= 1) AS refused_chain_shape,
       sum(sm - mx) FILTER (WHERE distinct_values > 1 AND n_direct <= 1) AS refused_chain_shape_shares,
       count(DISTINCT instrument_id) FILTER (WHERE distinct_values > 1 AND n_direct <= 1) AS instruments_chain_shape,
       count(*) FILTER (WHERE distinct_values > 1 AND n_direct >= 2) AS refused_multi_direct,
       sum(sm - mx) FILTER (WHERE distinct_values > 1 AND n_direct >= 2) AS refused_multi_direct_shares,
       count(*) FILTER (WHERE distinct_values > 1 AND n_direct_table_i <= 1) AS chain_shape_table_i_gated
  FROM cl
"""

# The same clusters, one row each, restricted to the ones the exact-value key refuses today.
# ``count(*)`` over this MUST equal ``JOINT_SQL``'s ``unequal_refused_today`` — asserted at
# run time in :func:`_reach` rather than trusted, because the two queries are only identical
# by sharing ``_JOINT_CLUSTER_CTE`` and a future edit could break that without a test noticing.
REFUSED_CLUSTERS_SQL = f"""
{_JOINT_CLUSTER_CTE}
SELECT instrument_id, acc
  FROM cl
 WHERE distinct_values > 1
"""

# ``--pipeline`` — the same-Table-I-line test.
#
# The question it answers is prior to every fold-key question this ticket has asked: when two
# members of one accession report different balances, are those two numbers TWO DISCLOSURES,
# or ONE line set read twice by two ingest pipelines?
#
# The source rule says it cannot be the first. ``<nonDerivativeTable>`` is a SIBLING of
# ``<reportingOwner>`` (sec-edgar skill §2.3), so a joint Form 3/4 carries ONE Table I and
# attributes no line to any co-filer. Both pipelines therefore have to invent an attribution,
# and they invent different ones:
#
#   * the XML parser gives every line to ``filers[0]``
#     (``app/services/insider_transactions.py:449``) — one identity per accession;
#   * the DERA bulk path writes every NONDERIV line to EVERY reporting owner
#     (``sec_insider_dataset_ingest._stage_owners``, one staged row per owner) — N identities,
#     each carrying a line that is not theirs.
#
# ``_INSIDER_DUAL_PIPELINE_DECOLLISION`` (#1805) removes the DERA copy only where the SAME
# ``holder_cik`` also has a plain row, i.e. only for ``filers[0]``. Co-filers 2..N keep theirs.
#
# ⚠ Two arms exist so the test can fail rather than merely report:
#
#   * ``value_on_a_line_of_its_OWN_accession`` is the claim. It is meaningless alone — a large
#     enough line set makes any value findable — so
#   * ``…_of_a_BORROWED_accession`` re-runs the identical lookup against a deterministically
#     mis-paired accession (each refused cluster borrows the NEXT one's lines, ordered by
#     accession). A harness that matched anything would score both arms alike. The run FAILS
#     if the control is not far below the claim, which is the prevention-log rule "a
#     measurement that cannot detect its own failure is not evidence".
#
# ``series`` is reported because it bounds the fix, not the finding: an accession whose lines
# span several ``(security_title, direct_indirect)`` groups holds several POSITIONS, which
# Form 4 General Instruction 4(b)(v) permits a joint filing to report separately. So "the
# members share one line set" does NOT license summing or folding them blind.
PIPELINE_SQL = f"""
{_JOINT_CLUSTER_CTE}
, refused AS (SELECT instrument_id, acc FROM cl WHERE distinct_values > 1)
, mem AS (
  SELECT l.instrument_id, l.source_accession AS acc, l.holder_identity_key AS ident,
         bool_or(l.table_i) AS has_xml, bool_or(NOT l.table_i) AS has_dera,
         max(l.shares) AS shares
    FROM live l
    JOIN refused r ON r.instrument_id = l.instrument_id AND r.acc = l.source_accession
   GROUP BY 1, 2, 3
)
, lines AS (
  SELECT t.instrument_id, t.accession_number AS acc, t.post_transaction_shares AS v,
         t.filer_cik, coalesce(t.security_title, '') AS sec,
         coalesce(t.direct_indirect, '') AS di, t.txn_row_num AS ord, 'form4' AS form
    FROM insider_transactions t
    JOIN refused r ON r.instrument_id = t.instrument_id AND r.acc = t.accession_number
   WHERE NOT t.is_derivative AND t.post_transaction_shares IS NOT NULL
  UNION ALL
  SELECT h.instrument_id, h.accession_number, h.shares, h.filer_cik,
         coalesce(h.security_title, ''), coalesce(h.direct_indirect, ''), h.row_num, 'form3'
    FROM insider_initial_holdings h
    JOIN refused r ON r.instrument_id = h.instrument_id AND r.acc = h.accession_number
   WHERE NOT h.is_derivative AND h.shares IS NOT NULL
)
, borrowed AS (
  SELECT instrument_id, acc,
         lead(instrument_id) OVER w AS other_iid,
         lead(acc) OVER w AS other_acc
    FROM refused
  WINDOW w AS (ORDER BY acc, instrument_id)
)
, per_member AS (
  SELECT m.instrument_id, m.acc, m.has_xml, m.shares,
         EXISTS (SELECT 1 FROM lines x
                  WHERE x.instrument_id = m.instrument_id AND x.acc = m.acc
                    AND x.v = m.shares) AS on_own_line,
         EXISTS (SELECT 1 FROM borrowed b
                  JOIN lines x ON x.instrument_id = b.other_iid AND x.acc = b.other_acc
                  WHERE b.instrument_id = m.instrument_id AND b.acc = m.acc
                    AND x.v = m.shares) AS on_borrowed_line
    FROM mem m
)
, per_cluster AS (
  SELECT instrument_id, acc,
         count(*) FILTER (WHERE has_xml) AS n_xml,
         count(*) FILTER (WHERE NOT has_xml) AS n_dera,
         count(DISTINCT shares) FILTER (WHERE NOT has_xml) AS dera_distinct,
         max(shares) FILTER (WHERE has_xml) AS v_xml,
         max(shares) FILTER (WHERE NOT has_xml) AS v_dera
    FROM per_member GROUP BY 1, 2
)
, per_lines AS (
  SELECT r.instrument_id, r.acc,
         count(x.*) AS n_lines,
         count(DISTINCT x.filer_cik) AS n_filer_cik,
         -- ⚠ FILTER, not a bare count(DISTINCT): the LEFT JOIN emits a (NULL, NULL) row for a
         -- cluster with no parsed lines, and ROW(NULL, NULL) is not NULL, so the unfiltered
         -- form scores that cluster as "one series" — the one shape this arm must not claim.
         count(DISTINCT (x.sec, x.di)) FILTER (WHERE x.v IS NOT NULL) AS n_series,
         min(x.form) AS form,
         (array_agg(x.v ORDER BY x.ord ASC))[1] AS first_line_v,
         (array_agg(x.v ORDER BY x.ord DESC))[1] AS last_line_v
    FROM refused r
    LEFT JOIN lines x ON x.instrument_id = r.instrument_id AND x.acc = r.acc
   GROUP BY 1, 2
)
SELECT
  (SELECT count(*) FROM refused)                                         AS refused_clusters,
  (SELECT count(*) FROM mem)                                             AS members,
  (SELECT count(*) FROM mem WHERE has_xml AND NOT has_dera)              AS xml_only_members,
  (SELECT count(*) FROM mem WHERE has_dera AND NOT has_xml)              AS dera_only_members,
  (SELECT count(*) FROM mem WHERE has_xml AND has_dera)                  AS dual_pipeline_members,
  (SELECT count(*) FROM per_member WHERE on_own_line)                    AS value_on_own_accession_line,
  (SELECT count(*) FROM per_member WHERE on_borrowed_line)               AS control_value_on_borrowed_line,
  (SELECT count(*) FROM per_cluster WHERE n_xml = 1)                     AS clusters_with_exactly_one_xml_member,
  (SELECT count(*) FROM per_cluster WHERE dera_distinct = 1)             AS clusters_where_dera_members_agree,
  (SELECT count(*) FROM per_cluster WHERE dera_distinct = 1
                                      AND v_xml IS DISTINCT FROM v_dera) AS clusters_where_only_xml_differs,
  (SELECT count(*) FROM per_lines WHERE n_lines = 0)                     AS clusters_with_no_parsed_lines,
  (SELECT count(*) FROM per_lines WHERE n_filer_cik = 1)                 AS clusters_whose_lines_name_one_filer,
  (SELECT count(*) FROM per_lines WHERE n_series = 1)                    AS clusters_spanning_one_series,
  (SELECT count(*) FROM per_lines WHERE n_series > 1)                    AS clusters_spanning_several_series,
  (SELECT count(*) FROM per_lines l JOIN per_cluster c USING (instrument_id, acc)
     WHERE l.form = 'form3')                                             AS form3_clusters,
  (SELECT count(*) FROM per_lines l JOIN per_cluster c USING (instrument_id, acc)
     WHERE l.form = 'form3' AND c.v_dera = l.first_line_v)               AS form3_dera_took_the_first_line,
  (SELECT count(*) FROM per_lines l JOIN per_cluster c USING (instrument_id, acc)
     WHERE l.form = 'form4')                                             AS form4_clusters,
  (SELECT count(*) FROM per_lines l JOIN per_cluster c USING (instrument_id, acc)
     WHERE l.form = 'form4' AND c.v_dera = l.last_line_v)                AS form4_dera_took_the_last_line
"""

# Blast radius of the fix the finding implies, priced so the next session does not re-derive
# it. Today's de-collision drops a DERA row only when the SAME ``holder_cik`` has a plain row
# on that accession; the widening drops every DERA row on an accession that carries ANY plain
# row, because the DERA fan-out attributes no line to any particular owner.
#
# ⚠ This is an UPPER BOUND on a CANDIDATE set, not a measurement of rendered double-counting:
# the rollup applies cross-source dedup, family reconciliation and owner-once downstream. It
# is priced here to show the change is NOT confined to the 157 refused clusters, which is the
# reason it needs its own spec and a full-population A/B rather than a one-line edit.
WIDENED_DECOLLISION_SQL = """
SELECT count(*)                                                       AS live_insider_rows,
       count(*) FILTER (WHERE dera)                                   AS dera_rows,
       count(*) FILTER (WHERE dera AND any_plain_on_accession)        AS widening_would_drop,
       count(DISTINCT instrument_id)
         FILTER (WHERE dera AND any_plain_on_accession)               AS instruments_touched,
       sum(shares) FILTER (WHERE dera AND any_plain_on_accession)     AS candidate_shares_removed
  FROM (
    SELECT oc.instrument_id, oc.shares,
           (oc.source_document_id ~ ':(NDT|NDH):') AS dera,
           EXISTS (SELECT 1 FROM ownership_insiders_current p
                    WHERE p.instrument_id = oc.instrument_id
                      AND p.source_accession = oc.source_accession
                      AND p.source_document_id !~ ':(NDT|NDH):') AS any_plain_on_accession
      FROM ownership_insiders_current oc
     WHERE oc.source IN ('form3', 'form4')
       AND oc.source_accession IS NOT NULL
       AND btrim(oc.source_accession) <> ''
  ) q
"""

# The projection's own winner rule, spelled from ``ownership_observations.py`` so the census
# asks the same question the MERGE answers. The last key is a LEXICAL tie-break, and this
# census counts the keys it decides between same-filing observations at different values.
#
# ⚠⚠ STALE — DO NOT QUOTE THIS CENSUS'S FIGURE. The ORDER BY below is a hand copy of the
# pre-#3146 rule, and #3146 (PR #3148) added a component to exactly this key. Production now
# orders by ``_INSIDER_WINNER_ORDER_TAIL`` (``ownership_observations.py:299``) — accession
# prefix ASC, then the ``:NDT:`` surrogate key as a NUMERIC DESC (the filing's LAST Table I
# line, document order, measured by ``scripts/audit_3146_insider_line_order.py --order-rule``),
# and only THEN ``source_document_id ASC``. So the lexical key is no longer the tie-break that
# decides these keys; it is the third one, reached only when the prefix ties AND the NDT
# numeric ties or is NULL on both sides (XML rows and Form 3 ``:NDH:`` holdings).
#
# The 13,787 this still prints is therefore not the quantity its own label describes, and its
# ``winner`` join to the live row silently narrows the population to keys where the old and new
# rules happen to agree. Re-speccing it means ranking on the real tail and then counting only
# the keys the FINAL lexical key separates — a different question, not a one-line swap, which
# is why this is flagged rather than half-fixed.
#
# This is a live instance of the prevention-log entry "A test that re-spells a shared sort key
# INLINE is a copy, and a copy drifts when the key gains a component" (#2385, 2026-08-07).
# That entry's own prevention step — grep the key for inline re-spellings before changing it —
# was not run when #3146 changed it.
#
# ⚠ Two things this census deliberately does NOT claim, both caught at Codex checkpoint 1
# after the first draft asserted them:
#
# 1. "Lexically greater ``source_document_id``" is NOT proven to mean "a later transaction".
#    The DERA ``:NDT:<id>`` suffix is a dataset row identifier; that it tracks transaction
#    order is verified on ONE filing (IPAR ``0001753926-25-001883``: ``:NDT:8823454`` is the
#    option exercise and ``:NDT:8823455`` the same-day sale) and is NOT established on the
#    population. What IS established without that assumption: the projection has no
#    transaction-sequence rule, so which of two same-filing balances becomes current is
#    decided by string order.
# 2. Two different balances on one filing are not proven to be successive states of ONE
#    position — they can be separate lots. The column reported is the SIZE of the
#    disagreement the tie-break settles, not an error count.
#
# The winner is verified against the REAL ``ownership_insiders_current`` row rather than
# re-derived alone, so every counted key is a live current winner (the first draft re-ranked
# the observations and omitted the projection's own de-collision, counting keys that never
# reached the table). The comparison row is the LAST by ``source_document_id`` among the tied
# set — the one the tie-break actually ran against — not the largest, which biased the
# direction split.
BALANCES_SQL = """
WITH ranked AS (
  SELECT instrument_id, holder_identity_key, ownership_nature, shares,
         source, period_end, filed_at, source_document_id, source_accession,
         row_number() OVER (
           PARTITION BY instrument_id, holder_identity_key, ownership_nature
           ORDER BY CASE source WHEN 'form4' THEN 1 WHEN 'form3' THEN 2 WHEN '13d' THEN 3
                    WHEN '13g' THEN 3 WHEN 'def14a' THEN 4 WHEN '13f' THEN 5
                    WHEN 'nport' THEN 6 WHEN 'ncsr' THEN 6 WHEN 'xbrl_dei' THEN 7
                    WHEN '10k_note' THEN 8 WHEN 'finra_si' THEN 9 ELSE 10 END ASC,
                    period_end DESC, filed_at DESC, source ASC, source_document_id ASC
         ) AS rn
    FROM ownership_insiders_observations
   WHERE known_to IS NULL
),
-- Only winners that ARE the live row: same key, same value, same provenance.
winner AS (
  SELECT r.* FROM ranked r
    JOIN ownership_insiders_current c
      ON c.instrument_id = r.instrument_id
     AND c.holder_identity_key = r.holder_identity_key
     AND c.ownership_nature = r.ownership_nature
     AND c.shares = r.shares
     AND c.source_document_id = r.source_document_id
   WHERE r.rn = 1
),
tied AS (
  SELECT w.instrument_id, w.shares AS won_shares,
         (array_agg(r.shares ORDER BY r.source_document_id DESC))[1] AS lex_last_shares
    FROM ranked r
    JOIN winner w USING (instrument_id, holder_identity_key, ownership_nature)
   WHERE r.source = w.source AND r.period_end = w.period_end AND r.filed_at = w.filed_at
     AND r.source_accession = w.source_accession
     AND r.source_document_id > w.source_document_id
     AND r.shares IS DISTINCT FROM w.shares
   GROUP BY w.instrument_id, w.holder_identity_key, w.ownership_nature, w.shares
)
SELECT count(*) AS live_keys_decided_by_lexical_tiebreak,
       count(DISTINCT instrument_id) AS instruments,
       sum(abs(won_shares - lex_last_shares)) AS abs_disagreement_vs_lexically_last,
       count(*) FILTER (WHERE won_shares > lex_last_shares) AS winner_is_larger,
       count(*) FILTER (WHERE won_shares < lex_last_shares) AS winner_is_smaller
  FROM tied
"""


class _Row:
    """One insider holder row, in the shape the edge builder needs."""

    __slots__ = ("accession", "cik", "identity", "name", "nature", "shares", "source")

    def __init__(
        self,
        identity: str,
        cik: str | None,
        name: str,
        nature: str,
        source: str,
        accession: str,
        shares: Decimal,
    ):
        self.identity = identity
        self.cik = cik
        self.name = name
        self.nature = nature
        self.source = source
        self.accession = accession
        self.shares = shares


def _edges(rows: list[_Row], evidence: dict[tuple[str, Decimal], tuple[str, ...]]) -> list[dict[str, Any]]:
    """Directed naming edges among ``rows``, using the module's own containment rule.

    Keyed on the SPEAKER's own ``(accession, shares)`` line — per the sec-edgar skill §2.3,
    ``natureOfOwnership`` is per-ROW and not per-accession, so a filing that reports several
    holdings names a different record holder for each.

    ⚠ This is deliberately NOT identical to :func:`orl._named_record_holder`, and the
    difference is one-directional. That function pools every cluster member's evidence and
    asks which single member the pooled text names; this builds one edge per SPEAKING LINE
    and drops self-edges, because a cluster does not exist yet — membership is what is being
    measured. The uniqueness guard is therefore applied in :func:`_summarise` per speaking
    line, which is STRICTER than production on a pooled text naming both the speaker and one
    other member (production keeps that as a named rep; this census drops it as ambiguous).
    Stricter is the right error here: it can only understate how much of the signal survives.
    """
    normalised = [(r, orl._normalise_holder_text(r.name)) for r in rows]
    out: list[dict[str, Any]] = []
    for speaker in rows:
        texts = evidence.get((speaker.accession, speaker.shares), ())
        if not texts:
            continue
        blobs = [orl._normalise_holder_text(t) for t in texts]
        for target, target_name in normalised:
            if not target_name or target.identity == speaker.identity:
                continue
            if not any(target_name in b for b in blobs):
                continue
            out.append(
                {
                    "speaker": speaker.identity,
                    "speaker_shares": str(speaker.shares),
                    "speaker_source": speaker.source,
                    "speaker_nature": speaker.nature,
                    "speaker_accession": speaker.accession,
                    "target": target.identity,
                    "target_shares": str(target.shares),
                    "target_source": target.source,
                    "target_nature": target.nature,
                    "target_accession": target.accession,
                    "equal_value": speaker.shares == target.shares,
                    "same_accession": speaker.accession == target.accession,
                    # NULL CIKs are name-fallback identities and are NOT the same filer;
                    # ``None == None`` would label two distinct ones as one (Codex ckpt-1).
                    "distinct_cik": (speaker.cik is None or target.cik is None or speaker.cik != target.cik),
                }
            )
    return out


def _edge_census(out_path: str) -> int:
    errors = 0
    written = 0
    with psycopg.connect(settings.database_url) as conn:
        with snapshot_read(conn), conn.cursor() as cur, open(out_path, "w", encoding="utf-8") as fh:
            cur.execute(HOLDERS_SQL, {"form4_cutoff": form4_retention_cutoff()})
            by_instrument: dict[int, list[_Row]] = {}
            for iid, identity, cik, name, nature, source, accession, shares, _table_i in cur.fetchall():
                by_instrument.setdefault(int(iid), []).append(
                    _Row(
                        str(identity),
                        cik,
                        str(name or ""),
                        str(nature or ""),
                        str(source),
                        str(accession),
                        Decimal(shares),
                    )
                )
            total = len(by_instrument)
            print(f"instruments with insider rows: {total}", flush=True)
            # The expected population is written FIRST, so a truncated file is detectable by
            # the summariser rather than read as a complete census (Codex ckpt-1).
            fh.write(json.dumps({"manifest": {"instruments": total}}) + "\n")
            for n, (iid, rows) in enumerate(sorted(by_instrument.items()), start=1):
                # Each instrument gets its own SAVEPOINT: a psycopg error aborts the enclosing
                # transaction, so without this the FIRST failure would fail every later
                # instrument and the error count would measure transaction fallout instead of
                # independent failures (Codex ckpt-1).
                try:
                    with conn.transaction():
                        evidence = orl._read_record_holder_evidence(conn, [r.accession for r in rows])
                    edges = _edges(rows, evidence)
                except Exception as exc:  # noqa: BLE001 — a per-instrument failure must be visible, not fatal
                    errors += 1
                    fh.write(json.dumps({"instrument_id": iid, "error": repr(exc)}) + "\n")
                    continue
                fh.write(json.dumps({"instrument_id": iid, "rows": len(rows), "edges": edges}) + "\n")
                written += 1
                if n % 500 == 0:
                    print(f"  {n}/{total}", flush=True)
    print(f"instruments written: {written}  errors: {errors}", flush=True)
    if not written:
        print("FAIL: census wrote no instruments", flush=True)
        return 1
    return 1 if errors else 0


def _resolve_speaking_lines(edges: list[dict[str, Any]], counts: Counter[str]) -> list[tuple[str, dict[str, Any]]]:
    """Group ``edges`` into speaking lines and apply both ambiguity guards.

    Returns one ``(speaker, representative edge)`` per line that names exactly one target
    identity holding exactly one balance. Lines that fail either guard are counted into
    ``counts`` and dropped.

    Shared by :func:`_summarise` and :func:`_reach` deliberately. A second copy of these two
    guards would let the reach pass count a cluster as "reached" under a looser rule than the
    one that decides whether the edge is USABLE, which is the whole question.
    """
    by_speaker: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for e in edges:
        # Keyed on the SPEAKING LINE, which is (identity, accession, amount). Dropping the
        # accession pooled a holder's lines from different filings into one "speaker"
        # (Codex checkpoint 1, PR #3147).
        by_speaker[(e["speaker"], e["speaker_accession"], e["speaker_shares"])].append(e)
    resolved: list[tuple[str, dict[str, Any]]] = []
    for (speaker, _acc, _s), es in by_speaker.items():
        counts["speaker_lines"] += 1
        # A control-chain footnote names every tier, so a text naming >=2 members is not
        # evidence about one of them (``_named_record_holder`` fails closed on the same shape).
        if len({e["target"] for e in es}) != 1:
            counts["dropped_multi_named"] += 1
            continue
        # One target IDENTITY can still hold several balances (its direct and indirect rows).
        # Picking es[0] made the verdict depend on row order in the file — 199 of 253 such
        # groups could flip (Codex checkpoint 1, PR #3147).
        if len({e["target_shares"] for e in es}) != 1:
            counts["dropped_target_multi_balance"] += 1
            continue
        resolved.append((speaker, es[0]))
    return resolved


def _summarise(paths: list[str]) -> int:
    counts: Counter[str] = Counter()
    instruments_with_edge: set[int] = set()
    instruments_unique_unequal: set[int] = set()
    seen_instruments: set[int] = set()
    expected = 0
    errors = 0
    removable: dict[int, dict[str, Decimal]] = defaultdict(dict)
    for path in paths:
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                rec = json.loads(line)
                if "manifest" in rec:
                    expected += int(rec["manifest"]["instruments"])
                    continue
                if rec.get("error"):
                    errors += 1
                    continue
                iid = rec["instrument_id"]
                if iid in seen_instruments:
                    print(f"FAIL: instrument {iid} appears twice — inputs overlap", flush=True)
                    return 1
                seen_instruments.add(iid)
                counts["rows"] += rec["rows"]
                for e in rec["edges"]:
                    counts["edges"] += 1
                    instruments_with_edge.add(iid)
                    counts["edges_equal_value" if e["equal_value"] else "edges_unequal_value"] += 1
                    if e["same_accession"]:
                        counts["edges_same_accession"] += 1
                    if not e["distinct_cik"]:
                        counts["edges_same_cik"] += 1
                for speaker, e in _resolve_speaking_lines(rec["edges"], counts):
                    if e["equal_value"]:
                        counts["unique_equal"] += 1
                    elif Decimal(e["speaker_shares"]) < Decimal(e["target_shares"]):
                        counts["unique_speaker_lt_target"] += 1
                        instruments_unique_unequal.add(iid)
                        removable[iid][speaker] = Decimal(e["speaker_shares"])
                    else:
                        counts["unique_speaker_gt_target"] += 1
    if not seen_instruments:
        print("FAIL: summary read no instruments", flush=True)
        return 1
    if expected and len(seen_instruments) + errors != expected:
        print(
            f"FAIL: manifest expected {expected} instruments, read "
            f"{len(seen_instruments)} + {errors} errors — input truncated",
            flush=True,
        )
        return 1
    rows = [
        ("instruments scanned", len(seen_instruments)),
        ("insider rows scanned", counts["rows"]),
        ("naming edges", counts["edges"]),
        ("  equal-value (anchor sees it)", counts["edges_equal_value"]),
        ("  unequal-value (anchor blind)", counts["edges_unequal_value"]),
        ("  same accession", counts["edges_same_accession"]),
        ("  same CIK (not a 2nd identity)", counts["edges_same_cik"]),
        ("instruments with >=1 edge", len(instruments_with_edge)),
        ("speaking lines", counts["speaker_lines"]),
        ("  dropped, names >=2 members", counts["dropped_multi_named"]),
        ("  dropped, target has >1 balance", counts["dropped_target_multi_balance"]),
        ("  unique-named, equal value", counts["unique_equal"]),
        ("  unique-named, speaker < target", counts["unique_speaker_lt_target"]),
        ("  unique-named, speaker > target", counts["unique_speaker_gt_target"]),
        ("instruments, unique unequal edge", len(instruments_unique_unequal)),
        ("upper-bound shares (candidate set)", sum((sum(d.values()) for d in removable.values()), Decimal(0))),
        ("harness errors", errors),
    ]
    for label, value in rows:
        print(f"{label:36} {value}")
    return 1 if errors else 0


def _reach(path: str) -> int:
    """Does a usable naming edge exist between the members of a cluster the anchor refuses?

    The refused set is read from :data:`REFUSED_CLUSTERS_SQL`, which shares its cluster
    definition with the ``--joint`` census by construction, and the identity is asserted at
    run time against ``JOINT_SQL``'s own ``unequal_refused_today``.

    An edge counts as being INSIDE a cluster when both endpoints file under that cluster's
    accession at that instrument. That is membership by construction rather than by
    inference: the cluster is every identity with a live row under ``(instrument, accession)``
    and the edge census is built from the same ``_LIVE_INSIDER_ROWS`` population, so an edge
    whose speaker and target both carry that accession connects two members. ``_edges`` drops
    self-edges, so the two are always distinct identities.

    Three nested populations are reported, and the gaps between them are the finding:

    * **raw** — any same-accession edge at all, before the ambiguity guards. Separates "no
      signal" from "ambiguous signal", which are different verdicts with different fixes.
    * **resolved** — the edge survives both guards, so it names one member unambiguously.
    * **resolved and UNEQUAL** — the decisive one. The anchor already buckets equal-value
      members together, so an equal-value edge inside a refused cluster merges nothing the
      exact-value key does not already merge. Only an edge spanning two different values can
      collapse the buckets the refusal creates.

    ⚠ The membership question is DIRECTION-FREE and ``--summarise``'s 154 is not, so the two
    are not the same quantity and are not printed as though they were. ``_summarise`` counts
    only the ``speaker < target`` branch, because its output is an upper bound on REMOVABLE
    shares — who folds into whom. Whether two identities belong to one control group does not
    depend on which of them reports the larger balance, so the reach rows report either
    direction and then split out the fold direction, so the comparison against 154 is made on
    the same rule rather than across two.
    """
    counts: Counter[str] = Counter()
    # Every DB-derived figure in this report is taken on ONE connection inside ONE
    # ``snapshot_read``, so the refused set, the census cross-check and the evidence-presence
    # arm cannot be read against three different states of the corpus. An earlier draft
    # computed the last of these on a second, separately-opened connection — a genuinely
    # different snapshot, and the one real cross-snapshot hazard here (review NITPICK on
    # PR #3177).
    with psycopg.connect(settings.database_url) as conn, snapshot_read(conn), conn.cursor() as cur:
        cur.execute(REFUSED_CLUSTERS_SQL, {"form4_cutoff": form4_retention_cutoff()})
        refused = {(int(row[0]), str(row[1])) for row in cur.fetchall()}
        cur.execute(JOINT_SQL, {"form4_cutoff": form4_retention_cutoff()})
        joint = cur.fetchone()
        names = [d.name for d in cur.description or ()]
        # ⚠ Separates "the filers said nothing" from "they spoke and named nobody in the
        # cluster". Without it, a reach rate near zero is equally consistent with a BLIND
        # HARNESS — a census that never read the evidence would report the same 0, and a
        # check that cannot fail for the right reason is not evidence (prevention-log: "a
        # measurement that cannot detect its own failure"). This reads the same
        # ``_read_record_holder_evidence`` the rollup itself consumes, so a silent
        # evidence-side regression shows up here as a collapse to zero.
        #
        # One call over every refused accession rather than 157 calls of one: the reader is
        # normally handed one instrument's accessions, but its three queries are
        # ``accession_number``-leading either way, and holding a REPEATABLE READ connection
        # open for 471 round trips is the wrong thing to do on a cluster already at its
        # usable ``max_connections`` ceiling.
        evidence = orl._read_record_holder_evidence(conn, sorted({acc for _iid, acc in refused}))
        spoken = {acc for acc, _shares in evidence}
        speaks = sum(1 for _iid, acc in refused if acc in spoken)
    if not refused:
        print("FAIL: refused-cluster population is empty — nothing was measured", flush=True)
        return 1
    # The two queries are identical only because they share ``_JOINT_CLUSTER_CTE``. Asserting
    # it makes a future edit that breaks the sharing fail loudly instead of quietly reporting
    # a reach rate against a denominator the census never published.
    census_refused = dict(zip(names, joint or (), strict=False)).get("unequal_refused_today")
    if census_refused != len(refused):
        print(f"FAIL: refused clusters {len(refused)} != census unequal_refused_today {census_refused}", flush=True)
        return 1

    refused_instruments = {iid for iid, _acc in refused}
    raw: set[tuple[int, str]] = set()
    resolved_clusters: set[tuple[int, str]] = set()
    unequal_clusters: set[tuple[int, str]] = set()
    fold_direction_clusters: set[tuple[int, str]] = set()
    instruments_unique_unequal: set[int] = set()
    seen: set[int] = set()
    errors = 0
    expected = 0
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            rec = json.loads(line)
            if "manifest" in rec:
                expected += int(rec["manifest"]["instruments"])
                continue
            if rec.get("error"):
                errors += 1
                continue
            iid = int(rec["instrument_id"])
            seen.add(iid)
            edges = rec["edges"]
            for e in edges:
                if e["same_accession"] and (iid, e["speaker_accession"]) in refused:
                    raw.add((iid, e["speaker_accession"]))
            for _speaker, e in _resolve_speaking_lines(edges, counts):
                # The fold direction: a smaller speaker naming a larger target is the edge
                # that would fold the speaker into the block. This is the SAME branch
                # ``_summarise`` counts into ``instruments_unique_unequal``, so the coarse
                # figure below reconciles with the published 154.
                folds = not e["equal_value"] and Decimal(e["speaker_shares"]) < Decimal(e["target_shares"])
                if folds:
                    instruments_unique_unequal.add(iid)
                if not e["same_accession"]:
                    continue
                key = (iid, e["speaker_accession"])
                if key not in refused:
                    continue
                resolved_clusters.add(key)
                if not e["equal_value"]:
                    unequal_clusters.add(key)
                if folds:
                    fold_direction_clusters.add(key)
    if not seen:
        print("FAIL: reach pass read no instruments", flush=True)
        return 1
    if expected and len(seen) + errors != expected:
        print(
            f"FAIL: manifest expected {expected} instruments, read {len(seen)} + {errors} errors — input truncated",
            flush=True,
        )
        return 1

    coarse = instruments_unique_unequal & refused_instruments
    cluster_instruments = {iid for iid, _acc in unequal_clusters}
    fold_instruments = {iid for iid, _acc in fold_direction_clusters}
    rows: list[tuple[str, Any]] = [
        ("refused clusters (denominator)", len(refused)),
        ("  whose accession states ANY nature", speaks),
        ("  with any same-acc edge (raw)", len(raw)),
        ("  with a RESOLVED same-acc edge", len(resolved_clusters)),
        ("  ... and it is UNEQUAL-value", len(unequal_clusters)),
        ("      of which speaker < target", len(fold_direction_clusters)),
        ("refused instruments", len(refused_instruments)),
        ("  reached, either direction", len(cluster_instruments)),
        ("  reached, fold direction only", len(fold_instruments)),
        ("instruments, unique unequal edge", len(instruments_unique_unequal)),
        ("coarse instrument-level intersection", len(coarse)),
        ("  NOT reached per-cluster (overstated)", len(coarse - fold_instruments)),
        ("harness errors", errors),
    ]
    print("Does the naming signal reach the clusters the exact-value key refuses?")
    for label, value in rows:
        print(f"{label:38} {value}")
    return 1 if errors else 0


def _cases(path: str, symbols: list[str]) -> int:
    """Per-symbol edge readout for the instruments #2794 names, from the census file.

    The verdict's "the proposed key reaches N of 7" table is a CLAIM, so it is computed
    here rather than written into prose."""
    with psycopg.connect(settings.database_url) as conn, conn.cursor() as cur:
        cur.execute("SELECT instrument_id, symbol FROM instruments WHERE symbol = ANY(%s)", (symbols,))
        by_id = {int(i): str(s) for i, s in cur.fetchall()}
    missing = sorted(set(symbols) - set(by_id.values()))
    if missing:
        print(f"FAIL: symbols not in instruments: {missing}")
        return 1
    seen: set[str] = set()
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            rec = json.loads(line)
            if "manifest" in rec or rec.get("error") or rec["instrument_id"] not in by_id:
                continue
            symbol = by_id[rec["instrument_id"]]
            seen.add(symbol)
            by_speaker: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
            for e in rec["edges"]:
                by_speaker[(e["speaker"], e["speaker_shares"])].append(e)
            unique_unequal = sum(
                1 for es in by_speaker.values() if len({e["target"] for e in es}) == 1 and not es[0]["equal_value"]
            )
            print(
                f"{symbol:6} rows={rec['rows']:3} edges={len(rec['edges']):3} "
                f"speaking_lines={len(by_speaker):2} unique_unequal={unique_unequal}"
            )
    absent = sorted(set(by_id.values()) - seen)
    for symbol in absent:
        print(f"{symbol:6} ABSENT from the census file")
    # A requested symbol the census never covered means the table this feeds would silently
    # have a missing row, which reads as "no edges" (Codex checkpoint 1).
    return 1 if absent else 0


# The control has to be beaten by a MARGIN, not merely exceeded: two numbers one apart would
# satisfy ``>`` and prove nothing. 10x is a round threshold chosen by construction rather than
# fitted — there is no published formulation for "a lookup control is far enough below its
# claim" — and it is frozen here so a later run cannot quietly relax it. Measured on
# 2026-09-18 the two arms are 661 and a small residual, so the margin is not marginal.
_BORROWED_CONTROL_MAX_RATIO: Final[float] = 0.1


def _pipeline() -> int:
    """Are the two values in a refused cluster two disclosures, or one line set read twice?

    Every figure is computed by :data:`PIPELINE_SQL` at run time. Three guards, each able to
    fail for a different reason:

    * the refused population is asserted against ``JOINT_SQL``'s own ``unequal_refused_today``
      — the two queries agree only by sharing ``_JOINT_CLUSTER_CTE``, and a future edit could
      break that silently (same guard :func:`_reach` already carries);
    * the borrowed-accession control must sit far below the own-accession arm, or the lookup
      is matching on line-set size rather than on identity;
    * an empty population fails, because a census that measured nothing must not look clean.
    """
    with psycopg.connect(settings.database_url) as conn, snapshot_read(conn), conn.cursor() as cur:
        cur.execute(PIPELINE_SQL, {"form4_cutoff": form4_retention_cutoff()})
        row = cur.fetchone()
        names = [d.name for d in cur.description or ()]
        cur.execute(JOINT_SQL, {"form4_cutoff": form4_retention_cutoff()})
        joint = cur.fetchone()
        joint_names = [d.name for d in cur.description or ()]
        cur.execute(WIDENED_DECOLLISION_SQL)
        widened = cur.fetchone()
        widened_names = [d.name for d in cur.description or ()]

    if row is None or joint is None or widened is None:
        print("FAIL: a census returned no row")
        return 1
    stats = dict(zip(names, row, strict=True))

    print("Same-Table-I-line test over the clusters the exact-value anchor refuses:")
    for name, value in stats.items():
        print(f"  {name:38} {value}")
    print("\nBlast radius if the de-collision widened from holder_cik-matched to accession-matched:")
    for name, value in zip(widened_names, widened, strict=True):
        print(f"  {name:38} {value}")

    failures: list[str] = []
    if not stats["refused_clusters"]:
        failures.append("refused population is empty — nothing was measured")
    expected = dict(zip(joint_names, joint, strict=True))["unequal_refused_today"]
    if stats["refused_clusters"] != expected:
        failures.append(
            f"refused population {stats['refused_clusters']} disagrees with --joint's "
            f"unequal_refused_today {expected} — the shared cluster CTE has drifted"
        )
    claim, control = stats["value_on_own_accession_line"], stats["control_value_on_borrowed_line"]
    if not claim:
        failures.append("no member value was found on a line of its own accession — evidence side is dark")
    elif control > claim * _BORROWED_CONTROL_MAX_RATIO:
        failures.append(
            f"borrowed-accession control {control} is not far below the claim {claim} "
            f"(ratio bound {_BORROWED_CONTROL_MAX_RATIO}) — the lookup discriminates nothing"
        )
    for message in failures:
        print(f"FAIL: {message}")
    return 1 if failures else 0


def _scalar_census(sql: LiteralString, title: str, params: dict[str, Any] | None = None) -> int:
    """Run a one-row aggregate census and print it.

    ``params`` is per-query rather than a shared dict: ``JOINT_SQL`` carries
    ``%(form4_cutoff)s`` through ``_INSIDER_BEYOND_RETENTION_SQL`` and ``BALANCES_SQL`` takes
    no parameters at all. psycopg ignores unused named parameters silently, so passing one
    dict to both would hide a placeholder typo in whichever query stopped using it (review
    NITPICK on PR #3147)."""
    with psycopg.connect(settings.database_url) as conn, snapshot_read(conn), conn.cursor() as cur:
        cur.execute(sql, params or {})
        row = cur.fetchone()
        names = [d.name for d in cur.description or ()]
    print(title)
    if row is None:
        print("FAIL: census returned no row")
        return 1
    for name, value in zip(names, row, strict=True):
        print(f"  {name:34} {value}")
    # ⚠ An aggregate query returns a row even over an empty population, so "it ran" is not
    # evidence it measured anything — the guard has to read the leading count (Codex
    # checkpoint 1: the advertised empty-census failure could not fire on these two modes).
    if not row[0]:
        print("FAIL: census population is empty — nothing was measured")
        return 1
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--edges", action="store_true", help="run the naming-edge census (needs --out)")
    ap.add_argument("--out", help="write the per-instrument edge census to this JSONL path")
    ap.add_argument("--summarise", nargs="+", help="summarise one or more edge-census JSONL files")
    ap.add_argument("--joint", action="store_true", help="joint-accession cluster census")
    ap.add_argument("--balances", action="store_true", help="lexical-tie-break balance census")
    ap.add_argument("--reach", metavar="CENSUS", help="does the naming signal reach the refused clusters?")
    ap.add_argument("--cases", nargs="+", metavar="SYMBOL", help="per-symbol readout (needs --from)")
    ap.add_argument("--from", dest="src", help="edge-census JSONL to read --cases from")
    ap.add_argument("--pipeline", action="store_true", help="same-Table-I-line test on the refused clusters")
    args = ap.parse_args(argv)
    if args.pipeline:
        return _pipeline()
    if args.cases:
        if not args.src:
            ap.error("--cases requires --from <census.jsonl>")
        return _cases(args.src, args.cases)
    if args.reach:
        return _reach(args.reach)
    if args.summarise:
        return _summarise(args.summarise)
    if args.joint:
        return _scalar_census(
            JOINT_SQL,
            "Joint-accession insider clusters (identity-level):",
            {"form4_cutoff": form4_retention_cutoff()},
        )
    if args.balances:
        # The banner is printed, not just commented: the failure mode is someone reading the
        # number off a terminal, which a source comment does not reach.
        rc = _scalar_census(
            BALANCES_SQL,
            "Keys decided by the projection's lexical tie-break:\n"
            "  ⚠⚠ STALE since #3146 — this ORDER BY is a hand copy of the PRE-#3146 rule, so the\n"
            "     figure below is not the quantity the label describes. Do not quote it; see the\n"
            "     BALANCES_SQL header for what re-speccing it requires.",
        )
        # ⚠ Non-zero by CONTRACT, not because anything failed. This module's rule is that a
        # census which measured nothing must not look clean; one measuring the WRONG thing is
        # the worse case of it, because it looks clean AND hands back a number. A banner stops
        # a human reading a terminal — only the exit code stops a script (review WARNING,
        # PR #3178). No caller exists in `.py`/`.sh`/CI today, but the #2794 proposal doc
        # prints this command for an operator to run, so "nobody automates it" is a property
        # of this week rather than of the script.
        # 2, not 1, so "withdrawn pending re-spec" stays distinguishable from _scalar_census's
        # own 1 ("ran and measured nothing"), which is preserved when it fires.
        return rc or 2
    if args.edges or args.out:
        if not args.out:
            ap.error("--edges requires --out")
        return _edge_census(args.out)
    ap.error("one of --edges / --summarise / --joint / --balances / --reach / --pipeline is required")
    return 2


if __name__ == "__main__":
    sys.exit(main())
