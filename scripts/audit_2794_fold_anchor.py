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

Usage (read-only, one REPEATABLE READ snapshot per mode):

    PYTHONPATH=. uv run python -m scripts.audit_2794_fold_anchor --edges --out /tmp/a2794.jsonl
    PYTHONPATH=. uv run python -m scripts.audit_2794_fold_anchor --summarise /tmp/a2794.jsonl
    PYTHONPATH=. uv run python -m scripts.audit_2794_fold_anchor --joint
    PYTHONPATH=. uv run python -m scripts.audit_2794_fold_anchor --balances

Exits 1 on any per-instrument error, on an empty census, and on a summary whose input held
no instruments — a census that measured nothing must not look clean.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from decimal import Decimal
from typing import Any, LiteralString

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
JOINT_SQL = f"""
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

# The projection's own winner rule, spelled from ``ownership_observations.py:321-341`` so the
# census asks the same question the MERGE answers. The last key is a LEXICAL tie-break, and
# this census counts the keys it decides between same-filing observations at different values.
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
                # The uniqueness guard, per SPEAKING LINE: a control-chain footnote names
                # every tier, so a text naming ≥2 members is not evidence about one of them
                # (``_named_record_holder`` fails closed on the same shape).
                by_speaker: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
                for e in rec["edges"]:
                    counts["edges"] += 1
                    instruments_with_edge.add(iid)
                    counts["edges_equal_value" if e["equal_value"] else "edges_unequal_value"] += 1
                    if e["same_accession"]:
                        counts["edges_same_accession"] += 1
                    if not e["distinct_cik"]:
                        counts["edges_same_cik"] += 1
                    # Keyed on the SPEAKING LINE, which is (identity, accession, amount).
                    # Dropping the accession pooled a holder's lines from different filings
                    # into one "speaker" (Codex checkpoint 1).
                    by_speaker[(e["speaker"], e["speaker_accession"], e["speaker_shares"])].append(e)
                for (speaker, _acc, _s), es in by_speaker.items():
                    counts["speaker_lines"] += 1
                    if len({e["target"] for e in es}) != 1:
                        counts["dropped_multi_named"] += 1
                        continue
                    # One target IDENTITY can still hold several balances (its direct and
                    # indirect rows). Picking es[0] made the equal/less/greater verdict
                    # depend on row order in the file — 199 of 253 such groups could flip
                    # (Codex checkpoint 1). Ambiguity is now counted, not silently resolved.
                    target_values = {e["target_shares"] for e in es}
                    if len(target_values) != 1:
                        counts["dropped_target_multi_balance"] += 1
                        continue
                    e = es[0]
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


def _scalar_census(sql: LiteralString, title: str) -> int:
    with psycopg.connect(settings.database_url) as conn, snapshot_read(conn), conn.cursor() as cur:
        cur.execute(sql, {"form4_cutoff": form4_retention_cutoff()})
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
    ap.add_argument("--cases", nargs="+", metavar="SYMBOL", help="per-symbol readout (needs --from)")
    ap.add_argument("--from", dest="src", help="edge-census JSONL to read --cases from")
    args = ap.parse_args(argv)
    if args.cases:
        if not args.src:
            ap.error("--cases requires --from <census.jsonl>")
        return _cases(args.src, args.cases)
    if args.summarise:
        return _summarise(args.summarise)
    if args.joint:
        return _scalar_census(JOINT_SQL, "Joint-accession insider clusters (identity-level):")
    if args.balances:
        return _scalar_census(BALANCES_SQL, "Keys decided by the projection's lexical tie-break:")
    if args.edges or args.out:
        if not args.out:
            ap.error("--edges requires --out")
        return _edge_census(args.out)
    ap.error("one of --edges / --summarise / --joint / --balances is required")
    return 2


if __name__ == "__main__":
    sys.exit(main())
