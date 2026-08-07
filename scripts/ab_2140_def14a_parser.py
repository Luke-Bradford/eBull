"""Full-population A/B for the DEF 14A beneficial-ownership parser (#2140, #2158).

Re-parses EVERY stored ``def14a_body`` payload under the current working tree
and writes a JSON summary. Run once on ``main`` and once on the branch, then
diff the two summaries.

Offline — reads ``filing_raw_documents``, never fetches from EDGAR.

    PYTHONPATH=. uv run python scripts/ab_2140_def14a_parser.py --out /tmp/ab-main.json
    PYTHONPATH=. uv run python scripts/ab_2140_def14a_parser.py --out /tmp/ab-branch.json
    PYTHONPATH=. uv run python scripts/ab_2140_def14a_parser.py --diff /tmp/ab-main.json /tmp/ab-branch.json

The diff reports **distinct holders lost**, not row-count deltas: #2140 twice
found a row-count drop to be the OLD code losing garbage (an Item 402 award
table) or the identity dedup working as intended, and chasing the count would
have made the parser worse. Gains are enumerated for the same reason — #2140's
last real defect (address fragments parsed as holders) only appeared on the
gain side.

#2158 added ``--stored``, which compares a scan against the holder names
actually PERSISTED in ``def14a_beneficial_holdings``. The A/B proper compares
parse-to-parse and is therefore blind to filings where BOTH sides return
nothing — exactly the class the rewash guard blocks. Only a stored-row
comparison sees those.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from typing import Any

import psycopg

# Imported for their side effect on the parse path as well as direct use.
import app.providers.implementations.sec_def14a as parser_mod
from app.config import settings

_NAME_EVIDENCE_RE = re.compile(r"[A-Za-z]{2,}")
_GROUP_RE = re.compile(r"as a group", re.IGNORECASE)


def _scan(limit: int | None) -> dict[str, Any]:
    """Re-parse every stored def14a body; return aggregate + per-accession rows."""
    promoted: list[dict[str, Any]] = []

    # Instrument the two-row-header promotion so the promoted-row audit
    # (Codex ckpt-1) covers the whole corpus, not the five-filing panel.
    original_parse_table = parser_mod._parse_table_html
    pending: dict[str, Any] = {}

    # ``**kwargs`` is load-bearing, not defensive (#2175). The SCT call site passes
    # ``expand_spans=False``; a tracer that accepted only the positional argument
    # raised TypeError inside the SCT arm, the harness swallowed it, and the run
    # reported ``sct_rows: 67,828 -> 0`` — a total-collapse figure produced entirely
    # by the harness while the parser itself returned 17 rows for the same filing.
    # Any monkeypatch of a parser entry point must forward the full signature.
    def traced_parse_table(table_html: str, **kwargs: Any) -> Any:
        table = original_parse_table(table_html, **kwargs)
        if table is not None and table.column_headers != table.score_headers:
            pending.setdefault("promotions", []).append(
                {"headers": list(table.column_headers), "width": len(table.column_headers)}
            )
        return table

    parser_mod._parse_table_html = traced_parse_table

    totals = {
        "accessions": 0,
        "accessions_with_rows": 0,
        "rows": 0,
        "numeric_names": 0,
        "newline_names": 0,
        "group_rows": 0,
        "group_rows_tagged_group": 0,
        "shares_without_percent": 0,
        "sct_rows": 0,
    }
    per_accession: dict[str, int] = {}
    # Holder IDENTITIES per accession, so the diff can report distinct holders
    # lost/gained rather than row counts. Keyed exactly as the database keys
    # them — ``holder_name_key`` is ``lower(trim(holder_name))`` (sql/116:110) —
    # so "lost" here means the same thing it means in the rollup.
    per_accession_holders: dict[str, list[str]] = {}
    sct_fingerprints: dict[str, str] = {}

    # ``LIMIT %s`` as a bound parameter, never string-composed — Postgres
    # accepts NULL for "no limit", so one literal query covers both modes and
    # the prevention-log rule against interpolating into SQL is respected.
    sql = """
        SELECT accession_number, payload
        FROM filing_raw_documents
        WHERE document_kind = 'def14a_body'
        ORDER BY accession_number
        LIMIT %(limit)s
    """

    with psycopg.connect(settings.database_url) as conn:
        conn.execute("SET statement_timeout = 0")
        with conn.cursor(name="def14a_ab") as cur:
            cur.itersize = 20
            cur.execute(sql, {"limit": limit})
            for accession, body in cur:
                totals["accessions"] += 1
                pending.clear()
                try:
                    parsed = parser_mod.parse_beneficial_ownership_table(body)
                except Exception as exc:  # noqa: BLE001 — A/B must not abort mid-corpus
                    per_accession[accession] = -1
                    print(f"PARSE-ERROR {accession}: {type(exc).__name__}: {exc}", file=sys.stderr)
                    continue

                for promo in pending.get("promotions", []):
                    promoted.append({"accession": accession, **promo})

                rows = parsed.rows
                per_accession[accession] = len(rows)
                per_accession_holders[accession] = sorted({r.holder_name.strip().lower() for r in rows})
                if rows:
                    totals["accessions_with_rows"] += 1
                for holder in rows:
                    totals["rows"] += 1
                    name = holder.holder_name
                    if not _NAME_EVIDENCE_RE.search(name):
                        totals["numeric_names"] += 1
                    if "\n" in name:
                        totals["newline_names"] += 1
                    if _GROUP_RE.search(name):
                        totals["group_rows"] += 1
                        if holder.holder_role == "group":
                            totals["group_rows_tagged_group"] += 1
                    if holder.shares is not None and holder.percent_of_class is None:
                        totals["shares_without_percent"] += 1

                # Item 402(c) blast-radius proof: the SCT output must not move.
                try:
                    sct = parser_mod.parse_summary_compensation_table(body)
                except Exception:  # noqa: BLE001
                    sct_fingerprints[accession] = "ERROR"
                else:
                    totals["sct_rows"] += len(sct.rows)
                    sct_fingerprints[accession] = "|".join(
                        f"{r.executive_name}~{r.principal_position or ''}~{r.fiscal_year}~{r.total_comp or ''}"
                        for r in sct.rows
                    )

    parser_mod._parse_table_html = original_parse_table
    return {
        "totals": totals,
        "per_accession": per_accession,
        "per_accession_holders": per_accession_holders,
        "sct_fingerprints": sct_fingerprints,
        "promoted_rows": promoted,
    }


def _diff(before: dict[str, Any], after: dict[str, Any]) -> int:
    print("== totals ==")
    for key in before["totals"]:
        b, a = before["totals"][key], after["totals"].get(key)
        flag = "" if a == b else "   <-- CHANGED"
        print(f"  {key:28s} {b:>10} -> {a:>10}{flag}")

    print("\n== Item 402(c) SCT drift (must be empty) ==")
    sct_drift = [acc for acc, fp in before["sct_fingerprints"].items() if after["sct_fingerprints"].get(acc) != fp]
    print(f"  accessions with changed SCT output: {len(sct_drift)}")
    for acc in sct_drift[:20]:
        print(f"    {acc}")

    print("\n== per-accession beneficial-ownership row deltas ==")
    lost, gained = [], []
    for acc, b in before["per_accession"].items():
        a = after["per_accession"].get(acc, 0)
        if a < b:
            lost.append((acc, b, a))
        elif a > b:
            gained.append((acc, b, a))
    print(f"  accessions LOSING rows:  {len(lost)}")
    for acc, b, a in sorted(lost, key=lambda x: x[1] - x[2], reverse=True)[:25]:
        print(f"    {acc}  {b} -> {a}")
    print(f"  accessions GAINING rows: {len(gained)}")
    for acc, b, a in sorted(gained, key=lambda x: x[2] - x[1], reverse=True)[:25]:
        print(f"    {acc}  {b} -> {a}")

    print("\n== DISTINCT HOLDERS lost / gained (the metric that matters) ==")
    # Fail loudly rather than diff against an absent key. ``per_accession_holders``
    # is newer than some summaries on disk, and defaulting it to {} makes this
    # section report ZERO holders lost and gained — indistinguishable from a
    # clean run, which is the exact false-negative class the #2158 prevention-log
    # entry on simulated controls was written about (Codex ckpt-2 P2).
    for label, summary in (("BEFORE", before), ("AFTER", after)):
        if "per_accession_holders" not in summary:
            raise SystemExit(
                f"{label} summary predates the distinct-holder metric "
                "(no 'per_accession_holders'). Regenerate BOTH sides with the "
                "current script — a row-count-only diff is not an acceptable "
                "substitute (#2140, #2158)."
            )
    before_h = before["per_accession_holders"]
    after_h = after["per_accession_holders"]
    holders_lost: list[tuple[str, list[str]]] = []
    holders_gained: list[tuple[str, list[str]]] = []
    for acc, names in before_h.items():
        b_set, a_set = set(names), set(after_h.get(acc, []))
        if b_set - a_set:
            holders_lost.append((acc, sorted(b_set - a_set)))
        if a_set - b_set:
            holders_gained.append((acc, sorted(a_set - b_set)))
    print(f"  accessions losing >=1 distinct holder: {len(holders_lost)}")
    print(f"  distinct holders lost (total):         {sum(len(n) for _, n in holders_lost)}")
    for acc, names in sorted(holders_lost, key=lambda x: -len(x[1]))[:25]:
        print(f"    {acc}  -{len(names)}  {names[:6]}")
    print(f"  accessions gaining >=1 distinct holder: {len(holders_gained)}")
    print(f"  distinct holders gained (total):        {sum(len(n) for _, n in holders_gained)}")
    for acc, names in sorted(holders_gained, key=lambda x: -len(x[1]))[:25]:
        print(f"    {acc}  +{len(names)}  {names[:6]}")

    print("\n== promoted header rows (audit) ==")
    print(f"  before: {len(before['promoted_rows'])}   after: {len(after['promoted_rows'])}")
    seen = {json.dumps(p["headers"]) for p in before["promoted_rows"]}
    new = [p for p in after["promoted_rows"] if json.dumps(p["headers"]) not in seen]
    print(f"  newly-promoted distinct header shapes: {len({json.dumps(p['headers']) for p in new})}")
    for headers in sorted({json.dumps(p["headers"]) for p in new})[:40]:
        print(f"    {headers}")
    return 0


def _audit(limit: int | None) -> int:
    """#2158 Codex ckpt-1: full-population provenance for the ``score_headers``
    fold, in ONE process so both scoring modes are measured on identical input.

    Two things the parse-to-parse A/B cannot establish:

    * **Every** label-arm promotion changes ``score_headers``, not just newly
      promoted shapes — so a "new shapes" audit misses the risk entirely. This
      enumerates each promotion with its unfolded score, folded score, and
      whether the folded text trips ``_ITEM_402_AWARD_MARKERS`` (the fold is
      only safe if it strengthens, never weakens, the Item 402 rejection).
    * A compressed SCT fingerprint can match across a *table swap*. This
      compares the FULL emitted Item 402(c) rows plus the selected table's score
      under both modes.

    Run on the branch only — the unfolded mode reproduces ``main``'s behaviour
    exactly, because ``main`` differs from the branch by this fold alone.
    """
    promos: list[dict[str, Any]] = []
    sct_drift: list[dict[str, Any]] = []
    original_parse_table = parser_mod._parse_table_html
    seen_shapes: dict[str, dict[str, Any]] = {}
    mode = {"fold": True}
    current_accession = {"acc": ""}

    def unfolded_headers(table: Any) -> tuple[str, ...]:
        """The ``score_headers`` ``main`` would have produced for this table."""
        cols = tuple(table.column_headers)
        score = tuple(table.score_headers)
        if score == cols or len(score) <= len(cols) or score[-len(cols) :] != cols:
            return score  # no promotion happened
        parent = score[: len(score) - len(cols)]
        # The legacy arm folded on ``main`` too — only the label arm changed, so
        # only the label arm's promotions are drift.
        #
        # The arm test must reproduce ``_parse_table_html``'s BOTH conditions,
        # not just the keyword one. A keyword-only test silently mislabels every
        # label-arm promotion whose promoted row happens to contain a legacy
        # keyword — and an Item 402(c) SCT label row always does, because
        # § 229.402(c)(2)(x) prescribes a "Total" column. That made this audit
        # report ZERO SCT drift while the real main-vs-branch A/B showed 75
        # accessions going from 0 rows to 12-18. ``max_data_width`` is
        # recoverable exactly: the promoted row was ``cols`` wide and the rest
        # of the body survives as ``table.rows``.
        max_data_width = max([len(cols)] + [len(r) for r in table.rows])
        legacy_arm = len(parent) < max_data_width and parser_mod._looks_like_legacy_subheader(cols)
        return score if legacy_arm else parent

    def patched(table_html: str, **kwargs: Any) -> Any:
        # Forward the full signature — see the ``traced_parse_table`` comment above.
        table = original_parse_table(table_html, **kwargs)
        if table is None:
            return None
        unfolded = unfolded_headers(table)
        if unfolded != table.score_headers:
            folded = tuple(table.score_headers)
            key = json.dumps([list(unfolded), list(folded)])
            joined = " ".join(folded).lower()
            entry = seen_shapes.setdefault(
                key,
                {
                    "unfolded": list(unfolded),
                    "promoted": list(table.column_headers),
                    "score_unfolded": parser_mod._score_table_headers(unfolded),
                    "score_folded": parser_mod._score_table_headers(folded),
                    "item402_marker_after_fold": [m for m in parser_mod._ITEM_402_AWARD_MARKERS if m in joined],
                    "count": 0,
                    "example": current_accession["acc"],
                },
            )
            entry["count"] += 1
        if not mode["fold"]:
            return parser_mod._RawTable(
                score_headers=unfolded,
                column_headers=table.column_headers,
                rows=table.rows,
                line_rows=table.line_rows,
            )
        return table

    parser_mod._parse_table_html = patched
    sql = """
        SELECT accession_number, payload
        FROM filing_raw_documents
        WHERE document_kind = 'def14a_body'
        ORDER BY accession_number
        LIMIT %(limit)s
    """
    scanned = 0
    with psycopg.connect(settings.database_url) as conn:
        conn.execute("SET statement_timeout = 0")
        with conn.cursor(name="def14a_audit") as cur:
            cur.itersize = 20
            cur.execute(sql, {"limit": limit})
            for accession, body in cur:
                scanned += 1
                current_accession["acc"] = accession
                rendered = {}
                for fold in (False, True):
                    mode["fold"] = fold
                    try:
                        sct = parser_mod.parse_summary_compensation_table(body)
                    except Exception as exc:  # noqa: BLE001 — audit must not abort
                        rendered[fold] = f"ERROR {type(exc).__name__}: {exc}"
                        continue
                    rendered[fold] = {
                        "score": sct.raw_table_score,
                        "rows": [
                            (r.executive_name, r.principal_position, r.fiscal_year, str(r.total_comp)) for r in sct.rows
                        ],
                    }
                if rendered[False] != rendered[True]:
                    sct_drift.append({"accession": accession, "main": rendered[False], "branch": rendered[True]})

    parser_mod._parse_table_html = original_parse_table
    promos = sorted(seen_shapes.values(), key=lambda e: -e["count"])

    print(f"== label-arm promotions whose score_headers CHANGE (scanned {scanned} bodies) ==")
    print(f"  distinct header shapes: {len(promos)}   total tables: {sum(p['count'] for p in promos)}")
    worse = [p for p in promos if p["score_folded"] < p["score_unfolded"]]
    newly_disqualified = [p for p in promos if p["item402_marker_after_fold"] and p["score_unfolded"] > 0]
    # The inverse hazard, and the one that actually costs holders: a table that
    # was BELOW the score floor unfolded and clears it folded. An Item 402(g)
    # "Option Exercises and Stock Vested" table ('Number of Shares Acquired on
    # Vesting', 'Value Realized on Vesting') is the live example — it carries
    # none of the _ITEM_402_AWARD_MARKERS, so nothing disqualifies it, and at 5
    # it can outrank a genuine Item 403 table.
    newly_qualifying = [p for p in promos if p["score_unfolded"] < 3 <= p["score_folded"]]
    print(f"  shapes where the fold LOWERS the score:      {len(worse)}")
    print(f"  shapes newly disqualified as Item 402 award: {len(newly_disqualified)}")
    print(f"  shapes newly CLEARING the floor (hazard):    {len(newly_qualifying)}")
    for p in newly_disqualified[:20]:
        print(f"    x{p['count']:<5} {p['score_unfolded']:>3} -> 0  markers={p['item402_marker_after_fold']}")
        print(f"           promoted={p['promoted'][:6]}")
    print("  newly-clearing shapes:")
    for p in sorted(newly_qualifying, key=lambda e: -e["count"])[:30]:
        print(f"    x{p['count']:<5} {p['score_unfolded']:>3} -> {p['score_folded']:<3} example={p['example']}")
        print(f"           promoted={[c[:45] for c in p['promoted']][:5]}")
    print("  top shapes by frequency:")
    for p in promos[:25]:
        print(f"    x{p['count']:<5} {p['score_unfolded']:>3} -> {p['score_folded']:<3} example={p['example']}")
        print(f"           parent={[c[:40] for c in p['unfolded']][:5]}")
        print(f"           promoted={[c[:40] for c in p['promoted']][:5]}")

    print(f"\n== Item 402(c) SCT drift, FULL rows + selected-table score: {len(sct_drift)} accessions ==")
    for d in sct_drift[:25]:
        print(f"    {d['accession']}")
        print(f"      main  : {str(d['main'])[:300]}")
        print(f"      branch: {str(d['branch'])[:300]}")
    return 0


def _stored(summaries: list[dict[str, Any]]) -> int:
    """Compare one or two scans against the holder names PERSISTED in
    ``def14a_beneficial_holdings``.

    The parse-to-parse A/B cannot see a filing where both sides return nothing —
    the stored rows survive only because nothing force-rewashed them, and any
    unconditional rewash deletes them (#2158). This arm is the only one that
    does see them.

    With TWO summaries (main, branch) it also reports the regression invariant
    the single-summary form cannot express: **stored holders that ``main``
    reproduced and the branch does not**. "Real" is decided by the parser's own
    name-evidence predicate ``_looks_like_name_cell`` — the invariant #2140 §3
    settled ("a holder name must carry name evidence") — not by an ad-hoc
    classifier written for this diff.
    """
    stored: dict[str, set[str]] = {}
    with psycopg.connect(settings.database_url) as conn:
        conn.execute("SET statement_timeout = 0")
        with conn.cursor(name="def14a_stored") as cur:
            cur.itersize = 5000
            cur.execute("SELECT accession_number, lower(trim(holder_name)) FROM def14a_beneficial_holdings")
            for accession, name in cur:
                stored.setdefault(accession, set()).add(name)

    def unreproduced(summary: dict[str, Any]) -> dict[str, set[str]]:
        scanned: dict[str, list[str]] = summary.get("per_accession_holders", {})
        out: dict[str, set[str]] = {}
        for accession, names in stored.items():
            missing = names - set(scanned.get(accession, []))
            if missing:
                out[accession] = missing
        return out

    def split_real(names: set[str]) -> tuple[list[str], list[str]]:
        real = sorted(n for n in names if parser_mod._looks_like_name_cell(n))
        return real, sorted(names - set(real))

    last = unreproduced(summaries[-1])
    print("== stored rows the scanned parser does NOT reproduce ==")
    print(f"  accessions with stored rows:         {len(stored)}")
    print(f"  accessions with >=1 unreproduced:    {len(last)}")
    total_real = sum(len(split_real(v)[0]) for v in last.values())
    print(f"  unreproduced stored holders (total): {sum(len(v) for v in last.values())}")
    print(f"    ...carrying name evidence:         {total_real}")
    for accession, names in sorted(last.items(), key=lambda x: -len(x[1]))[:40]:
        real, junk = split_real(names)
        print(f"    {accession}  -{len(names)}  real={real[:4]}  no-name-evidence={junk[:4]}")

    if len(summaries) == 2:
        first = unreproduced(summaries[0])
        regressed: list[tuple[str, list[str]]] = []
        for accession, names in last.items():
            lost = names - first.get(accession, set())
            if lost:
                regressed.append((accession, sorted(lost)))
        print("\n== REGRESSION: stored holders main reproduced and the branch does not ==")
        print(f"  accessions: {len(regressed)}   holders: {sum(len(n) for _, n in regressed)}")
        real_total = sum(len(split_real(set(n))[0]) for _, n in regressed)
        print(f"  ...carrying name evidence: {real_total}")
        for accession, names in sorted(regressed, key=lambda x: -len(x[1]))[:40]:
            real, junk = split_real(set(names))
            print(f"    {accession}  -{len(names)}  real={real[:4]}  no-name-evidence={junk[:4]}")
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--out", help="write a scan summary to this path")
    p.add_argument("--limit", type=int, default=None, help="scan only the first N bodies (smoke)")
    p.add_argument("--diff", nargs=2, metavar=("BEFORE", "AFTER"), help="diff two summaries")
    p.add_argument(
        "--stored",
        nargs="+",
        metavar="SUMMARY",
        help="compare summaries against stored typed rows; pass MAIN BRANCH for the regression arm",
    )
    p.add_argument(
        "--audit",
        action="store_true",
        help="full-population score_headers-fold provenance + full-row SCT drift (branch only)",
    )
    args = p.parse_args(argv if argv is not None else sys.argv[1:])

    if args.audit:
        return _audit(args.limit)

    if args.stored:
        loaded = []
        for path in args.stored:
            with open(path) as f:
                loaded.append(json.load(f))
        return _stored(loaded)

    if args.diff:
        with open(args.diff[0]) as f:
            before = json.load(f)
        with open(args.diff[1]) as f:
            after = json.load(f)
        return _diff(before, after)

    if not args.out:
        p.error("one of --out, --diff, --stored or --audit is required")
    result = _scan(args.limit)
    with open(args.out, "w") as f:
        json.dump(result, f)
    print(json.dumps(result["totals"], indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
