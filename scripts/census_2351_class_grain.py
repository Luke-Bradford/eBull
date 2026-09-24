"""#2351 storage-model slice — full-population census of multi-class Item 403 tables.

Item 403(a)/(b) (17 CFR 229.403) reports beneficial ownership per *Title of class*
(column 1). The parser stores one row per holder per accession, reading ONE shares column
(`_resolve_columns`) and keeping a holder's FIRST row (`seen`). This census measures, over
EVERY stored ``def14a_body``, how often the parser's own selected tables carry more than
one class, in which shape, and how many (holder, class) lines the one-row model discards:

- ``V`` — a column captioned ``Title of class|series`` (Item 403 column 1 by its caption);
  a line = a (holder, class-cell) pair with a share cell.
- ``H`` — two or more share-bearing columns whose header captions name different classes
  (``Class A`` / ``Class B`` / a preferred word); a line = a (holder, class column) share cell.
- ``S`` — section-label rows below the header (no share cell, a class designator or a
  preferred word) splitting one table into per-class blocks.

Offline, read-only. Output: one JSON line per accession, then ``--summarise``.

    PYTHONPATH=. uv run python -m scripts.census_2351_class_grain --out var/census_2351/grain.jsonl
    PYTHONPATH=. uv run python -m scripts.census_2351_class_grain --summarise var/census_2351/grain.jsonl
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Any

import psycopg

from app.config import settings
from app.providers.implementations.sec_def14a import (
    _TITLE_OF_CLASS,
    _is_share_cell,
    _layout_name_key,
    _layout_rows,
    item403_table_htmls,
)
from app.services.def14a_recipients import _NON_COMMON_CAPTION, keyed_designators


def _class_key(texts: list[str]) -> str | None:
    keys = sorted({f"{k}:{letter}" for t in texts for k, letter in keyed_designators(t)})
    if len(keys) == 1:
        return keys[0]
    if len(keys) > 1:
        return "multi:" + ",".join(keys)
    if any(_NON_COMMON_CAPTION.search(t) for t in texts):
        return "noncommon"
    return None


def classify(accession: str, payload: str) -> dict[str, Any]:
    out: dict[str, Any] = {"accession": accession, "tables": 0, "shapes": [], "v_lines": 0, "v_holders_multi": 0}
    out.update({"h_lines": 0, "h_holders_multi": 0, "s_labels": 0, "error": None})
    try:
        tables = item403_table_htmls(payload)
    except Exception as exc:  # census: record and continue
        out["error"] = repr(exc)[:200]
        return out
    out["tables"] = len(tables)
    shapes: set[str] = set()
    for html in tables:
        grid = _layout_rows(html)
        header_end = next((r for r, row in enumerate(grid) if any(_is_share_cell(t) for t in row.values())), None)
        if header_end is None:
            continue
        header_texts: dict[int, list[str]] = {}
        for above in grid[:header_end]:
            for c, text in above.items():
                if text and text not in header_texts.setdefault(c, []):
                    header_texts[c].append(text)
        body = grid[header_end:]
        share_cols = sorted({c for row in body for c, t in row.items() if t and _is_share_cell(t)})
        class_cols = [c for c, texts in header_texts.items() if _TITLE_OF_CLASS.search(" ".join(texts))]
        # V: per holder name-key, distinct class cells on share-bearing rows.
        if class_cols:
            per_holder: dict[str, set[str]] = {}
            last: str | None = None
            for row in body:
                if not any(_is_share_cell(t) for t in row.values() if t):
                    continue
                cells = {row.get(c, "").strip() for c in class_cols} - {""}
                names = [
                    k
                    for c, t in row.items()
                    if t and c not in class_cols and not _is_share_cell(t)
                    if (k := _layout_name_key(t))
                ]
                # A continuation line (holder's next class, name cell blank) belongs to
                # the last named holder above it.
                holder = names[0] if names else last
                if names:
                    last = names[0]
                if holder and cells:
                    per_holder.setdefault(holder, set()).update(cells)
            if any(len(v) > 1 for v in per_holder.values()) or len({x for v in per_holder.values() for x in v}) > 1:
                shapes.add("V")
                out["v_lines"] += sum(len(v) for v in per_holder.values())
                out["v_holders_multi"] += sum(1 for v in per_holder.values() if len(v) > 1)
        # H: share-bearing columns labelled by different classes.
        col_class = {c: _class_key(header_texts.get(c, [])) for c in share_cols if c not in class_cols}
        labelled = {c: k for c, k in col_class.items() if k}
        if len(set(labelled.values())) > 1:
            shapes.add("H")
            for row in body:
                hit = {labelled[c] for c, t in row.items() if c in labelled and t and _is_share_cell(t)}
                out["h_lines"] += len(hit)
                out["h_holders_multi"] += 1 if len(hit) > 1 else 0
        # S: in-body section labels naming a class.
        labels = {
            k
            for row in body
            if not any(_is_share_cell(t) for t in row.values() if t)
            and (k := _class_key([t for t in row.values() if t]))
        }
        if labels:
            shapes.add("S")
            out["s_labels"] += len(labels)
    out["shapes"] = sorted(shapes)
    return out


def _work(item: tuple[str, str]) -> dict[str, Any]:
    return classify(*item)


def check_lines(accession: str, payload: str) -> dict[str, Any]:
    """M1 acceptance for one body: ``item403_lines`` beside the legacy parser."""
    import time

    from app.providers.implementations.sec_def14a import parse_beneficial_ownership_table
    from app.services.def14a_item403_lines import _full_key, class_evidence, item403_lines

    started = time.perf_counter()
    rec: dict[str, Any] = {"accession": accession, "error": None}
    try:
        parsed = parse_beneficial_ownership_table(payload)
        ex = item403_lines(payload)
    except Exception as exc:  # census: record and continue
        rec["error"] = repr(exc)[:200]
        return rec
    rec["ms"] = round((time.perf_counter() - started) * 1000, 1)
    rec["rows"] = len(parsed.rows)
    rec["lines"] = len(ex.lines)
    rec["table_errors"] = ex.errors
    rec["duplicate_keys"] = ex.duplicate_keys
    by_holder: dict[str, set[Any]] = {}
    owners: dict[tuple[int, int, int], set[str]] = {}
    states: Counter[str] = Counter()
    labelled_groups: dict[str, set[frozenset[str]]] = {}
    for line in ex.lines:
        states[line.class_state] += 1
        for cell in line.amount_cells:
            by_holder.setdefault(_full_key(line.holder_name), set()).add(cell.shares)
            owners.setdefault((line.table_ordinal, line.grid_row, cell.first_column), set()).add(line.holder_name)
        if line.class_state == "labelled" and any(c.shares for c in line.amount_cells):
            ev = (
                line.group_evidence
                | class_evidence(line.row_class_cells)
                | class_evidence([line.section_label] if line.section_label else [])
            ) - {"common"}
            labelled_groups.setdefault(line.holder_name, set()).add(ev)
    rec["misses"] = [
        [h.holder_name, str(h.shares)]
        for h in parsed.rows
        if h.shares is not None and h.shares not in by_holder.get(_full_key(h.holder_name), set())
    ]
    rec["shared_cells"] = sum(1 for v in owners.values() if len(v) > 1)
    rec["states"] = dict(states)
    rec["recovered_holders"] = sum(1 for v in labelled_groups.values() if len(v) > 1)
    return rec


def _work_lines(item: tuple[str, str]) -> dict[str, Any]:
    return check_lines(*item)


def run_lines(out_path: Path, workers: int) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with psycopg.connect(settings.database_url) as conn, out_path.open("w") as fh:
        with conn.cursor(name="bodies") as cur:
            cur.itersize = 200
            cur.execute(
                "SELECT accession_number, payload FROM filing_raw_documents WHERE document_kind = 'def14a_body'"
            )
            with ProcessPoolExecutor(max_workers=workers) as pool:
                done = 0
                while batch := cur.fetchmany(400):
                    for rec in pool.map(_work_lines, batch, chunksize=10):
                        fh.write(json.dumps(rec) + "\n")
                    done += len(batch)
                    print(f"{done} bodies", flush=True)
    print("COMPLETE", flush=True)


def lines_summary(lines_path: Path, grain_path: Path) -> int:
    shapes = {r["accession"]: r["shapes"] for r in map(json.loads, grain_path.open())}
    recs = [json.loads(line) for line in lines_path.open()]
    accs = [r["accession"] for r in recs]
    print(f"bodies={len(recs)} unique={len(set(accs))} errors={sum(1 for r in recs if r['error'])}")
    ok = [r for r in recs if not r["error"]]
    print(f"table_errors={sum(len(r['table_errors']) for r in ok)} rows={sum(r['rows'] for r in ok)}")
    print(f"lines={sum(r['lines'] for r in ok)} bodies_with_duplicate_keys={sum(1 for r in ok if r['duplicate_keys'])}")
    misses = [(r["accession"], m) for r in ok for m in r["misses"]]
    print(f"coverage misses={len(misses)} in {len({a for a, _ in misses})} bodies")
    shared = sum(r["shared_cells"] for r in ok)
    print(f"cells attached to >1 holder={shared}")
    for label, pop in (
        ("no shape", [r for r in ok if not shapes.get(r["accession"])]),
        ("multi-class shape", [r for r in ok if shapes.get(r["accession"])]),
    ):
        states: Counter[str] = Counter()
        for r in pop:
            states.update(r["states"])
        print(
            f"{label}: bodies={len(pop)} states={dict(states)} "
            f"recovered_holders={sum(r['recovered_holders'] for r in pop)}"
        )
    ms = sorted(r["ms"] for r in ok)
    if ms:
        print(f"ms/body median={ms[len(ms) // 2]} p99={ms[int(len(ms) * 0.99)]} max={ms[-1]}")
    return 1 if shared or any(r["error"] for r in recs) or len(set(accs)) != len(accs) else 0


def run(out_path: Path, workers: int) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with psycopg.connect(settings.database_url) as conn, out_path.open("w") as fh:
        meta = dict(
            conn.execute(
                """
                SELECT h.accession_number, jsonb_build_object('cik', min(h.issuer_cik),
                       'instruments', count(DISTINCT h.instrument_id), 'rows', count(*))
                FROM def14a_beneficial_holdings h GROUP BY h.accession_number
                """
            ).fetchall()
        )
        siblings = dict(
            conn.execute(
                "SELECT identifier_value, count(DISTINCT instrument_id) FROM external_identifiers "
                "WHERE provider = 'sec' AND identifier_type = 'cik' GROUP BY identifier_value"
            ).fetchall()
        )
        with conn.cursor(name="bodies") as cur:
            cur.itersize = 200
            cur.execute(
                "SELECT accession_number, payload FROM filing_raw_documents WHERE document_kind = 'def14a_body'"
            )
            with ProcessPoolExecutor(max_workers=workers) as pool:
                done = 0
                while batch := cur.fetchmany(400):
                    for rec in pool.map(_work, batch, chunksize=10):
                        m = meta.get(rec["accession"]) or {}
                        rec["stored_rows"] = m.get("rows", 0)
                        rec["stored_instruments"] = m.get("instruments", 0)
                        rec["cik"] = m.get("cik")
                        rec["cik_siblings"] = siblings.get(m.get("cik"), 0) if m.get("cik") else 0
                        fh.write(json.dumps(rec) + "\n")
                    done += len(batch)
                    print(f"{done} bodies", flush=True)


def summarise(path: Path) -> None:
    recs = [json.loads(line) for line in path.open()]
    stored = [r for r in recs if r["stored_rows"]]
    print(f"bodies={len(recs)} with_stored_rows={len(stored)} errors={sum(1 for r in recs if r['error'])}")
    for label, pop in (("all bodies", recs), ("bodies with stored rows", stored)):
        shapes = Counter("+".join(r["shapes"]) or "single" for r in pop)
        print(f"\n{label}: shape counts {dict(shapes.most_common())}")
        multi = [r for r in pop if r["shapes"]]
        print(f"  multi-class bodies: {len(multi)}; distinct CIKs {len({r['cik'] for r in multi if r['cik']})}")
        by_sib = Counter("multi-sibling" if r["cik_siblings"] > 1 else "single-instrument" for r in multi)
        print(f"  by issuer sibling count: {dict(by_sib)}")
        print(f"  V holders with >1 class line: {sum(r['v_holders_multi'] for r in pop)}")
        print(f"  H holders with >1 class column: {sum(r['h_holders_multi'] for r in pop)}")


def read_class(path: Path) -> None:
    """Which class's column each STORED row was read from, on single-instrument
    multi-class bodies (slice 3's ``row_label``, else slice 3b's ``row_class_key``)."""
    from app.providers.implementations.sec_def14a import share_locations
    from app.services.def14a_recipients import row_class_key, row_label

    recs = [json.loads(line) for line in path.open()]
    multi = [r for r in recs if r["shapes"] and r["stored_rows"] and r["cik_siblings"] <= 1]
    by_row: Counter[str] = Counter()
    by_body: Counter[str] = Counter()
    with psycopg.connect(settings.database_url) as conn:
        for r in multi:
            acc = r["accession"]
            (payload,) = conn.execute(
                "SELECT payload FROM filing_raw_documents "
                "WHERE document_kind = 'def14a_body' AND accession_number = %s",
                (acc,),
            ).fetchone() or (None,)
            rows = conn.execute(
                "SELECT DISTINCT holder_name, shares FROM def14a_beneficial_holdings "
                "WHERE accession_number = %s AND shares IS NOT NULL",
                (acc,),
            ).fetchall()
            labels: set[str] = set()
            for locs in share_locations(item403_table_htmls(payload or ""), [(h, s) for h, s in rows]):
                label = row_label(locs)
                if label is None and (key := row_class_key(locs)):
                    label = f"row:{key[1]}"
                by_row[label or "unbound"] += 1
                labels.add(label or "unbound")
            by_body["+".join(sorted(labels))] += 1
    print(f"single-instrument multi-class bodies: {len(multi)}")
    print("stored rows by read-class label:", by_row.most_common())
    print("bodies by label set:", by_body.most_common())


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", type=Path)
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--summarise", type=Path)
    ap.add_argument("--read-class", type=Path)
    ap.add_argument("--lines", action="store_true", help="M1 acceptance: run item403_lines beside the parser")
    ap.add_argument("--lines-summary", type=Path, help="summarise a --lines output (needs --grain)")
    ap.add_argument("--grain", type=Path, default=Path("var/census_2351/grain.jsonl"))
    args = ap.parse_args()
    if args.lines_summary:
        return lines_summary(args.lines_summary, args.grain)
    if args.lines and args.out:
        run_lines(args.out, args.workers)
    elif args.read_class:
        read_class(args.read_class)
    elif args.summarise:
        summarise(args.summarise)
    elif args.out:
        run(args.out, args.workers)
    else:
        ap.error("--out or --summarise")
    return 0


if __name__ == "__main__":
    sys.exit(main())
