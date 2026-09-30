"""#3518 full-population extraction census: MD&A text of each eligible name's target report.

Population and target are the fund-v1 spec rules
(``docs/proposals/execution/2026-09-30-3515-ai-discretionary-fund-v1.md`` §3 event rules, §4 target): the
shortlist-eligible names at ``as_of`` (production eligibility, via
``scripts.measure_3515_fund_pack_coverage._eligible_ids``), and per name the newest original ``10-K`` / ``10-KT`` /
``10-Q`` / ``10-QT`` ``filing_events`` row with exactly one such row per accession, ``created_at <= as_of`` and
``report_date`` set and not after the UTC ``as_of`` date, ordered by (report_date, filing_date, accession) descending.

Each distinct accession's primary document is fetched ONCE through the house SEC client under the cross-process
``sec_rate_gate`` (sec-edgar skill §4), then parsed OFFLINE by the public edgartools accessor
(``TenK['Item 7']`` / ``TenQ.get_item_with_part('Part I', 'Item 2')``) in a worker whose DNS and socket connects
raise. The raw accessor text is saved per accession (gzip) so every metric below can be recomputed; the summary
applies the fund-v1 §4 normalisation and the #3518 spec §4.3 caption rule to it.

    PYTHONPATH=. uv run python -m scripts.measure_3518_mdna_extraction_census --out /tmp/c3518.jsonl --bodies /tmp/c3518
    PYTHONPATH=. uv run python -m scripts.measure_3518_mdna_extraction_census --out /tmp/c3518.jsonl \
        --bodies /tmp/c3518 --summarise-only
"""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import re
import statistics
import subprocess
import time
import unicodedata
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeout
from pathlib import Path
from typing import Any

import psycopg

from app.config import settings

ANNUAL = ("10-K", "10-KT")
QUARTERLY = ("10-Q", "10-QT")
SUBSTANCE_FLOOR = 1_000  # fund-v1 §4 MDNA_MIN_SUBSTANTIVE_CHARS
PACK_CAP = 6_000  # fund-v1 §4 MDNA_MAX_CHARS
PARSE_TIMEOUT_S = 300  # #3518 spec §4.4
MAX_PENDING = 8  # documents fetched but not yet parsed (bounds census memory)

# --- fund-v1 §4 normalisation, in its stated order --------------------------------------------------------------
_VERTICAL = "\n\r\x0b\x0c\x1c\x1d\x1e\x1f\x85  "
_HSPACE = re.compile(r"[^\S" + re.escape(_VERTICAL) + r"]")
_SPACES = re.compile(r" {2,}")
_NL3 = re.compile(r"\n{3,}")


def normalise(text: str) -> str:
    """Tabs and other horizontal whitespace -> one space each; strip remaining control characters except newline;
    collapse runs of spaces to one; collapse three or more newlines to two; trim."""
    t = _HSPACE.sub(" ", text)
    t = "".join(ch for ch in t if ch == "\n" or unicodedata.category(ch) != "Cc")
    t = _SPACES.sub(" ", t)
    t = _NL3.sub("\n\n", t)
    return t.strip()


# --- #3518 spec §4.3 caption rule (Rule 12b-13 item captions) ----------------------------------------------------
CAPTION = re.compile(
    r"discussion\s*(?:and|&(?:amp;)?)\s*analysis|financial\s+condition\s*(?:and|&(?:amp;)?)\s*results\s+of\s+operations",
    re.IGNORECASE,
)
CAPTION_WINDOW = 1_000


def caption_present(normalised: str) -> bool:
    """Caption half within the first CAPTION_WINDOW characters after collapsing ALL whitespace to one space."""
    return bool(CAPTION.search(re.sub(r"\s+", " ", normalised)[:CAPTION_WINDOW]))


# --- measured, not gated -----------------------------------------------------------------------------------------
# A line-start heading of the item that FOLLOWS the MD&A (10-K Item 7A / 8; 10-Q Part I Items 3 / 4, Part II).
_NEXT_10K = re.compile(r"^[^\S\n]*(item\s*(?:7a|8)\s*[.:\-—–])", re.IGNORECASE | re.MULTILINE)
_NEXT_10Q = re.compile(r"^[^\S\n]*(item\s*[34]\s*[.:\-—–]|part\s+ii\b)", re.IGNORECASE | re.MULTILINE)
# Table cells run together: "EndedJune 30, 2026", "Profit$4,558".
_TABLE_GLUE = re.compile(
    r"[a-z](?:January|February|March|April|May|June|July|August|September|October|November|"
    r"December)\s\d{1,2},|[A-Za-z)]\$\d"
)
_PAGE_END = re.compile(r"\s\d{1,3}$")


def _excess_dup_share(text: str) -> float:
    """Share of paragraph characters (paragraphs >= 200 chars) that are EXCESS copies of an earlier paragraph."""
    paras = [p for p in text.split("\n\n") if len(p) >= 200]
    if not paras:
        return 0.0
    seen: set[str] = set()
    excess = 0
    for p in paras:
        if p in seen:
            excess += len(p)
        seen.add(p)
    return excess / sum(len(p) for p in paras)


def _toc_like(text: str) -> bool:
    """>= 30% of the first 40 non-empty lines end in a page number (a table-of-contents shape)."""
    lines = [ln for ln in text.split("\n") if ln.strip()][:40]
    return len(lines) >= 5 and sum(bool(_PAGE_END.search(ln)) for ln in lines) / len(lines) >= 0.3


def describe(raw: str | None, family: str) -> dict[str, Any]:
    if raw is None or not raw.strip():
        return {"status": "item_absent"}
    n = normalise(raw)
    nxt = (_NEXT_10K if family == "10-K" else _NEXT_10Q).search(n)
    prefix = n[:PACK_CAP]
    return {
        "status": "extracted" if caption_present(n) else "caption_absent",
        "chars": len(n),
        "next_item_at": nxt.start(1) if nxt else None,
        "excess_dup_share": _excess_dup_share(n),
        "prefix_excess_dup_share": _excess_dup_share(prefix),
        "prefix_table_glue": len(_TABLE_GLUE.findall(prefix)),
        "toc_like": _toc_like(n),
    }


# --- worker ------------------------------------------------------------------------------------------------------
class _Stub:
    def __init__(self, form: str, html: str, acc: str, url: str) -> None:
        self.form = form
        self._html = html
        self.accession_number = acc
        self.base_dir = url.rsplit("/", 1)[0]  # legacy ChunkedDocument's image-src prefix; a string, never fetched

    def html(self) -> str:
        return self._html


class _Capture(logging.Handler):
    def __init__(self) -> None:
        super().__init__(logging.WARNING)
        self.messages: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.messages.append(record.getMessage())


def _block_network() -> None:
    import socket

    def _deny(*_a: Any, **_k: Any) -> Any:
        raise RuntimeError("NETWORK_ATTEMPT")

    socket.getaddrinfo = _deny  # type: ignore[assignment]
    socket.create_connection = _deny  # type: ignore[assignment]
    socket.socket.connect = _deny  # type: ignore[method-assign,assignment]
    socket.socket.connect_ex = _deny  # type: ignore[method-assign,assignment]


def parse_one(args: tuple[str, str, str, str]) -> dict[str, Any]:
    """Parse one primary document offline with the public accessor; return the raw text plus path evidence."""
    import warnings

    _block_network()
    warnings.simplefilter("ignore")
    cap = _Capture()
    logging.getLogger().addHandler(cap)
    logging.getLogger("edgar.core").addHandler(cap)  # TenK/TenQ log their legacy fallback here

    from edgar.company_reports.ten_k import TenK
    from edgar.company_reports.ten_q import TenQ

    acc, form, html, url = args
    family = "10-K" if form in ANNUAL else "10-Q"
    key = "part_ii_item_7" if family == "10-K" else "part_i_item_2"
    out: dict[str, Any] = {"accession": acc, "form": form, "source_chars": len(html)}
    t0 = time.time()
    raw: str | None = None
    try:
        stub = _Stub(family, html, acc, url)
        if family == "10-K":
            rep: Any = TenK(stub)
            raw = rep["Item 7"]
        else:
            rep = TenQ(stub)
            raw = rep.get_item_with_part("Part I", "Item 2", markdown=False)
        sections = rep.sections or {}
        keyed = sections[key].text() if key in sections else None
        out["new_parser_key_present"] = key in sections
        out["equals_new_parser_key"] = bool(raw and raw.strip()) and (keyed or "").strip() == (raw or "").strip()
        out["legacy_fallback_logged"] = any("falling back to legacy parser" in m for m in cap.messages)
    except Exception as exc:  # noqa: BLE001 — census: every failure is a counted outcome
        out["error"] = f"{type(exc).__name__}: {str(exc)[:200]}"
    out["network_attempt"] = any("NETWORK_ATTEMPT" in m for m in cap.messages) or "NETWORK_ATTEMPT" in out.get(
        "error", ""
    )
    out["parse_s"] = round(time.time() - t0, 2)
    logging.getLogger().removeHandler(cap)
    logging.getLogger("edgar.core").removeHandler(cap)
    out["_raw"] = raw
    return out


# --- population --------------------------------------------------------------------------------------------------
def _targets(conn: psycopg.Connection, ids: list[int], as_of: Any) -> list[dict[str, Any]]:  # type: ignore[type-arg]
    rows = conn.execute(
        """
        WITH ev AS (
            SELECT fe.instrument_id, fe.provider_filing_id AS acc, min(fe.filing_type) AS form,
                   min(fe.filing_date) AS filing_date, min(fe.report_date) AS report_date,
                   min(fe.primary_document_url) AS url, count(*) AS n
              FROM filing_events fe
             WHERE fe.instrument_id = ANY(%(ids)s) AND fe.filing_type = ANY(%(forms)s)
               AND fe.created_at <= %(as_of)s
             GROUP BY 1, 2
        )
        SELECT DISTINCT ON (ev.instrument_id) ev.instrument_id, i.symbol, ev.acc, ev.form, ev.url,
               (SELECT count(*) FROM filing_events o
                 WHERE o.instrument_id = ev.instrument_id AND o.provider_filing_id = ev.acc
                   AND o.filing_type <> ALL(%(forms)s) AND o.created_at <= %(as_of)s) AS other_form_rows
          FROM ev JOIN instruments i USING (instrument_id)
         WHERE ev.n = 1 AND ev.report_date IS NOT NULL
           AND ev.report_date <= (%(as_of)s AT TIME ZONE 'UTC')::date
         ORDER BY ev.instrument_id, ev.report_date DESC, ev.filing_date DESC, ev.acc DESC
        """,
        {"ids": ids, "forms": list(ANNUAL + QUARTERLY), "as_of": as_of},
    ).fetchall()
    return [
        {
            "instrument_id": int(r[0]),
            "symbol": str(r[1]),
            "accession": str(r[2]),
            "form": str(r[3]),
            "url": r[4],
            "other_form_rows": int(r[5]),
        }
        for r in rows
    ]


def _pct(xs: list[float], q: float) -> float:
    if not xs:
        return float("nan")
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(q * (len(xs) - 1) + 0.5))]


# --- summary -----------------------------------------------------------------------------------------------------
def summarise(out: Path, bodies: Path) -> None:
    meta = json.loads(out.with_suffix(".meta.json").read_text())
    print("meta:", meta)
    per_name = [json.loads(line) for line in out.read_text().splitlines()]
    results = list({r["accession"]: r for r in per_name}.values())
    print("eligible names:", meta["eligible"], "| names with target:", len(per_name), "| accessions:", len(results))
    multi = Counter(r["accession"] for r in per_name)
    shared = [a for a, k in multi.items() if k > 1]
    inconsistent = [a for a in shared if len({(n["form"], n["url"]) for n in per_name if n["accession"] == a}) > 1]
    print("shared accessions:", len(shared), "| with inconsistent form/url across instruments:", len(inconsistent))
    print(
        "targets with a non-original filing_events row for the same accession:",
        sum(n["other_form_rows"] > 0 for n in per_name),
    )
    print("fetch:", dict(Counter(r["fetch"] for r in results)))
    ok = [r for r in results if r["fetch"] == "ok"]
    print(
        "parse errors:",
        dict(Counter(r.get("error", "")[:60] for r in ok if r.get("error"))),
        "| network attempts:",
        sum(r.get("network_attempt", False) for r in ok),
    )
    missing_body = 0
    for r in ok:
        f = bodies / f"{r['accession']}.raw.txt.gz"
        raw = None
        if not r.get("error") and r.get("has_text"):
            if f.exists():
                with gzip.open(f, "rt", newline="") as fh:
                    raw = fh.read()
            else:
                missing_body += 1
        family = "10-K" if r["form"] in ANNUAL else "10-Q"
        r["d"] = {"status": "parse_failed"} if r.get("error") else describe(raw, family)
    print("extracted rows whose body file is missing:", missing_body)
    for fam, forms in (("10-K", ANNUAL), ("10-Q", QUARTERLY)):
        rs = [r for r in ok if r["form"] in forms]
        ex = [r["d"] for r in rs if r["d"]["status"] == "extracted"]
        ch = [d["chars"] for d in ex]
        print(f"\n== {fam} family: {len(rs)} parsed; forms {dict(Counter(r['form'] for r in rs))}")
        print("  outcome:", dict(Counter(r["d"]["status"] for r in rs)))
        print("  caption_absent:", sorted(r["accession"] for r in rs if r["d"]["status"] == "caption_absent"))
        print(
            "  text via part-qualified new-parser key:",
            sum(r.get("equals_new_parser_key", False) for r in rs),
            "| legacy fallback logged:",
            sum(r.get("legacy_fallback_logged", False) for r in rs),
        )
        if ch:
            print(
                f"  extracted chars p10 {_pct(ch, 0.1):.0f} p50 {_pct(ch, 0.5):.0f} p90 {_pct(ch, 0.9):.0f} "
                f"max {max(ch)}; below {SUBSTANCE_FLOOR}: {sum(c < SUBSTANCE_FLOOR for c in ch)}"
            )
            print(
                "  next-item heading inside body:",
                sum(d["next_item_at"] is not None for d in ex),
                "| of which before char",
                PACK_CAP,
                ":",
                sum(d["next_item_at"] is not None and d["next_item_at"] < PACK_CAP for d in ex),
            )
            print(
                "  excess duplicate share > 5%: body",
                sum(d["excess_dup_share"] > 0.05 for d in ex),
                "| displayed prefix",
                sum(d["prefix_excess_dup_share"] > 0.05 for d in ex),
            )
            print(
                "  table-glue hits in displayed prefix: bodies with >= 1",
                sum(d["prefix_table_glue"] > 0 for d in ex),
                "| >= 5",
                sum(d["prefix_table_glue"] >= 5 for d in ex),
            )
            print(
                "  TOC-shaped: all",
                sum(d["toc_like"] for d in ex),
                "| with >=",
                SUBSTANCE_FLOOR,
                "chars",
                sum(d["toc_like"] and d["chars"] >= SUBSTANCE_FLOOR for d in ex),
            )
    sc = [r["source_chars"] for r in ok]
    ps = [r["parse_s"] for r in ok]
    print(f"\nsource chars p50 {_pct(sc, 0.5):.0f} p99 {_pct(sc, 0.99):.0f} max {max(sc, default=0)}")
    print(
        f"parse_s p50 {statistics.median(ps) if ps else None} p90 {_pct(ps, 0.9)} p99 {_pct(ps, 0.99)} "
        f"max {max(ps, default=None)} sum {sum(ps):.0f}; wall {meta.get('wall_s')} s with {meta['workers']} workers"
    )
    by_acc = {r["accession"]: r for r in results}

    def covered(n: dict[str, Any]) -> bool:
        r = by_acc[n["accession"]]
        return r["fetch"] == "ok" and r["d"]["status"] == "extracted" and r["d"]["chars"] >= SUBSTANCE_FLOOR

    good = sum(covered(n) for n in per_name)
    print(f"\neligible names with extracted, captioned text >= {SUBSTANCE_FLOOR} chars: {good} of {meta['eligible']}")


# --- main --------------------------------------------------------------------------------------------------------
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--bodies", required=True)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--summarise-only", action="store_true", help="re-summarise an existing --out / --bodies")
    a = ap.parse_args()
    out, bodies = Path(a.out), Path(a.bodies)
    if a.summarise_only:
        summarise(out, bodies)
        return
    bodies.mkdir(parents=True, exist_ok=True)

    from importlib.metadata import version

    from psycopg_pool import ConnectionPool

    from app.providers.implementations.sec_edgar import SecFilingsProvider
    from app.providers.postgres_rate_gate import PostgresFloorGate
    from app.providers.rate_gate import SEC_MIN_REQUEST_INTERVAL_S
    from app.providers.sec_rate_gate_holder import set_sec_rate_gate
    from scripts.measure_3515_fund_pack_coverage import _eligible_ids

    pool = ConnectionPool(settings.database_url, min_size=1, max_size=1, kwargs={"application_name": "census-3518"})
    set_sec_rate_gate(PostgresFloorGate(pool, budget="sec", floor_s=SEC_MIN_REQUEST_INTERVAL_S))

    with psycopg.connect(settings.database_url) as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        as_of = conn.execute("SELECT now()").fetchone()[0]  # type: ignore[index]
        ids = _eligible_ids(conn, as_of)
        targets = _targets(conn, ids, as_of)
    by_acc: dict[str, list[dict[str, Any]]] = {}
    for t in targets:
        by_acc.setdefault(t["accession"], []).append(t)
    accs = list(by_acc)[: a.limit or None]
    print("as_of:", as_of, "eligible:", len(ids), "with target:", len(targets), "accessions:", len(accs), flush=True)

    results: list[dict[str, Any]] = []
    t0 = time.time()

    def _collect(fut: Any, acc: str, form: str) -> None:
        try:
            r = fut.result(timeout=PARSE_TIMEOUT_S)
        except FutureTimeout:
            results.append(
                {"accession": acc, "form": form, "fetch": "ok", "error": "timeout", "parse_s": 0, "source_chars": 0}
            )
            return
        raw = r.pop("_raw", None)
        r["has_text"] = bool(raw and raw.strip())
        if r["has_text"]:
            with gzip.open(bodies / f"{acc}.raw.txt.gz", "wt", newline="") as fh:
                fh.write(raw)  # type: ignore[arg-type]
        r["fetch"] = "ok"
        results.append(r)

    with SecFilingsProvider(user_agent=settings.sec_user_agent) as prov, ProcessPoolExecutor(a.workers) as ex:
        pending: list[tuple[Any, str, str]] = []
        for i, acc in enumerate(accs):
            first = by_acc[acc][0]
            form, url = first["form"], first["url"]
            if not url:
                results.append({"accession": acc, "form": form, "fetch": "no_url"})
                continue
            try:
                html = prov.fetch_document_text(url)
            except Exception as exc:  # noqa: BLE001
                results.append({"accession": acc, "form": form, "fetch": f"raised {type(exc).__name__}"})
                continue
            if html is None:
                results.append({"accession": acc, "form": form, "fetch": "none_404_410"})
                continue
            if not html.strip():
                results.append({"accession": acc, "form": form, "fetch": "empty_or_whitespace_200"})
                continue
            pending.append((ex.submit(parse_one, (acc, form, html, url)), acc, form))
            del html
            while len(pending) >= MAX_PENDING:
                _collect(*pending.pop(0))
            if i % 100 == 0:
                print(f"fetched {i}/{len(accs)} t={time.time() - t0:.0f}s", flush=True)
        for p in pending:
            _collect(*p)
    pool.close()

    with open(out, "w") as fh:
        for r in results:
            for t in by_acc[r["accession"]]:
                fh.write(
                    json.dumps({**r, **{k: t[k] for k in ("instrument_id", "symbol", "url", "other_form_rows")}}) + "\n"
                )
    git_sha = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()
    out.with_suffix(".meta.json").write_text(
        json.dumps(
            {
                "as_of": str(as_of),
                "eligible": len(ids),
                "with_target": len(targets),
                "accessions": len(accs),
                "limit": a.limit,
                "workers": a.workers,
                "edgartools": version("edgartools"),
                "git_sha": git_sha,
                "wall_s": round(time.time() - t0),
            }
        )
    )
    summarise(out, bodies)


if __name__ == "__main__":
    main()
