"""#2351 slice 1 — can a DEF 14A class label be bound to a sibling instrument on EXACT evidence?

#2351 stores one Item 403 holder per instrument, so a CIK backing several
instruments (share classes, warrants, preferred) hands every sibling the same
common-stock rows. The 2026-09-24 handoff reframed the fix as FAIL-CLOSED: bind a
proxy class label to an instrument only on exact evidence, write nothing
otherwise. This script measures how much of the multi-sibling population each
exact rule can reach. It proposes nothing and writes nothing to the DB.

Source rule for "which class is instrument I"
---------------------------------------------
The 10-K / 10-Q cover's Section 12(b) table, tagged ``dei:Security12bTitle`` +
``dei:TradingSymbol`` + ``dei:SecurityExchangeName`` in ONE shared context per
registered class (Reg S-K Item 601(b)(104), Reg S-T Rule 406; see
``.claude/skills/data-sources/sec-edgar.md``). The same-context reader is
``scripts.census_2900_sec_cover_identity.parse_cover_contexts``, reused verbatim.
No published rule maps PROXY class text to a registered class, so the three
rules below are fixed by construction (the #2351 handoff, 2026-09-24):

- R1 ``ticker``: the sibling's cover ``TradingSymbol`` appears as a whole token in
  the selected Item 403 table text.
- R2 ``title``: the sibling's cover ``Security12bTitle``, normalised, appears in
  the selected table text.
- R3 ``sole_common``: the table has NO class dimension and exactly one sibling's
  cover title is common/ordinary equity (never a warrant, preferred, unit, note
  or depositary share).

It also applies the slice-2 RECIPIENT rule
(``docs/proposals/ownership/2026-09-24-2351-def14a-class-recipients.md``) in
``recipient_decisions``, which is that rule's reference implementation.

Point-in-time: the cover used for a proxy is the latest 10-K/10-Q/20-F filed ON OR
BEFORE the proxy's filing date — never a later one (look-ahead). An unfetchable or
tag-less newer cover is reported, and the walk-back to an older one is counted
separately (``cover_depth``), because a stale cover is a different claim.

Usage::

    PYTHONPATH=. uv run python -m scripts.census_2351_def14a_class_binding \\
      --cache var/census_2351 --out var/census_2351/report.json
"""

from __future__ import annotations

import argparse
import json
import re
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Final

import httpx
import lxml.etree as ET
import psycopg

import app.providers.implementations.sec_def14a as def14a
from app.config import settings
from scripts.census_2900_sec_cover_identity import _write_gzip_atomic, parse_cover_contexts

COVER_FORMS: Final = ("10-K", "10-K/A", "10-Q", "10-Q/A", "20-F", "20-F/A")
MAX_COVER_DEPTH: Final = 4
REQUEST_INTERVAL_S: Final = 0.15  # ~6.7 req/s, inside SEC's 10 req/s fair-use ceiling

# A class dimension is a class/series/preferred word that is NOT the Item 403
# "Percent of Class" caption.
_CLASS_WORD = re.compile(r"\b(class|series|preferred|warrants?)\b", re.IGNORECASE)
_PERCENT_OF_CLASS = re.compile(r"(percent(age)?|%)\s*(of)?\s*(the\s+)?(outstanding\s+)?class", re.IGNORECASE)
_NON_COMMON = re.compile(r"warrant|preferred|preference|unit|note|depositary|right|debenture|bond", re.IGNORECASE)
_COMMON = re.compile(r"common|ordinary|capital stock", re.IGNORECASE)


@dataclass
class Cover:
    accession: str
    form: str
    filing_date: date
    url: str


@dataclass
class Issuer:
    cik: str
    siblings: list[tuple[int, str]] = field(default_factory=list)
    proxies: list[tuple[str, date | None]] = field(default_factory=list)
    covers: list[Cover] = field(default_factory=list)


def norm_symbol(value: str) -> str:
    """eToro symbol / SEC TradingSymbol to one comparable key.

    eToro appends ``.US`` to disambiguate a second listing; SEC writes class
    suffixes as ``BF.B`` / ``BF-B`` / ``BFB``. Both reduce to ``BFB``.
    """
    upper = value.strip().upper()
    if upper.endswith(".US"):
        upper = upper[:-3]
    return re.sub(r"[.\-/ ]", "", upper)


def norm_text(value: str) -> str:
    return " ".join(re.sub(r"[^a-z0-9$]+", " ", value.lower()).split())


def load_population(conn: psycopg.Connection[Any]) -> dict[str, Issuer]:
    issuers: dict[str, Issuer] = {}
    rows = conn.execute(
        """
        WITH sib AS (
            SELECT identifier_value AS cik, array_agg(instrument_id) AS iids
              FROM external_identifiers
             WHERE provider = 'sec' AND identifier_type = 'cik'
             GROUP BY identifier_value
            HAVING count(*) > 1)
        SELECT s.cik, i.instrument_id, i.symbol
          FROM sib s JOIN instruments i ON i.instrument_id = ANY(s.iids)
         WHERE EXISTS (SELECT 1 FROM def14a_beneficial_holdings h WHERE h.instrument_id = ANY(s.iids))
         ORDER BY s.cik, i.symbol
        """
    ).fetchall()
    for cik, iid, symbol in rows:
        issuers.setdefault(cik, Issuer(cik=cik)).siblings.append((int(iid), str(symbol)))
    for issuer in issuers.values():
        iids = [iid for iid, _ in issuer.siblings]
        issuer.proxies = [
            (acc, filed)
            for acc, filed in conn.execute(
                """
                SELECT DISTINCT h.accession_number,
                       (SELECT min(f.filing_date) FROM filing_events f
                         WHERE f.provider = 'sec' AND f.provider_filing_id = h.accession_number)
                  FROM def14a_beneficial_holdings h
                 WHERE h.instrument_id = ANY(%(iids)s)
                 ORDER BY 1
                """,
                {"iids": iids},
            ).fetchall()
        ]
        seen: set[str] = set()
        for acc, form, filed, url in conn.execute(
            """
            SELECT provider_filing_id, filing_type, filing_date, primary_document_url
              FROM filing_events
             WHERE provider = 'sec' AND instrument_id = ANY(%(iids)s)
               AND filing_type = ANY(%(forms)s) AND primary_document_url IS NOT NULL
             ORDER BY filing_date DESC, provider_filing_id DESC
            """,
            {"iids": iids, "forms": list(COVER_FORMS)},
        ).fetchall():
            if acc in seen:
                continue
            seen.add(acc)
            issuer.covers.append(Cover(accession=acc, form=form, filing_date=filed, url=url))
    return issuers


def _instance_url(primary_document_url: str) -> str | None:
    # An inline-XBRL filing's extracted instance sits beside the primary
    # document as ``<stem>_htm.xml``. Pre-iXBRL filings have no cover dei tags.
    if not primary_document_url.lower().endswith(".htm"):
        return None
    return primary_document_url[:-4] + "_htm.xml"


class InstanceCache:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.client = httpx.Client(
            timeout=60.0,
            headers={"User-Agent": settings.sec_user_agent, "Accept-Encoding": "gzip, deflate"},
        )
        self._last = 0.0
        self.fetch_status: Counter[str] = Counter()

    def contexts(self, cover: Cover) -> list[dict[str, Any]] | str:
        """Parsed same-context triples, or a status string when unavailable."""
        path = self.root / "instances" / f"{cover.accession}.xml.gz"
        miss = self.root / "instances" / f"{cover.accession}.miss"
        if miss.is_file():
            return miss.read_text().strip()
        if not path.is_file():
            url = _instance_url(cover.url)
            if url is None:
                status = "no_ixbrl_instance"
            else:
                wait = REQUEST_INTERVAL_S - (time.monotonic() - self._last)
                if wait > 0:
                    time.sleep(wait)
                self._last = time.monotonic()
                try:
                    response = self.client.get(url)
                except httpx.HTTPError:
                    self.fetch_status["transport_error"] += 1
                    return "transport_error"  # not cached: unknown is not absent
                self.fetch_status[f"http_{response.status_code}"] += 1
                if response.status_code == 200 and response.content.lstrip().startswith(b"<"):
                    _write_gzip_atomic(path, response.content)
                    status = ""
                elif response.status_code in (403, 429) or response.status_code >= 500:
                    return f"http_{response.status_code}"  # not cached: retryable
                else:
                    status = f"http_{response.status_code}"
            if status:
                miss.parent.mkdir(parents=True, exist_ok=True)
                miss.write_text(status)
                return status
        try:
            return parse_cover_contexts(path)
        except ET.XMLSyntaxError, OSError, EOFError:
            return "parse_error"


def triples(contexts: list[dict[str, Any]]) -> list[tuple[str, str]]:
    """Distinct singleton (title, symbol) pairs from same-context cover facts."""
    out: set[tuple[str, str]] = set()
    for ctx in contexts:
        facts = ctx["facts"]
        titles, symbols = facts["Security12bTitle"], facts["TradingSymbol"]
        if len(titles) == 1 and len(symbols) == 1:
            out.add((titles[0].strip(), symbols[0].strip().upper()))
    return sorted(out)


def capture_tables_with_rows(html: str) -> list[tuple[list[str], int]]:
    """Per selected table: its text cells and how many holder rows it contributed."""
    captured: list[tuple[list[str], int]] = []
    original = def14a._extract_table_holders

    def hook(table: Any, **kwargs: Any) -> Any:
        if kwargs.get("rows") is None:
            # Eligibility probes call the same function without an output list;
            # only the final concatenation loop's calls are SELECTED tables.
            return original(table, **kwargs)
        cells = list(table.score_headers) + list(table.column_headers)
        for row in table.rows:
            cells.extend(row)
        before = len(kwargs["rows"])
        result = original(table, **kwargs)
        captured.append(([c for c in cells if c and c.strip()], len(kwargs["rows"]) - before))
        return result

    def14a._extract_table_holders = hook  # type: ignore[assignment]
    try:
        def14a.parse_beneficial_ownership_table(html)
    finally:
        def14a._extract_table_holders = original  # type: ignore[assignment]
    return captured


def has_class_dimension(cells: list[str]) -> bool:
    return any(_CLASS_WORD.search(_PERCENT_OF_CLASS.sub(" ", cell)) for cell in cells)


def is_common(title: str) -> bool:
    return bool(_COMMON.search(title)) and not _NON_COMMON.search(title)


def evaluate(issuer: Issuer, conn: psycopg.Connection[Any], cache: InstanceCache) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for accession, filed in issuer.proxies:
        record: dict[str, Any] = {"cik": issuer.cik, "proxy": accession, "proxy_filed": str(filed)}
        records.append(record)
        if filed is None:
            record["cover_status"] = "proxy_date_unknown"
            continue
        # Point-in-time: covers filed on or before the proxy date only.
        eligible = [c for c in issuer.covers if c.filing_date <= filed]
        record["cover_status"] = "no_cover_on_or_before_proxy" if not eligible else None
        cover_pairs: list[tuple[str, str]] = []
        first_status: str | None = None
        for depth, cover in enumerate(eligible[:MAX_COVER_DEPTH]):
            parsed = cache.contexts(cover)
            status = parsed if isinstance(parsed, str) else ("no_12b_triples" if not triples(parsed) else "ok")
            if depth == 0:
                first_status = status
            if status == "ok":
                assert not isinstance(parsed, str)
                cover_pairs = triples(parsed)
                record.update(
                    cover_status="ok",
                    cover_accession=cover.accession,
                    cover_form=cover.form,
                    cover_filed=str(cover.filing_date),
                    cover_depth=depth,
                    cover_age_days=(filed - cover.filing_date).days,
                )
                break
            if status in ("transport_error", "parse_error", "http_403", "http_429") or status.startswith("http_5"):
                break  # unknown is not absent: never walk past it to a staler cover
        if record["cover_status"] is None:
            record["cover_status"] = f"first_cover_{first_status}"
        record["cover_pairs"] = cover_pairs

        payload_row = conn.execute(
            "SELECT payload FROM filing_raw_documents WHERE accession_number = %s AND document_kind = 'def14a_body'",
            (accession,),
        ).fetchone()
        tables_with_rows = capture_tables_with_rows(payload_row[0]) if payload_row and payload_row[0] else []
        tables = [table for table, _ in tables_with_rows]
        cells = [cell for table in tables for cell in table]
        record["tables_selected"] = len(tables)
        record["class_dimension"] = has_class_dimension(cells)
        tokens = {tok for cell in cells for tok in re.split(r"[^A-Z0-9.\-/]+", cell.upper()) if tok}
        norm_tokens = {norm_symbol(tok) for tok in tokens}
        text_norm = " | ".join(norm_text(cell) for cell in cells)

        by_norm: dict[str, list[tuple[str, str]]] = defaultdict(list)
        for title, symbol in cover_pairs:
            by_norm[norm_symbol(symbol)].append((title, symbol))
        sibling_keys = Counter(norm_symbol(sym) for _, sym in issuer.siblings)
        common_siblings = [
            sym
            for _, sym in issuer.siblings
            if len(by_norm.get(norm_symbol(sym), [])) == 1 and is_common(by_norm[norm_symbol(sym)][0][0])
        ]
        outcomes: dict[str, dict[str, Any]] = {}
        for _iid, symbol in issuer.siblings:
            key = norm_symbol(symbol)
            listed = by_norm.get(key, [])
            outcome: dict[str, Any] = {
                "cover_listed": len(listed) == 1,
                "cover_title": listed[0][0] if listed else None,
            }
            if sibling_keys[key] > 1:
                outcome["rule"] = "ambiguous_sibling_symbol"  # two instruments reduce to one ticker
            elif len(listed) != 1:
                outcome["rule"] = "not_on_cover" if not listed else "ambiguous_cover_symbol"
            elif key in norm_tokens:
                outcome["rule"] = "ticker"
            elif norm_text(listed[0][0]) and norm_text(listed[0][0]) in text_norm:
                outcome["rule"] = "title"
            elif not record["class_dimension"] and common_siblings == [symbol]:
                outcome["rule"] = "sole_common"
            else:
                outcome["rule"] = "unbound"
            outcomes[symbol] = outcome
        record["siblings"] = outcomes
        record["recipients"] = recipient_decisions(
            issuer,
            by_norm,
            sibling_keys,
            tables_with_rows,
        )
    return records


_PAR_CLAUSE = re.compile(r"[,(]?\s*(\$\s*[\d.]+\s*)?\bpar(\s+value)?\b.*$", re.IGNORECASE)


def title_key(title: str) -> str:
    """A cover title minus its par-value clause, normalised for phrase search."""
    return norm_text(_PAR_CLAUSE.sub("", title))


def bind_table(cells: list[str], keys: set[str]) -> str | None:
    """The ONE cover class a selected table reports, or None.

    Every cell is searched for cover title keys as whole-word phrases; a key
    contained in a longer matched key is dropped (``common stock`` inside
    ``class a common stock``), so the maximal match wins. The table binds only
    when exactly one maximal key appears across all its cells AND no cell,
    once its matched key and the Percent-of-Class caption are removed, still
    names a class/series/preferred/warrant — that residual is a class the cover
    does not register (ATRO's ``Class B Stock``) or a second class, so the
    rows cannot be attributed.
    """
    found: set[str] = set()
    for cell in cells:
        text = norm_text(_PERCENT_OF_CLASS.sub(" ", cell))
        padded = f" {text} "
        matched = {k for k in keys if k and f" {k} " in padded}
        maximal = {k for k in matched if not any(k != o and f" {k} " in f" {o} " for o in matched)}
        residual = padded
        for k in maximal:
            residual = residual.replace(f" {k} ", " ")
        if _CLASS_WORD.search(residual):
            return None
        found |= maximal
    return next(iter(found)) if len(found) == 1 else None


def recipient_decisions(
    issuer: Issuer,
    by_norm: dict[str, list[tuple[str, str]]],
    sibling_keys: Counter[str],
    tables: list[tuple[list[str], int]],
) -> dict[str, dict[str, Any]]:
    """The slice-2 recipient rule (docs/proposals/ownership/2026-09-24-2351-*.md).

    Today every sibling receives every row. The rule never ADDS a row; it
    withholds only where the point-in-time cover says the sibling is a
    non-common security or is not a registered class of this issuer. Dual-class
    common issuers stay on today's fan-out (``legacy_multiclass``) — the
    table-level binder is reported alongside (``table_bound_rows``) because it
    is what showed that slice cannot bind them.
    """
    listed = {key: pairs[0][0] for key, pairs in by_norm.items() if len(pairs) == 1}
    total = sum(n for _, n in tables)
    receiving_common = {
        title_key(listed[norm_symbol(sym)])
        for _, sym in issuer.siblings
        if norm_symbol(sym) in listed and is_common(listed[norm_symbol(sym)])
    }
    no_sibling_matched = not any(norm_symbol(sym) in listed for _, sym in issuer.siblings)
    out: dict[str, dict[str, Any]] = {}
    for _iid, symbol in issuer.siblings:
        key = norm_symbol(symbol)
        title = listed.get(key)
        entry: dict[str, Any] = {"rows_today": total}
        if len(issuer.siblings) == 1:
            entry["decision"] = "single_sibling"
        elif not by_norm:
            entry["decision"] = "legacy_no_cover"
        elif len(by_norm.get(key, [])) > 1:
            entry["decision"] = "legacy_ambiguous_cover"  # one symbol, several titles
        elif title is None:
            entry["decision"] = "withhold_not_on_cover"
            entry["no_sibling_matched"] = no_sibling_matched
        elif not is_common(title):
            # An ADS-only cover would otherwise withhold from every sibling.
            entry["decision"] = "withhold_non_common" if receiving_common else "legacy_no_common_on_cover"
        elif len(receiving_common) >= 2:
            entry["decision"] = "legacy_multiclass"
            entry["table_bound_rows"] = sum(
                n for cells, n in tables if bind_table(cells, receiving_common) == title_key(title)
            )
        elif sibling_keys[key] > 1:
            entry["decision"] = "legacy_duplicate"
        else:
            entry["decision"] = "receive"
        entry["rows_kept"] = 0 if entry["decision"].startswith("withhold") else total
        out[symbol] = entry
    return out


def summarise(records: list[dict[str, Any]]) -> dict[str, Any]:
    cover = Counter(r["cover_status"] for r in records)
    depth = Counter(r.get("cover_depth") for r in records if r["cover_status"] == "ok")
    rules = Counter(o["rule"] for r in records for o in r.get("siblings", {}).values())
    ok_ciks = {r["cik"] for r in records if r["cover_status"] == "ok"}
    # A proxy is fully resolvable when every sibling binds or is provably not a
    # listed class of its own (not_on_cover is still a write-nothing outcome).
    per_proxy = Counter(
        "all_bound"
        if all(o["rule"] in ("ticker", "title", "sole_common") for o in r["siblings"].values())
        else "some_bound"
        if any(o["rule"] in ("ticker", "title", "sole_common") for o in r["siblings"].values())
        else "none_bound"
        for r in records
        if r.get("siblings")
    )
    decisions = Counter(d["decision"] for r in records for d in r.get("recipients", {}).values())
    rows_today = sum(d["rows_today"] for r in records for d in r.get("recipients", {}).values())
    rows_kept = sum(d["rows_kept"] for r in records for d in r.get("recipients", {}).values())
    multiclass = [d for r in records for d in r.get("recipients", {}).values() if d["decision"] == "legacy_multiclass"]
    return {
        "recipient_decision": dict(decisions.most_common()),
        "recipient_rows_today": rows_today,
        "recipient_rows_kept": rows_kept,
        "multiclass_decisions_table_bound": sum(1 for d in multiclass if d["table_bound_rows"]),
        "multiclass_decisions": len(multiclass),
        "proxies": len(records),
        "ciks": len({r["cik"] for r in records}),
        "ciks_with_ok_cover_on_some_proxy": len(ok_ciks),
        "cover_status": dict(cover.most_common()),
        "cover_depth_when_ok": {str(k): v for k, v in sorted(depth.items())},
        "sibling_rule": dict(rules.most_common()),
        "proxy_binding": dict(per_proxy.most_common()),
        "class_dimension_proxies": sum(1 for r in records if r.get("class_dimension")),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--cache", type=Path, default=Path("var/census_2351"))
    parser.add_argument("--out", type=Path, default=None, help="write per-proxy records + summary as JSON")
    args = parser.parse_args()

    cache = InstanceCache(args.cache)
    records: list[dict[str, Any]] = []
    with psycopg.connect(settings.database_url) as conn:
        issuers = load_population(conn)
        for n, issuer in enumerate(sorted(issuers.values(), key=lambda i: i.cik), 1):
            records.extend(evaluate(issuer, conn, cache))
            print(f"[{n}/{len(issuers)}] {issuer.cik} {','.join(s for _, s in issuer.siblings)}", flush=True)
    summary = summarise(records)
    summary["fetch_status"] = dict(cache.fetch_status)
    print(json.dumps(summary, indent=2))
    if args.out is not None:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps({"summary": summary, "records": records}, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
