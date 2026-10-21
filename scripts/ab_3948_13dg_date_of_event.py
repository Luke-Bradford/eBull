"""Full-population A/B for #3948: the 13D/G cover's event date parsed from ``MM/DD/YYYY`` (read-only).

Re-parses EVERY stored ``primary_doc_13dg`` body through the real edgartools ``parse_xml`` and the real adapter,
inside one REPEATABLE READ snapshot, and predicts what a ``sec_rebuild`` re-drain will write: ``_upsert_filing_row``
fills a NULL ``date_of_event`` on a conflicting key and inserts a row for a key it does not find.

Arms per reporting person: base = the pre-#3948 adapter (``date.fromisoformat``, else NULL); new = the real adapter.
Cross-implementation check: the date text read straight from the body's XML tag, parsed independently here.

Gates (non-zero exit on any failure):
  * errors = 0 (body read, parse, adapter);
  * every non-blank tag value parses, or is listed as a reject with its text (reported; gated at 0);
  * new date = the independent parse of the body's tag on every parsed person;
  * base never differs from stored (the stored column is the base arm: NULL on every row);
  * every parsed person's key matches a stored row (a miss is a row the re-drain would INSERT).

Reported, not gated: stored rows the re-parse fills (by distinct key) and leaves NULL, and every date before
1990-01-01 or after the filing's ``filed_at`` date (UTC) + 1 day. The date is the filer's own statement, stored as
filed (2026-10-21: one, ``0002086358-25-000001``, a 13G stating ``09/22/2025`` filed 2025-09-19).

Usage: ``PYTHONPATH=. uv run python -m scripts.ab_3948_13dg_date_of_event [--limit N] [--out PATH]``
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from datetime import UTC, date, datetime, timedelta
from typing import Any, Literal

import psycopg
from edgar.beneficial_ownership.schedule13 import Schedule13D, Schedule13G

from app.config import settings
from app.db.snapshot import snapshot_read
from app.services.manifest_parsers._schedule13_adapter import build_filing_from_edgartools_dict
from app.services.raw_filings import stored_body

# Namespace-prefixed or not (``<ns2:eventDateRequiresFilingThisStatement>`` occurs).
_TAG = re.compile(r"<(?:\w+:)?(dateOfEvent|eventDateRequiresFilingThisStatement)>\s*([^<]*?)\s*<", re.S)
_FORMS: dict[str, Literal["sec_13d", "sec_13g"]] = {
    "SCHEDULE 13D": "sec_13d",
    "SCHEDULE 13D/A": "sec_13d",
    "SCHEDULE 13G": "sec_13g",
    "SCHEDULE 13G/A": "sec_13g",
}


def _independent(text: str) -> date | None:
    m = re.fullmatch(r"(\d{1,2})/(\d{1,2})/(\d{4})", text) or re.fullmatch(r"(\d{1,2})-(\d{1,2})-(\d{4})", text)
    try:
        if m:
            return date(int(m[3]), int(m[1]), int(m[2]))
        m = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", text)
        return date(int(m[1]), int(m[2]), int(m[3])) if m else None
    except ValueError:
        return None


def _base(value: Any) -> date | None:
    if not value or not isinstance(value, str):
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--out", default="/tmp/ab_3948.json")
    args = ap.parse_args()
    st: Counter[str] = Counter()
    fmt: Counter[str] = Counter()
    fails: list[str] = []
    rejects: list[tuple[str, str]] = []
    odd: list[tuple[str, str, str]] = []
    filled: set[tuple[str, str, str]] = set()
    with psycopg.connect(settings.database_url) as conn, snapshot_read(conn):
        stored: dict[str, dict[tuple[str, str], date | None]] = {}
        meta: dict[str, tuple[str, datetime | None]] = {}
        for acc, cik, name, form, doe, filed in conn.execute(
            """SELECT accession_number, COALESCE(reporter_cik, ''), reporter_name, submission_type, date_of_event,
                      filed_at FROM blockholder_filings"""
        ):
            stored.setdefault(acc, {})[(cik, name)] = doe
            meta[acc] = (form, filed)
            st["stored rows"] += 1
            st["stored rows with a date"] += doe is not None
        accs = [
            a
            for (a,) in conn.execute(
                "SELECT accession_number FROM filing_raw_documents WHERE document_kind = 'primary_doc_13dg' ORDER BY 1"
            )
        ]
        if args.limit:
            accs = accs[: args.limit]
        st["bodies"] = len(accs)
        for n, acc in enumerate(accs):
            if n % 5000 == 0:
                print("progress", n, len(accs), flush=True)
            if acc not in meta:
                st["body with no typed row"] += 1
                continue
            form, filed = meta[acc]
            source = _FORMS[form]
            try:
                body = stored_body(conn, accession_number=acc, document_kind="primary_doc_13dg")
                if body is None:
                    raise ValueError("no stored body")
                parsed = (Schedule13D if source == "sec_13d" else Schedule13G).parse_xml(body)
                filing = build_filing_from_edgartools_dict(
                    parsed, source=source, manifest_form=form, manifest_filer_cik="1", raw_xml=body
                )
            except Exception as exc:  # noqa: BLE001 — counted and gated at 0
                st["error"] += 1
                fails.append(f"{acc}: {exc!r}")
                continue
            tags = _TAG.findall(body)
            text = tags[0][1] if tags else ""
            shape = re.sub(r"\d", "9", text) if text else "(absent or blank)"
            fmt[f"{source} {shape}"] += 1
            want = _independent(text) if text else None
            if text and want is None:
                rejects.append((acc, text))
            raw = parsed.get("date_of_event" if source == "sec_13d" else "event_date")
            new, base = filing.date_of_event, _base(raw)
            if new != want:
                fails.append(f"{acc}: adapter {new} != independent {want} (tag {text!r})")
            if new is not None:
                filed_day = filed.astimezone(UTC).date() if filed else None
                if new < date(1990, 1, 1) or (filed_day is not None and new > filed_day + timedelta(days=1)):
                    odd.append((acc, str(new), str(filed_day)))
            for p in filing.reporting_persons:
                st["parsed persons"] += 1
                key = (p.cik or "", p.name)
                if key not in stored[acc]:
                    st["parsed person with no stored key (re-drain would INSERT)"] += 1
                    if st["parsed person with no stored key (re-drain would INSERT)"] <= 20:
                        print("NOKEY", acc, key, sorted(stored[acc])[:3])
                    continue
                cur = stored[acc][key]
                if cur != base and cur is None:
                    fails.append(f"{acc}: base arm {base} but stored NULL")
                if cur is None and new is not None:
                    filled.add((acc, *key))
                    st[f"persons with a date {source}"] += 1
                elif cur is not None and cur != new:
                    st["stored date kept, differs from re-parse"] += 1
    if st["error"]:
        fails.append(f"errors = {st['error']}")
    if rejects:
        fails.append(f"rejects = {len(rejects)}")
    st["stored rows filled (distinct key)"] = len(filled)
    st["stored rows left NULL"] = st["stored rows"] - st["stored rows with a date"] - len(filled)
    if st["parsed person with no stored key (re-drain would INSERT)"]:
        fails.append("parsed persons with no stored key")
    with open(args.out, "w") as fh:
        json.dump({"stats": st, "formats": fmt, "rejects": rejects, "odd": odd, "failures": fails}, fh, indent=1)
    for k, v in sorted(st.items()):
        print(f"{k}: {v}")
    for k, v in fmt.most_common():
        print(f"format {k}: {v}")
    for r in rejects[:20]:
        print("REJECT", r)
    for o in odd[:30]:
        print("ODD", o)
    for f in fails[:30]:
        print("FAIL", f)
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
