"""#3361 premise measurement: vendor price-series symbols vs dated Form 3/4/5 issuer symbols.

Read-only. For every ``icyDenev/Intrader`` series in ``research_price_series`` it counts the
distinct issuer CIKs whose Form 3/4/5 filings (SEC Insider Transactions Data Sets,
``SUBMISSION.tsv``: ``ISSUERCIK``, ``ISSUERTRADINGSYMBOL``, ``FILING_DATE``) name the series'
symbol inside the series' own bar range, and whether several CIKs' spans overlap or succeed
one another. Symbols compare after unifying the separators ``.`` ``-`` ``/`` ``_``.

Usage::

    PYTHONPATH=. uv run python scripts/measure_3361_symbol_evidence.py [--bulk-dir DIR]
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import re
import sys
import zipfile
from collections import Counter, defaultdict
from datetime import date, datetime
from pathlib import Path

VENDOR = "icyDenev/Intrader"
DEFAULT_BULK = Path.home() / "Library" / "Application Support" / "eBull" / "sec" / "bulk"
_SEPARATORS = re.compile(r"[.\-/_ ]")
#: Intrader symbol grammar: a trailing ``_<CLASS>`` group (``_WS``, ``_U``, ``_P_B``, ``_B``).
_SUFFIX = re.compile(r"_([A-Z]+)(?:_[A-Z0-9]+)*$")
_EMPTY = frozenset({"", "NONE", "N/A", "NA"})


def norm(symbol: str) -> str:
    return _SEPARATORS.sub(".", symbol.strip().upper())


def suffix_class(vendor_symbol: str) -> str:
    match = _SUFFIX.search(vendor_symbol.upper())
    return match.group(1) if match else "plain"


def load_evidence(bulk: Path) -> tuple[dict[str, list[tuple[date, str]]], dict[str, object]]:
    evidence: dict[str, list[tuple[date, str]]] = defaultdict(list)
    rows = unparsed = 0
    quarters = sorted(bulk.glob("insider_*.zip"))
    for path in quarters:
        with zipfile.ZipFile(path) as archive, archive.open("SUBMISSION.tsv") as handle:
            reader = csv.DictReader(
                io.TextIOWrapper(handle, "utf-8", errors="replace"), delimiter="\t", quoting=csv.QUOTE_NONE
            )
            for row in reader:
                rows += 1
                symbol = norm(row["ISSUERTRADINGSYMBOL"] or "")
                try:
                    filed = datetime.strptime(row["FILING_DATE"], "%d-%b-%Y").date()
                except ValueError:
                    unparsed += 1
                    continue
                if symbol in _EMPTY:
                    continue
                evidence[symbol].append((filed, row["ISSUERCIK"].strip().zfill(10)))
    meta = {
        "quarters": [p.name for p in quarters[:1] + quarters[-1:]],
        "n_quarters": len(quarters),
        "submission_rows": rows,
        "unparsed_dates": unparsed,
        "symbols": len(evidence),
    }
    return evidence, meta


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--bulk-dir", type=Path, default=DEFAULT_BULK)
    args = parser.parse_args()

    import psycopg

    from app.config import settings

    evidence, meta = load_evidence(args.bulk_dir)
    with psycopg.connect(settings.database_url) as conn:
        series = conn.execute(
            "SELECT vendor_symbol, first_bar, last_bar FROM research_price_series WHERE vendor = %(v)s",
            {"v": VENDOR},
        ).fetchall()

    outcome: Counter[tuple[str, str]] = Counter()
    shape: Counter[str] = Counter()
    for vendor_symbol, first_bar, last_bar in series:
        cls = suffix_class(vendor_symbol)
        observations = evidence.get(norm(vendor_symbol), [])
        spans: dict[str, tuple[date, date]] = {}
        for filed, cik in observations:
            if first_bar <= filed <= last_bar:
                lo, hi = spans.get(cik, (filed, filed))
                spans[cik] = (min(lo, filed), max(hi, filed))
        if last_bar < date(2006, 1, 1):
            key = "ends_before_form345"
        elif not observations:
            key = "no_symbol_evidence"
        elif not spans:
            key = "no_evidence_in_range"
        else:
            key = "one_cik" if len(spans) == 1 else "several_ciks"
        outcome[(cls if cls == "plain" else "suffixed", key)] += 1
        if len(spans) > 1:
            ordered = sorted(spans.values())
            overlap = any(ordered[i + 1][0] <= ordered[i][1] for i in range(len(ordered) - 1))
            shape["overlapping" if overlap else "disjoint_succession"] += 1

    json.dump(
        {
            "form345": meta,
            "series": len(series),
            "suffix_classes": dict(Counter(suffix_class(s) for s, _, _ in series).most_common()),
            "outcome": {f"{c}/{k}": n for (c, k), n in sorted(outcome.items())},
            "several_cik_shape": dict(shape),
        },
        sys.stdout,
        indent=1,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
