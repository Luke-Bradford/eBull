"""#3227 — DERA ``NONDERIV_HOLDING.NATURE_OF_OWNERSHIP`` is positionally compacted.

    PYTHONPATH=. uv run python -m scripts.audit_3227_dera_nature_compaction
    PYTHONPATH=. uv run python -m scripts.audit_3227_dera_nature_compaction --edgar

Read-only. Nothing is written.

## The finding

When a Table I holding line has no ``<natureOfOwnership>`` element, DERA's holdings file does not
leave that line's nature blank: the NEXT line's nature moves up onto it, and so on, and the last
line goes blank. MSBI ``0001466026-26-000008`` on EDGAR reads ``D`` (none), ``I`` Red Bird
Investors, ``I`` Summit Investors, ``I`` J.M. Schultz Investment; DERA's TSV puts Red Bird on the
``D`` line and leaves the last ``I`` line blank. A line whose element is present but empty keeps
its position, so not every accession with a blank nature is shifted.

This is the 18.5% DERA-vs-XML nature discordance recorded in the sec-edgar skill.

## What it measures

Default: every live Form 3 ``:NDH:`` accession that our XML parse (``insider_initial_holdings``)
also holds, aligned line by line in ``NONDERIV_HOLDING_SK`` order against ``row_num`` order. Only
accessions whose title, D/I and shares agree at every position are compared (an alignment that
fails is counted, not guessed at). For each aligned accession whose natures disagree, it tests
whether DERA's list equals the XML list with its blanks moved to the end.

``--edgar``: the old #3227 3a gate required ``(title, D/I, nature)`` to be pairwise distinct. For
every multi-line ``_current`` ``:NDH:`` group whose answer to that test could change with where
the blanks really sit, it fetches the filing from EDGAR (≈2 requests per accession, well under
SEC's 10 req/s) and reports how often DERA's nature gave the wrong answer. Needs a real
``SEC_USER_AGENT``; the placeholder soft-404s document fetches.
"""

from __future__ import annotations

import argparse
import re
import time
import urllib.request
import xml.etree.ElementTree as ET  # noqa: S405 — SEC EDGAR primary documents, trusted.
from collections import Counter, defaultdict
from decimal import Decimal
from itertools import combinations
from typing import Any

import psycopg

from app.config import settings

_DERA_SQL = """
SELECT DISTINCT o.source_accession, split_part(o.source_document_id, ':NDH:', 2)::bigint AS sk,
       o.security_title, o.direct_indirect, o.nature_of_ownership, o.shares
  FROM ownership_insiders_observations o
 WHERE o.source = 'form3' AND o.source_document_id ~ ':NDH:[0-9]+$' AND o.known_to IS NULL
   AND o.source_accession IN (SELECT accession_number FROM insider_initial_holdings WHERE NOT is_derivative)
"""

_XML_SQL = """
SELECT DISTINCT ON (accession_number, row_num)
       accession_number, row_num, security_title, direct_indirect, nature_of_ownership, shares
  FROM insider_initial_holdings
 WHERE NOT is_derivative
 ORDER BY accession_number, row_num
"""

# Multi-line single-owner ``:NDH:`` winner groups in ``_current``, lines in SK order.
_GROUPS_SQL = """
WITH w AS (SELECT * FROM ownership_insiders_current WHERE source_document_id ~ ':NDH:[0-9]+$')
SELECT w.source_accession, w.holder_cik,
       array_agg(o.security_title ORDER BY split_part(o.source_document_id, ':NDH:', 2)::bigint),
       array_agg(o.direct_indirect ORDER BY split_part(o.source_document_id, ':NDH:', 2)::bigint),
       array_agg(COALESCE(btrim(o.nature_of_ownership), '')
                 ORDER BY split_part(o.source_document_id, ':NDH:', 2)::bigint),
       array_agg(o.shares ORDER BY split_part(o.source_document_id, ':NDH:', 2)::bigint)
  FROM w JOIN ownership_insiders_observations o
    ON o.instrument_id = w.instrument_id AND o.holder_identity_key = w.holder_identity_key
   AND o.ownership_nature = w.ownership_nature AND o.source = w.source AND o.period_end = w.period_end
   AND o.known_to IS NULL AND o.source_document_id LIKE split_part(w.source_document_id, ':', 1) || ':NDH:%%'
 WHERE NOT EXISTS (
       SELECT 1 FROM ownership_insiders_observations x
        WHERE x.instrument_id = w.instrument_id AND x.known_to IS NULL
          AND x.source_document_id LIKE split_part(w.source_document_id, ':', 1) || ':NDH:%%'
          AND x.holder_identity_key <> w.holder_identity_key)
 GROUP BY w.instrument_id, w.holder_identity_key, w.ownership_nature, w.source,
          w.source_accession, w.holder_cik
HAVING count(*) > 1 AND count(DISTINCT o.security_title) = 1
"""


def _norm(s: str | None) -> str:
    return " ".join((s or "").split()).lower()


def compacted(xml_natures: list[str]) -> list[str]:
    """``xml_natures`` with every blank moved to the end, order otherwise kept — DERA's shape."""
    filled = [n for n in xml_natures if n]
    return filled + [""] * (len(xml_natures) - len(filled))


def _old_gate(titles: list[str], dis: list[str], natures: list[str]) -> bool:
    return len(set(zip(titles, dis, natures, strict=True))) == len(titles)


def census(conn: psycopg.Connection[Any]) -> Counter[str]:
    dera: dict[str, list[tuple[int, str, str, str, Decimal]]] = defaultdict(list)
    for acc, sk, title, di, nature, shares in conn.execute(_DERA_SQL):
        dera[acc].append((sk, title, di, _norm(nature), shares))
    xml: dict[str, list[tuple[str, str, str, Decimal]]] = defaultdict(list)
    for acc, _rn, title, di, nature, shares in conn.execute(_XML_SQL):
        xml[acc].append((title, di, _norm(nature), shares))
    st: Counter[str] = Counter()
    for acc, lines in dera.items():
        d = sorted(lines)
        x = xml.get(acc, [])
        if len(d) != len(x) or any((a[1], a[2], a[4]) != (b[0], b[1], b[3]) for a, b in zip(d, x, strict=True)):
            st["not aligned (skipped)"] += 1
            continue
        st["aligned"] += 1
        dn, xn = [a[3] for a in d], [b[2] for b in x]
        if dn == xn:
            st["  nature equal"] += 1
        elif dn == compacted(xn):
            st["  nature differs: EXACTLY compaction"] += 1
        else:
            st["  nature differs: other"] += 1
    return st


def _edgar_lines(ua: str, cik: str, accession: str) -> list[tuple[str, str, str, str]]:
    def get(url: str) -> bytes:
        time.sleep(0.25)
        req = urllib.request.Request(url, headers={"User-Agent": ua})
        with urllib.request.urlopen(req, timeout=30) as resp:  # noqa: S310 — fixed sec.gov host
            return resp.read()

    # EDGAR files an accession under every party's CIK folder, the reporting owner's included:
    # 74 of 74 sensitive accessions resolved this way on 2026-10-17. A failure is still counted
    # by the caller rather than aborting the run.
    folder = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession.replace('-', '')}/"
    index = get(folder).decode("utf-8", "replace")
    docs = [h for h in re.findall(r'href="([^"]+\.xml)"', index) if "index" not in h.lower()]
    root = ET.fromstring(get("https://www.sec.gov" + docs[0]))  # noqa: S314 — SEC primary document
    return [
        (
            (h.findtext("securityTitle/value") or "").strip(),
            h.findtext("ownershipNature/directOrIndirectOwnership/value") or "",
            _norm(h.findtext("ownershipNature/natureOfOwnership/value")),
            h.findtext("postTransactionAmounts/sharesOwnedFollowingTransaction/value") or "",
        )
        for h in root.iter("nonDerivativeHolding")
    ]


def edgar_check(conn: psycopg.Connection[Any]) -> Counter[str]:
    st: Counter[str] = Counter()
    for acc, cik, titles, dis, natures, shares in conn.execute(_GROUPS_SQL).fetchall():
        filled = [n for n in natures if n]
        if not filled or len(filled) == len(natures) or natures != compacted(natures):
            st["old gate cannot have been moved"] += 1
            continue
        answers = set()
        for pos in combinations(range(len(natures)), len(filled)):
            placed = [""] * len(natures)
            for p, n in zip(pos, filled, strict=True):
                placed[p] = n
            answers.add(_old_gate(titles, dis, placed))
        if len(answers) == 1:
            st["old gate same wherever the blanks sit"] += 1
            continue
        st["sensitive"] += 1
        try:
            truth = _edgar_lines(settings.sec_user_agent, cik, acc)
            # DERA rounds shares, so align on (title, D/I) and shares to 2 dp.
            aligned = len(truth) == len(titles) and all(
                (t[0], t[1]) == ((ti or "").strip(), di) and round(Decimal(t[3]), 2) == round(sh, 2)
                for t, ti, di, sh in zip(truth, titles, dis, shares, strict=True)
            )
        except (OSError, ValueError, TypeError, IndexError, ArithmeticError, ET.ParseError) as exc:
            st[f"  EDGAR fetch or parse failed ({type(exc).__name__})"] += 1
            continue
        if not aligned:
            st["  EDGAR does not align (skipped)"] += 1
            continue
        right = _old_gate(titles, dis, natures) == _old_gate(titles, dis, [t[2] for t in truth])
        st[f"  aligned: DERA nature gave the {'RIGHT' if right else 'WRONG'} answer"] += 1
    return st


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--edgar", action="store_true", help="also check the sensitive _current groups against EDGAR")
    args = ap.parse_args()
    with psycopg.connect(settings.database_url) as conn:
        conn.execute("SET statement_timeout = '900s'")
        print("Form 3 :NDH: accessions, DERA nature vs our XML parse")
        for k, v in sorted(census(conn).items()):
            print(f"  {k:<45} {v:>8,}")
        if args.edgar:
            print("\n_current multi-line :NDH: groups, the old nature test vs EDGAR")
            for k, v in sorted(edgar_check(conn).items()):
                print(f"  {k:<45} {v:>8,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
