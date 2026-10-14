"""#3616 slice 3d-3c-2 fact-finding: check a draft fund determination against its documents and the stored evidence.

Read-only. Two parts:

1. **Quotes.** Loads the draft determination (JSON, spec Design 1) and, for each cited document, the copy saved under
   ``--docs`` by URL (``index.txt`` maps path to URL). Each document is reduced to text by ``document_text`` (HTML
   tags dropped, ``script``/``style`` content dropped, entities unescaped, white space collapsed to one space), and
   each quote, with its white space collapsed the same way, must occur in it. Prints every quote with ``ok`` or
   ``MISSING`` and each document's SHA-256 and length. Nothing is fetched.
2. **The gate on the dev DB** (spec Design 3), in one REPEATABLE READ snapshot: the newest ``tax_fund_observations``
   row of each fund security; for a ``candidate``, whether it carries the determination's binding; the governing
   filings of the CIK in ``filing_events`` since the newest reviewed one (staleness); the content item's
   ``public_updated_at``; the security's lots in the observation's evidence run; and, for each disposal date, the
   application deadline, the bar and the reporting status the spec's rule gives.

The printout carries dates, symbols, CIKs, LEIs, ISINs and accession numbers, never an account identifier.

    uv run python -m scripts.measure_3616_slice3d3c2_determination \\
        --determination docs/specs/metrics/2026-10-14-3616-slice3d3c2-spy-determination-v1.json \\
        --docs var/research/3616/slice3d3c2/spy
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import date, datetime, timedelta
from html import unescape
from html.parser import HTMLParser
from pathlib import Path

import psycopg

from app.config import settings

#: Forms whose filing can change or restate a registered UIT's governing terms (spec Design 2, staleness).
GOVERNING_FORMS = (
    "485APOS",
    "485BPOS",
    "485BXT",
    "497",
    "497J",
    "497K",
    "N-CEN",
    "N-CEN/A",
    "N-8F",
    "N-8F/A",
)
#: reg 55(1), (4)(b) and (5): three 28-day periods, the response path without an agreed extension.
RESPONSE_DAYS = 84


class _Text(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self._skip = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in ("script", "style"):
            self._skip += 1
        self.parts.append(" ")

    def handle_endtag(self, tag: str) -> None:
        if tag in ("script", "style") and self._skip:
            self._skip -= 1
        self.parts.append(" ")

    def handle_data(self, data: str) -> None:
        if not self._skip:
            self.parts.append(data)


def collapse(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def document_text(body: bytes) -> str:
    parser = _Text()
    parser.feed(body.decode("utf-8", errors="strict"))
    parser.close()
    return collapse(unescape("".join(parser.parts)))


def _local_paths(docs: Path) -> dict[str, Path]:
    out: dict[str, Path] = {}
    for line in (docs / "index.txt").read_text().splitlines():
        cols = [c.strip() for c in line.split("|")]
        if len(cols) >= 2 and cols[1].startswith("https://") and not cols[0].endswith(".txt"):
            out[cols[1]] = docs / cols[0]
    return out


def check_quotes(det: dict, docs: Path) -> int:
    paths = _local_paths(docs)
    texts: dict[str, str] = {}
    for d in det["documents"]:
        path = paths.get(d["url"])
        if path is None:
            print(f"document {d['doc_id']}: no local copy for {d['url']}")
            continue
        body = path.read_bytes()
        texts[d["doc_id"]] = document_text(body)
        print(
            f"document {d['doc_id']} {d['accession']} {d['form']}: sha256 {hashlib.sha256(body).hexdigest()} "
            f"bytes {len(body)} text {len(texts[d['doc_id']])}"
        )
    missing = 0
    for f in det["findings"]:
        print(f"finding {f['question']} = {f['answer']}")
        for q in f["quotes"]:
            text = texts.get(q["doc_id"])
            ok = text is not None and collapse(q["text"]) in text
            missing += not ok
            print(f"  {'ok' if ok else 'MISSING'} [{q['doc_id']}] {q['text'][:90]}")
    print(f"quotes missing: {missing}")
    return missing


def add_months(d: date, months: int) -> date:
    y, m = divmod(d.month - 1 + months, 12)
    year, month = d.year + y, m + 1
    for day in (d.day, 30, 29, 28):
        try:
            return date(year, month, day)
        except ValueError:
            continue
    raise ValueError(d)


def period_end(d: date, month_day: str) -> date:
    month, day = (int(x) for x in month_day.split("-"))
    end = date(d.year, month, day)
    return end if end >= d else date(d.year + 1, month, day)


def application_deadline(d: date, month_day: str, first_acquisition: date) -> date:
    """reg 54(2): the later of the end of the period of account containing ``d`` and the expiry of three months
    beginning with the first day interests were available to UK residents, bounded above by ``first_acquisition``."""
    three_months_end = add_months(first_acquisition, 3) - timedelta(days=1)
    return max(period_end(d, month_day), three_months_end)


def reporting_status(
    search_outcome: str, published: datetime | None, d: date, month_day: str, first_acquisition: date
) -> tuple[str, date, date]:
    deadline = application_deadline(d, month_day, first_acquisition)
    bar = deadline + timedelta(days=RESPONSE_DAYS)
    if search_outcome != "not_found" or published is None:
        return "reporting_status_unestablished", deadline, bar
    if published.date() > bar:
        return "non_reporting", deadline, bar
    return "non_reporting_provisional", deadline, bar


def gate(det: dict) -> None:
    b = det["binding"]
    month_day = next(f["answer"] for f in det["findings"] if f["question"] == "period_of_account_end")
    with psycopg.connect(settings.database_url) as conn:
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        with conn.transaction():
            rows = conn.execute(
                """
                SELECT DISTINCT ON (tax_security_id) observation_id, tax_security_id, member_ids, symbol, outcome,
                       cik, lei, isin, mic, search_outcome, content_fetch_id, evidence_run_id, observed_at
                FROM tax_fund_observations
                ORDER BY tax_security_id, observed_at DESC, observation_id DESC
                """
            ).fetchall()
            for oid, sid, members, symbol, outcome, cik, lei, isin, mic, search, content_id, run, at in rows:
                print(f"\nsecurity {sid} {symbol} members {members}: observation {oid} at {at:%Y-%m-%dT%H:%M:%SZ}")
                print(f"  outcome {outcome}")
                if outcome != "candidate":
                    print("  no determination applies: not a candidate")
                    continue
                bound = (cik, lei, isin, mic, symbol) == (b["cik"], b["lei"], b["isin"], b["mic"], b["ticker"])
                print(
                    f"  candidate cik {cik} lei {lei} isin {isin} mic {mic}; binding matches: {bound}; search {search}"
                )
                newer = conn.execute(
                    """
                    SELECT filing_type, filing_date, provider_filing_id FROM filing_events
                    WHERE provider = 'sec' AND instrument_id = ANY(%s) AND filing_type = ANY(%s)
                      AND filing_date >= %s
                    ORDER BY filing_date, provider_filing_id
                    """,
                    (list(members), list(GOVERNING_FORMS), date.fromisoformat("2026-01-26")),
                ).fetchall()
                unreviewed = [r for r in newer if r[2] not in det["reviewed_filings"]]
                print(f"  governing filings since 2026-01-26: {[(t, str(fd), a) for t, fd, a in newer]}")
                print(f"  unreviewed (stale if any): {[(t, str(fd), a) for t, fd, a in unreviewed]}")
                published = None
                if content_id is not None:
                    body = conn.execute(
                        """
                        SELECT d.body FROM tax_fund_fetches f JOIN tax_fund_documents d USING (body_sha256)
                        WHERE f.fetch_id = %s AND f.source = 'hmrc_content_item'
                        """,
                        (content_id,),
                    ).fetchone()
                    if body is not None:
                        published = datetime.fromisoformat(json.loads(bytes(body[0]))["public_updated_at"])
                print(f"  content fetch {content_id} public_updated_at {published}")
                lots = conn.execute(
                    """
                    SELECT direction, uk_date, units FROM tax_evidence_lots
                    WHERE run_id = %s AND instrument_id = ANY(%s) ORDER BY uk_date, lot_key
                    """,
                    (run, list(members)),
                ).fetchall()
                acquisitions = [u for dirn, u, _ in lots if dirn == "acquisition"]
                disposals = sorted({u for dirn, u, _ in lots if dirn == "disposal"})
                print(
                    f"  lots in run {run}: {len(lots)}; first acquisition {min(acquisitions) if acquisitions else None}"
                )
                for d in disposals:
                    if not acquisitions:
                        break
                    in_range = d >= date.fromisoformat(det["valid_from"])
                    status, deadline, bar = reporting_status(search or "", published, d, month_day, min(acquisitions))
                    print(
                        f"  disposal {d}: within valid_from {in_range}; period end {period_end(d, month_day)}; "
                        f"application deadline {deadline}; bar {bar}; status {status}"
                    )


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--determination", type=Path, required=True)
    p.add_argument("--docs", type=Path, required=True)
    a = p.parse_args()
    det = json.loads(a.determination.read_text())
    print(f"determination {det['determination_id']} policy {det['policy_id']} binding {det['binding']}")
    check_quotes(det, a.docs)
    gate(det)


if __name__ == "__main__":
    main()
