"""#3619 slice 2b: load Form N-PORT Item B.5.a monthly total returns into ``sec_nport_monthly_returns``.

Every published quarterly N-PORT data set from 2019q4: ``MONTHLY_TOTAL_RETURN.tsv`` joined to ``SUBMISSION.tsv``,
read from the cached bulk ZIP when present, otherwise over HTTP range through the shared SEC rate gate (slice 1's
fetch, reused). Each source row becomes up to three rows, one per month position: MONTHLY_TOTAL_RETURN1..3 are the
"First/Second/Third Month total returns" of the three months ending at REPORT_DATE (Item B.5.a; data-set readme
§5.7). A blank cell stores no row. All classes are stored. Idempotent: rows are immutable filings, inserted
``ON CONFLICT DO NOTHING``, one transaction per quarter.

    PYTHONPATH=. uv run python -m scripts.load_3619_nport_returns
"""

from __future__ import annotations

import argparse
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path

import httpx
import psycopg

from app.config import settings
from scripts.report_3619_total_return_fidelity import _cached_tables, _nport_date, _quarters, _tsv, nport_months

SUB_TYPES = frozenset({"NPORT-P", "NPORT-P/A"})


@dataclass(frozen=True, slots=True)
class NportMonthRow:
    accession_number: str
    monthly_total_return_id: int
    month_position: int
    class_id: str | None
    month: date
    return_pct: Decimal
    report_date: date
    filing_date: date
    sub_type: str


def month_rows(returns: Iterable[dict[str, str]], submissions: dict[str, dict[str, str]]) -> Iterator[NportMonthRow]:
    """Normalise data-set rows to one row per month position, skipping blank cells.

    Raises on a return row whose accession has no N-PORT-P submission or no REPORT_DATE: the readme makes
    ACCESSION_NUMBER the join key, so a miss is a format change, not a row to drop quietly.
    """
    for r in returns:
        sub = submissions.get(r["ACCESSION_NUMBER"])
        if sub is None or sub["SUB_TYPE"] not in SUB_TYPES or not sub["REPORT_DATE"]:
            key = f"{r['ACCESSION_NUMBER']}/{r['MONTHLY_TOTAL_RETURN_ID']}"
            raise ValueError(f"return row {key} has no usable submission")
        report_date = _nport_date(sub["REPORT_DATE"])
        for position, (year, month) in enumerate(nport_months(report_date), start=1):
            raw = r[f"MONTHLY_TOTAL_RETURN{position}"].strip()
            if not raw:
                continue
            value = Decimal(raw)
            if not value.is_finite():
                raise ValueError(f"non-finite return {raw!r} in {r['ACCESSION_NUMBER']}")
            yield NportMonthRow(
                accession_number=r["ACCESSION_NUMBER"],
                monthly_total_return_id=int(r["MONTHLY_TOTAL_RETURN_ID"]),
                month_position=position,
                class_id=r["CLASS_ID"].strip() or None,
                month=date(year, month, 1),
                return_pct=value,
                report_date=report_date,
                filing_date=_nport_date(sub["FILING_DATE"]),
                sub_type=sub["SUB_TYPE"],
            )


_COLUMNS = (
    "accession_number, monthly_total_return_id, month_position, class_id, month, return_pct, report_date,"
    " filing_date, sub_type, dataset_quarter"
)


_CHANGED_SQL = """
SELECT count(*)
FROM nport_stage s
JOIN sec_nport_monthly_returns t USING (accession_number, monthly_total_return_id, month_position)
WHERE (s.class_id, s.month, s.return_pct, s.report_date, s.filing_date, s.sub_type)
      IS DISTINCT FROM (t.class_id, t.month, t.return_pct, t.report_date, t.filing_date, t.sub_type)
"""


#: Stored rows of this quarter that the new read no longer carries: a re-issue that REMOVED a row.
_REMOVED_SQL = """
SELECT count(*)
FROM sec_nport_monthly_returns t
WHERE t.dataset_quarter = %(quarter)s
  AND NOT EXISTS (
      SELECT 1 FROM nport_stage s
      WHERE (s.accession_number, s.monthly_total_return_id, s.month_position)
          = (t.accession_number, t.monthly_total_return_id, t.month_position)
  )
"""


def load_quarter(conn: psycopg.Connection, quarter: str, folder: Path) -> tuple[int, int]:
    """Insert one quarter in one transaction. Returns (rows offered, rows inserted).

    Rows already stored are kept, but a stored row whose values differ from this read, or that this read no longer
    carries (a re-issued data set that changed or removed a filing's row), aborts the quarter: an immutable filing
    that moved is a finding, not an update.
    """
    submissions = {s["ACCESSION_NUMBER"]: s for s in _tsv(folder / "SUBMISSION.tsv")}
    rows = list(month_rows(_tsv(folder / "MONTHLY_TOTAL_RETURN.tsv"), submissions))
    with conn.transaction():
        # Dropped explicitly as well: under a caller's open transaction this block is a savepoint, and
        # ON COMMIT DROP would leave the table for the next quarter.
        conn.execute("CREATE TEMP TABLE nport_stage (LIKE sec_nport_monthly_returns INCLUDING DEFAULTS) ON COMMIT DROP")
        with conn.cursor().copy(f"COPY nport_stage ({_COLUMNS}) FROM STDIN") as copy:
            for row in rows:
                copy.write_row(
                    (
                        row.accession_number,
                        row.monthly_total_return_id,
                        row.month_position,
                        row.class_id,
                        row.month,
                        row.return_pct,
                        row.report_date,
                        row.filing_date,
                        row.sub_type,
                        quarter,
                    )
                )
        inserted = conn.execute(
            f"INSERT INTO sec_nport_monthly_returns ({_COLUMNS}) SELECT {_COLUMNS} FROM nport_stage"
            " ON CONFLICT DO NOTHING"
        ).rowcount
        changed = conn.execute(_CHANGED_SQL).fetchone()
        if changed and changed[0]:
            raise RuntimeError(f"{quarter}: {changed[0]} stored rows differ from this read of the data set")
        removed = conn.execute(_REMOVED_SQL, {"quarter": quarter}).fetchone()
        if removed and removed[0]:
            raise RuntimeError(f"{quarter}: {removed[0]} stored rows are missing from this read of the data set")
        conn.execute("DROP TABLE nport_stage")
    return len(rows), inserted


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--today", type=date.fromisoformat, default=date.today())
    args = parser.parse_args()
    with (
        httpx.Client(headers={"User-Agent": settings.sec_user_agent}, timeout=120, follow_redirects=True) as client,
        psycopg.connect(settings.database_url, autocommit=True) as conn,
    ):
        unpublished: list[str] = []
        for quarter in _quarters(args.today):
            folder = _cached_tables(quarter, client)
            if folder is None:
                # 403 and 404 both read as "not published" in the shared fetch; only the trailing quarters may be.
                unpublished.append(quarter)
                print(f"{quarter}: not published")
                continue
            if unpublished:
                raise SystemExit(f"{unpublished} unavailable but the later {quarter} is published: history gap")
            offered, inserted = load_quarter(conn, quarter, folder)
            print(f"{quarter}: {offered} month rows offered, {inserted} inserted")


if __name__ == "__main__":
    main()
