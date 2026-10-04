"""Daily forward archive of Interactive Brokers' public stock-borrow file (#3622 slice 1).

IBKR publishes current borrow availability and fee rates on an anonymous FTP (user ``shortstock``) and keeps
no history, so every day not archived is lost. The file is an indicative borrow-cost PROXY and a long-side
filter input (Drechsler & Drechsler 2014); it does not price eToro CFD shorts.

A file that fails its own framing (``#BOF`` / header / ``#EOF`` row count) is refused, not stored. A new
file is always stored under its own ``#BOF`` stamp, however old. The liveness check fires on the other
case: the feed serves the SAME file again and its stamp is older than ``MAX_PROVIDER_AGE`` — a frozen feed
would otherwise report ``unchanged`` success every day while the archive stopped growing.
"""

from __future__ import annotations

import ftplib
import gzip
import hashlib
import io
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Final, Literal
from zoneinfo import ZoneInfo

import psycopg

FTP_HOST: Final = "ftp2.interactivebrokers.com"
FTP_USER: Final = "shortstock"
FILE_NAME: Final = "usa.txt"
#: The file is republished intraday (a 12:50Z fetch on 2026-10-04 carried an 08:36 ET stamp). Fixed by
#: construction: one day is the archive's own cadence, so a repeated file stamped earlier than that means
#: the feed is not moving.
MAX_PROVIDER_AGE: Final = timedelta(hours=24)
AVAILABLE_CAP_MARKER: Final = ">"

_HEADER: Final = "#SYM|CUR|NAME|CON|ISIN|REBATERATE|FEERATE|AVAILABLE|FIGI|"
_BOF: Final = re.compile(r"#BOF\|(\d{4})\.(\d{2})\.(\d{2})\|(\d{2}):(\d{2}):(\d{2})")
_EOF: Final = re.compile(r"#EOF\|(\d+)")
_NEW_YORK: Final = ZoneInfo("America/New_York")


class BorrowFileError(ValueError):
    """The fetched file does not satisfy its own framing or freshness contract."""


@dataclass(frozen=True)
class BorrowRate:
    conid: int
    symbol: str
    currency: str
    name: str
    isin: str | None
    figi: str | None
    rebate_rate_pct: Decimal | None
    fee_rate_pct: Decimal | None
    available_shares: int
    available_capped: bool


@dataclass(frozen=True)
class BorrowFile:
    provider_as_of: datetime
    rates: tuple[BorrowRate, ...]


@dataclass(frozen=True)
class BorrowArchiveReport:
    status: Literal["archived", "unchanged"]
    snapshot_id: int
    provider_as_of: datetime
    row_count: int
    response_sha256: str


def _rate(raw: str, *, line_number: int, field: str) -> Decimal | None:
    if raw == "NA":
        return None
    try:
        value = Decimal(raw)
    except InvalidOperation as exc:
        raise BorrowFileError(f"line {line_number}: {field} {raw!r} is not decimal") from exc
    if not value.is_finite():
        raise BorrowFileError(f"line {line_number}: {field} must be finite")
    return value


def _available(raw: str, *, line_number: int) -> tuple[int, bool]:
    capped = raw.startswith(AVAILABLE_CAP_MARKER)
    digits = raw[len(AVAILABLE_CAP_MARKER) :] if capped else raw
    if not digits.isdigit():
        raise BorrowFileError(f"line {line_number}: AVAILABLE {raw!r} is not a share count")
    return int(digits), capped


def parse_borrow_file(payload: bytes) -> BorrowFile:
    """Parse ``usa.txt``: ``#BOF|date|time``, the header, pipe rows with a trailing ``|``, ``#EOF|<rows>``."""
    try:
        lines = payload.decode("utf-8").splitlines()
    except UnicodeDecodeError as exc:
        raise BorrowFileError("file is not UTF-8") from exc
    if len(lines) < 3:
        raise BorrowFileError("file is shorter than its framing")
    bof = _BOF.fullmatch(lines[0].strip())
    if bof is None:
        raise BorrowFileError(f"first line {lines[0][:60]!r} is not #BOF|YYYY.MM.DD|HH:MM:SS")
    year, month, day, hour, minute, second = (int(part) for part in bof.groups())
    provider_as_of = datetime(year, month, day, hour, minute, second, tzinfo=_NEW_YORK).astimezone(UTC)
    if lines[1].strip() != _HEADER:
        raise BorrowFileError(f"header {lines[1][:120]!r}; expected {_HEADER!r}")
    eof = _EOF.fullmatch(lines[-1].strip())
    if eof is None:
        raise BorrowFileError("last line is not #EOF|<rows> (truncated transfer?)")
    body = lines[2:-1]
    if int(eof.group(1)) != len(body):
        raise BorrowFileError(f"#EOF declares {eof.group(1)} rows; file holds {len(body)}")

    rates: list[BorrowRate] = []
    seen: set[int] = set()
    for line_number, line in enumerate(body, start=3):
        cells = line.split("|")
        # Every row ends with "|", so a well-formed row splits into nine fields plus an empty tail.
        if len(cells) != 10 or cells[9] != "":
            raise BorrowFileError(f"line {line_number}: expected 9 pipe-terminated fields")
        symbol, currency, name, conid_raw, isin, rebate, fee, available, figi = cells[:9]
        if not conid_raw.isdigit():
            raise BorrowFileError(f"line {line_number}: CON {conid_raw!r} is not an integer")
        conid = int(conid_raw)
        if conid in seen:
            raise BorrowFileError(f"line {line_number}: duplicate CON {conid}")
        seen.add(conid)
        if not symbol or not currency:
            raise BorrowFileError(f"line {line_number}: empty SYM or CUR")
        shares, capped = _available(available, line_number=line_number)
        rates.append(
            BorrowRate(
                conid=conid,
                symbol=symbol,
                currency=currency,
                name=name,
                isin=isin or None,
                figi=figi or None,
                rebate_rate_pct=_rate(rebate, line_number=line_number, field="REBATERATE"),
                fee_rate_pct=_rate(fee, line_number=line_number, field="FEERATE"),
                available_shares=shares,
                available_capped=capped,
            )
        )
    if not rates:
        raise BorrowFileError("file holds no rows")
    return BorrowFile(provider_as_of=provider_as_of, rates=tuple(rates))


def fetch_borrow_file(*, host: str = FTP_HOST, file_name: str = FILE_NAME, timeout: float = 60.0) -> bytes:
    """Download one file from the anonymous ``shortstock`` FTP."""
    buffer = io.BytesIO()
    with ftplib.FTP(host, timeout=timeout) as ftp:
        ftp.login(FTP_USER, "")
        ftp.retrbinary(f"RETR {file_name}", buffer.write)
    return buffer.getvalue()


_INSERT_SNAPSHOT: Final = """
    INSERT INTO ibkr_borrow_snapshots (file_name, provider_as_of, response_sha256, payload_gzip, row_count)
    VALUES (%(file_name)s, %(provider_as_of)s, %(sha)s, %(payload)s, %(row_count)s)
    ON CONFLICT (file_name, response_sha256) DO NOTHING
    RETURNING snapshot_id
"""
_INSERT_RATE: Final = """
    INSERT INTO ibkr_borrow_rates
        (snapshot_id, conid, symbol, currency, name, isin, figi, rebate_rate_pct, fee_rate_pct,
         available_shares, available_capped)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


def archive_borrow_file(
    conn: psycopg.Connection[Any],
    *,
    fetch: Callable[[], bytes] = fetch_borrow_file,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> BorrowArchiveReport:
    """Fetch, validate and store one snapshot with its rows in a single transaction."""
    payload = fetch()
    parsed = parse_borrow_file(payload)
    sha = hashlib.sha256(payload).hexdigest()
    with conn.transaction():
        row = conn.execute(
            _INSERT_SNAPSHOT,
            {
                "file_name": FILE_NAME,
                "provider_as_of": parsed.provider_as_of,
                "sha": sha,
                "payload": gzip.compress(payload),
                "row_count": len(parsed.rates),
            },
        ).fetchone()
        if row is None:
            existing = conn.execute(
                "SELECT snapshot_id FROM ibkr_borrow_snapshots WHERE file_name = %s AND response_sha256 = %s",
                (FILE_NAME, sha),
            ).fetchone()
            if existing is None:
                raise RuntimeError("borrow snapshot conflict without an existing row")
            age = now() - parsed.provider_as_of
            if age > MAX_PROVIDER_AGE:
                raise BorrowFileError(
                    f"feed is serving the already-archived file stamped {parsed.provider_as_of.isoformat()} "
                    f"({age} old); it is not moving"
                )
            return BorrowArchiveReport("unchanged", int(existing[0]), parsed.provider_as_of, len(parsed.rates), sha)
        snapshot_id = int(row[0])
        with conn.cursor() as cursor:
            cursor.executemany(
                _INSERT_RATE,
                [
                    (
                        snapshot_id,
                        rate.conid,
                        rate.symbol,
                        rate.currency,
                        rate.name,
                        rate.isin,
                        rate.figi,
                        rate.rebate_rate_pct,
                        rate.fee_rate_pct,
                        rate.available_shares,
                        rate.available_capped,
                    )
                    for rate in parsed.rates
                ],
            )
    return BorrowArchiveReport("archived", snapshot_id, parsed.provider_as_of, len(parsed.rates), sha)


__all__ = [
    "FILE_NAME",
    "FTP_HOST",
    "MAX_PROVIDER_AGE",
    "BorrowArchiveReport",
    "BorrowFile",
    "BorrowFileError",
    "BorrowRate",
    "archive_borrow_file",
    "fetch_borrow_file",
    "parse_borrow_file",
]
