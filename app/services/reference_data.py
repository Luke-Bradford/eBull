"""Immutable free reference-data ingestion for R6 factor validation (#2912).

The source response is committed before normalization. A structural parser
failure therefore leaves the exact bytes and error in the database instead of
turning an upstream format change into an unreproducible job exception.
"""

from __future__ import annotations

import csv
import hashlib
import io
import re
import zipfile
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Final, Literal

import httpx
import psycopg
from openpyxl import load_workbook
from psycopg.rows import dict_row

ReferenceSource = Literal["kenneth_french", "aqr", "fred", "global_q", "jkp", "federal_reserve", "osap"]
ReferenceUnit = Literal[
    "decimal_return", "percent_per_annum", "binary_indicator", "probability", "usd_millions", "count"
]

FRENCH_FTP: Final = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp"
FRENCH_FIVE_FACTOR_URL: Final = f"{FRENCH_FTP}/F-F_Research_Data_5_Factors_2x3_CSV.zip"
FRENCH_MOMENTUM_URL: Final = f"{FRENCH_FTP}/F-F_Momentum_Factor_CSV.zip"
FRENCH_THREE_FACTOR_DAILY_URL: Final = f"{FRENCH_FTP}/F-F_Research_Data_Factors_daily_CSV.zip"
AQR_DATA_SETS: Final = "https://www.aqr.com/-/media/AQR/Documents/Insights/Data-Sets"
AQR_VME_MONTHLY_URL: Final = f"{AQR_DATA_SETS}/Value-and-Momentum-Everywhere-Factors-Monthly.xlsx"
FRED_CSV_URL: Final = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_key}"
#: global-q republishes the whole history yearly under a new year-stamped filename, so the file is
#: resolved from this index page at fetch time rather than pinned (a pin would go silently stale).
GLOBAL_Q_INDEX_URL: Final = "https://global-q.org/factors.html"
#: Re-estimated in full at every release (#3622), so every distinct response is archived as its own snapshot.
FED_EBP_URL: Final = "https://www.federalreserve.gov/econres/notes/feds-notes/ebp_csv.csv"
JKP_USA_MONTHLY_VW_CAP_URL: Final = (
    "https://jkpfactors-data.s3.amazonaws.com/public/%5Busa%5D_%5Ball_factors%5D_%5Bmonthly%5D_%5Bvw_cap%5D.zip"
)
#: #3609 step 1: the NYSE size breakpoints JKP's factors are built on (``me`` percentiles, USD millions).
JKP_NYSE_CUTOFFS_URL: Final = "https://jkpfactors-data.s3.amazonaws.com/public/other/nyse_cutoffs.csv"
#: #3730: the monthly return percentiles JKP winsorise non-CRSP holding returns at (step 1 spec Amendment 3).
JKP_RETURN_CUTOFFS_URL: Final = "https://jkpfactors-data.s3.amazonaws.com/public/other/return_cutoffs.csv"
#: Chen & Zimmermann's Open Source Asset Pricing data page. Each release is a new Google Drive folder, so
#: the wide long-short file is resolved from this page's link at fetch time rather than pinned.
OSAP_DATA_PAGE_URL: Final = "https://www.openassetpricing.com/data/"
#: The OSAP site answers 403 to httpx's default ``python-httpx/<version>`` User-Agent (checked 2026-10-06;
#: curl's and this one get 200), so the page fetch names itself.
OSAP_USER_AGENT: Final = "eBull/1.0 reference-data"

FRENCH_PARSER_VERSION: Final = "kenneth-french-monthly-csv-v2"
FRENCH_DAILY_PARSER_VERSION: Final = "kenneth-french-daily-csv-v1"
AQR_PARSER_VERSION: Final = "aqr-vme-monthly-xlsx-v2"
AQR_FACTOR_PARSER_VERSION: Final = "aqr-factor-monthly-xlsx-v1"
FRED_PARSER_VERSION: Final = "fred-csv-v1"
GLOBAL_Q_PARSER_VERSION: Final = "global-q-monthly-csv-v1"
JKP_PARSER_VERSION: Final = "jkp-monthly-csv-zip-v1"
JKP_CUTOFFS_PARSER_VERSION: Final = "jkp-nyse-cutoffs-csv-v1"
JKP_RETURN_CUTOFFS_PARSER_VERSION: Final = "jkp-return-cutoffs-csv-v1"
FED_EBP_PARSER_VERSION: Final = "fed-ebp-monthly-csv-v2"
OSAP_PARSER_VERSION: Final = "osap-predictor-ls-wide-csv-v1"

_FRENCH_MISSING: Final = frozenset({Decimal("-99.99"), Decimal("-999")})
_AQR_HEADER: Final = (
    "DATE",
    "VAL",
    "MOM",
    "VAL^SS",
    "MOM^SS",
    "VAL^AA",
    "MOM^AA",
    "VALLS_VME_US90",
    "MOMLS_VME_US90",
    "VALLS_VME_UK90",
    "MOMLS_VME_UK90",
    "VALLS_VME_ROE90",
    "MOMLS_VME_ROE90",
    "VALLS_VME_JP90",
    "MOMLS_VME_JP90",
    "VALLS_VME_EQ",
    "MOMLS_VME_EQ",
    "VALLS_VME_FX",
    "MOMLS_VME_FX",
    "VALLS_VME_FI",
    "MOMLS_VME_FI",
    "VALLS_VME_COM",
    "MOMLS_VME_COM",
)
#: QMJ and BAB share one country/aggregate column surface.
_AQR_COUNTRY_HEADER: Final = (
    "DATE",
    "AUS",
    "AUT",
    "BEL",
    "CAN",
    "CHE",
    "DEU",
    "DNK",
    "ESP",
    "FIN",
    "FRA",
    "GBR",
    "GRC",
    "HKG",
    "IRL",
    "ISR",
    "ITA",
    "JPN",
    "NLD",
    "NOR",
    "NZL",
    "PRT",
    "SGP",
    "SWE",
    "USA",
    "Global",
    "Global Ex USA",
    "Europe",
    "North America",
    "Pacific",
)
#: The TSMOM sheet leaves its date column's header cell empty.
_AQR_TSMOM_HEADER: Final = (None, "TSMOM", "TSMOM^CM", "TSMOM^EQ", "TSMOM^FI", "TSMOM^FX")

# Kenneth French univariate-sort files: the first table (value-weighted monthly returns) only.
_FRENCH_SORT_QUINTILE_DECILE: Final = (
    "Lo 20",
    "Qnt 2",
    "Qnt 3",
    "Qnt 4",
    "Hi 20",
    "Lo 10",
    "Dec 2",
    "Dec 3",
    "Dec 4",
    "Dec 5",
    "Dec 6",
    "Dec 7",
    "Dec 8",
    "Dec 9",
    "Hi 10",
)
_FRENCH_SORT_TERCILE: Final = ("Lo 30", "Med 40", "Hi 30")
_FRENCH_SORT_DEC_ALT: Final = (
    "Lo 20",
    "Qnt 2",
    "Qnt 3",
    "Qnt 4",
    "Hi 20",
    "Lo 10",
    "2-Dec",
    "3-Dec",
    "4-Dec",
    "5-Dec",
    "6-Dec",
    "7-Dec",
    "8-Dec",
    "9-Dec",
    "Hi 10",
)
_FRENCH_49_INDUSTRIES: Final = (
    "Agric",
    "Food",
    "Soda",
    "Beer",
    "Smoke",
    "Toys",
    "Fun",
    "Books",
    "Hshld",
    "Clths",
    "Hlth",
    "MedEq",
    "Drugs",
    "Chems",
    "Rubbr",
    "Txtls",
    "BldMt",
    "Cnstr",
    "Steel",
    "FabPr",
    "Mach",
    "ElcEq",
    "Autos",
    "Aero",
    "Ships",
    "Guns",
    "Gold",
    "Mines",
    "Coal",
    "Oil",
    "Util",
    "Telcm",
    "PerSv",
    "BusSv",
    "Hardw",
    "Softw",
    "Chips",
    "LabEq",
    "Paper",
    "Boxes",
    "Trans",
    "Whlsl",
    "Rtail",
    "Meals",
    "Banks",
    "Insur",
    "RlEst",
    "Fin",
    "Other",
)
_GLOBAL_Q_HEADER: Final = ("year", "month", "R_F", "R_MKT", "R_ME", "R_IA", "R_ROE", "R_EG")
_GLOBAL_Q_FILE: Final = re.compile(r'href="(/uploads/[0-9/]+/q5_factors_monthly_(\d{4})\.csv)"')
_FED_EBP_UNITS: Final[Mapping[str, ReferenceUnit]] = {
    "gz_spread": "percent_per_annum",
    "ebp": "percent_per_annum",
    "est_prob": "probability",
}
_FED_EBP_DATE_FORMATS: Final = ("%m/%d/%Y", "%Y-%m-%d")
_JKP_HEADER: Final = ("location", "name", "freq", "weighting", "direction", "n_stocks", "n_stocks_min", "date", "ret")
_JKP_CUTOFFS_HEADER: Final = ("eom", "n", "nyse_p1", "nyse_p20", "nyse_p50", "nyse_p80")
#: Three families of the same four percentiles: USD total return, local-currency total return, USD excess return.
_JKP_RETURN_PERCENTILES: Final = ("0_1", "1", "99", "99_9")
_JKP_RETURN_CUTOFFS_HEADER: Final = (
    "eom",
    "n",
    *(f"{family}_{p}" for family in ("ret", "ret_local", "ret_exc") for p in _JKP_RETURN_PERCENTILES),
)
#: The columns a row must carry: Amendment 3's rule reads the four bounds, and ``n`` is JKP's own row count, whose
#: absence would mean the file's shape drifted. A row missing one is refused.
_JKP_RETURN_REQUIRED_COLUMNS: Final = ("n", "ret_0_1", "ret_99_9", "ret_exc_0_1", "ret_exc_99_9")
#: The total and excess pairs are one monthly rate apart; the CSV's binary-float noise is far below any rate.
_JKP_RETURN_SHIFT_TOLERANCE: Final = Decimal("1e-9")
_OSAP_LS_WIDE_LINK: Final = re.compile(
    r'<a href="https://drive\.google\.com/file/d/([-\w]+)/[^"]*"[^>]*>'
    r"Monthly long-short returns of \d+ predictors following OPs \(wide csv\)</a>"
)
_HTML_TITLE: Final = re.compile(rb"<title>(.*?)</title>", re.IGNORECASE | re.DOTALL)


class ReferenceDataSourceError(ValueError):
    """A fetched response cannot satisfy its frozen source contract."""


@dataclass(frozen=True)
class ReferenceObservation:
    series_key: str
    observation_date: date
    value: Decimal
    unit: ReferenceUnit


@dataclass(frozen=True)
class ParsedReferenceData:
    observations: tuple[ReferenceObservation, ...]
    missing_count: int


Parser = Callable[[bytes], ParsedReferenceData]


@dataclass(frozen=True)
class ReferenceDatasetSpec:
    source: ReferenceSource
    dataset_key: str
    source_url: str
    parser_version: str
    parser: Parser
    #: Maps ``source_url`` to the file to fetch, for a source that renames its file on each release.
    resolve_url: Callable[[httpx.Client, str], str] | None = None


@dataclass(frozen=True)
class ReferenceRefreshReport:
    source: ReferenceSource
    dataset_key: str
    status: Literal["accepted", "not_modified", "unchanged"]
    snapshot_id: int
    response_sha256: str
    row_count: int
    missing_count: int
    first_observation: date | None
    last_observation: date | None


def _month_end(year: int, month: int) -> date:
    if month == 12:
        return date(year, 12, 31)
    return date(year, month + 1, 1).fromordinal(date(year, month + 1, 1).toordinal() - 1)


def _decimal(raw: object, *, context: str) -> Decimal:
    try:
        value = Decimal(str(raw).strip())
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ReferenceDataSourceError(f"{context}: value is not decimal") from exc
    if not value.is_finite():
        raise ReferenceDataSourceError(f"{context}: value must be finite")
    return value


def _validated(parsed: ParsedReferenceData) -> ParsedReferenceData:
    if not parsed.observations:
        raise ReferenceDataSourceError("source produced zero observations")
    seen: set[tuple[str, date]] = set()
    for item in parsed.observations:
        key = (item.series_key, item.observation_date)
        if key in seen:
            raise ReferenceDataSourceError(
                f"duplicate observation {item.series_key}/{item.observation_date.isoformat()}"
            )
        seen.add(key)
    return ParsedReferenceData(
        observations=tuple(sorted(parsed.observations, key=lambda item: (item.observation_date, item.series_key))),
        missing_count=parsed.missing_count,
    )


def parse_french_monthly_zip(
    payload: bytes,
    *,
    expected_series_keys: tuple[str, ...] | None = None,
) -> ParsedReferenceData:
    """Parse the one monthly table and stop before French's annual section."""
    return _parse_french_zip(payload, expected_series_keys=expected_series_keys, daily=False)


def parse_french_daily_zip(
    payload: bytes,
    *,
    expected_series_keys: tuple[str, ...] | None = None,
) -> ParsedReferenceData:
    """Parse a French daily file: one table of ``YYYYMMDD`` rows, percent returns per day."""
    return _parse_french_zip(payload, expected_series_keys=expected_series_keys, daily=True)


def _parse_french_zip(
    payload: bytes,
    *,
    expected_series_keys: tuple[str, ...] | None,
    daily: bool,
) -> ParsedReferenceData:
    try:
        with zipfile.ZipFile(io.BytesIO(payload)) as archive:
            names = [name for name in archive.namelist() if not name.endswith("/")]
            if len(names) != 1:
                raise ReferenceDataSourceError(f"French ZIP contains {len(names)} files; expected one")
            text = archive.read(names[0]).decode("utf-8-sig")
    except (UnicodeDecodeError, zipfile.BadZipFile) as exc:
        raise ReferenceDataSourceError("French response is not a valid UTF-8 CSV ZIP") from exc

    rows = list(csv.reader(io.StringIO(text)))
    header_index = next(
        (index for index, row in enumerate(rows) if row and row[0].strip() == "" and len(row) > 1),
        None,
    )
    if header_index is None:
        raise ReferenceDataSourceError("French monthly header was not found")
    series_keys = tuple(cell.strip() for cell in rows[header_index][1:])
    if not series_keys or any(not key for key in series_keys) or len(set(series_keys)) != len(series_keys):
        raise ReferenceDataSourceError(f"French series header is invalid: {series_keys!r}")
    if expected_series_keys is not None and series_keys != expected_series_keys:
        raise ReferenceDataSourceError(f"French series header {series_keys!r}; expected {expected_series_keys!r}")

    observations: list[ReferenceObservation] = []
    missing_count = 0
    table_rows = 0
    stamp_length = 8 if daily else 6
    for row_number, row in enumerate(rows[header_index + 1 :], start=header_index + 2):
        if not row:
            if table_rows:
                break
            continue
        stamp = row[0].strip()
        if len(stamp) != stamp_length or not stamp.isdigit():
            if table_rows:
                break
            continue
        if len(row) != len(series_keys) + 1:
            raise ReferenceDataSourceError(f"French row {row_number}: ragged row")
        year = int(stamp[:4])
        month = int(stamp[4:6])
        if daily:
            try:
                when = date(year, month, int(stamp[6:]))
            except ValueError as exc:
                raise ReferenceDataSourceError(f"French row {row_number}: invalid YYYYMMDD {stamp!r}") from exc
        elif not 1 <= month <= 12:
            raise ReferenceDataSourceError(f"French row {row_number}: invalid YYYYMM {stamp!r}")
        else:
            when = _month_end(year, month)
        table_rows += 1
        for series_key, raw in zip(series_keys, row[1:], strict=True):
            value = _decimal(raw, context=f"French row {row_number}/{series_key}")
            if value in _FRENCH_MISSING:
                missing_count += 1
                continue
            observations.append(ReferenceObservation(series_key, when, value / Decimal(100), "decimal_return"))
    return _validated(ParsedReferenceData(tuple(observations), missing_count))


def _aqr_date(raw: object, *, row_number: int) -> date:
    if isinstance(raw, datetime):
        return raw.date()
    if isinstance(raw, date):
        return raw
    if isinstance(raw, str):
        try:
            return datetime.strptime(raw.strip(), "%m/%d/%Y").date()
        except ValueError as exc:
            raise ReferenceDataSourceError(f"AQR row {row_number}: invalid DATE {raw!r}") from exc
    raise ReferenceDataSourceError(f"AQR row {row_number}: invalid DATE type {type(raw).__name__}")


def parse_aqr_monthly_sheet(
    payload: bytes,
    *,
    sheet: str,
    header: tuple[str | None, ...],
) -> ParsedReferenceData:
    """Parse one named AQR monthly sheet whose header row matches *header* exactly.

    Only the date column's header cell may be empty (the TSMOM sheet leaves it blank).
    """
    series_keys = tuple(key for key in header[1:] if key)
    if len(series_keys) != len(header) - 1:
        raise ValueError(f"AQR series header cells must be non-empty: {header!r}")
    try:
        workbook = load_workbook(io.BytesIO(payload), read_only=True, data_only=True)
    except Exception as exc:
        # openpyxl exposes several container/XML exception types. Convert them
        # at this external-data boundary while leaving later programmer errors
        # loud (iteration and validation occur outside this catch).
        raise ReferenceDataSourceError("AQR response is not a readable XLSX workbook") from exc
    try:
        if sheet not in workbook.sheetnames:
            raise ReferenceDataSourceError(f"AQR workbook has no {sheet!r} worksheet")
        rows = workbook[sheet].iter_rows(values_only=True)
        header_row: int | None = None
        for row_number, row in enumerate(rows, start=1):
            prefix = tuple(row[: len(header)])
            if prefix == header:
                if any(value is not None for value in row[len(header) :]):
                    raise ReferenceDataSourceError("AQR header has unexpected trailing columns")
                header_row = row_number
                break
            if row_number >= 100:
                break
        if header_row is None:
            raise ReferenceDataSourceError("AQR exact factor header was not found in the first 100 rows")

        observations: list[ReferenceObservation] = []
        missing_count = 0
        for row_number, row in enumerate(rows, start=header_row + 1):
            if not row or all(value is None or (isinstance(value, str) and not value.strip()) for value in row):
                continue
            if len(row) < len(header):
                raise ReferenceDataSourceError(f"AQR row {row_number}: ragged factor row")
            when = _aqr_date(row[0], row_number=row_number)
            for series_key, raw in zip(series_keys, row[1 : len(header)], strict=True):
                if raw is None or (isinstance(raw, str) and not raw.strip()):
                    missing_count += 1
                    continue
                value = _decimal(raw, context=f"AQR row {row_number}/{series_key}")
                observations.append(ReferenceObservation(series_key, when, value, "decimal_return"))
        return _validated(ParsedReferenceData(tuple(observations), missing_count))
    finally:
        workbook.close()


def parse_aqr_vme_monthly(payload: bytes) -> ParsedReferenceData:
    """Parse AQR's named monthly sheet and exact 23-column factor surface."""
    return parse_aqr_monthly_sheet(payload, sheet="VME Factors", header=_AQR_HEADER)


def parse_global_q_monthly_csv(payload: bytes) -> ParsedReferenceData:
    """Parse global-q's q5 monthly CSV: ``year,month`` then percent returns, normalised to decimal."""
    try:
        text = payload.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ReferenceDataSourceError("global-q response is not UTF-8 CSV") from exc
    reader = csv.reader(io.StringIO(text))
    header = tuple(cell.strip() for cell in next(reader, ()))
    if header != _GLOBAL_Q_HEADER:
        raise ReferenceDataSourceError(f"global-q header {header!r}; expected {_GLOBAL_Q_HEADER!r}")
    observations: list[ReferenceObservation] = []
    missing_count = 0
    for row_number, row in enumerate(reader, start=2):
        if not row or all(not cell.strip() for cell in row):
            continue
        if len(row) != len(header):
            raise ReferenceDataSourceError(f"global-q row {row_number}: ragged row")
        try:
            year, month = int(row[0]), int(row[1])
        except ValueError as exc:
            raise ReferenceDataSourceError(f"global-q row {row_number}: invalid year/month") from exc
        if not 1 <= month <= 12:
            raise ReferenceDataSourceError(f"global-q row {row_number}: invalid month {month}")
        when = _month_end(year, month)
        for series_key, raw in zip(header[2:], row[2:], strict=True):
            if not raw.strip():
                missing_count += 1
                continue
            value = _decimal(raw, context=f"global-q row {row_number}/{series_key}")
            observations.append(ReferenceObservation(series_key, when, value / Decimal(100), "decimal_return"))
    return _validated(ParsedReferenceData(tuple(observations), missing_count))


def _fed_ebp_date(raw: str, *, row_number: int) -> date:
    for fmt in _FED_EBP_DATE_FORMATS:
        try:
            return datetime.strptime(raw.strip(), fmt).date()
        except ValueError:
            continue
    raise ReferenceDataSourceError(f"EBP row {row_number}: invalid date {raw!r}")


def parse_fed_ebp_csv(payload: bytes) -> ParsedReferenceData:
    """Parse ``ebp_csv.csv``: ``date`` (first of month) then GZ spread, EBP and recession probability.

    The 2026-08 release wrote dates as M/D/YYYY; the 2026-10 release switched to ISO
    YYYY-MM-DD (#3716). Both forms are accepted so every archived vintage parses.
    """
    try:
        text = payload.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ReferenceDataSourceError("EBP response is not UTF-8 CSV") from exc
    reader = csv.reader(io.StringIO(text))
    header = tuple(cell.strip() for cell in next(reader, ()))
    expected = ("date", *_FED_EBP_UNITS)
    if header != expected:
        raise ReferenceDataSourceError(f"EBP header {header!r}; expected {expected!r}")
    observations: list[ReferenceObservation] = []
    missing_count = 0
    for row_number, row in enumerate(reader, start=2):
        if not row or all(not cell.strip() for cell in row):
            continue
        if len(row) != len(header):
            raise ReferenceDataSourceError(f"EBP row {row_number}: ragged row")
        when = _fed_ebp_date(row[0], row_number=row_number)
        for (series_key, unit), raw in zip(_FED_EBP_UNITS.items(), row[1:], strict=True):
            if not raw.strip() or raw.strip().upper() == "NA":
                missing_count += 1
                continue
            value = _decimal(raw, context=f"EBP row {row_number}/{series_key}")
            if unit == "probability" and not Decimal(0) <= value <= Decimal(1):
                raise ReferenceDataSourceError(f"EBP row {row_number}/{series_key}: probability outside [0, 1]")
            observations.append(ReferenceObservation(series_key, when, value, unit))
    return _validated(ParsedReferenceData(tuple(observations), missing_count))


def resolve_global_q_monthly_url(client: httpx.Client, index_url: str) -> str:
    """The newest ``q5_factors_monthly_<year>.csv`` linked from global-q's factors page."""
    response = client.get(index_url)
    response.raise_for_status()
    links = {int(year): path for path, year in _GLOBAL_Q_FILE.findall(response.text)}
    if not links:
        raise ReferenceDataSourceError("global-q factors page links no q5_factors_monthly_<year>.csv")
    return f"https://global-q.org{links[max(links)]}"


def parse_jkp_monthly_zip(
    payload: bytes,
    *,
    location: str,
    weighting: str,
) -> ParsedReferenceData:
    """Parse one JKP ``[location]_[all_factors]_[monthly]_[weighting]`` ZIP: one long-format CSV.

    ``ret`` is already a decimal return signed by the factor's ``direction`` (long the predicted
    out-performer), so it is stored as published, one series per factor ``name``.
    """
    try:
        with zipfile.ZipFile(io.BytesIO(payload)) as archive:
            names = [name for name in archive.namelist() if not name.endswith("/")]
            if len(names) != 1:
                raise ReferenceDataSourceError(f"JKP ZIP contains {len(names)} files; expected one")
            text = archive.read(names[0]).decode("utf-8-sig")
    except (UnicodeDecodeError, zipfile.BadZipFile) as exc:
        raise ReferenceDataSourceError("JKP response is not a valid UTF-8 CSV ZIP") from exc
    reader = csv.reader(io.StringIO(text))
    header = tuple(cell.strip() for cell in next(reader, ()))
    if header != _JKP_HEADER:
        raise ReferenceDataSourceError(f"JKP header {header!r}; expected {_JKP_HEADER!r}")
    observations: list[ReferenceObservation] = []
    missing_count = 0
    for row_number, row in enumerate(reader, start=2):
        if not row:
            continue
        if len(row) != len(header):
            raise ReferenceDataSourceError(f"JKP row {row_number}: ragged row")
        if (row[0], row[2], row[3]) != (location, "monthly", weighting):
            raise ReferenceDataSourceError(
                f"JKP row {row_number}: {row[0]}/{row[2]}/{row[3]}; expected {location}/monthly/{weighting}"
            )
        series_key = row[1].strip()
        if not series_key:
            raise ReferenceDataSourceError(f"JKP row {row_number}: empty factor name")
        try:
            when = date.fromisoformat(row[7].strip())
        except ValueError as exc:
            raise ReferenceDataSourceError(f"JKP row {row_number}: invalid date") from exc
        raw = row[8].strip()
        if not raw or raw.upper() in ("NA", "NAN"):
            missing_count += 1
            continue
        value = _decimal(raw, context=f"JKP row {row_number}/{series_key}")
        observations.append(ReferenceObservation(series_key, when, value, "decimal_return"))
    return _validated(ParsedReferenceData(tuple(observations), missing_count))


def resolve_osap_ls_wide_url(client: httpx.Client, data_page_url: str) -> str:
    """The direct-download URL of the wide long-short CSV linked from the OSAP data page."""
    response = client.get(data_page_url, headers={"User-Agent": OSAP_USER_AGENT})
    response.raise_for_status()
    file_ids = set(_OSAP_LS_WIDE_LINK.findall(response.text))
    if len(file_ids) != 1:
        raise ReferenceDataSourceError(
            f"OSAP data page links {len(file_ids)} wide long-short files; expected exactly one"
        )
    return f"https://drive.usercontent.google.com/download?id={file_ids.pop()}&export=download&confirm=t"


def parse_osap_ls_wide_csv(payload: bytes) -> ParsedReferenceData:
    """Parse OSAP ``PredictorLSretWide.csv``: ``date`` then one long-short return column per predictor.

    The layout, unit and sign are fixed by the producing code (OpenSourceAP/CrossSection v2.0.0):
    ``Portfolios/Code/20_PredictorPorts.R`` pivots each predictor's ``port == "LS"`` row wide by
    ``signalname``; ``01_PortfolioFunction.R`` builds LS as the long portfolio's return minus the short
    one's, following the original paper's sort; ``11_ProcessCRSP.R`` sets ``ret = 100*ret``, so values
    are percent and are normalised to decimal here. ``date`` is the CRSP monthly date (last trading day),
    stored as published: the validator aligns series on calendar month. R writes missing as ``NA``.

    Google Drive answers a refused download (its quota page) with HTTP 200 and HTML, which is refused
    by name rather than as a header mismatch.
    """
    if payload.lstrip()[:5].lower() in (b"<!doc", b"<html"):
        title = _HTML_TITLE.search(payload)
        label = title.group(1).decode("utf-8", "replace").strip() if title else "untitled"
        raise ReferenceDataSourceError(f"OSAP download returned an HTML page ({label!r}), not the CSV")
    try:
        text = payload.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ReferenceDataSourceError("OSAP response is not UTF-8 CSV") from exc
    reader = csv.reader(io.StringIO(text))
    header = tuple(cell.strip() for cell in next(reader, ()))
    if len(header) < 2 or header[0] != "date":
        raise ReferenceDataSourceError(f"OSAP header starts {header[:3]!r}; expected 'date' then predictors")
    signals = header[1:]
    if any(not name for name in signals) or len(set(signals)) != len(signals):
        raise ReferenceDataSourceError("OSAP header has an empty or duplicated predictor name")
    observations: list[ReferenceObservation] = []
    missing_count = 0
    previous: date | None = None
    for row_number, row in enumerate(reader, start=2):
        if not row or all(not cell.strip() for cell in row):
            continue
        if len(row) != len(header):
            raise ReferenceDataSourceError(f"OSAP row {row_number}: ragged row")
        try:
            when = date.fromisoformat(row[0].strip())
        except ValueError as exc:
            raise ReferenceDataSourceError(f"OSAP row {row_number}: invalid date {row[0]!r}") from exc
        if previous is not None and (when.year, when.month) <= (previous.year, previous.month):
            raise ReferenceDataSourceError(f"OSAP row {row_number}: {when} does not follow {previous}'s month")
        previous = when
        for series_key, raw in zip(signals, row[1:], strict=True):
            if not raw.strip() or raw.strip().upper() == "NA":
                missing_count += 1
                continue
            value = _decimal(raw, context=f"OSAP row {row_number}/{series_key}")
            observations.append(ReferenceObservation(series_key, when, value / Decimal(100), "decimal_return"))
    return _validated(ParsedReferenceData(tuple(observations), missing_count))


def parse_jkp_nyse_cutoffs_csv(payload: bytes) -> ParsedReferenceData:
    """Parse JKP ``nyse_cutoffs.csv``: NYSE market-equity percentiles at each month end (#3609).

    The percentiles are of JKP's ``me``, which their Documentation.pdf (Table "Market Equity") says
    "is quoted in million USD"; ``n`` is the NYSE stock count. Every month from the first to the last
    must be present, and the percentiles must be positive and non-decreasing.
    """
    try:
        text = payload.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ReferenceDataSourceError("JKP cutoffs response is not UTF-8 CSV") from exc
    reader = csv.reader(io.StringIO(text))
    header = tuple(cell.strip() for cell in next(reader, ()))
    if header != _JKP_CUTOFFS_HEADER:
        raise ReferenceDataSourceError(f"JKP cutoffs header {header!r}; expected {_JKP_CUTOFFS_HEADER!r}")
    observations: list[ReferenceObservation] = []
    missing_count = 0
    previous: date | None = None
    for row_number, row in enumerate(reader, start=2):
        if not row or all(not cell.strip() for cell in row):
            continue
        if len(row) != len(header):
            raise ReferenceDataSourceError(f"JKP cutoffs row {row_number}: ragged row")
        try:
            when = date.fromisoformat(row[0].strip())
        except ValueError as exc:
            raise ReferenceDataSourceError(f"JKP cutoffs row {row_number}: invalid eom") from exc
        if when != _month_end(when.year, when.month):
            raise ReferenceDataSourceError(f"JKP cutoffs row {row_number}: {when} is not a month end")
        if previous is not None and when != _month_end(*_next_month(previous)):
            raise ReferenceDataSourceError(f"JKP cutoffs row {row_number}: {when} does not follow {previous}")
        previous = when
        percentiles: list[Decimal] = []
        for series_key, raw in zip(header[1:], row[1:], strict=True):
            if not raw.strip() or raw.strip().upper() in ("NA", "NAN"):
                missing_count += 1
                continue
            value = _decimal(raw, context=f"JKP cutoffs row {row_number}/{series_key}")
            if value <= 0:
                raise ReferenceDataSourceError(f"JKP cutoffs row {row_number}/{series_key}: must be positive")
            if series_key == "n":
                if value != value.to_integral_value():
                    raise ReferenceDataSourceError(f"JKP cutoffs row {row_number}/n: not an integer count")
                observations.append(ReferenceObservation(series_key, when, value, "count"))
                continue
            percentiles.append(value)
            observations.append(ReferenceObservation(series_key, when, value, "usd_millions"))
        if percentiles != sorted(percentiles):
            raise ReferenceDataSourceError(f"JKP cutoffs row {row_number}: percentiles decrease")
    return _validated(ParsedReferenceData(tuple(observations), missing_count))


def parse_jkp_return_cutoffs_csv(payload: bytes) -> ParsedReferenceData:
    """Parse JKP ``return_cutoffs.csv``: each month's return percentiles over JKP's global rows (#3730).

    Produced by ``return_cutoffs`` (``GlobalFactors/project_macros.sas:78-92``, called with ``crsp_only=0``): the
    0.1st, 1st, 99th and 99.9th percentiles of the month's ``ret``, ``ret_local`` and ``ret_exc``, and the row
    count ``n``. Every month from the first to the last must be present and each family's percentiles must be
    non-decreasing. Amendment 3 of the #3609 step 1 spec clips a USD total return to ``ret_0_1`` / ``ret_99_9``
    because JKP's ``ret_exc`` is ``ret`` minus one monthly rate shared by the month: each row must therefore show
    the same shift at both ends (``ret_99_9 - ret_exc_99_9 == ret_0_1 - ret_exc_0_1``), and a row that does not, or
    lacks one of those four values or ``n``, is refused.
    """
    try:
        text = payload.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ReferenceDataSourceError("JKP return cutoffs response is not UTF-8 CSV") from exc
    reader = csv.reader(io.StringIO(text))
    header = tuple(cell.strip() for cell in next(reader, ()))
    if header != _JKP_RETURN_CUTOFFS_HEADER:
        raise ReferenceDataSourceError(f"JKP return cutoffs header {header!r}; expected {_JKP_RETURN_CUTOFFS_HEADER!r}")
    observations: list[ReferenceObservation] = []
    missing_count = 0
    previous: date | None = None
    for row_number, row in enumerate(reader, start=2):
        if not row or all(not cell.strip() for cell in row):
            continue
        context = f"JKP return cutoffs row {row_number}"
        if len(row) != len(header):
            raise ReferenceDataSourceError(f"{context}: ragged row")
        try:
            when = date.fromisoformat(row[0].strip())
        except ValueError as exc:
            raise ReferenceDataSourceError(f"{context}: invalid eom") from exc
        if when != _month_end(when.year, when.month):
            raise ReferenceDataSourceError(f"{context}: {when} is not a month end")
        if previous is not None and when != _month_end(*_next_month(previous)):
            raise ReferenceDataSourceError(f"{context}: {when} does not follow {previous}")
        previous = when
        values: dict[str, Decimal] = {}
        for series_key, raw in zip(header[1:], row[1:], strict=True):
            if not raw.strip() or raw.strip().upper() in ("NA", "NAN"):
                missing_count += 1
                continue
            values[series_key] = _decimal(raw, context=f"{context}/{series_key}")
        absent = [key for key in _JKP_RETURN_REQUIRED_COLUMNS if key not in values]
        if absent:
            raise ReferenceDataSourceError(f"{context}: missing {', '.join(absent)}")
        for family in ("ret", "ret_local", "ret_exc"):
            present = [values[k] for p in _JKP_RETURN_PERCENTILES if (k := f"{family}_{p}") in values]
            if present != sorted(present):
                raise ReferenceDataSourceError(f"{context}: {family} percentiles decrease")
        shift_high = values["ret_99_9"] - values["ret_exc_99_9"]
        shift_low = values["ret_0_1"] - values["ret_exc_0_1"]
        if abs(shift_high - shift_low) > _JKP_RETURN_SHIFT_TOLERANCE:
            raise ReferenceDataSourceError(f"{context}: total and excess bounds differ by unequal shifts")
        if values["n"] <= 0 or values["n"] != values["n"].to_integral_value():
            raise ReferenceDataSourceError(f"{context}/n: not a positive integer count")
        for series_key, value in values.items():
            unit: ReferenceUnit = "count" if series_key == "n" else "decimal_return"
            observations.append(ReferenceObservation(series_key, when, value, unit))
    return _validated(ParsedReferenceData(tuple(observations), missing_count))


def _next_month(when: date) -> tuple[int, int]:
    return (when.year + 1, 1) if when.month == 12 else (when.year, when.month + 1)


def parse_fred_csv(
    payload: bytes,
    *,
    series_key: str,
    unit: ReferenceUnit,
) -> ParsedReferenceData:
    """Parse one no-key FRED graph CSV with blank-as-missing semantics."""
    try:
        text = payload.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ReferenceDataSourceError("FRED response is not UTF-8 CSV") from exc
    reader = csv.DictReader(io.StringIO(text))
    expected = ("observation_date", series_key)
    if tuple(reader.fieldnames or ()) != expected:
        raise ReferenceDataSourceError(f"FRED header {reader.fieldnames!r}; expected {expected!r}")

    observations: list[ReferenceObservation] = []
    missing_count = 0
    for row_number, row in enumerate(reader, start=2):
        if None in row or any(row.get(key) is None for key in expected):
            raise ReferenceDataSourceError(f"FRED row {row_number}: ragged row")
        try:
            when = date.fromisoformat(str(row["observation_date"]).strip())
        except ValueError as exc:
            raise ReferenceDataSourceError(f"FRED row {row_number}: invalid observation_date") from exc
        raw = str(row[series_key]).strip()
        if not raw or raw == ".":
            missing_count += 1
            continue
        value = _decimal(raw, context=f"FRED row {row_number}/{series_key}")
        if unit == "binary_indicator" and value not in (Decimal(0), Decimal(1)):
            raise ReferenceDataSourceError(f"FRED row {row_number}/{series_key}: expected binary 0/1")
        observations.append(ReferenceObservation(series_key, when, value, unit))
    return _validated(ParsedReferenceData(tuple(observations), missing_count))


def _fred_parser(series_key: str, unit: ReferenceUnit) -> Parser:
    return lambda payload: parse_fred_csv(payload, series_key=series_key, unit=unit)


def _french_parser(expected_series_keys: tuple[str, ...]) -> Parser:
    return lambda payload: parse_french_monthly_zip(payload, expected_series_keys=expected_series_keys)


def _aqr_parser(sheet: str, header: tuple[str | None, ...]) -> Parser:
    return lambda payload: parse_aqr_monthly_sheet(payload, sheet=sheet, header=header)


def _jkp_parser(location: str, weighting: str) -> Parser:
    return lambda payload: parse_jkp_monthly_zip(payload, location=location, weighting=weighting)


def _french(dataset_key: str, filename: str, series_keys: tuple[str, ...]) -> ReferenceDatasetSpec:
    return ReferenceDatasetSpec(
        "kenneth_french", dataset_key, f"{FRENCH_FTP}/{filename}", FRENCH_PARSER_VERSION, _french_parser(series_keys)
    )


def _aqr(dataset_key: str, filename: str, sheet: str, header: tuple[str | None, ...]) -> ReferenceDatasetSpec:
    return ReferenceDatasetSpec(
        "aqr", dataset_key, f"{AQR_DATA_SETS}/{filename}", AQR_FACTOR_PARSER_VERSION, _aqr_parser(sheet, header)
    )


#: #3623 — the published series our constructions are validated against (#3609's factor families:
#: reversal, value E/P CF/P B/M, profitability, investment, accruals, net issuance, low volatility, beta,
#: and the 49 industries for within-industry ranks). Each sort file stores its value-weighted table.
_FRENCH_LIBRARY: Final = (
    _french("french_st_reversal_monthly", "F-F_ST_Reversal_Factor_CSV.zip", ("ST_Rev",)),
    _french("french_lt_reversal_monthly", "F-F_LT_Reversal_Factor_CSV.zip", ("LT_Rev",)),
    _french(
        "french_sort_be_me_monthly",
        "Portfolios_Formed_on_BE-ME_CSV.zip",
        ("<= 0", *_FRENCH_SORT_TERCILE, *_FRENCH_SORT_DEC_ALT),
    ),
    _french(
        "french_sort_e_p_monthly",
        "Portfolios_Formed_on_E-P_CSV.zip",
        ("<= 0", *_FRENCH_SORT_TERCILE, *_FRENCH_SORT_DEC_ALT),
    ),
    _french(
        "french_sort_cf_p_monthly",
        "Portfolios_Formed_on_CF-P_CSV.zip",
        ("<= 0", *_FRENCH_SORT_TERCILE, *_FRENCH_SORT_DEC_ALT),
    ),
    _french(
        "french_sort_op_monthly", "Portfolios_Formed_on_OP_CSV.zip", (*_FRENCH_SORT_TERCILE, *_FRENCH_SORT_DEC_ALT)
    ),
    _french(
        "french_sort_inv_monthly", "Portfolios_Formed_on_INV_CSV.zip", (*_FRENCH_SORT_TERCILE, *_FRENCH_SORT_DEC_ALT)
    ),
    _french("french_sort_ac_monthly", "Portfolios_Formed_on_AC_CSV.zip", _FRENCH_SORT_QUINTILE_DECILE),
    _french(
        "french_sort_ni_monthly", "Portfolios_Formed_on_NI_CSV.zip", ("< 0", "ZERO", *_FRENCH_SORT_QUINTILE_DECILE)
    ),
    _french("french_sort_var_monthly", "Portfolios_Formed_on_VAR_CSV.zip", _FRENCH_SORT_QUINTILE_DECILE),
    _french("french_sort_resvar_monthly", "Portfolios_Formed_on_RESVAR_CSV.zip", _FRENCH_SORT_QUINTILE_DECILE),
    _french("french_sort_beta_monthly", "Portfolios_Formed_on_BETA_CSV.zip", _FRENCH_SORT_QUINTILE_DECILE),
    _french("french_49_industries_monthly", "49_Industry_Portfolios_CSV.zip", _FRENCH_49_INDUSTRIES),
)
_AQR_LIBRARY: Final = (
    _aqr("aqr_qmj_monthly", "Quality-Minus-Junk-Factors-Monthly.xlsx", "QMJ Factors", _AQR_COUNTRY_HEADER),
    _aqr("aqr_bab_monthly", "Betting-Against-Beta-Equity-Factors-Monthly.xlsx", "BAB Factors", _AQR_COUNTRY_HEADER),
    _aqr("aqr_tsmom_monthly", "Time-Series-Momentum-Factors-Monthly.xlsx", "TSMOM Factors", _AQR_TSMOM_HEADER),
)
_FACTOR_LIBRARY: Final = (
    ReferenceDatasetSpec(
        "global_q",
        "global_q_q5_monthly",
        GLOBAL_Q_INDEX_URL,
        GLOBAL_Q_PARSER_VERSION,
        parse_global_q_monthly_csv,
        resolve_url=resolve_global_q_monthly_url,
    ),
    ReferenceDatasetSpec(
        "jkp",
        "jkp_usa_monthly_vw_cap",
        JKP_USA_MONTHLY_VW_CAP_URL,
        JKP_PARSER_VERSION,
        _jkp_parser("usa", "vw_cap"),
    ),
    ReferenceDatasetSpec(
        "jkp",
        "jkp_nyse_cutoffs",
        JKP_NYSE_CUTOFFS_URL,
        JKP_CUTOFFS_PARSER_VERSION,
        parse_jkp_nyse_cutoffs_csv,
    ),
    ReferenceDatasetSpec(
        "jkp",
        "jkp_return_cutoffs",
        JKP_RETURN_CUTOFFS_URL,
        JKP_RETURN_CUTOFFS_PARSER_VERSION,
        parse_jkp_return_cutoffs_csv,
    ),
    ReferenceDatasetSpec(
        "osap",
        "osap_predictor_ls_monthly",
        OSAP_DATA_PAGE_URL,
        OSAP_PARSER_VERSION,
        parse_osap_ls_wide_csv,
        resolve_url=resolve_osap_ls_wide_url,
    ),
)


REFERENCE_DATASETS: Final[Mapping[str, ReferenceDatasetSpec]] = {
    "french_five_factor_monthly": ReferenceDatasetSpec(
        "kenneth_french",
        "french_five_factor_monthly",
        FRENCH_FIVE_FACTOR_URL,
        FRENCH_PARSER_VERSION,
        _french_parser(("Mkt-RF", "SMB", "HML", "RMW", "CMA", "RF")),
    ),
    "french_momentum_monthly": ReferenceDatasetSpec(
        "kenneth_french",
        "french_momentum_monthly",
        FRENCH_MOMENTUM_URL,
        FRENCH_PARSER_VERSION,
        _french_parser(("Mom",)),
    ),
    #: #3609 step 1: the daily risk-free rate (``RF``) that excess daily returns are measured against.
    "french_three_factor_daily": ReferenceDatasetSpec(
        "kenneth_french",
        "french_three_factor_daily",
        FRENCH_THREE_FACTOR_DAILY_URL,
        FRENCH_DAILY_PARSER_VERSION,
        lambda payload: parse_french_daily_zip(payload, expected_series_keys=("Mkt-RF", "SMB", "HML", "RF")),
    ),
    "aqr_vme_monthly": ReferenceDatasetSpec(
        "aqr",
        "aqr_vme_monthly",
        AQR_VME_MONTHLY_URL,
        AQR_PARSER_VERSION,
        parse_aqr_vme_monthly,
    ),
    "fred_dgs3mo": ReferenceDatasetSpec(
        "fred",
        "fred_dgs3mo",
        FRED_CSV_URL.format(series_key="DGS3MO"),
        FRED_PARSER_VERSION,
        _fred_parser("DGS3MO", "percent_per_annum"),
    ),
    "fred_usrec": ReferenceDatasetSpec(
        "fred",
        "fred_usrec",
        FRED_CSV_URL.format(series_key="USREC"),
        FRED_PARSER_VERSION,
        _fred_parser("USREC", "binary_indicator"),
    ),
    "fed_ebp_monthly": ReferenceDatasetSpec(
        "federal_reserve", "fed_ebp_monthly", FED_EBP_URL, FED_EBP_PARSER_VERSION, parse_fed_ebp_csv
    ),
    **{spec.dataset_key: spec for spec in (*_FRENCH_LIBRARY, *_AQR_LIBRARY, *_FACTOR_LIBRARY)},
}

FRENCH_DATASET_KEYS: Final = (
    "french_five_factor_monthly",
    "french_momentum_monthly",
    "french_three_factor_daily",
    *(spec.dataset_key for spec in _FRENCH_LIBRARY),
)
AQR_DATASET_KEYS: Final = ("aqr_vme_monthly", *(spec.dataset_key for spec in _AQR_LIBRARY))
#: The daily macro group. The EBP rides it so a release is archived within a day of publication.
FRED_DATASET_KEYS: Final = ("fred_dgs3mo", "fred_usrec", "fed_ebp_monthly")
FACTOR_LIBRARY_DATASET_KEYS: Final = tuple(spec.dataset_key for spec in _FACTOR_LIBRARY)


def _latest_accepted(conn: psycopg.Connection[Any], spec: ReferenceDatasetSpec) -> Mapping[str, Any] | None:
    with conn.cursor(row_factory=dict_row) as cursor:
        cursor.execute(
            """
            SELECT snapshot_id, source_url, etag, last_modified, response_sha256,
                   row_count, missing_count, first_observation, last_observation
            FROM reference_data_snapshots
            WHERE source = %(source)s
              AND dataset_key = %(dataset_key)s
              AND parser_version = %(parser_version)s
              AND parse_status = 'accepted'
            ORDER BY fetched_at DESC, snapshot_id DESC
            LIMIT 1
            """,
            {
                "source": spec.source,
                "dataset_key": spec.dataset_key,
                "parser_version": spec.parser_version,
            },
        )
        return cursor.fetchone()


def _report_from_row(
    spec: ReferenceDatasetSpec,
    row: Mapping[str, Any],
    *,
    status: Literal["not_modified", "unchanged"],
) -> ReferenceRefreshReport:
    return ReferenceRefreshReport(
        source=spec.source,
        dataset_key=spec.dataset_key,
        status=status,
        snapshot_id=int(row["snapshot_id"]),
        response_sha256=str(row["response_sha256"]),
        row_count=int(row["row_count"]),
        missing_count=int(row["missing_count"]),
        first_observation=row["first_observation"],
        last_observation=row["last_observation"],
    )


def refresh_reference_dataset(
    conn: psycopg.Connection[Any],
    *,
    client: httpx.Client,
    spec: ReferenceDatasetSpec,
) -> ReferenceRefreshReport:
    """Fetch, raw-commit, parse and atomically accept one dataset snapshot."""
    if not conn.autocommit:
        raise RuntimeError("refresh_reference_dataset requires an autocommit connection")
    prior = _latest_accepted(conn, spec)
    source_url = spec.resolve_url(client, spec.source_url) if spec.resolve_url is not None else spec.source_url
    headers: dict[str, str] = {}
    # A prior snapshot's validators describe the file it came from; after a rename they do not apply.
    if prior is not None and prior["source_url"] == source_url:
        if prior["etag"]:
            headers["If-None-Match"] = str(prior["etag"])
        if prior["last_modified"]:
            headers["If-Modified-Since"] = str(prior["last_modified"])
    response = client.get(source_url, headers=headers)
    if response.status_code == 304:
        if prior is None:
            raise ReferenceDataSourceError("source returned 304 without an accepted prior snapshot")
        return _report_from_row(spec, prior, status="not_modified")
    response.raise_for_status()
    if not response.content:
        raise ReferenceDataSourceError("source returned an empty response body")

    response_sha256 = hashlib.sha256(response.content).hexdigest()
    with conn.transaction(), conn.cursor(row_factory=dict_row) as cursor:
        cursor.execute(
            """
            INSERT INTO reference_data_snapshots
                (source, dataset_key, source_url, etag, last_modified,
                 content_type, response_sha256, payload, parser_version)
            VALUES
                (%(source)s, %(dataset_key)s, %(source_url)s, %(etag)s,
                 %(last_modified)s, %(content_type)s, %(response_sha256)s,
                 %(payload)s, %(parser_version)s)
            ON CONFLICT (source, dataset_key, response_sha256, parser_version)
                DO NOTHING
            RETURNING snapshot_id, parse_status, parse_error, row_count,
                      missing_count, first_observation, last_observation
            """,
            {
                "source": spec.source,
                "dataset_key": spec.dataset_key,
                "source_url": source_url,
                "etag": response.headers.get("ETag"),
                "last_modified": response.headers.get("Last-Modified"),
                "content_type": response.headers.get("Content-Type"),
                "response_sha256": response_sha256,
                "payload": response.content,
                "parser_version": spec.parser_version,
            },
        )
        snapshot = cursor.fetchone()
        if snapshot is None:
            cursor.execute(
                """
                SELECT snapshot_id, parse_status, parse_error, row_count,
                       missing_count, first_observation, last_observation
                FROM reference_data_snapshots
                WHERE source = %(source)s
                  AND dataset_key = %(dataset_key)s
                  AND response_sha256 = %(response_sha256)s
                  AND parser_version = %(parser_version)s
                ORDER BY snapshot_id DESC
                LIMIT 1
                """,
                {
                    "source": spec.source,
                    "dataset_key": spec.dataset_key,
                    "response_sha256": response_sha256,
                    "parser_version": spec.parser_version,
                },
            )
            snapshot = cursor.fetchone()
    if snapshot is None:
        raise RuntimeError("snapshot insert/conflict lookup returned no row")
    snapshot_id = int(snapshot["snapshot_id"])
    if snapshot["parse_status"] == "accepted":
        existing = {
            **snapshot,
            "response_sha256": response_sha256,
        }
        return _report_from_row(spec, existing, status="unchanged")
    if snapshot["parse_status"] == "rejected":
        raise ReferenceDataSourceError(
            f"snapshot {snapshot_id} was already rejected by {spec.parser_version}: {snapshot['parse_error']}"
        )

    try:
        parsed = spec.parser(response.content)
    except ReferenceDataSourceError as exc:
        with conn.transaction():
            result = conn.execute(
                """
                UPDATE reference_data_snapshots
                SET parse_status = 'rejected', parse_error = %(error)s,
                    parsed_at = now()
                WHERE snapshot_id = %(snapshot_id)s AND parse_status = 'pending'
                """,
                {"snapshot_id": snapshot_id, "error": str(exc)[:4000]},
            )
            if result.rowcount != 1:
                raise RuntimeError(f"expected one pending snapshot {snapshot_id} to reject") from exc
        raise

    dates = [item.observation_date for item in parsed.observations]
    with conn.transaction():
        with conn.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO reference_data_observations
                    (snapshot_id, series_key, observation_date, value, unit)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (snapshot_id, series_key, observation_date) DO NOTHING
                """,
                [
                    (snapshot_id, item.series_key, item.observation_date, item.value, item.unit)
                    for item in parsed.observations
                ],
            )
        result = conn.execute(
            """
            UPDATE reference_data_snapshots
            SET parse_status = 'accepted', parse_error = NULL, parsed_at = now(),
                row_count = %(row_count)s, missing_count = %(missing_count)s,
                first_observation = %(first)s, last_observation = %(last)s
            WHERE snapshot_id = %(snapshot_id)s AND parse_status = 'pending'
            """,
            {
                "snapshot_id": snapshot_id,
                "row_count": len(parsed.observations),
                "missing_count": parsed.missing_count,
                "first": min(dates),
                "last": max(dates),
            },
        )
        if result.rowcount != 1:
            raise RuntimeError(f"expected one pending snapshot {snapshot_id} to accept")
    return ReferenceRefreshReport(
        source=spec.source,
        dataset_key=spec.dataset_key,
        status="accepted",
        snapshot_id=snapshot_id,
        response_sha256=response_sha256,
        row_count=len(parsed.observations),
        missing_count=parsed.missing_count,
        first_observation=min(dates),
        last_observation=max(dates),
    )


def refresh_reference_group(
    conn: psycopg.Connection[Any],
    *,
    client: httpx.Client,
    dataset_keys: Sequence[str],
) -> tuple[ReferenceRefreshReport, ...]:
    """Refresh every member of one source group, then surface any failures."""
    unknown = [key for key in dataset_keys if key not in REFERENCE_DATASETS]
    if unknown:
        raise ValueError(f"unknown reference dataset key(s): {unknown}")
    reports: list[ReferenceRefreshReport] = []
    failures: list[Exception] = []
    for key in dataset_keys:
        try:
            reports.append(refresh_reference_dataset(conn, client=client, spec=REFERENCE_DATASETS[key]))
        except Exception as exc:
            exc.add_note(f"reference dataset: {key}")
            failures.append(exc)
    if failures:
        keys = ", ".join(dataset_keys)
        raise ExceptionGroup(f"one or more reference refreshes failed in group [{keys}]", failures)
    return tuple(reports)


__all__ = [
    "AQR_DATASET_KEYS",
    "AQR_FACTOR_PARSER_VERSION",
    "AQR_PARSER_VERSION",
    "AQR_VME_MONTHLY_URL",
    "FRED_DATASET_KEYS",
    "FACTOR_LIBRARY_DATASET_KEYS",
    "FRED_PARSER_VERSION",
    "FRENCH_DAILY_PARSER_VERSION",
    "FRENCH_DATASET_KEYS",
    "FRENCH_PARSER_VERSION",
    "GLOBAL_Q_PARSER_VERSION",
    "JKP_CUTOFFS_PARSER_VERSION",
    "JKP_PARSER_VERSION",
    "JKP_RETURN_CUTOFFS_PARSER_VERSION",
    "OSAP_PARSER_VERSION",
    "OSAP_USER_AGENT",
    "REFERENCE_DATASETS",
    "ParsedReferenceData",
    "ReferenceDataSourceError",
    "ReferenceDatasetSpec",
    "ReferenceObservation",
    "ReferenceRefreshReport",
    "parse_aqr_monthly_sheet",
    "parse_aqr_vme_monthly",
    "parse_fed_ebp_csv",
    "parse_fred_csv",
    "parse_french_daily_zip",
    "parse_french_monthly_zip",
    "parse_global_q_monthly_csv",
    "parse_jkp_monthly_zip",
    "parse_jkp_nyse_cutoffs_csv",
    "parse_jkp_return_cutoffs_csv",
    "parse_osap_ls_wide_csv",
    "refresh_reference_dataset",
    "refresh_reference_group",
    "resolve_global_q_monthly_url",
    "resolve_osap_ls_wide_url",
]
