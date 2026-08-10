"""Source-only construction for the preregistered #2480 candidate.

This module deliberately has no price/return reader.  It can therefore be
reviewed, tested and run before the sealed recent outcome interval is opened.
Contract: ``docs/proposals/ta/2026-08-10-insider-purchase-preregistration.md``.
"""

from __future__ import annotations

import csv
import hashlib
import io
import re
import zipfile
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Final, Literal

import psycopg

from app.services.strategy_result import CORPUS_VENDORS

TRIAL_ID: Final = "form4-code-p-opportunistic-purchase-v1"
ARCHIVE_FIRST_QUARTER: Final = "2019q1"
PRIMARY_START: Final = date(2022, 1, 1)
MAX_FILING_LAG_DAYS: Final = 5

InsiderClass = Literal["routine", "opportunistic"]


@dataclass(frozen=True)
class PurchaseObservation:
    issuer_cik: str
    issuer_symbol: str
    accession_number: str
    filer_cik: str
    transaction_date: date
    filed_date: date
    disclosed_value: Decimal
    accepted_at: datetime | None = None
    instrument_id: int | None = None


@dataclass(frozen=True)
class ClassifiedPurchase:
    observation: PurchaseObservation
    insider_class: InsiderClass

    @property
    def signal_month(self) -> tuple[int, int]:
        return self.observation.filed_date.year, self.observation.filed_date.month


@dataclass(frozen=True)
class InsiderSourceBuild:
    purchases: tuple[PurchaseObservation, ...]
    source_classified: tuple[ClassifiedPurchase, ...]
    classified: tuple[ClassifiedPurchase, ...]
    refusals: Mapping[str, int]
    archive_manifest_sha256: str


def _parse_date(value: str | None) -> date | None:
    text = (value or "").strip()
    if not text:
        return None
    for fmt in ("%d-%b-%Y", "%d-%b-%y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text.title(), fmt).date()
        except ValueError:
            continue
    return None


def _positive_decimal(value: str | None) -> Decimal | None:
    try:
        parsed = Decimal((value or "").strip())
    except InvalidOperation, ValueError:
        return None
    return parsed if parsed.is_finite() and parsed > 0 else None


def _open_tsv(archive: zipfile.ZipFile, *names: str) -> Iterable[dict[str, str]]:
    available = set(archive.namelist())
    selected = next((name for name in names if name in available), None)
    if selected is None:
        selected = next(
            (item for name in names for item in available if item.endswith("/" + name)),
            None,
        )
    if selected is None:
        raise ValueError(f"archive is missing required table {names[0]}")
    with archive.open(selected) as raw:
        text = io.TextIOWrapper(raw, encoding="utf-8", newline="")
        yield from csv.DictReader(text, delimiter="\t")


def _archive_manifest_sha256(paths: Sequence[Path]) -> str:
    digest = hashlib.sha256()
    for path in sorted(paths, key=lambda item: item.name):
        digest.update(path.name.encode())
        digest.update(b"\0")
        with path.open("rb") as handle:
            digest.update(hashlib.file_digest(handle, "sha256").digest())
    return digest.hexdigest()


_ARCHIVE_NAME = re.compile(r"^(?P<year>\d{4})q(?P<quarter>[1-4])_form345\.zip$")


def validate_archive_sequence(paths: Sequence[Path], *, expected_last_quarter: str) -> None:
    """Require one contiguous, explicitly pinned 2019q1..frontier sequence."""
    labels: list[tuple[int, int]] = []
    for path in paths:
        match = _ARCHIVE_NAME.fullmatch(path.name)
        if match is None:
            raise ValueError(f"unexpected insider archive filename: {path.name}")
        labels.append((int(match.group("year")), int(match.group("quarter"))))
    if len(labels) != len(set(labels)):
        raise ValueError("duplicate insider archive quarter")
    labels.sort()
    expected_match = re.fullmatch(r"(?P<year>\d{4})q(?P<quarter>[1-4])", expected_last_quarter)
    if expected_match is None:
        raise ValueError("expected_last_quarter must be YYYYqN")
    expected_last = (int(expected_match.group("year")), int(expected_match.group("quarter")))
    expected: list[tuple[int, int]] = []
    cursor = (2019, 1)
    while cursor <= expected_last:
        expected.append(cursor)
        cursor = (cursor[0] + 1, 1) if cursor[1] == 4 else (cursor[0], cursor[1] + 1)
    if labels != expected:
        raise ValueError(f"archives must be the contiguous 2019q1..{expected_last_quarter} sequence")


def load_archive_purchases(
    paths: Sequence[Path], *, expected_last_quarter: str
) -> tuple[tuple[PurchaseObservation, ...], Counter[str], str]:
    """Load eligible code-P rows from exact SEC quarterly archives.

    Exclusion counters are terminal and mutually exclusive over every
    NONDERIV_TRANS row, so ``eligible_transaction_rows + excluded_*`` reconciles
    to ``nonderivative_transaction_rows``.
    """
    if not paths:
        raise ValueError("at least one insider archive is required")
    validate_archive_sequence(paths, expected_last_quarter=expected_last_quarter)
    missing = [path for path in paths if not path.is_file()]
    if missing:
        raise FileNotFoundError(f"insider archives missing: {missing}")

    counters: Counter[str] = Counter()
    aggregated: dict[tuple[str, str, str, int, int, date], PurchaseObservation] = {}
    accession_counts: Counter[str] = Counter()
    for path in sorted(paths, key=lambda item: item.name):
        with zipfile.ZipFile(path) as archive:
            accession_counts.update(
                row.get("ACCESSION_NUMBER", "").strip()
                for row in _open_tsv(archive, "SUBMISSION.tsv")
                if row.get("ACCESSION_NUMBER", "").strip()
            )
    duplicate_accessions = {accession for accession, count in accession_counts.items() if count > 1}
    counters["duplicate_archive_accessions"] = len(duplicate_accessions)

    for path in sorted(paths, key=lambda item: item.name):
        with zipfile.ZipFile(path) as archive:
            submissions = {
                row.get("ACCESSION_NUMBER", "").strip(): row
                for row in _open_tsv(archive, "SUBMISSION.tsv")
                if row.get("ACCESSION_NUMBER", "").strip()
            }
            owners: dict[str, list[dict[str, str]]] = defaultdict(list)
            for row in _open_tsv(archive, "REPORTINGOWNER.tsv", "REPORTING_OWNER.tsv"):
                accession = row.get("ACCESSION_NUMBER", "").strip()
                if accession:
                    owners[accession].append(row)

            for row in _open_tsv(archive, "NONDERIV_TRANS.tsv", "NON_DERIV_TRANS.tsv"):
                counters["nonderivative_transaction_rows"] += 1
                accession = row.get("ACCESSION_NUMBER", "").strip()
                if accession in duplicate_accessions:
                    counters["excluded_duplicate_archive_accession"] += 1
                    continue
                submission = submissions.get(accession)
                if submission is None:
                    counters["excluded_submission_missing"] += 1
                    continue
                if (submission.get("DOCUMENT_TYPE") or "").strip().upper() != "4":
                    counters["excluded_not_original_form4"] += 1
                    continue
                owner_rows = owners.get(accession, [])
                if len(owner_rows) != 1:
                    counters["excluded_joint_or_missing_owner"] += 1
                    continue
                filer_cik = (owner_rows[0].get("RPTOWNERCIK") or "").strip().zfill(10)
                if not filer_cik.strip("0"):
                    counters["excluded_filer_cik_missing"] += 1
                    continue
                if (row.get("TRANS_CODE") or "").strip().upper() != "P":
                    counters["excluded_not_code_p"] += 1
                    continue
                if (row.get("TRANS_ACQUIRED_DISP_CD") or "").strip().upper() != "A":
                    counters["excluded_not_acquired"] += 1
                    continue
                if (row.get("EQUITY_SWAP_INVOLVED") or "").strip() not in {"", "0"}:
                    counters["excluded_equity_swap"] += 1
                    continue
                if (row.get("DEEMED_EXECUTION_DATE") or "").strip():
                    counters["excluded_deemed_execution"] += 1
                    continue
                if (row.get("TRANS_TIMELINESS") or "").strip().upper() == "L":
                    counters["excluded_late_flag"] += 1
                    continue
                shares = _positive_decimal(row.get("TRANS_SHARES"))
                price = _positive_decimal(row.get("TRANS_PRICEPERSHARE"))
                if shares is None or price is None:
                    counters["excluded_nonpositive_or_missing_value"] += 1
                    continue
                transaction_date = _parse_date(row.get("TRANS_DATE"))
                filed_date = _parse_date(submission.get("FILING_DATE"))
                if transaction_date is None or filed_date is None:
                    counters["excluded_invalid_date"] += 1
                    continue
                lag = (filed_date - transaction_date).days
                if lag < 0:
                    counters["excluded_transaction_after_filing"] += 1
                    continue
                if lag > MAX_FILING_LAG_DAYS:
                    counters["excluded_filing_lag_over_five_days"] += 1
                    continue
                issuer_cik = (submission.get("ISSUERCIK") or "").strip().zfill(10)
                issuer_symbol = (submission.get("ISSUERTRADINGSYMBOL") or "").strip().upper()
                if not issuer_cik.strip("0") or not issuer_symbol:
                    counters["excluded_issuer_identity_missing"] += 1
                    continue

                key = (
                    issuer_cik,
                    accession,
                    filer_cik,
                    transaction_date.year,
                    transaction_date.month,
                    filed_date,
                )
                value = shares * price
                existing = aggregated.get(key)
                if existing is None:
                    aggregated[key] = PurchaseObservation(
                        issuer_cik=issuer_cik,
                        issuer_symbol=issuer_symbol,
                        accession_number=accession,
                        filer_cik=filer_cik,
                        transaction_date=transaction_date,
                        filed_date=filed_date,
                        disclosed_value=value,
                    )
                else:
                    aggregated[key] = replace(
                        existing,
                        transaction_date=min(existing.transaction_date, transaction_date),
                        disclosed_value=existing.disclosed_value + value,
                    )
                counters["eligible_transaction_rows"] += 1

    purchases = tuple(
        sorted(
            aggregated.values(),
            key=lambda item: (item.transaction_date, item.filer_cik, item.issuer_cik, item.accession_number),
        )
    )
    counters["aggregated_purchase_observations"] = len(purchases)
    return purchases, counters, _archive_manifest_sha256(paths)


def classify_purchases(
    purchases: Sequence[PurchaseObservation],
) -> tuple[tuple[ClassifiedPurchase, ...], Counter[str]]:
    """Apply the annual, prior-three-year purchase-calendar classifier."""
    months: dict[str, dict[int, set[int]]] = defaultdict(lambda: defaultdict(set))
    for purchase in purchases:
        months[purchase.filer_cik][purchase.transaction_date.year].add(purchase.transaction_date.month)

    classified: list[ClassifiedPurchase] = []
    counters: Counter[str] = Counter()
    for purchase in purchases:
        year = purchase.transaction_date.year
        prior = [months[purchase.filer_cik].get(year - offset, set()) for offset in (3, 2, 1)]
        if any(not values for values in prior):
            counters["unclassified_missing_prior_purchase_year"] += 1
            continue
        insider_class: InsiderClass = "routine" if set.intersection(*prior) else "opportunistic"
        classified.append(ClassifiedPurchase(observation=purchase, insider_class=insider_class))
        counters[f"classified_{insider_class}"] += 1
    classified.sort(
        key=lambda item: (
            item.observation.filed_date,
            item.observation.issuer_cik,
            item.observation.filer_cik,
            item.observation.accession_number,
        )
    )
    return tuple(classified), counters


def build_research_resolution(
    rows: Iterable[tuple[str, str, int]],
) -> tuple[dict[tuple[str, str], int], dict[str, int]]:
    """Build only unambiguous exact and unique-CIK resolution maps."""
    exact_candidates: dict[tuple[str, str], set[int]] = defaultdict(set)
    by_cik: dict[str, set[int]] = defaultdict(set)
    for cik, symbol, instrument_id in rows:
        exact_candidates[(str(cik), str(symbol))].add(int(instrument_id))
        by_cik[str(cik)].add(int(instrument_id))
    exact = {key: next(iter(ids)) for key, ids in exact_candidates.items() if len(ids) == 1}
    unique = {cik: next(iter(ids)) for cik, ids in by_cik.items() if len(ids) == 1}
    return exact, unique


def _load_research_resolution(conn: psycopg.Connection[Any]) -> tuple[dict[tuple[str, str], int], dict[str, int]]:
    rows = conn.execute(
        """
        SELECT lpad(e.identifier_value, 10, '0'), upper(s.vendor_symbol), s.instrument_id
        FROM research_price_series s
        JOIN external_identifiers e
          ON e.instrument_id = s.instrument_id
         AND e.provider = 'sec'
         AND e.identifier_type = 'cik'
         AND e.is_primary
        WHERE s.vendor = %s
        ORDER BY 1, 2, 3
        """,
        (CORPUS_VENDORS[0],),
    ).fetchall()
    return build_research_resolution((str(cik), str(symbol), int(instrument_id)) for cik, symbol, instrument_id in rows)


def resolve_research_instrument(
    issuer_cik: str,
    issuer_symbol: str,
    exact: Mapping[tuple[str, str], int],
    unique: Mapping[str, int],
) -> tuple[int | None, bool]:
    """Resolve exact corpus symbol first; return whether CIK fallback was used."""
    exact_match = exact.get((issuer_cik, issuer_symbol))
    if exact_match is not None:
        return exact_match, False
    fallback = unique.get(issuer_cik)
    return fallback, fallback is not None


def enrich_point_in_time(
    conn: psycopg.Connection[Any], purchases: Sequence[PurchaseObservation]
) -> tuple[tuple[PurchaseObservation, ...], Counter[str]]:
    exact, unique = _load_research_resolution(conn)
    accessions = sorted({item.accession_number for item in purchases})
    accepted: dict[str, datetime] = {}
    if accessions:
        rows = conn.execute(
            """
            SELECT accession_number, max(accepted_at)
            FROM sec_filing_manifest
            WHERE accession_number = ANY(%s) AND accepted_at IS NOT NULL
            GROUP BY accession_number
            """,
            (accessions,),
        ).fetchall()
        accepted = {str(row[0]): row[1] for row in rows}

    counters: Counter[str] = Counter()
    output: list[PurchaseObservation] = []
    for purchase in purchases:
        instrument_id, used_fallback = resolve_research_instrument(
            purchase.issuer_cik, purchase.issuer_symbol, exact, unique
        )
        if instrument_id is None:
            counters["unresolved_or_multiclass_research_series"] += 1
            continue
        if used_fallback:
            counters["unique_cik_symbol_history_fallback"] += 1
        acceptance = accepted.get(purchase.accession_number)
        if acceptance is None:
            counters["accepted_at_missing_historical_fallback"] += 1
        output.append(replace(purchase, instrument_id=instrument_id, accepted_at=acceptance))
    counters["research_series_resolved"] = len(output)
    return tuple(output), counters


def enrich_classified_point_in_time(
    conn: psycopg.Connection[Any], classified: Sequence[ClassifiedPurchase]
) -> tuple[tuple[ClassifiedPurchase, ...], Counter[str]]:
    enriched, counters = enrich_point_in_time(conn, [item.observation for item in classified])
    by_identity: dict[tuple[str, str, int, int], InsiderClass] = {
        (
            item.observation.accession_number,
            item.observation.filer_cik,
            item.observation.transaction_date.year,
            item.observation.transaction_date.month,
        ): item.insider_class
        for item in classified
    }
    output = tuple(
        ClassifiedPurchase(
            observation=observation,
            insider_class=by_identity[
                (
                    observation.accession_number,
                    observation.filer_cik,
                    observation.transaction_date.year,
                    observation.transaction_date.month,
                )
            ],
        )
        for observation in enriched
    )
    return output, counters


def build_source(
    conn: psycopg.Connection[Any], paths: Sequence[Path], *, expected_last_quarter: str
) -> InsiderSourceBuild:
    purchases, load_counts, manifest_digest = load_archive_purchases(paths, expected_last_quarter=expected_last_quarter)
    source_classified, classification_counts = classify_purchases(purchases)
    classified, resolution_counts = enrich_classified_point_in_time(conn, source_classified)
    counts = load_counts + resolution_counts + classification_counts
    counts["survivor_only_research_corpus_promotion_refusal"] = 1
    return InsiderSourceBuild(
        purchases=purchases,
        source_classified=source_classified,
        classified=classified,
        refusals=dict(sorted(counts.items())),
        archive_manifest_sha256=manifest_digest,
    )


__all__ = [
    "ARCHIVE_FIRST_QUARTER",
    "MAX_FILING_LAG_DAYS",
    "PRIMARY_START",
    "TRIAL_ID",
    "ClassifiedPurchase",
    "InsiderSourceBuild",
    "PurchaseObservation",
    "build_source",
    "build_research_resolution",
    "classify_purchases",
    "enrich_point_in_time",
    "enrich_classified_point_in_time",
    "load_archive_purchases",
    "resolve_research_instrument",
    "validate_archive_sequence",
]
