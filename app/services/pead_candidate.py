"""Point-in-time historical-SUE source construction for candidate #2476.

The module deliberately stops at signal construction.  It does not know an
outcome price and therefore can be exercised and reviewed before the sealed
recent return interval is opened.  Source and formula contract:
``docs/proposals/ta/2026-08-10-pead-preregistration.md``.
"""

from __future__ import annotations

import hashlib
import json
import zipfile
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import date, datetime
from decimal import Decimal
from math import ceil, isfinite
from pathlib import Path
from statistics import fmean, stdev
from typing import Any, Final

import psycopg

from app.services.strategy_result import CORPUS_VENDORS

TRIAL_ID: Final = "pead-historical-sue-net-income-v1"
SOURCE_CONCEPT: Final = "NetIncomeLoss"
SOURCE_TAXONOMY: Final = "us-gaap"
SOURCE_UNIT: Final = "USD"
ESTIMATION_DIFFERENCES: Final = 21
SEASONAL_LAG: Final = 4
REQUIRED_PRIOR_QUARTERS: Final = ESTIMATION_DIFFERENCES + SEASONAL_LAG
THRESHOLD_LOOKBACK_CALENDAR_QUARTERS: Final = 8
MIN_THRESHOLD_EVENTS: Final = 200


@dataclass(frozen=True)
class ReportedFact:
    instrument_id: int
    accession_number: str
    form_type: str
    filed_date: date
    accepted_at: datetime | None
    fiscal_year: int
    fiscal_period: str
    period_end: date
    values: tuple[Decimal, ...]


@dataclass(frozen=True)
class QuarterObservation:
    instrument_id: int
    fiscal_year: int
    fiscal_quarter: int
    value: Decimal
    filed_date: date
    accepted_at: datetime | None
    accession_number: str
    source_accessions: tuple[str, ...]
    derived_q4: bool

    @property
    def fiscal_key(self) -> int:
        return self.fiscal_year * 4 + self.fiscal_quarter


@dataclass(frozen=True)
class SueEvent:
    observation: QuarterObservation
    sue: float

    @property
    def calendar_quarter(self) -> int:
        return self.observation.filed_date.year * 4 + (self.observation.filed_date.month - 1) // 3 + 1


@dataclass(frozen=True)
class TriggeredSueEvent:
    event: SueEvent
    lower_threshold: float
    upper_threshold: float
    side: str | None
    threshold_population: int


@dataclass(frozen=True)
class PeadSourceBuild:
    observations: tuple[QuarterObservation, ...]
    sue_events: tuple[SueEvent, ...]
    triggers: tuple[TriggeredSueEvent, ...]
    instrument_alternatives: Mapping[int, tuple[int, ...]]
    refusals: Mapping[str, int]


@dataclass(frozen=True)
class ArchiveFactLoad:
    facts: tuple[ReportedFact, ...]
    archive_sha256: str
    instrument_alternatives: Mapping[int, tuple[int, ...]]
    refusals: Mapping[str, int]


def _quarter_number(fiscal_period: str) -> int | None:
    if fiscal_period in {"Q1", "Q2", "Q3"}:
        return int(fiscal_period[1])
    if fiscal_period == "FY":
        return 4
    return None


def construct_quarters(facts: Iterable[ReportedFact]) -> tuple[tuple[QuarterObservation, ...], Counter[str]]:
    """Build exact Q1-Q3 observations and causal residual Q4 observations."""
    refusals: Counter[str] = Counter()
    by_key: dict[tuple[int, int, int], list[ReportedFact]] = defaultdict(list)
    for fact in facts:
        quarter = _quarter_number(fact.fiscal_period)
        if quarter is None:
            refusals["unsupported_fiscal_period"] += 1
            continue
        if len(fact.values) != 1:
            refusals["ambiguous_current_fact"] += 1
            continue
        if not fact.values[0].is_finite():
            refusals["non_finite_source_value"] += 1
            continue
        by_key[(fact.instrument_id, fact.fiscal_year, quarter)].append(fact)

    selected: dict[tuple[int, int, int], ReportedFact] = {}
    for key, candidates in by_key.items():
        candidates.sort(key=lambda item: (item.filed_date, item.accession_number))
        if len(candidates) != 1:
            # Multiple original accessions for one fiscal slot are not silently
            # ordered into a truth.  They can be audited and fixed upstream.
            refusals["duplicate_fiscal_slot"] += 1
            continue
        selected[key] = candidates[0]

    observations: list[QuarterObservation] = []
    for (instrument_id, fiscal_year, quarter), fact in sorted(selected.items()):
        if quarter < 4:
            observations.append(
                QuarterObservation(
                    instrument_id=instrument_id,
                    fiscal_year=fiscal_year,
                    fiscal_quarter=quarter,
                    value=fact.values[0],
                    filed_date=fact.filed_date,
                    accepted_at=fact.accepted_at,
                    accession_number=fact.accession_number,
                    source_accessions=(fact.accession_number,),
                    derived_q4=False,
                )
            )
            continue

        legs = [selected.get((instrument_id, fiscal_year, item)) for item in (1, 2, 3)]
        if any(item is None for item in legs):
            refusals["q4_missing_quarter_leg"] += 1
            continue
        prior_legs = tuple(item for item in legs if item is not None)
        if any(item.filed_date >= fact.filed_date for item in prior_legs):
            refusals["q4_leg_not_known_at_annual_filing"] += 1
            continue
        value = fact.values[0] - sum((item.values[0] for item in prior_legs), start=Decimal("0"))
        observations.append(
            QuarterObservation(
                instrument_id=instrument_id,
                fiscal_year=fiscal_year,
                fiscal_quarter=4,
                value=value,
                filed_date=fact.filed_date,
                accepted_at=fact.accepted_at,
                accession_number=fact.accession_number,
                source_accessions=(fact.accession_number, *(item.accession_number for item in prior_legs)),
                derived_q4=True,
            )
        )
    observations.sort(key=lambda item: (item.instrument_id, item.fiscal_key, item.filed_date))
    return tuple(observations), refusals


def calculate_sue_events(
    observations: Sequence[QuarterObservation],
) -> tuple[tuple[SueEvent, ...], Counter[str]]:
    """Apply the frozen 21-difference seasonal random-walk model."""
    refusals: Counter[str] = Counter()
    by_instrument: dict[int, dict[int, QuarterObservation]] = defaultdict(dict)
    for observation in observations:
        if observation.fiscal_key in by_instrument[observation.instrument_id]:
            raise ValueError("construct_quarters emitted a duplicate fiscal key")
        by_instrument[observation.instrument_id][observation.fiscal_key] = observation

    events: list[SueEvent] = []
    for series in by_instrument.values():
        for key in sorted(series):
            required = range(key - REQUIRED_PRIOR_QUARTERS, key + 1)
            if any(item not in series for item in required):
                refusals["insufficient_consecutive_history"] += 1
                continue
            differences = [
                float(series[item].value - series[item - SEASONAL_LAG].value)
                for item in range(key - ESTIMATION_DIFFERENCES, key)
            ]
            if not all(isfinite(item) for item in differences):
                refusals["non_finite_forecast_error"] += 1
                continue
            drift = fmean(differences)
            dispersion = stdev(item - drift for item in differences)
            if not isfinite(drift) or not isfinite(dispersion):
                refusals["non_finite_forecast_error"] += 1
                continue
            if dispersion == 0:
                refusals["zero_forecast_error_dispersion"] += 1
                continue
            current = series[key]
            seasonal_change = float(current.value - series[key - SEASONAL_LAG].value)
            if not isfinite(seasonal_change):
                refusals["non_finite_forecast_error"] += 1
                continue
            events.append(SueEvent(observation=current, sue=(seasonal_change - drift) / dispersion))
    events.sort(key=lambda item: (item.observation.filed_date, item.observation.instrument_id))
    return tuple(events), refusals


def nearest_rank(values: Sequence[float], percentile: float) -> float:
    """Deterministic nearest-rank percentile; no interpolation or tuning."""
    if not values:
        raise ValueError("nearest_rank requires at least one value")
    if not 0 < percentile <= 1:
        raise ValueError("percentile must be in (0, 1]")
    ordered = sorted(values)
    return ordered[ceil(percentile * len(ordered)) - 1]


def classify_causal_triggers(events: Sequence[SueEvent]) -> tuple[tuple[TriggeredSueEvent, ...], Counter[str]]:
    """Rank against eight completed calendar quarters, never future peers."""
    refusals: Counter[str] = Counter()
    by_calendar_quarter: dict[int, list[float]] = defaultdict(list)
    for event in events:
        by_calendar_quarter[event.calendar_quarter].append(event.sue)

    classified: list[TriggeredSueEvent] = []
    for event in events:
        prior = [
            value
            for quarter in range(
                event.calendar_quarter - THRESHOLD_LOOKBACK_CALENDAR_QUARTERS,
                event.calendar_quarter,
            )
            for value in by_calendar_quarter.get(quarter, ())
        ]
        if len(prior) < MIN_THRESHOLD_EVENTS:
            refusals["thin_prior_cross_section"] += 1
            continue
        lower = nearest_rank(prior, 0.1)
        upper = nearest_rank(prior, 0.9)
        side = "long" if event.sue >= upper else "short" if event.sue <= lower else None
        classified.append(
            TriggeredSueEvent(
                event=event,
                lower_threshold=lower,
                upper_threshold=upper,
                side=side,
                threshold_population=len(prior),
            )
        )
    return tuple(classified), refusals


_FACT_SQL = """
    WITH eligible AS (
        SELECT f.instrument_id, f.accession_number, f.form_type, f.filed_date,
               f.fiscal_year, f.fiscal_period, f.period_start, f.period_end, f.val,
               max(f.period_end) OVER (PARTITION BY f.instrument_id, f.accession_number) AS latest_period_end
        FROM financial_facts_raw f
        JOIN research_price_series s
          ON s.instrument_id = f.instrument_id
         AND s.vendor = %(corpus_vendor)s
        WHERE f.taxonomy = %(taxonomy)s
          AND f.concept = %(concept)s
          AND f.unit = %(unit)s
          AND f.form_type IN ('10-Q', '10-K')
          AND f.period_start IS NOT NULL
          AND f.fiscal_year IS NOT NULL
          AND (
                (f.form_type = '10-Q' AND f.fiscal_period IN ('Q1','Q2','Q3')
                 AND f.period_end - f.period_start BETWEEN 70 AND 110)
             OR (f.form_type = '10-K' AND f.fiscal_period = 'FY'
                 AND f.period_end - f.period_start BETWEEN 300 AND 400)
          )
    ), manifest AS (
        SELECT instrument_id, accession_number, max(accepted_at) AS accepted_at
        FROM sec_filing_manifest
        WHERE form IN ('10-Q', '10-K')
        GROUP BY instrument_id, accession_number
    )
    SELECT e.instrument_id, e.accession_number, e.form_type, e.filed_date,
           m.accepted_at, e.fiscal_year, e.fiscal_period, e.period_end,
           array_agg(DISTINCT e.val ORDER BY e.val) AS values
    FROM eligible e
    LEFT JOIN manifest m
      ON m.instrument_id = e.instrument_id
     AND m.accession_number = e.accession_number
    WHERE e.period_end = e.latest_period_end
    GROUP BY e.instrument_id, e.accession_number, e.form_type, e.filed_date,
             m.accepted_at, e.fiscal_year, e.fiscal_period, e.period_end
    ORDER BY e.instrument_id, e.fiscal_year, e.fiscal_period, e.filed_date, e.accession_number
"""


def load_reported_facts(conn: psycopg.Connection[Any]) -> tuple[ReportedFact, ...]:
    rows = conn.execute(
        _FACT_SQL,
        {
            "corpus_vendor": CORPUS_VENDORS[0],
            "taxonomy": SOURCE_TAXONOMY,
            "concept": SOURCE_CONCEPT,
            "unit": SOURCE_UNIT,
        },
    ).fetchall()
    return tuple(
        ReportedFact(
            instrument_id=int(row[0]),
            accession_number=str(row[1]),
            form_type=str(row[2]),
            filed_date=row[3],
            accepted_at=row[4],
            fiscal_year=int(row[5]),
            fiscal_period=str(row[6]),
            period_end=row[7],
            values=tuple(Decimal(value) for value in row[8]),
        )
        for row in rows
    )


def _archive_sha256(archive_path: Path) -> str:
    sidecar = archive_path.with_name(archive_path.name + ".sha256")
    if not sidecar.is_file():
        raise FileNotFoundError(f"companyfacts integrity sidecar is missing: {sidecar}")
    value = sidecar.read_text().strip().lower()
    if len(value) != 64 or any(item not in "0123456789abcdef" for item in value):
        raise ValueError(f"companyfacts integrity sidecar is not a SHA-256: {sidecar}")
    with archive_path.open("rb") as handle:
        measured = hashlib.file_digest(handle, "sha256").hexdigest()
    if measured != value:
        raise ValueError(f"companyfacts archive SHA-256 mismatch: expected {value}, measured {measured}")
    return value


def load_archive_reported_facts(
    conn: psycopg.Connection[Any],
    archive_path: Path,
) -> ArchiveFactLoad:
    """Read the deep history transiently from the retained SEC bulk archive.

    The operational table intentionally keeps only three annual and eight
    quarterly accessions.  Relaxing that retention rule would bloat the hot
    database for a research-only need, so this loader streams the already
    cached public archive and retains only one declared concept in memory.
    """
    if not archive_path.is_file():
        raise FileNotFoundError(f"companyfacts archive is missing: {archive_path}")
    instrument_rows = conn.execute(
        """
        SELECT s.instrument_id, lpad(e.identifier_value, 10, '0') AS cik
        FROM research_price_series s
        JOIN external_identifiers e
          ON e.instrument_id = s.instrument_id
         AND e.provider = 'sec'
         AND e.identifier_type = 'cik'
        WHERE s.vendor = %s
        ORDER BY s.instrument_id, e.is_primary DESC, e.identifier_value
        """,
        (CORPUS_VENDORS[0],),
    ).fetchall()
    instrument_to_cik: dict[int, str] = {}
    ambiguous_instruments: set[int] = set()
    refusals: Counter[str] = Counter()
    for instrument_id_raw, cik_raw in instrument_rows:
        instrument_id = int(instrument_id_raw)
        cik = str(cik_raw)
        if instrument_id in ambiguous_instruments:
            continue
        existing = instrument_to_cik.get(instrument_id)
        if existing is not None and existing != cik:
            refusals["ambiguous_instrument_cik"] += 1
            instrument_to_cik.pop(instrument_id, None)
            ambiguous_instruments.add(instrument_id)
            continue
        instrument_to_cik[instrument_id] = cik
    by_cik: dict[str, list[int]] = defaultdict(list)
    for instrument_id, cik in instrument_to_cik.items():
        by_cik[cik].append(instrument_id)
    instrument_alternatives = {min(instrument_ids): tuple(sorted(instrument_ids)) for instrument_ids in by_cik.values()}
    refusals["share_class_source_duplicates_suppressed"] = sum(
        len(instrument_ids) - 1 for instrument_ids in by_cik.values()
    )

    manifest_rows = conn.execute(
        """
        SELECT instrument_id, accession_number, max(accepted_at)
        FROM sec_filing_manifest
        WHERE form IN ('10-Q', '10-K')
          AND instrument_id IS NOT NULL
        GROUP BY instrument_id, accession_number
        """
    ).fetchall()
    accepted_at = {(int(row[0]), str(row[1])): row[2] for row in manifest_rows}

    facts: list[ReportedFact] = []
    with zipfile.ZipFile(archive_path) as archive:
        names = set(archive.namelist())
        for cik, instrument_ids in sorted(by_cik.items()):
            representative = min(instrument_ids)
            entry_name = f"CIK{cik}.json"
            if entry_name not in names:
                refusals["cik_absent_from_companyfacts_archive"] += len(instrument_ids)
                continue
            try:
                with archive.open(entry_name) as handle:
                    payload = json.load(handle)
                raw_items = payload["facts"][SOURCE_TAXONOMY][SOURCE_CONCEPT]["units"][SOURCE_UNIT]
            except json.JSONDecodeError, KeyError, TypeError:
                refusals["declared_concept_absent_or_invalid"] += len(instrument_ids)
                continue

            grouped: dict[str, list[tuple[date, date, Decimal, date, int, str, str]]] = defaultdict(list)
            for raw in raw_items:
                try:
                    form_type = str(raw["form"])
                    fiscal_period = str(raw["fp"])
                    fiscal_year = int(raw["fy"])
                    period_start = date.fromisoformat(str(raw["start"]))
                    period_end = date.fromisoformat(str(raw["end"]))
                    filed_date = date.fromisoformat(str(raw["filed"]))
                    value = Decimal(str(raw["val"]))
                    accession = str(raw["accn"])
                except KeyError, TypeError, ValueError:
                    refusals["invalid_archive_fact_item"] += 1
                    continue
                duration = (period_end - period_start).days
                eligible = (form_type == "10-Q" and fiscal_period in {"Q1", "Q2", "Q3"} and 70 <= duration <= 110) or (
                    form_type == "10-K" and fiscal_period == "FY" and 300 <= duration <= 400
                )
                if not eligible or not value.is_finite():
                    continue
                grouped[accession].append(
                    (period_start, period_end, value, filed_date, fiscal_year, fiscal_period, form_type)
                )

            for accession, candidates in grouped.items():
                latest_end = max(item[1] for item in candidates)
                current = [item for item in candidates if item[1] == latest_end]
                metadata = {(item[3], item[4], item[5], item[6]) for item in current}
                if len(metadata) != 1:
                    refusals["ambiguous_archive_fact_metadata"] += len(instrument_ids)
                    continue
                filed_date, fiscal_year, fiscal_period, form_type = next(iter(metadata))
                values = tuple(sorted({item[2] for item in current}))
                known_acceptances = [
                    accepted
                    for instrument_id in instrument_ids
                    if (accepted := accepted_at.get((instrument_id, accession))) is not None
                ]
                facts.append(
                    ReportedFact(
                        instrument_id=representative,
                        accession_number=accession,
                        form_type=form_type,
                        filed_date=filed_date,
                        accepted_at=max(known_acceptances, default=None),
                        fiscal_year=fiscal_year,
                        fiscal_period=fiscal_period,
                        period_end=latest_end,
                        values=values,
                    )
                )
    facts.sort(
        key=lambda item: (
            item.instrument_id,
            item.fiscal_year,
            item.fiscal_period,
            item.filed_date,
            item.accession_number,
        )
    )
    return ArchiveFactLoad(
        facts=tuple(facts),
        archive_sha256=_archive_sha256(archive_path),
        instrument_alternatives=instrument_alternatives,
        refusals=dict(sorted(refusals.items())),
    )


def build_source(conn: psycopg.Connection[Any]) -> PeadSourceBuild:
    facts = load_reported_facts(conn)
    observations, quarter_refusals = construct_quarters(facts)
    events, sue_refusals = calculate_sue_events(observations)
    triggers, trigger_refusals = classify_causal_triggers(events)
    refusals = quarter_refusals + sue_refusals + trigger_refusals
    refusals["source_fact_groups"] = len(facts)
    refusals["quarter_observations"] = len(observations)
    refusals["sue_events"] = len(events)
    refusals["classified_events"] = len(triggers)
    refusals["long_triggers"] = sum(item.side == "long" for item in triggers)
    refusals["short_triggers"] = sum(item.side == "short" for item in triggers)
    refusals["accepted_at_present"] = sum(item.observation.accepted_at is not None for item in events)
    return PeadSourceBuild(
        observations=observations,
        sue_events=events,
        triggers=triggers,
        instrument_alternatives={item.instrument_id: (item.instrument_id,) for item in observations},
        refusals=dict(sorted(refusals.items())),
    )


def build_archive_source(conn: psycopg.Connection[Any], archive_path: Path) -> tuple[PeadSourceBuild, ArchiveFactLoad]:
    loaded = load_archive_reported_facts(conn, archive_path)
    observations, quarter_refusals = construct_quarters(loaded.facts)
    events, sue_refusals = calculate_sue_events(observations)
    triggers, trigger_refusals = classify_causal_triggers(events)
    refusals = Counter(loaded.refusals) + quarter_refusals + sue_refusals + trigger_refusals
    refusals["source_fact_groups"] = len(loaded.facts)
    refusals["quarter_observations"] = len(observations)
    refusals["sue_events"] = len(events)
    refusals["classified_events"] = len(triggers)
    refusals["long_triggers"] = sum(item.side == "long" for item in triggers)
    refusals["short_triggers"] = sum(item.side == "short" for item in triggers)
    refusals["accepted_at_present"] = sum(item.observation.accepted_at is not None for item in events)
    build = PeadSourceBuild(
        observations=observations,
        sue_events=events,
        triggers=triggers,
        instrument_alternatives=loaded.instrument_alternatives,
        refusals=dict(sorted(refusals.items())),
    )
    return build, loaded


def expand_instrument_alternatives(
    events: Sequence[TriggeredSueEvent],
    alternatives: Mapping[int, tuple[int, ...]],
) -> tuple[TriggeredSueEvent, ...]:
    """Fan issuer-level events out only after thresholds/control selection."""
    expanded: list[TriggeredSueEvent] = []
    for event in events:
        observation = event.event.observation
        for instrument_id in alternatives.get(observation.instrument_id, (observation.instrument_id,)):
            expanded.append(
                replace(
                    event,
                    event=replace(
                        event.event,
                        observation=replace(observation, instrument_id=instrument_id),
                    ),
                )
            )
    return tuple(expanded)


__all__ = [
    "ESTIMATION_DIFFERENCES",
    "MIN_THRESHOLD_EVENTS",
    "REQUIRED_PRIOR_QUARTERS",
    "PeadSourceBuild",
    "ArchiveFactLoad",
    "QuarterObservation",
    "ReportedFact",
    "SueEvent",
    "TriggeredSueEvent",
    "build_source",
    "build_archive_source",
    "calculate_sue_events",
    "classify_causal_triggers",
    "construct_quarters",
    "expand_instrument_alternatives",
    "load_reported_facts",
    "load_archive_reported_facts",
    "nearest_rank",
]
