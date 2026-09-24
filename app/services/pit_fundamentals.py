"""Point-in-time event store of raw companyfacts XBRL facts (#3360).

Spec: ``docs/proposals/ta/2026-09-24-3360-companyfacts-pit-bundle.md``. The rule numbers
cited below ("rule 4", ...) are that spec's "Construction rules".

The bundle answers one question: for CIK C, concept K, unit U and period (start, end),
what value(s) had admitted periodic reports put on EDGAR, **as accepted strictly before
decision session D** -- or a typed absence. It defines no economic field: aliasing,
derivation, period alignment and staleness are each research arm's declared construction.

Two halves live here, both pure:

* **Admission** (:func:`build_shard`) -- every raw companyfacts row for the concept set
  gets exactly one outcome (rule 4), first match wins. Acceptance comes from
  ``submissions`` (sec-edgar skill §7.4 pages, §7.8 UTC -> New York); companyfacts
  ``filed`` is never a clock (281 accessions were accepted AFTER their ``filed`` date --
  ``scripts/measure_3360_companyfacts_acceptance.py``).
* **Reading** (:class:`PitFundamentalsBundle`) -- a policy-bound loader over the frozen
  artefact written by ``scripts/build_3360_pit_fundamentals.py``, plus the value reader
  (rule 8) and prefix reader (rule 9).
"""

from __future__ import annotations

import hashlib
import importlib.metadata
import importlib.resources
import json
import platform
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from pathlib import Path, PurePosixPath
from typing import Any, Final, cast
from zoneinfo import ZoneInfo

from app.providers.implementations.sec_fundamentals import (
    _PERIOD_MAX,
    _PERIOD_MIN,
    _UNIT_PRIORITY,
    _extract_facts_from_section,
)
from app.services.r6_pit_bundle import read_verified_document

SHARD_SCHEMA: Final = "pit-fundamentals-shard-v1"
MANIFEST_SCHEMA: Final = "pit-fundamentals-manifest-v1"
MANIFEST_FILENAME: Final = "manifest.json"
SHARDS_DIRNAME: Final = "shards"

#: Rule 1. Storing a concept asserts nothing about its economic scope.
CONCEPT_SET: Final[tuple[tuple[str, str], ...]] = (
    ("dei", "EntityCommonStockSharesOutstanding"),
    ("us-gaap", "Assets"),
    ("us-gaap", "CommonStockSharesOutstanding"),
    ("us-gaap", "CostOfGoodsAndServicesSold"),
    ("us-gaap", "CostOfGoodsSold"),
    ("us-gaap", "CostOfRevenue"),
    ("us-gaap", "GrossProfit"),
    ("us-gaap", "NetCashProvidedByUsedInOperatingActivities"),
    ("us-gaap", "NetIncomeLoss"),
    ("us-gaap", "RevenueFromContractWithCustomerExcludingAssessedTax"),
    ("us-gaap", "RevenueFromContractWithCustomerIncludingAssessedTax"),
    ("us-gaap", "Revenues"),
    ("us-gaap", "SalesRevenueNet"),
    ("us-gaap", "StockholdersEquity"),
    ("us-gaap", "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"),
)
_CONCEPTS: Final = frozenset(CONCEPT_SET)

#: Policy, not an audit-status claim (operator steer 2026-08-22: audited periodic accounts
#: stay in scope for valuation). Form is taken from SUBMISSIONS, authoritative for the
#: accession.
ADMITTED_FORMS: Final = frozenset(
    form + suffix for form in ("10-K", "10-KT", "10-Q", "10-QT", "20-F", "40-F") for suffix in ("", "/A")
)

#: Columns every submissions block must carry (rule 5). ``items`` is read only for the
#: cross-page conflict check.
REQUIRED_SUBMISSION_COLUMNS: Final = ("accessionNumber", "acceptanceDateTime", "form", "items")


class Outcome(StrEnum):
    """Rule 4 row outcomes, in precedence order (first match wins)."""

    MALFORMED_STRUCTURE = "malformed_structure"
    OUT_OF_SCOPE_FORM = "out_of_scope_form"
    NO_ACCEPTANCE = "no_acceptance"
    FORM_MISMATCH = "form_mismatch"
    CHOKEPOINT_REJECT = "chokepoint_reject"
    UNIT_OUTSIDE_POLICY = "unit_outside_policy"
    PROSPECTIVE_PERIOD = "prospective_period"
    STORED = "stored"


#: Rejections that are dated and keyed and that the reader honours (rule 8).
#: ⚠ ``form_mismatch`` is not in the spec's rule-4 list but the spec says the row is
#: "rejected"; it is made BLOCKING because a non-blocking rejection on an admitted
#: accession would let the reader fall back past that filing to an older value -- the
#: one thing rule 8 forbids.
BLOCKING: Final = frozenset({Outcome.FORM_MISMATCH, Outcome.CHOKEPOINT_REJECT, Outcome.PROSPECTIVE_PERIOD})


class ReadStatus(StrEnum):
    AFTER_CAPTURE = "after_capture"
    CIK_NOT_IN_BUNDLE = "cik_not_in_bundle"
    CIK_INTEGRITY_EXCLUDED = "cik_integrity_excluded"
    CONCEPT_NOT_IN_POLICY = "concept_not_in_policy"
    ABSENT = "absent"
    BLOCKED_BY_REJECTION = "blocked_by_rejection"
    AMBIGUOUS = "ambiguous"
    VALUE = "value"
    OK = "ok"  # prefix reader only


class PitFundamentalsError(RuntimeError):
    """The bundle is absent, mutable, malformed, or bound to a different policy."""


# --------------------------------------------------------------------------- clocks


def _new_york() -> ZoneInfo:
    # Rule 7: the zone comes from the pinned ``tzdata`` package file, never the system TZ path.
    with importlib.resources.files("tzdata.zoneinfo").joinpath("America/New_York").open("rb") as handle:
        return ZoneInfo.from_file(handle, key="America/New_York")


NEW_YORK: Final = _new_york()


def normalise_acceptance(raw: object) -> str | None:
    """Submissions ``acceptanceDateTime`` -> fixed-width UTC ISO with ``Z``, or None.

    Fixed millisecond width makes lexicographic order chronological, which the total order
    (rule 10) and the reader's "maximal acceptance" rely on.
    """
    if not isinstance(raw, str) or not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def acceptance_ny_date(acceptance: str) -> date:
    return datetime.fromisoformat(acceptance).astimezone(NEW_YORK).date()


def canonical_decimal(value: Decimal) -> str:
    """Rule 3: exact positional rendering from ``as_tuple()``; no context arithmetic."""
    sign, digits, exponent = value.as_tuple()
    if not isinstance(exponent, int):
        raise ValueError(f"non-finite decimal: {value}")
    coefficient = "".join(map(str, digits)).lstrip("0")
    if not coefficient:
        return "0"
    stripped = coefficient.rstrip("0")
    exponent += len(coefficient) - len(stripped)
    if exponent >= 0:
        body = stripped + "0" * exponent
    elif len(stripped) > -exponent:
        body = f"{stripped[:exponent]}.{stripped[exponent:]}"
    else:
        body = "0." + "0" * (-exponent - len(stripped)) + stripped
    return ("-" if sign else "") + body


# --------------------------------------------------------------------------- submissions


@dataclass(frozen=True)
class Filing:
    accn: str
    form: str
    acceptance: str | None  # normalised; None when absent/unparseable


@dataclass(frozen=True)
class SubmissionsIndex:
    filings: Mapping[str, Filing]


def parse_submissions(cik10: str, main: object, read_page: Callable[[str], object | None]) -> SubmissionsIndex | str:
    """Rules 2 + 5: an index of the CIK's filings, or the integrity-failure reason.

    ``read_page(name)`` returns the parsed page, or None when the archive lacks it.
    """
    if not isinstance(main, dict):
        return "submissions_malformed"
    main = cast(dict[str, Any], main)
    if cik_int(main.get("cik")) != int(cik10):
        return "submissions_cik_mismatch"
    filings = main.get("filings")
    if not isinstance(filings, dict):
        return "submissions_malformed"
    filings = cast(dict[str, Any], filings)
    blocks: list[object] = [filings.get("recent")]
    pages = filings.get("files") or []
    if not isinstance(pages, list):
        return "submissions_malformed"
    for page in cast(list[Any], pages):
        name = page.get("name") if isinstance(page, dict) else None
        body = read_page(name) if isinstance(name, str) else None
        if body is None:
            return "submissions_page_missing"
        blocks.append(body)
    seen: dict[str, tuple[object, object, object]] = {}
    for block in blocks:
        if not isinstance(block, dict):
            return "submissions_malformed"
        columns = [cast(dict[str, Any], block).get(name) for name in REQUIRED_SUBMISSION_COLUMNS]
        if not all(isinstance(column, list) for column in columns):
            return "submissions_column_missing"
        lists = cast(list[list[Any]], columns)
        if len({len(column) for column in lists}) != 1:
            return "submissions_column_length_mismatch"
        for accn, accepted, form, items in zip(*lists, strict=True):
            if not isinstance(accn, str) or not isinstance(form, str):
                return "submissions_malformed"
            record = (accepted, form, items)
            if seen.setdefault(accn, record) != record:
                return "submissions_accession_conflict"
    return SubmissionsIndex(
        {
            accn: Filing(accn, cast(str, form), normalise_acceptance(accepted))
            for accn, (accepted, form, _) in seen.items()
        }
    )


def cik_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int | Decimal):
        return int(value) if value == int(value) else None
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None


# --------------------------------------------------------------------------- admission


def ledger_key(taxonomy: str, concept: str, unit: str) -> str:
    return f"{taxonomy}/{concept}/{unit}"


#: Ledger unit label for a concept whose container is not an object of unit lists.
MALFORMED_CONTAINER_UNIT: Final = "<container>"


def _key_date(raw: object) -> str | None:
    if not isinstance(raw, str):
        return None
    try:
        return date.fromisoformat(raw).isoformat()
    except ValueError:
        return raw


@dataclass
class ShardBuild:
    shard: dict[str, Any]
    ledger: Counter[tuple[str, str]] = field(default_factory=Counter)  # (ledger_key, outcome|"raw")
    max_fact_acceptance: str | None = None


def iter_raw_rows(payload: Mapping[str, Any]) -> Any:
    """Yield ``(taxonomy, concept, unit, index, row)`` for every concept-set row.

    A container that is not an object of lists yields ONE ``(…, MALFORMED_CONTAINER_UNIT |
    unit, -1, None)`` so malformed shapes are counted rather than skipped.
    """
    facts = payload.get("facts")
    if not isinstance(facts, dict):
        return
    for taxonomy, concept in CONCEPT_SET:
        section = cast(dict[str, Any], facts).get(taxonomy)
        if not isinstance(section, dict) or concept not in section:
            continue
        body = cast(dict[str, Any], section)[concept]
        units = body.get("units") if isinstance(body, dict) else None
        if not isinstance(units, dict):
            yield taxonomy, concept, MALFORMED_CONTAINER_UNIT, -1, None
            continue
        for unit in sorted(cast(dict[str, Any], units)):
            rows = units[unit]
            if not isinstance(rows, list):
                yield taxonomy, concept, str(unit), -1, None
                continue
            for index, row in enumerate(cast(list[Any], rows)):
                yield taxonomy, concept, str(unit), index, row


def count_raw_rows(payload: Mapping[str, Any]) -> Counter[str]:
    """Raw rows per ledger key, counted WITHOUT the admission walk, so the builder's
    reconciliation (raw = Σ outcomes) compares two independent tallies."""
    counts: Counter[str] = Counter()
    facts = payload.get("facts")
    for taxonomy, concept in CONCEPT_SET:
        section = facts.get(taxonomy) if isinstance(facts, dict) else None
        body = section.get(concept) if isinstance(section, dict) else None
        if body is None:
            continue
        units = body.get("units") if isinstance(body, dict) else None
        if not isinstance(units, dict):
            counts[ledger_key(taxonomy, concept, MALFORMED_CONTAINER_UNIT)] += 1
            continue
        for unit, rows in units.items():
            counts[ledger_key(taxonomy, concept, str(unit))] += len(rows) if isinstance(rows, list) else 1
    return counts


def build_shard(cik10: str, payload: Mapping[str, Any], submissions: SubmissionsIndex) -> ShardBuild:
    """Rules 3, 4, 6, 10 for one CIK whose integrity (rule 5) already passed."""
    build = ShardBuild(shard={})
    events: dict[tuple[Any, ...], int] = Counter()
    rejections: dict[tuple[Any, ...], list[int]] = {}
    fact_accessions: set[str] = set()

    for taxonomy, concept, unit, index, row in iter_raw_rows(payload):
        lk = ledger_key(taxonomy, concept, unit)
        outcome, record = _admit(taxonomy, concept, unit, row, submissions)
        build.ledger[(lk, outcome.value)] += 1
        if outcome is Outcome.STORED:
            events[record] += 1
            fact_accessions.add(record[5])
        elif outcome in BLOCKING:
            rejections.setdefault((*record, outcome.value), []).append(index)

    admitted = sorted(
        (f for f in submissions.filings.values() if f.form in ADMITTED_FORMS and f.acceptance is not None),
        key=lambda f: (f.acceptance, f.accn),
    )
    event_rows = [
        {
            "taxonomy": k[0],
            "concept": k[1],
            "unit": k[2],
            "start": k[3],
            "end": k[4],
            "accn": k[5],
            "acceptance": k[6],
            "value": k[7],
            "multiplicity": n,
        }
        for k, n in events.items()
    ]
    rejection_rows = [
        {
            "taxonomy": k[0],
            "concept": k[1],
            "unit": k[2],
            "start": k[3],
            "end": k[4],
            "accn": k[5],
            "acceptance": k[6],
            "reason": k[7],
            "rows": sorted(rows),
        }
        for k, rows in rejections.items()
    ]
    event_rows.sort(key=_event_order)
    rejection_rows.sort(key=_rejection_order)
    build.shard = {
        "schema": SHARD_SCHEMA,
        "cik": cik10,
        "events": event_rows,
        "rejections": rejection_rows,
        "accessions": [{"accn": f.accn, "form": f.form, "acceptance": f.acceptance} for f in admitted],
    }
    accepted = [submissions.filings[a].acceptance for a in fact_accessions]
    build.max_fact_acceptance = max((a for a in accepted if a is not None), default=None)
    return build


def _admit(
    taxonomy: str, concept: str, unit: str, row: object, submissions: SubmissionsIndex
) -> tuple[Outcome, tuple[Any, ...]]:
    if not isinstance(row, dict) or unit == MALFORMED_CONTAINER_UNIT:
        return Outcome.MALFORMED_STRUCTURE, ()
    row = cast(dict[str, Any], row)
    accn = row.get("accn")
    if not isinstance(accn, str) or not accn:
        # Every later outcome is a property of the accession; without one the row can be
        # neither dated nor blocked.
        return Outcome.MALFORMED_STRUCTURE, ()
    filing = submissions.filings.get(accn)
    if filing is not None and filing.form not in ADMITTED_FORMS:
        return Outcome.OUT_OF_SCOPE_FORM, ()
    if filing is None or filing.acceptance is None:
        return Outcome.NO_ACCEPTANCE, ()
    acceptance = filing.acceptance
    raw_key = (taxonomy, concept, unit, _key_date(row.get("start")), _key_date(row.get("end")), accn, acceptance)
    form = row.get("form")
    if isinstance(form, str) and form != filing.form:
        return Outcome.FORM_MISMATCH, raw_key
    # The chokepoint only walks ``_UNIT_PRIORITY`` units, so a row in any other unit is
    # probed under a policy unit: rule 4 puts ``chokepoint_reject`` BEFORE
    # ``unit_outside_policy``, and the probe keeps that precedence honest.
    probe_unit = unit if unit in _UNIT_PRIORITY else _UNIT_PRIORITY[0]
    extracted = _extract_facts_from_section(
        {concept: {"units": {probe_unit: [row]}}},
        taxonomy=taxonomy,
        allowed_tags=frozenset({concept}),
        retention_cutoff=None,
    )
    if len(extracted) != 1:
        return Outcome.CHOKEPOINT_REJECT, raw_key
    if unit not in _UNIT_PRIORITY:
        return Outcome.UNIT_OUTSIDE_POLICY, ()
    fact = extracted[0]
    start = fact.period_start.isoformat() if fact.period_start else None
    end = fact.period_end.isoformat()
    key = (taxonomy, concept, unit, start, end, accn, acceptance)
    if fact.period_end > acceptance_ny_date(acceptance):
        return Outcome.PROSPECTIVE_PERIOD, key
    return Outcome.STORED, (*key, canonical_decimal(fact.val))


def _key_order(row: Mapping[str, Any]) -> tuple[Any, ...]:
    start = row["start"]
    return (row["taxonomy"], row["concept"], row["unit"], start is not None, start or "", row["end"] or "")


def _event_order(row: Mapping[str, Any]) -> tuple[Any, ...]:
    return (*_key_order(row), row["acceptance"], row["accn"], row["value"])


def _rejection_order(row: Mapping[str, Any]) -> tuple[Any, ...]:
    return (*_key_order(row), row["acceptance"], row["accn"], row["reason"])


def _accession_order(row: Mapping[str, Any]) -> tuple[Any, ...]:
    return (row["acceptance"], row["accn"])


# --------------------------------------------------------------------------- policy


_REPO_ROOT: Final = Path(__file__).resolve().parents[2]
POLICY_FILES: Final = (
    "app/services/pit_fundamentals.py",
    "scripts/build_3360_pit_fundamentals.py",
    "app/providers/implementations/sec_fundamentals.py",
    "app/services/r6_pit_bundle.py",
    "scripts/build_2900_pit_bundle.py",
)


def policy_constants() -> dict[str, Any]:
    return {
        "admitted_forms": sorted(ADMITTED_FORMS),
        "blocking": sorted(BLOCKING),
        "concept_set": [list(pair) for pair in CONCEPT_SET],
        "manifest_schema": MANIFEST_SCHEMA,
        "outcome_precedence": [o.value for o in Outcome],
        "parser_window": [_PERIOD_MIN.isoformat(), _PERIOD_MAX.isoformat()],
        "required_submission_columns": list(REQUIRED_SUBMISSION_COLUMNS),
        "shard_schema": SHARD_SCHEMA,
        "units": list(_UNIT_PRIORITY),
    }


def policy_sha256() -> str:
    """``POLICY`` = constants + code + Python + tzdata (spec "Identity")."""
    code = {}
    for relative in POLICY_FILES:
        with (_REPO_ROOT / relative).open("rb") as handle:
            code[relative] = hashlib.file_digest(handle, "sha256").hexdigest()
    document = {
        "code": code,
        "constants": policy_constants(),
        "python": platform.python_version(),
        "tzdata": importlib.metadata.version("tzdata"),
    }
    return hashlib.sha256(canonical_json(document)).hexdigest()


def canonical_json(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


# --------------------------------------------------------------------------- reading


@dataclass(frozen=True)
class FactKey:
    taxonomy: str
    concept: str
    unit: str
    start: str | None
    end: str


@dataclass(frozen=True)
class ValueRead:
    status: ReadStatus
    values: tuple[str, ...] = ()
    accns: tuple[str, ...] = ()
    acceptance: str | None = None


@dataclass(frozen=True)
class PrefixRead:
    status: ReadStatus
    events: tuple[Mapping[str, Any], ...] = ()
    rejections: tuple[Mapping[str, Any], ...] = ()
    accessions: tuple[Mapping[str, Any], ...] = ()


@dataclass(frozen=True)
class _Shard:
    events: tuple[Mapping[str, Any], ...]
    rejections: tuple[Mapping[str, Any], ...]
    accessions: tuple[Mapping[str, Any], ...]
    ny_dates: Mapping[str, date]  # acceptance -> NY date
    by_key: Mapping[tuple[Any, ...], tuple[list[Mapping[str, Any]], list[Mapping[str, Any]]]]


class PitFundamentalsBundle:
    """A verified manifest; shards are verified lazily, each on first read."""

    def __init__(self, root: Path, manifest: Mapping[str, Any], manifest_sha256: str) -> None:
        self.root = root
        self.manifest_sha256 = manifest_sha256
        self.supported_through = date.fromisoformat(manifest["supported_through"])
        self.integrity_excluded = frozenset(entry["cik"] for entry in manifest["snapshot_integrity_failures"])
        self._entries = {entry["cik"]: entry for entry in manifest["shards"]}
        self._cache: dict[str, _Shard] = {}

    @property
    def ciks(self) -> tuple[str, ...]:
        return tuple(self._entries)

    def _gate(self, cik10: str, taxonomy: str, concept: str, decision: date) -> ReadStatus | None:
        if decision > self.supported_through:
            return ReadStatus.AFTER_CAPTURE
        if (taxonomy, concept) not in _CONCEPTS:
            return ReadStatus.CONCEPT_NOT_IN_POLICY
        if cik10 in self.integrity_excluded:
            return ReadStatus.CIK_INTEGRITY_EXCLUDED
        if cik10 not in self._entries:
            return ReadStatus.CIK_NOT_IN_BUNDLE
        return None

    def value_as_of(self, cik10: str, key: FactKey, decision: date) -> ValueRead:
        """Rule 8. Never falls back past the latest public acceptance for the key."""
        gated = self._gate(cik10, key.taxonomy, key.concept, decision)
        if gated is not None:
            return ValueRead(gated)
        shard = self._shard(cik10)
        all_events, all_blocks = shard.by_key.get((key.taxonomy, key.concept, key.unit, key.start, key.end), ([], []))
        events = [e for e in all_events if shard.ny_dates[e["acceptance"]] < decision]
        blocks = [r for r in all_blocks if shard.ny_dates[r["acceptance"]] < decision]
        if not events and not blocks:
            return ValueRead(ReadStatus.ABSENT)
        latest = max(row["acceptance"] for row in (*events, *blocks))
        if any(r["acceptance"] == latest for r in blocks):
            return ValueRead(ReadStatus.BLOCKED_BY_REJECTION, acceptance=latest)
        at_latest = [e for e in events if e["acceptance"] == latest]
        values = tuple(sorted({e["value"] for e in at_latest}))
        accns = tuple(sorted({e["accn"] for e in at_latest}))
        status = ReadStatus.VALUE if len(values) == 1 else ReadStatus.AMBIGUOUS
        return ValueRead(status, values, accns, latest)

    def public_events(self, cik10: str, taxonomy: str, concept: str, decision: date) -> PrefixRead:
        """Rule 9. Everything public for the concept; nothing ranked or selected."""
        gated = self._gate(cik10, taxonomy, concept, decision)
        if gated is not None:
            return PrefixRead(gated)
        shard = self._shard(cik10)

        def public(row: Mapping[str, Any]) -> bool:
            return shard.ny_dates[row["acceptance"]] < decision

        return PrefixRead(
            ReadStatus.OK,
            tuple(e for e in shard.events if (e["taxonomy"], e["concept"]) == (taxonomy, concept) and public(e)),
            tuple(r for r in shard.rejections if (r["taxonomy"], r["concept"]) == (taxonomy, concept) and public(r)),
            tuple(a for a in shard.accessions if public(a)),
        )

    def _shard(self, cik10: str) -> _Shard:
        cached = self._cache.get(cik10)
        if cached is None:
            entry = self._entries[cik10]
            cached = self._cache[cik10] = load_shard(self.root / entry["path"], entry)
        return cached

    def verify_all(self) -> None:
        for cik10 in self._entries:
            self._shard(cik10)


def _row_key(row: Mapping[str, Any]) -> tuple[Any, ...]:
    return (row["taxonomy"], row["concept"], row["unit"], row["start"], row["end"])


def load_pit_fundamentals(root: Path, *, expected_manifest_sha256: str) -> PitFundamentalsBundle:
    digest, data = read_verified_document(root / MANIFEST_FILENAME)
    if digest != expected_manifest_sha256:
        raise PitFundamentalsError(f"manifest digest {digest} != pinned {expected_manifest_sha256}")
    manifest = _json_object(data, label="manifest")
    if manifest.get("schema") != MANIFEST_SCHEMA:
        raise PitFundamentalsError("manifest schema mismatch")
    if manifest.get("policy") != policy_sha256():
        raise PitFundamentalsError("manifest policy differs from this code's POLICY")
    try:
        date.fromisoformat(manifest["supported_through"])
        shards = cast(list[dict[str, Any]], manifest["shards"])
        failures = cast(list[dict[str, Any]], manifest["snapshot_integrity_failures"])
        ciks = [entry["cik"] for entry in shards]
        paths = [entry["path"] for entry in shards]
        excluded = [entry["cik"] for entry in failures]
    except (KeyError, TypeError, ValueError) as exc:
        raise PitFundamentalsError("manifest is missing a required field") from exc
    if ciks != sorted(ciks) or len(set(ciks)) != len(ciks) or len(set(paths)) != len(paths):
        raise PitFundamentalsError("manifest shards must be unique and ordered by CIK")
    if set(ciks) & set(excluded):
        raise PitFundamentalsError("a CIK is both sharded and integrity-excluded")
    for entry in shards:
        expected = f"{SHARDS_DIRNAME}/CIK{entry['cik']}.json"
        if entry["path"] != expected or PurePosixPath(entry["path"]).name != f"CIK{entry['cik']}.json":
            raise PitFundamentalsError(f"shard path must be {expected}: {entry['path']!r}")
    return PitFundamentalsBundle(root, manifest, digest)


def load_shard(path: Path, entry: Mapping[str, Any]) -> _Shard:
    digest, data = read_verified_document(path)
    if digest != entry["sha256"]:
        raise PitFundamentalsError(f"shard digest moved: {path}")
    shard = _json_object(data, label=str(path))
    return validate_shard(shard, cik10=entry["cik"], expected_events=entry["events"])


def validate_shard(shard: Mapping[str, Any], *, cik10: str, expected_events: int) -> _Shard:
    if shard.get("schema") != SHARD_SCHEMA or shard.get("cik") != cik10:
        raise PitFundamentalsError(f"shard {cik10}: schema or cik mismatch")
    try:
        events = tuple(cast(list[dict[str, Any]], shard["events"]))
        rejections = tuple(cast(list[dict[str, Any]], shard["rejections"]))
        accessions = tuple(cast(list[dict[str, Any]], shard["accessions"]))
        _check_order(events, _event_order, unique=True, label="events")
        _check_order(rejections, _rejection_order, unique=True, label="rejections")
        _check_order(accessions, _accession_order, unique=True, label="accessions")
        ny_dates: dict[str, date] = {}
        for row in (*events, *rejections, *accessions):
            acceptance = row["acceptance"]
            if normalise_acceptance(acceptance) != acceptance:
                raise PitFundamentalsError(f"shard {cik10}: non-canonical acceptance {acceptance!r}")
            ny_dates.setdefault(acceptance, acceptance_ny_date(acceptance))
        by_accn = {a["accn"]: a["acceptance"] for a in accessions}
        if len(by_accn) != len(accessions) or any(a["form"] not in ADMITTED_FORMS for a in accessions):
            raise PitFundamentalsError(f"shard {cik10}: accession index malformed")
        for event in events:
            value = Decimal(event["value"])
            if not value.is_finite() or canonical_decimal(value) != event["value"]:
                raise PitFundamentalsError(f"shard {cik10}: non-canonical value {event['value']!r}")
            if not isinstance(event["multiplicity"], int) or event["multiplicity"] < 1:
                raise PitFundamentalsError(f"shard {cik10}: bad multiplicity")
        for row in (*events, *rejections):
            if by_accn.get(row["accn"]) != row["acceptance"]:
                raise PitFundamentalsError(f"shard {cik10}: row accession not in the admitted index")
        for rejection in rejections:
            if rejection["reason"] not in BLOCKING:
                raise PitFundamentalsError(f"shard {cik10}: non-blocking rejection stored")
    except PitFundamentalsError:
        raise
    except (KeyError, TypeError, ValueError, InvalidOperation) as exc:
        raise PitFundamentalsError(f"shard {cik10}: malformed row") from exc
    if len(events) != expected_events:
        raise PitFundamentalsError(f"shard {cik10}: {len(events)} events, manifest says {expected_events}")
    by_key: dict[tuple[Any, ...], tuple[list[Mapping[str, Any]], list[Mapping[str, Any]]]] = {}
    for event in events:
        by_key.setdefault(_row_key(event), ([], []))[0].append(event)
    for rejection in rejections:
        by_key.setdefault(_row_key(rejection), ([], []))[1].append(rejection)
    return _Shard(events, rejections, accessions, ny_dates, by_key)


def _check_order(
    rows: Sequence[Mapping[str, Any]],
    order: Callable[[Mapping[str, Any]], tuple[Any, ...]],
    *,
    unique: bool,
    label: str,
) -> None:
    keys = [order(row) for row in rows]
    if keys != sorted(keys) or (unique and len(set(keys)) != len(keys)):
        raise PitFundamentalsError(f"{label} are not in the rule-10 total order, or repeat")


def _json_object(data: bytes, *, label: str) -> dict[str, Any]:
    try:
        value = json.loads(data.decode("utf-8"))
    except ValueError as exc:
        raise PitFundamentalsError(f"{label} must be a UTF-8 JSON document") from exc
    if not isinstance(value, dict):
        raise PitFundamentalsError(f"{label} must be a JSON object")
    return cast(dict[str, Any], value)
