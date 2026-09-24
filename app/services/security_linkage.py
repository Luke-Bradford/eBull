"""Dated research price-series ↔ CIK linkage with typed abstentions (#3361).

Spec: ``docs/proposals/ta/2026-09-24-3361-security-linkage.md``. The rule numbers cited
below ("rule 4", ...) are that spec's "Construction rules".

One question: for research price series S and decision date D, which CIK did the SEC Form
3/4/5 evidence **accepted strictly before D** name as S's issuer -- or a typed abstention.
It links a vendor file to an entity. It does not pick a primary class, merge or split
series, classify instrument type, or give a succession or delisting economic meaning.

Three pure halves live here:

* **Admission** (:func:`admit_row`, :func:`admit_observation`) -- every ``SUBMISSION.tsv``
  row gets exactly one outcome (rule 2), first match wins. The clock is the ISSUER CIK's
  ``submissions`` acceptance (#3360's :func:`~app.services.pit_fundamentals.normalise_acceptance`);
  ``FILING_DATE`` is never read (752 accessions were accepted after it --
  ``scripts/measure_3361_symbol_evidence.py``).
* **Grammar + matching** (:func:`parse_vendor_symbol`, :func:`match_set`) -- rules 3-4.
* **Reading** (:class:`SecurityLinkageBundle`) -- the policy-bound loader over the frozen
  artefact written by ``scripts/build_3361_security_linkage.py`` and the reader
  :meth:`~SecurityLinkageBundle.link_as_of` (rules 5-7).
"""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
import platform
import re
from bisect import bisect_left
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Any, Final, cast

from app.services.pit_fundamentals import (
    SubmissionsIndex,
    acceptance_ny_date,
    canonical_json,
    normalise_acceptance,
)
from app.services.r6_pit_bundle import read_verified_document
from app.services.universe_selection import EXCHANGE_TEST_ISSUE_SYMBOLS

MANIFEST_SCHEMA: Final = "security-linkage-manifest-v1"
SERIES_SCHEMA: Final = "security-linkage-series-v1"
MANIFEST_FILENAME: Final = "manifest.json"
LEDGER_FILENAME: Final = "ledger.json"
SERIES_DIRNAME: Final = "series"

#: Scope (spec "Contract"): every other vendor returns ``vendor_out_of_scope``.
IN_SCOPE_VENDOR: Final = "icyDenev/Intrader"

#: Rule 5 window, by construction: the #3360 census population window. No insider-cadence
#: rule exists to derive one; it bounds the age of the supporting evidence.
WINDOW_DAYS: Final = 730
#: The Insider Transactions Data Sets are served from 2006q1.
COVERAGE_START: Final = date(2006, 1, 1)
FIRST_QUARTER: Final = (2006, 1)

#: The four ``SUBMISSION.tsv`` columns read; everything else is inert.
REQUIRED_COLUMNS: Final = ("ACCESSION_NUMBER", "DOCUMENT_TYPE", "ISSUERCIK", "ISSUERTRADINGSYMBOL")
DOCUMENT_TYPES: Final = frozenset({"3", "3/A", "4", "4/A", "5", "5/A"})
#: Evidence-side conventions only; a vendor root is never tested against these (rule 2).
PLACEHOLDERS: Final = frozenset({"", "NONE", "N/A", "NA"})

_ACCESSION: Final = re.compile(r"[0-9]{10}-[0-9]{2}-[0-9]{6}")
_CIK: Final = re.compile(r"[0-9]{1,10}")
_MULTI: Final = re.compile(r"[,;]|\s")
_SEPARATORS: Final = re.compile(r"[./_-]")
_ROOT: Final = re.compile(r"[A-Z0-9]{1,5}")
_LETTER: Final = re.compile(r"[A-Z]")

#: Rule 3: the first suffix token that marks a non-common issue (preferred, warrant, unit,
#: right, when-distributed, called).
NON_COMMON_TOKENS: Final = frozenset({"P", "WS", "W", "U", "R", "WD", "CL"})
#: Rule 4 Q alias: the bankruptcy-suffix shape (``BBBYQ``, ``YELLQ``, ``SRNEQ``).
Q_ALIAS_ROOT_LENGTH: Final = 4


class RowOutcome(StrEnum):
    """Rule 2 row outcomes, in precedence order (first match wins)."""

    MALFORMED_ROW = "malformed_row"
    MALFORMED_CIK = "malformed_cik"
    UNSUPPORTED_DOCUMENT_TYPE = "unsupported_document_type"
    EMPTY_SYMBOL = "empty_symbol"
    MULTI_SYMBOL = "multi_symbol"
    ACCESSION_CONFLICT = "accession_conflict"
    ISSUER_INTEGRITY_EXCLUDED = "issuer_integrity_excluded"
    NO_ACCEPTANCE = "no_acceptance"
    STORED = "stored"


class Reason(StrEnum):
    """Rule 6 result reasons, in precedence order (first match wins)."""

    SERIES_NOT_IN_BUNDLE = "series_not_in_bundle"
    VENDOR_OUT_OF_SCOPE = "vendor_out_of_scope"
    AFTER_CAPTURE = "after_capture"
    OUTSIDE_SERIES = "outside_series"
    VENDOR_TEST_SYMBOL = "vendor_test_symbol"
    UNPARSED_SYMBOL_FORM = "unparsed_symbol_form"
    NON_COMMON_SYMBOL_FORM = "non_common_symbol_form"
    VENDOR_SYMBOL_COLLISION = "vendor_symbol_collision"
    BEFORE_COVERAGE = "before_coverage"
    NO_RECENT_EVIDENCE = "no_recent_evidence"
    CONFLICTING_EVIDENCE = "conflicting_evidence"
    LINKED = "linked"


class SecurityLinkageError(RuntimeError):
    """The bundle is absent, mutable, malformed, or bound to a different policy."""


# --------------------------------------------------------------------------- admission


def canonical_cik(raw: str) -> str | None:
    """Rule 2: 1-10 ASCII digits, non-zero -> 10-digit zero-padded; else None."""
    if _CIK.fullmatch(raw) is None or int(raw) == 0:
        return None
    return f"{int(raw):010d}"


def unify(symbol: str) -> str:
    """Rule 4: separators ``.`` ``-`` ``/`` ``_`` -> ``.``."""
    return _SEPARATORS.sub(".", symbol)


def evidence_symbol(raw: str | None) -> str | None:
    """Trim + upper, placeholder -> None (checked BEFORE unifying, so ``N/A`` is a
    placeholder rather than ``N.A``), then separator-unified."""
    text = (raw or "").strip().upper()
    return None if text in PLACEHOLDERS else unify(text)


@dataclass(frozen=True)
class Admitted:
    """A row that survived rules 2.1-2.5; ``key`` is the accession-level identity."""

    accession: str
    cik: str
    symbol: str
    document_type: str

    @property
    def key(self) -> tuple[str, str, str]:
        return (self.cik, self.symbol, self.document_type)


def admit_row(fields: Sequence[str], columns: Mapping[str, int]) -> Admitted | RowOutcome:
    """Rules 2.1-2.5 for one TSV row. ``columns`` maps header name -> index."""
    if len(fields) != len(columns):
        return RowOutcome.MALFORMED_ROW
    accession = fields[columns["ACCESSION_NUMBER"]]
    if _ACCESSION.fullmatch(accession) is None:
        return RowOutcome.MALFORMED_ROW
    cik = canonical_cik(fields[columns["ISSUERCIK"]])
    if cik is None:
        return RowOutcome.MALFORMED_CIK
    document_type = fields[columns["DOCUMENT_TYPE"]]
    if document_type not in DOCUMENT_TYPES:
        return RowOutcome.UNSUPPORTED_DOCUMENT_TYPE
    trimmed = fields[columns["ISSUERTRADINGSYMBOL"]].strip().upper()
    if trimmed in PLACEHOLDERS:
        return RowOutcome.EMPTY_SYMBOL
    if _MULTI.search(trimmed):
        return RowOutcome.MULTI_SYMBOL
    return Admitted(accession, cik, unify(trimmed), document_type)


def admit_observation(accession: str, index: SubmissionsIndex | str) -> tuple[RowOutcome, str | None]:
    """Rule 2 per-observation step: ``(outcome, acceptance)``; the acceptance (normalised UTC
    ISO) is set iff the outcome is ``stored``.

    ``index`` is the issuer CIK's parsed submissions, or its #3360 rule-5 failure reason.
    ⚠ Returned as a pair, not ``str | RowOutcome``: ``RowOutcome`` is a StrEnum, so an
    ``isinstance(result, str)`` test would be true for both arms.
    """
    if isinstance(index, str):
        return RowOutcome.ISSUER_INTEGRITY_EXCLUDED, None
    filing = index.filings.get(accession)
    if filing is None or filing.acceptance is None or filing.form.removesuffix("/A") not in {"3", "4", "5"}:
        return RowOutcome.NO_ACCEPTANCE, None
    return RowOutcome.STORED, filing.acceptance


# --------------------------------------------------------------------------- grammar


@dataclass(frozen=True)
class Grammar:
    """Rule 3 result. ``label`` is what every result from reason 5 on carries."""

    kind: str  # "plain" | "class" | "non_common" | "unparsed" | "test"
    root: str | None = None
    share_class: str | None = None
    token: str | None = None

    @property
    def label(self) -> str:
        if self.kind == "class":
            return f"class:{self.share_class}"
        if self.kind == "non_common":
            return f"non_common:{self.token}"
        return self.kind

    def to_json(self) -> dict[str, Any]:
        return {"kind": self.kind, "root": self.root, "share_class": self.share_class, "token": self.token}


def _is_test_symbol(text: str) -> bool:
    return text in EXCHANGE_TEST_ISSUE_SYMBOLS


def parse_vendor_symbol(vendor_symbol: str) -> Grammar:
    """Rule 3 (Intrader). Exchange test issues are checked on the full symbol and the root."""
    parts = vendor_symbol.split("_")
    root, tokens = parts[0], parts[1:]
    if _is_test_symbol(unify(vendor_symbol.strip().upper())) or _is_test_symbol(root.strip().upper()):
        return Grammar("test")
    if _ROOT.fullmatch(root) is None:
        return Grammar("unparsed")
    if not tokens:
        return Grammar("plain", root)
    if len(tokens) == 1 and _LETTER.fullmatch(tokens[0]) and tokens[0] not in {"P", "W", "U", "R"}:
        return Grammar("class", root, share_class=tokens[0])
    if tokens[0] in NON_COMMON_TOKENS:
        return Grammar("non_common", root, token=tokens[0])
    return Grammar("unparsed", root)


def match_set(grammar: Grammar) -> frozenset[str]:
    """Rule 4: the separator-unified evidence symbols that match; empty = no match defined."""
    if grammar.kind == "plain" and grammar.root is not None:
        aliases = {grammar.root + "Q"} if len(grammar.root) == Q_ALIAS_ROOT_LENGTH else set()
        return frozenset({grammar.root, *aliases})
    if grammar.kind == "class" and grammar.root is not None:
        return frozenset({f"{grammar.root}.{grammar.share_class}"})
    return frozenset()


def is_q_alias(grammar: Grammar, symbol: str) -> bool:
    return grammar.kind == "plain" and symbol != grammar.root


def collisions(match_sets: Mapping[int, frozenset[str]]) -> frozenset[int]:
    """Rule 4 collision guard: every series whose match set overlaps another's."""
    owners: dict[str, list[int]] = {}
    for series_id, symbols in match_sets.items():
        for symbol in symbols:
            owners.setdefault(symbol, []).append(series_id)
    return frozenset(s for ids in owners.values() if len(ids) > 1 for s in ids)


# --------------------------------------------------------------------------- policy


_REPO_ROOT: Final = Path(__file__).resolve().parents[2]
POLICY_FILES: Final = (
    "app/services/security_linkage.py",
    "scripts/build_3361_security_linkage.py",
    "app/services/pit_fundamentals.py",
    "app/services/universe_selection.py",
    "app/services/r6_pit_bundle.py",
    "scripts/build_2900_pit_bundle.py",
)


def policy_constants(*, form25: bool) -> dict[str, Any]:
    return {
        "coverage_start": COVERAGE_START.isoformat(),
        "document_types": sorted(DOCUMENT_TYPES),
        "first_quarter": list(FIRST_QUARTER),
        "form25_mode": form25,
        "in_scope_vendor": IN_SCOPE_VENDOR,
        "manifest_schema": MANIFEST_SCHEMA,
        "non_common_tokens": sorted(NON_COMMON_TOKENS),
        "placeholders": sorted(PLACEHOLDERS),
        "q_alias_root_length": Q_ALIAS_ROOT_LENGTH,
        "reason_precedence": [r.value for r in Reason],
        "required_columns": list(REQUIRED_COLUMNS),
        "row_outcome_precedence": [o.value for o in RowOutcome],
        "series_schema": SERIES_SCHEMA,
        "test_symbols": sorted(EXCHANGE_TEST_ISSUE_SYMBOLS),
        "window_days": WINDOW_DAYS,
    }


def policy_sha256(*, form25: bool) -> str:
    """``POLICY`` = constants + code + Python + tzdata (spec "Artefact")."""
    code = {}
    for relative in POLICY_FILES:
        with (_REPO_ROOT / relative).open("rb") as handle:
            code[relative] = hashlib.file_digest(handle, "sha256").hexdigest()
    document = {
        "code": code,
        "constants": policy_constants(form25=form25),
        "python": platform.python_version(),
        "tzdata": importlib.metadata.version("tzdata"),
    }
    return hashlib.sha256(canonical_json(document)).hexdigest()


# --------------------------------------------------------------------------- reading


@dataclass(frozen=True)
class Observation:
    accession: str
    cik: str
    acceptance: str
    multiplicity: int
    q_alias: bool


@dataclass(frozen=True)
class Form25Flag:
    accession: str
    filed_date: str
    issuer_cik: str
    match: str  # "symbol_match" | "symbol_other" | "symbol_null"


@dataclass(frozen=True)
class LinkResult:
    """Rule 6. ``grammar`` from reason 5 on; the evidence fields on reasons 10-12 only."""

    reason: Reason
    grammar: str | None = None
    detail: str | None = None  # non_common token, or "never_seen"
    cik: str | None = None
    basis: str | None = None  # "single_cik" | "succession"
    q_alias: bool = False
    observations: tuple[Observation, ...] = ()
    form25_unobserved: bool = False
    form25: tuple[Form25Flag, ...] = ()

    @property
    def label(self) -> str:
        return f"{self.reason.value}:{self.detail}" if self.detail else self.reason.value


@dataclass(frozen=True)
class _Series:
    series_id: int
    first_bar: date
    last_bar: date
    grammar: Grammar
    symbols: frozenset[str]
    collision: bool
    observations: tuple[Mapping[str, Any], ...]
    ny_dates: tuple[date, ...]
    form25: tuple[Mapping[str, Any], ...]


def decide(window: Sequence[Mapping[str, Any]]) -> tuple[Reason, str | None, str | None]:
    """Rule 5 on a non-empty window: (reason, cik, basis)."""
    spans: dict[str, tuple[str, str]] = {}
    for row in window:
        first, last = spans.get(row["cik"], (row["acceptance"], row["acceptance"]))
        spans[row["cik"]] = (min(first, row["acceptance"]), max(last, row["acceptance"]))
    if len(spans) == 1:
        return Reason.LINKED, next(iter(spans)), "single_cik"
    ordered = sorted(spans.items(), key=lambda item: (item[1][0], item[0]))
    if all(ordered[i][1][1] < ordered[i + 1][1][0] for i in range(len(ordered) - 1)):
        return Reason.LINKED, ordered[-1][0], "succession"
    return Reason.CONFLICTING_EVIDENCE, None, None


def form25_match(resolved_symbol: str | None, symbols: frozenset[str]) -> str:
    symbol = evidence_symbol(resolved_symbol)
    if symbol is None:
        return "symbol_null"
    return "symbol_match" if symbol in symbols else "symbol_other"


class SecurityLinkageBundle:
    """A verified manifest; series documents are verified lazily, each on first read."""

    def __init__(self, root: Path, manifest: Mapping[str, Any], manifest_sha256: str) -> None:
        self.root = root
        self.manifest_sha256 = manifest_sha256
        self.form25_mode: bool = manifest["form25_mode"]
        self.supported_through = date.fromisoformat(manifest["supported_through"])
        span = manifest["form25_span"]
        self.form25_span = None if span is None else (date.fromisoformat(span[0]), date.fromisoformat(span[1]))
        self._entries = {entry["series_id"]: entry for entry in manifest["series"]}
        self._cache: dict[int, _Series] = {}

    @property
    def series_ids(self) -> tuple[int, ...]:
        return tuple(self._entries)

    def link_as_of(self, series_id: int, decision: date) -> LinkResult:
        """Rules 5-7. Uses no evidence accepted on or after ``decision``, except through the
        snapshot-integrity mask (accession conflicts, issuer integrity, collisions)."""
        entry = self._entries.get(series_id)
        if entry is None:
            return LinkResult(Reason.SERIES_NOT_IN_BUNDLE)
        if entry["vendor"] != IN_SCOPE_VENDOR:
            return LinkResult(Reason.VENDOR_OUT_OF_SCOPE)
        if decision > self.supported_through:
            return LinkResult(Reason.AFTER_CAPTURE)
        series = self._series(series_id)
        if not series.first_bar <= decision <= series.last_bar:
            return LinkResult(Reason.OUTSIDE_SERIES)
        grammar = series.grammar
        label = grammar.label
        if grammar.kind == "test":
            return LinkResult(Reason.VENDOR_TEST_SYMBOL, label)
        if grammar.kind == "unparsed":
            return LinkResult(Reason.UNPARSED_SYMBOL_FORM, label)
        if grammar.kind == "non_common":
            return LinkResult(Reason.NON_COMMON_SYMBOL_FORM, label, detail=grammar.token)
        if series.collision:
            return LinkResult(Reason.VENDOR_SYMBOL_COLLISION, label)
        floor = decision - timedelta(days=WINDOW_DAYS)
        if floor < COVERAGE_START:
            return LinkResult(Reason.BEFORE_COVERAGE, label)

        # Observations are in acceptance order, so their NY dates are non-decreasing.
        public = bisect_left(series.ny_dates, decision)
        window = series.observations[bisect_left(series.ny_dates, floor) : public]
        observations = tuple(
            Observation(o["accession"], o["cik"], o["acceptance"], o["multiplicity"], is_q_alias(grammar, o["symbol"]))
            for o in window
        )
        ciks = {o.cik for o in observations}
        # Rule 6: outside the register's own [min, max](filed_date) the flags are unobserved.
        unobserved = self.form25_span is None or not self.form25_span[0] <= decision <= self.form25_span[1]
        flags = (
            ()
            if unobserved
            else tuple(
                Form25Flag(
                    r["accession"], r["filed_date"], r["issuer_cik"], form25_match(r["resolved_symbol"], series.symbols)
                )
                for r in series.form25
                if r["issuer_cik"] in ciks and date.fromisoformat(r["filed_date"]) < decision
            )
        )
        evidence: dict[str, Any] = {"observations": observations, "form25_unobserved": unobserved, "form25": flags}
        if not window:
            return LinkResult(Reason.NO_RECENT_EVIDENCE, label, detail=None if public else "never_seen", **evidence)
        reason, cik, basis = decide(window)
        if reason is Reason.CONFLICTING_EVIDENCE:
            return LinkResult(reason, label, **evidence)
        q_alias = any(o.q_alias for o in observations if o.cik == cik)
        return LinkResult(reason, label, cik=cik, basis=basis, q_alias=q_alias, **evidence)

    def _series(self, series_id: int) -> _Series:
        cached = self._cache.get(series_id)
        if cached is None:
            entry = self._entries[series_id]
            cached = self._cache[series_id] = load_series(self.root / entry["path"], entry)
        return cached

    def verify_all(self) -> None:
        for series_id, entry in self._entries.items():
            if entry["vendor"] == IN_SCOPE_VENDOR:
                self._series(series_id)


def load_security_linkage(root: Path, *, expected_manifest_sha256: str) -> SecurityLinkageBundle:
    digest, data = read_verified_document(root / MANIFEST_FILENAME)
    if digest != expected_manifest_sha256:
        raise SecurityLinkageError(f"manifest digest {digest} != pinned {expected_manifest_sha256}")
    manifest = _json_object(data, label="manifest")
    if manifest.get("schema") != MANIFEST_SCHEMA:
        raise SecurityLinkageError("manifest schema mismatch")
    mode = manifest.get("form25_mode")
    if not isinstance(mode, bool) or manifest.get("policy") != policy_sha256(form25=mode):
        raise SecurityLinkageError("manifest policy differs from this code's POLICY")
    try:
        date.fromisoformat(manifest["supported_through"])
        span = manifest["form25_span"]
        if span is not None:
            [date.fromisoformat(d) for d in span]
        entries = cast(list[dict[str, Any]], manifest["series"])
        ids = [entry["series_id"] for entry in entries]
        ledger = cast(dict[str, Any], manifest["ledger"])
    except (KeyError, TypeError, ValueError) as exc:
        raise SecurityLinkageError("manifest is missing a required field") from exc
    if ids != sorted(ids) or len(set(ids)) != len(ids) or not all(type(i) is int and i > 0 for i in ids):
        raise SecurityLinkageError("manifest series must be unique positive ids in order")
    for entry in entries:
        in_scope = entry.get("vendor") == IN_SCOPE_VENDOR
        expected = f"{SERIES_DIRNAME}/{entry['series_id']}.json" if in_scope else None
        if entry.get("path") != expected or (in_scope != isinstance(entry.get("sha256"), str)):
            raise SecurityLinkageError(f"series {entry['series_id']}: path must be {expected!r}")
    ledger_digest, _ = read_verified_document(root / LEDGER_FILENAME)
    if ledger.get("path") != LEDGER_FILENAME or ledger.get("sha256") != ledger_digest:
        raise SecurityLinkageError("ledger digest moved")
    return SecurityLinkageBundle(root, manifest, digest)


def load_series(path: Path, entry: Mapping[str, Any]) -> _Series:
    digest, data = read_verified_document(path)
    if digest != entry["sha256"]:
        raise SecurityLinkageError(f"series digest moved: {path}")
    return validate_series(_json_object(data, label=str(path)), series_id=entry["series_id"])


def observation_order(row: Mapping[str, Any]) -> tuple[str, str, str]:
    return (row["acceptance"], row["accession"], row["cik"])


def form25_order(row: Mapping[str, Any]) -> tuple[str, str]:
    return (row["filed_date"], row["accession"])


def validate_series(document: Mapping[str, Any], *, series_id: int) -> _Series:
    label = f"series {series_id}"
    if document.get("schema") != SERIES_SCHEMA or document.get("series_id") != series_id:
        raise SecurityLinkageError(f"{label}: schema or id mismatch")
    try:
        grammar = Grammar(**document["grammar"])
        if grammar != parse_vendor_symbol(document["vendor_symbol"]):
            raise SecurityLinkageError(f"{label}: grammar does not re-derive from the vendor symbol")
        symbols = match_set(grammar)
        first_bar = date.fromisoformat(document["first_bar"])
        last_bar = date.fromisoformat(document["last_bar"])
        observations = tuple(cast(list[dict[str, Any]], document["observations"]))
        form25 = tuple(cast(list[dict[str, Any]], document["form25"]))
        collision = document["collision"]
        _check_order(observations, observation_order, label=f"{label} observations")
        _check_order(form25, form25_order, label=f"{label} form25")
        if len({o["accession"] for o in observations}) != len(observations):
            raise SecurityLinkageError(f"{label}: an accession repeats")
        for o in observations:
            if normalise_acceptance(o["acceptance"]) != o["acceptance"] or canonical_cik(o["cik"]) != o["cik"]:
                bad = f"{o['acceptance']!r}/{o['cik']!r}"
                raise SecurityLinkageError(f"{label}: non-canonical acceptance or CIK: {bad}")
            if o["symbol"] not in symbols or type(o["multiplicity"]) is not int or o["multiplicity"] < 1:
                raise SecurityLinkageError(f"{label}: observation outside the match set or bad multiplicity")
        ciks = {o["cik"] for o in observations}
        for r in form25:
            date.fromisoformat(r["filed_date"])
            if r["issuer_cik"] not in ciks:
                raise SecurityLinkageError(f"{label}: Form 25 row for a CIK with no observation")
    except SecurityLinkageError:
        raise
    except (KeyError, TypeError, ValueError) as exc:
        raise SecurityLinkageError(f"{label}: malformed document") from exc
    if first_bar > last_bar or type(collision) is not bool or (collision and not symbols):
        raise SecurityLinkageError(f"{label}: bounds or collision flag malformed")
    ny_dates = tuple(acceptance_ny_date(o["acceptance"]) for o in observations)
    return _Series(series_id, first_bar, last_bar, grammar, symbols, collision, observations, ny_dates, form25)


def _check_order(
    rows: Sequence[Mapping[str, Any]], order: Callable[[Mapping[str, Any]], tuple[str, ...]], *, label: str
) -> None:
    keys = [order(row) for row in rows]
    if keys != sorted(keys) or len(set(keys)) != len(keys):
        raise SecurityLinkageError(f"{label} are not in total order, or repeat")


def _json_object(data: bytes, *, label: str) -> dict[str, Any]:
    try:
        value = json.loads(data.decode("utf-8"))
    except ValueError as exc:
        raise SecurityLinkageError(f"{label} must be a UTF-8 JSON document") from exc
    if not isinstance(value, dict):
        raise SecurityLinkageError(f"{label} must be a JSON object")
    return cast(dict[str, Any], value)
