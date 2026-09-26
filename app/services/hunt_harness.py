"""The pattern-hunt harness: trial identity, registration before the number, and refusals.

#3385, spec ``docs/proposals/ta/2026-09-26-3385-hunt-harness.md`` (v5). This module is
slice 2a: ``TrialSpec`` and its two hashes, the identity binding, the programme lock,
``evaluate``'s registration order for DISCOVERY, the refusals that cost no search, the
budget and closure constants, and recording a look taken outside the harness.

What is NOT here yet, and how it fails meanwhile:

- **The evaluator** (slice 3). ``evaluate`` takes the computation as ``compute``; slice 3b
  supplies the frozen arm-minus-control evaluator. Its inference (slice 3a) is
  ``hunt_inference``; its code, and the shared statistics it calls, are hashed into
  ``HUNT_HARNESS_MODEL_ID``. Nothing in production can register a search today
  regardless: ``HUNT_BUDGETS`` is empty (a hunt with no budget refuses registration)
  and only ``real_stock_long_x1`` is priced (``HUNT_TARIFF``, re-fetched 2026-09-26).
- **The audited door** for validation and holdout (slice 2b). Their freezes need slice 3's
  M, V[SR] and BY numbers, so the door ships after it. Until then ``evaluate`` refuses
  both splits with ``door_unavailable`` before registering.

⚠⚠ THE LOG COMES BEFORE THE NUMBER. A discovery search is committed to ``hunt_trials``
before ``compute`` reads a single price, so a crash, a refusal or an abandoned run is
still counted in M. A statistical refusal is an outcome; an exception is not (it
propagates, writes no outcome, and the registration is reused on retry).

⚠ The programme lock is a SESSION advisory lock, non-re-entrant by a ContextVar: a nested
acquire on one session silently double-counts (prevention log, #2964), and a nested
acquire on a second connection in the same context would wait on itself forever.
"""

from __future__ import annotations

import ast
import hashlib
import json
import logging
import math
import re
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from types import MappingProxyType
from typing import Any, Final, Literal, get_args

import psycopg
from psycopg.pq import TransactionStatus

from app.services import deflated_sharpe, hunt_evaluator, hunt_inference, hunt_view, market_calendar, r6_monthly_trial
from app.services.cost_model import COST_MODEL_ID
from app.services.indicator_series import Universe
from app.services.r6_exclusion_trial import PROGRAMME_POLICIES, termination_identity
from app.services.strategies.validated_universe import load_validated_universe
from app.services.strategy_result import HOLDOUT_BOUNDARY
from app.services.universe_selection import UNIVERSE_SELECTION_RULE_VERSION, load_universe_selection

_LOG = logging.getLogger(__name__)

Split = Literal["discovery", "validation", "holdout"]
Point = Literal["open", "close"]
Lane = Literal["real_stock_long_x1", "stock_cfd_long_x1", "stock_cfd_short_x1", "index_cfd_hedge"]
SurvivorBias = Literal["favours_arm", "favours_control", "unknown"]
Purpose = Literal["evaluate", "recorded_after"]
RefusalReason = Literal[
    "identity_mismatch",
    "unpriced_lane",
    "refused_timeline",
    "split_burned",
    "hunt_closed",
    "discovery_closed",
    "door_unavailable",
    "no_budget",
    "budget_exhausted",
    "candidate_owned_by_other_hunt",
]

SPLITS: Final[tuple[Split, ...]] = get_args(Split)
POINTS: Final[tuple[Point, ...]] = get_args(Point)
LANES: Final[tuple[Lane, ...]] = get_args(Lane)
SURVIVOR_BIAS: Final[tuple[SurvivorBias, ...]] = get_args(SurvivorBias)

# ---------------------------------------------------------------------------
# Model constants — every one enters HUNT_HARNESS_MODEL_ID
# ---------------------------------------------------------------------------

#: Split first sessions and last sessions. Holdout's end is frozen in its declaration.
SPLIT_BOUNDS: Final[Mapping[Split, tuple[date, date | None]]] = MappingProxyType(
    {
        "discovery": (date(1990, 1, 2), date(2008, 12, 31)),
        "validation": (date(2009, 1, 2), date(2021, 6, 28)),
        "holdout": (HOLDOUT_BOUNDARY, None),
    }
)
#: lag + h − 1 ≤ 63, so every label sits inside the 63-session embargo.
HORIZON_CAP_SESSIONS: Final = 63
EMBARGO_SESSIONS: Final = 63
MAX_SELECTION_FRACTION: Final = 0.5
#: #3384 Finding 1: every series trading before this date survives to at least it.
SURVIVOR_CONDITIONING_DATE: Final = date(2013, 6, 21)
#: The only lane v1 can price, and only once ``HUNT_TARIFF`` pins the re-fetched fees page.
PRICEABLE_LANE: Final[Lane] = "real_stock_long_x1"
HUNT_COST_MODEL_PREFIX: Final = "hunt-cost-v1"

_HEX64 = re.compile(r"^[0-9a-f]{64}$")
_HUNT_ID = re.compile(r"^hunt-[1-9][0-9]*$")
_FAMILY = re.compile(r"^[a-z][a-z0-9_]*$")
#: ``<module>:<callable>`` inside ``app/services/hunt_signals``.
_SIGNAL_ID = re.compile(r"^([a-z][a-z0-9_]*):([a-z_][a-z0-9_]*)$")
_FLOAT_TAG: Final = "__float__"


#: Modules whose CODE is part of the model: editing one is a new model id (spec job 3).
#: The shared statistics the inference calls are included, so a change to them cannot
#: return a cached outcome under an unchanged identity (Codex ckpt-2).
MODEL_CODE_MODULES: Final = (hunt_evaluator, hunt_inference, hunt_view, deflated_sharpe, r6_monthly_trial)


def _module_code_sha256(module: Any) -> str:
    return hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest()


def _model_constants() -> dict[str, Any]:
    return {
        "model_code_sha256": {module.__name__: _module_code_sha256(module) for module in MODEL_CODE_MODULES},
        "splits": {
            split: [start.isoformat(), end.isoformat() if end else None] for split, (start, end) in SPLIT_BOUNDS.items()
        },
        "horizon_cap_sessions": HORIZON_CAP_SESSIONS,
        "embargo_sessions": EMBARGO_SESSIONS,
        "max_selection_fraction": MAX_SELECTION_FRACTION,
        "survivor_conditioning_date": SURVIVOR_CONDITIONING_DATE.isoformat(),
        "points": list(POINTS),
        "weighting": ["equal"],
        "lanes": list(LANES),
        "priceable_lane": PRICEABLE_LANE,
    }


# ---------------------------------------------------------------------------
# Canonical JSON
# ---------------------------------------------------------------------------


def canonical_form(value: Any) -> Any:
    """``value`` as a JSON-able structure whose serialisation is unambiguous.

    Floats become ``{"__float__": repr}`` so a float never collides with a string or an
    integer, and ``repr`` is the shortest round-trip form. Non-finite floats, non-string
    keys and a mapping that itself uses ``__float__`` as a key are refused.
    """
    if value is None or isinstance(value, bool | str):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"non-finite float {value!r} has no canonical form")
        return {_FLOAT_TAG: repr(value)}
    if isinstance(value, Mapping):
        out: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError(f"canonical mapping keys must be str, got {type(key).__name__}")
            if key == _FLOAT_TAG:
                raise ValueError(f"the key {_FLOAT_TAG!r} is reserved for tagged floats")
            out[key] = canonical_form(item)
        return out
    if isinstance(value, tuple | list):
        return [canonical_form(item) for item in value]
    raise TypeError(f"{type(value).__name__} has no canonical form")


def decode_form(form: Any) -> Any:
    """Inverse of :func:`canonical_form` for a stored value (lists stay lists)."""
    if isinstance(form, Mapping):
        if set(form) == {_FLOAT_TAG}:
            return float(form[_FLOAT_TAG])
        return {key: decode_form(item) for key, item in form.items()}
    if isinstance(form, list):
        return [decode_form(item) for item in form]
    return form


def dumps_form(form: Any) -> str:
    """Serialise an ALREADY canonical form: sorted keys, compact separators."""
    return json.dumps(form, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def sha256_form(form: Any) -> str:
    return hashlib.sha256(dumps_form(form).encode("utf-8")).hexdigest()


HUNT_HARNESS_MODEL_ID: Final = f"hunt-harness-v1+{sha256_form(canonical_form(_model_constants()))[:16]}"


# ---------------------------------------------------------------------------
# Operational constants — reviewed-PR changes only, and never in any hash
# ---------------------------------------------------------------------------

#: Discovery rows per hunt, both purposes. Set once by the #3387 PR that opens the hunt;
#: raising it is a new hunt. A hunt with no entry refuses registration.
HUNT_BUDGETS: Final[Mapping[str, int]] = MappingProxyType({})
#: hunt → terminal readout. Adding a hunt here IS the closing event.
HUNT_CLOSED: Final[Mapping[str, str]] = MappingProxyType({})


@dataclass(frozen=True)
class HuntTariff:
    """The re-fetched eToro fees page that prices ``real_stock_long_x1`` (slice 3 pins it).

    Only proportional charges can be priced on a return series; a fixed, minimum,
    capped or currency-denominated fee leaves the lane unpriced (spec "Lanes").
    """

    url: str
    fetched_on: date
    text_sha256: str
    #: eToro's stock commission is set per country of residence (and exchange).
    residence_country: str
    account_currency: str
    proportional_commission_per_side: float

    def __post_init__(self) -> None:
        if not _HEX64.match(self.text_sha256):
            raise ValueError("text_sha256 must be a sha256 hex digest")
        c = self.proportional_commission_per_side
        if not (type(c) is float and math.isfinite(c) and 0.0 <= c < 1.0):
            raise ValueError(f"proportional_commission_per_side must be a float in [0, 1), got {c!r}")

    def cost_model_id(self) -> str:
        form = canonical_form(
            {
                "url": self.url,
                "fetched_on": self.fetched_on.isoformat(),
                "text_sha256": self.text_sha256,
                "residence_country": self.residence_country,
                "account_currency": self.account_currency,
                "proportional_commission_per_side": self.proportional_commission_per_side,
                "cost_model_id": COST_MODEL_ID,
            }
        )
        return f"{HUNT_COST_MODEL_PREFIX}+{sha256_form(form)[:16]}"


#: The fees page's "Stocks" section as rendered on 2026-09-26, captured in the browser with
#: the country selector's SELECTED option written first, then the Germany row from the same
#: fetch as a control (the selector was live, not a default). Edits to the rendered text:
#: the selector's own heading and option list removed, and U+00A0 no-break spaces written
#: as plain spaces. ``HUNT_TARIFF.text_sha256`` is its sha256, computed in the browser on
#: exactly this string; a test recomputes it here and checks the selected country is the
#: tariff's residence. The page's "0.15% of trade value" commission belongs to Stock Margin
#: (leverage), not this lane.
HUNT_TARIFF_EVIDENCE: Final = (
    "Selected country: United Kingdom\n"
    "Stocks\n\nA commission fee of $1 or $2 may apply when opening and closing a stock position, depending on "
    "your country of residence and the stock exchange on which the asset is traded.\n\nPositions opened before "
    "the fee implementation date in your country will not incur a fee when closing.\n\nStock commission fees in "
    "USD\n\nStock Exchange\nAustralia, Hong Kong, Dubai, Abu Dhabi, Tokyo exchanges\tAll other exchanges\n$0\t$0"
    "\n\nPlease note that commission fees:\n– Are calculated in USD, regardless of the base currency of the stock "
    "being traded\n– Do not apply to CFD positions\n– Do not apply to ETFs\n– Do not apply to Copy trading or "
    "Smart Portfolios\n– Do not apply to Recurring investment plans on opening a position (may apply on closing "
    "position)\n\n"
    "Control capture, same fetch:\nSelected country: Germany\n"
    "Stock Exchange\nAustralia, Hong Kong, Dubai, Abu Dhabi, Tokyo exchanges\tAll other exchanges\t"
    "Implementation date\n$2\t$1\t2/3/2025\n"
)

#: Re-fetched 2026-09-26 in a browser (the etoro-api skill's protocol: the portal blocks
#: curl). For a UK-resident account a US-listed stock pays $0 commission per side, so the
#: lane's only charge is the frozen half-spread band: proportional, and priceable. The
#: residence is the operator's (the eToro (UK) Ltd account; the tax ledger is HMRC's); the
#: USD account is ``cost_model.FX_EVIDENCE``'s measurement. ⚠ A $1/$2 fixed fee applies in
#: most other countries: a change of residence or of the page is a new cost identity, and
#: a fixed fee cannot be priced on a return series (spec "Lanes").
HUNT_TARIFF: HuntTariff | None = HuntTariff(
    url="https://www.etoro.com/trading/fees/",
    fetched_on=date(2026, 9, 26),
    text_sha256="2ca786e878ebb635386c305f06d6d1bd1928dc11710fdcf9221305ef364a218e",
    residence_country="United Kingdom",
    account_currency="USD",
    proportional_commission_per_side=0.0,
)


def running_cost_model_id(lane: Lane) -> str | None:
    """The live cost identity for ``lane``, or ``None`` when the lane is unpriced."""
    tariff = HUNT_TARIFF
    if lane != PRICEABLE_LANE or tariff is None or tariff.account_currency != "USD":
        return None
    return tariff.cost_model_id()


# ---------------------------------------------------------------------------
# The trial
# ---------------------------------------------------------------------------


def _freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, list | tuple):
        return tuple(_freeze(item) for item in value)
    return value


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def _is_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_text(value: object) -> bool:
    return isinstance(value, str) and value.strip() != ""


@dataclass(frozen=True)
class UniverseIdentity:
    """What the admitted population is, read from metadata only (no prices)."""

    universe: Universe
    selection_rule_version: str
    validated_ids_sha256: str
    archive_sha256: str
    termination_identity_sha256: str

    def __post_init__(self) -> None:
        _require(self.universe in get_args(Universe), f"unknown universe {self.universe!r}")
        _require(_is_text(self.selection_rule_version), "selection_rule_version must be non-empty text")
        for name in ("validated_ids_sha256", "archive_sha256", "termination_identity_sha256"):
            value = getattr(self, name)
            _require(isinstance(value, str) and bool(_HEX64.match(value)), f"{name} must be a sha256 hex digest")

    def form(self) -> dict[str, Any]:
        return {
            "universe": self.universe,
            "selection_rule_version": self.selection_rule_version,
            "validated_ids_sha256": self.validated_ids_sha256,
            "archive_sha256": self.archive_sha256,
            "termination_identity_sha256": self.termination_identity_sha256,
        }


#: What defines the hypothesis and its data. Relabelling (hunt, family, split, prose)
#: cannot make a candidate new.
CANDIDATE_FIELDS: Final = (
    "signal_id",
    "signal_code_sha256",
    "sign",
    "selection",
    "lag",
    "h",
    "entry_point",
    "exit_point",
    "constants",
    "weighting",
    "lane",
    "universe_identity",
    "calendar_identity",
    "cost_model_id",
    "harness_model_id",
)
SPEC_FIELDS: Final = (*CANDIDATE_FIELDS, "hunt_id", "family", "split")


@dataclass(frozen=True)
class TrialSpec:
    """One search. Validated on construction; nested values are frozen."""

    hunt_id: str
    family: str
    split: Split
    signal_id: str
    signal_code_sha256: str
    sign: int
    selection: float
    lag: int
    h: int
    entry_point: Point
    exit_point: Point
    constants: Mapping[str, Any]
    weighting: str
    lane: Lane
    universe_identity: UniverseIdentity
    calendar_identity: str
    cost_model_id: str
    harness_model_id: str
    survivor_bias_direction: SurvivorBias
    survivor_bias_reason: str
    mechanism: str
    competing_explanation: str
    #: The canonical form, serialised: every accessor decodes a fresh copy, so no caller
    #: can mutate what the hashes were computed from.
    _form_json: str = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        _require(isinstance(self.hunt_id, str) and bool(_HUNT_ID.match(self.hunt_id)), f"bad hunt_id {self.hunt_id!r}")
        _require(isinstance(self.family, str) and bool(_FAMILY.match(self.family)), f"bad family {self.family!r}")
        _require(self.split in SPLITS, f"bad split {self.split!r}")
        _require(
            isinstance(self.signal_id, str) and bool(_SIGNAL_ID.match(self.signal_id)),
            f"bad signal_id {self.signal_id!r}",
        )
        _require(
            isinstance(self.signal_code_sha256, str) and bool(_HEX64.match(self.signal_code_sha256)),
            "signal_code_sha256 must be a sha256 hex digest",
        )
        _require(_is_int(self.sign) and self.sign in (1, -1), f"sign must be the int +1 or -1, got {self.sign!r}")
        _require(
            type(self.selection) is float
            and math.isfinite(self.selection)
            and 0.0 < self.selection <= MAX_SELECTION_FRACTION,
            f"selection must be a float in (0, {MAX_SELECTION_FRACTION}], got {self.selection!r}",
        )
        _require(_is_int(self.lag) and self.lag >= 1, f"lag must be an int >= 1, got {self.lag!r}")
        _require(_is_int(self.h) and self.h >= 1, f"h must be an int >= 1, got {self.h!r}")
        _require(self.entry_point in POINTS, f"bad entry_point {self.entry_point!r}")
        _require(self.exit_point in POINTS, f"bad exit_point {self.exit_point!r}")
        _require(isinstance(self.constants, Mapping), "constants must be a mapping")
        _require(self.weighting == "equal", f"weighting must be 'equal' in v1, got {self.weighting!r}")
        _require(self.lane in LANES, f"bad lane {self.lane!r}")
        _require(isinstance(self.universe_identity, UniverseIdentity), "universe_identity must be a UniverseIdentity")
        for name in ("calendar_identity", "cost_model_id", "harness_model_id"):
            _require(_is_text(getattr(self, name)), f"{name} must be non-empty text")
        _require(
            self.survivor_bias_direction in SURVIVOR_BIAS,
            f"bad survivor_bias_direction {self.survivor_bias_direction!r}",
        )
        for name in ("survivor_bias_reason", "mechanism", "competing_explanation"):
            _require(_is_text(getattr(self, name)), f"{name} must be non-empty text")
        object.__setattr__(self, "constants", _freeze(self.constants))
        # Computing the form here validates the constants (types, finiteness, reserved key).
        object.__setattr__(self, "_form_json", dumps_form(self._build_form()))

    def _build_form(self) -> dict[str, Any]:
        return canonical_form(
            {
                "hunt_id": self.hunt_id,
                "family": self.family,
                "split": self.split,
                "signal_id": self.signal_id,
                "signal_code_sha256": self.signal_code_sha256,
                "sign": self.sign,
                "selection": self.selection,
                "lag": self.lag,
                "h": self.h,
                "entry_point": self.entry_point,
                "exit_point": self.exit_point,
                "constants": self.constants,
                "weighting": self.weighting,
                "lane": self.lane,
                "universe_identity": self.universe_identity.form(),
                "calendar_identity": self.calendar_identity,
                "cost_model_id": self.cost_model_id,
                "harness_model_id": self.harness_model_id,
                "survivor_bias_direction": self.survivor_bias_direction,
                "survivor_bias_reason": self.survivor_bias_reason,
                "mechanism": self.mechanism,
                "competing_explanation": self.competing_explanation,
            }
        )

    def form(self) -> dict[str, Any]:
        """The canonical form stored as ``hunt_trials.spec`` (a detached copy)."""
        return json.loads(self._form_json)

    @property
    def candidate_sha256(self) -> str:
        return candidate_sha256_of(self.form())

    @property
    def spec_sha256(self) -> str:
        return spec_sha256_of(self.form())


def candidate_sha256_of(form: Mapping[str, Any]) -> str:
    return sha256_form({name: form[name] for name in CANDIDATE_FIELDS})


def spec_sha256_of(form: Mapping[str, Any]) -> str:
    return sha256_form({name: form[name] for name in SPEC_FIELDS})


def timeline_refusal(spec: TrialSpec) -> str | None:
    """Why the spec's timeline cannot be evaluated, or ``None``.

    The hold must have positive duration (with h = 1 only open → close), and
    lag + h − 1 ≤ 63 keeps every label inside the embargo.
    """
    if spec.lag + spec.h - 1 > HORIZON_CAP_SESSIONS:
        return f"lag + h - 1 = {spec.lag + spec.h - 1} exceeds the {HORIZON_CAP_SESSIONS}-session horizon cap"
    if spec.h == 1 and (spec.entry_point, spec.exit_point) != ("open", "close"):
        return f"h = 1 needs a positive-duration hold (open -> close), got {spec.entry_point} -> {spec.exit_point}"
    return None


# ---------------------------------------------------------------------------
# Identity readers
# ---------------------------------------------------------------------------

#: Signals live here, one module per family (spec "Signals").
SIGNAL_PACKAGE_DIR: Path = Path(__file__).resolve().parent / "hunt_signals"


def signal_code_sha256(signal_id: str) -> str | None:
    """sha256 of the signal's module file, or ``None`` if it or the callable is missing."""
    match = _SIGNAL_ID.match(signal_id)
    if match is None:
        return None
    path = SIGNAL_PACKAGE_DIR / f"{match.group(1)}.py"
    if not path.is_file():
        return None
    source = path.read_bytes()
    tree = ast.parse(source)
    defined = {node.name for node in tree.body if isinstance(node, ast.FunctionDef)}
    if match.group(2) not in defined:
        return None
    return hashlib.sha256(source).hexdigest()


def calendar_identity() -> str:
    return market_calendar.RULE_SET_VERSION


#: Series metadata the archive identity covers. ⚠ Reads the dividend and split COLUMNS
#: only to count non-trivial rows; no price column is selected. The live bar count and
#: date range per series come from the same scan, so an ingest that has committed some
#: bar batches but not yet rewritten the series row still moves the identity (Codex
#: ckpt-2). ⚠ Residual: a rewrite that keeps every series' count and range unchanged is
#: not detected; archive re-ingests are attended corpus events, not scheduled jobs.
_ARCHIVE_METADATA_SQL = """
    SELECT s.series_id, s.vendor, s.vendor_symbol, s.adjustment_basis, s.first_bar, s.last_bar,
           s.bar_count, s.updated_at, s.corporate_action_stamps, s.delisting_source,
           s.delisting_provision, coalesce(d.dividend_rows, 0), coalesce(d.split_rows, 0),
           coalesce(d.bar_rows, 0), d.first_bar_date, d.last_bar_date
    FROM research_price_series s
    LEFT JOIN (
        SELECT series_id,
               count(*) AS bar_rows, min(bar_date) AS first_bar_date, max(bar_date) AS last_bar_date,
               count(*) FILTER (WHERE dividend IS NOT NULL AND dividend <> 0) AS dividend_rows,
               count(*) FILTER (WHERE split_factor IS NOT NULL AND split_factor <> 1) AS split_rows
        FROM research_price_daily
        WHERE series_id = ANY(%(ids)s)
        GROUP BY series_id
    ) d USING (series_id)
    WHERE s.series_id = ANY(%(ids)s)
    ORDER BY s.series_id
"""


def _jsonable_row(row: tuple[Any, ...]) -> list[Any]:
    return [value.isoformat() if hasattr(value, "isoformat") else value for value in row]


def read_universe_identity(conn: psycopg.Connection[Any], universe: Universe) -> UniverseIdentity:
    """The admitted population's identity, from metadata (spec "Contract decisions" 73, 77–78)."""
    validated = load_validated_universe(conn)
    selection = load_universe_selection(conn, universe=universe, validated_ids=frozenset(validated))
    admitted = sorted(series.series_id for series in selection.admitted)
    rows = conn.execute(_ARCHIVE_METADATA_SQL, {"ids": admitted}).fetchall()
    archive = [_jsonable_row(tuple(row)) for row in rows]
    termination = termination_identity(PROGRAMME_POLICIES, None)
    return UniverseIdentity(
        universe=universe,
        selection_rule_version=UNIVERSE_SELECTION_RULE_VERSION,
        validated_ids_sha256=sha256_form(list(validated)),
        archive_sha256=sha256_form(canonical_form(archive)),
        termination_identity_sha256=sha256_form(canonical_form(termination)),
    )


# ---------------------------------------------------------------------------
# The programme lock
# ---------------------------------------------------------------------------

#: (ticket, 1), the repo's two-int advisory key convention.
HUNT_PROGRAMME_LOCK: Final = (3385, 1)
_PROGRAMME_LOCK_HELD: ContextVar[bool] = ContextVar("_hunt_programme_lock_held", default=False)


class HuntHarnessError(RuntimeError):
    """A misuse or an infrastructure fault. Never an outcome."""


def _require_idle(conn: psycopg.Connection[Any], caller: str) -> None:
    if conn.info.transaction_status != TransactionStatus.IDLE:
        raise HuntHarnessError(f"{caller} owns its commits and refuses a connection with an open transaction")


def _release_programme_lock(conn: psycopg.Connection[Any], *, raise_on_loss: bool) -> None:
    if conn.info.transaction_status != TransactionStatus.IDLE:
        conn.rollback()
    released = conn.execute("SELECT pg_advisory_unlock(%s, %s)", HUNT_PROGRAMME_LOCK).fetchone()
    conn.commit()
    if released == (True,):
        return
    message = "hunt programme lock ownership was lost"
    if raise_on_loss:
        raise HuntHarnessError(message)
    _LOG.error("%s while another exception was propagating", message)


@contextmanager
def hunt_programme_lock(conn: psycopg.Connection[Any]) -> Iterator[None]:
    """Serialise the whole hunt programme: evaluate, recording, freezes and readouts."""
    _require_idle(conn, "the hunt programme lock")
    if _PROGRAMME_LOCK_HELD.get():
        raise HuntHarnessError("the hunt programme lock is already held in this context; it is not re-entrant")
    conn.execute("SELECT pg_advisory_lock(%s, %s)", HUNT_PROGRAMME_LOCK)
    conn.commit()
    token = _PROGRAMME_LOCK_HELD.set(True)
    try:
        yield
    except BaseException:
        # The body's exception wins; a failed release on a dead connection has already
        # released the lock server-side.
        try:
            _release_programme_lock(conn, raise_on_loss=False)
        except Exception:
            _LOG.exception("releasing the hunt programme lock failed")
        raise
    else:
        _release_programme_lock(conn, raise_on_loss=True)
    finally:
        _PROGRAMME_LOCK_HELD.reset(token)


# ---------------------------------------------------------------------------
# Results
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class HuntRefused:
    """Refused before registration: no search was made and none is counted."""

    reason: RefusalReason
    detail: str


@dataclass(frozen=True)
class ComputedOutcome:
    """What ``compute`` returns. ``refused`` = the base canonical cell refused."""

    status: Literal["computed", "refused"]
    statistics: Mapping[str, Any]
    active_series: tuple[float, ...] | None


@dataclass(frozen=True)
class HuntOutcome:
    hunt_trial_id: int
    status: Literal["computed", "refused", "abandoned"]
    statistics: Mapping[str, Any]
    active_series: tuple[float, ...] | None
    outcome_sha256: str
    #: True when an earlier evaluation's stored outcome was returned.
    cached: bool


Compute = Callable[[psycopg.Connection[Any], TrialSpec], ComputedOutcome]


def outcome_sha256_of(
    *,
    spec_sha256: str,
    statistics_form: Any,
    active_series_form: Any,
    access_id: int | None,
    declaration_sha256: str | None,
) -> str:
    return sha256_form(
        {
            "spec_sha256": spec_sha256,
            "statistics": statistics_form,
            "active_series": active_series_form,
            "access_id": access_id,
            "declaration_sha256": declaration_sha256,
        }
    )


# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

_SELECT_EVALUATE_ROW = """
    SELECT t.hunt_trial_id, t.hunt_id, o.hunt_trial_id IS NOT NULL, t.spec_sha256
    FROM hunt_trials t
    LEFT JOIN hunt_trial_outcomes o USING (hunt_trial_id)
    WHERE t.candidate_sha256 = %(candidate)s AND t.split = %(split)s AND t.purpose = 'evaluate'
"""

#: A recording burns its candidate in its split; one whose candidate cannot be
#: reconstructed (``spec IS NULL``) burns the whole (hunt, split) it names.
_SELECT_BURN = """
    SELECT note
    FROM hunt_trials
    WHERE purpose = 'recorded_after' AND split = %(split)s
      AND (candidate_sha256 = %(candidate)s OR (spec IS NULL AND hunt_id = %(hunt)s))
    ORDER BY hunt_trial_id
    LIMIT 1
"""

_SELECT_VALIDATION_FROZEN = """
    SELECT 1 FROM strategy_preregistration_declarations WHERE strategy_id = %(strategy_id)s LIMIT 1
"""

_COUNT_DISCOVERY_ROWS = "SELECT count(*) FROM hunt_trials WHERE hunt_id = %(hunt)s AND split = 'discovery'"

_INSERT_TRIAL = """
    INSERT INTO hunt_trials (
        hunt_id, family, split, candidate_sha256, spec_sha256, spec, harness_model_id,
        registered_by, purpose, floor, note
    ) VALUES (
        %(hunt_id)s, %(family)s, %(split)s, %(candidate)s, %(spec_sha256)s, %(spec)s::jsonb,
        %(harness_model_id)s, %(registered_by)s, %(purpose)s, %(floor)s, %(note)s
    )
    ON CONFLICT DO NOTHING
    RETURNING hunt_trial_id
"""

_INSERT_OUTCOME = """
    INSERT INTO hunt_trial_outcomes (
        hunt_trial_id, status, statistics, active_series, access_id, declaration_sha256, outcome_sha256
    ) VALUES (
        %(trial)s, %(status)s, %(statistics)s::jsonb, %(active_series)s::jsonb, NULL, NULL, %(outcome)s
    )
"""

_SELECT_OUTCOME = """
    SELECT o.status, o.statistics, o.active_series, o.access_id, o.declaration_sha256, o.outcome_sha256,
           t.spec_sha256
    FROM hunt_trial_outcomes o
    JOIN hunt_trials t USING (hunt_trial_id)
    WHERE o.hunt_trial_id = %(trial)s
"""

_SELECT_RECORDING = """
    SELECT hunt_trial_id, hunt_id, split, candidate_sha256, floor
    FROM hunt_trials
    WHERE purpose = 'recorded_after' AND note = %(note)s
"""


# ---------------------------------------------------------------------------
# evaluate
# ---------------------------------------------------------------------------


def _identity_mismatches(conn: psycopg.Connection[Any], spec: TrialSpec) -> list[str]:
    """Every bound identity that differs from the running one. The universe scan runs last."""
    mismatches: list[str] = []
    if spec.harness_model_id != HUNT_HARNESS_MODEL_ID:
        mismatches.append(f"harness_model_id {spec.harness_model_id} != running {HUNT_HARNESS_MODEL_ID}")
    running_cost = running_cost_model_id(spec.lane)
    if spec.cost_model_id != running_cost:
        mismatches.append(f"cost_model_id {spec.cost_model_id} != running {running_cost}")
    running_calendar = calendar_identity()
    if spec.calendar_identity != running_calendar:
        mismatches.append(f"calendar_identity {spec.calendar_identity} != running {running_calendar}")
    running_signal = signal_code_sha256(spec.signal_id)
    if spec.signal_code_sha256 != running_signal:
        mismatches.append(f"signal {spec.signal_id} code sha256 != running {running_signal}")
    if mismatches:
        return mismatches
    running_universe = read_universe_identity(conn, spec.universe_identity.universe)
    if running_universe != spec.universe_identity:
        mismatches.append(f"universe_identity {spec.universe_identity.form()} != running {running_universe.form()}")
    return mismatches


def _refusal_before_registration(conn: psycopg.Connection[Any], spec: TrialSpec) -> HuntRefused | None:
    """Step 2: every reason to refuse that makes no search. Cheap checks first."""
    if running_cost_model_id(spec.lane) is None:
        return HuntRefused("unpriced_lane", f"lane {spec.lane} has no priced tariff in {HUNT_COST_MODEL_PREFIX}")
    timeline = timeline_refusal(spec)
    if timeline is not None:
        return HuntRefused("refused_timeline", timeline)
    if spec.hunt_id in HUNT_CLOSED:
        return HuntRefused("hunt_closed", f"{spec.hunt_id} closed: {HUNT_CLOSED[spec.hunt_id]}")
    if spec.split != "discovery":
        return HuntRefused(
            "door_unavailable", f"the audited {spec.split} door ships in #3385 slice 2b; nothing may open it yet"
        )
    if spec.hunt_id not in HUNT_BUDGETS:
        return HuntRefused("no_budget", f"{spec.hunt_id} has no HUNT_BUDGETS entry; the #3387 PR opening it sets one")
    burn = conn.execute(
        _SELECT_BURN, {"split": spec.split, "candidate": spec.candidate_sha256, "hunt": spec.hunt_id}
    ).fetchone()
    if burn is not None:
        return HuntRefused("split_burned", f"{spec.split} burned by the recorded look {burn[0]!r}")
    frozen = conn.execute(_SELECT_VALIDATION_FROZEN, {"strategy_id": f"{spec.hunt_id}-validation"}).fetchone()
    if frozen is not None:
        return HuntRefused("discovery_closed", f"{spec.hunt_id}'s validation declaration has frozen")
    mismatches = _identity_mismatches(conn, spec)
    if mismatches:
        return HuntRefused("identity_mismatch", "; ".join(mismatches))
    return None


def _read_outcome(conn: psycopg.Connection[Any], trial_id: int) -> HuntOutcome:
    row = conn.execute(_SELECT_OUTCOME, {"trial": trial_id}).fetchone()
    if row is None:
        raise HuntHarnessError(f"hunt trial {trial_id} has no outcome")
    status, statistics_form, series_form, access_id, declaration_sha256, stored_sha256, spec_sha256 = row
    recomputed = outcome_sha256_of(
        spec_sha256=spec_sha256,
        statistics_form=statistics_form,
        active_series_form=series_form,
        access_id=access_id,
        declaration_sha256=declaration_sha256,
    )
    if recomputed != stored_sha256:
        raise HuntHarnessError(f"hunt trial {trial_id}'s stored outcome does not match its outcome_sha256")
    return HuntOutcome(
        hunt_trial_id=trial_id,
        status=status,
        statistics=decode_form(statistics_form),
        active_series=None if series_form is None else tuple(decode_form(series_form)),
        outcome_sha256=stored_sha256,
        cached=True,
    )


def _insert_trial(conn: psycopg.Connection[Any], params: dict[str, Any]) -> int | None:
    row = conn.execute(_INSERT_TRIAL, params).fetchone()
    return None if row is None else int(row[0])


def evaluate(
    conn: psycopg.Connection[Any], spec: TrialSpec, *, registered_by: str, compute: Compute
) -> HuntOutcome | HuntRefused:
    """Register ``spec`` (committed) before ``compute`` reads a price, then store its outcome.

    Order (spec "Registration"): programme lock → refusals that make no search →
    ownership / cached outcome → reuse a registration without an outcome, else the
    budget check and insert, COMMITTED → compute → re-check the universe identity →
    outcome committed → lock released.
    """
    if not _is_text(registered_by):
        raise ValueError("registered_by must be non-empty text")
    with hunt_programme_lock(conn):
        refusal = _refusal_before_registration(conn, spec)
        if refusal is not None:
            conn.commit()
            return refusal

        existing = conn.execute(
            _SELECT_EVALUATE_ROW, {"candidate": spec.candidate_sha256, "split": spec.split}
        ).fetchone()
        if existing is not None:
            trial_id, owner, has_outcome = int(existing[0]), existing[1], bool(existing[2])
            # ⚠ A retry may carry a relabelled family; the outcome binds to the REGISTERED
            # spec, whose hash is the stored one (Codex ckpt-2).
            registered_spec_sha256 = str(existing[3])
            if owner != spec.hunt_id:
                conn.commit()
                return HuntRefused(
                    "candidate_owned_by_other_hunt", f"candidate {spec.candidate_sha256} is {owner}'s in {spec.split}"
                )
            if has_outcome:
                outcome = _read_outcome(conn, trial_id)
                conn.commit()
                return outcome
        else:
            budget = HUNT_BUDGETS[spec.hunt_id]
            used = conn.execute(_COUNT_DISCOVERY_ROWS, {"hunt": spec.hunt_id}).fetchone()
            if used is not None and int(used[0]) >= budget:
                conn.commit()
                return HuntRefused("budget_exhausted", f"{spec.hunt_id} has used {used[0]} of {budget} discovery rows")
            inserted = _insert_trial(
                conn,
                {
                    "hunt_id": spec.hunt_id,
                    "family": spec.family,
                    "split": spec.split,
                    "candidate": spec.candidate_sha256,
                    "spec_sha256": spec.spec_sha256,
                    "spec": dumps_form(spec.form()),
                    "harness_model_id": spec.harness_model_id,
                    "registered_by": registered_by,
                    "purpose": "evaluate",
                    "floor": False,
                    "note": None,
                },
            )
            if inserted is None:
                raise HuntHarnessError(f"registration of {spec.candidate_sha256} conflicted under the programme lock")
            trial_id = inserted
            registered_spec_sha256 = spec.spec_sha256
        # ⚠ The search is durable BEFORE any price is read.
        conn.commit()

        computed = compute(conn, spec)
        if computed.status not in ("computed", "refused"):
            raise HuntHarnessError(f"compute returned status {computed.status!r}")
        if conn.info.transaction_status != TransactionStatus.IDLE:
            conn.commit()
        after = read_universe_identity(conn, spec.universe_identity.universe)
        if after != spec.universe_identity:
            raise HuntHarnessError("the universe identity changed during the computation; retry the registered trial")

        statistics_form = canonical_form(computed.statistics)
        series_form = None if computed.active_series is None else canonical_form(computed.active_series)
        outcome_sha256 = outcome_sha256_of(
            spec_sha256=registered_spec_sha256,
            statistics_form=statistics_form,
            active_series_form=series_form,
            access_id=None,
            declaration_sha256=None,
        )
        conn.execute(
            _INSERT_OUTCOME,
            {
                "trial": trial_id,
                "status": computed.status,
                "statistics": dumps_form(statistics_form),
                "active_series": None if series_form is None else dumps_form(series_form),
                "outcome": outcome_sha256,
            },
        )
        conn.commit()
        return HuntOutcome(
            hunt_trial_id=trial_id,
            status=computed.status,
            statistics=decode_form(statistics_form),
            active_series=None if series_form is None else tuple(decode_form(series_form)),
            outcome_sha256=outcome_sha256,
            cached=False,
        )


# ---------------------------------------------------------------------------
# A look taken outside the harness
# ---------------------------------------------------------------------------


def record_outside_look(
    conn: psycopg.Connection[Any],
    *,
    note: str,
    registered_by: str,
    spec: TrialSpec | None = None,
    hunt_id: str | None = None,
    family: str | None = None,
    split: Split | None = None,
    floor: bool = False,
) -> int:
    """Record one variant looked at outside the harness; idempotent on ``note``.

    Pass the reconstructed ``spec``, or — when the candidate cannot be reconstructed —
    the ``hunt_id``, ``family`` and ``split`` the recorder names; that row burns the
    whole (hunt, split). Use ``floor=True`` when the number of variants looked at is
    unknown. A recording is never refused: it counts in M and in the budget even past
    the budget or after closure, because the look already happened.
    """
    if not _is_text(note):
        raise ValueError("note must be non-empty text: it names the look and its variant ordinal")
    if not _is_text(registered_by):
        raise ValueError("registered_by must be non-empty text")
    if spec is not None:
        if (hunt_id, family, split) != (None, None, None):
            raise ValueError("pass either a reconstructed spec or hunt_id/family/split, not both")
        hunt, fam, spl = spec.hunt_id, spec.family, spec.split
        candidate, spec_sha256, spec_json, model = (
            spec.candidate_sha256,
            spec.spec_sha256,
            dumps_form(spec.form()),
            spec.harness_model_id,
        )
    else:
        if hunt_id is None or family is None or split is None:
            raise ValueError("an unreconstructed look needs hunt_id, family and split")
        if not _HUNT_ID.match(hunt_id) or not _FAMILY.match(family) or split not in SPLITS:
            raise ValueError(f"bad hunt/family/split {(hunt_id, family, split)!r}")
        hunt, fam, spl = hunt_id, family, split
        candidate = spec_sha256 = hashlib.sha256(note.encode("utf-8")).hexdigest()
        spec_json, model = None, HUNT_HARNESS_MODEL_ID
    with hunt_programme_lock(conn):
        inserted = _insert_trial(
            conn,
            {
                "hunt_id": hunt,
                "family": fam,
                "split": spl,
                "candidate": candidate,
                "spec_sha256": spec_sha256,
                "spec": spec_json,
                "harness_model_id": model,
                "registered_by": registered_by,
                "purpose": "recorded_after",
                "floor": floor,
                "note": note,
            },
        )
        if inserted is not None:
            conn.commit()
            return inserted
        existing = conn.execute(_SELECT_RECORDING, {"note": note}).fetchone()
        conn.commit()
    if existing is None:
        raise HuntHarnessError(f"recording {note!r} conflicted but no recording carries that note")
    if (existing[1], existing[2], existing[3], existing[4]) != (hunt, spl, candidate, floor):
        raise HuntHarnessError(f"note {note!r} already records a different look (trial {existing[0]})")
    return int(existing[0])


# ---------------------------------------------------------------------------
# Obligation 83: a stored row agrees with its own spec
# ---------------------------------------------------------------------------


def verify_trial_row(
    *,
    hunt_id: str,
    family: str,
    split: str,
    candidate_sha256: str,
    spec_sha256: str,
    spec: Mapping[str, Any] | None,
    harness_model_id: str,
    note: str | None,
) -> list[str]:
    """Every disagreement between a ``hunt_trials`` row's columns and its stored spec.

    The migration's CHECK ties the scalar columns; this recomputes the two hashes,
    which the database cannot.
    """
    if spec is None:
        expected = hashlib.sha256((note or "").encode("utf-8")).hexdigest()
        problems = []
        if candidate_sha256 != expected or spec_sha256 != expected:
            problems.append("an unreconstructed recording's hashes must both be sha256(note)")
        return problems
    problems = []
    for name, column in (
        ("hunt_id", hunt_id),
        ("family", family),
        ("split", split),
        ("harness_model_id", harness_model_id),
    ):
        if spec.get(name) != column:
            problems.append(f"{name} column {column!r} != spec {spec.get(name)!r}")
    missing = [name for name in SPEC_FIELDS if name not in spec]
    if missing:
        problems.append(f"spec lacks {missing}")
        return problems
    if candidate_sha256_of(spec) != candidate_sha256:
        problems.append("candidate_sha256 does not match the stored spec")
    if spec_sha256_of(spec) != spec_sha256:
        problems.append("spec_sha256 does not match the stored spec")
    return problems


__all__ = [
    "CANDIDATE_FIELDS",
    "EMBARGO_SESSIONS",
    "HORIZON_CAP_SESSIONS",
    "HUNT_BUDGETS",
    "HUNT_CLOSED",
    "HUNT_HARNESS_MODEL_ID",
    "HUNT_PROGRAMME_LOCK",
    "HUNT_TARIFF",
    "LANES",
    "SPEC_FIELDS",
    "SPLIT_BOUNDS",
    "ComputedOutcome",
    "HuntHarnessError",
    "HuntOutcome",
    "HuntRefused",
    "HuntTariff",
    "TrialSpec",
    "UniverseIdentity",
    "calendar_identity",
    "canonical_form",
    "candidate_sha256_of",
    "decode_form",
    "evaluate",
    "hunt_programme_lock",
    "read_universe_identity",
    "record_outside_look",
    "running_cost_model_id",
    "signal_code_sha256",
    "spec_sha256_of",
    "timeline_refusal",
    "verify_trial_row",
]
