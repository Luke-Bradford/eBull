"""#3609 step 2's report: the declared run's construction root (slice 3c-iv).

Spec: ``docs/research/2026-10-06-3609-step2-factor-book.md`` (PR #3666) §"The book", §"Registration" and §"Slices"
item 3. This slice is the loader: a verified panel artefact, read once (§"Registration", finding 149), turned into
per-formation inputs for the scoring core (:mod:`app.services.factor_book`) and scored. The path, control,
references, gates and diagnostics consume :class:`PanelMonth` in later slices.

Every value comes from bytes :func:`scripts.build_3609_factor_panel.read_verified_artefact` hashed; no artefact path
is reopened after its check. The FF-12 map is the committed, pinned ``Siccodes12.zip`` (read once by
:func:`~app.services.factor_book_reference.load_ff12`); the JKP Table 9 signs are the artefact's frozen copy.

* **Raw close at s(M)** is the frozen decision bar (``inputs/decision_bars.jsonl.gz``) of the name's series, for
  every name with a row at M, admitted or not, so a holding that left the admitted population keeps its close.
  The builder priced each admitted row's ME from that same bar, so an admitted row whose ME close differs from it,
  or which has no bar, refuses.
* **First admitted bar** (archive seasoning) is the frozen ``inputs/first_bars.jsonl.gz`` entry of the name's
  series; the 12-month daily window cannot supply it (spec finding 156).
* **Industry**: the row's FF-12 group from its SUB ``sic``; ``sic_null`` and ``sic_unloaded`` rows are
  ``UNCLASSIFIED`` (§"Source rules").
* **Signed characteristics**: Table 9's sign times the row's value, for the five book characteristics; a ``None``
  value is no input.
"""

from __future__ import annotations

import gzip
import io
import json
import math
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import date
from typing import Any, Final, get_args

from app.services.factor_book import CHARACTERISTICS, UNCLASSIFIED, Bands, BookRefusal, Scores, composite_scores
from app.services.factor_book import bands as book_bands
from app.services.factor_book import universe as book_universe
from app.services.factor_book_path import Formation, HoldingReturn
from app.services.factor_book_reference import Ff12Map
from app.services.factor_panel_prices import HoldingStatus
from app.services.factor_panel_reference import parse_table9_signs
from app.services.strategy_result import AmbiguityArm
from scripts.build_3609_factor_panel import RESEARCH_ROOT, Frozen, VerifiedArtefact, holding_month, price_bound

#: The stage-A artefact the spec's freeze evidence pins (§"Registration").
STAGE_A_ARTEFACT: Final = RESEARCH_ROOT / "factor_panel_3609" / "2026-10-06-483ac6ae-stageA"
STAGE_A_MANIFEST_SHA256: Final = "ee1e8abc241f26596529509cd38f4692de4de8ff8700c0ac8ca19c2a412f3c1e"
#: The inputs the report consumes, relative to the artefact; passed as ``keep`` to ``read_verified_artefact``.
DECISION_BARS: Final = f"inputs/{Frozen.DECISION_BARS}"
TABLE9_SIGNS: Final = f"inputs/{Frozen.REFERENCE}/inputs/3609-jkp-table9-signs.csv"
FIRST_BARS: Final = f"inputs/{Frozen.FIRST_BARS}"
ADMITTED: Final = f"inputs/{Frozen.ADMITTED}"
CONSUMED_INPUTS: Final = (DECISION_BARS, FIRST_BARS, ADMITTED, TABLE9_SIGNS)
#: SUB classifications with no SIC (§"Source rules", missing classifications).
UNCLASSIFIED_STATUSES: Final = frozenset({"sic_null", "sic_unloaded"})
_ARMS: Final = frozenset(get_args(AmbiguityArm))


class ReportError(RuntimeError):
    """The artefact does not satisfy the loader's contract; the run stops."""


@dataclass(frozen=True)
class PanelName:
    """One admitted name at one formation."""

    series_id: int
    me: float
    industry: str
    #: JKP-signed values of the book characteristics present.
    signed: Mapping[str, float]
    holding: HoldingReturn


@dataclass(frozen=True)
class PanelMonth:
    formation: date
    #: s(M), the decision session.
    session: date
    admitted: Mapping[int, PanelName]
    #: Raw close at s(M) of every name with a row and a decision bar at M.
    close: Mapping[int, float]
    #: The first admitted bar of the series of every name with a row at M, where it has one.
    first_bar: Mapping[int, date]


@dataclass(frozen=True)
class ScoredMonth:
    universe: tuple[int, ...]
    scores: Scores
    bands: Bands


def _lines(payload: bytes) -> Iterator[Any]:
    with gzip.GzipFile(fileobj=io.BytesIO(payload)) as handle:
        for line in handle:
            yield json.loads(line)


def decision_closes(payload: bytes) -> dict[tuple[int, date], float]:
    """``(series_id, session) -> raw close`` from the frozen decision bars; a repeated key refuses."""
    out: dict[tuple[int, date], float] = {}
    for series_id, day, close in _lines(payload):
        key = (int(series_id), date.fromisoformat(day))
        if key in out:
            raise ReportError(f"decision bar {key} is repeated")
        out[key] = float(close)
    return out


def read_first_bars(payload: bytes, admitted: bytes, bound: date) -> dict[int, date]:
    """``series_id -> first admitted bar`` (spec §"Source rules", archive seasoning, Shape).

    Refuses a repeated or unadmitted ``series_id``, a malformed date, or a date after the stage's price bound."""
    series = {int(line["series_id"]) for line in _lines(admitted)}
    out: dict[int, date] = {}
    for series_id, day in _lines(payload):
        if series_id in out or series_id not in series:
            raise ReportError(f"first bar of series {series_id!r} is repeated or the series is not admitted")
        try:
            first = date.fromisoformat(day)
        except (TypeError, ValueError) as exc:
            raise ReportError(f"first bar of series {series_id} is not an ISO date: {day!r}") from exc
        if first > bound:
            raise ReportError(f"first bar of series {series_id} ({first}) is after the price bound {bound}")
        out[series_id] = first
    return out


def industry_of(row: Mapping[str, Any], ff12: Ff12Map) -> str:
    status = row["sic_status"]
    if status in UNCLASSIFIED_STATUSES:
        return UNCLASSIFIED
    if status != "sic" or not isinstance(row["sic"], int):
        raise ReportError(f"row {(row['M'], row['name_key'])}: sic_status {status!r} with sic {row['sic']!r}")
    return ff12.industry(row["sic"])


def holding_of(row: Mapping[str, Any]) -> HoldingReturn:
    holding = row["prices"]["holding"]
    by_arm = holding["by_arm"]
    if set(by_arm) != _ARMS:
        raise ReportError(f"row {(row['M'], row['name_key'])}: arms {sorted(by_arm)}, expected {sorted(_ARMS)}")
    return HoldingReturn(HoldingStatus(holding["status"]), {arm: float(value) for arm, value in by_arm.items()})


def admitted_name(row: Mapping[str, Any], ff12: Ff12Map, signs: Mapping[str, int]) -> PanelName:
    """§"The book" step 1: an admitted row with a non-finite or non-positive ME refuses (``ME_INVALID``)."""
    raw_me = row["me"]["value"]
    me = math.nan if raw_me is None else float(raw_me)
    if not (math.isfinite(me) and me > 0):
        raise BookRefusal("ME_INVALID", f"row {(row['M'], row['name_key'])} has ME {raw_me!r}")
    characteristics = row["characteristics"]
    signed = {
        c: signs[c] * float(characteristics[c]["value"])
        for c in CHARACTERISTICS
        if characteristics[c]["value"] is not None
    }
    return PanelName(row["series_id"], me, industry_of(row, ff12), signed, holding_of(row))


def read_panel(verified: VerifiedArtefact, ff12: Ff12Map) -> list[PanelMonth]:
    """The artefact's formations in order, each with its admitted names and raw closes.

    Refuses a row count other than the manifest's, formations other than the manifest's, a repeated
    ``(M, name_key)`` or ``(M, series_id)``, a holding month that does not follow M, or two decision sessions in one
    formation."""
    signs = parse_table9_signs(verified.files[TABLE9_SIGNS])
    missing = [c for c in CHARACTERISTICS if c not in signs]
    if missing:
        raise ReportError(f"Table 9 holds no sign for {missing}")
    bars = decision_closes(verified.files[DECISION_BARS])
    formations = [date.fromisoformat(m) for m in verified.manifest["formations"]]
    firsts = read_first_bars(verified.files[FIRST_BARS], verified.files[ADMITTED], price_bound(formations))
    sessions: dict[date, date] = {}
    admitted: dict[date, dict[int, PanelName]] = {m: {} for m in formations}
    close: dict[date, dict[int, float]] = {m: {} for m in formations}
    first_bar: dict[date, dict[int, date]] = {m: {} for m in formations}
    series_seen: set[tuple[date, int]] = set()
    names_seen: set[tuple[date, int]] = set()
    count = 0
    for row in _lines(verified.rows):
        count += 1
        formation, session, name = date.fromisoformat(row["M"]), date.fromisoformat(row["s_M"]), row["name_key"]
        if formation not in admitted:
            raise ReportError(f"row formation {formation} is not in the manifest's formations")
        if row["holding_month"] != holding_month(formation):
            raise ReportError(f"row {(row['M'], name)}: holding month {row['holding_month']} does not follow M")
        if sessions.setdefault(formation, session) != session:
            raise ReportError(f"formation {formation} has two decision sessions: {sessions[formation]}, {session}")
        if (formation, row["series_id"]) in series_seen:
            raise ReportError(f"(M, series_id) {(row['M'], row['series_id'])} is repeated")
        series_seen.add((formation, row["series_id"]))
        if (formation, name) in names_seen:
            raise ReportError(f"(M, name_key) {(row['M'], name)} is repeated")
        names_seen.add((formation, name))
        bar = bars.get((row["series_id"], session))
        if bar is not None:
            close[formation][name] = bar
        if row["series_id"] in firsts:
            first_bar[formation][name] = firsts[row["series_id"]]
        if row["exclusion"] is None:
            me_close = row["me"]["close"]
            if bar is None or me_close is None or float(me_close) != bar:
                raise ReportError(f"admitted row {(row['M'], name)}: ME close {me_close!r}, decision bar {bar!r}")
            admitted[formation][name] = admitted_name(row, ff12, signs)
    if count != verified.manifest["rows"]["count"]:
        raise ReportError(f"{count} rows read, the manifest says {verified.manifest['rows']['count']}")
    absent = [m for m in formations if m not in sessions]
    if absent:
        raise ReportError(f"no rows for formations {absent[:3]}")
    return [PanelMonth(m, sessions[m], admitted[m], close[m], first_bar[m]) for m in formations]


def score(month: PanelMonth) -> ScoredMonth:
    """§"The book" steps 1, 2 and 4 on one formation: the universe, the composite and its bands."""
    names = book_universe({name: panel.me for name, panel in month.admitted.items()})
    industry = {name: month.admitted[name].industry for name in names}
    signed: dict[str, dict[int, float]] = {c: {} for c in CHARACTERISTICS}
    for name in names:
        for c, value in month.admitted[name].signed.items():
            signed[c][name] = value
    scores = composite_scores(names, industry, signed)
    return ScoredMonth(tuple(names), scores, book_bands(scores.composite))


def formation_inputs(month: PanelMonth, scored: ScoredMonth) -> Formation:
    """The path's inputs for one formation: the universe, its bands, closes, first bars and holding returns."""
    return Formation(
        formation=month.formation,
        session=month.session,
        universe=frozenset(scored.universe),
        bands=scored.bands,
        close=month.close,
        first_bar=month.first_bar,
        returns={name: month.admitted[name].holding for name in scored.universe},
    )
