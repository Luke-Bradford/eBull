"""The pattern-hunt feature store: the discovery panel, persisted once and read per trial.

#3386 slice 1, spec ``docs/proposals/ta/2026-09-26-3386-hunt-feature-store.md`` (v3),
"Part A". Pure: numpy + stdlib, no DB, no reader. ``hunt_panel`` builds the parts from
the archive and hands them here; every discovery trial then computes on parts READ from
a store directory, hit or miss, so the two paths cannot differ.

⚠ Returns research prices, so it is on ``tests/test_sealed_outcome_scripts_are_gated.py``'s
reader list: only ``hunt_panel`` imports it. Its code is part of ``HUNT_HARNESS_MODEL_ID``.

⚠ The key is METADATA (spec "Key"): a value edit that keeps every count, range and flag
is invisible to it, and the store then serves the build's values. That is a declared
residual; ``store_content_sha256`` records which bytes a trial computed on.

Layout (spec "Layout"): series sorted by ``series_id``; ``series.npy`` int64 [n, 5] =
(``series_id``, bar start, dividend start, terminal session index or −1, termination
class code or −1); one file per bar column over every bar; the dividends' own session
indices and amounts; ``sessions.npy`` = ``date.toordinal()`` of each session. Two kinds
of ordinal, kept apart: a SESSION INDEX (bars, dividends, terminal) indexes ``sessions``;
a CALENDAR ORDINAL (``sessions.npy``) is a proleptic date number.
"""

from __future__ import annotations

import hashlib
import io
import json
import logging
import os
import shutil
from array import array
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from types import MappingProxyType
from typing import Any, Final

import numpy as np

from app.services.hunt_compute import PanelSeries
from app.services.hunt_view import Bars
from app.services.series_termination import TerminationClass

_LOG = logging.getLogger(__name__)

FORMAT: Final = "hunt-store-v1"
MANIFEST: Final = "manifest.json"
TMP_PREFIX: Final = ".tmp-"
#: Sorted, so a class code is stable while the enum's member set is.
TERMINATION_CLASSES: Final[tuple[str, ...]] = tuple(sorted(member.value for member in TerminationClass))

_I8: Final = np.dtype("<i8")
_F8: Final = np.dtype("<f8")
_U1: Final = np.dtype("|u1")
#: file stem → dtype; every bar column has one value per bar.
_BAR_COLUMNS: Final[Mapping[str, np.dtype[Any]]] = MappingProxyType(
    {
        "ordinal": _I8,
        "open": _F8,
        "high": _F8,
        "low": _F8,
        "close": _F8,
        "volume": _F8,
        "traded_open": _F8,
        "traded_close": _F8,
        "exclusion": _U1,
    }
)
_FILES: Final[tuple[str, ...]] = ("series", "sessions", *_BAR_COLUMNS, "dividend_ordinal", "dividend_amount")
#: ``hunt_compute.BAR_EXCLUSIONS`` has five reasons; code 0 = valid.
_MAX_EXCLUSION_CODE: Final = 5

if array("l").itemsize != 8:  # pragma: no cover - every supported platform is LP64
    raise ImportError("hunt_store needs an 8-byte C long to rebuild the loader's array('l') columns")


class HuntStoreError(RuntimeError):
    """A build could not be published (an infrastructure error: no outcome, retry)."""


@dataclass(frozen=True)
class PanelParts:
    """A panel without its regime labels, which ``hunt_panel`` attaches live (spec "Parts")."""

    sessions: tuple[date, ...]
    series: Mapping[int, PanelSeries]
    load_counts: Mapping[str, int]


@dataclass(frozen=True)
class StoredParts:
    parts: PanelParts
    content_sha256: str


# ---------------------------------------------------------------------------
# Canonical JSON (objects and arrays; every leaf a string or an integer)
# ---------------------------------------------------------------------------


def _check_json(value: Any) -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError(f"canonical JSON keys are strings, not {type(key).__name__}")
            _check_json(item)
    elif isinstance(value, list | tuple):
        for item in value:
            _check_json(item)
    elif isinstance(value, bool) or not isinstance(value, str | int):
        raise TypeError(f"canonical JSON leaves are strings or integers, not {type(value).__name__}")


def canonical_json(value: Any) -> bytes:
    _check_json(value)
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def store_key(*, through: date, universe_identity: Mapping[str, str], quarantine_identity: str, model_id: str) -> str:
    """The directory name: every identity the parts depend on (spec "Key")."""
    return _sha256(
        canonical_json(
            {
                "format": FORMAT,
                "through": through.isoformat(),
                "universe_identity": dict(universe_identity),
                "quarantine_identity": quarantine_identity,
                "model_id": model_id,
            }
        )
    )


# ---------------------------------------------------------------------------
# Write
# ---------------------------------------------------------------------------


def _npy_bytes(values: np.ndarray[Any, Any]) -> bytes:
    buffer = io.BytesIO()
    np.save(buffer, values, allow_pickle=False)
    return buffer.getvalue()


def _increasing(values: Any) -> bool:
    return all(values[i] < values[i + 1] for i in range(len(values) - 1))


def save_parts(parts: PanelParts, directory: Path, *, key: str, through: date) -> str:
    """Write ``parts`` into the new ``directory``; the manifest is written last. Returns its sha256."""
    if parts.sessions and parts.sessions[-1] > through:
        raise ValueError(f"parts run to {parts.sessions[-1]}, after the store's through date {through}")
    ids = list(parts.series)
    if ids != sorted(set(ids)):
        raise ValueError("the parts' series must be unique and in series_id order")
    rows: list[tuple[int, int, int, int, int]] = []
    bar_total = dividend_total = 0
    for series_id in ids:
        series = parts.series[series_id]
        dividend_ordinals = list(series.dividends)
        if not _increasing(dividend_ordinals):
            raise ValueError(f"series {series_id}: dividends must be in ex-date order")
        terminal = -1 if series.terminal_ordinal is None else series.terminal_ordinal
        code = -1 if series.termination_class is None else TERMINATION_CLASSES.index(series.termination_class.value)
        rows.append((series_id, bar_total, dividend_total, terminal, code))
        bar_total += len(series.ratio.ordinals)
        dividend_total += len(dividend_ordinals)

    def column(pick: Callable[[PanelSeries], Any], dtype: np.dtype[Any]) -> np.ndarray[Any, Any]:
        chunks = [np.asarray(pick(parts.series[sid]), dtype=dtype) for sid in ids]
        return np.concatenate(chunks) if chunks else np.zeros(0, dtype=dtype)

    # One array at a time (built, written, dropped): the whole set is ~0.9 GB for discovery.
    builders: dict[str, Callable[[], np.ndarray[Any, Any]]] = {
        "series": lambda: np.asarray(rows, dtype=_I8).reshape(len(rows), 5),
        "sessions": lambda: np.asarray([day.toordinal() for day in parts.sessions], dtype=_I8),
        "ordinal": lambda: column(lambda s: s.ratio.ordinals, _I8),
        "open": lambda: column(lambda s: s.ratio.open, _F8),
        "high": lambda: column(lambda s: s.ratio.high, _F8),
        "low": lambda: column(lambda s: s.ratio.low, _F8),
        "close": lambda: column(lambda s: s.ratio.close, _F8),
        "volume": lambda: column(lambda s: s.ratio.volume, _F8),
        "traded_open": lambda: column(lambda s: s.traded_open, _F8),
        "traded_close": lambda: column(lambda s: s.traded_close, _F8),
        "exclusion": lambda: column(lambda s: np.frombuffer(s.exclusion, dtype=_U1), _U1),
        "dividend_ordinal": lambda: column(lambda s: list(s.dividends), _I8),
        "dividend_amount": lambda: column(lambda s: list(s.dividends.values()), _F8),
    }
    directory.mkdir(parents=True)
    hashes: dict[str, str] = {}
    for name in _FILES:
        data = _npy_bytes(builders[name]())
        (directory / f"{name}.npy").write_bytes(data)
        hashes[name] = _sha256(data)
    manifest = canonical_json(
        {
            "format": FORMAT,
            "key": key,
            "through": through.isoformat(),
            "load_counts": dict(parts.load_counts),
            "termination_classes": list(TERMINATION_CLASSES),
            "files": hashes,
        }
    )
    (directory / MANIFEST).write_bytes(manifest)
    return _sha256(manifest)


# ---------------------------------------------------------------------------
# Read: any failure is a miss, never a partial panel
# ---------------------------------------------------------------------------


class _Invalid(ValueError):
    pass


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise _Invalid(message)


def _read_array(directory: Path, name: str, expected_sha256: str) -> np.ndarray[Any, Any]:
    data = (directory / f"{name}.npy").read_bytes()
    _require(_sha256(data) == expected_sha256, f"{name}.npy does not match its manifest hash")
    # Decoded from the bytes that were hashed, never from a second open of the file.
    return np.load(io.BytesIO(data), allow_pickle=False)


def _parse(directory: Path, *, key: str, through: date) -> StoredParts:
    manifest_bytes = (directory / MANIFEST).read_bytes()
    manifest = json.loads(manifest_bytes)
    _require(isinstance(manifest, dict), "manifest is not an object")
    _require(manifest.get("format") == FORMAT, f"unknown format {manifest.get('format')!r}")
    _require(manifest.get("key") == key, "manifest key differs")
    _require(manifest.get("through") == through.isoformat(), "manifest through date differs")
    _require(manifest.get("termination_classes") == list(TERMINATION_CLASSES), "termination class codes differ")
    _require(canonical_json(manifest) == manifest_bytes, "manifest is not canonical")
    hashes, counts = manifest.get("files"), manifest.get("load_counts")
    _require(isinstance(hashes, dict) and set(hashes) == set(_FILES), "manifest file set differs")
    _require(
        isinstance(counts, dict) and all(type(value) is int for value in counts.values()),
        "load_counts must map names to integers",
    )
    arrays = {name: _read_array(directory, name, hashes[name]) for name in _FILES}

    expected = {"series": _I8, "sessions": _I8, "dividend_ordinal": _I8, "dividend_amount": _F8, **_BAR_COLUMNS}
    for name, dtype in expected.items():
        _require(arrays[name].dtype == dtype, f"{name}.npy has dtype {arrays[name].dtype}, not {dtype}")
        _require(arrays[name].ndim == (2 if name == "series" else 1), f"{name}.npy has the wrong rank")
    table, sessions = arrays["series"], arrays["sessions"]
    _require(table.shape[1] == 5, "series.npy must have five columns")
    bar_total = len(arrays["ordinal"])
    _require(all(len(arrays[name]) == bar_total for name in _BAR_COLUMNS), "bar columns differ in length")
    dividend_total = len(arrays["dividend_ordinal"])
    _require(len(arrays["dividend_amount"]) == dividend_total, "dividend columns differ in length")
    _require(bool(np.all(np.diff(sessions) > 0)), "sessions must be strictly increasing")
    session_dates = tuple(date.fromordinal(int(value)) for value in sessions)
    _require(not session_dates or session_dates[-1] <= through, "a stored session is after the through date")
    _require(bool(np.all(arrays["exclusion"] <= _MAX_EXCLUSION_CODE)), "an exclusion code is out of range")

    ids = table[:, 0]
    _require(bool(np.all(np.diff(ids) > 0)), "series ids must be unique and increasing")
    bar_starts = [*table[:, 1].tolist(), bar_total]
    dividend_starts = [*table[:, 2].tolist(), dividend_total]
    _require(not len(table) or (bar_starts[0] == 0 and dividend_starts[0] == 0), "offsets must start at 0")
    _require(all(a < b for a, b in zip(bar_starts, bar_starts[1:])), "every series needs a bar; offsets increase")
    _require(all(a <= b for a, b in zip(dividend_starts, dividend_starts[1:])), "dividend offsets must not decrease")
    session_count = len(sessions)

    series: dict[int, PanelSeries] = {}
    for row, series_id in enumerate(ids.tolist()):
        b0, b1 = bar_starts[row], bar_starts[row + 1]
        d0, d1 = dividend_starts[row], dividend_starts[row + 1]
        ordinals = arrays["ordinal"][b0:b1]
        dividend_ordinals = arrays["dividend_ordinal"][d0:d1]
        for values in (ordinals, dividend_ordinals):
            _require(
                bool(np.all(np.diff(values) > 0)) and (not len(values) or 0 <= values[0] <= values[-1] < session_count),
                f"series {series_id}: session indices must be increasing and in range",
            )
        terminal, code = int(table[row, 3]), int(table[row, 4])
        _require((terminal < 0) == (code < 0), f"series {series_id}: terminal bar and class disagree")
        _require(code < len(TERMINATION_CLASSES) and terminal < session_count, f"series {series_id}: bad terminal")

        def column(name: str, typecode: str, start: int = b0, end: int = b1) -> array[Any]:
            out = array(typecode)
            out.frombytes(arrays[name][start:end].tobytes())
            return out

        series[series_id] = PanelSeries(
            series_id=series_id,
            ratio=Bars(
                column("ordinal", "l"),
                column("open", "d"),
                column("high", "d"),
                column("low", "d"),
                column("close", "d"),
                column("volume", "d"),
            ),
            traded_open=column("traded_open", "d"),
            traded_close=column("traded_close", "d"),
            exclusion=arrays["exclusion"][b0:b1].tobytes(),
            dividends=MappingProxyType(
                dict(zip(dividend_ordinals.tolist(), arrays["dividend_amount"][d0:d1].tolist(), strict=True))
            ),
            terminal_ordinal=None if terminal < 0 else terminal,
            termination_class=None if code < 0 else TerminationClass(TERMINATION_CLASSES[code]),
        )
    parts = PanelParts(sessions=session_dates, series=MappingProxyType(series), load_counts=MappingProxyType(counts))
    return StoredParts(parts, _sha256(manifest_bytes))


def load_parts(directory: Path, *, key: str, through: date) -> StoredParts | None:
    """The stored parts, or ``None`` on ANY missing, malformed or mismatched content (a miss)."""
    try:
        return _parse(directory, key=key, through=through)
    except (OSError, ValueError, TypeError, KeyError, IndexError) as error:
        # json.JSONDecodeError and _Invalid are ValueErrors; PanelSeries validation raises ValueError.
        if (directory / MANIFEST).exists():
            _LOG.warning("hunt store %s is unusable (%s); rebuilding", directory.name, error)
        return None


# ---------------------------------------------------------------------------
# Hit or build
# ---------------------------------------------------------------------------


def load_or_build(
    root: Path,
    *,
    key: str,
    through: date,
    build: Callable[[], PanelParts],
    verify: Callable[[], None],
) -> StoredParts:
    """The parts under ``root/key``; on a miss, build, write, read back, verify, publish.

    ``verify`` re-reads the identities the key came from and raises when one moved (spec
    "Build" step 4); nothing is published then. The caller holds the programme lock.
    """
    target = root / key
    stored = load_parts(target, key=key, through=through)
    if stored is not None:
        _LOG.info("hunt store hit %s", key[:12])
        return stored
    _LOG.info("hunt store miss %s; building", key[:12])
    root.mkdir(parents=True, exist_ok=True)
    for stale in root.glob(f"{TMP_PREFIX}*"):
        shutil.rmtree(stale, ignore_errors=True)
    temporary = root / f"{TMP_PREFIX}{key}-{os.getpid()}"
    try:
        save_parts(build(), temporary, key=key, through=through)
        stored = load_parts(temporary, key=key, through=through)
        if stored is None:
            raise HuntStoreError(f"the store just written to {temporary} does not read back")
        verify()
    except BaseException:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    if target.exists():
        shutil.rmtree(target)
    os.replace(temporary, target)
    return stored


__all__ = [
    "FORMAT",
    "HuntStoreError",
    "PanelParts",
    "StoredParts",
    "canonical_json",
    "load_or_build",
    "load_parts",
    "save_parts",
    "store_key",
]
