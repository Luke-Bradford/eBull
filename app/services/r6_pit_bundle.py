"""Immutable point-in-time evidence bundles for the recovered R6 spine.

Historical rankings read one content-addressed payload, never current database
projections. A later ingest may create a new bundle, but cannot alter the bytes
authorized by the frozen manifest. Any overwrite of those bytes fails before a
ranking record is returned.
"""

from __future__ import annotations

import hashlib
import json
import os
import stat
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Final

MANIFEST_SCHEMA: Final = "r6-pit-manifest-v1"
PAYLOAD_SCHEMA: Final = "r6-2900-pit-payload-v1"

#: Ceiling on a single evidence document held in memory while it is hashed and
#: parsed. There is no published bound to cite: it is fixed BY CONSTRUCTION as a
#: memory-safety ceiling, not a schema claim about how many records a bundle may
#: carry. The loader must hold the whole document to make the digest bind the
#: bytes it parses (below), so an unbounded read is an unbounded allocation
#: driven by a file on disk. A #2900-shaped payload is kilobytes per formation
#: session; anything at this scale is a different artefact and refusing is the
#: honest answer.
MAX_EVIDENCE_BYTES: Final = 64 * 1024 * 1024


class R6PitBundleError(RuntimeError):
    """The bundle is absent, mutable, malformed, or temporally inadmissible."""


@dataclass(frozen=True)
class R6PitRecord:
    formation_close: datetime
    identity_accepted_at: datetime
    share_accepted_at: datetime | None
    cik: str
    symbol: str
    security_title: str
    exchange: str
    current_shares: Decimal | None
    prior_shares: Decimal | None
    red_flag_scores: tuple[float, ...]
    red_flag_history_complete: bool


@dataclass(frozen=True)
class R6PitBundle:
    manifest_sha256: str
    payload_sha256: str
    records: tuple[R6PitRecord, ...]

    def records_at(self, formation_close: datetime) -> tuple[R6PitRecord, ...]:
        return tuple(record for record in self.records if record.formation_close == formation_close)

    def ranking_input_hash(self, formation_close: datetime) -> str:
        rows = [
            {
                "cik": record.cik,
                "current_shares": None if record.current_shares is None else str(record.current_shares),
                "exchange": record.exchange,
                "identity_accepted_at": record.identity_accepted_at.isoformat(),
                "prior_shares": None if record.prior_shares is None else str(record.prior_shares),
                "red_flag_history_complete": record.red_flag_history_complete,
                "red_flag_scores": list(record.red_flag_scores),
                "security_title": record.security_title,
                "share_accepted_at": (
                    None if record.share_accepted_at is None else record.share_accepted_at.isoformat()
                ),
                "symbol": record.symbol,
            }
            for record in self.records_at(formation_close)
        ]
        encoded = json.dumps(rows, sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(encoded).hexdigest()


def read_verified_document(path: Path) -> tuple[str, bytes]:
    """Return ``(sha256, bytes)`` from ONE read of ONE opened object.

    ⚠ The digest and the parse must see the SAME bytes (#2945). Hashing a
    pathname, closing it, then reopening that pathname to parse leaves a window
    in which the file is replaced: the loader then reports the ORIGINAL digest
    over CHANGED records, which is the one failure an evidence loader may never
    have. Holding a descriptor open and reading it twice is not a fix either --
    the second read still sees whatever the bytes are by then. The bytes are
    read once and both the digest and the parse are taken from that value.

    The regular-file / symlink rule is bound to the same descriptor for the same
    reason: ``Path.is_file()`` / ``Path.is_symlink()`` stat a PATHNAME that can
    be re-pointed before the open that follows. ``O_NOFOLLOW`` refuses a symlink
    at open time and ``fstat`` judges the object actually opened.
    """
    # ⚠ ``O_NONBLOCK`` is load-bearing, not defensive noise. Opening a FIFO for
    # reading BLOCKS until a writer appears, and that open happens before the
    # ``fstat`` that would reject it -- so a FIFO planted at an evidence path
    # would hang the loader forever where the old ``Path.is_file()`` refused it
    # at once. POSIX regular files ignore the flag, so nothing else changes.
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except OSError as exc:
        raise R6PitBundleError(f"evidence path must be a readable regular non-symlink file: {path}") from exc
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise R6PitBundleError(f"evidence path must be a regular non-symlink file: {path}")
        with os.fdopen(descriptor, "rb", closefd=False) as handle:
            # One byte past the ceiling, so "at the limit" and "over it" are
            # distinguishable without a separate stat (which would be a third
            # view of the file, and could disagree with this read).
            data = handle.read(MAX_EVIDENCE_BYTES + 1)
    finally:
        os.close(descriptor)
    if len(data) > MAX_EVIDENCE_BYTES:
        raise R6PitBundleError(f"evidence document exceeds {MAX_EVIDENCE_BYTES} bytes: {path}")
    return hashlib.sha256(data).hexdigest(), data


def _json_object(data: bytes, *, label: str) -> dict[str, Any]:
    """Parse the verified bytes -- never the pathname they came from."""
    try:
        value = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise R6PitBundleError(f"{label} must be a UTF-8 JSON document") from exc
    return _object(value, label=label)


def _object(value: object, *, label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise R6PitBundleError(f"{label} must be a JSON object with string keys")
    return value


def _timestamp(value: object, *, label: str) -> datetime:
    if not isinstance(value, str):
        raise R6PitBundleError(f"{label} must be an ISO timestamp")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise R6PitBundleError(f"{label} must be an ISO timestamp") from exc
    if parsed.tzinfo is not None:
        raise R6PitBundleError(f"{label} must use the SEC/New-York naive clock")
    return parsed


def _optional_positive_decimal(value: object, *, label: str) -> Decimal | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise R6PitBundleError(f"{label} must be a positive decimal string or null")
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise R6PitBundleError(f"{label} must be a positive decimal string or null") from exc
    if not parsed.is_finite() or parsed <= 0 or str(parsed) != value:
        raise R6PitBundleError(f"{label} must be a canonical positive decimal string or null")
    return parsed


def _record(value: object, *, index: int) -> R6PitRecord:
    row = _object(value, label=f"records[{index}]")
    formation = _timestamp(row.get("formation_close"), label=f"records[{index}].formation_close")
    identity_accepted = _timestamp(
        row.get("identity_accepted_at"),
        label=f"records[{index}].identity_accepted_at",
    )
    if identity_accepted > formation:
        raise R6PitBundleError(
            f"records[{index}] identity accepted at {identity_accepted.isoformat()} after formation "
            f"{formation.isoformat()}"
        )
    share_accepted_raw = row.get("share_accepted_at")
    share_accepted = (
        None
        if share_accepted_raw is None
        else _timestamp(share_accepted_raw, label=f"records[{index}].share_accepted_at")
    )
    if share_accepted is not None and share_accepted > formation:
        raise R6PitBundleError(
            f"records[{index}] shares accepted at {share_accepted.isoformat()} after formation {formation.isoformat()}"
        )
    text_fields: dict[str, str] = {}
    for name in ("cik", "symbol", "security_title", "exchange"):
        item = row.get(name)
        if not isinstance(item, str) or not item.strip():
            raise R6PitBundleError(f"records[{index}].{name} must be non-empty text")
        text_fields[name] = item.strip()
    if len(text_fields["cik"]) != 10 or not text_fields["cik"].isdigit():
        raise R6PitBundleError(f"records[{index}].cik must be ten decimal digits")
    if text_fields["symbol"] != text_fields["symbol"].upper():
        raise R6PitBundleError(f"records[{index}].symbol must be normalized uppercase")
    scores = row.get("red_flag_scores")
    if not isinstance(scores, list) or any(type(score) not in (int, float) or not 0 <= score <= 1 for score in scores):
        raise R6PitBundleError(f"records[{index}].red_flag_scores must contain values in [0,1]")
    complete = row.get("red_flag_history_complete")
    if type(complete) is not bool:
        raise R6PitBundleError(f"records[{index}].red_flag_history_complete must be boolean")
    current_shares = _optional_positive_decimal(row.get("current_shares"), label="current_shares")
    prior_shares = _optional_positive_decimal(row.get("prior_shares"), label="prior_shares")
    if (current_shares is None) != (prior_shares is None):
        raise R6PitBundleError(f"records[{index}] must carry both share counts or neither")
    if (current_shares is None) != (share_accepted is None):
        raise R6PitBundleError(f"records[{index}] share acceptance and share counts must be present together")
    return R6PitRecord(
        formation_close=formation,
        identity_accepted_at=identity_accepted,
        share_accepted_at=share_accepted,
        cik=text_fields["cik"],
        symbol=text_fields["symbol"],
        security_title=text_fields["security_title"],
        exchange=text_fields["exchange"],
        current_shares=current_shares,
        prior_shares=prior_shares,
        red_flag_scores=tuple(float(score) for score in scores),
        red_flag_history_complete=complete,
    )


def load_r6_pit_bundle(manifest_path: Path, *, expected_manifest_sha256: str) -> R6PitBundle:
    """Verify and load exactly the payload named by a frozen manifest."""
    manifest_sha, manifest_bytes = read_verified_document(manifest_path)
    if manifest_sha != expected_manifest_sha256:
        raise R6PitBundleError(
            f"manifest digest moved: expected {expected_manifest_sha256}, measured {manifest_sha}; "
            "historical ranking refused"
        )
    manifest = _json_object(manifest_bytes, label="manifest")
    if manifest.get("schema_version") != MANIFEST_SCHEMA:
        raise R6PitBundleError(f"unsupported manifest schema {manifest.get('schema_version')!r}")
    payload_meta = _object(manifest.get("payload"), label="manifest.payload")
    filename = payload_meta.get("filename")
    expected_sha = payload_meta.get("sha256")
    if not isinstance(filename, str) or Path(filename).name != filename:
        raise R6PitBundleError("manifest payload filename must be one safe basename")
    if not isinstance(expected_sha, str) or len(expected_sha) != 64:
        raise R6PitBundleError("manifest payload sha256 must be a 64-character digest")
    payload_path = manifest_path.parent / filename
    measured_sha, payload_bytes = read_verified_document(payload_path)
    if measured_sha != expected_sha:
        raise R6PitBundleError(
            f"payload digest moved: expected {expected_sha}, measured {measured_sha}; historical ranking refused"
        )
    payload = _json_object(payload_bytes, label="payload")
    if payload.get("schema_version") != PAYLOAD_SCHEMA:
        raise R6PitBundleError(f"unsupported payload schema {payload.get('schema_version')!r}")
    raw_records = payload.get("records")
    if not isinstance(raw_records, list):
        raise R6PitBundleError("payload.records must be an array")
    records = tuple(_record(value, index=index) for index, value in enumerate(raw_records))
    keys = [(record.formation_close, record.symbol) for record in records]
    if len(keys) != len(set(keys)):
        raise R6PitBundleError("duplicate formation/symbol natural key in payload")
    ordered = tuple(sorted(records, key=lambda record: (record.formation_close, record.symbol)))
    if records != ordered:
        raise R6PitBundleError("payload records must be sorted by formation_close then symbol")
    return R6PitBundle(manifest_sha256=manifest_sha, payload_sha256=measured_sha, records=records)


__all__ = [
    "MANIFEST_SCHEMA",
    "MAX_EVIDENCE_BYTES",
    "PAYLOAD_SCHEMA",
    "R6PitBundle",
    "R6PitBundleError",
    "R6PitRecord",
    "load_r6_pit_bundle",
    "read_verified_document",
]
