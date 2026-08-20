"""Pure helpers persisting what the thesis writer saw, per run (#2017).

`_assemble_context` (:mod:`app.services.thesis`) builds the writer's research
dict but does not persist it. These helpers derive compact, auditable
metadata from that dict — a content hash and a per-block
availability/status/as-of summary — stored on ``thesis_runs`` at run-insert.
Enough to audit availability-claim fabrication (#2007 Defect 2 class) and to
detect that sources moved, WITHOUT duplicating the source rows.

Pure: no DB, no I/O. ``prompt_version`` is a parameter (not an import) so this
module never depends on ``thesis`` — no import cycle.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping

# As-of field name per shaped context block (see thesis._assemble_context).
# List blocks are date-carrying element lists; the summary reports max(stamp).
_LIST_ASOF: dict[str, str] = {
    "fundamentals": "as_of_date",
    "filings": "filing_date",
    "news": "event_time",
}
_DICT_ASOF: dict[str, str] = {
    "prior_thesis": "created_at",
    "price_anchor": "price_date",
    "valuation": "price_as_of",
    # the band's OWN vintage (fair_value_band_current.as_of_date), NOT the
    # price leg (price_as_of) — the band is fundamentals-anchored (Codex ckpt-2).
    "fair_value_band": "as_of_date",
    "analytics_evidence": "as_of",
}
# A dict whose keys are ALL markers (no substantive payload) is "absent usable
# evidence", not present — e.g. a malformed/unsupported analytics wrapper.
_MARKER_KEYS: frozenset[str] = frozenset({"available", "reason", "status", "quality_status", "schema"})


def hash_context(context: Mapping[str, object]) -> str:
    """sha256 of the canonically-serialized context (stable key order, compact).

    Strict — no json ``default`` fallback. The thesis context is guaranteed
    JSON-shaped (shapers emit isoformat strings + float|None; ``_to_float``
    maps NaN/inf to None), so a non-serializable type is a bug to surface, not
    silently stringify. The fast-tier test proves the strict raise against a
    synthetic non-JSON input; ``tests/test_thesis.py::test_empty_surfaces_yield_honest_absences``
    hashes a REAL ``_assemble_context`` output, so the helpers are exercised against the true shapes,
    not just hand-built fixtures. At runtime the ``thesis.py`` call site wraps this so a raise degrades
    to NULL audit columns + a WARNING — never silently stringified into the hash, never aborts a thesis.

    Note: this fingerprints the exact context bytes; it does NOT prove
    "sources unchanged" by later recomputation (``_assemble_context`` is
    non-reproducible — the news query uses a wall-clock 30d cutoff). Drift is
    detected via the summary's as-of stamps, not by re-hashing.
    """
    blob = json.dumps(context, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def summarize_context(context: Mapping[str, object], prompt_version: str) -> dict[str, object]:
    """Per-block availability/status/as-of summary of the writer context.

    ``prompt_version`` is recorded (self-describing across future context-shape
    changes) — recording is not a ``_PROMPT_VERSION`` bump.
    """
    return {
        "prompt_version": prompt_version,
        "blocks": {key: _block_status(key, val) for key, val in context.items()},
    }


def _block_status(key: str, val: object) -> dict[str, object]:
    """Availability (+ optional status/as-of/count) for one context block.

    Total over the shapes ``_assemble_context`` actually produces (``.get()``
    only — no bracket indexing, no attribute access): every as-of field it
    emits is an ISO-8601 string, so ``max()`` over collected stamps is
    well-defined there. Not a guarantee over arbitrary/malformed shapes —
    heterogeneous non-orderable as-of values could make ``max()`` raise; the
    caller wraps the compute defensively as the backstop. A block absent
    from the maps still gets an ``available`` entry (drift-safe).
    """
    if val is None:
        return {"available": False}

    if isinstance(val, list):
        out: dict[str, object] = {"available": bool(val), "count": len(val)}
        asof_key = _LIST_ASOF.get(key)
        if asof_key is not None:
            stamps = [stamp for e in val if isinstance(e, Mapping) and (stamp := e.get(asof_key)) is not None]
            if stamps:
                # ISO date/timestamp strings sort lexicographically = chronologically.
                out["as_of"] = max(stamps)
        return out

    if isinstance(val, Mapping):
        if key == "risk_metrics":
            windows = val.get("windows") or []
            stamps = [stamp for w in windows if isinstance(w, Mapping) and (stamp := w.get("as_of_date")) is not None]
            out = {"available": True, "metric_version": val.get("metric_version")}
            if stamps:
                out["as_of"] = max(stamps)
            return out

        if "available" in val:
            available = bool(val["available"])
        else:
            # present iff it carries payload beyond status markers
            available = bool(set(val) - _MARKER_KEYS)
        out = {"available": available}
        for status_key in ("quality_status", "reason"):
            status = val.get(status_key)
            if status is not None:
                out["status"] = status
                break
        asof_key = _DICT_ASOF.get(key)
        if asof_key is not None:
            asof = val.get(asof_key)
            if asof is not None:
                out["as_of"] = asof
        return out

    # scalar / unexpected top-level type — defensive (not expected). The
    # `val is None` branch above already returned, so val is always a
    # non-None scalar here.
    return {"available": True}
