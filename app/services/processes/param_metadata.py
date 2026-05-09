"""ParamMetadata model + validate_job_params + materialise_scheduled_params.

PR1a of #1064 admin-control-hub follow-up sequence.
Plan: docs/internal/plans/pr1-job-registry-refactor.md (uncommitted).
Audit: docs/wiki/job-registry-audit.md.

## Why a Pydantic model + per-job tuple

Operator-locked decision: every scheduled/bootstrap job declares its
operator-exposable parameter surface as data, not code. The FE Advanced
disclosure (PR2) renders one form field per ``ParamMetadata`` entry
based on ``field_type`` — no bespoke per-job React components.

The model is a typed BE contract; ``frontend/src/api/types.ts`` carries
a hand-written mirror. **Drift between the two is a PREVENTION-grade
risk** — the FE renders generic Advanced disclosure fields off this
metadata, so a contract drift means operators see wrong inputs or no
inputs at all. Round-trip test in
``tests/test_param_metadata_round_trip.py`` covers one canonical job
per PR (full coverage is bot-enforced via review-bot reading
``frontend/src/api/types.ts`` against this module).

## Field-type taxonomy

10 archetypes, picked to cover every operator-exposable knob in
``docs/wiki/job-registry-audit.md`` §6:

* ``string``        — free text. Reserved; no operator field today
                      (provenance labels like ``source_label`` are
                      internal-only).
* ``int``           — number input with per-param ``min_value`` /
                      ``max_value`` bounds.
* ``float``         — number input with bounds + step. Reserved; no
                      operator field today (data-integrity-cliff knobs
                      like ``match_threshold`` are NOT exposed).
* ``date``          — date picker. ``start_date`` / ``end_date`` /
                      ``since`` / ``min_period_of_report``.
* ``quarter``       — bespoke ``YYYY[Q1-4]`` widget. Used by
                      ``cusip_universe_backfill``.
* ``ticker``        — typeahead resolves company-name/symbol → instrument_id (int).
* ``cik``           — typeahead resolves company-name/symbol → CIK (str).
* ``bool``          — checkbox. ``force_full`` / ``dry_run``.
* ``enum``          — single-select against ``enum_values``.
* ``multi_enum``    — multi-select against ``enum_values``.
                      ``filing_types`` / ``layer_allowlist`` /
                      ``source_filter``.

Rejected from the taxonomy: ``prefetch_urls``, ``follow_pagination``,
``use_bulk_zip``, ``paginate``, ``source_label``, ``match_threshold``.
Implementation-strategy knobs and provenance labels DO NOT belong in
operator UX — code review changes them.

## validate_job_params modes

Single validator, two modes:

* ``allow_internal_keys=False`` — manual API path. Unknown keys
  rejected with 400. This prevents the operator from setting a
  provenance ``source_label`` from the Advanced disclosure.
* ``allow_internal_keys=True`` — bootstrap dispatcher path. The
  per-job ``JOB_INTERNAL_KEYS`` allow-list permits audit-only keys
  (e.g. ``sec_13f_quarterly_sweep``: ``{"source_label"}``).

Both modes share the same coercion + bounds checking. Codex round-2
BLOCKING fix: parallel paths would drift; one validator with a flag
keeps the contract aligned.

## materialise_scheduled_params

Codex round-3 BLOCKING fix: scheduled cron fires must populate
``job_runs.params_snapshot`` from the registry's ``ParamMetadata.default``
values, not a raw ``{}``. Otherwise operator-history visibility regresses
(operator clicks a row, expects to see the effective params, gets ``{}``
instead). Helper resolves defaults at dispatch time so the snapshot
reflects what the invoker actually consumed.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict

ParamFieldType = Literal[
    "string",
    "int",
    "float",
    "date",
    "quarter",
    "ticker",
    "cik",
    "bool",
    "enum",
    "multi_enum",
]


class ParamMetadata(BaseModel):
    """Operator-exposable parameter declaration for a registered job.

    See module docstring for the field-type taxonomy and rationale.
    Mirrored in ``frontend/src/api/types.ts`` — drift between the two
    is the single biggest review-bot risk for the PR2 FE work.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str
    """Param key. Matches the kwarg the invoker reads from ``params``."""

    label: str
    """Operator-facing field label."""

    help_text: str
    """Operator-facing help text. Rendered as field hint in the form."""

    field_type: ParamFieldType
    """Picks the FE input widget. See module docstring."""

    default: Any | None = None
    """Default value when operator leaves field blank. ``None`` means
    'use the invoker's hardcoded default'; the FE renders an empty
    field. ``materialise_scheduled_params`` includes this in the
    snapshot only if non-None."""

    advanced_group: bool = True
    """When True, field renders inside the collapsed Advanced disclosure;
    when False, in the always-visible primary group. Most fields are
    advanced; rare exceptions (e.g. ``instrument_id`` triage targeting)
    surface in primary."""

    enum_values: tuple[str, ...] | None = None
    """Required for ``enum`` and ``multi_enum`` field types. None for
    every other type."""

    min_value: int | float | None = None
    """Optional lower bound for ``int`` / ``float`` field types."""

    max_value: int | float | None = None
    """Optional upper bound for ``int`` / ``float`` field types."""


# ---------------------------------------------------------------------------
# Per-job internal-key allow-list.
# ---------------------------------------------------------------------------
#
# Bootstrap dispatcher uses ``allow_internal_keys=True`` and may pass
# audit-only / provenance keys that operators must NOT be able to set
# via the manual API. Each entry in this dict permits the listed keys
# in addition to the job's ``ParamMetadata`` fields.
#
# Keep the entries minimal — every internal key is a knob that the
# operator cannot tune through the standard UX. Adding to this dict
# crosses a discipline boundary; document the reason in the value
# comment.

JOB_INTERNAL_KEYS: dict[str, frozenset[str]] = {
    # Bootstrap variant of sec_13f_quarterly_sweep tags rows with a
    # distinct source_label so audit history can distinguish bootstrap-
    # bounded sweeps from the standalone weekly historical sweep. The
    # operator never edits this — it lives in the bootstrap StageSpec's
    # params dict.
    "sec_13f_quarterly_sweep": frozenset({"source_label"}),
}


class ParamValidationError(ValueError):
    """Raised by ``validate_job_params`` on contract violation.

    The API layer maps this to 400 Bad Request with the message body.
    """


def _coerce_value(meta: ParamMetadata, raw: Any) -> Any:
    """Coerce a raw operator/dispatcher value to the declared field type.

    ``None`` passes through — the invoker treats it as 'not supplied'.
    Type mismatches raise ``ParamValidationError`` with a per-field
    message so the operator sees which field failed.
    """
    if raw is None:
        return None
    ft = meta.field_type
    try:
        if ft == "bool":
            if isinstance(raw, bool):
                return raw
            if isinstance(raw, str):
                lowered = raw.strip().lower()
                if lowered in {"true", "1", "yes", "on"}:
                    return True
                if lowered in {"false", "0", "no", "off"}:
                    return False
            raise ParamValidationError(f"param {meta.name!r}: cannot coerce {raw!r} to bool")
        if ft in {"int", "ticker"}:
            return int(raw)
        if ft == "float":
            return float(raw)
        if ft == "date":
            if isinstance(raw, date):
                return raw
            return date.fromisoformat(str(raw))
        if ft == "quarter":
            value = str(raw).strip().upper()
            # Format: YYYYQ[1-4], e.g. 2026Q1.
            if len(value) != 6 or value[4] != "Q" or value[5] not in "1234" or not value[:4].isdigit():
                raise ParamValidationError(f"param {meta.name!r}: quarter must match YYYY[Q1-4] (got {raw!r})")
            return value
        if ft == "cik":
            value = str(raw).strip()
            if not value.isdigit():
                raise ParamValidationError(f"param {meta.name!r}: cik must be a digit string (got {raw!r})")
            return value.zfill(10)
        if ft in {"string", "enum"}:
            return str(raw)
        if ft == "multi_enum":
            if not isinstance(raw, (list, tuple)):
                raise ParamValidationError(
                    f"param {meta.name!r}: multi_enum requires a list (got {type(raw).__name__})"
                )
            return [str(x) for x in raw]
    except ParamValidationError:
        raise
    except (TypeError, ValueError) as exc:
        raise ParamValidationError(f"param {meta.name!r}: cannot coerce {raw!r} to {ft} ({exc})") from exc
    raise ParamValidationError(f"param {meta.name!r}: unsupported field_type {ft!r}")


def _check_bounds(meta: ParamMetadata, value: Any) -> None:
    """Validate enum membership + min/max bounds. ``None`` skips."""
    if value is None:
        return
    if meta.field_type == "enum":
        assert meta.enum_values is not None
        if value not in meta.enum_values:
            raise ParamValidationError(f"param {meta.name!r}: value {value!r} not in enum_values {meta.enum_values}")
        return
    if meta.field_type == "multi_enum":
        assert meta.enum_values is not None
        for item in value:
            if item not in meta.enum_values:
                raise ParamValidationError(f"param {meta.name!r}: item {item!r} not in enum_values {meta.enum_values}")
        return
    if meta.field_type in {"int", "float"}:
        if meta.min_value is not None and value < meta.min_value:
            raise ParamValidationError(f"param {meta.name!r}: value {value} < min_value {meta.min_value}")
        if meta.max_value is not None and value > meta.max_value:
            raise ParamValidationError(f"param {meta.name!r}: value {value} > max_value {meta.max_value}")


def validate_job_params(
    job_name: str,
    params: Mapping[str, Any],
    *,
    allow_internal_keys: bool,
    metadata: tuple[ParamMetadata, ...] | None = None,
) -> dict[str, Any]:
    """Validate + coerce a params dict against the job's ``ParamMetadata``.

    Returns the coerced dict. Raises ``ParamValidationError`` on:

    * Unknown key (with ``allow_internal_keys=False`` and key not in
      ``ParamMetadata``).
    * Coercion failure (e.g. cannot parse string as date).
    * Bounds violation (enum membership, min/max).

    ``metadata`` is supplied by the call site so this module avoids a
    direct import of ``app/workers/scheduler.py``. The
    ``ParamValidationError`` carries the offending param name so the
    400 response identifies the field.
    """
    if metadata is None:
        # Look up via the registry. Local import to dodge cycles.
        metadata = _lookup_metadata(job_name)

    metadata_by_name = {meta.name: meta for meta in metadata}
    internal_keys = JOB_INTERNAL_KEYS.get(job_name, frozenset())

    # Reject unknown keys before coercion.
    for key in params:
        if key in metadata_by_name:
            continue
        if allow_internal_keys and key in internal_keys:
            continue
        raise ParamValidationError(
            f"unknown param {key!r} for job {job_name!r}"
            + (
                f" (declared params: {sorted(metadata_by_name)}, internal: {sorted(internal_keys)})"
                if allow_internal_keys
                else f" (declared params: {sorted(metadata_by_name)})"
            )
        )

    coerced: dict[str, Any] = {}
    for key, raw in params.items():
        if key in metadata_by_name:
            meta = metadata_by_name[key]
            value = _coerce_value(meta, raw)
            _check_bounds(meta, value)
            if value is not None:
                coerced[key] = value
        else:
            # Internal keys pass through verbatim (no metadata to coerce against).
            coerced[key] = raw

    return coerced


def materialise_scheduled_params(
    job_name: str,
    metadata: tuple[ParamMetadata, ...] | None = None,
) -> dict[str, Any]:
    """Build the params dict a scheduled fire of this job would invoke with.

    Reads each ``ParamMetadata.default``; omits ``None`` defaults so
    invoker logic can distinguish 'operator left it blank' from
    'operator set it to null'. The scheduled-fire path passes the
    result through ``validate_job_params(allow_internal_keys=False)``
    before invoker dispatch + ``params_snapshot`` write — so the
    snapshot reflects EFFECTIVE params, not raw defaults.

    Codex round-3 BLOCKING fix: snapshot path was previously
    underspecified; this helper makes it explicit.
    """
    if metadata is None:
        metadata = _lookup_metadata(job_name)
    return {meta.name: meta.default for meta in metadata if meta.default is not None}


def _lookup_metadata(job_name: str) -> tuple[ParamMetadata, ...]:
    """Look up a job's ``params_metadata`` from the scheduler registry.

    Local import to avoid a module-load cycle (scheduler imports this
    module for the ``ParamMetadata`` type; this function is only called
    at runtime, so the lazy import is safe).
    """
    from app.workers.scheduler import SCHEDULED_JOBS

    for job in SCHEDULED_JOBS:
        if job.name == job_name:
            return job.params_metadata
    raise ParamValidationError(f"job {job_name!r} not found in SCHEDULED_JOBS registry")


__all__ = [
    "JOB_INTERNAL_KEYS",
    "ParamFieldType",
    "ParamMetadata",
    "ParamValidationError",
    "materialise_scheduled_params",
    "validate_job_params",
]
