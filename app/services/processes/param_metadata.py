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
    # PR1c #1064 — bootstrap-only invokers promoted from bespoke
    # wrappers. These jobs are NOT in SCHEDULED_JOBS today (no cron
    # cadence), so they have no operator-facing ``ParamMetadata``;
    # the bootstrap dispatcher passes the wrapper's former hardcoded
    # values via ``StageSpec.params`` and the validator permits the
    # keys here under ``allow_internal_keys=True``. The manual API
    # path uses ``allow_internal_keys=False`` and would therefore
    # reject these keys — operator-tunability is deferred to a
    # future PR that promotes the jobs into SCHEDULED_JOBS with
    # proper ``ParamMetadata`` declarations.
    "filings_history_seed": frozenset({"days_back", "filing_types", "instrument_id"}),
    "sec_first_install_drain": frozenset({"max_subjects"}),
}


# ---------------------------------------------------------------------------
# MANUAL_TRIGGER_JOB_METADATA — operator params for manual-only jobs.
# ---------------------------------------------------------------------------
#
# Sibling to JOB_INTERNAL_KEYS for the inverse case: jobs that are
# operator-triggered (not orchestrator-dispatched) AND need full
# ParamMetadata validation via the manual API path, but have no
# cadence so they don't belong in SCHEDULED_JOBS. _lookup_metadata
# falls through to this dict so validate_job_params(allow_internal_keys=
# False) accepts the declared keys.
#
# Companion source-lock registry lives at app/jobs/sources.py
# MANUAL_TRIGGER_JOB_SOURCES — JobLock would otherwise KeyError at
# acquisition because its registry is built only from SCHEDULED_JOBS +
# _BOOTSTRAP_STAGE_SPECS.
#
# Adding to this dict crosses no discipline boundary by itself, but
# every entry needs a matching MANUAL_TRIGGER_JOB_SOURCES entry +
# _INVOKERS registration; tests/test_layer_123_wiring.py covers the
# triangle.

MANUAL_TRIGGER_JOB_METADATA: dict[str, tuple[ParamMetadata, ...]] = {
    # sec_rebuild — operator manual triage (#1155). Resets manifest +
    # scheduler rows for a scope, then (default) runs a discovery pass
    # against SEC submissions.json to fill missed accessions.
    "sec_rebuild": (
        ParamMetadata(
            name="instrument_id",
            label="Instrument ID",
            help_text=(
                "Numeric instrument_id from the instruments table. "
                "Triggers rebuild for every (subject, source) triple "
                "associated with this instrument."
            ),
            field_type="int",
            default=None,
            advanced_group=False,
            min_value=1,
        ),
        ParamMetadata(
            name="filer_cik",
            label="Filer CIK",
            help_text=(
                "CIK of an institutional or blockholder filer. Triggers "
                "rebuild for all that filer's 13F-HR / 13D / 13G "
                "history. Typeahead resolves company-name/symbol to CIK."
            ),
            field_type="cik",
            default=None,
            advanced_group=False,
        ),
        ParamMetadata(
            name="source",
            label="ManifestSource",
            help_text=(
                "Universe-wide rebuild for one source "
                "(sec_form4 / sec_13d / etc). Most expensive option. "
                "Note: sec_xbrl_facts / sec_n_csr / sec_10q / "
                "finra_short_interest may resolve to zero triples if "
                "data_freshness_index has no rows for that source, OR "
                "reset triples that the manifest worker then "
                "debug-skips (no parser registered yet). Operator-"
                "visible outcome is scope_triples=N + "
                "discovery_new=0 in the job log."
            ),
            field_type="enum",
            enum_values=(
                "sec_form3",
                "sec_form4",
                "sec_form5",
                "sec_13d",
                "sec_13g",
                "sec_13f_hr",
                "sec_def14a",
                "sec_n_port",
                "sec_n_csr",
                "sec_10k",
                "sec_10q",
                "sec_8k",
                "sec_xbrl_facts",
                "finra_short_interest",
            ),
            default=None,
            advanced_group=False,
        ),
        ParamMetadata(
            name="discover",
            label="Run discovery pass",
            help_text=(
                "If true (default), runs check_freshness against every "
                "CIK in scope to fill missed accessions. Set false to "
                "skip the SEC fetches and only flip already-known "
                "accessions back to pending."
            ),
            field_type="bool",
            default=True,
            advanced_group=True,
        ),
    ),
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
    """Validate enum membership + min/max bounds. ``None`` skips.

    Misconfigured ParamMetadata (e.g. ``field_type='enum'`` with
    ``enum_values=None``) raises ``ParamValidationError`` so the API layer
    maps to 400, not ``AssertionError`` which would escape to 500 (and be
    silently skipped under ``python -O``). Review-bot PR1a BLOCKING.
    """
    if value is None:
        return
    if meta.field_type == "enum":
        if meta.enum_values is None:
            raise ParamValidationError(f"param {meta.name!r}: field_type='enum' requires enum_values to be set")
        if value not in meta.enum_values:
            raise ParamValidationError(f"param {meta.name!r}: value {value!r} not in enum_values {meta.enum_values}")
        return
    if meta.field_type == "multi_enum":
        if meta.enum_values is None:
            raise ParamValidationError(f"param {meta.name!r}: field_type='multi_enum' requires enum_values to be set")
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

    Returns the job's ``params_metadata`` tuple if registered in
    ``SCHEDULED_JOBS``. Returns an empty tuple for bootstrap-only
    invokers (job_names present in ``_BOOTSTRAP_STAGE_SPECS`` but not
    in ``SCHEDULED_JOBS``) — those have no operator-exposable params
    today (the bootstrap dispatcher passes internal-only keys via
    ``JOB_INTERNAL_KEYS`` + ``allow_internal_keys=True``). PR1c may
    promote some of these to ``SCHEDULED_JOBS`` with their own
    ``params_metadata``.

    The bootstrap-only fallback addresses the PR1a review-bot WARNING:
    raising on unregistered job_names would break PR1b's bootstrap
    dispatch wiring (e.g. ``sec_bulk_download`` is bootstrap-only).

    Local import to avoid a module-load cycle (scheduler imports this
    module for the ``ParamMetadata`` type; this function is only called
    at runtime, so the lazy import is safe).
    """
    from app.workers.scheduler import SCHEDULED_JOBS

    for job in SCHEDULED_JOBS:
        if job.name == job_name:
            return job.params_metadata
    # Pass 2: manual-trigger-only jobs (sec_rebuild and future
    # operator-triggered tools). #1155.
    if job_name in MANUAL_TRIGGER_JOB_METADATA:
        return MANUAL_TRIGGER_JOB_METADATA[job_name]
    # Fallback for bootstrap-only invokers. Empty tuple means:
    #   - Manual API path (allow_internal_keys=False): every supplied key
    #     is rejected as unknown — operators cannot manually trigger
    #     bootstrap-only jobs through the standard /jobs/<name>/run path
    #     with arbitrary params (correct: bootstrap-only jobs are
    #     orchestrator-owned).
    #   - Bootstrap path (allow_internal_keys=True): only keys in
    #     JOB_INTERNAL_KEYS[job_name] are accepted; the StageSpec.params
    #     dict carries the internal-only knobs.
    return ()


__all__ = [
    "JOB_INTERNAL_KEYS",
    "MANUAL_TRIGGER_JOB_METADATA",
    "ParamFieldType",
    "ParamMetadata",
    "ParamValidationError",
    "materialise_scheduled_params",
    "validate_job_params",
]
