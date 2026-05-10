/**
 * AdvancedParamsForm — operator-tunable params form on the drill-in.
 *
 * Issue #1064 PR2 — admin control hub Advanced disclosure renderer.
 *
 * Renders one form field per ``ParamMetadata`` entry surfaced on
 * ``ProcessRowResponse.params_metadata``. The drill-in's Advanced tab
 * wires this component to ``runJob(jobName, {params})`` so an operator
 * can re-fetch a scheduled job with custom params (e.g.
 * ``min_period_of_report=2024-01-01`` for ``sec_13f_quarterly_sweep``)
 * without raw API POSTs.
 *
 * Field rendering matrix matches ``_coerce_value`` in
 * ``app/services/processes/param_metadata.py``:
 *
 *   string | ticker (int / instrument_id) | cik   → text/number input
 *   int | float                                   → number input
 *   date                                          → HTML5 date input
 *   quarter                                       → text with regex hint
 *   bool                                          → checkbox
 *   enum                                          → select
 *   multi_enum                                    → checkbox list
 *
 * BE-side ``validate_job_params`` is the authoritative validator;
 * the FE produces JSON-safe values and surfaces the BE 400 detail
 * string on rejection. Empty optional fields are omitted from the
 * submit payload — manual ``/jobs/{name}/run`` does NOT materialise
 * registry defaults (that helper is scheduled-fire only), so absent
 * keys fall through to the invoker's own ``params.get(key, fallback)``
 * default.
 */

import { useCallback, useState } from "react";

import type { ParamFieldType, ParamMetadata } from "@/api/types";

export interface AdvancedParamsFormProps {
  readonly metadata: readonly ParamMetadata[];
  readonly busy: boolean;
  readonly onSubmit: (params: Record<string, unknown>) => Promise<void>;
  readonly submitLabel?: string;
}

type FieldValue = string | boolean | string[];

function defaultValueFor(meta: ParamMetadata): FieldValue {
  if (meta.field_type === "bool") {
    return meta.default === true;
  }
  if (meta.field_type === "multi_enum") {
    return Array.isArray(meta.default) ? (meta.default as string[]) : [];
  }
  if (meta.default == null) return "";
  // Coerce primitive defaults to a stable string for controlled inputs;
  // submit-time coercion (Number(), array building) restores the right
  // JSON shape from the form state.
  return String(meta.default);
}

function isLocalEmpty(field_type: ParamFieldType, value: FieldValue): boolean {
  if (field_type === "bool") return false; // bools always submit
  if (field_type === "multi_enum")
    return Array.isArray(value) && value.length === 0;
  return value === "" || value === null || value === undefined;
}

function coerceForSubmit(
  meta: ParamMetadata,
  raw: FieldValue,
): { ok: true; value: unknown } | { ok: false; error: string } {
  switch (meta.field_type) {
    case "string":
    case "cik":
    case "date":
    case "quarter":
    case "enum":
      return { ok: true, value: raw };
    case "int":
    case "float":
    case "ticker": {
      // ticker is wired as ``int(raw)`` in BE _coerce_value (PR1a:
      // operator passes instrument_id, not symbol; symbol → id is a
      // future PR). Same numeric coercion as int.
      const n = Number(raw);
      if (!Number.isFinite(n)) {
        // Defense in depth: HTML5 number input blocks non-numeric
        // chars at the keystroke layer, so this branch only fires if
        // a programmatic value (Infinity, NaN) reaches the form
        // state. JSON.stringify emits null for those, which 400s on
        // BE with a confusing message — surface a field-specific
        // error inline instead.
        return {
          ok: false,
          error: `${meta.label}: must be a finite number`,
        };
      }
      return { ok: true, value: n };
    }
    case "bool":
      return { ok: true, value: raw === true };
    case "multi_enum":
      return { ok: true, value: Array.isArray(raw) ? raw : [] };
  }
}

export function AdvancedParamsForm({
  metadata,
  busy,
  onSubmit,
  submitLabel = "Run with these params",
}: AdvancedParamsFormProps) {
  const [values, setValues] = useState<Record<string, FieldValue>>(() => {
    const seed: Record<string, FieldValue> = {};
    for (const meta of metadata) {
      seed[meta.name] = defaultValueFor(meta);
    }
    return seed;
  });
  const [localError, setLocalError] = useState<string | null>(null);

  const updateValue = useCallback((name: string, value: FieldValue) => {
    setValues((prev) => ({ ...prev, [name]: value }));
  }, []);

  const handleSubmit = useCallback(
    async (event: React.FormEvent<HTMLFormElement>) => {
      event.preventDefault();
      setLocalError(null);
      const payload: Record<string, unknown> = {};
      for (const meta of metadata) {
        const raw = values[meta.name];
        if (raw === undefined) continue;
        if (isLocalEmpty(meta.field_type, raw)) continue;
        const coerced = coerceForSubmit(meta, raw);
        if (!coerced.ok) {
          setLocalError(coerced.error);
          return;
        }
        payload[meta.name] = coerced.value;
      }
      await onSubmit(payload);
    },
    [metadata, values, onSubmit],
  );

  if (metadata.length === 0) {
    return (
      <p className="text-sm text-slate-500 dark:text-slate-400">
        This job declares no operator-exposable params.
      </p>
    );
  }

  return (
    <form onSubmit={handleSubmit} className="space-y-4">
      {metadata.map((meta) => (
        <FieldRow
          key={meta.name}
          meta={meta}
          value={values[meta.name] ?? defaultValueFor(meta)}
          busy={busy}
          onChange={(v) => updateValue(meta.name, v)}
        />
      ))}
      {localError ? (
        <p
          role="alert"
          className="text-xs text-red-700 dark:text-red-300"
        >
          {localError}
        </p>
      ) : null}
      <div>
        <button
          type="submit"
          disabled={busy}
          className="rounded border border-slate-300 bg-white px-3 py-1 text-sm font-medium text-slate-700 hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-50 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-200 dark:hover:bg-slate-800/40"
        >
          {submitLabel}
        </button>
      </div>
    </form>
  );
}

function FieldRow({
  meta,
  value,
  busy,
  onChange,
}: {
  meta: ParamMetadata;
  value: FieldValue;
  busy: boolean;
  onChange: (v: FieldValue) => void;
}) {
  const id = `advanced-param-${meta.name}`;
  return (
    <div className="space-y-1">
      <label
        htmlFor={id}
        className="block text-sm font-medium text-slate-700 dark:text-slate-200"
      >
        {meta.label}
      </label>
      <FieldInput id={id} meta={meta} value={value} busy={busy} onChange={onChange} />
      <p className="text-xs text-slate-500 dark:text-slate-400">
        {meta.help_text}
      </p>
    </div>
  );
}

function FieldInput({
  id,
  meta,
  value,
  busy,
  onChange,
}: {
  id: string;
  meta: ParamMetadata;
  value: FieldValue;
  busy: boolean;
  onChange: (v: FieldValue) => void;
}) {
  const baseInputClass =
    "block w-full rounded border border-slate-300 bg-white px-2 py-1 text-sm text-slate-800 disabled:cursor-not-allowed disabled:opacity-50 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100";

  switch (meta.field_type) {
    case "bool":
      return (
        <input
          id={id}
          type="checkbox"
          checked={value === true}
          disabled={busy}
          onChange={(e) => onChange(e.target.checked)}
          className="h-4 w-4 rounded border-slate-300 text-blue-600 dark:border-slate-700"
        />
      );
    case "enum": {
      // Defensive: BE rejects field_type='enum' with enum_values=null at
      // ParamMetadata construction (validate_job_params raises). If a
      // misconfigured row slips through, fall back to a text input + a
      // console warning rather than rendering an empty <select>.
      if (meta.enum_values == null) {
        // eslint-disable-next-line no-console
        console.warn(
          `AdvancedParamsForm: param ${meta.name} has field_type='enum' but no enum_values; falling back to text`,
        );
        return (
          <input
            id={id}
            type="text"
            value={String(value ?? "")}
            disabled={busy}
            onChange={(e) => onChange(e.target.value)}
            className={baseInputClass}
          />
        );
      }
      return (
        <select
          id={id}
          value={String(value ?? "")}
          disabled={busy}
          onChange={(e) => onChange(e.target.value)}
          className={baseInputClass}
        >
          <option value="">— unset —</option>
          {meta.enum_values.map((opt) => (
            <option key={opt} value={opt}>
              {opt}
            </option>
          ))}
        </select>
      );
    }
    case "multi_enum": {
      if (meta.enum_values == null) {
        // eslint-disable-next-line no-console
        console.warn(
          `AdvancedParamsForm: param ${meta.name} has field_type='multi_enum' but no enum_values; falling back to text`,
        );
        return (
          <input
            id={id}
            type="text"
            value={Array.isArray(value) ? value.join(",") : ""}
            disabled={busy}
            onChange={(e) => onChange(e.target.value.split(",").map((s) => s.trim()).filter(Boolean))}
            className={baseInputClass}
          />
        );
      }
      const selected = new Set(Array.isArray(value) ? value : []);
      return (
        <div role="group" aria-labelledby={id} className="flex flex-wrap gap-3">
          {meta.enum_values.map((opt) => {
            const checkboxId = `${id}-${opt}`;
            return (
              <label
                key={opt}
                htmlFor={checkboxId}
                className="flex items-center gap-1 text-sm text-slate-700 dark:text-slate-200"
              >
                <input
                  id={checkboxId}
                  type="checkbox"
                  checked={selected.has(opt)}
                  disabled={busy}
                  onChange={(e) => {
                    const next = new Set(selected);
                    if (e.target.checked) next.add(opt);
                    else next.delete(opt);
                    onChange(Array.from(next));
                  }}
                  className="h-4 w-4 rounded border-slate-300 text-blue-600 dark:border-slate-700"
                />
                {opt}
              </label>
            );
          })}
        </div>
      );
    }
    case "int":
    case "float":
    case "ticker": {
      // ticker is wired as int(raw) in the BE coercer (instrument_id,
      // not symbol resolution — symbol → id is a future PR). Same
      // input shape as int.
      const step = meta.field_type === "float" ? "any" : "1";
      return (
        <input
          id={id}
          type="number"
          step={step}
          min={meta.min_value ?? undefined}
          max={meta.max_value ?? undefined}
          value={String(value ?? "")}
          disabled={busy}
          onChange={(e) => onChange(e.target.value)}
          className={baseInputClass}
        />
      );
    }
    case "date":
      return (
        <input
          id={id}
          type="date"
          value={String(value ?? "")}
          disabled={busy}
          onChange={(e) => onChange(e.target.value)}
          className={baseInputClass}
        />
      );
    case "quarter":
      return (
        <input
          id={id}
          type="text"
          inputMode="text"
          pattern="\d{4}Q[1-4]"
          placeholder="2026Q1"
          value={String(value ?? "")}
          disabled={busy}
          onChange={(e) => onChange(e.target.value)}
          className={baseInputClass}
        />
      );
    case "cik":
      return (
        <input
          id={id}
          type="text"
          inputMode="numeric"
          pattern="\d{1,10}"
          placeholder="0000320193"
          value={String(value ?? "")}
          disabled={busy}
          onChange={(e) => onChange(e.target.value)}
          className={baseInputClass}
        />
      );
    case "string":
      return (
        <input
          id={id}
          type="text"
          value={String(value ?? "")}
          disabled={busy}
          onChange={(e) => onChange(e.target.value)}
          className={baseInputClass}
        />
      );
  }
}
