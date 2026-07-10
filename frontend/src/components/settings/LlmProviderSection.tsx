/**
 * LLM provider knobs (#1919, split #1995) — runtime_config llm_provider /
 * llm_base_url / llm_model_writer / llm_model_critic, edited via PATCH /config.
 *
 * Writer and critic are separate model knobs (#1995): the bulk memo writer
 * and the adversarial critic may run different local models. Provider and
 * base URL stay shared.
 *
 * Local-first: the default is an operator-local OpenAI-compatible endpoint
 * (Ollama). API keys are env-only (LLM_API_KEY / ANTHROPIC_API_KEY) and
 * intentionally have no UI here — runtime_config_audit stores old/new
 * values in plaintext, so keys must never flow through this form.
 */
import { useState } from "react";
import type { FormEvent } from "react";

import { patchConfig } from "@/api/config";
import { SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { useConfig } from "@/lib/ConfigContext";

// Must match VALID_LLM_PROVIDERS in app/services/runtime_config.py.
const LLM_PROVIDERS = ["openai_compatible", "anthropic"] as const;

export function LlmProviderSection(): JSX.Element {
  const config = useConfig();

  // Local override > server value (BudgetConfigSection pattern): null
  // means "not edited", so a refetch cleanly repopulates from the server.
  const [provider, setProvider] = useState<string | null>(null);
  const [baseUrl, setBaseUrl] = useState<string | null>(null);
  const [writerModel, setWriterModel] = useState<string | null>(null);
  const [criticModel, setCriticModel] = useState<string | null>(null);
  const [reason, setReason] = useState("");
  const [saving, setSaving] = useState(false);
  const [success, setSuccess] = useState(false);
  const [error, setError] = useState(false);

  const runtime = config.data?.runtime ?? null;
  const displayedProvider = provider ?? runtime?.llm_provider ?? "openai_compatible";
  const displayedBaseUrl = baseUrl ?? runtime?.llm_base_url ?? "";
  const displayedWriterModel = writerModel ?? runtime?.llm_model_writer ?? "";
  const displayedCriticModel = criticModel ?? runtime?.llm_model_critic ?? "";

  const nothingChanged =
    provider === null && baseUrl === null && writerModel === null && criticModel === null;
  const reasonMissing = reason.trim().length === 0;

  async function handleSave(e: FormEvent) {
    e.preventDefault();
    setSaving(true);
    setError(false);
    setSuccess(false);
    try {
      // Only send fields that actually changed — the backend rejects
      // no-op patches with 422.
      await patchConfig({
        updated_by: "operator",
        reason,
        llm_provider: provider ?? undefined,
        llm_base_url: baseUrl ?? undefined,
        llm_model_writer: writerModel ?? undefined,
        llm_model_critic: criticModel ?? undefined,
      });
      setProvider(null);
      setBaseUrl(null);
      setWriterModel(null);
      setCriticModel(null);
      setReason("");
      setSuccess(true);
      config.refetch();
    } catch (err: unknown) {
      console.error("Failed to update LLM provider config", err);
      setError(true);
    } finally {
      setSaving(false);
    }
  }

  return (
    <section className="border-t border-slate-200 dark:border-slate-800 pt-3">
      <header>
        <h2 className="text-[11px] font-semibold uppercase tracking-[0.08em] text-slate-700">
          LLM Provider
        </h2>
        <p className="mt-1 text-xs text-slate-500">
          Thesis generation endpoint. Default is a local OpenAI-compatible
          server (Ollama) — no API key, no cloud spend. API keys stay in the
          server environment (LLM_API_KEY / ANTHROPIC_API_KEY), never here.
        </p>
      </header>

      <div className="mt-3">
        {config.loading ? (
          <SectionSkeleton rows={3} />
        ) : config.error !== null || runtime === null ? (
          <SectionError onRetry={config.refetch} />
        ) : (
          <form onSubmit={(e) => void handleSave(e)} className="space-y-3">
            <div className="flex flex-wrap items-end gap-4">
              <label className="block">
                <span className="text-xs text-slate-500">Provider</span>
                <select
                  value={displayedProvider}
                  onChange={(e) => {
                    const next = e.target.value;
                    setProvider(next === runtime.llm_provider ? null : next);
                    // Review round 2 WARNING: switching to anthropic disables
                    // the Base URL input — drop any pending override so Save
                    // can't silently submit a stale llm_base_url change (and
                    // an unintended audit row) alongside the provider switch.
                    if (next === "anthropic") {
                      setBaseUrl(null);
                    }
                  }}
                  disabled={saving}
                  className="mt-1 block rounded border border-slate-300 dark:border-slate-700 px-2 py-1.5 text-sm"
                >
                  {LLM_PROVIDERS.map((p) => (
                    <option key={p} value={p}>
                      {p}
                    </option>
                  ))}
                </select>
              </label>

              <label className="block">
                <span className="text-xs text-slate-500">Writer model</span>
                <input
                  type="text"
                  value={displayedWriterModel}
                  onChange={(e) =>
                    setWriterModel(e.target.value === runtime.llm_model_writer ? null : e.target.value)
                  }
                  disabled={saving}
                  placeholder="qwen3:14b"
                  className="mt-1 block w-56 rounded border border-slate-300 dark:border-slate-700 px-2 py-1.5 text-sm"
                />
              </label>

              <label className="block">
                <span className="text-xs text-slate-500">Critic model</span>
                <input
                  type="text"
                  value={displayedCriticModel}
                  onChange={(e) =>
                    setCriticModel(e.target.value === runtime.llm_model_critic ? null : e.target.value)
                  }
                  disabled={saving}
                  placeholder="qwen3:14b"
                  className="mt-1 block w-56 rounded border border-slate-300 dark:border-slate-700 px-2 py-1.5 text-sm"
                />
              </label>
            </div>

            <label className="block">
              <span className="text-xs text-slate-500">Base URL (OpenAI-compatible)</span>
              <input
                type="text"
                value={displayedBaseUrl}
                onChange={(e) => setBaseUrl(e.target.value === runtime.llm_base_url ? null : e.target.value)}
                disabled={saving || displayedProvider === "anthropic"}
                placeholder="http://localhost:11434/v1"
                className="mt-1 block w-full max-w-md rounded border border-slate-300 dark:border-slate-700 px-2 py-1.5 text-sm disabled:opacity-50"
              />
              {displayedProvider === "anthropic" && (
                <span className="mt-1 block text-[11px] text-slate-400">
                  Not used by the Anthropic provider (SDK endpoint; requires ANTHROPIC_API_KEY in the
                  server environment).
                </span>
              )}
            </label>

            <label className="block">
              <span className="text-xs text-slate-500">Reason (required)</span>
              <input
                type="text"
                value={reason}
                onChange={(e) => setReason(e.target.value)}
                disabled={saving}
                placeholder="Why are you changing this?"
                className="mt-1 block w-full max-w-md rounded border border-slate-300 dark:border-slate-700 px-2 py-1.5 text-sm"
              />
            </label>

            <button
              type="submit"
              disabled={saving || reasonMissing || nothingChanged}
              className="rounded bg-slate-800 px-3 py-1.5 text-sm font-medium text-white hover:bg-slate-700 disabled:cursor-not-allowed disabled:opacity-50"
            >
              {saving ? "Saving..." : "Save LLM config"}
            </button>

            {success && (
              <p className="rounded bg-emerald-50 dark:bg-emerald-950/40 px-2 py-1.5 text-xs text-emerald-700 dark:text-emerald-300">
                LLM provider config updated.
              </p>
            )}
            {error && (
              <p
                role="alert"
                className="rounded bg-rose-50 dark:bg-rose-950/40 px-2 py-1.5 text-xs text-rose-700 dark:text-rose-300"
              >
                Failed to save LLM config. Check the browser console for details.
              </p>
            )}
          </form>
        )}
      </div>
    </section>
  );
}
