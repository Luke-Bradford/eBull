import { useEffect, useState } from "react";
import { apiFetch } from "@/api/client";

const SUPPORTED_CURRENCIES = ["GBP", "USD", "EUR"] as const;

interface Props {
  currentCurrency: string;
  onChanged: () => void;
}

export function DisplayCurrencySection({ currentCurrency, onChanged }: Props) {
  const [selected, setSelected] = useState(currentCurrency);

  // Sync local state when prop updates (e.g. after save + reload).
  useEffect(() => {
    setSelected(currentCurrency);
  }, [currentCurrency]);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function handleSave() {
    if (selected === currentCurrency) return;
    setSaving(true);
    setError(null);
    try {
      await apiFetch("/config", {
        method: "PATCH",
        body: JSON.stringify({
          updated_by: "operator",
          reason: `Changed display currency to ${selected}`,
          display_currency: selected,
        }),
      });
      onChanged();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to save");
    } finally {
      setSaving(false);
    }
  }

  return (
    <section className="space-y-4">
      <div>
        <h2 className="text-sm font-medium text-slate-700">Display currency</h2>
        <p className="text-xs text-slate-500">
          All monetary values across the dashboard will be converted to and
          displayed in this currency.
        </p>
      </div>

      <div className="max-w-sm space-y-3 rounded border border-slate-200 dark:border-slate-800 bg-white p-4">
        <div className="flex items-center gap-3">
          <select
            value={selected}
            onChange={(e) => setSelected(e.target.value)}
            disabled={saving}
            className="rounded border border-slate-300 dark:border-slate-700 px-2 py-1.5 text-sm"
          >
            {SUPPORTED_CURRENCIES.map((c) => (
              <option key={c} value={c}>
                {c}
              </option>
            ))}
          </select>
          <button
            type="button"
            onClick={() => void handleSave()}
            disabled={saving || selected === currentCurrency}
            className="rounded bg-slate-800 px-3 py-1.5 text-sm font-medium text-white disabled:bg-slate-400"
          >
            {saving ? "Saving..." : "Save currency"}
          </button>
        </div>
        {error !== null && (
          <div
            role="alert"
            className="rounded bg-rose-50 px-2 py-1.5 text-xs text-rose-700"
          >
            {error}
          </div>
        )}
      </div>
    </section>
  );
}
