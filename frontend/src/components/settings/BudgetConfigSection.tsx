import { useState } from "react";
import type { FormEvent } from "react";
import {
  createCapitalEvent,
  fetchBudgetConfig,
  fetchCapitalEvents,
  updateBudgetConfig,
} from "@/api/budget";
import type { CapitalEventResponse } from "@/api/types";
import { SectionSkeleton, SectionError } from "@/components/dashboard/Section";
import { formatDateTime, formatMoney } from "@/lib/format";
import { useAsync } from "@/lib/useAsync";

export function BudgetConfigSection() {
  // ---- Config state ----
  const config = useAsync(fetchBudgetConfig, []);
  const [cashBufferPct, setCashBufferPct] = useState<number | null>(null);
  const [cgtScenario, setCgtScenario] = useState<"basic" | "higher" | null>(
    null,
  );
  const [configReason, setConfigReason] = useState("");
  const [configSaving, setConfigSaving] = useState(false);
  const [configSuccess, setConfigSuccess] = useState(false);
  const [configError, setConfigError] = useState(false);

  // Derive displayed values: local override > server value > default.
  // Backend stores cash_buffer_pct as a fraction (0.05 = 5%); the input
  // operates on a percentage scale so we multiply by 100 for display and
  // divide by 100 when sending.
  const serverBufferPct =
    config.data?.cash_buffer_pct != null
      ? Math.round(config.data.cash_buffer_pct * 100)
      : null;
  const displayedBufferPct = cashBufferPct ?? serverBufferPct ?? 5;
  const displayedCgtScenario =
    cgtScenario ??
    (config.data?.cgt_scenario === "higher" ? "higher" : "basic");

  async function handleConfigSave(e: FormEvent) {
    e.preventDefault();
    setConfigSaving(true);
    setConfigError(false);
    setConfigSuccess(false);
    try {
      // Only send fields that actually changed — the backend rejects
      // no-op patches with 422 "no fields changed".
      const bufferChanged = cashBufferPct !== null;
      const scenarioChanged = cgtScenario !== null;

      await updateBudgetConfig({
        // Convert percentage (display) back to fraction (API).
        cash_buffer_pct: bufferChanged ? displayedBufferPct / 100 : undefined,
        cgt_scenario: scenarioChanged ? displayedCgtScenario : undefined,
        updated_by: "operator",
        reason: configReason,
      });
      setCashBufferPct(null);
      setCgtScenario(null);
      setConfigReason("");
      setConfigSuccess(true);
      // Refetch after local state resets so the re-render from refetch
      // completion sees null overrides → uses new server values cleanly.
      config.refetch();
    } catch (err: unknown) {
      console.error("Failed to update budget config", err);
      setConfigError(true);
    } finally {
      setConfigSaving(false);
    }
  }

  // ---- Capital event form state ----
  const [eventType, setEventType] = useState<"injection" | "withdrawal">(
    "injection",
  );
  const [eventAmount, setEventAmount] = useState("");
  const [eventCurrency, setEventCurrency] = useState<"USD" | "GBP">("USD");
  const [eventNote, setEventNote] = useState("");
  const [eventSaving, setEventSaving] = useState(false);
  const [eventSuccess, setEventSuccess] = useState(false);
  const [eventError, setEventError] = useState(false);

  // ---- Capital events history ----
  const events = useAsync(() => fetchCapitalEvents(20, 0), []);

  async function handleEventSubmit(e: FormEvent) {
    e.preventDefault();
    const parsed = Number(eventAmount);
    if (!Number.isFinite(parsed) || parsed <= 0) return;
    setEventSaving(true);
    setEventError(false);
    setEventSuccess(false);
    try {
      await createCapitalEvent({
        event_type: eventType,
        amount: parsed,
        currency: eventCurrency,
        note: eventNote.trim() || undefined,
      });
      events.refetch();
      setEventType("injection");
      setEventAmount("");
      setEventCurrency("USD");
      setEventNote("");
      setEventSuccess(true);
    } catch (err: unknown) {
      console.error("Failed to create capital event", err);
      setEventError(true);
    } finally {
      setEventSaving(false);
    }
  }

  const configReasonMissing = configReason.trim().length === 0;
  const configNothingChanged = cashBufferPct === null && cgtScenario === null;
  const parsedEventAmount = Number(eventAmount);
  const eventAmountInvalid =
    eventAmount === "" || !Number.isFinite(parsedEventAmount) || parsedEventAmount <= 0;

  return (
    <section className="rounded-md border border-slate-200 bg-white shadow-sm">
      <header className="border-b border-slate-100 px-4 py-3">
        <h2 className="text-sm font-semibold text-slate-700">
          Budget Configuration
        </h2>
      </header>

      <div className="space-y-6 p-4">
        {/* ---- Sub-section 1: Config Controls ---- */}
        <div>
          <h3 className="text-xs font-semibold uppercase tracking-wide text-slate-500">
            Config Controls
          </h3>

          {config.loading ? (
            <div className="mt-2">
              <SectionSkeleton rows={3} />
            </div>
          ) : config.error ? (
            <div className="mt-2">
              <SectionError onRetry={config.refetch} />
            </div>
          ) : (
            <form onSubmit={(e) => void handleConfigSave(e)} className="mt-3 space-y-3">
              <div className="flex flex-wrap items-end gap-4">
                <label className="block">
                  <span className="text-xs text-slate-500">
                    Cash buffer %
                  </span>
                  <input
                    type="number"
                    min={0}
                    max={50}
                    step={1}
                    value={displayedBufferPct}
                    onChange={(e) => {
                      const val = Number(e.target.value);
                      setCashBufferPct(val === serverBufferPct ? null : val);
                    }}
                    disabled={configSaving}
                    className="mt-1 block w-24 rounded border border-slate-300 px-2 py-1.5 text-sm"
                  />
                </label>

                <label className="block">
                  <span className="text-xs text-slate-500">CGT scenario</span>
                  <select
                    value={displayedCgtScenario}
                    onChange={(e) => {
                      const val = e.target.value as "basic" | "higher";
                      const serverScenario =
                        config.data?.cgt_scenario === "higher"
                          ? "higher"
                          : "basic";
                      setCgtScenario(val === serverScenario ? null : val);
                    }}
                    disabled={configSaving}
                    className="mt-1 block rounded border border-slate-300 px-2 py-1.5 text-sm"
                  >
                    <option value="basic">basic</option>
                    <option value="higher">higher</option>
                  </select>
                </label>
              </div>

              <label className="block">
                <span className="text-xs text-slate-500">
                  Reason (required)
                </span>
                <input
                  type="text"
                  value={configReason}
                  onChange={(e) => setConfigReason(e.target.value)}
                  disabled={configSaving}
                  placeholder="Why are you changing this?"
                  className="mt-1 block w-full max-w-md rounded border border-slate-300 px-2 py-1.5 text-sm"
                />
              </label>

              <button
                type="submit"
                disabled={configSaving || configReasonMissing || configNothingChanged}
                className="rounded bg-slate-800 px-3 py-1.5 text-sm font-medium text-white hover:bg-slate-700 disabled:cursor-not-allowed disabled:opacity-50"
              >
                {configSaving ? "Saving..." : "Save config"}
              </button>

              {configSuccess && (
                <p className="rounded bg-emerald-50 px-2 py-1.5 text-xs text-emerald-700">
                  Budget config updated.
                </p>
              )}
              {configError && (
                <p
                  role="alert"
                  className="rounded bg-rose-50 px-2 py-1.5 text-xs text-rose-700"
                >
                  Failed to save budget config. Check the browser console for
                  details.
                </p>
              )}
            </form>
          )}
        </div>

        {/* ---- Sub-section 2: Capital Event Form ---- */}
        <div className="border-t border-slate-100 pt-4">
          <h3 className="text-xs font-semibold uppercase tracking-wide text-slate-500">
            Record Capital Event
          </h3>

          <form
            onSubmit={(e) => void handleEventSubmit(e)}
            className="mt-3 space-y-3"
          >
            <div className="flex flex-wrap items-end gap-4">
              <label className="block">
                <span className="text-xs text-slate-500">Type</span>
                <select
                  value={eventType}
                  onChange={(e) =>
                    setEventType(
                      e.target.value as "injection" | "withdrawal",
                    )
                  }
                  disabled={eventSaving}
                  className="mt-1 block rounded border border-slate-300 px-2 py-1.5 text-sm"
                >
                  <option value="injection">injection</option>
                  <option value="withdrawal">withdrawal</option>
                </select>
              </label>

              <label className="block">
                <span className="text-xs text-slate-500">Amount</span>
                <input
                  type="number"
                  min={0}
                  step="any"
                  value={eventAmount}
                  onChange={(e) => setEventAmount(e.target.value)}
                  disabled={eventSaving}
                  placeholder="0.00"
                  className="mt-1 block w-32 rounded border border-slate-300 px-2 py-1.5 text-sm"
                />
              </label>

              <label className="block">
                <span className="text-xs text-slate-500">Currency</span>
                <select
                  value={eventCurrency}
                  onChange={(e) =>
                    setEventCurrency(e.target.value as "USD" | "GBP")
                  }
                  disabled={eventSaving}
                  className="mt-1 block rounded border border-slate-300 px-2 py-1.5 text-sm"
                >
                  <option value="USD">USD</option>
                  <option value="GBP">GBP</option>
                </select>
              </label>
            </div>

            <label className="block">
              <span className="text-xs text-slate-500">Note (optional)</span>
              <textarea
                value={eventNote}
                onChange={(e) => setEventNote(e.target.value)}
                disabled={eventSaving}
                rows={2}
                className="mt-1 block w-full max-w-md rounded border border-slate-300 px-2 py-1.5 text-sm"
              />
            </label>

            <button
              type="submit"
              disabled={eventSaving || eventAmountInvalid}
              className="rounded bg-slate-800 px-3 py-1.5 text-sm font-medium text-white hover:bg-slate-700 disabled:cursor-not-allowed disabled:opacity-50"
            >
              {eventSaving ? "Submitting..." : "Record event"}
            </button>

            {eventSuccess && (
              <p className="rounded bg-emerald-50 px-2 py-1.5 text-xs text-emerald-700">
                Capital event recorded.
              </p>
            )}
            {eventError && (
              <p
                role="alert"
                className="rounded bg-rose-50 px-2 py-1.5 text-xs text-rose-700"
              >
                Failed to record capital event. Check the browser console for
                details.
              </p>
            )}
          </form>
        </div>

        {/* ---- Sub-section 3: Capital Events History ---- */}
        <div className="border-t border-slate-100 pt-4">
          <h3 className="text-xs font-semibold uppercase tracking-wide text-slate-500">
            Capital Events History
          </h3>

          <div className="mt-3">
            {events.loading ? (
              <SectionSkeleton rows={3} />
            ) : events.error ? (
              <SectionError onRetry={events.refetch} />
            ) : events.data && events.data.length > 0 ? (
              <div className="overflow-x-auto">
                <table className="w-full text-left text-sm">
                  <thead>
                    <tr>
                      <th className="py-1.5 text-xs text-slate-500">Time</th>
                      <th className="py-1.5 text-xs text-slate-500">Type</th>
                      <th className="py-1.5 text-xs text-slate-500">Amount</th>
                      <th className="py-1.5 text-xs text-slate-500">
                        Currency
                      </th>
                      <th className="py-1.5 text-xs text-slate-500">Note</th>
                    </tr>
                  </thead>
                  <tbody>
                    {events.data.map((ev: CapitalEventResponse) => (
                      <tr key={ev.event_id}>
                        <td className="py-1.5">
                          {formatDateTime(ev.event_time)}
                        </td>
                        <td className="py-1.5">{ev.event_type}</td>
                        <td className="py-1.5">
                          {formatMoney(ev.amount, ev.currency)}
                        </td>
                        <td className="py-1.5">{ev.currency}</td>
                        <td className="py-1.5 text-slate-500">
                          {ev.note ?? "—"}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <p className="text-sm text-slate-500">
                No capital events recorded yet.
              </p>
            )}
          </div>
        </div>
      </div>
    </section>
  );
}
