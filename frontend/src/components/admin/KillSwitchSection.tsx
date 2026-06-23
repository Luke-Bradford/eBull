/**
 * Kill-switch toggle panel (#1231).
 *
 * The one operator-facing surface that can DEACTIVATE the system-wide
 * kill switch without raw SQL. Lives on the admin page only — per
 * `.claude/skills/frontend/safety-state-ui.md` ("mutate on admin
 * surfaces only") and the settled `kill switch separate from config
 * flags` decision. Display surfaces (dashboard banner) stay read-only.
 *
 * State + attribution:
 *   - Live state comes from the shared `useConfig()` context, so a
 *     successful toggle's `refetch()` updates this pill AND every other
 *     consumer (the operator banner) at once — no second fetch.
 *   - `activated_by` is the authenticated operator's username
 *     (`useSession`). The backend requires non-empty `reason` +
 *     `activated_by` on every transition (422 otherwise).
 *
 * Confirm UX mirrors the post-#1264 convention: a `Modal` double-confirm
 * (no typed phrase) with a mandatory reason as the deliberate friction.
 */
import { useState } from "react";

import { ApiError } from "@/api/client";
import { postKillSwitch } from "@/api/config";
import { Modal } from "@/components/ui/Modal";
import { SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { useConfig } from "@/lib/ConfigContext";
import { formatDateTime } from "@/lib/format";
import { useSession } from "@/lib/session";

// Pre-fill options for the required reason field (ticket #1231). The
// operator can edit the text after selecting, or type their own.
const COMMON_REASONS = [
  "manual ops",
  "investigating outage",
  "after maintenance",
] as const;

export function KillSwitchSection(): JSX.Element {
  const config = useConfig();
  const { operator } = useSession();

  const [modalOpen, setModalOpen] = useState(false);
  const [reason, setReason] = useState<string>(COMMON_REASONS[0]);
  const [submitting, setSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState<string | null>(null);

  const refetchConfig = config.refetch;
  const ks = config.data?.kill_switch ?? null;
  const active = ks?.active ?? false;
  // Audit attribution must be a REAL identity — never a fabricated
  // fallback string. The backend's non-empty check would happily accept
  // a literal "operator", silently writing a false runtime_config_audit
  // row. Block the transition instead when no operator is authenticated.
  const operatorName = operator?.username ?? null;

  function openModal(): void {
    setReason(COMMON_REASONS[0]);
    setSubmitError(null);
    setModalOpen(true);
  }

  function closeModal(): void {
    if (submitting) return;
    setModalOpen(false);
  }

  const canSubmit =
    reason.trim().length > 0 && operatorName !== null && !submitting;

  async function handleConfirm(): Promise<void> {
    if (!canSubmit || operatorName === null) return;
    setSubmitting(true);
    setSubmitError(null);
    try {
      await postKillSwitch({
        // Toggle to the opposite of the current live state.
        active: !active,
        reason: reason.trim(),
        activated_by: operatorName,
      });
      // Success: reset busy state, close the modal, THEN refetch the
      // shared config. Closing first means any refetch error surfaces on
      // the section (shared context), never hidden under the overlay
      // (prevention-log: "refresh under modal").
      setSubmitting(false);
      setModalOpen(false);
      refetchConfig();
    } catch (err) {
      // Reset busy on the error path too (prevention-log: reset
      // submitting on both branches). Fixed phrases only — never echo
      // exception text into the DOM.
      setSubmitting(false);
      if (err instanceof ApiError && err.status === 503) {
        setSubmitError(
          "Kill switch unavailable — the config singleton row is missing. Re-seed it before toggling.",
        );
      } else {
        setSubmitError("Failed to update the kill switch. Check the browser console for details.");
      }
      // eslint-disable-next-line no-console
      console.error("Kill switch toggle failed:", err);
    }
  }

  return (
    <section className="space-y-3 rounded border border-slate-200 bg-white p-4 dark:border-slate-800 dark:bg-slate-900">
      <div>
        <h2 className="text-sm font-semibold text-slate-700 dark:text-slate-200">
          Kill switch
        </h2>
        <p className="text-xs text-slate-500 dark:text-slate-400">
          DB-backed runtime flag. When active, every operator-action trigger
          and all order execution is blocked until it is deactivated.
        </p>
      </div>

      {config.loading && config.data === null ? (
        <SectionSkeleton rows={2} />
      ) : config.error !== null && config.data === null ? (
        <SectionError onRetry={refetchConfig} />
      ) : (
        <div className="flex flex-wrap items-center gap-3">
          <KillSwitchPill active={active} />
          {active && (
            <span className="text-xs text-slate-500 dark:text-slate-400">
              since {formatDateTime(ks?.activated_at ?? null)}
              {ks?.activated_by ? ` · ${ks.activated_by}` : ""}
              {ks?.reason ? ` · ${ks.reason}` : ""}
            </span>
          )}
          <button
            type="button"
            onClick={openModal}
            className={
              active
                ? "ml-auto rounded border border-red-300 bg-red-50 px-3 py-1.5 text-sm font-medium text-red-700 hover:bg-red-100 dark:border-red-800 dark:bg-red-950/40 dark:text-red-300"
                : "ml-auto rounded border border-slate-300 bg-white px-3 py-1.5 text-sm font-medium text-slate-700 hover:bg-slate-50 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-200 dark:hover:bg-slate-800/40"
            }
          >
            {active ? "Deactivate kill switch" : "Activate kill switch"}
          </button>
        </div>
      )}

      {modalOpen && (
        <Modal isOpen={true} onRequestClose={closeModal} labelledBy="kill-switch-title">
          <h2
            id="kill-switch-title"
            className="text-sm font-semibold text-slate-800 dark:text-slate-100"
          >
            {active ? "Deactivate kill switch" : "Activate kill switch"}
          </h2>
          <p className="mt-2 text-sm text-slate-700 dark:text-slate-300">
            {active ? (
              <>
                Re-enables operator-action triggers and order execution. Only
                deactivate once you have confirmed the system is safe to trade.
              </>
            ) : (
              <>
                Blocks every operator-action trigger and all order execution
                until the kill switch is deactivated.
              </>
            )}
          </p>

          <p className="mt-4 text-xs font-semibold uppercase tracking-wide text-slate-500 dark:text-slate-400">
            Reason
          </p>
          <select
            aria-label="Common reason"
            value={COMMON_REASONS.includes(reason as (typeof COMMON_REASONS)[number]) ? reason : ""}
            onChange={(e) => {
              if (e.target.value !== "") setReason(e.target.value);
            }}
            disabled={submitting}
            className="mt-1 w-full rounded border border-slate-300 px-2 py-1.5 text-sm dark:border-slate-700 dark:bg-slate-900"
          >
            {COMMON_REASONS.map((r) => (
              <option key={r} value={r}>
                {r}
              </option>
            ))}
            <option value="">Other (type below)</option>
          </select>
          <input
            type="text"
            aria-label="Reason"
            value={reason}
            onChange={(e) => setReason(e.target.value)}
            disabled={submitting}
            placeholder="Reason for this transition"
            className="mt-2 w-full rounded border border-slate-300 px-2 py-1.5 text-sm dark:border-slate-700 dark:bg-slate-900"
          />
          {operatorName !== null ? (
            <p className="mt-2 text-xs text-slate-500 dark:text-slate-400">
              Attributed to {operatorName}.
            </p>
          ) : (
            <p
              role="alert"
              className="mt-2 text-xs font-medium text-red-700 dark:text-red-300"
            >
              No authenticated operator — cannot attribute this action. Sign in
              again before toggling.
            </p>
          )}

          {submitError !== null && (
            <div
              role="alert"
              className="mt-3 rounded bg-red-50 px-2 py-1.5 text-xs text-red-700 dark:bg-red-950/40 dark:text-red-300"
            >
              {submitError}
            </div>
          )}

          <div className="mt-4 flex justify-end gap-2">
            <button
              type="button"
              onClick={closeModal}
              disabled={submitting}
              className="rounded border border-slate-300 bg-white px-3 py-1 text-xs font-medium text-slate-700 hover:bg-slate-50 disabled:opacity-50 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-200 dark:hover:bg-slate-800/40"
            >
              Cancel
            </button>
            <button
              type="button"
              onClick={() => void handleConfirm()}
              disabled={!canSubmit}
              className="rounded bg-red-600 px-3 py-1 text-xs font-medium text-white hover:bg-red-700 disabled:opacity-50"
            >
              {submitting
                ? "Working…"
                : active
                  ? "Deactivate"
                  : "Activate"}
            </button>
          </div>
        </Modal>
      )}
    </section>
  );
}

function KillSwitchPill({ active }: { active: boolean }): JSX.Element {
  return (
    <span
      className={
        active
          ? "rounded px-1.5 py-0.5 text-[10px] font-medium uppercase tracking-wide bg-red-100 text-red-700 dark:bg-red-950/50 dark:text-red-300"
          : "rounded px-1.5 py-0.5 text-[10px] font-medium uppercase tracking-wide bg-emerald-100 text-emerald-700 dark:bg-emerald-950/50 dark:text-emerald-300"
      }
    >
      {active ? "Active" : "Inactive"}
    </span>
  );
}
