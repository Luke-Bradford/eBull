/**
 * /setup page — first-run wizard.
 *
 * Step 1: Operator
 *   Original single-step setup form (#106 / Ticket G). Captures the
 *   first operator's username, password, and (if running in non-loopback
 *   bootstrap-token mode) the one-shot setup token printed to the
 *   server log. POST /auth/setup. Generic-404 mapping is preserved --
 *   every non-2xx surfaces the same fixed string so the page never
 *   leaks which input was wrong.
 *
 * Step 2: Broker credential (optional)  -- ADR-0003 Ticket 2c (#122)
 *   Brand-new step. The operator MAY enter a broker credential right
 *   here, in which case the same first-save flow as the post-onboarding
 *   broker-credentials section runs inline (see SettingsPage / #121).
 *   The step is fully skippable: "Skip for now" navigates to / without
 *   storing anything, and the operator can save their first credential
 *   later from /settings.
 *
 *   Edge case 2 (ADR-0003 §5 row 3): if the broker key file already
 *   exists at wizard time, the backend silently re-uses it and the
 *   POST /broker-credentials response carries no recovery_phrase. The
 *   wizard then completes with NO modal shown -- "no phrase displayed
 *   at any point". The frontend does not need a separate signal for
 *   this case; the absence of `recovery_phrase` on the response is
 *   the same flag #121 already keys off, and that single condition
 *   covers both "fresh install, lazy gen ran" and "key file already
 *   present".
 *
 * markAuthenticated discipline (#122):
 *   In the single-step incarnation we called markAuthenticated AND
 *   navigate("/") inside the operator-create success path. With the
 *   second step in play we MUST defer markAuthenticated until the
 *   wizard is fully done -- otherwise the redirect-on-authenticated
 *   effect would bounce the operator off /setup before they ever saw
 *   step 2. The cookie is already set by /auth/setup itself, so the
 *   broker-credential POST in step 2 still authenticates correctly
 *   even with the in-memory React flag deferred.
 */

import { useEffect, useState } from "react";
import type { FormEvent } from "react";
import { useNavigate } from "react-router-dom";

import { ApiError } from "@/api/client";
import { postSetup } from "@/api/auth";
import type { Operator } from "@/api/auth";
import { createBrokerCredential } from "@/api/brokerCredentials";
import { useRecoveryPhraseModal } from "@/components/security/RecoveryPhraseModal";
import { useSession } from "@/lib/session";

const GENERIC_ERROR = "Setup unavailable or invalid token.";
const MIN_PASSWORD_LEN = 12;
const MIN_SECRET_LEN = 4;
const BROKER_GENERIC_ERROR = "Could not save credential.";
const BROKER_CONFLICT_ERROR =
  "A credential with that label already exists for this provider.";
const BROKER_VALIDATION_ERROR = "Provider, label, or secret is invalid.";

type WizardStep = "operator" | "broker";

export function SetupPage(): JSX.Element {
  const { status, markAuthenticated } = useSession();
  const navigate = useNavigate();

  const [step, setStep] = useState<WizardStep>("operator");
  // The operator is created at the end of step 1 and held here until
  // the wizard completes. markAuthenticated is deferred until then so
  // the redirect-on-authenticated effect below does not yank the
  // operator off the page mid-wizard.
  const [pendingOperator, setPendingOperator] = useState<Operator | null>(null);

  // Step 1 form state.
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [setupToken, setSetupToken] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Step 2 form state. Provider is locked to "etoro" -- mirroring the
  // SettingsPage broker-credentials section.
  const [brokerLabel, setBrokerLabel] = useState("");
  const [brokerSecret, setBrokerSecret] = useState("");
  const [brokerSubmitting, setBrokerSubmitting] = useState(false);
  const [brokerError, setBrokerError] = useState<string | null>(null);

  function completeWizard(): void {
    // Single shared "wizard is finished" path. Used by:
    //   - "Skip for now" on step 2 (no credential stored)
    //   - successful save with no recovery_phrase (edge case 2)
    //   - the recovery-phrase modal close (challenge confirm OR
    //     "Close anyway" -- both run through the hook's onClose)
    // Flips the in-memory session flag and navigates to /. The cookie
    // was set by /auth/setup back in step 1, so the operator is
    // already authenticated as far as the backend is concerned; this
    // just makes RequireAuth let them through.
    if (pendingOperator !== null) {
      markAuthenticated(pendingOperator);
    }
    navigate("/", { replace: true });
  }

  const phraseModal = useRecoveryPhraseModal({
    onClose: completeWizard,
  });

  // Bootstrap bounce: if the SessionProvider has already determined
  // the operator is fully authenticated (e.g. they hit /setup directly
  // after a previous setup completed), bounce off this page. We
  // deliberately do NOT bounce on `status === "authenticated"` while
  // the wizard is mid-flight -- markAuthenticated only runs at the
  // end of the wizard, so during steps 1-2 the SessionProvider's
  // status is still "needs_setup" and this effect is a no-op.
  useEffect(() => {
    if (status === "authenticated") {
      navigate("/", { replace: true });
    } else if (status === "unauthenticated") {
      navigate("/login", { replace: true });
    }
  }, [status, navigate]);

  async function handleOperatorSubmit(e: FormEvent<HTMLFormElement>): Promise<void> {
    e.preventDefault();
    setError(null);
    setSubmitting(true);
    try {
      const { operator } = await postSetup(
        username,
        password,
        setupToken.trim() === "" ? null : setupToken.trim(),
      );
      // Stash the operator for the deferred markAuthenticated call
      // and advance to step 2. The cookie is already on the response
      // so the broker POST in step 2 will authenticate.
      setPendingOperator(operator);
      setStep("broker");
    } catch {
      // Single fixed phrase for every failure mode -- matches the
      // backend's generic-404 discipline.
      setError(GENERIC_ERROR);
    } finally {
      setSubmitting(false);
    }
  }

  async function handleBrokerSubmit(e: FormEvent<HTMLFormElement>): Promise<void> {
    e.preventDefault();
    setBrokerError(null);
    setBrokerSubmitting(true);
    try {
      const response = await createBrokerCredential({
        provider: "etoro",
        label: brokerLabel,
        secret: brokerSecret,
      });
      // Drop the secret from local state immediately on success.
      setBrokerSecret("");
      if (
        response.recovery_phrase != null &&
        response.recovery_phrase.length > 0
      ) {
        // Lazy-gen path: open the phrase modal. completeWizard runs
        // from the modal's onClose so navigation only happens after
        // the operator has acknowledged the phrase (or accepted the
        // installation-wide warning on the cancel gate).
        phraseModal.open(response.recovery_phrase);
      } else {
        // Edge case 2: a key file already exists, no phrase to show.
        // Wizard is done.
        completeWizard();
      }
    } catch (err: unknown) {
      // Operator stays on step 2 with form values preserved (except
      // the secret, which is preserved here too because we have not
      // confirmed that it actually committed). "Skip for now" remains
      // available so a transient backend failure does not lock the
      // operator out of completing setup.
      if (err instanceof ApiError && err.status === 409) {
        setBrokerError(BROKER_CONFLICT_ERROR);
      } else if (err instanceof ApiError && err.status === 400) {
        setBrokerError(BROKER_VALIDATION_ERROR);
      } else {
        setBrokerError(BROKER_GENERIC_ERROR);
      }
    } finally {
      setBrokerSubmitting(false);
    }
  }

  function handleSkipBroker(): void {
    completeWizard();
  }

  if (status === "loading") {
    return (
      <div className="flex h-screen w-screen items-center justify-center text-sm text-slate-400">
        Loading…
      </div>
    );
  }

  return (
    <div className="flex h-screen w-screen items-center justify-center bg-slate-50">
      {step === "operator" ? (
        <form
          onSubmit={handleOperatorSubmit}
          className="w-full max-w-sm rounded border border-slate-200 bg-white p-6 shadow-sm"
        >
          <h1 className="mb-1 text-lg font-semibold text-slate-800">
            First-run setup
          </h1>
          <p className="mb-4 text-xs text-slate-500">
            Create the first operator for this eBull instance. If the server
            printed a setup token at startup, paste it below.
          </p>
          <label className="mb-3 block text-sm">
            <span className="mb-1 block text-slate-600">Username</span>
            <input
              type="text"
              autoComplete="username"
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              required
              className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
            />
          </label>
          <label className="mb-3 block text-sm">
            <span className="mb-1 block text-slate-600">
              Password{" "}
              <span className="text-slate-400">(min {MIN_PASSWORD_LEN} chars)</span>
            </span>
            <input
              type="password"
              autoComplete="new-password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              minLength={MIN_PASSWORD_LEN}
              required
              className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
            />
          </label>
          <label className="mb-4 block text-sm">
            <span className="mb-1 block text-slate-600">
              Setup token{" "}
              <span className="text-slate-400">(optional on loopback)</span>
            </span>
            <input
              type="text"
              autoComplete="off"
              value={setupToken}
              onChange={(e) => setSetupToken(e.target.value)}
              className="w-full rounded border border-slate-300 px-2 py-1.5 font-mono text-xs"
            />
          </label>
          {error !== null && (
            <div
              role="alert"
              className="mb-3 rounded bg-rose-50 px-2 py-1.5 text-xs text-rose-700"
            >
              {error}
            </div>
          )}
          <button
            type="submit"
            disabled={
              submitting || username === "" || password.length < MIN_PASSWORD_LEN
            }
            className="w-full rounded bg-slate-800 py-2 text-sm font-medium text-white disabled:bg-slate-400"
          >
            {submitting ? "Creating…" : "Create operator"}
          </button>
        </form>
      ) : (
        <form
          onSubmit={handleBrokerSubmit}
          className="w-full max-w-sm rounded border border-slate-200 bg-white p-6 shadow-sm"
        >
          <h1 className="mb-1 text-lg font-semibold text-slate-800">
            Add a broker credential
          </h1>
          <p className="mb-4 text-xs text-slate-500">
            You can add an eToro credential now or skip this step and add one
            later from Settings. eBull will not place any orders until at least
            one credential is saved.
          </p>
          <label className="mb-3 block text-sm">
            <span className="mb-1 block text-slate-600">Provider</span>
            <select
              value="etoro"
              disabled
              className="w-full rounded border border-slate-300 bg-slate-50 px-2 py-1.5 text-sm text-slate-700"
            >
              <option value="etoro">eToro</option>
            </select>
          </label>
          <label className="mb-3 block text-sm">
            <span className="mb-1 block text-slate-600">Label</span>
            <input
              type="text"
              autoComplete="off"
              value={brokerLabel}
              onChange={(e) => setBrokerLabel(e.target.value)}
              className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
            />
          </label>
          <label className="mb-4 block text-sm">
            <span className="mb-1 block text-slate-600">Secret</span>
            <input
              type="password"
              autoComplete="off"
              value={brokerSecret}
              onChange={(e) => setBrokerSecret(e.target.value)}
              minLength={MIN_SECRET_LEN}
              className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
            />
          </label>
          {brokerError !== null && (
            <div
              role="alert"
              className="mb-3 rounded bg-rose-50 px-2 py-1.5 text-xs text-rose-700"
            >
              {brokerError}
            </div>
          )}
          <div className="flex gap-2">
            <button
              type="button"
              onClick={handleSkipBroker}
              disabled={brokerSubmitting}
              className="flex-1 rounded border border-slate-300 bg-white py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 disabled:opacity-50"
            >
              Skip for now
            </button>
            <button
              type="submit"
              disabled={
                brokerSubmitting ||
                brokerLabel.trim() === "" ||
                brokerSecret.length < MIN_SECRET_LEN
              }
              className="flex-1 rounded bg-slate-800 py-2 text-sm font-medium text-white disabled:bg-slate-400"
            >
              {brokerSubmitting ? "Saving…" : "Save credential"}
            </button>
          </div>
        </form>
      )}
      {phraseModal.element}
    </div>
  );
}
