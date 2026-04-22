/**
 * /setup page — first-run wizard.
 *
 * Step 1: Operator
 *   Original single-step setup form (#106 / Ticket G). Captures the
 *   first operator's username, password, and (if running in non-loopback
 *   bootstrap-token mode) the one-shot setup token printed to the
 *   server log. POST /auth/setup. Generic-error mapping is preserved --
 *   every non-2xx surfaces the same fixed string so the page never
 *   leaks which input was wrong (#98). See useSetupWizard.ts — the
 *   OPERATOR_SUBMIT_ERROR action carries no payload; the reducer
 *   hard-codes GENERIC_ERROR unconditionally.
 *
 * Step 2: Broker credential (optional)  -- ADR-0003 Ticket 2c (#122)
 *   Updated in #139 PR D for two-key credential model. The operator
 *   enters both API key and user key. Labels are fixed as "api_key"
 *   and "user_key"; environment is hardcoded to "demo" in v1.
 *
 *   The "Test connection" button calls POST /broker-credentials/validate,
 *   which is session-gated. This works because step 2 only runs after
 *   step 1 completes — POST /auth/setup sets the session cookie on its
 *   response, so the browser is already authenticated by the time step 2
 *   renders. The session cookie is present even though markAuthenticated
 *   is deferred (deferred for the React in-memory flag, not the cookie).
 *
 *   Credential-set mode detection mirrors SettingsPage: after a partial
 *   save failure, the form re-derives mode from the credential list and
 *   shows only the missing key's field. See useSetupWizard.ts —
 *   submitBroker re-fetches credRows on save failure for repair-mode
 *   derivation.
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
 *
 * State management (#327):
 *   Wizard state machine lives in useSetupWizard (step + submit/error/
 *   validation flags + credRows). Form-field inputs (username, password,
 *   setupToken, brokerApiKey, brokerUserKey) stay as component-local
 *   useState — field churn doesn't belong in the state machine. Derived
 *   broker-mode (create/repair/complete) stays a pure selector over
 *   state.credRows via deriveCredentialSetMode.
 */

import { useCallback, useEffect, useRef, useState } from "react";
import type { FormEvent } from "react";
import { useNavigate } from "react-router-dom";

import { ValidationResultDisplay } from "@/components/broker/ValidationResultDisplay";
import { useRecoveryPhraseModal } from "@/components/security/RecoveryPhraseModal";
import { deriveCredentialSetMode } from "@/lib/credentialSetMode";
import { useSession } from "@/lib/session";
import { GENERIC_ERROR } from "@/pages/setupErrorMessages";
import { useSetupWizard } from "@/pages/useSetupWizard";

const MIN_PASSWORD_LEN = 12;
const MIN_SECRET_LEN = 4;

// Re-export so tests that import GENERIC_ERROR via this module keep working.
export { GENERIC_ERROR };

export function SetupPage(): JSX.Element {
  const { status, markAuthenticated } = useSession();
  const navigate = useNavigate();

  // Form-field state (stays local per #327 design — field churn doesn't
  // belong in the wizard state machine).
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [setupToken, setSetupToken] = useState("");
  const [brokerApiKey, setBrokerApiKey] = useState("");
  const [brokerUserKey, setBrokerUserKey] = useState("");

  // Stable-identity `onComplete` for useSetupWizard. The hook memoises
  // skipBroker/completeWizard against `onComplete` identity, so a new
  // inline arrow each render would re-create those dispatchers on
  // every wizard state tick — defeats the hook's own useCallback.
  //
  // Fix: wrap a ref. `onComplete` reads `completeRef.current` (stable
  // identity via useCallback([])). The ref is updated via useEffect
  // whenever the real `completeWizard` identity changes, so `pendingOperator`
  // is always fresh at call time.
  const completeRef = useRef<() => void>(() => {});
  const onComplete = useCallback(() => completeRef.current(), []);
  const wizard = useSetupWizard({ onComplete });

  const completeWizard = useCallback((): void => {
    if (wizard.state.pendingOperator !== null) {
      markAuthenticated(wizard.state.pendingOperator);
    }
    navigate("/", { replace: true });
  }, [markAuthenticated, navigate, wizard.state.pendingOperator]);

  // Keep the ref pointing at the latest completeWizard closure so the
  // stable `onComplete` reads a fresh pendingOperator on each invocation.
  useEffect(() => {
    completeRef.current = completeWizard;
  }, [completeWizard]);

  const derived = deriveCredentialSetMode(wizard.state.credRows);
  const mode = derived.mode;
  const missingLabel = derived.missingLabel;

  // Fetch existing credentials when entering step 2 so partial-save
  // state from a prior session is correctly detected.
  useEffect(() => {
    if (wizard.state.step === "broker") {
      void wizard.loadCredentials();
    }
    // loadCredentials is useCallback([]) so identity is stable; include
    // it anyway for lint cleanliness.
  }, [wizard.state.step, wizard.loadCredentials]);

  // Clear stale validation result when inputs change or mode transitions.
  // VALIDATION_START only fires on explicit Test-connection click, so
  // typing into either key field without clicking Test would otherwise
  // leave prior pass/fail banner on screen against stale values. This
  // effect dispatches VALIDATION_CLEAR-equivalent via the hook's helper.
  useEffect(() => {
    if (wizard.state.validation !== null || wizard.state.validationError !== null) {
      wizard.clearValidation();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [brokerApiKey, brokerUserKey, mode]);

  const phraseModal = useRecoveryPhraseModal({
    onClose: completeWizard,
  });

  useEffect(() => {
    if (status === "authenticated") {
      navigate("/", { replace: true });
    } else if (status === "unauthenticated") {
      navigate("/login", { replace: true });
    }
  }, [status, navigate]);

  async function handleOperatorSubmit(e: FormEvent<HTMLFormElement>): Promise<void> {
    e.preventDefault();
    await wizard.submitOperator({ username, password, setupToken });
  }

  async function handleTestConnection(): Promise<void> {
    await wizard.validateCredentials({ apiKey: brokerApiKey, userKey: brokerUserKey });
  }

  async function handleBrokerSubmit(e: FormEvent<HTMLFormElement>): Promise<void> {
    e.preventDefault();
    const result = await wizard.submitBroker({ apiKey: brokerApiKey, userKey: brokerUserKey });
    if (!result.ok) return;
    setBrokerApiKey("");
    setBrokerUserKey("");
    if (result.recoveryPhrase !== null) {
      phraseModal.open(result.recoveryPhrase);
      return;
    }
    completeWizard();
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

  const submitting = wizard.state.operatorSubmitting;
  const error = wizard.state.operatorError;
  const brokerSubmitting = wizard.state.brokerSubmitting;
  const brokerError = wizard.state.brokerError;
  const validating = wizard.state.validating;
  const validationResult = wizard.state.validation;
  const validationError = wizard.state.validationError;
  const step = wizard.state.step;

  const showApiKeyField = mode === "create" || (mode === "repair" && missingLabel === "api_key");
  const showUserKeyField = mode === "create" || (mode === "repair" && missingLabel === "user_key");
  const canTestConnection =
    mode === "create" && brokerApiKey.length >= MIN_SECRET_LEN && brokerUserKey.length >= MIN_SECRET_LEN;
  const canSave =
    !brokerSubmitting &&
    (showApiKeyField ? brokerApiKey.length >= MIN_SECRET_LEN : true) &&
    (showUserKeyField ? brokerUserKey.length >= MIN_SECRET_LEN : true);

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
      ) : mode === "complete" ? (
        <div className="w-full max-w-sm rounded border border-slate-200 bg-white p-6 shadow-sm">
          <h1 className="mb-1 text-lg font-semibold text-slate-800">
            Credentials configured
          </h1>
          <p className="mb-4 text-xs text-slate-500">
            Both eToro credentials are saved. You can manage them from Settings.
          </p>
          <button
            type="button"
            onClick={completeWizard}
            className="w-full rounded bg-slate-800 py-2 text-sm font-medium text-white"
          >
            Continue
          </button>
        </div>
      ) : (
        <form
          onSubmit={handleBrokerSubmit}
          className="w-full max-w-sm space-y-3 rounded border border-slate-200 bg-white p-6 shadow-sm"
        >
          <h1 className="text-lg font-semibold text-slate-800">
            {mode === "repair" ? "Complete credential setup" : "Add eToro credentials"}
          </h1>
          <p className="text-xs text-slate-500">
            {mode === "repair"
              ? `One key was already saved. Enter the missing ${missingLabel === "api_key" ? "API key" : "user key"} to complete the credential pair.`
              : "You can add your eToro API key and user key now or skip this step and add them later from Settings. eBull will not place any orders until both credentials are saved."}
          </p>

          {showApiKeyField && (
            <label className="block text-sm">
              <span className="mb-1 block text-slate-600">API key</span>
              <input
                type="password"
                name="broker-credential-api-key"
                autoComplete="new-password"
                value={brokerApiKey}
                onChange={(e) => setBrokerApiKey(e.target.value)}
                minLength={MIN_SECRET_LEN}
                required
                className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
              />
            </label>
          )}

          {showUserKeyField && (
            <label className="block text-sm">
              <span className="mb-1 block text-slate-600">User key</span>
              <input
                type="password"
                name="broker-credential-user-key"
                autoComplete="new-password"
                value={brokerUserKey}
                onChange={(e) => setBrokerUserKey(e.target.value)}
                minLength={MIN_SECRET_LEN}
                required
                className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
              />
            </label>
          )}

          {/* Test connection — only in Create mode (both keys present). */}
          <div className="space-y-1">
            <button
              type="button"
              onClick={() => void handleTestConnection()}
              disabled={!canTestConnection || validating}
              className="rounded border border-slate-300 px-3 py-1.5 text-sm text-slate-700 hover:bg-slate-50 disabled:opacity-50"
            >
              {validating ? "Testing…" : "Test connection"}
            </button>
            {mode === "repair" && (
              <p className="text-xs text-slate-400">
                Connection testing requires both keys. The already-saved key cannot be read back.
              </p>
            )}
          </div>

          <ValidationResultDisplay
            result={validationResult}
            error={validationError}
          />

          {brokerError !== null && (
            <div
              role="alert"
              className="rounded bg-rose-50 px-2 py-1.5 text-xs text-rose-700"
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
              disabled={!canSave}
              className="flex-1 rounded bg-slate-800 py-2 text-sm font-medium text-white disabled:bg-slate-400"
            >
              {brokerSubmitting ? "Saving…" : "Save credentials"}
            </button>
          </div>
        </form>
      )}
      {phraseModal.element}
    </div>
  );
}
