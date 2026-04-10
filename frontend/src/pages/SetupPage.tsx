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
 *   shows only the missing key's field.
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
import {
  type BrokerCredentialView,
  type ValidateCredentialResponse,
  createBrokerCredential,
  listBrokerCredentials,
  validateBrokerCredential,
} from "@/api/brokerCredentials";
import { useRecoveryPhraseModal } from "@/components/security/RecoveryPhraseModal";
import { deriveCredentialSetMode, ENVIRONMENT } from "@/lib/credentialSetMode";
import { useSession } from "@/lib/session";

const GENERIC_ERROR = "Setup unavailable or invalid token.";
const MIN_PASSWORD_LEN = 12;
const MIN_SECRET_LEN = 4;

type WizardStep = "operator" | "broker";

export function SetupPage(): JSX.Element {
  const { status, markAuthenticated } = useSession();
  const navigate = useNavigate();

  const [step, setStep] = useState<WizardStep>("operator");
  const [pendingOperator, setPendingOperator] = useState<Operator | null>(null);

  // Step 1 form state.
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [setupToken, setSetupToken] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Step 2 form state — two-key model.
  const [brokerApiKey, setBrokerApiKey] = useState("");
  const [brokerUserKey, setBrokerUserKey] = useState("");
  const [brokerSubmitting, setBrokerSubmitting] = useState(false);
  const [brokerError, setBrokerError] = useState<string | null>(null);

  // Credential-set mode detection for partial-save recovery.
  const [credRows, setCredRows] = useState<BrokerCredentialView[] | null>(null);
  const derived = deriveCredentialSetMode(credRows);
  const mode = derived.mode;
  const missingLabel = derived.missingLabel;

  // Validation state.
  const [validating, setValidating] = useState(false);
  const [validationResult, setValidationResult] =
    useState<ValidateCredentialResponse | null>(null);
  const [validationError, setValidationError] = useState<string | null>(null);

  // Fetch existing credentials when entering step 2 so that
  // partial-save state from a prior session is correctly detected.
  useEffect(() => {
    if (step === "broker") {
      void refreshCredentials();
    }
  }, [step]);

  // Clear stale validation result when inputs change or mode transitions.
  useEffect(() => {
    setValidationResult(null);
    setValidationError(null);
  }, [brokerApiKey, brokerUserKey, mode]);

  function completeWizard(): void {
    if (pendingOperator !== null) {
      markAuthenticated(pendingOperator);
    }
    navigate("/", { replace: true });
  }

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
    setError(null);
    setSubmitting(true);
    try {
      const { operator } = await postSetup(
        username,
        password,
        setupToken.trim() === "" ? null : setupToken.trim(),
      );
      setPendingOperator(operator);
      setStep("broker");
    } catch {
      setError(GENERIC_ERROR);
    } finally {
      setSubmitting(false);
    }
  }

  async function refreshCredentials(): Promise<void> {
    try {
      const data = await listBrokerCredentials();
      setCredRows(data);
    } catch {
      // On the setup page, failure to list credentials is non-fatal —
      // the operator can still enter both keys (Create mode).
      setCredRows(null);
    }
  }

  async function handleTestConnection(): Promise<void> {
    setValidationResult(null);
    setValidationError(null);
    setValidating(true);
    try {
      const result = await validateBrokerCredential({
        api_key: brokerApiKey,
        user_key: brokerUserKey,
        environment: ENVIRONMENT,
      });
      setValidationResult(result);
    } catch {
      setValidationError("Could not reach the validation endpoint.");
    } finally {
      setValidating(false);
    }
  }

  async function handleBrokerSubmit(e: FormEvent<HTMLFormElement>): Promise<void> {
    e.preventDefault();
    setBrokerError(null);
    setBrokerSubmitting(true);
    try {
      let phrase: readonly string[] | null = null;

      // Save api_key if needed (Create mode or Repair with api_key missing).
      if (mode === "create" || (mode === "repair" && missingLabel === "api_key")) {
        const response = await createBrokerCredential({
          provider: "etoro",
          label: "api_key",
          environment: ENVIRONMENT,
          secret: brokerApiKey,
        });
        if (response.recovery_phrase != null && response.recovery_phrase.length > 0) {
          phrase = response.recovery_phrase;
        }
      }

      // Save user_key if needed (Create mode or Repair with user_key missing).
      if (mode === "create" || (mode === "repair" && missingLabel === "user_key")) {
        await createBrokerCredential({
          provider: "etoro",
          label: "user_key",
          environment: ENVIRONMENT,
          secret: brokerUserKey,
        });
      }

      setBrokerApiKey("");
      setBrokerUserKey("");

      // If the first save triggered a recovery phrase, show the modal
      // now that both credentials are durable.
      if (phrase !== null) {
        phraseModal.open(phrase);
        return;
      }

      completeWizard();
    } catch (err: unknown) {
      if (err instanceof ApiError && err.status === 409) {
        setBrokerError("A credential with that label already exists. Revoke it from Settings to replace.");
      } else if (err instanceof ApiError && err.status === 400) {
        setBrokerError("Invalid API key or user key value.");
      } else {
        setBrokerError("Could not save credential.");
      }
      // Re-derive mode from the refreshed list in case the first call
      // succeeded but the second failed (partial state).
      await refreshCredentials();
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

  const showApiKeyField = mode === "create" || (mode === "repair" && missingLabel === "api_key");
  const showUserKeyField = mode === "create" || (mode === "repair" && missingLabel === "user_key");
  const canTestConnection = mode === "create" && brokerApiKey.length >= MIN_SECRET_LEN && brokerUserKey.length >= MIN_SECRET_LEN;
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

          {/* Validation result display. */}
          {validationError !== null && (
            <div role="alert" className="rounded bg-rose-50 px-2 py-1.5 text-xs text-rose-700">
              {validationError}
            </div>
          )}
          {validationResult !== null && !validationResult.auth_valid && (
            <div role="alert" className="rounded bg-rose-50 px-2 py-1.5 text-xs text-rose-700">
              Authentication failed — check your API key and user key.
            </div>
          )}
          {validationResult !== null && validationResult.auth_valid && !validationResult.env_valid && (
            <div className="space-y-1">
              <div className="rounded bg-amber-50 px-2 py-1.5 text-xs text-amber-700">
                Authenticated, but environment check failed: {validationResult.env_check}
              </div>
              {validationResult.note && (
                <p className="text-xs text-slate-400">{validationResult.note}</p>
              )}
            </div>
          )}
          {validationResult !== null && validationResult.auth_valid && validationResult.env_valid && (
            <div className="space-y-1">
              <div className="rounded bg-emerald-50 px-2 py-1.5 text-xs text-emerald-700">
                Connection verified
                {validationResult.identity?.gcid != null && (
                  <span className="ml-1 text-emerald-600">
                    (account {validationResult.identity.gcid})
                  </span>
                )}
              </div>
              {validationResult.note && (
                <p className="text-xs text-slate-400">{validationResult.note}</p>
              )}
            </div>
          )}

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
