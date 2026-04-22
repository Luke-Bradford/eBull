/**
 * Shared error-message constants for the operator setup wizard.
 *
 * GENERIC_ERROR is the single string every operator-setup fetch failure
 * maps to. Used by SetupPage.tsx + useSetupWizard.ts to preserve the
 * #98 non-leaky-error contract: an unauthenticated attacker must not
 * be able to distinguish failure modes of the /auth/setup endpoint.
 */
export const GENERIC_ERROR = "Setup unavailable or invalid token.";
