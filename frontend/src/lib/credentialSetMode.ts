/**
 * Credential-set mode detection for the eToro two-key model.
 *
 * Shared between SettingsPage and SetupPage. The eToro provider
 * requires exactly two active credential rows (label="api_key" and
 * label="user_key") for provider="etoro" in a given environment.
 * This function inspects the loaded credential list to derive the
 * current UI mode.
 */

import type { BrokerCredentialView } from "@/api/brokerCredentials";

export type CredentialSetMode = "create" | "repair" | "complete";

export const ENVIRONMENT = "demo";

export function deriveCredentialSetMode(
  rows: BrokerCredentialView[] | null,
): { mode: CredentialSetMode; missingLabel: "api_key" | "user_key" | null } {
  if (rows === null) return { mode: "create", missingLabel: null };

  const active = rows.filter(
    (r) =>
      r.provider === "etoro" &&
      r.environment === ENVIRONMENT &&
      r.revoked_at === null,
  );
  const hasApiKey = active.some((r) => r.label === "api_key");
  const hasUserKey = active.some((r) => r.label === "user_key");

  if (hasApiKey && hasUserKey) return { mode: "complete", missingLabel: null };
  if (hasApiKey) return { mode: "repair", missingLabel: "user_key" };
  if (hasUserKey) return { mode: "repair", missingLabel: "api_key" };
  return { mode: "create", missingLabel: null };
}
